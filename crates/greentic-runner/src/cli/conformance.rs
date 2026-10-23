use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;
#[cfg(feature = "fault-injection")]
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(feature = "fault-injection")]
use anyhow::anyhow;
use anyhow::{Context, Result, bail};
use clap::{Parser, ValueEnum};
#[cfg(feature = "fault-injection")]
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use tokio::time::{Duration, timeout};
use walkdir::WalkDir;

use greentic_runner_host::RunnerWasiPolicy;
use greentic_runner_host::config::{
    FlowRetryConfig, HostConfig, OperatorPolicy, RateLimits, SecretsPolicy, StateStorePolicy,
    WebhookPolicy,
};
use greentic_runner_host::pack::PackRuntime;
use greentic_runner_host::secrets::default_manager;
#[cfg(feature = "fault-injection")]
use greentic_runner_host::testing::fault_injection::{
    FaultInjector, FaultSpec, clear_injector, set_injector, stats,
};
use greentic_runner_host::trace::{TraceConfig, TraceMode};
use greentic_runner_host::validate::ValidationConfig;
use greentic_runner_host::{Activity, HostBuilder, RunnerHost};

const DEFAULT_TIMEOUT_SECS: u64 = 8;
const PREFIX_FILTERS: &[&str] = &["messaging-", "events-", "secrets-"];

#[derive(Debug, Parser)]
pub struct ConformanceArgs {
    /// Directory containing *.gtpack
    #[arg(long, value_name = "DIR")]
    pub packs: PathBuf,

    /// Write JSON report to path
    #[arg(long, value_name = "PATH")]
    pub report: Option<PathBuf>,

    /// Conformance level
    #[arg(long, value_enum, default_value = "l1")]
    pub level: ConformanceLevel,

    /// Filter packs by provider name
    #[arg(long, value_name = "NAME")]
    pub provider: Option<String>,

    /// Stop on the first failure
    #[arg(long)]
    pub fail_fast: bool,

    /// Emit traces using existing trace infra
    #[arg(long)]
    pub trace: bool,

    /// Run a fault matrix (requires fault-injection feature)
    #[arg(long, value_name = "PATH")]
    pub faults: Option<PathBuf>,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum ConformanceLevel {
    L0,
    L1,
    L2,
}

#[derive(Serialize)]
struct ConformanceReport {
    summary: SummaryReport,
    packs: Vec<PackReport>,
}

#[derive(Serialize)]
struct SummaryReport {
    level: String,
    total: usize,
    passed: usize,
    failed: usize,
}

#[derive(Serialize)]
struct PackReport {
    pack: String,
    version: String,
    level: String,
    status: String,
    stage: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    case: Option<String>,
    diagnostics: Vec<Diagnostic>,
    timing_ms: TimingReport,
}

#[derive(Serialize)]
struct Diagnostic {
    code: String,
    message: String,
}

#[derive(Serialize, Default)]
struct TimingReport {
    resolve: u64,
    run: u64,
    state: u64,
}

#[cfg(feature = "fault-injection")]
#[derive(Deserialize, Serialize)]
struct FaultMatrix {
    cases: Vec<FaultCase>,
}

#[cfg(feature = "fault-injection")]
#[derive(Deserialize, Serialize)]
struct FaultCase {
    name: String,
    pack: String,
    #[serde(default)]
    flow: Option<String>,
    faults: Vec<FaultSpec>,
    expect: FaultExpect,
    #[serde(default)]
    seed: Option<u64>,
}

#[cfg(feature = "fault-injection")]
#[derive(Deserialize, Serialize)]
struct FaultExpect {
    retries: u32,
    status: String,
    dlq: bool,
}

#[cfg(feature = "fault-injection")]
struct InjectorGuard;

#[cfg(feature = "fault-injection")]
impl Drop for InjectorGuard {
    fn drop(&mut self) {
        clear_injector();
    }
}

pub async fn run(args: ConformanceArgs) -> Result<()> {
    let reports = if let Some(faults) = args.faults.as_ref() {
        #[cfg(feature = "fault-injection")]
        {
            run_fault_matrix(&args, faults).await?
        }
        #[cfg(not(feature = "fault-injection"))]
        {
            let _ = faults;
            bail!("fault injection support requires --features fault-injection");
        }
    } else {
        let mut reports = Vec::new();
        let pack_paths = discover_packs(&args.packs)?;
        if pack_paths.is_empty() {
            bail!("no .gtpack files found in {}", args.packs.display());
        }

        for pack_path in pack_paths {
            let report = run_pack(&args, &pack_path).await?;
            if let Some(report) = report {
                let failed = report.status == "fail";
                reports.push(report);
                if failed && args.fail_fast {
                    break;
                }
            }
        }
        reports
    };

    emit_report(&args, reports)
}

fn summarize(reports: &[PackReport], level: ConformanceLevel) -> SummaryReport {
    let mut passed = 0;
    let mut failed = 0;
    for report in reports {
        if report.status == "pass" {
            passed += 1;
        } else {
            failed += 1;
        }
    }
    SummaryReport {
        level: level_label(level),
        total: reports.len(),
        passed,
        failed,
    }
}

fn emit_report(args: &ConformanceArgs, reports: Vec<PackReport>) -> Result<()> {
    let summary = summarize(&reports, args.level);
    let payload = ConformanceReport {
        summary,
        packs: reports,
    };
    if let Some(path) = args.report.as_ref() {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create report dir {}", parent.display()))?;
        }
        let json = serde_json::to_vec_pretty(&payload)?;
        fs::write(path, json).with_context(|| format!("write report {}", path.display()))?;
    } else {
        println!("{}", serde_json::to_string_pretty(&payload)?);
    }
    ensure_report_passed(&payload.summary)
}

/// A conformance run only succeeds when at least one pack ran and none failed.
fn ensure_report_passed(summary: &SummaryReport) -> Result<()> {
    if summary.total == 0 {
        bail!("conformance ran no packs (is the --provider filter correct?)");
    }
    if summary.failed > 0 {
        bail!(
            "conformance failed: {} of {} packs failed",
            summary.failed,
            summary.total
        );
    }
    Ok(())
}

fn discover_packs(root: &Path) -> Result<Vec<PathBuf>> {
    if !root.exists() {
        bail!("packs directory {} does not exist", root.display());
    }
    let mut packs = Vec::new();
    for entry in WalkDir::new(root).into_iter().filter_map(Result::ok) {
        if entry.file_type().is_file()
            && entry
                .path()
                .extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext.eq_ignore_ascii_case("gtpack"))
                .unwrap_or(false)
        {
            packs.push(entry.path().to_path_buf());
        }
    }
    packs.sort();
    Ok(packs)
}

/// Run one pack against a fresh host. The host is always stopped, even when
/// the inner run bails out early with `?`.
async fn run_pack(args: &ConformanceArgs, pack_path: &Path) -> Result<Option<PackReport>> {
    let host = build_host(args.trace)?;
    host.start().await?;
    let outcome = run_pack_on_host(args, pack_path, &host).await;
    let stopped = host.stop().await;
    match (outcome, stopped) {
        (Err(err), _) => Err(err),
        (Ok(report), Ok(())) => Ok(report),
        (Ok(_), Err(stop_err)) => Err(stop_err),
    }
}

async fn run_pack_on_host(
    args: &ConformanceArgs,
    pack_path: &Path,
    host: &RunnerHost,
) -> Result<Option<PackReport>> {
    let timeout_limit = Duration::from_secs(DEFAULT_TIMEOUT_SECS);
    let mut timing = TimingReport::default();
    let mut diagnostics = Vec::new();

    let resolve_start = Instant::now();
    let load_result = timeout(timeout_limit, host.load_pack("conformance", pack_path)).await;
    timing.resolve = resolve_start.elapsed().as_millis() as u64;

    let load_result = match load_result {
        Ok(result) => result,
        Err(_) => {
            return Ok(Some(PackReport {
                pack: pack_path.display().to_string(),
                version: "unknown".to_string(),
                level: level_label(args.level),
                status: "fail".to_string(),
                stage: "resolve".to_string(),
                case: None,
                diagnostics: vec![Diagnostic {
                    code: "timeout".to_string(),
                    message: "pack load timed out".to_string(),
                }],
                timing_ms: timing,
            }));
        }
    };
    if let Err(err) = load_result {
        return Ok(Some(PackReport {
            pack: pack_path.display().to_string(),
            version: "unknown".to_string(),
            level: level_label(args.level),
            status: "fail".to_string(),
            stage: "resolve".to_string(),
            case: None,
            diagnostics: vec![Diagnostic {
                code: "load_error".to_string(),
                message: err.to_string(),
            }],
            timing_ms: timing,
        }));
    }

    let Some(tenant) = host.tenant("conformance").await else {
        bail!(
            "tenant runtime not available for pack {}",
            pack_path.display()
        );
    };
    let pack = tenant.pack();
    let pack_id = pack.metadata().pack_id.clone();
    let pack_version = pack.metadata().version.clone();

    if !matches_provider(&pack_id, args.provider.as_deref()) {
        return Ok(None);
    }

    let mut prefix_mismatch = false;
    if !matches_prefix(&pack_id) {
        prefix_mismatch = true;
    }

    let flow_descriptors = pack.list_flows().await?;
    if flow_descriptors.is_empty() {
        return Ok(Some(PackReport {
            pack: pack_id,
            version: pack_version,
            level: level_label(args.level),
            status: "fail".to_string(),
            stage: "resolve".to_string(),
            case: None,
            diagnostics: vec![Diagnostic {
                code: "no_flows".to_string(),
                message: "pack exports no flows".to_string(),
            }],
            timing_ms: timing,
        }));
    }

    if prefix_mismatch {
        diagnostics.push(Diagnostic {
            code: "prefix_mismatch".to_string(),
            message: format!(
                "pack id '{}' does not match expected prefixes {:?}",
                pack_id, PREFIX_FILTERS
            ),
        });
    }

    let entry_flow = select_entry_flow(&pack, &flow_descriptors)?;
    let flow_type = flow_descriptors
        .iter()
        .find(|flow| flow.id == entry_flow)
        .map(|flow| flow.flow_type.clone())
        .unwrap_or_else(|| "messaging".to_string());

    if matches!(args.level, ConformanceLevel::L0) {
        for flow in &flow_descriptors {
            pack.load_flow(flow.id.as_str())?;
        }
        return Ok(Some(PackReport {
            pack: pack_id,
            version: pack_version,
            level: level_label(args.level),
            status: "pass".to_string(),
            stage: "resolve".to_string(),
            case: None,
            diagnostics,
            timing_ms: timing,
        }));
    }

    let run_start = Instant::now();
    let session_id = if matches!(args.level, ConformanceLevel::L2) {
        Some("conformance-session")
    } else {
        None
    };
    let activity = build_activity(&pack_id, &entry_flow, &flow_type, session_id);
    let run_result = timeout(timeout_limit, host.handle_activity("conformance", activity)).await;
    timing.run = run_start.elapsed().as_millis() as u64;

    let run_result = match run_result {
        Ok(result) => result,
        Err(_) => {
            diagnostics.push(Diagnostic {
                code: "timeout".to_string(),
                message: "execution timed out".to_string(),
            });
            return Ok(Some(PackReport {
                pack: pack_id,
                version: pack_version,
                level: level_label(args.level),
                status: "fail".to_string(),
                stage: "execute".to_string(),
                case: None,
                diagnostics,
                timing_ms: timing,
            }));
        }
    };

    if let Err(err) = run_result {
        diagnostics.push(Diagnostic {
            code: "execute_error".to_string(),
            message: err.to_string(),
        });
        return Ok(Some(PackReport {
            pack: pack_id,
            version: pack_version,
            level: level_label(args.level),
            status: "fail".to_string(),
            stage: "execute".to_string(),
            case: None,
            diagnostics,
            timing_ms: timing,
        }));
    }

    if matches!(args.level, ConformanceLevel::L1) {
        return Ok(Some(PackReport {
            pack: pack_id,
            version: pack_version,
            level: level_label(args.level),
            status: "pass".to_string(),
            stage: "execute".to_string(),
            case: None,
            diagnostics,
            timing_ms: timing,
        }));
    }

    let state_start = Instant::now();
    let activity = build_activity(
        &pack_id,
        &entry_flow,
        &flow_type,
        Some("conformance-session"),
    );
    let second = timeout(timeout_limit, host.handle_activity("conformance", activity)).await;
    timing.state = state_start.elapsed().as_millis() as u64;

    let second = match second {
        Ok(result) => result,
        Err(_) => {
            diagnostics.push(Diagnostic {
                code: "timeout".to_string(),
                message: "state execution timed out".to_string(),
            });
            return Ok(Some(PackReport {
                pack: pack_id,
                version: pack_version,
                level: level_label(args.level),
                status: "fail".to_string(),
                stage: "state".to_string(),
                case: None,
                diagnostics,
                timing_ms: timing,
            }));
        }
    };

    match second {
        Ok(replies) => {
            if !is_structured(&replies) {
                diagnostics.push(Diagnostic {
                    code: "invalid_output".to_string(),
                    message: "runner returned unstructured output".to_string(),
                });
                return Ok(Some(PackReport {
                    pack: pack_id,
                    version: pack_version,
                    level: level_label(args.level),
                    status: "fail".to_string(),
                    stage: "state".to_string(),
                    case: None,
                    diagnostics,
                    timing_ms: timing,
                }));
            }
        }
        Err(err) => {
            diagnostics.push(Diagnostic {
                code: "execute_error".to_string(),
                message: err.to_string(),
            });
            return Ok(Some(PackReport {
                pack: pack_id,
                version: pack_version,
                level: level_label(args.level),
                status: "fail".to_string(),
                stage: "state".to_string(),
                case: None,
                diagnostics,
                timing_ms: timing,
            }));
        }
    }

    Ok(Some(PackReport {
        pack: pack_id,
        version: pack_version,
        level: level_label(args.level),
        status: "pass".to_string(),
        stage: "state".to_string(),
        case: None,
        diagnostics,
        timing_ms: timing,
    }))
}

#[cfg(feature = "fault-injection")]
async fn run_fault_matrix(args: &ConformanceArgs, faults_path: &Path) -> Result<Vec<PackReport>> {
    let pack_paths = discover_packs(&args.packs)?;
    if pack_paths.is_empty() {
        bail!("no .gtpack files found in {}", args.packs.display());
    }
    let payload = fs::read(faults_path)
        .with_context(|| format!("read faults file {}", faults_path.display()))?;
    let matrix: FaultMatrix = serde_json::from_slice(&payload)
        .with_context(|| format!("parse faults file {}", faults_path.display()))?;
    let mut reports = Vec::new();
    for case in matrix.cases {
        let report = run_fault_case(args, &case, &pack_paths).await?;
        let failed = report.status == "fail";
        reports.push(report);
        if failed && args.fail_fast {
            break;
        }
    }
    Ok(reports)
}

#[cfg(feature = "fault-injection")]
async fn run_fault_case(
    args: &ConformanceArgs,
    case: &FaultCase,
    pack_paths: &[PathBuf],
) -> Result<PackReport> {
    let timeout_limit = Duration::from_secs(DEFAULT_TIMEOUT_SECS);
    let mut timing = TimingReport::default();
    let mut diagnostics = Vec::new();
    let pack_path = find_pack_path(pack_paths, &case.pack)
        .with_context(|| format!("pack {} not found in {}", case.pack, args.packs.display()))?;
    let mut injector = FaultInjector::new(case.faults.clone())
        .with_seed(case.seed.unwrap_or(0))
        .with_pack_id(case.pack.clone());
    if let Some(flow_id) = case.flow.as_deref() {
        injector = injector.with_flow_id(flow_id.to_string());
    }
    set_injector(injector);
    let _injector_guard = InjectorGuard;

    let resolve_start = Instant::now();
    let host = build_host(args.trace)?;
    host.start().await?;
    let load_result = timeout(timeout_limit, host.load_pack("conformance", &pack_path)).await;
    timing.resolve = resolve_start.elapsed().as_millis() as u64;

    let load_result = match load_result {
        Ok(result) => result,
        Err(_) => {
            host.stop().await?;
            return Ok(PackReport {
                pack: case.pack.clone(),
                version: "unknown".to_string(),
                level: level_label(args.level),
                status: "fail".to_string(),
                stage: "resolve".to_string(),
                case: Some(case.name.clone()),
                diagnostics: vec![Diagnostic {
                    code: "timeout".to_string(),
                    message: "pack load timed out".to_string(),
                }],
                timing_ms: timing,
            });
        }
    };
    if let Err(err) = load_result {
        host.stop().await?;
        return Ok(PackReport {
            pack: case.pack.clone(),
            version: "unknown".to_string(),
            level: level_label(args.level),
            status: "fail".to_string(),
            stage: "resolve".to_string(),
            case: Some(case.name.clone()),
            diagnostics: vec![Diagnostic {
                code: "load_error".to_string(),
                message: err.to_string(),
            }],
            timing_ms: timing,
        });
    }

    let Some(tenant) = host.tenant("conformance").await else {
        host.stop().await?;
        bail!(
            "tenant runtime not available for pack {}",
            pack_path.display()
        );
    };
    let pack = tenant.pack();
    let pack_id = pack.metadata().pack_id.clone();
    let pack_version = pack.metadata().version.clone();

    if pack_id != case.pack {
        diagnostics.push(Diagnostic {
            code: "pack_mismatch".to_string(),
            message: format!("expected pack {}, got {}", case.pack, pack_id),
        });
    }

    let flow_descriptors = pack.list_flows().await?;
    let flow_id = match case.flow.as_deref() {
        Some(flow) => flow.to_string(),
        None => select_entry_flow(&pack, &flow_descriptors)?,
    };
    let flow_type = flow_descriptors
        .iter()
        .find(|flow| flow.id == flow_id)
        .map(|flow| flow.flow_type.clone())
        .unwrap_or_else(|| "messaging".to_string());

    let run_start = Instant::now();
    let session_id = if matches!(args.level, ConformanceLevel::L2) {
        Some("conformance-session")
    } else {
        None
    };
    let activity = build_activity(&pack_id, &flow_id, &flow_type, session_id);
    let run_result = timeout(timeout_limit, host.handle_activity("conformance", activity)).await;
    timing.run = run_start.elapsed().as_millis() as u64;

    let mut actual_status = "fail".to_string();
    let mut error_message = None;
    let mut replies_payload = Value::Null;
    let run_result = match run_result {
        Ok(result) => result,
        Err(_) => {
            error_message = Some("execution timed out".to_string());
            Err(anyhow!("execution timed out"))
        }
    };
    match run_result {
        Ok(replies) => {
            replies_payload = serde_json::to_value(&replies).unwrap_or(Value::Null);
            // Session flows surface node failures as an Ok reply carrying
            // metadata.error_kind. Re-classify as "fail" so fault scenarios
            // that expect fail still match.
            let envelope = replies_payload
                .as_array()
                .and_then(|arr| arr.iter().find_map(extract_error_envelope))
                .or_else(|| extract_error_envelope(&replies_payload));
            if let Some((kind, message)) = envelope {
                actual_status = "fail".to_string();
                error_message = Some(message.clone());
                diagnostics.push(Diagnostic {
                    code: "flow_error_envelope".to_string(),
                    message: format!("{kind}: {message}"),
                });
            } else {
                actual_status = "pass".to_string();
            }
        }
        Err(err) => {
            error_message = Some(err.to_string());
            diagnostics.push(Diagnostic {
                code: "execute_error".to_string(),
                message: err.to_string(),
            });
        }
    }

    let stats = stats();
    host.stop().await?;

    let expected_status = case.expect.status.to_lowercase();
    let actual_retries = stats.max_attempt.saturating_sub(1);
    let mut case_pass = true;
    if expected_status != actual_status {
        diagnostics.push(Diagnostic {
            code: "status_mismatch".to_string(),
            message: format!("expected {}, got {}", expected_status, actual_status),
        });
        case_pass = false;
    }
    if case.expect.retries != actual_retries {
        diagnostics.push(Diagnostic {
            code: "retries_mismatch".to_string(),
            message: format!(
                "expected {} retries, got {}",
                case.expect.retries, actual_retries
            ),
        });
        case_pass = false;
    }
    if case.expect.dlq {
        let has_dlq = error_message
            .as_deref()
            .map(|msg| msg.contains("dlq_recommended"))
            .unwrap_or(false);
        if !has_dlq {
            diagnostics.push(Diagnostic {
                code: "dlq_missing".to_string(),
                message: "expected dlq recommendation".to_string(),
            });
            case_pass = false;
        }
    }
    if !case.expect.dlq
        && error_message
            .as_deref()
            .map(|msg| msg.contains("dlq_recommended"))
            .unwrap_or(false)
    {
        diagnostics.push(Diagnostic {
            code: "dlq_unexpected".to_string(),
            message: "unexpected dlq recommendation".to_string(),
        });
        case_pass = false;
    }

    let inputs_payload = serde_json::json!({
        "pack": pack_id,
        "flow": flow_id,
        "level": level_label(args.level),
        "session_id": session_id,
        "payload": Value::Null,
    });
    let result_payload = serde_json::json!({
        "status": actual_status,
        "error": error_message,
        "retries": actual_retries,
        "replies": replies_payload,
    });
    if !case_pass {
        let trace_path = if args.trace {
            Some(TraceConfig::from_env().out_path)
        } else {
            None
        };
        write_fault_artifacts(case, &inputs_payload, &result_payload, trace_path)?;
    }

    Ok(PackReport {
        pack: pack_id,
        version: pack_version,
        level: level_label(args.level),
        status: if case_pass {
            "pass".to_string()
        } else {
            "fail".to_string()
        },
        stage: "execute".to_string(),
        case: Some(case.name.clone()),
        diagnostics,
        timing_ms: timing,
    })
}

#[cfg(feature = "fault-injection")]
fn find_pack_path(pack_paths: &[PathBuf], pack_id: &str) -> Option<PathBuf> {
    for path in pack_paths {
        if let Some(stem) = path.file_stem().and_then(|s| s.to_str())
            && stem == pack_id
        {
            return Some(path.clone());
        }
    }
    for path in pack_paths {
        let candidate = path.to_string_lossy();
        if candidate.contains(pack_id) {
            return Some(path.clone());
        }
    }
    None
}

#[cfg(feature = "fault-injection")]
fn write_fault_artifacts(
    case: &FaultCase,
    inputs: &Value,
    result: &Value,
    trace_path: Option<PathBuf>,
) -> Result<()> {
    write_fault_artifacts_under(Path::new("target"), case, inputs, result, trace_path)
}

#[cfg(feature = "fault-injection")]
fn write_fault_artifacts_under(
    base: &Path,
    case: &FaultCase,
    inputs: &Value,
    result: &Value,
    trace_path: Option<PathBuf>,
) -> Result<()> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let root = base
        .join("fault-artifacts")
        .join(case.name.replace(' ', "_"))
        .join(timestamp.to_string());
    fs::create_dir_all(&root)
        .with_context(|| format!("create fault artifact dir {}", root.display()))?;
    let faults_path = root.join("faults.json");
    let inputs_path = root.join("inputs.json");
    let result_path = root.join("result.json");
    fs::write(faults_path, serde_json::to_vec_pretty(case)?).context("write faults.json")?;
    fs::write(inputs_path, serde_json::to_vec_pretty(inputs)?).context("write inputs.json")?;
    fs::write(result_path, serde_json::to_vec_pretty(result)?).context("write result.json")?;
    if let Some(path) = trace_path
        && path.exists()
    {
        let dest = root.join("trace.txt");
        let _ = fs::copy(path, dest);
    }
    Ok(())
}

fn build_activity(
    pack_id: &str,
    flow_id: &str,
    flow_type: &str,
    session_id: Option<&str>,
) -> Activity {
    let mut activity = Activity::custom("conformance", Value::Null)
        .with_pack(pack_id)
        .with_flow(flow_id)
        .with_flow_type(flow_type);
    if let Some(session) = session_id {
        activity = activity.with_session(session);
    }
    activity
}

/// Look for `error_kind` + `error_message` at any of the carrier paths an
/// Activity reply may nest them under (`payload.metadata`, `metadata`, root).
#[cfg(feature = "fault-injection")]
fn extract_error_envelope(value: &Value) -> Option<(String, String)> {
    let candidates = [
        value.get("payload").and_then(|p| p.get("metadata")),
        value.get("metadata"),
        Some(value),
    ];
    for metadata in candidates.into_iter().flatten() {
        let kind = metadata
            .get("error_kind")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let message = metadata
            .get("error_message")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty());
        if let (Some(k), Some(m)) = (kind, message) {
            return Some((k.to_string(), m.to_string()));
        }
    }
    None
}

fn is_structured(replies: &[Activity]) -> bool {
    replies.iter().all(|reply| {
        serde_json::to_value(reply)
            .ok()
            .map(|value| !value.is_null())
            .unwrap_or(false)
    })
}

fn level_label(level: ConformanceLevel) -> String {
    match level {
        ConformanceLevel::L0 => "L0".to_string(),
        ConformanceLevel::L1 => "L1".to_string(),
        ConformanceLevel::L2 => "L2".to_string(),
    }
}

fn matches_prefix(pack_id: &str) -> bool {
    PREFIX_FILTERS
        .iter()
        .any(|prefix| pack_id.starts_with(prefix))
}

fn matches_provider(pack_id: &str, provider: Option<&str>) -> bool {
    provider
        .map(|value| pack_id.contains(value))
        .unwrap_or(true)
}

fn select_entry_flow(
    pack: &PackRuntime,
    flows: &[greentic_runner_host::pack::FlowDescriptor],
) -> Result<String> {
    let metadata = pack.metadata();
    if let Some(entry) = metadata.entry_flows.first() {
        return Ok(entry.clone());
    }
    flows
        .first()
        .map(|flow| flow.id.clone())
        .ok_or_else(|| anyhow::anyhow!("pack has no flows"))
}

fn build_host(enable_trace: bool) -> Result<RunnerHost> {
    let trace = if enable_trace {
        TraceConfig::from_env().with_overrides(TraceMode::Always, None)
    } else {
        TraceConfig::from_env().with_overrides(TraceMode::Off, None)
    };
    let config = HostConfig {
        tenant: "conformance".to_string(),
        bindings_path: PathBuf::from("<conformance>"),
        flow_type_bindings: std::collections::HashMap::new(),
        rate_limits: RateLimits::default(),
        retry: FlowRetryConfig::default(),
        http_enabled: false,
        secrets_policy: SecretsPolicy::allow_all(),
        state_store_policy: StateStorePolicy::default(),
        webhook_policy: WebhookPolicy::default(),
        timers: Vec::new(),
        oauth: None,
        mocks: None,
        pack_bindings: Vec::new(),
        env_passthrough: Vec::new(),
        trace,
        validation: ValidationConfig::from_env(),
        operator_policy: OperatorPolicy::allow_all(),
        fast2flow: Default::default(),
        #[cfg(feature = "agentic-worker")]
        agents: std::collections::HashMap::new(),
        #[cfg(feature = "agentic-worker")]
        graphs: std::collections::HashMap::new(),
    };

    let wasi_policy = RunnerWasiPolicy::default().inherit_stdio(false);
    let secrets = default_manager().context("failed to init secrets manager")?;
    let host = HostBuilder::new()
        .with_config(config)
        .with_wasi_policy(wasi_policy)
        .with_secrets_manager(secrets)
        .build()?;
    Ok(host)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn sample_report(status: &str) -> PackReport {
        PackReport {
            pack: "pack.demo".into(),
            version: "1.0.0".into(),
            level: "L1".into(),
            status: status.into(),
            stage: "run".into(),
            case: None,
            diagnostics: Vec::new(),
            timing_ms: TimingReport::default(),
        }
    }

    fn args_for_report(report_path: Option<PathBuf>) -> ConformanceArgs {
        ConformanceArgs {
            packs: PathBuf::from("/tmp/unused"),
            report: report_path,
            level: ConformanceLevel::L1,
            provider: None,
            fail_fast: false,
            trace: false,
            faults: None,
        }
    }

    #[test]
    fn summarize_counts_pass_and_fail() {
        let summary = summarize(
            &[sample_report("pass"), sample_report("fail")],
            ConformanceLevel::L1,
        );
        assert_eq!(summary.level, "L1");
        assert_eq!(summary.total, 2);
        assert_eq!(summary.passed, 1);
        assert_eq!(summary.failed, 1);
    }

    #[test]
    fn summarize_handles_empty_and_all_passing() {
        let empty = summarize(&[], ConformanceLevel::L0);
        assert_eq!(empty.total, 0);
        assert_eq!(empty.passed, 0);
        assert_eq!(empty.failed, 0);
        assert_eq!(empty.level, "L0");

        let all_pass = summarize(
            &[sample_report("pass"), sample_report("pass")],
            ConformanceLevel::L2,
        );
        assert_eq!(all_pass.passed, 2);
        assert_eq!(all_pass.failed, 0);
        assert_eq!(all_pass.level, "L2");
    }

    #[test]
    fn discover_packs_finds_only_gtpack_files() {
        let temp = TempDir::new().expect("tempdir");
        let nested = temp.path().join("nested");
        fs::create_dir_all(&nested).expect("nested");
        fs::write(temp.path().join("a.gtpack"), b"a").expect("a");
        fs::write(nested.join("b.GTPACK"), b"b").expect("b");
        fs::write(temp.path().join("ignore.txt"), b"x").expect("x");

        let packs = discover_packs(temp.path()).expect("discover packs");
        assert_eq!(packs.len(), 2);
    }

    #[test]
    fn discover_packs_errors_on_missing_directory() {
        let temp = TempDir::new().expect("tempdir");
        let missing = temp.path().join("does-not-exist");
        let err = discover_packs(&missing).unwrap_err();
        assert!(err.to_string().contains("does not exist"));
    }

    #[test]
    fn helper_filters_and_activity_builder_behave() {
        assert!(matches_prefix("messaging-demo"));
        assert!(matches_prefix("events-demo"));
        assert!(matches_prefix("secrets-demo"));
        assert!(!matches_prefix("other-demo"));
        assert!(matches_provider("provider-slack", Some("slack")));
        assert!(!matches_provider("provider-webex", Some("slack")));
        assert!(matches_provider("anything", None));
        assert_eq!(level_label(ConformanceLevel::L0), "L0");
        assert_eq!(level_label(ConformanceLevel::L1), "L1");
        assert_eq!(level_label(ConformanceLevel::L2), "L2");

        let activity = build_activity("pack.demo", "flow.demo", "message", Some("session-1"));
        assert_eq!(activity.pack_id(), Some("pack.demo"));
        assert_eq!(activity.flow_id(), Some("flow.demo"));
        assert_eq!(activity.flow_type(), Some("message"));
        assert_eq!(activity.session_id(), Some("session-1"));

        let no_session = build_activity("pack.demo", "flow.demo", "message", None);
        assert!(no_session.session_id().is_none());
    }

    #[test]
    fn structured_check_and_build_host_work() {
        let replies = vec![Activity::custom("one", serde_json::json!({"ok": true}))];
        assert!(is_structured(&replies));
        // Empty list is vacuously structured.
        assert!(is_structured(&[]));
        assert!(build_host(false).is_ok());
        assert!(build_host(true).is_ok());
    }

    #[test]
    fn emit_report_writes_pretty_json_to_path() -> Result<()> {
        let temp = TempDir::new()?;
        let nested = temp.path().join("nested/deeper");
        let report_path = nested.join("report.json");
        let args = args_for_report(Some(report_path.clone()));
        let reports = vec![sample_report("pass"), sample_report("fail")];
        // The report is still written, but a failing pack makes the run fail.
        let err = emit_report(&args, reports).expect_err("failed pack must error");
        assert!(err.to_string().contains("1 of 2"), "got: {err}");
        assert!(report_path.exists(), "parent directory should be created");
        let contents = fs::read_to_string(&report_path)?;
        let parsed: Value = serde_json::from_str(&contents)?;
        assert_eq!(parsed["summary"]["total"], 2);
        assert_eq!(parsed["summary"]["passed"], 1);
        assert_eq!(parsed["summary"]["failed"], 1);
        assert_eq!(parsed["summary"]["level"], "L1");
        assert_eq!(parsed["packs"][0]["status"], "pass");
        Ok(())
    }

    #[test]
    fn emit_report_prints_to_stdout_when_no_path() -> Result<()> {
        let args = args_for_report(None);
        // Output goes to stdout; an all-pass run succeeds.
        emit_report(&args, vec![sample_report("pass")])
    }

    #[test]
    fn emit_report_errors_when_no_packs_ran() {
        let args = args_for_report(None);
        let err = emit_report(&args, Vec::new()).expect_err("empty run must error");
        assert!(err.to_string().contains("no packs"), "got: {err}");
    }

    #[cfg(feature = "fault-injection")]
    #[test]
    fn find_pack_path_matches_by_stem_then_substring() {
        let alpha = PathBuf::from("/packs/messaging-conformance.gtpack");
        let beta = PathBuf::from("/packs/other-pack.gtpack");
        let gamma = PathBuf::from("/packs/something-with-fragment-inside.gtpack");
        let paths = vec![alpha.clone(), beta.clone(), gamma.clone()];

        // Exact stem match wins.
        assert_eq!(find_pack_path(&paths, "messaging-conformance"), Some(alpha));
        // Falls back to substring match when no stem matches.
        assert_eq!(find_pack_path(&paths, "fragment"), Some(gamma));
        // Returns None when neither match.
        assert_eq!(find_pack_path(&paths, "absent"), None);
    }

    #[cfg(feature = "fault-injection")]
    #[test]
    fn write_fault_artifacts_creates_expected_files() -> Result<()> {
        let temp = TempDir::new()?;
        // Write under <tmp>/target/fault-artifacts/<case_name>/<ts>/ without
        // touching the process-global cwd (other tests run in parallel).
        let target = temp.path().join("target");
        {
            let case = FaultCase {
                name: "case under test".to_string(),
                pack: "messaging-conformance".to_string(),
                flow: None,
                faults: Vec::new(),
                expect: FaultExpect {
                    retries: 0,
                    status: "pass".into(),
                    dlq: false,
                },
                seed: None,
            };
            let inputs = serde_json::json!({"k": "v"});
            let result = serde_json::json!({"status": "fail"});
            let trace_src = temp.path().join("trace.txt");
            fs::write(&trace_src, b"trace-bytes")?;
            write_fault_artifacts_under(&target, &case, &inputs, &result, Some(trace_src))?;

            let root = temp.path().join("target").join("fault-artifacts");
            let case_dir = root.join("case_under_test");
            assert!(case_dir.exists(), "case dir should exist");
            let timestamps: Vec<PathBuf> = fs::read_dir(&case_dir)?
                .filter_map(Result::ok)
                .map(|e| e.path())
                .collect();
            assert_eq!(timestamps.len(), 1, "exactly one timestamped run dir");
            let run_dir = &timestamps[0];
            assert!(run_dir.join("faults.json").exists());
            assert!(run_dir.join("inputs.json").exists());
            assert!(run_dir.join("result.json").exists());
            assert!(run_dir.join("trace.txt").exists());
            // No trace path: should still write the three JSON files.
            write_fault_artifacts_under(&target, &case, &inputs, &result, None)?;
            Ok(())
        }
    }
}
