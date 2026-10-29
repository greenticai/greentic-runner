use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use clap::{ArgAction, Parser};
use serde_json::Value;

use greentic_runner_host::RunnerWasiPolicy;
use greentic_runner_host::component_api::node::{
    ExecCtx as ComponentExecCtx, TenantCtx as ComponentTenantCtx,
};
use greentic_runner_host::config::{
    FlowRetryConfig, HostConfig, OperatorPolicy, RateLimits, SecretsPolicy, StateStorePolicy,
    WebhookPolicy,
};
use greentic_runner_host::pack::{ComponentResolution, PackRuntime};
use greentic_runner_host::secrets::default_manager;
use greentic_runner_host::storage::{new_session_store, new_state_store};
use greentic_runner_host::trace::{TraceEnvelope, TraceHash};

#[derive(Debug, Parser)]
pub struct ReplayArgs {
    /// Path to trace.json
    #[arg(value_name = "TRACE")]
    pub trace: PathBuf,

    /// Optional pack path override (.gtpack or directory)
    #[arg(long, value_name = "PATH")]
    pub pack: Option<PathBuf>,

    /// Stop after reaching the failing step (default true; `--until-failure=false` to disable)
    #[arg(long, action = ArgAction::Set, default_value_t = true, value_name = "BOOL")]
    pub until_failure: bool,

    /// Run a specific 1-based step index
    #[arg(long, value_name = "N")]
    pub step: Option<usize>,

    /// Compare hashes when available (default true; `--compare-hashes=false` to disable)
    #[arg(long, action = ArgAction::Set, default_value_t = true, value_name = "BOOL")]
    pub compare_hashes: bool,
}

pub async fn run(args: ReplayArgs) -> Result<()> {
    let trace = load_trace(&args.trace)?;
    let pack_path = resolve_pack_path(&trace, args.pack.as_deref())?;
    let pack = load_pack(&pack_path).await?;

    let target_step = resolve_target_step(args.step, trace.steps.len())?;
    let last_index = trace.steps.len().saturating_sub(1);
    let mut divergences = 0usize;

    for (idx, step) in trace.steps.iter().enumerate() {
        if let Some(target) = target_step
            && idx != target
        {
            continue;
        }

        println!("step {}: {} {}", idx + 1, step.component_id, step.operation);
        let Some(invocation) = step.invocation_json.as_ref() else {
            println!("  missing invocation_json; skipping replay");
            if target_step.is_some() {
                break;
            }
            continue;
        };

        let output = replay_step(&pack, &trace, step, invocation)
            .await
            .with_context(|| format!("replay failed at step {}", idx + 1))?;

        let Some(output) = output else {
            println!("  host-managed step; not re-executed, output hash not compared");
            if step.error.is_some() && args.until_failure {
                println!("  stopping at failing step");
                break;
            }
            continue;
        };

        if args.compare_hashes {
            if let Some(expected) = step.output_hash.as_ref() {
                match compare_output_hash(&output, expected)? {
                    HashCheck::Match => println!("  output hash matches"),
                    HashCheck::Diverged { expected, actual } => {
                        divergences += 1;
                        println!("  divergence: expected output hash {expected}, got {actual}");
                    }
                }
            } else {
                println!("  output hash missing; skipping compare");
            }
        }

        if step.error.is_some() && args.until_failure {
            println!("  stopping at failing step");
            break;
        }

        if idx == last_index {
            println!("  reached end of trace");
        }
    }

    if divergences > 0 {
        bail!("replay diverged from the recorded trace at {divergences} step(s)");
    }
    Ok(())
}

/// Convert the 1-based `--step` value into a 0-based index, rejecting `0` and
/// values beyond the end of the trace instead of silently doing nothing.
fn resolve_target_step(step: Option<usize>, total: usize) -> Result<Option<usize>> {
    match step {
        None => Ok(None),
        Some(0) => bail!("--step is 1-based; 0 is not a valid step"),
        Some(n) if n > total => bail!("--step {n} is out of range: the trace has {total} step(s)"),
        Some(n) => Ok(Some(n - 1)),
    }
}

/// Steps the host executes itself (no component to re-run), so replay has no
/// output to compare against the recorded hash.
fn is_host_managed_step(component_id: &str) -> bool {
    component_id.starts_with("emit.")
        || component_id == "flow.call"
        || component_id == "session.wait"
}

fn load_trace(path: &Path) -> Result<TraceEnvelope> {
    let bytes =
        std::fs::read(path).with_context(|| format!("failed to read trace {}", path.display()))?;
    serde_json::from_slice(&bytes).context("failed to parse trace.json")
}

fn resolve_pack_path(trace: &TraceEnvelope, override_path: Option<&Path>) -> Result<PathBuf> {
    if let Some(path) = override_path {
        return Ok(path.to_path_buf());
    }
    let candidate = Path::new(trace.pack.pack_ref.as_str());
    if candidate.exists() {
        return Ok(candidate.to_path_buf());
    }
    bail!(
        "pack path not found; provide --pack (trace pack_ref was {})",
        trace.pack.pack_ref
    );
}

async fn load_pack(path: &Path) -> Result<Arc<PackRuntime>> {
    let config = Arc::new(replay_host_config());
    let session_store = new_session_store();
    let state_store = new_state_store();
    let secrets = default_manager().context("failed to init secrets manager")?;
    let archive_source = if path.is_file() { Some(path) } else { None };
    let pack = PackRuntime::load(
        path,
        Arc::clone(&config),
        None,
        archive_source,
        Some(session_store),
        Some(state_store),
        Arc::new(RunnerWasiPolicy::default()),
        secrets,
        None,
        false,
        ComponentResolution::default(),
    )
    .await
    .context("failed to load pack for replay")?;
    Ok(Arc::new(pack))
}

fn replay_host_config() -> HostConfig {
    HostConfig {
        tenant: "replay".to_string(),
        bindings_path: PathBuf::from("<replay>"),
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
        trace: greentic_runner_host::trace::TraceConfig::from_env(),
        validation: greentic_runner_host::validate::ValidationConfig::from_env(),
        operator_policy: OperatorPolicy::allow_all(),
        fast2flow: Default::default(),
        #[cfg(feature = "agentic-worker")]
        agents: std::collections::HashMap::new(),
        #[cfg(feature = "agentic-worker")]
        graphs: std::collections::HashMap::new(),
    }
}

async fn replay_step(
    pack: &PackRuntime,
    trace: &TraceEnvelope,
    step: &greentic_runner_host::trace::TraceStep,
    invocation: &Value,
) -> Result<Option<Value>> {
    let component_id = step.component_id.as_str();
    let payload = invocation.get("payload").cloned().unwrap_or(Value::Null);

    if is_host_managed_step(component_id) {
        return Ok(None);
    }
    if component_id == "provider.invoke" {
        return replay_provider(pack, trace, step, &payload).await.map(Some);
    }

    let (component_ref, operation, input, config) = if component_id == "component.exec" {
        let component_ref = payload
            .get("component")
            .or_else(|| payload.get("component_ref"))
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("component.exec missing component ref"))?;
        let operation = payload
            .get("operation")
            .and_then(Value::as_str)
            .unwrap_or(step.operation.as_str());
        let input = payload.get("input").cloned().unwrap_or(Value::Null);
        let config = payload.get("config").cloned().unwrap_or(Value::Null);
        (
            component_ref.to_string(),
            operation.to_string(),
            input,
            config,
        )
    } else {
        (
            component_id.to_string(),
            step.operation.clone(),
            payload,
            Value::Null,
        )
    };

    let ctx = component_exec_ctx(trace, &step.node_id);
    let input_json = serde_json::to_string(&input)?;
    let config_json = if config.is_null() {
        None
    } else {
        Some(serde_json::to_string(&config)?)
    };
    pack.invoke_component(&component_ref, ctx, &operation, config_json, input_json)
        .await
        .context("component invoke failed")
        .map(Some)
}

async fn replay_provider(
    pack: &PackRuntime,
    trace: &TraceEnvelope,
    step: &greentic_runner_host::trace::TraceStep,
    payload: &Value,
) -> Result<Value> {
    let provider_id = payload.get("provider_id").and_then(Value::as_str);
    let provider_type = payload.get("provider_type").and_then(Value::as_str);
    let op = payload
        .get("op")
        .or_else(|| payload.get("operation"))
        .and_then(Value::as_str)
        .unwrap_or(step.operation.as_str());
    let input = payload.get("input").cloned().unwrap_or(Value::Null);
    let ctx = component_exec_ctx(trace, &step.node_id);
    let binding = pack.resolve_provider(provider_id, provider_type)?;
    let input_json = serde_json::to_vec(&input)?;
    pack.invoke_provider(&binding, ctx, op, input_json)
        .await
        .context("provider invoke failed")
}

fn component_exec_ctx(trace: &TraceEnvelope, node_id: &str) -> ComponentExecCtx {
    ComponentExecCtx {
        tenant: ComponentTenantCtx {
            tenant: "replay".to_string(),
            team: None,
            user: None,
            trace_id: None,
            i18n_id: None,
            correlation_id: None,
            deadline_unix_ms: None,
            attempt: 1,
            idempotency_key: None,
        },
        i18n_id: None,
        flow_id: trace.flow.id.clone(),
        node_id: Some(node_id.to_string()),
    }
}

#[derive(Debug, PartialEq, Eq)]
enum HashCheck {
    Match,
    Diverged { expected: String, actual: String },
}

/// Hash the replayed output with the algorithm the trace recorded and compare.
fn compare_output_hash(value: &Value, expected: &TraceHash) -> Result<HashCheck> {
    let actual = hash_value(value, expected)?;
    if actual.value == expected.value {
        Ok(HashCheck::Match)
    } else {
        Ok(HashCheck::Diverged {
            expected: expected.value.clone(),
            actual: actual.value,
        })
    }
}

fn hash_value(value: &Value, expected: &TraceHash) -> Result<TraceHash> {
    let bytes = serde_json::to_vec(value).context("failed to serialize replay output")?;
    let digest = match expected.algorithm.as_str() {
        "blake3" => blake3::hash(&bytes).to_hex().to_string(),
        other => bail!("unsupported trace hash algorithm `{other}`"),
    };
    Ok(TraceHash {
        algorithm: expected.algorithm.clone(),
        value: digest,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn recorded(value: &Value) -> TraceHash {
        TraceHash {
            algorithm: "blake3".to_string(),
            value: blake3::hash(&serde_json::to_vec(value).expect("serialize"))
                .to_hex()
                .to_string(),
        }
    }

    #[test]
    fn matching_output_is_not_a_divergence() {
        let out = json!({"a": 1});
        assert_eq!(
            compare_output_hash(&out, &recorded(&out)).expect("compare"),
            HashCheck::Match
        );
    }

    #[test]
    fn changed_output_is_a_divergence() {
        let expected = recorded(&json!({"a": 1}));
        let check = compare_output_hash(&json!({"a": 2}), &expected).expect("compare");
        assert!(matches!(check, HashCheck::Diverged { .. }));
    }

    #[test]
    fn emit_flow_call_and_session_wait_are_not_re_executed() {
        // Their recorded output hash covers the node payload, which replay
        // cannot reproduce, so they must be skipped rather than hashed as null.
        assert!(is_host_managed_step("emit.response"));
        assert!(is_host_managed_step("flow.call"));
        assert!(is_host_managed_step("session.wait"));
        assert!(!is_host_managed_step("component.exec"));
        assert!(!is_host_managed_step("provider.invoke"));
        let recorded_emit = recorded(&json!({"text": "hi"}));
        assert!(matches!(
            compare_output_hash(&Value::Null, &recorded_emit).expect("compare"),
            HashCheck::Diverged { .. }
        ));
    }

    #[test]
    fn step_selection_is_validated() {
        assert_eq!(resolve_target_step(None, 3).expect("none"), None);
        assert_eq!(resolve_target_step(Some(1), 3).expect("first"), Some(0));
        assert_eq!(resolve_target_step(Some(3), 3).expect("last"), Some(2));
        assert!(resolve_target_step(Some(0), 3).is_err());
        assert!(resolve_target_step(Some(4), 3).is_err());
        assert!(resolve_target_step(Some(1), 0).is_err());
    }

    #[test]
    fn unknown_hash_algorithm_is_an_error() {
        let mut expected = recorded(&json!(1));
        expected.algorithm = "md5".to_string();
        assert!(compare_output_hash(&json!(1), &expected).is_err());
    }

    #[test]
    fn bool_flags_can_be_turned_off() {
        #[derive(Parser)]
        struct W {
            #[command(flatten)]
            args: ReplayArgs,
        }
        let on = W::try_parse_from(["x", "trace.json"]).expect("parse");
        assert!(on.args.until_failure && on.args.compare_hashes);
        let off = W::try_parse_from([
            "x",
            "trace.json",
            "--until-failure=false",
            "--compare-hashes",
            "false",
        ])
        .expect("parse");
        assert!(!off.args.until_failure && !off.args.compare_hashes);
    }
}
