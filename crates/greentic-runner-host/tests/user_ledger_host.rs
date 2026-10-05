//! The user ledger (shared context, Phase C) installed the way greentic-start
//! will install it: `TenantRuntime::load_revision_with` with
//! `RevisionHostOptions::with_user_ledger(target)`, over a pack carrying
//! `assets/user-ledger.json`. Everything between the host option and the
//! door is real: the option threading in `runtime.rs`, the binding built next
//! to `share_policy`, the real `RuntimeAgentNodeHandler` and `AgentRuntime`,
//! the real `HttpUserLedger`. The door and the LLM are one wiremock server on
//! loopback, so every ledger call is a recorded HTTP request.
//!
//! What it proves:
//! - an OUTER verified `dw.agent` turn reads the ledger once and appends its
//!   reply once, with the runtime's tenant (a tenant mismatch would read
//!   nothing);
//! - the SAME agent, on the SAME runtime, run NESTED inside a `flow:` tool
//!   (`PackRuntime::run_flow_for_tool_interactive` under a `ToolCallFrame`
//!   whose verified caller is pinned onto the nested run) reaches the LLM,
//!   produces a reply, and makes ZERO ledger calls;
//! - with no host option, the same outer verified turn makes zero ledger
//!   calls (no behaviour change for existing hosts).
//!
//! What it does NOT cover: a real messaging provider stamping the caller (the
//! outer turn passes the caller block on `FlowContext` directly, the place the
//! ingress path puts it), and the outer agent's own `flow:` tool call: the
//! nested run is driven directly under a hand-built frame, which is exactly
//! what the aw-runtime tool loop establishes around `run_flow_for_tool_interactive`.
//!
//! The tests mutate process env, so they hold one lock; this file is its own
//! test binary.

#![cfg(feature = "agentic-worker")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use greentic_aw_runtime::ToolCallFrame;
use greentic_aw_runtime::VerifiedCaller;
use greentic_aw_runtime::tool_call_frame::within;
use greentic_aw_runtime::user_ledger::{UserLedgerTarget, appends_settled};
use greentic_deploy_spec::ids::{BundleId, DeploymentId, RevisionId};
use greentic_runner_host::config::{
    FlowRetryConfig, HostConfig, OperatorPolicy, RateLimits, SecretsPolicy, StateStorePolicy,
    WebhookPolicy,
};
use greentic_runner_host::runner::engine::{FlowContext, RetryConfig};
use greentic_runner_host::runtime::{
    RevisionHostOptions, RevisionLoad, RevisionPackRef, TenantRuntime,
};
use greentic_runner_host::storage::{
    new_session_store, new_state_store, session_host_from, state_host_from,
};
use greentic_runner_host::trace::TraceConfig;
use greentic_runner_host::validate::ValidationConfig;
use greentic_types::{
    ComponentCapabilities, ComponentManifest, ComponentProfiles, ExtensionInline, ExtensionRef,
    PackFlowEntry, PackKind, PackManifest, ResourceHints, encode_pack_manifest,
};
use runner_core::packs::PackDigest;
use semver::Version;
use serde_json::{Value, json};
use tempfile::TempDir;
use wiremock::matchers::{method, path, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};
use zip::ZipArchive;
use zip::write::FileOptions;

const RUNTIME_FLOW_EXTENSION_ID: &str = "greentic.pack.runtime_flow";
const PACK_ID: &str = "user.ledger.host";
const TENANT: &str = "demo";
const LEDGER_PATH: &str = "/api/v1/ingest/ledger";
const REPLY: &str = "Your order ships on Tuesday.";
/// What the LLM answers the OUTER turn of the nested test, so its append is
/// told apart from any (wrong) nested append.
const OUTER_REPLY: &str = "Your refund was approved.";
/// Generous bound for [`appends_settled`]: an append is at most
/// `APPEND_TIMEOUT` (3 s).
const SETTLE: Duration = Duration::from_secs(10);
const TOKEN: &str = "gtm_host_test_token";

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .map(PathBuf::from)
        .expect("workspace root")
}

fn component_artifact_path(temp_dir: &Path) -> Result<PathBuf> {
    let local =
        workspace_root().join("tests/fixtures/packs/runner-components/components/qa_process.wasm");
    if local.exists() {
        return Ok(local);
    }
    let archive_path =
        workspace_root().join("tests/fixtures/packs/runner-components/runner-components.gtpack");
    let mut archive = ZipArchive::new(File::open(&archive_path).context("open fixture gtpack")?)?;
    let mut wasm = archive
        .by_name("components/qa.process@0.1.0/component.wasm")
        .context("qa.process component missing from fixture pack")?;
    let out = temp_dir.join("qa_process.wasm");
    let mut buf = Vec::new();
    wasm.read_to_end(&mut buf)?;
    std::fs::write(&out, &buf)?;
    Ok(out)
}

fn host_config(bindings_path: &Path) -> HostConfig {
    let agent: greentic_aw_runtime::AgentConfig = serde_json::from_value(json!({
        "agent_id": "helper",
        "system_prompt": "be brief",
        "tools": [],
        "llm": { "provider": "openai", "model": "mock-model" }
    }))
    .expect("agent config");
    HostConfig {
        tenant: TENANT.into(),
        bindings_path: bindings_path.to_path_buf(),
        flow_type_bindings: Default::default(),
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
        trace: TraceConfig::from_env(),
        validation: ValidationConfig::from_env(),
        operator_policy: OperatorPolicy::allow_all(),
        fast2flow: Default::default(),
        agents: std::collections::HashMap::from([("helper".to_string(), agent)]),
        graphs: std::collections::HashMap::new(),
    }
}

/// `agent.flow`: `agent` (dw.agent `helper`) → end, plus the user-ledger
/// sidecar granting `helper` read_write.
fn build_pack(pack_path: &Path) -> Result<()> {
    let flows = json!([
        {
            "id": "agent.flow",
            "flow_type": "messaging",
            "start": "agent",
            "nodes": {
                "agent": {
                    "component": "dw.agent",
                    "operation": "helper",
                    "input": { "user_text": "when does my order ship?" },
                    "routing": "end"
                }
            }
        }
    ]);
    let mut extensions = BTreeMap::new();
    extensions.insert(
        RUNTIME_FLOW_EXTENSION_ID.to_string(),
        ExtensionRef {
            kind: RUNTIME_FLOW_EXTENSION_ID.to_string(),
            version: "2.0.0".into(),
            digest: None,
            location: None,
            inline: Some(ExtensionInline::Other(json!({ "flows": flows }))),
        },
    );
    let manifest = PackManifest {
        schema_version: "1.0".into(),
        pack_id: PACK_ID.parse()?,
        name: Some("User ledger host".into()),
        version: Version::parse("0.0.0")?,
        kind: PackKind::Application,
        publisher: "test".into(),
        components: vec![ComponentManifest {
            id: "qa.process".parse()?,
            version: Version::parse("0.1.0")?,
            supports: vec![greentic_types::FlowKind::Messaging],
            world: "greentic:component@0.4.0".into(),
            profiles: ComponentProfiles::default(),
            capabilities: ComponentCapabilities::default(),
            configurators: None,
            operations: Vec::new(),
            config_schema: None,
            resources: ResourceHints::default(),
            dev_flows: BTreeMap::new(),
        }],
        flows: Vec::<PackFlowEntry>::new(),
        dependencies: Vec::new(),
        capabilities: Vec::new(),
        signatures: Default::default(),
        secret_requirements: Vec::new(),
        bootstrap: None,
        agents: BTreeMap::new(),
        extensions: Some(extensions),
    };
    let mut zip = zip::ZipWriter::new(File::create(pack_path).context("create pack archive")?);
    let options: FileOptions<'_, ()> =
        FileOptions::default().compression_method(zip::CompressionMethod::Stored);
    zip.start_file("manifest.cbor", options)?;
    zip.write_all(&encode_pack_manifest(&manifest)?)?;
    zip.start_file("assets/user-ledger.json", options)?;
    zip.write_all(br#"{"helper":"read_write"}"#)?;
    let component_path = component_artifact_path(
        pack_path
            .parent()
            .context("pack path should have a parent temp dir")?,
    )?;
    zip.start_file("components/qa.process.wasm", options)?;
    std::io::copy(&mut File::open(&component_path)?, &mut zip)?;
    zip.finish().context("finalise pack archive")?;
    Ok(())
}

/// Async: it is held across the whole turn, `.await`s included.
static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Every variable the host reads to decide how a `dw.agent` runs, which LLM it
/// reaches, and whether the ledger is on: cleared so the shell cannot decide.
const HOST_ENV: &[&str] = &[
    "GREENTIC_AW_DISPATCH",
    "GREENTIC_AW_NESTED_FLOW_AGENTS",
    "GREENTIC_AW_REDIS_URL",
    "GREENTIC_AW_STATE_BACKEND",
    "GREENTIC_EVENTS_NATS_URL",
    "GREENTIC_AW_LLM_EXTENSION",
    "GREENTIC_AW_USER_LEDGER",
    "GREENTIC_LLM_API_KEY",
    "GREENTIC_LLM_PROVIDER",
    "OPENAI_API_KEY",
    "GREENTIC_LLM_BASE_URL",
];

struct EnvGuard(Vec<(&'static str, Option<String>)>);

impl EnvGuard {
    fn set(vars: &[(&'static str, String)]) -> Self {
        let prev = HOST_ENV
            .iter()
            .copied()
            .chain(vars.iter().map(|(k, _)| *k))
            .map(|k| (k, std::env::var(k).ok()))
            .collect();
        for key in HOST_ENV {
            unsafe { std::env::remove_var(key) };
        }
        for (key, value) in vars {
            unsafe { std::env::set_var(key, value) };
        }
        Self(prev)
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for (key, old) in self.0.iter().rev() {
            match old {
                Some(value) => unsafe { std::env::set_var(key, value) },
                None => unsafe { std::env::remove_var(key) },
            }
        }
    }
}

fn alice() -> VerifiedCaller {
    VerifiedCaller {
        user_verified: true,
        sub: Some("alice".into()),
        ..VerifiedCaller::default()
    }
}

/// One server for both: the ledger door (`/read`, `/append`) and an
/// OpenAI-shaped chat completion that always answers [`REPLY`].
async fn door_and_llm() -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(format!("{LEDGER_PATH}/read")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "events": [] })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(format!("{LEDGER_PATH}/append")))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({})))
        .mount(&server)
        .await;
    llm_answering(REPLY).mount(&server).await;
    server
}

/// An OpenAI-shaped chat completion that always answers `reply`.
fn llm_answering(reply: &str) -> Mock {
    Mock::given(method("POST"))
        .and(path_regex("chat/completions$"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "chatcmpl-1",
            "object": "chat.completion",
            "created": 0,
            "model": "mock-model",
            "choices": [{
                "index": 0,
                "message": { "role": "assistant", "content": reply },
                "finish_reason": "stop"
            }],
            "usage": { "prompt_tokens": 3, "completion_tokens": 5, "total_tokens": 8 }
        })))
}

#[derive(Debug, Default, PartialEq, Eq)]
struct Calls {
    reads: usize,
    appends: usize,
    llm: usize,
}

async fn calls(server: &MockServer) -> Calls {
    let mut out = Calls::default();
    for request in server.received_requests().await.unwrap_or_default() {
        let p = request.url.path();
        if p == format!("{LEDGER_PATH}/read") {
            out.reads += 1;
        } else if p == format!("{LEDGER_PATH}/append") {
            out.appends += 1;
        } else if p.ends_with("chat/completions") {
            out.llm += 1;
        }
    }
    out
}

/// Wait (bounded) until `pred` holds; the append is a background task.
async fn settle(server: &MockServer, pred: impl Fn(&Calls) -> bool) -> Calls {
    for _ in 0..50 {
        let now = calls(server).await;
        if pred(&now) {
            return now;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    calls(server).await
}

async fn load(temp: &TempDir, options: RevisionHostOptions) -> Result<Arc<TenantRuntime>> {
    let pack_path = temp.path().join("ledger.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, format!("tenant: {TENANT}"))?;
    build_pack(&pack_path)?;
    let digest = PackDigest::sha256_from_bytes(&std::fs::read(&pack_path)?).raw_string();
    let pack_refs = vec![RevisionPackRef {
        path: pack_path,
        digest,
    }];
    let session_store = new_session_store();
    let session_host = session_host_from(Arc::clone(&session_store));
    let state_store = new_state_store();
    let state_host = state_host_from(Arc::clone(&state_store));
    let configs = BTreeMap::new();
    let refs = BTreeMap::new();
    TenantRuntime::load_revision_with(
        RevisionLoad {
            pack_refs: &pack_refs,
            config: Arc::new(host_config(&bindings_path)),
            mocks: None,
            wasi_policy: Arc::new(greentic_runner_host::RunnerWasiPolicy::new()),
            session_host,
            session_store,
            state_store,
            state_host,
            secrets_manager: greentic_runner_host::secrets::default_manager()?,
            deployment_id: DeploymentId::new(),
            bundle_id: BundleId::from("ledger.unit"),
            revision_id: RevisionId::new(),
            customer_id: None,
            runtime_configs_by_pack_id: &configs,
            runtime_refs_by_pack_id: &refs,
            runtime_ref_resolver: None,
        },
        options,
    )
    .await
}

/// An OUTER turn: the flow run straight on the tenant's engine with the
/// verified caller on the context (where the ingress path puts it), no tool
/// call frame.
async fn outer_turn(runtime: &TenantRuntime, caller: &Value) -> Result<Value> {
    let ctx = FlowContext {
        tenant: TENANT,
        pack_id: PACK_ID,
        flow_id: "agent.flow",
        node_id: None,
        tool: None,
        action: None,
        session_id: Some("outer-session"),
        provider_id: None,
        reply_scope: None,
        retry_config: RetryConfig {
            max_attempts: 1,
            base_delay_ms: 0,
        },
        attempt: 1,
        observer: None,
        mocks: None,
        caller: Some(caller),
    };
    Ok(runtime.engine().execute(ctx, json!({})).await?.output)
}

fn target(server: &MockServer) -> UserLedgerTarget {
    UserLedgerTarget {
        base_url: format!("{}{LEDGER_PATH}", server.uri()),
        token: TOKEN.into(),
        tenant_slug: "demo-slug".into(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_nested_agent_never_touches_the_ledger_while_the_outer_turn_does() -> Result<()> {
    let _lock = ENV_LOCK.lock().await;
    let server = door_and_llm().await;
    let _env = EnvGuard::set(&[("GREENTIC_LLM_BASE_URL", server.uri())]);
    let temp = TempDir::new()?;
    let runtime = load(
        &temp,
        RevisionHostOptions::default().with_user_ledger(target(&server)),
    )
    .await?;
    let pack = runtime.all_packs().first().cloned().context("a pack")?;

    // NESTED: the same agent, inside a `flow:` tool, with a verified caller
    // pinned from the frame (#829). It must run (LLM reached, reply produced)
    // and touch the ledger not at all.
    let nested = within(
        ToolCallFrame::new(Some("outer-session"), "call-1").with_caller(alice()),
        pack.run_flow_for_tool_interactive("agent.flow", json!({})),
    )
    .await
    .map_err(anyhow::Error::msg)?;
    let nested_text = format!("{nested:?}");
    assert!(
        nested_text.contains(REPLY),
        "the nested agent must really have answered: {nested_text}"
    );
    // An append is counted before the turn returns, so this deterministically
    // waits out any (wrong) nested append; the short sleep is belt and braces
    // for the recorder.
    assert!(appends_settled(SETTLE).await, "appends did not settle");
    tokio::time::sleep(Duration::from_millis(100)).await;
    let after_nested = calls(&server).await;
    assert!(after_nested.llm >= 1, "nested agent ran: {after_nested:?}");
    assert_eq!(
        (after_nested.reads, after_nested.appends),
        (0, 0),
        "a nested agent must neither read nor append the user ledger"
    );

    // OUTER (positive control): same runtime, same agent, same verified user,
    // no tool call frame. Reads once, appends its reply once. The LLM now
    // answers OUTER_REPLY, so the append is provably the outer turn's.
    llm_answering(OUTER_REPLY)
        .with_priority(1)
        .mount(&server)
        .await;
    let output = outer_turn(&runtime, &serde_json::to_value(alice())?).await?;
    assert!(output.to_string().contains(OUTER_REPLY), "{output}");
    assert!(appends_settled(SETTLE).await, "appends did not settle");
    let after_outer = settle(&server, |c| c.appends >= 1).await;
    assert_eq!(
        (after_outer.reads, after_outer.appends),
        (1, 1),
        "{after_outer:?}"
    );

    // The calls carry the host's token and slug and the verified subject.
    let requests = server.received_requests().await.unwrap_or_default();
    let appends: Vec<_> = requests
        .iter()
        .filter(|r| r.url.path().ends_with("/append"))
        .collect();
    assert_eq!(appends.len(), 1);
    let append = appends[0];
    assert_eq!(
        append
            .headers
            .get("authorization")
            .and_then(|v| v.to_str().ok()),
        Some(format!("Bearer {TOKEN}").as_str())
    );
    let body: Value = serde_json::from_slice(&append.body)?;
    assert_eq!(body["subject"], "alice", "{body}");
    assert_eq!(body["tenant_slug"], "demo-slug", "{body}");
    let summary = body["summary"].as_str().unwrap_or("");
    assert!(summary.contains(OUTER_REPLY), "{body}");
    assert!(
        !summary.contains(REPLY),
        "the nested reply was appended: {body}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn without_the_host_option_a_verified_turn_makes_no_ledger_call() -> Result<()> {
    let _lock = ENV_LOCK.lock().await;
    let server = door_and_llm().await;
    let _env = EnvGuard::set(&[("GREENTIC_LLM_BASE_URL", server.uri())]);
    let temp = TempDir::new()?;
    // The sidecar is in the pack; only the host option is missing.
    let runtime = load(&temp, RevisionHostOptions::default()).await?;
    let output = outer_turn(&runtime, &serde_json::to_value(alice())?).await?;
    assert!(output.to_string().contains(REPLY), "{output}");
    assert!(appends_settled(SETTLE).await, "appends did not settle");
    tokio::time::sleep(Duration::from_millis(100)).await;
    let now = calls(&server).await;
    assert!(now.llm >= 1, "{now:?}");
    assert_eq!((now.reads, now.appends), (0, 0), "{now:?}");
    Ok(())
}
