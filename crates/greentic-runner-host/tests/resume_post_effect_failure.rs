//! A RESUMED run that fails after one of its own nodes performed an external
//! effect must not leave its park behind (audit round 26, L1).
//!
//! `FlowResumeStore::fetch` does not consume a snapshot, so a resume that ends
//! in `Err` keeps the wait it resumed from; the next message would resume the
//! same wait and repeat the effect. Driven through the production turn path
//! (`StateMachineRuntime::handle` -> `PackFlowAdapter::call_traced`):
//!
//! - wait -> `media.create` (the effect, counted by a stub artifact port) ->
//!   a node that fails: the second message answers with the failure envelope
//!   and clears the park, and the third message starts a FRESH run (parks at the
//!   wait again) without running the effect a second time;
//! - wait -> a node that fails with NO earlier effect: the failure is still an
//!   `Err` and the park is kept (a transient failure stays retryable).

#![cfg(feature = "agentic-worker")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use greentic_ext_runtime::host_ports::{
    ArtifactPort, ArtifactPortError, ArtifactPutRequest, HostCallContext,
};
use greentic_runner_host::config::{
    FlowRetryConfig, HostConfig, OperatorPolicy, RateLimits, SecretsPolicy, StateStorePolicy,
    WebhookPolicy,
};
use greentic_runner_host::engine::runtime::{IngressEnvelope, StateMachineRuntime};
use greentic_runner_host::pack::{ComponentResolution, PackRuntime};
use greentic_runner_host::runner::engine::FlowEngine;
use greentic_runner_host::storage::new_session_store;
use greentic_runner_host::storage::session::session_host_from;
use greentic_runner_host::storage::state::{new_state_store, state_host_from};
use greentic_runner_host::trace::TraceConfig;
use greentic_runner_host::validate::ValidationConfig;
use greentic_types::{
    ExtensionInline, ExtensionRef, PackFlowEntry, PackKind, PackManifest, ReplyScope,
    encode_pack_manifest,
};
use once_cell::sync::Lazy;
use semver::Version;
use serde_json::{Value, json};
use tempfile::TempDir;
use zip::write::FileOptions;

const RUNTIME_FLOW_EXTENSION_ID: &str = "greentic.pack.runtime_flow";
const PACK_ID: &str = "resume.post.effect";

static RUNTIME: Lazy<&'static tokio::runtime::Runtime> = Lazy::new(|| {
    Box::leak(Box::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("test runtime"),
    ))
});

/// Counts how many files the `media.create` node stored.
#[derive(Default)]
struct CountingPort {
    puts: Mutex<u64>,
}

impl ArtifactPort for CountingPort {
    fn put(
        &self,
        _extension_id: &str,
        _ctx: &HostCallContext,
        _request: ArtifactPutRequest,
    ) -> Result<String, ArtifactPortError> {
        let mut puts = self.puts.lock().unwrap();
        *puts += 1;
        Ok(format!("artifact://{:0>64x}", 0xc000 + *puts))
    }
}

fn host_config(bindings_path: &Path) -> HostConfig {
    HostConfig {
        tenant: "demo".into(),
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
        agents: std::collections::HashMap::new(),
        graphs: std::collections::HashMap::new(),
    }
}

fn build_pack(pack_path: &Path) -> Result<()> {
    let flows = json!([
        {
            // wait -> effect (media.create) -> a node the pack does not have.
            "id": "post.flow",
            "flow_type": "messaging",
            "start": "ask",
            "nodes": {
                "ask": {
                    "component": "session.wait",
                    "input": { "reason": "awaiting the user" },
                    "routing": { "next": { "node_id": "make" } }
                },
                "make": {
                    "component": "media.create",
                    "input": { "format": "txt", "name": "note", "text": "hello" },
                    "routing": { "next": { "node_id": "boom" } }
                },
                "boom": {
                    "component": "missing.component",
                    "input": {},
                    "routing": "end"
                }
            }
        },
        {
            // wait -> failing node, no effect before it.
            "id": "pre.flow",
            "flow_type": "messaging",
            "start": "ask",
            "nodes": {
                "ask": {
                    "component": "session.wait",
                    "input": { "reason": "awaiting the user" },
                    "routing": { "next": { "node_id": "boom" } }
                },
                "boom": {
                    "component": "missing.component",
                    "input": {},
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
        name: None,
        version: Version::parse("0.0.0")?,
        kind: PackKind::Application,
        publisher: "test".into(),
        components: Vec::new(),
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
    zip.finish().context("finalise pack archive")?;
    Ok(())
}

struct Harness {
    _temp: TempDir,
    runtime: StateMachineRuntime,
    port: Arc<CountingPort>,
}

fn harness() -> Result<Harness> {
    let rt = *RUNTIME;
    let temp = TempDir::new()?;
    let pack_path = temp.path().join("post-effect.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;
    build_pack(&pack_path)?;
    let config = Arc::new(host_config(&bindings_path));
    let pack = Arc::new(rt.block_on(PackRuntime::load(
        &pack_path,
        Arc::clone(&config),
        None,
        None,
        None,
        Some(new_state_store()),
        Arc::new(greentic_runner_host::wasi::RunnerWasiPolicy::new()),
        greentic_runner_host::secrets::default_manager()?,
        None,
        false,
        ComponentResolution::default(),
    ))?);
    let mut engine = rt.block_on(FlowEngine::new(
        vec![Arc::clone(&pack)],
        Arc::clone(&config),
    ))?;
    let port = Arc::new(CountingPort::default());
    engine.set_ext_artifact_port(Some(Arc::clone(&port) as Arc<dyn ArtifactPort>));
    let session_store = new_session_store();
    let session_host = session_host_from(Arc::clone(&session_store));
    let state_host = state_host_from(new_state_store());
    let runtime = StateMachineRuntime::from_flow_engine_with_run_outcome_sink(
        config,
        Arc::new(engine),
        std::collections::HashMap::new(),
        session_host,
        session_store,
        state_host,
        greentic_runner_host::secrets::default_manager()?,
        None,
        None,
        None,
    )?;
    Ok(Harness {
        _temp: temp,
        runtime,
        port,
    })
}

fn envelope(flow_id: &str, text: &str) -> IngressEnvelope {
    IngressEnvelope {
        tenant: "demo".into(),
        env: Some("local".into()),
        pack_id: Some(PACK_ID.into()),
        flow_id: flow_id.into(),
        flow_type: Some("messaging".into()),
        action: Some("messaging".into()),
        session_hint: Some(format!("demo:webchat:chan:conv-{flow_id}:u-7")),
        provider: Some("webchat".into()),
        messaging_endpoint_id: None,
        channel: Some("chan".into()),
        conversation: Some(format!("conv-{flow_id}")),
        user: Some("u-7".into()),
        entry_node: None,
        activity_id: Some("activity-1".into()),
        timestamp: None,
        payload: json!({ "text": text }),
        metadata: None,
        reply_scope: Some(ReplyScope {
            conversation: format!("conv-{flow_id}"),
            thread: None,
            reply_to: None,
            correlation: None,
        }),
    }
}

fn puts(h: &Harness) -> u64 {
    *h.port.puts.lock().unwrap()
}

/// Whether a turn's reply is the parked-at-a-wait envelope.
fn is_parked(reply: &Value) -> bool {
    reply.get("status").and_then(Value::as_str) == Some("pending")
}

#[test]
fn a_resume_that_fails_after_an_effect_clears_the_park_and_never_repeats_the_effect() -> Result<()>
{
    let rt = *RUNTIME;
    let h = harness()?;

    // Message 1: runs to the wait.
    let first = rt.block_on(h.runtime.handle(envelope("post.flow", "start")))?;
    assert!(is_parked(&first), "first turn should park: {first}");
    assert_eq!(puts(&h), 0);

    // Message 2: resumes, the effect runs once, the next node fails. The failure
    // is answered (not an Err) the way a fresh run's failure is.
    let second = rt
        .block_on(h.runtime.handle(envelope("post.flow", "go")))
        .context("a resume failing after an effect must answer, not error")?;
    assert_eq!(puts(&h), 1, "the effect ran once");
    assert_eq!(
        second
            .pointer("/metadata/error_kind")
            .and_then(Value::as_str),
        Some("flow_execution_failed"),
        "failure envelope expected: {second}"
    );

    // Message 3: the park is gone, so this is a FRESH run that parks at the
    // wait again; the effect must not run a second time.
    let third = rt.block_on(h.runtime.handle(envelope("post.flow", "again")))?;
    assert!(
        is_parked(&third),
        "third turn should start a fresh run and park: {third}"
    );
    assert_eq!(puts(&h), 1, "the effect was not repeated");
    Ok(())
}

#[test]
fn a_resume_that_fails_before_any_effect_keeps_the_park() -> Result<()> {
    let rt = *RUNTIME;
    let h = harness()?;

    let first = rt.block_on(h.runtime.handle(envelope("pre.flow", "start")))?;
    assert!(is_parked(&first), "first turn should park: {first}");

    // No effect ran before the failing node: the failure stays an Err ...
    let second = rt.block_on(h.runtime.handle(envelope("pre.flow", "go")));
    assert!(
        second.is_err(),
        "pre-effect failure stays an Err: {second:?}"
    );

    // ... and the park is kept, so the next message resumes the SAME wait (and
    // fails the same way) instead of starting a fresh run that would park.
    let third = rt.block_on(h.runtime.handle(envelope("pre.flow", "again")));
    assert!(
        third.is_err(),
        "the park must survive a pre-effect failure: {third:?}"
    );
    Ok(())
}
