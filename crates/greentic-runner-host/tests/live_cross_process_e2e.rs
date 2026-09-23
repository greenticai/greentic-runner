//! Broker-gated, cross-process live end-to-end tests for the `sorla.call` and
//! `operala.call` await paths. Unlike `tests/runtime_session_resumer.rs` (which
//! resumes by calling the resumer directly with an in-test stub dispatcher),
//! these tests stand up the REAL runner side:
//!
//!   * a real [`NatsDispatcher`] as the engine's remote dispatch handler, so the
//!     `sorla.call` / `operala.call` node publishes a real request onto real NATS,
//!     and
//!   * a real [`run_response_listener`] spawned over a
//!     [`RuntimeSessionResumer`], so the runtime resumes itself when a response
//!     arrives on `greentic.sorla.response.v1` / `greentic.operala.response.v1`.
//!
//! The tests do NOT respond to the request themselves. The response MUST come
//! from an EXTERNAL bridge process subscribed to the appropriate request subject
//! that replies on the corresponding response subject, echoing the
//! `Greentic-Correlation-Id` header.
//!
//! What each test proves end-to-end:
//!   1. a real flow with a remote-dispatch await node PAUSES (a wait is saved in
//!      the shared `FlowResumeStore`),
//!   2. the real `NatsDispatcher` published the request the external bridge
//!      answers,
//!   3. the real `run_response_listener` + `RuntimeSessionResumer` consume the
//!      reply and resume the flow, CONSUMING the saved wait.
//!
//! Consumption is observed directly: a `FlowResumeStore` built over the SAME
//! backing session store is polled with `fetch(&envelope)` — it returns `Some`
//! while the flow waits and `None` once the external bridge's reply has driven
//! the resume to completion.
//!
//! Self-skips when `GREENTIC_TEST_NATS_URL` is unset so broker-free CI stays
//! green. The pack-building + runtime-construction helpers mirror
//! `tests/runtime_session_resumer.rs`; the only differences here are the real
//! `NatsDispatcher`, the spawned real listener, and observing async consumption.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use greentic_runner_host::config::{
    FlowRetryConfig, HostConfig, OperatorPolicy, RateLimits, SecretsPolicy, StateStorePolicy,
    WebhookPolicy,
};
use greentic_runner_host::engine::runtime::{
    FlowResumeStore, IngressEnvelope, StateMachineRuntime,
};
use greentic_runner_host::pack::{ComponentResolution, PackRuntime};
use greentic_runner_host::runner::dispatch_listener::{SessionResumer, run_response_listener};
use greentic_runner_host::runner::engine::FlowEngine;
use greentic_runner_host::runner::remote_dispatch::NatsDispatcher;
use greentic_runner_host::runner::runtime_session_resumer::RuntimeSessionResumer;
use greentic_runner_host::storage::new_session_store;
use greentic_runner_host::storage::session::{DynSessionStore, session_host_from};
use greentic_runner_host::storage::state::new_state_store;
use greentic_runner_host::trace::TraceConfig;
use greentic_runner_host::validate::ValidationConfig;
use greentic_types::{
    ComponentCapabilities, ComponentManifest, ComponentProfiles, ExtensionInline, ExtensionRef,
    PackFlowEntry, PackKind, PackManifest, ReplyScope, ResourceHints, encode_pack_manifest,
};
use semver::Version;
use serde_json::json;
use tempfile::TempDir;
use zip::ZipArchive;
use zip::write::FileOptions;

const RUNTIME_FLOW_EXTENSION_ID: &str = "greentic.pack.runtime_flow";
const PACK_ID: &str = "live.cross.process.e2e";
const SORLA_FLOW_ID: &str = "sorla.live.flow";
const BARE_HINT: &str = "demo:provider:chan:conv:user";
/// The runtime name dispatched to / listened on (subjects `greentic.sorla.*`).
const RUNTIME_NAME: &str = "sorla";
/// Flow id for the real-sorx variant (distinct correlation key from the echo
/// variant so the two tests never collide on a shared broker).
const SORX_FLOW_ID: &str = "sorla.live.sorx.flow";
/// The real sorx endpoint exercised by the real-deployment variant.
const SORX_TARGET_OPERATION: &str = "tenant.create";
/// Runtime name for the operala dispatch path (subjects `greentic.operala.*`).
const OPERALA_RUNTIME_NAME: &str = "operala";
/// Flow id for the real-operax variant. Distinct from all sorla flow ids so its
/// correlation key never collides with sorla tests on a shared broker.
const OPERALA_FLOW_ID: &str = "operala.live.operax.flow";
/// The operax endpoint exercised by the real-deployment variant.
const OPERAX_TARGET_OPERATION: &str = "run";

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .map(PathBuf::from)
        .expect("workspace root")
}

/// Locate the prebuilt `qa.process` WASM, unpacking it from the fixture pack if
/// it is not already on disk. Copied from `tests/runtime_session_resumer.rs`.
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
        #[cfg(feature = "agentic-worker")]
        agents: std::collections::HashMap::new(),
        #[cfg(feature = "agentic-worker")]
        graphs: std::collections::HashMap::new(),
    }
}

/// Build a `.gtpack` whose only flow is `sorla.call` (await) -> `emit.response`.
/// The node targets `echo.do`/`echo.do` so an external echo bridge can answer.
/// The await node routes forward to `done` so the resume has a node to advance
/// into. Mirrors `build_sorla_wait_pack` in `tests/runtime_session_resumer.rs`.
fn build_sorla_wait_pack(pack_path: &Path) -> Result<()> {
    build_remote_call_wait_pack(
        pack_path,
        "sorla.call",
        SORLA_FLOW_ID,
        "echo.do",
        json!({ "await": true, "operation": "echo.do", "input": { "hi": 1 } }),
    )
}

/// Parameterized pack builder: a single-flow `.gtpack` whose remote-dispatch
/// await node (identified by `component`, e.g. `"sorla.call"` or
/// `"operala.call"`) targets `target_operation` with the given `node_input`,
/// routing forward to a terminal `emit.response`. Lets the echo, real-sorx,
/// and real-operax variants share one builder while differing only in
/// component / target / input.
fn build_remote_call_wait_pack(
    pack_path: &Path,
    component: &str,
    flow_id: &str,
    target_operation: &str,
    node_input: serde_json::Value,
) -> Result<()> {
    let runtime_flow = json!({
        "id": flow_id,
        "flow_type": "messaging",
        "start": "call",
        "nodes": {
            "call": {
                "component": component,
                "operation": target_operation,
                "input": node_input,
                "routing": { "next": { "node_id": "done" } }
            },
            "done": {
                "component": "emit.response",
                "input": { "text": "resumed and completed" },
                "routing": "end"
            }
        }
    });
    let runtime_extension = json!({ "flows": [runtime_flow] });

    let mut extensions = BTreeMap::new();
    extensions.insert(
        RUNTIME_FLOW_EXTENSION_ID.to_string(),
        ExtensionRef {
            kind: RUNTIME_FLOW_EXTENSION_ID.to_string(),
            version: "2.0.0".into(),
            digest: None,
            location: None,
            inline: Some(ExtensionInline::Other(runtime_extension)),
        },
    );

    let manifest = PackManifest {
        schema_version: "1.0".into(),
        pack_id: PACK_ID.parse()?,
        name: None,
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
    let manifest_bytes = encode_pack_manifest(&manifest)?;
    zip.start_file("manifest.cbor", options)?;
    zip.write_all(&manifest_bytes)?;
    let component_path = component_artifact_path(
        pack_path
            .parent()
            .expect("pack path should have a parent temp dir"),
    )?;
    zip.start_file("components/qa.process.wasm", options)?;
    let mut comp_file = File::open(&component_path)?;
    std::io::copy(&mut comp_file, &mut zip)?;
    zip.finish().context("finalise pack archive")?;
    Ok(())
}

/// The inbound envelope that triggers turn 1 (pause at `sorla.call`). Its
/// `session_hint`/`pack_id`/`conversation` determine the stored wait key, and
/// must reproduce exactly for `FlowResumeStore::fetch` to find / observe it.
fn sorla_inbound_envelope() -> IngressEnvelope {
    sorla_inbound_envelope_for(SORLA_FLOW_ID)
}

/// As [`sorla_inbound_envelope`] but for an arbitrary `flow_id`, so the real-sorx
/// variant triggers its own flow with its own (distinct) correlation key.
fn sorla_inbound_envelope_for(flow_id: &str) -> IngressEnvelope {
    IngressEnvelope {
        tenant: "demo".into(),
        env: Some("local".into()),
        pack_id: Some(PACK_ID.into()),
        flow_id: flow_id.into(),
        flow_type: Some("messaging".into()),
        action: Some("messaging".into()),
        session_hint: Some(BARE_HINT.into()),
        provider: Some("provider".into()),
        channel: Some("chan".into()),
        conversation: Some("conv".into()),
        user: Some("user".into()),
        entry_node: None,
        activity_id: Some("activity-1".into()),
        timestamp: None,
        messaging_endpoint_id: None,
        payload: json!({ "text": "start" }),
        metadata: None,
        reply_scope: Some(ReplyScope {
            conversation: "conv".into(),
            thread: None,
            reply_to: None,
            correlation: None,
        }),
    }
}

/// Build the real runtime with a real [`NatsDispatcher`] wired as the engine's
/// remote dispatch handler, returning the runtime AND the shared session store
/// (so the test can build a sibling `FlowResumeStore` to observe consumption).
async fn build_runtime_with_nats_dispatch(
    pack_path: &Path,
    config: Arc<HostConfig>,
    client: async_nats::Client,
) -> Result<(Arc<StateMachineRuntime>, DynSessionStore)> {
    let pack = Arc::new(
        PackRuntime::load(
            pack_path,
            Arc::clone(&config),
            None,
            None,
            None,
            None,
            Arc::new(greentic_runner_host::wasi::RunnerWasiPolicy::new()),
            greentic_runner_host::secrets::default_manager()?,
            None,
            false,
            ComponentResolution::default(),
        )
        .await?,
    );
    let mut engine = FlowEngine::new(vec![Arc::clone(&pack)], Arc::clone(&config)).await?;
    // The only difference from the gold harness: a REAL NATS dispatcher.
    engine.set_remote_dispatch_handler(Arc::new(NatsDispatcher::new(client)));
    let engine = Arc::new(engine);

    // The session host and the observing resume store MUST share the same
    // backing store so the wait saved on turn 1 is visible to `fetch`.
    let session_store = new_session_store();
    let session_host = session_host_from(Arc::clone(&session_store));
    let state_store = new_state_store();
    let state_host = greentic_runner_host::storage::state::state_host_from(state_store);
    let secrets = greentic_runner_host::secrets::default_manager()?;

    let runtime = StateMachineRuntime::from_flow_engine(
        Arc::clone(&config),
        engine,
        std::collections::HashMap::new(),
        session_host,
        Arc::clone(&session_store),
        state_host,
        secrets,
        None,
        None,
    )?;
    Ok((Arc::new(runtime), session_store))
}

/// LIVE CROSS-PROCESS TEST: a real `sorla.call` await flow pauses, the real
/// `NatsDispatcher` publishes the request, an EXTERNAL bridge process answers on
/// `greentic.sorla.response.v1`, and the real `run_response_listener` +
/// `RuntimeSessionResumer` resume the flow — consuming the saved wait.
///
/// Self-skips when `GREENTIC_TEST_NATS_URL` is unset (broker-free CI).
///
/// Run against a live broker AND an external echo bridge:
/// ```text
/// GREENTIC_TEST_NATS_URL=nats://127.0.0.1:4222 \
///   cargo test -p greentic-runner-host --test live_cross_process_e2e \
///   -- --nocapture
/// ```
/// (The bridge must be running separately, echoing the correlation header.)
#[tokio::test(flavor = "multi_thread")]
async fn sorla_call_resumes_via_external_nats_bridge() {
    let Ok(url) = std::env::var("GREENTIC_TEST_NATS_URL") else {
        eprintln!(
            "skipping: GREENTIC_TEST_NATS_URL not set \
             (needs NATS + an external echo bridge on greentic.sorla.*)"
        );
        return;
    };

    // 1. Connect two clients: one for the dispatcher (engine), one for listener.
    let dispatcher_client = async_nats::connect(&url)
        .await
        .expect("connect dispatcher client to NATS");
    let listener_client = async_nats::connect(&url)
        .await
        .expect("connect listener client to NATS");

    // 2. Build the real runtime + engine; wire the real NatsDispatcher.
    let temp = TempDir::new().expect("temp dir");
    let pack_path = temp.path().join("live-sorla.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo").expect("write bindings");
    build_sorla_wait_pack(&pack_path).expect("build sorla await pack");

    let config = Arc::new(host_config(&bindings_path));
    let (runtime, session_store) =
        build_runtime_with_nats_dispatch(&pack_path, Arc::clone(&config), dispatcher_client)
            .await
            .expect("build runtime with NATS dispatcher");

    // A sibling resume store over the SAME backing session store; used only to
    // OBSERVE whether the saved wait is still present (Some) or consumed (None).
    let observe_store = FlowResumeStore::new(Arc::clone(&session_store));

    // 3. Build the production resumer over the runtime and spawn the REAL
    //    response listener. When the external bridge replies, the listener
    //    decodes it and calls resumer.resume -> runtime.handle, resuming the
    //    flow without any second manual trigger from this test.
    let resumer: Arc<dyn SessionResumer> =
        Arc::new(RuntimeSessionResumer::new(Arc::clone(&runtime)));
    tokio::spawn(async move {
        if let Err(error) =
            run_response_listener(listener_client, RUNTIME_NAME.to_owned(), resumer).await
        {
            eprintln!("response listener exited with error: {error}");
        }
    });

    // Give the listener subscription a moment to be acknowledged by the broker
    // before the dispatcher publishes (so the response is not missed).
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 4. Trigger the flow. The sorla.call await node publishes a real request
    //    (which the external bridge answers) and the flow PAUSES.
    let envelope = sorla_inbound_envelope();
    runtime
        .handle(envelope.clone())
        .await
        .expect("turn 1 should run the sorla.call node and pause awaiting the response");

    // 5. Assert the flow PAUSED: a wait is saved in the shared resume store.
    //    (Note: a fast external bridge could in principle resume before this
    //    check runs; the load-bearing assertion is the consumption poll below,
    //    so we only warn rather than fail if the wait is already gone.)
    match observe_store.fetch(&envelope).await {
        Ok(Some(_)) => { /* paused as expected */ }
        Ok(None) => eprintln!(
            "note: wait already consumed before the pause assertion \
             (external bridge replied very fast); continuing to consumption poll"
        ),
        Err(error) => panic!("failed to inspect the saved wait: {error}"),
    }

    // 6. Poll until the saved wait is CONSUMED (fetch returns None), proving the
    //    external bridge replied and the listener + resumer resumed + completed
    //    the flow. Fail with a clear message if it never consumes in time.
    let consumed = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            match observe_store.fetch(&envelope).await {
                Ok(None) => break,
                Ok(Some(_)) => tokio::time::sleep(Duration::from_millis(150)).await,
                Err(error) => panic!("failed to inspect the saved wait while polling: {error}"),
            }
        }
    })
    .await;

    assert!(
        consumed.is_ok(),
        "timed out after 8s waiting for the saved wait to be consumed — \
         the external echo bridge on greentic.sorla.* must reply on \
         greentic.sorla.response.v1 echoing the Greentic-Correlation-Id header"
    );

    eprintln!(
        "PASSED: sorla.call await flow paused, external NATS bridge replied, \
         and the real listener + resumer consumed the saved wait"
    );
}

/// LIVE CROSS-PROCESS TEST against a REAL `greentic-sorx` deployment (not an echo
/// bridge). A real `sorla.call` await flow targets the real sorx `tenant.create`
/// endpoint; an EXTERNAL real sorx process (started separately) serves that
/// endpoint and auto-runs its bridge on the same NATS, replying on
/// `greentic.sorla.response.v1`.
///
/// Two assertions, in order:
///   1. REAL ENDPOINT EXECUTED — an OBSERVER subscribed to the response subject
///      BEFORE triggering receives a `RuntimeDispatchResponse` with `ok == true`
///      whose serialized body contains the substring `"tenant-1"` (the id the
///      real `tenant.create` endpoint echoes back in `result.id`). NATS core
///      pub/sub fan-out delivers the same message to BOTH the observer and the
///      runner's listener, so observing does not steal the runner's copy.
///   2. FLOW RESUMED — the saved wait in the shared `FlowResumeStore` is CONSUMED
///      (fetch returns `None`), proving the real listener + resumer drove the
///      resume to completion off the real reply.
///
/// Self-skips when `GREENTIC_TEST_NATS_URL` is unset (broker-free CI). Requires a
/// live broker AND a real `greentic-sorx` serving `tenant.create` with its bridge
/// enabled on the same NATS:
/// ```text
/// GREENTIC_TEST_NATS_URL=nats://127.0.0.1:4222 \
///   cargo test -p greentic-runner-host --test live_cross_process_e2e \
///   sorla_call_resumes_via_real_sorx_deployment -- --nocapture
/// ```
#[tokio::test(flavor = "multi_thread")]
async fn sorla_call_resumes_via_real_sorx_deployment() {
    let Ok(url) = std::env::var("GREENTIC_TEST_NATS_URL") else {
        eprintln!(
            "skipping: GREENTIC_TEST_NATS_URL not set \
             (needs NATS + a real greentic-sorx serving tenant.create with the bridge enabled)"
        );
        return;
    };

    // 1. Connect three clients: dispatcher (engine), listener (runner resume),
    //    and an independent OBSERVER that proves the real endpoint executed.
    let dispatcher_client = async_nats::connect(&url)
        .await
        .expect("connect dispatcher client to NATS");
    let listener_client = async_nats::connect(&url)
        .await
        .expect("connect listener client to NATS");
    let observer_client = async_nats::connect(&url)
        .await
        .expect("connect observer client to NATS");

    // 2. Subscribe the observer to the response subject BEFORE triggering so the
    //    real sorx reply is captured. Core pub/sub fan-out also delivers the same
    //    message to the runner's listener, so this does not steal the response.
    use futures::StreamExt as _;
    let response_subject = greentic_types::response_topic(RUNTIME_NAME);
    let mut observer_subscription = observer_client
        .subscribe(response_subject)
        .await
        .expect("observer subscription to the sorla response subject");

    // 3. Build the real runtime + engine; wire the real NatsDispatcher. The flow's
    //    sorla.call node targets the REAL sorx `tenant.create` endpoint.
    let temp = TempDir::new().expect("temp dir");
    let pack_path = temp.path().join("live-sorx.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo").expect("write bindings");
    let node_input = json!({
        "await": true,
        "operation": SORX_TARGET_OPERATION,
        "input": {
            "id": "tenant-1",
            "landlord_id": "landlord-1",
            "name": "Avery Stone",
            "active": "true"
        }
    });
    build_remote_call_wait_pack(
        &pack_path,
        "sorla.call",
        SORX_FLOW_ID,
        SORX_TARGET_OPERATION,
        node_input,
    )
    .expect("build sorla await pack targeting the real sorx tenant.create endpoint");

    let config = Arc::new(host_config(&bindings_path));
    let (runtime, session_store) =
        build_runtime_with_nats_dispatch(&pack_path, Arc::clone(&config), dispatcher_client)
            .await
            .expect("build runtime with NATS dispatcher");

    // A sibling resume store over the SAME backing session store; used only to
    // OBSERVE whether the saved wait is still present (Some) or consumed (None).
    let observe_store = FlowResumeStore::new(Arc::clone(&session_store));

    // 4. Build the production resumer over the runtime and spawn the REAL
    //    response listener, so the real sorx reply drives the resume.
    let resumer: Arc<dyn SessionResumer> =
        Arc::new(RuntimeSessionResumer::new(Arc::clone(&runtime)));
    tokio::spawn(async move {
        if let Err(error) =
            run_response_listener(listener_client, RUNTIME_NAME.to_owned(), resumer).await
        {
            eprintln!("response listener exited with error: {error}");
        }
    });

    // Give the listener + observer subscriptions a moment to be acknowledged by
    // the broker before the dispatcher publishes (so the response is not missed).
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 5. Trigger the flow. The sorla.call await node publishes a real request to
    //    the real sorx endpoint and the flow PAUSES awaiting the response.
    let envelope = sorla_inbound_envelope_for(SORX_FLOW_ID);
    runtime
        .handle(envelope.clone())
        .await
        .expect("turn 1 should run the sorla.call node and pause awaiting the real sorx response");

    // 6. OBSERVE the real reply: await the observer for ~8s, deserialize it as a
    //    RuntimeDispatchResponse, and assert the REAL endpoint executed — ok and
    //    its serialized body contains "tenant-1" (loose nesting-independent check).
    let observed = tokio::time::timeout(Duration::from_secs(8), observer_subscription.next())
        .await
        .expect(
            "timed out after 8s waiting for the real sorx response on \
             greentic.sorla.response.v1 — a real greentic-sorx serving tenant.create \
             with its bridge enabled must be running on the same NATS",
        )
        .expect("observer subscription closed before a response arrived");

    let response: greentic_types::RuntimeDispatchResponse =
        serde_json::from_slice(&observed.payload)
            .expect("observed response body should deserialize as RuntimeDispatchResponse");
    assert!(
        response.ok,
        "real sorx response was not ok: {:?}",
        response.error
    );
    let serialized = serde_json::to_string(&response).expect("re-serialize observed response");
    assert!(
        serialized.contains("tenant-1"),
        "observed real sorx response did not contain the expected id \"tenant-1\"; \
         the real tenant.create endpoint should echo result.id == \"tenant-1\". body: {serialized}"
    );

    // 7. Poll until the saved wait is CONSUMED (fetch returns None), proving the
    //    real listener + resumer resumed + completed the flow off the real reply.
    let consumed = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            match observe_store.fetch(&envelope).await {
                Ok(None) => break,
                Ok(Some(_)) => tokio::time::sleep(Duration::from_millis(150)).await,
                Err(error) => panic!("failed to inspect the saved wait while polling: {error}"),
            }
        }
    })
    .await;

    assert!(
        consumed.is_ok(),
        "timed out after 8s waiting for the saved wait to be consumed — \
         the real sorx reply must drive the runner's listener + resumer to resume the flow"
    );

    eprintln!(
        "PASSED: sorla.call await flow dispatched to the REAL sorx tenant.create \
         endpoint, the observed response was ok and contained \"tenant-1\", \
         and the real listener + resumer consumed the saved wait"
    );
}

/// LIVE CROSS-PROCESS TEST against a REAL `greentic-operax` deployment. A real
/// `operala.call` await flow targets the operax `run` endpoint; an EXTERNAL real
/// operax process (started separately, `gtc operax serve`) serves that endpoint
/// and auto-runs its bridge on the same NATS, replying on
/// `greentic.operala.response.v1`.
///
/// Two assertions, in order:
///   1. REAL ENDPOINT EXECUTED — an OBSERVER subscribed to
///      `greentic.operala.response.v1` BEFORE triggering receives a message
///      whose body deserializes as a well-formed `RuntimeDispatchResponse`.
///      Core pub/sub fan-out delivers the same message to BOTH the observer and
///      the runner's listener, so observing does not steal the runner's copy.
///   2. FLOW RESUMED — the saved wait in the shared `FlowResumeStore` is CONSUMED
///      (fetch returns `None`), proving the real listener + resumer drove the
///      resume to completion off the real reply.
///
/// Assertions are intentionally LOOSE on the response payload: operax's dry-run
/// result on a minimal `{ transactions: [] }` input is not guaranteed to be `ok`,
/// and the test does not assert any specific substring in the body. What matters
/// is that a well-formed `RuntimeDispatchResponse` was received (the real operax
/// processed the request) and the flow resumed (the runner consumed it).
///
/// Self-skips when `GREENTIC_TEST_NATS_URL` is unset (broker-free CI). Requires
/// a live broker AND a real `greentic-operax` serving `run` with its bridge
/// enabled on the same NATS:
/// ```text
/// GREENTIC_TEST_NATS_URL=nats://127.0.0.1:4222 \
///   cargo test -p greentic-runner-host --test live_cross_process_e2e \
///   operala_call_resumes_via_real_operax_deployment -- --nocapture
/// ```
#[tokio::test(flavor = "multi_thread")]
async fn operala_call_resumes_via_real_operax_deployment() {
    let Ok(url) = std::env::var("GREENTIC_TEST_NATS_URL") else {
        eprintln!(
            "skipping: GREENTIC_TEST_NATS_URL not set \
             (needs NATS + a real greentic-operax serving `run` with the bridge enabled)"
        );
        return;
    };

    // 1. Connect three clients: dispatcher (engine), listener (runner resume),
    //    and an independent OBSERVER that proves the real endpoint executed.
    let dispatcher_client = async_nats::connect(&url)
        .await
        .expect("connect dispatcher client to NATS");
    let listener_client = async_nats::connect(&url)
        .await
        .expect("connect listener client to NATS");
    let observer_client = async_nats::connect(&url)
        .await
        .expect("connect observer client to NATS");

    // 2. Subscribe the observer to the operala response subject BEFORE triggering
    //    so the real operax reply is captured. Core pub/sub fan-out also delivers
    //    the same message to the runner's listener without stealing it.
    use futures::StreamExt as _;
    let operala_response_subject = greentic_types::response_topic(OPERALA_RUNTIME_NAME);
    let mut observer_subscription = observer_client
        .subscribe(operala_response_subject)
        .await
        .expect("observer subscription to the operala response subject");

    // 3. Build the real runtime + engine; wire the real NatsDispatcher. The flow's
    //    operala.call node targets the REAL operax `run` endpoint with a minimal
    //    input; operax may return ok=false on this input, which is acceptable.
    let temp = TempDir::new().expect("temp dir");
    let pack_path = temp.path().join("live-operax.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo").expect("write bindings");
    let node_input = json!({
        "await": true,
        "operation": OPERAX_TARGET_OPERATION,
        "input": { "transactions": [] }
    });
    build_remote_call_wait_pack(
        &pack_path,
        "operala.call",
        OPERALA_FLOW_ID,
        OPERAX_TARGET_OPERATION,
        node_input,
    )
    .expect("build operala await pack targeting the real operax run endpoint");

    let config = Arc::new(host_config(&bindings_path));
    let (runtime, session_store) =
        build_runtime_with_nats_dispatch(&pack_path, Arc::clone(&config), dispatcher_client)
            .await
            .expect("build runtime with NATS dispatcher");

    // A sibling resume store over the SAME backing session store; used only to
    // OBSERVE whether the saved wait is still present (Some) or consumed (None).
    let observe_store = FlowResumeStore::new(Arc::clone(&session_store));

    // 4. Build the production resumer over the runtime and spawn the REAL
    //    response listener tuned to the `operala` runtime name, so the real
    //    operax reply drives the resume.
    let resumer: Arc<dyn SessionResumer> =
        Arc::new(RuntimeSessionResumer::new(Arc::clone(&runtime)));
    tokio::spawn(async move {
        if let Err(error) =
            run_response_listener(listener_client, OPERALA_RUNTIME_NAME.to_owned(), resumer).await
        {
            eprintln!("operala response listener exited with error: {error}");
        }
    });

    // Give the listener + observer subscriptions a moment to be acknowledged by
    // the broker before the dispatcher publishes (so the response is not missed).
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 5. Trigger the flow. The operala.call await node publishes a real request
    //    to the real operax endpoint and the flow PAUSES awaiting the response.
    let envelope = sorla_inbound_envelope_for(OPERALA_FLOW_ID);
    runtime.handle(envelope.clone()).await.expect(
        "turn 1 should run the operala.call node and pause awaiting the real operax response",
    );

    // 6. OBSERVE the real reply: await the observer for ~10s and deserialize it
    //    as a RuntimeDispatchResponse (confirms the real operax processed the
    //    request). We do NOT assert ok==true — operax dry-run on minimal input
    //    may legitimately return ok=false.
    let observed = tokio::time::timeout(Duration::from_secs(10), observer_subscription.next())
        .await
        .expect(
            "timed out after 10s waiting for the real operax response on \
             greentic.operala.response.v1 — a real greentic-operax serving `run` \
             with its bridge enabled must be running on the same NATS",
        )
        .expect("observer subscription closed before a response arrived");

    let _response: greentic_types::RuntimeDispatchResponse =
        serde_json::from_slice(&observed.payload)
            .expect("observed operala response body should deserialize as RuntimeDispatchResponse");

    // 7. Poll until the saved wait is CONSUMED (fetch returns None), proving the
    //    real listener + resumer resumed + completed the flow off the real reply.
    let consumed = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match observe_store.fetch(&envelope).await {
                Ok(None) => break,
                Ok(Some(_)) => tokio::time::sleep(Duration::from_millis(150)).await,
                Err(error) => panic!("failed to inspect the saved wait while polling: {error}"),
            }
        }
    })
    .await;

    assert!(
        consumed.is_ok(),
        "timed out after 10s waiting for the saved wait to be consumed — \
         the real operax reply must drive the runner's listener + resumer to resume the flow"
    );

    eprintln!(
        "PASSED: operala.call await flow dispatched to the REAL operax run endpoint, \
         a well-formed RuntimeDispatchResponse was observed on \
         greentic.operala.response.v1, and the real listener + resumer consumed the \
         saved wait"
    );
}
