//! End-to-end characterization of the `component == "a2a"` flow node
//! (LOCKED ENCODING v1: `agent`/`message`/`output` carried in the node
//! payload/config).
//!
//! This is the flow-execution A2A path: a plain flow asking an external agent
//! one question, without an agentic worker anywhere in it. A single-node
//! `.gtpack` — carrying the `assets/a2a-routes.json` sidecar the designer
//! writes — is built and run through the real [`FlowEngine`], against a
//! wiremock agent serving its own card and speaking the `SendMessage`
//! JSON-RPC contract.
//!
//! The harness mirrors `mcp_flow_node.rs`'s (the sibling flow-node path); the
//! agent fakes mirror the in-module `a2a_source` tests in
//! `greentic-aw-runtime`, whose helpers are test-only and cannot be reused
//! from here.
//!
//! What each test is for:
//!
//! * a `completed` answer reaches the node output, and `reply` is where a
//!   later step reads it from;
//! * every non-`completed` remote state arrives as its OWN `status` and
//!   carries no `reply` — the rule a flow routes on;
//! * a `requires_auth` route with no resolvable credential fails loudly,
//!   naming the agent and the secret address, and sends NOTHING;
//! * an agent the pack does not declare is refused rather than substituted;
//! * the continuation rule, both directions: a second call in one session
//!   continues the same remote task, and a call with no session hint does not.

#![cfg(feature = "agentic-worker")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use greentic_runner_host::config::{
    FlowRetryConfig, HostConfig, OperatorPolicy, RateLimits, SecretsPolicy, StateStorePolicy,
    WebhookPolicy,
};
use greentic_runner_host::pack::{ComponentResolution, PackRuntime};
use greentic_runner_host::runner::engine::{FlowContext, FlowEngine, FlowStatus};
use greentic_runner_host::trace::TraceConfig;
use greentic_runner_host::validate::ValidationConfig;
use greentic_secrets_lib::SecretsManager;
use greentic_types::{
    ExtensionInline, ExtensionRef, PackFlowEntry, PackKind, PackManifest, encode_pack_manifest,
};
use once_cell::sync::Lazy;
use semver::Version;
use serde_json::{Value, json};
use tempfile::TempDir;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};
use zip::write::FileOptions;

const RUNTIME_FLOW_EXTENSION_ID: &str = "greentic.pack.runtime_flow";
const PACK_ID: &str = "a2a.flow.node.test";
const FLOW_ID: &str = "a2a.flow";
const SESSION_HINT: &str = "demo:provider:chan:conv:user";
/// A hyphenated id, as the admin mints them. Used verbatim in the sidecar,
/// in the node config and in the secret name.
const AGENT_ID: &str = "8d2f0c1e-recipe";
const RPC_PATH: &str = "/a2a";

static RUNTIME: Lazy<&'static tokio::runtime::Runtime> = Lazy::new(|| {
    Box::leak(Box::new(
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime"),
    ))
});

/// Serializes every test here: they all read the process-global
/// `SECRETS_BACKEND` / `GREENTIC_AW_A2A` the engine consumes when it builds
/// the A2A source.
static ENV_GUARD: Mutex<()> = Mutex::new(());

/// Take [`ENV_GUARD`], recovering from a poisoned lock.
///
/// A plain `.lock().unwrap()` turns the FIRST failing test into a
/// `PoisonError` in every test after it, so a run reports seven broken things
/// where one is broken — which is exactly the wrong report to hand someone
/// mid-debugging. The guard protects environment variables every test
/// overwrites anyway, so there is no invariant a panic could have left
/// half-applied.
fn env_guard() -> std::sync::MutexGuard<'static, ()> {
    ENV_GUARD
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Clear the two process-global switches the A2A source reads, so a test's
/// outcome does not depend on the developer's shell.
///
/// `SECRETS_BACKEND` must be UNSET for the engine to dispatch with the
/// manager a test injects: `choose_mcp_secrets` gives an explicitly requested
/// backend precedence over an injected one.
fn clear_env() {
    // SAFETY: every caller holds ENV_GUARD, so no other test thread observes
    // the environment mid-mutation.
    unsafe {
        std::env::remove_var("SECRETS_BACKEND");
        std::env::remove_var("GREENTIC_AW_A2A");
    }
}

// ── the fake agent ───────────────────────────────────────────────────────────

/// A real-shaped agent card whose single interface points back at this
/// server's [`RPC_PATH`].
fn card_for(server: &MockServer) -> String {
    json!({
        "name": "Recipe Agent",
        "description": "Suggests dishes.",
        "version": "1.0.0",
        "supportedInterfaces": [
            { "url": format!("{}{RPC_PATH}", server.uri()),
              "protocolBinding": "JSONRPC", "protocolVersion": "1.0" }
        ],
        "capabilities": { "streaming": false, "pushNotifications": false },
        "defaultInputModes": ["text/plain"],
        "defaultOutputModes": ["text/plain"],
        "skills": []
    })
    .to_string()
}

/// Serve the card, and answer every `SendMessage` with `reply`.
async fn agent_answering(reply: Value) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/.well-known/agent-card.json"))
        .respond_with(ResponseTemplate::new(200).set_body_string(card_for(&server)))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(RPC_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0", "id": 1, "result": reply
        })))
        .mount(&server)
        .await;
    server
}

/// A `SendMessage` result that is a bare agent `Message` — the commonest
/// shape, and an answer.
fn message_reply(text: &str) -> Value {
    json!({ "message": {
        "messageId": "m-2", "contextId": "ctx-1", "role": "ROLE_AGENT",
        "parts": [{ "text": text }]
    }})
}

/// A `SendMessage` result that is a `Task` in `state`, with `detail` as its
/// status message and `artifact` as its (optional) answer text.
fn task_reply(state: &str, detail: Option<&str>, artifact: Option<&str>) -> Value {
    let mut task = json!({
        "id": "t-1",
        "contextId": "ctx-1",
        "status": { "state": state },
    });
    if let Some(detail) = detail {
        task["status"]["message"] = json!({
            "messageId": "m-s", "role": "ROLE_AGENT", "parts": [{ "text": detail }]
        });
    }
    if let Some(artifact) = artifact {
        task["artifacts"] = json!([{ "parts": [{ "text": artifact }] }]);
    }
    json!({ "task": task })
}

/// The `SendMessage` request bodies the agent received, oldest first.
async fn send_message_bodies(server: &MockServer) -> Vec<Value> {
    server
        .received_requests()
        .await
        .expect("wiremock records requests")
        .iter()
        .filter(|request| request.method.as_str() == "POST")
        .map(|request| serde_json::from_slice(&request.body).expect("a JSON-RPC body"))
        .collect()
}

// ── secrets ──────────────────────────────────────────────────────────────────

/// A secrets manager holding exactly the URIs it was given.
struct MapSecrets(std::collections::HashMap<String, Vec<u8>>);

#[async_trait::async_trait]
impl SecretsManager for MapSecrets {
    async fn read(&self, path: &str) -> greentic_secrets_lib::Result<Vec<u8>> {
        self.0
            .get(path)
            .cloned()
            .ok_or_else(|| greentic_secrets_lib::SecretError::NotFound(path.to_string()))
    }
    async fn write(&self, _: &str, _: &[u8]) -> greentic_secrets_lib::Result<()> {
        Ok(())
    }
    async fn delete(&self, _: &str) -> greentic_secrets_lib::Result<()> {
        Ok(())
    }
}

fn secrets(pairs: &[(&str, &str)]) -> Arc<dyn SecretsManager> {
    Arc::new(MapSecrets(
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.as_bytes().to_vec()))
            .collect(),
    ))
}

// ── pack harness ─────────────────────────────────────────────────────────────

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

/// Build a `.gtpack` whose only flow is a single `component == "a2a"` node,
/// carrying `sidecar` as `assets/a2a-routes.json`.
///
/// No WASM component is needed: the node is a native runtime dispatch, exactly
/// as the `mcp` node is.
fn build_a2a_pack(pack_path: &Path, node_input: Value, sidecar: Option<&str>) -> Result<()> {
    let mut nodes = serde_json::Map::new();
    nodes.insert(
        "ask".to_string(),
        json!({
            "component": "a2a",
            "input": node_input,
            "routing": "end",
        }),
    );

    let runtime_flow = json!({
        "id": FLOW_ID,
        "flow_type": "messaging",
        "start": "ask",
        "nodes": Value::Object(nodes),
    });

    let mut extensions = BTreeMap::new();
    extensions.insert(
        RUNTIME_FLOW_EXTENSION_ID.to_string(),
        ExtensionRef {
            kind: RUNTIME_FLOW_EXTENSION_ID.to_string(),
            version: "2.0.0".into(),
            digest: None,
            location: None,
            inline: Some(ExtensionInline::Other(json!({ "flows": [runtime_flow] }))),
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
    if let Some(sidecar) = sidecar {
        zip.start_file("assets/a2a-routes.json", options)?;
        zip.write_all(sidecar.as_bytes())?;
    }
    zip.finish().context("finalise pack archive")?;
    Ok(())
}

/// One sidecar record, as the designer's `a2a_routes` writer emits it.
fn sidecar(agent_id: &str, base_url: &str, requires_auth: bool) -> String {
    json!([{
        "agent_id": agent_id,
        "name": "Recipe agent",
        "base_url": base_url,
        "auth_header_name": null,
        "auth_team": null,
        "requires_auth": requires_auth,
    }])
    .to_string()
}

/// A loaded pack plus an engine over it, with a real in-memory flow-state
/// store (the continuation map's home) and `secrets` as the injected manager.
fn build_engine(
    pack_path: &Path,
    config: Arc<HostConfig>,
    secrets: Option<Arc<dyn SecretsManager>>,
) -> Result<(Arc<PackRuntime>, FlowEngine)> {
    let rt = *RUNTIME;
    let pack = Arc::new(rt.block_on(PackRuntime::load(
        pack_path,
        Arc::clone(&config),
        None,
        None,
        None,
        Some(greentic_runner_host::storage::state::new_state_store()),
        Arc::new(greentic_runner_host::wasi::RunnerWasiPolicy::new()),
        greentic_runner_host::secrets::default_manager()?,
        None,
        false,
        ComponentResolution::default(),
    ))?);
    let engine = rt
        .block_on(FlowEngine::new(
            vec![Arc::clone(&pack)],
            Arc::clone(&config),
        ))?
        .with_mcp_secrets(secrets);
    Ok((pack, engine))
}

fn flow_ctx<'a>(
    config: &'a HostConfig,
    pack_id: &'a str,
    session_id: Option<&'a str>,
) -> FlowContext<'a> {
    FlowContext {
        tenant: config.tenant.as_str(),
        pack_id,
        flow_id: FLOW_ID,
        node_id: None,
        tool: None,
        action: Some("messaging"),
        session_id,
        provider_id: Some("provider"),
        reply_scope: None,
        retry_config: config.retry.clone().into(),
        attempt: 1,
        observer: None,
        mocks: None,
        caller: None,
    }
}

/// Recursively find a value at `key` anywhere in the (envelope-wrapped)
/// output tree.
fn find_key<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    match value {
        Value::Object(map) => {
            if let Some(found) = map.get(key) {
                return Some(found);
            }
            map.values().find_map(|v| find_key(v, key))
        }
        Value::Array(items) => items.iter().find_map(|v| find_key(v, key)),
        _ => None,
    }
}

/// The value the node bound under `output: "answer"`.
fn answer(output: &Value) -> Value {
    find_key(output, "answer")
        .unwrap_or_else(|| panic!("output key `answer` missing, got {output:?}"))
        .clone()
}

/// Everything a test needs: a temp dir, a loaded pack and an engine over it.
struct Harness {
    _temp: TempDir,
    config: Arc<HostConfig>,
    pack: Arc<PackRuntime>,
    engine: FlowEngine,
}

impl Harness {
    /// A one-node flow asking `AGENT_ID` and binding the result under
    /// `answer`.
    fn new(sidecar_json: Option<&str>, secrets: Option<Arc<dyn SecretsManager>>) -> Result<Self> {
        Self::asking(AGENT_ID, sidecar_json, secrets)
    }

    fn asking(
        agent: &str,
        sidecar_json: Option<&str>,
        secrets: Option<Arc<dyn SecretsManager>>,
    ) -> Result<Self> {
        let temp = TempDir::new()?;
        let pack_path = temp.path().join("a2a.gtpack");
        let bindings_path = temp.path().join("bindings.yaml");
        std::fs::write(&bindings_path, b"tenant: demo")?;
        build_a2a_pack(
            &pack_path,
            json!({
                "agent": agent,
                "message": "{{ entry.text }}",
                "output": "answer",
            }),
            sidecar_json,
        )?;
        let config = Arc::new(host_config(&bindings_path));
        let (pack, engine) = build_engine(&pack_path, Arc::clone(&config), secrets)?;
        Ok(Self {
            _temp: temp,
            config,
            pack,
            engine,
        })
    }

    /// Run the flow once with `text` as the inbound message, asserting the
    /// flow COMPLETES (an A2A node never parks) and returning the bound value.
    fn run(&self, text: &str, session_id: Option<&str>) -> Result<Value> {
        let pack_id = self.pack.metadata().pack_id.to_string();
        let ctx = flow_ctx(&self.config, &pack_id, session_id);
        let execution = RUNTIME
            .block_on(self.engine.execute(ctx, json!({ "text": text })))
            .context("a2a flow run")?;
        match execution.status {
            FlowStatus::Completed => {}
            FlowStatus::Waiting(wait) => {
                anyhow::bail!("an a2a node must never park; got {:?}", wait.reason)
            }
        }
        Ok(answer(&execution.output))
    }
}

// ── tests ────────────────────────────────────────────────────────────────────

/// The happy path, and the shape a later step reads: a completed turn's text
/// is the node's `reply`, so `{{node.ask.answer.reply}}` resolves.
#[test]
fn a_completed_turn_puts_the_agents_reply_in_the_node_output() -> Result<()> {
    let _guard = env_guard();
    clear_env();
    let agent = RUNTIME.block_on(agent_answering(message_reply("an omelette")));
    let harness = Harness::new(Some(&sidecar(AGENT_ID, &agent.uri(), false)), None)?;

    let value = harness.run("what's for dinner?", Some(SESSION_HINT))?;

    assert_eq!(value["status"], "completed");
    assert_eq!(value["agent"], AGENT_ID);
    assert_eq!(value["reply"], "an omelette");
    // The node sent what the flow templated, not the raw template.
    let bodies = RUNTIME.block_on(send_message_bodies(&agent));
    assert_eq!(bodies.len(), 1);
    assert_eq!(
        bodies[0]["params"]["message"]["parts"][0]["text"],
        "what's for dinner?"
    );
    Ok(())
}

/// The rule the whole node shape turns on, measured end to end rather than in
/// the classifier: every remote state that is not an answer arrives under its
/// own `status` and carries NO `reply`, so a flow routing on
/// `{{node.ask.answer.status}}` can tell them apart — and a flow reading
/// `reply` can never be handed a non-answer.
#[test]
fn every_non_completed_state_is_distinguishable_and_carries_no_reply() -> Result<()> {
    let _guard = env_guard();
    // (remote task state, the status the flow sees, the key carrying the detail)
    let cases = [
        (
            "TASK_STATE_INPUT_REQUIRED",
            "input_required",
            "question",
            None,
        ),
        ("TASK_STATE_WORKING", "working", "detail", Some("working")),
        (
            "TASK_STATE_SUBMITTED",
            "working",
            "detail",
            Some("submitted"),
        ),
        ("TASK_STATE_FAILED", "failed", "error", Some("failed")),
        ("TASK_STATE_CANCELED", "failed", "error", Some("canceled")),
        ("TASK_STATE_REJECTED", "failed", "error", Some("rejected")),
        (
            "TASK_STATE_AUTH_REQUIRED",
            "failed",
            "error",
            Some("auth_required"),
        ),
    ];
    for (remote_state, status, detail_key, state_token) in cases {
        clear_env();
        let agent = RUNTIME.block_on(agent_answering(task_reply(
            remote_state,
            Some("Which city?"),
            None,
        )));
        let harness = Harness::new(Some(&sidecar(AGENT_ID, &agent.uri(), false)), None)?;

        let value = harness.run("hello", Some(SESSION_HINT))?;

        assert_eq!(value["status"], status, "for {remote_state}: {value}");
        assert_eq!(value["agent"], AGENT_ID, "for {remote_state}");
        assert!(
            value.get("reply").is_none(),
            "{remote_state} is not an answer and must carry no `reply`: {value}"
        );
        assert!(
            value[detail_key].is_string(),
            "{remote_state} must carry `{detail_key}`: {value}"
        );
        if let Some(token) = state_token {
            assert_eq!(
                value["state"], token,
                "{remote_state} must name the remote state it is: {value}"
            );
        }
    }
    Ok(())
}

/// A completed task with no readable artifact is NOT an empty answer. Relaying
/// silence as the agent's reply is the failure the reply-iff-completed rule
/// exists to prevent, so this arrives as an end state instead.
#[test]
fn a_completed_task_with_nothing_to_read_is_not_reported_as_an_answer() -> Result<()> {
    let _guard = env_guard();
    clear_env();
    let agent = RUNTIME.block_on(agent_answering(task_reply(
        "TASK_STATE_COMPLETED",
        None,
        None,
    )));
    let harness = Harness::new(Some(&sidecar(AGENT_ID, &agent.uri(), false)), None)?;

    let value = harness.run("hello", Some(SESSION_HINT))?;

    assert_ne!(value["status"], "completed", "{value}");
    assert!(value.get("reply").is_none(), "{value}");
    Ok(())
}

/// Decision 3. A route that declares a credential and has none resolvable
/// fails LOUDLY: the value names the agent and every secret address tried,
/// and — the half that matters — nothing is sent to the agent at all. There is
/// no unauthenticated retry.
#[test]
fn a_missing_credential_errors_naming_the_agent_and_sends_nothing() -> Result<()> {
    let _guard = env_guard();
    clear_env();
    let agent = RUNTIME.block_on(agent_answering(message_reply("an omelette")));
    let harness = Harness::new(
        Some(&sidecar(AGENT_ID, &agent.uri(), true)),
        // A store holding nothing: every candidate URI misses.
        Some(secrets(&[])),
    )?;

    let value = harness.run("hello", Some(SESSION_HINT))?;

    assert_eq!(value["status"], "error", "{value}");
    assert!(value.get("reply").is_none(), "{value}");
    let error = value["error"].as_str().expect("an error string");
    assert!(error.contains(AGENT_ID), "must name the agent: {error}");
    assert!(
        error.contains(&format!("secrets://default/demo/_/a2a/{AGENT_ID}")),
        "must name the secret address so an operator knows where to put it: {error}"
    );
    assert!(
        RUNTIME.block_on(send_message_bodies(&agent)).is_empty(),
        "a credentialed route with no credential must never be called unauthenticated"
    );
    Ok(())
}

/// The same route WITH the credential in the store is the control: it is the
/// missing secret that refused the call, not the harness.
#[test]
fn the_same_route_with_its_credential_calls_the_agent_and_carries_the_token() -> Result<()> {
    let _guard = env_guard();
    clear_env();
    let agent = RUNTIME.block_on(agent_answering(message_reply("an omelette")));
    let uri = format!("secrets://default/demo/_/a2a/{AGENT_ID}");
    let harness = Harness::new(
        Some(&sidecar(AGENT_ID, &agent.uri(), true)),
        Some(secrets(&[(uri.as_str(), "tok-1")])),
    )?;

    let value = harness.run("hello", Some(SESSION_HINT))?;

    assert_eq!(value["reply"], "an omelette", "{value}");
    let requests = RUNTIME
        .block_on(agent.received_requests())
        .expect("recorded");
    let post = requests
        .iter()
        .find(|request| request.method.as_str() == "POST")
        .expect("the agent must have been called");
    assert_eq!(
        post.headers
            .get("authorization")
            .and_then(|value| value.to_str().ok()),
        Some("Bearer tok-1")
    );
    for get in requests.iter().filter(|r| r.method.as_str() == "GET") {
        assert!(
            get.headers.get("authorization").is_none(),
            "the card fetch is public and must carry no credential"
        );
    }
    Ok(())
}

/// Decision 4. An agent the pack does not declare is an error naming it, never
/// a call to some other agent the pack happens to carry.
#[test]
fn an_agent_the_pack_does_not_declare_is_refused_rather_than_substituted() -> Result<()> {
    let _guard = env_guard();
    clear_env();
    let agent = RUNTIME.block_on(agent_answering(message_reply("an omelette")));
    // The sidecar declares `other-agent`; the node asks for AGENT_ID.
    let harness = Harness::asking(
        AGENT_ID,
        Some(&sidecar("other-agent", &agent.uri(), false)),
        None,
    )?;

    let value = harness.run("hello", Some(SESSION_HINT))?;

    assert_eq!(value["status"], "error", "{value}");
    assert_eq!(value["agent"], AGENT_ID, "{value}");
    assert!(
        value["error"].as_str().unwrap().contains(AGENT_ID),
        "{value}"
    );
    assert!(
        RUNTIME.block_on(send_message_bodies(&agent)).is_empty(),
        "the declared agent must not be called in place of the one asked for"
    );
    Ok(())
}

/// A pack with no sidecar at all declares no agents, so every `a2a` node in it
/// refuses — with a message naming the pack, because rebuilding that pack is
/// the fix.
#[test]
fn a_pack_with_no_sidecar_refuses_and_names_itself() -> Result<()> {
    let _guard = env_guard();
    clear_env();
    let harness = Harness::new(None, None)?;

    let value = harness.run("hello", Some(SESSION_HINT))?;

    assert_eq!(value["status"], "error", "{value}");
    assert!(
        value["error"].as_str().unwrap().contains(PACK_ID),
        "the refusal must name the pack to rebuild: {value}"
    );
    Ok(())
}

/// Decision 2, direction one. Two calls in ONE session continue the same
/// remote conversation: the second `SendMessage` carries the `contextId` the
/// agent named, and the `taskId` of the task it left open.
///
/// This is what makes `input_required` usable from a flow at all — the flow
/// routes the question to a card, comes back, and ANSWERS the remote instead
/// of asking it again.
#[test]
fn a_second_call_in_one_session_continues_the_same_remote_task() -> Result<()> {
    let _guard = env_guard();
    clear_env();
    let agent = RUNTIME.block_on(agent_answering(task_reply(
        "TASK_STATE_INPUT_REQUIRED",
        Some("Which city?"),
        None,
    )));
    let harness = Harness::new(Some(&sidecar(AGENT_ID, &agent.uri(), false)), None)?;

    let first = harness.run("book me a table", Some(SESSION_HINT))?;
    assert_eq!(first["status"], "input_required", "{first}");
    let second = harness.run("Jakarta", Some(SESSION_HINT))?;
    assert_eq!(second["status"], "input_required", "{second}");

    let bodies = RUNTIME.block_on(send_message_bodies(&agent));
    assert_eq!(bodies.len(), 2);
    assert!(
        bodies[0]["params"]["message"].get("contextId").is_none(),
        "the first call opens the conversation: {}",
        bodies[0]
    );
    assert_eq!(
        bodies[1]["params"]["message"]["contextId"], "ctx-1",
        "the second call must continue the same remote conversation: {}",
        bodies[1]
    );
    assert_eq!(
        bodies[1]["params"]["message"]["taskId"], "t-1",
        "the open task must be continued, not reopened: {}",
        bodies[1]
    );
    Ok(())
}

/// Decision 2, direction two. A run with no session hint belongs to no
/// conversation, so nothing is remembered and every call is its own remote
/// context. Same engine, same agent, same pack as the test above — only the
/// session differs.
#[test]
fn without_a_session_hint_every_call_is_its_own_remote_context() -> Result<()> {
    let _guard = env_guard();
    clear_env();
    let agent = RUNTIME.block_on(agent_answering(task_reply(
        "TASK_STATE_INPUT_REQUIRED",
        Some("Which city?"),
        None,
    )));
    let harness = Harness::new(Some(&sidecar(AGENT_ID, &agent.uri(), false)), None)?;

    harness.run("book me a table", None)?;
    harness.run("Jakarta", None)?;

    let bodies = RUNTIME.block_on(send_message_bodies(&agent));
    assert_eq!(bodies.len(), 2);
    for (index, body) in bodies.iter().enumerate() {
        assert!(
            body["params"]["message"].get("contextId").is_none()
                && body["params"]["message"].get("taskId").is_none(),
            "call {index} carried a continuation it cannot own: {body}"
        );
    }
    Ok(())
}

/// The other half of the continuation key: two DIFFERENT sessions in one
/// tenant never share a remote context, even against the same agent.
#[test]
fn two_sessions_never_share_a_remote_context() -> Result<()> {
    let _guard = env_guard();
    clear_env();
    let agent = RUNTIME.block_on(agent_answering(task_reply(
        "TASK_STATE_INPUT_REQUIRED",
        Some("Which city?"),
        None,
    )));
    let harness = Harness::new(Some(&sidecar(AGENT_ID, &agent.uri(), false)), None)?;

    harness.run("book me a table", Some(SESSION_HINT))?;
    harness.run("Jakarta", Some("demo:provider:chan:other-conv:user"))?;

    let bodies = RUNTIME.block_on(send_message_bodies(&agent));
    assert_eq!(bodies.len(), 2);
    assert!(
        bodies[1]["params"]["message"].get("contextId").is_none(),
        "another session's remote context must not be reused: {}",
        bodies[1]
    );
    Ok(())
}
