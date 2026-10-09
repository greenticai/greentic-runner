//! Tests for the A2A tool source.
//!
//! Split by concern:
//! - `catalog` — catalog building from live agent cards, resilience to dead
//!   or malformed agents
//! - `credentials` — per-call credential resolution and where it may be sent
//! - `dispatch` — `SendMessage` request/response handling

use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

mod catalog;
mod credentials;
mod dispatch;

/// A real-shaped agent card. `PLACEHOLDER` becomes the interface URL, so a
/// dispatch test can post back to the same mock server that served the card.
const CARD: &str = r#"{
  "name": "Recipe Agent",
  "description": "Helps with recipes and cooking.",
  "version": "1.0.0",
  "supportedInterfaces": [
    { "url": "PLACEHOLDER", "protocolBinding": "JSONRPC", "protocolVersion": "1.0" }
  ],
  "capabilities": { "streaming": false, "pushNotifications": false },
  "defaultInputModes": ["text/plain"],
  "defaultOutputModes": ["text/plain"],
  "skills": [
    { "id": "suggest", "name": "Suggest a recipe", "description": "Suggests a dish.", "tags": ["cooking"] }
  ]
}"#;

/// The path every test's agent interface lives at.
const RPC_PATH: &str = "/a2a";

/// Serve [`CARD`] at the well-known path, its interface pointing at
/// [`RPC_PATH`] on the same server.
async fn mount_card(server: &MockServer) {
    let card = CARD.replace("PLACEHOLDER", &format!("{}{RPC_PATH}", server.uri()));
    mount_raw_card(server, card).await;
}

/// Serve `card` verbatim at the well-known path, for tests that need a card
/// [`mount_card`] cannot express (a tenant, a foreign interface URL).
async fn mount_raw_card(server: &MockServer, card: String) {
    Mock::given(method("GET"))
        .and(path("/.well-known/agent-card.json"))
        .respond_with(ResponseTemplate::new(200).set_body_string(card))
        .mount(server)
        .await;
}

/// A source over the given `(agent_id, base)` pairs.
fn source_for(agents: Vec<(&str, String)>) -> crate::a2a_source::A2aToolSource {
    crate::a2a_source::A2aToolSource::new(
        agents
            .into_iter()
            .map(|(id, base)| (id.to_string(), base))
            .collect(),
    )
    .expect("a client with no redirect policy and a timeout is buildable")
}

/// Answer the agent's `SendMessage` with `body` as the JSON-RPC response.
async fn mount_reply(server: &MockServer, body: serde_json::Value) {
    Mock::given(method("POST"))
        .and(path(RPC_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(server)
        .await;
}

/// A secrets manager whose entries can change between calls (rotation), and
/// which counts every read so a test can prove the store was never consulted.
struct TestSecrets {
    entries: std::sync::Mutex<std::collections::HashMap<String, Vec<u8>>>,
    reads: std::sync::atomic::AtomicUsize,
}

impl TestSecrets {
    fn with(pairs: &[(&str, &str)]) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            entries: std::sync::Mutex::new(
                pairs
                    .iter()
                    .map(|(k, v)| ((*k).to_string(), v.as_bytes().to_vec()))
                    .collect(),
            ),
            reads: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    fn set(&self, uri: &str, value: &str) {
        self.entries
            .lock()
            .unwrap()
            .insert(uri.to_string(), value.as_bytes().to_vec());
    }

    fn reads(&self) -> usize {
        self.reads.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl greentic_secrets_lib::SecretsManager for TestSecrets {
    async fn read(&self, path: &str) -> greentic_secrets_lib::Result<Vec<u8>> {
        self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.entries
            .lock()
            .unwrap()
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

/// One sidecar-shaped route.
fn route(
    agent_id: &str,
    base: String,
    header: Option<&str>,
    team: Option<&str>,
    requires_auth: bool,
) -> crate::a2a_source::A2aRoute {
    crate::a2a_source::A2aRoute {
        agent_id: agent_id.to_string(),
        base_url: base,
        auth_header_name: header.map(str::to_string),
        auth_team: team.map(str::to_string),
        requires_auth,
    }
}

/// A source for tenant `acme` resolving credentials from `secrets`.
fn credentialed_source(
    routes: Vec<crate::a2a_source::A2aRoute>,
    secrets: std::sync::Arc<TestSecrets>,
    unit: Option<&str>,
) -> crate::a2a_source::A2aToolSource {
    let secrets: std::sync::Arc<dyn greentic_secrets_lib::SecretsManager> = secrets;
    crate::a2a_source::A2aToolSource::from_routes(
        routes,
        Some(secrets),
        "acme",
        unit.map(str::to_string),
    )
    .expect("a client with no redirect policy and a timeout is buildable")
}

/// The value of header `name` on a recorded request, if present.
fn header_of(request: &wiremock::Request, name: &str) -> Option<String> {
    request
        .headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
}
