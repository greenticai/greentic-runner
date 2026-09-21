//! Tests for the A2A tool source.
//!
//! Split by concern:
//! - `catalog` — catalog building from live agent cards, resilience to dead
//!   or malformed agents
//! - `dispatch` — `SendMessage` request/response handling

use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

mod catalog;
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
