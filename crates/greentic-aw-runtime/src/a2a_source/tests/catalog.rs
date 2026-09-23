//! Catalog-building tests: an agent's tool entry comes from its live card,
//! and one dead or malformed agent must not remove another's.

use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::{mount_card, mount_raw_card, source_for};

#[tokio::test]
async fn a_catalog_entry_describes_the_agent_from_its_card() {
    let server = MockServer::start().await;
    mount_card(&server).await;

    let source = source_for(vec![("recipe", server.uri())]);
    let catalog = source.catalog().await;

    let entry = catalog.tool_entry("recipe").expect("entry present");
    assert_eq!(entry.description, "Helps with recipes and cooking.");
    assert!(
        catalog.error_for("recipe").is_none(),
        "a fetched card must not also be recorded as an error"
    );
}

#[tokio::test]
async fn an_unreachable_agent_yields_no_entry_rather_than_failing_the_catalog() {
    // Nothing listens on port 1. A dead agent must not remove a worker's
    // OTHER tools, so the catalog still builds.
    let source = source_for(vec![("dead", "http://127.0.0.1:1/".to_string())]);
    let catalog = source.catalog().await;

    assert!(catalog.tool_entry("dead").is_none());
    assert!(
        catalog.error_for("dead").is_some(),
        "a dispatch error later has to name the real cause"
    );
}

#[tokio::test]
async fn one_dead_agent_does_not_hide_a_live_one() {
    let server = MockServer::start().await;
    mount_card(&server).await;

    let source = source_for(vec![
        ("live", server.uri()),
        ("dead", "http://127.0.0.1:1/".to_string()),
    ]);
    let catalog = source.catalog().await;

    assert!(
        catalog.tool_entry("live").is_some(),
        "the live agent's entry must survive alongside the dead one"
    );
    assert!(catalog.tool_entry("dead").is_none());
    assert!(catalog.error_for("dead").is_some());
}

#[tokio::test]
async fn an_agent_serving_the_spec_shaped_security_requirements_still_yields_a_tool() {
    // The A2A proto wraps a requirement's scopes in a `StringList`, so a card
    // says `{"schemes": {"main": {"list": []}}}` and not `{"main": []}`. Our
    // own deployed workers serve the wrapped form. `serde` fails the WHOLE
    // card on one unreadable field, so reading only the bare form did not
    // merely lose the requirement — it dropped the agent out of the worker's
    // tool list entirely, with a warn line as the only signal.
    let server = MockServer::start().await;
    let card = r#"{
      "name": "Recipe Agent",
      "description": "Helps with recipes and cooking.",
      "version": "1.0.0",
      "supportedInterfaces": [
        { "url": "https://api.example.com/a2a", "protocolBinding": "JSONRPC", "protocolVersion": "1.0" }
      ],
      "capabilities": { "streaming": false },
      "defaultInputModes": ["text/plain"],
      "defaultOutputModes": ["text/plain"],
      "securitySchemes": {
        "main": { "httpAuthSecurityScheme": { "scheme": "bearer" } }
      },
      "securityRequirements": [{ "schemes": { "main": { "list": ["read"] } } }],
      "skills": [
        { "id": "suggest", "name": "Suggest a recipe", "description": "Suggests a dish.", "tags": ["cooking"] }
      ]
    }"#;
    mount_raw_card(&server, card.to_string()).await;

    let source = source_for(vec![("recipe", server.uri())]);
    let catalog = source.catalog().await;

    let entry = catalog
        .tool_entry("recipe")
        .expect("a spec-shaped card must still produce a tool");
    assert_eq!(entry.description, "Helps with recipes and cooking.");
    assert!(catalog.error_for("recipe").is_none());
}

/// [`super::CARD`] with `capabilities.extensions` set to `extensions`.
fn card_declaring_extensions(server: &MockServer, extensions: &str) -> String {
    super::CARD
        .replace("PLACEHOLDER", &format!("{}/a2a", server.uri()))
        .replace(
            r#""capabilities": { "streaming": false, "pushNotifications": false }"#,
            &format!(r#""capabilities": {{ "streaming": false, "extensions": {extensions} }}"#),
        )
}

#[tokio::test]
async fn an_agent_requiring_an_extension_we_do_not_implement_yields_no_tools() {
    // `required` means the client "must understand and comply with the
    // extension's requirements", so this agent has told us its wire behaviour
    // is not the plain protocol. We implement no extensions, so binding its
    // tools would mean calling it as though it were plain — a violation it
    // warned us about, and one that surfaces as wrong answers rather than as
    // an error, because the request we would send is well-formed A2A.
    let server = MockServer::start().await;
    let card = card_declaring_extensions(
        &server,
        r#"[{ "uri": "https://example.com/ext/signing", "required": true }]"#,
    );
    mount_raw_card(&server, card).await;

    let source = source_for(vec![("recipe", server.uri())]);
    let catalog = source.catalog().await;

    assert!(
        catalog.tool_entry("recipe").is_none(),
        "a required extension we cannot honour must not produce a bindable tool"
    );
    let reason = catalog
        .error_for("recipe")
        .expect("the refusal must be reported like any other unusable agent");
    assert!(
        reason.contains("https://example.com/ext/signing"),
        "the reason must name the extension so an operator can act on it, got {reason:?}"
    );
}

#[tokio::test]
async fn calling_an_agent_that_requires_an_unimplemented_extension_sends_nothing() {
    // The catalogue entry alone would not have stopped this: `dispatch`
    // attempts a call for every CONFIGURED agent, entry or not. No
    // `SendMessage` mock is mounted, so any POST reaching the server fails
    // the call for the wrong reason — the assertion is on the message.
    let server = MockServer::start().await;
    let card = card_declaring_extensions(
        &server,
        r#"[{ "uri": "https://example.com/ext/signing", "required": true }]"#,
    );
    mount_raw_card(&server, card).await;

    let source = source_for(vec![("recipe", server.uri())]);
    let catalog = source.catalog().await;
    let result = catalog
        .dispatch("recipe", &serde_json::json!("hello"))
        .await;

    let error = result["error"].as_str().expect("an error value");
    assert!(
        error.contains("https://example.com/ext/signing"),
        "the refusal must name the extension, got {error:?}"
    );
}

#[tokio::test]
async fn an_optional_extension_changes_nothing() {
    // `required: false` — and an absent `required`, which is how proto3 emits
    // false — are the agent saying "I support this if you do". Refusing those
    // would strand every agent that advertises an extension at all.
    let server = MockServer::start().await;
    let card = card_declaring_extensions(
        &server,
        r#"[
          { "uri": "https://example.com/ext/one", "required": false },
          { "uri": "https://example.com/ext/two", "description": "no required key" }
        ]"#,
    );
    mount_raw_card(&server, card).await;

    let source = source_for(vec![("recipe", server.uri())]);
    let catalog = source.catalog().await;

    let entry = catalog
        .tool_entry("recipe")
        .expect("an optional extension must behave exactly as no extension does");
    assert_eq!(entry.description, "Helps with recipes and cooking.");
    assert!(catalog.error_for("recipe").is_none());
}

#[tokio::test]
async fn a_card_that_is_not_valid_json_is_recorded_as_an_error_not_a_panic() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/.well-known/agent-card.json"))
        .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
        .mount(&server)
        .await;

    let source = source_for(vec![("bad", server.uri())]);
    let catalog = source.catalog().await;

    assert!(catalog.tool_entry("bad").is_none());
    assert!(catalog.error_for("bad").is_some());
}
