//! `message.metadata.caller` placement on the A2A `SendMessage` request.

use chrono::DateTime;
use serde_json::{Value, json};
use wiremock::MockServer;

use super::{mount_card, mount_reply, source_for};
use crate::a2a_source::{A2aCallOptions, A2aContinuations};
use crate::tenant::TenantContext;

fn text_reply() -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": {"message": {
            "messageId": "r1", "role": "ROLE_AGENT",
            "parts": [{"text": "ok"}]
        }}
    })
}

async fn sent_body(server: &MockServer) -> Value {
    let requests = server.received_requests().await.expect("request log");
    requests
        .iter()
        .find(|r| r.method.as_str() == "POST")
        .expect("a POST reached the agent")
        .body_json()
        .expect("a json body")
}

async fn send_with(caller: Option<Value>) -> Value {
    let server = MockServer::start().await;
    mount_card(&server).await;
    mount_reply(&server, text_reply()).await;
    let catalog = source_for(vec![("recipe", server.uri())]).catalog().await;
    catalog
        .dispatch_in_conversation_with(
            "recipe",
            &json!("eggs?"),
            &TenantContext::new("acme", "prod"),
            &mut A2aContinuations::default(),
            DateTime::from_timestamp(1_800_000_000, 0).expect("ts"),
            A2aCallOptions {
                caller,
                ..Default::default()
            },
        )
        .await;
    sent_body(&server).await
}

#[tokio::test]
async fn no_caller_option_leaves_the_message_without_metadata() {
    let body = send_with(None).await;
    let message = &body["params"]["message"];
    assert!(message.get("metadata").is_none(), "got: {message}");
}

#[tokio::test]
async fn a_caller_option_lands_on_message_metadata_not_request_metadata() {
    let block = json!({"sub": "u-1", "teams": ["t"], "groups": [], "user_verified": true});
    let body = send_with(Some(block.clone())).await;
    assert_eq!(body["params"]["message"]["metadata"]["caller"], block);
    assert!(body["params"].get("metadata").is_none(), "got: {body}");
}

#[tokio::test]
async fn golden_without_a_caller_the_message_has_exactly_the_pre_existing_keys() {
    let body = send_with(None).await;
    let mut keys: Vec<&str> = body["params"]["message"]
        .as_object()
        .expect("message object")
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(keys, ["messageId", "parts", "role"], "got: {body}");
    assert_eq!(
        body["params"]["message"]["parts"],
        json!([{"text": "eggs?"}])
    );
}
