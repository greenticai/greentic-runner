//! Dispatch tests: `SendMessage` request/response handling.

use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::{RPC_PATH, mount_card, source_for};

#[tokio::test]
async fn calling_an_agent_posts_send_message_and_returns_its_text() {
    let server = MockServer::start().await;
    mount_card(&server).await;
    Mock::given(method("POST"))
        .and(path(RPC_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0", "id": 1,
            "result": {
                "message": {
                    "messageId": "m-2",
                    "role": "ROLE_AGENT",
                    "parts": [{ "text": "an omelette" }]
                }
            }
        })))
        .mount(&server)
        .await;

    let source = source_for(vec![("recipe", server.uri())]);
    let reply = source
        .call("recipe", "what can I make with eggs?")
        .await
        .expect("call must succeed");

    assert_eq!(reply, "an omelette");

    let requests = server.received_requests().await.expect("request log");
    let rpc_request = requests
        .iter()
        .find(|r| r.method.as_str() == "POST")
        .expect("a POST must have reached the mock server");
    let body: serde_json::Value = rpc_request.body_json().expect("a json body");
    assert_eq!(body["method"], "SendMessage", "method must be PascalCase");
    assert_eq!(
        body["params"]["message"]["parts"][0]["text"], "what can I make with eggs?",
        "the prompt text must reach the agent"
    );
}

#[tokio::test]
async fn a_json_rpc_error_reply_becomes_a_tool_error_carrying_its_message() {
    let server = MockServer::start().await;
    mount_card(&server).await;
    Mock::given(method("POST"))
        .and(path(RPC_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0", "id": 1,
            "error": { "code": -32602, "message": "bad params" }
        })))
        .mount(&server)
        .await;

    let source = source_for(vec![("recipe", server.uri())]);
    let err = source
        .call("recipe", "what can I make with eggs?")
        .await
        .expect_err("a json-rpc error must become an Err");

    assert_eq!(
        err, "bad params",
        "the agent's own sentence must not be replaced with a generic one"
    );
}

#[tokio::test]
async fn an_unknown_agent_id_is_an_error_naming_the_id() {
    let source = source_for(vec![("recipe", "http://127.0.0.1:1/".to_string())]);

    let err = source
        .call("not-recipe", "hi")
        .await
        .expect_err("an unbound agent id must be an error");

    assert!(
        err.contains("not-recipe"),
        "an operator must be able to see which binding is wrong, got: {err}"
    );
}

#[tokio::test]
async fn a_non_success_http_status_is_reported_with_its_code() {
    // A JSON-RPC error arrives inside a 200; a 5xx is a transport failure, and
    // the operator needs its code rather than "unreadable reply".
    let server = MockServer::start().await;
    mount_card(&server).await;
    Mock::given(method("POST"))
        .and(path(RPC_PATH))
        .respond_with(ResponseTemplate::new(502).set_body_string("<html>bad gateway</html>"))
        .mount(&server)
        .await;

    let source = source_for(vec![("recipe", server.uri())]);
    let err = source
        .call("recipe", "hi")
        .await
        .expect_err("a 502 must be an Err");

    assert!(err.contains("502"), "the status must be named, got: {err}");
}
