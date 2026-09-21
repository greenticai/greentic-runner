//! Dispatch tests: `SendMessage` request/response handling.

use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::{CARD, RPC_PATH, mount_card, mount_raw_card, mount_reply, source_for};

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

    assert!(
        err.contains("bad params"),
        "the agent's own sentence must survive, got: {err}"
    );
    assert!(
        err.contains("recipe"),
        "the error must name the agent, like every other path, got: {err}"
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

    // "HTTP 502", not "502": reqwest's own error text carries the URL, whose
    // random port could contain "502" and pass this test for the wrong reason.
    assert!(
        err.contains("HTTP 502"),
        "the status must be named, got: {err}"
    );
}

#[tokio::test]
async fn the_interface_tenant_is_echoed_into_the_request() {
    // The A2A spec makes echoing the interface's tenant a MUST for clients.
    let server = MockServer::start().await;
    let card = CARD
        .replace("PLACEHOLDER", &format!("{}{RPC_PATH}", server.uri()))
        .replace(
            r#""protocolVersion": "1.0" }"#,
            r#""protocolVersion": "1.0", "tenant": "acme" }"#,
        );
    mount_raw_card(&server, card).await;
    mount_reply(&server, text_reply("ok")).await;

    source_for(vec![("recipe", server.uri())])
        .call("recipe", "hi")
        .await
        .expect("call must succeed");

    let requests = server.received_requests().await.expect("request log");
    let rpc = requests
        .iter()
        .find(|r| r.method.as_str() == "POST")
        .expect("a POST must have reached the mock server");
    let body: serde_json::Value = rpc.body_json().expect("a json body");
    assert_eq!(body["params"]["tenant"], "acme");
}

#[tokio::test]
async fn a_card_naming_a_plaintext_remote_interface_is_refused_before_sending() {
    // The card itself is fetched securely, but the interface it names is just
    // text in that card. An https card pointing at plain http elsewhere would
    // otherwise send the message, and later a credential, in the clear.
    let server = MockServer::start().await;
    let card = CARD.replace("PLACEHOLDER", "http://agent.example.com/a2a");
    mount_raw_card(&server, card).await;

    let err = source_for(vec![("recipe", server.uri())])
        .call("recipe", "hi")
        .await
        .expect_err("a plaintext remote interface must be refused");

    // Assert the REASON. Without the check this still fails — the made-up host
    // does not resolve — so "an error that names the agent" alone would pass
    // for the wrong reason. Only the scheme refusal says "must be https".
    assert!(
        err.contains("recipe") && err.contains("must be https"),
        "must be refused for its scheme, not fail on the network, got: {err}"
    );
    let posts = server
        .received_requests()
        .await
        .expect("request log")
        .iter()
        .filter(|r| r.method.as_str() == "POST")
        .count();
    assert_eq!(posts, 0, "nothing may be sent to a refused interface");
}

#[tokio::test]
async fn a_multi_part_reply_keeps_its_parts_apart() {
    let server = MockServer::start().await;
    mount_card(&server).await;
    mount_reply(
        &server,
        json!({"jsonrpc": "2.0", "id": 1, "result": {"message": {
            "messageId": "m-2", "role": "ROLE_AGENT",
            "parts": [{"text": "Step 1."}, {"data": {"k": 1}}, {"text": "Step 2."}]
        }}}),
    )
    .await;

    let reply = source_for(vec![("recipe", server.uri())])
        .call("recipe", "hi")
        .await
        .expect("call must succeed");

    assert_eq!(reply, "Step 1.\nStep 2.");
}

#[tokio::test]
async fn a_reply_with_no_text_part_is_an_error_not_an_empty_result() {
    let server = MockServer::start().await;
    mount_card(&server).await;
    mount_reply(
        &server,
        json!({"jsonrpc": "2.0", "id": 1, "result": {"message": {
            "messageId": "m-2", "role": "ROLE_AGENT", "parts": [{"data": {"k": 1}}]
        }}}),
    )
    .await;

    let err = source_for(vec![("recipe", server.uri())])
        .call("recipe", "hi")
        .await
        .expect_err("an empty tool result tells the model nothing");

    assert!(err.contains("no text part"), "got: {err}");
}

#[tokio::test]
async fn a_completed_task_is_answered_from_its_artifacts() {
    let server = MockServer::start().await;
    mount_card(&server).await;
    mount_reply(&server, task_reply("TASK_STATE_COMPLETED", "an omelette")).await;

    let reply = source_for(vec![("recipe", server.uri())])
        .call("recipe", "hi")
        .await
        .expect("a completed task is a result");

    assert_eq!(reply, "an omelette");
}

#[tokio::test]
async fn a_failed_task_is_an_error_so_the_model_does_not_read_it_as_success() {
    let server = MockServer::start().await;
    mount_card(&server).await;
    mount_reply(&server, task_reply("TASK_STATE_FAILED", "ignored")).await;

    let err = source_for(vec![("recipe", server.uri())])
        .call("recipe", "hi")
        .await
        .expect_err("a failed task must not be an Ok result");

    assert!(
        err.contains("recipe") && err.contains("Failed"),
        "got: {err}"
    );
}

#[tokio::test]
async fn a_task_still_in_flight_is_reported_as_pending() {
    let server = MockServer::start().await;
    mount_card(&server).await;
    mount_reply(&server, task_reply("TASK_STATE_WORKING", "ignored")).await;

    let reply = source_for(vec![("recipe", server.uri())])
        .call("recipe", "hi")
        .await
        .expect("an in-flight task is pending, not failed");

    assert!(
        reply.contains("t-1") && reply.contains("Working"),
        "got: {reply}"
    );
}

/// A JSON-RPC reply carrying a direct message with one text part.
fn text_reply(text: &str) -> serde_json::Value {
    json!({"jsonrpc": "2.0", "id": 1, "result": {"message": {
        "messageId": "m-2", "role": "ROLE_AGENT", "parts": [{"text": text}]
    }}})
}

/// A JSON-RPC reply carrying task `t-1` in `state`, with one text artifact.
fn task_reply(state: &str, artifact_text: &str) -> serde_json::Value {
    json!({"jsonrpc": "2.0", "id": 1, "result": {"task": {
        "id": "t-1",
        "status": {"state": state},
        "artifacts": [{"parts": [{"text": artifact_text}]}]
    }}})
}
