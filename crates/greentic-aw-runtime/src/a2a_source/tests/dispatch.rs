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
        err.contains("recipe") && err.contains("failed"),
        "got: {err}"
    );
}

#[tokio::test]
async fn a_task_still_in_flight_is_not_returned_as_a_reply() {
    // `call` has a `Result<String, String>` and no third arm, so a task that
    // has not answered can only be an `Err` there. It used to be `Ok` of a
    // sentence about our own polling support, which the model read as the
    // agent's answer. The structured shape lives on the catalogue path.
    let server = MockServer::start().await;
    mount_card(&server).await;
    mount_reply(&server, task_reply("TASK_STATE_WORKING", "ignored")).await;

    let err = source_for(vec![("recipe", server.uri())])
        .call("recipe", "hi")
        .await
        .expect_err("an in-flight task is not a reply");

    assert!(err.contains("recipe"), "got: {err}");
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

#[tokio::test]
async fn a_catalogue_dispatches_a_call_and_wraps_the_reply() {
    let server = MockServer::start().await;
    mount_card(&server).await;
    mount_reply(&server, text_reply("an omelette")).await;

    let catalog = source_for(vec![("recipe", server.uri())]).catalog().await;
    let value = catalog
        .dispatch("recipe", &json!({ "message": "eggs?" }))
        .await;

    assert_eq!(
        value,
        json!({ "status": "completed", "agent": "recipe", "reply": "an omelette" })
    );
    let requests = server.received_requests().await.expect("request log");
    let rpc = requests
        .iter()
        .find(|r| r.method.as_str() == "POST")
        .expect("a POST must have reached the agent");
    let body: serde_json::Value = rpc.body_json().expect("a json body");
    assert_eq!(
        body["params"]["message"]["parts"][0]["text"], "eggs?",
        "the `message` field is what the agent receives"
    );
}

#[tokio::test]
async fn a_catalogue_built_while_an_agent_was_down_still_calls_it_once_it_is_back() {
    // The listing advertises an unreachable agent's tool from its author
    // contract. That fallback is only worth anything if dispatch still tries.
    let server = MockServer::start().await;
    let catalog = source_for(vec![("recipe", server.uri())]).catalog().await;
    assert!(
        catalog.tool_entry("recipe").is_none(),
        "precondition: no card was served when the catalogue was built"
    );

    mount_card(&server).await;
    mount_reply(&server, text_reply("back again")).await;

    let value = catalog.dispatch("recipe", &json!("hi")).await;
    assert_eq!(
        value,
        json!({ "status": "completed", "agent": "recipe", "reply": "back again" })
    );
}

#[tokio::test]
async fn dispatching_an_unconfigured_agent_is_an_error_value_naming_it() {
    let server = MockServer::start().await;
    let catalog = source_for(vec![("recipe", server.uri())]).catalog().await;

    let value = catalog.dispatch("stranger", &json!("hi")).await;

    let error = value["error"]
        .as_str()
        .expect("an error value, not a panic");
    assert!(error.contains("stranger"), "got: {error}");
}

#[tokio::test]
async fn a_failed_call_is_an_error_value_the_model_can_read() {
    let server = MockServer::start().await;
    mount_card(&server).await;
    mount_reply(
        &server,
        json!({"jsonrpc": "2.0", "id": 1, "error": {"code": -32000, "message": "overloaded"}}),
    )
    .await;

    let catalog = source_for(vec![("recipe", server.uri())]).catalog().await;
    let value = catalog.dispatch("recipe", &json!("hi")).await;

    let error = value["error"].as_str().expect("an error value");
    assert!(error.contains("overloaded"), "got: {error}");
}

// --- Multi-turn continuation ---------------------------------------------
//
// The gap these close: every `SendMessage` used to carry `contextId: None`,
// so a remote agent that answered `input-required` was asking a question
// nothing could ever answer. The next call opened a new task and the remote
// asked again.

use chrono::{DateTime, Utc};

use crate::a2a_source::{A2aContinuations, A2aToolCatalog};
use crate::tenant::TenantContext;

fn acme() -> TenantContext {
    TenantContext::new("acme", "prod")
}

fn at(secs: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(1_800_000_000 + secs, 0).expect("a valid timestamp")
}

/// A JSON-RPC reply carrying a task in `state` with `contextId` and a status
/// message, which is how a real agent asks for more input.
fn task_in_context(id: &str, context_id: &str, state: &str, says: &str) -> serde_json::Value {
    json!({"jsonrpc": "2.0", "id": 1, "result": {"task": {
        "id": id,
        "contextId": context_id,
        "status": {
            "state": state,
            "message": {
                "messageId": "m-s", "role": "ROLE_AGENT",
                "parts": [{"text": says}]
            }
        }
    }}})
}

/// The `{contextId, taskId}` each POST carried, in order.
async fn sent_references(server: &MockServer) -> Vec<(Option<String>, Option<String>)> {
    server
        .received_requests()
        .await
        .expect("request log")
        .iter()
        .filter(|r| r.method.as_str() == "POST")
        .map(|r| {
            let body: serde_json::Value = r.body_json().expect("a json body");
            let get = |k: &str| body["params"]["message"][k].as_str().map(str::to_string);
            (get("contextId"), get("taskId"))
        })
        .collect()
}

#[tokio::test]
async fn a_follow_up_resumes_the_same_remote_task_instead_of_starting_a_new_one() {
    let server = MockServer::start().await;
    mount_card(&server).await;
    // Turn 1: the agent parks on input-required. Turn 2: it completes.
    Mock::given(method("POST"))
        .and(path(RPC_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(task_in_context(
            "t-1",
            "ctx-1",
            "TASK_STATE_INPUT_REQUIRED",
            "Which city?",
        )))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    mount_reply(
        &server,
        json!({"jsonrpc": "2.0", "id": 1, "result": {"task": {
            "id": "t-1",
            "contextId": "ctx-1",
            "status": {"state": "TASK_STATE_COMPLETED"},
            "artifacts": [{"parts": [{"text": "Hotel Diagonal, Barcelona"}]}]
        }}}),
    )
    .await;

    let catalog = source_for(vec![("travel", server.uri())]).catalog().await;
    let tenant = acme();
    let mut state = A2aContinuations::default();

    let first = catalog
        .dispatch_in_conversation(
            "travel",
            &json!({"message": "book a hotel in Spain"}),
            &tenant,
            &mut state,
            at(0),
        )
        .await;
    assert_eq!(first["status"], "input_required", "got: {first}");
    assert_eq!(first["question"], "Which city?");

    let second = catalog
        .dispatch_in_conversation(
            "travel",
            &json!({"message": "Barcelona"}),
            &tenant,
            &mut state,
            at(30),
        )
        .await;
    assert_eq!(second["status"], "completed", "got: {second}");
    assert_eq!(second["reply"], "Hotel Diagonal, Barcelona");

    let sent = sent_references(&server).await;
    assert_eq!(sent.len(), 2, "two calls must have reached the agent");
    assert_eq!(
        sent[0],
        (None, None),
        "the FIRST call opens the conversation and names nothing"
    );
    assert_eq!(
        sent[1],
        (Some("ctx-1".into()), Some("t-1".into())),
        "the follow-up must resume the SAME remote context and task"
    );
}

#[tokio::test]
async fn a_fresh_conversation_gets_a_fresh_remote_context() {
    let server = MockServer::start().await;
    mount_card(&server).await;
    mount_reply(
        &server,
        task_in_context("t-1", "ctx-1", "TASK_STATE_INPUT_REQUIRED", "Which city?"),
    )
    .await;

    let catalog = source_for(vec![("travel", server.uri())]).catalog().await;
    let tenant = acme();
    // Two conversations == two `ConversationState`s == two of these.
    let mut one = A2aContinuations::default();
    let mut two = A2aContinuations::default();

    catalog
        .dispatch_in_conversation("travel", &json!("hi"), &tenant, &mut one, at(0))
        .await;
    catalog
        .dispatch_in_conversation("travel", &json!("hi"), &tenant, &mut two, at(1))
        .await;

    let sent = sent_references(&server).await;
    assert_eq!(
        sent,
        vec![(None, None), (None, None)],
        "a second conversation must not inherit the first's remote context"
    );
    // And each now holds its own.
    assert!(one.resume(&tenant, "travel", at(2)).is_some());
    assert!(two.resume(&tenant, "travel", at(2)).is_some());
}

#[tokio::test]
async fn a_conversation_never_sends_another_tenants_remote_context() {
    let server = MockServer::start().await;
    mount_card(&server).await;
    mount_reply(
        &server,
        task_in_context(
            "t-1",
            "ctx-acme",
            "TASK_STATE_INPUT_REQUIRED",
            "Which city?",
        ),
    )
    .await;

    let catalog = source_for(vec![("travel", server.uri())]).catalog().await;
    let mut state = A2aContinuations::default();
    catalog
        .dispatch_in_conversation("travel", &json!("hi"), &acme(), &mut state, at(0))
        .await;

    // The same state blob, reached while running as a different tenant.
    let globex = TenantContext::new("globex", "prod");
    catalog
        .dispatch_in_conversation("travel", &json!("hi"), &globex, &mut state, at(1))
        .await;

    let sent = sent_references(&server).await;
    assert_eq!(
        sent[1],
        (None, None),
        "globex must not resume acme's remote conversation"
    );
}

#[tokio::test]
async fn a_completed_task_keeps_the_context_but_drops_the_task_id() {
    // §7: contextId is the conversation, taskId is one execution. A completed
    // task will not accept another message, so sending its id again is at
    // best ignored; the context is what makes the next question land in the
    // same remote thread.
    let server = MockServer::start().await;
    mount_card(&server).await;
    mount_reply(
        &server,
        json!({"jsonrpc": "2.0", "id": 1, "result": {"task": {
            "id": "t-1",
            "contextId": "ctx-1",
            "status": {"state": "TASK_STATE_COMPLETED"},
            "artifacts": [{"parts": [{"text": "done"}]}]
        }}}),
    )
    .await;

    let catalog = source_for(vec![("travel", server.uri())]).catalog().await;
    let tenant = acme();
    let mut state = A2aContinuations::default();
    catalog
        .dispatch_in_conversation("travel", &json!("hi"), &tenant, &mut state, at(0))
        .await;

    let held = state.resume(&tenant, "travel", at(1)).expect("a context");
    assert_eq!(held.context_id, "ctx-1");
    assert_eq!(held.task_id, None, "a finished task must not be resumed");
}

#[tokio::test]
async fn a_failed_task_drops_the_task_id_and_never_reads_as_an_answer() {
    let server = MockServer::start().await;
    mount_card(&server).await;
    mount_reply(
        &server,
        task_in_context("t-1", "ctx-1", "TASK_STATE_FAILED", "upstream timeout"),
    )
    .await;

    let catalog = source_for(vec![("travel", server.uri())]).catalog().await;
    let tenant = acme();
    let mut state = A2aContinuations::default();
    let value = catalog
        .dispatch_in_conversation("travel", &json!("hi"), &tenant, &mut state, at(0))
        .await;

    assert_eq!(value["status"], "failed", "got: {value}");
    assert_eq!(value["state"], "failed");
    assert!(
        value.get("reply").is_none(),
        "a failure must not carry a reply key; got: {value}"
    );
    let error = value["error"].as_str().expect("an error sentence");
    assert!(error.contains("travel") && error.contains("upstream timeout"));
    assert_eq!(
        state.resume(&tenant, "travel", at(1)).unwrap().task_id,
        None
    );
}

#[tokio::test]
async fn an_auth_required_task_is_a_failure_that_names_authentication() {
    let server = MockServer::start().await;
    mount_card(&server).await;
    mount_reply(
        &server,
        task_in_context("t-1", "ctx-1", "TASK_STATE_AUTH_REQUIRED", "log in first"),
    )
    .await;

    let catalog = source_for(vec![("travel", server.uri())]).catalog().await;
    let value = catalog
        .dispatch_in_conversation(
            "travel",
            &json!("hi"),
            &acme(),
            &mut A2aContinuations::default(),
            at(0),
        )
        .await;

    assert_eq!(value["status"], "failed");
    assert_eq!(value["state"], "auth_required");
    assert!(value.get("reply").is_none());
}

#[tokio::test]
async fn a_transport_failure_keeps_the_conversation_rather_than_losing_it() {
    // One flaky call says nothing about the remote task. Dropping the
    // continuation here would strand an open `input-required` exchange.
    let server = MockServer::start().await;
    mount_card(&server).await;
    Mock::given(method("POST"))
        .and(path(RPC_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(task_in_context(
            "t-1",
            "ctx-1",
            "TASK_STATE_INPUT_REQUIRED",
            "Which city?",
        )))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(RPC_PATH))
        .respond_with(ResponseTemplate::new(502).set_body_string("bad gateway"))
        .mount(&server)
        .await;

    let catalog = source_for(vec![("travel", server.uri())]).catalog().await;
    let tenant = acme();
    let mut state = A2aContinuations::default();
    catalog
        .dispatch_in_conversation("travel", &json!("hi"), &tenant, &mut state, at(0))
        .await;
    let value = catalog
        .dispatch_in_conversation("travel", &json!("Barcelona"), &tenant, &mut state, at(1))
        .await;

    assert_eq!(value["status"], "error", "got: {value}");
    assert!(value.get("reply").is_none());
    let held = state
        .resume(&tenant, "travel", at(2))
        .expect("the conversation survives a flaky call");
    assert_eq!(held.task_id.as_deref(), Some("t-1"));
}

#[tokio::test]
async fn an_idle_conversation_past_the_ttl_starts_a_fresh_remote_context() {
    let server = MockServer::start().await;
    mount_card(&server).await;
    mount_reply(
        &server,
        task_in_context("t-1", "ctx-1", "TASK_STATE_INPUT_REQUIRED", "Which city?"),
    )
    .await;

    let catalog = source_for(vec![("travel", server.uri())]).catalog().await;
    let tenant = acme();
    let mut state = A2aContinuations::default();
    catalog
        .dispatch_in_conversation("travel", &json!("hi"), &tenant, &mut state, at(0))
        .await;
    catalog
        .dispatch_in_conversation(
            "travel",
            &json!("Barcelona"),
            &tenant,
            &mut state,
            at(crate::a2a_source::CONTINUATION_IDLE_TTL_SECS + 1),
        )
        .await;

    let sent = sent_references(&server).await;
    assert_eq!(
        sent[1],
        (None, None),
        "a reference the remote has probably collected must not be sent"
    );
}

#[tokio::test]
async fn the_no_conversation_dispatch_never_resumes_anything() {
    // `A2aToolCatalog::dispatch` is the one-shot door the agent-graph node
    // uses. It must stay stateless: a per-process memory there would leak one
    // caller's remote conversation into another's.
    let server = MockServer::start().await;
    mount_card(&server).await;
    mount_reply(
        &server,
        task_in_context("t-1", "ctx-1", "TASK_STATE_INPUT_REQUIRED", "Which city?"),
    )
    .await;

    let catalog: std::sync::Arc<A2aToolCatalog> =
        source_for(vec![("travel", server.uri())]).catalog().await;
    catalog.dispatch("travel", &json!("hi")).await;
    catalog.dispatch("travel", &json!("Barcelona")).await;

    let sent = sent_references(&server).await;
    assert_eq!(sent, vec![(None, None), (None, None)]);
}

/// The exact wire shape greentic-start answers a completed structured turn
/// with (worker-interop contract D13, `interop::a2a::rpc::artifacts_for`):
/// artifact 1 is the turn's prose under the name `reply`, and one
/// `application/json` artifact follows per structured output, named after the
/// producing node.
fn structured_task_reply() -> serde_json::Value {
    json!({"jsonrpc": "2.0", "id": 1, "result": {"task": {
        "id": "t-1",
        "contextId": "ctx-1",
        "status": {"state": "TASK_STATE_COMPLETED"},
        "artifacts": [
            {"artifactId": "a-1", "name": "reply",
             "parts": [{"text": "It is 21.3 degrees in Barcelona."}]},
            {"artifactId": "a-2", "name": "weather_lookup",
             "parts": [{"data": {"temp_c": 21.3, "city": "Barcelona"},
                        "mediaType": "application/json"}]}
        ]
    }}})
}

#[tokio::test]
async fn a_structured_answer_reaches_the_model_beside_the_prose() {
    // The half this client dropped until 2026-09-25: it read `text` parts
    // only, so a callee's structured result — the whole reason a program
    // calls an agent — never reached the model, with nothing red anywhere.
    let server = MockServer::start().await;
    mount_card(&server).await;
    mount_reply(&server, structured_task_reply()).await;

    let reply = source_for(vec![("weather", server.uri())])
        .call("weather", "Barcelona?")
        .await
        .expect("a completed task is a result");

    assert!(
        reply.contains("It is 21.3 degrees in Barcelona."),
        "the prose must still lead: {reply}"
    );
    assert!(
        reply.contains("\"temp_c\":21.3") && reply.contains("\"city\":\"Barcelona\""),
        "the structured output must reach the model: {reply}"
    );
}

#[tokio::test]
async fn an_adaptive_card_part_is_not_read_as_a_structured_answer() {
    // Contract D10: a card reaches an agent caller only when it asked for
    // one, and this client never asks. Matching the media type exactly is
    // what keeps a non-conformant server from putting card markup in front
    // of the model through the artifact door.
    let server = MockServer::start().await;
    mount_card(&server).await;
    mount_reply(
        &server,
        json!({"jsonrpc": "2.0", "id": 1, "result": {"task": {
            "id": "t-1",
            "status": {"state": "TASK_STATE_COMPLETED"},
            "artifacts": [
                {"parts": [{"text": "Pick one."}]},
                {"parts": [{"data": {"type": "AdaptiveCard"},
                            "mediaType": "application/vnd.microsoft.card.adaptive+json"}]}
            ]
        }}}),
    )
    .await;

    let reply = source_for(vec![("picker", server.uri())])
        .call("picker", "hi")
        .await
        .expect("the prose is still an answer");

    assert_eq!(reply, "Pick one.");
}

#[tokio::test]
async fn a_structured_only_task_is_answered_rather_than_reported_as_empty() {
    // greentic-start always leads with the prose, so this is a shape only
    // another implementation produces. A task carrying a result and no
    // sentence has still answered; reporting it as "no text artifact" would
    // tell the model the agent said nothing while holding what it said.
    let server = MockServer::start().await;
    mount_card(&server).await;
    mount_reply(
        &server,
        json!({"jsonrpc": "2.0", "id": 1, "result": {"task": {
            "id": "t-1",
            "status": {"state": "TASK_STATE_COMPLETED"},
            "artifacts": [{"parts": [{"data": {"ok": true},
                                      "mediaType": "application/json"}]}]
        }}}),
    )
    .await;

    let reply = source_for(vec![("probe", server.uri())])
        .call("probe", "hi")
        .await
        .expect("a structured result is an answer");

    assert_eq!(reply, "{\"ok\":true}");
}
