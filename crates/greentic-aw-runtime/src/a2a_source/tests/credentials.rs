//! Credential tests: the token is resolved per call from the contract's §5
//! scopes, sent only on the `SendMessage` POST, and a missing one refuses the
//! call rather than sending it unauthenticated. A credential is sent only to
//! the host and port the admin configured, never to one the card names.

use serde_json::json;

use super::{
    CARD, RPC_PATH, TestSecrets, credentialed_source, header_of, mount_card, mount_raw_card,
    mount_reply, route,
};

const TENANT_DEFAULT_URI: &str = "secrets://default/acme/_/a2a/recipe";

fn ok_reply() -> serde_json::Value {
    json!({"jsonrpc": "2.0", "id": 1, "result": {"message": {
        "messageId": "m-2", "role": "ROLE_AGENT", "parts": [{"text": "ok"}]
    }}})
}

async fn posts(server: &wiremock::MockServer) -> Vec<wiremock::Request> {
    server
        .received_requests()
        .await
        .expect("request log")
        .into_iter()
        .filter(|r| r.method.as_str() == "POST")
        .collect()
}

#[tokio::test]
async fn a_credentialed_call_sends_bearer_on_send_message_and_nothing_on_the_card_fetch() {
    let server = wiremock::MockServer::start().await;
    mount_card(&server).await;
    mount_reply(&server, ok_reply()).await;
    let secrets = TestSecrets::with(&[(TENANT_DEFAULT_URI, "tok-1")]);
    let source = credentialed_source(
        vec![route("recipe", server.uri(), None, None, true)],
        secrets,
        None,
    );

    let catalog = source.catalog().await;
    let value = catalog.dispatch("recipe", &json!({"message": "hi"})).await;
    assert_eq!(value, json!({"reply": "ok"}));

    let requests = server.received_requests().await.expect("request log");
    let gets: Vec<_> = requests
        .iter()
        .filter(|r| r.method.as_str() == "GET")
        .collect();
    assert!(!gets.is_empty(), "the card must have been fetched");
    for get in gets {
        assert_eq!(
            header_of(get, "authorization"),
            None,
            "the agent card is public; the token must not ride the card fetch"
        );
    }
    let post = posts(&server).await;
    assert_eq!(post.len(), 1);
    assert_eq!(
        header_of(&post[0], "authorization").as_deref(),
        Some("Bearer tok-1")
    );
}

#[tokio::test]
async fn a_custom_header_name_carries_the_raw_token_and_no_authorization() {
    let server = wiremock::MockServer::start().await;
    mount_card(&server).await;
    mount_reply(&server, ok_reply()).await;
    let secrets = TestSecrets::with(&[(TENANT_DEFAULT_URI, "tok-1\n")]);
    let source = credentialed_source(
        vec![route("recipe", server.uri(), Some("X-Api-Key"), None, true)],
        secrets,
        None,
    );

    source
        .call("recipe", "hi")
        .await
        .expect("call must succeed");

    let post = posts(&server).await;
    assert_eq!(
        header_of(&post[0], "x-api-key").as_deref(),
        Some("tok-1"),
        "a custom header sends the raw token, trailing whitespace trimmed"
    );
    assert_eq!(header_of(&post[0], "authorization"), None);
}

#[tokio::test]
async fn an_explicit_authorization_header_name_still_gets_the_bearer_prefix() {
    // Same rule as greentic-mcp-client's McpAuth::header, so one admin value
    // means one thing for MCP and A2A.
    let server = wiremock::MockServer::start().await;
    mount_card(&server).await;
    mount_reply(&server, ok_reply()).await;
    let secrets = TestSecrets::with(&[(TENANT_DEFAULT_URI, "tok-1")]);
    let source = credentialed_source(
        vec![route(
            "recipe",
            server.uri(),
            Some("Authorization"),
            None,
            true,
        )],
        secrets,
        None,
    );

    source
        .call("recipe", "hi")
        .await
        .expect("call must succeed");

    let post = posts(&server).await;
    assert_eq!(
        header_of(&post[0], "authorization").as_deref(),
        Some("Bearer tok-1")
    );
}

#[tokio::test]
async fn the_team_scope_is_read_before_the_tenant_default() {
    let server = wiremock::MockServer::start().await;
    mount_card(&server).await;
    mount_reply(&server, ok_reply()).await;
    let secrets = TestSecrets::with(&[
        ("secrets://default/acme/sales/a2a/recipe", "team-token"),
        (TENANT_DEFAULT_URI, "tenant-token"),
    ]);
    let source = credentialed_source(
        vec![route("recipe", server.uri(), None, Some("sales"), true)],
        secrets,
        None,
    );

    source
        .call("recipe", "hi")
        .await
        .expect("call must succeed");

    let post = posts(&server).await;
    assert_eq!(
        header_of(&post[0], "authorization").as_deref(),
        Some("Bearer team-token")
    );
}

#[tokio::test]
async fn the_unit_scope_wins_when_running_as_a_unit() {
    let server = wiremock::MockServer::start().await;
    mount_card(&server).await;
    mount_reply(&server, ok_reply()).await;
    let unit_key = crate::mcp_secrets::mcp_unit_secret_key("recipe", "worker-a").unwrap();
    let unit_uri = format!("secrets://default/acme/_/a2a/{unit_key}");
    let secrets = TestSecrets::with(&[
        (unit_uri.as_str(), "unit-token"),
        (TENANT_DEFAULT_URI, "tenant-token"),
    ]);
    let source = credentialed_source(
        vec![route("recipe", server.uri(), None, None, true)],
        secrets,
        Some("worker-a"),
    );

    source
        .call("recipe", "hi")
        .await
        .expect("call must succeed");

    let post = posts(&server).await;
    assert_eq!(
        header_of(&post[0], "authorization").as_deref(),
        Some("Bearer unit-token")
    );
}

#[tokio::test]
async fn a_missing_credential_names_the_agent_and_every_scope_and_sends_nothing() {
    let server = wiremock::MockServer::start().await;
    mount_card(&server).await;
    mount_reply(&server, ok_reply()).await;
    let source = credentialed_source(
        vec![route("recipe", server.uri(), None, Some("sales"), true)],
        TestSecrets::with(&[]),
        None,
    );

    let err = source
        .call("recipe", "hi")
        .await
        .expect_err("a credentialed route with no credential must not be called");

    assert!(
        err.starts_with("a2a agent recipe: no credential at "),
        "got: {err}"
    );
    assert!(
        err.contains("secrets://default/acme/sales/a2a/recipe") && err.contains(TENANT_DEFAULT_URI),
        "every scope looked in must be named, got: {err}"
    );
    // The public card may be fetched: the same-host check needs the interface
    // URL before the secrets store is read. What must never happen is the
    // SendMessage POST going out without the credential.
    assert!(
        posts(&server).await.is_empty(),
        "no unauthenticated SendMessage"
    );
}

#[tokio::test]
async fn a_missing_credential_is_an_error_value_through_dispatch_with_no_post() {
    let server = wiremock::MockServer::start().await;
    mount_card(&server).await;
    mount_reply(&server, ok_reply()).await;
    let source = credentialed_source(
        vec![route("recipe", server.uri(), None, None, true)],
        TestSecrets::with(&[]),
        None,
    );

    let value = source
        .catalog()
        .await
        .dispatch("recipe", &json!({"message": "hi"}))
        .await;

    let error = value["error"].as_str().expect("an error value");
    assert!(error.contains("no credential at"), "got: {error}");
    assert!(posts(&server).await.is_empty(), "no unauthenticated call");
}

#[tokio::test]
async fn the_token_is_read_per_call_so_rotation_takes_effect_without_a_restart() {
    let server = wiremock::MockServer::start().await;
    mount_card(&server).await;
    mount_reply(&server, ok_reply()).await;
    let secrets = TestSecrets::with(&[(TENANT_DEFAULT_URI, "tok-1")]);
    let source = credentialed_source(
        vec![route("recipe", server.uri(), None, None, true)],
        secrets.clone(),
        None,
    );

    source.call("recipe", "hi").await.expect("first call");
    secrets.set(TENANT_DEFAULT_URI, "tok-2");
    source.call("recipe", "hi").await.expect("second call");

    let sent: Vec<Option<String>> = posts(&server)
        .await
        .iter()
        .map(|r| header_of(r, "authorization"))
        .collect();
    assert_eq!(
        sent,
        vec![
            Some("Bearer tok-1".to_string()),
            Some("Bearer tok-2".to_string())
        ]
    );
}

#[tokio::test]
async fn a_route_without_a_credential_sends_no_auth_even_when_one_is_stored() {
    let server = wiremock::MockServer::start().await;
    mount_card(&server).await;
    mount_reply(&server, ok_reply()).await;
    let source = credentialed_source(
        vec![route("recipe", server.uri(), None, None, false)],
        TestSecrets::with(&[(TENANT_DEFAULT_URI, "tok-1")]),
        None,
    );

    source
        .call("recipe", "hi")
        .await
        .expect("call must succeed");

    assert_eq!(header_of(&posts(&server).await[0], "authorization"), None);
}

#[tokio::test]
async fn no_secrets_manager_for_a_credentialed_route_refuses_the_call() {
    let server = wiremock::MockServer::start().await;
    mount_card(&server).await;
    mount_reply(&server, ok_reply()).await;
    let source = crate::a2a_source::A2aToolSource::from_routes(
        vec![route("recipe", server.uri(), None, None, true)],
        None,
        "acme",
        None,
    )
    .expect("buildable");

    let err = source.call("recipe", "hi").await.expect_err("must refuse");

    assert!(
        err.contains("recipe") && err.contains("no secrets manager"),
        "got: {err}"
    );
    assert!(posts(&server).await.is_empty());
}

#[tokio::test]
async fn a_credential_that_cannot_be_a_header_value_is_refused_without_echoing_it() {
    let server = wiremock::MockServer::start().await;
    mount_card(&server).await;
    mount_reply(&server, ok_reply()).await;
    let source = credentialed_source(
        vec![route("recipe", server.uri(), None, None, true)],
        TestSecrets::with(&[(TENANT_DEFAULT_URI, "sk-live\u{1}rest")]),
        None,
    );

    let err = source.call("recipe", "hi").await.expect_err("must refuse");

    assert!(
        !err.contains("sk-live"),
        "the token must never be echoed: {err}"
    );
    assert!(err.contains("recipe"), "got: {err}");
    assert!(posts(&server).await.is_empty());
}

#[tokio::test]
async fn a_credential_is_never_sent_to_a_host_the_card_names_but_the_admin_did_not() {
    // The card is served from the configured (loopback) base but names a
    // remote https interface. `require_secure_interface` accepts that, so the
    // same-host rule is the only thing keeping the token off `other.example`.
    let server = wiremock::MockServer::start().await;
    mount_raw_card(
        &server,
        CARD.replace("PLACEHOLDER", "https://other.example/a2a"),
    )
    .await;
    mount_reply(&server, ok_reply()).await;
    let secrets = TestSecrets::with(&[(TENANT_DEFAULT_URI, "tok-1")]);
    let source = credentialed_source(
        vec![route("recipe", server.uri(), None, None, true)],
        secrets.clone(),
        None,
    );

    let err = source
        .call("recipe", "hi")
        .await
        .expect_err("a credential must not travel to a host the admin did not configure");

    assert!(
        err.starts_with("a2a agent recipe: ")
            && err.contains("other.example")
            && err.contains("127.0.0.1"),
        "the error must name both hosts, got: {err}"
    );
    assert!(
        !err.contains("tok-1"),
        "the token must never be echoed: {err}"
    );
    assert!(posts(&server).await.is_empty(), "nothing is sent");
    assert_eq!(
        secrets.reads(),
        0,
        "a host mismatch is decided before the secrets store is touched"
    );
}

#[tokio::test]
async fn a_credential_is_sent_when_the_interface_shares_the_base_host() {
    let server = wiremock::MockServer::start().await;
    mount_card(&server).await;
    mount_reply(&server, ok_reply()).await;
    let secrets = TestSecrets::with(&[(TENANT_DEFAULT_URI, "tok-1")]);
    let source = credentialed_source(
        vec![route("recipe", server.uri(), None, None, true)],
        secrets,
        None,
    );

    source
        .call("recipe", "hi")
        .await
        .expect("call must succeed");

    assert_eq!(
        header_of(&posts(&server).await[0], "authorization").as_deref(),
        Some("Bearer tok-1")
    );
}

#[tokio::test]
async fn a_credential_is_never_sent_to_another_port_on_the_same_host() {
    let server = wiremock::MockServer::start().await;
    let elsewhere = wiremock::MockServer::start().await;
    mount_raw_card(
        &server,
        CARD.replace("PLACEHOLDER", &format!("{}{RPC_PATH}", elsewhere.uri())),
    )
    .await;
    mount_reply(&elsewhere, ok_reply()).await;
    let secrets = TestSecrets::with(&[(TENANT_DEFAULT_URI, "tok-1")]);
    let source = credentialed_source(
        vec![route("recipe", server.uri(), None, None, true)],
        secrets.clone(),
        None,
    );

    let err = source.call("recipe", "hi").await.expect_err("must refuse");

    assert!(err.contains("differs from configured host"), "got: {err}");
    assert!(posts(&server).await.is_empty());
    assert!(
        posts(&elsewhere).await.is_empty(),
        "nothing reaches the other port"
    );
    assert_eq!(secrets.reads(), 0);
}

#[tokio::test]
async fn a_route_without_a_credential_may_call_an_interface_on_another_host() {
    // Nothing to leak, so the same-host rule does not apply.
    let server = wiremock::MockServer::start().await;
    let elsewhere = wiremock::MockServer::start().await;
    mount_raw_card(
        &server,
        CARD.replace("PLACEHOLDER", &format!("{}{RPC_PATH}", elsewhere.uri())),
    )
    .await;
    mount_reply(&elsewhere, ok_reply()).await;
    let source = credentialed_source(
        vec![route("recipe", server.uri(), None, None, false)],
        TestSecrets::with(&[]),
        None,
    );

    source
        .call("recipe", "hi")
        .await
        .expect("call must succeed");

    assert_eq!(posts(&elsewhere).await.len(), 1);
}

#[tokio::test]
async fn a_token_carrying_crlf_is_refused_rather_than_smuggled_into_headers() {
    let server = wiremock::MockServer::start().await;
    mount_card(&server).await;
    mount_reply(&server, ok_reply()).await;
    let source = credentialed_source(
        vec![route("recipe", server.uri(), None, None, true)],
        TestSecrets::with(&[(TENANT_DEFAULT_URI, "tok\r\nX-Injected: yes")]),
        None,
    );

    let err = source.call("recipe", "hi").await.expect_err("must refuse");

    assert!(!err.contains("X-Injected"), "never echoed: {err}");
    assert!(posts(&server).await.is_empty(), "nothing is sent");
}

#[tokio::test]
async fn an_interface_url_with_userinfo_is_refused_before_anything_is_sent() {
    // reqwest turns userinfo into its own `Authorization: Basic`, so the POST
    // would carry two competing credentials.
    let server = wiremock::MockServer::start().await;
    let with_userinfo = server.uri().replace("http://", "http://user:pass@");
    mount_raw_card(
        &server,
        CARD.replace("PLACEHOLDER", &format!("{with_userinfo}{RPC_PATH}")),
    )
    .await;
    mount_reply(&server, ok_reply()).await;
    let secrets = TestSecrets::with(&[(TENANT_DEFAULT_URI, "tok-1")]);
    let source = credentialed_source(
        vec![route("recipe", server.uri(), None, None, true)],
        secrets.clone(),
        None,
    );

    let err = source.call("recipe", "hi").await.expect_err("must refuse");

    assert!(err.contains("userinfo"), "got: {err}");
    assert!(posts(&server).await.is_empty(), "nothing is sent");
    assert_eq!(secrets.reads(), 0, "refused before the store is read");
}

#[tokio::test]
async fn a_header_name_that_frames_the_request_cannot_carry_the_token() {
    let server = wiremock::MockServer::start().await;
    mount_card(&server).await;
    mount_reply(&server, ok_reply()).await;
    for name in ["Host", "content-length", "Transfer-Encoding"] {
        let source = credentialed_source(
            vec![route("recipe", server.uri(), Some(name), None, true)],
            TestSecrets::with(&[(TENANT_DEFAULT_URI, "tok-1")]),
            None,
        );

        let err = source.call("recipe", "hi").await.expect_err(name);

        assert!(!err.contains("tok-1"), "never echoed: {err}");
    }
    assert!(posts(&server).await.is_empty(), "nothing is sent");
}

#[tokio::test]
async fn an_agent_id_that_could_leave_its_secret_key_is_never_looked_up() {
    let server = wiremock::MockServer::start().await;
    mount_card(&server).await;
    mount_reply(&server, ok_reply()).await;
    let secrets = TestSecrets::with(&[(TENANT_DEFAULT_URI, "tok-1")]);
    let source = credentialed_source(
        vec![route("../recipe", server.uri(), None, None, true)],
        secrets.clone(),
        None,
    );

    let err = source
        .call("../recipe", "hi")
        .await
        .expect_err("must refuse");

    assert!(err.contains("not a safe secret name"), "got: {err}");
    assert_eq!(secrets.reads(), 0, "never looked up");
    assert!(posts(&server).await.is_empty(), "nothing is sent");
}

#[tokio::test]
async fn an_auth_team_that_could_leave_its_secret_path_is_never_looked_up() {
    // `auth_team` becomes the team segment of the same URI as the agent id and
    // comes from the same sidecar, so it gets the same rule.
    let server = wiremock::MockServer::start().await;
    mount_card(&server).await;
    mount_reply(&server, ok_reply()).await;
    let secrets = TestSecrets::with(&[(TENANT_DEFAULT_URI, "tok-1")]);
    for bad in ["../other-tenant/x", "a/b", ".."] {
        let source = credentialed_source(
            vec![route("recipe", server.uri(), None, Some(bad), true)],
            secrets.clone(),
            None,
        );

        let err = source.call("recipe", "hi").await.expect_err(bad);

        assert!(err.contains("auth_team"), "{bad}: {err}");
    }
    assert_eq!(secrets.reads(), 0, "never looked up");
    assert!(posts(&server).await.is_empty(), "nothing is sent");
}
