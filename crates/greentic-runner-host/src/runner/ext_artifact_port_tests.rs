#![allow(clippy::unwrap_used, clippy::expect_used, unsafe_code)]

use super::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use greentic_ext_runtime::host_ports::{
    ArtifactPort, ArtifactPortError, ArtifactPutRequest, HostCallContext,
};
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::host::ExtArtifactPort;

fn ctx(tenant: &str) -> HostCallContext {
    HostCallContext {
        tenant: Some(tenant.into()),
        ..HostCallContext::default()
    }
}

fn request() -> ArtifactPutRequest {
    ArtifactPutRequest {
        bytes: vec![1, 2, 3],
        mime_type: "image/png".into(),
        name: "a.png".into(),
    }
}

fn port_for(server: &MockServer, token: &str) -> HttpArtifactPort {
    HttpArtifactPort::new(
        format!("{}/artifacts", server.uri()),
        token.into(),
        tokio::runtime::Handle::current(),
    )
    .unwrap()
}

/// `put` is synchronous (wasmtime host fns are wired on the sync linker), so
/// call it from a blocking thread the way the host does.
async fn put(port: HttpArtifactPort, tenant: &'static str) -> Result<String, ArtifactPortError> {
    tokio::task::spawn_blocking(move || port.put("greentic.media", &ctx(tenant), request()))
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_posts_the_contract_body_and_returns_the_id() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/artifacts/put"))
        .and(header("authorization", "Bearer gtm_secret"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "artifact://abc", "sha256": "aa", "size_bytes": 3,
            "kind": "image", "mime_type": "image/png"
        })))
        .expect(1)
        .mount(&server)
        .await;
    assert_eq!(
        put(port_for(&server, "gtm_secret"), "acme").await.unwrap(),
        "artifact://abc"
    );
    let received = server.received_requests().await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&received[0].body).unwrap();
    assert_eq!(body["name"], "a.png");
    assert_eq!(body["mime_type"], "image/png");
    assert_eq!(body["data_base64"], "AQID");
    assert!(body["derived_from"].is_null());
    assert!(
        body["conversation_id"].is_null(),
        "an extension call has no conversation"
    );
    assert!(
        body.get("tenant").is_none(),
        "the tenant comes from the token, never the body"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn door_statuses_map_onto_port_errors() {
    for (status, want) in [
        (415u16, "InvalidMediaType"),
        (422, "QuotaExceeded"),
        (401, "Unavailable"),
        (403, "Unavailable"),
        (413, "Unavailable"),
        (429, "Unavailable"),
        (503, "Unavailable"),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(status))
            .mount(&server)
            .await;
        let err = put(port_for(&server, "t"), "acme").await.unwrap_err();
        assert!(format!("{err:?}").starts_with(want), "{status} -> {err:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_200_with_an_unreadable_body_is_unavailable() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
        .mount(&server)
        .await;
    let err = put(port_for(&server, "t"), "acme").await.unwrap_err();
    assert!(matches!(err, ArtifactPortError::Unavailable(_)), "{err:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_redirect_is_never_followed() {
    let server = MockServer::start().await;
    let elsewhere = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(307)
                .insert_header("location", format!("{}/steal", elsewhere.uri()).as_str()),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&elsewhere)
        .await;
    let err = put(port_for(&server, "gtm_secret"), "acme")
        .await
        .unwrap_err();
    assert!(matches!(err, ArtifactPortError::Unavailable(_)), "{err:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_blank_tenant_is_refused_without_calling_the_door() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&server)
        .await;
    let err = put(port_for(&server, "t"), "  ").await.unwrap_err();
    assert!(matches!(err, ArtifactPortError::Unavailable(_)), "{err:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_token_never_appears_in_debug_or_in_an_error() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(503).set_body_string("gtm_topsecret echoed"))
        .mount(&server)
        .await;
    let port = port_for(&server, "gtm_topsecret");
    assert!(!format!("{port:?}").contains("gtm_topsecret"));
    let err = put(port, "acme").await.unwrap_err();
    assert!(!format!("{err:?}").contains("gtm_topsecret"));
    assert!(!err.to_string().contains("gtm_topsecret"));
}

#[test]
fn an_unusable_token_is_refused_at_construction() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    for bad in ["", "   ", "gtm\u{7}x"] {
        assert!(
            HttpArtifactPort::new(
                "https://admin.example/artifacts".into(),
                bad.into(),
                rt.handle().clone()
            )
            .is_err(),
            "{bad:?}"
        );
    }
}

#[test]
fn a_current_thread_runtime_is_refused_instead_of_panicking() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let port = HttpArtifactPort::new(
        "http://127.0.0.1:1/artifacts".into(),
        "t".into(),
        rt.handle().clone(),
    )
    .unwrap();
    // `block_in_place` panics on a current-thread runtime; the port must say so.
    let err = rt
        .block_on(async { port.put("x", &ctx("acme"), request()) })
        .unwrap_err();
    assert!(matches!(err, ArtifactPortError::Unavailable(_)), "{err:?}");
}

struct Noop;
impl ArtifactPort for Noop {}

fn env_probe(called: &AtomicUsize) -> impl FnOnce() -> Option<ExtArtifactPort> + '_ {
    move || {
        called.fetch_add(1, Ordering::SeqCst);
        Some(Arc::new(Noop) as ExtArtifactPort)
    }
}

#[test]
fn a_single_tenant_host_uses_its_port_else_the_env_port() {
    let called = AtomicUsize::new(0);
    let host: ExtArtifactPort = Arc::new(Noop);
    let chosen =
        crate::host::host_ext_artifact_port(Some(host.clone()), 1, true, env_probe(&called))
            .unwrap();
    assert!(Arc::ptr_eq(&chosen, &host));
    assert_eq!(called.load(Ordering::SeqCst), 0, "env not read");
    assert!(crate::host::host_ext_artifact_port(None, 1, true, env_probe(&called)).is_some());
    assert_eq!(called.load(Ordering::SeqCst), 1);
}

#[test]
fn a_multi_tenant_host_has_no_port_and_never_reads_the_env() {
    let called = AtomicUsize::new(0);
    let host: ExtArtifactPort = Arc::new(Noop);
    assert!(crate::host::host_ext_artifact_port(Some(host), 2, true, env_probe(&called)).is_none());
    assert!(crate::host::host_ext_artifact_port(None, 2, true, env_probe(&called)).is_none());
    assert_eq!(called.load(Ordering::SeqCst), 0);
}

#[test]
#[serial_test::serial]
fn env_port_needs_both_variables_and_a_runtime() {
    unsafe {
        std::env::remove_var("GREENTIC_ARTIFACT_ENDPOINT");
        std::env::remove_var("GREENTIC_ARTIFACT_TOKEN");
    }
    assert!(artifact_port_from_env().is_none());
    unsafe {
        std::env::set_var(
            "GREENTIC_ARTIFACT_ENDPOINT",
            "https://admin.example/api/v1/ingest/artifacts",
        )
    };
    assert!(
        artifact_port_from_env().is_none(),
        "endpoint alone is not enough"
    );
    unsafe { std::env::set_var("GREENTIC_ARTIFACT_TOKEN", "gtm_x") };
    // Outside a tokio runtime there is no handle to block on: no port, not a panic.
    assert!(artifact_port_from_env().is_none());
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    assert!(rt.block_on(async { artifact_port_from_env() }).is_some());
    unsafe {
        std::env::remove_var("GREENTIC_ARTIFACT_ENDPOINT");
        std::env::remove_var("GREENTIC_ARTIFACT_TOKEN");
    }
}

fn squash(src: &str) -> String {
    src.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// The port reaches `build_ext_runtime` on both host paths; below the host
/// nothing falls back to the env.
#[test]
fn the_port_is_threaded_from_both_host_paths_and_never_from_the_env_below_them() {
    let agent = squash(include_str!("agent_node.rs"));
    assert!(
        !agent.contains("artifact_port_from_env"),
        "below the host, nothing may fall back to the env port"
    );
    let runtime = squash(include_str!("../runtime.rs"));
    assert!(
        runtime.contains("options.ext_artifact_port,"),
        "load_revision_with"
    );
    assert!(!runtime.contains("artifact_port_from_env"));
    let mut calls = 0;
    for needle in [
        "build_agent_node_wiring_metered(",
        "build_agent_node_wiring_ephemeral_metered(",
    ] {
        for (start, _) in runtime.match_indices(needle) {
            let rest = &runtime[start..];
            let end = rest.find(".await").expect("call has an .await");
            assert!(
                rest[..end].contains("ext_artifact_port.clone(),"),
                "`{needle}` must receive the port: {}",
                &rest[..end]
            );
            calls += 1;
        }
    }
    assert_eq!(calls, 3);
    let host = squash(include_str!("../host.rs"));
    assert!(
        host.contains("self.ext_artifact_port(),"),
        "host prepare_runtime"
    );
    let watcher = squash(include_str!("../watcher.rs"));
    assert!(
        watcher.contains("ext_artifact_port.clone(),"),
        "pack reload"
    );
}

/// The port shares the reader's endpoint rule: https, or loopback http only,
/// and never userinfo. A hostile endpoint gets no request at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hostile_endpoint_gets_no_request_and_a_fixed_error() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&server)
        .await;
    let port_no = server.address().port();
    for endpoint in [
        format!("http://user:pw@127.0.0.1:{port_no}/artifacts"),
        "http://admin.example/artifacts".to_string(),
        "file:///tmp/artifacts".to_string(),
        "admin.example/artifacts".to_string(),
    ] {
        let port = HttpArtifactPort::new(
            endpoint.clone(),
            "gtm_secret".into(),
            tokio::runtime::Handle::current(),
        )
        .unwrap();
        match put(port, "acme").await {
            Err(ArtifactPortError::Unavailable(msg)) => {
                assert_eq!(msg, "artifact endpoint is not usable", "{endpoint}")
            }
            other => panic!("{endpoint}: {other:?}"),
        }
    }
}

/// What matters is the runtime the CALL runs on, not the stored one:
/// `block_in_place` panics on a current-thread runtime even when the stored
/// handle is multi-thread. The port refuses instead.
#[test]
fn a_call_from_a_current_thread_runtime_is_refused_even_with_a_multi_thread_handle() {
    let stored = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let port = HttpArtifactPort::new(
        "http://127.0.0.1:1/artifacts".into(),
        "t".into(),
        stored.handle().clone(),
    )
    .unwrap();
    let caller = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let err = caller
        .block_on(async { port.put("x", &ctx("acme"), request()) })
        .unwrap_err();
    match err {
        ArtifactPortError::Unavailable(msg) => {
            assert_eq!(msg, "artifact put needs a multi-thread runtime")
        }
        other => panic!("{other:?}"),
    }
}

/// The env fallback is OPT-IN: a single-tenant host that did not opt in gets
/// no env port, and a builder starts with the fallback off.
#[test]
fn the_env_port_fallback_is_off_unless_the_host_opts_in() {
    let called = AtomicUsize::new(0);
    assert!(crate::host::host_ext_artifact_port(None, 1, false, env_probe(&called)).is_none());
    assert_eq!(called.load(Ordering::SeqCst), 0, "env not read");
    assert!(!crate::host::HostBuilder::new().artifact_env_fallback_for_tests());
    assert!(
        crate::host::HostBuilder::new()
            .with_artifact_env_fallback(true)
            .artifact_env_fallback_for_tests()
    );
}
