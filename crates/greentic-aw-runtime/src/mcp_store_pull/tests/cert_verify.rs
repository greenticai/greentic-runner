//! Integration tests for the S4 gated certificate verification: mint a cert,
//! serve the DID document, and verify through the runner's `verify_certified`
//! seam — mint, embed, and verify all through `greentic_trust`, so a
//! wire-format drift turns this red, not production.

#![allow(clippy::unwrap_used, clippy::expect_used, unsafe_code)]

use std::time::Duration;

use ed25519_dalek::SigningKey;
use greentic_trust::{DidWeb, HttpResolver};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::{sample_describe, sign_describe_like_store};
use crate::mcp_store_pull::{StorePullError, verify_certified};

/// A far-future expiry so the runner's real `Utc::now()` is always before it.
const NOT_AFTER: &str = "2999-01-01T00:00:00Z";

/// did:web pointing at the mock server (plain HTTP, so the resolver must
/// allow it explicitly). Mirrors greentic-trust's own resolver-test `did_for`.
fn did_for(server: &MockServer) -> String {
    let authority = server
        .uri()
        .trim_start_matches("http://")
        .replace(':', "%3A");
    format!("did:web:{authority}")
}

fn allow_http_resolver() -> HttpResolver {
    HttpResolver::new(Duration::from_secs(300), 16)
        .expect("client builds")
        .allow_http()
        .expect("client builds")
}

/// Build a describe signed by `publisher`, then embed `certificate` into its
/// signature block (the order store-server uses at publish: sign over
/// describe-minus-signature, then attach the cert).
fn signed_describe_with_cert(
    publisher: &SigningKey,
    cert: Option<&greentic_trust::PublisherCert>,
) -> serde_json::Value {
    let signed_bytes = sign_describe_like_store(&sample_describe(), publisher);
    let mut describe: serde_json::Value = serde_json::from_slice(&signed_bytes).unwrap();
    if let Some(cert) = cert {
        describe["signature"]["certificate"] = serde_json::to_value(cert).unwrap();
    }
    describe
}

async fn mount_did_document(server: &MockServer, did: &str, root: &SigningKey) {
    let doc = greentic_trust::ceremony::build_document(
        &DidWeb::parse(did).expect("did parses"),
        &[root.verifying_key()],
    )
    .expect("document builds");
    Mock::given(method("GET"))
        .and(path("/.well-known/did.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(doc))
        .mount(server)
        .await;
}

#[tokio::test]
async fn certified_describe_verifies_through_verify_certified() {
    // The star: cert minted (S3a), embedded (S3b-style), verified (S4) — all
    // through greentic-trust, end to end within the runner's boundary.
    let server = MockServer::start().await;
    let did = did_for(&server);
    let root = SigningKey::from_bytes(&[7u8; 32]);
    let publisher = SigningKey::from_bytes(&[9u8; 32]);
    mount_did_document(&server, &did, &root).await;

    let cert = greentic_trust::ceremony::mint_cert(
        &root,
        &publisher.verifying_key(),
        "store-key-1",
        NOT_AFTER,
    )
    .expect("cert mints");
    let describe = signed_describe_with_cert(&publisher, Some(&cert));

    verify_certified(&describe, &did, &allow_http_resolver())
        .await
        .expect("a root-vouched cert must verify");
}

#[tokio::test]
async fn certless_describe_is_rejected_when_did_set() {
    let server = MockServer::start().await;
    let did = did_for(&server);
    let root = SigningKey::from_bytes(&[7u8; 32]);
    let publisher = SigningKey::from_bytes(&[9u8; 32]);
    mount_did_document(&server, &did, &root).await;

    let describe = signed_describe_with_cert(&publisher, None);

    let err = verify_certified(&describe, &did, &allow_http_resolver())
        .await
        .expect_err("a describe with no certificate must be rejected");
    assert!(matches!(err, StorePullError::Signature(_)), "got {err:?}");
}

#[tokio::test]
async fn foreign_root_cert_is_rejected() {
    let server = MockServer::start().await;
    let did = did_for(&server);
    let served_root = SigningKey::from_bytes(&[7u8; 32]);
    let foreign_root = SigningKey::from_bytes(&[8u8; 32]);
    let publisher = SigningKey::from_bytes(&[9u8; 32]);
    // The DID publishes served_root, but the cert is signed by foreign_root.
    mount_did_document(&server, &did, &served_root).await;

    let cert = greentic_trust::ceremony::mint_cert(
        &foreign_root,
        &publisher.verifying_key(),
        "store-key-1",
        NOT_AFTER,
    )
    .expect("cert mints");
    let describe = signed_describe_with_cert(&publisher, Some(&cert));

    let err = verify_certified(&describe, &did, &allow_http_resolver())
        .await
        .expect_err("a cert not chaining to the published root must be rejected");
    assert!(matches!(err, StorePullError::Signature(_)), "got {err:?}");
}

#[tokio::test]
#[serial_test::serial]
async fn did_unset_uses_the_legacy_allowlist() {
    use crate::mcp_store_pull::{TRUST_DID_ENV, verify_authenticity_for_test};

    unsafe { std::env::remove_var(TRUST_DID_ENV) };
    let publisher = SigningKey::from_bytes(&[9u8; 32]);
    // A describe with NO certificate, signed by `publisher`; the legacy path
    // must accept it purely on the allowlist.
    let describe = signed_describe_with_cert(&publisher, None);
    unsafe {
        std::env::set_var(
            "GREENTIC_MCP_TRUSTED_SIGNERS",
            super::pubkey_env_value(&publisher),
        )
    };

    verify_authenticity_for_test(&describe)
        .await
        .expect("legacy allowlist accepts the cert-less describe when no DID is set");

    unsafe { std::env::remove_var("GREENTIC_MCP_TRUSTED_SIGNERS") };
}

#[tokio::test]
#[serial_test::serial]
async fn did_set_routes_through_the_cert_path() {
    use crate::mcp_store_pull::{TRUST_DID_ENV, verify_authenticity_for_test};

    // Locks the gate direction: with the DID set, `verify_authenticity` must
    // route to the cert path, NOT the legacy allowlist. The did:web is an
    // unreachable `.invalid` host (RFC 6761 — never resolves, so this is
    // offline-safe and fails fast), so verification fails at resolution and is
    // mapped by `map_trust_error`. That mapping's prefix is what discriminates:
    // only the cert path yields "did:web trust verification failed"; the legacy
    // path (were the gate inverted) would report "no trusted signers".
    unsafe { std::env::remove_var("GREENTIC_MCP_TRUSTED_SIGNERS") };
    unsafe { std::env::set_var(TRUST_DID_ENV, "did:web:did-set-routing.invalid") };

    let publisher = SigningKey::from_bytes(&[9u8; 32]);
    let describe = signed_describe_with_cert(&publisher, None);

    let err = verify_authenticity_for_test(&describe)
        .await
        .expect_err("an unreachable trusted DID must fail closed via the cert path");
    unsafe { std::env::remove_var(TRUST_DID_ENV) };

    match err {
        StorePullError::Signature(msg) => assert!(
            msg.contains("did:web trust verification failed"),
            "expected the cert-path error mapping, got: {msg}"
        ),
        other => panic!("expected Signature from the cert path, got {other:?}"),
    }
}
