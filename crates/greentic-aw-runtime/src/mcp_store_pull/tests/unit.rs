//! No-network unit tests for mcp_store_pull (marker, signature, trusted_signers).

#![allow(clippy::unwrap_used, clippy::expect_used, unsafe_code)]

use super::super::*;
use super::{pubkey_env_value, sample_describe, sign_describe_like_store};
use ed25519_dalek::SigningKey;

#[test]
fn greentic_trust_dependency_links() {
    // Smoke: the cross-org git dependency resolves, builds, and links.
    assert!(greentic_trust::DidWeb::parse("did:web:example.com").is_ok());
}

/// The gtxpack marker is what makes a cache hit honour version/digest
/// upgrades: a cached wasm is only reused when its marker equals the
/// requested `component_digest`. Missing or mismatched => false => re-pull.
#[test]
fn gtxpack_marker_matches_is_case_insensitive_and_fails_safe() {
    let dir = tempfile::tempdir().unwrap();
    let marker = dir.path().join("router_echo.wasm.gtxpack");

    // Missing marker (e.g. a pre-upgrade cache without one) => re-pull.
    assert!(!gtxpack_marker_matches(&marker, "abc123"));

    // Present + equal (case-insensitive, whitespace-trimmed) => cache hit.
    std::fs::write(&marker, "  ABC123\n").unwrap();
    assert!(gtxpack_marker_matches(&marker, "abc123"));

    // Present but DIFFERENT (the digest was upgraded) => re-pull, so the new
    // version actually reaches the runner instead of serving stale wasm.
    assert!(!gtxpack_marker_matches(&marker, "def456"));
}

/// The marker path is the wasm path with `.gtxpack` appended, distinct from
/// the `.sha256` execution-digest sidecar.
#[test]
fn gtxpack_marker_path_appends_extension() {
    let wasm = std::path::Path::new("/cache/router_echo.wasm");
    assert_eq!(
        gtxpack_marker_path(wasm),
        std::path::Path::new("/cache/router_echo.wasm.gtxpack")
    );
    assert_ne!(gtxpack_marker_path(wasm), sidecar_path(wasm));
}

#[test]
#[serial_test::serial]
fn verify_describe_signature_round_trips_store_signing() {
    let signing = SigningKey::from_bytes(&[3u8; 32]);
    let describe = sample_describe();
    let signed_bytes = sign_describe_like_store(&describe, &signing);
    let signed: serde_json::Value = serde_json::from_slice(&signed_bytes).unwrap();
    let trusted = vec![signing.verifying_key()];
    verify_describe_signature(&signed, &trusted).expect("valid signature must verify");
}

/// Proves the verify genuinely uses JCS (RFC 8785), not plain
/// `serde_json::to_vec`: a describe carrying an integer-valued float
/// (`64.0`) serializes as `64.0` under serde_json but `64` under JCS, so the
/// two canonicalizers DIVERGE on this document. The store signs with JCS, so
/// the verify must reconstruct with JCS — the old serde_json reconstruction
/// would compute different bytes and fail. This guards against a silent
/// regression back to `serde_json::to_vec`.
#[test]
#[serial_test::serial]
fn verify_describe_signature_uses_jcs_canonicalization() {
    let describe = serde_json::json!({
        "apiVersion": "greentic.ai/v1",
        "kind": "ProviderExtension",
        "runtime": { "memoryLimitMB": 64.0 },
        "metadata": { "id": "router_echo", "version": "1.0.0", "summary": "x" }
    });
    // The two canonicalizers MUST differ on this value, else the test would
    // pass even with a serde_json reconstruction (defeating its purpose).
    assert_ne!(
        serde_json::to_vec(&describe).unwrap(),
        serde_jcs::to_vec(&describe).unwrap(),
        "expected serde_json and JCS to diverge on an integer-valued float"
    );
    let signing = SigningKey::from_bytes(&[9u8; 32]);
    let signed_bytes = sign_describe_like_store(&describe, &signing);
    let signed: serde_json::Value = serde_json::from_slice(&signed_bytes).unwrap();
    verify_describe_signature(&signed, &[signing.verifying_key()])
        .expect("store-signed (JCS) describe must verify under the JCS reconstruction");
}

#[test]
#[serial_test::serial]
fn verify_describe_signature_fails_closed_on_empty_allowlist() {
    let signing = SigningKey::from_bytes(&[4u8; 32]);
    let signed_bytes = sign_describe_like_store(&sample_describe(), &signing);
    let signed: serde_json::Value = serde_json::from_slice(&signed_bytes).unwrap();
    let err = verify_describe_signature(&signed, &[]).unwrap_err();
    assert!(matches!(err, StorePullError::Signature(_)), "got: {err:?}");
}

#[test]
#[serial_test::serial]
fn verify_describe_signature_rejects_untrusted_key() {
    let signer = SigningKey::from_bytes(&[5u8; 32]);
    let other = SigningKey::from_bytes(&[6u8; 32]);
    let signed_bytes = sign_describe_like_store(&sample_describe(), &signer);
    let signed: serde_json::Value = serde_json::from_slice(&signed_bytes).unwrap();
    let err = verify_describe_signature(&signed, &[other.verifying_key()]).unwrap_err();
    assert!(matches!(err, StorePullError::Signature(_)), "got: {err:?}");
}

#[test]
#[serial_test::serial]
fn verify_describe_signature_rejects_tampered_describe() {
    let signing = SigningKey::from_bytes(&[7u8; 32]);
    let signed_bytes = sign_describe_like_store(&sample_describe(), &signing);
    let mut signed: serde_json::Value = serde_json::from_slice(&signed_bytes).unwrap();
    // Mutate a signed field after the fact; the signature no longer matches.
    signed["metadata"]["summary"] = serde_json::json!("tampered");
    let err = verify_describe_signature(&signed, &[signing.verifying_key()]).unwrap_err();
    assert!(matches!(err, StorePullError::Signature(_)), "got: {err:?}");
}

#[test]
#[serial_test::serial]
fn trusted_signers_parses_and_rejects_garbage() {
    let signing = SigningKey::from_bytes(&[8u8; 32]);
    let good = pubkey_env_value(&signing);
    unsafe {
        std::env::set_var(
            TRUSTED_SIGNERS_ENV,
            format!("{good}, , rsa:AAAA, not-base64!!"),
        )
    };
    let keys = trusted_signers();
    unsafe { std::env::remove_var(TRUSTED_SIGNERS_ENV) };
    assert_eq!(keys.len(), 1, "only the one valid ed25519 entry survives");
    assert_eq!(keys[0].to_bytes(), signing.verifying_key().to_bytes());
}

#[test]
#[serial_test::serial]
fn trust_did_is_none_when_unset() {
    unsafe { std::env::remove_var(crate::mcp_store_pull::TRUST_DID_ENV) };
    assert_eq!(crate::mcp_store_pull::trust_did(), None);
}

#[test]
#[serial_test::serial]
fn trust_did_is_none_when_empty() {
    unsafe { std::env::set_var(crate::mcp_store_pull::TRUST_DID_ENV, "") };
    assert_eq!(crate::mcp_store_pull::trust_did(), None);
    unsafe { std::env::remove_var(crate::mcp_store_pull::TRUST_DID_ENV) };
}

#[test]
#[serial_test::serial]
fn trust_did_returns_the_configured_did() {
    unsafe {
        std::env::set_var(
            crate::mcp_store_pull::TRUST_DID_ENV,
            "did:web:trust.greentic.cloud",
        )
    };
    assert_eq!(
        crate::mcp_store_pull::trust_did(),
        Some("did:web:trust.greentic.cloud".to_string())
    );
    unsafe { std::env::remove_var(crate::mcp_store_pull::TRUST_DID_ENV) };
}

#[test]
#[serial_test::serial]
fn trust_did_trims_surrounding_whitespace() {
    // A file-sourced (e.g. mounted-secret) value can carry a trailing
    // newline; that must never reach `DidWeb::parse`.
    unsafe {
        std::env::set_var(
            crate::mcp_store_pull::TRUST_DID_ENV,
            "  did:web:trust.greentic.cloud\n",
        )
    };
    assert_eq!(
        crate::mcp_store_pull::trust_did(),
        Some("did:web:trust.greentic.cloud".to_string())
    );
    unsafe { std::env::remove_var(crate::mcp_store_pull::TRUST_DID_ENV) };
}

#[test]
fn map_trust_error_folds_into_signature() {
    let mapped = crate::mcp_store_pull::map_trust_error(greentic_trust::TrustError::CertMissing);
    match mapped {
        crate::mcp_store_pull::StorePullError::Signature(msg) => {
            assert!(
                msg.contains("trust verification"),
                "reason should be legible: {msg}"
            );
        }
        other => panic!("expected Signature, got {other:?}"),
    }
}
