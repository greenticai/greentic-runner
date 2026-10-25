//! Integration tests for ensure_cached: require the router_echo fixture wasm
//! and/or a wiremock store. Tests that do not need the real wasm still use
//! minimal fake bytes for the archive entry.

#![allow(clippy::unwrap_used, clippy::expect_used, unsafe_code)]

use super::super::*;
use super::{
    build_gtxpack, fixture_wasm, mock_store, pubkey_env_value, sample_describe,
    sign_describe_like_store,
};
use ed25519_dalek::SigningKey;

// ---- integration tests (require the router_echo fixture wasm) ----

#[tokio::test]
#[serial_test::serial]
async fn ensure_cached_verifies_and_caches_happy_path() {
    let Some(src) = fixture_wasm() else {
        return; // self-skip without the fixture wasm
    };
    let wasm = std::fs::read(&src).unwrap();
    let signing = SigningKey::from_bytes(&[11u8; 32]);
    let signed_describe = sign_describe_like_store(&sample_describe(), &signing);
    let archive = build_gtxpack(&signed_describe, &wasm);
    let digest = hex_sha256(&archive);
    let server = mock_store(archive).await;

    let cache = tempfile::tempdir().unwrap();
    unsafe {
        std::env::set_var("GREENTIC_MCP_LOCAL_CACHE_DIR", cache.path());
        std::env::set_var(STORE_URL_ENV, server.uri());
        std::env::set_var(TRUSTED_SIGNERS_ENV, pubkey_env_value(&signing));
    }

    let result = ensure_cached("router_echo", "1.0.0", &digest).await;

    let dest = cache.path().join("router_echo.wasm");
    let cached_ok = dest.exists();
    let cached_bytes = if cached_ok {
        std::fs::read(&dest).ok()
    } else {
        None
    };
    unsafe {
        std::env::remove_var("GREENTIC_MCP_LOCAL_CACHE_DIR");
        std::env::remove_var(STORE_URL_ENV);
        std::env::remove_var(TRUSTED_SIGNERS_ENV);
    }

    result.expect("happy path must verify + cache");
    assert!(cached_ok, "extension.wasm must be written to the cache");
    assert_eq!(cached_bytes.as_deref(), Some(wasm.as_slice()));
}

#[tokio::test]
#[serial_test::serial]
async fn ensure_cached_wrong_digest_errors_and_caches_nothing() {
    let Some(src) = fixture_wasm() else {
        return;
    };
    let wasm = std::fs::read(&src).unwrap();
    let signing = SigningKey::from_bytes(&[12u8; 32]);
    let signed_describe = sign_describe_like_store(&sample_describe(), &signing);
    let archive = build_gtxpack(&signed_describe, &wasm);
    let server = mock_store(archive).await;

    let cache = tempfile::tempdir().unwrap();
    unsafe {
        std::env::set_var("GREENTIC_MCP_LOCAL_CACHE_DIR", cache.path());
        std::env::set_var(STORE_URL_ENV, server.uri());
        std::env::set_var(TRUSTED_SIGNERS_ENV, pubkey_env_value(&signing));
    }

    let wrong_digest = "0".repeat(64);
    let result = ensure_cached("router_echo", "1.0.0", &wrong_digest).await;

    let dest = cache.path().join("router_echo.wasm");
    let leaked = dest.exists();
    unsafe {
        std::env::remove_var("GREENTIC_MCP_LOCAL_CACHE_DIR");
        std::env::remove_var(STORE_URL_ENV);
        std::env::remove_var(TRUSTED_SIGNERS_ENV);
    }

    assert!(
        matches!(result, Err(StorePullError::Integrity(_))),
        "got: {result:?}"
    );
    assert!(!leaked, "nothing must be cached on a digest mismatch");
}

#[tokio::test]
#[serial_test::serial]
async fn ensure_cached_untrusted_signature_errors_and_caches_nothing() {
    let Some(src) = fixture_wasm() else {
        return;
    };
    let wasm = std::fs::read(&src).unwrap();
    let signer = SigningKey::from_bytes(&[13u8; 32]);
    let untrusted = SigningKey::from_bytes(&[14u8; 32]);
    let signed_describe = sign_describe_like_store(&sample_describe(), &signer);
    let archive = build_gtxpack(&signed_describe, &wasm);
    let digest = hex_sha256(&archive);
    let server = mock_store(archive).await;

    let cache = tempfile::tempdir().unwrap();
    unsafe {
        std::env::set_var("GREENTIC_MCP_LOCAL_CACHE_DIR", cache.path());
        std::env::set_var(STORE_URL_ENV, server.uri());
        // Trust a DIFFERENT key than the one that signed the describe.
        std::env::set_var(TRUSTED_SIGNERS_ENV, pubkey_env_value(&untrusted));
    }

    let result = ensure_cached("router_echo", "1.0.0", &digest).await;

    let dest = cache.path().join("router_echo.wasm");
    let leaked = dest.exists();
    unsafe {
        std::env::remove_var("GREENTIC_MCP_LOCAL_CACHE_DIR");
        std::env::remove_var(STORE_URL_ENV);
        std::env::remove_var(TRUSTED_SIGNERS_ENV);
    }

    assert!(
        matches!(result, Err(StorePullError::Signature(_))),
        "got: {result:?}"
    );
    assert!(!leaked, "an untrusted signature must cache nothing");
}

// ---- Fix 1 regression tests (fixture-independent) ----

/// Pre-seed a sidecar-less `<ref>.wasm` and confirm that `ensure_cached`
/// performs a full re-pull + writes the sidecar, proving the fail-open gap is
/// closed: a wasm without its companion `.sha256` is not treated as verified.
#[tokio::test]
#[serial_test::serial]
async fn wasm_without_sidecar_triggers_repull() {
    let signing = SigningKey::from_bytes(&[42u8; 32]);
    // Use minimal fake wasm bytes — ensure_cached never executes them.
    let fake_wasm = b"fake-wasm-bytes-for-sidecar-test";
    let signed_describe = sign_describe_like_store(&sample_describe(), &signing);
    let archive = build_gtxpack(&signed_describe, fake_wasm);
    let digest = hex_sha256(&archive);

    let cache = tempfile::tempdir().unwrap();
    // Pre-seed the wasm WITHOUT its sidecar — simulates operator-seeded or
    // stale entry from a previous ref that never completed verification.
    let dest = cache.path().join("router_echo.wasm");
    std::fs::write(&dest, b"stale-unverified-bytes").unwrap();
    assert!(dest.exists(), "pre-condition: wasm exists");
    assert!(
        !sidecar_path(&dest).exists(),
        "pre-condition: sidecar absent"
    );

    let server = mock_store(archive).await;
    unsafe {
        std::env::set_var("GREENTIC_MCP_LOCAL_CACHE_DIR", cache.path());
        std::env::set_var(STORE_URL_ENV, server.uri());
        std::env::set_var(TRUSTED_SIGNERS_ENV, pubkey_env_value(&signing));
    }

    let result = ensure_cached("router_echo", "1.0.0", &digest).await;

    let sidecar_exists = sidecar_path(&dest).exists();
    unsafe {
        std::env::remove_var("GREENTIC_MCP_LOCAL_CACHE_DIR");
        std::env::remove_var(STORE_URL_ENV);
        std::env::remove_var(TRUSTED_SIGNERS_ENV);
    }

    result.expect("re-pull of sidecar-less wasm must succeed");
    assert!(
        sidecar_exists,
        "sidecar must be written after verified re-pull"
    );
}

/// When the wasm, its sidecar, AND a gtxpack marker MATCHING the requested
/// digest all exist, `ensure_cached` short-circuits without hitting the
/// network. No mock server is set up, so any network attempt would error,
/// proving the short-circuit actually happened.
#[tokio::test]
#[serial_test::serial]
async fn cache_with_matching_marker_short_circuits() {
    let cache = tempfile::tempdir().unwrap();
    let dest = cache.path().join("router_echo.wasm");
    // Seed wasm + sidecar + a marker matching the digest we will request.
    std::fs::write(&dest, b"cached-wasm").unwrap();
    std::fs::write(sidecar_path(&dest), b"somedigest").unwrap();
    std::fs::write(gtxpack_marker_path(&dest), b"pinneddigest").unwrap();

    unsafe {
        std::env::set_var("GREENTIC_MCP_LOCAL_CACHE_DIR", cache.path());
        // Bogus store URL — any network attempt would fail.
        std::env::set_var(STORE_URL_ENV, "http://127.0.0.1:1");
        std::env::remove_var(TRUSTED_SIGNERS_ENV);
    }

    let result = ensure_cached("router_echo", "1.0.0", "pinneddigest").await;

    unsafe {
        std::env::remove_var("GREENTIC_MCP_LOCAL_CACHE_DIR");
        std::env::remove_var(STORE_URL_ENV);
    }

    result.expect("wasm + sidecar + matching marker must short-circuit with Ok");
}

/// Regression for the cache-staleness bug: when the cached wasm's marker
/// records a DIFFERENT (older) gtxpack digest than the one now requested —
/// i.e. the component was upgraded/re-registered — `ensure_cached` must NOT
/// short-circuit on the stale wasm but re-pull and replace it, so the new
/// (e.g. security-patched) version actually reaches the runner.
#[tokio::test]
#[serial_test::serial]
async fn stale_marker_triggers_repull_on_upgrade() {
    let signing = SigningKey::from_bytes(&[7u8; 32]);
    let new_wasm = b"new-verified-wasm-bytes";
    let signed_describe = sign_describe_like_store(&sample_describe(), &signing);
    let archive = build_gtxpack(&signed_describe, new_wasm);
    let new_digest = hex_sha256(&archive);

    let cache = tempfile::tempdir().unwrap();
    let dest = cache.path().join("router_echo.wasm");
    // Pre-seed a COMPLETE older cache entry: wasm + sidecar + a marker that
    // records the OLD digest (different from `new_digest`).
    std::fs::write(&dest, b"old-cached-wasm").unwrap();
    std::fs::write(sidecar_path(&dest), hex_sha256(b"old-cached-wasm")).unwrap();
    std::fs::write(gtxpack_marker_path(&dest), b"0000oldolddigest").unwrap();

    let server = mock_store(archive).await;
    unsafe {
        std::env::set_var("GREENTIC_MCP_LOCAL_CACHE_DIR", cache.path());
        std::env::set_var(STORE_URL_ENV, server.uri());
        std::env::set_var(TRUSTED_SIGNERS_ENV, pubkey_env_value(&signing));
    }

    // Same version path (the mock serves 1.0.0) but a DIFFERENT digest: the
    // staleness check keys on the pinned digest (the marker), not the URL
    // version, so a changed digest alone must force the re-pull.
    let result = ensure_cached("router_echo", "1.0.0", &new_digest).await;

    let cached_after = std::fs::read(&dest).unwrap_or_default();
    let marker_after = std::fs::read_to_string(gtxpack_marker_path(&dest)).unwrap_or_default();
    unsafe {
        std::env::remove_var("GREENTIC_MCP_LOCAL_CACHE_DIR");
        std::env::remove_var(STORE_URL_ENV);
        std::env::remove_var(TRUSTED_SIGNERS_ENV);
    }

    result.expect("stale-marker cache must re-pull the upgraded component");
    assert_eq!(
        cached_after, new_wasm,
        "the upgraded wasm must replace the stale one"
    );
    assert!(
        marker_after.trim().eq_ignore_ascii_case(&new_digest),
        "the marker must be rewritten to the new pinned digest"
    );
}

/// Concurrent pulls of one ref are serialised: the second waits for the first
/// and then finds a current cache instead of downloading and rewriting the
/// sidecar/wasm/marker triple underneath it.
#[tokio::test]
#[serial_test::serial]
async fn concurrent_pulls_of_one_ref_download_once() {
    let signing = SigningKey::from_bytes(&[21u8; 32]);
    let signed_describe = sign_describe_like_store(&sample_describe(), &signing);
    let archive = build_gtxpack(&signed_describe, b"fake-wasm-bytes");
    let digest = hex_sha256(&archive);
    let server = mock_store(archive).await;

    let cache = tempfile::tempdir().unwrap();
    unsafe {
        std::env::set_var("GREENTIC_MCP_LOCAL_CACHE_DIR", cache.path());
        std::env::set_var(STORE_URL_ENV, server.uri());
        std::env::set_var(TRUSTED_SIGNERS_ENV, pubkey_env_value(&signing));
    }

    let (a, b, c) = tokio::join!(
        ensure_cached("router_echo", "1.0.0", &digest),
        ensure_cached("router_echo", "1.0.0", &digest),
        ensure_cached("router_echo", "1.0.0", &digest),
    );
    let downloads = server.received_requests().await.unwrap().len();
    let dest = cache.path().join("router_echo.wasm");
    let current = cache_is_current(&dest, &digest);
    unsafe {
        std::env::remove_var("GREENTIC_MCP_LOCAL_CACHE_DIR");
        std::env::remove_var(STORE_URL_ENV);
        std::env::remove_var(TRUSTED_SIGNERS_ENV);
    }

    a.unwrap();
    b.unwrap();
    c.unwrap();
    assert_eq!(downloads, 1, "the pull lock must dedupe concurrent pulls");
    assert!(current);
}

/// Different refs do not share a lock.
#[test]
fn pull_locks_are_per_component_ref() {
    let a1 = pull_lock("alpha");
    let a2 = pull_lock("alpha");
    let b = pull_lock("beta");
    assert!(std::sync::Arc::ptr_eq(&a1, &a2));
    assert!(!std::sync::Arc::ptr_eq(&a1, &b));
}
