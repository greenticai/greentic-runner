//! A misconfigured durable store must fail LOUDLY, at construction.
//!
//! These need no Redis — they point at a port nothing listens on — so they run
//! wherever the rest of the suite runs. That matters: the failure this file
//! guards against is a host that starts perfectly and silently restarts every
//! conversation, and a gate that only runs when a service container happens to
//! be wired would not catch it.

use anyhow::Result;
use greentic_runner_host::engine::host::SessionKey;
use greentic_runner_host::storage::{
    SessionBackend, StateBackend, StorageConfig, StorageConfigError, session_host_from,
    session_store_from_config, state_host_from, state_store_from_config, stores_from_config,
};
use greentic_types::{EnvId, TenantCtx, TenantId};
use serde_json::json;

/// Nothing listens here, so a connection attempt is refused immediately rather
/// than hanging for a timeout.
const UNREACHABLE: &str = "redis://127.0.0.1:1";

#[tokio::test]
async fn the_default_config_builds_two_working_in_memory_stores() -> Result<()> {
    // The path every caller that predates `with_storage` still takes: no URL
    // anywhere, no network, and a round-trip that works.
    let config = StorageConfig::default();
    assert!(!config.is_durable());
    let (session, state) = stores_from_config(&config)?;

    let ctx = TenantCtx::new(EnvId::new("local")?, TenantId::new("memorytest")?);
    let key = SessionKey::new(&ctx, "pack.mem", "flow.main", Some("session".into()));

    let state_host = state_host_from(state);
    state_host.set_json(&key, json!({ "value": 7 })).await?;
    assert_eq!(
        state_host.get_json(&key).await?,
        Some(json!({ "value": 7 }))
    );

    // And the session store is a live store rather than a stub.
    let session_host = session_host_from(session);
    assert!(session_host.get(&key).await?.is_none());
    Ok(())
}

#[test]
fn an_unreachable_session_backend_is_refused_at_construction() -> Result<()> {
    let backend = SessionBackend::redis(UNREACHABLE, "greentic:test:unreachable")?;
    // `expect_err` needs `Debug` on the Ok side, and `dyn SessionStore` has
    // none — match instead.
    let Err(err) = session_store_from_config(&backend) else {
        panic!("an unreachable session backend must not build");
    };
    let rendered = format!("{err:#}");
    assert!(
        rendered.contains("unreachable"),
        "the refusal must say the backend could not be reached, got: {rendered}"
    );
    Ok(())
}

#[test]
fn an_unreachable_state_backend_is_refused_at_construction() -> Result<()> {
    let backend = StateBackend::redis(UNREACHABLE)?;
    let Err(err) = state_store_from_config(&backend) else {
        panic!("an unreachable state backend must not build");
    };
    let rendered = format!("{err:#}");
    assert!(
        rendered.contains("unreachable"),
        "the refusal must say the backend could not be reached, got: {rendered}"
    );
    Ok(())
}

#[test]
fn an_unreachable_backend_never_degrades_into_an_in_memory_one() -> Result<()> {
    // The whole point: `stores_from_config` returns Err rather than a pair of
    // healthy-looking in-memory stores. A host built on those would pass every
    // health check and lose every parked conversation.
    let config = StorageConfig::redis(UNREACHABLE, "greentic:test:unreachable")?;
    assert!(stores_from_config(&config).is_err());
    Ok(())
}

#[test]
fn a_session_backend_with_no_namespace_is_refused_before_a_socket_is_opened() {
    let err = SessionBackend::redis("redis://127.0.0.1:6379", "   ")
        .expect_err("a blank namespace must be refused");
    assert_eq!(err, StorageConfigError::MissingSessionNamespace);
}

#[test]
fn a_backend_with_no_url_is_refused_before_a_socket_is_opened() {
    assert_eq!(
        StateBackend::redis("").expect_err("blank url"),
        StorageConfigError::MissingRedisUrl { store: "state" }
    );
    assert_eq!(
        SessionBackend::redis("", "ns").expect_err("blank url"),
        StorageConfigError::MissingRedisUrl { store: "session" }
    );
}
