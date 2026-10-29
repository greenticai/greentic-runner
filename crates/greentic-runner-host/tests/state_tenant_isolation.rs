//! State host imports must use the host-bound tenant: a guest-supplied
//! `ctx.tenant` / `ctx.env` must never redirect reads or writes to another
//! tenant's state, and binary values must survive a round trip.

use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use greentic_interfaces_wasmtime::host_helpers::v1::state_store::{
    StateStoreHost, TenantCtx as GuestCtx,
};
use greentic_runner_host::config::HostConfig;
use greentic_runner_host::pack::HostState;
use greentic_runner_host::secrets::default_manager;
use greentic_runner_host::storage::DynStateStore;
use greentic_state::inmemory::InMemoryStateStore;
use greentic_types::{EnvId, StateKey, TenantCtx, TenantId};
use reqwest::blocking::Client as BlockingClient;
use serde_json::Value;
use tempfile::TempDir;

fn config() -> Result<(TempDir, Arc<HostConfig>)> {
    let temp = TempDir::new()?;
    let path = temp.path().join("bindings.yaml");
    std::fs::write(
        &path,
        "tenant: demo\nflow_type_bindings: {}\nrate_limits: {}\nretry: {}\ntimers: []\n",
    )?;
    let cfg = HostConfig::load_from_path(&path).context("load minimal host bindings")?;
    Ok((temp, Arc::new(cfg)))
}

fn host_state(config: &Arc<HostConfig>, store: DynStateStore) -> Result<HostState> {
    HostState::new(
        "state-tenant-isolation".to_string(),
        Arc::clone(config),
        Arc::new(BlockingClient::builder().build()?),
        None,
        None,
        Some(store),
        default_manager()?,
        None,
        None,
        None,
        false,
        None,
        None,
    )
}

fn guest_ctx(tenant: &str, env: &str) -> GuestCtx {
    GuestCtx {
        env: env.to_string(),
        tenant: tenant.to_string(),
        tenant_id: tenant.to_string(),
        team: None,
        team_id: None,
        user: None,
        user_id: None,
        trace_id: None,
        i18n_id: None,
        correlation_id: None,
        attributes: Vec::new(),
        session_id: None,
        flow_id: None,
        node_id: None,
        provider_id: None,
        deadline_ms: None,
        attempt: 0,
        idempotency_key: None,
        impersonation: None,
    }
}

fn host_ctx(tenant: &str) -> Result<TenantCtx> {
    let env = std::env::var("GREENTIC_ENV").unwrap_or_else(|_| "local".to_string());
    Ok(TenantCtx::new(
        EnvId::from_str(&env)?,
        TenantId::from_str(tenant)?,
    ))
}

#[test]
fn guest_supplied_tenant_cannot_redirect_state_access() -> Result<()> {
    let (_dir, config) = config()?;
    let store: DynStateStore = Arc::new(InMemoryStateStore::new());
    let mut host = host_state(&config, Arc::clone(&store))?;

    // Victim data under another tenant.
    store
        .set_json(
            &host_ctx("victim")?,
            "runner",
            &StateKey::new("secret"),
            None,
            &Value::String("victim-data".into()),
            None,
        )
        .map_err(|e| anyhow!("{e}"))?;

    // Guest claims to be the victim tenant: the read must not see its data.
    let read = StateStoreHost::read(
        &mut host,
        "secret".into(),
        Some(guest_ctx("victim", "local")),
    );
    assert!(read.is_err(), "guest ctx must not reach another tenant");

    // A guest write claiming the victim tenant lands in the host-bound tenant.
    StateStoreHost::write(
        &mut host,
        "secret".into(),
        b"\"overwrite\"".to_vec(),
        Some(guest_ctx("victim", "local")),
    )
    .map_err(|e| anyhow!("{}: {}", e.code, e.message))?;
    let victim = store
        .get_json(
            &host_ctx("victim")?,
            "runner",
            &StateKey::new("secret"),
            None,
        )
        .map_err(|e| anyhow!("{e}"))?;
    assert_eq!(victim, Some(Value::String("victim-data".into())));
    let own = store
        .get_json(&host_ctx("demo")?, "runner", &StateKey::new("secret"), None)
        .map_err(|e| anyhow!("{e}"))?;
    assert_eq!(own, Some(Value::String("overwrite".into())));
    Ok(())
}

#[test]
fn binary_state_values_round_trip_without_loss() -> Result<()> {
    let (_dir, config) = config()?;
    let store: DynStateStore = Arc::new(InMemoryStateStore::new());
    let mut host = host_state(&config, store)?;
    let payload = vec![0xff, 0xfe, 0x00, 0x80, 0x41];
    StateStoreHost::write(&mut host, "bin".into(), payload.clone(), None)
        .map_err(|e| anyhow!("{}: {}", e.code, e.message))?;
    let read = StateStoreHost::read(&mut host, "bin".into(), None)
        .map_err(|e| anyhow!("{}: {}", e.code, e.message))?;
    assert_eq!(read, payload);
    Ok(())
}
