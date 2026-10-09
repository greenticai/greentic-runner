//! Per-unit isolation of a pack's EXTENSION credentials, at the boundary a
//! `component.exec` WASM node actually crosses: `SecretsStoreHost::get` and
//! `SecretsStoreHostV1_1::put` on [`HostState`].
//!
//! `secrets_scoping.rs` pins the tenant/team half of the same address and
//! `secrets_canonicalization.rs` pins the key half; neither carries a unit, so
//! both keep asserting today's bare-pack shape unchanged. This file is the
//! third axis: two units of the SAME pack in one environment must be able to
//! hold different values.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use async_trait::async_trait;
use greentic_interfaces_wasmtime::host_helpers::v1::secrets_store::{
    SecretsStoreHost, SecretsStoreHostV1_1,
};
use greentic_runner_host::config::HostConfig;
use greentic_runner_host::pack::HostState;
use greentic_runner_host::secrets::{
    DynSecretsManager, scoped_secret_path_for_pack, scoped_secret_path_for_unit, unit_pack_segment,
};
use greentic_secrets_lib::{SecretError, SecretsManager};
use reqwest::blocking::Client as BlockingClient;
use serial_test::serial;
use tempfile::TempDir;

const PACK_ID: &str = "unit-scoping";
const KEY: &str = "HUBSPOT/ACCESS_TOKEN";
const CANONICAL_KEY: &str = "hubspot_access_token";

#[derive(Default)]
struct MapSecretsManager {
    entries: Mutex<HashMap<String, Vec<u8>>>,
    reads: Mutex<Vec<String>>,
    writes: Mutex<Vec<(String, Vec<u8>)>>,
}

#[async_trait]
impl SecretsManager for MapSecretsManager {
    async fn read(&self, path: &str) -> Result<Vec<u8>, SecretError> {
        self.reads
            .lock()
            .expect("reads lock")
            .push(path.to_string());
        self.entries
            .lock()
            .expect("entries lock")
            .get(path)
            .cloned()
            .ok_or_else(|| SecretError::NotFound(path.to_string()))
    }

    async fn write(&self, path: &str, bytes: &[u8]) -> Result<(), SecretError> {
        self.writes
            .lock()
            .expect("writes lock")
            .push((path.to_string(), bytes.to_vec()));
        self.entries
            .lock()
            .expect("entries lock")
            .insert(path.to_string(), bytes.to_vec());
        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<(), SecretError> {
        self.entries.lock().expect("entries lock").remove(path);
        Ok(())
    }
}

fn host_config() -> Result<Arc<HostConfig>> {
    let temp = TempDir::new()?;
    let path = temp.path().join("bindings.yaml");
    std::fs::write(
        &path,
        r#"
tenant: demo
flow_type_bindings: {}
rate_limits: {}
retry: {}
timers: []
"#,
    )?;
    let mut cfg = HostConfig::load_from_path(&path).context("load minimal host bindings")?;
    cfg.secrets_policy = greentic_runner_host::config::SecretsPolicy::allow_all();
    Ok(Arc::new(cfg))
}

fn host_state(
    config: &Arc<HostConfig>,
    secrets: DynSecretsManager,
    unit: Option<&str>,
) -> Result<HostState> {
    Ok(HostState::new(
        PACK_ID.to_string(),
        Arc::clone(config),
        Arc::new(BlockingClient::builder().build()?),
        None,
        None,
        None,
        secrets,
        None,
        None,
        Some("component.hubspot".to_string()),
        false,
        None,
        None,
    )?
    .with_unit(unit.map(str::to_string)))
}

fn unit_uri(config: &Arc<HostConfig>, unit: &str) -> String {
    scoped_secret_path_for_unit(&config.tenant_ctx(), PACK_ID, unit, KEY)
        .expect("unit uri")
        .expect("a unit scope exists")
}

fn bare_uri(config: &Arc<HostConfig>) -> String {
    scoped_secret_path_for_pack(&config.tenant_ctx(), PACK_ID, KEY).expect("bare uri")
}

struct EnvGuard {
    key: String,
    prev: Option<String>,
}

impl EnvGuard {
    fn set(key: &str, value: &str) -> Self {
        let prev = std::env::var(key).ok();
        unsafe { std::env::set_var(key, value) };
        EnvGuard {
            key: key.to_string(),
            prev,
        }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match self.prev.clone() {
            Some(val) => unsafe { std::env::set_var(&self.key, val) },
            None => unsafe { std::env::remove_var(&self.key) },
        }
    }
}

/// The address the component reads carries the unit, and the shared address is
/// not consulted once it hits.
#[test]
#[serial]
fn a_units_own_value_wins_over_the_value_shared_by_every_unit() -> Result<()> {
    let _guard = EnvGuard::set("GREENTIC_PROVIDER_CORE_ONLY", "0");
    let config = host_config()?;
    let manager = Arc::new(MapSecretsManager::default());
    manager.entries.lock().expect("entries lock").extend([
        (unit_uri(&config, "web-assistant"), b"unit-token".to_vec()),
        (bare_uri(&config), b"shared-token".to_vec()),
    ]);

    let secrets: DynSecretsManager = manager.clone();
    let mut state = host_state(&config, secrets, Some("web-assistant"))?;
    let got = SecretsStoreHost::get(&mut state, KEY.to_string())
        .expect("read")
        .expect("a value");

    assert_eq!(got, b"unit-token".to_vec());
    assert_eq!(
        manager.reads.lock().expect("reads lock").as_slice(),
        &[unit_uri(&config, "web-assistant")],
        "the shared address must not be read once the unit address hits"
    );
    Ok(())
}

/// Compatibility: an environment staged before the unit scope existed keeps
/// resolving. Nothing that resolved before stops resolving.
#[test]
#[serial]
fn a_unit_with_no_value_of_its_own_falls_back_to_the_shared_address() -> Result<()> {
    let _guard = EnvGuard::set("GREENTIC_PROVIDER_CORE_ONLY", "0");
    let config = host_config()?;
    let manager = Arc::new(MapSecretsManager::default());
    manager
        .entries
        .lock()
        .expect("entries lock")
        .insert(bare_uri(&config), b"shared-token".to_vec());

    let secrets: DynSecretsManager = manager.clone();
    let mut state = host_state(&config, secrets, Some("web-assistant"))?;
    let got = SecretsStoreHost::get(&mut state, KEY.to_string())
        .expect("read")
        .expect("a value");

    assert_eq!(got, b"shared-token".to_vec());
    assert_eq!(
        manager.reads.lock().expect("reads lock").as_slice(),
        &[unit_uri(&config, "web-assistant"), bare_uri(&config)],
    );
    Ok(())
}

/// The property this whole change exists for.
#[test]
#[serial]
fn two_units_of_the_same_pack_read_different_values() -> Result<()> {
    let _guard = EnvGuard::set("GREENTIC_PROVIDER_CORE_ONLY", "0");
    let config = host_config()?;
    let manager = Arc::new(MapSecretsManager::default());
    manager.entries.lock().expect("entries lock").extend([
        (unit_uri(&config, "sales-bot"), b"token-sales".to_vec()),
        (unit_uri(&config, "support-bot"), b"token-support".to_vec()),
        (bare_uri(&config), b"shared-token".to_vec()),
    ]);

    assert_ne!(
        unit_uri(&config, "sales-bot"),
        unit_uri(&config, "support-bot"),
        "two units of one pack must not share an address"
    );

    for (unit, expected) in [
        ("sales-bot", "token-sales"),
        ("support-bot", "token-support"),
    ] {
        let secrets: DynSecretsManager = manager.clone();
        let mut state = host_state(&config, secrets, Some(unit))?;
        let got = SecretsStoreHost::get(&mut state, KEY.to_string())
            .expect("read")
            .expect("a value");
        assert_eq!(got, expected.as_bytes().to_vec(), "unit {unit}");
    }
    Ok(())
}

/// A host state with no unit must address exactly what it addressed before —
/// the legacy tenant-only runtime and the process-level serve path depend on it.
#[test]
#[serial]
fn no_unit_reads_exactly_the_pre_existing_address() -> Result<()> {
    let _guard = EnvGuard::set("GREENTIC_PROVIDER_CORE_ONLY", "0");
    let config = host_config()?;
    let manager = Arc::new(MapSecretsManager::default());
    manager
        .entries
        .lock()
        .expect("entries lock")
        .insert(bare_uri(&config), b"shared-token".to_vec());

    let secrets: DynSecretsManager = manager.clone();
    let mut state = host_state(&config, secrets, None)?;
    let got = SecretsStoreHost::get(&mut state, KEY.to_string())
        .expect("read")
        .expect("a value");

    assert_eq!(got, b"shared-token".to_vec());
    assert_eq!(
        manager.reads.lock().expect("reads lock").as_slice(),
        &[bare_uri(&config)],
    );
    Ok(())
}

/// A component writing a refreshed token back must not overwrite the value
/// every other unit of the same pack reads.
#[test]
#[serial]
fn a_write_lands_at_the_units_own_address_and_nowhere_else() -> Result<()> {
    let _guard = EnvGuard::set("GREENTIC_PROVIDER_CORE_ONLY", "0");
    let config = host_config()?;
    let manager = Arc::new(MapSecretsManager::default());
    manager
        .entries
        .lock()
        .expect("entries lock")
        .insert(bare_uri(&config), b"shared-token".to_vec());

    let secrets: DynSecretsManager = manager.clone();
    let mut state = host_state(&config, secrets, Some("web-assistant"))?;
    SecretsStoreHostV1_1::put(&mut state, KEY.to_string(), b"refreshed".to_vec());

    assert_eq!(
        manager.writes.lock().expect("writes lock").as_slice(),
        &[(unit_uri(&config, "web-assistant"), b"refreshed".to_vec())],
    );
    assert_eq!(
        manager
            .entries
            .lock()
            .expect("entries lock")
            .get(&bare_uri(&config))
            .cloned(),
        Some(b"shared-token".to_vec()),
        "the shared value must be untouched by a unit's write"
    );
    Ok(())
}

/// The unit rides in the PACK (category) segment, the key keeps its own
/// canonicalization, and the whole thing is a five-segment URI — the shape
/// every downstream writer has to reproduce.
#[test]
fn the_unit_scoped_address_keeps_the_five_segment_shape() -> Result<()> {
    let config = host_config()?;
    let uri = unit_uri(&config, "web-assistant");
    let segment = unit_pack_segment(PACK_ID, "web-assistant").expect("segment");

    assert_eq!(
        uri,
        format!(
            "secrets://{}/{}/_/{}/{}",
            config.tenant_ctx().env.as_str(),
            config.tenant_ctx().tenant.as_str(),
            segment,
            CANONICAL_KEY,
        )
    );
    assert!(greentic_secrets_lib::spec::SecretUri::parse(&uri).is_ok());
    assert_ne!(uri, bare_uri(&config));
    Ok(())
}
