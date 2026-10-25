use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use async_trait::async_trait;
use greentic_interfaces_wasmtime::host_helpers::v1::secrets_store::SecretsStoreHostV1_1;
use greentic_runner_host::config::HostConfig;
use greentic_runner_host::pack::HostState;
use greentic_runner_host::secrets::{DynSecretsManager, scoped_secret_path_for_pack};
use greentic_secrets_lib::{SecretError, SecretsManager};
use reqwest::blocking::Client as BlockingClient;
use serial_test::serial;
use tempfile::TempDir;

const SECRETS_STORE_PACK_ID: &str = "secrets-store-put";

#[derive(Default)]
struct RecordingSecretsManager {
    writes: Mutex<Vec<(String, Vec<u8>)>>,
}

#[async_trait]
impl SecretsManager for RecordingSecretsManager {
    async fn read(&self, _path: &str) -> Result<Vec<u8>, SecretError> {
        Err(SecretError::NotFound("missing".into()))
    }

    async fn write(&self, path: &str, bytes: &[u8]) -> Result<(), SecretError> {
        let mut writes = self.writes.lock().expect("writes lock");
        writes.push((path.to_string(), bytes.to_vec()));
        Ok(())
    }

    async fn delete(&self, _path: &str) -> Result<(), SecretError> {
        Ok(())
    }
}

#[test]
#[serial]
fn secrets_store_put_allows_write_when_policy_allows() -> Result<()> {
    let _guard = EnvGuard::set("GREENTIC_PROVIDER_CORE_ONLY", "0");
    let config = write_minimal_config(true)?;
    let manager = Arc::new(RecordingSecretsManager::default());
    let secrets: DynSecretsManager = manager.clone();
    let mut host_state = HostState::new(
        "secrets-store-put".to_string(),
        Arc::clone(&config),
        Arc::new(BlockingClient::builder().build()?),
        None,
        None,
        None,
        secrets,
        None,
        None,
        Some("component.alpha".to_string()),
        false,
        None,
        None,
    )?;

    SecretsStoreHostV1_1::put(&mut host_state, "demo-key".to_string(), b"value".to_vec());

    let writes = manager.writes.lock().expect("writes lock");
    assert_eq!(writes.len(), 1);
    let expected_path =
        scoped_secret_path_for_pack(&config.tenant_ctx(), SECRETS_STORE_PACK_ID, "demo_key")?;
    assert_eq!(writes[0].0, expected_path);
    assert!(
        writes[0].1.as_slice() == b"value",
        "secret write value mismatch"
    );
    Ok(())
}

#[test]
#[serial]
fn secrets_store_put_denies_when_policy_blocks() -> Result<()> {
    let _guard = EnvGuard::set("GREENTIC_PROVIDER_CORE_ONLY", "0");
    let config = write_minimal_config(false)?;
    let manager = Arc::new(RecordingSecretsManager::default());
    let secrets: DynSecretsManager = manager.clone();
    let mut host_state = HostState::new(
        "secrets-store-put".to_string(),
        Arc::clone(&config),
        Arc::new(BlockingClient::builder().build()?),
        None,
        None,
        None,
        secrets,
        None,
        None,
        Some("component.beta".to_string()),
        false,
        None,
        None,
    )?;

    // The trait `put` has no result type: a refusal must be dropped, never a
    // panic. The fallible entry point reports it.
    SecretsStoreHostV1_1::put(&mut host_state, "blocked".to_string(), b"nope".to_vec());
    let err = host_state
        .try_put_secret("blocked", b"nope")
        .expect_err("put should be denied");
    let msg = err.to_string();
    assert!(msg.contains("blocked"));
    assert!(msg.contains("denied"));
    assert!(!msg.contains("nope"));

    // Guest-reachable refusals are errors, not panics: empty key, and a key
    // carrying a newline is sanitised in the message.
    let err = host_state
        .try_put_secret("   ", b"x")
        .expect_err("empty key refused");
    assert!(err.to_string().contains("invalid key"));
    let err = host_state
        .try_put_secret("evil\nWARN forged", b"x")
        .expect_err("policy refused");
    assert!(!err.to_string().contains('\n'));

    let writes = manager.writes.lock().expect("writes lock");
    assert_eq!(writes.len(), 0);
    Ok(())
}

fn write_minimal_config(allow_all: bool) -> Result<Arc<HostConfig>> {
    let temp = TempDir::new()?;
    let path = temp.path().join("bindings.yaml");
    let contents = r#"
tenant: demo
flow_type_bindings: {}
rate_limits: {}
retry: {}
timers: []
"#;
    std::fs::write(&path, contents)?;
    let mut cfg = HostConfig::load_from_path(&path).context("load minimal host bindings")?;
    if allow_all {
        cfg.secrets_policy = greentic_runner_host::config::SecretsPolicy::allow_all();
    }
    Ok(Arc::new(cfg))
}

struct EnvGuard {
    key: String,
    prev: Option<String>,
}

impl EnvGuard {
    fn set(key: &str, value: &str) -> Self {
        let prev = std::env::var(key).ok();
        unsafe {
            std::env::set_var(key, value);
        }
        EnvGuard {
            key: key.to_string(),
            prev,
        }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        if let Some(val) = self.prev.clone() {
            unsafe {
                std::env::set_var(&self.key, val);
            }
        } else {
            unsafe {
                std::env::remove_var(&self.key);
            }
        }
    }
}
