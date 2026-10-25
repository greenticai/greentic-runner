//! Defensive ceilings on guest-supplied sizes: a state value above
//! `GREENTIC_STATE_MAX_VALUE_MB` and an outbound request body above
//! `GREENTIC_HTTP_MAX_REQUEST_MB` are refused with a normal error, and `0`
//! disables the cap.

use std::sync::Arc;

use anyhow::{Context, Result};
use greentic_interfaces_wasmtime::host_helpers::v1::http_client::{HttpClientHost, Request};
use greentic_interfaces_wasmtime::host_helpers::v1::state_store::StateStoreHost;
use greentic_runner_host::config::HostConfig;
use greentic_runner_host::pack::HostState;
use greentic_runner_host::secrets::default_manager;
use greentic_runner_host::storage::DynStateStore;
use greentic_state::inmemory::InMemoryStateStore;
use reqwest::blocking::Client as BlockingClient;
use serial_test::serial;
use tempfile::TempDir;

struct EnvGuard {
    key: &'static str,
    prev: Option<String>,
}

impl EnvGuard {
    fn set(key: &'static str, value: &str) -> Self {
        let prev = std::env::var(key).ok();
        // SAFETY: tests using this guard are `#[serial]`.
        unsafe { std::env::set_var(key, value) };
        Self { key, prev }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        // SAFETY: tests using this guard are `#[serial]`.
        unsafe {
            match &self.prev {
                Some(v) => std::env::set_var(self.key, v),
                None => std::env::remove_var(self.key),
            }
        }
    }
}

fn host_state() -> Result<(TempDir, HostState)> {
    let temp = TempDir::new()?;
    let path = temp.path().join("bindings.yaml");
    std::fs::write(
        &path,
        "tenant: demo\nflow_type_bindings: {}\nrate_limits: {}\nretry: {}\ntimers: []\n",
    )?;
    let mut cfg = HostConfig::load_from_path(&path).context("load minimal host bindings")?;
    cfg.http_enabled = true;
    let store: DynStateStore = Arc::new(InMemoryStateStore::new());
    let state = HostState::new(
        "guest-input-caps".to_string(),
        Arc::new(cfg),
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
    )?;
    Ok((temp, state))
}

#[test]
#[serial]
fn state_value_over_cap_is_refused_and_zero_disables() -> Result<()> {
    let (_dir, mut host) = host_state()?;
    let _cap = EnvGuard::set("GREENTIC_STATE_MAX_VALUE_MB", "1");

    let small = StateStoreHost::write(&mut host, "k".into(), b"\"ok\"".to_vec(), None);
    assert!(small.is_ok());

    let big = vec![b'a'; 1024 * 1024 + 1];
    let err = StateStoreHost::write(&mut host, "big".into(), big.clone(), None)
        .expect_err("value over the cap must be refused");
    assert_eq!(err.code, "too-large");

    drop(_cap);
    let _off = EnvGuard::set("GREENTIC_STATE_MAX_VALUE_MB", "0");
    assert!(StateStoreHost::write(&mut host, "big".into(), big, None).is_ok());
    Ok(())
}

#[test]
#[serial]
fn http_request_body_over_cap_is_refused() -> Result<()> {
    let (_dir, mut host) = host_state()?;
    let _cap = EnvGuard::set("GREENTIC_HTTP_MAX_REQUEST_MB", "1");
    let req = Request {
        method: "POST".into(),
        url: "http://127.0.0.1:9/".into(),
        headers: Vec::new(),
        body: Some(vec![0u8; 1024 * 1024 + 1]),
    };
    let err = HttpClientHost::send(&mut host, req, None)
        .expect_err("oversized body must be refused before any network call");
    assert_eq!(err.code, "too-large");
    Ok(())
}
