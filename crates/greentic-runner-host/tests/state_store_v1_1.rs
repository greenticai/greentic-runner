//! `greentic:state/state-store@1.1.0` served by the runner host: a component
//! importing `write-if-absent` instantiates and creates exactly once, a
//! component importing only `@1.0.0` still works from the same registration,
//! and a backend without an atomic implementation surfaces `unsupported`.

use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use greentic_interfaces_wasmtime::host_helpers::v1::state_store::{
    StateStoreHost, StateStoreHostV1_1,
};
use greentic_runner_host::config::HostConfig;
use greentic_runner_host::pack::{self, ComponentState, HostState};
use greentic_runner_host::runtime_wasmtime::{Component, Engine, Linker, Store};
use greentic_runner_host::secrets::default_manager;
use greentic_runner_host::storage::DynStateStore;
use greentic_runner_host::wasi::RunnerWasiPolicy;
use greentic_state::inmemory::InMemoryStateStore;
use greentic_state::{StatePath, StateStore};
use greentic_types::{EnvId, GResult, StateKey, TenantCtx, TenantId};
use reqwest::blocking::Client as BlockingClient;
use serde_json::Value;
use tempfile::TempDir;

const TYPES: &str = r#"
    (type $imp0 (record (field "actor-id" string) (field "reason" (option string))))
    (export "impersonation" (type $imp (eq $imp0)))
    (type $attr0 (tuple string string))
    (export "attr" (type $attr (eq $attr0)))
    (type $attrs0 (list $attr))
    (export "attrs" (type $attrs (eq $attrs0)))
    (type $oimp0 (option $imp))
    (export "oimp" (type $oimp (eq $oimp0)))
    (type $ctx0 (record
      (field "env" string) (field "tenant" string) (field "tenant-id" string)
      (field "team" (option string)) (field "team-id" (option string))
      (field "user" (option string)) (field "user-id" (option string))
      (field "trace-id" (option string)) (field "i18n-id" (option string))
      (field "correlation-id" (option string))
      (field "attributes" $attrs)
      (field "session-id" (option string)) (field "flow-id" (option string))
      (field "node-id" (option string)) (field "provider-id" (option string))
      (field "deadline-ms" (option s64)) (field "attempt" u32)
      (field "idempotency-key" (option string))
      (field "impersonation" $oimp)))
    (export "tenant-ctx" (type $ctx (eq $ctx0)))
    (type $octx0 (option $ctx))
    (export "octx" (type $octx (eq $octx0)))
    (type $herr0 (record (field "code" string) (field "message" string)))
    (export "host-error" (type $herr (eq $herr0)))
    (type $rbool0 (result bool (error $herr)))
    (export "rbool" (type $rbool (eq $rbool0)))
    (type $ack0 (enum "ok"))
    (export "op-ack" (type $ack (eq $ack0)))
    (type $rack0 (result $ack (error $herr)))
    (export "rack" (type $rack (eq $rack0)))
"#;

/// Guest importing ONLY `@1.1.0`: `run` calls `write-if-absent("k", "v")`,
/// `run2` calls it with `("k", "w")`. Returns `(disc << 8) | bool`.
fn guest_v1_1() -> String {
    format!(
        r#"(component
  (import "greentic:state/state-store@1.1.0" (instance $s11
{types}
    (export "write-if-absent"
      (func (param "key" string) (param "bytes" (list u8))
            (param "ctx" $octx)
            (result $rbool)))))
  (core module $mem
    (memory (export "mem") 1)
    (global $bump (mut i32) (i32.const 8192))
    (func (export "realloc") (param i32 i32 i32 i32) (result i32)
      (local $p i32)
      (local.set $p (global.get $bump))
      (global.set $bump (i32.add (global.get $bump) (local.get 3)))
      (local.get $p)))
  (core instance $mi (instantiate $mem))
  (alias core export $mi "mem" (core memory $m))
  (alias core export $mi "realloc" (core func $r))
  (alias export $s11 "write-if-absent" (func $wia))
  (core func $wia_l (canon lower (func $wia) (memory $m) (realloc $r)))
  (core module $main
    (import "h" "mem" (memory 1))
    (import "h" "wia" (func $wia (param i32 i32)))
    (data (i32.const 1024) "k")
    (data (i32.const 1032) "v")
    (data (i32.const 1040) "w")
    (func $call (param $bytes i32) (result i32)
      (i32.store (i32.const 0) (i32.const 1024))
      (i32.store (i32.const 4) (i32.const 1))
      (i32.store (i32.const 8) (local.get $bytes))
      (i32.store (i32.const 12) (i32.const 1))
      (i32.store8 (i32.const 16) (i32.const 0))
      (call $wia (i32.const 0) (i32.const 512))
      (i32.or
        (i32.shl (i32.load8_u (i32.const 512)) (i32.const 8))
        (i32.load8_u (i32.const 516))))
    (func (export "run") (result i32) (call $call (i32.const 1032)))
    (func (export "run2") (result i32) (call $call (i32.const 1040))))
  (core instance $hi
    (export "mem" (memory $m))
    (export "wia" (func $wia_l)))
  (core instance $mainI (instantiate $main (with "h" (instance $hi))))
  (alias core export $mainI "run" (core func $run_c))
  (alias core export $mainI "run2" (core func $run2_c))
  (func (export "run") (result u32) (canon lift (core func $run_c)))
  (func (export "run2") (result u32) (canon lift (core func $run2_c))))"#,
        types = TYPES
    )
}

/// Guest importing ONLY `@1.0.0`: `run` calls `write("k", "v")` and returns the
/// result discriminant (0 = ok).
fn guest_v1_0() -> String {
    format!(
        r#"(component
  (import "greentic:state/state-store@1.0.0" (instance $s10
{types}
    (export "write"
      (func (param "key" string) (param "bytes" (list u8))
            (param "ctx" $octx)
            (result $rack)))))
  (core module $mem
    (memory (export "mem") 1)
    (global $bump (mut i32) (i32.const 8192))
    (func (export "realloc") (param i32 i32 i32 i32) (result i32)
      (local $p i32)
      (local.set $p (global.get $bump))
      (global.set $bump (i32.add (global.get $bump) (local.get 3)))
      (local.get $p)))
  (core instance $mi (instantiate $mem))
  (alias core export $mi "mem" (core memory $m))
  (alias core export $mi "realloc" (core func $r))
  (alias export $s10 "write" (func $wr))
  (core func $wr_l (canon lower (func $wr) (memory $m) (realloc $r)))
  (core module $main
    (import "h" "mem" (memory 1))
    (import "h" "wr" (func $wr (param i32 i32)))
    (data (i32.const 1024) "k")
    (data (i32.const 1032) "v")
    (func (export "run") (result i32)
      (i32.store (i32.const 0) (i32.const 1024))
      (i32.store (i32.const 4) (i32.const 1))
      (i32.store (i32.const 8) (i32.const 1032))
      (i32.store (i32.const 12) (i32.const 1))
      (i32.store8 (i32.const 16) (i32.const 0))
      (call $wr (i32.const 0) (i32.const 512))
      (i32.load8_u (i32.const 512))))
  (core instance $hi
    (export "mem" (memory $m))
    (export "wr" (func $wr_l)))
  (core instance $mainI (instantiate $main (with "h" (instance $hi))))
  (alias core export $mainI "run" (core func $run_c))
  (func (export "run") (result u32) (canon lift (core func $run_c))))"#,
        types = TYPES
    )
}

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
        "state-store-v1-1".to_string(),
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

fn instantiate(
    wat: &str,
    config: &Arc<HostConfig>,
    store: DynStateStore,
) -> Result<(Store<ComponentState>, wasmtime::component::Instance)> {
    let engine = Engine::default();
    let component =
        Component::new(&engine, wat).map_err(|err| anyhow!("component compile: {err:#}"))?;
    let host = host_state(config, store)?;
    let policy = Arc::new(RunnerWasiPolicy::default());
    let mut wasm_store = Store::new(&engine, ComponentState::new(host, policy)?);
    let mut linker = Linker::new(&engine);
    pack::register_all(&mut linker, true)?;
    let instance = linker
        .instantiate(&mut wasm_store, &component)
        .map_err(|err| anyhow!("instantiate: {err:#}"))?;
    Ok((wasm_store, instance))
}

/// What the host derives for a context-less call: env `local`, tenant `demo`.
fn tenant_ctx() -> Result<TenantCtx> {
    let env = std::env::var("GREENTIC_ENV").unwrap_or_else(|_| "local".to_string());
    Ok(TenantCtx::new(
        EnvId::from_str(&env)?,
        TenantId::from_str("demo")?,
    ))
}

fn stored(store: &DynStateStore, key: &str) -> Result<Option<Value>> {
    store
        .get_json(&tenant_ctx()?, "runner", &StateKey::new(key), None)
        .map_err(|err| anyhow!("{err}"))
}

#[test]
fn a_component_importing_write_if_absent_creates_exactly_once() -> Result<()> {
    let (_dir, config) = config()?;
    let store: DynStateStore = Arc::new(InMemoryStateStore::new());
    let (mut wasm, instance) = instantiate(&guest_v1_1(), &config, Arc::clone(&store))?;
    let run = instance
        .get_typed_func::<(), (u32,)>(&mut wasm, "run")
        .map_err(|e| anyhow!("{e:#}"))?;
    let run2 = instance
        .get_typed_func::<(), (u32,)>(&mut wasm, "run2")
        .map_err(|e| anyhow!("{e:#}"))?;

    // disc 0 (ok) in the high byte, the bool in the low byte.
    assert_eq!(
        run.call(&mut wasm, ()).map_err(|e| anyhow!("{e:#}"))?.0,
        0x0001
    );
    assert_eq!(
        run.call(&mut wasm, ()).map_err(|e| anyhow!("{e:#}"))?.0,
        0x0000
    );
    // A different value on a live key is also refused, and the value is unchanged.
    assert_eq!(
        run2.call(&mut wasm, ()).map_err(|e| anyhow!("{e:#}"))?.0,
        0x0000
    );
    assert_eq!(stored(&store, "k")?, Some(Value::String("v".into())));

    // The same host serves the 1.1.0 read.
    let bytes = StateStoreHostV1_1::read(&mut wasm.data_mut().host, "k".into(), None)
        .map_err(|e| anyhow!("{}: {}", e.code, e.message))?;
    assert_eq!(bytes, b"\"v\"".to_vec());
    Ok(())
}

#[test]
fn a_component_importing_only_state_store_1_0_0_still_works() -> Result<()> {
    let (_dir, config) = config()?;
    let store: DynStateStore = Arc::new(InMemoryStateStore::new());
    let (mut wasm, instance) = instantiate(&guest_v1_0(), &config, Arc::clone(&store))?;
    let run = instance
        .get_typed_func::<(), (u32,)>(&mut wasm, "run")
        .map_err(|e| anyhow!("{e:#}"))?;
    assert_eq!(
        run.call(&mut wasm, ()).map_err(|e| anyhow!("{e:#}"))?.0,
        0,
        "write is ok"
    );
    assert_eq!(stored(&store, "k")?, Some(Value::String("v".into())));
    Ok(())
}

#[test]
fn concurrent_write_if_absent_through_the_host_creates_once() -> Result<()> {
    const THREADS: usize = 16;
    let (_dir, config) = config()?;
    let store: DynStateStore = Arc::new(InMemoryStateStore::new());
    let barrier = Arc::new(std::sync::Barrier::new(THREADS));
    let created: usize = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..THREADS)
            .map(|i| {
                let config = Arc::clone(&config);
                let store = Arc::clone(&store);
                let barrier = Arc::clone(&barrier);
                scope.spawn(move || -> Result<bool> {
                    let mut host = host_state(&config, store)?;
                    barrier.wait();
                    StateStoreHostV1_1::write_if_absent(
                        &mut host,
                        "race".into(),
                        format!("\"t{i}\"").into_bytes(),
                        None,
                    )
                    .map_err(|e| anyhow!("{}: {}", e.code, e.message))
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().map_err(|_| anyhow!("thread panicked"))?)
            .collect::<Result<Vec<bool>>>()
            .map(|all| all.into_iter().filter(|created| *created).count())
    })?;
    assert_eq!(created, 1, "exactly one caller creates the key");
    assert!(stored(&store, "race")?.is_some());
    Ok(())
}

/// A backend implementing only the four original methods.
struct LegacyStore(InMemoryStateStore);

impl StateStore for LegacyStore {
    fn get_json(
        &self,
        tenant: &TenantCtx,
        prefix: &str,
        key: &StateKey,
        path: Option<&StatePath>,
    ) -> GResult<Option<Value>> {
        self.0.get_json(tenant, prefix, key, path)
    }

    fn set_json(
        &self,
        tenant: &TenantCtx,
        prefix: &str,
        key: &StateKey,
        path: Option<&StatePath>,
        value: &Value,
        ttl_secs: Option<u32>,
    ) -> GResult<()> {
        self.0.set_json(tenant, prefix, key, path, value, ttl_secs)
    }

    fn del(&self, tenant: &TenantCtx, prefix: &str, key: &StateKey) -> GResult<bool> {
        self.0.del(tenant, prefix, key)
    }

    fn del_prefix(&self, tenant: &TenantCtx, prefix: &str) -> GResult<u64> {
        self.0.del_prefix(tenant, prefix)
    }
}

#[test]
fn a_backend_without_an_atomic_implementation_reports_unsupported() -> Result<()> {
    let (_dir, config) = config()?;
    let store: DynStateStore = Arc::new(LegacyStore(InMemoryStateStore::new()));
    let mut host = host_state(&config, store)?;
    let err = StateStoreHostV1_1::write_if_absent(&mut host, "k".into(), b"1".to_vec(), None)
        .expect_err("default implementation refuses");
    assert_eq!(err.code, "unsupported");
    // The legacy write path is unaffected.
    StateStoreHost::write(&mut host, "k".into(), b"1".to_vec(), None)
        .map_err(|e| anyhow!("{}: {}", e.code, e.message))?;
    Ok(())
}
