use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use greentic_state::inmemory::InMemoryStateStore;
use greentic_state::{StateKey as StoreStateKey, StateStore};
use greentic_types::{EnvId, TenantCtx, TenantId};
use serde_json::Value;

use crate::engine::error::{GResult, RunnerError};
use crate::engine::host::{SessionKey, StateHost};
use crate::fault::wrap_state_store;
use crate::storage::config::StateBackend;
use crate::storage::offload::{Offloaded, StoreKind};

pub type DynStateStore = Arc<dyn StateStore>;

pub(crate) const STATE_PREFIX: &str = "runner";

/// Adapter that backs the runner state host with a greentic-state store.
///
/// The store is held behind [`Offloaded`], so every call reaches it through
/// `spawn_blocking`. `StateStore` is a synchronous trait, and its Redis
/// implementation is a blocking `redis::Connection` behind a mutex — calling it
/// straight from these `async fn`s would park a tokio worker on a socket AND
/// serialise every state access in the process behind one connection.
pub struct StateStoreHost {
    store: Offloaded<dyn StateStore>,
    /// Expiry applied to every `set_json`. `None` keeps entries until deleted.
    ttl_secs: Option<u32>,
}

/// Env var bounding the lifetime of flow state (seconds). Unset or `0` keeps
/// the historical "never expires" behaviour, which on Redis means abandoned
/// conversations leave permanent keys.
pub const STATE_TTL_ENV: &str = "GREENTIC_FLOW_STATE_TTL_SECS";

impl StateStoreHost {
    pub fn new(store: DynStateStore) -> Self {
        Self {
            store: Offloaded::new(store, StoreKind::State),
            ttl_secs: None,
        }
    }

    /// Expire every state entry written through this host after `ttl_secs`.
    #[must_use]
    pub fn with_ttl_secs(mut self, ttl_secs: Option<u32>) -> Self {
        self.ttl_secs = ttl_secs.filter(|secs| *secs > 0);
        self
    }
}

fn ttl_from_env() -> Option<u32> {
    std::env::var(STATE_TTL_ENV)
        .ok()
        .and_then(|raw| raw.trim().parse::<u32>().ok())
        .filter(|secs| *secs > 0)
}

pub fn new_state_store() -> DynStateStore {
    let store: DynStateStore = Arc::new(InMemoryStateStore::new());
    wrap_state_store(store)
}

/// Build the flow-state store named by `backend`.
///
/// Unlike the session store this needs no namespace: `greentic_state::key::fqn`
/// already scopes every key by environment and tenant. See
/// [`crate::storage::config`] for the evidence.
pub fn store_from_config(backend: &StateBackend) -> Result<DynStateStore> {
    match backend {
        StateBackend::InMemory => Ok(new_state_store()),
        StateBackend::Redis { url } => {
            let store = greentic_state::redis_store::RedisStateStore::from_url(url)
                .map_err(|err| anyhow!("{err}"))
                .context("failed to open the Redis flow-state store")?;
            let store: DynStateStore = Arc::new(store);
            probe(&store)?;
            Ok(wrap_state_store(store))
        }
    }
}

/// Force a connection so an unreachable backend fails at construction rather
/// than mid-turn. `RedisStateStore::from_url` only parses the URL.
fn probe(store: &DynStateStore) -> Result<()> {
    let ctx = TenantCtx::new(
        EnvId::from_str("local").map_err(|err| anyhow!("{err}"))?,
        TenantId::from_str("greenticprobe").map_err(|err| anyhow!("{err}"))?,
    );
    store
        .get_json(
            &ctx,
            STATE_PREFIX,
            &StoreStateKey::from("startup-probe"),
            None,
        )
        .map(|_| ())
        .map_err(|err| anyhow!("{err}"))
        .context("the configured Redis flow-state store is unreachable")
}

pub fn state_host_from(store: DynStateStore) -> Arc<dyn StateHost> {
    Arc::new(StateStoreHost::new(store).with_ttl_secs(ttl_from_env()))
}

#[async_trait]
impl StateHost for StateStoreHost {
    async fn get_json(&self, key: &SessionKey) -> GResult<Option<Value>> {
        let tenant = tenant_ctx_from_key(key)?;
        let state_key = derive_state_key(key);
        self.store
            .call(move |store| store.get_json(&tenant, STATE_PREFIX, &state_key, None))
            .await
            .map_err(map_state_error)
    }

    async fn set_json(&self, key: &SessionKey, value: Value) -> GResult<()> {
        let tenant = tenant_ctx_from_key(key)?;
        let state_key = derive_state_key(key);
        let ttl_secs = self.ttl_secs;
        self.store
            .call(move |store| {
                store.set_json(&tenant, STATE_PREFIX, &state_key, None, &value, ttl_secs)
            })
            .await
            .map_err(map_state_error)
    }

    async fn del(&self, key: &SessionKey) -> GResult<()> {
        let tenant = tenant_ctx_from_key(key)?;
        let state_key = derive_state_key(key);
        self.store
            .call(move |store| store.del(&tenant, STATE_PREFIX, &state_key))
            .await
            .map_err(map_state_error)?;
        Ok(())
    }

    async fn del_prefix(&self, _key_prefix: &str) -> GResult<()> {
        // Deliberately NOT forwarded: `RedisStateStore::del_prefix` runs an
        // unbounded `SCAN MATCH` across the whole keyspace, which on a shared
        // Redis is a cost paid by every other tenant on the box. It used to
        // return `Ok(())`, which told the caller the keys were deleted when
        // they were not; it now reports the operation as unsupported so the
        // caller finds out. Pinned by `del_prefix_is_unsupported_and_never_reaches_the_backing_store`.
        Err(RunnerError::State {
            reason: "del_prefix is not supported by the durable state store".to_string(),
        })
    }
}

fn tenant_ctx_from_key(key: &SessionKey) -> GResult<TenantCtx> {
    let (env, tenant) = key
        .tenant_key
        .split_once("::")
        .ok_or_else(|| RunnerError::State {
            reason: format!("invalid tenant descriptor '{}'", key.tenant_key),
        })?;
    let env_id = EnvId::from_str(env).map_err(|err| RunnerError::State {
        reason: format!("invalid env id {env}: {err}"),
    })?;
    let tenant_id = TenantId::from_str(tenant).map_err(|err| RunnerError::State {
        reason: format!("invalid tenant id {tenant}: {err}"),
    })?;
    Ok(TenantCtx::new(env_id, tenant_id))
}

/// Escape `%` and `/` so a segment can never contain the key separator.
fn escape_segment(segment: &str) -> String {
    let mut out = String::with_capacity(segment.len());
    for ch in segment.chars() {
        match ch {
            '%' => out.push_str("%25"),
            '/' => out.push_str("%2F"),
            other => out.push(other),
        }
    }
    out
}

/// Injective key for one session's state.
///
/// Segments are escaped (`%` and `/`), so `pack/a/flow/b/session/x` cannot be
/// produced by a different `(pack, flow, hint)` triple. `-` is the marker for
/// "no hint"; a literal `-` hint is escaped to `%2D` so the two cannot alias.
/// Keys without `%`, `/` or a bare `-` hint are identical to the historical
/// format, so existing state stays addressable.
fn derive_state_key(key: &SessionKey) -> StoreStateKey {
    let hint = match key.session_hint.as_deref() {
        None => "-".to_string(),
        Some("-") => "%2D".to_string(),
        Some(hint) => escape_segment(hint),
    };
    StoreStateKey::from(format!(
        "pack/{}/flow/{}/session/{hint}",
        escape_segment(&key.pack_id),
        escape_segment(&key.flow_id),
    ))
}

fn map_state_error(err: greentic_types::GreenticError) -> RunnerError {
    RunnerError::State {
        reason: err.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use greentic_state::StatePath;
    use greentic_types::GResult as TypesResult;
    use std::sync::Mutex;

    /// Records every call that reaches the backing store.
    #[derive(Default)]
    struct RecordingStore {
        calls: Mutex<Vec<String>>,
    }

    impl RecordingStore {
        fn calls(&self) -> Vec<String> {
            self.calls.lock().map(|c| c.clone()).unwrap_or_default()
        }

        fn record(&self, call: &str) {
            if let Ok(mut calls) = self.calls.lock() {
                calls.push(call.to_string());
            }
        }
    }

    impl StateStore for RecordingStore {
        fn get_json(
            &self,
            _tenant: &TenantCtx,
            _prefix: &str,
            _key: &StoreStateKey,
            _path: Option<&StatePath>,
        ) -> TypesResult<Option<Value>> {
            self.record("get_json");
            Ok(None)
        }

        fn set_json(
            &self,
            _tenant: &TenantCtx,
            _prefix: &str,
            _key: &StoreStateKey,
            _path: Option<&StatePath>,
            _value: &Value,
            _ttl_secs: Option<u32>,
        ) -> TypesResult<()> {
            self.record("set_json");
            Ok(())
        }

        fn set_json_if_absent(
            &self,
            _tenant: &TenantCtx,
            _prefix: &str,
            _key: &StoreStateKey,
            _value: &Value,
            _ttl_secs: Option<u32>,
        ) -> TypesResult<bool> {
            self.record("set_json_if_absent");
            Ok(true)
        }

        fn del(
            &self,
            _tenant: &TenantCtx,
            _prefix: &str,
            _key: &StoreStateKey,
        ) -> TypesResult<bool> {
            self.record("del");
            Ok(true)
        }

        fn del_prefix(&self, _tenant: &TenantCtx, _prefix: &str) -> TypesResult<u64> {
            self.record("del_prefix");
            Ok(0)
        }
    }

    fn sample_key() -> SessionKey {
        SessionKey {
            tenant_key: "local::demo".into(),
            pack_id: "pack.demo".into(),
            flow_id: "flow.main".into(),
            session_hint: Some("hint".into()),
        }
    }

    /// `RedisStateStore::del_prefix` walks the WHOLE keyspace with an unbounded
    /// `SCAN MATCH`, a cost every other tenant on a shared Redis pays. Nothing
    /// in this crate needs prefix deletion, so the adapter refuses it (rather
    /// than pretending to succeed) without touching the store. If that ever
    /// changes it must be a decision, and this test is what forces it to be one.
    #[tokio::test]
    async fn del_prefix_is_unsupported_and_never_reaches_the_backing_store() {
        let store = Arc::new(RecordingStore::default());
        let host = StateStoreHost::new(store.clone());

        assert!(host.del_prefix("runner/pack").await.is_err());

        assert!(
            store.calls().is_empty(),
            "del_prefix forwarded to the backing store: {:?}",
            store.calls()
        );
    }

    /// The other three DO reach it — so the test above is about `del_prefix`
    /// and not about a host that forwards nothing.
    #[tokio::test]
    async fn the_other_operations_do_reach_the_backing_store() {
        let store = Arc::new(RecordingStore::default());
        let host = StateStoreHost::new(store.clone());
        let key = sample_key();

        host.set_json(&key, Value::Null).await.expect("set");
        host.get_json(&key).await.expect("get");
        host.del(&key).await.expect("del");

        assert_eq!(store.calls(), vec!["set_json", "get_json", "del"]);
    }

    #[test]
    fn state_keys_do_not_alias() {
        let mut a = sample_key();
        a.pack_id = "p/flow/f".into();
        a.flow_id = "x".into();
        let mut b = sample_key();
        b.pack_id = "p".into();
        b.flow_id = "f/session/h".into();
        b.session_hint = None;
        assert_ne!(derive_state_key(&a), derive_state_key(&b));

        let mut none = sample_key();
        none.session_hint = None;
        let mut dash = sample_key();
        dash.session_hint = Some("-".into());
        assert_ne!(derive_state_key(&none), derive_state_key(&dash));

        // Plain keys keep the historical shape.
        assert_eq!(
            derive_state_key(&sample_key()),
            StoreStateKey::from("pack/pack.demo/flow/flow.main/session/hint")
        );
    }

    #[derive(Default)]
    struct TtlRecorder(Mutex<Vec<Option<u32>>>);

    impl StateStore for TtlRecorder {
        fn get_json(
            &self,
            _t: &TenantCtx,
            _p: &str,
            _k: &StoreStateKey,
            _path: Option<&StatePath>,
        ) -> TypesResult<Option<Value>> {
            Ok(None)
        }
        fn set_json(
            &self,
            _t: &TenantCtx,
            _p: &str,
            _k: &StoreStateKey,
            _path: Option<&StatePath>,
            _v: &Value,
            ttl_secs: Option<u32>,
        ) -> TypesResult<()> {
            if let Ok(mut seen) = self.0.lock() {
                seen.push(ttl_secs);
            }
            Ok(())
        }
        fn set_json_if_absent(
            &self,
            _t: &TenantCtx,
            _p: &str,
            _k: &StoreStateKey,
            _v: &Value,
            _ttl: Option<u32>,
        ) -> TypesResult<bool> {
            Ok(true)
        }
        fn del(&self, _t: &TenantCtx, _p: &str, _k: &StoreStateKey) -> TypesResult<bool> {
            Ok(true)
        }
        fn del_prefix(&self, _t: &TenantCtx, _p: &str) -> TypesResult<u64> {
            Ok(0)
        }
    }

    #[tokio::test]
    async fn configured_ttl_reaches_set_json() {
        let store = Arc::new(TtlRecorder::default());
        let host = StateStoreHost::new(store.clone()).with_ttl_secs(Some(90));
        host.set_json(&sample_key(), Value::Null)
            .await
            .expect("set");
        let plain = StateStoreHost::new(store.clone());
        plain
            .set_json(&sample_key(), Value::Null)
            .await
            .expect("set");
        let zero = StateStoreHost::new(store.clone()).with_ttl_secs(Some(0));
        zero.set_json(&sample_key(), Value::Null)
            .await
            .expect("set");
        let seen = store.0.lock().map(|v| v.clone()).unwrap_or_default();
        assert_eq!(seen, vec![Some(90), None, None]);
    }

    /// The in-memory default must still be reachable with no configuration.
    #[test]
    fn the_default_backend_is_in_memory() {
        assert!(store_from_config(&StateBackend::InMemory).is_ok());
        assert!(!StateBackend::InMemory.is_durable());
    }
}
