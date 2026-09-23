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
}

impl StateStoreHost {
    pub fn new(store: DynStateStore) -> Self {
        Self {
            store: Offloaded::new(store, StoreKind::State),
        }
    }
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
    Arc::new(StateStoreHost::new(store))
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
        self.store
            .call(move |store| {
                store.set_json(&tenant, STATE_PREFIX, &state_key, None, &value, None)
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
        // Deliberately a no-op, and it must stay one until someone decides
        // otherwise on purpose. `RedisStateStore::del_prefix` runs an unbounded
        // `SCAN MATCH` across the whole keyspace, which on a shared Redis is a
        // cost paid by every other tenant on the box. Nothing in this crate
        // needs prefix deletion; forwarding would make that cost arrive by
        // accident. Pinned by `del_prefix_never_reaches_the_backing_store`.
        Ok(())
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

fn derive_state_key(key: &SessionKey) -> StoreStateKey {
    let hint = key.session_hint.as_deref().unwrap_or("-");
    StoreStateKey::from(format!(
        "pack/{}/flow/{}/session/{hint}",
        key.pack_id, key.flow_id
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
    /// in this crate needs prefix deletion, so the adapter answers it without
    /// touching the store. If that ever changes it must be a decision, and this
    /// test is what forces it to be one.
    #[tokio::test]
    async fn del_prefix_never_reaches_the_backing_store() {
        let store = Arc::new(RecordingStore::default());
        let host = StateStoreHost::new(store.clone());

        host.del_prefix("runner/pack").await.expect("del_prefix");

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

    /// The in-memory default must still be reachable with no configuration.
    #[test]
    fn the_default_backend_is_in_memory() {
        assert!(store_from_config(&StateBackend::InMemory).is_ok());
        assert!(!StateBackend::InMemory.is_durable());
    }
}
