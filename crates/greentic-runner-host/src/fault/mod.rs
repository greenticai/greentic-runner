use std::collections::HashMap;
use std::env;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use anyhow::{Result, anyhow};
use greentic_state::{StateKey, StatePath, StateStore};
use greentic_types::{GResult, TenantCtx};
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use serde_json::Value;

use crate::storage::state::DynStateStore;

#[derive(Clone, Debug)]
pub struct FaultConfig {
    pub drop_state_write: bool,
    pub delay_state_read_ms: u64,
    pub stale_state_read: bool,
    pub asset_transient: Option<FaultRate>,
    pub asset_delay_ms: u64,
    pub seed: u64,
}

#[derive(Copy, Clone, Debug)]
pub struct FaultRate {
    pub numerator: u64,
    pub denominator: u64,
}

#[derive(Default)]
struct FaultCounters {
    state_reads: std::sync::atomic::AtomicU64,
    asset_ops: std::sync::atomic::AtomicU64,
}

struct FaultState {
    config: FaultConfig,
    counters: FaultCounters,
}

static FAULT: Lazy<Mutex<Option<Arc<FaultState>>>> = Lazy::new(|| Mutex::new(None));

fn fault_state() -> Option<Arc<FaultState>> {
    let mut guard = FAULT.lock();
    if let Some(state) = guard.as_ref() {
        return Some(Arc::clone(state));
    }
    let config = FaultConfig::from_env();
    if !config.is_enabled() {
        return None;
    }
    let state = Arc::new(FaultState {
        config,
        counters: FaultCounters::default(),
    });
    *guard = Some(Arc::clone(&state));
    Some(state)
}

impl FaultConfig {
    pub fn from_env() -> Self {
        Self {
            drop_state_write: env::var("GREENTIC_FAIL_DROP_STATE_WRITE").ok().as_deref()
                == Some("1"),
            delay_state_read_ms: parse_u64("GREENTIC_FAIL_DELAY_STATE_READ_MS"),
            stale_state_read: env::var("GREENTIC_FAIL_STATE_STALE_READ").ok().as_deref()
                == Some("1"),
            asset_transient: parse_rate(env::var("GREENTIC_FAIL_ASSET_TRANSIENT").ok()),
            asset_delay_ms: parse_u64("GREENTIC_FAIL_ASSET_DELAY_MS"),
            seed: parse_u64_with_default("GREENTIC_FAIL_SEED", 0),
        }
    }

    fn is_enabled(&self) -> bool {
        self.drop_state_write
            || self.delay_state_read_ms > 0
            || self.stale_state_read
            || self.asset_transient.is_some()
            || self.asset_delay_ms > 0
    }
}

pub fn wrap_state_store(store: DynStateStore) -> DynStateStore {
    match fault_state() {
        Some(state) => Arc::new(FaultyStateStore::new(store, state)),
        None => store,
    }
}

pub async fn maybe_fail_asset(reference: &str) -> Result<()> {
    let Some(state) = fault_state() else {
        return Ok(());
    };
    if state.config.asset_delay_ms > 0 {
        tokio::time::sleep(Duration::from_millis(state.config.asset_delay_ms)).await;
    }
    let Some(rate) = state.config.asset_transient else {
        return Ok(());
    };
    let step = state
        .counters
        .asset_ops
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    if should_fail(state.config.seed, step, rate) {
        return Err(anyhow!(
            "fault injection: transient asset failure for {reference}"
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, Eq)]
struct CacheKey {
    tenant: TenantCtx,
    prefix: String,
    key: StateKey,
}

impl PartialEq for CacheKey {
    fn eq(&self, other: &Self) -> bool {
        self.tenant == other.tenant && self.prefix == other.prefix && self.key == other.key
    }
}

impl Hash for CacheKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.tenant.hash(state);
        self.prefix.hash(state);
        self.key.hash(state);
    }
}

struct FaultyStateStore {
    inner: DynStateStore,
    state: Arc<FaultState>,
    cache: Mutex<HashMap<CacheKey, Value>>,
}

impl FaultyStateStore {
    fn new(inner: DynStateStore, state: Arc<FaultState>) -> Self {
        Self {
            inner,
            state,
            cache: Mutex::new(HashMap::new()),
        }
    }

    fn key(&self, tenant: &TenantCtx, prefix: &str, key: &StateKey) -> CacheKey {
        CacheKey {
            tenant: tenant.clone(),
            prefix: prefix.to_string(),
            key: key.clone(),
        }
    }

    fn maybe_delay_read(&self) {
        let delay = self.state.config.delay_state_read_ms;
        if delay > 0 {
            thread::sleep(Duration::from_millis(delay));
        }
    }
}

impl StateStore for FaultyStateStore {
    fn get_json(
        &self,
        tenant: &TenantCtx,
        prefix: &str,
        key: &StateKey,
        path: Option<&StatePath>,
    ) -> GResult<Option<Value>> {
        self.maybe_delay_read();
        let _ = self
            .state
            .counters
            .state_reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if self.state.config.stale_state_read {
            let cache_key = self.key(tenant, prefix, key);
            if let Some(value) = self.cache.lock().get(&cache_key).cloned() {
                return Ok(Some(value));
            }
        }
        let value = self.inner.get_json(tenant, prefix, key, path)?;
        if self.state.config.stale_state_read
            && let Some(ref value) = value
        {
            let cache_key = self.key(tenant, prefix, key);
            self.cache.lock().insert(cache_key, value.clone());
        }
        Ok(value)
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
        if self.state.config.drop_state_write {
            return Ok(());
        }
        let result = self
            .inner
            .set_json(tenant, prefix, key, path, value, ttl_secs);
        if self.state.config.stale_state_read && result.is_ok() && path.is_none() {
            let cache_key = self.key(tenant, prefix, key);
            self.cache.lock().insert(cache_key, value.clone());
        }
        result
    }

    fn set_json_if_absent(
        &self,
        tenant: &TenantCtx,
        prefix: &str,
        key: &StateKey,
        value: &Value,
        ttl_secs: Option<u32>,
    ) -> GResult<bool> {
        // Like `set_json`, a dropped write is acknowledged without touching the
        // inner store (reported as "created").
        if self.state.config.drop_state_write {
            return Ok(true);
        }
        let created = self
            .inner
            .set_json_if_absent(tenant, prefix, key, value, ttl_secs)?;
        if created && self.state.config.stale_state_read {
            let cache_key = self.key(tenant, prefix, key);
            self.cache.lock().insert(cache_key, value.clone());
        }
        Ok(created)
    }

    fn del(&self, tenant: &TenantCtx, prefix: &str, key: &StateKey) -> GResult<bool> {
        if self.state.config.drop_state_write {
            return Ok(true);
        }
        let result = self.inner.del(tenant, prefix, key);
        if self.state.config.stale_state_read {
            let cache_key = self.key(tenant, prefix, key);
            self.cache.lock().remove(&cache_key);
        }
        result
    }

    fn del_prefix(&self, tenant: &TenantCtx, prefix: &str) -> GResult<u64> {
        if self.state.config.drop_state_write {
            return Ok(0);
        }
        let result = self.inner.del_prefix(tenant, prefix);
        if self.state.config.stale_state_read {
            self.cache
                .lock()
                .retain(|key, _| key.tenant != *tenant || key.prefix != prefix);
        }
        result
    }
}

fn should_fail(seed: u64, step: u64, rate: FaultRate) -> bool {
    if rate.denominator == 0 || rate.numerator == 0 {
        return false;
    }
    let value = splitmix64(seed.wrapping_add(step));
    (value % rate.denominator) < rate.numerator
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = value;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

fn parse_rate(raw: Option<String>) -> Option<FaultRate> {
    let raw = raw?;
    let (num, den) = raw.split_once('/')?;
    let numerator = num.trim().parse::<u64>().ok()?;
    let denominator = den.trim().parse::<u64>().ok()?;
    if denominator == 0 {
        return None;
    }
    Some(FaultRate {
        numerator,
        denominator,
    })
}

fn parse_u64(key: &str) -> u64 {
    parse_u64_with_default(key, 0)
}

fn parse_u64_with_default(key: &str, default_value: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default_value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_parses_ratio() {
        let rate = parse_rate(Some("2/5".to_string())).expect("rate");
        assert_eq!(rate.numerator, 2);
        assert_eq!(rate.denominator, 5);
    }

    fn faulty(drop_state_write: bool, stale_state_read: bool) -> FaultyStateStore {
        let state = Arc::new(FaultState {
            config: FaultConfig {
                drop_state_write,
                delay_state_read_ms: 0,
                stale_state_read,
                asset_transient: None,
                asset_delay_ms: 0,
                seed: 0,
            },
            counters: FaultCounters::default(),
        });
        FaultyStateStore::new(
            Arc::new(greentic_state::inmemory::InMemoryStateStore::new()),
            state,
        )
    }

    fn ctx() -> TenantCtx {
        use std::str::FromStr;
        TenantCtx::new(
            greentic_types::EnvId::from_str("local").expect("env"),
            greentic_types::TenantId::from_str("demo").expect("tenant"),
        )
    }

    #[test]
    fn if_absent_forwards_to_the_inner_atomic_store() {
        let store = faulty(false, false);
        let key = StateKey::new("k");
        let first = serde_json::json!("one");
        let second = serde_json::json!("two");
        assert!(
            store
                .set_json_if_absent(&ctx(), "p", &key, &first, None)
                .expect("first")
        );
        assert!(
            !store
                .set_json_if_absent(&ctx(), "p", &key, &second, None)
                .expect("second")
        );
        let stored = store.get_json(&ctx(), "p", &key, None).expect("get");
        assert_eq!(stored, Some(first));
    }

    #[test]
    fn if_absent_honours_drop_state_write_without_writing() {
        let store = faulty(true, false);
        let key = StateKey::new("k");
        let created = store
            .set_json_if_absent(&ctx(), "p", &key, &serde_json::json!(1), None)
            .expect("acknowledged");
        assert!(created, "a dropped write is acknowledged like set_json");
        assert_eq!(store.get_json(&ctx(), "p", &key, None).expect("get"), None);
    }

    #[test]
    fn if_absent_updates_the_stale_read_cache_only_when_created() {
        let store = faulty(false, true);
        let key = StateKey::new("k");
        assert!(
            store
                .set_json_if_absent(&ctx(), "p", &key, &serde_json::json!("a"), None)
                .expect("first")
        );
        assert!(
            !store
                .set_json_if_absent(&ctx(), "p", &key, &serde_json::json!("b"), None)
                .expect("second")
        );
        let cached = store.get_json(&ctx(), "p", &key, None).expect("get");
        assert_eq!(cached, Some(serde_json::json!("a")));
    }

    #[test]
    fn splitmix_is_deterministic() {
        let first = splitmix64(42);
        let second = splitmix64(42);
        assert_eq!(first, second);
    }
}
