//! Redis-backed `AgentStateStore` impl using `redis::aio::ConnectionManager`.
//!
//! Shares the Redis instance with `greentic-state` (flow state) but uses
//! the distinct `aw:*` key namespace (see spec §5.5). `greentic-state`
//! itself is sync-only; the AW runtime talks to Redis directly via the
//! `redis` crate's async `ConnectionManager` (cheap to clone, multiplexed,
//! auto-reconnecting).

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use redis::AsyncCommands;
use redis::aio::ConnectionManager;

use crate::error::StateError;
use crate::state::{
    AgentStateStore, ConversationState, STATE_SCHEMA_VERSION, SessionLock, SessionLockInner,
};
use crate::tenant::TenantContext;

const STATE_TTL_SECS: u64 = 7 * 24 * 60 * 60; // 7 days
const LOCK_TTL_SECS: i64 = 90;
const LOCK_POLL_MS: u64 = 50;

/// Lua: refresh the lock TTL only if we still own it (value matches). A
/// missing key or a different owner both return 0 (lock lost): the lock is
/// never re-created, because a lapsed lock may have been taken, used and
/// released by another turn whose newer state our stale copy would overwrite.
const REFRESH_LUA: &str = r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('EXPIRE', KEYS[1], ARGV[2])
else
  return 0
end
"#;

/// Lua: delete the lock only if we still own it (value matches).
const RELEASE_LUA: &str = r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
else
  return 0
end
"#;

/// Production state store backed by a multiplexed `ConnectionManager`.
///
/// Shares the Redis instance with `greentic-state` but stays in the
/// `aw:*` namespace. The manager is `Clone` (cheap, reference-counted)
/// so per-call clones are intentional and free of new connections.
pub struct RedisAgentStateStore {
    manager: ConnectionManager,
}

impl RedisAgentStateStore {
    /// Wrap an already-established connection manager.
    pub fn new(manager: ConnectionManager) -> Self {
        Self { manager }
    }

    /// Open a client at `url` and establish a multiplexed connection.
    pub async fn connect(url: &str) -> Result<Self, StateError> {
        let client =
            redis::Client::open(url).map_err(|e| StateError::Redis(format!("open: {e}")))?;
        let manager = ConnectionManager::new(client)
            .await
            .map_err(|e| StateError::Redis(format!("connect: {e}")))?;
        Ok(Self { manager })
    }

    /// Expose a clone of the connection manager so Phase 3's token meter
    /// + idempotency ledger can share the same multiplexed connection.
    pub fn manager(&self) -> ConnectionManager {
        self.manager.clone()
    }

    fn state_key(tenant: &TenantContext, session_id: &str) -> String {
        format!("{}:{session_id}:state", tenant.key_prefix())
    }

    fn lock_key(tenant: &TenantContext, session_id: &str) -> String {
        format!("{}:{session_id}:lock", tenant.key_prefix())
    }
}

impl AgentStateStore for RedisAgentStateStore {
    fn load<'a>(
        &'a self,
        tenant: &'a TenantContext,
        session_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<ConversationState, StateError>> + Send + 'a>> {
        Box::pin(async move {
            let key = Self::state_key(tenant, session_id);
            let mut conn = self.manager.clone();
            let raw: Option<String> = conn
                .get(&key)
                .await
                .map_err(|e| StateError::Redis(format!("get: {e}")))?;
            let Some(json) = raw else {
                return Ok(ConversationState::empty(tenant, session_id));
            };
            let state: ConversationState = serde_json::from_str(&json)
                .map_err(|e| StateError::Decode(format!("state json: {e}")))?;
            if state.schema_version > STATE_SCHEMA_VERSION {
                return Err(StateError::SchemaIncompatible {
                    found: state.schema_version,
                    supported: STATE_SCHEMA_VERSION,
                });
            }
            Ok(state)
        })
    }

    fn save<'a>(
        &'a self,
        tenant: &'a TenantContext,
        session_id: &'a str,
        state: &'a ConversationState,
    ) -> Pin<Box<dyn Future<Output = Result<(), StateError>> + Send + 'a>> {
        Box::pin(async move {
            let key = Self::state_key(tenant, session_id);
            let json = serde_json::to_string(state)
                .map_err(|e| StateError::Decode(format!("state json: {e}")))?;
            let mut conn = self.manager.clone();
            let _: () = conn
                .set_ex(&key, json, STATE_TTL_SECS)
                .await
                .map_err(|e| StateError::Redis(format!("set_ex: {e}")))?;
            Ok(())
        })
    }

    fn acquire_lock<'a>(
        &'a self,
        tenant: &'a TenantContext,
        session_id: &'a str,
        wait: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<SessionLock, StateError>> + Send + 'a>> {
        Box::pin(async move {
            let key = Self::lock_key(tenant, session_id);
            let value = uuid::Uuid::new_v4().to_string();
            let deadline = std::time::Instant::now() + wait;
            loop {
                let mut conn = self.manager.clone();
                let res: Option<String> = redis::cmd("SET")
                    .arg(&key)
                    .arg(&value)
                    .arg("NX")
                    .arg("EX")
                    .arg(LOCK_TTL_SECS)
                    .query_async(&mut conn)
                    .await
                    .map_err(|e| StateError::Redis(format!("set nx: {e}")))?;
                if res.is_some() {
                    let inner = RedisSessionLock {
                        manager: self.manager.clone(),
                        key,
                        value,
                    };
                    return Ok(SessionLock::new(Box::new(inner)));
                }
                if std::time::Instant::now() >= deadline {
                    return Err(StateError::LockTimeout(wait));
                }
                tokio::time::sleep(Duration::from_millis(LOCK_POLL_MS)).await;
            }
        })
    }
}

struct RedisSessionLock {
    manager: ConnectionManager,
    key: String,
    value: String,
}

impl SessionLockInner for RedisSessionLock {
    fn refresh<'a>(&'a self) -> Pin<Box<dyn Future<Output = Result<(), StateError>> + Send + 'a>> {
        Box::pin(async move {
            let mut conn = self.manager.clone();
            let refreshed: i64 = redis::Script::new(REFRESH_LUA)
                .key(&self.key)
                .arg(&self.value)
                .arg(LOCK_TTL_SECS)
                .invoke_async(&mut conn)
                .await
                .map_err(|e| StateError::Redis(format!("refresh eval: {e}")))?;
            if refreshed == 1 {
                return Ok(());
            }
            // Absent (the TTL lapsed) or held by another owner. Both are a
            // lost lock: an absent key does NOT mean nobody ran in the gap
            // (another turn may have taken it, saved and released), so it is
            // never re-acquired here and the step aborts without saving.
            Err(StateError::LockLost)
        })
    }

    fn release(&self) {
        // Drop cannot be async. Spawn a detached task to run the
        // check-and-delete Lua if a Tokio runtime is available;
        // otherwise rely on the 90s TTL safety net.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let mut conn = self.manager.clone();
            let key = self.key.clone();
            let value = self.value.clone();
            handle.spawn(async move {
                let _: Result<i64, _> = redis::Script::new(RELEASE_LUA)
                    .key(&key)
                    .arg(&value)
                    .invoke_async(&mut conn)
                    .await;
            });
        }
    }
}
