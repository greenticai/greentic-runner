//! `AgentStateStore` over [`crate::kv::AwKv`]. Mirrors `state_redis.rs` key
//! formats and TTLs but works against any `AwKv` (memory or on-disk redb), so
//! the Agentic Worker runs without Redis.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use crate::error::StateError;
use crate::kv::AwKv;
use crate::state::{
    AgentStateStore, ConversationState, STATE_SCHEMA_VERSION, SessionLock, SessionLockInner,
};
use crate::tenant::TenantContext;

const STATE_TTL: Duration = Duration::from_secs(7 * 24 * 60 * 60); // 7 days
const LOCK_TTL: Duration = Duration::from_secs(90);
const LOCK_POLL: Duration = Duration::from_millis(50);

fn state_key(tenant: &TenantContext, session_id: &str) -> String {
    format!("{}:{session_id}:state", tenant.key_prefix())
}

fn lock_key(tenant: &TenantContext, session_id: &str) -> String {
    format!("{}:{session_id}:lock", tenant.key_prefix())
}

/// `AgentStateStore` backed by an [`AwKv`]. Cheap to clone the `Arc`.
pub struct KvAgentStateStore {
    kv: Arc<dyn AwKv>,
}

impl KvAgentStateStore {
    /// Wrap a shared key-value backend.
    pub fn new(kv: Arc<dyn AwKv>) -> Self {
        Self { kv }
    }
}

impl AgentStateStore for KvAgentStateStore {
    fn load<'a>(
        &'a self,
        tenant: &'a TenantContext,
        session_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<ConversationState, StateError>> + Send + 'a>> {
        Box::pin(async move {
            let key = state_key(tenant, session_id);
            let Some(bytes) = self.kv.get(&key).await? else {
                return Ok(ConversationState::empty(tenant, session_id));
            };
            let state: ConversationState = serde_json::from_slice(&bytes)
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
            let key = state_key(tenant, session_id);
            let bytes = serde_json::to_vec(state)
                .map_err(|e| StateError::Decode(format!("state json: {e}")))?;
            self.kv.set_ex(&key, bytes, STATE_TTL).await
        })
    }

    fn acquire_lock<'a>(
        &'a self,
        tenant: &'a TenantContext,
        session_id: &'a str,
        wait: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<SessionLock, StateError>> + Send + 'a>> {
        Box::pin(async move {
            let key = lock_key(tenant, session_id);
            let token = uuid::Uuid::new_v4().to_string();
            let deadline = std::time::Instant::now() + wait;
            loop {
                if self
                    .kv
                    .set_nx(&key, token.clone().into_bytes(), LOCK_TTL)
                    .await?
                {
                    let inner = KvSessionLock {
                        kv: self.kv.clone(),
                        key,
                        token,
                    };
                    return Ok(SessionLock::new(Box::new(inner)));
                }
                if std::time::Instant::now() >= deadline {
                    return Err(StateError::LockTimeout(wait));
                }
                tokio::time::sleep(LOCK_POLL).await;
            }
        })
    }
}

struct KvSessionLock {
    kv: Arc<dyn AwKv>,
    key: String,
    token: String,
}

impl SessionLockInner for KvSessionLock {
    fn refresh<'a>(&'a self) -> Pin<Box<dyn Future<Output = Result<(), StateError>> + Send + 'a>> {
        Box::pin(async move {
            let ok = self
                .kv
                .compare_refresh(&self.key, self.token.as_bytes(), LOCK_TTL)
                .await?;
            if ok {
                return Ok(());
            }
            // Absent (the TTL lapsed) or held by another owner. An absent key
            // does NOT mean nobody ran in the gap (another turn may have
            // taken it, saved and released), so it is never re-acquired here:
            // the step aborts without saving its stale state.
            Err(StateError::LockLost)
        })
    }

    fn release(&self) {
        // Drop cannot be async. Spawn a detached compare-del when a Tokio
        // runtime is available; otherwise the 90s TTL is the safety net.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let kv = self.kv.clone();
            let key = self.key.clone();
            let token = self.token.clone().into_bytes();
            handle.spawn(async move {
                let _ = kv.compare_del(&key, &token).await;
            });
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::kv::MemoryKv;
    use crate::state::ChatMessage;
    use std::time::Duration;

    fn store() -> KvAgentStateStore {
        KvAgentStateStore::new(Arc::new(MemoryKv::new()))
    }

    #[tokio::test]
    async fn load_missing_returns_empty_state() {
        let s = store();
        let t = TenantContext::new("acme", "prod");
        let state = s.load(&t, "sess").await.unwrap();
        assert_eq!(state.schema_version, STATE_SCHEMA_VERSION);
        assert!(state.messages.is_empty());
    }

    #[tokio::test]
    async fn save_then_load_roundtrips_messages() {
        let s = store();
        let t = TenantContext::new("acme", "prod");
        let mut state = ConversationState::empty(&t, "sess");
        state.messages.push(ChatMessage::User {
            content: "hi".into(),
            attachments: Vec::new(),
        });
        s.save(&t, "sess", &state).await.unwrap();
        let loaded = s.load(&t, "sess").await.unwrap();
        assert_eq!(loaded.messages.len(), 1);
    }

    #[tokio::test]
    async fn second_lock_times_out_while_first_held() {
        let s = store();
        let t = TenantContext::new("acme", "prod");
        let _held = s
            .acquire_lock(&t, "sess", Duration::from_millis(10))
            .await
            .unwrap();
        let err = s.acquire_lock(&t, "sess", Duration::from_millis(20)).await;
        assert!(matches!(err, Err(StateError::LockTimeout(_))));
    }

    #[tokio::test]
    async fn refresh_reports_lock_lost_when_the_lock_lapsed() {
        let kv: Arc<dyn AwKv> = Arc::new(MemoryKv::new());
        let s = KvAgentStateStore::new(kv.clone());
        let t = TenantContext::new("acme", "prod");
        let held = s
            .acquire_lock(&t, "sess", Duration::from_millis(10))
            .await
            .unwrap();
        // Simulate the TTL lapsing with nobody else having taken it.
        kv.del(&lock_key(&t, "sess")).await.unwrap();
        assert!(matches!(held.refresh().await, Err(StateError::LockLost)));
        // The lock was not silently re-created.
        assert!(kv.get(&lock_key(&t, "sess")).await.unwrap().is_none());
    }

    /// Stale-holder interleaving: A's lock lapses, B takes it, saves newer
    /// state and releases; A's refresh must report the loss (so the loop
    /// aborts before its stale save) instead of re-taking the free key.
    #[tokio::test]
    async fn lapsed_holder_cannot_retake_after_another_turn_ran() {
        let kv: Arc<dyn AwKv> = Arc::new(MemoryKv::new());
        let s = KvAgentStateStore::new(kv.clone());
        let t = TenantContext::new("acme", "prod");
        let a = s
            .acquire_lock(&t, "sess", Duration::from_millis(10))
            .await
            .unwrap();
        kv.del(&lock_key(&t, "sess")).await.unwrap();
        {
            let b = s
                .acquire_lock(&t, "sess", Duration::from_millis(10))
                .await
                .unwrap();
            let mut newer = ConversationState::empty(&t, "sess");
            newer.messages.push(ChatMessage::User {
                content: "newer".into(),
                attachments: Vec::new(),
            });
            s.save(&t, "sess", &newer).await.unwrap();
            drop(b);
        }
        // Let B's detached release run.
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(matches!(a.refresh().await, Err(StateError::LockLost)));
    }

    #[tokio::test]
    async fn refresh_reports_lock_lost_when_another_owner_holds_it() {
        let kv: Arc<dyn AwKv> = Arc::new(MemoryKv::new());
        let s = KvAgentStateStore::new(kv.clone());
        let t = TenantContext::new("acme", "prod");
        let held = s
            .acquire_lock(&t, "sess", Duration::from_millis(10))
            .await
            .unwrap();
        kv.del(&lock_key(&t, "sess")).await.unwrap();
        let _other = s
            .acquire_lock(&t, "sess", Duration::from_millis(10))
            .await
            .unwrap();
        assert!(matches!(held.refresh().await, Err(StateError::LockLost)));
    }

    #[tokio::test]
    async fn lock_released_on_drop_allows_reacquire() {
        let s = store();
        let t = TenantContext::new("acme", "prod");
        {
            let _held = s
                .acquire_lock(&t, "sess", Duration::from_millis(10))
                .await
                .unwrap();
        }
        // After drop the lock key is gone; re-acquire succeeds immediately.
        let _again = s
            .acquire_lock(&t, "sess", Duration::from_millis(10))
            .await
            .unwrap();
    }
}
