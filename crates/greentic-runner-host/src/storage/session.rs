use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use greentic_session::inmemory::InMemorySessionStore;
use greentic_session::{SessionData, SessionKey as StoreSessionKey, SessionStore};
use greentic_types::{
    EnvId, FlowId, GreenticError, PackId, SessionCursor as TypesSessionCursor, TenantCtx, TenantId,
    UserId,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::engine::error::{GResult, RunnerError};
use crate::engine::host::{
    OutboxKey, SessionCursor, SessionHost, SessionKey, SessionOutboxEntry, SessionSnapshot,
    WaitState,
};
use crate::storage::config::SessionBackend;
use crate::storage::offload::{Offloaded, StoreKind};

pub type DynSessionStore = Arc<dyn SessionStore>;

/// Adapter that backs the runner session host with a greentic-session store.
///
/// The store is held behind [`Offloaded`], so every call reaches it through
/// `spawn_blocking` — `SessionStore` is a synchronous trait and its Redis
/// implementation blocks on a socket.
pub struct SessionStoreHost {
    store: Offloaded<dyn SessionStore>,
}

impl SessionStoreHost {
    pub fn new(store: DynSessionStore) -> Self {
        Self {
            store: Offloaded::new(store, StoreKind::Session),
        }
    }

    async fn lookup_entry(&self, key: &SessionKey) -> GResult<Option<StoreEntry>> {
        let base_ctx = tenant_ctx_from_key(key)?;
        let user = user_id_from_key(key)?;
        let ctx = base_ctx.clone().with_user(Some(user.clone()));
        let result = {
            let ctx = ctx.clone();
            let user = user.clone();
            self.store
                .call(move |store| {
                    #[allow(deprecated)]
                    store.find_by_user(&ctx, &user)
                })
                .await
                .map_err(map_store_error)?
        };
        if let Some((store_key, data)) = result {
            let snapshot = decode_snapshot(&data)?;
            if snapshot.key.tenant_key != key.tenant_key
                || snapshot.key.pack_id != key.pack_id
                || snapshot.key.flow_id != key.flow_id
                || snapshot.key.session_hint != key.session_hint
            {
                return Ok(None);
            }
            Ok(Some(StoreEntry {
                key: store_key,
                snapshot,
                ctx,
                user,
            }))
        } else {
            Ok(None)
        }
    }

    async fn upsert(
        &self,
        snapshot: &SessionSnapshot,
        ctx: TenantCtx,
        user: &UserId,
    ) -> GResult<StoreSessionKey> {
        let data = encode_snapshot(snapshot, ctx.clone(), user)?;
        let user = user.clone();
        self.store
            .call(move |store| {
                #[allow(deprecated)]
                let existing = store.find_by_user(&ctx, &user)?;
                match existing {
                    Some((store_key, _)) => {
                        store.update_session(&store_key, data)?;
                        Ok(store_key)
                    }
                    None => store.create_session(&ctx, data),
                }
            })
            .await
            .map_err(map_store_error)
    }
}

struct StoreEntry {
    key: StoreSessionKey,
    snapshot: SessionSnapshot,
    ctx: TenantCtx,
    user: UserId,
}

pub fn new_session_store() -> DynSessionStore {
    Arc::new(InMemorySessionStore::new())
}

/// Build the session store named by `backend`.
///
/// The Redis arm goes through `greentic_session::create_session_store` rather
/// than naming `RedisSessionStore`: that type lives in a private `backends`
/// module, so the factory is the only public door to it.
pub fn store_from_config(backend: &SessionBackend) -> Result<DynSessionStore> {
    match backend {
        SessionBackend::InMemory => Ok(new_session_store()),
        SessionBackend::Redis { url, namespace } => redis_store(url, namespace),
    }
}

#[cfg(feature = "session-redis")]
fn redis_store(url: &str, namespace: &str) -> Result<DynSessionStore> {
    use anyhow::Context;
    use greentic_session::{SessionBackendConfig, create_session_store};

    let store = create_session_store(SessionBackendConfig::RedisUrlWithNamespace {
        url: url.to_string(),
        namespace: namespace.to_string(),
    })
    .map_err(|err| anyhow!("{err}"))
    .with_context(|| format!("failed to open the Redis session store under '{namespace}'"))?;
    let store: DynSessionStore = Arc::from(store);
    probe(&store, namespace)?;
    Ok(store)
}

#[cfg(not(feature = "session-redis"))]
fn redis_store(_url: &str, namespace: &str) -> Result<DynSessionStore> {
    // Refuse rather than fall back: a host asked for durable sessions and this
    // build cannot provide them, so starting with in-memory ones would be a
    // deployment that silently restarts every conversation.
    Err(anyhow!(
        "a Redis session store was configured (namespace '{namespace}') but this build of          greentic-runner-host lacks the `session-redis` feature; rebuild with it enabled          rather than starting on in-memory sessions"
    ))
}

/// Force a connection so an unreachable backend fails here rather than in front
/// of a user.
///
/// `redis::Client::open` parses the URL and opens no socket, so construction
/// alone proves nothing. This reads one key that is never written; both a hit
/// and a miss are success, and only a transport failure is an error.
#[cfg(feature = "session-redis")]
fn probe(store: &DynSessionStore, namespace: &str) -> Result<()> {
    use anyhow::Context;

    let probe_key = StoreSessionKey::new("greentic-runner-host::startup-probe");
    store
        .get_session(&probe_key)
        .map(|_| ())
        .map_err(|err| anyhow!("{err}"))
        .with_context(|| {
            format!("the configured Redis session store (namespace '{namespace}') is unreachable")
        })
}

pub fn session_host_from(store: DynSessionStore) -> Arc<dyn SessionHost> {
    Arc::new(SessionStoreHost::new(store))
}

#[async_trait]
impl SessionHost for SessionStoreHost {
    async fn get(&self, key: &SessionKey) -> GResult<Option<SessionSnapshot>> {
        Ok(self.lookup_entry(key).await?.map(|entry| entry.snapshot))
    }

    async fn put(&self, snapshot: SessionSnapshot) -> GResult<()> {
        let base_ctx = tenant_ctx_from_key(&snapshot.key)?;
        let user = user_id_from_key(&snapshot.key)?;
        let ctx = base_ctx.with_user(Some(user.clone()));
        self.upsert(&snapshot, ctx, &user).await?;
        Ok(())
    }

    async fn update_cas(
        &self,
        mut snapshot: SessionSnapshot,
        expected_revision: u64,
    ) -> GResult<bool> {
        let Some(entry) = self.lookup_entry(&snapshot.key).await? else {
            return Ok(false);
        };
        if entry.snapshot.revision != expected_revision {
            return Ok(false);
        }
        snapshot.revision = expected_revision.saturating_add(1);
        self.upsert(&snapshot, entry.ctx, &entry.user).await?;
        Ok(true)
    }

    async fn delete(&self, key: &SessionKey) -> GResult<()> {
        if let Some(entry) = self.lookup_entry(key).await? {
            self.store
                .call(move |store| store.remove_session(&entry.key))
                .await
                .map_err(map_store_error)?;
        }
        Ok(())
    }

    async fn touch(&self, key: &SessionKey, ttl: Duration) -> GResult<()> {
        if let Some(mut entry) = self.lookup_entry(key).await? {
            entry.snapshot.ttl = ttl;
            self.upsert(&entry.snapshot, entry.ctx, &entry.user).await?;
        }
        Ok(())
    }
}

fn encode_snapshot(
    snapshot: &SessionSnapshot,
    mut ctx: TenantCtx,
    user: &UserId,
) -> GResult<SessionData> {
    let flow_id = FlowId::from_str(snapshot.key.flow_id.as_str()).map_err(map_store_error)?;
    let pack_id = PackId::from_str(snapshot.key.pack_id.as_str()).map_err(map_store_error)?;
    ctx = ctx
        .with_flow(snapshot.key.flow_id.clone())
        .with_session(snapshot.session_id.clone());
    let cursor = TypesSessionCursor {
        node_pointer: format!("pos-{}", snapshot.cursor.position),
        wait_reason: snapshot.waiting.as_ref().map(|wait| wait.reason.clone()),
        outbox_marker: Some(snapshot.cursor.outbox_seq.to_string()),
    };
    let payload = PersistedSnapshot::from(snapshot);
    let context_json = serde_json::to_string(&payload).map_err(|err| RunnerError::Session {
        reason: format!("failed to encode session snapshot: {err}"),
    })?;
    Ok(SessionData {
        tenant_ctx: ctx.with_user(Some(user.clone())),
        flow_id,
        pack_id: Some(pack_id),
        cursor,
        context_json,
    })
}

fn decode_snapshot(data: &SessionData) -> GResult<SessionSnapshot> {
    let stored: PersistedSnapshot =
        serde_json::from_str(&data.context_json).map_err(|err| RunnerError::Session {
            reason: format!("failed to decode session snapshot: {err}"),
        })?;
    Ok(stored.into())
}

fn tenant_ctx_from_key(key: &SessionKey) -> GResult<TenantCtx> {
    let (env, tenant) = key
        .tenant_key
        .split_once("::")
        .ok_or_else(|| RunnerError::Session {
            reason: format!("invalid tenant descriptor '{}'", key.tenant_key),
        })?;
    let env_id = EnvId::from_str(env).map_err(map_store_error)?;
    let tenant_id = TenantId::from_str(tenant).map_err(map_store_error)?;
    Ok(TenantCtx::new(env_id, tenant_id))
}

fn user_id_from_key(key: &SessionKey) -> GResult<UserId> {
    let hint = key
        .session_hint
        .clone()
        .unwrap_or_else(|| format!("{}::{}::{}", key.tenant_key, key.pack_id, key.flow_id));
    let digest = Sha256::digest(hint.as_bytes());
    let slug = format!("sess{}", hex::encode(&digest[..8]));
    UserId::from_str(&slug).map_err(map_store_error)
}

fn map_store_error(err: GreenticError) -> RunnerError {
    RunnerError::Session {
        reason: err.to_string(),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PersistedSnapshot {
    key: SessionKey,
    session_id: String,
    revision: u64,
    cursor: SessionCursor,
    state: serde_json::Value,
    #[serde(default)]
    outbox: Vec<PersistedOutboxEntry>,
    waiting: Option<WaitState>,
    last_outcome: Option<serde_json::Value>,
    ttl_secs: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PersistedOutboxEntry {
    key: OutboxKey,
    entry: SessionOutboxEntry,
}

impl From<&SessionSnapshot> for PersistedSnapshot {
    fn from(snapshot: &SessionSnapshot) -> Self {
        Self {
            key: snapshot.key.clone(),
            session_id: snapshot.session_id.clone(),
            revision: snapshot.revision,
            cursor: snapshot.cursor.clone(),
            state: snapshot.state.clone(),
            outbox: snapshot
                .outbox
                .iter()
                .map(|(key, entry)| PersistedOutboxEntry {
                    key: key.clone(),
                    entry: entry.clone(),
                })
                .collect(),
            waiting: snapshot.waiting.clone(),
            last_outcome: snapshot.last_outcome.clone(),
            ttl_secs: snapshot.ttl.as_secs(),
        }
    }
}

impl From<PersistedSnapshot> for SessionSnapshot {
    fn from(stored: PersistedSnapshot) -> Self {
        let mut outbox = HashMap::new();
        for entry in stored.outbox {
            outbox.insert(entry.key, entry.entry);
        }
        SessionSnapshot {
            key: stored.key,
            session_id: stored.session_id,
            revision: stored.revision,
            cursor: stored.cursor,
            state: stored.state,
            outbox,
            waiting: stored.waiting,
            last_outcome: stored.last_outcome,
            ttl: Duration::from_secs(stored.ttl_secs),
        }
    }
}
