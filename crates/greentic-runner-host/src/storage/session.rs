use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use greentic_session::inmemory::InMemorySessionStore;
use greentic_session::{SessionData, SessionKey as StoreSessionKey, SessionResult, SessionStore};
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
use greentic_session::ReplyScope;

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
        SessionBackend::Redis {
            url,
            namespace,
            wait_ttl,
        } => redis_store(url, namespace, *wait_ttl),
    }
}

#[cfg(feature = "session-redis")]
fn redis_store(
    url: &str,
    namespace: &str,
    wait_ttl: Option<std::time::Duration>,
) -> Result<DynSessionStore> {
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
    Ok(match wait_ttl {
        Some(ttl) => Arc::new(DefaultWaitTtl::new(store, ttl)),
        None => store,
    })
}

#[cfg(not(feature = "session-redis"))]
fn redis_store(
    _url: &str,
    namespace: &str,
    _wait_ttl: Option<std::time::Duration>,
) -> Result<DynSessionStore> {
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

/// Supplies a default expiry to every parked wait written through it.
///
/// `FlowResumeStore::save` calls `register_wait` with `ttl: None`, and it has no
/// per-wait lifetime to pass: the value it holds is a
/// [`FlowSnapshot`](crate::runner::engine::FlowSnapshot), which carries no TTL
/// field. (The `ttl` on [`SessionSnapshot`](crate::engine::host::SessionSnapshot)
/// belongs to the other adapter in this module and reaches a different trait
/// method — see [`SessionStoreHost`].) With an in-memory store `None` was
/// harmless: the wait died with the process. On Redis it means the key never
/// expires, so every parked-and-never-resumed conversation becomes a permanent
/// key and nothing reports the growth.
///
/// A decorator rather than a parameter threaded down from `HostBuilder`: every
/// caller that builds a `FlowResumeStore` — `TenantRuntime::load`, the welcome
/// probe, an embedding host's own — inherits the bound without knowing it
/// exists, and none of their signatures change.
///
/// **An explicit caller TTL always wins.** `ttl.or(default)` supplies a floor,
/// it does not impose a ceiling, so a future call site that knows a wait's real
/// lifetime keeps it.
///
/// What this does NOT bound: `register_wait` also adds the wait to a per-user
/// SET (`<ns>:waits:user:…`) that `greentic-session` never expires. The payload
/// and the scope key both go, leaving one small index member behind, which the
/// next `list_waits_for_user` for that user prunes (and Redis drops the set once
/// it empties). Completing a flow clears everything via `clear_wait`; the
/// residue is only for conversations abandoned mid-wait. Expiring that set is an
/// upstream change in `greentic-session`.
struct DefaultWaitTtl {
    inner: DynSessionStore,
    ttl: Duration,
}

impl DefaultWaitTtl {
    fn new(inner: DynSessionStore, ttl: Duration) -> Self {
        Self { inner, ttl }
    }
}

impl SessionStore for DefaultWaitTtl {
    fn create_session(&self, ctx: &TenantCtx, data: SessionData) -> SessionResult<StoreSessionKey> {
        self.inner.create_session(ctx, data)
    }

    fn get_session(&self, key: &StoreSessionKey) -> SessionResult<Option<SessionData>> {
        self.inner.get_session(key)
    }

    fn update_session(&self, key: &StoreSessionKey, data: SessionData) -> SessionResult<()> {
        self.inner.update_session(key, data)
    }

    fn remove_session(&self, key: &StoreSessionKey) -> SessionResult<()> {
        self.inner.remove_session(key)
    }

    fn register_wait(
        &self,
        ctx: &TenantCtx,
        user_id: &UserId,
        scope: &ReplyScope,
        session_key: &StoreSessionKey,
        data: SessionData,
        ttl: Option<Duration>,
    ) -> SessionResult<()> {
        self.inner.register_wait(
            ctx,
            user_id,
            scope,
            session_key,
            data,
            ttl.or(Some(self.ttl)),
        )
    }

    fn find_wait_by_scope(
        &self,
        ctx: &TenantCtx,
        user_id: &UserId,
        scope: &ReplyScope,
    ) -> SessionResult<Option<StoreSessionKey>> {
        self.inner.find_wait_by_scope(ctx, user_id, scope)
    }

    fn list_waits_for_user(
        &self,
        ctx: &TenantCtx,
        user_id: &UserId,
    ) -> SessionResult<Vec<StoreSessionKey>> {
        self.inner.list_waits_for_user(ctx, user_id)
    }

    fn clear_wait(
        &self,
        ctx: &TenantCtx,
        user_id: &UserId,
        scope: &ReplyScope,
    ) -> SessionResult<()> {
        self.inner.clear_wait(ctx, user_id, scope)
    }

    fn find_by_user(
        &self,
        ctx: &TenantCtx,
        user: &UserId,
    ) -> SessionResult<Option<(StoreSessionKey, SessionData)>> {
        #[allow(deprecated)]
        self.inner.find_by_user(ctx, user)
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

#[cfg(test)]
mod wait_ttl_tests {
    use super::*;
    use greentic_types::EnvId;
    use std::sync::Mutex;

    /// Records the TTL every `register_wait` arrived with.
    #[derive(Default)]
    struct TtlRecorder {
        seen: Mutex<Vec<Option<Duration>>>,
    }

    impl TtlRecorder {
        fn seen(&self) -> Vec<Option<Duration>> {
            self.seen.lock().map(|s| s.clone()).unwrap_or_default()
        }
    }

    impl SessionStore for TtlRecorder {
        fn create_session(
            &self,
            _ctx: &TenantCtx,
            _data: SessionData,
        ) -> SessionResult<StoreSessionKey> {
            Ok(StoreSessionKey::new("recorded"))
        }

        fn get_session(&self, _key: &StoreSessionKey) -> SessionResult<Option<SessionData>> {
            Ok(None)
        }

        fn update_session(&self, _key: &StoreSessionKey, _data: SessionData) -> SessionResult<()> {
            Ok(())
        }

        fn remove_session(&self, _key: &StoreSessionKey) -> SessionResult<()> {
            Ok(())
        }

        fn register_wait(
            &self,
            _ctx: &TenantCtx,
            _user_id: &UserId,
            _scope: &ReplyScope,
            _session_key: &StoreSessionKey,
            _data: SessionData,
            ttl: Option<Duration>,
        ) -> SessionResult<()> {
            if let Ok(mut seen) = self.seen.lock() {
                seen.push(ttl);
            }
            Ok(())
        }

        fn find_wait_by_scope(
            &self,
            _ctx: &TenantCtx,
            _user_id: &UserId,
            _scope: &ReplyScope,
        ) -> SessionResult<Option<StoreSessionKey>> {
            Ok(None)
        }

        fn list_waits_for_user(
            &self,
            _ctx: &TenantCtx,
            _user_id: &UserId,
        ) -> SessionResult<Vec<StoreSessionKey>> {
            Ok(Vec::new())
        }

        fn clear_wait(
            &self,
            _ctx: &TenantCtx,
            _user_id: &UserId,
            _scope: &ReplyScope,
        ) -> SessionResult<()> {
            Ok(())
        }

        fn find_by_user(
            &self,
            _ctx: &TenantCtx,
            _user: &UserId,
        ) -> SessionResult<Option<(StoreSessionKey, SessionData)>> {
            Ok(None)
        }
    }

    fn ctx() -> TenantCtx {
        let env = EnvId::from_str("local").unwrap_or_else(|_| unreachable!("static env id"));
        let tenant = TenantId::from_str("demo").unwrap_or_else(|_| unreachable!("static tenant"));
        TenantCtx::new(env, tenant)
    }

    fn sample_data(ctx: &TenantCtx) -> SessionData {
        SessionData {
            tenant_ctx: ctx.clone(),
            flow_id: FlowId::from_str("flow.main")
                .unwrap_or_else(|_| unreachable!("static flow id")),
            pack_id: None,
            cursor: TypesSessionCursor {
                node_pointer: "n0".into(),
                wait_reason: None,
                outbox_marker: None,
            },
            context_json: "{}".into(),
        }
    }

    fn drive(ttl: Option<Duration>) -> Vec<Option<Duration>> {
        let recorder = Arc::new(TtlRecorder::default());
        let store: DynSessionStore = Arc::clone(&recorder) as DynSessionStore;
        let wrapped = DefaultWaitTtl::new(store, Duration::from_secs(900));
        let ctx = ctx();
        let user = UserId::from_str("sessdeadbeef").unwrap_or_else(|_| unreachable!("static user"));
        let scope = ReplyScope {
            conversation: "conv".into(),
            thread: None,
            reply_to: None,
            correlation: None,
        };
        let _ = wrapped.register_wait(
            &ctx,
            &user,
            &scope,
            &StoreSessionKey::new("key"),
            sample_data(&ctx),
            ttl,
        );
        recorder.seen()
    }

    /// The case `FlowResumeStore::save` actually produces: no lifetime to pass,
    /// so the decorator's bound is what reaches the store.
    #[test]
    fn a_wait_with_no_ttl_is_given_the_configured_default() {
        assert_eq!(drive(None), vec![Some(Duration::from_secs(900))]);
    }

    /// A floor, not a ceiling. A caller that knows a wait's real lifetime keeps
    /// it — otherwise this decorator would silently rewrite a deliberate value.
    #[test]
    fn an_explicit_caller_ttl_wins_over_the_default() {
        assert_eq!(
            drive(Some(Duration::from_secs(5))),
            vec![Some(Duration::from_secs(5))]
        );
    }
}
