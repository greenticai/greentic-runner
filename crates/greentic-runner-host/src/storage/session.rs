use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
#[cfg(any(feature = "session-redis", test))]
use greentic_session::SessionResult;
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
#[cfg(any(feature = "session-redis", test))]
use greentic_session::ReplyScope;

pub type DynSessionStore = Arc<dyn SessionStore>;

/// Adapter that backs the runner session host with a greentic-session store.
///
/// The store is held behind [`Offloaded`], so every call reaches it through
/// `spawn_blocking` — `SessionStore` is a synchronous trait and its Redis
/// implementation blocks on a socket.
///
/// # Known limitation (deferred)
///
/// `greentic_session`'s `find_by_user` only sees sessions registered as
/// *waits*, so against the in-memory and Redis stores a plain `put` is not found
/// again by `get`. Registering sessions as waits fixes that, but it changes what
/// the flow engine observes (the parked-flow and resumer end-to-end suites
/// regress), so it needs an engine-side decision first. `touch` therefore also
/// cannot enforce a store-level TTL yet.
///
/// # Concurrency
///
/// Read-modify-write operations (`put`, `update_cas`, `touch`, `delete`) on one
/// session are serialised by a per-session in-process lock (dropped from the
/// map again as soon as nobody holds or waits on it, so the map stays bounded
/// by the number of in-flight operations), so two concurrent
/// `update_cas` calls with the same expected revision cannot both succeed
/// within one process. The `SessionStore` trait offers no atomic
/// compare-and-swap, so across processes sharing a Redis store this is still
/// last-writer-wins; closing that needs a CAS primitive in `greentic-session`.
pub struct SessionStoreHost {
    store: Offloaded<dyn SessionStore>,
    locks: dashmap::DashMap<String, Arc<tokio::sync::Mutex<()>>>,
}

impl SessionStoreHost {
    pub fn new(store: DynSessionStore) -> Self {
        Self {
            store: Offloaded::new(store, StoreKind::Session),
            locks: dashmap::DashMap::new(),
        }
    }

    /// Take the in-process lock guarding one session (keyed by its derived user
    /// id, which covers tenant, pack, flow and hint). The map entry is removed
    /// when the returned guard drops and no other task holds or awaits it.
    async fn lock_session(&self, key: &SessionKey) -> GResult<SessionLockGuard<'_>> {
        let id = user_id_from_key(key)?.as_str().to_string();
        let mutex = {
            let entry = self
                .locks
                .entry(id.clone())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())));
            Arc::clone(entry.value())
        };
        // Build the guard BEFORE awaiting: if this future is cancelled while
        // waiting (after the holder released, so the holder's own sweep saw the
        // waiter's Arc and kept the entry), dropping the still-empty guard
        // sweeps the entry instead of leaking it until the next op on this id.
        let mut guard = SessionLockGuard {
            locks: &self.locks,
            id,
            guard: None,
        };
        guard.guard = Some(mutex.lock_owned().await);
        Ok(guard)
    }

    async fn lookup_entry(&self, key: &SessionKey) -> GResult<Option<StoreEntry>> {
        let user = user_id_from_key(key)?;
        if let Some(entry) = self.lookup_entry_for(key, user.clone()).await? {
            return Ok(Some(entry));
        }
        // Sessions written before the slot was derived from tenant/pack/flow
        // live under a hint-only slot; without this fallback an upgrade would
        // orphan every parked flow. The snapshot's own key is still verified.
        let legacy = legacy_user_id_from_key(key)?;
        if legacy == user {
            return Ok(None);
        }
        self.lookup_entry_for(key, legacy).await
    }

    async fn lookup_entry_for(
        &self,
        key: &SessionKey,
        user: UserId,
    ) -> GResult<Option<StoreEntry>> {
        let base_ctx = tenant_ctx_from_key(key)?;
        let ctx = base_ctx.with_user(Some(user.clone()));
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

    /// Remove the legacy hint-only slot of `key`, if one still exists.
    async fn remove_legacy(&self, key: &SessionKey) -> GResult<()> {
        let new_user = user_id_from_key(key)?;
        let legacy = legacy_user_id_from_key(key)?;
        if legacy == new_user {
            return Ok(());
        }
        if let Some(entry) = self.lookup_entry_for(key, legacy).await? {
            self.store
                .call(move |store| store.remove_session(&entry.key))
                .await
                .map_err(map_store_error)?;
        }
        Ok(())
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

/// Holds one session's in-process lock and removes the map entry on drop when
/// no other task holds or awaits it (`strong_count == 1` means only the map
/// still owns the mutex; the check runs under the shard lock, so a concurrent
/// `lock_session` either sees the entry or inserts a fresh one).
struct SessionLockGuard<'a> {
    locks: &'a dashmap::DashMap<String, Arc<tokio::sync::Mutex<()>>>,
    id: String,
    guard: Option<tokio::sync::OwnedMutexGuard<()>>,
}

impl Drop for SessionLockGuard<'_> {
    fn drop(&mut self) {
        // Release the mutex (and its Arc) before checking the count.
        drop(self.guard.take());
        self.locks
            .remove_if(&self.id, |_, mutex| Arc::strong_count(mutex) == 1);
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
        let _guard = self.lock_session(&snapshot.key).await?;
        let base_ctx = tenant_ctx_from_key(&snapshot.key)?;
        let user = user_id_from_key(&snapshot.key)?;
        let ctx = base_ctx.with_user(Some(user.clone()));
        self.upsert(&snapshot, ctx, &user).await?;
        // Migrate: the new slot now owns the session, drop the legacy one.
        self.remove_legacy(&snapshot.key).await?;
        Ok(())
    }

    async fn update_cas(
        &self,
        mut snapshot: SessionSnapshot,
        expected_revision: u64,
    ) -> GResult<bool> {
        let _guard = self.lock_session(&snapshot.key).await?;
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
        let _guard = self.lock_session(key).await?;
        if let Some(entry) = self.lookup_entry_for(key, user_id_from_key(key)?).await? {
            self.store
                .call(move |store| store.remove_session(&entry.key))
                .await
                .map_err(map_store_error)?;
        }
        self.remove_legacy(key).await?;
        Ok(())
    }

    async fn touch(&self, key: &SessionKey, ttl: Duration) -> GResult<()> {
        let _guard = self.lock_session(key).await?;
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
    let digest = Sha256::digest(session_identity(key).as_bytes());
    let slug = format!("sess{}", hex::encode(&digest[..8]));
    UserId::from_str(&slug).map_err(map_store_error)
}

/// Slot id used before the slot was derived from tenant/pack/flow: the hint
/// alone (or `tenant::pack::flow` when there is none).
fn legacy_user_id_from_key(key: &SessionKey) -> GResult<UserId> {
    let hint = key
        .session_hint
        .clone()
        .unwrap_or_else(|| format!("{}::{}::{}", key.tenant_key, key.pack_id, key.flow_id));
    let digest = Sha256::digest(hint.as_bytes());
    let slug = format!("sess{}", hex::encode(&digest[..8]));
    UserId::from_str(&slug).map_err(map_store_error)
}

/// Identity string of one session: tenant, pack, flow AND hint. The hint alone
/// is not enough: two packs or flows (or tenants) sharing a conversation hint
/// must not share a store slot. Length-prefixed so segment boundaries cannot
/// alias (`a::b` + `c` vs `a` + `b::c`).
fn session_identity(key: &SessionKey) -> String {
    let hint = key.session_hint.as_deref().unwrap_or("");
    format!(
        "{}:{}|{}:{}|{}:{}|{}:{}",
        key.tenant_key.len(),
        key.tenant_key,
        key.pack_id.len(),
        key.pack_id,
        key.flow_id.len(),
        key.flow_id,
        hint.len(),
        hint,
    )
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
#[cfg(any(feature = "session-redis", test))]
struct DefaultWaitTtl {
    inner: DynSessionStore,
    ttl: Duration,
}

#[cfg(any(feature = "session-redis", test))]
impl DefaultWaitTtl {
    fn new(inner: DynSessionStore, ttl: Duration) -> Self {
        Self { inner, ttl }
    }
}

#[cfg(any(feature = "session-redis", test))]
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

    /// Every other `SessionStore` method must reach the wrapped store untouched;
    /// the decorator exists only to supply a TTL, so a dropped delegation would
    /// silently break sessions on the Redis path.
    #[test]
    fn every_other_store_call_is_delegated_unchanged() {
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
        let key = StoreSessionKey::new("key");

        assert!(wrapped.create_session(&ctx, sample_data(&ctx)).is_ok());
        assert!(matches!(wrapped.get_session(&key), Ok(None)));
        assert!(wrapped.update_session(&key, sample_data(&ctx)).is_ok());
        assert!(wrapped.remove_session(&key).is_ok());
        assert!(matches!(
            wrapped.find_wait_by_scope(&ctx, &user, &scope),
            Ok(None)
        ));
        assert!(
            wrapped
                .list_waits_for_user(&ctx, &user)
                .map(|waits| waits.is_empty())
                .unwrap_or(false)
        );
        assert!(wrapped.clear_wait(&ctx, &user, &scope).is_ok());
        #[allow(deprecated)]
        let found = wrapped.find_by_user(&ctx, &user);
        assert!(matches!(found, Ok(None)));
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

#[cfg(test)]
mod host_tests {
    use super::*;

    fn key_for(hint: &str) -> SessionKey {
        let env = EnvId::from_str("local").unwrap_or_else(|_| unreachable!("static env id"));
        let tenant = TenantId::from_str("demo").unwrap_or_else(|_| unreachable!("static tenant"));
        SessionKey::new(
            &TenantCtx::new(env, tenant),
            "pack.main",
            "flow.main",
            Some(hint.to_string()),
        )
    }

    /// `InMemorySessionStore::find_by_user` only sees registered waits, so a plain
    /// `put` is never found again through it. This keeps one session per user the
    /// way the Redis store does, which is what the host adapter is written for.
    #[derive(Default)]
    struct PerUserStore {
        slots: std::sync::Mutex<Vec<(UserId, StoreSessionKey, SessionData)>>,
        /// Monotonic slot ids: deriving the id from `slots.len()` reused the id
        /// of a live slot after a removal, so a later remove took both.
        next_slot: std::sync::atomic::AtomicUsize,
    }

    impl PerUserStore {
        fn slots(&self) -> std::sync::MutexGuard<'_, Vec<(UserId, StoreSessionKey, SessionData)>> {
            self.slots
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
        }
    }

    impl SessionStore for PerUserStore {
        fn create_session(
            &self,
            _ctx: &TenantCtx,
            data: SessionData,
        ) -> SessionResult<StoreSessionKey> {
            let user = data
                .tenant_ctx
                .user_id
                .clone()
                .unwrap_or_else(|| unreachable!("encode_snapshot always sets a user"));
            let mut slots = self.slots();
            let id = self
                .next_slot
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let key = StoreSessionKey::new(format!("slot-{id}"));
            slots.push((user, key.clone(), data));
            Ok(key)
        }

        fn get_session(&self, key: &StoreSessionKey) -> SessionResult<Option<SessionData>> {
            Ok(self
                .slots()
                .iter()
                .find(|(_, k, _)| k == key)
                .map(|(_, _, d)| d.clone()))
        }

        fn update_session(&self, key: &StoreSessionKey, data: SessionData) -> SessionResult<()> {
            if let Some(slot) = self.slots().iter_mut().find(|(_, k, _)| k == key) {
                slot.2 = data;
            }
            Ok(())
        }

        fn remove_session(&self, key: &StoreSessionKey) -> SessionResult<()> {
            self.slots().retain(|(_, k, _)| k != key);
            Ok(())
        }

        fn register_wait(
            &self,
            _ctx: &TenantCtx,
            _user_id: &UserId,
            _scope: &ReplyScope,
            _session_key: &StoreSessionKey,
            _data: SessionData,
            _ttl: Option<Duration>,
        ) -> SessionResult<()> {
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
            user: &UserId,
        ) -> SessionResult<Option<(StoreSessionKey, SessionData)>> {
            Ok(self
                .slots()
                .iter()
                .find(|(u, _, _)| u == user)
                .map(|(_, k, d)| (k.clone(), d.clone())))
        }
    }

    fn host() -> Arc<dyn SessionHost> {
        session_host_from(Arc::new(PerUserStore::default()))
    }

    #[tokio::test]
    async fn put_then_get_round_trips_a_snapshot() -> Result<()> {
        let host = host();
        let key = key_for("round-trip");
        let mut snapshot = SessionSnapshot::new(key.clone(), "sid-1".into());
        snapshot.state = serde_json::json!({ "answer": 42 });
        snapshot.cursor = SessionCursor {
            position: 3,
            outbox_seq: 2,
        };
        host.put(snapshot.clone()).await?;
        let loaded = host.get(&key).await?;
        assert_eq!(loaded, Some(snapshot));
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_update_cas_has_a_single_winner() -> Result<()> {
        let host = host();
        let key = key_for("cas");
        host.put(SessionSnapshot::new(key.clone(), "sid".into()))
            .await?;
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let host = Arc::clone(&host);
            let key = key.clone();
            tasks.push(tokio::spawn(async move {
                host.update_cas(SessionSnapshot::new(key, "sid".into()), 0)
                    .await
            }));
        }
        let mut wins = 0;
        for task in tasks {
            if task.await.map_err(|e| anyhow!("{e}"))?? {
                wins += 1;
            }
        }
        assert_eq!(wins, 1);
        Ok(())
    }

    #[test]
    fn user_ids_differ_across_pack_flow_and_tenant_for_one_hint() -> Result<()> {
        let base = key_for("shared-hint");
        let mut other_pack = base.clone();
        other_pack.pack_id = "pack.other".into();
        let mut other_flow = base.clone();
        other_flow.flow_id = "flow.other".into();
        let mut other_tenant = base.clone();
        other_tenant.tenant_key = "local::other".into();
        let ids = [
            user_id_from_key(&base)?,
            user_id_from_key(&other_pack)?,
            user_id_from_key(&other_flow)?,
            user_id_from_key(&other_tenant)?,
        ];
        for (i, a) in ids.iter().enumerate() {
            for b in &ids[i + 1..] {
                assert_ne!(a, b);
            }
        }
        Ok(())
    }

    /// A waiter cancelled after the holder released (but before it ran) must
    /// not leave the lock-map entry behind.
    #[tokio::test]
    async fn cancelled_waiter_does_not_leak_its_lock_entry() -> Result<()> {
        let host = SessionStoreHost::new(Arc::new(PerUserStore::default()));
        let key = key_for("cancel");
        let holder = host.lock_session(&key).await?;
        assert_eq!(host.locks.len(), 1);

        let mut waiter = Box::pin(host.lock_session(&key));
        tokio::select! {
            biased;
            _ = &mut waiter => anyhow::bail!("lock is held, waiter must pend"),
            () = async move { drop(holder) } => {}
        }
        // The holder's sweep saw the waiter's Arc and kept the entry.
        assert_eq!(host.locks.len(), 1);
        drop(waiter);
        assert_eq!(host.locks.len(), 0, "cancelled waiter leaked its entry");
        Ok(())
    }

    #[tokio::test]
    async fn get_of_an_unknown_session_is_none() -> Result<()> {
        assert_eq!(host().get(&key_for("nobody")).await?, None);
        Ok(())
    }

    #[tokio::test]
    async fn update_cas_only_succeeds_on_the_expected_revision() -> Result<()> {
        let host = host();
        let key = key_for("cas");
        // No session yet: nothing to compare against.
        let missing = SessionSnapshot::new(key.clone(), "sid".into());
        assert!(!host.update_cas(missing.clone(), 0).await?);

        host.put(missing.clone()).await?;
        // Stale expected revision is refused and leaves the stored one alone.
        assert!(!host.update_cas(missing.clone(), 7).await?);
        assert_eq!(host.get(&key).await?.map(|s| s.revision), Some(0));

        // The right revision wins and bumps the stored revision by one.
        assert!(host.update_cas(missing, 0).await?);
        assert_eq!(host.get(&key).await?.map(|s| s.revision), Some(1));
        Ok(())
    }

    #[tokio::test]
    async fn touch_rewrites_the_ttl_and_ignores_unknown_sessions() -> Result<()> {
        let host = host();
        let key = key_for("touch");
        host.touch(&key, Duration::from_secs(5)).await?;
        assert_eq!(host.get(&key).await?, None);

        host.put(SessionSnapshot::new(key.clone(), "sid".into()))
            .await?;
        host.touch(&key, Duration::from_secs(5)).await?;
        assert_eq!(
            host.get(&key).await?.map(|s| s.ttl),
            Some(Duration::from_secs(5))
        );
        Ok(())
    }

    #[tokio::test]
    async fn delete_removes_the_session_and_is_idempotent() -> Result<()> {
        let host = host();
        let key = key_for("delete");
        host.put(SessionSnapshot::new(key.clone(), "sid".into()))
            .await?;
        host.delete(&key).await?;
        assert_eq!(host.get(&key).await?, None);
        host.delete(&key).await?;
        Ok(())
    }

    #[tokio::test]
    async fn sessions_with_different_hints_do_not_collide() -> Result<()> {
        let host = host();
        let (a, b) = (key_for("a"), key_for("b"));
        host.put(SessionSnapshot::new(a.clone(), "sid-a".into()))
            .await?;
        host.put(SessionSnapshot::new(b.clone(), "sid-b".into()))
            .await?;
        assert_eq!(
            host.get(&a).await?.map(|s| s.session_id),
            Some("sid-a".into())
        );
        assert_eq!(
            host.get(&b).await?.map(|s| s.session_id),
            Some("sid-b".into())
        );
        Ok(())
    }

    /// Write `snapshot` the way a pre-upgrade runner did: under the hint-only slot.
    fn seed_legacy(store: &PerUserStore, snapshot: &SessionSnapshot) -> Result<()> {
        let legacy = legacy_user_id_from_key(&snapshot.key)?;
        let ctx = tenant_ctx_from_key(&snapshot.key)?.with_user(Some(legacy.clone()));
        let data = encode_snapshot(snapshot, ctx.clone(), &legacy)?;
        store.create_session(&ctx, data)?;
        Ok(())
    }

    #[tokio::test]
    async fn a_session_in_the_legacy_hint_only_slot_is_still_found() -> Result<()> {
        let store = Arc::new(PerUserStore::default());
        let host = SessionStoreHost::new(store.clone());
        let key = key_for("legacy");
        seed_legacy(&store, &SessionSnapshot::new(key.clone(), "sid-old".into()))?;

        assert_eq!(
            host.get(&key).await?.map(|s| s.session_id),
            Some("sid-old".into())
        );
        // A CAS against the legacy revision succeeds and keeps one slot.
        let mut next = SessionSnapshot::new(key.clone(), "sid-new".into());
        next.revision = 1;
        assert!(host.update_cas(next, 0).await?);
        assert_eq!(store.slots().len(), 1);
        assert_eq!(
            host.get(&key).await?.map(|s| s.session_id),
            Some("sid-new".into())
        );
        Ok(())
    }

    #[tokio::test]
    async fn put_migrates_a_legacy_slot_and_delete_clears_it() -> Result<()> {
        let store = Arc::new(PerUserStore::default());
        let host = SessionStoreHost::new(store.clone());
        let key = key_for("migrate");
        seed_legacy(&store, &SessionSnapshot::new(key.clone(), "sid-old".into()))?;

        host.put(SessionSnapshot::new(key.clone(), "sid-put".into()))
            .await?;
        assert_eq!(store.slots().len(), 1, "legacy slot must be migrated");
        assert_eq!(
            host.get(&key).await?.map(|s| s.session_id),
            Some("sid-put".into())
        );

        let other = key_for("delete");
        seed_legacy(&store, &SessionSnapshot::new(other.clone(), "sid-x".into()))?;
        host.delete(&other).await?;
        assert!(host.get(&other).await?.is_none());
        assert_eq!(store.slots().len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn a_legacy_slot_of_another_pack_is_not_adopted() -> Result<()> {
        let store = Arc::new(PerUserStore::default());
        let host = SessionStoreHost::new(store.clone());
        let mut foreign = key_for("shared");
        foreign.pack_id = "pack.other".into();
        seed_legacy(&store, &SessionSnapshot::new(foreign, "sid-foreign".into()))?;
        assert!(host.get(&key_for("shared")).await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn the_lock_map_does_not_outlive_its_operations() -> Result<()> {
        let host = SessionStoreHost::new(Arc::new(PerUserStore::default()));
        for i in 0..20 {
            let key = key_for(&format!("s{i}"));
            host.put(SessionSnapshot::new(key.clone(), format!("sid-{i}")))
                .await?;
            host.touch(&key, Duration::from_secs(1)).await?;
        }
        assert!(
            host.locks.is_empty(),
            "{} stale lock entries",
            host.locks.len()
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_malformed_tenant_descriptor_is_a_session_error() {
        let mut key = key_for("bad");
        key.tenant_key = "no-separator".into();
        let host = host();
        assert!(host.get(&key).await.is_err());
        assert!(
            host.put(SessionSnapshot::new(key, "sid".into()))
                .await
                .is_err()
        );
    }

    #[test]
    fn store_from_config_builds_in_memory_and_user_ids_are_stable() -> Result<()> {
        let _ = store_from_config(&SessionBackend::InMemory)?;
        let key = key_for("stable");
        assert_eq!(user_id_from_key(&key)?, user_id_from_key(&key)?);
        // Without a hint the id is derived from the whole key.
        let mut hintless = key;
        hintless.session_hint = None;
        assert!(user_id_from_key(&hintless)?.as_str().starts_with("sess"));
        Ok(())
    }

    #[test]
    fn decode_of_garbage_context_is_a_session_error() {
        let data = SessionData {
            tenant_ctx: TenantCtx::new(
                EnvId::from_str("local").unwrap_or_else(|_| unreachable!("static env id")),
                TenantId::from_str("demo").unwrap_or_else(|_| unreachable!("static tenant")),
            ),
            flow_id: FlowId::from_str("flow.main")
                .unwrap_or_else(|_| unreachable!("static flow id")),
            pack_id: None,
            cursor: TypesSessionCursor {
                node_pointer: "n0".into(),
                wait_reason: None,
                outbox_marker: None,
            },
            context_json: "not json".into(),
        };
        assert!(decode_snapshot(&data).is_err());
    }
}
