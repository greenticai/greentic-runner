//! Environment-driven selection + construction of the Agentic Worker state
//! backends (state store, token meter, tool ledger, graph checkpoint). Replaces
//! the previously-duplicated Redis wiring in `agent_node.rs` / `graph_node.rs`.

use std::path::PathBuf;
use std::sync::Arc;

use greentic_aw_runtime::cost::TokenMeter;
use greentic_aw_runtime::graph::CheckpointStore;
use greentic_aw_runtime::state::AgentStateStore;
use greentic_aw_runtime::tools::ToolLedger;

/// Which backend the operator's environment resolves to.
#[derive(Debug, PartialEq)]
pub(crate) enum StateBackendChoice {
    Redis(String),
    Memory,
    Disk(PathBuf),
    Disabled,
}

const ENV_BACKEND: &str = "GREENTIC_AW_STATE_BACKEND";
const ENV_REDIS_URL: &str = "GREENTIC_AW_REDIS_URL";
const ENV_STATE_PATH: &str = "GREENTIC_AW_STATE_PATH";

fn default_disk_path() -> PathBuf {
    match std::env::var("HOME") {
        Ok(home) if !home.is_empty() => PathBuf::from(home).join(".greentic/aw-state.redb"),
        _ => PathBuf::from("/var/lib/greentic/aw-state.redb"),
    }
}

/// Pure selector — see spec §"Backend selector" for the precedence rules.
pub(crate) fn select_state_backend(
    backend: Option<&str>,
    redis_url: Option<&str>,
    disk_path: Option<&str>,
) -> StateBackendChoice {
    let url = redis_url.filter(|u| !u.is_empty());
    match backend.map(str::trim).filter(|b| !b.is_empty()) {
        Some("redis") => match url {
            Some(u) => StateBackendChoice::Redis(u.to_string()),
            None => StateBackendChoice::Disabled, // explicit redis, no URL → honest misconfig
        },
        Some("memory") => StateBackendChoice::Memory,
        Some("disk") => StateBackendChoice::Disk(
            disk_path
                .filter(|p| !p.is_empty())
                .map(PathBuf::from)
                .unwrap_or_else(default_disk_path),
        ),
        Some(other) => {
            tracing::warn!(
                backend = %other,
                "unknown GREENTIC_AW_STATE_BACKEND (expected redis|memory|disk); \
                 falling back to auto-selection"
            );
            auto_select(url)
        }
        None => auto_select(url),
    }
}

/// No explicit backend: Redis if a URL is present (backward compatible),
/// else ephemeral memory.
fn auto_select(url: Option<&str>) -> StateBackendChoice {
    match url {
        Some(u) => StateBackendChoice::Redis(u.to_string()),
        None => StateBackendChoice::Memory,
    }
}

/// The four state backends the AW runtime needs, resolved once per builder.
pub(crate) struct AwBackends {
    pub state_store: Arc<dyn AgentStateStore>,
    pub token_meter: Arc<dyn TokenMeter>,
    pub tool_ledger: Arc<dyn ToolLedger>,
    pub checkpoint_store: Arc<dyn CheckpointStore>,
}

/// Build the AW state backends from the environment. Returns `None` only when
/// the worker must be disabled (explicit `redis` with no URL, or a Redis
/// connect failure). Memory/disk paths always succeed (disk falls back to
/// memory on open failure).
pub(crate) async fn build_aw_backends() -> Option<AwBackends> {
    let backend = std::env::var(ENV_BACKEND).ok();
    let redis_url = std::env::var(ENV_REDIS_URL).ok();
    let disk_path = std::env::var(ENV_STATE_PATH).ok();

    match select_state_backend(
        backend.as_deref(),
        redis_url.as_deref(),
        disk_path.as_deref(),
    ) {
        StateBackendChoice::Disabled => {
            tracing::info!(
                "GREENTIC_AW_STATE_BACKEND=redis but GREENTIC_AW_REDIS_URL unset; \
                 DwAgent nodes disabled"
            );
            None
        }
        StateBackendChoice::Redis(url) => build_redis_backends(&url).await,
        choice @ (StateBackendChoice::Memory | StateBackendChoice::Disk(_)) => {
            Some(backends_from_kv(shared_kv(&choice)))
        }
    }
}

/// Process-global KV backend for the non-Redis paths. Built exactly once so that
/// every AW handler (agent build, graph build, in-proc serve) shares one store.
static SHARED_AW_KV: std::sync::OnceLock<Arc<dyn greentic_aw_runtime::AwKv>> =
    std::sync::OnceLock::new();

/// Return the process-global memory/disk KV, constructing it on first use.
///
/// First-call-wins is correct here because the backend choice derives from
/// process-wide env, so every caller resolves the same choice. Building once is
/// also what makes `disk` durable: redb refuses a second open of the same file
/// within one process (`DatabaseAlreadyOpen`), so a per-call open would silently
/// fall back to ephemeral memory. Sharing the single `MemoryKv`/redb handle also
/// keeps token budgets metered once and session locks coordinated across the
/// agent and graph handlers instead of each holding a private map.
fn shared_kv(choice: &StateBackendChoice) -> Arc<dyn greentic_aw_runtime::AwKv> {
    SHARED_AW_KV.get_or_init(|| build_shared_kv(choice)).clone()
}

/// Construct the shared KV backend once. Only `Memory` and `Disk` reach here
/// (Redis and Disabled are handled by the caller before `shared_kv`); any other
/// choice conservatively falls back to ephemeral memory.
fn build_shared_kv(choice: &StateBackendChoice) -> Arc<dyn greentic_aw_runtime::AwKv> {
    match choice {
        StateBackendChoice::Disk(path) => build_shared_disk_kv(path),
        _ => {
            tracing::warn!(
                "AW state backend is in-memory (ephemeral): state, budgets, and \
                 idempotency are NOT persisted across restarts. Set GREENTIC_AW_REDIS_URL \
                 or GREENTIC_AW_STATE_BACKEND=disk for durability."
            );
            Arc::new(greentic_aw_runtime::MemoryKv::new())
        }
    }
}

/// Open the on-disk (redb) KV, falling back to ephemeral memory on any failure
/// or when the `state-disk` feature is compiled out.
fn build_shared_disk_kv(path: &std::path::Path) -> Arc<dyn greentic_aw_runtime::AwKv> {
    #[cfg(feature = "state-disk")]
    {
        match greentic_aw_runtime::kv::open_disk(path) {
            Ok(kv) => {
                tracing::info!(path = %path.display(), "AW state backend: on-disk (redb)");
                return kv;
            }
            Err(error) => {
                tracing::warn!(
                    path = %path.display(), error = %error,
                    "AW disk backend open failed; falling back to in-memory (ephemeral)"
                );
            }
        }
    }
    #[cfg(not(feature = "state-disk"))]
    {
        let _ = path;
        tracing::warn!(
            "GREENTIC_AW_STATE_BACKEND=disk but this build lacks the state-disk feature; \
             falling back to in-memory (ephemeral)"
        );
    }
    Arc::new(greentic_aw_runtime::MemoryKv::new())
}

async fn build_redis_backends(url: &str) -> Option<AwBackends> {
    use greentic_aw_runtime::RedisAgentStateStore;
    use greentic_aw_runtime::cost::RedisTokenMeter;
    use greentic_aw_runtime::graph::RedisCheckpointStore;
    use greentic_aw_runtime::tools::RedisToolLedger;

    let state_store = match RedisAgentStateStore::connect(url).await {
        Ok(store) => Arc::new(store),
        Err(error) => {
            tracing::warn!(error = %error, "AW Redis connect failed; DwAgent nodes disabled");
            return None;
        }
    };
    let manager = state_store.manager();
    Some(AwBackends {
        state_store,
        token_meter: Arc::new(RedisTokenMeter::new(manager.clone())),
        tool_ledger: Arc::new(RedisToolLedger::new(manager.clone())),
        checkpoint_store: Arc::new(RedisCheckpointStore::new(manager)),
    })
}

/// Assemble the four adapters over a single shared KV backend.
fn backends_from_kv(kv: Arc<dyn greentic_aw_runtime::AwKv>) -> AwBackends {
    use greentic_aw_runtime::cost::KvTokenMeter;
    use greentic_aw_runtime::graph::KvCheckpointStore;
    use greentic_aw_runtime::{KvAgentStateStore, KvToolLedger};

    AwBackends {
        state_store: Arc::new(KvAgentStateStore::new(kv.clone())),
        token_meter: Arc::new(KvTokenMeter::new(kv.clone())),
        tool_ledger: Arc::new(KvToolLedger::new(kv.clone())),
        checkpoint_store: Arc::new(KvCheckpointStore::new(kv)),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn explicit_memory_wins_over_redis_url() {
        assert_eq!(
            select_state_backend(Some("memory"), Some("redis://x"), None),
            StateBackendChoice::Memory
        );
    }

    #[test]
    fn unset_with_redis_url_is_redis() {
        assert_eq!(
            select_state_backend(None, Some("redis://x"), None),
            StateBackendChoice::Redis("redis://x".into())
        );
    }

    #[test]
    fn unset_without_anything_is_memory() {
        assert_eq!(
            select_state_backend(None, None, None),
            StateBackendChoice::Memory
        );
    }

    #[test]
    fn empty_redis_url_is_treated_as_absent() {
        assert_eq!(
            select_state_backend(None, Some(""), None),
            StateBackendChoice::Memory
        );
    }

    #[test]
    fn explicit_redis_without_url_disables() {
        assert_eq!(
            select_state_backend(Some("redis"), None, None),
            StateBackendChoice::Disabled
        );
    }

    #[test]
    fn disk_uses_explicit_path() {
        assert_eq!(
            select_state_backend(Some("disk"), None, Some("/data/x.redb")),
            StateBackendChoice::Disk(PathBuf::from("/data/x.redb"))
        );
    }

    #[test]
    fn unknown_backend_falls_back_to_auto() {
        assert_eq!(
            select_state_backend(Some("cassandra"), None, None),
            StateBackendChoice::Memory
        );
    }
}

#[cfg(test)]
mod build_tests {
    use super::*;
    use serial_test::serial;

    // The crate denies unsafe, but `std::env::set_var`/`remove_var` are `unsafe`
    // as of edition 2024. These helpers are only reachable from `#[serial]`
    // tests, so no other thread observes the environment mid-mutation.
    #[allow(unsafe_code)]
    fn set(key: &str, val: &str) {
        // SAFETY: env-mutating tests are serialized via `#[serial]`.
        unsafe { std::env::set_var(key, val) };
    }
    #[allow(unsafe_code)]
    fn unset(key: &str) {
        // SAFETY: env-mutating tests are serialized via `#[serial]`.
        unsafe { std::env::remove_var(key) };
    }

    #[tokio::test]
    #[serial]
    async fn builds_memory_backend_with_no_redis() {
        unset(ENV_REDIS_URL);
        unset(ENV_STATE_PATH);
        set(ENV_BACKEND, "memory");
        let backends = build_aw_backends().await;
        assert!(
            backends.is_some(),
            "memory backend must build without Redis"
        );
        unset(ENV_BACKEND);
    }

    /// Two separate `build_aw_backends()` calls (as the agent and graph handlers
    /// do) must share ONE underlying KV store, not each get a private map.
    /// Proof: state saved via the first set of backends is visible through the
    /// second. `SHARED_AW_KV` is a process-global `OnceLock`, so this holds
    /// regardless of which memory-backed test ran first in this binary.
    #[tokio::test]
    #[serial]
    async fn memory_backends_share_one_store_across_calls() {
        use greentic_aw_runtime::state::{ChatMessage, ConversationState};
        use greentic_aw_runtime::tenant::TenantContext;

        unset(ENV_REDIS_URL);
        unset(ENV_STATE_PATH);
        set(ENV_BACKEND, "memory");

        let backends1 = build_aw_backends().await.unwrap();
        let backends2 = build_aw_backends().await.unwrap();

        let tenant = TenantContext::new("tenant-share", "env-share");
        let session_id = "share-session";
        let mut state = ConversationState::empty(&tenant, session_id);
        state.messages.push(ChatMessage::User {
            content: "hello from handler one".to_string(),
        });

        backends1
            .state_store
            .save(&tenant, session_id, &state)
            .await
            .unwrap();

        let loaded = backends2
            .state_store
            .load(&tenant, session_id)
            .await
            .unwrap();

        assert_eq!(
            loaded.messages.len(),
            1,
            "message saved via first backend must be visible via second (shared store)"
        );
        match &loaded.messages[0] {
            ChatMessage::User { content } => {
                assert_eq!(content, "hello from handler one");
            }
            other => panic!("unexpected message variant: {other:?}"),
        }

        unset(ENV_BACKEND);
    }
}
