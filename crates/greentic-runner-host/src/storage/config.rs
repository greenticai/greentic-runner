//! Which stores a [`RunnerHost`](crate::RunnerHost) keeps its sessions and flow
//! state in.
//!
//! The default is unchanged from before this module existed: both stores are
//! in-memory, so a parked flow's [`SessionSnapshot`] dies with the process. That
//! is correct for a desktop run and for every test; it is wrong for a deployed
//! worker, where a revision rollout or a cold start silently restarts the
//! conversation. [`StorageConfig`] is how an embedding host asks for something
//! durable instead.
//!
//! # What the backing crates do about namespacing
//!
//! Scoping matters here: two deployments sharing one Redis must not read each
//! other's sessions. The two crates solve it differently, and the difference is
//! why only one of the two variants below carries a namespace.
//!
//! * **`greentic-session`** namespaces at the *store*. Its Redis backend keys a
//!   session entry as `<namespace>:session:<session key>`, where the session key
//!   is the runner's `<session hint>::<scope hash>` — and the session hint
//!   carries tenant, provider, channel, conversation and user but **not the
//!   environment**. The wait index keys (`<namespace>:waits:user:<env>:<tenant>:…`)
//!   do carry it, so a cross-environment collision does not leak a read: it
//!   makes [`find_wait_by_scope`] see a stored context whose `env` differs, drop
//!   the scope entry and return `None` — i.e. two environments sharing one
//!   keyspace would evict each other's parked flows. That is why
//!   [`SessionBackend::redis`] **requires** a namespace rather than accepting
//!   the crate's `greentic:session` default.
//! * **`greentic-state`** namespaces at the *key*. `RedisStateStore::from_url`
//!   takes no namespace and does not need one: every key goes through
//!   `greentic_state::key::fqn`, which composes
//!   `greentic:state:<env>:<tenant>[:<team>][:<user>]:<prefix>:<key>`, and this
//!   crate's own key adds `pack/<pack>/flow/<flow>/session/<hint>` on top. Env
//!   and tenant are therefore already in every key.
//!
//! Verified against `greentic-session 1.2.0-dev.35754484367` and
//! `greentic-state 1.2.0-dev.35754483639` — the versions this workspace's
//! `Cargo.lock` resolves. Re-read `backends/redis.rs` and `key.rs` before
//! trusting either paragraph after a bump.
//!
//! `RedisStateStore::del_prefix` runs an unbounded `SCAN MATCH` over the whole
//! keyspace, which would be a real hazard on a shared Redis. It is unreachable
//! from this crate: [`StateStoreHost::del_prefix`](crate::storage::state) is a
//! no-op and never forwards. `del_prefix_never_reaches_the_backing_store` pins
//! that, so a future implementation has to make the cost a deliberate decision.
//!
//! [`SessionSnapshot`]: crate::engine::host::SessionSnapshot
//! [`find_wait_by_scope`]: greentic_session::SessionStore::find_wait_by_scope

use std::fmt;

/// Environment variable naming the session backend (`memory` | `redis`).
pub const ENV_SESSION_BACKEND: &str = "GREENTIC_RUNNER_SESSION_BACKEND";
/// Environment variable naming the flow-state backend (`memory` | `redis`).
pub const ENV_STATE_BACKEND: &str = "GREENTIC_RUNNER_STATE_BACKEND";
/// Environment variable holding the Redis connection URL for both stores.
pub const ENV_REDIS_URL: &str = "GREENTIC_RUNNER_REDIS_URL";
/// Environment variable holding an explicit session keyspace prefix.
pub const ENV_SESSION_NAMESPACE: &str = "GREENTIC_RUNNER_SESSION_NAMESPACE";
/// Environment variable naming the deployment environment, used to derive a
/// session namespace when [`ENV_SESSION_NAMESPACE`] is unset.
pub const ENV_GREENTIC_ENV: &str = "GREENTIC_ENV";

/// A storage backend was asked for and could not be honoured.
///
/// Every variant is a refusal taken *before* any store is handed out, so a
/// misconfigured durable backend can never degrade into a silent in-memory one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageConfigError {
    /// A Redis backend was named but no URL was supplied.
    MissingRedisUrl { store: &'static str },
    /// A session Redis backend was named but no keyspace could be resolved.
    MissingSessionNamespace,
    /// The backend name was not one this build understands.
    UnknownBackend { store: &'static str, value: String },
}

impl fmt::Display for StorageConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingRedisUrl { store } => write!(
                f,
                "{store} backend 'redis' was requested but no Redis URL was supplied \
                 (set {ENV_REDIS_URL}); refusing rather than falling back to in-memory, \
                 which would silently lose every parked conversation"
            ),
            Self::MissingSessionNamespace => write!(
                f,
                "session backend 'redis' was requested but no keyspace could be resolved: \
                 set {ENV_SESSION_NAMESPACE} explicitly, or {ENV_GREENTIC_ENV} to derive one. \
                 A session entry key carries no environment, so sharing the crate default \
                 keyspace lets two environments evict each other's parked flows"
            ),
            Self::UnknownBackend { store, value } => write!(
                f,
                "unknown {store} backend '{value}' (expected 'memory' or 'redis')"
            ),
        }
    }
}

impl std::error::Error for StorageConfigError {}

/// Where parked-flow sessions live.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum SessionBackend {
    /// Process-local. A parked flow does not survive the process.
    #[default]
    InMemory,
    /// Redis, under a keyspace unique to this deployment environment.
    Redis {
        /// Connection URL, e.g. `redis://127.0.0.1:6379`.
        url: String,
        /// Keyspace prefix every key of this store is written under.
        namespace: String,
    },
}

/// Where per-session flow state lives.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum StateBackend {
    /// Process-local. Flow state does not survive the process.
    #[default]
    InMemory,
    /// Redis. Keys are scoped by environment and tenant by `greentic-state`
    /// itself — see this module's header.
    Redis {
        /// Connection URL, e.g. `redis://127.0.0.1:6379`.
        url: String,
    },
}

impl SessionBackend {
    /// Redis-backed sessions under `namespace`.
    ///
    /// The namespace is required, not defaulted: see this module's header for
    /// what two environments sharing one keyspace do to each other.
    pub fn redis(
        url: impl Into<String>,
        namespace: impl Into<String>,
    ) -> Result<Self, StorageConfigError> {
        let url = url.into();
        let namespace = namespace.into();
        if url.trim().is_empty() {
            return Err(StorageConfigError::MissingRedisUrl { store: "session" });
        }
        if namespace.trim().is_empty() {
            return Err(StorageConfigError::MissingSessionNamespace);
        }
        Ok(Self::Redis { url, namespace })
    }

    /// The conventional keyspace for `env`: `greentic:session:<env>`.
    ///
    /// Environment is the only scope the crate's own session entry key omits;
    /// tenant, pack and conversation are already inside the key, and two
    /// deployments of one environment SHOULD share them — that sharing is what
    /// makes a conversation survive a revision rollout.
    pub fn namespace_for_env(env: &str) -> Result<String, StorageConfigError> {
        let env = env.trim();
        if env.is_empty() {
            return Err(StorageConfigError::MissingSessionNamespace);
        }
        Ok(format!("greentic:session:{env}"))
    }

    /// `true` when this backend performs network I/O and must not be called
    /// directly from a tokio worker.
    pub fn is_durable(&self) -> bool {
        matches!(self, Self::Redis { .. })
    }

    /// A log-safe description: the backend name and, for Redis, the keyspace.
    ///
    /// Never the URL. A Redis URL may carry `redis://user:password@host`, and
    /// the `Debug` impl would print it verbatim into whatever log the operator
    /// ships off the box.
    pub fn describe(&self) -> String {
        match self {
            Self::InMemory => "in-memory".to_string(),
            Self::Redis { namespace, .. } => format!("redis (namespace '{namespace}')"),
        }
    }
}

impl StateBackend {
    /// Redis-backed flow state.
    pub fn redis(url: impl Into<String>) -> Result<Self, StorageConfigError> {
        let url = url.into();
        if url.trim().is_empty() {
            return Err(StorageConfigError::MissingRedisUrl { store: "state" });
        }
        Ok(Self::Redis { url })
    }

    /// `true` when this backend performs network I/O and must not be called
    /// directly from a tokio worker.
    pub fn is_durable(&self) -> bool {
        matches!(self, Self::Redis { .. })
    }

    /// A log-safe description. Never the URL — see
    /// [`SessionBackend::describe`].
    pub fn describe(&self) -> &'static str {
        match self {
            Self::InMemory => "in-memory",
            Self::Redis { .. } => "redis",
        }
    }
}

/// The pair of stores a host is built over.
///
/// [`StorageConfig::default()`] is both-in-memory, which is what
/// [`HostBuilder::build`](crate::HostBuilder::build) uses when no storage is
/// named — so every caller that predates this type keeps exactly the behaviour
/// it had.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StorageConfig {
    /// Parked-flow sessions.
    pub session: SessionBackend,
    /// Per-session flow state.
    pub state: StateBackend,
}

impl StorageConfig {
    /// Both stores in memory — the default.
    pub fn in_memory() -> Self {
        Self::default()
    }

    /// Both stores on one Redis, sessions under `namespace`.
    pub fn redis(
        url: impl Into<String>,
        namespace: impl Into<String>,
    ) -> Result<Self, StorageConfigError> {
        let url = url.into();
        Ok(Self {
            session: SessionBackend::redis(url.clone(), namespace)?,
            state: StateBackend::redis(url)?,
        })
    }

    /// Resolve from the process environment.
    ///
    /// Deliberately NOT called by `HostBuilder::build`: reading the environment
    /// there would move an existing embedder's sessions to Redis the moment
    /// these variables appeared for some other reason. An embedder that wants
    /// this asks for it.
    ///
    /// Precedence mirrors `runner::aw_backends::select_state_backend` with one
    /// deliberate difference: an explicitly-named Redis backend with
    /// no URL is an `Err` here, not a quiet disable. A worker that cannot reach
    /// its state store should not start.
    ///
    /// A backend that is not named at all resolves to memory — never to Redis
    /// just because a URL happens to be present.
    pub fn from_env() -> Result<Self, StorageConfigError> {
        Self::from_vars(
            std::env::var(ENV_SESSION_BACKEND).ok().as_deref(),
            std::env::var(ENV_STATE_BACKEND).ok().as_deref(),
            std::env::var(ENV_REDIS_URL).ok().as_deref(),
            std::env::var(ENV_SESSION_NAMESPACE).ok().as_deref(),
            std::env::var(ENV_GREENTIC_ENV).ok().as_deref(),
        )
    }

    /// The pure half of [`from_env`](Self::from_env), so the precedence is
    /// testable without touching process-global state.
    pub fn from_vars(
        session_backend: Option<&str>,
        state_backend: Option<&str>,
        redis_url: Option<&str>,
        session_namespace: Option<&str>,
        greentic_env: Option<&str>,
    ) -> Result<Self, StorageConfigError> {
        let url = redis_url.map(str::trim).filter(|value| !value.is_empty());

        let session = match named(session_backend) {
            None | Some("memory") => SessionBackend::InMemory,
            Some("redis") => {
                let url = url.ok_or(StorageConfigError::MissingRedisUrl { store: "session" })?;
                let namespace = match named(session_namespace) {
                    Some(explicit) => explicit.to_string(),
                    None => SessionBackend::namespace_for_env(
                        named(greentic_env).ok_or(StorageConfigError::MissingSessionNamespace)?,
                    )?,
                };
                SessionBackend::redis(url, namespace)?
            }
            Some(other) => {
                return Err(StorageConfigError::UnknownBackend {
                    store: "session",
                    value: other.to_string(),
                });
            }
        };

        let state = match named(state_backend) {
            None | Some("memory") => StateBackend::InMemory,
            Some("redis") => {
                let url = url.ok_or(StorageConfigError::MissingRedisUrl { store: "state" })?;
                StateBackend::redis(url)?
            }
            Some(other) => {
                return Err(StorageConfigError::UnknownBackend {
                    store: "state",
                    value: other.to_string(),
                });
            }
        };

        Ok(Self { session, state })
    }

    /// `true` when either store performs network I/O.
    pub fn is_durable(&self) -> bool {
        self.session.is_durable() || self.state.is_durable()
    }
}

fn named(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_is_both_stores_in_memory() {
        let config = StorageConfig::default();
        assert_eq!(config.session, SessionBackend::InMemory);
        assert_eq!(config.state, StateBackend::InMemory);
        assert!(!config.is_durable());
    }

    #[test]
    fn an_unnamed_backend_stays_in_memory_even_when_a_url_is_present() {
        // Deliberately unlike `aw_backends::select_state_backend`, which
        // auto-selects Redis from a bare URL. A URL appearing in the
        // environment for some other reason must not move a host's sessions.
        let config = StorageConfig::from_vars(None, None, Some("redis://x"), None, Some("prod"))
            .expect("resolve");
        assert_eq!(config, StorageConfig::in_memory());
    }

    #[test]
    fn explicit_redis_without_a_url_is_refused_not_disabled() {
        let err = StorageConfig::from_vars(Some("redis"), None, None, Some("ns"), None)
            .expect_err("must refuse");
        assert_eq!(
            err,
            StorageConfigError::MissingRedisUrl { store: "session" }
        );

        let err = StorageConfig::from_vars(None, Some("redis"), None, None, None)
            .expect_err("must refuse");
        assert_eq!(err, StorageConfigError::MissingRedisUrl { store: "state" });
    }

    #[test]
    fn an_empty_url_reads_as_absent_rather_than_as_a_host_named_nothing() {
        let err = StorageConfig::from_vars(Some("redis"), None, Some("   "), Some("ns"), None)
            .expect_err("must refuse");
        assert_eq!(
            err,
            StorageConfigError::MissingRedisUrl { store: "session" }
        );
    }

    #[test]
    fn a_session_redis_backend_without_a_resolvable_namespace_is_refused() {
        let err = StorageConfig::from_vars(Some("redis"), None, Some("redis://x"), None, None)
            .expect_err("must refuse");
        assert_eq!(err, StorageConfigError::MissingSessionNamespace);
    }

    #[test]
    fn the_namespace_falls_back_to_the_environment_name() {
        let config =
            StorageConfig::from_vars(Some("redis"), None, Some("redis://x"), None, Some("prod"))
                .expect("resolve");
        assert_eq!(
            config.session,
            SessionBackend::Redis {
                url: "redis://x".into(),
                namespace: "greentic:session:prod".into(),
            }
        );
    }

    #[test]
    fn an_explicit_namespace_beats_the_derived_one() {
        let config = StorageConfig::from_vars(
            Some("redis"),
            None,
            Some("redis://x"),
            Some("acme:sessions"),
            Some("prod"),
        )
        .expect("resolve");
        assert_eq!(
            config.session,
            SessionBackend::Redis {
                url: "redis://x".into(),
                namespace: "acme:sessions".into(),
            }
        );
    }

    #[test]
    fn two_environments_derive_different_namespaces() {
        let prod = SessionBackend::namespace_for_env("prod").expect("prod");
        let staging = SessionBackend::namespace_for_env("staging").expect("staging");
        assert_ne!(prod, staging);
    }

    #[test]
    fn an_unknown_backend_name_is_refused_rather_than_guessed() {
        let err = StorageConfig::from_vars(Some("cassandra"), None, None, None, None)
            .expect_err("must refuse");
        assert_eq!(
            err,
            StorageConfigError::UnknownBackend {
                store: "session",
                value: "cassandra".into(),
            }
        );
    }

    #[test]
    fn a_description_never_carries_the_connection_url() {
        let backend = SessionBackend::redis("redis://user:hunter2@redis:6379", "ns").expect("cfg");
        let described = backend.describe();
        assert!(
            !described.contains("hunter2"),
            "credential leaked: {described}"
        );
        assert!(!described.contains("redis://"), "url leaked: {described}");
        assert!(described.contains("ns"));

        let state = StateBackend::redis("redis://user:hunter2@redis:6379").expect("cfg");
        assert!(!state.describe().contains("hunter2"));
    }

    #[test]
    fn every_refusal_names_the_variable_that_fixes_it() {
        assert!(
            StorageConfigError::MissingRedisUrl { store: "state" }
                .to_string()
                .contains(ENV_REDIS_URL)
        );
        let namespace = StorageConfigError::MissingSessionNamespace.to_string();
        assert!(namespace.contains(ENV_SESSION_NAMESPACE));
        assert!(namespace.contains(ENV_GREENTIC_ENV));
    }
}
