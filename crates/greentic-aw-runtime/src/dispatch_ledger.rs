//! Dispatch-level idempotency ledger.
//!
//! JetStream at-least-once delivery redelivers a message whenever the consumer
//! crashes between `step()` completion and the JetStream `ack`. Without this
//! ledger, the LLM step would re-run, causing duplicate replies and wasted
//! credits.
//!
//! The ledger is keyed by `{tenant}:{env}:{dispatch_id}`, where `dispatch_id`
//! is the random per-publish `Greentic-Dispatch-Id` header (see
//! `serve::ledger_key`). It is NOT the correlation/idempotency hint: that is the
//! conversation's session hint and is the same for every turn, so keying on it
//! replayed turn 1's reply as turn 2's. A dispatch without the header is not
//! deduplicated. On a cache hit the
//! invoker short-circuits and returns the cached [`InvokeOutcome`] output
//! without calling [`AgentRuntime::step`] again.
//!
//! Pattern mirrors [`crate::tools::RedisToolLedger`] / [`crate::tools::ToolLedger`].

use std::future::Future;
use std::pin::Pin;

use redis::AsyncCommands;
use redis::aio::ConnectionManager;

use crate::error::StateError;

/// Dispatch-level idempotency ledger.
///
/// Dyn-safe (`Arc<dyn DispatchLedger>`); production uses [`RedisDispatchLedger`],
/// the serve-mode default is [`NoopDispatchLedger`], and tests use
/// [`InMemoryDispatchLedger`].
pub trait DispatchLedger: Send + Sync {
    /// Return the cached dispatch output for `key`, or `None` on a miss.
    fn get<'a>(
        &'a self,
        key: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<serde_json::Value>, StateError>> + Send + 'a>>;

    /// Persist the dispatch output for `key` so a redelivery returns it.
    fn record<'a>(
        &'a self,
        key: &'a str,
        output: serde_json::Value,
    ) -> Pin<Box<dyn Future<Output = Result<(), StateError>> + Send + 'a>>;
}

// ---------------------------------------------------------------------------
// Noop — used by `aw-serve` / existing callers of RuntimeAgentDispatchInvoker::new
// ---------------------------------------------------------------------------

/// No-op dispatch ledger: every `get` returns `None`, `record` is a no-op.
///
/// Used by the `serve` entry-point and unit tests that don't require
/// cross-redelivery caching.
pub struct NoopDispatchLedger;

impl DispatchLedger for NoopDispatchLedger {
    fn get<'a>(
        &'a self,
        _key: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<serde_json::Value>, StateError>> + Send + 'a>>
    {
        Box::pin(async { Ok(None) })
    }

    fn record<'a>(
        &'a self,
        _key: &'a str,
        _output: serde_json::Value,
    ) -> Pin<Box<dyn Future<Output = Result<(), StateError>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
}

// ---------------------------------------------------------------------------
// Redis — production implementation
// ---------------------------------------------------------------------------

/// TTL for a cached dispatch result (1 hour). Covers realistic JetStream
/// redelivery windows; shorter than the tool-call ledger (7 days) because
/// dispatch-level dedup is only needed while the original message is still
/// in-flight in the stream.
const DISPATCH_TTL_SECS: u64 = 3600;

fn dispatch_key(key: &str) -> String {
    format!("aw:dispatch:{key}")
}

/// Production dispatch ledger backed by a multiplexed Redis
/// [`ConnectionManager`].
///
/// Shares the Redis instance with [`crate::state_redis::RedisAgentStateStore`]
/// and [`crate::tools::RedisToolLedger`]. The manager is `Clone` (cheap,
/// reference-counted) so per-call clones open no new connections.
///
/// **Wiring note:** this ledger should be constructed in
/// `greentic-runner-host::agent_node::build_agent_runtime`, where a Redis
/// `ConnectionManager` is already available, and supplied via
/// `RuntimeAgentDispatchInvoker::with_ledger`. PR2 ships the seam and
/// defaults to `NoopDispatchLedger`; the production wiring is a follow-up.
pub struct RedisDispatchLedger {
    manager: ConnectionManager,
}

impl RedisDispatchLedger {
    pub fn new(manager: ConnectionManager) -> Self {
        Self { manager }
    }
}

impl DispatchLedger for RedisDispatchLedger {
    fn get<'a>(
        &'a self,
        key: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<serde_json::Value>, StateError>> + Send + 'a>>
    {
        Box::pin(async move {
            let redis_key = dispatch_key(key);
            let mut conn = self.manager.clone();
            let raw: Option<String> = conn
                .get(&redis_key)
                .await
                .map_err(|e| StateError::Redis(format!("dispatch ledger get: {e}")))?;
            match raw {
                Some(json) => {
                    let value: serde_json::Value = serde_json::from_str(&json)
                        .map_err(|e| StateError::Decode(format!("dispatch ledger decode: {e}")))?;
                    Ok(Some(value))
                }
                None => Ok(None),
            }
        })
    }

    fn record<'a>(
        &'a self,
        key: &'a str,
        output: serde_json::Value,
    ) -> Pin<Box<dyn Future<Output = Result<(), StateError>> + Send + 'a>> {
        Box::pin(async move {
            let redis_key = dispatch_key(key);
            let json = serde_json::to_string(&output)
                .map_err(|e| StateError::Decode(format!("dispatch ledger encode: {e}")))?;
            let mut conn = self.manager.clone();
            let _: () = conn
                .set_ex(&redis_key, json, DISPATCH_TTL_SECS)
                .await
                .map_err(|e| StateError::Redis(format!("dispatch ledger set_ex: {e}")))?;
            Ok(())
        })
    }
}

// ---------------------------------------------------------------------------
// InMemoryDispatchLedger — test helper
// ---------------------------------------------------------------------------

#[cfg(any(test, feature = "test-mock"))]
pub use in_memory::InMemoryDispatchLedger;

#[cfg(any(test, feature = "test-mock"))]
mod in_memory {
    use std::collections::HashMap;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Mutex;

    use crate::error::StateError;

    use super::DispatchLedger;

    /// In-memory dispatch ledger for tests.
    ///
    /// Stores entries in a `Mutex<HashMap>`. Can be pre-seeded via
    /// [`InMemoryDispatchLedger::with`] so a `get` returns a sentinel value
    /// on the first call, proving that `RuntimeAgentDispatchInvoker` does NOT
    /// call `step()` when a cache hit is found.
    pub struct InMemoryDispatchLedger {
        entries: Mutex<HashMap<String, serde_json::Value>>,
    }

    impl Default for InMemoryDispatchLedger {
        fn default() -> Self {
            Self {
                entries: Mutex::new(HashMap::new()),
            }
        }
    }

    impl InMemoryDispatchLedger {
        /// Pre-seed the ledger with one entry so the first `get(key)` returns
        /// `Some(value)` — simulating a redelivered dispatch that already has
        /// a cached result.
        pub fn with(key: &str, value: serde_json::Value) -> Self {
            let mut map = HashMap::new();
            map.insert(key.to_string(), value);
            Self {
                entries: Mutex::new(map),
            }
        }

        /// Inspect the stored entry (for post-call assertions).
        #[allow(clippy::expect_used)]
        pub fn stored(&self, key: &str) -> Option<serde_json::Value> {
            self.entries
                .lock()
                .expect("InMemoryDispatchLedger mutex poisoned")
                .get(key)
                .cloned()
        }
    }

    impl DispatchLedger for InMemoryDispatchLedger {
        #[allow(clippy::expect_used)]
        fn get<'a>(
            &'a self,
            key: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<Option<serde_json::Value>, StateError>> + Send + 'a>>
        {
            let result = self
                .entries
                .lock()
                .expect("InMemoryDispatchLedger mutex poisoned")
                .get(key)
                .cloned();
            Box::pin(async move { Ok(result) })
        }

        #[allow(clippy::expect_used)]
        fn record<'a>(
            &'a self,
            key: &'a str,
            output: serde_json::Value,
        ) -> Pin<Box<dyn Future<Output = Result<(), StateError>> + Send + 'a>> {
            self.entries
                .lock()
                .expect("InMemoryDispatchLedger mutex poisoned")
                .insert(key.to_string(), output);
            Box::pin(async { Ok(()) })
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn noop_ledger_always_misses() {
        let ledger = NoopDispatchLedger;
        assert!(ledger.get("any-key").await.unwrap().is_none());
        ledger
            .record("any-key", serde_json::json!({"ok": true}))
            .await
            .unwrap();
        // Still a miss — noop never stores.
        assert!(ledger.get("any-key").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn in_memory_ledger_stores_and_retrieves() {
        let ledger = InMemoryDispatchLedger::default();
        assert!(ledger.get("k1").await.unwrap().is_none());
        ledger
            .record("k1", serde_json::json!({"reply": "hello"}))
            .await
            .unwrap();
        let got = ledger.get("k1").await.unwrap();
        assert_eq!(got, Some(serde_json::json!({"reply": "hello"})));
    }

    #[tokio::test]
    async fn in_memory_ledger_with_preseed_returns_sentinel_on_first_get() {
        let ledger = InMemoryDispatchLedger::with("k1", serde_json::json!({"reply": "CACHED"}));
        let got = ledger.get("k1").await.unwrap();
        assert_eq!(got, Some(serde_json::json!({"reply": "CACHED"})));
    }

    #[test]
    fn dispatch_key_format() {
        assert_eq!(
            dispatch_key("sess::pack=p::flow=f"),
            "aw:dispatch:sess::pack=p::flow=f"
        );
    }
}
