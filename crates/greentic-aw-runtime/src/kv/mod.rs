//! Redis-shaped key-value abstraction backing the Redis-free AW state
//! backends. Domain adapters (state store, token meter, tool ledger, graph
//! checkpoint) are written once over `AwKv`; only the primitives differ per
//! backend (`MemoryKv`, `RedbKv`).

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use crate::error::StateError;

pub mod memory;
#[cfg(feature = "state-disk")]
pub mod redb;

pub use memory::MemoryKv;
#[cfg(feature = "state-disk")]
pub use redb::RedbKv;

/// Open an on-disk redb backend as a boxed `AwKv`. Feature-gated helper so
/// callers don't need to name `RedbKv` behind `#[cfg]`.
#[cfg(feature = "state-disk")]
pub fn open_disk(path: &std::path::Path) -> Result<std::sync::Arc<dyn AwKv>, StateError> {
    Ok(std::sync::Arc::new(RedbKv::open(path)?))
}

/// Boxed, `Send` future returned by every [`AwKv`] method (object-safe:
/// mirrors the `AgentStateStore` / `CheckpointStore` convention in this crate).
pub type KvFut<'a, T> = Pin<Box<dyn Future<Output = Result<T, StateError>> + Send + 'a>>;

/// A small Redis-shaped primitive set shared by the AW state concerns. All
/// operations are atomic within a single process for the bundled impls; they
/// are NOT distributed (see the single-process locking constraint in the spec).
pub trait AwKv: Send + Sync {
    /// Value for `key`, or `None` when absent or expired.
    fn get<'a>(&'a self, key: &'a str) -> KvFut<'a, Option<Vec<u8>>>;

    /// Set `key` to `val` with a fresh `ttl` (overwrites any existing value).
    fn set_ex<'a>(&'a self, key: &'a str, val: Vec<u8>, ttl: Duration) -> KvFut<'a, ()>;

    /// Delete `key` (no-op when absent).
    fn del<'a>(&'a self, key: &'a str) -> KvFut<'a, ()>;

    /// Set `key` to `val` with `ttl` only if absent. Returns `true` when the
    /// key was newly created (SET NX EX semantics).
    fn set_nx<'a>(&'a self, key: &'a str, val: Vec<u8>, ttl: Duration) -> KvFut<'a, bool>;

    /// Refresh `key`'s TTL to `ttl` only if its current value equals
    /// `expected`. Returns `true` when refreshed (lock still owned).
    fn compare_refresh<'a>(
        &'a self,
        key: &'a str,
        expected: &'a [u8],
        ttl: Duration,
    ) -> KvFut<'a, bool>;

    /// Delete `key` only if its current value equals `expected`. Returns
    /// `true` when deleted (lock released by the owner).
    fn compare_del<'a>(&'a self, key: &'a str, expected: &'a [u8]) -> KvFut<'a, bool>;

    /// Atomically add `delta` to the u64 counter at `key`, (re)setting `ttl`.
    /// Returns the new counter value. A missing/expired key starts at 0.
    fn incr_by<'a>(&'a self, key: &'a str, delta: u64, ttl: Duration) -> KvFut<'a, u64>;

    /// Read the u64 counter at `key`, or 0 when absent/expired.
    fn get_u64<'a>(&'a self, key: &'a str) -> KvFut<'a, u64> {
        Box::pin(async move {
            match self.get(key).await? {
                Some(bytes) => decode_u64(&bytes),
                None => Ok(0),
            }
        })
    }
}

/// Canonical counter encoding shared by `incr_by` / `get_u64` across backends:
/// 8-byte big-endian.
pub(crate) fn encode_u64(value: u64) -> Vec<u8> {
    value.to_be_bytes().to_vec()
}

pub(crate) fn decode_u64(bytes: &[u8]) -> Result<u64, StateError> {
    let arr: [u8; 8] = bytes
        .try_into()
        .map_err(|_| StateError::Decode(format!("counter not 8 bytes: len={}", bytes.len())))?;
    Ok(u64::from_be_bytes(arr))
}
