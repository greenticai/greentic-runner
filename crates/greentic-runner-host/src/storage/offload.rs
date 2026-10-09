//! Moves a synchronous store call off the tokio worker that awaited it.
//!
//! Both backing traits — `greentic_session::SessionStore` and
//! `greentic_state::StateStore` — are SYNCHRONOUS, and both Redis
//! implementations are blocking `redis::Connection` calls (the state one behind
//! a `Mutex<Option<Connection>>`, so it also serialises). The runner's adapters
//! are `#[async_trait]`, so calling a store directly from one parks a tokio
//! worker thread on a socket for the duration of the round-trip. With the
//! default worker count that is one of N threads gone per concurrent turn.
//!
//! [`Offloaded`] is the only way this crate reaches a store from an async
//! context, and its only accessor is [`Offloaded::call`], which runs the closure
//! on [`tokio::task::spawn_blocking`]. The inner handle is private, so a future
//! maintainer cannot write `self.store.get_json(..)` inline in an async fn and
//! have it compile — the failure mode this type exists to prevent is silent
//! (nothing is red; throughput just collapses under load), so it is enforced by
//! the type rather than by a comment.
//!
//! `spawn_blocking` rather than [`tokio::task::block_in_place`]: the latter
//! panics when called from a `current_thread` runtime or inside a `LocalSet`,
//! and this is a library — it does not get to choose the embedder's runtime
//! flavour. `#[tokio::test]` alone is a `current_thread` runtime.
//!
//! The offload is unconditional, including for the in-memory default. It is one
//! task spawn per store call on a path that already instantiates WASM per node,
//! and making it conditional would mean threading a "is this backend blocking"
//! flag through every constructor between here and `HostBuilder` — a flag that,
//! set wrongly once, reintroduces exactly the invisible regression above.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use greentic_types::{ErrorCode, GResult, GreenticError};

/// Names the store in a degradation warning. Not an error type — it only ever
/// reaches a log line.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StoreKind {
    /// Parked-flow sessions.
    Session,
    /// Per-session flow state.
    State,
}

impl StoreKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Session => "session",
            Self::State => "state",
        }
    }
}

/// A store handle whose calls always run on the blocking pool.
pub(crate) struct Offloaded<T: ?Sized + Send + Sync + 'static> {
    inner: Arc<T>,
    kind: StoreKind,
    /// Whether the last call failed. Read and written only to decide the log
    /// level, never to decide whether a call is attempted.
    degraded: Arc<AtomicBool>,
}

impl<T: ?Sized + Send + Sync + 'static> Clone for Offloaded<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            kind: self.kind,
            degraded: Arc::clone(&self.degraded),
        }
    }
}

impl<T: ?Sized + Send + Sync + 'static> Offloaded<T> {
    pub(crate) fn new(inner: Arc<T>, kind: StoreKind) -> Self {
        Self {
            inner,
            kind,
            degraded: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Run `f` against the store on the blocking pool.
    ///
    /// A failing store is reported, not hidden: the error is returned to the
    /// caller so the turn fails honestly rather than reading as an empty
    /// session. What is suppressed is only the *repetition* — the first failure
    /// after a healthy call warns, and everything until the next success logs at
    /// debug. A store that comes back resets the notice, so a second outage is
    /// as loud as the first.
    pub(crate) async fn call<F, R>(&self, f: F) -> GResult<R>
    where
        F: for<'a> FnOnce(&'a T) -> GResult<R> + Send + 'static,
        R: Send + 'static,
    {
        let inner = Arc::clone(&self.inner);
        let joined = tokio::task::spawn_blocking(move || f(inner.as_ref())).await;

        let result = match joined {
            Ok(result) => result,
            Err(err) => Err(GreenticError::new(
                ErrorCode::Internal,
                format!(
                    "{} store call panicked or was cancelled: {err}",
                    self.kind.as_str()
                ),
            )),
        };

        match &result {
            Ok(_) => {
                if self.degraded.swap(false, Ordering::Relaxed) {
                    tracing::info!(
                        store = self.kind.as_str(),
                        "storage backend recovered; resuming normal operation"
                    );
                }
            }
            Err(err) => {
                if self.degraded.swap(true, Ordering::Relaxed) {
                    tracing::debug!(
                        store = self.kind.as_str(),
                        error = %err,
                        "storage backend still failing"
                    );
                } else {
                    tracing::warn!(
                        store = self.kind.as_str(),
                        error = %err,
                        "storage backend call failed; the host keeps serving and every \
                         affected turn reports the error rather than reading as an empty \
                         session"
                    );
                }
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::thread::ThreadId;

    struct ThreadProbe {
        seen: Mutex<Vec<ThreadId>>,
        fail: AtomicBool,
    }

    impl ThreadProbe {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                seen: Mutex::new(Vec::new()),
                fail: AtomicBool::new(false),
            })
        }

        fn record(&self) -> GResult<()> {
            if let Ok(mut seen) = self.seen.lock() {
                seen.push(std::thread::current().id());
            }
            if self.fail.load(Ordering::Relaxed) {
                return Err(GreenticError::new(ErrorCode::Internal, "probe failure"));
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn a_store_call_runs_on_a_different_thread_than_the_caller() {
        let probe = ThreadProbe::new();
        let handle = Offloaded::new(Arc::clone(&probe), StoreKind::State);
        let caller = std::thread::current().id();

        handle.call(|probe| probe.record()).await.expect("call");

        let seen = probe.seen.lock().expect("probe lock");
        assert_eq!(seen.len(), 1, "the closure must run exactly once");
        assert_ne!(
            seen[0], caller,
            "the store call ran on the awaiting thread; spawn_blocking was bypassed"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_offload_holds_on_a_multi_thread_runtime_too() {
        let probe = ThreadProbe::new();
        let handle = Offloaded::new(Arc::clone(&probe), StoreKind::Session);
        let caller = std::thread::current().id();

        handle.call(|probe| probe.record()).await.expect("call");

        let seen = probe.seen.lock().expect("probe lock");
        assert_ne!(seen[0], caller);
    }

    #[tokio::test]
    async fn a_failing_store_still_returns_its_error_to_the_caller() {
        let probe = ThreadProbe::new();
        probe.fail.store(true, Ordering::Relaxed);
        let handle = Offloaded::new(Arc::clone(&probe), StoreKind::State);

        // Twice: the second call takes the "still failing" branch. Neither may
        // swallow the error — degradation is reported, never absorbed.
        assert!(handle.call(|probe| probe.record()).await.is_err());
        assert!(handle.call(|probe| probe.record()).await.is_err());

        assert!(
            handle.degraded.load(Ordering::Relaxed),
            "a failing store must be marked degraded"
        );

        // A recovered store clears the notice, so the next outage warns again
        // instead of going out at debug forever.
        probe.fail.store(false, Ordering::Relaxed);
        assert!(handle.call(|probe| probe.record()).await.is_ok());
        assert!(
            !handle.degraded.load(Ordering::Relaxed),
            "recovery must rearm the warning"
        );
    }
}
