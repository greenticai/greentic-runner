//! Retry and concurrency rules shared by every client of the admin `artifacts`
//! door (master plan C3): the attachment reader here (`get`) and runner-host's
//! extension artifact port (`put`). greentic-start's door client applies the
//! same rules.
//!
//! - The door runs four transfers per admin process and refuses the rest
//!   (`503 artifact_busy`), so at most [`DOOR_CONCURRENCY`] requests per
//!   client are in flight; the slot is held until the caller has read the
//!   answer, because the transfer lasts until then.
//! - `408`, `429`, `502`, `503`, `504` and transport failures are retried,
//!   [`DOOR_ATTEMPTS`] attempts in all, waiting the door's `Retry-After` (whole
//!   seconds, at most [`MAX_RETRY_AFTER`]) or else a doubling backoff from
//!   [`FIRST_BACKOFF`]. Every other answer is final and handed back unread.
//! - A `get` reads and a `put` is content-addressed, so a retry stores nothing
//!   twice.
//!
//! Errors carry fixed sentences only: never a URL, a token or a response body.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Requests in flight per client.
pub const DOOR_CONCURRENCY: usize = 2;
/// Attempts per request, the first included.
pub const DOOR_ATTEMPTS: usize = 3;
/// The first wait between attempts; it doubles after each retry.
const FIRST_BACKOFF: Duration = Duration::from_millis(250);
/// Longest wait a door's `Retry-After` can impose.
const MAX_RETRY_AFTER: Duration = Duration::from_secs(3);

/// A request that never got an answer from the door, after every attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DoorSendError {
    #[error("artifact door request timed out")]
    Timeout,
    #[error("artifact door could not be reached")]
    Connect,
    #[error("artifact door request failed")]
    Failed,
    #[error("artifact door client is shutting down")]
    Closed,
}

/// The door's final answer, holding the client's concurrency slot until it is
/// dropped: read the body before dropping it.
pub struct DoorReply {
    pub response: reqwest::Response,
    _slot: OwnedSemaphorePermit,
}

/// Bounds and retries the requests of ONE door client. Clone-free: each client
/// owns one, so its slots are shared by every request that client sends.
#[derive(Debug)]
pub struct DoorRetry {
    slots: Arc<Semaphore>,
}

impl Default for DoorRetry {
    fn default() -> Self {
        Self::new()
    }
}

impl DoorRetry {
    pub fn new() -> Self {
        Self {
            slots: Arc::new(Semaphore::new(DOOR_CONCURRENCY)),
        }
    }

    /// Sends the request `build` makes (called once per attempt) until the door
    /// gives a final answer or the attempts run out. The last answer is
    /// returned even when it is a retryable status; only a transport failure on
    /// the last attempt is an error.
    pub async fn send<F>(&self, build: F) -> Result<DoorReply, DoorSendError>
    where
        F: Fn() -> reqwest::RequestBuilder,
    {
        let slot = Arc::clone(&self.slots)
            .acquire_owned()
            .await
            .map_err(|_| DoorSendError::Closed)?;
        let mut backoff = FIRST_BACKOFF;
        let mut attempt = 1;
        loop {
            let last = attempt >= DOOR_ATTEMPTS;
            let wait = match build().send().await {
                Ok(response) if last || !is_retryable(response.status()) => {
                    return Ok(DoorReply {
                        response,
                        _slot: slot,
                    });
                }
                Ok(response) => retry_after(response.headers()).unwrap_or(backoff),
                Err(err) if last => return Err(transport(&err)),
                Err(_) => backoff,
            };
            tokio::time::sleep(wait).await;
            backoff = backoff.saturating_mul(2);
            attempt += 1;
        }
    }
}

/// Whether the door may answer differently on another try.
pub fn is_retryable(status: reqwest::StatusCode) -> bool {
    matches!(status.as_u16(), 408 | 429 | 502 | 503 | 504)
}

fn transport(err: &reqwest::Error) -> DoorSendError {
    if err.is_timeout() {
        DoorSendError::Timeout
    } else if err.is_connect() {
        DoorSendError::Connect
    } else {
        DoorSendError::Failed
    }
}

/// The door's `Retry-After` in whole seconds, at most [`MAX_RETRY_AFTER`]. An
/// HTTP-date or anything else unreadable is ignored (the backoff applies).
fn retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    let seconds: u64 = headers
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse()
        .ok()?;
    Some(Duration::from_secs(seconds).min(MAX_RETRY_AFTER))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use reqwest::header::{HeaderMap, HeaderValue, RETRY_AFTER};

    fn headers(value: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(RETRY_AFTER, HeaderValue::from_str(value).unwrap());
        h
    }

    #[test]
    fn retry_after_reads_seconds_and_caps_them() {
        assert_eq!(retry_after(&headers("1")), Some(Duration::from_secs(1)));
        assert_eq!(retry_after(&headers(" 0 ")), Some(Duration::ZERO));
        assert_eq!(retry_after(&headers("60")), Some(MAX_RETRY_AFTER));
        assert_eq!(retry_after(&headers("Wed, 21 Oct 2015 07:28:00 GMT")), None);
        assert_eq!(retry_after(&headers("-1")), None);
        assert_eq!(retry_after(&HeaderMap::new()), None);
    }

    #[test]
    fn only_transient_statuses_are_retryable() {
        for s in [408u16, 429, 502, 503, 504] {
            assert!(
                is_retryable(reqwest::StatusCode::from_u16(s).unwrap()),
                "{s}"
            );
        }
        for s in [200u16, 307, 400, 401, 403, 404, 413, 415, 422, 500, 501] {
            assert!(
                !is_retryable(reqwest::StatusCode::from_u16(s).unwrap()),
                "{s}"
            );
        }
    }

    /// A transport failure on every attempt is a fixed error, after three tries.
    #[tokio::test]
    async fn a_door_that_never_answers_is_a_fixed_error() {
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let calls = std::sync::atomic::AtomicUsize::new(0);
        let started = std::time::Instant::now();
        let got = DoorRetry::new()
            .send(|| {
                calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                client.post("http://127.0.0.1:1/artifacts/get")
            })
            .await;
        assert_eq!(got.err(), Some(DoorSendError::Connect));
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            DOOR_ATTEMPTS
        );
        // 250 ms then 500 ms between the three attempts.
        assert!(started.elapsed() >= Duration::from_millis(740));
    }
}
