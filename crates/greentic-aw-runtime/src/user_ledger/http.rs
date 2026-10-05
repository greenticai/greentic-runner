//! HTTP client for the admin's user-ledger door (`{base}/read`, `{base}/append`).
//!
//! The client sends an end user's data with a bearer token, so: the endpoint
//! must be https (or loopback http), redirects are never followed, the token
//! travels only in the `Authorization` header and is redacted from `Debug`,
//! the response body is capped, and errors/logs carry status codes and static
//! text only (never the subject, a summary or any response body).

use std::sync::Mutex;
use std::time::Duration;

use secrecy::{ExposeSecret, SecretString};
use serde_json::json;
use tokio::time::Instant;
use tracing::warn;
use url::Url;

use super::{LedgerError, LedgerEvent, LedgerFuture, UserLedger};

/// Per-request ceiling, just UNDER the turn's read budget (`READ_TIMEOUT`,
/// 1.5 s) so the client sees its own timeout, and counts it for the breaker,
/// before the turn gives up waiting.
const REQUEST_TIMEOUT: Duration = Duration::from_millis(1200);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(1);
/// How long a `401`/`403` stops every call.
const REFUSAL_SUSPEND: Duration = Duration::from_secs(300);
/// Ceiling on a `Retry-After` the door sends with `429` (default when absent: 60 s).
const RATE_SUSPEND_MAX: Duration = Duration::from_secs(300);
/// Consecutive timeouts / transport errors / 5xx that open the breaker.
const BREAKER_THRESHOLD: u32 = 3;
/// How long an open breaker stops every call.
const BREAKER_SUSPEND: Duration = Duration::from_secs(30);
/// Largest response body read; the door's read answer is far below this.
const MAX_RESPONSE_BYTES: usize = 64 * 1024;

/// Consecutive-failure counter plus the suspension deadline shared by every
/// cause. Times are `tokio::time::Instant` so paused-time tests can drive it.
#[derive(Default)]
struct Breaker {
    state: Mutex<BreakerState>,
}

#[derive(Default)]
struct BreakerState {
    consecutive_failures: u32,
    suspended_until: Option<Instant>,
}

impl Breaker {
    fn lock(&self) -> std::sync::MutexGuard<'_, BreakerState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn suspended(&self, now: Instant) -> bool {
        self.lock().suspended_until.is_some_and(|until| now < until)
    }

    fn suspend_for(&self, now: Instant, d: Duration) {
        self.lock().suspended_until = Some(now + d);
    }

    /// One more timeout / transport error / 5xx; the third in a row suspends.
    fn record_failure(&self, now: Instant) {
        let mut state = self.lock();
        state.consecutive_failures += 1;
        if state.consecutive_failures >= BREAKER_THRESHOLD {
            state.consecutive_failures = 0;
            state.suspended_until = Some(now + BREAKER_SUSPEND);
        }
    }

    fn record_success(&self) {
        self.lock().consecutive_failures = 0;
    }
}

/// Why a [`UserLedgerTarget`] could not build a client. Never names the token
/// or the URL.
#[derive(Debug, thiserror::Error)]
pub enum UserLedgerTargetError {
    #[error("user ledger needs a non-blank `{0}`")]
    Blank(&'static str),
    #[error("user ledger base_url is not a valid absolute URL")]
    BadUrl,
    #[error(
        "user ledger endpoint is not https and not loopback http; refusing to send a bearer token in cleartext"
    )]
    UnsafeEndpoint,
    #[error("user ledger has no HTTP client: {0}")]
    Client(String),
}

/// The admin user-ledger door client (one per unit).
pub struct HttpUserLedger {
    /// The door base, any query kept; verbs are pushed as path segments.
    base: Url,
    token: SecretString,
    tenant_slug: String,
    http: reqwest::Client,
    breaker: Breaker,
}

impl std::fmt::Debug for HttpUserLedger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpUserLedger")
            .field("host", &self.base.host_str())
            .field("token", &"<redacted>")
            .field("tenant_slug", &self.tenant_slug)
            .finish()
    }
}

#[derive(serde::Deserialize)]
struct ReadResp {
    events: Vec<LedgerEvent>,
}

fn required(field: &'static str, value: &str) -> Result<String, UserLedgerTargetError> {
    let v = value.trim();
    if v.is_empty() {
        return Err(UserLedgerTargetError::Blank(field));
    }
    Ok(v.to_string())
}

impl HttpUserLedger {
    pub fn new(target: UserLedgerTarget) -> Result<Self, UserLedgerTargetError> {
        let raw = required("base_url", &target.base_url)?;
        let token = required("token", target.token.expose_secret())?;
        let tenant_slug = required("tenant_slug", &target.tenant_slug)?;
        let base = Url::parse(&raw).map_err(|_| UserLedgerTargetError::BadUrl)?;
        if !crate::billing::worker_usage::endpoint_is_safe(base.as_str()) {
            return Err(UserLedgerTargetError::UnsafeEndpoint);
        }
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .connect_timeout(CONNECT_TIMEOUT)
            // A redirect would resend the bearer token (and the user's data)
            // wherever the response points.
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| UserLedgerTargetError::Client(e.without_url().to_string()))?;
        Ok(Self {
            base,
            token: SecretString::from(token),
            tenant_slug,
            http,
            breaker: Breaker::default(),
        })
    }

    /// `{base}/{verb}` with the verb as a path segment: a trailing slash on
    /// the base is absorbed and a query on the base is kept after the path.
    fn door_url(&self, verb: &str) -> Result<Url, LedgerError> {
        let mut url = self.base.clone();
        url.path_segments_mut()
            .map_err(|()| LedgerError::Unavailable("base url cannot carry a path".into()))?
            .pop_if_empty()
            .push(verb);
        Ok(url)
    }

    async fn post(
        &self,
        verb: &str,
        body: serde_json::Value,
    ) -> Result<reqwest::Response, LedgerError> {
        if self.breaker.suspended(Instant::now()) {
            return Err(LedgerError::Suspended);
        }
        let url = self.door_url(verb)?;
        let resp = match self
            .http
            .post(url)
            .bearer_auth(self.token.expose_secret())
            .json(&body)
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(e) => {
                self.breaker.record_failure(Instant::now());
                // Static text only: a reqwest error can carry URL fragments.
                let what = if e.is_timeout() {
                    "request timed out"
                } else if e.is_connect() {
                    "could not connect"
                } else {
                    "transport error"
                };
                return Err(LedgerError::Unavailable(what.into()));
            }
        };
        let status = resp.status();
        if status.is_success() {
            self.breaker.record_success();
            return Ok(resp);
        }
        match status.as_u16() {
            // `403 tenant_mismatch` suspends like `purpose_not_granted`: both
            // are configuration errors no retry fixes within minutes.
            401 | 403 => {
                self.breaker.suspend_for(Instant::now(), REFUSAL_SUSPEND);
                warn!(
                    status = status.as_u16(),
                    "user ledger refused this unit's token (revoked, or the `ledger` purpose is \
                     not granted); suspended for 5 minutes"
                );
            }
            429 => {
                let secs = resp
                    .headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.trim().parse::<u64>().ok())
                    .unwrap_or(60);
                self.breaker.suspend_for(
                    Instant::now(),
                    Duration::from_secs(secs).min(RATE_SUSPEND_MAX),
                );
            }
            500..=599 => self.breaker.record_failure(Instant::now()),
            // The door answered: not a failure run. The body is never read.
            _ => self.breaker.record_success(),
        }
        Err(LedgerError::Refused(status.as_u16()))
    }
}

/// Read the body, refusing more than [`MAX_RESPONSE_BYTES`] without ever
/// buffering past the cap.
async fn read_capped(mut resp: reqwest::Response) -> Result<Vec<u8>, LedgerError> {
    if resp
        .content_length()
        .is_some_and(|n| n > MAX_RESPONSE_BYTES as u64)
    {
        return Err(LedgerError::Unavailable("response too large".into()));
    }
    let mut buf: Vec<u8> = Vec::new();
    loop {
        match resp.chunk().await {
            Ok(Some(chunk)) => {
                if buf.len() + chunk.len() > MAX_RESPONSE_BYTES {
                    return Err(LedgerError::Unavailable("response too large".into()));
                }
                buf.extend_from_slice(&chunk);
            }
            Ok(None) => return Ok(buf),
            Err(_) => return Err(LedgerError::Unavailable("response body unreadable".into())),
        }
    }
}

impl UserLedger for HttpUserLedger {
    fn read<'a>(&'a self, subject: &'a str, limit: u32) -> LedgerFuture<'a, Vec<LedgerEvent>> {
        Box::pin(async move {
            let body =
                json!({ "tenant_slug": self.tenant_slug, "subject": subject, "limit": limit });
            let resp = self.post("read", body).await?;
            let bytes = read_capped(resp).await?;
            let parsed: ReadResp = serde_json::from_slice(&bytes)
                .map_err(|_| LedgerError::Unavailable("response not understood".into()))?;
            Ok(parsed.events)
        })
    }

    fn append<'a>(
        &'a self,
        subject: &'a str,
        kind: &'a str,
        summary: &'a str,
    ) -> LedgerFuture<'a, ()> {
        Box::pin(async move {
            let body = json!({ "tenant_slug": self.tenant_slug, "subject": subject,
                               "kind": kind, "summary": summary });
            self.post("append", body).await.map(|_| ())
        })
    }
}

/// Where and as whom one unit reads and writes the user ledger. greentic-start
/// builds it from the unit's staged `metering {endpoint, token}` block:
/// `base_url` = the worker-usage endpoint with its last segment swapped for
/// `ledger` (`{admin}/api/v1/ingest/ledger`).
#[derive(Clone)]
pub struct UserLedgerTarget {
    /// `{admin}/api/v1/ingest/ledger` — the client appends `/read`, `/append`.
    pub base_url: String,
    /// The unit's `gtm_` worker-usage token. Only ever sent as a header.
    pub token: secrecy::SecretString,
    /// The workspace slug the token belongs to; the door refuses any other.
    pub tenant_slug: String,
}

impl std::fmt::Debug for UserLedgerTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UserLedgerTarget")
            .field("base_url", &self.base_url)
            .field("token", &"<redacted>")
            .field("tenant_slug", &self.tenant_slug)
            .finish()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::user_ledger::UserLedger;
    use serde_json::json;
    use wiremock::matchers::{body_json, header, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn target(base: &str) -> UserLedgerTarget {
        UserLedgerTarget {
            base_url: base.into(),
            token: "gtm_secret".into(),
            tenant_slug: "alpha".into(),
        }
    }

    fn ledger(server: &MockServer) -> HttpUserLedger {
        HttpUserLedger::new(target(&format!("{}/api/v1/ingest/ledger", server.uri()))).unwrap()
    }

    #[test]
    fn new_refuses_blank_fields_and_cleartext_off_loopback() {
        assert!(matches!(
            HttpUserLedger::new(target("not a url")),
            Err(UserLedgerTargetError::BadUrl)
        ));
        assert!(matches!(
            HttpUserLedger::new(target("http://admin.example/api/v1/ingest/ledger")),
            Err(UserLedgerTargetError::UnsafeEndpoint)
        ));
        assert!(HttpUserLedger::new(target("ftp://admin.example/x")).is_err());
        let mut t = target("https://admin.example/api/v1/ingest/ledger");
        t.token = " ".into();
        assert!(matches!(
            HttpUserLedger::new(t),
            Err(UserLedgerTargetError::Blank("token"))
        ));
        let mut t = target("https://admin.example/api/v1/ingest/ledger");
        t.tenant_slug = "".into();
        assert!(matches!(
            HttpUserLedger::new(t),
            Err(UserLedgerTargetError::Blank("tenant_slug"))
        ));
        assert!(HttpUserLedger::new(target("https://admin.example/api/v1/ingest/ledger/")).is_ok());
        assert!(HttpUserLedger::new(target("http://127.0.0.1:9/api/v1/ingest/ledger")).is_ok());
    }

    #[test]
    fn debug_and_errors_never_print_the_token_or_the_url_query() {
        let l = HttpUserLedger::new(target("https://admin.example/ledger?k=querysecret")).unwrap();
        let d = format!("{l:?}");
        assert!(
            !d.contains("gtm_secret") && !d.contains("querysecret"),
            "{d}"
        );
        assert!(!format!("{:?}", target("x")).contains("gtm_secret"));
        let e =
            HttpUserLedger::new(target("http://evil.example/ledger?k=querysecret")).unwrap_err();
        assert!(!format!("{e} {e:?}").contains("querysecret"));
    }

    #[tokio::test]
    async fn read_posts_the_contract_body_and_parses_events_tolerating_extra_fields() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v1/ingest/ledger/read"))
            .and(header("authorization", "Bearer gtm_secret"))
            .and(body_json(
                json!({ "tenant_slug": "alpha", "subject": "sub-1", "limit": 20 }),
            ))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({ "extra": 1, "events": [
                    { "event_id": "e1", "unit": "unit-1", "kind": "reply", "future": true,
                      "summary": "booked", "at": "2026-10-05T10:00:00Z" }
                ]})),
            )
            .expect(1)
            .mount(&server)
            .await;
        let events = ledger(&server).read("sub-1", 20).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].unit, "unit-1");
        assert_eq!(events[0].summary, "booked");
    }

    #[tokio::test]
    async fn a_query_on_the_base_url_is_kept_and_the_verb_lands_in_the_path() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v1/ingest/ledger/read"))
            .and(query_param("v", "1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "events": [] })))
            .expect(2)
            .mount(&server)
            .await;
        let with_query = format!("{}/api/v1/ingest/ledger?v=1", server.uri());
        let l = HttpUserLedger::new(target(&with_query)).unwrap();
        assert!(l.read("sub-1", 20).await.unwrap().is_empty());
        // A trailing slash is absorbed too.
        let l = HttpUserLedger::new(target(&format!(
            "{}/api/v1/ingest/ledger/?v=1",
            server.uri()
        )))
        .unwrap();
        assert!(l.read("sub-1", 20).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn append_posts_the_contract_body() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v1/ingest/ledger/append"))
            .and(header("authorization", "Bearer gtm_secret"))
            .and(body_json(
                json!({ "tenant_slug": "alpha", "subject": "sub-1",
                                   "kind": "reply", "summary": "done" }),
            ))
            .respond_with(ResponseTemplate::new(201).set_body_json(json!({
                "event_id": "e1", "at": "2026-10-05T10:00:00Z" })))
            .expect(1)
            .mount(&server)
            .await;
        ledger(&server)
            .append("sub-1", "reply", "done")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn a_refused_token_suspends_the_ledger() {
        for status in [401u16, 403] {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .respond_with(ResponseTemplate::new(status).set_body_json(json!({
                    "error": { "code": "purpose_not_granted", "message": "x" } })))
                .expect(1)
                .mount(&server)
                .await;
            let l = ledger(&server);
            assert!(
                matches!(l.read("sub-1", 20).await, Err(LedgerError::Refused(s)) if s == status)
            );
            // Suspended: the second call sends nothing (`.expect(1)` above).
            assert!(matches!(
                l.append("sub-1", "reply", "x").await,
                Err(LedgerError::Suspended)
            ));
        }
    }

    #[tokio::test]
    async fn a_rate_limit_suspends_for_retry_after_within_a_cap() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(429).insert_header("retry-after", "7"))
            .expect(1)
            .mount(&server)
            .await;
        let l = ledger(&server);
        assert!(matches!(
            l.read("s", 20).await,
            Err(LedgerError::Refused(429))
        ));
        assert!(matches!(l.read("s", 20).await, Err(LedgerError::Suspended)));
        // Retry-After honoured: ~7 s; a huge value is capped at 300 s.
        let until = l.breaker.lock().suspended_until.unwrap();
        let left = until - Instant::now();
        assert!(
            left <= Duration::from_secs(7) && left > Duration::from_secs(5),
            "{left:?}"
        );

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(429).insert_header("retry-after", "999999"))
            .mount(&server)
            .await;
        let l = ledger(&server);
        let _ = l.read("s", 20).await;
        let left = l.breaker.lock().suspended_until.unwrap() - Instant::now();
        assert!(left <= RATE_SUSPEND_MAX, "{left:?}");
    }

    #[tokio::test]
    async fn a_server_error_is_refused_and_one_or_two_do_not_suspend() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500))
            .expect(2)
            .mount(&server)
            .await;
        let l = ledger(&server);
        assert!(matches!(
            l.read("s", 20).await,
            Err(LedgerError::Refused(500))
        ));
        assert!(matches!(
            l.read("s", 20).await,
            Err(LedgerError::Refused(500))
        ));
    }

    /// Real HTTP: three 5xx in a row stop the client from asking (the fourth
    /// call sends nothing: `.expect(3)`).
    #[tokio::test]
    async fn three_server_errors_in_a_row_suspend_the_client() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(503))
            .expect(3)
            .mount(&server)
            .await;
        let l = ledger(&server);
        for _ in 0..3 {
            assert!(matches!(
                l.read("s", 20).await,
                Err(LedgerError::Refused(503))
            ));
        }
        assert!(matches!(l.read("s", 20).await, Err(LedgerError::Suspended)));
        assert!(matches!(
            l.append("s", "reply", "x").await,
            Err(LedgerError::Suspended)
        ));
    }

    /// Timeouts count toward the breaker too, and the error text is static.
    #[tokio::test]
    async fn three_timeouts_in_a_row_suspend_the_client() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(5)))
            .expect(3)
            .mount(&server)
            .await;
        let l = ledger(&server);
        for _ in 0..3 {
            match l.read("s", 20).await {
                Err(LedgerError::Unavailable(m)) => assert_eq!(m, "request timed out"),
                other => panic!("{other:?}"),
            }
        }
        assert!(matches!(l.read("s", 20).await, Err(LedgerError::Suspended)));
    }

    /// Other 4xx (the extractor's plain-text 400/413/415/422): refused, never
    /// suspended, and the body is never echoed anywhere.
    #[tokio::test]
    async fn other_4xx_is_refused_without_suspension_and_never_echoes_the_body() {
        for status in [400u16, 413, 415, 422] {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .respond_with(ResponseTemplate::new(status).set_body_string("LEAK-subject-sub-1"))
                .expect(2)
                .mount(&server)
                .await;
            let l = ledger(&server);
            for _ in 0..2 {
                let e = l.read("sub-1", 20).await.unwrap_err();
                assert!(matches!(e, LedgerError::Refused(s) if s == status));
                assert!(!format!("{e} {e:?}").contains("LEAK"));
            }
            // Many more would still be sent: no suspension, no breaker run.
            assert!(l.breaker.lock().suspended_until.is_none());
        }
    }

    /// A 302 to another host is neither followed nor does it carry the token.
    #[tokio::test]
    async fn a_redirect_is_not_followed_and_the_token_goes_nowhere_else() {
        let other = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "events": [] })))
            .expect(0)
            .mount(&other)
            .await;
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(307)
                    .insert_header("location", format!("{}/steal", other.uri()).as_str()),
            )
            .expect(2)
            .mount(&server)
            .await;
        let l = ledger(&server);
        assert!(matches!(
            l.read("s", 20).await,
            Err(LedgerError::Refused(307))
        ));
        assert!(matches!(
            l.append("s", "reply", "x").await,
            Err(LedgerError::Refused(307))
        ));
    }

    #[tokio::test]
    async fn an_oversized_response_is_refused_not_buffered() {
        // Declared length (content-length) over the cap.
        let server = MockServer::start().await;
        let big = "x".repeat(MAX_RESPONSE_BYTES + 1);
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_string(big.clone()))
            .mount(&server)
            .await;
        match ledger(&server).read("s", 20).await {
            Err(LedgerError::Unavailable(m)) => assert_eq!(m, "response too large"),
            other => panic!("{other:?}"),
        }
        // Just under the cap but a valid body still parses.
        let server = MockServer::start().await;
        let pad = "p".repeat(MAX_RESPONSE_BYTES - 100);
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({ "events": [], "pad": pad })),
            )
            .mount(&server)
            .await;
        assert!(ledger(&server).read("s", 20).await.unwrap().is_empty());
    }

    /// No `content-length`: a chunked body past the cap is cut off while
    /// streaming, never buffered whole.
    #[tokio::test]
    async fn a_chunked_oversized_response_is_refused_without_a_content_length() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                let mut buf = [0u8; 4096];
                let _ = sock.read(&mut buf).await;
                let _ = sock
                    .write_all(
                        b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\ncontent-type: application/json\r\n\r\n",
                    )
                    .await;
                let chunk = "x".repeat(8 * 1024);
                for _ in 0..16 {
                    let frame = format!("{:x}\r\n{chunk}\r\n", chunk.len());
                    if sock.write_all(frame.as_bytes()).await.is_err() {
                        return;
                    }
                }
                let _ = sock.write_all(b"0\r\n\r\n").await;
            }
        });
        let l =
            HttpUserLedger::new(target(&format!("http://{addr}/api/v1/ingest/ledger"))).unwrap();
        match l.read("s", 20).await {
            Err(LedgerError::Unavailable(m)) => assert_eq!(m, "response too large"),
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn an_unparseable_success_body_is_unavailable_with_static_text() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_string("LEAK not json"))
            .mount(&server)
            .await;
        let e = ledger(&server).read("s", 20).await.unwrap_err();
        assert!(!format!("{e}").contains("LEAK"));
    }

    /// The breaker itself, on paused tokio time.
    #[tokio::test(start_paused = true)]
    async fn the_breaker_suspends_after_three_failures_and_recovers() {
        let b = Breaker::default();
        let now = tokio::time::Instant::now();
        assert!(!b.suspended(now));
        b.record_failure(now);
        b.record_failure(now);
        assert!(!b.suspended(now), "two failures do not suspend");
        b.record_success();
        b.record_failure(now);
        b.record_failure(now);
        assert!(!b.suspended(now), "a success resets the run");
        b.record_failure(now);
        assert!(b.suspended(now), "the third consecutive failure suspends");
        tokio::time::advance(Duration::from_secs(29)).await;
        assert!(b.suspended(tokio::time::Instant::now()));
        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(!b.suspended(tokio::time::Instant::now()), "back after 30 s");
        let later = tokio::time::Instant::now();
        b.record_failure(later);
        assert!(!b.suspended(later), "a fresh run of three is needed");
    }
}
