//! Outbound HTTP guard shared by the host `http-client` import and the
//! `wasi:http` path.
//!
//! Defaults (all can be relaxed/tightened by env):
//!
//! * **Always on**: link-local (`169.254.0.0/16`, `fe80::/10`) and the cloud
//!   metadata addresses (`169.254.169.254`, `fd00:ec2::254`,
//!   `100.100.100.200`) are refused. `GREENTIC_HTTP_ALLOW_METADATA=1` disables
//!   this block.
//! * Loopback, RFC1918, ULA and CGNAT stay allowed (in-cluster calls are
//!   legitimate). `GREENTIC_HTTP_DENY_PRIVATE=1` also refuses loopback,
//!   `0.0.0.0/8`, `10/8`, `172.16/12`, `192.168/16`, `100.64/10` and
//!   `fc00::/7`.
//! * `GREENTIC_HTTP_MAX_RESPONSE_MB` (default 64, `0` disables) caps a response
//!   body.
//! * `GREENTIC_WASI_HTTP_ENFORCE_POLICY=1` (default off) makes the `wasi:http`
//!   path honour a tenant's `http_enabled = false`. It is opt-in because
//!   `http_enabled` is derived from the presence of a `messaging` binding, so
//!   enforcing it by default would cut off wasi-http components of tenants
//!   that have none.
//!
//! DNS rebinding: for the host `http-client` import the check runs inside the
//! HTTP client's DNS resolver, i.e. on the addresses it actually connects to
//! (and redirect targets are re-checked). For `wasi:http`, the upstream
//! connector re-resolves the name on its own, so the name is resolved and
//! every address validated immediately before the connect; a hostile DNS
//! server that answers differently to two consecutive lookups can still win
//! that race. IP-literal hosts are exact in both paths. Deployments that need
//! a hard guarantee should also egress-filter at the network layer.

#[cfg(test)]
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::BodyExt;
use http_body_util::combinators::UnsyncBoxBody;
use hyper::body::{Body, Frame};
use wasmtime_wasi_http::p2::bindings::http::types::ErrorCode;
use wasmtime_wasi_http::p2::body::{HyperIncomingBody, HyperOutgoingBody};
use wasmtime_wasi_http::p2::types::{self, OutgoingRequestConfig};
use wasmtime_wasi_http::p2::{HttpResult, WasiHttpHooks, default_send_request_handler};

pub(crate) const WASI_ENFORCE_ENV: &str = "GREENTIC_WASI_HTTP_ENFORCE_POLICY";

// The pure policy lives in `runner_core::net_guard` so `greentic-aw-runtime`
// (MCP / A2A clients) applies the same rules; re-exported to keep call sites.
#[allow(unused_imports)]
pub(crate) use runner_core::net_guard::{
    ALLOW_METADATA_ENV, BLOCKED_MARKER, DEFAULT_MAX_RESPONSE_MB, DENY_PRIVATE_ENV, HttpGuard,
    IpClass, MAX_RESPONSE_MB_ENV, classify, is_blocked_error, truthy,
};

/// DNS resolver for the blocking `reqwest` client: validates the addresses it
/// is about to connect to.
pub(crate) struct GuardedResolver {
    pub guard: HttpGuard,
}

impl reqwest::dns::Resolve for GuardedResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let guard = self.guard;
        Box::pin(async move {
            let addrs: Vec<SocketAddr> =
                tokio::net::lookup_host((name.as_str(), 0)).await?.collect();
            guard
                .check_addrs(&addrs)
                .map_err(|reason| -> Box<dyn std::error::Error + Send + Sync> { reason.into() })?;
            let iter: reqwest::dns::Addrs = Box::new(addrs.into_iter());
            Ok(iter)
        })
    }
}

/// Redirect policy that re-checks IP-literal targets (domains go through the
/// resolver) and keeps reqwest's default 10-hop limit.
pub(crate) fn redirect_policy(guard: HttpGuard) -> reqwest::redirect::Policy {
    reqwest::redirect::Policy::custom(move |attempt| {
        if attempt.previous().len() >= 10 {
            return attempt.error("too many redirects");
        }
        match guard.check_url_literal(attempt.url()) {
            Ok(()) => attempt.follow(),
            Err(reason) => attempt.error(reason),
        }
    })
}

/// Body wrapper that fails once more than `limit` data bytes have been read.
pub(crate) struct LimitedBody<B> {
    inner: B,
    limit: u64,
    seen: u64,
}

impl<B> LimitedBody<B> {
    pub(crate) fn new(inner: B, limit: u64) -> Self {
        Self {
            inner,
            limit,
            seen: 0,
        }
    }
}

impl<B> Body for LimitedBody<B>
where
    B: Body<Data = Bytes, Error = ErrorCode> + Unpin,
{
    type Data = Bytes;
    type Error = ErrorCode;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = &mut *self;
        match Pin::new(&mut this.inner).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    this.seen = this.seen.saturating_add(data.len() as u64);
                    if this.seen > this.limit {
                        return Poll::Ready(Some(Err(ErrorCode::HttpResponseBodySize(Some(
                            this.seen,
                        )))));
                    }
                }
                Poll::Ready(Some(Ok(frame)))
            }
            other => other,
        }
    }
}

/// `wasi:http` hooks: policy gate, address guard, timeout ceiling, body cap.
pub(crate) struct GuardedHooks {
    guard: HttpGuard,
    ceiling: Duration,
    deny_all: bool,
}

impl GuardedHooks {
    /// `http_enabled` is the tenant's setting; it only denies when
    /// `GREENTIC_WASI_HTTP_ENFORCE_POLICY` is truthy.
    pub(crate) fn from_env(http_enabled: bool) -> Self {
        Self {
            guard: HttpGuard::from_env(),
            ceiling: crate::http_timeout_hooks::ceiling(),
            deny_all: wasi_denied(
                std::env::var(WASI_ENFORCE_ENV).ok().as_deref(),
                http_enabled,
            ),
        }
    }
}

/// Whether `wasi:http` is refused outright: only when the operator opted in
/// to enforcement AND the tenant has `http_enabled = false`.
pub(crate) fn wasi_denied(enforce_raw: Option<&str>, http_enabled: bool) -> bool {
    truthy(enforce_raw) && !http_enabled
}

fn clamp(config: OutgoingRequestConfig, ceiling: Duration) -> OutgoingRequestConfig {
    OutgoingRequestConfig {
        use_tls: config.use_tls,
        connect_timeout: config.connect_timeout.min(ceiling),
        first_byte_timeout: config.first_byte_timeout.min(ceiling),
        between_bytes_timeout: config.between_bytes_timeout.min(ceiling),
    }
}

impl WasiHttpHooks for GuardedHooks {
    fn send_request(
        &mut self,
        request: hyper::Request<HyperOutgoingBody>,
        config: OutgoingRequestConfig,
    ) -> HttpResult<types::HostFutureIncomingResponse> {
        if self.deny_all {
            return Err(ErrorCode::HttpRequestDenied.into());
        }
        let config = clamp(config, self.ceiling);
        let guard = self.guard;
        let authority = request.uri().authority().map(|a| a.to_string());
        let default_port = if config.use_tls { 443 } else { 80 };
        let handle = wasmtime_wasi::runtime::spawn(async move {
            if let Some(authority) = authority
                && let Err(reason) = guard.check_authority(&authority, default_port).await
            {
                tracing::warn!(%authority, %reason, "wasi-http request refused");
                return Ok(Err(ErrorCode::HttpRequestDenied));
            }
            let mut result = default_send_request_handler(request, config).await;
            if let (Ok(incoming), Some(limit)) = (&mut result, guard.max_response_bytes) {
                if let Some(len) = incoming
                    .resp
                    .headers()
                    .get(hyper::header::CONTENT_LENGTH)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<u64>().ok())
                    && len > limit
                {
                    return Ok(Err(ErrorCode::HttpResponseBodySize(Some(len))));
                }
                let (parts, body) =
                    std::mem::replace(&mut incoming.resp, hyper::Response::new(empty_body()))
                        .into_parts();
                let limited: HyperIncomingBody = UnsyncBoxBody::new(LimitedBody::new(body, limit));
                incoming.resp = hyper::Response::from_parts(parts, limited);
            }
            Ok(result)
        });
        Ok(types::HostFutureIncomingResponse::pending(handle))
    }
}

fn empty_body() -> HyperIncomingBody {
    http_body_util::Empty::<Bytes>::new()
        .map_err(|never| match never {})
        .boxed_unsync()
}

/// Read a response body, failing once it exceeds `limit` bytes (`None` =
/// unlimited). A declared `Content-Length` over the limit fails before reading.
pub(crate) fn read_capped_body(
    mut response: reqwest::blocking::Response,
    limit: Option<u64>,
) -> std::io::Result<Vec<u8>> {
    use std::io::Read;
    let Some(limit) = limit else {
        let mut buf = Vec::new();
        response.read_to_end(&mut buf)?;
        return Ok(buf);
    };
    let too_big = |what: String| {
        std::io::Error::other(format!("response body exceeds {limit} bytes ({what})"))
    };
    if let Some(len) = response.content_length()
        && len > limit
    {
        return Err(too_big(format!("content-length {len}")));
    }
    let mut buf = Vec::new();
    response
        .take(limit.saturating_add(1))
        .read_to_end(&mut buf)?;
    if buf.len() as u64 > limit {
        return Err(too_big("streamed".to_string()));
    }
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().expect("ip literal")
    }

    #[test]
    fn classifier_table() {
        use IpClass::*;
        let cases = [
            ("8.8.8.8", Public),
            ("2606:4700::1111", Public),
            ("127.0.0.1", Loopback),
            ("127.9.9.9", Loopback),
            ("::1", Loopback),
            ("0.0.0.0", Private),
            ("::", Private),
            ("10.1.2.3", Private),
            ("172.16.0.1", Private),
            ("172.31.255.255", Private),
            ("172.32.0.1", Public),
            ("192.168.1.1", Private),
            ("100.64.0.1", Private),
            ("100.127.255.255", Private),
            ("100.128.0.1", Public),
            ("fd12:3456::1", Private),
            ("fc00::1", Private),
            ("169.254.1.1", LinkLocal),
            ("169.254.169.254", Metadata),
            ("100.100.100.200", Metadata),
            ("fe80::1", LinkLocal),
            ("febf::1", LinkLocal),
            ("fec0::1", Public),
            ("fd00:ec2::254", Metadata),
            ("fd00:ec2::253", Private),
            // IPv4 embedded in IPv6.
            ("::ffff:169.254.169.254", Metadata),
            ("::ffff:127.0.0.1", Loopback),
            ("::ffff:10.0.0.1", Private),
            ("::ffff:8.8.8.8", Public),
            ("64:ff9b::a9fe:a9fe", Metadata),
            ("2002:a9fe:a9fe::1", Metadata),
            ("::a9fe:a9fe", Metadata),
        ];
        for (addr, want) in cases {
            assert_eq!(classify(ip(addr)), want, "{addr}");
        }
    }

    #[test]
    fn default_guard_blocks_link_local_and_metadata_only() {
        let g = HttpGuard::from_raw(None, None, None);
        for blocked in [
            "169.254.169.254",
            "169.254.0.5",
            "fe80::1",
            "fd00:ec2::254",
            "100.100.100.200",
            "::ffff:169.254.169.254",
        ] {
            assert!(g.check_ip(ip(blocked)).is_err(), "{blocked}");
        }
        for allowed in [
            "127.0.0.1",
            "10.0.0.1",
            "192.168.0.1",
            "fd00::1",
            "8.8.8.8",
            "0.0.0.0",
        ] {
            assert!(g.check_ip(ip(allowed)).is_ok(), "{allowed}");
        }
        let msg = g.check_ip(ip("169.254.169.254")).expect_err("blocked");
        assert!(msg.contains(BLOCKED_MARKER));
    }

    #[test]
    fn deny_private_and_allow_metadata_flags() {
        let strict = HttpGuard::from_raw(None, Some("1"), None);
        for blocked in [
            "127.0.0.1",
            "10.0.0.1",
            "172.20.0.1",
            "192.168.0.1",
            "100.64.0.1",
            "fd00::1",
            "::1",
            "0.0.0.0",
        ] {
            assert!(strict.check_ip(ip(blocked)).is_err(), "{blocked}");
        }
        assert!(strict.check_ip(ip("8.8.8.8")).is_ok());

        let relaxed = HttpGuard::from_raw(Some("true"), None, None);
        assert!(relaxed.check_ip(ip("169.254.169.254")).is_ok());
        assert!(relaxed.check_ip(ip("fe80::1")).is_ok());
        // Unrecognised values do not enable the flags.
        let junk = HttpGuard::from_raw(Some("maybe"), Some("0"), None);
        assert!(!junk.allow_metadata && !junk.deny_private);
    }

    #[test]
    fn nonstandard_ipv4_spellings_are_normalised_by_parsing() {
        let g = HttpGuard::from_raw(None, Some("1"), None);
        for spelled in [
            "http://2130706433/",         // decimal 127.0.0.1
            "http://0x7f.1/",             // hex / short form
            "http://0177.0.0.1/",         // octal
            "http://2852039166/",         // decimal 169.254.169.254
            "http://0xa9fea9fe/",         // hex 169.254.169.254
            "http://[::ffff:a9fe:a9fe]/", // v4-mapped metadata
            "http://[::1]/",
        ] {
            let url = url::Url::parse(spelled).expect(spelled);
            assert!(g.check_url_literal(&url).is_err(), "{spelled}");
        }
        let ok = url::Url::parse("http://example.com/").expect("url");
        assert!(g.check_url_literal(&ok).is_ok());
    }

    #[test]
    fn response_cap_parsing() {
        assert_eq!(
            HttpGuard::from_raw(None, None, None).max_response_bytes,
            Some(64 * 1024 * 1024)
        );
        assert_eq!(
            HttpGuard::from_raw(None, None, Some("0")).max_response_bytes,
            None
        );
        assert_eq!(
            HttpGuard::from_raw(None, None, Some("2")).max_response_bytes,
            Some(2 * 1024 * 1024)
        );
        assert_eq!(
            HttpGuard::from_raw(None, None, Some("x")).max_response_bytes,
            Some(64 * 1024 * 1024)
        );
    }

    #[test]
    fn blocked_error_is_detected_through_the_source_chain() {
        #[derive(Debug)]
        struct Outer(std::io::Error);
        impl std::fmt::Display for Outer {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "request failed")
            }
        }
        impl std::error::Error for Outer {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                Some(&self.0)
            }
        }
        let inner = std::io::Error::other(format!("x {BLOCKED_MARKER}"));
        assert!(is_blocked_error(&Outer(inner)));
        assert!(!is_blocked_error(&std::io::Error::other("timeout")));
    }

    #[tokio::test]
    async fn limited_body_enforces_the_cap() {
        use http_body_util::{BodyExt, Full};
        let make = |len: usize, limit: u64| {
            let inner: HyperIncomingBody = Full::new(Bytes::from(vec![0u8; len]))
                .map_err(|never| match never {})
                .boxed_unsync();
            UnsyncBoxBody::<Bytes, ErrorCode>::new(LimitedBody::new(inner, limit))
        };
        let ok = make(10, 10).collect().await.expect("within cap");
        assert_eq!(ok.to_bytes().len(), 10);
        let err = make(11, 10).collect().await.expect_err("over cap");
        assert!(matches!(err, ErrorCode::HttpResponseBodySize(Some(11))));
    }

    #[tokio::test]
    async fn authority_check_covers_literals_and_localhost_lookup() {
        let g = HttpGuard::from_raw(None, None, None);
        assert!(g.check_authority("169.254.169.254:80", 80).await.is_err());
        assert!(g.check_authority("2852039166", 80).await.is_err());
        assert!(g.check_authority("127.0.0.1:8080", 80).await.is_ok());
        let strict = HttpGuard::from_raw(None, Some("1"), None);
        assert!(strict.check_authority("127.0.0.1:8080", 80).await.is_err());
        assert!(strict.check_authority("localhost:8080", 80).await.is_err());
    }

    #[test]
    fn wasi_enforcement_is_opt_in() {
        assert!(
            !wasi_denied(None, false),
            "default: http_enabled=false is not enforced"
        );
        assert!(!wasi_denied(Some("1"), true));
        assert!(wasi_denied(Some("1"), false));
        assert!(wasi_denied(Some("true"), false));
    }

    fn serve_once(body: &'static [u8]) -> String {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        std::thread::spawn(move || {
            if let Ok((mut conn, _)) = listener.accept() {
                let mut buf = [0u8; 1024];
                let _ = conn.read(&mut buf);
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = conn.write_all(head.as_bytes());
                let _ = conn.write_all(body);
            }
        });
        format!("http://{addr}/")
    }

    #[test]
    fn read_capped_body_enforces_limit_and_zero_means_unlimited() {
        let client = reqwest::blocking::Client::builder()
            .no_proxy()
            .build()
            .expect("client");
        let get = |body: &'static [u8]| client.get(serve_once(body)).send().expect("send");
        let ok = read_capped_body(get(b"0123456789"), Some(10)).expect("at the cap");
        assert_eq!(ok.len(), 10);
        assert!(read_capped_body(get(b"0123456789A"), Some(10)).is_err());
        let all = read_capped_body(get(b"0123456789A"), None).expect("unlimited");
        assert_eq!(all.len(), 11);
    }
}
