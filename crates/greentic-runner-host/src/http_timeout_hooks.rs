//! Host-configured ceiling on outbound `wasi:http` request timeouts.
//!
//! Without this, a component's outbound HTTP call is bounded only by
//! `wasmtime-wasi-http`'s own library default — 600 seconds per phase
//! (connect / first byte / between bytes) — so a hung remote server hangs
//! the whole flow for up to ten minutes. [`HttpTimeoutHooks`] clamps each
//! phase down to [`ceiling()`] before delegating to the library's own
//! [`default_send_request`], so a component's own shorter, explicitly-set
//! timeout is never raised — only an absent or longer one is lowered.
//!
//! This bounds a hung connection, not total transfer time: `connect_timeout`
//! and `first_byte_timeout` are each a single bound and this makes both
//! effectively fire within the ceiling, but `between_bytes_timeout` resets on
//! every chunk received, so a server that trickles one byte every N seconds
//! (N < ceiling) never trips it and can hold the connection open
//! indefinitely.

use std::time::Duration;

use wasmtime_wasi_http::p2::body::HyperOutgoingBody;
use wasmtime_wasi_http::p2::types::OutgoingRequestConfig;
use wasmtime_wasi_http::p2::{HttpResult, WasiHttpHooks, default_send_request, types};

/// Ceiling applied when no operator override is set. Far below the
/// library's own 600s-per-phase default — generous enough for a normal API
/// call, short enough that "the flow hangs forever" becomes "the flow fails
/// within half a minute."
pub(crate) const DEFAULT_HTTP_TIMEOUT: Duration = Duration::from_secs(30);

/// Operator override for [`DEFAULT_HTTP_TIMEOUT`], in whole seconds.
pub(crate) const HTTP_TIMEOUT_ENV: &str = "GREENTIC_HTTP_OUTBOUND_TIMEOUT_SECS";

/// Resolve the ceiling from an already-read env value. Pure so it is
/// testable without mutating the process environment. Anything unusable —
/// absent, unparseable, or zero — falls back to the default rather than
/// failing: a malformed knob must not make every outbound HTTP call
/// instantly fail, which is the opposite of what this feature exists to fix.
pub(crate) fn ceiling_from(raw: Option<&str>) -> Duration {
    raw.and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_HTTP_TIMEOUT)
}

/// The outbound HTTP timeout ceiling for this process.
pub(crate) fn ceiling() -> Duration {
    ceiling_from(std::env::var(HTTP_TIMEOUT_ENV).ok().as_deref())
}

/// [`WasiHttpHooks`] implementation that clamps every outbound request's
/// three timeout phases to a host ceiling before delegating to
/// [`default_send_request`] for everything else — the real async-level
/// `tokio::time::timeout` enforcement lives there, unchanged.
#[derive(Debug, Clone, Copy)]
pub(crate) struct HttpTimeoutHooks {
    ceiling: Duration,
}

impl HttpTimeoutHooks {
    /// Build from the process environment (reads [`HTTP_TIMEOUT_ENV`]).
    pub(crate) fn from_env() -> Self {
        Self { ceiling: ceiling() }
    }

    #[cfg(test)]
    fn with_ceiling(ceiling: Duration) -> Self {
        Self { ceiling }
    }
}

/// Clamp all three timeout phases to the given ceiling.
fn clamp_config(config: OutgoingRequestConfig, ceiling: Duration) -> OutgoingRequestConfig {
    OutgoingRequestConfig {
        use_tls: config.use_tls,
        connect_timeout: config.connect_timeout.min(ceiling),
        first_byte_timeout: config.first_byte_timeout.min(ceiling),
        between_bytes_timeout: config.between_bytes_timeout.min(ceiling),
    }
}

impl WasiHttpHooks for HttpTimeoutHooks {
    fn send_request(
        &mut self,
        request: hyper::Request<HyperOutgoingBody>,
        config: OutgoingRequestConfig,
    ) -> HttpResult<types::HostFutureIncomingResponse> {
        let clamped = clamp_config(config, self.ceiling);
        Ok(default_send_request(request, clamped))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ceiling_from_missing_env_falls_back_to_default() {
        assert_eq!(ceiling_from(None), DEFAULT_HTTP_TIMEOUT);
    }

    #[test]
    fn ceiling_from_unparseable_env_falls_back_to_default() {
        assert_eq!(ceiling_from(Some("not-a-number")), DEFAULT_HTTP_TIMEOUT);
    }

    #[test]
    fn ceiling_from_zero_falls_back_to_default() {
        assert_eq!(ceiling_from(Some("0")), DEFAULT_HTTP_TIMEOUT);
    }

    #[test]
    fn ceiling_from_valid_env_is_honored() {
        assert_eq!(ceiling_from(Some("45")), Duration::from_secs(45));
    }

    #[test]
    fn ceiling_from_env_trims_whitespace() {
        assert_eq!(ceiling_from(Some("  45  ")), Duration::from_secs(45));
    }

    #[test]
    fn clamp_config_lowers_a_longer_or_absent_field() {
        let ceiling = Duration::from_secs(10);
        // The library's own 600s-per-phase default, unmodified — the "absent
        // guest override" case.
        let config = OutgoingRequestConfig {
            use_tls: false,
            connect_timeout: Duration::from_secs(600),
            first_byte_timeout: Duration::from_secs(600),
            between_bytes_timeout: Duration::from_secs(600),
        };
        let clamped = clamp_config(config, ceiling);
        assert_eq!(clamped.connect_timeout, Duration::from_secs(10));
        assert_eq!(clamped.first_byte_timeout, Duration::from_secs(10));
        assert_eq!(clamped.between_bytes_timeout, Duration::from_secs(10));
    }

    #[test]
    fn a_guest_supplied_shorter_value_is_never_raised() {
        let ceiling = Duration::from_secs(30);
        let guest_value = Duration::from_secs(3);
        let config = OutgoingRequestConfig {
            use_tls: false,
            connect_timeout: guest_value,
            first_byte_timeout: guest_value,
            between_bytes_timeout: guest_value,
        };
        let clamped = clamp_config(config, ceiling);
        assert_eq!(
            clamped.connect_timeout, guest_value,
            "a 3s guest value must survive unchanged against a 30s ceiling"
        );
        assert_eq!(clamped.first_byte_timeout, guest_value);
        assert_eq!(clamped.between_bytes_timeout, guest_value);
    }

    #[test]
    fn a_guest_supplied_longer_value_is_lowered() {
        let ceiling = Duration::from_secs(30);
        let guest_value = Duration::from_secs(600);
        let config = OutgoingRequestConfig {
            use_tls: false,
            connect_timeout: guest_value,
            first_byte_timeout: guest_value,
            between_bytes_timeout: guest_value,
        };
        let clamped = clamp_config(config, ceiling);
        assert_eq!(
            clamped.connect_timeout, ceiling,
            "a 600s guest value must be lowered to the 30s ceiling"
        );
        assert_eq!(clamped.first_byte_timeout, ceiling);
        assert_eq!(clamped.between_bytes_timeout, ceiling);
    }

    fn test_request(addr: std::net::SocketAddr) -> hyper::Request<HyperOutgoingBody> {
        use http_body_util::BodyExt;
        hyper::Request::builder()
            .uri(format!("http://{addr}"))
            .body(
                http_body_util::Empty::<bytes::Bytes>::new()
                    .map_err(|_: std::convert::Infallible| unreachable!())
                    .boxed_unsync(),
            )
            .unwrap()
    }

    /// Proves both halves the review flagged: that `send_request` itself
    /// (the trait impl, not just `clamp_config`) applies the ceiling, and
    /// that it bounds real WALL-CLOCK time — not just eventually resolving.
    ///
    /// A TCP listener accepts the connection (so `connect_timeout` is not
    /// what fires) and then never writes a response, holding it open so
    /// `default_send_request` hangs waiting for a first byte. The `config`
    /// passed in carries the library's own 600s-per-phase default for every
    /// field, so if `send_request` ever forwarded `config` unclamped, this
    /// test would hang for minutes rather than complete in well under the 5s
    /// budget below — that gap is what makes this a real regression guard,
    /// not just a smoke test.
    #[tokio::test]
    async fn send_request_bounds_wall_time_to_the_ceiling() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("failed to bind an ephemeral localhost port");
        let addr = listener
            .local_addr()
            .expect("failed to read the bound local address");

        // Accept the connection and then hold it open forever without ever
        // writing a response byte. Aborted implicitly when the test's
        // #[tokio::test] runtime is torn down at the end of the test.
        tokio::spawn(async move {
            if let Ok((_stream, _peer)) = listener.accept().await {
                std::future::pending::<()>().await;
            }
        });

        let ceiling = Duration::from_millis(300);
        let mut hooks = HttpTimeoutHooks::with_ceiling(ceiling);
        let config = OutgoingRequestConfig {
            use_tls: false,
            connect_timeout: Duration::from_secs(600),
            first_byte_timeout: Duration::from_secs(600),
            between_bytes_timeout: Duration::from_secs(600),
        };
        let request = test_request(addr);

        let start = std::time::Instant::now();
        let response = hooks
            .send_request(request, config)
            .expect("send_request should not fail synchronously");
        let types::HostFutureIncomingResponse::Pending(handle) = response else {
            panic!("expected Pending — default_send_request always spawns");
        };
        let outer = handle.await;
        let elapsed = start.elapsed();

        // Generous enough to avoid flakiness in a slow sandbox, but two
        // orders of magnitude below the library's 600s default, so this
        // only passes if the 300ms ceiling — not the 600s config field —
        // actually bounded wall time.
        assert!(
            elapsed < Duration::from_secs(5),
            "expected the 300ms ceiling to bound wall time, took {elapsed:?}"
        );

        let inner = outer.expect(
            "expected a host-level Ok wrapping the guest-visible result, got a host-level Err",
        );
        assert!(
            inner.is_err(),
            "expected a first-byte-timeout failure, got a real response: {inner:?}"
        );
    }
}
