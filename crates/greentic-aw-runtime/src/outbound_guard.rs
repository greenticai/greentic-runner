//! Outbound destination guard for clients that connect to a URL a pack (or
//! other tenant-supplied config) names.
//!
//! The classification and env policy are `runner_core::net_guard`, the same
//! code the host's `http-client` / `wasi:http` path uses:
//!
//! * link-local and cloud-metadata addresses are refused
//!   (`GREENTIC_HTTP_ALLOW_METADATA=1` disables that);
//! * loopback / private ranges stay allowed, because a local MCP or A2A server
//!   is legitimate (`GREENTIC_HTTP_DENY_PRIVATE=1` refuses them too).
//!
//! Two enforcement shapes, because two kinds of client exist:
//!
//! * a client WE build (`reqwest::ClientBuilder`) takes [`GuardedResolver`] and
//!   [`redirect_policy`], so the check runs on the addresses it really
//!   connects to and on every redirect hop;
//! * `greentic_mcp_client::McpHttpClient` builds its own `reqwest` client and
//!   exposes no seam, so [`check_endpoint`] resolves the host and validates
//!   every address just before the client is constructed. Residual, stated
//!   rather than hidden: that client re-resolves on connect (a DNS-rebinding
//!   answer that differs between the two lookups wins) and follows redirects
//!   unchecked. Closing both needs a resolver/redirect hook in
//!   `greentic-mcp-client`; egress-filter at the network layer if it matters.

use std::net::SocketAddr;

pub use runner_core::net_guard::{BLOCKED_MARKER, HttpGuard, is_blocked_error};

/// DNS resolver that refuses a name resolving to a blocked address.
pub struct GuardedResolver {
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
/// resolver) and keeps reqwest's 10-hop limit.
#[must_use]
pub fn redirect_policy(guard: HttpGuard) -> reqwest::redirect::Policy {
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

/// Validate `url`'s destination under `guard`: an IP-literal host is checked
/// directly, a domain is resolved and EVERY address must pass.
pub async fn check_endpoint_with(guard: &HttpGuard, url: &url::Url) -> Result<(), String> {
    guard.check_url_literal(url)?;
    if let Some(url::Host::Domain(name)) = url.host() {
        let port = url.port_or_known_default().unwrap_or(443);
        let addrs: Vec<SocketAddr> = tokio::net::lookup_host((name, port))
            .await
            .map_err(|err| format!("dns lookup failed for {name}: {err}"))?
            .collect();
        guard.check_addrs(&addrs)?;
    }
    Ok(())
}

/// [`check_endpoint_with`] under the env-configured policy.
pub async fn check_endpoint(url: &url::Url) -> Result<(), String> {
    check_endpoint_with(&HttpGuard::from_env(), url).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn guard() -> HttpGuard {
        HttpGuard::from_raw(None, None, None)
    }

    fn url(s: &str) -> Result<url::Url, url::ParseError> {
        url::Url::parse(s)
    }

    #[tokio::test]
    async fn metadata_and_link_local_literals_are_refused() -> Result<(), url::ParseError> {
        for target in [
            "http://169.254.169.254/latest/meta-data",
            "http://169.254.10.1:8080/mcp",
            "http://[fe80::1]/mcp",
            "http://[fd00:ec2::254]/mcp",
            "http://100.100.100.200/mcp",
        ] {
            let err = check_endpoint_with(&guard(), &url(target)?)
                .await
                .err()
                .unwrap_or_default();
            assert!(err.contains(BLOCKED_MARKER), "{target}: {err:?}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn loopback_and_private_stay_allowed_by_default() -> Result<(), url::ParseError> {
        for target in [
            "http://127.0.0.1:9000/mcp",
            "http://10.1.2.3/mcp",
            "http://localhost:9000/",
        ] {
            assert_eq!(
                check_endpoint_with(&guard(), &url(target)?).await,
                Ok(()),
                "{target}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn the_policy_flags_are_honoured() -> Result<(), url::ParseError> {
        let deny_private = HttpGuard::from_raw(None, Some("1"), None);
        assert!(
            check_endpoint_with(&deny_private, &url("http://127.0.0.1:9000/")?)
                .await
                .is_err()
        );
        let allow_meta = HttpGuard::from_raw(Some("1"), None, None);
        assert_eq!(
            check_endpoint_with(&allow_meta, &url("http://169.254.169.254/")?).await,
            Ok(())
        );
        Ok(())
    }

    #[tokio::test]
    async fn the_resolver_refuses_a_name_resolving_to_a_blocked_address() {
        // `localhost` resolves to loopback: refused only under deny-private,
        // which proves the resolver consults the policy on RESOLVED addresses.
        use reqwest::dns::{Name, Resolve};
        use std::str::FromStr;
        let resolver = GuardedResolver {
            guard: HttpGuard::from_raw(None, Some("1"), None),
        };
        let Ok(name) = Name::from_str("localhost") else {
            return;
        };
        assert!(resolver.resolve(name).await.is_err());

        let open = GuardedResolver { guard: guard() };
        let Ok(name) = Name::from_str("localhost") else {
            return;
        };
        assert!(open.resolve(name).await.is_ok());
    }
}
