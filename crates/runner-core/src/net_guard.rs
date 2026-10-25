//! Outbound network policy shared by the host and the agentic-worker runtime.
//!
//! The pure IP classification, the env-driven policy ([`HttpGuard`]) and the
//! error-chain helper live here (the `reqwest` resolver adapters stay with each
//! crate because they implement the trait of their own `reqwest` major) so that
//! `greentic-runner-host` (`http-client`, `wasi:http`) and `greentic-aw-runtime`
//! (MCP, A2A) apply the SAME rules instead of two drifting copies.
//!
//! Defaults: link-local (`169.254/16`, `fe80::/10`) and cloud-metadata
//! addresses are refused (`GREENTIC_HTTP_ALLOW_METADATA=1` disables this);
//! loopback / RFC1918 / ULA / CGNAT stay allowed because in-cluster and local
//! servers are legitimate (`GREENTIC_HTTP_DENY_PRIVATE=1` also refuses them).

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

pub const ALLOW_METADATA_ENV: &str = "GREENTIC_HTTP_ALLOW_METADATA";
pub const DENY_PRIVATE_ENV: &str = "GREENTIC_HTTP_DENY_PRIVATE";
pub const MAX_RESPONSE_MB_ENV: &str = "GREENTIC_HTTP_MAX_RESPONSE_MB";
pub const DEFAULT_MAX_RESPONSE_MB: u64 = 64;

/// Marker embedded in every refusal message so callers can map it to `denied`.
pub const BLOCKED_MARKER: &str = "blocked by outbound http policy";

/// Coarse classification of a destination address.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpClass {
    Public,
    Loopback,
    /// RFC1918, ULA, CGNAT, `0.0.0.0/8`, `::`.
    Private,
    LinkLocal,
    /// Cloud instance-metadata endpoints.
    Metadata,
}

fn classify_v4(ip: Ipv4Addr) -> IpClass {
    let o = ip.octets();
    if ip == Ipv4Addr::new(169, 254, 169, 254) || ip == Ipv4Addr::new(100, 100, 100, 200) {
        IpClass::Metadata
    } else if o[0] == 169 && o[1] == 254 {
        IpClass::LinkLocal
    } else if o[0] == 127 {
        IpClass::Loopback
    } else if o[0] == 0
        || o[0] == 10
        || (o[0] == 172 && (16..=31).contains(&o[1]))
        || (o[0] == 192 && o[1] == 168)
        || (o[0] == 100 && (64..=127).contains(&o[1]))
    {
        IpClass::Private
    } else {
        IpClass::Public
    }
}

/// IPv4 address embedded in an IPv6 one that would be routed to it: v4-mapped
/// (`::ffff:a.b.c.d`), NAT64 (`64:ff9b::/96`), 6to4 (`2002::/16`) and the
/// deprecated v4-compatible form (`::a.b.c.d`).
fn embedded_v4(ip: Ipv6Addr) -> Option<Ipv4Addr> {
    let s = ip.segments();
    let v4 = |hi: u16, lo: u16| {
        let [a, b] = hi.to_be_bytes();
        let [c, d] = lo.to_be_bytes();
        Ipv4Addr::new(a, b, c, d)
    };
    if let Some(mapped) = ip.to_ipv4_mapped() {
        return Some(mapped);
    }
    if s[0] == 0x64 && s[1] == 0xff9b && s[2..6].iter().all(|x| *x == 0) {
        return Some(v4(s[6], s[7]));
    }
    if s[0] == 0x2002 {
        return Some(v4(s[1], s[2]));
    }
    if s[..6].iter().all(|x| *x == 0) && (s[6] != 0 || s[7] > 1) {
        return Some(v4(s[6], s[7]));
    }
    None
}

/// Classify an address, looking through IPv6-embedded IPv4.
pub fn classify(ip: IpAddr) -> IpClass {
    match ip {
        IpAddr::V4(v4) => classify_v4(v4),
        IpAddr::V6(v6) => {
            if v6 == Ipv6Addr::new(0xfd00, 0x0ec2, 0, 0, 0, 0, 0, 0x0254) {
                return IpClass::Metadata;
            }
            if let Some(v4) = embedded_v4(v6) {
                return classify_v4(v4);
            }
            let first = v6.segments()[0];
            if v6 == Ipv6Addr::LOCALHOST {
                IpClass::Loopback
            } else if v6 == Ipv6Addr::UNSPECIFIED || (first & 0xfe00) == 0xfc00 {
                IpClass::Private
            } else if (first & 0xffc0) == 0xfe80 {
                IpClass::LinkLocal
            } else {
                IpClass::Public
            }
        }
    }
}

/// Resolved outbound policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HttpGuard {
    pub allow_metadata: bool,
    pub deny_private: bool,
    /// `None` = unlimited.
    pub max_response_bytes: Option<u64>,
}

pub fn truthy(raw: Option<&str>) -> bool {
    matches!(
        raw.map(|v| v.trim().to_ascii_lowercase()).as_deref(),
        Some("1" | "true" | "yes" | "on")
    )
}

impl HttpGuard {
    pub fn from_raw(
        allow_metadata: Option<&str>,
        deny_private: Option<&str>,
        max_mb: Option<&str>,
    ) -> Self {
        let mb = max_mb
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(DEFAULT_MAX_RESPONSE_MB);
        Self {
            allow_metadata: truthy(allow_metadata),
            deny_private: truthy(deny_private),
            max_response_bytes: if mb == 0 {
                None
            } else {
                Some(mb.saturating_mul(1024 * 1024))
            },
        }
    }

    pub fn from_env() -> Self {
        Self::from_raw(
            std::env::var(ALLOW_METADATA_ENV).ok().as_deref(),
            std::env::var(DENY_PRIVATE_ENV).ok().as_deref(),
            std::env::var(MAX_RESPONSE_MB_ENV).ok().as_deref(),
        )
    }

    /// `Err(reason)` when `ip` may not be contacted.
    pub fn check_ip(&self, ip: IpAddr) -> Result<(), String> {
        let blocked = match classify(ip) {
            IpClass::Public => None,
            IpClass::Metadata | IpClass::LinkLocal => {
                (!self.allow_metadata).then_some("link-local or metadata address")
            }
            IpClass::Loopback | IpClass::Private => {
                self.deny_private.then_some("loopback or private address")
            }
        };
        match blocked {
            Some(what) => Err(format!("{what} {ip} {BLOCKED_MARKER}")),
            None => Ok(()),
        }
    }

    /// Every address must pass: a name with one blocked record is refused.
    pub fn check_addrs(&self, addrs: &[SocketAddr]) -> Result<(), String> {
        addrs.iter().try_for_each(|a| self.check_ip(a.ip()))
    }

    /// Check an IP-literal host (the URL parser has already normalised
    /// decimal/octal/hex IPv4 spellings). Domain hosts pass: they are checked
    /// when resolved.
    pub fn check_url_literal(&self, url: &url::Url) -> Result<(), String> {
        match url.host() {
            Some(url::Host::Ipv4(ip)) => self.check_ip(IpAddr::V4(ip)),
            Some(url::Host::Ipv6(ip)) => self.check_ip(IpAddr::V6(ip)),
            _ => Ok(()),
        }
    }

    /// Resolve and validate an `host[:port]` authority.
    pub async fn check_authority(&self, authority: &str, default_port: u16) -> Result<(), String> {
        let url = url::Url::parse(&format!("http://{authority}"))
            .map_err(|err| format!("invalid authority {authority:?}: {err}"))?;
        self.check_url_literal(&url)?;
        if let Some(url::Host::Domain(name)) = url.host() {
            let port = url.port().unwrap_or(default_port);
            let addrs: Vec<SocketAddr> = tokio::net::lookup_host((name, port))
                .await
                .map_err(|err| format!("dns lookup failed for {name}: {err}"))?
                .collect();
            self.check_addrs(&addrs)?;
        }
        Ok(())
    }
}

/// True when an error (or anything in its source chain) is one of ours.
pub fn is_blocked_error(err: &(dyn std::error::Error + 'static)) -> bool {
    let mut cur: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(e) = cur {
        if e.to_string().contains(BLOCKED_MARKER) {
            return true;
        }
        cur = e.source();
    }
    false
}
