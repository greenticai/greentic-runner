//! Fail-closed splitting of a URL into scheme, host and remainder.
//!
//! Shared by every place that must show a URL without its credentials
//! (`redact_locator`, `log_safe_origin`, the run-outcome redactor). The rule is
//! the same as `greentic_aw_runtime::url_redact::redact_http_url`: the
//! authority ends at the first `/`, `?` or `#`; an `@` anywhere AFTER that
//! point means the authority cannot be trusted (a password with a raw
//! delimiter, e.g. `https://u:pa/ss@host/`), so the host is unknown.

/// A URL split at its authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UrlParts<'a> {
    /// `scheme://`, as written.
    pub scheme_prefix: &'a str,
    /// Host (and port) with any userinfo removed. Empty when [`Self::ambiguous`].
    pub host: &'a str,
    /// Whether the authority carried userinfo.
    pub had_userinfo: bool,
    /// Everything from the first `/`, `?` or `#` on.
    pub remainder: &'a str,
    /// An `@` follows the authority, so the real host cannot be told apart
    /// from credential text. Callers must show no host at all.
    pub ambiguous: bool,
}

/// Length of a valid `scheme://` prefix (ALPHA *( ALPHA / DIGIT / "+" / "-" / "." )).
pub fn scheme_prefix_len(s: &str) -> Option<usize> {
    let idx = s.find("://")?;
    let mut chars = s[..idx].chars();
    let first_ok = chars.next().is_some_and(|c| c.is_ascii_alphabetic());
    let rest_ok = chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'));
    (first_ok && rest_ok).then_some(idx + 3)
}

/// Split `url`. `None` when it does not start with a valid `scheme://`.
pub fn split_url(url: &str) -> Option<UrlParts<'_>> {
    let prefix_len = scheme_prefix_len(url)?;
    let (scheme_prefix, rest) = url.split_at(prefix_len);
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let (authority, remainder) = rest.split_at(authority_end);
    if remainder.contains('@') {
        return Some(UrlParts {
            scheme_prefix,
            host: "",
            had_userinfo: true,
            remainder,
            ambiguous: true,
        });
    }
    let (host, had_userinfo) = match authority.rfind('@') {
        Some(at) => (&authority[at + 1..], true),
        None => (authority, false),
    };
    Some(UrlParts {
        scheme_prefix,
        host,
        had_userinfo,
        remainder,
        ambiguous: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_and_userinfo_urls_split() {
        let p = split_url("https://u:p@host:8443/a?x=1").expect("valid");
        assert_eq!(p.scheme_prefix, "https://");
        assert_eq!(p.host, "host:8443");
        assert!(p.had_userinfo && !p.ambiguous);
        assert_eq!(p.remainder, "/a?x=1");
        let p = split_url("http://store").expect("valid");
        assert!(!p.had_userinfo && p.remainder.is_empty());
    }

    #[test]
    fn at_after_authority_is_ambiguous() {
        for url in [
            "https://deploy:pa/ss@host/p.gtpack",
            "https://u:p?w@h/",
            "https://u:p#w@h/",
        ] {
            let p = split_url(url).expect("valid");
            assert!(p.ambiguous, "{url}");
            assert!(p.host.is_empty(), "{url}");
        }
    }

    #[test]
    fn invalid_scheme_is_rejected() {
        assert!(split_url("x/bot123:TOKEN?r=http://a").is_none());
        assert!(split_url("1http://a").is_none());
        assert!(split_url("://a").is_none());
        assert!(split_url("no-scheme").is_none());
    }
}
