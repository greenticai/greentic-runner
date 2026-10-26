//! Redaction of URLs for logs and error text.
//!
//! A broker URL such as `nats://user:pass@host:4222` carries its credential in
//! the userinfo part. Anything that logs the URL, or puts it in an error
//! context, must go through [`redact_url`] instead.

/// Replace the `user:password@` part of every URL in `url` with `[redacted]@`.
///
/// Fail-closed: NATS URLs carry no path, query or fragment, so once past the
/// scheme, everything up to the LAST `@` is treated as credential, whatever
/// characters it contains (`/`, `?`, `#`, `,` are legal unescaped in a
/// password as far as the NATS client is concerned). A comma list (the form
/// `async_nats::connect` takes) is only split on commas that are followed by a
/// `scheme://`; any other comma is assumed to belong to a password, so a list
/// with a scheme-less member is masked as one string. A URL without `@` is
/// returned unchanged, so the result still says which broker was meant.
pub fn redact_url(url: &str) -> String {
    let mut parts = Vec::new();
    let mut start = 0;
    for (i, _) in url.match_indices(',') {
        if scheme_len(&url[i + 1..]).is_some() {
            parts.push(redact_one(&url[start..i]));
            start = i + 1;
        }
    }
    parts.push(redact_one(&url[start..]));
    parts.join(",")
}

/// Reduce an HTTP(S) request URL to its ORIGIN for logs and error text:
/// `scheme://host[:port]`, plus `/…` when the URL had a path, query or
/// fragment. Fail-closed on every part that can carry a credential:
///
/// - the userinfo (basic auth) is masked as `[redacted]@`. Like [`redact_url`],
///   everything up to the LAST `@` after the scheme is credential, so a raw
///   `/`, `?` or `#` inside the password (`https://u:p?w@h/`) cannot end the
///   authority early and leak the front of it. The price is that an `@` in a
///   path or query (`/@me`, `?mail=a@b`) also reads as userinfo and the text
///   after it is shown as the host: wrong but never a leak.
/// - the path is dropped, not shown: webhook-style endpoints and some stores
///   embed a token in it (`/bot<TOKEN>/file`), and the query (presigned
///   signature) and fragment go the same way. The caller already names the
///   artifact it was fetching; the origin says which server failed.
pub fn redact_http_url(url: &str) -> String {
    let scheme_end = scheme_len(url).unwrap_or(0);
    let rest = &url[scheme_end..];
    let (masked, after_userinfo) = match rest.rfind('@') {
        Some(at) => (true, &rest[at + 1..]),
        None => (false, rest),
    };
    let origin_end = after_userinfo
        .find(['/', '?', '#'])
        .unwrap_or(after_userinfo.len());
    let remainder = &after_userinfo[origin_end..];
    let mut out = String::from(&url[..scheme_end]);
    if masked {
        out.push_str("[redacted]@");
    }
    out.push_str(&after_userinfo[..origin_end]);
    if !remainder.is_empty() && remainder != "/" {
        out.push_str("/…");
    }
    out
}

/// Length of the `scheme://` prefix of `s`, if it has a valid one.
fn scheme_len(s: &str) -> Option<usize> {
    let idx = s.find("://")?;
    let mut chars = s[..idx].chars();
    let first_ok = chars.next().is_some_and(|c| c.is_ascii_alphabetic());
    let rest_ok = chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'));
    (first_ok && rest_ok).then_some(idx + 3)
}

fn redact_one(part: &str) -> String {
    let scheme_end = scheme_len(part).unwrap_or(0);
    let rest = &part[scheme_end..];
    match rest.rfind('@') {
        Some(at) => format!("{}[redacted]{}", &part[..scheme_end], &rest[at..]),
        None => part.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_url_is_reduced_to_its_origin() {
        assert_eq!(
            redact_http_url("https://store.example/a/b.gtxpack?X-Amz-Signature=abc&x=1#frag"),
            "https://store.example/…"
        );
        assert_eq!(
            redact_http_url("https://user:pw@store.example:8443/a?token=t"),
            "https://[redacted]@store.example:8443/…"
        );
        assert_eq!(redact_http_url("http://store"), "http://store");
        assert_eq!(redact_http_url("http://store/"), "http://store");
        assert_eq!(redact_http_url("http://store?t=1"), "http://store/…");
    }

    #[test]
    fn http_url_path_embedded_secret_is_dropped() {
        let out = redact_http_url("https://api.example/bot123:SECRET/file.gtxpack");
        assert_eq!(out, "https://api.example/…");
        assert!(!out.contains("SECRET"));
    }

    #[test]
    fn http_url_raw_delimiters_in_userinfo_do_not_leak() {
        for (input, secret) in [
            ("https://u:p?w@h/", "p?w"),
            ("https://u:p#w@h/x", "p#w"),
            ("https://u:pa/ss@h:8443/x?y=1", "pa"),
            ("https://u:p@ss@h/x", "ss"),
            ("https://u:pa?ss@h", "ss"),
        ] {
            let out = redact_http_url(input);
            assert!(!out.contains(secret), "{out} leaks {secret}");
            assert!(out.starts_with("https://[redacted]@h"), "{out}");
        }
        assert_eq!(redact_http_url("https://u:p?w@h/"), "https://[redacted]@h");
    }

    #[test]
    fn http_url_at_sign_in_path_is_fail_closed() {
        let out = redact_http_url("https://h/users/@me?k=v");
        assert_eq!(out, "https://[redacted]@me/…");
    }

    #[test]
    fn userinfo_is_masked() {
        assert_eq!(
            redact_url("nats://user:pass@host:4222"),
            "nats://[redacted]@host:4222"
        );
        assert_eq!(redact_url("nats://token@host"), "nats://[redacted]@host");
    }

    #[test]
    fn urls_without_userinfo_are_unchanged() {
        assert_eq!(redact_url("nats://127.0.0.1:4222"), "nats://127.0.0.1:4222");
    }

    #[test]
    fn lists_and_schemeless_forms_are_masked() {
        assert_eq!(
            redact_url("nats://a:b@h1:4222,nats://h2:4222,nats://c:d@h3"),
            "nats://[redacted]@h1:4222,nats://h2:4222,nats://[redacted]@h3"
        );
        assert_eq!(redact_url("c:d@h3"), "[redacted]@h3");
        // Scheme-less member after a comma: fail closed, it is merged into the
        // preceding segment and everything up to its `@` is masked.
        assert_eq!(
            redact_url("nats://a:b@h1:4222,nats://h2:4222,c:d@h3"),
            "nats://[redacted]@h1:4222,nats://[redacted]@h3"
        );
    }

    #[test]
    fn password_containing_at_is_fully_masked() {
        let out = redact_url("nats://user:p@ss@host:4222");
        assert_eq!(out, "nats://[redacted]@host:4222");
        assert!(!out.contains("ss"));
    }

    fn assert_masked(input: &str, secret: &str, expected: &str) {
        let out = redact_url(input);
        assert_eq!(out, expected);
        assert!(!out.contains(secret), "{out} leaks {secret}");
    }

    #[test]
    fn raw_delimiters_in_password_are_masked() {
        assert_masked("nats://u:pa/ss@h:4222", "pa", "nats://[redacted]@h:4222");
        assert_masked("nats://u:pa?ss@h:4222", "pa", "nats://[redacted]@h:4222");
        assert_masked("nats://u:pa#ss@h:4222", "pa", "nats://[redacted]@h:4222");
        assert_masked("nats://u:pa,ss@h", "pa", "nats://[redacted]@h");
        assert_masked("nats://u:pa,ss@h", "ss", "nats://[redacted]@h");
    }

    #[test]
    fn ipv6_tls_and_empty_password_are_masked() {
        assert_masked(
            "nats://u:p@[::1]:4222",
            "u:p",
            "nats://[redacted]@[::1]:4222",
        );
        assert_eq!(redact_url("nats://[::1]:4222"), "nats://[::1]:4222");
        assert_masked(
            "nats+tls://u:secret@h:4222",
            "secret",
            "nats+tls://[redacted]@h:4222",
        );
        assert_masked("nats://user:@h:4222", "user", "nats://[redacted]@h:4222");
        assert_masked("user:pw@h:4222", "pw", "[redacted]@h:4222");
        assert_masked("u:p://x@h", "p:", "[redacted]@h");
    }
}
