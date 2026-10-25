//! Redaction of URLs for logs and error text.
//!
//! A broker URL such as `nats://user:pass@host:4222` carries its credential in
//! the userinfo part. Anything that logs the URL, or puts it in an error
//! context, must go through [`redact_url`] instead.

/// Replace the `user:password@` part of every URL in `url` with `[redacted]@`.
///
/// Accepts a comma-separated list (the form `async_nats::connect` takes) and
/// scheme-less `user:pass@host:port`. A URL without userinfo is returned
/// unchanged, so the result is still useful for diagnosing which broker was
/// meant.
pub fn redact_url(url: &str) -> String {
    url.split(',').map(redact_one).collect::<Vec<_>>().join(",")
}

fn redact_one(part: &str) -> String {
    let scheme_end = part.find("://").map_or(0, |i| i + 3);
    let rest = &part[scheme_end..];
    let authority_len = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    match rest[..authority_len].rfind('@') {
        Some(at) => format!("{}[redacted]{}", &part[..scheme_end], &rest[at..]),
        None => part.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(
            redact_url("nats://host:4222/path@x"),
            "nats://host:4222/path@x"
        );
    }

    #[test]
    fn lists_and_schemeless_forms_are_masked() {
        assert_eq!(
            redact_url("nats://a:b@h1:4222,nats://h2:4222,c:d@h3"),
            "nats://[redacted]@h1:4222,nats://h2:4222,[redacted]@h3"
        );
    }

    #[test]
    fn password_containing_at_is_fully_masked() {
        let out = redact_url("nats://user:p@ss@host:4222");
        assert_eq!(out, "nats://[redacted]@host:4222");
        assert!(!out.contains("ss"));
    }
}
