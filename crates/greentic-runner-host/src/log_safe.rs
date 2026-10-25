//! Helpers that make guest- or peer-supplied text safe to put in a host log
//! line: control characters (newlines, ANSI escapes) are replaced so one value
//! cannot forge extra log records, and the length is bounded so it cannot
//! flood the log.

use std::borrow::Cow;

/// Default cap for a guest-supplied identifier (secret key, correlation id,
/// tenant name, ...) re-emitted into a log line.
pub(crate) const DEFAULT_LOG_CHARS: usize = 256;

/// [`log_safe_max`] with the default cap.
pub(crate) fn log_safe(raw: &str) -> Cow<'_, str> {
    log_safe_max(raw, DEFAULT_LOG_CHARS)
}

/// Replace control characters with a space and keep at most `max_chars`
/// characters (never splitting a multibyte character). Borrows when the input
/// needs no change.
pub(crate) fn log_safe_max(raw: &str, max_chars: usize) -> Cow<'_, str> {
    let clean = raw.chars().count() <= max_chars && !raw.chars().any(char::is_control);
    if clean {
        return Cow::Borrowed(raw);
    }
    Cow::Owned(
        raw.chars()
            .take(max_chars)
            .map(|c| if c.is_control() { ' ' } else { c })
            .collect(),
    )
}

/// A URL for a log line: userinfo and query are redacted by the shared
/// credential redactor, then the result is made log-safe.
pub(crate) fn log_safe_url(raw: &str) -> String {
    let redacted = crate::run_outcome::error::redact(raw);
    log_safe(&redacted).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clean_input_is_borrowed() {
        assert!(matches!(log_safe("tenant-1"), Cow::Borrowed("tenant-1")));
    }

    #[test]
    fn newline_cannot_forge_a_record() {
        assert_eq!(log_safe("a\nWARN fake\r\nb"), "a WARN fake  b");
    }

    #[test]
    fn ansi_escape_is_neutralised() {
        let out = log_safe("x\u{1b}[31mred\u{1b}[0m");
        assert!(!out.contains('\u{1b}'));
        assert_eq!(out, "x [31mred [0m");
    }

    #[test]
    fn length_is_capped() {
        assert_eq!(log_safe_max(&"x".repeat(100), 10).chars().count(), 10);
        assert_eq!(
            log_safe(&"y".repeat(10_000)).chars().count(),
            DEFAULT_LOG_CHARS
        );
    }

    #[test]
    fn multibyte_is_not_split() {
        assert_eq!(log_safe_max("ééééé", 3), "ééé");
        assert_eq!(log_safe_max("日本語日本語", 4), "日本語日");
    }

    #[test]
    fn url_userinfo_and_query_are_stripped() {
        let out = log_safe_url("https://user:pw@example.com/p?token=abc\nfake");
        assert!(!out.contains("pw"));
        assert!(!out.contains("abc"));
        assert!(!out.contains('\n'));
        assert!(out.contains("example.com/p"));
    }
}
