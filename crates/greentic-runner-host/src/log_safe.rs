//! Helpers that make guest- or peer-supplied text safe to put in a host log
//! line: control characters (newlines, ANSI escapes) are replaced so one value
//! cannot forge extra log records, and the length is bounded so it cannot
//! flood the log.
//!
//! "Unsafe" is [`is_unsafe_log_char`]: C0/C1 controls plus the Unicode
//! characters that can forge or visually reorder a log line without being
//! `char::is_control` (line/paragraph separators, bidi controls, zero-width and
//! format characters, BOM).

use std::borrow::Cow;

/// Default cap for a guest-supplied identifier (secret key, correlation id,
/// tenant name, ...) re-emitted into a log line.
pub(crate) const DEFAULT_LOG_CHARS: usize = 256;

/// True for a character that must not reach a log line verbatim.
///
/// `char::is_control` alone misses U+2028/U+2029 (line/paragraph separators,
/// which many log viewers render as a line break), the bidi embedding,
/// override and isolate controls (U+202A-U+202E, U+2066-U+2069) that reorder
/// displayed text, and the zero-width / directional marks U+200B-U+200F and
/// U+FEFF that make two different values look identical.
pub(crate) fn is_unsafe_log_char(c: char) -> bool {
    c.is_control()
        || matches!(
            c,
            '\u{200B}'..='\u{200F}'
                | '\u{2028}'..='\u{2029}'
                | '\u{202A}'..='\u{202E}'
                | '\u{2066}'..='\u{2069}'
                | '\u{FEFF}'
        )
}

/// [`log_safe_max`] with the default cap.
pub(crate) fn log_safe(raw: &str) -> Cow<'_, str> {
    log_safe_max(raw, DEFAULT_LOG_CHARS)
}

/// Replace unsafe characters ([`is_unsafe_log_char`]) with a space and keep at most `max_chars`
/// characters (never splitting a multibyte character). Borrows when the input
/// needs no change.
pub(crate) fn log_safe_max(raw: &str, max_chars: usize) -> Cow<'_, str> {
    let clean = raw.chars().count() <= max_chars && !raw.chars().any(is_unsafe_log_char);
    if clean {
        return Cow::Borrowed(raw);
    }
    Cow::Owned(
        raw.chars()
            .take(max_chars)
            .map(|c| if is_unsafe_log_char(c) { ' ' } else { c })
            .collect(),
    )
}

/// A URL reduced to `scheme://host[:port]` for a host-level log line. Path,
/// query, fragment and userinfo are all dropped: the path itself can carry a
/// credential (`api.telegram.org/bot<TOKEN>/sendMessage`, Slack webhook paths),
/// which a path-preserving redaction would keep.
pub(crate) fn log_safe_origin(raw: &str) -> String {
    let Some((scheme, rest)) = raw.split_once("://") else {
        return "<non-url>".to_string();
    };
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    let host = authority.rsplit('@').next().unwrap_or_default();
    log_safe_max(&format!("{scheme}://{host}"), 128).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn origin_drops_path_query_and_userinfo() {
        assert_eq!(
            log_safe_origin("https://api.telegram.org/bot123:SECRET/sendMessage?x=1"),
            "https://api.telegram.org"
        );
        assert_eq!(
            log_safe_origin("http://u:p@localhost:8080/hook#f"),
            "http://localhost:8080"
        );
        assert_eq!(log_safe_origin("not a url"), "<non-url>");
    }

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
    fn unicode_line_separators_are_neutralised() {
        assert_eq!(log_safe("a\u{2028}b\u{2029}c"), "a b c");
    }

    #[test]
    fn bidi_controls_are_neutralised() {
        for c in ('\u{202A}'..='\u{202E}').chain('\u{2066}'..='\u{2069}') {
            let raw = format!("a{c}b");
            let out = log_safe(&raw);
            assert_eq!(out, "a b", "U+{:04X}", c as u32);
        }
    }

    #[test]
    fn zero_width_and_format_chars_are_neutralised() {
        for c in ('\u{200B}'..='\u{200F}').chain(['\u{FEFF}']) {
            let raw = format!("a{c}b");
            let out = log_safe(&raw);
            assert_eq!(out, "a b", "U+{:04X}", c as u32);
        }
    }

    #[test]
    fn ordinary_unicode_is_kept() {
        assert!(matches!(log_safe("日本語 é"), Cow::Borrowed(_)));
        assert!(!is_unsafe_log_char('\u{2027}'));
        assert!(!is_unsafe_log_char('\u{2030}'));
    }

    #[test]
    fn truncation_stays_on_char_boundary_with_unsafe_chars() {
        assert_eq!(log_safe_max("日\u{202E}本語日", 3), "日 本");
        assert_eq!(log_safe_max("é\u{2028}éé", 2), "é ");
    }
}
