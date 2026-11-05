//! The citation fields an index hit may carry — `title`, `source_url`,
//! `section` — and how they reach `RetrievedChunk.metadata`.
//!
//! They are a source's identity, not its content: every one is optional, each
//! is checked here again (the index server is not trusted to have done it) and
//! one that fails is left out rather than repaired. Chunk text is untouched
//! by this module, so the consumer's disclosure rules for text are unchanged.

use serde_json::{Map, Value};
use url::Url;

/// Metadata keys, shared with the designer's reader and greentic-start's
/// provenance output.
pub(super) const KEY_TITLE: &str = "title";
pub(super) const KEY_SOURCE_URL: &str = "source_url";
pub(super) const KEY_SECTION: &str = "section";

const MAX_TEXT_CHARS: usize = 300;
const MAX_URL_LEN: usize = 2048;

/// `Some(text)` for a non-blank value of at most 300 characters with no
/// control character. Over-long text is dropped, not truncated.
pub(super) fn checked_text(raw: Option<&str>) -> Option<String> {
    let raw = raw?.trim();
    let ok = !raw.is_empty()
        && raw.chars().count() <= MAX_TEXT_CHARS
        && !raw.chars().any(char::is_control);
    ok.then(|| raw.to_string())
}

/// `Some(link)` for an absolute `https` URL with a host, no credentials, no
/// control character and at most 2048 bytes.
pub(super) fn checked_url(raw: Option<&str>) -> Option<String> {
    let raw = raw?.trim();
    if raw.is_empty() || raw.len() > MAX_URL_LEN || raw.chars().any(char::is_control) {
        return None;
    }
    let url = Url::parse(raw).ok()?;
    let ok = url.scheme() == "https"
        && url.host_str().is_some()
        && url.username().is_empty()
        && url.password().is_none();
    ok.then(|| url.to_string())
}

/// Writes the checked fields into `metadata`, leaving absent ones out (never
/// `null`, so "unknown" stays distinguishable from "none").
pub(super) fn stamp(
    metadata: &mut Map<String, Value>,
    title: Option<&str>,
    source_url: Option<&str>,
    section: Option<&str>,
) {
    for (key, value) in [
        (KEY_TITLE, checked_text(title)),
        (KEY_SOURCE_URL, checked_url(source_url)),
        (KEY_SECTION, checked_text(section)),
    ] {
        if let Some(value) = value {
            metadata.insert(key.to_string(), Value::String(value));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_plain_https_link_survives() {
        assert_eq!(
            checked_url(Some("https://drive.google.com/f/1")).as_deref(),
            Some("https://drive.google.com/f/1")
        );
        for bad in [
            "",
            "http://e.com/a",
            "javascript:alert(1)",
            "data:text/html,x",
            "//e.com/a",
            "/relative",
            "https://u:p@e.com/a",
            "https://e.com/a\nb",
        ] {
            assert_eq!(checked_url(Some(bad)), None, "{bad}");
        }
        assert_eq!(
            checked_url(Some(&format!("https://e.com/{}", "a".repeat(2100)))),
            None
        );
        assert_eq!(checked_url(None), None);
    }

    #[test]
    fn text_is_bounded_trimmed_and_never_blank() {
        assert_eq!(checked_text(Some(" A > B ")).as_deref(), Some("A > B"));
        assert_eq!(checked_text(Some("  ")), None);
        assert_eq!(checked_text(Some("a\u{7}b")), None);
        assert!(checked_text(Some(&"x".repeat(300))).is_some());
        assert_eq!(checked_text(Some(&"x".repeat(301))), None);
    }

    #[test]
    fn stamp_writes_only_what_passed() {
        let mut m = Map::new();
        stamp(&mut m, Some("T"), Some("http://e.com"), None);
        assert_eq!(m.get(KEY_TITLE), Some(&Value::String("T".into())));
        assert!(!m.contains_key(KEY_SOURCE_URL));
        assert!(!m.contains_key(KEY_SECTION));
    }
}
