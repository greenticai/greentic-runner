//! Safe file-name stems for artifacts. Pure.

/// Longest stem kept, in characters (all ASCII after cleaning).
const MAX_STEM_CHARS: usize = 64;

fn is_edge(c: char) -> bool {
    c == '-' || c == '.'
}

/// Reduce a model-supplied name to `[A-Za-z0-9._-]`, at most 64 chars, never
/// empty, never starting or ending with `-` or `.` (so it cannot be a path,
/// a hidden file or a traversal segment). Every other character, including
/// path separators, controls and bidi overrides, becomes `-`.
#[must_use]
pub fn sanitize_stem(raw: Option<&str>, fallback: &str) -> String {
    let cleaned: String = raw
        .unwrap_or("")
        .trim()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '-'
            }
        })
        .collect();
    let capped: String = cleaned
        .trim_matches(is_edge)
        .chars()
        .take(MAX_STEM_CHARS)
        .collect();
    // Trim again: the cut may have landed right after a `.` or `-`.
    let stem = capped.trim_matches(is_edge);
    if stem.is_empty() {
        fallback.to_string()
    } else {
        stem.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::sanitize_stem;

    #[test]
    fn path_traversal_is_flattened() {
        assert_eq!(sanitize_stem(Some("../../etc/passwd"), "x"), "etc-passwd");
        assert_eq!(sanitize_stem(Some("..\\..\\win.ini"), "x"), "win.ini");
    }

    #[test]
    fn spaces_become_dashes() {
        assert_eq!(
            sanitize_stem(Some("Quarterly Report"), "x"),
            "Quarterly-Report"
        );
    }

    #[test]
    fn bidi_and_control_characters_are_replaced() {
        // U+202E RIGHT-TO-LEFT OVERRIDE would make "gpj.exe" render as "exe.jpg".
        assert_eq!(sanitize_stem(Some("a\u{202e}gpj.exe"), "x"), "a-gpj.exe");
        assert_eq!(sanitize_stem(Some("a\nb\0c"), "x"), "a-b-c");
    }

    #[test]
    fn empty_and_missing_use_the_fallback() {
        assert_eq!(sanitize_stem(None, "document"), "document");
        assert_eq!(sanitize_stem(Some("  "), "document"), "document");
        assert_eq!(sanitize_stem(Some("///"), "document"), "document");
    }

    #[test]
    fn length_is_capped() {
        assert_eq!(sanitize_stem(Some(&"a".repeat(100)), "x").len(), 64);
    }

    #[test]
    fn a_cut_never_leaves_a_trailing_dot_or_dash() {
        let raw = format!("{}.-b", "a".repeat(62));
        let s = sanitize_stem(Some(&raw), "x");
        assert_eq!(s, "a".repeat(62));
    }
}
