//! The technical-error excerpt a v2 run-outcome event may carry (wire
//! contract v2 §2): a support reference, a summary built from identifiers
//! only, and the error chain — redacted and capped — for the admin's
//! short-lived `run_errors` table.
//!
//! # What is and is not sent
//!
//! [`ErrorExcerpt::safe_summary`] is built from the node id, the error class
//! and the retry count, and nothing else — it is what a viewer WITHOUT log
//! permission may be shown. [`ErrorExcerpt::redacted_excerpt`] is the error's
//! own text (every `source()` joined with `"\ncaused by: "`), passed through
//! [`redact`] before it leaves the process and cut to [`MAX_EXCERPT_BYTES`].
//! The top-level `error_code` still never carries text.
//!
//! # Redaction
//!
//! greentic-runner had no shared string redactor to reuse (the desktop
//! runner's `redact_value` masks JSON by KEY, which an error string does not
//! have), so this module owns one. It replaces with `[redacted]`:
//!
//! - `secrets://…` / `secret://…` URIs, whole;
//! - the credential after `Bearer ` / `Basic ` (any case);
//! - the value of an `Authorization:` header, to the end of the line;
//! - the query string and any `user:pass@` userinfo of a `scheme://` URL
//!   (scheme, host and path are kept, so a support engineer can still tell
//!   WHICH service failed);
//! - the value of a `key=value` / `key: value` / `"key": "value"` pair whose
//!   key names a credential (`token`, `password`, `secret`, `api_key`, …).
//!
//! It is a best-effort filter over free text, not a guarantee: the excerpt is
//! still stored only in the admin's permission-gated, 14-day `run_errors`
//! table, which re-applies its own pass.

use serde::Serialize;

/// Largest `redacted_excerpt` sent, INCLUDING [`TRUNCATION_MARKER`].
pub(crate) const MAX_EXCERPT_BYTES: usize = 64 * 1024;

/// Appended to an excerpt that was cut.
pub(crate) const TRUNCATION_MARKER: &str = "\n…[truncated]";

/// Largest `safe_summary` sent.
pub(crate) const MAX_SUMMARY_BYTES: usize = 512;

/// Joins the `Display` of each error in a chain.
const CAUSED_BY: &str = "\ncaused by: ";

/// What every redacted span is replaced with.
const REDACTED: &str = "[redacted]";

/// Keys whose value is a credential when they appear as `key=value`,
/// `key: value` or `"key": "value"`. Matched case-insensitively as a whole
/// key (so `token` matches `access_token`'s suffix only through its own entry).
const SENSITIVE_KEYS: &[&str] = &[
    "access_token",
    "refresh_token",
    "id_token",
    "token",
    "api_key",
    "apikey",
    "api-key",
    "x-api-key",
    "client_secret",
    "secret",
    "password",
    "passwd",
    "pwd",
    "private_key",
    "session",
    "cookie",
    "set-cookie",
    "signature",
    "sig",
];

/// The `error` object of a v2 run-outcome event. Serialised `snake_case`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ErrorExcerpt {
    /// ULID; identical to the event's top-level `error_ref`.
    pub error_ref: String,
    /// The same short class as the event's top-level `error_code`.
    pub error_code: String,
    /// ≤ [`MAX_SUMMARY_BYTES`], from node id + class + retry count only.
    pub safe_summary: String,
    /// The redacted error chain, ≤ [`MAX_EXCERPT_BYTES`] including the marker.
    pub redacted_excerpt: String,
    /// `true` when the excerpt was cut; it then ends with [`TRUNCATION_MARKER`].
    pub truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_count: Option<u32>,
}

impl ErrorExcerpt {
    /// Build the excerpt for one technical error. `chain` is the error text
    /// BEFORE redaction; it is redacted and capped here and nowhere else.
    pub(crate) fn build(
        error_ref: &str,
        error_code: &str,
        node_id: Option<&str>,
        retry_count: u32,
        chain: &str,
    ) -> Self {
        let (redacted_excerpt, truncated) = cap(redact(chain));
        Self {
            error_ref: error_ref.to_string(),
            error_code: error_code.to_string(),
            safe_summary: safe_summary(node_id, error_code, retry_count),
            redacted_excerpt,
            truncated,
            node_id: node_id
                .filter(|id| id.len() <= super::http::MAX_FIELD_BYTES)
                .map(str::to_string),
            retry_count: Some(retry_count),
        }
    }
}

/// A fresh support reference.
pub(crate) fn new_error_ref() -> String {
    ulid::Ulid::new().to_string()
}

/// The `Display` of every error in a chain, outermost first.
pub(crate) fn chain_text(error: &dyn std::error::Error) -> String {
    let mut parts = vec![error.to_string()];
    let mut source = error.source();
    while let Some(err) = source {
        parts.push(err.to_string());
        source = err.source();
    }
    parts.join(CAUSED_BY)
}

/// [`chain_text`] for an `anyhow::Error`, whose `chain()` already walks it.
pub(crate) fn anyhow_chain_text(error: &anyhow::Error) -> String {
    error
        .chain()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(CAUSED_BY)
}

/// `Node `<id>` failed after N retries: <class>`, or `Flow failed …` when no
/// node is known. Built from identifiers only; never from error text.
pub(crate) fn safe_summary(node_id: Option<&str>, error_code: &str, retry_count: u32) -> String {
    let retries = if retry_count == 1 { "retry" } else { "retries" };
    let tail = format!(" failed after {retry_count} {retries}: {error_code}");
    let summary = match node_id {
        Some(node) => {
            // Keep the whole sentence within the cap by shortening the id.
            let room = MAX_SUMMARY_BYTES.saturating_sub(tail.len() + "Node ``".len());
            format!("Node `{}`{tail}", truncate_at_char(node, room))
        }
        None => format!("Flow{tail}"),
    };
    truncate_at_char(&summary, MAX_SUMMARY_BYTES).to_string()
}

/// Cut `text` to [`MAX_EXCERPT_BYTES`] including the marker, on a char
/// boundary. Returns the text and whether it was cut.
pub(crate) fn cap(text: String) -> (String, bool) {
    if text.len() <= MAX_EXCERPT_BYTES {
        return (text, false);
    }
    let room = MAX_EXCERPT_BYTES - TRUNCATION_MARKER.len();
    let mut out = truncate_at_char(&text, room).to_string();
    out.push_str(TRUNCATION_MARKER);
    (out, true)
}

fn truncate_at_char(text: &str, max: usize) -> &str {
    if text.len() <= max {
        return text;
    }
    let mut end = max;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

/// Strip credentials from free text. See the module doc for the set.
pub(crate) fn redact(text: &str) -> String {
    let text = redact_secret_uris(text);
    let text = redact_urls(&text);
    let text = redact_authorization_headers(&text);
    let text = redact_auth_schemes(&text);
    redact_sensitive_pairs(&text)
}

/// Characters that end a URI / token in running text.
fn is_token_end(c: char) -> bool {
    c.is_whitespace()
        || matches!(
            c,
            '"' | '\'' | '`' | '<' | '>' | ')' | ']' | '}' | ',' | ';'
        )
}

fn token_end(text: &str, from: usize) -> usize {
    text[from..]
        .char_indices()
        .find(|(_, c)| is_token_end(*c))
        .map(|(i, _)| from + i)
        .unwrap_or(text.len())
}

/// Byte offset of the next ASCII-case-insensitive occurrence of `needle`
/// (ASCII) at or after `from`.
fn find_ci(text: &str, needle: &str, from: usize) -> Option<usize> {
    let hay = text.as_bytes();
    let needle = needle.as_bytes();
    if needle.is_empty() || hay.len() < needle.len() {
        return None;
    }
    (from..=hay.len() - needle.len())
        .find(|&i| hay[i..i + needle.len()].eq_ignore_ascii_case(needle))
}

fn redact_secret_uris(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut pos = 0;
    loop {
        let next = ["secrets://", "secret://"]
            .iter()
            .filter_map(|scheme| find_ci(text, scheme, pos))
            .min();
        let Some(start) = next else {
            out.push_str(&text[pos..]);
            return out;
        };
        out.push_str(&text[pos..start]);
        out.push_str(REDACTED);
        pos = token_end(text, start);
    }
}

/// Keep `scheme://host/path` of every URL; replace userinfo and query.
fn redact_urls(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut pos = 0;
    while let Some(sep) = text[pos..].find("://").map(|i| pos + i) {
        // Walk back over the scheme letters.
        let scheme_start = text[..sep]
            .char_indices()
            .rev()
            .take_while(|(_, c)| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
            .last()
            .map(|(i, _)| i)
            .unwrap_or(sep);
        let end = token_end(text, sep + 3);
        out.push_str(&text[pos..scheme_start]);
        out.push_str(&redact_one_url(&text[scheme_start..end]));
        pos = end;
    }
    out.push_str(&text[pos..]);
    out
}

fn redact_one_url(url: &str) -> String {
    let Some(sep) = url.find("://") else {
        return url.to_string();
    };
    let (scheme, rest) = url.split_at(sep + 3);
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let (authority, tail) = rest.split_at(authority_end);
    let host = match authority.rfind('@') {
        Some(at) => format!("{REDACTED}@{}", &authority[at + 1..]),
        None => authority.to_string(),
    };
    let path_end = tail.find(['?', '#']).unwrap_or(tail.len());
    let (path, query) = tail.split_at(path_end);
    let query = if query.is_empty() {
        String::new()
    } else {
        format!("?{REDACTED}")
    };
    format!("{scheme}{host}{path}{query}")
}

/// `Authorization: <anything to end of line>`.
fn redact_authorization_headers(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut pos = 0;
    while let Some(start) = find_ci(text, "authorization", pos) {
        let after = start + "authorization".len();
        let rest = &text[after..];
        let trimmed = rest.trim_start_matches([' ', '"', '\'']);
        let sep_offset = rest.len() - trimmed.len();
        if !(trimmed.starts_with(':') || trimmed.starts_with('=')) {
            out.push_str(&text[pos..after]);
            pos = after;
            continue;
        }
        let value_start = after + sep_offset + 1;
        let value_end = text[value_start..]
            .find(['\n', '\r'])
            .map(|i| value_start + i)
            .unwrap_or(text.len());
        out.push_str(&text[pos..value_start]);
        out.push(' ');
        out.push_str(REDACTED);
        pos = value_end;
    }
    out.push_str(&text[pos..]);
    out
}

/// The credential after `Bearer ` / `Basic `.
fn redact_auth_schemes(text: &str) -> String {
    let mut out = text.to_string();
    for scheme in ["bearer", "basic"] {
        let mut result = String::with_capacity(out.len());
        let mut pos = 0;
        while let Some(start) = find_ci(&out, scheme, pos) {
            let after = start + scheme.len();
            let boundary_before = start == 0
                || !out[..start]
                    .chars()
                    .next_back()
                    .is_some_and(|c| c.is_ascii_alphanumeric());
            let spaces = out[after..].len() - out[after..].trim_start_matches(' ').len();
            let token_start = after + spaces;
            if !boundary_before || spaces == 0 || token_start >= out.len() {
                result.push_str(&out[pos..after]);
                pos = after;
                continue;
            }
            let end = token_end(&out, token_start);
            if end == token_start || out[token_start..end] == *REDACTED {
                result.push_str(&out[pos..after]);
                pos = after;
                continue;
            }
            result.push_str(&out[pos..token_start]);
            result.push_str(REDACTED);
            pos = end;
        }
        result.push_str(&out[pos..]);
        out = result;
    }
    out
}

/// `key=value`, `key: value`, `"key": "value"` for a credential key.
fn redact_sensitive_pairs(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    let mut pos = 0;
    let mut i = 0;
    while i < bytes.len() {
        if !is_key_byte(bytes[i]) || (i > 0 && is_key_byte(bytes[i - 1])) {
            i += 1;
            continue;
        }
        let key_end = (i..bytes.len())
            .find(|&j| !is_key_byte(bytes[j]))
            .unwrap_or(bytes.len());
        let key = &text[i..key_end];
        if !SENSITIVE_KEYS.iter().any(|k| k.eq_ignore_ascii_case(key)) {
            i = key_end;
            continue;
        }
        // Optional closing quote of a JSON key, then `=` or `:`.
        let mut j = key_end;
        if j < bytes.len() && matches!(bytes[j], b'"' | b'\'') {
            j += 1;
        }
        while j < bytes.len() && bytes[j] == b' ' {
            j += 1;
        }
        if j >= bytes.len() || !matches!(bytes[j], b'=' | b':') {
            i = key_end;
            continue;
        }
        j += 1;
        while j < bytes.len() && bytes[j] == b' ' {
            j += 1;
        }
        let (value_start, value_end) = if j < bytes.len() && matches!(bytes[j], b'"' | b'\'') {
            let quote = bytes[j];
            let start = j + 1;
            let end = (start..bytes.len())
                .find(|&k| bytes[k] == quote)
                .unwrap_or(bytes.len());
            (start, end)
        } else {
            let end = (j..bytes.len())
                .find(|&k| {
                    bytes[k].is_ascii_whitespace()
                        || matches!(bytes[k], b'&' | b',' | b';' | b'}' | b')' | b']')
                })
                .unwrap_or(bytes.len());
            (j, end)
        };
        if value_end == value_start || &text[value_start..value_end] == REDACTED {
            i = key_end;
            continue;
        }
        out.push_str(&text[pos..value_start]);
        out.push_str(REDACTED);
        pos = value_end;
        i = value_end;
    }
    out.push_str(&text[pos..]);
    out
}

fn is_key_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'-'
}

#[cfg(test)]
#[path = "error_tests.rs"]
mod tests;
