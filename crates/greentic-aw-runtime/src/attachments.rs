//! Attachment references handed to an agent turn.
//!
//! The runner never holds attachment bytes in conversation state: a reference
//! (`artifact://<id>`) plus metadata is enough, and the LLM backend resolves
//! bytes for the CURRENT turn only. Contract: the attachments master plan, C1.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// At most this many attachments are used per message (spec 3.4).
pub const MAX_ATTACHMENTS: usize = 5;

const ARTIFACT_SCHEME: &str = "artifact://";
/// Longest attachment name echoed into a skip reason.
const MAX_LABEL_CHARS: usize = 60;
/// An artifact id is the lowercase hex SHA-256 of its content (contract C1/C2).
const ARTIFACT_ID_LEN: usize = 64;

/// `artifact://` followed by exactly 64 characters of `[0-9a-f]`; nothing else.
fn is_artifact_ref(s: &str) -> bool {
    s.strip_prefix(ARTIFACT_SCHEME).is_some_and(|id| {
        id.len() == ARTIFACT_ID_LEN && id.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttachmentKind {
    Image,
    Document,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AttachmentRef {
    /// `artifact://<id>`.
    pub id: String,
    pub mime_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    pub kind: AttachmentKind,
    /// Derived artifact holding the extracted text of a document.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text_ref: Option<String>,
}

/// Structured reason an attachment was not used. Carries no free text from the
/// sender or the host except `Host`, whose code is host-supplied and must be
/// mapped to a fixed sentence by the consumer before it reaches a prompt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SkipCode {
    Duplicate,
    InvalidReference,
    OverLimit,
    /// No usable `artifact://` url and no host note.
    NotStored,
    /// The host's `attachment_notes[i].code`, or `None` when it sent none.
    Host(Option<String>),
}

#[derive(Debug, Default)]
pub struct ParsedAttachments {
    pub refs: Vec<AttachmentRef>,
    /// Human-readable reasons for every attachment that was not used. These
    /// embed the sender's attachment name and the host's note text: for logs
    /// and diagnostics only, NEVER for a model prompt (use `skipped_codes`).
    pub skipped: Vec<String>,
    /// One entry per `skipped` entry, same order.
    pub skipped_codes: Vec<SkipCode>,
}

fn kind_from_mime(mime: &str) -> AttachmentKind {
    if mime.starts_with("image/") {
        AttachmentKind::Image
    } else {
        AttachmentKind::Document
    }
}

/// Build refs from the flow node's `attachments` (envelope list),
/// `attachment_meta` (envelope `extensions["artifacts"]`) and `attachment_notes`
/// (envelope `extensions["attachment_notes"]`), all parallel by index.
///
/// Anything that is not an array (including `""`, which is what an unresolved
/// template renders as, and `null`) yields an empty result: a message with no
/// attachments is the ordinary case and is never reported as a failure.
pub fn parse_flow_attachments(
    attachments: &Value,
    meta: &Value,
    notes: &Value,
) -> ParsedAttachments {
    let mut out = ParsedAttachments::default();
    let Some(list) = attachments.as_array() else {
        return out;
    };
    let meta_list = meta.as_array();
    let notes_list = notes.as_array();
    for (index, item) in list.iter().enumerate() {
        let label = item
            .get("name")
            .and_then(Value::as_str)
            .map(|n| n.chars().take(MAX_LABEL_CHARS).collect::<String>())
            .unwrap_or_else(|| format!("attachment #{}", index + 1));
        let url = item.get("url").and_then(Value::as_str).unwrap_or("");
        if url.starts_with(ARTIFACT_SCHEME) && !is_artifact_ref(url) {
            // Never echo the offending value: it may be arbitrarily long.
            out.skipped
                .push(format!("{label}: not a valid artifact reference"));
            out.skipped_codes.push(SkipCode::InvalidReference);
            continue;
        }
        if !is_artifact_ref(url) {
            // The host leaves `url: null` and explains why in `attachment_notes[i]`.
            let note = notes_list.and_then(|l| l.get(index));
            let code = note.and_then(|n| n.get("code")).and_then(Value::as_str);
            let message = note.and_then(|n| n.get("message")).and_then(Value::as_str);
            out.skipped_codes.push(match code {
                Some(c) => SkipCode::Host(Some(c.to_string())),
                None if message.is_some() => SkipCode::Host(None),
                None => SkipCode::NotStored,
            });
            out.skipped.push(match (code, message) {
                (Some(code), Some(message)) => format!("{label}: {code} ({message})"),
                (Some(code), None) => format!("{label}: {code}"),
                _ => format!("{label}: not available to the agent (not stored as an artifact)"),
            });
            continue;
        }
        if out.refs.iter().any(|r| r.id == url) {
            out.skipped.push(format!(
                "{label}: duplicate of an attachment already in this message"
            ));
            out.skipped_codes.push(SkipCode::Duplicate);
            continue;
        }
        if out.refs.len() >= MAX_ATTACHMENTS {
            out.skipped.push(format!(
                "{label}: more than {MAX_ATTACHMENTS} attachments in one message"
            ));
            out.skipped_codes.push(SkipCode::OverLimit);
            continue;
        }
        let mime_type = item
            .get("mime_type")
            .and_then(Value::as_str)
            .unwrap_or("application/octet-stream")
            .to_string();
        let m = meta_list.and_then(|l| l.get(index));
        let kind = m
            .and_then(|m| m.get("kind"))
            .and_then(Value::as_str)
            .and_then(|k| match k {
                "image" => Some(AttachmentKind::Image),
                "document" => Some(AttachmentKind::Document),
                _ => None,
            })
            .unwrap_or_else(|| kind_from_mime(&mime_type));
        let text_ref = m
            .and_then(|m| m.get("text_ref"))
            .and_then(Value::as_str)
            .filter(|t| is_artifact_ref(t))
            .map(str::to_string);
        out.refs.push(AttachmentRef {
            id: url.to_string(),
            mime_type,
            name: item.get("name").and_then(Value::as_str).map(str::to_string),
            size_bytes: item.get("size_bytes").and_then(Value::as_u64),
            kind,
            text_ref,
        });
    }
    out
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A well-formed artifact ref whose id is `c` repeated 64 times.
    fn aid(c: char) -> String {
        format!("artifact://{}", c.to_string().repeat(64))
    }

    fn img(url: &str, name: &str) -> Value {
        json!({"mime_type":"image/png","url":url,"name":name})
    }

    #[test]
    fn zips_attachments_with_extension_metadata_by_index() {
        let atts = json!([
            {"mime_type":"image/png","url":aid('a'),"name":"a.png","size_bytes":10},
            {"mime_type":"application/pdf","url":aid('b'),"name":"b.pdf"}
        ]);
        let meta = json!([
            {"sha256":"11","kind":"image","text_ref":null},
            {"sha256":"22","kind":"document","text_ref":aid('c')}
        ]);
        let parsed = parse_flow_attachments(&atts, &meta, &json!(null));
        assert!(parsed.skipped.is_empty());
        assert_eq!(parsed.refs.len(), 2);
        assert_eq!(parsed.refs[0].kind, AttachmentKind::Image);
        assert_eq!(parsed.refs[1].text_ref.as_deref(), Some(aid('c').as_str()));
    }

    #[test]
    fn unprocessed_urls_are_skipped_and_reported() {
        let atts = json!([
            {"mime_type":"image/png","url":null},
            {"mime_type":"image/png","url":"https://example.com/x.png"},
            img(&aid('d'), "ok.png")
        ]);
        let meta = json!([{"kind":"image"},{"kind":"image"},{"kind":"image"}]);
        let parsed = parse_flow_attachments(&atts, &meta, &json!(null));
        assert_eq!(parsed.refs.len(), 1);
        assert_eq!(parsed.skipped.len(), 2);
    }

    #[test]
    fn more_than_five_are_truncated_and_reported() {
        let list: Vec<Value> = "012345".chars().map(|c| img(&aid(c), "x.png")).collect();
        let parsed = parse_flow_attachments(&Value::Array(list), &json!(null), &json!(null));
        assert_eq!(parsed.refs.len(), MAX_ATTACHMENTS);
        assert_eq!(parsed.skipped.len(), 1);
    }

    #[test]
    fn missing_or_non_array_input_is_empty_not_an_error() {
        let parsed = parse_flow_attachments(&json!(null), &json!(null), &json!(null));
        assert!(parsed.refs.is_empty() && parsed.skipped.is_empty());
        let parsed = parse_flow_attachments(&json!("nope"), &json!({}), &json!(7));
        assert!(parsed.refs.is_empty() && parsed.skipped.is_empty());
    }

    #[test]
    fn unresolved_template_renders_empty_string_and_means_no_attachments() {
        let parsed = parse_flow_attachments(&json!(""), &json!(""), &json!(""));
        assert!(parsed.refs.is_empty());
        assert!(
            parsed.skipped.is_empty(),
            "no attachments is not a failure to report"
        );
    }

    #[test]
    fn missing_metadata_defaults_kind_from_mime() {
        let atts = json!([{"mime_type":"image/webp","url":aid('e'),"name":"z.webp"}]);
        let parsed = parse_flow_attachments(&atts, &json!(null), &json!(null));
        assert_eq!(parsed.refs[0].kind, AttachmentKind::Image);
    }

    #[test]
    fn a_failed_attachment_is_reported_with_the_hosts_note() {
        let atts = json!([
            {"mime_type":"application/pdf","url":null,"name":"report.pdf"},
            img(&aid('f'), "ok.png")
        ]);
        let notes = json!([{"code":"too_large","message":"file is over 10 MB"}, null]);
        let parsed = parse_flow_attachments(&atts, &json!([{}, {"kind":"image"}]), &notes);
        assert_eq!(parsed.refs.len(), 1);
        assert_eq!(parsed.skipped.len(), 1);
        assert!(parsed.skipped[0].contains("report.pdf"));
        assert!(parsed.skipped[0].contains("too_large"));
    }

    #[test]
    fn a_note_with_a_code_but_no_message_reports_the_code_alone() {
        let atts = json!([{"mime_type":"image/png","url":null,"name":"a.png"}]);
        let parsed = parse_flow_attachments(&atts, &json!(null), &json!([{"code":"fetch_failed"}]));
        assert_eq!(parsed.skipped, vec!["a.png: fetch_failed".to_string()]);
    }

    #[test]
    fn notes_longer_or_shorter_than_attachments_never_index_out_of_bounds() {
        let atts = json!([{"mime_type":"image/png","url":null,"name":"a.png"}]);
        let long =
            json!([{"code":"fetch_failed","message":"x"}, {"code":"too_large","message":"y"}]);
        let parsed = parse_flow_attachments(&atts, &json!(null), &long);
        assert_eq!(parsed.skipped.len(), 1);
        let parsed = parse_flow_attachments(&atts, &json!(null), &json!([]));
        assert_eq!(
            parsed.skipped.len(),
            1,
            "no note: generic reason, still reported"
        );
    }

    #[test]
    fn malformed_artifact_ids_are_skipped_and_never_become_refs() {
        let hex = "a".repeat(64);
        let bad = [
            format!("artifact://{}", "A".repeat(64)),
            format!("artifact://{}", "a".repeat(63)),
            format!("artifact://{}", "a".repeat(65)),
            format!("artifact://{hex}/../x"),
            format!("artifact://{}\u{e9}", "a".repeat(63)),
            "artifact://".to_string(),
            format!("ARTIFACT://{hex}"),
        ];
        for url in bad {
            let parsed =
                parse_flow_attachments(&json!([img(&url, "x.png")]), &json!(null), &json!(null));
            assert!(parsed.refs.is_empty(), "accepted {url}");
            assert_eq!(parsed.skipped.len(), 1, "not reported: {url}");
        }
    }

    #[test]
    fn a_very_long_invalid_id_is_not_echoed_in_the_report() {
        let url = format!("artifact://{}", "z".repeat(5000));
        let parsed =
            parse_flow_attachments(&json!([img(&url, "x.png")]), &json!(null), &json!(null));
        assert!(parsed.skipped[0].len() < 200, "{}", parsed.skipped[0].len());
        assert!(!parsed.skipped[0].contains("zzzz"));
    }

    #[test]
    fn an_invalid_text_ref_is_dropped_but_the_attachment_is_kept() {
        let atts = json!([{"mime_type":"application/pdf","url":aid('a'),"name":"b.pdf"}]);
        for text_ref in [
            json!("https://example.com/t.txt"),
            json!(format!("artifact://{}", "G".repeat(64))),
            json!(format!("artifact://{}/../x", "a".repeat(64))),
        ] {
            let meta = json!([{"kind":"document","text_ref":text_ref}]);
            let parsed = parse_flow_attachments(&atts, &meta, &json!(null));
            assert_eq!(parsed.refs.len(), 1);
            assert_eq!(parsed.refs[0].text_ref, None);
            assert!(parsed.skipped.is_empty());
        }
    }

    #[test]
    fn a_duplicate_ref_counts_once_and_is_reported() {
        let atts = json!([
            img(&aid('a'), "one.png"),
            img(&aid('a'), "again.png"),
            img(&aid('b'), "two.png")
        ]);
        let parsed = parse_flow_attachments(&atts, &json!(null), &json!(null));
        assert_eq!(parsed.refs.len(), 2);
        assert_eq!(parsed.refs[0].name.as_deref(), Some("one.png"));
        assert_eq!(parsed.skipped.len(), 1);
        assert!(parsed.skipped[0].contains("duplicate"));
    }

    #[test]
    fn duplicates_do_not_consume_the_cap() {
        // 5 distinct + 5 repeats of the first, then a 6th distinct one.
        let mut list: Vec<Value> = "01234".chars().map(|c| img(&aid(c), "x.png")).collect();
        list.extend((0..5).map(|_| img(&aid('0'), "dup.png")));
        list.push(img(&aid('5'), "sixth.png"));
        let parsed = parse_flow_attachments(&Value::Array(list), &json!(null), &json!(null));
        assert_eq!(parsed.refs.len(), MAX_ATTACHMENTS);
        let dups = parsed
            .skipped
            .iter()
            .filter(|s| s.contains("duplicate"))
            .count();
        assert_eq!(dups, 5);
        assert_eq!(
            parsed.skipped.len(),
            6,
            "the sixth distinct one is over the cap"
        );
        let ids: Vec<_> = parsed.refs.iter().map(|r| r.id.clone()).collect();
        assert_eq!(
            ids.iter().collect::<std::collections::HashSet<_>>().len(),
            5
        );
    }

    #[test]
    fn a_non_object_element_is_skipped_without_panicking() {
        let atts = json!([7, null, "x", img(&aid('a'), "ok.png")]);
        let parsed = parse_flow_attachments(&atts, &json!(null), &json!(null));
        assert_eq!(parsed.refs.len(), 1);
        assert_eq!(parsed.skipped.len(), 3);
    }

    #[test]
    fn skip_codes_run_parallel_to_skip_reasons() {
        let atts = json!([
            {"mime_type":"image/png","url":null,"name":"x"},
            {"mime_type":"image/png","url":null,"name":"y"},
            img("artifact://short", "z"),
            img(&aid('a'), "ok.png"),
            img(&aid('a'), "dup.png")
        ]);
        let notes = json!([{"code":"too_large","message":"m"}]);
        let parsed = parse_flow_attachments(&atts, &json!(null), &notes);
        assert_eq!(parsed.skipped.len(), parsed.skipped_codes.len());
        assert_eq!(
            parsed.skipped_codes,
            vec![
                SkipCode::Host(Some("too_large".into())),
                SkipCode::NotStored,
                SkipCode::InvalidReference,
                SkipCode::Duplicate
            ]
        );
    }
}
