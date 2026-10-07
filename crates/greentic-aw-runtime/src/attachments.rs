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

#[derive(Debug, Default)]
pub struct ParsedAttachments {
    pub refs: Vec<AttachmentRef>,
    /// Human-readable reasons for every attachment that was not used.
    pub skipped: Vec<String>,
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
            .map(str::to_string)
            .unwrap_or_else(|| format!("attachment #{}", index + 1));
        if out.refs.len() >= MAX_ATTACHMENTS {
            out.skipped.push(format!(
                "{label}: more than {MAX_ATTACHMENTS} attachments in one message"
            ));
            continue;
        }
        let url = item.get("url").and_then(Value::as_str).unwrap_or("");
        if !url.starts_with(ARTIFACT_SCHEME) {
            // The host leaves `url: null` and explains why in `attachment_notes[i]`.
            let note = notes_list.and_then(|l| l.get(index));
            let code = note.and_then(|n| n.get("code")).and_then(Value::as_str);
            let message = note.and_then(|n| n.get("message")).and_then(Value::as_str);
            out.skipped.push(match (code, message) {
                (Some(code), Some(message)) => format!("{label}: {code} ({message})"),
                (Some(code), None) => format!("{label}: {code}"),
                _ => format!("{label}: not available to the agent (not stored as an artifact)"),
            });
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
            .filter(|t| t.starts_with(ARTIFACT_SCHEME))
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

    #[test]
    fn zips_attachments_with_extension_metadata_by_index() {
        let atts = json!([
            {"mime_type":"image/png","url":"artifact://aa","name":"a.png","size_bytes":10},
            {"mime_type":"application/pdf","url":"artifact://bb","name":"b.pdf"}
        ]);
        let meta = json!([
            {"sha256":"11","kind":"image","text_ref":null},
            {"sha256":"22","kind":"document","text_ref":"artifact://bb-text"}
        ]);
        let parsed = parse_flow_attachments(&atts, &meta, &json!(null));
        assert!(parsed.skipped.is_empty());
        assert_eq!(parsed.refs.len(), 2);
        assert_eq!(parsed.refs[0].kind, AttachmentKind::Image);
        assert_eq!(
            parsed.refs[1].text_ref.as_deref(),
            Some("artifact://bb-text")
        );
    }

    #[test]
    fn unprocessed_urls_are_skipped_and_reported() {
        let atts = json!([
            {"mime_type":"image/png","url":null},
            {"mime_type":"image/png","url":"https://example.com/x.png"},
            {"mime_type":"image/png","url":"artifact://ok","name":"ok.png"}
        ]);
        let meta = json!([{"kind":"image"},{"kind":"image"},{"kind":"image"}]);
        let parsed = parse_flow_attachments(&atts, &meta, &json!(null));
        assert_eq!(parsed.refs.len(), 1);
        assert_eq!(parsed.skipped.len(), 2);
    }

    #[test]
    fn more_than_five_are_truncated_and_reported() {
        let one = json!({"mime_type":"image/png","url":"artifact://x","name":"x.png"});
        let atts = json!([one, one, one, one, one, one]);
        let meta = json!([{"kind":"image"},{"kind":"image"},{"kind":"image"},
                          {"kind":"image"},{"kind":"image"},{"kind":"image"}]);
        let parsed = parse_flow_attachments(&atts, &meta, &json!(null));
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
        // An unresolved `{{in.attachments}}` renders as "" (not an error).
        let parsed = parse_flow_attachments(&json!(""), &json!(""), &json!(""));
        assert!(parsed.refs.is_empty());
        assert!(
            parsed.skipped.is_empty(),
            "no attachments is not a failure to report"
        );
    }

    #[test]
    fn missing_metadata_defaults_kind_from_mime() {
        let atts = json!([{"mime_type":"image/webp","url":"artifact://z","name":"z.webp"}]);
        let parsed = parse_flow_attachments(&atts, &json!(null), &json!(null));
        assert_eq!(parsed.refs[0].kind, AttachmentKind::Image);
    }

    #[test]
    fn a_failed_attachment_is_reported_with_the_hosts_note() {
        let atts = json!([
            {"mime_type":"application/pdf","url":null,"name":"report.pdf"},
            {"mime_type":"image/png","url":"artifact://ok","name":"ok.png"}
        ]);
        let notes = json!([
            {"code":"too_large","message":"file is over 10 MB"},
            null
        ]);
        let parsed = parse_flow_attachments(&atts, &json!([{}, {"kind":"image"}]), &notes);
        assert_eq!(parsed.refs.len(), 1);
        assert_eq!(parsed.skipped.len(), 1);
        assert!(parsed.skipped[0].contains("report.pdf"));
        assert!(parsed.skipped[0].contains("too_large"));
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
}
