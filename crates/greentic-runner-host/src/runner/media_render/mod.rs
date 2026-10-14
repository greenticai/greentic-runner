//! Pure rendering for the `media.create` flow node: `txt`, `md`, `csv` and
//! `pdf` documents.
//!
//! Ported from `component-media-ext` (`document.rs`, `pdf.rs`, `input.rs`,
//! `names.rs`, `limits.rs`, `output.rs`) so a plain flow can create a file
//! without the `greentic.media` extension installed. Behaviour and limits are
//! kept identical on purpose; the duplication is recorded debt (design
//! `2026-10-13-media-create-flow-node-design.md`, decision 2).
//!
//! Nothing here touches the host: parsing, rendering and the C5 result values
//! (`docs/media-contract-v1.md`) are all pure.

mod document;
mod names;
mod output;
mod pdf;

pub use document::{Rendered, render};
pub use names::sanitize_stem;
pub use output::{Artifact, Failure, codes, failure, success};

use serde::Deserialize;
use serde_json::Value;

/// Largest artifact stored (matches the platform limit).
pub const MAX_ARTIFACT_BYTES: usize = 10 * 1024 * 1024;
/// Largest `text`, in Unicode scalar values.
pub const MAX_DOC_CHARS: usize = 2_000_000;
/// Largest PDF rendered. Checked while wrapping, before the pages are built.
pub const MAX_PDF_PAGES: usize = 2000;

/// Document formats the node can create.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Txt,
    Md,
    Csv,
    Pdf,
}

/// A validated `media.create` request.
#[derive(Debug, Clone, PartialEq)]
pub struct DocRequest {
    pub format: Format,
    pub stem: String,
    pub text: String,
    pub rows: Vec<Vec<Value>>,
    pub escape_formulas: bool,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct RawDoc {
    format: String,
    name: Option<String>,
    text: Option<String>,
    rows: Option<Vec<Vec<Value>>>,
    escape_formulas: Option<bool>,
}

/// Parse and validate the rendered node payload. `Err` is an `invalid_input`
/// (or `unsupported_format`) message.
pub fn parse_create_document(args: &Value) -> Result<DocRequest, Failure> {
    let raw: RawDoc = serde_json::from_value(args.clone())
        .map_err(|e| Failure::new(codes::INVALID_INPUT, format!("invalid arguments: {e}")))?;
    let format = match raw.format.trim().to_ascii_lowercase().as_str() {
        "txt" => Format::Txt,
        "md" => Format::Md,
        "csv" => Format::Csv,
        "pdf" => Format::Pdf,
        other => {
            return Err(Failure::new(
                codes::UNSUPPORTED_FORMAT,
                format!("`format` must be one of txt, md, csv, pdf (got `{other}`)"),
            ));
        }
    };
    let invalid = |m: String| Failure::new(codes::INVALID_INPUT, m);
    let stem = sanitize_stem(raw.name.as_deref(), "document");
    let escape_formulas = raw.escape_formulas.unwrap_or(true);
    if format == Format::Csv {
        let rows = raw.rows.unwrap_or_default();
        if rows.is_empty() {
            return Err(invalid(
                "`rows` is required for csv and must not be empty".into(),
            ));
        }
        for (r, row) in rows.iter().enumerate() {
            for (c, cell) in row.iter().enumerate() {
                if matches!(cell, Value::Array(_) | Value::Object(_)) {
                    return Err(invalid(format!(
                        "cell [{r}][{c}] must be a string, number, boolean or null"
                    )));
                }
            }
        }
        return Ok(DocRequest {
            format,
            stem,
            text: String::new(),
            rows,
            escape_formulas,
        });
    }
    let text = raw.text.unwrap_or_default();
    if text.trim().is_empty() {
        return Err(invalid("`text` is required and must not be blank".into()));
    }
    if text.chars().count() > MAX_DOC_CHARS {
        return Err(invalid(format!(
            "`text` is longer than {MAX_DOC_CHARS} characters"
        )));
    }
    Ok(DocRequest {
        format,
        stem,
        text,
        rows: Vec::new(),
        escape_formulas,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn text_formats_need_text() {
        for f in ["txt", "md", "pdf", "PDF"] {
            assert!(parse_create_document(&json!({"format": f, "text": "hello"})).is_ok());
            let err = parse_create_document(&json!({"format": f, "text": "  "})).unwrap_err();
            assert_eq!(err.code, "invalid_input");
        }
    }

    #[test]
    fn csv_needs_scalar_rows() {
        let ok = parse_create_document(&json!({"format":"csv","rows":[["a","b"],[1,true]]}));
        assert_eq!(ok.unwrap().rows.len(), 2);
        assert!(parse_create_document(&json!({"format":"csv","rows":[]})).is_err());
        assert!(parse_create_document(&json!({"format":"csv"})).is_err());
        let nested = parse_create_document(&json!({"format":"csv","rows":[["a",{"x":1}]]}));
        assert!(nested.unwrap_err().message.contains("[0][1]"));
    }

    #[test]
    fn unknown_format_is_unsupported_and_names_are_sanitised() {
        let err = parse_create_document(&json!({"format":"docx","text":"x"})).unwrap_err();
        assert_eq!(err.code, "unsupported_format");
        let d = parse_create_document(&json!({"format":"txt","text":"x","name":"../a b"}));
        assert_eq!(d.unwrap().stem, "a-b");
        let d = parse_create_document(&json!({"format":"txt","text":"x"}));
        assert_eq!(d.unwrap().stem, "document");
    }

    #[test]
    fn text_over_the_limit_is_rejected() {
        let big = "a".repeat(MAX_DOC_CHARS + 1);
        assert!(parse_create_document(&json!({"format":"txt","text":big})).is_err());
        let fits = "a".repeat(MAX_DOC_CHARS);
        assert!(parse_create_document(&json!({"format":"txt","text":fits})).is_ok());
    }
}
