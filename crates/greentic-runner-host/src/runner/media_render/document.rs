//! Turn a validated `DocRequest` into bytes. Pure.

use serde_json::Value;

use super::MAX_PDF_PAGES;
use super::pdf::{TooManyPages, text_to_pdf};
use super::{DocRequest, Format};
use super::{Failure, codes};

pub struct Rendered {
    pub bytes: Vec<u8>,
    pub mime: &'static str,
    pub ext: &'static str,
    pub warnings: Vec<Failure>,
}

/// Would a spreadsheet read this cell as a formula? A leading tab or CR, or
/// (after any leading whitespace, which spreadsheets skip) `=`, `+`, `@`,
/// their full-width forms `＝ ＋ ＠`, `－`, or a `-` that does not start a
/// number. Leading whitespace and full-width signs are covered because some
/// spreadsheets normalise them before deciding.
fn looks_like_formula(s: &str) -> bool {
    if s.starts_with(['\t', '\r']) {
        return true;
    }
    let t = s.trim_start();
    match t.chars().next() {
        Some('=' | '+' | '@' | '\u{FF1D}' | '\u{FF0B}' | '\u{FF20}' | '\u{FF0D}') => true,
        Some('-') => t.parse::<f64>().is_err(),
        _ => false,
    }
}

/// A leading apostrophe makes a risky cell text. `escape` is the caller's
/// `escape_formulas` (default true); `false` writes cells verbatim.
fn guard(s: &str, escape: bool) -> String {
    if escape && looks_like_formula(s) {
        format!("'{s}")
    } else {
        s.to_string()
    }
}

fn quote(s: &str) -> String {
    if s.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

fn cell(v: &Value, escape: bool) -> String {
    match v {
        Value::String(s) => quote(&guard(s, escape)),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        _ => String::new(),
    }
}

fn csv(rows: &[Vec<Value>], escape: bool) -> String {
    let mut out = String::new();
    for row in rows {
        out.push_str(
            &row.iter()
                .map(|c| cell(c, escape))
                .collect::<Vec<_>>()
                .join(","),
        );
        out.push_str("\r\n");
    }
    out
}

fn text(req: &DocRequest, mime: &'static str, ext: &'static str) -> Rendered {
    Rendered {
        bytes: req.text.clone().into_bytes(),
        mime,
        ext,
        warnings: vec![],
    }
}

/// Render the document; `Err` is a `too_large` failure for a PDF over
/// `MAX_PDF_PAGES` pages, refused before its pages are built.
pub fn render(req: &DocRequest) -> Result<Rendered, Failure> {
    Ok(match req.format {
        Format::Txt => text(req, "text/plain", "txt"),
        Format::Md => text(req, "text/markdown", "md"),
        Format::Csv => Rendered {
            bytes: csv(&req.rows, req.escape_formulas).into_bytes(),
            mime: "text/csv",
            ext: "csv",
            warnings: vec![],
        },
        Format::Pdf => {
            let pdf = text_to_pdf(&req.text).map_err(|TooManyPages| {
                Failure::new(
                    codes::TOO_LARGE,
                    format!("the PDF would have more than {MAX_PDF_PAGES} pages; split the text"),
                )
            })?;
            let warnings = if pdf.replaced > 0 {
                vec![Failure::new(
                    codes::CHARACTERS_REPLACED,
                    format!(
                        "{} character(s) outside Latin-1 were replaced with `?` (the PDF font cannot encode them)",
                        pdf.replaced
                    ),
                )]
            } else {
                vec![]
            };
            Rendered {
                bytes: pdf.bytes,
                mime: "application/pdf",
                ext: "pdf",
                warnings,
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::super::parse_create_document;
    use super::*;

    fn render_args(args: &str) -> Rendered {
        render(&parse_create_document(&serde_json::from_str(args).unwrap()).unwrap()).unwrap()
    }

    #[test]
    fn txt_and_md_are_utf8_passthrough() {
        let t = render_args(r#"{"format":"txt","text":"héllo 日本"}"#);
        assert_eq!((t.mime, t.ext), ("text/plain", "txt"));
        assert_eq!(String::from_utf8(t.bytes).unwrap(), "héllo 日本");
        assert!(t.warnings.is_empty());
        let m = render_args(r##"{"format":"md","text":"# Title"}"##);
        assert_eq!((m.mime, m.ext), ("text/markdown", "md"));
    }

    #[test]
    fn csv_quotes_and_uses_crlf() {
        let c = render_args(
            r#"{"format":"csv","rows":[["name","note"],["Ann","a,b"],["Bo","say \"hi\""],[1,null],["x\ny",false]]}"#,
        );
        assert_eq!((c.mime, c.ext), ("text/csv", "csv"));
        let s = String::from_utf8(c.bytes).unwrap();
        assert_eq!(
            s,
            "name,note\r\nAnn,\"a,b\"\r\nBo,\"say \"\"hi\"\"\"\r\n1,\r\n\"x\ny\",false\r\n"
        );
    }

    #[test]
    fn csv_neutralises_formulas_unless_disabled() {
        let on = render_args(
            r#"{"format":"csv","rows":[["=SUM(A1)","+1","@x","-y","-5",-5,"\tz","\r=1","ok"]]}"#,
        );
        assert_eq!(
            String::from_utf8(on.bytes).unwrap(),
            "'=SUM(A1),'+1,'@x,'-y,-5,-5,'\tz,\"'\r=1\",ok\r\n"
        );
        let off = render_args(r#"{"format":"csv","escape_formulas":false,"rows":[["=1"]]}"#);
        assert_eq!(String::from_utf8(off.bytes).unwrap(), "=1\r\n");
    }

    #[test]
    fn leading_spaces_and_full_width_signs_do_not_hide_a_formula() {
        let on = render_args(
            r#"{"format":"csv","rows":[[" =1","  +A1"," @x"," -y"," -5","\uff1d1","\uff0b1","\uff0d1","\uff20x"," \uff1d1","a =1"," ok"]]}"#,
        );
        assert_eq!(
            String::from_utf8(on.bytes).unwrap(),
            "' =1,'  +A1,' @x,' -y, -5,'\u{ff1d}1,'\u{ff0b}1,'\u{ff0d}1,'\u{ff20}x,' \u{ff1d}1,a =1, ok\r\n"
        );
        let off =
            render_args(r#"{"format":"csv","escape_formulas":false,"rows":[[" =1","\uff1d1"]]}"#);
        assert_eq!(String::from_utf8(off.bytes).unwrap(), " =1,\u{ff1d}1\r\n");
    }

    #[test]
    fn a_formula_with_a_comma_is_both_neutralised_and_quoted() {
        let c = render_args(r#"{"format":"csv","rows":[["=HYPERLINK(\"http://x\",\"y\")"]]}"#);
        assert_eq!(
            String::from_utf8(c.bytes).unwrap(),
            "\"'=HYPERLINK(\"\"http://x\"\",\"\"y\"\")\"\r\n"
        );
    }

    #[test]
    fn pdf_reports_unencodable_characters_as_a_warning() {
        let p = render_args(r#"{"format":"pdf","text":"日本 ok"}"#);
        assert_eq!((p.mime, p.ext), ("application/pdf", "pdf"));
        assert!(p.bytes.starts_with(b"%PDF-"));
        assert_eq!(p.warnings.len(), 1);
        assert_eq!(p.warnings[0].code, "characters_replaced");
        assert!(p.warnings[0].message.contains('2'));
        let clean = render_args(r#"{"format":"pdf","text":"plain"}"#);
        assert!(clean.warnings.is_empty());
    }
}
