//! Minimal text-only PDF (Helvetica, WinAnsi, A4). Pure.
//!
//! Every byte of user text reaches the file only inside a PDF literal string,
//! with `\`, `(` and `)` escaped and everything outside printable ASCII
//! written as an octal escape or replaced by `?`, so text cannot close the
//! string and inject PDF operators or objects.

use std::fmt::Write as _;

use super::MAX_PDF_PAGES;

const PAGE_W: i32 = 595;
const PAGE_H: i32 = 842;
const MARGIN: i32 = 50;
const FONT_SIZE: i32 = 11;
const LEADING: i32 = 14;
const MAX_COLS: usize = 90;

/// A rendered PDF.
pub struct Pdf {
    pub bytes: Vec<u8>,
    #[cfg_attr(not(test), allow(dead_code))]
    pub pages: usize,
    /// Characters that could not be encoded and were written as `?`.
    pub replaced: usize,
}

fn lines_per_page() -> usize {
    usize::try_from((PAGE_H - 2 * MARGIN) / LEADING).unwrap_or(1)
}

/// Greedy word wrap at `MAX_COLS` characters; a word longer than a line is
/// cut into line-sized pieces (linear in the word's length). Returns `None`
/// as soon as the text needs more than `cap` lines (checked after each input
/// line), so an oversized text is refused without materialising all of its
/// lines. One input line adds at most `MAX_DOC_CHARS / MAX_COLS` lines past
/// the check, which bounds the overshoot.
fn wrap_capped(text: &str, cap: usize) -> Option<Vec<String>> {
    let normalised = text
        .replace("\r\n", "\n")
        .replace('\r', "\n")
        .replace('\t', "    ");
    let mut out = Vec::new();
    for raw in normalised.split('\n') {
        let mut line = String::new();
        let mut line_len = 0usize;
        for word in raw.split(' ') {
            let w: Vec<char> = word.chars().collect();
            let sep = usize::from(line_len > 0);
            if line_len + sep + w.len() <= MAX_COLS {
                if sep == 1 {
                    line.push(' ');
                }
                line.extend(w.iter());
                line_len += sep + w.len();
                continue;
            }
            if line_len > 0 {
                out.push(std::mem::take(&mut line));
            }
            let mut pieces = w.chunks(MAX_COLS).peekable();
            line_len = 0;
            while let Some(piece) = pieces.next() {
                if pieces.peek().is_some() {
                    out.push(piece.iter().collect());
                } else {
                    line = piece.iter().collect();
                    line_len = piece.len();
                }
            }
        }
        out.push(line);
        if out.len() > cap {
            return None;
        }
    }
    Some(out)
}

#[cfg(test)]
fn wrap(text: &str) -> Vec<String> {
    wrap_capped(text, usize::MAX).unwrap_or_default()
}

/// Escape one line for a PDF literal string in WinAnsi. Printable ASCII is
/// kept (with `\`, `(`, `)` escaped), Latin-1 letters become octal escapes,
/// and everything else (controls, C1, characters outside Latin-1) becomes
/// `?` and is counted.
fn encode_line(line: &str, replaced: &mut usize) -> String {
    let mut s = String::new();
    for ch in line.chars() {
        let code = u32::from(ch);
        match code {
            0x5C | 0x28 | 0x29 => {
                s.push('\\');
                s.push(ch);
            }
            0x20..=0x7E => s.push(ch),
            0xA0..=0xFF => {
                let _ = write!(s, "\\{code:03o}");
            }
            _ => {
                *replaced += 1;
                s.push('?');
            }
        }
    }
    s
}

fn push_obj(out: &mut Vec<u8>, offsets: &mut Vec<usize>, body: &[u8]) {
    offsets.push(out.len());
    out.extend_from_slice(format!("{} 0 obj\n", offsets.len()).as_bytes());
    out.extend_from_slice(body);
    out.extend_from_slice(b"\nendobj\n");
}

/// The text needs more than `MAX_PDF_PAGES` pages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TooManyPages;

/// Render plain text as a paginated A4 PDF of at most `MAX_PDF_PAGES` pages.
pub fn text_to_pdf(text: &str) -> Result<Pdf, TooManyPages> {
    let mut replaced = 0;
    let lines = wrap_capped(text, lines_per_page() * MAX_PDF_PAGES).ok_or(TooManyPages)?;
    let pages_of: Vec<&[String]> = lines.chunks(lines_per_page()).collect();
    let n_pages = pages_of.len().max(1);

    let mut out: Vec<u8> = b"%PDF-1.4\n".to_vec();
    let mut offsets: Vec<usize> = Vec::new();
    let kids = (0..n_pages)
        .map(|i| format!("{} 0 R", 4 + 2 * i))
        .collect::<Vec<_>>()
        .join(" ");
    push_obj(&mut out, &mut offsets, b"<< /Type /Catalog /Pages 2 0 R >>");
    push_obj(
        &mut out,
        &mut offsets,
        format!("<< /Type /Pages /Kids [{kids}] /Count {n_pages} >>").as_bytes(),
    );
    push_obj(
        &mut out,
        &mut offsets,
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /Encoding /WinAnsiEncoding >>",
    );

    for i in 0..n_pages {
        let chunk: &[String] = pages_of.get(i).copied().unwrap_or(&[]);
        let mut content = format!(
            "BT /F1 {FONT_SIZE} Tf {LEADING} TL {MARGIN} {} Td\n",
            PAGE_H - MARGIN - FONT_SIZE
        );
        for (n, line) in chunk.iter().enumerate() {
            if n > 0 {
                content.push_str("T*\n");
            }
            let _ = writeln!(content, "({}) Tj", encode_line(line, &mut replaced));
        }
        content.push_str("ET");
        let page = format!(
            "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 {PAGE_W} {PAGE_H}] /Resources << /Font << /F1 3 0 R >> >> /Contents {} 0 R >>",
            5 + 2 * i
        );
        push_obj(&mut out, &mut offsets, page.as_bytes());
        let stream = format!(
            "<< /Length {} >>\nstream\n{content}\nendstream",
            content.len()
        );
        push_obj(&mut out, &mut offsets, stream.as_bytes());
    }

    let xref_pos = out.len();
    let total = offsets.len() + 1;
    out.extend_from_slice(format!("xref\n0 {total}\n0000000000 65535 f \n").as_bytes());
    for off in &offsets {
        out.extend_from_slice(format!("{off:010} 00000 n \n").as_bytes());
    }
    out.extend_from_slice(
        format!("trailer\n<< /Size {total} /Root 1 0 R >>\nstartxref\n{xref_pos}\n%%EOF\n")
            .as_bytes(),
    );
    Ok(Pdf {
        bytes: out,
        pages: n_pages,
        replaced,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok_pdf(text: &str) -> Pdf {
        text_to_pdf(text).expect("within the page limit")
    }

    fn content_of(pdf: &Pdf) -> String {
        String::from_utf8(pdf.bytes.clone()).expect("the writer emits ASCII only")
    }

    #[test]
    fn output_is_a_well_formed_pdf() {
        let pdf = ok_pdf("Hello (world) \\ done");
        assert!(pdf.bytes.starts_with(b"%PDF-1.4\n"));
        assert!(pdf.bytes.ends_with(b"%%EOF\n"));
        assert_eq!(pdf.pages, 1);
        assert_eq!(pdf.replaced, 0);
        assert!(content_of(&pdf).contains(r"(Hello \(world\) \\ done) Tj"));
    }

    #[test]
    fn xref_offsets_point_at_objects() {
        let pdf = ok_pdf(&"line\n".repeat(120));
        let text = content_of(&pdf);
        let xref_at = text.rfind("\nxref\n").unwrap() + 1;
        let entries: Vec<&str> = text[xref_at..]
            .lines()
            .skip(3)
            .take_while(|l| l.ends_with(" n "))
            .collect();
        assert_eq!(entries.len(), 3 + 2 * pdf.pages);
        for (i, e) in entries.iter().enumerate() {
            let off: usize = e[..10].parse().unwrap();
            assert!(
                text[off..].starts_with(&format!("{} 0 obj", i + 1)),
                "object {}",
                i + 1
            );
        }
        let startxref: usize = text[text.rfind("startxref\n").unwrap() + 10..]
            .lines()
            .next()
            .unwrap()
            .parse()
            .unwrap();
        assert_eq!(startxref, xref_at);
    }

    #[test]
    fn stream_lengths_are_exact() {
        let text = content_of(&ok_pdf("a\nb (c)\n\u{e9}"));
        let at = text.find("/Length ").unwrap() + 8;
        let len: usize = text[at..].split(' ').next().unwrap().parse().unwrap();
        let start = text[at..].find("stream\n").unwrap() + at + 7;
        assert_eq!(&text[start + len..start + len + 10], "\nendstream");
    }

    #[test]
    fn text_cannot_inject_operators() {
        let evil = ") Tj ET BT /F1 99 Tf (owned) Tj ET\nendstream\nendobj\n9 0 obj";
        let pdf = ok_pdf(evil);
        let s = content_of(&pdf);
        assert!(s.contains(r"(\) Tj ET BT /F1 99 Tf \(owned\) Tj ET) Tj"));
        // One page: catalog, pages, font, page, content. No injected object.
        let objects = s
            .lines()
            .filter(|l| l.ends_with(" 0 obj") && l.starts_with(|c: char| c.is_ascii_digit()))
            .count();
        assert_eq!(objects, 5);
        assert_eq!(
            s.matches("endstream").count(),
            2,
            "the user line plus the real one"
        );
    }

    #[test]
    fn control_characters_are_replaced() {
        let pdf = ok_pdf("a\u{0}b\u{1b}c\u{7f}d\u{85}e");
        assert_eq!(pdf.replaced, 4);
        assert!(content_of(&pdf).contains("(a?b?c?d?e) Tj"));
    }

    #[test]
    fn a_text_over_the_page_limit_is_refused() {
        let per_page = lines_per_page();
        let at_limit = "x\n".repeat(per_page * MAX_PDF_PAGES - 1);
        assert_eq!(
            text_to_pdf(&at_limit).map(|p| p.pages).ok(),
            Some(MAX_PDF_PAGES)
        );
        let over = "x\n".repeat(per_page * MAX_PDF_PAGES);
        assert!(text_to_pdf(&over).is_err());
        // Blank lines count too: a text of newlines must not render
        // millions of empty lines before anything is checked.
        assert!(text_to_pdf(&"\n".repeat(crate::runner::media_render::MAX_DOC_CHARS)).is_err());
    }

    #[test]
    fn the_capped_wrap_stops_at_the_cap() {
        assert_eq!(wrap_capped("a\nb\nc", 3).map(|l| l.len()), Some(3));
        assert!(wrap_capped("a\nb\nc\nd", 3).is_none());
        assert_eq!(wrap_capped(&"a".repeat(181), 3).map(|l| l.len()), Some(3));
        assert!(wrap_capped(&"a".repeat(271), 3).is_none());
    }

    #[test]
    fn long_text_paginates() {
        assert_eq!(ok_pdf(&"x\n".repeat(120)).pages, 3);
        assert_eq!(ok_pdf("").pages, 1);
    }

    #[test]
    fn long_words_are_hard_wrapped() {
        for l in wrap(&"a".repeat(200)) {
            assert!(l.chars().count() <= 90);
        }
        assert_eq!(wrap(&"a".repeat(200)).len(), 3);
        assert_eq!(wrap("ab cd"), vec!["ab cd".to_string()]);
    }

    #[test]
    fn a_huge_single_word_wraps_in_linear_time() {
        let n = crate::runner::media_render::MAX_DOC_CHARS;
        assert_eq!(wrap(&"a".repeat(n)).len(), n.div_ceil(90));
    }

    #[test]
    fn unencodable_characters_are_counted_not_dropped_silently() {
        let pdf = ok_pdf("日本 café");
        assert_eq!(pdf.replaced, 2);
        assert!(content_of(&pdf).contains(r"(?? caf\351) Tj"));
    }
}
