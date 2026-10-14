use anyhow::{Result, anyhow};
use handlebars::Handlebars;
use once_cell::sync::Lazy;
use serde_json::{Map as JsonMap, Value};

#[derive(Clone, Copy, Debug, Default)]
pub struct TemplateOptions {
    pub allow_pointer: bool,
}

static HANDLEBARS: Lazy<Handlebars<'static>> = Lazy::new(|| {
    let mut registry = Handlebars::new();
    registry.set_strict_mode(false);
    registry.register_escape_fn(handlebars::no_escape);
    registry
});

pub fn render_template_value(
    template: &Value,
    ctx: &Value,
    options: TemplateOptions,
) -> Result<Value> {
    match template {
        Value::String(raw) => render_template_string(raw, ctx, options),
        Value::Array(items) => {
            let mut rendered = Vec::with_capacity(items.len());
            for item in items {
                rendered.push(render_template_value(item, ctx, options)?);
            }
            Ok(Value::Array(rendered))
        }
        Value::Object(map) => {
            let mut rendered = JsonMap::new();
            for (key, value) in map {
                rendered.insert(key.clone(), render_template_value(value, ctx, options)?);
            }
            Ok(Value::Object(rendered))
        }
        other => Ok(other.clone()),
    }
}

fn render_template_string(raw: &str, ctx: &Value, options: TemplateOptions) -> Result<Value> {
    if options.allow_pointer && raw.starts_with('/') && !raw.contains("{{") {
        return ctx
            .pointer(raw)
            .cloned()
            .ok_or_else(|| anyhow!("mapping path `{raw}` not found"));
    }

    if let Some(expr) = extract_exact_expression(raw)
        && let Some(path) = parse_path_expression(expr)
    {
        return Ok(match resolve_path(ctx, &path) {
            Some(value) => value.clone(),
            None => {
                tracing::warn!(
                    template = expr,
                    "template expression resolved to empty (path not found)"
                );
                Value::String(String::new())
            }
        });
    }

    if raw.contains("{{") {
        let rendered = HANDLEBARS
            .render_template(raw, ctx)
            .map_err(|err| anyhow!("template render failed: {err}"))?;
        return Ok(Value::String(rendered));
    }

    Ok(Value::String(raw.to_string()))
}

fn extract_exact_expression(raw: &str) -> Option<&str> {
    let trimmed = raw.trim();
    if trimmed.starts_with("{{") && trimmed.ends_with("}}") {
        let inner = trimmed.trim_start_matches('{').trim_end_matches('}').trim();
        if !inner.is_empty() {
            return Some(inner);
        }
    }
    None
}

#[derive(Debug)]
enum PathSegment {
    Key(String),
    Index(usize),
}

fn parse_path_expression(expr: &str) -> Option<Vec<PathSegment>> {
    let mut chars = expr.trim().chars().peekable();
    let mut segments = Vec::new();
    while let Some(&ch) = chars.peek() {
        match ch {
            '.' => {
                chars.next();
            }
            '[' => {
                chars.next();
                let segment = parse_bracket_segment(&mut chars)?;
                segments.push(segment);
            }
            _ => {
                let ident = parse_identifier(&mut chars)?;
                segments.push(PathSegment::Key(ident));
            }
        }
    }
    if segments.is_empty() {
        return None;
    }
    if matches!(segments.first(), Some(PathSegment::Key(key)) if key == "this") {
        segments.remove(0);
    }
    Some(segments)
}

fn parse_bracket_segment<I>(chars: &mut std::iter::Peekable<I>) -> Option<PathSegment>
where
    I: Iterator<Item = char>,
{
    match chars.peek().copied() {
        Some('"') | Some('\'') => {
            let quote = chars.next()?;
            let mut buf = String::new();
            for ch in chars.by_ref() {
                if ch == quote {
                    break;
                }
                buf.push(ch);
            }
            consume_bracket_end(chars)?;
            Some(PathSegment::Key(buf))
        }
        Some(ch) if ch.is_ascii_digit() => {
            let mut buf = String::new();
            while let Some(ch) = chars.peek().copied() {
                if ch.is_ascii_digit() {
                    chars.next();
                    buf.push(ch);
                } else {
                    break;
                }
            }
            consume_bracket_end(chars)?;
            let idx = buf.parse::<usize>().ok()?;
            Some(PathSegment::Index(idx))
        }
        Some(_) => {
            let ident = parse_identifier(chars)?;
            consume_bracket_end(chars)?;
            Some(PathSegment::Key(ident))
        }
        None => None,
    }
}

fn consume_bracket_end<I>(chars: &mut std::iter::Peekable<I>) -> Option<()>
where
    I: Iterator<Item = char>,
{
    for ch in chars.by_ref() {
        if ch == ']' {
            return Some(());
        }
        if !ch.is_whitespace() {
            return None;
        }
    }
    None
}

fn parse_identifier<I>(chars: &mut std::iter::Peekable<I>) -> Option<String>
where
    I: Iterator<Item = char>,
{
    let mut buf = String::new();
    while let Some(&ch) = chars.peek() {
        if ch == '.' || ch == '[' || ch == ']' {
            break;
        }
        buf.push(ch);
        chars.next();
    }
    let ident = buf.trim();
    if ident.is_empty() {
        return None;
    }
    Some(ident.to_string())
}

fn resolve_path<'a>(root: &'a Value, path: &[PathSegment]) -> Option<&'a Value> {
    let mut current = root;
    for segment in path {
        match (segment, current) {
            (PathSegment::Key(key), Value::Object(map)) => {
                current = map.get(key)?;
            }
            (PathSegment::Index(index), Value::Array(items)) => {
                current = items.get(*index)?;
            }
            _ => return None,
        }
    }
    Some(current)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn renders_prev_and_node_outputs() {
        let ctx = json!({
            "entry": {},
            "prev": { "text": "hello" },
            "node": {
                "start": { "user": { "id": 7 } }
            },
            "state": {},
        });
        let template = json!({
            "prev_text": "{{prev.text}}",
            "user_id": "{{node.start.user.id}}"
        });
        let rendered = render_template_value(&template, &ctx, TemplateOptions::default()).unwrap();
        assert_eq!(
            rendered,
            json!({
                "prev_text": "hello",
                "user_id": 7
            })
        );
    }

    #[test]
    fn typed_insertion_keeps_json_types() {
        let ctx = json!({
            "entry": { "enabled": true, "count": 3 },
            "prev": {},
            "node": {},
            "state": {},
        });
        let rendered = render_template_value(
            &Value::String("{{entry.enabled}}".to_string()),
            &ctx,
            TemplateOptions::default(),
        )
        .unwrap();
        assert_eq!(rendered, json!(true));

        let rendered = render_template_value(
            &Value::String("{{entry.count}}".to_string()),
            &ctx,
            TemplateOptions::default(),
        )
        .unwrap();
        assert_eq!(rendered, json!(3));
    }

    #[test]
    fn missing_exact_path_renders_empty_string() {
        let ctx = json!({
            "entry": { "input": { "metadata": { "user_question": "what" } } },
            "prev": {},
            "node": {},
            "state": {},
        });
        let rendered = render_template_value(
            &Value::String("{{entry.input.metadata.provider}}".to_string()),
            &ctx,
            TemplateOptions::default(),
        )
        .unwrap();
        assert_eq!(rendered, Value::String(String::new()));
    }

    #[test]
    fn mixed_template_renders_as_string() {
        let ctx = json!({
            "entry": { "user_id": 42 },
            "prev": {},
            "node": {},
            "state": {},
        });
        let rendered = render_template_value(
            &Value::String("https://x/{{entry.user_id}}".to_string()),
            &ctx,
            TemplateOptions::default(),
        )
        .unwrap();
        assert_eq!(rendered, Value::String("https://x/42".to_string()));
    }

    #[test]
    fn entry_attachments_resolve_by_index_and_keep_json_types() {
        let ctx = json!({
            "entry": { "attachments": [
                {"mime_type":"application/pdf","url":"artifact://d","name":"d.pdf","text":"hello world"},
                {"mime_type":"image/png","url":"artifact://i","name":"i.png"}
            ]},
            "prev": {}, "node": {}, "state": {},
        });
        let text = render_template_value(
            &Value::String("{{entry.attachments[0].text}}".into()),
            &ctx,
            TemplateOptions::default(),
        )
        .unwrap();
        assert_eq!(text, json!("hello world"));
        let all = render_template_value(
            &Value::String("{{entry.attachments}}".into()),
            &ctx,
            TemplateOptions::default(),
        )
        .unwrap();
        assert_eq!(
            all.as_array().map(Vec::len),
            Some(2),
            "exact expression keeps the array"
        );
        let second = render_template_value(
            &Value::String("{{entry.attachments[1].url}}".into()),
            &ctx,
            TemplateOptions::default(),
        )
        .unwrap();
        assert_eq!(second, json!("artifact://i"));
    }

    #[test]
    fn extension_metadata_and_notes_resolve_through_the_in_alias() {
        // These are the exact expressions greentic-dw-authoring maps (contract C4b).
        let ctx = json!({
            "entry": {
                "attachments": [{"mime_type":"application/pdf","url":null,"name":"big.pdf"}],
                "extensions": {
                    "artifacts": [{"sha256":"aa","kind":"document","text_ref":null}],
                    "attachment_notes": [{"code":"too_large","message":"over 10 MB"}]
                }
            },
            "in": {
                "extensions": {
                    "artifacts": [{"sha256":"aa","kind":"document","text_ref":null}],
                    "attachment_notes": [{"code":"too_large","message":"over 10 MB"}]
                }
            },
            "prev": {}, "node": {}, "state": {},
        });
        let notes = render_template_value(
            &Value::String("{{in.extensions.attachment_notes}}".into()),
            &ctx,
            TemplateOptions::default(),
        )
        .unwrap();
        assert_eq!(
            notes[0]["code"],
            json!("too_large"),
            "exact expression keeps the array"
        );
        let meta = render_template_value(
            &Value::String("{{in.extensions.artifacts}}".into()),
            &ctx,
            TemplateOptions::default(),
        )
        .unwrap();
        assert_eq!(meta[0]["kind"], json!("document"));
    }

    #[test]
    fn unresolved_extension_keys_render_empty_string_which_the_agent_node_treats_as_none() {
        let ctx =
            json!({"entry": {"attachments": []}, "in": {}, "prev": {}, "node": {}, "state": {}});
        for expr in [
            "{{in.extensions.artifacts}}",
            "{{in.extensions.attachment_notes}}",
            "{{in.attachments}}",
        ] {
            let out = render_template_value(
                &Value::String(expr.into()),
                &ctx,
                TemplateOptions::default(),
            )
            .unwrap();
            assert_eq!(out, json!(""), "{expr}");
        }
    }

    #[test]
    fn missing_attachment_index_renders_empty_string_like_any_missing_path() {
        let ctx = json!({"entry": {"attachments": []}, "prev": {}, "node": {}, "state": {}});
        let out = render_template_value(
            &Value::String("{{entry.attachments[0].text}}".into()),
            &ctx,
            TemplateOptions::default(),
        )
        .unwrap();
        assert_eq!(out, json!(""));
    }
}
