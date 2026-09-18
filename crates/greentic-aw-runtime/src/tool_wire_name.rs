//! Wire-safe function names for the LLM request.
//!
//! Every provider in the OpenAI-compatible family validates
//! `tools[].function.name` against `^[a-zA-Z0-9_-]{1,64}$` and rejects the
//! WHOLE request with a 400 when one entry fails. A badly-named tool therefore
//! does not degrade to a missing tool — it takes every turn of the
//! conversation down, surfacing to the operator as the sanitised
//! [`crate::error::LlmError`] copy ("I'm having trouble reaching my reasoning
//! system") with nothing naming the tool that caused it.
//!
//! [`crate::encode_tool_name`] escapes exactly one character (`.` → `_DOT_`)
//! because it was written for dotted extension ids. Four of the id families
//! [`crate::tools::list_tools_for_llm`] resolves are not dotted:
//! `mcp:<server_id>`, `component:<oci-ref>`, `sorla:<pack>` and `flow:<ref>`
//! all carry a colon, an OCI ref adds `/` and `@`, an MCP server may name its
//! own tools with dots (the upstream codec escapes the extension id only,
//! never the tool name), and `mcp:` + a UUID + a tool name is comfortably past
//! 64 characters. All four leak straight onto the wire.
//!
//! The rule here: keep the [`crate::encode_tool_name`] name whenever it is
//! already acceptable — so every tool that works today is byte-identical, and
//! a name recorded by an older build still decodes — and otherwise slugify,
//! cap, and append a digest of the exact `(extension_id, tool_name)` pair.
//! The digest is what keeps a LOSSY, TRUNCATED name unique: `mcp:srv-1` and
//! `mcp:srv/1` slugify to the same string, and dispatching one tool's
//! arguments to another tool is a worse failure than the 400 this module
//! exists to prevent.
//!
//! A sanitised name is not reversible by string surgery, so decoding goes
//! through [`ToolNameCodec`], built per request from the tool list the model
//! was actually shown.

use std::collections::HashMap;

use sha2::{Digest, Sha256};

use crate::llm::LlmToolSchema;
use crate::llm_openai::{encode_tool_name, split_tool_name};

/// Provider cap on `function.name`.
const MAX_WIRE_LEN: usize = 64;
/// Hex characters of the pair digest appended to a sanitised name.
const DIGEST_LEN: usize = 8;
/// Joins the two halves, mirroring [`encode_tool_name`].
const SEPARATOR: &str = "_FN_";
/// Characters the extension slug keeps before the tool slug has to give budget
/// back, so a long tool name can never starve the half naming WHICH server or
/// component the tool came from.
const MIN_EXT_SLUG: usize = 8;

/// True when a provider will accept `name` as a `function.name`.
#[must_use]
pub fn is_wire_safe(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_WIRE_LEN
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

/// The name to put on the wire for one `(extension_id, tool_name)` pair.
///
/// Deterministic: the same pair always encodes to the same name, so a tool
/// call recorded in the conversation history keeps matching the tool list on
/// every later turn.
#[must_use]
pub fn wire_tool_name(extension_id: &str, tool_name: &str) -> String {
    let upstream = encode_tool_name(extension_id, tool_name);
    if is_wire_safe(&upstream) {
        return upstream;
    }

    let digest = pair_digest(extension_id, tool_name);
    // `-1` for the underscore that joins the body to the digest.
    let budget = MAX_WIRE_LEN - DIGEST_LEN - 1;
    let body = compose(&slug(extension_id), &slug(tool_name), budget);
    if body.is_empty() {
        // An id made entirely of punctuation still has to reach the model as
        // something, and the digest alone is both safe and unique.
        return digest;
    }
    format!("{body}_{digest}")
}

/// Decoder for one request's tool list.
///
/// A sanitised name cannot be split back apart — the map is the inverse. A
/// name the map does not know (one [`encode_tool_name`] already made safe, or
/// a tool the agent stopped declaring between turns) falls back to
/// [`split_tool_name`], which is exactly right for the unchanged branch of
/// [`wire_tool_name`].
#[derive(Debug, Default)]
pub struct ToolNameCodec {
    by_wire: HashMap<String, (String, String)>,
}

impl ToolNameCodec {
    #[must_use]
    pub fn for_tools(tools: &[LlmToolSchema]) -> Self {
        let mut by_wire: HashMap<String, (String, String)> = HashMap::new();
        for tool in tools {
            let wire = wire_tool_name(&tool.extension_id, &tool.tool_name);
            let pair = (tool.extension_id.clone(), tool.tool_name.clone());
            if let Some(existing) = by_wire.get(&wire)
                && *existing != pair
            {
                // Two distinct tools must not share a wire name: the model
                // would pick one and the runtime would dispatch the other.
                // Keep the first binding and say so — overwriting silently is
                // how a tool call ends up running a different tool.
                tracing::warn!(
                    wire_name = %wire, kept = ?existing, dropped = ?pair,
                    "two declared tools encode to the same LLM tool name; keeping the first"
                );
                continue;
            }
            by_wire.insert(wire, pair);
        }
        Self { by_wire }
    }

    /// `(extension_id, tool_name)` for a name the model emitted.
    #[must_use]
    pub fn decode(&self, wire: &str) -> (String, String) {
        self.by_wire
            .get(wire)
            .cloned()
            .unwrap_or_else(|| split_tool_name(wire))
    }
}

/// First [`DIGEST_LEN`] hex characters of the pair's SHA-256. The unit
/// separator keeps `("ab", "c")` and `("a", "bc")` apart.
fn pair_digest(extension_id: &str, tool_name: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(extension_id.as_bytes());
    hasher.update([0x1f]);
    hasher.update(tool_name.as_bytes());
    let bytes = hasher.finalize();
    let mut out = String::with_capacity(DIGEST_LEN);
    for byte in bytes.iter().take(DIGEST_LEN / 2) {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

/// Keep what the provider accepts, collapse runs of anything else into a
/// single `_`, and trim the separators off both ends.
fn slug(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut prev_underscore = false;
    for c in raw.chars() {
        if c.is_ascii_alphanumeric() || c == '-' {
            out.push(c);
            prev_underscore = false;
        } else if !prev_underscore {
            out.push('_');
            prev_underscore = true;
        }
    }
    out.trim_matches('_').to_string()
}

/// Join the two slugs within `budget`, truncating the extension half first so
/// the tool half — the part telling the model what the tool DOES — survives.
fn compose(ext_slug: &str, tool_slug: &str, budget: usize) -> String {
    if ext_slug.is_empty() {
        return clamp(tool_slug, budget);
    }
    let reserved = ext_slug.len().min(MIN_EXT_SLUG);
    let tool = clamp(tool_slug, budget.saturating_sub(SEPARATOR.len() + reserved));
    let ext = clamp(
        ext_slug,
        budget.saturating_sub(SEPARATOR.len() + tool.len()),
    );
    if ext.is_empty() {
        return tool;
    }
    if tool.is_empty() {
        return ext;
    }
    format!("{ext}{SEPARATOR}{tool}")
}

/// Truncate to `max` bytes (the input is ASCII by construction) and drop any
/// separator the cut left dangling.
fn clamp(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    s[..max].trim_end_matches(['_', '-']).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_safe(name: &str) {
        assert!(
            is_wire_safe(name),
            "provider would reject this function name: {name:?}"
        );
    }

    fn schema(extension_id: &str, tool_name: &str) -> LlmToolSchema {
        LlmToolSchema {
            extension_id: extension_id.into(),
            tool_name: tool_name.into(),
            description: String::new(),
            parameters: serde_json::json!({ "type": "object" }),
        }
    }

    /// A dotted extension id must stay BYTE-IDENTICAL to what
    /// [`encode_tool_name`] produces: a name a running conversation already
    /// carries has to keep decoding, and renaming a working tool buys nothing.
    #[test]
    fn an_already_acceptable_name_is_left_exactly_as_encode_tool_name_makes_it() {
        for (ext, tool) in [
            ("greentic.adaptive-cards", "validate_card"),
            ("http", "fetch"),
            ("", "remember"),
        ] {
            assert_eq!(wire_tool_name(ext, tool), encode_tool_name(ext, tool));
        }
    }

    /// The four prefixed families, each of which 400'd the whole request.
    #[test]
    fn every_prefixed_id_family_is_made_acceptable() {
        for (ext, tool) in [
            ("mcp:7f1c9a80-2b4e-4d55-9f1a-6c0d2e3b4a51", "get_quote"),
            ("component:ghcr.io/greentic/demo@sha256:abc123", "quote"),
            ("sorla:acme.insurance", "create_claim"),
            ("flow:quote-flow", "run"),
            // Upstream escapes the extension id only, so a dotted TOOL name
            // reached the wire untouched.
            ("mcp:srv-1", "insurance.quote"),
        ] {
            assert_safe(&wire_tool_name(ext, tool));
        }
    }

    #[test]
    fn a_colon_bearing_id_keeps_its_tool_half_readable() {
        let name = wire_tool_name("mcp:7f1c9a80-2b4e-4d55-9f1a-6c0d2e3b4a51", "get_quote");
        assert!(name.contains("get_quote"), "got: {name}");
    }

    #[test]
    fn an_over_long_pair_is_capped_at_the_provider_limit() {
        let name = wire_tool_name(&format!("mcp:{}", "x".repeat(120)), &"long_tool".repeat(9));
        assert_safe(&name);
        assert_eq!(name.len(), MAX_WIRE_LEN, "the cap should be used in full");
    }

    #[test]
    fn an_all_punctuation_pair_still_yields_a_usable_name() {
        assert_safe(&wire_tool_name("...", "///"));
    }

    #[test]
    fn encoding_is_deterministic() {
        assert_eq!(
            wire_tool_name("mcp:srv-1", "quote"),
            wire_tool_name("mcp:srv-1", "quote")
        );
    }

    #[test]
    fn the_codec_decodes_every_declared_tool_back_to_its_dispatch_key() {
        let tools = vec![
            schema("mcp:7f1c9a80-2b4e-4d55-9f1a-6c0d2e3b4a51", "get_quote"),
            schema("component:ghcr.io/greentic/demo:1.2.0", "quote"),
            schema("sorla:acme.insurance", "create_claim"),
            schema("flow:quote-flow", "run"),
            schema("greentic.adaptive-cards", "validate_card"),
        ];
        let codec = ToolNameCodec::for_tools(&tools);
        for tool in &tools {
            let wire = wire_tool_name(&tool.extension_id, &tool.tool_name);
            assert_eq!(
                codec.decode(&wire),
                (tool.extension_id.clone(), tool.tool_name.clone()),
                "{wire} must dispatch to the tool the model was shown"
            );
        }
    }

    #[test]
    fn an_unknown_name_falls_back_to_the_split() {
        let codec = ToolNameCodec::default();
        assert_eq!(
            codec.decode("greentic_DOT_adaptive-cards_FN_validate_card"),
            (
                "greentic.adaptive-cards".to_string(),
                "validate_card".to_string()
            )
        );
    }

    /// The digest is what keeps two ids that slugify identically apart.
    #[test]
    fn ids_that_slugify_identically_still_get_distinct_names() {
        let a = wire_tool_name("mcp:srv-1", "quote");
        let b = wire_tool_name("mcp:srv/1", "quote");
        assert_ne!(a, b, "slug collision leaked into the wire name");
        let codec =
            ToolNameCodec::for_tools(&[schema("mcp:srv-1", "quote"), schema("mcp:srv/1", "quote")]);
        assert_eq!(codec.decode(&a).0, "mcp:srv-1");
        assert_eq!(codec.decode(&b).0, "mcp:srv/1");
    }
}
