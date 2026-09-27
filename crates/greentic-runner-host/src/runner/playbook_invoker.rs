//! Runner-host implementation of `greentic_aw_runtime::PlaybookSource`.
//!
//! Reads the `assets/playbooks/<id>.yaml` documents the loaded packs carry and
//! projects each into a `PlaybookOperation`. This is the runner-host half of the
//! `playbook:` tool seam; the aw-runtime half depends only on the trait + JSON,
//! so this is where `PackRuntime` and YAML enter.
//!
//! Contract: `docs/playbook-contract-v1.md` in greentic-designer. This reader is
//! deliberately its OWN projection rather than an import of the designer's
//! `PlaybookDoc` — runner-host cannot depend on greentic-designer, exactly as it
//! keeps its own reader for `assets/mcp-routes.json`. The contract is what keeps
//! the two in step; there is no shared type to lean on.
//!
//! Every refusal here OMITS the playbook and warns, never fails a load: a
//! malformed document costs its own tool and nothing else, so a worker binding
//! four skills and one bad file keeps three.

#![cfg(feature = "agentic-worker")]

use std::collections::BTreeMap;
use std::sync::Arc;

use greentic_aw_runtime::config::{GuardrailRef, ToolRef};
use greentic_aw_runtime::{
    PlaybookLlmCapability, PlaybookLlmRequirement, PlaybookLlmTier, PlaybookOperation,
    PlaybookSource,
};
use serde::Deserialize;
use serde_yaml_bw as serde_yaml;

use crate::pack::PackRuntime;

/// The highest `descriptor_version` this build knows.
///
/// A document above it is omitted rather than read for the fields we recognise:
/// a playbook is a procedure with guardrails, and partially honouring one is
/// worse than declining it.
///
/// **2, not 1, and the difference is every playbook that exists.** An ABSENT
/// field reads as 1 (`default_descriptor_version`, matching the designer's own
/// `PlaybookDoc`), but the Playbook Studio writes 2 on every document it
/// creates (`EMPTY_PLAYBOOK` in `web/src/features/playbook-composer/types.ts`)
/// and its LLM auto-fill emits 2 as well. A ceiling of 1 therefore refuses
/// every authored playbook and offers none of them as a tool — the silent
/// tool-dropping this whole reader exists to make impossible, in the reader
/// itself. Raise it only alongside the fields a new version adds.
const MAX_DESCRIPTOR_VERSION: u32 = 2;

// ---------------------------------------------------------------------------
// The document, as this reader projects it
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct Document {
    #[serde(default = "default_descriptor_version")]
    descriptor_version: u32,
    playbook_id: String,
    #[serde(default)]
    summary: String,
    #[serde(default)]
    execution: Execution,
    #[serde(default)]
    inputs: Vec<Input>,
    #[serde(default)]
    instructions: Option<String>,
    #[serde(default)]
    llm: Option<Llm>,
    #[serde(default)]
    tools: Vec<Tool>,
    #[serde(default)]
    guardrails: Vec<Guardrail>,
}

fn default_descriptor_version() -> u32 {
    1
}

#[derive(Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Execution {
    /// The DEFAULT arm, and the one nothing can run: `steps` is an opaque value
    /// whose semantics belong to the extension that authored them.
    #[default]
    Deterministic,
    Agentic,
}

#[derive(Debug, Deserialize)]
struct Input {
    name: String,
    #[serde(default)]
    input_type: Option<String>,
    #[serde(default)]
    required: bool,
    #[serde(default)]
    description: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Llm {
    #[serde(default)]
    role: Option<String>,
    #[serde(default)]
    requires: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct Tool {
    extension_id: String,
    tool_name: String,
}

#[derive(Debug, Deserialize)]
struct Guardrail {
    capability: String,
    #[serde(default)]
    config: Option<serde_json::Value>,
}

// ---------------------------------------------------------------------------
// Projection
// ---------------------------------------------------------------------------

/// JSON Schema for the document's declared inputs.
///
/// A playbook declares its inputs, so this reader emits a real schema rather
/// than the open `{"type": "object"}` the flow invoker falls back to — the
/// information a flow lacks at list time is present here.
///
/// An `input_type` this function does not recognise leaves the property
/// UNTYPED rather than defaulting it to `string`: an untyped property accepts
/// anything, which is honest about not knowing, while a wrong `type` makes the
/// provider refuse arguments the playbook would have accepted.
fn parameters_for(inputs: &[Input]) -> serde_json::Value {
    let mut properties = serde_json::Map::new();
    let mut required = Vec::new();
    for input in inputs {
        let mut property = serde_json::Map::new();
        if let Some(kind) = input.input_type.as_deref().and_then(json_schema_type) {
            property.insert("type".into(), serde_json::Value::String(kind.into()));
        }
        if let Some(description) = input
            .description
            .as_deref()
            .filter(|d| !d.trim().is_empty())
        {
            property.insert(
                "description".into(),
                serde_json::Value::String(description.to_string()),
            );
        }
        properties.insert(input.name.clone(), serde_json::Value::Object(property));
        if input.required {
            required.push(serde_json::Value::String(input.name.clone()));
        }
    }
    let mut schema = serde_json::Map::new();
    schema.insert("type".into(), serde_json::Value::String("object".into()));
    schema.insert("properties".into(), serde_json::Value::Object(properties));
    if !required.is_empty() {
        schema.insert("required".into(), serde_json::Value::Array(required));
    }
    serde_json::Value::Object(schema)
}

/// The JSON Schema type a declared `input_type` names, if any.
fn json_schema_type(declared: &str) -> Option<&'static str> {
    match declared.trim().to_ascii_lowercase().as_str() {
        "string" | "text" => Some("string"),
        "number" | "float" => Some("number"),
        "integer" | "int" => Some("integer"),
        "boolean" | "bool" => Some("boolean"),
        "object" | "map" => Some("object"),
        "array" | "list" => Some("array"),
        _ => None,
    }
}

fn tier_for(role: Option<&str>) -> PlaybookLlmTier {
    match role.map(str::trim).map(str::to_ascii_lowercase).as_deref() {
        Some("fast") => PlaybookLlmTier::Fast,
        Some("reasoning") => PlaybookLlmTier::Reasoning,
        // `balanced`, absent, and anything unrecognised. Defaulting an unknown
        // tier to the middle is deliberate: a document from a newer publisher
        // should run, and the requirement is a check the host makes rather than
        // a promise this reader can keep.
        _ => PlaybookLlmTier::Balanced,
    }
}

fn capability_for(declared: &str) -> Option<PlaybookLlmCapability> {
    match declared.trim().to_ascii_lowercase().as_str() {
        "tool_calling" => Some(PlaybookLlmCapability::ToolCalling),
        "structured_output" => Some(PlaybookLlmCapability::StructuredOutput),
        "vision" => Some(PlaybookLlmCapability::Vision),
        "long_context" => Some(PlaybookLlmCapability::LongContext),
        _ => None,
    }
}

/// Project one parsed document, or say why it cannot be offered as a tool.
fn operation_for(doc: Document) -> Result<PlaybookOperation, String> {
    if doc.descriptor_version > MAX_DESCRIPTOR_VERSION {
        return Err(format!(
            "descriptor_version {} is newer than this build understands ({MAX_DESCRIPTOR_VERSION})",
            doc.descriptor_version
        ));
    }
    if doc.execution != Execution::Agentic {
        return Err(
            "execution is not `agentic`; nothing in this runtime can run a deterministic playbook"
                .to_string(),
        );
    }
    let instructions = doc
        .instructions
        .as_deref()
        .map(str::trim)
        .filter(|i| !i.is_empty())
        .ok_or_else(|| "an agentic playbook with no instructions has no turn to run".to_string())?
        .to_string();
    if doc.playbook_id.trim().is_empty() {
        return Err("playbook_id is empty".to_string());
    }

    let llm = doc.llm.unwrap_or(Llm {
        role: None,
        requires: Vec::new(),
    });
    let mut requires: Vec<PlaybookLlmCapability> = llm
        .requires
        .iter()
        .filter_map(|c| capability_for(c))
        .collect();
    requires.sort();
    requires.dedup();

    let summary = if doc.summary.trim().is_empty() {
        doc.playbook_id.clone()
    } else {
        doc.summary.clone()
    };

    Ok(PlaybookOperation {
        parameters: parameters_for(&doc.inputs),
        playbook_id: doc.playbook_id,
        description: summary,
        instructions,
        llm: PlaybookLlmRequirement {
            tier: tier_for(llm.role.as_deref()),
            requires,
        },
        allow_list: doc
            .tools
            .into_iter()
            .map(|t| ToolRef {
                extension_id: t.extension_id,
                tool_name: t.tool_name,
                description: None,
                input_schema: None,
                usage_note: None,
            })
            .collect(),
        guardrails: doc
            .guardrails
            .into_iter()
            .map(|g| GuardrailRef {
                cap_id: g.capability,
                offer_id: None,
                config: g.config.unwrap_or(serde_json::Value::Null),
                mode: Default::default(),
            })
            .collect(),
    })
}

/// Parse and project one document's bytes.
pub(crate) fn operation_from_yaml(bytes: &[u8]) -> Result<PlaybookOperation, String> {
    let text = std::str::from_utf8(bytes).map_err(|e| format!("not UTF-8: {e}"))?;
    let doc: Document = serde_yaml::from_str(text).map_err(|e| format!("not a playbook: {e}"))?;
    operation_for(doc)
}

// ---------------------------------------------------------------------------
// The source
// ---------------------------------------------------------------------------

/// [`PlaybookSource`] backed by the operator's loaded packs.
pub struct PackRuntimePlaybookSource {
    packs: Vec<Arc<PackRuntime>>,
}

impl PackRuntimePlaybookSource {
    /// Construct over the operator's loaded packs.
    pub fn new(packs: Vec<Arc<PackRuntime>>) -> Self {
        Self { packs }
    }
}

impl PlaybookSource for PackRuntimePlaybookSource {
    /// Every playbook every loaded pack carries, projected for the tool seam.
    ///
    /// An id declared by two packs resolves to the FIRST pack that carries it,
    /// so a later pack cannot silently replace another's skill. The collision
    /// is warned rather than merged: two documents under one id is an authoring
    /// mistake, and picking the second would make which one runs depend on load
    /// order.
    fn list_playbooks(&self) -> Vec<PlaybookOperation> {
        let mut by_id: BTreeMap<String, PlaybookOperation> = BTreeMap::new();
        for pack in &self.packs {
            for id in pack.playbook_ids() {
                let entry = format!("assets/playbooks/{id}.yaml");
                let bytes = match pack.read_asset(&entry) {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        tracing::warn!(
                            playbook = %id, error = %error,
                            "listed a playbook the pack could not read; skipping"
                        );
                        continue;
                    }
                };
                match operation_from_yaml(&bytes) {
                    Ok(op) => {
                        if by_id.contains_key(&op.playbook_id) {
                            tracing::warn!(
                                playbook = %op.playbook_id,
                                "two loaded packs carry this playbook; keeping the first"
                            );
                            continue;
                        }
                        by_id.insert(op.playbook_id.clone(), op);
                    }
                    Err(reason) => tracing::warn!(
                        playbook = %id, reason = %reason,
                        "playbook not offered as a tool"
                    ),
                }
            }
        }
        by_id.into_values().collect()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    const AGENTIC: &str = r#"
descriptor_version: 2
playbook_id: refund
summary: Issue a refund
execution: agentic
instructions: Follow the refund policy.
llm:
  role: reasoning
  requires: [tool_calling]
inputs:
  - name: order_id
    input_type: string
    required: true
    description: The order to refund
  - name: dry_run
    input_type: boolean
tools:
  - extension_id: greentic.billing
    tool_name: refund
guardrails:
  - capability: cap://guardrail/pii
"#;

    #[test]
    fn an_agentic_document_becomes_a_tool() {
        let op = operation_from_yaml(AGENTIC.as_bytes()).expect("projects");
        assert_eq!(op.playbook_id, "refund");
        assert_eq!(op.description, "Issue a refund");
        assert_eq!(op.instructions, "Follow the refund policy.");
        assert_eq!(op.llm.tier, PlaybookLlmTier::Reasoning);
        assert_eq!(op.llm.requires, vec![PlaybookLlmCapability::ToolCalling]);
        assert_eq!(op.allow_list.len(), 1);
        assert_eq!(op.guardrails.len(), 1);
        assert_eq!(op.guardrails[0].cap_id, "cap://guardrail/pii");
    }

    #[test]
    fn declared_inputs_become_a_real_schema_not_an_open_object() {
        let op = operation_from_yaml(AGENTIC.as_bytes()).expect("projects");
        assert_eq!(op.parameters["type"], "object");
        assert_eq!(op.parameters["properties"]["order_id"]["type"], "string");
        assert_eq!(
            op.parameters["properties"]["order_id"]["description"],
            "The order to refund"
        );
        assert_eq!(op.parameters["properties"]["dry_run"]["type"], "boolean");
        assert_eq!(
            op.parameters["required"],
            serde_json::json!(["order_id"]),
            "only the required input is listed"
        );
    }

    /// The default arm, and the one nothing can run.
    #[test]
    fn a_deterministic_playbook_is_refused() {
        let doc = r#"
playbook_id: nightly
instructions: ignored
steps: { anything: true }
"#;
        let err = operation_from_yaml(doc.as_bytes()).expect_err("must refuse");
        assert!(err.contains("agentic"), "got: {err}");
    }

    /// Absent `execution` means deterministic, so it is refused too.
    #[test]
    fn an_absent_execution_reads_as_deterministic() {
        let doc = "playbook_id: p\ninstructions: x\n";
        assert!(operation_from_yaml(doc.as_bytes()).is_err());
    }

    #[test]
    fn a_newer_descriptor_version_is_refused_rather_than_partly_read() {
        let doc = r#"
descriptor_version: 3
playbook_id: refund
execution: agentic
instructions: Follow the policy.
"#;
        let err = operation_from_yaml(doc.as_bytes()).expect_err("must refuse");
        assert!(err.contains("descriptor_version"), "got: {err}");
    }

    /// The version the Playbook Studio actually writes. This test used the
    /// value 2 as its "newer, therefore refused" case, which passed while
    /// refusing every playbook anyone has ever authored — so the accepted case
    /// is now pinned explicitly rather than left implied by the fixture.
    #[test]
    fn the_version_the_studio_writes_is_accepted() {
        let doc = r#"
descriptor_version: 2
playbook_id: refund
execution: agentic
instructions: Follow the policy.
"#;
        let op = operation_from_yaml(doc.as_bytes()).expect("a studio document must be offered");
        assert_eq!(op.playbook_id, "refund");
    }

    /// An absent field still reads as 1, so a document predating the field is
    /// offered rather than refused.
    #[test]
    fn an_absent_descriptor_version_is_accepted_as_version_one() {
        let doc = "playbook_id: p
execution: agentic
instructions: x
";
        let op =
            operation_from_yaml(doc.as_bytes()).expect("a pre-version document must be offered");
        assert_eq!(op.playbook_id, "p");
    }

    #[test]
    fn an_agentic_playbook_with_no_instructions_is_refused() {
        let doc = "playbook_id: p\nexecution: agentic\n";
        let err = operation_from_yaml(doc.as_bytes()).expect_err("must refuse");
        assert!(err.contains("instructions"), "got: {err}");
    }

    #[test]
    fn blank_instructions_count_as_none() {
        let doc = "playbook_id: p\nexecution: agentic\ninstructions: \"   \"\n";
        assert!(operation_from_yaml(doc.as_bytes()).is_err());
    }

    #[test]
    fn an_unparseable_document_is_refused_not_panicked() {
        let err = operation_from_yaml(b"this: is: not: a: playbook").expect_err("must refuse");
        assert!(err.contains("not a playbook"), "got: {err}");
    }

    #[test]
    fn an_empty_summary_falls_back_to_the_id() {
        let doc = "playbook_id: refund\nexecution: agentic\ninstructions: x\n";
        let op = operation_from_yaml(doc.as_bytes()).expect("projects");
        assert_eq!(op.description, "refund");
    }

    /// An unknown type leaves the property untyped rather than guessing
    /// `string`: a wrong `type` makes the provider refuse arguments the
    /// playbook would have accepted.
    #[test]
    fn an_unrecognised_input_type_leaves_the_property_untyped() {
        let doc = r#"
playbook_id: p
execution: agentic
instructions: x
inputs:
  - name: odd
    input_type: quaternion
"#;
        let op = operation_from_yaml(doc.as_bytes()).expect("projects");
        let property = &op.parameters["properties"]["odd"];
        assert!(
            property.get("type").is_none(),
            "an unknown type must not be guessed, got: {property}"
        );
    }

    #[test]
    fn an_unrecognised_tier_defaults_to_balanced_rather_than_refusing() {
        let doc = r#"
playbook_id: p
execution: agentic
instructions: x
llm:
  role: telepathic
  requires: [precognition]
"#;
        let op = operation_from_yaml(doc.as_bytes()).expect("projects");
        assert_eq!(op.llm.tier, PlaybookLlmTier::Balanced);
        assert!(
            op.llm.requires.is_empty(),
            "an unknown capability is dropped, not invented"
        );
    }

    #[test]
    fn duplicate_capabilities_are_deduplicated() {
        let doc = r#"
playbook_id: p
execution: agentic
instructions: x
llm:
  requires: [tool_calling, tool_calling, vision]
"#;
        let op = operation_from_yaml(doc.as_bytes()).expect("projects");
        assert_eq!(op.llm.requires.len(), 2);
    }

    #[test]
    fn an_empty_playbook_id_is_refused() {
        let doc = "playbook_id: \"  \"\nexecution: agentic\ninstructions: x\n";
        let err = operation_from_yaml(doc.as_bytes()).expect_err("must refuse");
        assert!(err.contains("playbook_id"), "got: {err}");
    }

    /// A field this projection does not model must not fail the parse — the
    /// designer's own model is wider and will grow.
    #[test]
    fn an_unmodelled_field_does_not_fail_the_parse() {
        let doc = r#"
playbook_id: p
execution: agentic
instructions: x
output: { shape: whatever }
some_future_field: 3
"#;
        assert!(operation_from_yaml(doc.as_bytes()).is_ok());
    }
}
