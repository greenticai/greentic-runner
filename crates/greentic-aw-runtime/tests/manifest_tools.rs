//! Integration tests for [`greentic_aw_runtime::manifest_tools::manifest_to_tool_refs`].
//!
//! Verifies the additive bridge from `DigitalWorkerManifest.extension_tools`
//! into the AW runtime's `ToolRef` catalog: agentic-worker filtering, order
//! preservation, and exact-pair de-duplication.
//!
//! `ExtensionTool` fixtures are built via JSON deserialisation because
//! `greentic-dw-manifest` does not re-export `AgenticWorkerMetadata`
//! (from `greentic-extension-sdk-contract`), and that crate is not a direct
//! dependency of this crate. The required `agentic_worker_metadata` field
//! accepts `{}` (it derives `Default`), keeping the test free of an extra
//! dev-dependency.

use greentic_aw_runtime::ToolRef;
use greentic_aw_runtime::manifest_tools::manifest_to_tool_refs;
use greentic_dw_manifest::{DigitalWorkerManifest, ExtensionTool};

/// Build an [`ExtensionTool`] fixture from primitive parts via JSON.
fn extension_tool(extension_id: &str, tool_name: &str, capabilities: &[&str]) -> ExtensionTool {
    let capabilities_json = serde_json::to_string(capabilities).expect("encode capabilities");
    let raw = format!(
        r#"{{
            "extension_id": "{extension_id}",
            "extension_version": "1.0.0",
            "tool_name": "{tool_name}",
            "description": "snapshot description",
            "input_schema_json": "{{\"type\":\"object\"}}",
            "capabilities": {capabilities_json},
            "agentic_worker_metadata": {{}}
        }}"#
    );
    serde_json::from_str(&raw).expect("decode ExtensionTool fixture")
}

/// Build a minimal v0.3 [`DigitalWorkerManifest`] carrying the given tools.
fn manifest_with_tools(tools: Vec<ExtensionTool>) -> DigitalWorkerManifest {
    let mut manifest: DigitalWorkerManifest = serde_json::from_str(
        r#"{
            "version": "0.3",
            "id": "dw.test",
            "display_name": "Test Worker",
            "tenancy": { "tenant": "tenant-a", "team_policy": "disabled" },
            "locale": {
                "worker_default_locale": "en-US",
                "policy": "prefer_requested",
                "propagation": "propagate_to_delegates",
                "output": "match_requested"
            },
            "extension_tools": []
        }"#,
    )
    .expect("decode DigitalWorkerManifest fixture");
    manifest.extension_tools = tools;
    manifest
}

#[test]
fn maps_agentic_worker_tools_to_tool_refs() {
    let manifest = manifest_with_tools(vec![
        extension_tool("greentic.http", "fetch", &["agentic_worker"]),
        extension_tool("greentic.cards", "validate", &["flow", "agentic_worker"]),
    ]);

    let tool_refs = manifest_to_tool_refs(&manifest);

    assert_eq!(
        tool_refs,
        vec![
            ToolRef {
                extension_id: "greentic.http".to_string(),
                tool_name: "fetch".to_string(),
                description: None,
                input_schema: None,
                usage_note: None,
            },
            ToolRef {
                extension_id: "greentic.cards".to_string(),
                tool_name: "validate".to_string(),
                description: None,
                input_schema: None,
                usage_note: None,
            },
        ]
    );
}

#[test]
fn skips_tools_without_agentic_worker_capability() {
    let manifest = manifest_with_tools(vec![
        extension_tool("greentic.http", "fetch", &["agentic_worker"]),
        extension_tool("greentic.flow_only", "route", &["flow"]),
    ]);

    let tool_refs = manifest_to_tool_refs(&manifest);

    assert_eq!(
        tool_refs,
        vec![ToolRef {
            extension_id: "greentic.http".to_string(),
            tool_name: "fetch".to_string(),
            description: None,
            input_schema: None,
            usage_note: None,
        }]
    );
}

#[test]
fn empty_manifest_yields_empty_vec() {
    let manifest = manifest_with_tools(vec![]);
    assert!(manifest_to_tool_refs(&manifest).is_empty());
}

#[test]
fn deduplicates_exact_pairs_keeping_first() {
    let manifest = manifest_with_tools(vec![
        extension_tool("greentic.http", "fetch", &["agentic_worker"]),
        extension_tool("greentic.http", "fetch", &["agentic_worker", "flow"]),
        extension_tool("greentic.http", "post", &["agentic_worker"]),
    ]);

    let tool_refs = manifest_to_tool_refs(&manifest);

    assert_eq!(
        tool_refs,
        vec![
            ToolRef {
                extension_id: "greentic.http".to_string(),
                tool_name: "fetch".to_string(),
                description: None,
                input_schema: None,
                usage_note: None,
            },
            ToolRef {
                extension_id: "greentic.http".to_string(),
                tool_name: "post".to_string(),
                description: None,
                input_schema: None,
                usage_note: None,
            },
        ]
    );
}

#[test]
fn preserves_declaration_order() {
    let manifest = manifest_with_tools(vec![
        extension_tool("ext.c", "gamma", &["agentic_worker"]),
        extension_tool("ext.a", "alpha", &["agentic_worker"]),
        extension_tool("ext.b", "beta", &["agentic_worker"]),
    ]);

    let tool_refs = manifest_to_tool_refs(&manifest);
    let ordered: Vec<&str> = tool_refs.iter().map(|t| t.tool_name.as_str()).collect();

    assert_eq!(ordered, vec!["gamma", "alpha", "beta"]);
}

/// A pair with the same `tool_name` but a different `extension_id` is NOT a
/// duplicate — only exact `(extension_id, tool_name)` pairs are collapsed.
#[test]
fn same_tool_name_different_extension_is_not_deduped() {
    let manifest = manifest_with_tools(vec![
        extension_tool("ext.a", "send", &["agentic_worker"]),
        extension_tool("ext.b", "send", &["agentic_worker"]),
    ]);

    let tool_refs = manifest_to_tool_refs(&manifest);

    assert_eq!(tool_refs.len(), 2);
    assert_eq!(tool_refs[0].extension_id, "ext.a");
    assert_eq!(tool_refs[1].extension_id, "ext.b");
}
