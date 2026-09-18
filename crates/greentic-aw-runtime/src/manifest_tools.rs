//! Bridge a [`DigitalWorkerManifest`]'s `extension_tools` snapshot into the
//! AW runtime's [`ToolRef`] catalog.
//!
//! Sub-project #2 snapshots the tools a Digital Worker invokes in
//! `DigitalWorkerManifest.extension_tools: Vec<ExtensionTool>`. A
//! [`crate::ConfigProvider`] can call [`manifest_to_tool_refs`] to derive the
//! `(extension_id, tool_name)` set to wire as `AgentConfig.tools`.
//!
//! ## Snapshot-wins-at-runtime is out of scope
//!
//! This converter only supplies the *set* of `ToolRef`s. The runtime keeps its
//! existing **live** schema resolution: `tools::list_tools_for_llm` resolves
//! each `ToolRef` against `ExtensionRuntime::list_tools()` at call time. The
//! snapshot's `description` / `input_schema_json` are intentionally ignored
//! here.

use crate::config::ToolRef;
use greentic_dw_manifest::DigitalWorkerManifest;

/// Capability literal a tool must declare to be usable by an agentic worker.
/// The manifest's own validation requires every `ExtensionTool` to carry this.
const AGENTIC_WORKER_CAPABILITY: &str = "agentic_worker";

/// Convert a manifest's `extension_tools` into AW runtime [`ToolRef`]s.
///
/// Only tools whose `capabilities` declare [`AGENTIC_WORKER_CAPABILITY`] are
/// included; flow-only tools are skipped. Declaration order is preserved, and
/// exact `(extension_id, tool_name)` pairs are de-duplicated keeping the first
/// occurrence.
///
/// # Arguments
/// * `manifest` - the Digital Worker manifest carrying the tool snapshot
///
/// # Returns
/// The ordered, de-duplicated list of agentic-worker [`ToolRef`]s. Empty when
/// the manifest declares no agentic-worker-capable tools.
///
/// # Example
/// ```no_run
/// # use greentic_dw_manifest::DigitalWorkerManifest;
/// # use greentic_aw_runtime::manifest_tools::manifest_to_tool_refs;
/// # fn demo(manifest: &DigitalWorkerManifest) {
/// let tool_refs = manifest_to_tool_refs(manifest);
/// // feed `tool_refs` into `AgentConfig.tools`
/// # let _ = tool_refs;
/// # }
/// ```
pub fn manifest_to_tool_refs(manifest: &DigitalWorkerManifest) -> Vec<ToolRef> {
    let mut seen_pairs: Vec<(String, String)> = Vec::new();
    let mut tool_refs: Vec<ToolRef> = Vec::new();

    for extension_tool in &manifest.extension_tools {
        let supports_agentic_worker = extension_tool
            .capabilities
            .iter()
            .any(|capability| capability == AGENTIC_WORKER_CAPABILITY);
        if !supports_agentic_worker {
            continue;
        }

        let pair = (
            extension_tool.extension_id.clone(),
            extension_tool.tool_name.clone(),
        );
        if seen_pairs.contains(&pair) {
            continue;
        }

        seen_pairs.push(pair);
        tool_refs.push(ToolRef {
            extension_id: extension_tool.extension_id.clone(),
            tool_name: extension_tool.tool_name.clone(),
            description: None,
            input_schema: None,
            usage_note: None,
        });
    }

    tool_refs
}
