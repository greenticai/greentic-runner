//! Phase A: prove the agentic-worker tool-dispatch plumbing is wired and
//! fail-safe.
//!
//! This is the always-on guarantee. It exercises the AW-side helpers
//! (`list_tools_for_llm` + `dispatch_tool_call`) against an empty
//! [`ExtensionRuntime`], asserting that:
//!   * an allow-listed tool whose extension is not loaded is invisible to the
//!     LLM (no schema emitted), and
//!   * dispatching such a call surfaces a wrapped [`AgentError::ToolDispatch`]
//!     error rather than panicking or hanging.
//!
//! The positive end-to-end path — a *real* extension's tool actually listed and
//! invoked through WASM — requires a `cargo-component`/`gtdx`-built extension
//! and is covered by `greentic-ext-runtime`'s own gated tests
//! (`tests/ac_invoke.rs`, gated on `GTDX_TEST_GTXPACK`; `tests/scaffold_e2e.rs`,
//! gated on `GTDX_RUN_BUILD=1`). The in-tree `signed_fixture` builds an empty
//! component that exports no tools interface, so it cannot stand in for that
//! path here — duplicating a gated copy in this crate would add no coverage.

use std::sync::Arc;

use greentic_aw_runtime::config::ToolRef;
use greentic_aw_runtime::state::ToolCallRecord;
use greentic_aw_runtime::tenant::TenantContext;
use greentic_aw_runtime::tools::{dispatch_tool_call, list_tools_for_llm};
use greentic_ext_runtime::ExtensionRuntime;

#[tokio::test(flavor = "multi_thread")]
async fn unloaded_tool_is_invisible_to_llm_and_dispatch_fails_safe() {
    let runtime = ExtensionRuntime::for_test().unwrap();

    let allowed = vec![ToolRef {
        extension_id: "greentic.absent".into(),
        tool_name: "nope".into(),
        description: None,
        input_schema: None,
        usage_note: None,
    }];

    // A tool whose extension isn't loaded never reaches the LLM's tool list.
    let schemas = list_tools_for_llm(&runtime, None, None, None, None, &allowed);
    assert!(
        schemas.is_empty(),
        "an unloaded extension must yield no LLM-visible tools, got {schemas:?}"
    );

    // Dispatching it must fail safely: the runtime's NotFound is wrapped by
    // dispatch_tool_call into AgentError::ToolDispatch("invoke: ..."), never a
    // panic and never a hang.
    let call = ToolCallRecord {
        call_id: "c1".into(),
        extension_id: "greentic.absent".into(),
        tool_name: "nope".into(),
        args: serde_json::json!({}),
    };
    let tc = TenantContext::new("t", "e");
    let result = dispatch_tool_call(Arc::new(runtime), None, None, None, None, call, &tc).await;

    let err = result.expect_err("dispatch against an unloaded extension must error");
    let message = err.to_string();
    assert!(
        message.contains("invoke"),
        "dispatch error must come from the invoke path (wrapped tool dispatch), got: {message}"
    );
}
