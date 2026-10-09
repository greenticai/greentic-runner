//! Regression test for the describe-v2 parse failure that made every deployed
//! runner report `loaded design extensions loaded=0`.
//!
//! `ExtensionRuntime::register_loaded_from_dir` — the call at
//! `greentic-runner-host/src/runner/agent_node.rs` that populates the agentic
//! worker's tool catalogue — deserializes the extension's `describe.json`
//! through `greentic_extension_sdk_contract::DescribeJson`. That struct and
//! its nested `Tool` both carry `#[serde(deny_unknown_fields)]`, so a field
//! the compiled-in contract does not know about does not decode to `None`: it
//! fails the whole parse, the loader logs `skipping extension that failed to
//! load`, and the extension's tools are silently absent from every turn.
//!
//! The contract this lane used to pin (`=1.2.1-research`) had a three-field
//! `Tool` with no `capabilities`, and its private deserialize helper had no
//! `manifestSha256`. Current gtdx emits BOTH, so every current extension hit
//! two independent rejections. This test pins that both now decode.
//!
//! Deliberately a pure deserialization test: the rejection happened during
//! `serde_json::from_*` on the describe, strictly before any wasm was touched,
//! so proving the parse needs no wasm fixture and no signing chain.

/// Placeholder component digest. Never verified on this path — the parse under
/// test happens strictly before any hashing.
const ZERO_SHA256: &str = "0000000000000000000000000000000000000000000000000000000000000000";

/// The `manifestSha256` the fixture commits to, read back by the assertion.
const FIXTURE_MANIFEST_SHA256: &str =
    "1111111111111111111111111111111111111111111111111111111111111111";

/// A describe.json in the shape current gtdx emits: `manifestSha256` at the
/// root, and a tool carrying `capabilities: ["agentic_worker"]`.
///
/// Both fields were rejected by the previously pinned contract. Neither may be
/// removed from this fixture to make the test pass — each one is a separate
/// half of the outage being pinned.
fn describe_v2_fixture() -> serde_json::Value {
    serde_json::json!({
        "$schema": "https://store.greentic.cloud/schemas/describe-v2.json",
        "apiVersion": "greentic.ai/v2",
        "kind": "DesignExtension",
        "compat": {
            "min_designer_version": ">=1.2.0",
            "min_runner_version": "^0.12.0",
            "contract_version": "1.2.0"
        },
        "metadata": {
            "id": "greentic.describe-v2-fixture",
            "name": "describe-v2 fixture",
            "version": "0.1.0",
            "summary": "Fixture pinning describe-v2 tool capabilities",
            "description": "Not a real extension; exists to pin the parse.",
            "author": { "name": "Greentic", "email": "team@greentic.ai" },
            "license": "MIT",
            "repository": "https://github.com/greenticai/greentic-runner",
            "keywords": ["fixture"]
        },
        "capabilities": { "offered": [], "required": [] },
        "runtime": {
            "memoryLimitMB": 16,
            "permissions": { "network": [], "secrets": [], "callExtensionKinds": [] },
            "components": {
                "fixture": {
                    "gtpack": {
                        "file": "extension.wasm",
                        "sha256": ZERO_SHA256,
                        "pack_id": "greentic.describe-v2-fixture",
                        "component_version": "0.1.0"
                    },
                    "sha256": ZERO_SHA256,
                    "world": "greentic:extension-design/design-extension@0.2.0"
                }
            }
        },
        // The root field the old contract's private deserialize helper had no
        // slot for at all.
        "manifestSha256": FIXTURE_MANIFEST_SHA256,
        "contributions": {
            "tools": [{
                "name": "fixture_search",
                "export": "fixture-search",
                // The per-tool field the old three-field `Tool` rejected.
                "capabilities": ["agentic_worker"],
                "description": "A tool a deployed runner must be able to see."
            }]
        }
    })
}

/// The parse the extension loader performs must accept a current describe.
///
/// Asserting only "it parses" would pass against a contract that silently
/// dropped either field, so both values are read back.
#[test]
fn a_describe_v2_tool_keeps_its_capabilities_and_manifest_sha() {
    let describe: greentic_extension_sdk_contract::DescribeJson =
        serde_json::from_value(describe_v2_fixture()).expect(
            "a current gtdx describe.json must deserialize; this failing is the \
                     `loaded design extensions loaded=0` outage",
        );

    assert_eq!(
        describe.manifest_sha256.as_deref(),
        Some(FIXTURE_MANIFEST_SHA256),
        "root manifestSha256 must survive the parse, not be dropped"
    );

    let tool = describe
        .contributions
        .tools
        .first()
        .expect("the fixture's single tool must be present");
    assert_eq!(tool.name, "fixture_search");
    assert_eq!(
        tool.capabilities.as_deref(),
        Some(["agentic_worker".to_string()].as_slice()),
        "a tool declaring the agentic_worker capability must keep it; dropping it \
         is what leaves an agentic worker with no tools bound"
    );
}
