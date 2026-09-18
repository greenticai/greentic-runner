//! End-to-end test: load the real `component-guardrail-pii` WASM extension,
//! wire it through `AgentRuntime`, and assert masking + deny behaviour.
//!
//! This test exercises the full stack:
//!   tempdir ─► properly-signed describe.json + manifest.json
//!           ─► `ExtensionRuntime::register_loaded_from_dir`
//!           ─► capability registry populated with `greentic:guardrail/pii`
//!           ─► `AgentRuntime::with_guardrails` (real evaluator)
//!           ─► `AgentRuntime::step` → inbound + outbound WASM calls
//!
//! ## Loader signature contract (findings)
//!
//! `ExtensionRuntime::register_loaded_from_dir` calls `verify_dir_signature`
//! which:
//! 1. Reads `describe.json` and calls
//!    `verify_describe_self_consistent` — requires an Ed25519 signature
//!    field (self-integrity check; not anchored to a trust store).
//! 2. Calls `verify_dir_manifest` — requires a `manifest.json` whose SHA-256
//!    is committed to by `describe.manifestSha256`, with per-file hash entries.
//!
//! A `dev-allow-unsigned` feature flag + `GREENTIC_EXT_ALLOW_UNSIGNED` env var
//! bypass is available but NOT compiled in by default.  This test produces a
//! fully valid signed artefact at runtime using an ephemeral Ed25519 key —
//! no bypass needed.  The signed describe covers the whole-archive manifest,
//! and the manifest covers `extension.wasm`, matching the production chain.
//!
//! ## Why the describe.json in dist/ is patched
//!
//! The component-guardrail-pii `dist/describe.json` carries a
//! `contributions.guardrails` field not yet in the SDK's
//! `greentic_extension_sdk_contract::Contributions` struct at v1.2.8-research
//! (`deny_unknown_fields` would reject it during `load_from_dir`).  The helper
//! below builds a minimal but structurally-valid describe that omits the
//! unknown contributions key while keeping the `capabilities.offered` entry
//! (`greentic:guardrail/pii`) that matters for registry resolution.
//!
//! ## Export name resolution (FIXED)
//!
//! `ExtensionRuntime::evaluate_guardrail` resolves the interface by calling
//! `resolve_iface_versions` which looks for
//! `greentic:extension-design/guardrail@0.3.0` in the component's exports.
//!
//! The previously committed dist WASM was stale and exported the interface
//! under the old private namespace `greentic:guardrail-pii/guardrail@0.1.0`.
//! The freshly-built binary (from `component-guardrail-pii` source) correctly
//! exports `greentic:extension-design/guardrail@0.3.0` — the dist artifact
//! has been updated to match (commit fix: sync dist wasm + describe sha).
//!
//! All three e2e tests are now active.

#![cfg(feature = "test-mock")]

use std::sync::Arc;
use std::time::Duration;

use greentic_aw_runtime::config::{
    AgentConfig, AgentLimits, GuardrailMode, GuardrailRef, LlmProviderRef,
};
use greentic_aw_runtime::cost::MockTokenMeter;
use greentic_aw_runtime::error::AgentError;
use greentic_aw_runtime::guardrail::{ExtRuntimeGuardrailEvaluator, StaticGuardrailPolicy};
use greentic_aw_runtime::llm::{LlmRequest, LlmResponse};
use greentic_aw_runtime::mock::{
    MockAgentStateStore, MockConfigProvider, MockTelemetry, NoopToolLedger,
};
use greentic_aw_runtime::tenant::TenantContext;
use greentic_aw_runtime::{AgentInput, AgentRuntime, LlmBackend, LlmError};
use greentic_ext_runtime::{DiscoveryPaths, ExtensionRuntime, RuntimeConfig};

// ─── Helper: an LLM that echoes the last user message back verbatim ─────────

struct EchoLlmBackend;

impl LlmBackend for EchoLlmBackend {
    fn complete<'a>(
        &'a self,
        req: LlmRequest,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<LlmResponse, LlmError>> + Send + 'a>,
    > {
        let last_user = req
            .history
            .iter()
            .filter_map(|m| {
                if let greentic_aw_runtime::state::ChatMessage::User { content } = m {
                    Some(content.clone())
                } else {
                    None
                }
            })
            .next_back()
            .unwrap_or_default();
        Box::pin(async move {
            Ok(LlmResponse {
                content: Some(last_user),
                tool_calls: vec![],
                tokens_in: 1,
                tokens_out: 1,
            })
        })
    }
}

// ─── Helper: build signed extension dir ─────────────────────────────────────

/// Copies `extension.wasm` from the component dist dir into `dest_dir`,
/// writes a minimal but serde-valid and fully-signed `describe.json` +
/// `manifest.json`.
///
/// Uses an ephemeral deterministic Ed25519 key (fixed seed bytes — no
/// randomness required).  Only self-consistency is verified by the loader
/// (the public key is embedded in the describe and the signature covers the
/// canonical JCS payload), so any valid key produces a loadable artefact.
fn write_signed_extension_dir(wasm_src: &std::path::Path, dest_dir: &std::path::Path) {
    use greentic_extension_sdk_contract::{
        MANIFEST_ENTRY_NAME, bind_manifest, build_manifest, sign_describe,
    };

    // 1. Copy the real WASM binary.
    let wasm_bytes = std::fs::read(wasm_src).expect("extension.wasm must be readable");
    std::fs::write(dest_dir.join("extension.wasm"), &wasm_bytes).expect("write extension.wasm");

    // 2. Build a serde-valid DescribeJson.
    //    `contributions.guardrails` is NOT a field in SDK v1.2.8-research
    //    (deny_unknown_fields), so we use an empty contributions block.
    //    The capabilities.offered entry drives registry resolution.
    let describe_value = serde_json::json!({
        "$schema": "https://store.greentic.cloud/schemas/describe-v2.json",
        "apiVersion": "greentic.ai/v2",
        "kind": "DesignExtension",
        "compat": {
            "min_designer_version": ">=1.2.0",
            "min_runner_version": "^0.12.0",
            "contract_version": "1.2.0"
        },
        "metadata": {
            "id": "greentic.guardrail-pii",
            "name": "PII-masking guardrail",
            "version": "0.1.0",
            "summary": "Reference guardrail: masks emails/phones, denies on blocklist",
            "description": "Stateless guardrail extension.",
            "author": { "name": "Greentic", "email": "team@greentic.ai" },
            "license": "MIT",
            "repository": "https://github.com/greenticai/component-guardrail-pii",
            "keywords": ["guardrail", "pii"]
        },
        "capabilities": {
            "offered": [{ "id": "greentic:guardrail/pii", "version": "1.0.0" }],
            "required": []
        },
        "runtime": {
            "memoryLimitMB": 16,
            "permissions": { "network": [], "secrets": [], "callExtensionKinds": [] },
            "components": {
                "guardrail-pii": {
                    "gtpack": {
                        "file": "extension.wasm",
                        "sha256": "0000000000000000000000000000000000000000000000000000000000000000",
                        "pack_id": "greentic.guardrail-pii",
                        "component_version": "0.1.0"
                    },
                    "sha256": "0000000000000000000000000000000000000000000000000000000000000000",
                    "world": "greentic:guardrail-pii/guardrail-pii-extension@0.1.0"
                }
            }
        },
        "contributions": {}
    });

    let mut describe: greentic_extension_sdk_contract::DescribeJson =
        serde_json::from_value(describe_value).expect("describe must deserialize");

    // 3. Build the manifest listing extension.wasm.
    let manifest = build_manifest([("extension.wasm", wasm_bytes.as_slice())]);
    let manifest_bytes = serde_json::to_vec(&manifest).expect("manifest must serialize");

    // 4. Bind the manifest SHA-256 into the describe BEFORE signing.
    bind_manifest(&mut describe, &manifest_bytes);

    // 5. Sign with a deterministic ephemeral key.
    let signing_key = ed25519_dalek::SigningKey::from_bytes(&[0x42u8; 32]);
    sign_describe(&mut describe, &signing_key).expect("sign_describe must succeed");

    // 6. Write both files.
    let describe_json =
        serde_json::to_string_pretty(&describe).expect("describe must re-serialize");
    std::fs::write(dest_dir.join("describe.json"), &describe_json).expect("write describe.json");
    std::fs::write(dest_dir.join(MANIFEST_ENTRY_NAME), &manifest_bytes)
        .expect("write manifest.json");
}

/// Return the path to the built extension.wasm, or `None` if not present.
fn pii_wasm_src() -> Option<std::path::PathBuf> {
    // CARGO_MANIFEST_DIR = <runner-root>/.worktrees/guardrail-capability/crates/greentic-aw-runtime
    // 5 levels up = <greentic-workspace-root>
    let p = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../../../component-guardrail-pii/dist/greentic.guardrail-pii/extension.wasm");
    if p.exists() { Some(p) } else { None }
}

// ─── Smoke: verify signature chain works and the capability lands in registry ─

/// Verifies that `register_loaded_from_dir` succeeds with a properly signed
/// artefact and populates `greentic:guardrail/pii` in the capability registry.
///
/// This test proves the signature chain, manifest binding, and registry
/// wiring are all correct.  It does NOT call `evaluate_guardrail` (which is
/// blocked — see module-level docs).
#[tokio::test]
async fn pii_extension_registers_and_capability_appears_in_registry() {
    let Some(wasm_src) = pii_wasm_src() else {
        eprintln!("SKIP: component-guardrail-pii WASM not found");
        return;
    };

    let tmp = tempfile::TempDir::new().expect("create tempdir");
    let ext_dir = tmp.path().join("greentic.guardrail-pii");
    std::fs::create_dir_all(&ext_dir).expect("create ext subdir");
    write_signed_extension_dir(&wasm_src, &ext_dir);

    let paths = DiscoveryPaths::new(tmp.path().to_path_buf());
    // Root the trust store at the tempdir. Unset, it resolves to the real
    // `$GREENTIC_HOME`/`~/.greentic` — the same store `gtdx install` writes —
    // so this test would pin its throwaway fixture key against the real
    // `greentic.guardrail-pii` id, and the next run (a fresh random key for
    // the same id) would fail `PublisherKeyChanged`. Green once, red forever.
    let mut ext_runtime = ExtensionRuntime::new(
        RuntimeConfig::from_paths(paths).with_trust_root(tmp.path().to_path_buf()),
    )
    .expect("ExtensionRuntime::new");

    ext_runtime
        .register_loaded_from_dir(&ext_dir)
        .expect("signature verification + registration must succeed");

    let registry = ext_runtime.capability_registry();
    let required = [greentic_extension_sdk_contract::CapabilityRef {
        id: "greentic:guardrail/pii".parse().unwrap(),
        version: "*".to_string(),
        deprecated: None,
    }];
    let plan = registry.resolve("guardrail-e2e-test", &required);
    let cap_id: greentic_extension_sdk_contract::CapabilityId =
        "greentic:guardrail/pii".parse().unwrap();

    assert!(
        plan.resolved.contains_key(&cap_id),
        "greentic:guardrail/pii must be in the registry after registration; \
         resolved: {:?}",
        plan.resolved.keys().collect::<Vec<_>>()
    );
    let binding = &plan.resolved[&cap_id];
    assert_eq!(
        binding.extension_id, "greentic.guardrail-pii",
        "binding must point at the registered extension"
    );
}

// ─── Full-stack e2e: masked email and blocklist deny ─────────────────────────

/// Proves: with the PII guardrail wired as a mandatory policy, a prompt
/// containing an email is masked before reaching the echoing LLM, so the
/// final `AgentOutput.reply` does not contain the raw address.
#[tokio::test]
async fn pii_guardrail_masks_inbound_email() {
    let Some(wasm_src) = pii_wasm_src() else {
        eprintln!("SKIP: component-guardrail-pii WASM not found");
        return;
    };

    let tmp = tempfile::TempDir::new().expect("create tempdir");
    let ext_dir = tmp.path().join("greentic.guardrail-pii");
    std::fs::create_dir_all(&ext_dir).expect("create ext subdir");
    write_signed_extension_dir(&wasm_src, &ext_dir);

    let (runtime, tc) = build_full_runtime(
        &wasm_src,
        &tmp,
        &ext_dir,
        serde_json::Value::Null,
        GuardrailMode::Enforce,
    );

    let output = runtime
        .step(
            tc,
            "session-e2e-mask",
            "pii-agent",
            AgentInput {
                text: "email me at x@y.com please".into(),
                conversational: false,
            },
        )
        .await
        .expect("step must succeed — guardrail masks but does not deny");

    assert!(
        !output.reply.contains("x@y.com"),
        "raw email must not appear in reply; got: {:?}",
        output.reply
    );
    assert!(
        output.reply.contains("[REDACTED_EMAIL]"),
        "masked email marker expected in reply; got: {:?}",
        output.reply
    );
}

/// Proves: a blocklist match in the guardrail config causes `run_step` to
/// return `AgentError::GuardrailDenied { direction: Inbound, code: "permission_denied" }`.
#[tokio::test]
async fn pii_guardrail_denies_blocklist_match() {
    let Some(wasm_src) = pii_wasm_src() else {
        eprintln!("SKIP: component-guardrail-pii WASM not found");
        return;
    };

    let tmp = tempfile::TempDir::new().expect("create tempdir");
    let ext_dir = tmp.path().join("greentic.guardrail-pii");
    std::fs::create_dir_all(&ext_dir).expect("create ext subdir");
    write_signed_extension_dir(&wasm_src, &ext_dir);

    let blocklist_config = serde_json::json!({ "blocklist": ["forbidden"] });
    let (runtime, tc) = build_full_runtime(
        &wasm_src,
        &tmp,
        &ext_dir,
        blocklist_config,
        GuardrailMode::Enforce,
    );

    let result = runtime
        .step(
            tc,
            "session-e2e-deny",
            "pii-agent",
            AgentInput {
                text: "this message contains forbidden content".into(),
                conversational: false,
            },
        )
        .await;

    match result {
        Err(AgentError::GuardrailDenied {
            direction,
            code,
            message,
            ..
        }) => {
            assert_eq!(
                direction,
                greentic_aw_runtime::guardrail::GuardrailDirection::Inbound,
                "blocklist denial must be inbound"
            );
            assert_eq!(
                code, "permission_denied",
                "component uses 'permission_denied'"
            );
            assert!(!message.is_empty(), "denial message must not be empty");
        }
        other => panic!("expected GuardrailDenied, got: {other:?}"),
    }
}

// ─── Observer wiring: real `run_step` → `StepObserver::on_guardrail` ────────
//
// The two `guardrail_notify_pattern_reports_a_*_denial` unit tests in
// `crates/greentic-aw-runtime/src/loop.rs` pin the notify *pattern* only
// (they call a test-local re-implementation of the notify logic, not
// `run_step`). These two tests close that gap: they drive the REAL
// `AgentRuntime::step_with_observer` → `run_step` → `observer.on_guardrail`
// path, through the same signed WASM guardrail extension the rest of this
// file uses, for both Enforce (blocked) and Monitor (recorded, not blocked)
// modes.

#[derive(Default)]
struct RecordingObserver {
    guardrails: std::sync::Mutex<Vec<greentic_aw_runtime::guardrail::GuardrailObservation>>,
}

impl greentic_aw_runtime::StepObserver for RecordingObserver {
    fn on_guardrail(&self, obs: &greentic_aw_runtime::guardrail::GuardrailObservation) {
        self.guardrails.lock().unwrap().push(obs.clone());
    }
}

/// Proves the real `run_step` wiring notifies a `StepObserver` for an
/// Enforce-mode blocklist denial (the turn is blocked AND observed).
#[tokio::test]
async fn pii_guardrail_enforce_denial_notifies_real_observer() {
    let Some(wasm_src) = pii_wasm_src() else {
        eprintln!("SKIP: component-guardrail-pii WASM not found");
        return;
    };

    let tmp = tempfile::TempDir::new().expect("create tempdir");
    let ext_dir = tmp.path().join("greentic.guardrail-pii");
    std::fs::create_dir_all(&ext_dir).expect("create ext subdir");
    write_signed_extension_dir(&wasm_src, &ext_dir);

    let blocklist_config = serde_json::json!({ "blocklist": ["forbidden"] });
    let (runtime, tc) = build_full_runtime(
        &wasm_src,
        &tmp,
        &ext_dir,
        blocklist_config,
        GuardrailMode::Enforce,
    );

    let observer = std::sync::Arc::new(RecordingObserver::default());
    let result = runtime
        .step_with_observer(
            tc,
            "session-e2e-deny-observed",
            "pii-agent",
            AgentInput {
                text: "this message contains forbidden content".into(),
                conversational: false,
            },
            observer.clone(),
        )
        .await;

    assert!(
        matches!(result, Err(AgentError::GuardrailDenied { .. })),
        "expected GuardrailDenied, got: {result:?}"
    );
    let seen = observer.guardrails.lock().unwrap();
    assert_eq!(seen.len(), 1, "a blocked denial must reach the observer");
    assert_eq!(
        seen[0].action,
        greentic_aw_runtime::guardrail::GuardrailAction::Blocked
    );
}

/// Proves the real `run_step` wiring notifies a `StepObserver` for a
/// Monitor-mode blocklist denial (the turn passes through AND is observed).
#[tokio::test]
async fn pii_guardrail_monitor_denial_notifies_real_observer_without_blocking() {
    let Some(wasm_src) = pii_wasm_src() else {
        eprintln!("SKIP: component-guardrail-pii WASM not found");
        return;
    };

    let tmp = tempfile::TempDir::new().expect("create tempdir");
    let ext_dir = tmp.path().join("greentic.guardrail-pii");
    std::fs::create_dir_all(&ext_dir).expect("create ext subdir");
    write_signed_extension_dir(&wasm_src, &ext_dir);

    let blocklist_config = serde_json::json!({ "blocklist": ["forbidden"] });
    let (runtime, tc) = build_full_runtime(
        &wasm_src,
        &tmp,
        &ext_dir,
        blocklist_config,
        GuardrailMode::Monitor,
    );

    let observer = std::sync::Arc::new(RecordingObserver::default());
    let result = runtime
        .step_with_observer(
            tc,
            "session-e2e-monitor-observed",
            "pii-agent",
            AgentInput {
                text: "this message contains forbidden content".into(),
                conversational: false,
            },
            observer.clone(),
        )
        .await;

    assert!(
        result.is_ok(),
        "monitor mode must not block the turn: {result:?}"
    );
    let seen = observer.guardrails.lock().unwrap();
    // Monitor mode passes the ORIGINAL content through unchanged (per the
    // mode semantics, untouched by this fix pass), so the echoing LLM's
    // reply still contains "forbidden" — the outbound guardrail hook fires
    // too. Both the inbound and outbound denials must reach the observer.
    assert_eq!(
        seen.len(),
        2,
        "both the inbound and outbound monitored denials must reach the observer"
    );
    assert!(
        seen.iter()
            .all(|obs| obs.action == greentic_aw_runtime::guardrail::GuardrailAction::Monitored),
        "monitor mode must never block: {seen:?}"
    );
    assert!(
        seen.iter()
            .any(|obs| obs.direction == greentic_aw_runtime::guardrail::GuardrailDirection::Inbound)
    );
    assert!(
        seen.iter().any(
            |obs| obs.direction == greentic_aw_runtime::guardrail::GuardrailDirection::Outbound
        )
    );
}

// ─── Helper: build a full AgentRuntime with PII guardrail ───────────────────

fn build_full_runtime(
    wasm_src: &std::path::Path,
    tmp: &tempfile::TempDir,
    ext_dir: &std::path::Path,
    guardrail_config: serde_json::Value,
    mode: GuardrailMode,
) -> (AgentRuntime, TenantContext) {
    let paths = DiscoveryPaths::new(tmp.path().to_path_buf());
    // Root the trust store at the tempdir — see the note on the direct
    // construction above. Every caller of this helper registers a freshly
    // signed fixture under a real extension id, so an unrooted store would
    // pin throwaway keys into `~/.greentic` and self-poison the next run.
    let mut ext_runtime = ExtensionRuntime::new(
        RuntimeConfig::from_paths(paths).with_trust_root(tmp.path().to_path_buf()),
    )
    .expect("ExtensionRuntime::new");
    ext_runtime
        .register_loaded_from_dir(ext_dir)
        .expect("register_loaded_from_dir");
    let ext_runtime = Arc::new(ext_runtime);

    let evaluator = Arc::new(ExtRuntimeGuardrailEvaluator {
        ext_runtime: ext_runtime.clone(),
    });

    let mandatory_policy = Arc::new(StaticGuardrailPolicy(vec![GuardrailRef {
        cap_id: "greentic:guardrail/pii".into(),
        offer_id: None,
        config: guardrail_config,
        mode,
    }]));

    let tc = TenantContext::new("test-tenant", "test");
    let agent_config = AgentConfig {
        agent_id: "pii-agent".into(),
        system_prompt: "You are a helpful assistant.".into(),
        tools: vec![],
        guardrails: vec![],
        llm: LlmProviderRef {
            provider: "mock".into(),
            model: "echo".into(),
            credential_ref: None,
        },
        limits: AgentLimits {
            max_iter: 2,
            timeout: Duration::from_secs(30),
            ..AgentLimits::default()
        },
        memory: None,
        knowledge: None,
        conversational: false,
        opening_message: None,
    };

    let cp = MockConfigProvider::new();
    cp.insert(&tc, "pii-agent", agent_config);

    let runtime = AgentRuntime::new(
        Arc::new(cp),
        Arc::new(MockAgentStateStore::new()),
        ext_runtime,
        Arc::new(EchoLlmBackend),
        Arc::new(MockTelemetry::new()),
        Arc::new(MockTokenMeter::new(0)),
        Arc::new(NoopToolLedger),
        None,
    )
    .with_guardrails(mandatory_policy, evaluator);

    let _ = wasm_src; // consumed via write_signed_extension_dir; kept in signature for clarity
    (runtime, tc)
}
