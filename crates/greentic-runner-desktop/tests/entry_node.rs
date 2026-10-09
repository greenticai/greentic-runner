//! `RunOptions::entry_node` — start a run at a named node.
//!
//! Card-driven messaging packs name the next node to run on the inbound
//! activity. Without this option a host can only restart the flow at its
//! entrypoint every turn, so the nodes chained between two cards never execute.
//!
//! The `runner-components` fixture flow is `qa` (component.exec) -> `emit`
//! (emit.response). That is enough to tell the two entries apart: only a run
//! that honours `entry_node: "emit"` skips `qa`.
//!
//! Note the fixture must be the built `.gtpack`, not the source directory —
//! running the directory verifies nothing and executes no flow nodes at all.

use greentic_runner_desktop::{RunOptions, RunResult, TenantContext, run_pack_with_options};
use serde_json::json;
use tempfile::TempDir;

fn fixture_pack() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/packs/runner-components/runner-components.gtpack")
}

fn executed_nodes(result: &RunResult) -> Vec<String> {
    result
        .node_summaries
        .iter()
        .map(|n| n.node_id.clone())
        .collect()
}

fn run(entry_node: Option<&str>) -> RunResult {
    run_pack_with_options(
        fixture_pack(),
        RunOptions {
            entry_flow: Some("demo.flow".to_string()),
            entry_node: entry_node.map(str::to_string),
            input: json!({}),
            ..RunOptions::default()
        },
    )
    .expect("run should not fail to start")
}

fn run_with_session(entry_node: Option<&str>, session_dir: &std::path::Path) -> RunResult {
    run_pack_with_options(
        fixture_pack(),
        RunOptions {
            entry_flow: Some("demo.flow".to_string()),
            entry_node: entry_node.map(str::to_string),
            input: json!({}),
            ctx: TenantContext {
                session_id: Some("sess-precedence".to_string()),
                ..TenantContext::default_local()
            },
            session_state_dir: Some(session_dir.to_path_buf()),
            ..RunOptions::default()
        },
    )
    .expect("run should not fail to start")
}

/// Plant a resume snapshot that would send the run to `emit`, skipping `qa`.
fn plant_snapshot(session_dir: &std::path::Path, pack_id: &str) {
    let snapshot = json!({
        "pack_id": pack_id,
        "flow_id": "demo.flow",
        "next_node": "emit",
        "state": {
            "entry": {}, "input": {}, "nodes": {}, "egress": [], "redirect_count": 0
        }
    });
    std::fs::write(
        session_dir.join("sess-precedence.snapshot.json"),
        serde_json::to_vec_pretty(&snapshot).expect("encode snapshot"),
    )
    .expect("write snapshot");
}

/// A parked snapshot must WIN over an explicit entry node.
///
/// A card parks awaiting the user's submit, and only re-dispatching THAT card
/// on resume attaches the submitted `answers` to its output — which is what the
/// nodes downstream read (`{{node.<card>.answers.<field>}}`). Jumping to an
/// entry node instead skips the card, so the answers never exist and every
/// downstream binding resolves empty. Measured on the meridian quote journey:
/// with the precedence this way round the page-3 fields reached the MCP call
/// (`contact_name: "Jane"`); with it backwards every argument was `""`.
///
/// `entry_node` therefore drives only the FIRST hop into a flow.
#[test]
fn a_parked_snapshot_outranks_an_entry_node() {
    let tmp = TempDir::new().expect("tempdir");
    let session_dir = tmp.path().join("sessions");
    std::fs::create_dir_all(&session_dir).expect("session dir");
    let pack_id = run(None).pack_id;

    // Premise: prove `entry_node` reaches `qa` at all when nothing is parked.
    // Without this the assertion below could pass simply because the entry node
    // never worked in this harness.
    let fresh = executed_nodes(&run_with_session(Some("qa"), &session_dir));
    assert!(
        fresh.iter().any(|n| n == "qa"),
        "entry_node did not reach `qa` even with no snapshot, so this test \
         cannot prove anything about precedence — got {fresh:?}"
    );

    // The real assertion: with a snapshot parked at `emit`, the same entry node
    // must lose. Only resume reaches `emit` without running `qa`.
    plant_snapshot(&session_dir, &pack_id);
    let resumed = executed_nodes(&run_with_session(Some("qa"), &session_dir));
    assert!(
        resumed.iter().any(|n| n == "emit"),
        "the parked snapshot must win and resume at `emit`, got {resumed:?}"
    );
    assert!(
        !resumed.iter().any(|n| n == "qa"),
        "resuming must not run the entry node `qa`, got {resumed:?}"
    );
}

/// Baseline that makes the `entry_node` assertion below discriminating: with no
/// `entry_node` the run starts at the flow's declared entrypoint, so `qa`
/// executes. If this ever stopped executing `qa`, the assertion that
/// `entry_node` SKIPS it would pass for the wrong reason.
#[test]
fn without_entry_node_the_run_starts_at_the_entrypoint() {
    let nodes = executed_nodes(&run(None));
    assert!(
        nodes.iter().any(|n| n == "qa"),
        "entrypoint run must execute the entry node `qa`, got {nodes:?}"
    );
}

#[test]
fn entry_node_starts_the_run_at_the_named_node() {
    let nodes = executed_nodes(&run(Some("emit")));
    assert!(
        nodes.iter().any(|n| n == "emit"),
        "run must execute the named entry node `emit`, got {nodes:?}"
    );
    assert!(
        !nodes.iter().any(|n| n == "qa"),
        "`qa` is upstream of `emit` and must NOT run, got {nodes:?}"
    );
}

/// The option must not silently degrade to an entrypoint restart when the node
/// does not exist — that silent fallback is the bug this option removes.
#[test]
fn unknown_entry_node_fails_rather_than_restarting_at_the_entrypoint() {
    let result = run_pack_with_options(
        fixture_pack(),
        RunOptions {
            entry_flow: Some("demo.flow".to_string()),
            entry_node: Some("no_such_node".to_string()),
            input: json!({}),
            ..RunOptions::default()
        },
    );

    match result {
        Err(err) => {
            let msg = format!("{err:#}");
            assert!(
                msg.contains("no_such_node"),
                "error should name the missing node, got {msg}"
            );
        }
        Ok(res) => {
            let nodes = executed_nodes(&res);
            assert!(
                !nodes.iter().any(|n| n == "qa"),
                "an unknown entry node must never fall back to the entrypoint, got {nodes:?}"
            );
            assert_ne!(
                res.status,
                greentic_runner_desktop::RunStatus::Success,
                "an unknown entry node must not report success, got {res:?}"
            );
        }
    }
}
