//! A desktop resume that fails AFTER one of its own effects must delete the
//! session snapshot (audit round 27, M2).
//!
//! Only the `Completed` outcome removes `<id>.snapshot.json`. A plain
//! `FlowEngine::resume` ended such a run in `Err`, the snapshot stayed at the old
//! wait and the next activity resumed it again, repeating the effect. The
//! desktop runner now resumes through `resume_surfacing_failures`:
//!
//! - wait -> `media.create` (the effect) -> a node that fails: the second run
//!   removes the snapshot;
//! - wait -> a node that fails with no earlier effect: the snapshot is kept so
//!   a transient failure stays retryable.

#![cfg(feature = "agentic-worker")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::fs::File;
use std::io::Write;
use std::path::Path;

use anyhow::{Context, Result};
use greentic_runner_desktop::{RunOptions, RunResult, TenantContext, run_pack_with_options};
use greentic_types::{
    ExtensionInline, ExtensionRef, PackFlowEntry, PackKind, PackManifest, encode_pack_manifest,
};
use semver::Version;
use serde_json::json;
use tempfile::TempDir;
use zip::write::FileOptions;

const RUNTIME_FLOW_EXTENSION_ID: &str = "greentic.pack.runtime_flow";
const PACK_ID: &str = "desktop.resume.post.effect";
const SESSION: &str = "sess-post-effect";

fn build_pack(pack_path: &Path) -> Result<()> {
    let flows = json!([
        {
            // wait -> effect (media.create) -> a component the pack does not have.
            "id": "post.flow",
            "flow_type": "messaging",
            "start": "ask",
            "nodes": {
                "ask": {
                    "component": "session.wait",
                    "input": { "reason": "awaiting the user" },
                    "routing": { "next": { "node_id": "make" } }
                },
                "make": {
                    "component": "media.create",
                    "input": { "format": "txt", "name": "note", "text": "hello" },
                    "routing": { "next": { "node_id": "boom" } }
                },
                "boom": {
                    "component": "missing.component",
                    "input": {},
                    "routing": "end"
                }
            }
        },
        {
            // wait -> failing node, no effect before it.
            "id": "pre.flow",
            "flow_type": "messaging",
            "start": "ask",
            "nodes": {
                "ask": {
                    "component": "session.wait",
                    "input": { "reason": "awaiting the user" },
                    "routing": { "next": { "node_id": "boom" } }
                },
                "boom": {
                    "component": "missing.component",
                    "input": {},
                    "routing": "end"
                }
            }
        }
    ]);
    let mut extensions = BTreeMap::new();
    extensions.insert(
        RUNTIME_FLOW_EXTENSION_ID.to_string(),
        ExtensionRef {
            kind: RUNTIME_FLOW_EXTENSION_ID.to_string(),
            version: "2.0.0".into(),
            digest: None,
            location: None,
            inline: Some(ExtensionInline::Other(json!({ "flows": flows }))),
        },
    );
    let manifest = PackManifest {
        schema_version: "1.0".into(),
        pack_id: PACK_ID.parse()?,
        name: None,
        version: Version::parse("0.0.0")?,
        kind: PackKind::Application,
        publisher: "test".into(),
        components: Vec::new(),
        flows: Vec::<PackFlowEntry>::new(),
        dependencies: Vec::new(),
        capabilities: Vec::new(),
        signatures: Default::default(),
        secret_requirements: Vec::new(),
        bootstrap: None,
        agents: BTreeMap::new(),
        extensions: Some(extensions),
    };
    let mut zip = zip::ZipWriter::new(File::create(pack_path).context("create pack archive")?);
    let options: FileOptions<'_, ()> =
        FileOptions::default().compression_method(zip::CompressionMethod::Stored);
    zip.start_file("manifest.cbor", options)?;
    zip.write_all(&encode_pack_manifest(&manifest)?)?;
    zip.finish().context("finalise pack archive")?;
    Ok(())
}

fn run(pack: &Path, flow: &str, session_dir: &Path) -> Result<RunResult> {
    run_pack_with_options(
        pack,
        RunOptions {
            entry_flow: Some(flow.to_string()),
            input: json!({ "text": "hi" }),
            ctx: TenantContext {
                session_id: Some(SESSION.to_string()),
                ..TenantContext::default_local()
            },
            session_state_dir: Some(session_dir.to_path_buf()),
            ..RunOptions::default()
        },
    )
}

fn snapshot_path(session_dir: &Path) -> std::path::PathBuf {
    session_dir.join(format!("{SESSION}.snapshot.json"))
}

fn fixture() -> Result<(TempDir, std::path::PathBuf, std::path::PathBuf)> {
    let temp = TempDir::new()?;
    let pack = temp.path().join("post-effect.gtpack");
    build_pack(&pack)?;
    let session_dir = temp.path().join("sessions");
    std::fs::create_dir_all(&session_dir)?;
    Ok((temp, pack, session_dir))
}

#[test]
fn a_resume_that_fails_after_an_effect_deletes_the_snapshot() -> Result<()> {
    let (_temp, pack, session_dir) = fixture()?;

    // Run 1 parks at the wait and persists the snapshot.
    run(&pack, "post.flow", &session_dir)?;
    assert!(
        snapshot_path(&session_dir).exists(),
        "the first run must park and persist a snapshot"
    );

    // Run 2 resumes: the effect runs, the next node fails. The park is spent.
    run(&pack, "post.flow", &session_dir)?;
    assert!(
        !snapshot_path(&session_dir).exists(),
        "a resume failing after an effect must not leave its snapshot behind"
    );
    Ok(())
}

#[test]
fn a_resume_that_fails_before_any_effect_keeps_the_snapshot() -> Result<()> {
    let (_temp, pack, session_dir) = fixture()?;

    run(&pack, "pre.flow", &session_dir)?;
    assert!(snapshot_path(&session_dir).exists(), "first run parks");

    // No effect ran before the failing node: the park survives for a retry.
    let second = run(&pack, "pre.flow", &session_dir)?;
    assert_ne!(
        second.status,
        greentic_runner_desktop::RunStatus::Success,
        "a pre-effect resume failure is not reported as success: {second:?}"
    );
    assert!(
        snapshot_path(&session_dir).exists(),
        "a pre-effect failure keeps the snapshot so the wait can be retried"
    );
    Ok(())
}
