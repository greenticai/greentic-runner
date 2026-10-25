#![recursion_limit = "512"]
use greentic_runner_desktop::{RunOptions, run_pack_with_options, run_pack_with_options_async};
use serde_json::json;

#[tokio::test]
async fn async_runner_matches_sync_runner() {
    let pack_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/packs/runner-components/runner-components.gtpack");

    let opts = RunOptions {
        entry_flow: Some("demo.flow".to_string()),
        input: json!({}),
        ..RunOptions::default()
    };

    let sync_pack_path = pack_path.clone();
    let sync_opts = opts.clone();
    let sync = tokio::task::spawn_blocking(move || {
        run_pack_with_options(&sync_pack_path, sync_opts).expect("sync run failed")
    })
    .await
    .expect("sync join failed");
    let async_res = run_pack_with_options_async(&pack_path, opts)
        .await
        .expect("async run failed");

    // Guard against both runs "agreeing" on an empty pack: the pack must have
    // really loaded and executed the requested flow.
    assert!(!sync.pack_id.is_empty(), "pack id must be resolved");
    assert_eq!(sync.flow_id, "demo.flow");
    assert!(
        !sync.node_summaries.is_empty(),
        "the flow must have executed at least one node"
    );
    assert_eq!(sync.pack_id, async_res.pack_id);
    assert_eq!(sync.flow_id, async_res.flow_id);
    assert_eq!(sync.status, async_res.status);
}
