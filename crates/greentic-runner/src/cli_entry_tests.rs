//! Unit tests for the stock CLI wiring in [`super`]. The subprocess tests under
//! `tests/` exercise the same code through the real binary; these run in-process
//! so the argument handling, lock-file parsing and describe canonicalisation are
//! covered without spawning one.

use super::*;
use serde_json::json;
use tempfile::TempDir;

fn parse(args: &[&str]) -> Cli {
    let mut argv = vec!["greentic-runner"];
    argv.extend_from_slice(args);
    Cli::try_parse_from(argv).expect("arguments must parse")
}

fn write(dir: &Path, name: &str, contents: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, contents).expect("write fixture");
    path
}

fn describe_json() -> Value {
    json!({
        "operations": [
            {
                "id": "run",
                "input": { "schema": { "type": "object", "b": 1, "a": 2 } },
                "output": { "schema": { "type": "string" } }
            },
            { "name": "other", "input_schema": { "type": "null" } }
        ],
        "config_schema": { "z": true, "y": false }
    })
}

#[test]
fn cli_parses_run_flags_and_subcommands() {
    let cli = parse(&[
        "--bindings",
        "a.gtbind",
        "--bindings-dir",
        "dir",
        "--port",
        "9001",
        "--no-cache",
        "--json",
        "--trace",
        "always",
        "--trace-capture-inputs",
        "on",
        "--validation",
        "warn",
        "--locale",
        "id",
        "--allow-dev",
        "--config-explain",
    ]);
    assert!(cli.command.is_none());
    assert_eq!(cli.run.port, Some(9001));
    assert!(cli.run.no_cache && cli.run.json && cli.run.allow_dev && cli.run.config_explain);
    assert_eq!(cli.run.locale.as_deref(), Some("id"));
    assert!(matches!(cli.run.trace, Some(TraceArg::Always)));
    assert!(matches!(
        cli.run.trace_capture_inputs,
        Some(TraceCaptureArg::On)
    ));
    assert!(matches!(cli.run.validation, Some(ValidationArg::Warn)));

    assert!(matches!(
        parse(&["info", "--json"]).command,
        Some(Command::Info { json: true })
    ));
    assert!(matches!(
        parse(&["cache", "doctor"]).command,
        Some(Command::Cache(CacheCommand::Doctor))
    ));
    assert!(matches!(
        parse(&["cache", "prune", "--dry-run"]).command,
        Some(Command::Cache(CacheCommand::Prune(CachePruneArgs {
            dry_run: true
        })))
    ));
    assert!(matches!(
        parse(&["cache", "warmup", "--pack", "p", "--mode", "memory"]).command,
        Some(Command::Cache(CacheCommand::Warmup(CacheWarmupArgs {
            mode: CacheWarmupMode::Memory,
            ..
        })))
    ));
    assert!(matches!(
        parse(&["contract", "--contract", "d.cbor"]).command,
        Some(Command::Contract(ContractArgs {
            describe_path: Some(_),
            ..
        }))
    ));
}

#[tokio::test]
async fn info_runs_in_both_output_modes() {
    run_with_cli(parse(&["info"])).await.expect("human info");
    run_with_cli(parse(&["info", "--json"]))
        .await
        .expect("json info");
}

#[tokio::test]
async fn a_locale_flag_is_accepted_alongside_a_subcommand() {
    run_with_cli(parse(&["--locale", "en", "info"]))
        .await
        .expect("info with locale");
}

#[tokio::test]
async fn running_without_bindings_is_refused() {
    let err = run_with_cli(parse(&[])).await.expect_err("no bindings");
    assert!(err.to_string().contains("--bindings"), "got: {err}");
}

#[tokio::test]
async fn config_explain_prints_and_returns_before_loading_bindings() {
    run_with_cli(parse(&[
        "--bindings",
        "never-read.gtbind",
        "--config-explain",
    ]))
    .await
    .expect("config explain");
}

#[tokio::test]
async fn cache_doctor_and_dry_run_prune_succeed() {
    run_with_cli(parse(&["cache", "doctor"]))
        .await
        .expect("doctor");
    run_with_cli(parse(&["cache", "prune", "--dry-run"]))
        .await
        .expect("dry-run prune");
}

#[test]
fn json_errors_carry_the_cause_chain() {
    let err = anyhow::anyhow!("root cause").context("outer failure");
    // Prints to stdout; the assertion is that it does not panic on a chain.
    emit_json_error(&err);
    emit_json_error(&anyhow::anyhow!("single"));
}

#[test]
fn build_resolver_reads_toml_and_json_layers() {
    let dir = TempDir::new().expect("tmp");
    let toml_path = write(dir.path(), "greentic.toml", "");
    let json_path = write(dir.path(), "greentic.json", "{}");
    assert!(build_resolver(Some(&toml_path), false).is_ok());
    assert!(build_resolver(Some(&json_path), true).is_ok());
    assert!(build_resolver(None, false).is_ok());
}

#[test]
fn build_resolver_reports_unreadable_and_malformed_files() {
    let dir = TempDir::new().expect("tmp");
    assert!(build_resolver(Some(&dir.path().join("missing.toml")), false).is_err());
    let bad_json = write(dir.path(), "bad.json", "{ not json");
    assert!(build_resolver(Some(&bad_json), false).is_err());
    let bad_toml = write(dir.path(), "bad.toml", "= nope");
    assert!(build_resolver(Some(&bad_toml), false).is_err());
}

fn component(value: Value) -> PackLockComponent {
    serde_json::from_value(value).expect("component")
}

#[test]
fn lock_component_prefers_agreeing_names_and_rejects_conflicts() {
    let primary = component(json!({
        "name": "c", "source_ref": "oci://x/y:1", "bundled_path": "a.wasm", "wasm_sha256": "aa"
    }));
    assert_eq!(primary.source_ref().expect("ref"), "oci://x/y:1");
    assert_eq!(primary.bundled_path(), Some("a.wasm"));
    assert_eq!(primary.wasm_digest().as_deref(), Some("aa"));

    let legacy = component(json!({
        "name": "c", "ref": "oci://x/y:2", "path": "b.wasm", "sha256": "bb"
    }));
    assert_eq!(legacy.source_ref().expect("ref"), "oci://x/y:2");
    assert_eq!(legacy.bundled_path(), Some("b.wasm"));
    assert_eq!(legacy.wasm_digest().as_deref(), Some("bb"));

    let agreeing = component(json!({
        "name": "c", "source_ref": "r", "ref": "r", "bundled_path": "p", "path": "p",
        "wasm_sha256": "d", "sha256": "d"
    }));
    assert_eq!(agreeing.source_ref().expect("ref"), "r");
    assert_eq!(agreeing.bundled_path(), Some("p"));
    assert_eq!(agreeing.wasm_digest().as_deref(), Some("d"));

    let conflicting = component(json!({
        "name": "c", "source_ref": "r1", "ref": "r2", "bundled_path": "p1", "path": "p2",
        "wasm_sha256": "d1", "sha256": "d2"
    }));
    let err = conflicting.source_ref().expect_err("conflict");
    assert!(err.to_string().contains("conflicting refs"));
    assert_eq!(conflicting.bundled_path(), None);
    assert_eq!(conflicting.wasm_digest(), None);

    let bare = component(json!({ "name": "bare" }));
    assert!(bare.source_ref().is_err());
}

#[test]
fn digests_and_operations_are_normalised() {
    assert_eq!(normalize_digest("abc"), "sha256:abc");
    assert_eq!(normalize_digest("sha256:abc"), "sha256:abc");
    assert_eq!(normalize_digest("blake3:abc"), "blake3:abc");
    assert_eq!(normalize_operation_id("  "), "run");
    assert_eq!(normalize_operation_id(" go "), "go");
    assert_eq!(to_hex(&[0, 255, 16]), "00ff10");
    assert!(sha256_prefixed(b"x").starts_with("sha256:"));
}

#[test]
fn lock_path_helpers_recognise_pack_files() {
    let dir = TempDir::new().expect("tmp");
    assert!(pick_lock_path(dir.path()).is_none());
    write(dir.path(), "pack.lock.json", "{}");
    assert_eq!(
        pick_lock_path(dir.path()),
        Some(dir.path().join("pack.lock.json"))
    );
    write(dir.path(), "pack.lock", "{}");
    assert_eq!(
        pick_lock_path(dir.path()),
        Some(dir.path().join("pack.lock"))
    );
    assert!(is_pack_lock(Path::new("x/pack.lock")));
    assert!(is_pack_lock(Path::new("pack.lock.json")));
    assert!(!is_pack_lock(Path::new("pack.yaml")));
    assert!(is_pack_yaml(Path::new("a/pack.yaml")));
    assert!(!is_pack_yaml(Path::new("a/pack.lock")));
}

#[tokio::test]
async fn read_pack_lock_accepts_a_directory_lock_file_or_pack_yaml() {
    let dir = TempDir::new().expect("tmp");
    let lock = write(
        dir.path(),
        "pack.lock",
        r#"{"schema_version":1,"components":[{"name":"c","source_ref":"oci://a/b:1"}]}"#,
    );
    let yaml = write(dir.path(), "pack.yaml", "name: demo\n");

    for path in [dir.path().to_path_buf(), lock, yaml] {
        let (root, parsed) = read_pack_lock(&path).await.expect("lock");
        assert_eq!(root, dir.path());
        assert_eq!(parsed.components.len(), 1);
        assert_eq!(parsed.components[0].name, "c");
    }
}

#[tokio::test]
async fn read_pack_lock_refuses_bad_inputs() {
    let empty = TempDir::new().expect("tmp");
    let err = read_pack_lock(empty.path()).await.expect_err("no lock");
    assert!(err.to_string().contains("pack.lock not found"));

    let yaml_only = write(empty.path(), "pack.yaml", "name: demo\n");
    let err = read_pack_lock(&yaml_only).await.expect_err("yaml, no lock");
    assert!(err.to_string().contains("pack.lock not found"));

    let other = write(empty.path(), "notes.txt", "hi");
    let err = read_pack_lock(&other).await.expect_err("unsupported");
    assert!(err.to_string().contains("unsupported pack path"));

    let missing = empty.path().join("gone").join("pack.lock");
    assert!(read_pack_lock(&missing).await.is_err());

    let garbage = TempDir::new().expect("tmp");
    let path = write(garbage.path(), "pack.lock", "not json");
    let err = read_pack_lock(&path).await.expect_err("garbage");
    assert!(err.to_string().contains("failed to parse pack.lock"));

    let wrong = TempDir::new().expect("tmp");
    let path = write(
        wrong.path(),
        "pack.lock",
        r#"{"schema_version":2,"components":[]}"#,
    );
    let err = read_pack_lock(&path).await.expect_err("schema version");
    assert!(err.to_string().contains("schema_version must be 1"));
}

#[tokio::test]
async fn warmup_reports_a_lock_entry_without_a_digest() {
    let dir = TempDir::new().expect("tmp");
    let lock = write(
        dir.path(),
        "pack.lock",
        r#"{"schema_version":1,"components":[{"name":"nodigest","source_ref":"oci://a/b:1"}]}"#,
    );
    let err = warmup_cache(CacheWarmupArgs {
        pack: lock,
        mode: CacheWarmupMode::Disk,
    })
    .await
    .expect_err("missing digest");
    assert!(err.to_string().contains("missing digest"), "got: {err}");
}

#[tokio::test]
async fn warmup_reports_a_lock_entry_without_a_source() {
    let dir = TempDir::new().expect("tmp");
    let lock = write(
        dir.path(),
        "pack.lock",
        r#"{"schema_version":1,"components":[{"name":"nosrc","wasm_sha256":"aa"}]}"#,
    );
    let err = warmup_cache(CacheWarmupArgs {
        pack: lock,
        mode: CacheWarmupMode::Memory,
    })
    .await
    .expect_err("missing source");
    assert!(err.to_string().contains("missing source_ref"), "got: {err}");
}

#[tokio::test]
async fn bundled_component_bytes_are_read_from_the_pack_root() {
    let dir = TempDir::new().expect("tmp");
    std::fs::write(dir.path().join("c.wasm"), b"wasm-bytes").expect("write");
    let entry = component(json!({
        "name": "c", "source_ref": "oci://a/b:1", "bundled_path": "c.wasm"
    }));
    let client = DistClient::new(DistOptions::default());
    let bytes = resolve_component_bytes(dir.path(), &entry, "oci://a/b:1", &client)
        .await
        .expect("bundled bytes");
    assert_eq!(bytes, b"wasm-bytes");
}

#[tokio::test]
async fn non_oci_component_sources_are_refused() {
    let dir = TempDir::new().expect("tmp");
    let entry = component(json!({ "name": "c", "source_ref": "file:///tmp/x.wasm" }));
    let client = DistClient::new(DistOptions::default());
    let result = resolve_component_bytes(dir.path(), &entry, "file:///tmp/x.wasm", &client).await;
    assert!(result.is_err(), "a non-OCI ref must not resolve");
    let bogus = resolve_component_bytes(dir.path(), &entry, "%%not-a-ref%%", &client).await;
    assert!(bogus.is_err());
}

#[test]
fn describe_artifacts_load_from_json_cbor_and_empty_files() {
    let dir = TempDir::new().expect("tmp");
    let json_path = write(dir.path(), "d.json", &describe_json().to_string());
    let loaded = load_describe_artifact(&json_path).expect("json");
    assert_eq!(loaded.operations.len(), 2);

    let cbor_path = dir.path().join("d.cbor");
    std::fs::write(
        &cbor_path,
        serde_cbor::to_vec(&describe_json()).expect("cbor"),
    )
    .expect("write");
    let loaded = load_describe_artifact(&cbor_path).expect("cbor");
    assert_eq!(loaded.operations.len(), 2);

    let empty = write(dir.path(), "empty", "");
    let loaded = load_describe_artifact(&empty).expect("empty");
    assert!(loaded.operations.is_empty());
    assert!(loaded.config_schema.is_null());

    assert!(load_describe_artifact(&dir.path().join("missing")).is_err());
    let garbage = write(dir.path(), "garbage", "\u{1}\u{2} not a describe");
    assert!(load_describe_artifact(&garbage).is_err());
}

#[test]
fn canonical_describe_round_trips_and_creates_parent_directories() {
    let dir = TempDir::new().expect("tmp");
    let typed =
        parse_typed_describe_from_value(describe_json(), "test payload").expect("typed describe");
    let out = dir.path().join("nested/deeper/describe.cbor");
    write_canonical_describe(&out, &typed).expect("write canonical");
    let reloaded = load_describe_artifact(&out).expect("reload");
    assert_eq!(
        describe_hash(&typed).expect("hash"),
        describe_hash(&reloaded).expect("hash")
    );
    // A bare file name has an empty parent and must not try to create it.
    let cwd_relative = dir.path().join("flat.cbor");
    write_canonical_describe(&cwd_relative, &typed).expect("flat write");
}

#[test]
fn undecodable_describe_payloads_name_their_source() {
    let err = parse_typed_describe_from_value(json!({ "operations": "nope" }), "my source")
        .expect_err("operations must be a list");
    assert!(err.to_string().contains("my source"));
}

#[test]
fn canonicalize_json_sorts_keys_recursively() {
    let value = canonicalize_json(json!({ "b": [ { "z": 1, "a": 2 } ], "a": null }));
    assert_eq!(
        serde_json::to_string(&value).expect("string"),
        r#"{"a":null,"b":[{"a":2,"z":1}]}"#
    );
}

#[test]
fn operation_selection_prefers_requested_then_run_then_first() {
    let typed = parse_typed_describe_from_value(describe_json(), "t").expect("typed");
    assert_eq!(
        select_operation_from_describe(&typed, "run").as_deref(),
        Some("run")
    );
    // The first operation is `run`, which wins over an unknown request.
    assert_eq!(
        select_operation_from_describe(&typed, "unknown").as_deref(),
        Some("run")
    );
    let only_other =
        parse_typed_describe_from_value(json!({ "operations": [ { "name": "other" } ] }), "t")
            .expect("typed");
    assert_eq!(
        select_operation_from_describe(&only_other, "missing").as_deref(),
        Some("other")
    );
    let none = parse_typed_describe_from_value(json!({}), "t").expect("typed");
    assert!(select_operation_from_describe(&none, "run").is_none());
}

#[test]
fn contract_report_carries_schemas_and_stable_hashes() {
    let typed = parse_typed_describe_from_value(describe_json(), "t").expect("typed");
    let report = contract_from_describe("run", &typed, "unit".into()).expect("report");
    assert_eq!(report.selected_operation, "run");
    assert_eq!(report.source, "unit");
    assert_eq!(report.output_schema, json!({ "type": "string" }));
    assert_eq!(report.config_schema, json!({ "y": false, "z": true }));
    assert!(report.describe_hash.starts_with("sha256:"));
    assert!(report.schema_hash.starts_with("sha256:"));
    let again = contract_from_describe("run", &typed, "unit".into()).expect("report");
    assert_eq!(report.describe_hash, again.describe_hash);
    assert_eq!(report.schema_hash, again.schema_hash);

    // A requested operation that is not first is selected, and the legacy
    // `input_schema` field is honoured when `input.schema` is null.
    let other = contract_from_describe("other", &typed, "unit".into()).expect("report");
    assert_eq!(other.selected_operation, "other");
    assert_eq!(other.input_schema, json!({ "type": "null" }));
    let unknown = contract_from_describe("missing", &typed, "unit".into()).expect("report");
    assert_eq!(unknown.selected_operation, "run");

    let empty = parse_typed_describe_from_value(json!({}), "t").expect("typed");
    let err = contract_from_describe("run", &empty, "unit".into()).expect_err("no ops");
    assert!(err.to_string().contains("not found"));
}

#[test]
fn legacy_schema_fields_back_the_operation_schemas() {
    let typed = parse_typed_describe_from_value(
        json!({ "operations": [ {
            "id": "legacy",
            "input_schema": { "type": "integer" },
            "output_schema": { "type": "boolean" }
        } ] }),
        "t",
    )
    .expect("typed");
    let report = contract_from_describe("legacy", &typed, "unit".into()).expect("report");
    assert_eq!(report.input_schema, json!({ "type": "integer" }));
    assert_eq!(report.output_schema, json!({ "type": "boolean" }));
}

fn contract_args(describe: Option<PathBuf>) -> ContractArgs {
    ContractArgs {
        pack: None,
        component: None,
        operation: "run".into(),
        describe_path: describe,
        emit_describe: None,
        print_contract: false,
        no_verify: true,
        validate_only: true,
    }
}

#[tokio::test]
async fn contract_inspection_of_a_gated_artifact_succeeds() {
    let dir = TempDir::new().expect("tmp");
    let path = write(dir.path(), "d.json", &describe_json().to_string());
    run_contract(contract_args(Some(path)))
        .await
        .expect("gated artifact inspection");
}

#[tokio::test]
async fn contract_inspection_enforces_its_gates() {
    let dir = TempDir::new().expect("tmp");
    let path = write(dir.path(), "d.json", &describe_json().to_string());

    let mut ungated = contract_args(Some(path.clone()));
    ungated.no_verify = false;
    let err = run_contract(ungated).await.expect_err("needs both flags");
    assert!(err.to_string().contains("--no-verify and --validate-only"));

    let mut orphan = contract_args(Some(path));
    orphan.component = Some("c".into());
    let err = run_contract(orphan)
        .await
        .expect_err("component needs pack");
    assert!(err.to_string().contains("--pack is required"));

    let mut emit_only = contract_args(None);
    emit_only.emit_describe = Some(dir.path().join("out.cbor"));
    assert!(run_contract(emit_only).await.is_err());

    let err = run_contract(contract_args(None))
        .await
        .expect_err("nothing");
    assert!(err.to_string().contains("provide either --pack"));
}

#[tokio::test]
async fn contract_with_a_pack_but_no_component_is_refused() {
    let mut args = contract_args(None);
    args.pack = Some(PathBuf::from("does-not-matter.gtpack"));
    let err = run_contract(args).await.expect_err("component required");
    assert!(err.to_string().contains("--component is required"));
}

#[tokio::test]
async fn contract_with_an_unreadable_pack_fails_to_load() {
    let dir = TempDir::new().expect("tmp");
    let mut args = contract_args(None);
    args.pack = Some(dir.path().join("missing.gtpack"));
    args.component = Some("c".into());
    assert!(run_contract(args).await.is_err());
}

#[test]
fn operation_selection_finds_a_non_first_requested_operation() {
    let typed = parse_typed_describe_from_value(
        json!({ "operations": [ { "id": "first" }, { "id": "second" } ] }),
        "t",
    )
    .expect("typed");
    assert_eq!(
        select_operation_from_describe(&typed, "second").as_deref(),
        Some("second")
    );
    // `run` fallback is also found when it is not the first operation.
    let with_run = parse_typed_describe_from_value(
        json!({ "operations": [ { "id": "first" }, { "id": "run" } ] }),
        "t",
    )
    .expect("typed");
    assert_eq!(
        select_operation_from_describe(&with_run, "missing").as_deref(),
        Some("run")
    );
}

#[test]
fn port_flag_is_optional_so_env_is_not_always_overridden() {
    assert_eq!(parse(&["--bindings", "a.gtbind"]).run.port, None);
}

#[test]
fn trace_flags_only_override_env_when_given() {
    let base = TraceConfig {
        mode: TraceMode::Always,
        out_path: PathBuf::from("env-trace.json"),
        buffer_size: 8,
        capture_inputs: true,
    };
    let untouched = build_trace_config(base.clone(), None, None, None);
    assert_eq!(untouched.mode, TraceMode::Always);
    assert_eq!(untouched.out_path, PathBuf::from("env-trace.json"));
    assert!(untouched.capture_inputs);

    let overridden = build_trace_config(
        base,
        Some(TraceArg::Off),
        Some(PathBuf::from("cli-trace.json")),
        Some(TraceCaptureArg::Off),
    );
    assert_eq!(overridden.mode, TraceMode::Off);
    assert_eq!(overridden.out_path, PathBuf::from("cli-trace.json"));
    assert!(!overridden.capture_inputs);
}

#[test]
fn component_digest_is_verified_before_caching() {
    let bytes = b"component-bytes";
    let good = sha256_prefixed(bytes);
    assert!(verify_component_digest("c", &good, bytes).is_ok());
    let blake = format!("blake3:{}", blake3::hash(bytes).to_hex());
    assert!(verify_component_digest("c", &blake, bytes).is_ok());
    let err = verify_component_digest("c", &good, b"tampered").expect_err("mismatch");
    assert!(err.to_string().contains("digest mismatch"), "got: {err}");
    assert!(verify_component_digest("c", "md5:abc", bytes).is_err());
}
