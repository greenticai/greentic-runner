use std::fs;
use std::path::{Path, PathBuf};

use greentic_runner::gen_bindings::{self, GeneratorOptions};
use greentic_runner_host::gtbind;
use greentic_types::cbor::encode_pack_manifest;
use greentic_types::{
    Flow, FlowId, FlowKind, PackFlowEntry, PackId, PackKind, PackManifest, PackSignatures,
};
use indexmap::IndexMap;
use semver::Version;
use serde_yaml_bw as serde_yaml;
use std::collections::BTreeMap;

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("tests/fixtures/gen-bindings")
        .join(name)
}

fn generate_serialized(path: &Path, opts: GeneratorOptions) -> String {
    let metadata = gen_bindings::load_pack(path).expect("load pack");
    let bindings = gen_bindings::generate_bindings(&metadata, opts).expect("generate bindings");
    serde_yaml::to_string(&bindings).expect("serialize")
}

fn write_manifest_cbor(dir: &Path) -> PathBuf {
    let flow_id: FlowId = "main".parse().expect("flow id");
    let flow = Flow {
        schema_version: "greentic.flow.v1".to_string(),
        id: flow_id.clone(),
        kind: FlowKind::Messaging,
        entrypoints: BTreeMap::new(),
        nodes: IndexMap::with_hasher(Default::default()),
        metadata: Default::default(),
    };
    let manifest = PackManifest {
        agents: Default::default(),
        schema_version: "greentic.pack-manifest.v1".to_string(),
        pack_id: "demo.pack".parse::<PackId>().expect("pack id"),
        name: None,
        version: Version::new(0, 1, 0),
        kind: PackKind::Application,
        publisher: "demo".to_string(),
        components: Vec::new(),
        flows: vec![PackFlowEntry {
            id: flow_id,
            kind: FlowKind::Messaging,
            flow,
            tags: Vec::new(),
            entrypoints: Vec::new(),
        }],
        dependencies: Vec::new(),
        capabilities: Vec::new(),
        secret_requirements: Vec::new(),
        signatures: PackSignatures::default(),
        bootstrap: None,
        extensions: None,
    };
    let bytes = encode_pack_manifest(&manifest).expect("encode manifest");
    let path = dir.join("manifest.cbor");
    fs::write(&path, bytes).expect("write manifest");
    path
}

#[test]
fn complete_binding_matches_golden() {
    let dir = fixture("weather-demo");
    let yaml = generate_serialized(
        &dir,
        GeneratorOptions {
            complete: true,
            pack_locator: Some("fs:///packs/weather-demo.gtpack".to_string()),
            ..Default::default()
        },
    );
    let golden =
        fs::read_to_string(dir.join("bindings.complete.yaml")).expect("read golden complete");
    assert_eq!(golden, yaml);
}

#[test]
fn strict_binding_matches_golden() {
    let dir = fixture("weather-demo-strict");
    let yaml = generate_serialized(
        &dir,
        GeneratorOptions {
            complete: true,
            strict: true,
            pack_locator: Some("fs:///packs/weather-demo-strict.gtpack".to_string()),
            ..Default::default()
        },
    );
    let golden = fs::read_to_string(dir.join("bindings.strict.yaml")).expect("read golden strict");
    assert_eq!(golden, yaml);
}

#[test]
fn load_pack_root_accepts_manifest_cbor() {
    let temp = tempfile::tempdir().expect("temp dir");
    write_manifest_cbor(temp.path());
    let metadata = gen_bindings::load_pack_root(temp.path()).expect("load pack root from manifest");
    assert_eq!(metadata.pack_id, "demo.pack");
    assert_eq!(metadata.flows.len(), 1);
}

#[test]
fn generated_bindings_are_gtbind_compatible() {
    let dir = fixture("weather-demo");
    let yaml = generate_serialized(
        &dir,
        GeneratorOptions {
            complete: true,
            pack_locator: Some("fs:///packs/weather-demo.gtpack".to_string()),
            ..Default::default()
        },
    );
    let temp = tempfile::tempdir().expect("temp dir");
    let path = temp.path().join("bindings.gtbind");
    fs::write(&path, yaml).expect("write bindings");

    let tenants = gtbind::load_gtbinds(&[path]).expect("load gtbind");
    let tenant = tenants
        .get("Weather Demo Pack")
        .expect("tenant entry present");
    assert_eq!(tenant.packs.len(), 1);
    assert_eq!(tenant.packs[0].pack_id, "demo.weather");
    assert_eq!(tenant.packs[0].pack_ref, "demo.weather@0.1.0");
    assert_eq!(
        tenant.packs[0]
            .pack_locator
            .as_deref()
            .expect("pack locator"),
        "fs:///packs/weather-demo.gtpack"
    );
}

#[test]
fn strict_capability_checks_use_component_features() {
    use greentic_runner::gen_bindings::component::ComponentFeatures;

    let mut metadata = gen_bindings::load_pack(&fixture("weather-demo")).expect("load pack");
    metadata.flows.clear();
    metadata.hints.secrets.required.clear();
    metadata.hints.network.allow.clear();
    let strict_with = |features: ComponentFeatures| {
        gen_bindings::generate_bindings(
            &metadata,
            GeneratorOptions {
                strict: true,
                component: Some(features),
                ..Default::default()
            },
        )
    };
    assert!(strict_with(ComponentFeatures::default()).is_ok());
    for features in [
        ComponentFeatures {
            secrets: true,
            ..Default::default()
        },
        ComponentFeatures {
            http: true,
            ..Default::default()
        },
    ] {
        let message = strict_with(features)
            .expect_err("capability without hints must fail strict")
            .to_string();
        assert!(message.contains("bindings.hints.yaml"), "{message}");
        assert!(!message.contains("--complete"), "{message}");
    }
}
