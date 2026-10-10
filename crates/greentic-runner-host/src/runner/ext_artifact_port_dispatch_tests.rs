#![allow(clippy::unwrap_used, clippy::expect_used, unsafe_code)]

//! A real extension component calls `host.artifact.put` through a runtime built
//! by `build_ext_runtime`, the only `ExtensionRuntime` this crate builds. The
//! component (`tests/fixtures/artifact_probe/probe.wasm`, rebuilt from
//! `probe.core.wat`) is carried in a signed `.gtxpack` inside a pack, so the
//! real load gate runs. Its one tool stores 3 bytes and answers the id, or
//! `err:<artifact-error case>`.

use std::io::Write;
use std::sync::{Arc, Mutex};

use greentic_ext_runtime::host_ports::{
    ArtifactPort, ArtifactPortError, ArtifactPutRequest, HostCallContext,
};
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::host::ExtArtifactPort;
use crate::runner::agent_node::{EnvSecretsBackend, build_ext_runtime};

const PROBE: &[u8] = include_bytes!("../../tests/fixtures/artifact_probe/probe.wasm");
const EXT_ID: &str = "acme.artifact-probe";

/// `artifact-error` case numbers in the WIT order.
const UNSUPPORTED: &str = "err:0";
const TENANT_REQUIRED: &str = "err:1";
const UNAVAILABLE: &str = "err:6";

/// One put as the port received it: extension, tenant, name, media type, bytes.
type SeenPut = (String, Option<String>, String, String, Vec<u8>);

#[derive(Default)]
struct RecordingPort {
    seen: Mutex<Vec<SeenPut>>,
}

impl ArtifactPort for RecordingPort {
    fn put(
        &self,
        extension_id: &str,
        ctx: &HostCallContext,
        request: ArtifactPutRequest,
    ) -> Result<String, ArtifactPortError> {
        self.seen.lock().unwrap().push((
            extension_id.to_string(),
            ctx.tenant.clone(),
            request.name,
            request.mime_type,
            request.bytes,
        ));
        Ok("artifact://made".to_string())
    }
}

fn zip_bytes(entries: &[(&str, &[u8])]) -> Vec<u8> {
    let mut writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    let options: zip::write::FileOptions<'_, ()> =
        zip::write::FileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (name, bytes) in entries {
        writer.start_file(*name, options).unwrap();
        writer.write_all(bytes).unwrap();
    }
    writer.finish().unwrap().into_inner()
}

/// A signed `.gtxpack` carrying the probe component (bind → sign, as the SDK does).
fn signed_probe_archive() -> Vec<u8> {
    use greentic_extension_sdk_contract::{
        DescribeJson, artifact_sha256, bind_manifest, build_manifest, sign_describe,
    };
    let wasm_sha = artifact_sha256(PROBE);
    let mut describe: DescribeJson = serde_json::from_value(serde_json::json!({
        "apiVersion": "greentic.ai/v2",
        "kind": "DesignExtension",
        "compat": {
            "min_designer_version": ">=1.0.0",
            "min_runner_version": "^0.12.0",
            "contract_version": "1.2.0"
        },
        "metadata": {
            "id": EXT_ID, "name": EXT_ID, "version": "0.1.0",
            "summary": "artifact probe", "author": { "name": "test" }, "license": "MIT"
        },
        "engine": { "greenticDesigner": "*", "extRuntime": "*" },
        "capabilities": {
            "offered": [{ "id": "greentic:test/ping", "version": "1.0.0" }],
            "required": []
        },
        "runtime": {
            "memoryLimitMB": 64,
            "permissions": {},
            "components": {
                "probe": {
                    "gtpack": {
                        "file": "extension.wasm", "sha256": wasm_sha,
                        "pack_id": EXT_ID, "component_version": "0.1.0"
                    },
                    "sha256": wasm_sha,
                    "world": "test:artifact-probe/probe"
                }
            }
        },
        "contributions": {}
    }))
    .unwrap();
    let manifest = build_manifest(vec![("extension.wasm", PROBE)]);
    let manifest_json = serde_json::to_vec(&manifest).unwrap();
    bind_manifest(&mut describe, &manifest_json);
    let key = ed25519_dalek::SigningKey::from_bytes(&[11u8; 32]);
    sign_describe(&mut describe, &key).unwrap();
    let describe_json = serde_json::to_vec_pretty(&describe).unwrap();
    zip_bytes(&[
        ("describe.json", describe_json.as_slice()),
        ("manifest.json", manifest_json.as_slice()),
        ("extension.wasm", PROBE),
    ])
}

/// Everything a runtime needs, kept alive for the test.
struct Fixture {
    runtime: Arc<greentic_ext_runtime::ExtensionRuntime>,
    _dirs: Vec<tempfile::TempDir>,
}

/// Build the runtime the way the agent wiring does: `build_ext_runtime` with
/// the probe carried in a pack. The trust root and the on-disk extension dir
/// are temp dirs (env, hence `#[serial]` on every caller).
fn runtime_with(port: Option<ExtArtifactPort>) -> Fixture {
    let home = tempfile::tempdir().unwrap();
    let extensions = tempfile::tempdir().unwrap();
    unsafe {
        std::env::set_var("GREENTIC_HOME", home.path());
        std::env::set_var("GREENTIC_EXTENSIONS_DIR", extensions.path());
    }
    let holder = tempfile::tempdir().unwrap();
    let pack_path = holder.path().join("worker.gtpack");
    let mut writer = zip::ZipWriter::new(std::fs::File::create(&pack_path).unwrap());
    let options: zip::write::FileOptions<'_, ()> =
        zip::write::FileOptions::default().compression_method(zip::CompressionMethod::Stored);
    writer
        .start_file(format!("extensions/{EXT_ID}.gtxpack"), options)
        .unwrap();
    writer.write_all(&signed_probe_archive()).unwrap();
    writer.finish().unwrap();
    let pack = Arc::new(crate::pack::tests::pack_runtime_for_dir(&pack_path));

    let runtime = build_ext_runtime(Arc::new(EnvSecretsBackend), None, None, port, &[pack])
        .expect("runtime builds");
    unsafe {
        std::env::remove_var("GREENTIC_HOME");
        std::env::remove_var("GREENTIC_EXTENSIONS_DIR");
    }
    assert!(
        runtime.loaded().keys().any(|id| id.as_str() == EXT_ID),
        "the probe must pass the load gate"
    );
    Fixture {
        runtime,
        _dirs: vec![home, extensions, holder],
    }
}

fn call(fixture: &Fixture, tenant: Option<&str>) -> String {
    let ctx = HostCallContext {
        tenant: tenant.map(str::to_string),
        user_email: None,
    };
    fixture
        .runtime
        .invoke_tool_ctx(EXT_ID, "make", "{}", &ctx)
        .expect("the tool call itself succeeds")
}

#[test]
#[serial_test::serial]
fn an_extensions_put_reaches_the_installed_port_with_the_calls_tenant() {
    let port = Arc::new(RecordingPort::default());
    let fixture = runtime_with(Some(port.clone()));
    assert_eq!(call(&fixture, Some("acme")), "artifact://made");
    assert_eq!(call(&fixture, Some("beta")), "artifact://made");
    let seen = port.seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0].0, EXT_ID);
    assert_eq!(
        seen[0].1.as_deref(),
        Some("acme"),
        "tenant passed unchanged"
    );
    assert_eq!(
        seen[1].1.as_deref(),
        Some("beta"),
        "per call, never a global"
    );
    assert_eq!(
        (seen[0].2.as_str(), seen[0].3.as_str(), seen[0].4.as_slice()),
        ("a.png", "image/png", [1u8, 2, 3].as_slice())
    );
}

#[test]
#[serial_test::serial]
fn a_call_without_a_tenant_is_refused_before_the_port() {
    let port = Arc::new(RecordingPort::default());
    let fixture = runtime_with(Some(port.clone()));
    assert_eq!(call(&fixture, None), TENANT_REQUIRED);
    assert_eq!(call(&fixture, Some("  ")), TENANT_REQUIRED);
    assert!(port.seen.lock().unwrap().is_empty());
}

#[test]
#[serial_test::serial]
fn without_a_port_the_extension_is_told_unsupported() {
    let fixture = runtime_with(None);
    assert_eq!(call(&fixture, Some("acme")), UNSUPPORTED);
}

/// The real door-backed port, end to end: a door failure reaches the guest as
/// `unavailable`, and nothing the guest sees carries the token.
#[test]
#[serial_test::serial]
fn a_door_failure_reaches_the_guest_as_unavailable_without_the_token() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let server = rt.block_on(async {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(503).set_body_string("gtm_topsecret"))
            .mount(&server)
            .await;
        server
    });
    let port = super::HttpArtifactPort::new(
        format!("{}/artifacts", server.uri()),
        "gtm_topsecret".into(),
        rt.handle().clone(),
    )
    .unwrap();
    let fixture = runtime_with(Some(Arc::new(port)));
    // Called from a worker thread of the multi-thread runtime, as the agent
    // loop's tool dispatch is.
    let answer = rt.block_on(async {
        let runtime = fixture.runtime.clone();
        tokio::task::spawn(async move {
            runtime
                .invoke_tool_ctx(
                    EXT_ID,
                    "make",
                    "{}",
                    &HostCallContext {
                        tenant: Some("acme".into()),
                        user_email: None,
                    },
                )
                .unwrap()
        })
        .await
        .unwrap()
    });
    assert_eq!(answer, UNAVAILABLE);
    assert!(!answer.contains("gtm_topsecret"));
}
