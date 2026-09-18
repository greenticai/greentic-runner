//! Test helpers shared across unit and integration test submodules.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, unsafe_code)]

use super::*;
use ed25519_dalek::{Signer, SigningKey};
use std::io::Write as _;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

pub(super) mod cert_verify;
pub(super) mod pull;
pub(super) mod unit;

// ---- shared helpers ----

/// Resolve a built `router_echo` wasm if present; else `None` (self-skip).
pub(super) fn fixture_wasm() -> Option<std::path::PathBuf> {
    let p = std::env::var("GREENTIC_MCP_ROUTER_ECHO_WASM")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| {
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../../greentic-mcp/target/wasm32-wasip2/release/router_echo.wasm")
        });
    p.exists().then_some(p)
}

/// A minimal but realistic unsigned describe document.
pub(super) fn sample_describe() -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "greentic.ai/v1",
        "kind": "ProviderExtension",
        "metadata": {
            "id": "router_echo",
            "version": "1.0.0",
            "summary": "echo router for tests"
        }
    })
}

/// Sign `describe` THE SAME WAY THE STORE DOES: JCS-canonicalize the
/// unsigned describe with `serde_jcs::to_vec` (RFC 8785), sign those bytes,
/// then inject the `signature {algorithm, publicKey, value}` object. Returns
/// the signed describe bytes (what the archive's `describe.json` carries).
pub(super) fn sign_describe_like_store(
    describe: &serde_json::Value,
    signing: &SigningKey,
) -> Vec<u8> {
    let message = serde_jcs::to_vec(describe).unwrap();
    let signature = signing.sign(&message);
    let signature_b64 = base64::engine::general_purpose::STANDARD.encode(signature.to_bytes());
    let public_b64 =
        base64::engine::general_purpose::STANDARD.encode(signing.verifying_key().to_bytes());
    let mut signed = describe.clone();
    signed.as_object_mut().unwrap().insert(
        "signature".to_string(),
        serde_json::json!({
            "algorithm": "ed25519",
            "publicKey": public_b64,
            "value": signature_b64,
        }),
    );
    serde_json::to_vec(&signed).unwrap()
}

/// Build a `.gtxpack` ZIP with the given `describe.json` + `extension.wasm`.
pub(super) fn build_gtxpack(describe_json: &[u8], wasm: &[u8]) -> Vec<u8> {
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
        let options: zip::write::FileOptions<()> =
            zip::write::FileOptions::default().compression_method(zip::CompressionMethod::Deflated);
        writer.start_file(DESCRIBE_ENTRY, options).unwrap();
        writer.write_all(describe_json).unwrap();
        writer.start_file(WASM_ENTRY, options).unwrap();
        writer.write_all(wasm).unwrap();
        writer.finish().unwrap();
    }
    buf
}

pub(super) fn pubkey_env_value(signing: &SigningKey) -> String {
    format!(
        "ed25519:{}",
        base64::engine::general_purpose::STANDARD.encode(signing.verifying_key().to_bytes())
    )
}

/// Stand up a mock store that serves `archive` at the artifact route.
pub(super) async fn mock_store(archive: Vec<u8>) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/extensions/router_echo/1.0.0/artifact"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/octet-stream")
                .set_body_bytes(archive),
        )
        .mount(&server)
        .await;
    server
}
