use std::fs;
use std::io::ErrorKind;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::thread;

use anyhow::{Result, anyhow};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use ed25519_dalek::{Signer, SigningKey};
use greentic_pack::builder::{FlowBundle, PACK_VERSION, PackBuilder, PackMeta};
use runner_core::packs::{PackDigest, PackPolicy};
use runner_core::{Index, IndexLocation, PackConfig, PackManager, PackSource};
use semver::Version;
use serde_json::json;
use tiny_http::{Response, Server};

fn sample_meta() -> PackMeta {
    PackMeta {
        pack_version: PACK_VERSION,
        pack_id: "ai.greentic.runner.tests".into(),
        version: Version::parse("0.1.0").unwrap(),
        name: "Test Runner".into(),
        kind: None,
        description: None,
        authors: vec!["Greentic".into()],
        license: None,
        homepage: None,
        support: None,
        vendor: None,
        imports: vec![],
        entry_flows: vec!["qa".into()],
        created_at_utc: "2025-01-01T00:00:00Z".into(),
        events: None,
        repo: None,
        messaging: None,
        interfaces: Vec::new(),
        annotations: serde_json::Map::new(),
        distribution: None,
        components: Vec::new(),
    }
}

fn sample_flow() -> FlowBundle {
    let flow_json = json!({
        "id": "qa",
        "kind": "flow/v1",
        "entry": "start",
        "nodes": []
    });
    let hash = blake3::hash(&serde_json::to_vec(&flow_json).unwrap())
        .to_hex()
        .to_string();
    FlowBundle {
        id: "qa".into(),
        kind: "flow/v1".into(),
        entry: "start".into(),
        yaml: "id: qa\nentry: start\n".into(),
        json: flow_json,
        hash_blake3: hash,
        nodes: Vec::new(),
    }
}

fn build_test_pack(dir: &Path) -> Result<PathBuf> {
    let wasm_path = dir.join("component.wasm");
    fs::write(&wasm_path, b"\0asm\x01\0\0\0")?;
    let out_path = dir.join("sample.gtpack");
    PackBuilder::new(sample_meta())
        .with_flow(sample_flow())
        .with_component_wasm(
            "demo.component",
            Version::parse("1.0.0")?,
            wasm_path.as_path(),
        )
        .build(&out_path)?;
    Ok(out_path)
}

fn compute_digest(path: &Path) -> Result<PackDigest> {
    let bytes = fs::read(path)?;
    Ok(PackDigest::sha256_from_bytes(&bytes))
}

fn write_index(path: &Path, locator: &str, digest: &PackDigest) -> Result<()> {
    let index = json!({
        "demo": {
            "main_pack": {
                "name": "runner.demo",
                "version": "0.1.0",
                "locator": locator,
                "digest": digest.as_str(),
            },
            "overlays": []
        }
    });
    fs::write(path, serde_json::to_vec_pretty(&index)?)?;
    Ok(())
}

fn build_config(index: &Path, cache_dir: &Path, source: PackSource) -> PackConfig {
    PackConfig {
        source,
        index_location: IndexLocation::File(index.to_path_buf()),
        cache_dir: cache_dir.to_path_buf(),
        public_key: None,
        network: None,
    }
}

#[test]
fn resolves_fs_pack() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let pack_path = build_test_pack(temp.path())?;
    let digest = compute_digest(&pack_path)?;
    let index_path = temp.path().join("index.json");
    write_index(&index_path, pack_path.to_str().unwrap(), &digest)?;

    let config = build_config(&index_path, &temp.path().join("cache"), PackSource::Fs);
    let index = Index::load(&config.index_location)?;
    let manager = PackManager::new(config)?;
    let resolved = manager.resolve_all_for_index(&index)?;
    let tenant = resolved.tenants().get("demo").expect("tenant missing");
    assert_eq!(tenant.overlays.len(), 0);
    assert_eq!(
        tenant.main.manifest.meta.pack_id,
        "ai.greentic.runner.tests"
    );
    assert_eq!(tenant.main.digest.as_str(), digest.as_str());
    Ok(())
}

#[test]
fn resolves_http_pack() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let pack_path = build_test_pack(temp.path())?;
    let digest = compute_digest(&pack_path)?;
    let bytes = fs::read(&pack_path)?;

    let listener = match TcpListener::bind("127.0.0.1:0") {
        Ok(listener) => listener,
        Err(err) if err.kind() == ErrorKind::PermissionDenied => {
            eprintln!("skipping http resolver test: {err}");
            return Ok(());
        }
        Err(err) => return Err(err.into()),
    };
    let addr = listener.local_addr()?;
    let server =
        Server::from_listener(listener, None).map_err(|err| anyhow!("server error: {err}"))?;
    thread::spawn(move || {
        if let Ok(request) = server.recv() {
            let response = Response::from_data(bytes.clone());
            let _ = request.respond(response);
        }
    });

    let locator = format!("http://{}/pack.gtpack", addr);
    let index_path = temp.path().join("index.json");
    write_index(&index_path, &locator, &digest)?;
    let config = build_config(&index_path, &temp.path().join("cache"), PackSource::Http);
    let index = Index::load(&config.index_location)?;
    let manager = PackManager::new(config)?;
    let resolved = manager.resolve_all_for_index(&index)?;
    let tenant = resolved.tenants().get("demo").expect("tenant missing");
    assert_eq!(tenant.main.digest.as_str(), digest.as_str());
    Ok(())
}

fn write_signed_index(path: &Path, locator: &str, digest: &PackDigest, signature: Option<&str>) {
    let index = json!({
        "demo": {
            "main_pack": {
                "name": "runner.demo",
                "version": "0.1.0",
                "locator": locator,
                "digest": digest.as_str(),
                "signature": signature,
            },
            "overlays": []
        }
    });
    fs::write(path, serde_json::to_vec_pretty(&index).unwrap()).unwrap();
}

fn pub_value(key: &SigningKey) -> String {
    format!(
        "ed25519:{}",
        STANDARD.encode(key.verifying_key().as_bytes())
    )
}

#[test]
fn strict_without_public_key_fails_closed() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let index = temp.path().join("index.json");
    let config = build_config(&index, &temp.path().join("cache"), PackSource::Fs);
    let policy = PackPolicy::default().strict(true);
    assert!(PackManager::with_policy(config, policy).is_err());
    Ok(())
}

#[test]
fn strict_requires_signature_and_accepts_any_rotated_key() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let pack_path = build_test_pack(temp.path())?;
    let digest = compute_digest(&pack_path)?;
    let index_path = temp.path().join("index.json");
    let locator = pack_path.to_str().unwrap();
    let old_key = SigningKey::from_bytes(&[11u8; 32]);
    let new_key = SigningKey::from_bytes(&[12u8; 32]);

    let mut config = build_config(&index_path, &temp.path().join("cache"), PackSource::Fs);
    config.public_key = Some(format!("{},{}", pub_value(&old_key), pub_value(&new_key)));
    let policy = PackPolicy {
        verify_strict: true,
        fs_allowed_roots: Some(vec![temp.path().to_path_buf()]),
        allow_legacy_signatures: false,
    };
    let manager = PackManager::with_policy(config.clone(), policy)?;

    // Missing signature is rejected.
    write_signed_index(&index_path, locator, &digest, None);
    let index = Index::load(&config.index_location)?;
    assert!(manager.resolve_all_for_index(&index).is_err());

    // Canonical message signed by the second (rotated-in) key is accepted.
    let message = format!(
        "greentic-pack-sig-v1\nname=runner.demo\nversion=0.1.0\ndigest={}",
        digest.as_str()
    );
    let sig = STANDARD.encode(new_key.sign(message.as_bytes()).to_bytes());
    write_signed_index(&index_path, locator, &digest, Some(&sig));
    let index = Index::load(&config.index_location)?;
    manager.resolve_all_for_index(&index)?;

    // A signature from an unknown key is rejected.
    let rogue = SigningKey::from_bytes(&[99u8; 32]);
    let bad = STANDARD.encode(rogue.sign(message.as_bytes()).to_bytes());
    write_signed_index(&index_path, locator, &digest, Some(&bad));
    let index = Index::load(&config.index_location)?;
    assert!(manager.resolve_all_for_index(&index).is_err());
    Ok(())
}

#[test]
fn strict_confines_absolute_fs_locators() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let pack_path = build_test_pack(temp.path())?;
    let digest = compute_digest(&pack_path)?;
    let index_path = temp.path().join("index.json");
    let key = SigningKey::from_bytes(&[21u8; 32]);
    let message = format!(
        "greentic-pack-sig-v1\nname=runner.demo\nversion=0.1.0\ndigest={}",
        digest.as_str()
    );
    let sig = STANDARD.encode(key.sign(message.as_bytes()).to_bytes());
    write_signed_index(
        &index_path,
        pack_path.to_str().unwrap(),
        &digest,
        Some(&sig),
    );

    let mut config = build_config(&index_path, &temp.path().join("cache"), PackSource::Fs);
    config.public_key = Some(pub_value(&key));
    // No allowed roots configured: temp dir is outside cwd, so it is rejected.
    let manager = PackManager::with_policy(config, PackPolicy::default().strict(true))?;
    let index = Index::load(&IndexLocation::File(index_path))?;
    let err = manager.resolve_all_for_index(&index).expect_err("rejected");
    assert!(format!("{err:#}").contains("outside the allowed"));
    Ok(())
}

#[test]
fn strict_rejects_legacy_digest_only_signature_unless_allowed() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let pack_path = build_test_pack(temp.path())?;
    let digest = compute_digest(&pack_path)?;
    let index_path = temp.path().join("index.json");
    let locator = pack_path.to_str().unwrap();
    let key = SigningKey::from_bytes(&[31u8; 32]);
    let legacy = STANDARD.encode(key.sign(digest.as_str().as_bytes()).to_bytes());
    write_signed_index(&index_path, locator, &digest, Some(&legacy));

    let mut config = build_config(&index_path, &temp.path().join("cache"), PackSource::Fs);
    config.public_key = Some(pub_value(&key));
    let index = Index::load(&config.index_location)?;
    let roots = Some(vec![temp.path().to_path_buf()]);

    let strict = PackPolicy {
        verify_strict: true,
        fs_allowed_roots: roots.clone(),
        allow_legacy_signatures: false,
    };
    let manager = PackManager::with_policy(config.clone(), strict)?;
    assert!(manager.resolve_all_for_index(&index).is_err());

    let allowed = PackPolicy {
        verify_strict: true,
        fs_allowed_roots: roots.clone(),
        allow_legacy_signatures: true,
    };
    PackManager::with_policy(config.clone(), allowed)?.resolve_all_for_index(&index)?;

    let lax = PackPolicy {
        verify_strict: false,
        fs_allowed_roots: roots,
        allow_legacy_signatures: false,
    };
    PackManager::with_policy(config, lax)?.resolve_all_for_index(&index)?;
    Ok(())
}
