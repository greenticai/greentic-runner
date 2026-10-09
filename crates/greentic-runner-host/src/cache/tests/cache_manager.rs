use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tempfile::TempDir;

use crate::cache::engine_profile::{CpuPolicy, EngineProfile};
use crate::cache::keys::ArtifactKey;
use crate::cache::metadata::ArtifactMetadata;
use crate::cache::{CacheConfig, CacheManager};

fn fixture_bytes() -> Vec<u8> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/packs/secrets_store_smoke/components/echo_secret.wasm");
    std::fs::read(path).expect("fixture wasm")
}

fn build_key(engine: &wasmtime::Engine) -> ArtifactKey {
    let profile = EngineProfile::from_engine(engine, CpuPolicy::Native, "default".to_string());
    ArtifactKey::new(profile.id().to_string(), "sha256:test".to_string())
}

#[tokio::test]
async fn singleflight_compiles_once() {
    let temp = TempDir::new().expect("temp dir");
    let engine = Arc::new(wasmtime::Engine::default());
    let profile = EngineProfile::from_engine(&engine, CpuPolicy::Native, "default".to_string());
    let config = CacheConfig {
        root: temp.path().to_path_buf(),
        disk_enabled: false,
        memory_enabled: true,
        memory_max_bytes: 1024 * 1024,
        ..CacheConfig::default()
    };
    let cache = Arc::new(CacheManager::new(config, profile));
    let key = build_key(&engine);
    let bytes = fixture_bytes();
    let counter = Arc::new(AtomicU64::new(0));

    let mut tasks = Vec::new();
    for _ in 0..8 {
        let cache = Arc::clone(&cache);
        let key = key.clone();
        let bytes = bytes.clone();
        let counter = Arc::clone(&counter);
        let engine = Arc::clone(&engine);
        tasks.push(tokio::spawn(async move {
            let _ = cache
                .get_component(engine.as_ref(), &key, move || {
                    counter.fetch_add(1, Ordering::SeqCst);
                    Ok(bytes)
                })
                .await
                .expect("component");
        }));
    }

    for task in tasks {
        task.await.expect("task");
    }

    assert_eq!(counter.load(Ordering::SeqCst), 1);
    assert_eq!(cache.metrics().compiles, 1);
}

#[tokio::test]
async fn disk_hit_skips_compile() {
    let temp = TempDir::new().expect("temp dir");
    let engine = Arc::new(wasmtime::Engine::default());
    let profile = EngineProfile::from_engine(&engine, CpuPolicy::Native, "default".to_string());
    let config = CacheConfig {
        root: temp.path().to_path_buf(),
        disk_enabled: true,
        memory_enabled: false,
        ..CacheConfig::default()
    };
    let cache = CacheManager::new(config, profile);
    let key = build_key(&engine);
    let bytes = fixture_bytes();
    let counter = Arc::new(AtomicU64::new(0));

    let _ = cache
        .get_component(engine.as_ref(), &key, {
            let bytes = bytes.clone();
            let counter = Arc::clone(&counter);
            move || {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(bytes)
            }
        })
        .await
        .expect("component");

    let _ = cache
        .get_component(engine.as_ref(), &key, {
            let bytes = bytes.clone();
            let counter = Arc::clone(&counter);
            move || {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(bytes)
            }
        })
        .await
        .expect("component");

    assert_eq!(counter.load(Ordering::SeqCst), 1);
    assert!(cache.metrics().disk_hits >= 1);
}

#[tokio::test]
async fn memory_hit_skips_disk() {
    let temp = TempDir::new().expect("temp dir");
    let engine = Arc::new(wasmtime::Engine::default());
    let profile = EngineProfile::from_engine(&engine, CpuPolicy::Native, "default".to_string());
    let config = CacheConfig {
        root: temp.path().to_path_buf(),
        disk_enabled: true,
        memory_enabled: true,
        memory_max_bytes: 1024 * 1024,
        ..CacheConfig::default()
    };
    let cache = CacheManager::new(config, profile);
    let key = build_key(&engine);
    let bytes = fixture_bytes();

    let _ = cache
        .get_component(engine.as_ref(), &key, {
            let bytes = bytes.clone();
            move || Ok(bytes)
        })
        .await
        .expect("component");
    let before = cache.metrics();
    let _ = cache
        .get_component(engine.as_ref(), &key, {
            let bytes = bytes.clone();
            move || Ok(bytes)
        })
        .await
        .expect("component");
    let after = cache.metrics();

    assert!(after.memory_hits > before.memory_hits);
    assert_eq!(after.disk_reads, before.disk_reads);
}

#[tokio::test]
async fn warmup_persists_and_hits_disk() {
    let temp = TempDir::new().expect("temp dir");
    let engine = Arc::new(wasmtime::Engine::default());
    let profile =
        EngineProfile::from_engine(engine.as_ref(), CpuPolicy::Native, "default".to_string());
    let config = CacheConfig {
        root: temp.path().to_path_buf(),
        disk_enabled: true,
        memory_enabled: false,
        ..CacheConfig::default()
    };
    let cache = CacheManager::new(config.clone(), profile.clone());
    let key = ArtifactKey::new(profile.id().to_string(), "sha256:warmup".to_string());
    let bytes = fixture_bytes();

    let _ = cache
        .get_component(engine.as_ref(), &key, {
            let bytes = bytes.clone();
            move || Ok(bytes)
        })
        .await
        .expect("component");

    let disk_root = config.disk_root(profile.id());
    let artifact_path = disk_root.join("artifacts/sha256_warmup.cwasm");
    assert!(artifact_path.exists());

    let cache_again = CacheManager::new(config, profile);
    let before = cache_again.metrics();
    let _ = cache_again
        .get_component(engine.as_ref(), &key, {
            let bytes = bytes.clone();
            move || Ok(bytes)
        })
        .await
        .expect("component");
    let after = cache_again.metrics();
    assert!(after.disk_hits > before.disk_hits);
    assert_eq!(after.compiles, before.compiles);
}

#[tokio::test]
async fn doctor_warmup_and_prune_report_expected_defaults() {
    let temp = TempDir::new().expect("temp dir");
    let engine = wasmtime::Engine::default();
    let profile = EngineProfile::from_engine(&engine, CpuPolicy::Native, "default".to_string());
    let config = CacheConfig {
        root: temp.path().to_path_buf(),
        disk_enabled: false,
        memory_enabled: true,
        memory_max_bytes: 1024,
        ..CacheConfig::default()
    };
    let cache = CacheManager::new(config, profile.clone());
    let key = ArtifactKey::new(profile.id().to_string(), "sha256:doctor".to_string());

    assert_eq!(cache.engine_profile_id(), profile.id());
    assert_eq!(cache.disk_stats().expect("disk stats").artifact_count, 0);

    let warmup = cache
        .warmup(
            &engine,
            &[crate::cache::WarmupItem {
                key,
                bytes: fixture_bytes(),
            }],
            crate::cache::WarmupMode::BestEffort,
        )
        .await
        .expect("warmup");
    assert_eq!(warmup.warmed, 1);
    assert_eq!(warmup.skipped, 0);

    let doctor = cache.doctor();
    assert!(!doctor.disk_enabled);
    assert!(doctor.memory_enabled);
    assert_eq!(doctor.entries_checked, 0);

    let prune = cache.prune_disk(true).await.expect("prune");
    assert_eq!(prune.removed_entries, 0);
}

#[tokio::test]
async fn warmup_writes_cwasm_to_disk_and_skips_existing() {
    let temp = TempDir::new().expect("temp dir");
    let engine = wasmtime::Engine::default();
    let profile = EngineProfile::from_engine(&engine, CpuPolicy::Native, "default".to_string());
    let config = CacheConfig {
        root: temp.path().to_path_buf(),
        disk_enabled: true,
        memory_enabled: false,
        ..CacheConfig::default()
    };
    let cache = CacheManager::new(config.clone(), profile.clone());
    let key = ArtifactKey::new(
        profile.id().to_string(),
        "sha256:warmup-precompile".to_string(),
    );
    let bytes = fixture_bytes();

    let report = cache
        .warmup(
            &engine,
            &[crate::cache::WarmupItem {
                key: key.clone(),
                bytes: bytes.clone(),
            }],
            crate::cache::WarmupMode::Strict,
        )
        .await
        .expect("warmup");
    assert_eq!(report.warmed, 1);
    assert_eq!(report.skipped, 0);

    let artifact_path = config
        .disk_root(profile.id())
        .join("artifacts/sha256_warmup-precompile.cwasm");
    assert!(
        artifact_path.exists(),
        "expected cwasm at {artifact_path:?}"
    );

    let report2 = cache
        .warmup(
            &engine,
            &[crate::cache::WarmupItem { key, bytes }],
            crate::cache::WarmupMode::Strict,
        )
        .await
        .expect("second warmup");
    assert_eq!(report2.warmed, 0);
    assert_eq!(report2.skipped, 1);
}

#[tokio::test]
async fn corrupt_disk_entry_logs_and_recompiles() {
    let temp = TempDir::new().expect("temp dir");
    let engine = Arc::new(wasmtime::Engine::default());
    let profile = EngineProfile::from_engine(&engine, CpuPolicy::Native, "default".to_string());
    let config = CacheConfig {
        root: temp.path().to_path_buf(),
        disk_enabled: true,
        memory_enabled: false,
        ..CacheConfig::default()
    };
    let key = ArtifactKey::new(profile.id().to_string(), "sha256:corrupt".to_string());

    // Seed a disk entry whose metadata passes try_read validation but whose
    // artifact bytes are not a valid serialized component.
    let corrupt_bytes = vec![0xDE, 0xAD, 0xBE, 0xEF];
    let meta = ArtifactMetadata::new(
        &profile,
        key.wasm_digest.clone(),
        corrupt_bytes.len() as u64,
    );
    let disk_root = config.disk_root(profile.id());
    let artifacts_dir = disk_root.join("artifacts");
    std::fs::create_dir_all(&artifacts_dir).expect("create artifacts dir");
    std::fs::write(artifacts_dir.join("sha256_corrupt.cwasm"), &corrupt_bytes)
        .expect("write corrupt artifact");
    std::fs::write(
        artifacts_dir.join("sha256_corrupt.json"),
        serde_json::to_vec_pretty(&meta).expect("serialize meta"),
    )
    .expect("write meta");

    let cache = CacheManager::new(config.clone(), profile.clone());
    let bytes = fixture_bytes();

    // get_component should recover by recompiling from wasm_bytes.
    let _ = cache
        .get_component(engine.as_ref(), &key, {
            let bytes = bytes.clone();
            move || Ok(bytes)
        })
        .await
        .expect("component after corrupt entry");

    let m = cache.metrics();
    assert_eq!(
        m.deserialize_failures, 1,
        "expected one deserialize failure"
    );
    assert_eq!(m.compiles, 1, "expected one recompile");

    // The corrupt entry should have been replaced. A fresh CacheManager over the
    // same disk root should get a clean disk hit with no further failures.
    let cache2 = CacheManager::new(config, profile);
    let _ = cache2
        .get_component(engine.as_ref(), &key, {
            let bytes = bytes.clone();
            move || Ok(bytes)
        })
        .await
        .expect("component from healed entry");

    let m2 = cache2.metrics();
    assert_eq!(
        m2.deserialize_failures, 0,
        "expected no deserialize failures on healed entry"
    );
    assert_eq!(m2.disk_hits, 1, "expected a clean disk hit");
    assert_eq!(m2.compiles, 0, "expected no recompile on healed entry");
}
