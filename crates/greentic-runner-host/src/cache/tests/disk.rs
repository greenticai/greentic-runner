use std::fs;
use std::path::{Path, PathBuf};

use chrono::{Duration, Utc};
use serde_json;
use tempfile::TempDir;

use crate::cache::disk::DiskCache;
use crate::cache::engine_profile::{CpuPolicy, EngineProfile};
use crate::cache::keys::ArtifactKey;
use crate::cache::metadata::ArtifactMetadata;

fn build_cache_root(temp: &TempDir, profile: &EngineProfile) -> PathBuf {
    temp.path().join("v1").join(profile.id())
}

fn artifact_paths(root: &Path, digest: &str) -> (PathBuf, PathBuf) {
    let name = digest.replace(':', "_");
    let artifacts_dir = root.join("artifacts");
    (
        artifacts_dir.join(format!("{name}.cwasm")),
        artifacts_dir.join(format!("{name}.json")),
    )
}

#[test]
fn disk_cache_write_and_read_roundtrip() {
    let temp = TempDir::new().expect("temp dir");
    let engine = wasmtime::Engine::default();
    let profile = EngineProfile::from_engine(&engine, CpuPolicy::Native, "default".to_string());
    let root = build_cache_root(&temp, &profile);
    let cache = DiskCache::new(root.clone(), profile.clone(), None);

    let digest = "sha256:abcd".to_string();
    let key = ArtifactKey::new(profile.id().to_string(), digest.clone());
    let bytes = b"hello".to_vec();
    let meta = ArtifactMetadata::new(&profile, digest.clone(), bytes.len() as u64);
    cache.write_atomic(&key, &bytes, &meta).expect("write");

    let loaded = cache.try_read(&key).expect("read");
    assert_eq!(loaded, Some(bytes));

    let (artifact_path, meta_path) = artifact_paths(&root, &digest);
    assert!(artifact_path.exists());
    assert!(meta_path.exists());
}

#[test]
fn disk_cache_artifact_digest_filename_is_path_safe() {
    let temp = TempDir::new().expect("temp dir");
    let engine = wasmtime::Engine::default();
    let profile = EngineProfile::from_engine(&engine, CpuPolicy::Native, "default".to_string());
    let root = build_cache_root(&temp, &profile);
    let cache = DiskCache::new(root.clone(), profile.clone(), None);

    let digest = "sha256:abc".to_string();
    let key = ArtifactKey::new(profile.id().to_string(), digest.clone());
    let bytes = b"hello".to_vec();
    let meta = ArtifactMetadata::new(&profile, digest.clone(), bytes.len() as u64);
    cache.write_atomic(&key, &bytes, &meta).expect("write");

    let (artifact_path, meta_path) = artifact_paths(&root, &digest);
    assert_eq!(
        artifact_path
            .file_name()
            .and_then(|name| name.to_str())
            .expect("artifact filename"),
        "sha256_abc.cwasm"
    );
    assert!(artifact_path.exists());
    assert!(meta_path.exists());
}

#[test]
fn disk_cache_metadata_mismatch_is_miss() {
    let temp = TempDir::new().expect("temp dir");
    let engine = wasmtime::Engine::default();
    let profile = EngineProfile::from_engine(&engine, CpuPolicy::Native, "default".to_string());
    let root = build_cache_root(&temp, &profile);
    let cache = DiskCache::new(root.clone(), profile.clone(), None);

    let digest = "sha256:abcd".to_string();
    let key = ArtifactKey::new(profile.id().to_string(), digest.clone());
    let bytes = b"hello".to_vec();
    let meta = ArtifactMetadata::new(&profile, digest.clone(), bytes.len() as u64);
    cache.write_atomic(&key, &bytes, &meta).expect("write");
    let (_artifact_path, meta_path) = artifact_paths(&root, &digest);
    let mut updated: ArtifactMetadata =
        serde_json::from_str(&fs::read_to_string(&meta_path).expect("read meta"))
            .expect("parse meta");
    updated.engine_profile_id = "sha256:other".to_string();
    fs::write(
        &meta_path,
        serde_json::to_string_pretty(&updated).expect("meta json"),
    )
    .expect("rewrite meta");

    let loaded = cache.try_read(&key).expect("read");
    assert!(loaded.is_none());
}

#[test]
fn corrupt_artifact_is_deleted_and_misses() {
    let temp = TempDir::new().expect("temp dir");
    let engine = wasmtime::Engine::default();
    let profile = EngineProfile::from_engine(&engine, CpuPolicy::Native, "default".to_string());
    let root = build_cache_root(&temp, &profile);
    let cache = DiskCache::new(root.clone(), profile.clone(), None);

    let digest = "sha256:abcd".to_string();
    let key = ArtifactKey::new(profile.id().to_string(), digest.clone());
    let bytes = b"hello".to_vec();
    let meta = ArtifactMetadata::new(&profile, digest.clone(), bytes.len() as u64 + 1);
    cache.write_atomic(&key, &bytes, &meta).expect("write");

    let loaded = cache.try_read(&key).expect("read");
    assert!(loaded.is_none());

    let (artifact_path, meta_path) = artifact_paths(&root, &digest);
    assert!(!artifact_path.exists());
    assert!(!meta_path.exists());
}

#[test]
fn prune_removes_oldest_entries() {
    let temp = TempDir::new().expect("temp dir");
    let engine = wasmtime::Engine::default();
    let profile = EngineProfile::from_engine(&engine, CpuPolicy::Native, "default".to_string());
    let root = build_cache_root(&temp, &profile);
    let cache = DiskCache::new(root.clone(), profile.clone(), Some(15));

    let now = Utc::now();
    let digests = ["sha256:one", "sha256:two", "sha256:three"];
    let sizes = [5usize, 6usize, 7usize];
    for ((digest, size), offset) in digests.iter().zip(sizes).zip([3, 2, 1]) {
        let key = ArtifactKey::new(profile.id().to_string(), (*digest).to_string());
        let bytes = vec![0u8; size];
        let mut meta = ArtifactMetadata::new(&profile, (*digest).to_string(), size as u64);
        meta.last_access_at = (now - Duration::seconds(offset)).to_rfc3339();
        cache.write_atomic(&key, &bytes, &meta).expect("write");
    }

    let report = cache.prune_to_limit(false).expect("prune");
    assert!(report.removed_entries >= 1);
    let remaining = cache.approx_size_bytes().expect("size");
    assert!(remaining <= 15);

    let _ = fs::metadata(root);
}

#[test]
fn tampered_artifact_fails_integrity_check_and_is_deleted() {
    let temp = TempDir::new().expect("temp dir");
    let engine = wasmtime::Engine::default();
    let profile = EngineProfile::from_engine(&engine, CpuPolicy::Native, "default".to_string());
    let root = build_cache_root(&temp, &profile);
    let cache = DiskCache::new(root.clone(), profile.clone(), None);

    let digest = "sha256:feed".to_string();
    let key = ArtifactKey::new(profile.id().to_string(), digest.clone());
    let bytes = b"hello".to_vec();
    let meta = ArtifactMetadata::new(&profile, digest.clone(), bytes.len() as u64);
    cache.write_atomic(&key, &bytes, &meta).expect("write");

    // Same length, different content: size check passes, hash check must not.
    let (artifact_path, meta_path) = artifact_paths(&root, &digest);
    std::fs::write(&artifact_path, b"HELLO").expect("tamper");
    assert_eq!(cache.try_read(&key).expect("read"), None);
    assert!(!artifact_path.exists());
    assert!(!meta_path.exists());
}

fn backdate(path: &Path) {
    let file = fs::OpenOptions::new()
        .write(true)
        .open(path)
        .expect("open for backdate");
    file.set_modified(std::time::SystemTime::now() - std::time::Duration::from_secs(3600))
        .expect("backdate");
}

#[test]
fn fresh_artifact_without_metadata_is_not_deleted_by_a_reader() {
    let temp = TempDir::new().expect("temp dir");
    let engine = wasmtime::Engine::default();
    let profile = EngineProfile::from_engine(&engine, CpuPolicy::Native, "default".to_string());
    let root = build_cache_root(&temp, &profile);
    let cache = DiskCache::new(root.clone(), profile.clone(), None);
    let digest = "sha256:cafe".to_string();
    let key = ArtifactKey::new(profile.id().to_string(), digest.clone());
    // A writer that has renamed the artifact but not yet the metadata.
    let (artifact_path, _meta_path) = artifact_paths(&root, &digest);
    fs::create_dir_all(artifact_path.parent().expect("parent")).expect("dir");
    fs::write(&artifact_path, b"in flight").expect("write");

    assert_eq!(cache.try_read(&key).expect("read"), None);
    assert!(artifact_path.exists(), "in-flight write must survive");

    backdate(&artifact_path);
    assert_eq!(cache.try_read(&key).expect("read"), None);
    assert!(!artifact_path.exists(), "abandoned artifact is removed");
}

#[test]
fn prune_sweeps_old_orphans_and_stale_temp_files_only() {
    let temp = TempDir::new().expect("temp dir");
    let engine = wasmtime::Engine::default();
    let profile = EngineProfile::from_engine(&engine, CpuPolicy::Native, "default".to_string());
    let root = build_cache_root(&temp, &profile);
    let cache = DiskCache::new(root.clone(), profile.clone(), None);
    let artifacts = root.join("artifacts");
    let tmp = root.join("tmp");
    fs::create_dir_all(&artifacts).expect("artifacts");
    fs::create_dir_all(&tmp).expect("tmp");
    let old_orphan = artifacts.join("sha256_old.cwasm");
    let young_orphan = artifacts.join("sha256_young.cwasm");
    let old_tmp = tmp.join("tmp_1_2_artifact");
    let young_tmp = tmp.join("tmp_3_4_meta");
    for path in [&old_orphan, &young_orphan, &old_tmp, &young_tmp] {
        fs::write(path, b"x").expect("write");
    }
    backdate(&old_orphan);
    backdate(&old_tmp);

    let report = cache.prune_to_limit(false).expect("prune");
    assert_eq!(report.removed_entries, 2);
    assert!(!old_orphan.exists());
    assert!(!old_tmp.exists());
    assert!(young_orphan.exists());
    assert!(young_tmp.exists());
}

#[cfg(unix)]
#[test]
fn cache_dirs_are_owner_only() {
    use std::os::unix::fs::PermissionsExt;
    let temp = TempDir::new().expect("temp dir");
    let engine = wasmtime::Engine::default();
    let profile = EngineProfile::from_engine(&engine, CpuPolicy::Native, "default".to_string());
    let root = temp.path().join("fresh-cache");
    let cache = DiskCache::new(root.clone(), profile.clone(), None);
    let digest = "sha256:beef".to_string();
    let key = ArtifactKey::new(profile.id().to_string(), digest.clone());
    let meta = ArtifactMetadata::new(&profile, digest, 1);
    cache.write_atomic(&key, b"x", &meta).expect("write");
    let mode = std::fs::metadata(root.join("artifacts"))
        .expect("stat")
        .permissions()
        .mode();
    assert_eq!(mode & 0o077, 0);
}
