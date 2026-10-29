use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};

use crate::cache::PruneReport;
use crate::cache::engine_profile::EngineProfile;
use crate::cache::keys::ArtifactKey;
use crate::cache::metadata::ArtifactMetadata;

#[derive(Clone, Debug)]
pub struct DiskCache {
    root: PathBuf,
    profile: EngineProfile,
    disk_max_bytes: Option<u64>,
}

impl DiskCache {
    pub fn new(root: PathBuf, profile: EngineProfile, disk_max_bytes: Option<u64>) -> Self {
        Self {
            root,
            profile,
            disk_max_bytes,
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn try_read(&self, key: &ArtifactKey) -> Result<Option<Vec<u8>>> {
        let paths = self.paths_for(key)?;
        if !paths.meta_path.exists() {
            // `write_entry` renames the artifact before its metadata, so an
            // artifact without metadata may be a write in flight. Only a
            // pair that has stayed incomplete past the grace period is junk.
            if paths.artifact_path.exists() && older_than_grace(&paths.artifact_path) {
                let _ = fs::remove_file(&paths.artifact_path);
            }
            return Ok(None);
        }
        let meta = match fs::read_to_string(&paths.meta_path) {
            Ok(raw) => match serde_json::from_str::<ArtifactMetadata>(&raw) {
                Ok(meta) => meta,
                Err(_) => {
                    self.delete_entry(&paths)?;
                    return Ok(None);
                }
            },
            Err(_) => {
                self.delete_entry(&paths)?;
                return Ok(None);
            }
        };
        if meta.validate_for_profile(&self.profile).is_err() {
            self.delete_entry(&paths)?;
            return Ok(None);
        }
        if meta.wasm_digest != key.wasm_digest {
            self.delete_entry(&paths)?;
            return Ok(None);
        }
        if !paths.artifact_path.exists() {
            if older_than_grace(&paths.meta_path) {
                self.delete_entry(&paths)?;
            }
            return Ok(None);
        }
        let artifact_bytes = fs::read(&paths.artifact_path).ok();
        let Some(artifact_bytes) = artifact_bytes else {
            self.delete_entry(&paths)?;
            return Ok(None);
        };
        if meta.artifact_bytes != artifact_bytes.len() as u64 {
            self.delete_entry(&paths)?;
            return Ok(None);
        }
        // Integrity: the artifact is later passed to `Component::deserialize`
        // (unsafe), so refuse anything whose hash does not match the metadata.
        let expected_hash = meta.artifact_sha256.as_deref();
        if expected_hash != Some(sha256_hex(&artifact_bytes).as_str()) {
            tracing::warn!(
                wasm_digest = %key.wasm_digest,
                "cached artifact failed integrity check; deleting entry"
            );
            self.delete_entry(&paths)?;
            return Ok(None);
        }
        self.update_access(&paths, meta).ok();
        Ok(Some(artifact_bytes))
    }

    pub fn write_atomic(
        &self,
        key: &ArtifactKey,
        bytes: &[u8],
        meta: &ArtifactMetadata,
    ) -> Result<()> {
        meta.validate_for_profile(&self.profile)
            .context("cache metadata does not match engine profile")?;
        if meta.wasm_digest != key.wasm_digest {
            bail!("cache metadata digest does not match artifact key");
        }
        let paths = self.paths_for(key)?;
        create_private_dir(&paths.artifacts_dir).with_context(|| {
            format!(
                "failed to create cache dir {}",
                paths.artifacts_dir.display()
            )
        })?;
        create_private_dir(&paths.tmp_dir).with_context(|| {
            format!("failed to create cache tmp dir {}", paths.tmp_dir.display())
        })?;
        let tmp_artifact = paths.tmp_path("artifact");
        let tmp_meta = paths.tmp_path("meta");
        let result = self.write_entry(&paths, &tmp_artifact, &tmp_meta, bytes, meta);
        if result.is_err() {
            // Never leave partial temp files behind after a failed write.
            let _ = fs::remove_file(&tmp_artifact);
            let _ = fs::remove_file(&tmp_meta);
        }
        result
    }

    fn write_entry(
        &self,
        paths: &DiskPaths,
        tmp_artifact: &Path,
        tmp_meta: &Path,
        bytes: &[u8],
        meta: &ArtifactMetadata,
    ) -> Result<()> {
        let meta = meta.clone().with_artifact_sha256(sha256_hex(bytes));
        write_private_file(tmp_artifact, bytes)
            .with_context(|| format!("failed to write {}", tmp_artifact.display()))?;
        let meta_json =
            serde_json::to_vec_pretty(&meta).context("failed to serialize cache metadata")?;
        write_private_file(tmp_meta, &meta_json)
            .with_context(|| format!("failed to write {}", tmp_meta.display()))?;
        fs::rename(tmp_artifact, &paths.artifact_path)
            .with_context(|| format!("failed to rename {}", paths.artifact_path.display()))?;
        fs::rename(tmp_meta, &paths.meta_path)
            .with_context(|| format!("failed to rename {}", paths.meta_path.display()))?;
        Ok(())
    }

    pub fn approx_size_bytes(&self) -> Result<u64> {
        let artifacts_dir = self.root.join("artifacts");
        if !artifacts_dir.exists() {
            return Ok(0);
        }
        let mut total = 0u64;
        for entry in fs::read_dir(&artifacts_dir)
            .with_context(|| format!("failed to read {}", artifacts_dir.display()))?
        {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("cwasm") {
                continue;
            }
            let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
            total = total.saturating_add(size);
        }
        Ok(total)
    }

    /// Remove files nothing can ever read again: an artifact without
    /// metadata (or the reverse), and leftover temp files from a crashed
    /// write. Anything younger than [`INCOMPLETE_ENTRY_GRACE`] is left alone
    /// because a writer may still be between its renames.
    fn sweep_orphans(&self, dry_run: bool) -> PruneReport {
        let mut report = PruneReport {
            removed_entries: 0,
            removed_bytes: 0,
        };
        let mut remove = |path: &Path| {
            if !older_than_grace(path) {
                return;
            }
            let size = fs::metadata(path).map(|m| m.len()).unwrap_or(0);
            if dry_run || fs::remove_file(path).is_ok() {
                report.removed_entries = report.removed_entries.saturating_add(1);
                report.removed_bytes = report.removed_bytes.saturating_add(size);
            }
        };
        if let Ok(entries) = fs::read_dir(self.root.join("artifacts")) {
            for entry in entries.flatten() {
                let path = entry.path();
                match path.extension().and_then(|ext| ext.to_str()) {
                    Some("cwasm") if !path.with_extension("json").exists() => remove(&path),
                    Some("json") if !path.with_extension("cwasm").exists() => remove(&path),
                    _ => {}
                }
            }
        }
        if let Ok(entries) = fs::read_dir(self.root.join("tmp")) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file() {
                    remove(&path);
                }
            }
        }
        report
    }

    pub fn prune_to_limit(&self, dry_run: bool) -> Result<PruneReport> {
        let mut report = self.sweep_orphans(dry_run);
        let Some(limit) = self.disk_max_bytes else {
            return Ok(report);
        };
        let artifacts_dir = self.root.join("artifacts");
        if !artifacts_dir.exists() {
            return Ok(report);
        }
        let mut entries = Vec::new();
        let mut total_bytes = 0u64;
        for entry in fs::read_dir(&artifacts_dir)
            .with_context(|| format!("failed to read {}", artifacts_dir.display()))?
        {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
                continue;
            }
            let raw = match fs::read_to_string(&path) {
                Ok(raw) => raw,
                Err(_) => continue,
            };
            let meta: ArtifactMetadata = match serde_json::from_str(&raw) {
                Ok(meta) => meta,
                Err(_) => continue,
            };
            let access = meta.last_access_time();
            let artifact_path = path.with_extension("cwasm");
            let size = fs::metadata(&artifact_path).map(|m| m.len()).unwrap_or(0);
            total_bytes = total_bytes.saturating_add(size);
            entries.push((access, meta, artifact_path, path, size));
        }
        entries.sort_by_key(|(access, _, _, _, _)| {
            access.map(|ts| ts.timestamp()).unwrap_or(i64::MIN)
        });
        let mut removed_entries = 0u64;
        let mut removed_bytes = 0u64;
        let mut remaining = total_bytes;
        for (_access, _meta, artifact_path, meta_path, size) in entries {
            if remaining <= limit {
                break;
            }
            if !dry_run {
                let _ = fs::remove_file(&artifact_path);
                let _ = fs::remove_file(&meta_path);
            }
            removed_entries = removed_entries.saturating_add(1);
            removed_bytes = removed_bytes.saturating_add(size);
            remaining = remaining.saturating_sub(size);
        }
        report.removed_entries = report.removed_entries.saturating_add(removed_entries);
        report.removed_bytes = report.removed_bytes.saturating_add(removed_bytes);
        Ok(report)
    }

    pub fn artifact_count(&self) -> Result<u64> {
        let artifacts_dir = self.root.join("artifacts");
        if !artifacts_dir.exists() {
            return Ok(0);
        }
        let mut count = 0u64;
        for entry in fs::read_dir(&artifacts_dir)
            .with_context(|| format!("failed to read {}", artifacts_dir.display()))?
        {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) == Some("cwasm") {
                count = count.saturating_add(1);
            }
        }
        Ok(count)
    }

    pub fn delete(&self, key: &ArtifactKey) -> Result<()> {
        let paths = self.paths_for(key)?;
        self.delete_entry(&paths)
    }

    fn update_access(&self, paths: &DiskPaths, mut meta: ArtifactMetadata) -> Result<()> {
        meta.touch();
        let tmp = paths.tmp_path("meta");
        let json = serde_json::to_vec_pretty(&meta)?;
        let _ = create_private_dir(&paths.tmp_dir);
        if write_private_file(&tmp, &json).is_ok() {
            if fs::rename(&tmp, &paths.meta_path).is_err() {
                let _ = fs::remove_file(&tmp);
            }
        } else {
            let _ = fs::remove_file(&tmp);
        }
        Ok(())
    }

    fn delete_entry(&self, paths: &DiskPaths) -> Result<()> {
        let _ = fs::remove_file(&paths.artifact_path);
        let _ = fs::remove_file(&paths.meta_path);
        Ok(())
    }

    fn paths_for(&self, key: &ArtifactKey) -> Result<DiskPaths> {
        if key.engine_profile_id != self.profile.engine_profile_id {
            bail!("artifact key engine_profile_id mismatch");
        }
        let artifacts_dir = self.root.join("artifacts");
        let tmp_dir = self.root.join("tmp");
        let name = digest_to_filename(&key.wasm_digest);
        let artifact_path = artifacts_dir.join(format!("{}.cwasm", name));
        let meta_path = artifacts_dir.join(format!("{}.json", name));
        Ok(DiskPaths {
            artifacts_dir,
            tmp_dir,
            artifact_path,
            meta_path,
        })
    }
}

struct DiskPaths {
    artifacts_dir: PathBuf,
    tmp_dir: PathBuf,
    artifact_path: PathBuf,
    meta_path: PathBuf,
}

impl DiskPaths {
    fn tmp_path(&self, suffix: &str) -> PathBuf {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let pid = std::process::id();
        self.tmp_dir.join(format!("tmp_{}_{}_{}", pid, now, suffix))
    }
}

/// How long a half-written cache entry (or a temp file) is left alone before
/// it counts as abandoned. Generous next to a write, which is two renames.
pub(crate) const INCOMPLETE_ENTRY_GRACE: std::time::Duration = std::time::Duration::from_secs(60);

/// True when `path` was last modified more than [`INCOMPLETE_ENTRY_GRACE`]
/// ago. A file whose age cannot be read is treated as young, so uncertainty
/// never deletes something a writer may own.
fn older_than_grace(path: &Path) -> bool {
    fs::metadata(path)
        .and_then(|meta| meta.modified())
        .ok()
        .and_then(|modified| SystemTime::now().duration_since(modified).ok())
        .is_some_and(|age| age > INCOMPLETE_ENTRY_GRACE)
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// Create `path` (and parents) restricted to the owner (0700) on unix.
fn create_private_dir(path: &Path) -> std::io::Result<()> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(path)
}

/// Write `bytes` to `path`, owner read/write only (0600) on unix.
fn write_private_file(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let mut options = fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(bytes)
}

fn digest_to_filename(digest: &str) -> String {
    digest.replace(':', "_")
}
