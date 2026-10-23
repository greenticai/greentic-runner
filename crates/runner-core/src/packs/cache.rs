use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};

use super::{PackDigest, PackEntry, PackRef, PackVersion};
pub struct PackCache {
    root: PathBuf,
}

impl PackCache {
    pub fn new(root: PathBuf) -> Self {
        let root = root.canonicalize().unwrap_or(root);
        Self { root }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn store(&self, entry: &PackEntry, source: &Path, digest: &PackDigest) -> Result<PathBuf> {
        let dest_dir = self.dir_for(&entry.reference);
        fs::create_dir_all(&dest_dir)
            .with_context(|| format!("failed to create cache dir {}", dest_dir.display()))?;
        let dest_path = dest_dir.join("pack.gtpack");
        // Stale content is replaced atomically below. Never remove it first,
        // or a failed copy would leave no cached pack at all.
        if dest_path.exists() && digest.matches_file(&dest_path)? {
            return Ok(dest_path);
        }

        if same_path(source, &dest_path) {
            return Ok(dest_path);
        }

        copy_atomic(source, &dest_path, digest)?;
        Ok(dest_path)
    }

    fn dir_for(&self, reference: &PackRef) -> PathBuf {
        match &reference.version {
            PackVersion::Semver(version) => self
                .root
                .join(super::sanitize_segment(&reference.name))
                .join(version.to_string()),
            PackVersion::Digest(digest) => self
                .root
                .join(super::sanitize_segment(&reference.name))
                .join(digest.cache_label()),
        }
    }
}

fn copy_atomic(source: &Path, dest: &Path, digest: &PackDigest) -> Result<()> {
    let tmp = dest.with_extension(format!(
        "tmp-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|dur| dur.as_nanos())
            .unwrap_or(0)
    ));
    fs::copy(source, &tmp)
        .with_context(|| format!("failed to copy {} -> {}", source.display(), dest.display()))?;
    // Verify the copy (not just the source) so a source swapped between the
    // digest check and the copy is never cached.
    match digest.matches_file(&tmp) {
        Ok(true) => {}
        Ok(false) => {
            let _ = fs::remove_file(&tmp);
            anyhow::bail!(
                "digest mismatch after copying {} to cache (expected {})",
                source.display(),
                digest.as_str()
            );
        }
        Err(err) => {
            let _ = fs::remove_file(&tmp);
            return Err(err);
        }
    }
    if let Err(err) = fs::rename(&tmp, dest) {
        let _ = fs::remove_file(&tmp);
        return Err(err)
            .with_context(|| format!("failed to place cached pack at {}", dest.display()));
    }
    Ok(())
}

fn same_path(a: &Path, b: &Path) -> bool {
    if let (Ok(a), Ok(b)) = (a.canonicalize(), b.canonicalize()) {
        a == b
    } else {
        a == b
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry() -> PackEntry {
        PackEntry {
            reference: PackRef {
                name: "demo".into(),
                version: PackVersion::Semver(semver::Version::new(1, 0, 0)),
            },
            locator: crate::packs::index::PackLocator::new("fs://x"),
            content_digest: None,
            signature: None,
        }
    }

    #[test]
    fn store_replaces_stale_cache_atomically() {
        let temp = tempfile::tempdir().expect("tmp");
        let cache = PackCache::new(temp.path().join("cache"));
        let entry = entry();
        let source = temp.path().join("src.gtpack");
        fs::write(&source, b"new-bytes").expect("write");
        let digest = PackDigest::sha256_from_bytes(b"new-bytes");
        let dest_dir = cache.dir_for(&entry.reference);
        fs::create_dir_all(&dest_dir).expect("dir");
        fs::write(dest_dir.join("pack.gtpack"), b"stale").expect("stale");

        let stored = cache.store(&entry, &source, &digest).expect("store");
        assert_eq!(fs::read(stored).expect("read"), b"new-bytes");
    }

    #[test]
    fn store_rejects_source_that_does_not_match_digest_and_keeps_old_cache() {
        let temp = tempfile::tempdir().expect("tmp");
        let cache = PackCache::new(temp.path().join("cache"));
        let entry = entry();
        let source = temp.path().join("src.gtpack");
        fs::write(&source, b"tampered").expect("write");
        let digest = PackDigest::sha256_from_bytes(b"expected");
        let dest_dir = cache.dir_for(&entry.reference);
        fs::create_dir_all(&dest_dir).expect("dir");
        fs::write(dest_dir.join("pack.gtpack"), b"stale").expect("stale");

        let err = cache
            .store(&entry, &source, &digest)
            .expect_err("mismatch rejected");
        assert!(err.to_string().contains("digest mismatch"));
        assert_eq!(
            fs::read(dest_dir.join("pack.gtpack")).expect("old kept"),
            b"stale"
        );
        let leftovers = fs::read_dir(&dest_dir).expect("ls").count();
        assert_eq!(leftovers, 1, "temp file must be cleaned up");
    }
}
