use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};

use super::{PackDigest, PackEntry, PackRef, PackVersion};

/// A cache hit: the archive plus the digest computed from its bytes.
#[derive(Debug, Clone)]
pub struct CachedPack {
    pub path: PathBuf,
    /// Canonical lowercase digest of the file, not the caller's spelling.
    pub digest: PackDigest,
}

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
        let dest_dir = self.dir_for(&entry.reference, digest);
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

    /// The cached `pack.gtpack` for `reference` whose bytes hash to `digest`,
    /// if one exists. A file that no longer matches (truncated, edited) is
    /// treated as a miss so the caller re-fetches.
    pub fn lookup(&self, reference: &PackRef, digest: &PackDigest) -> Result<Option<CachedPack>> {
        let path = self.dir_for(reference, digest).join("pack.gtpack");
        if !path.is_file() {
            return Ok(None);
        }
        match digest.verified_file_digest(&path) {
            Ok(Some(computed)) => Ok(Some(CachedPack {
                path,
                digest: computed,
            })),
            Ok(None) => Ok(None),
            Err(err) => {
                tracing::warn!(path = %path.display(), error = %err, "cached pack unreadable; re-fetching");
                Ok(None)
            }
        }
    }

    /// Cache directory for one pack. The content digest is ALWAYS part of the
    /// key: two tenants may publish the same `name@semver` with different
    /// bytes, and a `name/version`-only key made them overwrite one another's
    /// `pack.gtpack`.
    fn dir_for(&self, reference: &PackRef, content: &PackDigest) -> PathBuf {
        match &reference.version {
            PackVersion::Semver(version) => self
                .root
                .join(super::sanitize_segment(&reference.name))
                .join(version.to_string())
                .join(content.cache_label().to_ascii_lowercase()),
            PackVersion::Digest(digest) => self
                .root
                .join(super::sanitize_segment(&reference.name))
                .join(digest.cache_label().to_ascii_lowercase()),
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

    fn ref_named(name: &str) -> PackRef {
        PackRef {
            name: name.into(),
            version: PackVersion::Semver(semver::Version::new(1, 0, 0)),
        }
    }

    fn test_digest() -> PackDigest {
        PackDigest::sha256_from_bytes(b"demo")
    }

    #[test]
    fn same_name_and_version_with_different_bytes_coexist() {
        let temp = tempfile::tempdir().expect("tmp");
        let cache = PackCache::new(temp.path().join("cache"));
        let entry = entry();
        let first = temp.path().join("a.gtpack");
        let second = temp.path().join("b.gtpack");
        fs::write(&first, b"tenant-a-bytes").expect("write");
        fs::write(&second, b"tenant-b-bytes").expect("write");
        let first_path = cache
            .store(
                &entry,
                &first,
                &PackDigest::sha256_from_bytes(b"tenant-a-bytes"),
            )
            .expect("store a");
        let second_path = cache
            .store(
                &entry,
                &second,
                &PackDigest::sha256_from_bytes(b"tenant-b-bytes"),
            )
            .expect("store b");
        assert_ne!(first_path, second_path);
        assert_eq!(fs::read(first_path).expect("read a"), b"tenant-a-bytes");
        assert_eq!(fs::read(second_path).expect("read b"), b"tenant-b-bytes");
    }

    #[test]
    fn namespaced_and_hostile_names_stay_inside_the_cache_root() {
        let temp = tempfile::tempdir().expect("tmp");
        let cache = PackCache::new(temp.path().join("cache"));
        for name in ["a/b", "..", "a/../b", "../x", "a\\b", "/etc/x"] {
            let dir = cache.dir_for(&ref_named(name), &test_digest());
            assert!(dir.starts_with(cache.root()), "{name:?} escaped: {dir:?}");
            let rel = dir.strip_prefix(cache.root()).expect("under root");
            assert!(
                rel.components()
                    .all(|c| matches!(c, std::path::Component::Normal(_))),
                "{name:?} has non-normal components: {rel:?}"
            );
        }
    }

    #[test]
    fn slash_and_underscore_names_do_not_share_a_cache_dir() {
        let temp = tempfile::tempdir().expect("tmp");
        let cache = PackCache::new(temp.path().join("cache"));
        assert_ne!(
            cache.dir_for(&ref_named("a/b"), &test_digest()),
            cache.dir_for(&ref_named("a_b"), &test_digest())
        );
    }

    #[test]
    fn lookup_returns_the_canonical_lowercase_digest_for_an_uppercase_pin() {
        let temp = tempfile::tempdir().expect("tmp");
        let cache = PackCache::new(temp.path().join("cache"));
        let source = temp.path().join("src.gtpack");
        fs::write(&source, b"bytes").expect("write");
        let canonical = PackDigest::sha256_from_bytes(b"bytes");
        cache.store(&entry(), &source, &canonical).expect("store");
        let upper = PackDigest::parse(format!("SHA256:{}", canonical.value().to_ascii_uppercase()))
            .expect("parse");
        let hit = cache
            .lookup(&entry().reference, &upper)
            .expect("lookup")
            .expect("hit");
        assert_eq!(hit.digest.as_str(), canonical.as_str());
    }

    #[test]
    fn store_replaces_stale_cache_atomically() {
        let temp = tempfile::tempdir().expect("tmp");
        let cache = PackCache::new(temp.path().join("cache"));
        let entry = entry();
        let source = temp.path().join("src.gtpack");
        fs::write(&source, b"new-bytes").expect("write");
        let digest = PackDigest::sha256_from_bytes(b"new-bytes");
        let dest_dir = cache.dir_for(&entry.reference, &digest);
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
        let dest_dir = cache.dir_for(&entry.reference, &digest);
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
