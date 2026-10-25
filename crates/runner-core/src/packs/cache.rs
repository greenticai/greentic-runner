use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};

use super::{PackDigest, PackEntry, PackRef, PackVersion};

/// Newest sibling digest directories kept (besides the current one) after a
/// store, so a rollback or a runtime still reading its previous archive keeps
/// working.
const KEEP_RECENT_SIBLINGS: usize = 2;
/// A sibling is only removed once it has gone unused this long. Every cache hit
/// refreshes the archive's mtime, so a pack a tenant still resolves (the
/// watcher re-resolves each refresh interval) is never old enough to qualify.
const PRUNE_MIN_IDLE: Duration = Duration::from_secs(60 * 60);

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
            touch(&dest_path);
            return Ok(dest_path);
        }

        if same_path(source, &dest_path) {
            return Ok(dest_path);
        }

        copy_atomic(source, &dest_path, digest)?;
        self.prune_stale(&dest_dir, SystemTime::now());
        Ok(dest_path)
    }

    /// Reclaim space left by earlier publishes, after a new archive was stored
    /// in `current`:
    ///
    /// - sibling digest directories (`sha256_*`) beyond the newest
    ///   [`KEEP_RECENT_SIBLINGS`] that have been idle for [`PRUNE_MIN_IDLE`];
    /// - the legacy `<name>/<version>/pack.gtpack` file from before the cache
    ///   was keyed by digest, and `pack.tmp-*` leftovers of an interrupted
    ///   copy, once they are that old.
    ///
    /// A loaded runtime keeps its archive PATH and re-opens it lazily (assets,
    /// schemas), so nothing in use may be removed: use refreshes the mtime, the
    /// age gate keeps anything recent, and the newest siblings are kept for
    /// rollback. Best effort: a failed removal is logged, never an error.
    fn prune_stale(&self, current: &Path, now: SystemTime) {
        let Some(parent) = current.parent() else {
            return;
        };
        remove_stale_files(parent, now);
        let Ok(entries) = fs::read_dir(parent) else {
            return;
        };
        let mut siblings: Vec<(SystemTime, PathBuf)> = entries
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .filter(|path| {
                path != current
                    && path.is_dir()
                    && path
                        .file_name()
                        .and_then(|name| name.to_str())
                        .is_some_and(|name| name.starts_with("sha256_"))
            })
            .map(|path| (last_used(&path), path))
            .collect();
        // Newest first; keep the first N.
        siblings.sort_by_key(|sibling| std::cmp::Reverse(sibling.0));
        for (index, (used, dir)) in siblings.into_iter().enumerate() {
            if index >= KEEP_RECENT_SIBLINGS && is_idle(used, now) {
                if let Err(err) = fs::remove_dir_all(&dir) {
                    tracing::warn!(dir = %dir.display(), error = %err, "failed to prune stale pack cache dir");
                }
            } else {
                // Kept: still drop its interrupted-copy leftovers.
                remove_stale_files(&dir, now);
            }
        }
        // Leftover temp copies inside the current directory.
        remove_stale_files(current, now);
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
            Ok(Some(computed)) => {
                touch(&path);
                Ok(Some(CachedPack {
                    path,
                    digest: computed,
                }))
            }
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

/// Record "used just now" on an archive (best effort) so pruning never
/// mistakes a pack a tenant still resolves for an abandoned one.
fn touch(path: &Path) {
    let result = fs::OpenOptions::new()
        .write(true)
        .open(path)
        .and_then(|file| file.set_modified(SystemTime::now()));
    if let Err(err) = result {
        tracing::debug!(path = %path.display(), error = %err, "could not refresh cached pack mtime");
    }
}

fn mtime(path: &Path) -> SystemTime {
    fs::metadata(path)
        .and_then(|meta| meta.modified())
        .unwrap_or(UNIX_EPOCH)
}

/// Last use of a digest directory: its archive's mtime (refreshed on every
/// hit), falling back to the directory's.
fn last_used(dir: &Path) -> SystemTime {
    let archive = dir.join("pack.gtpack");
    if archive.is_file() {
        mtime(&archive)
    } else {
        mtime(dir)
    }
}

fn is_idle(used: SystemTime, now: SystemTime) -> bool {
    now.duration_since(used)
        .map(|age| age > PRUNE_MIN_IDLE)
        .unwrap_or(false)
}

/// Remove a legacy `pack.gtpack` file and `pack.tmp-*` leftovers directly
/// inside `dir` once they are idle. (In a digest directory `pack.gtpack` is the
/// live archive and is never touched here.)
fn remove_stale_files(dir: &Path, now: SystemTime) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    let in_digest_dir = dir
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.starts_with("sha256_"));
    for entry in entries.filter_map(|entry| entry.ok()) {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let stale_name = name.starts_with("pack.tmp-") || (!in_digest_dir && name == "pack.gtpack");
        if !stale_name || !path.is_file() || !is_idle(mtime(&path), now) {
            continue;
        }
        if let Err(err) = fs::remove_file(&path) {
            tracing::warn!(path = %path.display(), error = %err, "failed to remove stale pack cache file");
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

    fn age(path: &Path, secs: u64) {
        let when = SystemTime::now() - Duration::from_secs(secs);
        fs::OpenOptions::new()
            .write(true)
            .open(path)
            .and_then(|f| f.set_modified(when))
            .expect("set mtime");
    }

    fn store_bytes(cache: &PackCache, dir: &Path, bytes: &[u8]) -> PathBuf {
        let source = dir.join("src.gtpack");
        fs::write(&source, bytes).expect("write source");
        cache
            .store(&entry(), &source, &PackDigest::sha256_from_bytes(bytes))
            .expect("store")
    }

    #[test]
    fn lookup_returns_the_canonical_lowercase_digest_for_an_uppercase_pin() {
        let temp = tempfile::tempdir().expect("tmp");
        let cache = PackCache::new(temp.path().join("cache"));
        store_bytes(&cache, temp.path(), b"bytes");
        let canonical = PackDigest::sha256_from_bytes(b"bytes");
        let upper = PackDigest::parse(format!("SHA256:{}", canonical.value().to_ascii_uppercase()))
            .expect("parse");
        let hit = cache
            .lookup(&entry().reference, &upper)
            .expect("lookup")
            .expect("hit");
        assert_eq!(hit.digest.as_str(), canonical.as_str());
    }

    #[test]
    fn storing_a_republish_prunes_idle_siblings_beyond_the_newest_two() {
        let temp = tempfile::tempdir().expect("tmp");
        let cache = PackCache::new(temp.path().join("cache"));
        let old: Vec<PathBuf> = (0..4u8)
            .map(|i| store_bytes(&cache, temp.path(), &[i; 8]))
            .collect();
        // Age them: index 0 oldest ... 3 newest, all long idle.
        for (i, path) in old.iter().enumerate() {
            age(path, 10 * 3600 - (i as u64) * 600);
        }
        let current = store_bytes(&cache, temp.path(), b"current");
        assert!(current.is_file());
        assert!(!old[0].exists(), "oldest idle sibling must be pruned");
        assert!(!old[1].exists(), "next idle sibling beyond N=2 pruned");
        assert!(old[2].exists(), "newest two siblings are kept for rollback");
        assert!(old[3].exists());
    }

    #[test]
    fn a_recently_used_sibling_is_never_pruned() {
        let temp = tempfile::tempdir().expect("tmp");
        let cache = PackCache::new(temp.path().join("cache"));
        let first = store_bytes(&cache, temp.path(), b"tenant-a");
        for i in 0..3u8 {
            let p = store_bytes(&cache, temp.path(), &[i; 8]);
            age(&p, 5 * 3600);
        }
        // `first` is the oldest by creation but a tenant keeps resolving it:
        // the cache hit refreshes its mtime.
        age(&first, 6 * 3600);
        let hit = cache
            .lookup(
                &entry().reference,
                &PackDigest::sha256_from_bytes(b"tenant-a"),
            )
            .expect("lookup");
        assert!(hit.is_some());
        let _ = store_bytes(&cache, temp.path(), b"newest");
        assert!(first.exists(), "an in-use pack must survive pruning");
    }

    #[test]
    fn legacy_flat_archive_and_stale_tmp_files_are_swept_but_fresh_ones_stay() {
        let temp = tempfile::tempdir().expect("tmp");
        let cache = PackCache::new(temp.path().join("cache"));
        let current = store_bytes(&cache, temp.path(), b"current-bytes");
        let current_dir = current.parent().expect("dir").to_path_buf();
        let version_dir = current_dir.parent().expect("version dir").to_path_buf();
        let legacy = version_dir.join("pack.gtpack");
        let stale_tmp = current_dir.join("pack.tmp-123");
        let fresh_tmp = current_dir.join("pack.tmp-456");
        let parent_tmp = version_dir.join("pack.tmp-789");
        for path in [&legacy, &stale_tmp, &fresh_tmp, &parent_tmp] {
            fs::write(path, b"x").expect("write");
        }
        for path in [&legacy, &stale_tmp, &parent_tmp] {
            age(path, 3 * 3600);
        }
        // Trigger a sweep with another publish of the same version.
        let second = store_bytes(&cache, temp.path(), b"second-publish");
        assert!(!legacy.exists(), "legacy flat archive swept");
        assert!(!parent_tmp.exists(), "stale tmp in version dir swept");
        assert!(fresh_tmp.exists(), "a fresh tmp may be an in-flight copy");
        assert!(current.is_file(), "the live archive is never swept");
        // A kept sibling digest dir loses its stale interrupted-copy file too.
        assert!(second.is_file());
        assert!(!stale_tmp.exists(), "stale tmp in a kept sibling swept");
    }
}
