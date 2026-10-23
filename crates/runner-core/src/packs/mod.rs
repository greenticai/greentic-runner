use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use greentic_pack::builder::PackManifest;
use greentic_pack::reader::{PackLoad, SigningPolicy, VerifyReport, open_pack};
use semver::Version;

use crate::env::PackConfig;

pub use cache::PackCache;
pub use index::{Index, PackEntry, TenantRecord};
pub use resolver::{FetchResponse, FsResolver, ResolverRegistry};
pub use verify::PackVerifier;

use verify::parse_verifiers;

mod cache;
mod index;
pub mod resolver;
mod verify;

/// Reference to a pack as defined in the index.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PackRef {
    pub name: String,
    pub version: PackVersion,
}

impl PackRef {
    pub fn cache_key(&self) -> String {
        format!(
            "{}-{}",
            sanitize_segment(&self.name),
            self.version.cache_label()
        )
    }
}

/// Version metadata for a pack.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PackVersion {
    Semver(Version),
    Digest(PackDigest),
}

impl PackVersion {
    pub fn cache_label(&self) -> Cow<'_, str> {
        match self {
            Self::Semver(v) => Cow::Owned(v.to_string()),
            Self::Digest(digest) => Cow::Owned(digest.cache_label()),
        }
    }

    pub fn as_digest(&self) -> Option<&PackDigest> {
        match self {
            Self::Digest(digest) => Some(digest),
            _ => None,
        }
    }
}

/// Digest (algorithm:value) to assert pack integrity.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PackDigest {
    raw: String,
    algorithm: String,
    value: String,
}

impl PackDigest {
    pub fn parse(raw: impl Into<String>) -> Result<Self> {
        let raw_string = raw.into();
        let (algorithm, value) = {
            let (algorithm_raw, value_raw) = raw_string
                .split_once(':')
                .ok_or_else(|| anyhow!("invalid digest `{raw_string}`; expected algo:value"))?;
            if algorithm_raw.is_empty() || value_raw.is_empty() {
                bail!("invalid digest format `{raw_string}`");
            }
            (algorithm_raw.to_ascii_lowercase(), value_raw.to_string())
        };
        Ok(Self {
            raw: raw_string,
            algorithm,
            value,
        })
    }

    pub fn sha256_from_bytes(bytes: &[u8]) -> Self {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        let digest = hasher.finalize();
        let digest_hex = to_hex(&digest);
        Self {
            raw: format!("sha256:{digest_hex}"),
            algorithm: "sha256".into(),
            value: digest_hex,
        }
    }

    pub fn algorithm(&self) -> &str {
        &self.algorithm
    }

    pub fn value(&self) -> &str {
        &self.value
    }

    pub fn as_str(&self) -> &str {
        &self.raw
    }

    pub fn raw_string(&self) -> String {
        self.raw.clone()
    }

    pub fn cache_label(&self) -> String {
        self.raw.replace(':', "_")
    }

    pub fn matches_file(&self, path: &Path) -> Result<bool> {
        let computed = compute_digest(path)?;
        Ok(computed.raw.eq_ignore_ascii_case(&self.raw))
    }
}

/// Cached and verified pack metadata.
#[derive(Debug, Clone)]
pub struct ResolvedPack {
    pub reference: PackRef,
    pub locator: String,
    pub path: PathBuf,
    pub manifest: PackManifest,
    pub digest: PackDigest,
    pub report: VerifyReport,
}

/// Per-tenant resolved packs.
#[derive(Debug, Clone)]
pub struct TenantPacks {
    pub main: ResolvedPack,
    pub overlays: Vec<ResolvedPack>,
}

#[derive(Debug, Clone)]
pub struct ResolvedSet {
    pub tenants: BTreeMap<String, TenantPacks>,
}

impl ResolvedSet {
    pub fn tenants(&self) -> &BTreeMap<String, TenantPacks> {
        &self.tenants
    }
}

/// Verification policy for [`PackManager`].
#[derive(Debug, Clone, Default)]
pub struct PackPolicy {
    /// Fail closed: a public key and a valid signature are required for every
    /// pack, and non-filesystem sources must pin a digest.
    pub verify_strict: bool,
    /// Extra roots (besides the working directory) that absolute `fs://`
    /// locators may point into. Confinement is enforced when this is `Some`
    /// or when `verify_strict` is set; otherwise absolute paths are
    /// unrestricted (dev behaviour).
    pub fs_allowed_roots: Option<Vec<PathBuf>>,
}

impl PackPolicy {
    /// Read `PACK_VERIFY_STRICT` and `PACK_FS_ALLOWED_ROOTS` (path list).
    pub fn from_env() -> Self {
        let verify_strict = std::env::var("PACK_VERIFY_STRICT")
            .map(|value| is_truthy(&value))
            .unwrap_or(false);
        let fs_allowed_roots = std::env::var_os("PACK_FS_ALLOWED_ROOTS")
            .map(|raw| std::env::split_paths(&raw).collect::<Vec<_>>());
        Self {
            verify_strict,
            fs_allowed_roots,
        }
    }

    pub fn strict(mut self, strict: bool) -> Self {
        self.verify_strict = strict;
        self
    }
}

fn is_truthy(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// Coordinates resolvers, cache, and verification.
pub struct PackManager {
    cfg: PackConfig,
    cache: PackCache,
    registry: ResolverRegistry,
    verifiers: Vec<PackVerifier>,
    policy: PackPolicy,
}

impl PackManager {
    /// Build a manager using the policy from the process environment.
    pub fn new(cfg: PackConfig) -> Result<Self> {
        Self::with_policy(cfg, PackPolicy::from_env())
    }

    pub fn with_policy(cfg: PackConfig, policy: PackPolicy) -> Result<Self> {
        let verifiers = match cfg.public_key.as_deref() {
            Some(value) => parse_verifiers(value)?,
            None => Vec::new(),
        };
        if policy.verify_strict && verifiers.is_empty() {
            bail!("strict pack verification is enabled but no public key is configured");
        }
        let mut registry = ResolverRegistry::default();
        let fs_root = std::env::current_dir()
            .context("failed to resolve current directory")?
            .canonicalize()
            .context("failed to canonicalize current directory")?;
        let fs_allowed_roots = match (&policy.fs_allowed_roots, policy.verify_strict) {
            (Some(roots), _) => Some(roots.clone()),
            (None, true) => Some(Vec::new()),
            (None, false) => None,
        };
        registry.register_builtin_confined(fs_root, cfg.network.as_ref(), fs_allowed_roots)?;
        Ok(Self {
            cache: PackCache::new(cfg.cache_dir.clone()),
            cfg,
            registry,
            verifiers,
            policy,
        })
    }

    /// Verify the entry signature against any configured key. The canonical
    /// identity message `{name, version, digest}` is tried first; the legacy
    /// digest-only message is still accepted for existing signatures.
    fn verify_signature(&self, entry: &PackEntry, digest: &PackDigest) -> Result<()> {
        if self.verifiers.is_empty() {
            return Ok(());
        }
        let signature = entry.signature.as_deref().ok_or_else(|| {
            anyhow!(
                "signature missing for pack {} but a public key is configured",
                entry.reference.name
            )
        })?;
        let canonical = canonical_signing_message(&entry.reference, digest);
        let mut last_err = None;
        for verifier in &self.verifiers {
            match verifier.verify(canonical.as_bytes(), signature) {
                Ok(()) => return Ok(()),
                Err(err) => last_err = Some(err),
            }
        }
        for verifier in &self.verifiers {
            if verifier
                .verify(digest.as_str().as_bytes(), signature)
                .is_ok()
            {
                // Legacy digest-only signature: still accepted so existing
                // signed indexes keep working.
                return Ok(());
            }
        }
        match last_err {
            Some(err) => Err(err.context(format!(
                "no configured public key verified the signature for pack {}",
                entry.reference.name
            ))),
            None => bail!("no public key configured"),
        }
    }

    /// Resolve all packs referenced in the provided index.
    pub fn resolve_all_for_index(&self, index: &Index) -> Result<ResolvedSet> {
        let mut tenants = BTreeMap::new();
        for (tenant, record) in index.tenants() {
            tenants.insert(tenant.clone(), self.resolve_tenant(record)?);
        }
        Ok(ResolvedSet { tenants })
    }

    /// Resolve every tenant independently. A failing tenant yields an `Err`
    /// entry instead of aborting the whole set, so callers can keep serving
    /// the previous runtime for it while applying the others.
    pub fn resolve_each_for_index(&self, index: &Index) -> BTreeMap<String, Result<TenantPacks>> {
        index
            .tenants()
            .iter()
            .map(|(tenant, record)| (tenant.clone(), self.resolve_tenant(record)))
            .collect()
    }

    /// Resolve the main pack and overlays for one tenant.
    pub fn resolve_tenant(&self, record: &TenantRecord) -> Result<TenantPacks> {
        let main = self.resolve_entry(&record.main_pack)?;
        let mut overlays = Vec::new();
        for overlay in &record.overlays {
            overlays.push(self.resolve_entry(overlay)?);
        }
        Ok(TenantPacks { main, overlays })
    }

    fn resolve_entry(&self, entry: &PackEntry) -> Result<ResolvedPack> {
        let locator = entry
            .locator
            .with_fallback(self.cfg.source)
            .context("pack locator missing scheme")?;
        let response = self
            .registry
            .fetch(&locator)
            .with_context(|| format!("resolver failed for {}", locator))?;

        let fetched_digest = compute_digest(response.path())?;
        let has_pinned_digest = entry
            .content_digest
            .as_ref()
            .or_else(|| entry.reference.version.as_digest())
            .is_some();
        if self.policy.verify_strict
            && !has_pinned_digest
            && !locator.to_ascii_lowercase().starts_with("fs://")
        {
            bail!(
                "strict verification requires a pinned digest for remote pack {}",
                entry.reference.name
            );
        }
        if entry
            .content_digest
            .as_ref()
            .or_else(|| entry.reference.version.as_digest())
            .map(|expected| {
                expected
                    .as_str()
                    .eq_ignore_ascii_case(fetched_digest.as_str())
            })
            == Some(false)
        {
            let expected = entry
                .content_digest
                .as_ref()
                .or_else(|| entry.reference.version.as_digest())
                .map(|value| value.as_str())
                .unwrap_or("<unknown>");
            bail!(
                "digest mismatch for {}: expected {}, found {}",
                entry.reference.name,
                expected,
                fetched_digest.as_str()
            );
        }

        self.verify_signature(entry, &fetched_digest)?;

        let cached = self.cache.store(entry, response.path(), &fetched_digest)?;
        let PackLoad {
            manifest, report, ..
        } = open_pack(&cached, SigningPolicy::DevOk).map_err(|err| {
            anyhow!(
                "failed to open cached pack {}: {}",
                cached.display(),
                err.message
            )
        })?;

        Ok(ResolvedPack {
            reference: entry.reference.clone(),
            locator,
            path: cached,
            manifest,
            digest: fetched_digest,
            report,
        })
    }
}

/// Canonical message signed by pack publishers: binds the pack identity to
/// its content digest so a valid signature cannot be replayed for another
/// name/version.
pub fn canonical_signing_message(reference: &PackRef, digest: &PackDigest) -> String {
    format!(
        "greentic-pack-sig-v1\nname={}\nversion={}\ndigest={}",
        reference.name,
        reference.version.cache_label(),
        digest.as_str()
    )
}

fn compute_digest(path: &Path) -> Result<PackDigest> {
    use sha2::{Digest, Sha256};
    const BUF_SIZE: usize = 64 * 1024;
    let file = File::open(path)
        .with_context(|| format!("failed to open {} for hashing", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut hasher = Sha256::new();
    let mut buf = [0u8; BUF_SIZE];
    loop {
        let read = reader.read(&mut buf)?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
    }
    let digest = hasher.finalize();
    let digest_hex = to_hex(&digest);
    PackDigest::parse(format!("sha256:{digest_hex}"))
}

fn sanitize_segment(value: &str) -> String {
    value
        .chars()
        .map(|ch| match ch {
            '/' | '\\' | ':' => '_',
            other => other,
        })
        .collect()
}

fn to_hex(digest: &[u8]) -> String {
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}
