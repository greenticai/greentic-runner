use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use reqwest::blocking::Client;
use semver::Version;
use serde::Deserialize;

use crate::env::{IndexLocation, PackSource};

use super::{PackDigest, PackRef, PackVersion};

#[derive(Debug, Clone)]
pub struct Index {
    tenants: BTreeMap<String, TenantRecord>,
}

impl Index {
    pub fn tenants(&self) -> &BTreeMap<String, TenantRecord> {
        &self.tenants
    }

    pub fn load(location: &IndexLocation) -> Result<Self> {
        match location {
            IndexLocation::File(path) => {
                let file = File::open(path)
                    .with_context(|| format!("failed to open index {}", path.display()))?;
                let base_dir = path.parent().map(Path::to_path_buf);
                Self::from_reader_with_base(BufReader::new(file), base_dir.as_deref())
            }
            IndexLocation::Remote(url) => {
                let client = Client::builder().build()?;
                let response = client
                    .get(url.clone())
                    .send()
                    .with_context(|| format!("failed to fetch index {}", url))?
                    .error_for_status()
                    .with_context(|| format!("index download failed {}", url))?;
                let bytes = response
                    .bytes()
                    .context("failed to read index response body")?;
                Self::from_slice_with_base(&bytes, None)
            }
        }
    }

    pub fn from_reader<R: Read>(reader: R) -> Result<Self> {
        Self::from_reader_with_base(reader, None)
    }

    pub fn from_reader_with_base<R: Read>(reader: R, base_dir: Option<&Path>) -> Result<Self> {
        let raw: RawIndex = serde_json::from_reader(reader).context("index JSON is not valid")?;
        Ok(Self {
            tenants: raw.into_tenants(base_dir)?,
        })
    }

    pub fn from_slice(bytes: &[u8]) -> Result<Self> {
        Self::from_slice_with_base(bytes, None)
    }

    pub fn from_slice_with_base(bytes: &[u8], base_dir: Option<&Path>) -> Result<Self> {
        let raw: RawIndex = serde_json::from_slice(bytes).context("index JSON is not valid")?;
        Ok(Self {
            tenants: raw.into_tenants(base_dir)?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct TenantRecord {
    pub main_pack: PackEntry,
    pub overlays: Vec<PackEntry>,
}

#[derive(Debug, Clone)]
pub struct PackEntry {
    pub reference: PackRef,
    pub locator: PackLocator,
    pub content_digest: Option<PackDigest>,
    pub signature: Option<String>,
}

impl PackEntry {
    pub fn locator(&self) -> &PackLocator {
        &self.locator
    }
}

#[derive(Debug, Clone)]
pub struct PackLocator {
    raw: String,
}

impl PackLocator {
    pub fn new(raw: impl Into<String>) -> Self {
        Self { raw: raw.into() }
    }

    pub fn with_fallback(&self, source: PackSource) -> Result<String> {
        if self.raw.contains("://") {
            return Ok(self.raw.clone());
        }
        match source {
            PackSource::Fs => Ok(format!("{}://{}", source.scheme(), self.raw)),
            _ => bail!(
                "locator `{}` is missing a scheme; specify an explicit URI",
                self.raw
            ),
        }
    }
}

#[derive(Deserialize)]
struct RawIndex {
    #[serde(flatten)]
    tenants: BTreeMap<String, RawTenantRecord>,
}

impl RawIndex {
    fn into_tenants(self, base_dir: Option<&Path>) -> Result<BTreeMap<String, TenantRecord>> {
        let mut tenants = BTreeMap::new();
        for (tenant, raw) in self.tenants {
            tenants.insert(tenant, raw.into_tenant(base_dir)?);
        }
        Ok(tenants)
    }
}

#[derive(Deserialize)]
struct RawTenantRecord {
    main_pack: RawPackEntry,
    #[serde(default)]
    overlays: Vec<RawPackEntry>,
}

impl RawTenantRecord {
    fn into_tenant(self, base_dir: Option<&Path>) -> Result<TenantRecord> {
        Ok(TenantRecord {
            main_pack: self.main_pack.into_entry(base_dir)?,
            overlays: self
                .overlays
                .into_iter()
                .map(|entry| entry.into_entry(base_dir))
                .collect::<Result<Vec<_>>>()?,
        })
    }
}

impl TryFrom<RawTenantRecord> for TenantRecord {
    type Error = anyhow::Error;

    fn try_from(value: RawTenantRecord) -> Result<Self> {
        value.into_tenant(None)
    }
}

#[derive(Deserialize)]
struct RawPackEntry {
    name: String,
    #[serde(default)]
    version: Option<String>,
    #[serde(default)]
    digest: Option<String>,
    locator: String,
    #[serde(default)]
    signature: Option<String>,
}

impl RawPackEntry {
    fn into_entry(self, base_dir: Option<&Path>) -> Result<PackEntry> {
        validate_pack_name(&self.name)?;
        let (version, digest) = parse_version_and_digest(self.version, self.digest)?;
        let locator = resolve_locator(&self.locator, base_dir);
        Ok(PackEntry {
            reference: PackRef {
                name: self.name,
                version,
            },
            locator: PackLocator::new(locator),
            content_digest: digest,
            signature: self.signature,
        })
    }
}

impl TryFrom<RawPackEntry> for PackEntry {
    type Error = anyhow::Error;

    fn try_from(value: RawPackEntry) -> Result<Self> {
        value.into_entry(None)
    }
}

/// A pack name becomes a cache directory segment, so it must be a plain,
/// single path component.
fn validate_pack_name(name: &str) -> Result<()> {
    if name.is_empty() || name == "." || name == ".." || name.contains(['/', '\\', '\0']) {
        bail!(
            "invalid pack name `{name}`: must be non-empty, not `.`/`..`, and contain no path separators"
        );
    }
    Ok(())
}

fn parse_version_and_digest(
    version: Option<String>,
    digest: Option<String>,
) -> Result<(PackVersion, Option<PackDigest>)> {
    match (version, digest) {
        (Some(ver), digest) => {
            let version =
                Version::parse(&ver).with_context(|| format!("invalid semver version `{ver}`"))?;
            let digest = digest.map(PackDigest::parse).transpose()?;
            Ok((PackVersion::Semver(version), digest))
        }
        (None, Some(digest)) => {
            let parsed = PackDigest::parse(digest)?;
            Ok((PackVersion::Digest(parsed.clone()), Some(parsed)))
        }
        (None, None) => bail!("pack entry is missing version or digest pin"),
    }
}

fn resolve_locator(locator: &str, base_dir: Option<&Path>) -> String {
    if locator.contains("://") {
        return locator.to_string();
    }
    if let Some(base) = base_dir {
        let path = PathBuf::from(locator);
        if path.is_relative() {
            return base.join(path).to_string_lossy().into_owned();
        }
    }
    locator.to_string()
}

#[cfg(test)]
mod name_tests {
    use super::*;

    fn raw(name: &str) -> RawPackEntry {
        RawPackEntry {
            name: name.to_string(),
            version: Some("1.0.0".to_string()),
            digest: None,
            locator: "file:///tmp/x.gtpack".to_string(),
            signature: None,
        }
    }

    #[test]
    fn rejects_names_that_escape_the_cache() {
        for bad in ["", ".", "..", "a/b", "a\\b", "../x", "a\0b"] {
            assert!(
                raw(bad).into_entry(None).is_err(),
                "{bad:?} must be rejected"
            );
        }
    }

    #[test]
    fn accepts_ordinary_names() {
        for good in ["demo", "acme.pack-v2", "a_b"] {
            assert!(
                raw(good).into_entry(None).is_ok(),
                "{good:?} must be accepted"
            );
        }
    }
}
