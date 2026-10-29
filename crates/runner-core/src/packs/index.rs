use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use reqwest::blocking::Client;
use semver::Version;
use serde::Deserialize;

use crate::env::{IndexLocation, PackSource};

use super::resolver::{DEFAULT_MAX_INDEX_DOWNLOAD_MB, copy_capped, env_cap_bytes};
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
                let client = Client::builder()
                    .timeout(std::time::Duration::from_secs(60))
                    .build()?;
                let mut response = client
                    .get(url.clone())
                    .send()
                    .map_err(reqwest::Error::without_url)
                    .with_context(|| {
                        format!(
                            "failed to fetch index {}",
                            super::resolver::redact_locator(url.as_str())
                        )
                    })?
                    .error_for_status()
                    .map_err(reqwest::Error::without_url)
                    .with_context(|| {
                        format!(
                            "index download failed {}",
                            super::resolver::redact_locator(url.as_str())
                        )
                    })?;
                let cap =
                    env_cap_bytes("GREENTIC_PACK_INDEX_MAX_MB", DEFAULT_MAX_INDEX_DOWNLOAD_MB);
                if let (Some(cap), Some(len)) = (cap, response.content_length())
                    && len > cap
                {
                    bail!(
                        "index {} is {len} bytes, over the {cap} byte limit",
                        super::resolver::redact_locator(url.as_str())
                    );
                }
                let mut bytes = Vec::new();
                copy_capped(&mut response, &mut bytes, cap, "index")
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

/// A pack name is an identifier, not a path. Namespaced names (`acme/pack`)
/// are legitimate; the cache never uses the raw name as a path, it goes through
/// `sanitize_segment`, which maps separators away and keeps `..` from escaping.
/// So only names that are meaningless or unsafe to sanitize are rejected:
/// empty, `.`, `..` and NUL. One bad entry aborts the whole reload for every
/// tenant, which is why this stays as permissive as the cache allows.
fn validate_pack_name(name: &str) -> Result<()> {
    if name.is_empty() || name == "." || name == ".." || name.contains('\0') {
        bail!("invalid pack name `{name}`: must be non-empty, not `.`/`..`, and contain no NUL");
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
    fn rejects_empty_dot_dotdot_and_nul_names() {
        for bad in ["", ".", "..", "a\0b"] {
            assert!(
                raw(bad).into_entry(None).is_err(),
                "{bad:?} must be rejected"
            );
        }
    }

    #[test]
    fn accepts_ordinary_names() {
        for good in [
            "demo",
            "acme.pack-v2",
            "a_b",
            "a/b",
            "a\\b",
            "a/../b",
            "../x",
        ] {
            assert!(
                raw(good).into_entry(None).is_ok(),
                "{good:?} must be accepted"
            );
        }
    }
}

#[cfg(test)]
mod remote_error_tests {
    use super::*;
    use std::io::Write as _;

    /// A server that announces a body far over the default index cap and hangs up.
    fn oversized_index_server() -> u16 {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().expect("addr").port();
        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 1024];
                let _ = std::io::Read::read(&mut stream, &mut buf);
                let _ = stream.write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 999999999\r\nConnection: close\r\n\r\n",
                );
            }
        });
        port
    }

    #[test]
    fn oversized_index_error_does_not_leak_the_url_query() {
        let port = oversized_index_server();
        let url = url::Url::parse(&format!(
            "http://127.0.0.1:{port}/index.json?X-Amz-Signature=SECRET123"
        ))
        .expect("url");
        let err = Index::load(&IndexLocation::Remote(url)).expect_err("over the cap");
        let chain = format!("{err:#}");
        assert!(chain.contains("byte limit"), "{chain}");
        assert!(!chain.contains("SECRET123"), "{chain}");
    }
}
