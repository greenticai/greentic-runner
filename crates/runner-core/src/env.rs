use greentic_config_types::PathsConfig;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::{Context, Result, anyhow, bail};
use url::Url;

/// Environment-driven configuration for pack management.
#[derive(Debug, Clone)]
pub struct PackConfig {
    pub source: PackSource,
    pub index_location: IndexLocation,
    pub cache_dir: PathBuf,
    pub public_key: Option<String>,
    pub network: Option<greentic_config_types::NetworkConfig>,
}

impl PackConfig {
    /// Build a [`PackConfig`] using greentic-config paths and sensible defaults.
    pub fn default_for_paths(paths: &PathsConfig) -> Result<Self> {
        let cache_dir = paths.cache_dir.join("packs");
        let default_index = paths.greentic_root.join("index.json");
        let index_location = if default_index.exists() {
            IndexLocation::File(default_index)
        } else if Path::new("examples/index.json").exists() {
            IndexLocation::File(PathBuf::from("examples/index.json"))
        } else {
            IndexLocation::File(default_index)
        };
        Ok(Self {
            source: PackSource::Fs,
            index_location,
            cache_dir,
            public_key: None,
            network: None,
        })
    }

    /// Build from the structured packs section of greentic-config.
    pub fn from_packs(cfg: &greentic_config_types::PacksConfig) -> Result<Self> {
        let index_location = match &cfg.source {
            greentic_config_types::PackSourceConfig::LocalIndex { path } => {
                IndexLocation::File(path.clone())
            }
            greentic_config_types::PackSourceConfig::HttpIndex { url } => {
                IndexLocation::from_value(url)?
            }
            greentic_config_types::PackSourceConfig::OciRegistry { reference } => {
                bail!(
                    "OCI registry pack sources are not supported (reference `{reference}`); \
                     use a local or HTTP(S) index"
                )
            }
        };
        let public_key = join_public_keys(cfg.trust.as_ref().map(|t| t.public_keys.as_slice()));
        Ok(Self {
            source: PackSource::Fs,
            index_location,
            cache_dir: cfg.cache_dir.clone(),
            public_key,
            network: None,
        })
    }
}

/// Join configured trust keys into one multi-key value (comma separated) so
/// every key is tried during verification (key rotation).
pub fn join_public_keys(keys: Option<&[String]>) -> Option<String> {
    let keys = keys?;
    let joined = keys
        .iter()
        .map(|key| key.trim())
        .filter(|key| !key.is_empty())
        .collect::<Vec<_>>()
        .join(",");
    if joined.is_empty() {
        None
    } else {
        Some(joined)
    }
}

/// Location of the pack index document (supports file paths and HTTP/S URLs).
#[derive(Debug, Clone)]
pub enum IndexLocation {
    File(PathBuf),
    Remote(Url),
}

impl IndexLocation {
    pub fn from_value(value: &str) -> Result<Self> {
        if value.starts_with("http://") || value.starts_with("https://") {
            let url = Url::parse(value).context("PACK_INDEX_URL is not a valid URL")?;
            return Ok(Self::Remote(url));
        }
        if value.starts_with("file://") {
            let url = Url::parse(value).context("PACK_INDEX_URL is not a valid file:// URL")?;
            let path = url
                .to_file_path()
                .map_err(|_| anyhow!("PACK_INDEX_URL points to an invalid file URI"))?;
            return Ok(Self::File(path));
        }
        Ok(Self::File(PathBuf::from(value)))
    }

    pub fn display(&self) -> String {
        match self {
            Self::File(path) => path.display().to_string(),
            Self::Remote(url) => url.to_string(),
        }
    }
}

/// Supported default sources for packs when the index omits the URI scheme.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PackSource {
    Fs,
    Http,
    Oci,
    S3,
    Gcs,
    AzBlob,
}

impl PackSource {
    pub fn scheme(self) -> &'static str {
        match self {
            Self::Fs => "fs",
            Self::Http => "http",
            Self::Oci => "oci",
            Self::S3 => "s3",
            Self::Gcs => "gcs",
            Self::AzBlob => "azblob",
        }
    }
}

impl FromStr for PackSource {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value.to_ascii_lowercase().as_str() {
            "fs" => Ok(Self::Fs),
            "http" | "https" => Ok(Self::Http),
            "oci" => Ok(Self::Oci),
            "s3" => Ok(Self::S3),
            "gcs" => Ok(Self::Gcs),
            "azblob" | "azure" | "azureblob" => Ok(Self::AzBlob),
            other => bail!("unsupported PACK_SOURCE `{other}`"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use greentic_config_types::{PackSourceConfig, PackTrustConfig, PacksConfig, PathsConfig};

    fn paths(root: &std::path::Path) -> PathsConfig {
        PathsConfig {
            greentic_root: root.join("greentic"),
            state_dir: root.join("state"),
            cache_dir: root.join("cache"),
            logs_dir: root.join("logs"),
        }
    }

    #[test]
    fn index_location_parses_remote_and_file_values() {
        match IndexLocation::from_value("https://example.com/index.json").unwrap() {
            IndexLocation::Remote(url) => {
                assert_eq!(url.as_str(), "https://example.com/index.json")
            }
            IndexLocation::File(_) => panic!("expected remote index"),
        }

        match IndexLocation::from_value("file:///tmp/index.json").unwrap() {
            IndexLocation::File(path) => assert_eq!(path, PathBuf::from("/tmp/index.json")),
            IndexLocation::Remote(_) => panic!("expected file index"),
        }

        match IndexLocation::from_value("relative/index.json").unwrap() {
            IndexLocation::File(path) => assert_eq!(path, PathBuf::from("relative/index.json")),
            IndexLocation::Remote(_) => panic!("expected local file"),
        }
    }

    #[test]
    fn default_for_paths_prefers_greentic_root_index_when_present() {
        let temp = tempfile::tempdir_in(std::env::current_dir().expect("cwd")).expect("tempdir");
        let path_cfg = paths(temp.path());
        std::fs::create_dir_all(&path_cfg.cache_dir).expect("cache dir");
        std::fs::create_dir_all(&path_cfg.greentic_root).expect("greentic root");
        let expected_index = path_cfg.greentic_root.join("index.json");
        std::fs::write(&expected_index, "{}").expect("index file");

        let config = PackConfig::default_for_paths(&path_cfg).expect("default config");
        match config.index_location {
            IndexLocation::File(path) => assert_eq!(path, expected_index),
            IndexLocation::Remote(_) => panic!("expected local example index"),
        }
        assert_eq!(config.cache_dir, path_cfg.cache_dir.join("packs"));
    }

    #[test]
    fn from_packs_uses_cache_and_public_key() {
        let cfg = PacksConfig {
            source: PackSourceConfig::HttpIndex {
                url: "https://example.com/index.json".into(),
            },
            cache_dir: PathBuf::from("/tmp/packs-cache"),
            index_cache_ttl_secs: None,
            trust: Some(PackTrustConfig {
                public_keys: vec!["ed25519:abc".into()],
                require_signatures: true,
            }),
        };

        let pack = PackConfig::from_packs(&cfg).expect("pack config");
        assert_eq!(pack.cache_dir, PathBuf::from("/tmp/packs-cache"));
        assert_eq!(pack.public_key.as_deref(), Some("ed25519:abc"));
        match pack.index_location {
            IndexLocation::Remote(url) => {
                assert_eq!(url.as_str(), "https://example.com/index.json")
            }
            IndexLocation::File(_) => panic!("expected remote index"),
        }
    }

    #[test]
    fn from_packs_joins_all_trust_keys_and_rejects_oci_sources() {
        let mut cfg = PacksConfig {
            source: PackSourceConfig::LocalIndex {
                path: PathBuf::from("/tmp/index.json"),
            },
            cache_dir: PathBuf::from("/tmp/packs-cache"),
            index_cache_ttl_secs: None,
            trust: Some(PackTrustConfig {
                public_keys: vec!["ed25519:one".into(), "ed25519:two".into()],
                require_signatures: false,
            }),
        };
        let pack = PackConfig::from_packs(&cfg).expect("pack config");
        assert_eq!(pack.public_key.as_deref(), Some("ed25519:one,ed25519:two"));

        cfg.source = PackSourceConfig::OciRegistry {
            reference: "oci://example/packs".into(),
        };
        let err = PackConfig::from_packs(&cfg).expect_err("oci unsupported");
        assert!(err.to_string().contains("not supported"));
    }

    #[test]
    fn pack_source_accepts_aliases() {
        assert_eq!(PackSource::from_str("https").unwrap(), PackSource::Http);
        assert_eq!(
            PackSource::from_str("azureblob").unwrap(),
            PackSource::AzBlob
        );
        assert_eq!(PackSource::AzBlob.scheme(), "azblob");
        assert!(PackSource::from_str("ftp").is_err());
    }
}
