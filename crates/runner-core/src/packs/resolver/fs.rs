use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use url::Url;

use crate::path_safety::normalize_under_root;

use super::{FetchResponse, PackResolver};

#[derive(Debug)]
pub struct FsResolver {
    root: PathBuf,
    /// When `Some`, absolute locators must resolve under `root` or one of
    /// these roots. `None` keeps the permissive dev behaviour.
    allowed_roots: Option<Vec<PathBuf>>,
}

impl FsResolver {
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            allowed_roots: None,
        }
    }

    /// Confine absolute locators to `root` plus the given extra roots.
    pub fn with_allowed_roots(mut self, extra_roots: Vec<PathBuf>) -> Self {
        let mut roots = vec![self.root.clone()];
        roots.extend(extra_roots);
        self.allowed_roots = Some(roots);
        self
    }

    fn ensure_allowed(&self, path: &Path) -> Result<()> {
        let Some(roots) = &self.allowed_roots else {
            return Ok(());
        };
        let canonical = path
            .canonicalize()
            .with_context(|| format!("failed to canonicalize {}", path.display()))?;
        let allowed = roots
            .iter()
            .filter_map(|root| root.canonicalize().ok())
            .any(|root| canonical.starts_with(&root));
        if !allowed {
            anyhow::bail!(
                "fs resolver: {} is outside the allowed pack roots",
                canonical.display()
            );
        }
        Ok(())
    }

    fn parse_path(&self, locator: &str) -> Result<PathBuf> {
        if let Some(stripped) = locator.strip_prefix("fs://") {
            if stripped.starts_with('/')
                || stripped.starts_with("./")
                || stripped.starts_with("../")
            {
                return Ok(PathBuf::from(stripped));
            }
            if cfg!(windows) && stripped.chars().nth(1) == Some(':') {
                return Ok(PathBuf::from(stripped));
            }
            let file_url = format!("file://{stripped}");
            let url = Url::parse(&file_url).context("failed to parse fs:// locator as file URL")?;
            return url
                .to_file_path()
                .map_err(|_| anyhow!("fs locator {locator} cannot be represented as a path"));
        }
        Ok(PathBuf::from(locator))
    }

    fn normalize(&self, path: PathBuf) -> Result<PathBuf> {
        if path.is_absolute() {
            let parent = path
                .parent()
                .ok_or_else(|| anyhow!("fs locator missing parent: {}", path.display()))?;
            let root = parent
                .canonicalize()
                .with_context(|| format!("failed to canonicalize {}", parent.display()))?;
            let file = path
                .file_name()
                .ok_or_else(|| anyhow!("fs locator missing file name: {}", path.display()))?;
            return normalize_under_root(&root, Path::new(file));
        }
        normalize_under_root(&self.root, &path)
    }
}

impl PackResolver for FsResolver {
    fn scheme(&self) -> &'static str {
        "fs"
    }

    fn fetch(&self, locator: &str) -> Result<FetchResponse> {
        let path = self.parse_path(locator)?;
        let path = self.normalize(path)?;
        self.ensure_allowed(&path)?;
        if !path.exists() {
            anyhow::bail!("fs resolver: {} does not exist", path.display());
        }
        Ok(FetchResponse::from_path(path))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn confined_resolver_rejects_absolute_path_outside_roots() {
        let root = tempfile::tempdir().expect("root");
        let outside = tempfile::tempdir().expect("outside");
        let file = outside.path().join("pack.gtpack");
        std::fs::write(&file, b"x").expect("write");
        let resolver = FsResolver::new(root.path().to_path_buf()).with_allowed_roots(Vec::new());
        let err = resolver
            .fetch(&format!("fs://{}", file.display()))
            .err()
            .expect("outside path must be rejected");
        assert!(err.to_string().contains("outside the allowed"));
    }

    #[test]
    fn confined_resolver_accepts_extra_allowed_root() {
        let root = tempfile::tempdir().expect("root");
        let extra = tempfile::tempdir().expect("extra");
        let file = extra.path().join("pack.gtpack");
        std::fs::write(&file, b"x").expect("write");
        let resolver = FsResolver::new(root.path().to_path_buf())
            .with_allowed_roots(vec![extra.path().to_path_buf()]);
        resolver
            .fetch(&format!("fs://{}", file.display()))
            .expect("allowed root accepted");
    }

    #[test]
    fn unconfined_resolver_keeps_dev_behaviour() {
        let root = tempfile::tempdir().expect("root");
        let outside = tempfile::tempdir().expect("outside");
        let file = outside.path().join("pack.gtpack");
        std::fs::write(&file, b"x").expect("write");
        let resolver = FsResolver::new(root.path().to_path_buf());
        resolver
            .fetch(&format!("fs://{}", file.display()))
            .expect("permissive by default");
    }
}
