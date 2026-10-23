use std::path::Path;

use anyhow::{Context, Result, bail};
use tokio::fs;

/// Structural check of a pack archive: it must be a non-empty regular file
/// that opens as a valid zip with a readable central directory.
///
/// This does NOT verify signatures or content digests; those are enforced
/// upstream by `runner_core::PackManager` when packs are resolved from an
/// index. Callers must not describe a successful result as "verified".
pub async fn verify_pack(path: &Path) -> Result<()> {
    let metadata = fs::metadata(path).await?;
    if !metadata.is_file() {
        bail!("pack path {path:?} is not a file");
    }
    if metadata.len() == 0 {
        bail!("pack archive {path:?} is empty");
    }
    let owned = path.to_path_buf();
    tokio::task::spawn_blocking(move || -> Result<()> {
        let file = std::fs::File::open(&owned)
            .with_context(|| format!("failed to open pack archive {owned:?}"))?;
        zip::ZipArchive::new(file)
            .with_context(|| format!("pack archive {owned:?} is not a valid zip"))?;
        Ok(())
    })
    .await
    .context("pack archive check task failed")?
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[tokio::test]
    async fn rejects_directory_empty_and_non_zip() {
        let dir = tempfile::tempdir().expect("tmp");
        assert!(verify_pack(dir.path()).await.is_err());

        let empty = dir.path().join("empty.gtpack");
        std::fs::write(&empty, b"").expect("write");
        assert!(verify_pack(&empty).await.is_err());

        let junk = dir.path().join("junk.gtpack");
        std::fs::write(&junk, b"definitely not a zip archive").expect("write");
        assert!(verify_pack(&junk).await.is_err());
    }

    #[tokio::test]
    async fn accepts_valid_zip() {
        let dir = tempfile::tempdir().expect("tmp");
        let path = dir.path().join("ok.gtpack");
        let file = std::fs::File::create(&path).expect("create");
        let mut writer = zip::ZipWriter::new(file);
        writer
            .start_file("manifest.json", zip::write::SimpleFileOptions::default())
            .expect("start");
        writer.write_all(b"{}").expect("write");
        writer.finish().expect("finish");
        verify_pack(&path).await.expect("valid zip");
    }
}
