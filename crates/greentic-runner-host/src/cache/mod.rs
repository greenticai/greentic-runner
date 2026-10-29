use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
use wasmtime::Engine;
use wasmtime::component::Component;

pub mod config;
pub mod disk;
pub mod engine_profile;
pub mod keys;
pub mod memory;
pub mod metadata;
pub mod singleflight;

pub use config::CacheConfig;
pub use engine_profile::{CpuPolicy, EngineProfile};
pub use keys::ArtifactKey;
pub use memory::MemoryStats;
pub use metadata::ArtifactMetadata;

use disk::DiskCache;
use memory::MemoryCache;
use singleflight::Singleflight;

#[derive(Clone, Debug)]
pub struct CacheManager {
    config: CacheConfig,
    profile: EngineProfile,
    memory: MemoryCache,
    disk: DiskCache,
    singleflight: Singleflight,
    metrics: Arc<CacheMetrics>,
}

#[derive(Debug, Default)]
struct CacheMetrics {
    memory_hits: AtomicU64,
    disk_hits: AtomicU64,
    disk_reads: AtomicU64,
    compiles: AtomicU64,
    deserialize_failures: AtomicU64,
}

#[derive(Clone, Debug, Default)]
pub struct CacheMetricsSnapshot {
    pub memory_hits: u64,
    pub disk_hits: u64,
    pub disk_reads: u64,
    pub compiles: u64,
    pub deserialize_failures: u64,
}

#[derive(Clone, Debug, Default)]
pub struct DiskStats {
    pub artifact_bytes: u64,
    pub artifact_count: u64,
}

impl CacheManager {
    pub fn new(config: CacheConfig, profile: EngineProfile) -> Self {
        let disk_root = config.disk_root(profile.id());
        let memory_max_bytes = config.memory_max_bytes;
        let lfu_protect_hits = config.lfu_protect_hits;
        let disk_max_bytes = config.disk_max_bytes;
        let memory = MemoryCache::new(memory_max_bytes, lfu_protect_hits);
        Self {
            config,
            profile: profile.clone(),
            memory,
            disk: DiskCache::new(disk_root, profile, disk_max_bytes),
            singleflight: Singleflight::new(),
            metrics: Arc::new(CacheMetrics::default()),
        }
    }

    pub fn engine_profile_id(&self) -> &str {
        self.profile.id()
    }

    pub fn metrics(&self) -> CacheMetricsSnapshot {
        CacheMetricsSnapshot {
            memory_hits: self.metrics.memory_hits.load(Ordering::Relaxed),
            disk_hits: self.metrics.disk_hits.load(Ordering::Relaxed),
            disk_reads: self.metrics.disk_reads.load(Ordering::Relaxed),
            compiles: self.metrics.compiles.load(Ordering::Relaxed),
            deserialize_failures: self.metrics.deserialize_failures.load(Ordering::Relaxed),
        }
    }

    pub fn memory_stats(&self) -> MemoryStats {
        self.memory.stats()
    }

    pub fn disk_stats(&self) -> Result<DiskStats> {
        if !self.config.disk_enabled {
            return Ok(DiskStats::default());
        }
        Ok(DiskStats {
            artifact_bytes: self.disk.approx_size_bytes()?,
            artifact_count: self.disk.artifact_count()?,
        })
    }

    pub async fn get_component(
        &self,
        engine: &Engine,
        key: &ArtifactKey,
        wasm_bytes: impl FnOnce() -> Result<Vec<u8>>,
    ) -> Result<Arc<Component>> {
        if self.config.memory_enabled
            && let Some(component) = self.memory.get(key)
        {
            self.metrics.memory_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(component);
        }
        if self.config.disk_enabled
            && let Some(component) = self.try_disk_hit(engine, key).await?
        {
            return Ok(component);
        }

        let _guard = self.singleflight.acquire(key.clone()).await;
        if self.config.memory_enabled
            && let Some(component) = self.memory.get(key)
        {
            self.metrics.memory_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(component);
        }
        if self.config.disk_enabled
            && let Some(component) = self.try_disk_hit(engine, key).await?
        {
            return Ok(component);
        }

        let bytes = wasm_bytes()?;
        self.metrics.compiles.fetch_add(1, Ordering::Relaxed);
        let wasm_len = bytes.len();
        // Compilation, serialization and the disk write are CPU/IO heavy; keep
        // them off the async workers.
        let engine_owned = engine.clone();
        let disk = self.disk.clone();
        let profile = self.profile.clone();
        let wasm_digest = key.wasm_digest.clone();
        let key_owned = key.clone();
        let disk_enabled = self.config.disk_enabled;
        let component = tokio::task::spawn_blocking(move || -> Result<Component> {
            let component = Component::from_binary(&engine_owned, &bytes)?;
            if disk_enabled && let Ok(serialized) = component.serialize() {
                let meta = ArtifactMetadata::new(&profile, wasm_digest, serialized.len() as u64);
                match disk.write_atomic(&key_owned, &serialized, &meta) {
                    Ok(()) => {
                        if let Err(error) = disk.prune_to_limit(false) {
                            tracing::warn!(error = %error, "disk cache prune failed");
                        }
                    }
                    Err(error) => {
                        tracing::warn!(error = %error, "disk cache write failed");
                    }
                }
            }
            Ok(component)
        })
        .await
        .context("component compile task failed")??;
        let component = Arc::new(component);
        if self.config.memory_enabled {
            self.memory
                .insert(key.clone(), Arc::clone(&component), wasm_len, false);
        }
        Ok(component)
    }

    /// Read the disk cache (off the async workers) and deserialize on a hit.
    async fn try_disk_hit(
        &self,
        engine: &Engine,
        key: &ArtifactKey,
    ) -> Result<Option<Arc<Component>>> {
        self.metrics.disk_reads.fetch_add(1, Ordering::Relaxed);
        let disk = self.disk.clone();
        let read_key = key.clone();
        let serialized = tokio::task::spawn_blocking(move || disk.try_read(&read_key))
            .await
            .context("disk cache read task failed")??;
        match serialized {
            Some(serialized) => Ok(self.deserialize_disk_hit(engine, key, serialized).await),
            None => Ok(None),
        }
    }

    /// Try to deserialize a disk-cached artifact. Returns `Some` on success
    /// (bumping `disk_hits` and inserting into the memory cache), or `None` on
    /// failure. On failure the corrupt/stale entry is deleted — the caller
    /// always has a `wasm_bytes` closure so a recompile will repopulate it via
    /// `write_atomic`, making the cache self-healing.
    #[allow(unsafe_code)]
    async fn deserialize_disk_hit(
        &self,
        engine: &Engine,
        key: &ArtifactKey,
        serialized: Vec<u8>,
    ) -> Option<Arc<Component>> {
        let engine_owned = engine.clone();
        let serialized_len = serialized.len();
        // Safety: serialized components are only loaded within the same engine
        // profile, and `DiskCache::try_read` verified the artifact hash.
        let outcome = tokio::task::spawn_blocking(move || unsafe {
            Component::deserialize(&engine_owned, &serialized)
        })
        .await;
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(join_error) => Err(wasmtime::Error::msg(format!(
                "deserialize task failed: {join_error}"
            ))),
        };
        match outcome {
            Ok(component) => {
                self.metrics.disk_hits.fetch_add(1, Ordering::Relaxed);
                let component = Arc::new(component);
                if self.config.memory_enabled {
                    self.memory
                        .insert(key.clone(), Arc::clone(&component), serialized_len, false);
                }
                Some(component)
            }
            Err(error) => {
                self.metrics
                    .deserialize_failures
                    .fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    wasm_digest = %key.wasm_digest,
                    engine_profile_id = %self.profile.engine_profile_id,
                    error = %error,
                    "cached component failed to deserialize; deleting stale entry"
                );
                // Delete the stale entry — get_component always receives a wasm_bytes
                // closure, so a recompile is possible and write_atomic will repopulate
                // the disk slot, making the cache self-healing.
                let _ = self.disk.delete(key);
                None
            }
        }
    }

    pub async fn warmup(
        &self,
        engine: &Engine,
        items: &[WarmupItem],
        mode: WarmupMode,
    ) -> Result<WarmupReport> {
        let mut warmed: u64 = 0;
        let mut skipped: u64 = 0;

        for item in items {
            if self.config.disk_enabled {
                let disk = self.disk.clone();
                let key = item.key.clone();
                let cached = tokio::task::spawn_blocking(move || disk.try_read(&key))
                    .await
                    .context("disk cache read task failed")?;
                if let Ok(Some(_)) = cached {
                    skipped += 1;
                    continue;
                }
            }

            let engine_owned = engine.clone();
            let disk = self.disk.clone();
            let profile = self.profile.clone();
            let disk_enabled = self.config.disk_enabled;
            let key = item.key.clone();
            let bytes = item.bytes.clone();
            let outcome = tokio::task::spawn_blocking(move || -> Result<()> {
                let serialized = engine_owned.precompile_component(&bytes)?;
                if disk_enabled {
                    let meta = ArtifactMetadata::new(
                        &profile,
                        key.wasm_digest.clone(),
                        serialized.len() as u64,
                    );
                    disk.write_atomic(&key, &serialized, &meta)?;
                }
                Ok(())
            })
            .await
            .context("warmup compile task failed")
            .and_then(|inner| inner);
            if outcome.is_ok() {
                self.metrics.compiles.fetch_add(1, Ordering::Relaxed);
            }

            match (outcome, mode) {
                (Ok(()), _) => warmed += 1,
                (Err(_), WarmupMode::BestEffort) => skipped += 1,
                (Err(e), WarmupMode::Strict) => return Err(e),
            }
        }

        Ok(WarmupReport { warmed, skipped })
    }

    pub fn doctor(&self) -> CacheDoctorReport {
        CacheDoctorReport {
            disk_enabled: self.config.disk_enabled,
            memory_enabled: self.config.memory_enabled,
            entries_checked: 0,
        }
    }

    pub async fn prune_disk(&self, dry_run: bool) -> Result<PruneReport> {
        self.disk.prune_to_limit(dry_run)
    }
}

#[derive(Clone, Debug)]
pub struct WarmupItem {
    pub key: ArtifactKey,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Copy, Debug)]
pub enum WarmupMode {
    BestEffort,
    Strict,
}

#[derive(Clone, Debug)]
pub struct WarmupReport {
    pub warmed: u64,
    pub skipped: u64,
}

#[derive(Clone, Debug)]
pub struct CacheDoctorReport {
    pub disk_enabled: bool,
    pub memory_enabled: bool,
    pub entries_checked: u64,
}

#[derive(Clone, Debug)]
pub struct PruneReport {
    pub removed_entries: u64,
    pub removed_bytes: u64,
}

#[cfg(test)]
mod tests;
