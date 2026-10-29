use anyhow::{Result, bail};
use chrono::{DateTime, Utc};

use crate::cache::engine_profile::EngineProfile;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ArtifactMetadata {
    pub schema_version: u32,
    pub engine_profile_id: String,
    pub wasmtime_version: String,
    pub target_triple: String,
    pub cpu_policy: String,
    pub config_fingerprint: String,
    pub wasm_digest: String,
    pub artifact_bytes: u64,
    pub created_at: String,
    pub last_access_at: String,
    pub hit_count: u64,
    /// Hex sha256 of the serialized `.cwasm` bytes, verified on every read so
    /// a tampered or truncated artifact is never deserialized. Entries written
    /// before this field existed have `None` and are treated as a miss.
    #[serde(default)]
    pub artifact_sha256: Option<String>,
}

impl ArtifactMetadata {
    pub fn new(profile: &EngineProfile, wasm_digest: String, artifact_bytes: u64) -> Self {
        let now = Utc::now();
        let ts = now.to_rfc3339();
        Self {
            schema_version: 1,
            engine_profile_id: profile.engine_profile_id.clone(),
            wasmtime_version: profile.wasmtime_version.clone(),
            target_triple: profile.target_triple.clone(),
            cpu_policy: profile.cpu_policy.as_str().to_string(),
            config_fingerprint: profile.config_fingerprint.clone(),
            wasm_digest,
            artifact_bytes,
            created_at: ts.clone(),
            last_access_at: ts,
            hit_count: 0,
            artifact_sha256: None,
        }
    }

    pub fn with_artifact_sha256(mut self, hex_digest: String) -> Self {
        self.artifact_sha256 = Some(hex_digest);
        self
    }

    pub fn validate_for_profile(&self, profile: &EngineProfile) -> Result<()> {
        if self.schema_version != 1 {
            bail!("cache metadata schema_version must be 1");
        }
        if self.engine_profile_id != profile.engine_profile_id {
            bail!("cache metadata engine_profile_id mismatch");
        }
        if self.wasmtime_version != profile.wasmtime_version {
            bail!("cache metadata wasmtime_version mismatch");
        }
        if self.target_triple != profile.target_triple {
            bail!("cache metadata target_triple mismatch");
        }
        if self.cpu_policy != profile.cpu_policy.as_str() {
            bail!("cache metadata cpu_policy mismatch");
        }
        if self.config_fingerprint != profile.config_fingerprint {
            bail!("cache metadata config_fingerprint mismatch");
        }
        Ok(())
    }

    pub fn last_access_time(&self) -> Option<DateTime<Utc>> {
        DateTime::parse_from_rfc3339(&self.last_access_at)
            .ok()
            .map(|dt| dt.with_timezone(&Utc))
            .or_else(|| {
                DateTime::parse_from_rfc3339(&self.created_at)
                    .ok()
                    .map(|dt| dt.with_timezone(&Utc))
            })
    }

    pub fn touch(&mut self) {
        self.last_access_at = Utc::now().to_rfc3339();
        self.hit_count = self.hit_count.saturating_add(1);
    }
}
