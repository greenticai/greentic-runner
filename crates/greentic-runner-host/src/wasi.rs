use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use wasmtime_wasi::{DirPerms, FilePerms, WasiCtx, WasiCtxBuilder};

/// Guest paths that are backed by per-tenant host directories.
const TENANT_SCOPED_GUEST_PATHS: [&str; 3] = ["/state", "/cache", "/logs"];

/// Validate a tenant id for use as a single host directory name.
///
/// Accepts `[A-Za-z0-9._-]{1,128}` and rejects `.`/`..`, so the result can
/// never contain a path separator or climb out of its parent.
pub(crate) fn tenant_dir_segment(tenant: &str) -> Result<&str> {
    let ok = !tenant.is_empty()
        && tenant.len() <= 128
        && tenant != "."
        && tenant != ".."
        && tenant
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
    if ok {
        Ok(tenant)
    } else {
        Err(anyhow!(
            "tenant id {tenant:?} is not safe to use as a directory name"
        ))
    }
}

/// Specification for exposing a host directory to the guest.
#[derive(Clone, Debug)]
pub struct PreopenSpec {
    pub host_path: PathBuf,
    pub guest_path: String,
    pub read_only: bool,
}

impl PreopenSpec {
    pub fn new(host_path: impl Into<PathBuf>, guest_path: impl Into<String>) -> Self {
        Self {
            host_path: host_path.into(),
            guest_path: guest_path.into(),
            read_only: false,
        }
    }

    pub fn read_only(mut self, value: bool) -> Self {
        self.read_only = value;
        self
    }

    fn validate(&self) -> Result<()> {
        let meta = fs::metadata(&self.host_path)
            .with_context(|| format!("failed to stat preopen {}", self.host_path.display()))?;
        if !meta.is_dir() {
            return Err(anyhow!(
                "preopen {} must point to a directory",
                self.host_path.display()
            ));
        }
        Ok(())
    }
}

/// Policy describing which WASI capabilities are surfaced into packs.
#[derive(Clone, Debug)]
pub struct RunnerWasiPolicy {
    pub inherit_stdio: bool,
    pub env_allow: Vec<String>,
    pub env_set: HashMap<String, String>,
    pub preopens: Vec<PreopenSpec>,
}

impl Default for RunnerWasiPolicy {
    fn default() -> Self {
        Self {
            inherit_stdio: true,
            env_allow: Vec::new(),
            env_set: HashMap::new(),
            preopens: Vec::new(),
        }
    }
}

impl RunnerWasiPolicy {
    pub fn new() -> Self {
        Self::default()
    }

    /// Locked-down WASI policy for identity probes.
    ///
    /// No preopens, no env passthrough, no stdio inheritance. This is the
    /// only reduced-authority boundary that the probe path can enforce today
    /// (the linker import surface must match the full component; see
    /// [`crate::pack::register_identity_probe`] doc comment).
    pub fn probe() -> Self {
        Self::new().inherit_stdio(false)
    }

    pub fn allow_env(mut self, key: impl Into<String>) -> Self {
        self.env_allow.push(key.into());
        self
    }

    pub fn with_env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env_set.insert(key.into(), value.into());
        self
    }

    pub fn with_preopen(mut self, spec: PreopenSpec) -> Self {
        self.preopens.push(spec);
        self
    }

    pub fn inherit_stdio(mut self, inherit: bool) -> Self {
        self.inherit_stdio = inherit;
        self
    }

    pub(crate) fn instantiate(&self) -> Result<WasiCtx> {
        let mut builder = WasiCtxBuilder::new();
        if self.inherit_stdio {
            // Route component stdout/stderr through the telemetry scanner so
            // structured guest fallback lines from greentic-telemetry land in
            // the host tracing pipeline (and from there in any configured
            // OTLP exporter) rather than escaping as free-form text. Lines
            // that don't match the telemetry format are forwarded verbatim
            // to the host's real stdout/stderr, preserving legacy println!
            // visibility.
            builder.stdout(crate::telemetry_scan::TelemetryStream::stdout());
            builder.stderr(crate::telemetry_scan::TelemetryStream::stderr());
            // stdin is deliberately NOT inherited: a guest has no business
            // reading the host process's stdin (the builder default is an
            // empty, closed stream).
        }
        let env_pairs = self.collect_env();
        if !env_pairs.is_empty() {
            let borrowed = env_pairs
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect::<Vec<_>>();
            builder.envs(&borrowed);
        }
        for spec in &self.preopens {
            self.install_preopen(&mut builder, spec)?;
        }
        Ok(builder.build())
    }

    /// Derive the policy a single tenant's packs run under.
    ///
    /// * The guest-visible `/state`, `/cache` and `/logs` preopens keep their
    ///   guest paths but are re-backed by `<host_path>/tenants/<tenant>`
    ///   (created on demand), so two tenants never share a host directory and
    ///   a guest never sees the sibling pack-artifact cache (`<cache>/packs`).
    ///   Data written under the previous host-wide directories is NOT migrated.
    /// * `GREENTIC_{STATE,CACHE,LOGS}_DIR` that point at a rewritten host path
    ///   follow it.
    /// * `env_allow` gains only this tenant's `env_passthrough` (the policy's
    ///   own entries, e.g. from an embedder, are kept).
    ///
    /// A tenant id is only validated when there is a tenant-scoped preopen to
    /// rewrite, so policies without such preopens are returned unchanged
    /// (apart from the env allowlist).
    pub fn for_tenant(&self, tenant: &str, env_passthrough: &[String]) -> Result<Self> {
        let mut out = self.clone();
        for key in env_passthrough {
            if !out.env_allow.contains(key) {
                out.env_allow.push(key.clone());
            }
        }
        for spec in &mut out.preopens {
            if !TENANT_SCOPED_GUEST_PATHS.contains(&spec.guest_path.as_str()) {
                continue;
            }
            let segment = tenant_dir_segment(tenant)?;
            let original = spec.host_path.clone();
            let scoped = original.join("tenants").join(segment);
            fs::create_dir_all(&scoped).with_context(|| {
                format!("failed to create tenant directory {}", scoped.display())
            })?;
            for value in out.env_set.values_mut() {
                if Path::new(value.as_str()) == original {
                    *value = scoped.display().to_string();
                }
            }
            spec.host_path = scoped;
        }
        Ok(out)
    }

    fn collect_env(&self) -> Vec<(String, String)> {
        let mut envs = Vec::new();
        for key in &self.env_allow {
            if let Ok(value) = std::env::var(key) {
                envs.push((key.clone(), value));
            }
        }
        for (key, value) in &self.env_set {
            // Remove any previously allowed env so explicit overrides win.
            if let Some(pos) = envs.iter().position(|(existing, _)| existing == key) {
                envs.remove(pos);
            }
            envs.push((key.clone(), value.clone()));
        }
        envs
    }

    fn install_preopen(&self, builder: &mut WasiCtxBuilder, spec: &PreopenSpec) -> Result<()> {
        spec.validate()?;
        let (dir_perms, file_perms) = if spec.read_only {
            (DirPerms::READ, FilePerms::READ)
        } else {
            (DirPerms::all(), FilePerms::all())
        };
        builder
            .preopened_dir(&spec.host_path, &spec.guest_path, dir_perms, file_perms)
            .map_err(|err| {
                anyhow!(
                    "failed to preopen {} as {}: {err}",
                    spec.host_path.display(),
                    spec.guest_path
                )
            })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A `WasiCtx` cannot be introspected for its stdin, so pin the source:
    /// a guest must never be handed the host's stdin.
    #[test]
    fn guest_stdin_is_never_inherited() {
        let src = include_str!("wasi.rs");
        let needle = ["inherit", "_stdin("].concat();
        let calls = src
            .lines()
            .filter(|l| !l.trim_start().starts_with("//") && l.contains(&needle))
            .count();
        // The only mention left is this test's own `needle` construction.
        assert_eq!(
            calls, 0,
            "wasi.rs must not call the stdin-inheriting builder method"
        );
    }

    fn base(root: &Path) -> RunnerWasiPolicy {
        let state = root.join("state");
        let cache = root.join("cache");
        let logs = root.join("logs");
        for d in [&state, &cache, &logs, &cache.join("packs")] {
            fs::create_dir_all(d).expect("mkdir");
        }
        RunnerWasiPolicy::default()
            .with_env("GREENTIC_STATE_DIR", state.display().to_string())
            .with_preopen(PreopenSpec::new(&state, "/state"))
            .with_preopen(PreopenSpec::new(&cache, "/cache"))
            .with_preopen(PreopenSpec::new(&logs, "/logs"))
    }

    #[test]
    fn tenants_get_disjoint_host_dirs_with_same_guest_paths() {
        let tmp = tempfile::TempDir::new().expect("tmp");
        let policy = base(tmp.path());
        let a = policy.for_tenant("acme", &[]).expect("a");
        let b = policy.for_tenant("globex", &[]).expect("b");
        for (pa, pb) in a.preopens.iter().zip(b.preopens.iter()) {
            assert_eq!(pa.guest_path, pb.guest_path);
            assert_ne!(pa.host_path, pb.host_path);
            assert!(pa.host_path.is_dir() && pb.host_path.is_dir());
            assert!(!pa.host_path.starts_with(&pb.host_path));
        }
        let cache = a
            .preopens
            .iter()
            .find(|p| p.guest_path == "/cache")
            .expect("cache");
        assert!(!cache.host_path.ends_with("packs"));
        assert!(cache.host_path.ends_with("tenants/acme"));
        assert_eq!(
            a.env_set.get("GREENTIC_STATE_DIR").map(String::as_str),
            Some(a.preopens[0].host_path.display().to_string().as_str())
        );
    }

    #[test]
    fn env_allowlist_is_per_tenant() {
        let tmp = tempfile::TempDir::new().expect("tmp");
        let policy = base(tmp.path());
        let a = policy.for_tenant("a", &["KEY_A".to_string()]).expect("a");
        let b = policy.for_tenant("b", &["KEY_B".to_string()]).expect("b");
        assert_eq!(a.env_allow, vec!["KEY_A"]);
        assert_eq!(b.env_allow, vec!["KEY_B"]);
        assert!(policy.env_allow.is_empty());
    }

    #[test]
    fn traversal_tenant_names_are_rejected() {
        let tmp = tempfile::TempDir::new().expect("tmp");
        let policy = base(tmp.path());
        for bad in ["", ".", "..", "../x", "a/b", "a\\b", "a b", "x\0y"] {
            assert!(policy.for_tenant(bad, &[]).is_err(), "{bad:?}");
        }
        assert!(policy.for_tenant("ok.tenant-1_x", &[]).is_ok());
    }

    #[test]
    fn policy_without_scoped_preopens_ignores_tenant_name() {
        let policy = RunnerWasiPolicy::new();
        assert!(policy.for_tenant("../x", &[]).is_ok());
    }
}
