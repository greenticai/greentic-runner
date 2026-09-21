use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::runtime::block_on;
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use greentic_secrets_lib::env::EnvSecretsManager;
use greentic_secrets_lib::{SecretScope, SecretsManager};
use greentic_types::TenantCtx;
use lru::LruCache;
use parking_lot::Mutex;

/// Shared secrets manager handle used by the host.
pub type DynSecretsManager = Arc<dyn SecretsManager>;

/// Supported secrets backend kinds recognised by the runner.
#[derive(Clone, Debug)]
pub enum SecretsBackend {
    Env,
    /// HTTP secrets broker (greentic-secrets-broker). The `endpoint` is the
    /// base URL (e.g. `http://secrets-broker:8080`) and `token` is the Bearer
    /// auth token (may be empty for unauthenticated local deployments).
    Broker {
        endpoint: String,
        token: String,
    },
}

impl SecretsBackend {
    pub fn from_env(value: Option<String>) -> Result<Self> {
        match value
            .unwrap_or_else(|| "env".into())
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "" | "env" => Ok(SecretsBackend::Env),
            "broker" => Self::broker_from_strings(
                "broker",
                std::env::var("SECRETS_BROKER_ENDPOINT")
                    .unwrap_or_default()
                    .as_str(),
                std::env::var("SECRETS_BROKER_TOKEN")
                    .unwrap_or_default()
                    .as_str(),
            ),
            other => Err(anyhow!("unsupported SECRETS_BACKEND `{other}`")),
        }
    }

    /// Pure constructor used by both `from_env` and tests. Validates that
    /// `endpoint` is non-empty; `token` may be empty for unauthenticated use.
    /// `kind` is passed solely for error-message context.
    pub(crate) fn broker_from_strings(kind: &str, endpoint: &str, token: &str) -> Result<Self> {
        let endpoint = endpoint.trim().trim_end_matches('/');
        if endpoint.is_empty() {
            return Err(anyhow!(
                "SECRETS_BACKEND={kind} requires SECRETS_BROKER_ENDPOINT to be set"
            ));
        }
        Ok(SecretsBackend::Broker {
            endpoint: endpoint.to_owned(),
            token: token.to_owned(),
        })
    }

    pub fn from_config(cfg: &greentic_config_types::SecretsBackendRefConfig) -> Result<Self> {
        match cfg.kind.trim().to_ascii_lowercase().as_str() {
            "" | "none" | "env" => Ok(SecretsBackend::Env),
            // `SecretsBackendRefConfig` has no dedicated endpoint/token fields.
            // When kind="broker", use `reference` as the endpoint (if set) and
            // fall back to SECRETS_BROKER_ENDPOINT from the environment.
            // Token always comes from SECRETS_BROKER_TOKEN env.
            "broker" => {
                let endpoint = cfg
                    .reference
                    .as_deref()
                    .unwrap_or("")
                    .trim()
                    .trim_end_matches('/');
                let endpoint = if endpoint.is_empty() {
                    std::env::var("SECRETS_BROKER_ENDPOINT").unwrap_or_default()
                } else {
                    endpoint.to_owned()
                };
                let token = std::env::var("SECRETS_BROKER_TOKEN").unwrap_or_default();
                Self::broker_from_strings("broker", &endpoint, &token)
            }
            other => Err(anyhow!("unsupported secrets backend `{other}`")),
        }
    }

    pub fn build_manager(&self) -> Result<DynSecretsManager> {
        let inner: DynSecretsManager = match self {
            SecretsBackend::Env => {
                ensure_env_secrets_allowed()?;
                Arc::new(EnvSecretsManager)
            }
            SecretsBackend::Broker { endpoint, token } => Arc::new(
                crate::secrets_broker::BrokerSecretsManager::new(endpoint, token),
            ),
        };
        Ok(CachingSecretsManager::wrap(inner))
    }
}

pub fn default_manager() -> Result<DynSecretsManager> {
    SecretsBackend::Env.build_manager()
}

// ---------------------------------------------------------------------------
// Caching wrapper
// ---------------------------------------------------------------------------

const DEFAULT_CACHE_TTL_SECS: u64 = 300;
const DEFAULT_CACHE_MAX_ENTRIES: usize = 512;

fn cache_ttl() -> Duration {
    let secs = std::env::var("SECRETS_CACHE_TTL_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(DEFAULT_CACHE_TTL_SECS);
    Duration::from_secs(secs)
}

fn cache_max_entries() -> NonZeroUsize {
    let n = std::env::var("SECRETS_CACHE_MAX_ENTRIES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(DEFAULT_CACHE_MAX_ENTRIES);
    NonZeroUsize::new(n).expect("cache max entries must be > 0")
}

struct CacheEntry {
    data: Vec<u8>,
    inserted_at: Instant,
}

/// A [`SecretsManager`] wrapper that caches read results in an LRU cache with
/// a time-to-live. Writes and deletes invalidate the corresponding entry so
/// subsequent reads always see the latest value.
pub struct CachingSecretsManager {
    inner: DynSecretsManager,
    cache: Mutex<LruCache<String, CacheEntry>>,
    ttl: Duration,
}

impl CachingSecretsManager {
    pub fn wrap(inner: DynSecretsManager) -> DynSecretsManager {
        Self::wrap_with(inner, cache_ttl(), cache_max_entries())
    }

    fn wrap_with(inner: DynSecretsManager, ttl: Duration, max: NonZeroUsize) -> DynSecretsManager {
        if ttl.is_zero() {
            tracing::info!("secrets cache disabled (TTL=0)");
            return inner;
        }
        tracing::info!(
            ttl_secs = ttl.as_secs(),
            max_entries = max.get(),
            "secrets value cache enabled"
        );
        Arc::new(Self {
            inner,
            cache: Mutex::new(LruCache::new(max)),
            ttl,
        })
    }
}

#[async_trait]
impl SecretsManager for CachingSecretsManager {
    async fn read(&self, path: &str) -> greentic_secrets_lib::Result<Vec<u8>> {
        // Check cache first.
        {
            let mut cache = self.cache.lock();
            if let Some(entry) = cache.get(path) {
                if entry.inserted_at.elapsed() < self.ttl {
                    return Ok(entry.data.clone());
                }
                // Expired — remove and fall through.
                cache.pop(path);
            }
        }

        let data = self.inner.read(path).await?;

        {
            let mut cache = self.cache.lock();
            cache.put(
                path.to_owned(),
                CacheEntry {
                    data: data.clone(),
                    inserted_at: Instant::now(),
                },
            );
        }
        Ok(data)
    }

    async fn write(&self, path: &str, bytes: &[u8]) -> greentic_secrets_lib::Result<()> {
        self.inner.write(path, bytes).await?;
        // Invalidate so the next read sees the fresh value.
        self.cache.lock().pop(path);
        Ok(())
    }

    async fn delete(&self, path: &str) -> greentic_secrets_lib::Result<()> {
        self.inner.delete(path).await?;
        self.cache.lock().pop(path);
        Ok(())
    }
}

fn normalize_pack_segment(pack_id: &str) -> String {
    pack_id
        .chars()
        .map(|ch| {
            let ch = ch.to_ascii_lowercase();
            match ch {
                'a'..='z' | '0'..='9' | '_' | '-' => ch,
                _ => '_',
            }
        })
        .collect()
}

/// Marker between the pack segment and the unit segment in a unit-scoped pack
/// segment: `<pack>_unit_<unit slug>_<unit hash>`.
///
/// A SINGLE underscore, not a double one. `greentic_secrets_lib`'s
/// `canonical_secret_name` — which greentic-setup / greentic-deployer /
/// greentic-start run over a secret name — **collapses runs of `_`**, so a
/// `__unit_` marker is not a fixed point of it: one side would write
/// `pack__unit_x` and the other would read `pack_unit_x`, at addresses that
/// never meet, with nothing failing at any layer.
const UNIT_PACK_SEGMENT_MARKER: &str = "_unit_";

/// Longest slug kept from the pack id / the unit id before their hash
/// suffixes. Bounds the segment on a long deployment name.
const UNIT_SEGMENT_SLUG_MAX: usize = 40;

/// Bytes of `sha256(..)` rendered as a hash suffix (12 lowercase hex chars).
const UNIT_SEGMENT_HASH_BYTES: usize = 6;

/// Lowercase `[a-z0-9_]` slug: every ASCII alphanumeric is lowercased, every
/// other character becomes `_`, runs of `_` collapse to one, leading/trailing
/// `_` are trimmed, the result is cut to `max` bytes and re-trimmed.
///
/// Collapsing and trimming are what make the output a FIXED POINT of
/// `greentic_secrets_lib::canonical_secret_name` (which collapses and trims),
/// and restricting to `[a-z0-9_]` is what makes it a fixed point of
/// [`canonicalize_secret_key`] (which maps everything else to `_`). Hyphens
/// are deliberately NOT kept, unlike [`normalize_pack_segment`]: the spec
/// canonicalizer maps `-` to `_`, so a surviving hyphen would break the fixed
/// point.
fn underscore_slug(raw: &str, max: usize) -> String {
    let mut slug = String::with_capacity(raw.len());
    for ch in raw.chars() {
        let mapped = if ch.is_ascii_alphanumeric() {
            ch.to_ascii_lowercase()
        } else {
            '_'
        };
        if mapped == '_' && (slug.is_empty() || slug.ends_with('_')) {
            continue;
        }
        slug.push(mapped);
    }
    // ASCII-only by construction, so a byte cut never splits a char.
    slug.truncate(max);
    slug.trim_end_matches('_').to_string()
}

/// First [`UNIT_SEGMENT_HASH_BYTES`] bytes of `sha256(raw)`, lowercase hex.
fn short_hash(raw: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(raw.as_bytes());
    digest[..UNIT_SEGMENT_HASH_BYTES]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// The PACK segment a deployed unit's own copy of a pack secret lives under:
/// `<pack slug>_unit_<unit slug>_<unit hash>`.
///
/// THE one normalisation for this address — the designer (which stages the
/// value) and greentic-setup / greentic-deployer / greentic-start must produce
/// exactly this string, so they should call this function rather than copy it.
/// It is the pack-secret counterpart of
/// `greentic_aw_runtime::mcp_secrets::mcp_unit_segment`, and differs from it in
/// exactly one way that matters: an MCP secret's NAME is written and read
/// VERBATIM by admin, so that segment may use `-`; a pack secret's address is
/// canonicalized on both sides, so this one may not.
///
/// The full URI is
/// `secrets://<env>/<tenant>/<team|_>/<this segment>/<canonical key>` — the
/// unit rides in the CATEGORY (pack) segment, not in a sixth segment, because
/// `greentic_secrets_lib`'s `SecretUri::parse` accepts exactly five and rejects
/// `ExtraSegments`.
///
/// Rules, applied to the trimmed inputs:
///
/// 1. the pack id and the unit id are each reduced by [`underscore_slug`];
/// 2. the unit ALWAYS carries a `sha256` suffix, so two unit ids the slug
///    alone would merge (`Web Assistant` / `web-assistant`, or two ids sharing
///    a 40-byte prefix) still get different segments — and so no two
///    `(pack, unit)` pairs can produce one string by splitting at a different
///    `_unit_`;
/// 3. a pack id whose slug is empty (punctuation only) falls back to
///    `pack_<hash>` rather than an empty leading segment, which would put a
///    leading `_` in the output and break the fixed point.
///
/// Returns `None` for a blank pack id or a blank unit id: there is no unit
/// scope then, and the caller must fall through to the bare pack segment.
pub fn unit_pack_segment(pack_id: &str, unit_id: &str) -> Option<String> {
    let pack_raw = pack_id.trim();
    let unit_raw = unit_id.trim();
    if pack_raw.is_empty() || unit_raw.is_empty() {
        return None;
    }

    let pack_slug = underscore_slug(pack_raw, UNIT_SEGMENT_SLUG_MAX);
    let pack_part = if pack_slug.is_empty() {
        format!("pack_{}", short_hash(pack_raw))
    } else {
        pack_slug
    };

    let unit_hash = short_hash(unit_raw);
    let unit_slug = underscore_slug(unit_raw, UNIT_SEGMENT_SLUG_MAX);
    let unit_part = if unit_slug.is_empty() {
        unit_hash
    } else {
        format!("{unit_slug}_{unit_hash}")
    };

    Some(format!("{pack_part}{UNIT_PACK_SEGMENT_MARKER}{unit_part}"))
}

/// The secret URI for one deployed unit's own copy of a pack secret:
/// [`scoped_secret_path_for_pack`] with the pack segment replaced by
/// [`unit_pack_segment`]. `Ok(None)` when there is no unit scope to build.
pub fn scoped_secret_path_for_unit(
    ctx: &TenantCtx,
    pack_id: &str,
    unit_id: &str,
    key: &str,
) -> Result<Option<String>> {
    match unit_pack_segment(pack_id, unit_id) {
        // The unit segment is already `[a-z0-9_]`, so re-normalising it inside
        // `scoped_secret_path_for_pack` is the identity — which is the whole
        // point of the fixed-point rule above.
        Some(segment) => scoped_secret_path_for_pack(ctx, &segment, key).map(Some),
        None => Ok(None),
    }
}

/// Every URI a pack-secret read will try, in order:
///
/// 1. the UNIT scope — `<pack>_unit_<unit>` — when a unit is known;
/// 2. today's bare pack scope.
///
/// REVIEW BY 2026-12-18: candidate 2 is a COMPATIBILITY fallback when a unit is
/// known, not a design choice. It exists so an environment staged before the
/// unit scope existed keeps its credential when its runner moves; the next
/// stage writes the unit-scoped address. Once every lane stages unit-scoped
/// pack secrets (greentic-designer `per-unit-setup-isolation` Phase 2), the
/// fallback lets one unit silently read a value another unit's writer left at
/// the shared address — decide then whether to drop it for the unit case.
/// (Candidate 2 remains the ONLY candidate when no unit is known — the legacy
/// tenant-only runtime and the process-level serve path.)
fn pack_secret_path_candidates(
    ctx: &TenantCtx,
    pack_id: &str,
    unit_id: Option<&str>,
    key: &str,
) -> Result<Vec<String>> {
    let bare = scoped_secret_path_for_pack(ctx, pack_id, key)?;
    let unit = match unit_id {
        Some(unit) => scoped_secret_path_for_unit(ctx, pack_id, unit, key)?,
        None => None,
    };
    Ok(match unit {
        // A unit URI equal to the bare one would make the fallback a duplicate
        // read rather than a fallback; it cannot happen (the unit segment always
        // carries `_unit_<hash>`) but the guard keeps that a fact, not a hope.
        Some(unit) if unit != bare => vec![unit, bare],
        _ => vec![bare],
    })
}

/// Read a pack secret for one deployed unit: the unit scope first, then the
/// bare pack scope (see [`pack_secret_path_candidates`]). First hit wins.
///
/// `unit_id` is the running revision's `bundle_id` — the deployment name,
/// unique within an environment — taken from the runtime that built this pack,
/// never from a process environment variable: one greentic-start process serves
/// every revision of an environment, so an env var cannot differ per unit.
///
/// There is no case where a previously-resolving secret stops resolving: the
/// bare scope is still tried, and only after the unit scope misses.
pub fn read_pack_secret_blocking(
    manager: &DynSecretsManager,
    ctx: &TenantCtx,
    pack_id: &str,
    unit_id: Option<&str>,
    key: &str,
) -> Result<Vec<u8>> {
    let candidates = pack_secret_path_candidates(ctx, pack_id, unit_id, key)?;
    let mut last_error = anyhow!("no scope tried");
    for candidate in &candidates {
        match block_on(manager.read(candidate.as_str())) {
            Ok(bytes) => return Ok(bytes),
            Err(err) => last_error = anyhow!(err.to_string()),
        }
    }
    Err(anyhow!(
        "no secret at {} ({last_error})",
        candidates.join(" or ")
    ))
}

/// Write a pack secret for one deployed unit.
///
/// **The unit scope is the ONLY write target when a unit is known** — there is
/// deliberately no write-through to the bare pack address. A component writing
/// its refreshed token back must not overwrite the value every other unit of
/// the same pack reads; that is the isolation this whole address exists for.
/// With no unit known the write lands at the bare address exactly as before.
pub fn write_pack_secret_blocking(
    manager: &DynSecretsManager,
    ctx: &TenantCtx,
    pack_id: &str,
    unit_id: Option<&str>,
    key: &str,
    value: &[u8],
) -> Result<()> {
    let target = match unit_id {
        Some(unit) => scoped_secret_path_for_unit(ctx, pack_id, unit, key)?,
        None => None,
    };
    let target = match target {
        Some(target) => target,
        None => scoped_secret_path_for_pack(ctx, pack_id, key)?,
    };
    block_on(manager.write(target.as_str(), value)).map_err(|err| anyhow!(err.to_string()))?;
    Ok(())
}

pub fn canonicalize_secret_key(raw: &str) -> String {
    raw.trim()
        .chars()
        .map(|ch| {
            let ch = ch.to_ascii_lowercase();
            match ch {
                'a'..='z' | '0'..='9' | '_' => ch,
                _ => '_',
            }
        })
        .collect()
}

fn normalize_team_segment(team: Option<&str>) -> String {
    match team
        .map(str::trim)
        .filter(|value| !value.is_empty() && !value.eq_ignore_ascii_case("default"))
    {
        Some(value) => value.to_string(),
        None => "_".to_string(),
    }
}

pub fn scoped_secret_path_for_pack(ctx: &TenantCtx, pack_id: &str, key: &str) -> Result<String> {
    let key = key.trim();
    if key.is_empty() {
        return Err(anyhow!("secret key must not be empty"));
    }
    let safe_key = canonicalize_secret_key(key);
    let team = ctx.team_id.as_ref().or(ctx.team.as_ref());
    let scope = SecretScope {
        env: ctx.env.as_str().to_string(),
        tenant: ctx.tenant.as_str().to_string(),
        team: team.map(|value| value.as_str().to_string()),
    };
    let team_segment = normalize_team_segment(scope.team.as_deref());
    let pack_segment = pack_id.trim();
    if pack_segment.is_empty() {
        return Err(anyhow!("pack_id must not be empty for scoped secrets"));
    }
    let pack_segment = normalize_pack_segment(pack_segment);
    Ok(format!(
        "secrets://{}/{}/{}/{}/{}",
        scope.env, scope.tenant, team_segment, pack_segment, safe_key
    ))
}

pub fn read_secret_blocking(
    manager: &DynSecretsManager,
    ctx: &TenantCtx,
    pack_id: &str,
    key: &str,
) -> Result<Vec<u8>> {
    let scoped_key = scoped_secret_path_for_pack(ctx, pack_id, key)?;
    let bytes =
        block_on(manager.read(scoped_key.as_str())).map_err(|err| anyhow!(err.to_string()))?;
    Ok(bytes)
}

pub fn write_secret_blocking(
    manager: &DynSecretsManager,
    ctx: &TenantCtx,
    pack_id: &str,
    key: &str,
    value: &[u8],
) -> Result<()> {
    let scoped_key = scoped_secret_path_for_pack(ctx, pack_id, key)?;
    block_on(manager.write(scoped_key.as_str(), value)).map_err(|err| anyhow!(err.to_string()))?;
    Ok(())
}

fn ensure_env_secrets_allowed() -> Result<()> {
    let env = std::env::var("GREENTIC_ENV").unwrap_or_else(|_| "local".to_string());
    let env = env.trim().to_ascii_lowercase();
    if matches!(env.as_str(), "local" | "dev" | "test") {
        Ok(())
    } else {
        Err(anyhow!(
            "env secrets backend is disabled for env '{env}' (dev/test only)"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use greentic_config_types::SecretsBackendRefConfig;
    use greentic_types::{EnvId, TeamId, TenantId, UserId};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn tenant_ctx() -> greentic_types::TenantCtx {
        greentic_types::TenantCtx::new(
            EnvId::new("local").expect("env"),
            TenantId::new("tenant-a").expect("tenant"),
        )
        .with_team(Some(TeamId::new("team-a").expect("team")))
        .with_user(Some(UserId::new("user-a").expect("user")))
    }

    #[test]
    fn scoped_secret_path_normalizes_pack_and_key_segments() {
        let path =
            scoped_secret_path_for_pack(&tenant_ctx(), "My Pack/Prod", " API/KEY value ").unwrap();
        assert_eq!(
            path,
            "secrets://local/tenant-a/team-a/my_pack_prod/api_key_value"
        );
    }

    #[test]
    fn scoped_secret_path_rejects_empty_inputs() {
        let ctx = tenant_ctx();
        assert!(scoped_secret_path_for_pack(&ctx, "demo", "   ").is_err());
        assert!(scoped_secret_path_for_pack(&ctx, "   ", "key").is_err());
    }

    #[test]
    fn scoped_secret_path_maps_default_team_to_underscore() {
        let ctx = greentic_types::TenantCtx::new(
            EnvId::new("dev").expect("env"),
            TenantId::new("demo").expect("tenant"),
        )
        .with_team(Some(TeamId::new("default").expect("team")));
        let path = scoped_secret_path_for_pack(&ctx, "ollama-runtime-repro", "ollama_api_key")
            .expect("scoped secret path");
        assert_eq!(
            path,
            "secrets://dev/demo/_/ollama-runtime-repro/ollama_api_key"
        );
    }

    #[test]
    fn scoped_secret_path_canonicalizes_provider_secret_keys() {
        let ctx = greentic_types::TenantCtx::new(
            EnvId::new("dev").expect("env"),
            TenantId::new("demo").expect("tenant"),
        )
        .with_team(Some(TeamId::new("default").expect("team")))
        .with_user(Some(UserId::new("operator").expect("user")));
        let path = scoped_secret_path_for_pack(&ctx, "ollama-runtime-repro", "OLLAMA_API_KEY")
            .expect("scoped secret path");
        assert_eq!(
            path,
            "secrets://dev/demo/_/ollama-runtime-repro/ollama_api_key"
        );
    }

    // ── unit-scoped pack segment ────────────────────────────────────────

    /// The property the whole address depends on: the segment must survive
    /// BOTH canonicalizers untouched. The runner applies
    /// [`canonicalize_secret_key`]; greentic-setup / greentic-deployer /
    /// greentic-start apply `greentic_secrets_lib::canonical_secret_name`,
    /// which additionally COLLAPSES runs of `_` and trims them. A segment that
    /// is not a fixed point of both produces an address one side writes and the
    /// other never reads, with nothing failing at any layer — which is why this
    /// is asserted explicitly rather than left implied by the character set.
    #[test]
    fn the_unit_segment_is_a_fixed_point_of_both_canonicalizers() {
        let ids = [
            ("pack.dw.support", "web-assistant"),
            ("My Pack/Prod", "Web Assistant (prod)"),
            ("demo  pack", "unit_01JTKS.v2"),
            ("pack--x", "ÜNÏCÖDÉ-Ünit"),
            ("a.b-c_d", "---"),
            ("...", "a/b/c@d"),
            ("ollama-runtime-repro", "   x   "),
            (
                "a-very-long-pack-id-that-exceeds-the-slug-limit-by-a-lot-of-bytes",
                "a-very-long-deployment-name-that-exceeds-the-slug-limit-by-a-lot",
            ),
        ];
        for (pack, unit) in ids {
            let segment = unit_pack_segment(pack, unit).expect("segment");
            assert_eq!(
                canonicalize_secret_key(&segment),
                segment,
                "`{pack}` / `{unit}` is not a fixed point of canonicalize_secret_key"
            );
            assert_eq!(
                greentic_secrets_lib::spec::canonical_secret_name(&segment),
                segment,
                "`{pack}` / `{unit}` is not a fixed point of canonical_secret_name"
            );
            // And it must still be a legal URI component.
            assert!(
                segment
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'),
                "`{pack}` / `{unit}` -> `{segment}`"
            );
            assert!(!segment.starts_with('_') && !segment.ends_with('_'));
            assert!(!segment.contains("__"));
        }
    }

    /// A whole URI built from the segment must parse, and must round-trip
    /// through `normalize_pack_segment` (which `scoped_secret_path_for_pack`
    /// re-applies) unchanged.
    #[test]
    fn a_unit_scoped_uri_parses_and_re_normalizes_to_itself() {
        let ctx = tenant_ctx();
        for (pack, unit) in [("pack.dw.support", "web-assistant"), ("...", "a/b@c")] {
            let segment = unit_pack_segment(pack, unit).expect("segment");
            assert_eq!(normalize_pack_segment(&segment), segment);
            let uri = scoped_secret_path_for_unit(&ctx, pack, unit, "HUBSPOT/ACCESS_TOKEN")
                .expect("uri")
                .expect("some");
            assert!(
                greentic_secrets_lib::spec::SecretUri::parse(&uri).is_ok(),
                "`{uri}` is not a valid secret URI"
            );
            assert!(uri.ends_with("/hubspot_access_token"), "got {uri}");
        }
    }

    #[test]
    fn the_unit_segment_is_deterministic_and_carries_a_hash() {
        let a = unit_pack_segment("demo", "web-assistant").expect("segment");
        assert_eq!(a, unit_pack_segment("demo", "web-assistant").expect("again"));
        assert_eq!(
            a,
            unit_pack_segment("  demo  ", "  web-assistant  ").expect("trimmed")
        );
        let unit_part = a.strip_prefix("demo_unit_").expect("marker");
        let (slug, hash) = unit_part.rsplit_once('_').expect("hash suffix");
        assert_eq!(slug, "web_assistant");
        assert_eq!(hash.len(), UNIT_SEGMENT_HASH_BYTES * 2);
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
    }

    /// Pinned literals, so a change to the rule is a visible, deliberate diff:
    /// every downstream writer stages under exactly these strings.
    #[test]
    fn the_unit_segment_matches_the_pinned_values() {
        assert_eq!(
            unit_pack_segment("demo", "web-assistant").expect("segment"),
            "demo_unit_web_assistant_9456a3393b7e"
        );
        assert_eq!(
            unit_pack_segment("pack.dw.support", "Web Assistant (prod)").expect("segment"),
            "pack_dw_support_unit_web_assistant_prod_3ee04e3f306a"
        );
    }

    #[test]
    fn ids_the_slug_would_merge_still_get_distinct_segments() {
        // The hash is what separates these; the slug alone merges every pair,
        // and the last pair also proves a `_unit_` inside a pack id cannot be
        // confused with the marker.
        let pairs = [
            (("demo", "Web Assistant"), ("demo", "web-assistant")),
            (("demo", "web.assistant"), ("demo", "web_assistant")),
            (("demo", "a/b"), ("demo", "a-b")),
            (("demo", "unit_b"), ("demo_unit_b", "unit")),
            (
                ("demo", "a-very-long-deployment-name-over-the-slug-limit-one"),
                ("demo", "a-very-long-deployment-name-over-the-slug-limit-two"),
            ),
        ];
        for ((pack_l, unit_l), (pack_r, unit_r)) in pairs {
            assert_ne!(
                unit_pack_segment(pack_l, unit_l),
                unit_pack_segment(pack_r, unit_r),
                "`{pack_l}`/`{unit_l}` and `{pack_r}`/`{unit_r}` must not share a segment"
            );
        }
    }

    #[test]
    fn a_punctuation_only_pack_id_does_not_produce_a_leading_underscore() {
        let segment = unit_pack_segment("...", "unit-a").expect("segment");
        assert!(segment.starts_with("pack_"), "got {segment}");
        assert_ne!(
            unit_pack_segment("...", "unit-a"),
            unit_pack_segment("///", "unit-a"),
            "two punctuation-only pack ids must not collapse together"
        );
    }

    #[test]
    fn a_blank_pack_or_unit_id_has_no_unit_scope() {
        assert!(unit_pack_segment("demo", "").is_none());
        assert!(unit_pack_segment("demo", "   ").is_none());
        assert!(unit_pack_segment("", "unit-a").is_none());
        assert!(unit_pack_segment("   ", "unit-a").is_none());
        assert!(
            scoped_secret_path_for_unit(&tenant_ctx(), "demo", "  ", "key")
                .expect("ok")
                .is_none()
        );
    }

    #[test]
    fn candidates_put_the_unit_scope_first_and_keep_the_bare_scope() {
        let ctx = tenant_ctx();
        let with_unit =
            pack_secret_path_candidates(&ctx, "demo", Some("unit-a"), "API_KEY").expect("uris");
        assert_eq!(
            with_unit,
            vec![
                scoped_secret_path_for_unit(&ctx, "demo", "unit-a", "API_KEY")
                    .expect("ok")
                    .expect("some"),
                scoped_secret_path_for_pack(&ctx, "demo", "API_KEY").expect("bare"),
            ]
        );

        let without_unit =
            pack_secret_path_candidates(&ctx, "demo", None, "API_KEY").expect("uris");
        assert_eq!(
            without_unit,
            vec![scoped_secret_path_for_pack(&ctx, "demo", "API_KEY").expect("bare")],
            "no unit known must keep today's single candidate exactly"
        );
    }

    /// A secrets manager holding exactly the URIs it was given, recording reads.
    struct MapSecrets {
        entries: std::collections::HashMap<String, Vec<u8>>,
        seen: Mutex<Vec<String>>,
        written: Mutex<Vec<(String, Vec<u8>)>>,
    }

    impl MapSecrets {
        fn with(pairs: &[(&str, &str)]) -> Arc<Self> {
            Arc::new(Self {
                entries: pairs
                    .iter()
                    .map(|(k, v)| ((*k).to_string(), v.as_bytes().to_vec()))
                    .collect(),
                seen: Mutex::new(Vec::new()),
                written: Mutex::new(Vec::new()),
            })
        }
    }

    #[async_trait]
    impl SecretsManager for MapSecrets {
        async fn read(&self, path: &str) -> greentic_secrets_lib::Result<Vec<u8>> {
            self.seen.lock().push(path.to_string());
            self.entries
                .get(path)
                .cloned()
                .ok_or_else(|| greentic_secrets_lib::SecretError::NotFound(path.to_string()))
        }
        async fn write(&self, path: &str, bytes: &[u8]) -> greentic_secrets_lib::Result<()> {
            self.written.lock().push((path.to_string(), bytes.to_vec()));
            Ok(())
        }
        async fn delete(&self, _: &str) -> greentic_secrets_lib::Result<()> {
            Ok(())
        }
    }

    fn unit_uri(ctx: &TenantCtx, pack: &str, unit: &str, key: &str) -> String {
        scoped_secret_path_for_unit(ctx, pack, unit, key)
            .expect("ok")
            .expect("some")
    }

    #[test]
    fn the_unit_scope_wins_over_the_bare_pack_scope() {
        let ctx = tenant_ctx();
        let unit = unit_uri(&ctx, "demo", "unit-a", "API_KEY");
        let bare = scoped_secret_path_for_pack(&ctx, "demo", "API_KEY").expect("bare");
        let secrets = MapSecrets::with(&[
            (unit.as_str(), "unit-value"),
            (bare.as_str(), "shared-value"),
        ]);
        let manager: DynSecretsManager = secrets.clone();
        let got =
            read_pack_secret_blocking(&manager, &ctx, "demo", Some("unit-a"), "API_KEY").unwrap();
        assert_eq!(got, b"unit-value".to_vec());
        assert_eq!(
            secrets.seen.lock().as_slice(),
            &[unit],
            "the bare scope must not be read once the unit scope hits"
        );
    }

    #[test]
    fn a_missing_unit_scope_falls_back_to_the_bare_pack_scope() {
        let ctx = tenant_ctx();
        let unit = unit_uri(&ctx, "demo", "unit-a", "API_KEY");
        let bare = scoped_secret_path_for_pack(&ctx, "demo", "API_KEY").expect("bare");
        let secrets = MapSecrets::with(&[(bare.as_str(), "shared-value")]);
        let manager: DynSecretsManager = secrets.clone();
        let got =
            read_pack_secret_blocking(&manager, &ctx, "demo", Some("unit-a"), "API_KEY").unwrap();
        assert_eq!(got, b"shared-value".to_vec());
        assert_eq!(secrets.seen.lock().as_slice(), &[unit, bare]);
    }

    #[test]
    fn two_units_of_one_pack_resolve_their_own_values() {
        let ctx = tenant_ctx();
        let a = unit_uri(&ctx, "demo", "unit-a", "API_KEY");
        let b = unit_uri(&ctx, "demo", "unit-b", "API_KEY");
        let bare = scoped_secret_path_for_pack(&ctx, "demo", "API_KEY").expect("bare");
        assert_ne!(a, b);
        let secrets = MapSecrets::with(&[
            (a.as_str(), "token-a"),
            (b.as_str(), "token-b"),
            (bare.as_str(), "shared-value"),
        ]);
        let manager: DynSecretsManager = secrets.clone();
        for (unit, expected) in [("unit-a", "token-a"), ("unit-b", "token-b")] {
            let got =
                read_pack_secret_blocking(&manager, &ctx, "demo", Some(unit), "API_KEY").unwrap();
            assert_eq!(got, expected.as_bytes().to_vec(), "unit {unit}");
        }
    }

    #[test]
    fn no_unit_reads_exactly_todays_address() {
        let ctx = tenant_ctx();
        let bare = scoped_secret_path_for_pack(&ctx, "demo", "API_KEY").expect("bare");
        let secrets = MapSecrets::with(&[(bare.as_str(), "shared-value")]);
        let manager: DynSecretsManager = secrets.clone();
        let got = read_pack_secret_blocking(&manager, &ctx, "demo", None, "API_KEY").unwrap();
        assert_eq!(got, b"shared-value".to_vec());
        assert_eq!(secrets.seen.lock().as_slice(), &[bare]);
    }

    #[test]
    fn a_total_miss_names_every_uri_tried() {
        let ctx = tenant_ctx();
        let secrets = MapSecrets::with(&[]);
        let manager: DynSecretsManager = secrets.clone();
        let err = read_pack_secret_blocking(&manager, &ctx, "demo", Some("unit-a"), "API_KEY")
            .expect_err("no value anywhere");
        let rendered = err.to_string();
        assert!(
            rendered.contains(&unit_uri(&ctx, "demo", "unit-a", "API_KEY"))
                && rendered.contains(
                    &scoped_secret_path_for_pack(&ctx, "demo", "API_KEY").expect("bare")
                ),
            "got: {rendered}"
        );
    }

    #[test]
    fn a_write_lands_at_the_unit_address_only() {
        let ctx = tenant_ctx();
        let secrets = MapSecrets::with(&[]);
        let manager: DynSecretsManager = secrets.clone();
        write_pack_secret_blocking(&manager, &ctx, "demo", Some("unit-a"), "API_KEY", b"fresh")
            .unwrap();
        assert_eq!(
            secrets.written.lock().as_slice(),
            &[(
                unit_uri(&ctx, "demo", "unit-a", "API_KEY"),
                b"fresh".to_vec()
            )],
            "a unit write must never touch the address other units read"
        );
    }

    #[test]
    fn a_write_with_no_unit_lands_at_the_bare_address() {
        let ctx = tenant_ctx();
        let secrets = MapSecrets::with(&[]);
        let manager: DynSecretsManager = secrets.clone();
        write_pack_secret_blocking(&manager, &ctx, "demo", None, "API_KEY", b"fresh").unwrap();
        assert_eq!(
            secrets.written.lock().as_slice(),
            &[(
                scoped_secret_path_for_pack(&ctx, "demo", "API_KEY").expect("bare"),
                b"fresh".to_vec()
            )]
        );
    }

    #[test]
    fn from_env_parses_broker() {
        // Test purely via the internal helper — avoids unsafe env mutation
        // (crate uses #![deny(unsafe_code)]).
        let b = SecretsBackend::broker_from_strings("broker", "http://localhost:9", "").unwrap();
        assert!(matches!(b, SecretsBackend::Broker { .. }));
    }

    #[test]
    fn from_env_broker_rejects_empty_endpoint() {
        let err = SecretsBackend::broker_from_strings("broker", "", "").unwrap_err();
        assert!(
            err.to_string().contains("SECRETS_BROKER_ENDPOINT"),
            "error was: {err}"
        );
    }

    #[test]
    fn from_config_parses_broker() {
        let b = SecretsBackend::from_config(&SecretsBackendRefConfig {
            kind: "broker".into(),
            reference: Some("http://localhost:9".into()),
        })
        .unwrap();
        assert!(matches!(b, SecretsBackend::Broker { .. }));
    }

    #[test]
    fn backend_parsers_reject_unknown_kinds() {
        let err =
            SecretsBackend::from_env(Some("vault".into())).expect_err("backend should be rejected");
        assert!(err.to_string().contains("unsupported SECRETS_BACKEND"));

        let err = SecretsBackend::from_config(&SecretsBackendRefConfig {
            kind: "vault".into(),
            reference: None,
        })
        .expect_err("backend config should be rejected");
        assert!(err.to_string().contains("unsupported secrets backend"));
    }

    #[test]
    fn backend_parsers_accept_default_aliases() {
        assert!(matches!(
            SecretsBackend::from_env(Some("".into())).unwrap(),
            SecretsBackend::Env
        ));
        assert!(matches!(
            SecretsBackend::from_config(&SecretsBackendRefConfig {
                kind: "none".into(),
                reference: None,
            })
            .unwrap(),
            SecretsBackend::Env
        ));
    }

    // -----------------------------------------------------------------------
    // CachingSecretsManager tests
    // -----------------------------------------------------------------------

    /// In-memory mock that counts how many times `read` is called.
    struct CountingManager {
        read_count: AtomicUsize,
    }

    impl CountingManager {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                read_count: AtomicUsize::new(0),
            })
        }

        fn reads(&self) -> usize {
            self.read_count.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl SecretsManager for CountingManager {
        async fn read(&self, _path: &str) -> greentic_secrets_lib::Result<Vec<u8>> {
            self.read_count.fetch_add(1, Ordering::SeqCst);
            Ok(b"secret-value".to_vec())
        }

        async fn write(&self, _path: &str, _bytes: &[u8]) -> greentic_secrets_lib::Result<()> {
            Ok(())
        }

        async fn delete(&self, _path: &str) -> greentic_secrets_lib::Result<()> {
            Ok(())
        }
    }

    fn make_cached(inner: Arc<CountingManager>) -> CachingSecretsManager {
        CachingSecretsManager {
            inner: inner as DynSecretsManager,
            cache: Mutex::new(LruCache::new(NonZeroUsize::new(64).unwrap())),
            ttl: Duration::from_secs(300),
        }
    }

    #[tokio::test]
    async fn cached_read_avoids_backend_on_second_call() {
        let inner = CountingManager::new();
        let cached = make_cached(Arc::clone(&inner));

        let v1 = cached.read("secrets://dev/t/team/pack/key").await.unwrap();
        let v2 = cached.read("secrets://dev/t/team/pack/key").await.unwrap();
        assert_eq!(v1, v2);
        assert_eq!(inner.reads(), 1, "second read should hit cache");
    }

    #[tokio::test]
    async fn write_invalidates_cache() {
        let inner = CountingManager::new();
        let cached = make_cached(Arc::clone(&inner));

        cached.read("secrets://dev/t/team/pack/key").await.unwrap();
        assert_eq!(inner.reads(), 1);

        cached
            .write("secrets://dev/t/team/pack/key", b"new")
            .await
            .unwrap();
        cached.read("secrets://dev/t/team/pack/key").await.unwrap();
        assert_eq!(inner.reads(), 2, "read after write should bypass cache");
    }

    #[tokio::test]
    async fn delete_invalidates_cache() {
        let inner = CountingManager::new();
        let cached = make_cached(Arc::clone(&inner));

        cached.read("secrets://dev/t/team/pack/key").await.unwrap();
        cached
            .delete("secrets://dev/t/team/pack/key")
            .await
            .unwrap();
        cached.read("secrets://dev/t/team/pack/key").await.unwrap();
        assert_eq!(inner.reads(), 2, "read after delete should bypass cache");
    }

    #[tokio::test]
    async fn expired_entry_triggers_backend_read() {
        let inner = CountingManager::new();
        let cached = CachingSecretsManager {
            inner: Arc::clone(&inner) as DynSecretsManager,
            cache: Mutex::new(LruCache::new(NonZeroUsize::new(64).unwrap())),
            ttl: Duration::from_millis(1), // expires almost immediately
        };

        cached.read("secrets://dev/t/team/pack/key").await.unwrap();
        std::thread::sleep(Duration::from_millis(5));
        cached.read("secrets://dev/t/team/pack/key").await.unwrap();
        assert_eq!(inner.reads(), 2, "expired entry should re-fetch");
    }

    #[test]
    fn wrap_with_zero_ttl_returns_inner_directly() {
        let inner: DynSecretsManager = CountingManager::new();
        let ptr_before = Arc::as_ptr(&inner);
        let wrapped = CachingSecretsManager::wrap_with(
            Arc::clone(&inner),
            Duration::ZERO,
            NonZeroUsize::new(64).unwrap(),
        );
        let ptr_after = Arc::as_ptr(&wrapped);
        // When TTL is 0, wrap should return the same Arc (no caching layer).
        assert_eq!(ptr_before, ptr_after);
    }
}
