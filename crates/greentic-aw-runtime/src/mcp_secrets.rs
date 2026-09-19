//! Adapts the runner's `SecretsManager` to the `SecretsStore` trait
//! `greentic-mcp-exec` expects, so a `local-wasm` MCP component's
//! `secret_get` resolves instead of returning `secrets-unavailable`.
//!
//! The URI shape is dictated by greentic-designer-admin, which writes these
//! secrets: `secrets://<env>/<tenant>/<team>/mcp/<name>`
//! (parity source: `greentic-designer-admin/src/secrets/scope.rs`
//! `SecretScope::uri`). That `mcp`-category writer emits the name and team
//! **verbatim** — it does NOT canonicalize. This reader matches it exactly:
//! the key and team pass through unchanged, and an absent team becomes `_`
//! (`team.unwrap_or("_")`). Canonicalizing here would read a different URI
//! than admin wrote — and admin already keys the http-transport auth token by
//! a hyphenated UUID that lowercasing/`_`-substitution would corrupt. Only
//! admin's separate *pack* path canonicalizes; the `mcp` category does not.

use std::sync::Arc;

use greentic_mcp_exec::SecretsStore;
use greentic_secrets_lib::SecretsManager;
use greentic_types::TenantCtx;

/// Category segment admin uses for MCP secrets.
const MCP_CATEGORY: &str = "mcp";

/// Env segment for MCP secrets. Admin pins ALL MCP secrets to `default`
/// regardless of the tenant's flow env — both when sealing (`mcp_scope`) and
/// resolving (`ResolveCtx`) in greentic-designer-admin. The reader must match,
/// or it would look under the flow env (`prod`/`local`) and silently miss
/// admin's write.
const MCP_ENV_SEGMENT: &str = "default";

/// Team segment admin writes for a secret that is not scoped to a team.
pub const MCP_TENANT_DEFAULT_TEAM: &str = "_";

/// Build the secret URI for an MCP key, byte-for-byte compatible with admin's
/// `SecretScope::uri` for the `mcp` category: env pinned to `default`, key and
/// team emitted verbatim, absent team becomes `_`. An empty key is rejected as
/// a caller error rather than producing a trailing-slash URI.
///
/// THE one builder for this shape. `greentic-runner-host`'s flow MCP node had a
/// byte-for-byte second copy (`runner::mcp_node::aw::pack_route_secret_uri`),
/// whose own doc comment said so; two copies is how the flow path and the agent
/// path start resolving different URIs for the same server with nothing
/// failing. Both now call this.
pub fn mcp_secret_uri(tenant: &str, team: Option<&str>, key: &str) -> Result<String, String> {
    if key.trim().is_empty() {
        return Err("secret key must not be empty".to_string());
    }
    Ok(format!(
        "secrets://{}/{}/{}/{}/{}",
        MCP_ENV_SEGMENT,
        tenant,
        team.unwrap_or(MCP_TENANT_DEFAULT_TEAM),
        MCP_CATEGORY,
        key
    ))
}

/// [`mcp_secret_uri`] with the tenant and team read off a [`TenantCtx`].
pub(crate) fn mcp_secret_uri_for_ctx(ctx: &TenantCtx, key: &str) -> Result<String, String> {
    let team = ctx
        .team_id
        .as_ref()
        .or(ctx.team.as_ref())
        .map(|value| value.as_str());
    mcp_secret_uri(ctx.tenant.as_str(), team, key)
}

/// Marker between the server id and the unit segment in a unit-scoped MCP
/// secret NAME: `<server_id>.unit-<unit segment>`.
///
/// The unit rides in the NAME, not in a sixth URI segment, because
/// `greentic-secrets`' `SecretUri::parse` accepts exactly five segments
/// (`env/tenant/team/category/name`) and rejects `ExtraSegments`. Keeping the
/// category `mcp` is what keeps every existing `mcp` carve-out applying to it
/// unchanged: greentic-deployer's `dev_store_key` / `validate_dev_store_secret_path`
/// and greentic-start's dev-store canonicalizer both key the exemption on the
/// CATEGORY segment, so the name is stored and read verbatim.
pub const MCP_UNIT_KEY_MARKER: &str = ".unit-";

/// Longest slug kept from a unit id before the hash suffix. Bounds the secret
/// name on a unit whose deployment name is long.
const MCP_UNIT_SLUG_MAX: usize = 40;

/// Bytes of `sha256(unit_id)` rendered as the hash suffix (12 hex chars).
const MCP_UNIT_HASH_BYTES: usize = 6;

/// Normalise a deployed unit's id (a revision's `bundle_id`, which
/// greentic-deploy-spec types as an OPAQUE string) into a segment that is
/// valid inside a `secrets://` name, deterministically and collision-resistantly.
///
/// THE one normalisation — the designer (which stages the value) must produce
/// exactly this string, so it should call this function rather than copy it.
///
/// Rule, applied to `unit_id.trim()`:
///
/// 1. **slug**: every ASCII alphanumeric is lowercased; every other char
///    (including `.`, `_`, `/`, whitespace and non-ASCII) becomes `-`; runs of
///    `-` collapse to one; leading/trailing `-` are trimmed; the result is cut
///    to [`MCP_UNIT_SLUG_MAX`] bytes and trailing `-` trimmed again.
/// 2. **hash**: the first [`MCP_UNIT_HASH_BYTES`] bytes of `sha256` of the
///    trimmed id's UTF-8 bytes, lowercase hex.
/// 3. `"<slug>-<hash>"`, or just `"<hash>"` when the slug is empty.
///
/// The hash is ALWAYS appended, so two ids the slug alone would merge
/// (`Web Assistant` / `web-assistant`, or two ids sharing a 40-byte prefix)
/// still get different segments. The output uses only `[a-z0-9-]`, a subset of
/// what `SecretUri` accepts, and never contains `.`, so the
/// [`MCP_UNIT_KEY_MARKER`] stays unambiguous in the name.
///
/// Returns `None` for an empty/blank id: there is no unit scope then, and the
/// caller must fall through to the team / `_` scopes.
pub fn mcp_unit_segment(unit_id: &str) -> Option<String> {
    use sha2::{Digest, Sha256};

    let raw = unit_id.trim();
    if raw.is_empty() {
        return None;
    }

    let mut slug = String::with_capacity(raw.len());
    for ch in raw.chars() {
        let mapped = if ch.is_ascii_alphanumeric() {
            ch.to_ascii_lowercase()
        } else {
            '-'
        };
        if mapped == '-' && (slug.is_empty() || slug.ends_with('-')) {
            continue;
        }
        slug.push(mapped);
    }
    // ASCII-only by construction, so a byte cut never splits a char.
    slug.truncate(MCP_UNIT_SLUG_MAX);
    let slug = slug.trim_end_matches('-');

    let digest = Sha256::digest(raw.as_bytes());
    let hash: String = digest[..MCP_UNIT_HASH_BYTES]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();

    Some(if slug.is_empty() {
        hash
    } else {
        format!("{slug}-{hash}")
    })
}

/// The secret NAME a unit-scoped MCP credential is stored under:
/// `<server_id>.unit-<mcp_unit_segment(unit_id)>`.
///
/// The server id passes through verbatim, exactly as in the unscoped name (see
/// the module docs — admin keys it by a hyphenated UUID). `None` when either
/// the server id or the unit id is blank.
///
/// Full URI: `mcp_secret_uri(tenant, team, &mcp_unit_secret_key(server, unit)?)`,
/// i.e. `secrets://default/<tenant>/<team|_>/mcp/<server_id>.unit-<segment>`.
/// The team segment is the SAME one the team-scoped candidate uses (the
/// route's `auth_team`, else `_`), so a writer that already stages
/// `default/<team>/mcp/<server_id>` only changes the name.
pub fn mcp_unit_secret_key(server_id: &str, unit_id: &str) -> Option<String> {
    if server_id.trim().is_empty() {
        return None;
    }
    let segment = mcp_unit_segment(unit_id)?;
    Some(format!("{server_id}{MCP_UNIT_KEY_MARKER}{segment}"))
}

/// Every credential URI [`read_mcp_secret_for_unit`] will try, in order:
///
/// 1. the UNIT scope — `…/<team|_>/mcp/<key>.unit-<segment>` — when a unit is
///    known;
/// 2. the caller's team scope;
/// 3. the tenant-default `_` scope.
///
/// 2 and 3 mirror greentic-designer-admin's own `AdminSecretResolver`
/// precedence, so a deployed pack resolves the same value the composer resolved
/// when the author bound the tool. A caller with no team, or one already naming
/// `_`, yields a single tenant-default URI for them.
///
/// REVIEW BY 2026-12-18: when a unit is known, candidates 2 and 3 are a
/// COMPATIBILITY fallback, not a design choice. They exist so a deployment
/// staged before the unit scope existed keeps its credential when its runner
/// moves; the next deploy writes the unit-scoped key. Once every lane stages
/// unit-scoped keys (greentic-designer `per-unit-setup-isolation` Phase 2), the
/// fallback lets one unit silently read a value another unit's writer left at
/// the shared address — decide then whether to drop it for the unit case.
/// (Candidates 2 and 3 remain the ONLY candidates when no unit is known — the
/// legacy tenant-only runtime and the process-level serve path.)
fn mcp_secret_uri_candidates(
    tenant: &str,
    team: Option<&str>,
    unit: Option<&str>,
    key: &str,
) -> Result<Vec<String>, String> {
    let team = team.filter(|t| !t.is_empty() && *t != MCP_TENANT_DEFAULT_TEAM);
    let mut uris = Vec::with_capacity(3);
    if let Some(unit_key) = unit.and_then(|unit| mcp_unit_secret_key(key, unit)) {
        uris.push(mcp_secret_uri(tenant, team, &unit_key)?);
    }
    if let Some(team) = team {
        uris.push(mcp_secret_uri(tenant, Some(team), key)?);
    }
    uris.push(mcp_secret_uri(tenant, None, key)?);
    Ok(uris)
}

/// No credential was readable at any scope [`read_mcp_secret`] tried.
///
/// Carries the URIs rather than an opaque `NotFound` because on this path the
/// two are indistinguishable to an operator: a missing secret and an
/// unregistered server look identical, and the most common cause is a backend
/// that cannot resolve a `secrets://` URI at all.
#[derive(Debug, Clone)]
pub struct McpSecretMiss {
    pub uris: Vec<String>,
    pub error: String,
}

impl std::fmt::Display for McpSecretMiss {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "no credential at {} ({}). Note that MCP requires SECRETS_BACKEND=broker; \
             the env backend cannot resolve a secrets:// URI.",
            self.uris.join(" or "),
            self.error
        )
    }
}

/// Read an MCP secret with no unit scope: the caller's team scope, then the
/// tenant-default `_` scope. Equivalent to [`read_mcp_secret_for_unit`] with
/// `unit = None`; kept for callers that have no deployed unit (the
/// process-level serve path, the legacy tenant-only runtime).
///
/// The team half is what makes a team-scoped MCP server's token reachable at
/// run time at all: admin seals the token at the server ROW's team scope, and
/// the deployed runtime carries no team of its own — so every lane resolved
/// `_` only and a team-scoped server silently had no credential.
pub async fn read_mcp_secret(
    secrets: &dyn SecretsManager,
    tenant: &str,
    team: Option<&str>,
    key: &str,
) -> Result<Vec<u8>, McpSecretMiss> {
    read_mcp_secret_for_unit(secrets, tenant, team, None, key).await
}

/// Read an MCP secret for one deployed unit: the unit scope first, then the
/// team scope, then the tenant-default `_` scope (see
/// [`mcp_secret_uri_candidates`]). First hit wins.
///
/// `unit` is the running revision's `bundle_id` — the deployment name,
/// unique within an environment — taken from the runtime's exec context, never
/// from a process environment variable: one greentic-start process serves many
/// revisions locally, so an env var cannot differ per unit.
///
/// There is no case where a previously-resolving token stops resolving: the
/// team and `_` scopes are still tried, and only after the unit scope misses.
pub async fn read_mcp_secret_for_unit(
    secrets: &dyn SecretsManager,
    tenant: &str,
    team: Option<&str>,
    unit: Option<&str>,
    key: &str,
) -> Result<Vec<u8>, McpSecretMiss> {
    let uris =
        mcp_secret_uri_candidates(tenant, team, unit, key).map_err(|error| McpSecretMiss {
            uris: Vec::new(),
            error,
        })?;
    let mut last_error = String::from("no scope tried");
    for uri in &uris {
        match secrets.read(uri).await {
            Ok(bytes) => return Ok(bytes),
            Err(e) => last_error = e.to_string(),
        }
    }
    Err(McpSecretMiss {
        uris,
        error: last_error,
    })
}

/// `SecretsStore` backed by the runner's secrets manager.
pub struct McpSecretsStore {
    secrets: Arc<dyn SecretsManager>,
}

impl McpSecretsStore {
    pub fn new(secrets: Arc<dyn SecretsManager>) -> Self {
        Self { secrets }
    }
}

impl SecretsStore for McpSecretsStore {
    fn read(&self, scope: &TenantCtx, name: &str) -> Result<Vec<u8>, String> {
        let uri = mcp_secret_uri_for_ctx(scope, name)?;
        // `SecretsManager` is async; the mcp-exec host trait is sync and is
        // already invoked from inside `spawn_blocking`, so a current-thread
        // block here does not stall the async runtime.
        futures::executor::block_on(self.secrets.read(&uri)).map_err(|e| e.to_string())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use greentic_types::{EnvId, TenantId};
    use std::sync::Mutex;

    struct FakeSecrets {
        seen: Mutex<Vec<String>>,
        value: Vec<u8>,
    }

    #[async_trait]
    impl SecretsManager for FakeSecrets {
        async fn read(&self, path: &str) -> greentic_secrets_lib::Result<Vec<u8>> {
            self.seen.lock().unwrap().push(path.to_string());
            Ok(self.value.clone())
        }
        async fn write(&self, _: &str, _: &[u8]) -> greentic_secrets_lib::Result<()> {
            Ok(())
        }
        async fn delete(&self, _: &str) -> greentic_secrets_lib::Result<()> {
            Ok(())
        }
    }

    fn ctx() -> TenantCtx {
        TenantCtx::new(EnvId::new("prod").unwrap(), TenantId::new("acme").unwrap())
    }

    #[test]
    fn uri_matches_admin_shape_without_team() {
        // env is pinned to `default` (admin's convention), NOT the flow env, and
        // the key is raw — exactly as admin's `SecretScope::uri` (mcp category)
        // writes it.
        assert_eq!(
            mcp_secret_uri_for_ctx(&ctx(), "EXAMPLE_KEY").unwrap(),
            "secrets://default/acme/_/mcp/EXAMPLE_KEY"
        );
    }

    #[test]
    fn uri_pins_env_to_default_ignoring_flow_env() {
        // `ctx()` has env `prod`, but admin seals MCP secrets at env `default`
        // regardless of the tenant's flow env. Teeth guard against reading at
        // `ctx.env`, which would silently miss admin's write.
        let uri = mcp_secret_uri_for_ctx(&ctx(), "K").unwrap();
        assert!(
            uri.starts_with("secrets://default/"),
            "env must be pinned to `default` to match admin, got: {uri}"
        );
    }

    #[test]
    fn uri_preserves_key_case_and_punctuation_verbatim() {
        // The admin `mcp` category writer does NOT canonicalize; canonicalizing
        // here would read a different URI than admin wrote. This is the teeth
        // guard against reintroducing lowercase/`_`-substitution.
        assert_eq!(
            mcp_secret_uri_for_ctx(&ctx(), "Petstore-API.key").unwrap(),
            "secrets://default/acme/_/mcp/Petstore-API.key"
        );
    }

    #[test]
    fn uri_emits_team_segment_verbatim_matching_admin() {
        // Admin uses `team.unwrap_or("_")` — a raw passthrough, no empty/default
        // folding. A team is emitted as-is.
        let mut with_team = ctx();
        with_team.team_id = Some(greentic_types::TeamId::new("Sales").unwrap());
        assert_eq!(
            mcp_secret_uri_for_ctx(&with_team, "K").unwrap(),
            "secrets://default/acme/Sales/mcp/K"
        );
    }

    #[test]
    fn uri_rejects_empty_key() {
        assert!(mcp_secret_uri_for_ctx(&ctx(), "   ").is_err());
    }

    #[test]
    fn read_uses_the_scoped_uri_and_returns_bytes() {
        let fake = Arc::new(FakeSecrets {
            seen: Mutex::new(Vec::new()),
            value: b"sk-live".to_vec(),
        });
        let store = McpSecretsStore::new(fake.clone());
        let got = store.read(&ctx(), "EXAMPLE_KEY").unwrap();
        assert_eq!(got, b"sk-live".to_vec());
        assert_eq!(
            fake.seen.lock().unwrap().as_slice(),
            &["secrets://default/acme/_/mcp/EXAMPLE_KEY".to_string()]
        );
    }

    /// A secrets manager holding exactly the URIs it was given.
    struct MapSecrets {
        entries: std::collections::HashMap<String, Vec<u8>>,
        seen: Mutex<Vec<String>>,
    }

    impl MapSecrets {
        fn with(pairs: &[(&str, &str)]) -> Arc<Self> {
            Arc::new(Self {
                entries: pairs
                    .iter()
                    .map(|(k, v)| ((*k).to_string(), v.as_bytes().to_vec()))
                    .collect(),
                seen: Mutex::new(Vec::new()),
            })
        }
    }

    #[async_trait]
    impl SecretsManager for MapSecrets {
        async fn read(&self, path: &str) -> greentic_secrets_lib::Result<Vec<u8>> {
            self.seen.lock().unwrap().push(path.to_string());
            self.entries
                .get(path)
                .cloned()
                .ok_or_else(|| greentic_secrets_lib::SecretError::NotFound(path.to_string()))
        }
        async fn write(&self, _: &str, _: &[u8]) -> greentic_secrets_lib::Result<()> {
            Ok(())
        }
        async fn delete(&self, _: &str) -> greentic_secrets_lib::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn read_prefers_the_team_scope_when_it_holds_the_token() {
        let secrets = MapSecrets::with(&[
            ("secrets://default/acme/sales/mcp/srv-1", "team-token"),
            ("secrets://default/acme/_/mcp/srv-1", "tenant-token"),
        ]);
        let got = read_mcp_secret(secrets.as_ref(), "acme", Some("sales"), "srv-1")
            .await
            .unwrap();
        assert_eq!(got, b"team-token".to_vec());
        assert_eq!(
            secrets.seen.lock().unwrap().as_slice(),
            &["secrets://default/acme/sales/mcp/srv-1".to_string()],
            "the tenant-default scope must not be read once the team scope hits"
        );
    }

    #[tokio::test]
    async fn read_falls_back_to_the_tenant_default_scope() {
        // The pre-existing shape: a tenant-default server row. Naming a team
        // must never make a token that resolved before stop resolving.
        let secrets = MapSecrets::with(&[("secrets://default/acme/_/mcp/srv-1", "tenant-token")]);
        let got = read_mcp_secret(secrets.as_ref(), "acme", Some("sales"), "srv-1")
            .await
            .unwrap();
        assert_eq!(got, b"tenant-token".to_vec());
        assert_eq!(
            secrets.seen.lock().unwrap().as_slice(),
            &[
                "secrets://default/acme/sales/mcp/srv-1".to_string(),
                "secrets://default/acme/_/mcp/srv-1".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn read_with_no_team_tries_only_the_tenant_default_scope() {
        let secrets = MapSecrets::with(&[("secrets://default/acme/_/mcp/srv-1", "tenant-token")]);
        assert!(
            read_mcp_secret(secrets.as_ref(), "acme", None, "srv-1")
                .await
                .is_ok()
        );
        assert_eq!(secrets.seen.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn read_does_not_try_the_underscore_scope_twice() {
        // `_` IS the tenant-default segment; a record naming it explicitly must
        // not produce a duplicate lookup.
        let secrets = MapSecrets::with(&[]);
        let miss = read_mcp_secret(secrets.as_ref(), "acme", Some("_"), "srv-1")
            .await
            .unwrap_err();
        assert_eq!(miss.uris, vec!["secrets://default/acme/_/mcp/srv-1"]);
    }

    #[tokio::test]
    async fn miss_names_every_uri_tried_and_the_broker_requirement() {
        let secrets = MapSecrets::with(&[]);
        let miss = read_mcp_secret(secrets.as_ref(), "acme", Some("sales"), "srv-1")
            .await
            .unwrap_err();
        let rendered = miss.to_string();
        assert!(
            rendered.contains("secrets://default/acme/sales/mcp/srv-1")
                && rendered.contains("secrets://default/acme/_/mcp/srv-1"),
            "got: {rendered}"
        );
        assert!(
            rendered.contains("SECRETS_BACKEND=broker"),
            "the operator needs the cause, not an opaque NotFound; got: {rendered}"
        );
    }

    // ── unit scope ──────────────────────────────────────────────────────────

    #[test]
    fn unit_segment_is_deterministic_and_carries_a_hash() {
        let a = mcp_unit_segment("web-assistant").unwrap();
        assert_eq!(a, mcp_unit_segment("web-assistant").unwrap());
        assert_eq!(a, mcp_unit_segment("  web-assistant  ").unwrap());
        let (slug, hash) = a.rsplit_once('-').unwrap();
        assert_eq!(slug, "web-assistant");
        assert_eq!(hash.len(), MCP_UNIT_HASH_BYTES * 2);
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
    }

    /// Pinned literals, so a change to the rule is a visible, deliberate diff:
    /// the designer stages under exactly these strings. Hashes are
    /// `sha256(<trimmed id>)[..6]` in lowercase hex.
    #[test]
    fn unit_segment_matches_the_pinned_values() {
        assert_eq!(
            mcp_unit_segment("web-assistant").unwrap(),
            "web-assistant-9456a3393b7e"
        );
        assert_eq!(
            mcp_unit_segment("Web Assistant (prod)").unwrap(),
            "web-assistant-prod-3ee04e3f306a"
        );
        assert_eq!(
            mcp_unit_secret_key("srv-1", "web-assistant").unwrap(),
            "srv-1.unit-web-assistant-9456a3393b7e"
        );
    }

    #[test]
    fn ids_the_slug_would_merge_still_get_distinct_segments() {
        let pairs = [
            ("Web Assistant", "web-assistant"),
            ("web.assistant", "web_assistant"),
            ("a/b", "a-b"),
            (
                "a-very-long-deployment-name-that-exceeds-the-slug-limit-one",
                "a-very-long-deployment-name-that-exceeds-the-slug-limit-two",
            ),
        ];
        for (left, right) in pairs {
            assert_ne!(
                mcp_unit_segment(left),
                mcp_unit_segment(right),
                "`{left}` and `{right}` must not share a unit scope"
            );
        }
    }

    #[test]
    fn awkward_unit_ids_produce_secret_uri_valid_names() {
        let awkward = [
            "Web Assistant (prod)",
            "unit_01JTKS.v2",
            "ÜNÏCÖDÉ-Ünit",
            "---",
            "a/b/c@d",
            "   x   ",
            "01JTKSZ8Q9M3N4P5R6S7T8V9W0",
            "a-very-long-deployment-name-that-exceeds-the-slug-limit-by-a-lot-of-bytes",
        ];
        for id in awkward {
            let segment = mcp_unit_segment(id).unwrap();
            assert!(
                segment
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'),
                "`{id}` -> `{segment}`"
            );
            assert!(!segment.starts_with('-') && !segment.ends_with('-'));
            assert!(segment.len() <= MCP_UNIT_SLUG_MAX + 1 + MCP_UNIT_HASH_BYTES * 2);

            let key = mcp_unit_secret_key("ff308b9c-951a-4c1e-9b1d-0a6c2e3f4a5b", id).unwrap();
            let uri = mcp_secret_uri("acme", Some("sales"), &key).unwrap();
            greentic_secrets_lib::spec::SecretUri::parse(&uri)
                .unwrap_or_else(|e| panic!("`{id}` produced an invalid URI `{uri}`: {e}"));
        }
    }

    #[test]
    fn blank_ids_have_no_unit_scope() {
        assert!(mcp_unit_segment("").is_none());
        assert!(mcp_unit_segment("   ").is_none());
        assert!(mcp_unit_secret_key("srv-1", " ").is_none());
        assert!(mcp_unit_secret_key(" ", "unit-a").is_none());
    }

    #[test]
    fn unit_key_keeps_the_server_id_verbatim() {
        let key = mcp_unit_secret_key("Srv-1", "unit-a").unwrap();
        assert!(key.starts_with("Srv-1.unit-unit-a-"), "got {key}");
    }

    #[tokio::test]
    async fn the_unit_scope_wins_over_team_and_tenant_default() {
        let unit_key = mcp_unit_secret_key("srv-1", "unit-a").unwrap();
        let unit_uri = format!("secrets://default/acme/sales/mcp/{unit_key}");
        let secrets = MapSecrets::with(&[
            (unit_uri.as_str(), "unit-token"),
            ("secrets://default/acme/sales/mcp/srv-1", "team-token"),
            ("secrets://default/acme/_/mcp/srv-1", "tenant-token"),
        ]);
        let got = read_mcp_secret_for_unit(
            secrets.as_ref(),
            "acme",
            Some("sales"),
            Some("unit-a"),
            "srv-1",
        )
        .await
        .unwrap();
        assert_eq!(got, b"unit-token".to_vec());
        assert_eq!(secrets.seen.lock().unwrap().as_slice(), &[unit_uri]);
    }

    #[tokio::test]
    async fn a_missing_unit_scope_falls_back_to_team_then_tenant_default() {
        let unit_key = mcp_unit_secret_key("srv-1", "unit-a").unwrap();

        let team_only = MapSecrets::with(&[
            ("secrets://default/acme/sales/mcp/srv-1", "team-token"),
            ("secrets://default/acme/_/mcp/srv-1", "tenant-token"),
        ]);
        let got = read_mcp_secret_for_unit(
            team_only.as_ref(),
            "acme",
            Some("sales"),
            Some("unit-a"),
            "srv-1",
        )
        .await
        .unwrap();
        assert_eq!(got, b"team-token".to_vec());

        let tenant_only =
            MapSecrets::with(&[("secrets://default/acme/_/mcp/srv-1", "tenant-token")]);
        let got = read_mcp_secret_for_unit(
            tenant_only.as_ref(),
            "acme",
            Some("sales"),
            Some("unit-a"),
            "srv-1",
        )
        .await
        .unwrap();
        assert_eq!(got, b"tenant-token".to_vec());
        assert_eq!(
            tenant_only.seen.lock().unwrap().as_slice(),
            &[
                format!("secrets://default/acme/sales/mcp/{unit_key}"),
                "secrets://default/acme/sales/mcp/srv-1".to_string(),
                "secrets://default/acme/_/mcp/srv-1".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn with_no_team_the_unit_scope_sits_at_the_underscore_team() {
        let unit_key = mcp_unit_secret_key("srv-1", "unit-a").unwrap();
        let secrets = MapSecrets::with(&[]);
        let miss =
            read_mcp_secret_for_unit(secrets.as_ref(), "acme", None, Some("unit-a"), "srv-1")
                .await
                .unwrap_err();
        assert_eq!(
            miss.uris,
            vec![
                format!("secrets://default/acme/_/mcp/{unit_key}"),
                "secrets://default/acme/_/mcp/srv-1".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn two_units_with_one_server_id_resolve_their_own_values() {
        let key_a = mcp_unit_secret_key("srv-1", "unit-a").unwrap();
        let key_b = mcp_unit_secret_key("srv-1", "unit-b").unwrap();
        let uri_a = format!("secrets://default/acme/_/mcp/{key_a}");
        let uri_b = format!("secrets://default/acme/_/mcp/{key_b}");
        let secrets = MapSecrets::with(&[
            (uri_a.as_str(), "token-a"),
            (uri_b.as_str(), "token-b"),
            ("secrets://default/acme/_/mcp/srv-1", "shared-token"),
        ]);
        for (unit, expected) in [("unit-a", "token-a"), ("unit-b", "token-b")] {
            let got = read_mcp_secret_for_unit(secrets.as_ref(), "acme", None, Some(unit), "srv-1")
                .await
                .unwrap();
            assert_eq!(got, expected.as_bytes().to_vec(), "unit {unit}");
        }
    }

    #[tokio::test]
    async fn no_unit_keeps_the_pre_existing_candidates_exactly() {
        let secrets = MapSecrets::with(&[]);
        let with_none =
            read_mcp_secret_for_unit(secrets.as_ref(), "acme", Some("sales"), None, "srv-1")
                .await
                .unwrap_err();
        let legacy = read_mcp_secret(secrets.as_ref(), "acme", Some("sales"), "srv-1")
            .await
            .unwrap_err();
        assert_eq!(with_none.uris, legacy.uris);
        assert_eq!(
            legacy.uris,
            vec![
                "secrets://default/acme/sales/mcp/srv-1".to_string(),
                "secrets://default/acme/_/mcp/srv-1".to_string(),
            ]
        );
    }
}
