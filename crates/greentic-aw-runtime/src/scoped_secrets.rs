//! Category-parameterised `secrets://` URIs for the credentials an agent tool
//! resolves at run time.
//!
//! The shape is greentic-designer-admin's `SecretScope::uri`:
//! `secrets://default/<tenant>/<team|_>/<category>/<name>`. The env is pinned
//! to `default`, the team and name are emitted VERBATIM (never canonicalised),
//! and an absent team is written `_`.
//!
//! This began as `mcp_secrets`' private builder with the category hard-coded.
//! A2A (category `a2a`, cross-repo contract 2026-09-21 §5) needs the identical
//! candidate order, so the category is a parameter here. `mcp_secrets`
//! delegates, so there is no second copy that could start resolving a
//! different URI with nothing failing.

use greentic_secrets_lib::SecretsManager;

/// Env segment every category is sealed under. Admin pins these secrets to
/// `default` regardless of the tenant's flow env. Reading at the flow env
/// (`prod`/`local`) would silently miss admin's write.
pub const ENV_SEGMENT: &str = "default";

/// Team segment for a secret that is not scoped to a team.
pub const TENANT_DEFAULT_TEAM: &str = "_";

/// Build `secrets://default/<tenant>/<team|_>/<category>/<key>`.
///
/// An empty category or key is rejected as a caller error rather than
/// producing a URI with an empty segment.
pub fn secret_uri(
    category: &str,
    tenant: &str,
    team: Option<&str>,
    key: &str,
) -> Result<String, String> {
    if category.trim().is_empty() {
        return Err("secret category must not be empty".to_string());
    }
    if key.trim().is_empty() {
        return Err("secret key must not be empty".to_string());
    }
    Ok(format!(
        "secrets://{ENV_SEGMENT}/{tenant}/{}/{category}/{key}",
        team.unwrap_or(TENANT_DEFAULT_TEAM)
    ))
}

/// Every URI [`read_secret_for_unit`] tries, in order:
///
/// 1. the UNIT scope, `…/<team|_>/<category>/<key>.unit-<segment>`, when a unit
///    is known. The name comes from [`crate::mcp_secrets::mcp_unit_secret_key`],
///    whose shape is category-neutral and which the designer calls by that name
///    when it stages a value;
/// 2. the caller's team scope;
/// 3. the tenant-default `_` scope.
///
/// Steps 2 and 3 mirror greentic-designer-admin's `AdminSecretResolver`
/// precedence. A caller with no team, or one naming `_`, yields a single
/// tenant-default URI for them.
///
/// REVIEW BY 2026-12-18: when a unit is known, candidates 2 and 3 are a
/// COMPATIBILITY fallback for deployments staged before the unit scope
/// existed; see `mcp_secrets` for the full note.
pub fn secret_uri_candidates(
    category: &str,
    tenant: &str,
    team: Option<&str>,
    unit: Option<&str>,
    key: &str,
) -> Result<Vec<String>, String> {
    let team = team.filter(|t| !t.is_empty() && *t != TENANT_DEFAULT_TEAM);
    let mut uris = Vec::with_capacity(3);
    if let Some(unit_key) = unit.and_then(|unit| crate::mcp_secrets::mcp_unit_secret_key(key, unit))
    {
        uris.push(secret_uri(category, tenant, team, &unit_key)?);
    }
    if let Some(team) = team {
        uris.push(secret_uri(category, tenant, Some(team), key)?);
    }
    uris.push(secret_uri(category, tenant, None, key)?);
    Ok(uris)
}

/// No credential was readable at any scope [`read_secret_for_unit`] tried.
///
/// Carries the URIs because an operator cannot tell a missing secret from a
/// backend that cannot resolve `secrets://` URIs at all without them.
#[derive(Debug, Clone)]
pub struct SecretMiss {
    pub uris: Vec<String>,
    pub error: String,
}

impl std::fmt::Display for SecretMiss {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "no credential at {} ({})",
            self.uris.join(" or "),
            self.error
        )
    }
}

/// Read a secret for one deployed unit: the first URI of
/// [`secret_uri_candidates`] that resolves wins.
pub async fn read_secret_for_unit(
    secrets: &dyn SecretsManager,
    category: &str,
    tenant: &str,
    team: Option<&str>,
    unit: Option<&str>,
    key: &str,
) -> Result<Vec<u8>, SecretMiss> {
    let uris =
        secret_uri_candidates(category, tenant, team, unit, key).map_err(|error| SecretMiss {
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
    Err(SecretMiss {
        uris,
        error: last_error,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    /// A secrets manager holding exactly the URIs it was given, recording every
    /// URI it was asked for.
    struct MapSecrets {
        entries: HashMap<String, Vec<u8>>,
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

    #[test]
    fn the_category_is_the_fourth_segment_and_everything_else_is_verbatim() {
        // A hyphenated UUID is the A2A agent id; canonicalising it would read a
        // URI nothing writes (contract §5, §6).
        assert_eq!(
            secret_uri("a2a", "acme", None, "8d2f0c1e-7b1a-4c3d").unwrap(),
            "secrets://default/acme/_/a2a/8d2f0c1e-7b1a-4c3d"
        );
        assert_eq!(
            secret_uri("a2a", "acme", Some("Sales"), "K").unwrap(),
            "secrets://default/acme/Sales/a2a/K"
        );
    }

    #[test]
    fn an_empty_category_or_key_is_a_caller_error() {
        assert!(secret_uri("", "acme", None, "k").is_err());
        assert!(secret_uri("a2a", "acme", None, "  ").is_err());
    }

    #[test]
    fn candidates_are_unit_then_team_then_tenant_default() {
        let unit_key = crate::mcp_secrets::mcp_unit_secret_key("agent-1", "worker-a").unwrap();
        assert_eq!(
            secret_uri_candidates("a2a", "acme", Some("sales"), Some("worker-a"), "agent-1")
                .unwrap(),
            vec![
                format!("secrets://default/acme/sales/a2a/{unit_key}"),
                "secrets://default/acme/sales/a2a/agent-1".to_string(),
                "secrets://default/acme/_/a2a/agent-1".to_string(),
            ]
        );
    }

    #[test]
    fn an_explicit_underscore_team_is_not_tried_twice() {
        assert_eq!(
            secret_uri_candidates("a2a", "acme", Some("_"), None, "agent-1").unwrap(),
            vec!["secrets://default/acme/_/a2a/agent-1".to_string()]
        );
    }

    #[tokio::test]
    async fn the_first_scope_that_resolves_wins() {
        let secrets = MapSecrets::with(&[
            ("secrets://default/acme/sales/a2a/agent-1", "team-token"),
            ("secrets://default/acme/_/a2a/agent-1", "tenant-token"),
        ]);
        let got = read_secret_for_unit(
            secrets.as_ref(),
            "a2a",
            "acme",
            Some("sales"),
            None,
            "agent-1",
        )
        .await
        .unwrap();
        assert_eq!(got, b"team-token".to_vec());
        assert_eq!(secrets.seen.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_miss_names_every_uri_tried() {
        let secrets = MapSecrets::with(&[]);
        let miss = read_secret_for_unit(
            secrets.as_ref(),
            "a2a",
            "acme",
            Some("sales"),
            None,
            "agent-1",
        )
        .await
        .unwrap_err();
        let rendered = miss.to_string();
        assert!(
            rendered.starts_with("no credential at ")
                && rendered.contains("secrets://default/acme/sales/a2a/agent-1")
                && rendered.contains("secrets://default/acme/_/a2a/agent-1"),
            "got: {rendered}"
        );
    }
}
