//! The credential an `a2a:` call carries: resolved per call from the secrets
//! store, never held by the source, never sent on the card fetch.
//!
//! URIs are the cross-repo contract's §5 candidates, built by
//! [`crate::scoped_secrets`] with category [`A2A_CATEGORY`]. The agent id is
//! used verbatim; it is a hyphenated UUID, and canonicalising it would read a
//! URI nothing writes.

use std::sync::Arc;

use greentic_secrets_lib::SecretsManager;
use reqwest::header::{AUTHORIZATION, HeaderName, HeaderValue};

use super::types::A2aRoute;

/// The secrets category A2A credentials are sealed under (contract §5/§6).
pub(super) const A2A_CATEGORY: &str = "a2a";

/// Where a credentialed route's token is looked up: the manager, and the
/// tenant and unit captured when the source was built.
pub(super) struct CredentialScope {
    secrets: Option<Arc<dyn SecretsManager>>,
    tenant: String,
    unit: Option<String>,
}

impl CredentialScope {
    pub(super) fn new(
        secrets: Option<Arc<dyn SecretsManager>>,
        tenant: String,
        unit: Option<String>,
    ) -> Self {
        Self {
            secrets,
            tenant,
            unit,
        }
    }

    /// The scope of a source built with no credentials at all. Every route it
    /// holds has `requires_auth: false`, so it is never consulted.
    pub(super) fn none() -> Self {
        Self::new(None, String::new(), None)
    }

    /// The header to add to the `SendMessage` POST: `None` for a route that
    /// stores no credential, `Err` when one is expected and cannot be sent.
    ///
    /// Errors name the agent and the URIs looked in, and never the value.
    pub(super) async fn header_for(
        &self,
        route: &A2aRoute,
    ) -> Result<Option<(HeaderName, HeaderValue)>, String> {
        if !route.requires_auth {
            return Ok(None);
        }
        let agent_id = route.agent_id.as_str();
        let team = route.auth_team.as_deref();
        let unit = self.unit.as_deref();
        let Some(secrets) = self.secrets.as_ref() else {
            let uris = crate::scoped_secrets::secret_uri_candidates(
                A2A_CATEGORY,
                &self.tenant,
                team,
                unit,
                agent_id,
            )
            .unwrap_or_default();
            return Err(format!(
                "a2a agent {agent_id}: no credential at {} (no secrets manager is \
                 configured on this runner)",
                uris.join(" or ")
            ));
        };
        let bytes = crate::scoped_secrets::read_secret_for_unit(
            secrets.as_ref(),
            A2A_CATEGORY,
            &self.tenant,
            team,
            unit,
            agent_id,
        )
        .await
        .map_err(|miss| format!("a2a agent {agent_id}: {miss}"))?;
        let token = String::from_utf8(bytes).map_err(|_| {
            format!("a2a agent {agent_id}: the stored credential is not valid UTF-8")
        })?;
        let token = token.trim();
        if token.is_empty() {
            return Err(format!(
                "a2a agent {agent_id}: the stored credential is empty"
            ));
        }
        auth_header(agent_id, route.auth_header_name.as_deref(), token).map(Some)
    }
}

/// The `(name, value)` pair for `token`, following greentic-mcp-client's
/// `McpAuth::header` rule: no name, or any spelling of `authorization`, sends
/// `Authorization: Bearer <token>`; any other name sends the raw token.
fn auth_header(
    agent_id: &str,
    header_name: Option<&str>,
    token: &str,
) -> Result<(HeaderName, HeaderValue), String> {
    let name = match header_name.map(str::trim).filter(|n| !n.is_empty()) {
        None => AUTHORIZATION,
        Some(raw) => HeaderName::from_bytes(raw.as_bytes()).map_err(|_| {
            format!("a2a agent {agent_id}: '{raw}' is not a valid HTTP header name")
        })?,
    };
    let raw_value = if name == AUTHORIZATION {
        format!("Bearer {token}")
    } else {
        token.to_string()
    };
    let mut value = HeaderValue::from_str(&raw_value).map_err(|_| {
        format!(
            "a2a agent {agent_id}: the stored credential cannot be sent as an HTTP header value"
        )
    })?;
    value.set_sensitive(true);
    Ok((name, value))
}

/// Refuse to carry a credential to any host other than the one the admin
/// configured.
///
/// The interface URL comes from the agent CARD, which the agent controls, and
/// `require_secure_interface` accepts any https host it names. Without this
/// rule a card could route the admin's token to a host nobody vetted. Host
/// and port must both equal the configured `base_url`'s (the port through
/// `port_or_known_default`, so `https://a` and `https://a:443` agree).
///
/// Called before the secrets store is read, so a mismatch never touches it.
pub(super) fn ensure_same_host(
    agent_id: &str,
    base_url: &str,
    target: &url::Url,
) -> Result<(), String> {
    let base = url::Url::parse(base_url).map_err(|_| {
        format!("a2a agent {agent_id}: configured base url '{base_url}' is not a valid URL")
    })?;
    let same_host = match (base.host_str(), target.host_str()) {
        (Some(configured), Some(interface)) => configured.eq_ignore_ascii_case(interface),
        _ => false,
    };
    if same_host && base.port_or_known_default() == target.port_or_known_default() {
        return Ok(());
    }
    Err(format!(
        "a2a agent {agent_id}: interface host {} differs from configured host {}; \
         refusing to send its credential",
        authority(target),
        authority(&base)
    ))
}

/// `host:port` for an error message, the port being the effective one.
fn authority(url: &url::Url) -> String {
    let host = url.host_str().unwrap_or("<no host>");
    match url.port_or_known_default() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    }
}
