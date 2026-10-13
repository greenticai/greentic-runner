//! OAuth 2.0 `client_credentials` for outbound MCP and A2A routes (access
//! policy, phase 2).
//!
//! A route whose sidecar record says `auth_mode: "oauth_client_credentials"`
//! does not carry a static bearer. The admin seals the client id and secret as
//! one JSON secret, and the runner exchanges them for a short-lived access
//! token at the endpoint the sidecar names. [`TokenProvider`] does that
//! exchange and is shared by the MCP and A2A paths.
//!
//! What lives where:
//!
//! - the sidecar carries only non-secret parameters: `auth_mode`,
//!   `oauth_endpoint`, `oauth_scope`, `oauth_audience` (field names must not
//!   contain `secret`, `credential` or `token`; the designer ratchets that);
//! - the client id and secret travel only in the sealed secret, parsed by
//!   [`ClientCredentials::from_secret_bytes`];
//! - the access token exists only inside [`TokenProvider`]'s cache and the
//!   header of the request it authorises, always as a [`SecretString`].
//!
//! Failure policy: an unknown `auth_mode`, a malformed secret, or a JSON
//! client-credentials secret on a route that is not in OAuth mode all fail
//! THAT route with an explicit message. None of them ever falls back to
//! sending the stored value as a bearer.

mod cache;
mod provider;
mod route;

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests;
#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests_route;

pub use provider::{OAuthError, TokenProvider};
pub use route::{
    AuthMode, ClientCredentials, ResolvedAuth, RouteAuthSpec, RouteOAuth, parse_auth_mode,
    resolve_route_auth, route_token_key,
};

/// The sidecar value that selects this flow.
pub const AUTH_MODE_OAUTH_CLIENT_CREDENTIALS: &str = "oauth_client_credentials";
