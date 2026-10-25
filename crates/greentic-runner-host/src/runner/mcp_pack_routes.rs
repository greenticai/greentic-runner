//! MCP route material carried by a `.gtpack`, so an `mcp` flow node can be
//! dispatched without reaching the admin.
//!
//! A pack's `mcp` node carries only an opaque `server` id. Resolving it used to
//! require the tenant's admin catalog, which a deployed runner has no
//! credentials for — so every MCP node in a deployed bundle bound
//! `{"error": "MCP is not configured on this runner"}`. The designer now writes
//! `assets/mcp-routes.json` into the pack with the non-secret half of the
//! route.
//!
//! The sidecar carries no credential. An `http` route's token is resolved at
//! dispatch time from `secrets://default/<tenant>/<team>/mcp/<server_id>` — see
//! `greentic_aw_runtime::mcp_secrets` and `runner::mcp_node`.
//!
//! Every failure path yields `None` so the caller falls back to the admin
//! catalog: a pack that fails to LOAD takes every other node down with it,
//! which is far worse than one MCP node reporting an error.

use std::collections::HashMap;
use std::io::Read;

use serde::Deserialize;

/// The pack entry name. The designer's writer and every reader here address
/// the sidecar by this exact path.
pub const MCP_ROUTES_ENTRY: &str = "assets/mcp-routes.json";

/// One server's non-secret route material. Field names match the designer's
/// writer and the admin wire shape.
#[derive(Debug, Clone, Deserialize)]
pub struct PackMcpRoute {
    pub server_id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default = "default_transport")]
    pub transport: String,
    #[serde(default)]
    pub transport_url: Option<String>,
    #[serde(default)]
    pub auth_header_name: Option<String>,
    /// Team slug the authoring session resolved this server's token under.
    ///
    /// Not a credential — a team slug already travels as a request header and
    /// scopes the pack itself. It exists because the admin seals an MCP token at
    /// the server ROW's team scope while the deployed runtime carries no team at
    /// all, so without it every lane resolves the tenant-default `_` scope only
    /// and a team-scoped server's token is unreachable at run time.
    ///
    /// Absent (a pack predating this field) means `_`-only, which is exactly the
    /// behaviour before it existed. The designer-side writer is a separate
    /// change; reading it here is additive in both directions.
    #[serde(default)]
    pub auth_team: Option<String>,
    #[serde(default)]
    pub component_ref: Option<String>,
    #[serde(default)]
    pub component_version: Option<String>,
    #[serde(default)]
    pub component_digest: Option<String>,
    /// `bearer` / `header` / `oauth_client_credentials`. Absent keeps the
    /// pre-existing behaviour. Read as a string and validated per route, so an
    /// unknown (or mistyped) value refuses that one server and never the file.
    #[serde(default, deserialize_with = "lenient_string")]
    pub auth_mode: Option<String>,
    #[serde(default, deserialize_with = "lenient_string")]
    pub oauth_endpoint: Option<String>,
    #[serde(default, deserialize_with = "lenient_string")]
    pub oauth_scope: Option<String>,
    #[serde(default, deserialize_with = "lenient_string")]
    pub oauth_audience: Option<String>,
}

impl PackMcpRoute {
    /// The non-secret OAuth parameters of this record.
    #[cfg(feature = "agentic-worker")]
    pub fn auth_spec(&self) -> greentic_aw_runtime::oauth::RouteAuthSpec<'_> {
        greentic_aw_runtime::oauth::RouteAuthSpec {
            auth_mode: self.auth_mode.as_deref(),
            oauth_endpoint: self.oauth_endpoint.as_deref(),
            oauth_scope: self.oauth_scope.as_deref(),
            oauth_audience: self.oauth_audience.as_deref(),
        }
    }
}

/// A string field that tolerates any other JSON type. A non-string value
/// becomes the marker `"<invalid>"`, which no auth mode matches, so a record
/// with a mistyped `auth_mode` fails closed instead of being dropped (which
/// would hand the server to the admin catalog) or read as "no auth_mode".
fn lenient_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    Ok(match value {
        None | Some(serde_json::Value::Null) => None,
        Some(serde_json::Value::String(text)) => Some(text),
        Some(_) => Some("<invalid>".to_string()),
    })
}

fn default_transport() -> String {
    "http".to_string()
}

fn route_segments_are_safe(route: &PackMcpRoute) -> bool {
    use crate::secrets_broker::is_safe_secret_segment;
    is_safe_secret_segment(&route.server_id)
        && route
            .auth_team
            .as_deref()
            .is_none_or(|team| team.is_empty() || is_safe_secret_segment(team))
}

/// Every route a pack declares, keyed by server id.
#[derive(Debug, Clone, Default)]
pub struct PackMcpRoutes {
    by_server: HashMap<String, PackMcpRoute>,
}

impl PackMcpRoutes {
    /// Parse the sidecar out of `.gtpack` bytes. `None` when the entry is
    /// absent (a pack predating this feature) or unreadable.
    pub fn from_pack_bytes(pack_bytes: &[u8]) -> Option<Self> {
        let mut archive = zip::ZipArchive::new(std::io::Cursor::new(pack_bytes))
            .inspect_err(|e| tracing::warn!(error = %e, "mcp-routes: pack is not readable"))
            .ok()?;
        // A pack predating this feature simply has no entry — not a warning.
        let mut entry = archive.by_name(MCP_ROUTES_ENTRY).ok()?;
        let mut bytes = Vec::new();
        entry
            .read_to_end(&mut bytes)
            .inspect_err(|e| tracing::warn!(error = %e, "mcp-routes: entry unreadable"))
            .ok()?;
        Self::from_sidecar_bytes(&bytes)
    }

    /// Parse the sidecar from its own bytes, already extracted from the pack.
    ///
    /// The read counterpart used by the runtime: a pack can be materialized as
    /// a DIRECTORY rather than a `.gtpack` archive, in which case there are no
    /// zip bytes to hand [`from_pack_bytes`] at all. `PackRuntime::read_pack_file`
    /// resolves either layout and yields the entry bytes, which land here.
    ///
    /// [`from_pack_bytes`]: PackMcpRoutes::from_pack_bytes
    pub fn from_sidecar_bytes(sidecar_bytes: &[u8]) -> Option<Self> {
        let records: Vec<serde_json::Value> = serde_json::from_slice(sidecar_bytes)
            .inspect_err(|e| tracing::warn!(error = %e, "mcp-routes: malformed; ignoring sidecar"))
            .ok()?;

        // Lenient per record: one unreadable record must not void the others.
        let mut by_server = HashMap::new();
        for (index, record) in records.into_iter().enumerate() {
            match serde_json::from_value::<PackMcpRoute>(record) {
                Ok(route) if !route_segments_are_safe(&route) => {
                    // server_id and auth_team become secret-path segments.
                    tracing::warn!(
                        index,
                        "mcp-routes: record has an unsafe server_id or auth_team; dropped"
                    );
                }
                Ok(route) => {
                    by_server.insert(route.server_id.clone(), route);
                }
                Err(e) => {
                    tracing::warn!(index, error = %e, "mcp-routes: record unreadable; dropped");
                }
            }
        }
        Some(Self { by_server })
    }

    pub fn get(&self, server_id: &str) -> Option<&PackMcpRoute> {
        self.by_server.get(server_id)
    }

    /// Every route the pack declares. The flow node resolves one server at a
    /// time; the agent loop has to build a whole catalog up front, because a
    /// worker's `mcp:` tool must be advertised to the LLM before any call
    /// names a server.
    pub fn iter(&self) -> impl Iterator<Item = &PackMcpRoute> {
        self.by_server.values()
    }

    pub fn is_empty(&self) -> bool {
        self.by_server.is_empty()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn auth_team_round_trips_from_the_sidecar() {
        let routes = PackMcpRoutes::from_sidecar_bytes(
            br#"[{"server_id":"srv-1","transport_url":"https://x/","auth_team":"sales"}]"#,
        )
        .expect("sidecar parses");
        assert_eq!(
            routes.get("srv-1").unwrap().auth_team.as_deref(),
            Some("sales")
        );
    }

    #[test]
    fn records_with_traversal_in_server_id_or_auth_team_are_dropped() {
        let routes = PackMcpRoutes::from_sidecar_bytes(
            br#"[
                {"server_id":"..","transport_url":"https://x/"},
                {"server_id":"a/b","transport_url":"https://x/"},
                {"server_id":"%2e%2e","transport_url":"https://x/"},
                {"server_id":"a?b","transport_url":"https://x/"},
                {"server_id":"a#b","transport_url":"https://x/"},
                {"server_id":"bad-team","auth_team":"../other","transport_url":"https://x/"},
                {"server_id":"bad-team2","auth_team":"a/b","transport_url":"https://x/"},
                {"server_id":"ok","auth_team":"sales","transport_url":"https://x/"}
            ]"#,
        )
        .expect("sidecar parses");
        let ids: Vec<&str> = routes.iter().map(|r| r.server_id.as_str()).collect();
        assert_eq!(ids, ["ok"]);
    }

    #[test]
    fn a_sidecar_predating_auth_team_still_parses_and_means_tenant_default() {
        // Forward/backward compatible in both directions: the designer-side
        // writer is a separate change, so a new runner must keep reading an old
        // sidecar, and `None` must mean exactly today's `_`-only behaviour.
        let routes = PackMcpRoutes::from_sidecar_bytes(
            br#"[{"server_id":"srv-1","transport_url":"https://x/"}]"#,
        )
        .expect("sidecar parses");
        assert_eq!(routes.get("srv-1").unwrap().auth_team, None);
    }

    #[test]
    fn an_unknown_sidecar_field_is_ignored_rather_than_failing_the_pack() {
        // An OLD runner reading a NEW sidecar is the mirror case, and a parse
        // failure here would drop every route in the pack, not just one field.
        let routes = PackMcpRoutes::from_sidecar_bytes(
            br#"[{"server_id":"srv-1","transport_url":"https://x/","future_field":1}]"#,
        )
        .expect("sidecar parses");
        assert!(routes.get("srv-1").is_some());
    }

    #[test]
    fn iter_yields_every_declared_route() {
        let routes = PackMcpRoutes::from_sidecar_bytes(
            br#"[{"server_id":"a","transport_url":"https://a/"},
                 {"server_id":"b","transport":"local-wasm","component_ref":"w.component"}]"#,
        )
        .expect("sidecar parses");
        let mut ids: Vec<&str> = routes.iter().map(|r| r.server_id.as_str()).collect();
        ids.sort_unstable();
        assert_eq!(ids, ["a", "b"]);
        // The default matters: an entry with no `transport` is `http`, and the
        // agent path branches on that string to decide whether to read a token.
        assert_eq!(routes.get("a").unwrap().transport, "http");
        assert_eq!(routes.get("b").unwrap().transport, "local-wasm");
    }

    #[test]
    fn oauth_parameters_round_trip_and_old_sidecars_read_as_none() {
        let routes = PackMcpRoutes::from_sidecar_bytes(
            br#"[{"server_id":"a","transport_url":"https://a/","auth_mode":"oauth_client_credentials",
                  "oauth_endpoint":"https://idp/token","oauth_scope":"s","oauth_audience":"aud"},
                 {"server_id":"b","transport_url":"https://b/"}]"#,
        )
        .expect("parses");
        let a = routes.get("a").unwrap();
        assert_eq!(a.auth_mode.as_deref(), Some("oauth_client_credentials"));
        assert_eq!(a.oauth_endpoint.as_deref(), Some("https://idp/token"));
        assert_eq!(a.oauth_scope.as_deref(), Some("s"));
        assert_eq!(a.oauth_audience.as_deref(), Some("aud"));
        assert_eq!(routes.get("b").unwrap().auth_mode, None);
    }

    #[test]
    fn one_bad_record_no_longer_voids_the_file() {
        let routes = PackMcpRoutes::from_sidecar_bytes(
            br#"[{"transport_url":"https://nameless/"},
                 {"server_id":"ok","transport_url":"https://ok/"}]"#,
        )
        .expect("the file still parses");
        assert!(routes.get("ok").is_some());
    }

    #[test]
    fn a_mistyped_auth_mode_is_kept_as_an_unknown_mode_not_dropped() {
        let routes = PackMcpRoutes::from_sidecar_bytes(
            br#"[{"server_id":"a","transport_url":"https://a/","auth_mode":7}]"#,
        )
        .expect("parses");
        assert_eq!(
            routes.get("a").unwrap().auth_mode.as_deref(),
            Some("<invalid>")
        );
        assert!(
            greentic_aw_runtime::oauth::parse_auth_mode(
                routes.get("a").unwrap().auth_mode.as_deref()
            )
            .is_err()
        );
    }

    #[test]
    fn a_malformed_sidecar_is_ignored_rather_than_failing_the_pack() {
        assert!(PackMcpRoutes::from_sidecar_bytes(b"{not json").is_none());
    }
}
