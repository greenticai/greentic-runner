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
}

fn default_transport() -> String {
    "http".to_string()
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
        let routes: Vec<PackMcpRoute> = serde_json::from_slice(sidecar_bytes)
            .inspect_err(|e| tracing::warn!(error = %e, "mcp-routes: malformed; ignoring sidecar"))
            .ok()?;

        let by_server = routes
            .into_iter()
            .map(|route| (route.server_id.clone(), route))
            .collect();
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
    fn a_malformed_sidecar_is_ignored_rather_than_failing_the_pack() {
        assert!(PackMcpRoutes::from_sidecar_bytes(b"{not json").is_none());
    }
}
