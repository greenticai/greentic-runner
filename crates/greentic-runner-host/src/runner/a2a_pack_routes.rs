//! A2A agent route material carried by a `.gtpack`, so a deployed worker's
//! `a2a:<agent_id>` tools resolve without reaching the admin.
//!
//! The designer writes `assets/a2a-routes.json` (cross-repo contract
//! 2026-09-21 §4). It is a SEPARATE file from `assets/mcp-routes.json`: that
//! format keys on `server_id` and defaults `transport` to `"http"`, so every
//! MCP reader would take an A2A record for an MCP server.
//!
//! The sidecar carries no credential. A route with `requires_auth` has its
//! token read at CALL time from
//! `secrets://default/<tenant>/<auth_team|_>/a2a/<agent_id>` (§5); see
//! `greentic_aw_runtime::a2a_source`.
//!
//! Twin of [`super::mcp_pack_routes`], and one step more lenient. Malformed
//! JSON reads as ABSENT, with a warning. A single record that does not parse
//! (a missing `base_url`, a wrong type, a blank id) is dropped with a warning,
//! and the rest of the file is kept. One bad agent must not take a worker's
//! other agents with it.

use std::collections::HashSet;

use serde::Deserialize;

/// The pack entry name the designer's writer and this reader agree on.
pub const A2A_ROUTES_ENTRY: &str = "assets/a2a-routes.json";

/// One agent's non-secret route material (contract §4). Unknown fields are
/// ignored so an old runner can read a newer sidecar.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct PackA2aRoute {
    pub agent_id: String,
    #[serde(default)]
    pub name: Option<String>,
    pub base_url: String,
    /// Header the token travels in; `None` means `Authorization: Bearer`.
    #[serde(default)]
    pub auth_header_name: Option<String>,
    /// Team slug the credential is sealed under; `None` means `_`.
    #[serde(default)]
    pub auth_team: Option<String>,
    /// A token is stored and must be sent. Absent means `false`.
    #[serde(default)]
    pub requires_auth: bool,
}

/// Every agent a pack declares, in declaration order, unique by `agent_id`.
#[derive(Debug, Clone, Default)]
pub struct PackA2aRoutes {
    routes: Vec<PackA2aRoute>,
}

impl PackA2aRoutes {
    /// Parse the sidecar from its own bytes, already extracted from the pack
    /// by `PackRuntime::read_pack_file`, which handles both the directory and
    /// the `.gtpack` layout.
    ///
    /// `None` when the bytes are not a JSON array. A record that fails on its
    /// own is dropped; the first of two records with one `agent_id` wins.
    pub fn from_sidecar_bytes(sidecar_bytes: &[u8]) -> Option<Self> {
        let records: Vec<serde_json::Value> = serde_json::from_slice(sidecar_bytes)
            .inspect_err(|e| tracing::warn!(error = %e, "a2a-routes: malformed; ignoring sidecar"))
            .ok()?;

        let mut seen = HashSet::new();
        let mut routes = Vec::with_capacity(records.len());
        for (index, record) in records.into_iter().enumerate() {
            let route: PackA2aRoute = match serde_json::from_value(record) {
                Ok(route) => route,
                Err(e) => {
                    tracing::warn!(index, error = %e, "a2a-routes: record unreadable; dropped");
                    continue;
                }
            };
            if route.agent_id.trim().is_empty() || route.base_url.trim().is_empty() {
                tracing::warn!(
                    index,
                    "a2a-routes: record has a blank agent_id or base_url; dropped"
                );
                continue;
            }
            if !seen.insert(route.agent_id.clone()) {
                tracing::warn!(
                    index,
                    agent_id = %route.agent_id,
                    "a2a-routes: duplicate agent_id; the first record wins"
                );
                continue;
            }
            routes.push(route);
        }
        Some(Self { routes })
    }

    pub fn get(&self, agent_id: &str) -> Option<&PackA2aRoute> {
        self.routes.iter().find(|route| route.agent_id == agent_id)
    }

    /// Every route, in the order the sidecar declares them.
    pub fn iter(&self) -> impl Iterator<Item = &PackA2aRoute> {
        self.routes.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.routes.is_empty()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn a_contract_shaped_record_round_trips() {
        let routes = PackA2aRoutes::from_sidecar_bytes(
            br#"[{"agent_id":"8d2f-1","name":"Recipe agent","base_url":"https://agent.example.com",
                  "auth_header_name":null,"auth_team":"sales","requires_auth":true}]"#,
        )
        .expect("sidecar parses");
        let route = routes.get("8d2f-1").expect("route present");
        assert_eq!(route.base_url, "https://agent.example.com");
        assert_eq!(route.name.as_deref(), Some("Recipe agent"));
        assert_eq!(route.auth_header_name, None);
        assert_eq!(route.auth_team.as_deref(), Some("sales"));
        assert!(route.requires_auth);
    }

    #[test]
    fn omitted_optional_fields_mean_no_credential_and_the_tenant_default_team() {
        let routes = PackA2aRoutes::from_sidecar_bytes(
            br#"[{"agent_id":"a","base_url":"https://a.example"}]"#,
        )
        .expect("sidecar parses");
        let route = routes.get("a").unwrap();
        assert_eq!(route.auth_team, None);
        assert_eq!(route.name, None);
        assert!(!route.requires_auth);
    }

    #[test]
    fn an_unknown_field_is_ignored() {
        let routes = PackA2aRoutes::from_sidecar_bytes(
            br#"[{"agent_id":"a","base_url":"https://a.example","future_field":{"x":1}}]"#,
        )
        .expect("sidecar parses");
        assert!(routes.get("a").is_some());
    }

    #[test]
    fn malformed_json_reads_as_absent() {
        assert!(PackA2aRoutes::from_sidecar_bytes(b"{not json").is_none());
        assert!(PackA2aRoutes::from_sidecar_bytes(br#"{"agent_id":"a"}"#).is_none());
    }

    #[test]
    fn one_bad_record_is_dropped_without_losing_the_others() {
        let routes = PackA2aRoutes::from_sidecar_bytes(
            br#"[{"agent_id":"good","base_url":"https://g.example"},
                 {"agent_id":"no-base"},
                 {"agent_id":"","base_url":"https://blank.example"},
                 {"agent_id":"typed-wrong","base_url":42}]"#,
        )
        .expect("sidecar parses");
        let ids: Vec<&str> = routes.iter().map(|r| r.agent_id.as_str()).collect();
        assert_eq!(ids, ["good"]);
    }

    #[test]
    fn a_duplicate_agent_id_keeps_the_first_record() {
        let routes = PackA2aRoutes::from_sidecar_bytes(
            br#"[{"agent_id":"a","base_url":"https://first.example"},
                 {"agent_id":"a","base_url":"https://second.example"}]"#,
        )
        .expect("sidecar parses");
        assert_eq!(routes.get("a").unwrap().base_url, "https://first.example");
        assert_eq!(routes.iter().count(), 1);
    }

    #[test]
    fn an_empty_array_is_present_but_empty() {
        let routes = PackA2aRoutes::from_sidecar_bytes(b"[]").expect("parses");
        assert!(routes.is_empty());
    }
}
