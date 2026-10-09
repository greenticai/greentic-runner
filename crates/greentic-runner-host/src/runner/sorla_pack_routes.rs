//! Reads `assets/sorla-routes.json`: the SoRs a pack's flows and workers need.
//!
//! Requirements only. The address and credential of each SoR arrive at call
//! time as a route document from the secrets manager (see `sorla_route`), so a
//! pack built once runs in any environment.

use serde::Deserialize;

pub const SORLA_ROUTES_ENTRY: &str = "assets/sorla-routes.json";

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct PackSorlaRoute {
    pub sor: String,
    #[serde(default)]
    pub actions: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct PackSorlaRoutes {
    routes: Vec<PackSorlaRoute>,
}

impl PackSorlaRoutes {
    /// Lenient per record: a record that does not parse, or names a blank SoR,
    /// is dropped; on a duplicate SoR the first record wins. A document that is
    /// not a JSON array at all is `None`.
    pub fn from_sidecar_bytes(bytes: &[u8]) -> Option<Self> {
        let raw: Vec<serde_json::Value> = serde_json::from_slice(bytes).ok()?;
        let mut routes: Vec<PackSorlaRoute> = Vec::new();
        for value in raw {
            let Ok(route) = serde_json::from_value::<PackSorlaRoute>(value) else {
                continue;
            };
            let sor = route.sor.trim();
            if sor.is_empty() || routes.iter().any(|r| r.sor == sor) {
                continue;
            }
            routes.push(PackSorlaRoute {
                sor: sor.to_string(),
                actions: route.actions,
            });
        }
        Some(Self { routes })
    }

    pub fn iter(&self) -> impl Iterator<Item = &PackSorlaRoute> {
        self.routes.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.routes.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_records_and_skips_blank_or_duplicate_sors() {
        let bytes = br#"[
            {"sor":"landlord","actions":["record_rent_payment"]},
            {"sor":"","actions":[]},
            {"sor":"landlord","actions":["other"]},
            {"sor":"billing"}
        ]"#;
        let routes = PackSorlaRoutes::from_sidecar_bytes(bytes).expect("parses");
        let sors: Vec<&str> = routes.iter().map(|r| r.sor.as_str()).collect();
        assert_eq!(sors, ["landlord", "billing"]);
        assert_eq!(
            routes.iter().next().map(|r| r.actions.clone()),
            Some(vec!["record_rent_payment".to_string()])
        );
    }

    #[test]
    fn malformed_json_is_none_not_a_panic() {
        assert!(PackSorlaRoutes::from_sidecar_bytes(b"{not json").is_none());
    }

    #[test]
    fn unknown_fields_are_ignored() {
        let routes =
            PackSorlaRoutes::from_sidecar_bytes(br#"[{"sor":"a","future":1}]"#).expect("parses");
        assert!(!routes.is_empty());
    }
}
