//! Which agents use the user ledger, carried by a `.gtpack` (shared context,
//! Phase C). Written by the designer as `assets/user-ledger.json`:
//!
//! ```json
//! { "<agent_id>": "none" | "read" | "read_write" }
//! ```
//!
//! Twin of [`super::run_context_routes`] and as lenient per entry: malformed
//! JSON reads as ABSENT with a warning, and a non-string or unknown mode (or
//! an empty agent id) drops that agent with a warning and keeps the rest;
//! `"none"` is dropped silently. Unlike that sidecar this one is size-bounded
//! (`MAX_SIDECAR_BYTES`, `MAX_AGENTS`): over either cap the WHOLE sidecar
//! reads as absent, i.e. fail closed (no agent gets the ledger). Warnings
//! never include file contents. Absent means no agent uses the ledger. Unknown
//! top-level fields do not exist in this flat shape: every key is an agent id.
//! The sidecar only SELECTS agents and a mode; the host binding is the
//! capability. This module is ungated and carries only the wire spelling.

use std::collections::BTreeMap;

/// The pack entry name the designer's writer and this reader agree on.
pub const USER_LEDGER_ENTRY: &str = "assets/user-ledger.json";

/// Largest sidecar accepted; larger reads as absent (fail closed).
pub const MAX_SIDECAR_BYTES: usize = 64 * 1024;
/// Most agent entries accepted; more reads as absent (fail closed).
pub const MAX_AGENTS: usize = 256;

#[derive(Debug, Clone, Default)]
pub struct PackUserLedger {
    agents: BTreeMap<String, String>,
}

impl PackUserLedger {
    pub fn from_sidecar_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() > MAX_SIDECAR_BYTES {
            tracing::warn!(
                bytes = bytes.len(),
                "user-ledger: sidecar too large; ignoring it"
            );
            return None;
        }
        let raw: serde_json::Map<String, serde_json::Value> = serde_json::from_slice(bytes)
            .inspect_err(|e| tracing::warn!(error = %e, "user-ledger: malformed; ignoring sidecar"))
            .ok()?;
        if raw.len() > MAX_AGENTS {
            tracing::warn!(
                agents = raw.len(),
                "user-ledger: too many agents; ignoring sidecar"
            );
            return None;
        }
        let mut agents = BTreeMap::new();
        for (agent_id, mode) in raw {
            match mode.as_str() {
                _ if agent_id.is_empty() => {
                    tracing::warn!("user-ledger: empty agent id; dropped");
                }
                Some(m @ ("read" | "read_write")) => {
                    agents.insert(agent_id, m.to_string());
                }
                Some("none") => {}
                _ => tracing::warn!(
                    agent = %agent_id,
                    "user-ledger: unknown mode (expected none, read or read_write); agent dropped"
                ),
            }
        }
        Some(Self { agents })
    }

    /// Every agent that uses the ledger and its mode, sorted by agent id.
    pub fn agents(&self) -> impl Iterator<Item = (&String, &String)> {
        self.agents.iter()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn modes(rc: &PackUserLedger) -> Vec<(String, String)> {
        rc.agents().map(|(a, m)| (a.clone(), m.clone())).collect()
    }

    #[test]
    fn a_valid_sidecar_keeps_read_and_read_write_and_drops_none() {
        let rc = PackUserLedger::from_sidecar_bytes(br#"{"a":"read","b":"read_write","c":"none"}"#)
            .unwrap();
        assert_eq!(
            modes(&rc),
            vec![
                ("a".into(), "read".into()),
                ("b".into(), "read_write".into())
            ]
        );
    }

    #[test]
    fn a_bad_entry_drops_only_itself() {
        let rc = PackUserLedger::from_sidecar_bytes(
            br#"{"a":"READ","b":3,"c":{"x":1},"d":"read","e":" read","f":"read_write ","g":null,"":"read"}"#,
        )
        .unwrap();
        assert_eq!(modes(&rc), vec![("d".into(), "read".into())]);
    }

    #[test]
    fn a_malformed_sidecar_reads_as_absent() {
        assert!(PackUserLedger::from_sidecar_bytes(b"[1]").is_none());
        assert!(PackUserLedger::from_sidecar_bytes(b"not json").is_none());
        assert!(PackUserLedger::from_sidecar_bytes(b"").is_none());
        assert!(PackUserLedger::from_sidecar_bytes(br#"{"a":"read""#).is_none());
    }

    #[test]
    fn an_empty_object_grants_nothing() {
        let rc = PackUserLedger::from_sidecar_bytes(b"{}").unwrap();
        assert_eq!(rc.agents().count(), 0);
    }

    #[test]
    fn an_oversized_sidecar_reads_as_absent() {
        let pad = " ".repeat(MAX_SIDECAR_BYTES);
        let body = format!(r#"{{"a":"read"}}{pad}"#);
        assert!(body.len() > MAX_SIDECAR_BYTES);
        assert!(PackUserLedger::from_sidecar_bytes(body.as_bytes()).is_none());
        // Exactly at the cap is accepted.
        let ok = format!(r#"{{"a":"read"}}{}"#, " ".repeat(MAX_SIDECAR_BYTES - 12));
        assert_eq!(ok.len(), MAX_SIDECAR_BYTES);
        assert!(PackUserLedger::from_sidecar_bytes(ok.as_bytes()).is_some());
    }

    #[test]
    fn too_many_agents_reads_as_absent_but_the_cap_itself_is_accepted() {
        let build = |n: usize| {
            let body: Vec<String> = (0..n).map(|i| format!(r#""a{i}":"read""#)).collect();
            format!("{{{}}}", body.join(","))
        };
        let at = PackUserLedger::from_sidecar_bytes(build(MAX_AGENTS).as_bytes()).unwrap();
        assert_eq!(at.agents().count(), MAX_AGENTS);
        assert!(PackUserLedger::from_sidecar_bytes(build(MAX_AGENTS + 1).as_bytes()).is_none());
    }

    #[test]
    fn a_duplicate_key_resolves_to_the_last_value_and_never_escalates_past_it() {
        let rc = PackUserLedger::from_sidecar_bytes(br#"{"a":"read_write","a":"none"}"#).unwrap();
        assert_eq!(rc.agents().count(), 0);
    }

    #[test]
    fn a_warning_never_carries_the_file_contents() {
        // Structural: the module only logs error text from serde (which may
        // quote a token) never the bytes; the secret below is a valid string
        // value in a dropped entry and must not be a logged field. Checked by
        // the source ratchet below rather than a log capture.
        let src = include_str!("user_ledger_routes.rs");
        let code = src.split("#[cfg(test)]").next().unwrap();
        assert!(!code.contains("from_utf8"));
        assert!(!code.contains("%mode"));
        assert!(!code.contains("?mode"));
        assert!(!code.contains("?bytes"));
    }
}
