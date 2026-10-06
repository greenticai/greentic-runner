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
//! carry no file contents except an offending agent id (a key of the file);
//! the malformed-JSON warn logs only serde's error category, line and column,
//! never its text (which can quote content). Absent means no agent uses the
//! ledger. Unknown top-level fields do not exist in this flat shape: every key
//! is an agent id.
//!
//! The sidecar decides Read vs ReadWrite DIRECTLY: `UserLedgerBinding::new`
//! takes the agents map with no ceiling, so a pack alone can enable
//! `read_write` for its own agents. The gates that remain are the door token's
//! `ledger` purpose, the kill switch, the tenant match and the verified
//! subject. Duplicate JSON keys resolve last-wins (the pack author's own
//! declaration). `PackRuntime::user_ledger` caches the result of its first
//! read for the life of the runtime, a failed read included (a hot reload
//! builds a fresh one), like its run-context twin. This module is ungated and
//! carries only the wire spelling.

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
            .inspect_err(|e| {
                tracing::warn!(
                    category = ?e.classify(),
                    line = e.line(),
                    column = e.column(),
                    "user-ledger: malformed; ignoring sidecar"
                )
            })
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
                    agent = ?agent_id,
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
    fn a_duplicate_key_resolves_last_wins_so_a_trailing_none_withdraws_the_grant() {
        let rc = PackUserLedger::from_sidecar_bytes(br#"{"a":"read_write","a":"none"}"#).unwrap();
        assert_eq!(rc.agents().count(), 0);
    }

    #[test]
    fn a_duplicate_key_resolves_last_wins_so_a_trailing_grant_stands() {
        // The pack author's own declaration: documented, not an escalation.
        let rc = PackUserLedger::from_sidecar_bytes(br#"{"a":"none","a":"read_write"}"#).unwrap();
        assert_eq!(modes(&rc), vec![("a".into(), "read_write".into())]);
    }
}
