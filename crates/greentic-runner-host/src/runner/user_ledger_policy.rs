//! Build a runtime's [`UserLedgerBinding`] from the embedding host's door
//! target and the packs' `assets/user-ledger.json` sidecars (shared context,
//! Phase C).
//!
//! Both are required. No target (the host passed no
//! `RevisionHostOptions::with_user_ledger`) or no agent named by any pack means
//! no binding, and the runtime behaves exactly as before. The runner never
//! derives a target itself: greentic-start builds it from the unit's staged
//! `metering` block and hands it over; this module does not read that block.
//!
//! A target the client refuses (blank field, cleartext URL off loopback,
//! userinfo in the URL) is one warning naming the reason class, never the URL
//! or the token, and the runtime boots without the ledger.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use greentic_aw_runtime::user_ledger::{HttpUserLedger, UserLedgerTarget};
use greentic_aw_runtime::{LedgerMode, UserLedger, UserLedgerBinding};

/// Agent id → mode across one revision's packs; the first pack naming an
/// agent wins, as for A2A routes. An agent the runtime does not carry is
/// warned about (almost always a key written under the display name).
pub(crate) fn ledger_modes_from_packs<'a>(
    packs: &[Arc<crate::pack::PackRuntime>],
    known_agents: impl IntoIterator<Item = &'a str>,
) -> HashMap<String, LedgerMode> {
    let known: HashSet<&str> = known_agents.into_iter().collect();
    let mut modes = HashMap::new();
    for pack in packs {
        let Some(declared) = pack.user_ledger() else {
            continue;
        };
        for (agent_id, mode) in declared.agents() {
            if modes.contains_key(agent_id) {
                continue;
            }
            let Some(mode) = LedgerMode::parse(mode) else {
                continue;
            };
            if !known.contains(agent_id.as_str()) {
                tracing::warn!(
                    agent = ?agent_id,
                    "user-ledger: sidecar names an agent this runtime does not carry"
                );
            }
            modes.insert(agent_id.clone(), mode);
        }
    }
    modes
}

/// The binding for one runtime, or `None` (off). `tenant` must be the tenant
/// this runtime's turns run under: the binding refuses any other.
pub(crate) fn user_ledger_binding<'a>(
    tenant: &str,
    packs: &[Arc<crate::pack::PackRuntime>],
    known_agents: impl IntoIterator<Item = &'a str>,
    target: Option<UserLedgerTarget>,
) -> Option<Arc<UserLedgerBinding>> {
    let target = target?;
    let modes = ledger_modes_from_packs(packs, known_agents);
    if modes.is_empty() {
        return None;
    }
    let ledger: Arc<dyn UserLedger> = match HttpUserLedger::new(target) {
        Ok(client) => Arc::new(client),
        Err(error) => {
            // `UserLedgerTargetError` names the class of fault only; it never
            // carries the URL or the token.
            tracing::warn!(%error, "user ledger is off: its target is not usable");
            return None;
        }
    };
    tracing::info!(agents = modes.len(), "user ledger binding constructed");
    Some(Arc::new(UserLedgerBinding::new(tenant, ledger, modes)))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use greentic_aw_runtime::user_ledger::UserLedgerTarget;

    fn pack_with(sidecar: Option<&str>) -> (tempfile::TempDir, Arc<crate::pack::PackRuntime>) {
        let dir = tempfile::tempdir().unwrap();
        if let Some(body) = sidecar {
            std::fs::create_dir_all(dir.path().join("assets")).unwrap();
            std::fs::write(dir.path().join("assets/user-ledger.json"), body).unwrap();
        }
        let pack = Arc::new(crate::pack::tests::pack_runtime_for_dir(dir.path()));
        (dir, pack)
    }

    fn target(base: &str) -> Option<UserLedgerTarget> {
        Some(UserLedgerTarget {
            base_url: base.into(),
            token: "gtm_t".into(),
            tenant_slug: "alpha".into(),
        })
    }

    #[test]
    fn a_target_and_a_sidecar_naming_a_known_agent_build_a_binding() {
        let (_d, pack) = pack_with(Some(r#"{"helper":"read_write"}"#));
        let binding = user_ledger_binding(
            "acme",
            &[pack],
            ["helper"],
            target("https://admin.example/api/v1/ingest/ledger"),
        );
        assert!(binding.is_some());
    }

    #[test]
    fn missing_target_missing_sidecar_or_unsafe_target_is_off() {
        let (_d1, with) = pack_with(Some(r#"{"helper":"read"}"#));
        let (_d2, without) = pack_with(None);
        let (_d3, nobody) = pack_with(Some(r#"{"helper":"none"}"#));
        assert!(
            user_ledger_binding("acme", std::slice::from_ref(&with), ["helper"], None).is_none()
        );
        assert!(
            user_ledger_binding(
                "acme",
                &[without],
                ["helper"],
                target("https://a.example/l")
            )
            .is_none()
        );
        assert!(
            user_ledger_binding("acme", &[nobody], ["helper"], target("https://a.example/l"))
                .is_none(),
            "a sidecar granting nobody is off"
        );
        // Cleartext off loopback, userinfo, and a blank token are refused by
        // the client's own validation: no binding, never a boot failure.
        for unsafe_base in [
            "http://a.example/l",
            "https://user:pw@a.example/l",
            "ftp://a.example/l",
            "not a url",
        ] {
            assert!(
                user_ledger_binding(
                    "acme",
                    std::slice::from_ref(&with),
                    ["helper"],
                    target(unsafe_base)
                )
                .is_none(),
                "{unsafe_base} must not build a binding"
            );
        }
        let blank_token = Some(UserLedgerTarget {
            base_url: "https://a.example/l".into(),
            token: "  ".into(),
            tenant_slug: "alpha".into(),
        });
        assert!(user_ledger_binding("acme", &[with], ["helper"], blank_token).is_none());
    }

    #[test]
    fn the_first_pack_naming_an_agent_wins() {
        let (_d1, p1) = pack_with(Some(r#"{"helper":"read"}"#));
        let (_d2, p2) = pack_with(Some(r#"{"helper":"read_write","other":"read_write"}"#));
        let modes = ledger_modes_from_packs(&[p1, p2], ["helper"]);
        assert_eq!(
            modes.get("helper"),
            Some(&greentic_aw_runtime::LedgerMode::Read)
        );
        assert_eq!(
            modes.get("other"),
            Some(&greentic_aw_runtime::LedgerMode::ReadWrite),
            "an agent only a later pack names still gets that pack's mode"
        );
        assert_eq!(modes.len(), 2);
    }

    /// The binding's tenant is the one the host passed: the binding refuses
    /// every turn of another tenant, so a wrong value here is a ledger that
    /// silently never runs.
    #[test]
    #[serial_test::serial]
    #[allow(unsafe_code)]
    fn the_binding_is_bound_to_the_runtimes_tenant() {
        // `turn_for` reads the kill switch; clear it so the shell cannot
        // decide (SAFETY: #[serial] serializes env-mutating tests).
        unsafe { std::env::remove_var("GREENTIC_AW_USER_LEDGER") };
        let (_d, pack) = pack_with(Some(r#"{"helper":"read"}"#));
        let binding = user_ledger_binding(
            "acme",
            &[pack],
            ["helper"],
            target("https://admin.example/api/v1/ingest/ledger"),
        )
        .unwrap();
        let caller = greentic_aw_runtime::VerifiedCaller {
            user_verified: true,
            sub: Some("alice".into()),
            ..Default::default()
        };
        let acme =
            greentic_aw_runtime::TenantContext::new("acme", "e").with_caller(Some(caller.clone()));
        let other = greentic_aw_runtime::TenantContext::new("other", "e").with_caller(Some(caller));
        assert!(binding.turn_for(&acme, "helper").is_some());
        assert!(binding.turn_for(&other, "helper").is_none());
        assert!(
            binding.turn_for(&acme, "stranger").is_none(),
            "an agent the sidecar does not name"
        );
    }

    /// The binding's Debug never prints the door URL or the token.
    #[test]
    fn the_binding_debug_carries_no_url_or_token() {
        let (_d, pack) = pack_with(Some(r#"{"helper":"read"}"#));
        let binding = user_ledger_binding(
            "acme",
            &[pack],
            ["helper"],
            Some(UserLedgerTarget {
                base_url: "https://admin.example/secret-path/ledger".into(),
                token: "gtm_SECRET".into(),
                tenant_slug: "alpha".into(),
            }),
        )
        .unwrap();
        let text = format!("{binding:?}");
        assert!(!text.contains("gtm_SECRET"), "{text}");
        assert!(!text.contains("secret-path"), "{text}");
    }

    /// Every agent-runtime build in `from_packs_with_rollout` (two
    /// `build_agent_node_wiring_metered(`, one
    /// `build_agent_node_wiring_ephemeral_metered(`) must receive the target,
    /// and the host's option must reach that function: a dropped argument only
    /// shows as a ledger that never runs.
    #[test]
    fn every_wiring_call_in_runtime_rs_receives_the_target() {
        // Whitespace-normalised, so rustfmt and re-indentation cannot break it.
        let src = include_str!("../runtime.rs")
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        let mut calls = 0;
        for needle in [
            "build_agent_node_wiring_metered(",
            "build_agent_node_wiring_ephemeral_metered(",
        ] {
            for (start, _) in src.match_indices(needle) {
                let rest = &src[start..];
                let end = rest.find(".await").expect("call has an .await");
                assert!(
                    rest[..end].contains("user_ledger.clone(),"),
                    "`{needle}` must receive the host's ledger target: {}",
                    &rest[..end]
                );
                calls += 1;
            }
        }
        assert_eq!(calls, 3, "the wiring call sites moved; re-check them");
        assert!(src.contains("options.user_ledger,"), "load_revision_with");
        assert_eq!(
            src.matches("user_ledger: Option<").count(),
            3,
            "the option field and both forwarding parameters"
        );
        assert!(
            src.contains("approval_inbox, #[cfg(feature = \"agentic-worker\")] user_ledger, )"),
            "load_revision_impl must forward the target to from_packs_with_rollout"
        );
    }
}
