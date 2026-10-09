//! What an `in_progress` event says the run is waiting for (`wait_kind`) and
//! until when (`response_due_at`) — audit wire contract v2 §1 and §4.
//!
//! # Which node parked
//!
//! The engine parks right after dispatching a node, so the parking node is the
//! LAST node the observer saw start (`ObservedTurn::parked`). That is the card
//! whose routing fell through, the `session.wait` node (whose snapshot names
//! its SUCCESSOR, so the snapshot's `next_node` would be the wrong node to ask),
//! the conversational `dw.agent`, or the dispatch node awaiting its runtime.
//!
//! # Kind
//!
//! - the parking node is an `approval.call` → [`WaitKind::Approval`], whether
//!   it is awaiting its first decision (a remote dispatch) or re-parked;
//! - the wait reason is a remote dispatch in flight (`await-runtime:<id>`,
//!   written by the sorla / operala / agentic / telco-x dispatch) →
//!   [`WaitKind::Processing`];
//! - anything else (a card, `session.wait`, a conversational agent waiting for
//!   the next message) → [`WaitKind::UserInput`].
//!
//! # Deadline
//!
//! Only a user-input wait has one: `now + timeout`, where timeout is the
//! parking node's `response_timeout_secs` (a positive integer at the root of
//! its compiled `input.mapping`), else `GREENTIC_RUNNER_SESSION_WAIT_TTL_SECS`
//! when it is a positive integer, else 86 400 s. The env var's `0` ("the
//! session store never expires a wait") is not a deadline, so it falls
//! through to the default too.

use chrono::{DateTime, Utc};

use super::{WaitKind, WaitState};
use crate::storage::config::ENV_SESSION_WAIT_TTL_SECS;

/// The deadline when neither the node nor the environment names one.
pub(crate) const DEFAULT_RESPONSE_TIMEOUT_SECS: u64 = 24 * 60 * 60;

/// Prefix of the wait reason every remote-dispatch park writes
/// (`FlowEngine::dispatch_remote`).
const REMOTE_AWAIT_REASON_PREFIX: &str = "await-runtime:";

/// The node a turn parked at, as the observer saw it start.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ParkedNode {
    pub(crate) approval: bool,
    pub(crate) response_timeout_secs: Option<u64>,
}

/// Classify a park. Pure: see the module doc.
pub(crate) fn wait_kind(reason: Option<&str>, parked: Option<ParkedNode>) -> WaitKind {
    if parked.is_some_and(|node| node.approval) {
        WaitKind::Approval
    } else if reason.is_some_and(|r| r.starts_with(REMOTE_AWAIT_REASON_PREFIX)) {
        WaitKind::Processing
    } else {
        WaitKind::UserInput
    }
}

/// The timeout, in seconds, a user-input wait at `parked` runs for.
pub(crate) fn response_timeout_secs(parked: Option<ParkedNode>, env_ttl: Option<&str>) -> u64 {
    parked
        .and_then(|node| node.response_timeout_secs)
        .filter(|secs| *secs > 0)
        .or_else(|| {
            env_ttl
                .map(str::trim)
                .and_then(|raw| raw.parse::<u64>().ok())
                .filter(|secs| *secs > 0)
        })
        .unwrap_or(DEFAULT_RESPONSE_TIMEOUT_SECS)
}

/// [`wait_state`] with the clock and the environment passed in.
pub(crate) fn wait_state_at(
    reason: Option<&str>,
    parked: Option<ParkedNode>,
    now: DateTime<Utc>,
    env_ttl: Option<&str>,
) -> WaitState {
    let kind = wait_kind(reason, parked);
    let response_due_at = (kind == WaitKind::UserInput).then(|| {
        let secs = response_timeout_secs(parked, env_ttl);
        // Clamp so an absurd configured value cannot overflow the clock.
        let secs = i64::try_from(secs)
            .unwrap_or(i64::MAX)
            .min(100 * 365 * 86_400);
        (now + chrono::Duration::seconds(secs)).to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
    });
    WaitState {
        kind,
        response_due_at,
    }
}

/// The wait an `in_progress` event reports for a park with `reason` at
/// `parked`, as of now.
pub(crate) fn wait_state(reason: Option<&str>, parked: Option<ParkedNode>) -> WaitState {
    let env_ttl = std::env::var(ENV_SESSION_WAIT_TTL_SECS).ok();
    wait_state_at(reason, parked, Utc::now(), env_ttl.as_deref())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn at() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-09-26T10:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn an_approval_gate_is_an_approval_wait_even_while_its_dispatch_is_in_flight() {
        let parked = Some(ParkedNode {
            approval: true,
            response_timeout_secs: Some(60),
        });
        let state = wait_state_at(Some("await-runtime:abc"), parked, at(), None);
        assert_eq!(state.kind, WaitKind::Approval);
        assert_eq!(state.response_due_at, None, "approval never drops off");
    }

    #[test]
    fn a_remote_dispatch_is_processing_without_a_deadline() {
        let state = wait_state_at(
            Some("await-runtime:abc"),
            Some(ParkedNode::default()),
            at(),
            None,
        );
        assert_eq!(state.kind, WaitKind::Processing);
        assert_eq!(state.response_due_at, None);
    }

    #[test]
    fn a_user_wait_uses_the_nodes_timeout_first() {
        let parked = Some(ParkedNode {
            approval: false,
            response_timeout_secs: Some(600),
        });
        let state = wait_state_at(
            Some("awaiting user submit at node `ask`"),
            parked,
            at(),
            Some("30"),
        );
        assert_eq!(state.kind, WaitKind::UserInput);
        assert_eq!(
            state.response_due_at.as_deref(),
            Some("2026-09-26T10:10:00.000Z")
        );
    }

    #[test]
    fn a_user_wait_falls_back_to_the_env_then_a_day() {
        let state = wait_state_at(None, None, at(), Some("120"));
        assert_eq!(
            state.response_due_at.as_deref(),
            Some("2026-09-26T10:02:00.000Z")
        );
        for env in [None, Some("0"), Some("-5"), Some("soon"), Some("")] {
            let state = wait_state_at(None, Some(ParkedNode::default()), at(), env);
            assert_eq!(
                state.response_due_at.as_deref(),
                Some("2026-09-27T10:00:00.000Z"),
                "env {env:?}"
            );
        }
    }

    #[test]
    fn an_absurd_timeout_does_not_overflow() {
        let parked = Some(ParkedNode {
            approval: false,
            response_timeout_secs: Some(u64::MAX),
        });
        assert!(
            wait_state_at(None, parked, at(), None)
                .response_due_at
                .is_some()
        );
    }
}
