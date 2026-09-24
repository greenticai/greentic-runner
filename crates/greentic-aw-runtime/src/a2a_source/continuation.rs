//! What one Greentic conversation remembers about the remote A2A agents it
//! has talked to, so a second call continues the first instead of starting
//! over.
//!
//! # Why this lives in [`crate::state::ConversationState`]
//!
//! A remote `contextId` is per-conversation state by definition: it is the
//! remote agent's name for "this exchange with this caller". The worker
//! already persists exactly one such thing — `ConversationState`, keyed
//! `aw:{tenant}:{env}:{session}:state` — so this rides in there rather than
//! in a second store of its own. Three properties follow for free and would
//! each have to be rebuilt otherwise: it survives a process restart on the
//! durable backends, it is shared across instances on Redis, and it cannot
//! be read by another tenant, because the tenant is a segment of the key.
//!
//! # The mapping (design §7)
//!
//! | A2A | Greentic |
//! |---|---|
//! | `contextId` | this conversation ↔ this agent, for as long as it is live |
//! | `taskId` | ONE remote execution, only while it is still open |
//!
//! Those two have different lifetimes and conflating them is the bug this
//! module is shaped to avoid. A `taskId` is dropped the moment the remote
//! task reaches any end state — a completed task will not accept another
//! message, so sending its id again is at best ignored and at worst an
//! error. The `contextId` outlives it, which is what makes the NEXT question
//! land in the same remote thread rather than in a fresh one.

use std::collections::BTreeMap;

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

use crate::tenant::TenantContext;

/// How long a remote reference is kept after the last call that used it.
///
/// **The lifetime rule.** A continuation is *idle-expiring*: every successful
/// call refreshes it, and one hour after the last call it is dropped and the
/// next call starts a fresh remote context.
///
/// One hour is chosen from both ends. An `input-required` exchange is a human
/// answering a question — minutes, occasionally tens of minutes if they walk
/// away from the chat — so anything much shorter would break the very case
/// this exists for. In the other direction a remote agent's own task and
/// context retention is not ours to know, and sending a reference the remote
/// has already collected is worse than sending none: depending on the agent
/// it is an error, or it silently opens a new task while we believe we
/// resumed one. An hour keeps the window where a stale reference is likely
/// small, at the cost of an occasional new context for a conversation that
/// went quiet.
pub const CONTINUATION_IDLE_TTL_SECS: i64 = 60 * 60;

/// How many agents one conversation remembers a context for.
///
/// A worker may be bound to many A2A agents, and this map is serialised into
/// the conversation state on every save. Without a cap a long-running
/// conversation that fans out across agents grows that blob without bound.
/// Sixteen is comfortably above any realistic binding count; past it the
/// least recently used entry is dropped, which costs a fresh context on the
/// next call to that agent and nothing else.
const MAX_REMEMBERED_AGENTS: usize = 16;

/// The remote references one conversation holds for ONE A2A agent.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct A2aContinuation {
    /// The remote agent's id for this conversation. Sent on every follow-up.
    pub context_id: String,
    /// The remote task still awaiting us, when there is one. `None` once the
    /// task reached any end state — see the module docs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    /// The tenant this reference was minted under.
    ///
    /// Redundant with the state key on every ordinary path, and deliberately
    /// so. A conversation state blob is a serialised value: it can be
    /// restored from a backup, copied by a support tool, or written by a
    /// caller that assembled it itself ([`crate::state::ConversationState`]
    /// is public and the designer builds one). The key protects the store;
    /// this protects the value, so a blob that reached the wrong tenant
    /// cannot make that tenant send a credentialed message into another
    /// tenant's remote conversation.
    pub tenant_id: String,
    /// The environment this reference was minted under. Same reasoning as
    /// `tenant_id`: `dev` and `prod` are different worlds.
    pub env_id: String,
    /// When the last successful call used this reference. The idle TTL is
    /// measured from here.
    pub last_used_at: DateTime<Utc>,
}

impl A2aContinuation {
    /// Whether this reference was minted by `tenant` in the same environment.
    fn belongs_to(&self, tenant: &TenantContext) -> bool {
        self.tenant_id == tenant.tenant_id && self.env_id == tenant.env_id
    }

    /// Whether `now` is still inside the idle window.
    fn is_fresh(&self, now: DateTime<Utc>) -> bool {
        now.signed_duration_since(self.last_used_at) < Duration::seconds(CONTINUATION_IDLE_TTL_SECS)
    }
}

/// Every remote agent this conversation is mid-exchange with, keyed by the
/// `agent_id` of its `a2a:<agent_id>` tool ref.
///
/// `BTreeMap` rather than `HashMap` so the serialised form is stable: this is
/// persisted on every turn, and a map that reorders itself makes every save a
/// diff and every test fixture order-dependent.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct A2aContinuations {
    by_agent: BTreeMap<String, A2aContinuation>,
}

impl A2aContinuations {
    /// The live continuation for `agent_id`, if this conversation has one.
    ///
    /// Takes `&mut self` because a lookup is also the eviction point: a
    /// reference that has gone stale, or that belongs to another tenant, is
    /// REMOVED here rather than merely skipped. Leaving it would keep
    /// answering `None` for the rest of the conversation while still paying
    /// for it in every state save, and — for the foreign case — would leave
    /// another tenant's identifier sitting in this tenant's state.
    pub fn resume(
        &mut self,
        tenant: &TenantContext,
        agent_id: &str,
        now: DateTime<Utc>,
    ) -> Option<A2aContinuation> {
        let entry = self.by_agent.get(agent_id)?;
        if !entry.belongs_to(tenant) {
            tracing::warn!(
                agent = %agent_id,
                minted_for = %entry.tenant_id,
                running_as = %tenant.tenant_id,
                "discarding an a2a continuation minted under another tenant"
            );
            self.by_agent.remove(agent_id);
            return None;
        }
        if !entry.is_fresh(now) {
            self.by_agent.remove(agent_id);
            return None;
        }
        Some(entry.clone())
    }

    /// Record what the remote agent answered with, so the next call to
    /// `agent_id` in this conversation continues it.
    ///
    /// `task_id` is `None` whenever the remote task is not open — the caller
    /// has already applied [`greentic_a2a::message::TaskState::progress`], so
    /// this type never has to know the protocol.
    pub fn remember(
        &mut self,
        tenant: &TenantContext,
        agent_id: &str,
        context_id: String,
        task_id: Option<String>,
        now: DateTime<Utc>,
    ) {
        self.by_agent.insert(
            agent_id.to_string(),
            A2aContinuation {
                context_id,
                task_id,
                tenant_id: tenant.tenant_id.clone(),
                env_id: tenant.env_id.clone(),
                last_used_at: now,
            },
        );
        self.evict_to_cap(agent_id);
    }

    /// Forget everything about `agent_id`, so the next call starts fresh.
    pub fn forget(&mut self, agent_id: &str) {
        self.by_agent.remove(agent_id);
    }

    /// How many agents are remembered. Bounded by [`MAX_REMEMBERED_AGENTS`].
    #[must_use]
    pub fn len(&self) -> usize {
        self.by_agent.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_agent.is_empty()
    }

    /// The stored entry for `agent_id` without the freshness or ownership
    /// checks. Only for tests that need to see what was written.
    #[cfg(test)]
    pub(crate) fn peek(&self, agent_id: &str) -> Option<&A2aContinuation> {
        self.by_agent.get(agent_id)
    }

    /// Drop least-recently-used entries until the cap holds. `keep` is the
    /// entry just written, which must survive even if it is somehow the
    /// oldest by timestamp (a caller passing a `now` in the past).
    fn evict_to_cap(&mut self, keep: &str) {
        while self.by_agent.len() > MAX_REMEMBERED_AGENTS {
            let oldest = self
                .by_agent
                .iter()
                .filter(|(agent, _)| agent.as_str() != keep)
                .min_by_key(|(_, entry)| entry.last_used_at)
                .map(|(agent, _)| agent.clone());
            match oldest {
                Some(agent) => {
                    self.by_agent.remove(&agent);
                }
                // Only reachable if the cap is 0 and `keep` is the sole entry.
                None => break,
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn tenant(id: &str, env: &str) -> TenantContext {
        TenantContext::new(id, env)
    }

    fn at(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_800_000_000 + secs, 0).expect("a valid timestamp")
    }

    #[test]
    fn a_remembered_context_comes_back_for_the_same_agent() {
        let tc = tenant("acme", "prod");
        let mut c = A2aContinuations::default();
        c.remember(&tc, "recipe", "ctx-1".into(), Some("t-1".into()), at(0));

        let resumed = c.resume(&tc, "recipe", at(10)).expect("still live");
        assert_eq!(resumed.context_id, "ctx-1");
        assert_eq!(resumed.task_id.as_deref(), Some("t-1"));
    }

    #[test]
    fn a_different_agent_in_the_same_conversation_has_its_own_context() {
        let tc = tenant("acme", "prod");
        let mut c = A2aContinuations::default();
        c.remember(&tc, "recipe", "ctx-recipe".into(), None, at(0));
        c.remember(&tc, "travel", "ctx-travel".into(), None, at(0));

        assert_eq!(
            c.resume(&tc, "recipe", at(1)).unwrap().context_id,
            "ctx-recipe"
        );
        assert_eq!(
            c.resume(&tc, "travel", at(1)).unwrap().context_id,
            "ctx-travel"
        );
    }

    #[test]
    fn a_context_minted_by_another_tenant_is_refused_and_dropped() {
        // The state key already scopes by tenant; this is the check that
        // survives a state blob arriving by some other route.
        let acme = tenant("acme", "prod");
        let other = tenant("globex", "prod");
        let mut c = A2aContinuations::default();
        c.remember(
            &acme,
            "recipe",
            "ctx-acme".into(),
            Some("t-1".into()),
            at(0),
        );

        assert!(c.resume(&other, "recipe", at(1)).is_none());
        assert!(
            c.peek("recipe").is_none(),
            "the foreign reference must be removed, not merely skipped"
        );
    }

    #[test]
    fn the_same_tenant_in_another_environment_is_a_different_world() {
        let prod = tenant("acme", "prod");
        let dev = tenant("acme", "dev");
        let mut c = A2aContinuations::default();
        c.remember(&prod, "recipe", "ctx-prod".into(), None, at(0));

        assert!(c.resume(&dev, "recipe", at(1)).is_none());
    }

    #[test]
    fn a_context_idle_past_its_ttl_is_dropped_so_the_next_call_starts_fresh() {
        let tc = tenant("acme", "prod");
        let mut c = A2aContinuations::default();
        c.remember(&tc, "recipe", "ctx-1".into(), Some("t-1".into()), at(0));

        assert!(
            c.resume(&tc, "recipe", at(CONTINUATION_IDLE_TTL_SECS - 1))
                .is_some(),
            "inside the window it must still resume"
        );
        assert!(
            c.resume(&tc, "recipe", at(CONTINUATION_IDLE_TTL_SECS))
                .is_none(),
            "at the window's edge it must expire"
        );
        assert!(c.peek("recipe").is_none(), "an expired entry is evicted");
    }

    #[test]
    fn every_call_refreshes_the_idle_window() {
        let tc = tenant("acme", "prod");
        let mut c = A2aContinuations::default();
        c.remember(&tc, "recipe", "ctx-1".into(), None, at(0));
        c.remember(
            &tc,
            "recipe",
            "ctx-1".into(),
            None,
            at(CONTINUATION_IDLE_TTL_SECS - 1),
        );

        assert!(
            c.resume(&tc, "recipe", at(CONTINUATION_IDLE_TTL_SECS + 1))
                .is_some(),
            "the TTL runs from the LAST call, not the first"
        );
    }

    #[test]
    fn the_map_is_capped_and_drops_the_least_recently_used() {
        let tc = tenant("acme", "prod");
        let mut c = A2aContinuations::default();
        // `agent-00` is the oldest; write one more than the cap.
        for n in 0..=MAX_REMEMBERED_AGENTS {
            c.remember(
                &tc,
                &format!("agent-{n:02}"),
                format!("ctx-{n}"),
                None,
                at(n as i64),
            );
        }
        assert_eq!(c.len(), MAX_REMEMBERED_AGENTS);
        assert!(c.peek("agent-00").is_none(), "the oldest must be evicted");
        assert!(
            c.peek(&format!("agent-{MAX_REMEMBERED_AGENTS:02}"))
                .is_some(),
            "the entry just written must survive"
        );
    }

    #[test]
    fn forgetting_an_agent_starts_the_next_call_fresh() {
        let tc = tenant("acme", "prod");
        let mut c = A2aContinuations::default();
        c.remember(&tc, "recipe", "ctx-1".into(), None, at(0));
        c.forget("recipe");
        assert!(c.resume(&tc, "recipe", at(1)).is_none());
    }

    #[test]
    fn an_empty_map_serialises_to_an_empty_object_so_old_state_still_decodes() {
        let json = serde_json::to_string(&A2aContinuations::default()).unwrap();
        assert_eq!(json, "{}");
        let back: A2aContinuations = serde_json::from_str("{}").unwrap();
        assert!(back.is_empty());
    }
}
