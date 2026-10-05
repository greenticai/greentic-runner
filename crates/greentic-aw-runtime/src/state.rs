//! Conversation state + persistence trait + session lock.
//!
//! The state struct is JSON-serialised into Redis under the key
//! `aw:{tenant}:{env}:{session}:state`. `schema_version` is the FIRST
//! field so older readers can fail fast on incompatible bumps.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::a2a_source::A2aContinuations;
use crate::error::StateError;
use crate::tenant::TenantContext;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

/// Current schema version emitted on save. Bump when [`ConversationState`]
/// shape changes in a way that older runners cannot decode.
pub const STATE_SCHEMA_VERSION: u32 = 1;

/// Full conversation state persisted per session.
///
/// Serialised to JSON and stored in Redis at
/// `aw:{tenant}:{env}:{session}:state`. `schema_version` is always the
/// first field so readers can detect and reject incompatible shapes
/// before attempting to decode the rest of the payload.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConversationState {
    pub schema_version: u32,
    pub session_id: String,
    pub tenant_id: String,
    pub env_id: String,
    pub messages: Vec<ChatMessage>,
    /// Remote A2A references this conversation holds, per agent, so a
    /// follow-up resumes the same remote task instead of starting a new one.
    ///
    /// `#[serde(default)]` and NOT a `schema_version` bump: an older runner
    /// reading a state written by a newer one simply ignores the field, and
    /// `load` rejects only a version GREATER than its own — so bumping would
    /// take every older instance in a mixed fleet offline for a field they do
    /// not need. The cost of not bumping is that an old runner drops the
    /// continuations on its next save, which loses at worst one remote
    /// conversation's thread.
    #[serde(default)]
    pub a2a: A2aContinuations,
    /// A `flow:` tool call that parked on the user (a card awaiting its
    /// submit). While set, the conversation's history ends in an assistant
    /// turn whose `call_id` has no tool result yet; the next step either
    /// resumes the flow with the user's answer or cancels it, and in both
    /// cases answers that `call_id` before anything else is sent to the LLM.
    ///
    /// `#[serde(default)]` for the same fleet-compatibility reason as `a2a`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_tool: Option<PendingToolCall>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ConversationState {
    /// Create a fresh, empty state for the given tenant + session pair.
    pub fn empty(tenant: &TenantContext, session_id: &str) -> Self {
        let now = Utc::now();
        Self {
            schema_version: STATE_SCHEMA_VERSION,
            session_id: session_id.to_string(),
            tenant_id: tenant.tenant_id.clone(),
            env_id: tenant.env_id.clone(),
            messages: Vec::new(),
            a2a: A2aContinuations::default(),
            pending_tool: None,
            created_at: now,
            updated_at: now,
        }
    }

    /// Truncate oldest user-assistant pairs until the count of non-system
    /// messages is at or below `max_turns`.
    ///
    /// System messages are preserved relative to neighbours — truncation
    /// drops the oldest non-system message first, repeatedly, until the
    /// target is reached.
    pub fn truncate_history(&mut self, max_turns: u32) {
        let max = max_turns as usize;
        while self
            .messages
            .iter()
            .filter(|m| !matches!(m, ChatMessage::System { .. }))
            .count()
            > max
        {
            if let Some(position) = self
                .messages
                .iter()
                .position(|m| !matches!(m, ChatMessage::System { .. }))
            {
                self.messages.remove(position);
            } else {
                break;
            }
        }
    }
}

/// How long a parked flow tool waits for the user before the next turn
/// cancels it instead of resuming it. Idle expiry, like the A2A continuation
/// TTL: every re-park (the flow asked again) refreshes it.
pub const PENDING_TOOL_IDLE_TTL_SECS: i64 = 60 * 60;

/// Side turns answered while one tool stays parked, before a typed message
/// cancels it instead.
pub const MAX_SIDE_TURNS: u32 = 20;

/// A side turn refreshes the idle expiry but never beyond this age of the park.
pub const PENDING_TOOL_MAX_AGE_SECS: i64 = 24 * 60 * 60;

/// A `flow:` tool call suspended on the user. See
/// [`ConversationState::pending_tool`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PendingToolCall {
    /// The LLM's call id; the eventual tool result is recorded under it.
    pub call_id: String,
    /// The tool name the LLM called (for the trail and observers).
    pub tool_name: String,
    /// The `extension_id` the LLM called (`flow:<flow_ref>`).
    #[serde(default)]
    pub extension_id: String,
    /// The arguments the LLM called the tool with (for the trail).
    #[serde(default)]
    pub args: serde_json::Value,
    /// The flow being run, without the `flow:` prefix.
    pub flow_ref: String,
    /// Opaque host snapshot of the parked flow, handed back to
    /// [`crate::FlowInvoker::resume`] verbatim.
    pub flow_snapshot: serde_json::Value,
    /// Plan-Act-Observe iterations the suspended turn had already spent, so
    /// the resumed turn continues the budget rather than restarting it.
    pub iterations_used: u32,
    /// When this suspension stops being resumable.
    pub expires_at: DateTime<Utc>,
    /// The card the flow parked on, kept so a side turn can re-offer it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub presentation: Option<serde_json::Value>,
    /// When the park began (caps how far side turns can extend `expires_at`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parked_at: Option<DateTime<Utc>>,
    /// Side turns answered since the park.
    #[serde(default)]
    pub side_turns: u32,
}

impl PendingToolCall {
    /// Expiry `PENDING_TOOL_IDLE_TTL_SECS` from `now`.
    pub fn expiry_from(now: DateTime<Utc>) -> DateTime<Utc> {
        now + chrono::Duration::seconds(PENDING_TOOL_IDLE_TTL_SECS)
    }

    /// Whether the suspension has lapsed at `now`.
    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        now >= self.expires_at
    }
}

/// A single message in the conversation history.
///
/// Tagged with `"role"` in JSON for compatibility with LLM provider
/// message formats.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "role", rename_all = "snake_case")]
pub enum ChatMessage {
    System {
        content: String,
    },
    User {
        content: String,
    },
    Assistant {
        content: String,
        tool_calls: Vec<ToolCallRecord>,
    },
    Tool {
        call_id: String,
        content: serde_json::Value,
    },
}

/// Record of a single tool invocation appended to the assistant turn.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolCallRecord {
    pub call_id: String,
    pub extension_id: String,
    pub tool_name: String,
    pub args: serde_json::Value,
}

/// Persists and locks conversation state. `state_redis.rs` provides
/// the production impl (Phase 2); tests use [`crate::mock::MockAgentStateStore`].
///
/// **Dyn-safety:** stored as `Arc<dyn AgentStateStore>` by `AgentRuntime`,
/// so every async method uses `Pin<Box<dyn Future>>` return types instead
/// of bare `async fn`, which is not object-safe in Rust 1.95.
pub trait AgentStateStore: Send + Sync {
    /// Load the conversation state for the given session.
    ///
    /// Returns an empty, initialised [`ConversationState`] when no
    /// persisted record exists — callers never receive `None`.
    fn load<'a>(
        &'a self,
        tenant: &'a TenantContext,
        session_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<ConversationState, StateError>> + Send + 'a>>;

    /// Persist conversation state.
    ///
    /// Implementations refresh the session TTL on every call (7 days
    /// for the Redis impl).
    fn save<'a>(
        &'a self,
        tenant: &'a TenantContext,
        session_id: &'a str,
        state: &'a ConversationState,
    ) -> Pin<Box<dyn Future<Output = Result<(), StateError>> + Send + 'a>>;

    /// Acquire a distributed lock for the session.
    ///
    /// Returns an RAII [`SessionLock`] guard on success; `Drop` releases
    /// the lock (best-effort). The Redis SET-NX TTL of 90 s is the
    /// safety net for crashed workers. Blocks for at most `wait` before
    /// returning [`StateError::LockTimeout`].
    fn acquire_lock<'a>(
        &'a self,
        tenant: &'a TenantContext,
        session_id: &'a str,
        wait: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<SessionLock, StateError>> + Send + 'a>>;
}

/// RAII handle holding a per-session distributed lock.
///
/// `Drop` releases the underlying Redis key best-effort. The lock TTL
/// is 90 s; callers **MUST** call [`SessionLock::refresh`] once per
/// loop iteration to extend the window and avoid spurious expiry.
pub struct SessionLock {
    pub(crate) inner: Box<dyn SessionLockInner>,
}

impl SessionLock {
    /// Wrap a concrete lock implementation in the public RAII guard.
    #[allow(dead_code)] // consumed by state_redis.rs and mock.rs in later tasks
    pub(crate) fn new(inner: Box<dyn SessionLockInner>) -> Self {
        Self { inner }
    }

    /// Extend the TTL by another 90 s window.
    ///
    /// On error the loop should log and continue — losing the extension
    /// is preferable to aborting a partially-complete turn.
    pub async fn refresh(&self) -> Result<(), StateError> {
        self.inner.refresh().await
    }
}

impl Drop for SessionLock {
    fn drop(&mut self) {
        self.inner.release();
    }
}

/// Sealed inner trait — implementors live in `state_redis.rs` and `mock.rs`.
///
/// The trait is not `pub` in the crate-external sense; `SessionLock` owns a
/// `Box<dyn SessionLockInner>` and is the only public-facing handle.
pub trait SessionLockInner: Send + Sync {
    /// Async TTL refresh, returned as a boxed future for object safety.
    fn refresh<'a>(&'a self) -> Pin<Box<dyn Future<Output = Result<(), StateError>> + Send + 'a>>;

    /// Best-effort synchronous release called from [`SessionLock::drop`].
    fn release(&self);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(clippy::unwrap_used, clippy::expect_used)]
    /// A state saved before `pending_tool` existed (no key at all) must still
    /// load, as "nothing pending"; and an empty one must not write the key.
    #[test]
    fn state_without_pending_tool_deserializes_and_omits_it_on_save() {
        let legacy = serde_json::json!({
            "schema_version": 1,
            "session_id": "s",
            "tenant_id": "t",
            "env_id": "e",
            "messages": [],
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z"
        });
        let state: ConversationState = serde_json::from_value(legacy).expect("legacy state");
        assert!(state.pending_tool.is_none());
        let saved = serde_json::to_value(&state).expect("serialize");
        assert!(saved.get("pending_tool").is_none());
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn pending_tool_round_trips_and_expires() {
        let now = Utc::now();
        let pending = PendingToolCall {
            call_id: "c1".into(),
            tool_name: "form".into(),
            extension_id: "flow:form".into(),
            args: serde_json::json!({}),
            flow_ref: "form".into(),
            flow_snapshot: serde_json::json!({ "next_node": "card" }),
            iterations_used: 2,
            expires_at: PendingToolCall::expiry_from(now),
            presentation: Some(serde_json::json!({ "card": "A" })),
            parked_at: Some(now),
            side_turns: 3,
        };
        let back: PendingToolCall =
            serde_json::from_value(serde_json::to_value(&pending).unwrap()).unwrap();
        assert_eq!(back, pending);
        assert!(!pending.is_expired(now));
        assert!(pending.is_expired(now + chrono::Duration::seconds(PENDING_TOOL_IDLE_TTL_SECS)));
    }

    #[test]
    fn empty_state_has_schema_version_1() {
        let tenant_context = TenantContext::new("a", "b");
        let conversation_state = ConversationState::empty(&tenant_context, "sess");
        assert_eq!(conversation_state.schema_version, STATE_SCHEMA_VERSION);
        assert_eq!(conversation_state.schema_version, 1);
        assert_eq!(conversation_state.session_id, "sess");
        assert_eq!(conversation_state.tenant_id, "a");
        assert_eq!(conversation_state.env_id, "b");
        assert!(conversation_state.messages.is_empty());
        assert!(conversation_state.a2a.is_empty());
    }

    #[test]
    #[allow(clippy::expect_used)]
    fn state_written_before_a2a_continuations_existed_still_decodes() {
        // The reason the field is `#[serde(default)]` and the schema version
        // stayed at 1: every row already in a live store predates it.
        let json = r#"{
            "schema_version": 1,
            "session_id": "s",
            "tenant_id": "acme",
            "env_id": "prod",
            "messages": [],
            "created_at": "2026-09-01T00:00:00Z",
            "updated_at": "2026-09-01T00:00:00Z"
        }"#;
        let state: ConversationState =
            serde_json::from_str(json).expect("pre-existing state must still decode");
        assert_eq!(state.schema_version, STATE_SCHEMA_VERSION);
        assert!(state.a2a.is_empty());
    }

    #[test]
    #[allow(clippy::panic)] // diagnostic branch in test — intentional
    fn truncate_history_drops_oldest_non_system_first() {
        let tenant_context = TenantContext::new("a", "b");
        let mut conversation_state = ConversationState::empty(&tenant_context, "x");
        conversation_state.messages.push(ChatMessage::System {
            content: "sys".into(),
        });
        conversation_state.messages.push(ChatMessage::User {
            content: "u1".into(),
        });
        conversation_state.messages.push(ChatMessage::Assistant {
            content: "a1".into(),
            tool_calls: vec![],
        });
        conversation_state.messages.push(ChatMessage::User {
            content: "u2".into(),
        });
        conversation_state.messages.push(ChatMessage::Assistant {
            content: "a2".into(),
            tool_calls: vec![],
        });

        conversation_state.truncate_history(2);

        // System always preserved; only u2 + a2 kept among non-system messages.
        assert_eq!(conversation_state.messages.len(), 3);
        assert!(matches!(
            conversation_state.messages[0],
            ChatMessage::System { .. }
        ));
        if let ChatMessage::User { content } = &conversation_state.messages[1] {
            assert_eq!(content, "u2");
        } else {
            panic!("expected User u2 at position 1");
        }
    }

    #[test]
    fn a_pending_tool_stored_before_side_turns_still_deserialises() {
        let old = r#"{"call_id":"c","tool_name":"t","flow_ref":"f","flow_snapshot":{},"iterations_used":1,"expires_at":"2026-10-05T00:00:00Z"}"#;
        let p: PendingToolCall = serde_json::from_str(old).unwrap();
        assert!(p.presentation.is_none() && p.parked_at.is_none() && p.side_turns == 0);
    }
}
