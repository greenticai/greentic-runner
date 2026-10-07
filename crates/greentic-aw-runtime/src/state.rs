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

    /// Truncate the oldest messages until the count of non-system messages is
    /// at or below `max_turns`.
    ///
    /// System messages are always kept. The unit of removal is a GROUP: an
    /// assistant turn carrying `tool_calls` goes together with the `Tool`
    /// results that answer it (a provider refuses a `tool` message with no
    /// preceding `tool_calls`, and an assistant `tool_calls` turn with no
    /// answer), and a stray `Tool` is removed alone. The group holding the
    /// parked flow tool's call is never removed, so the result stays
    /// answerable; if only protected groups remain, truncation stops short of
    /// `max_turns`.
    pub fn truncate_history(&mut self, max_turns: u32) {
        let max = max_turns as usize;
        let pending_id = self.pending_tool.as_ref().map(|p| p.call_id.clone());
        loop {
            let non_system = self
                .messages
                .iter()
                .filter(|m| !matches!(m, ChatMessage::System { .. }))
                .count();
            if non_system <= max {
                return;
            }
            let Some((start, end)) = self.oldest_removable_group(pending_id.as_deref()) else {
                return;
            };
            self.messages.drain(start..end);
        }
    }

    /// `[start, end)` of the oldest non-system group that does not hold
    /// `protected_call_id`.
    fn oldest_removable_group(&self, protected_call_id: Option<&str>) -> Option<(usize, usize)> {
        let mut i = 0;
        while i < self.messages.len() {
            if matches!(self.messages[i], ChatMessage::System { .. }) {
                i += 1;
                continue;
            }
            let mut end = i + 1;
            if matches!(&self.messages[i],
                ChatMessage::Assistant { tool_calls, .. } if !tool_calls.is_empty())
            {
                while matches!(self.messages.get(end), Some(ChatMessage::Tool { .. })) {
                    end += 1;
                }
            }
            let holds_protected = protected_call_id.is_some_and(|id| {
                self.messages[i..end].iter().any(|m| match m {
                    ChatMessage::Assistant { tool_calls, .. } => {
                        tool_calls.iter().any(|c| c.call_id == id)
                    }
                    ChatMessage::Tool { call_id, .. } => call_id == id,
                    _ => false,
                })
            });
            if !holds_protected {
                return Some((i, end));
            }
            i = end;
        }
        None
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
        /// References only (never bytes): the LLM backend resolves them for
        /// the current turn. Absent in state stored before attachments existed.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        attachments: Vec<crate::attachments::AttachmentRef>,
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
            attachments: Vec::new(),
        });
        conversation_state.messages.push(ChatMessage::Assistant {
            content: "a1".into(),
            tool_calls: vec![],
        });
        conversation_state.messages.push(ChatMessage::User {
            content: "u2".into(),
            attachments: Vec::new(),
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
        if let ChatMessage::User { content, .. } = &conversation_state.messages[1] {
            assert_eq!(content, "u2");
        } else {
            panic!("expected User u2 at position 1");
        }
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn a_pending_tool_stored_before_side_turns_still_deserialises() {
        let old = r#"{"call_id":"c","tool_name":"t","flow_ref":"f","flow_snapshot":{},"iterations_used":1,"expires_at":"2026-10-05T00:00:00Z"}"#;
        let p: PendingToolCall = serde_json::from_str(old).unwrap();
        assert!(p.presentation.is_none() && p.parked_at.is_none() && p.side_turns == 0);
    }

    fn user(text: &str) -> ChatMessage {
        ChatMessage::User {
            content: text.into(),
            attachments: Vec::new(),
        }
    }

    fn asst_text(text: &str) -> ChatMessage {
        ChatMessage::Assistant {
            content: text.into(),
            tool_calls: vec![],
        }
    }

    fn asst_calls(ids: &[&str]) -> ChatMessage {
        ChatMessage::Assistant {
            content: String::new(),
            tool_calls: ids
                .iter()
                .map(|id| ToolCallRecord {
                    call_id: (*id).into(),
                    extension_id: "flow:form".into(),
                    tool_name: "form".into(),
                    args: serde_json::json!({}),
                })
                .collect(),
        }
    }

    fn tool(id: &str) -> ChatMessage {
        ChatMessage::Tool {
            call_id: id.into(),
            content: serde_json::json!({}),
        }
    }

    /// Every assistant `tool_calls` id is answered by exactly one directly
    /// following `Tool`, and every `Tool` answers the assistant before it.
    fn pairing_is_valid(messages: &[ChatMessage]) -> bool {
        let mut i = 0;
        while i < messages.len() {
            match &messages[i] {
                ChatMessage::Assistant { tool_calls, .. } if !tool_calls.is_empty() => {
                    let mut wanted: Vec<&str> =
                        tool_calls.iter().map(|c| c.call_id.as_str()).collect();
                    let mut j = i + 1;
                    while let Some(ChatMessage::Tool { call_id, .. }) = messages.get(j) {
                        match wanted.iter().position(|w| w == call_id) {
                            Some(pos) => {
                                wanted.remove(pos);
                            }
                            None => return false,
                        }
                        j += 1;
                    }
                    if !wanted.is_empty() {
                        return false;
                    }
                    i = j;
                }
                ChatMessage::Tool { .. } => return false,
                _ => i += 1,
            }
        }
        true
    }

    fn pending_for(call_id: &str, now: DateTime<Utc>) -> PendingToolCall {
        PendingToolCall {
            call_id: call_id.into(),
            tool_name: "form".into(),
            extension_id: "flow:form".into(),
            args: serde_json::json!({}),
            flow_ref: "form".into(),
            flow_snapshot: serde_json::json!({}),
            iterations_used: 0,
            expires_at: PendingToolCall::expiry_from(now),
            presentation: None,
            parked_at: Some(now),
            side_turns: 0,
        }
    }

    #[test]
    fn truncate_never_leaves_an_orphan_tool_or_a_split_group() {
        let mut s = ConversationState::empty(&TenantContext::new("a", "b"), "x");
        s.messages = vec![
            user("a"),
            asst_calls(&["c1", "c2"]),
            tool("c1"),
            tool("c2"),
            user("b"),
            asst_text("r"),
        ];
        s.truncate_history(3);
        assert!(pairing_is_valid(&s.messages), "{:?}", s.messages);
        assert!(!matches!(
            s.messages.first(),
            Some(ChatMessage::Tool { .. })
        ));
    }

    #[test]
    fn truncate_keeps_the_group_holding_the_pending_call() {
        let mut s = ConversationState::empty(&TenantContext::new("a", "b"), "x");
        s.pending_tool = Some(pending_for("c1", Utc::now()));
        s.messages = vec![
            user("a"),
            asst_calls(&["c1"]),
            tool("c1"),
            user("q1"),
            asst_text("r1"),
            user("q2"),
            asst_text("r2"),
        ];
        s.truncate_history(2);
        assert!(
            s.messages.iter().any(|m| matches!(m,
                ChatMessage::Assistant { tool_calls, .. }
                    if tool_calls.iter().any(|c| c.call_id == "c1"))),
            "{:?}",
            s.messages
        );
        assert!(pairing_is_valid(&s.messages), "{:?}", s.messages);
    }
    #[test]
    #[allow(clippy::unwrap_used)]
    fn user_message_without_attachments_serializes_exactly_as_before() {
        let msg = ChatMessage::User {
            content: "hi".into(),
            attachments: Vec::new(),
        };
        assert_eq!(
            serde_json::to_value(&msg).unwrap(),
            serde_json::json!({"role":"user","content":"hi"})
        );
    }

    #[test]
    #[allow(clippy::unwrap_used, clippy::panic)]
    fn user_message_stored_before_attachments_existed_still_loads() {
        let old = serde_json::json!({"role":"user","content":"hello"});
        let msg: ChatMessage = serde_json::from_value(old).unwrap();
        match msg {
            ChatMessage::User {
                content,
                attachments,
            } => {
                assert_eq!(content, "hello");
                assert!(attachments.is_empty());
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn user_message_attachments_round_trip_as_references_only() {
        use crate::attachments::{AttachmentKind, AttachmentRef};
        let id = format!("artifact://{}", "a".repeat(64));
        let msg = ChatMessage::User {
            content: "see".into(),
            attachments: vec![AttachmentRef {
                id: id.clone(),
                mime_type: "image/png".into(),
                name: Some("a.png".into()),
                size_bytes: Some(3),
                kind: AttachmentKind::Image,
                text_ref: None,
            }],
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains(&id));
        assert!(!json.contains("base64"));
        let back: ChatMessage = serde_json::from_str(&json).unwrap();
        assert!(
            matches!(back, ChatMessage::User { ref attachments, .. } if attachments.len() == 1 && attachments[0].id == id)
        );
    }
}
