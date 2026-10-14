//! Suspend / resume of an interactive `flow:` tool inside the agent loop.
//!
//! A flow tool may park on the user — typically an Adaptive Card whose submit
//! is routed — instead of completing. The loop then ends the turn with
//! [`TerminationReason::AwaitingToolInput`], storing a
//! [`PendingToolCall`] on the conversation, and the host shows the card. The
//! next turn arrives with either the user's answer
//! ([`crate::AgentInput::resume_payload`]), which resumes the flow, or without
//! one, which cancels it.
//!
//! One invariant holds throughout: the suspended call's `call_id` always gets
//! a tool result before anything else reaches the LLM — the completed flow's
//! output, or a `cancelled` value. A history holding an assistant `tool_calls`
//! turn with no matching `tool` message is refused by OpenAI outright.
//!
//! [`TerminationReason::AwaitingToolInput`]: crate::error::TerminationReason::AwaitingToolInput

use std::time::Instant;

use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use tracing::warn;

use crate::config::ParkedTextPolicy;
use crate::error::TerminationReason;
use crate::flow_source::FlowInvokeOutcome;
use crate::state::{
    ChatMessage, ConversationState, MAX_SIDE_TURNS, PENDING_TOOL_MAX_AGE_SECS, PendingToolCall,
    ToolCallRecord,
};
use crate::tenant::TenantContext;
use crate::tool_session::ToolCatalogs;
use crate::{AgentRuntime, AgentStep, StepObserver};

/// The error value every tool call queued behind a suspended one receives.
pub(crate) const BEHIND_SUSPENSION_ERROR: &str = "another step is waiting for the user";

/// The result recorded for a parked call while it waits for the user, so
/// messages appended after it keep the transcript provider-valid.
pub(crate) const AWAITING_PLACEHOLDER: &str = "awaiting_user_input";

/// Index of the LATEST assistant turn carrying a tool call `call_id`. Matching
/// on the call id alone would hit an older turn when a backend reuses ids.
fn latest_assistant_with(messages: &[ChatMessage], call_id: &str) -> Option<usize> {
    messages.iter().rposition(|m| {
        matches!(m, ChatMessage::Assistant { tool_calls, .. }
            if tool_calls.iter().any(|c| c.call_id == call_id))
    })
}

/// End of the run of `Tool` messages answering the assistant turn at `at`.
fn tool_run_end(messages: &[ChatMessage], at: usize) -> usize {
    let mut end = at + 1;
    while matches!(messages.get(end), Some(ChatMessage::Tool { .. })) {
        end += 1;
    }
    end
}

/// Record `content` as the result of `call_id`: replace the `Tool` already
/// answering the latest assistant turn that made the call (the park
/// placeholder) in place, or add one at the end of that turn's results when
/// there is none. If that assistant turn is no longer in history nothing is
/// added: a `Tool` with no assistant turn before it is refused by the provider.
pub(crate) fn patch_tool_result(state: &mut ConversationState, call_id: &str, content: Value) {
    let Some(at) = latest_assistant_with(&state.messages, call_id) else {
        return;
    };
    let end = tool_run_end(&state.messages, at);
    let existing = state.messages[at + 1..end]
        .iter_mut()
        .find_map(|m| match m {
            ChatMessage::Tool {
                call_id: id,
                content,
            } if id == call_id => Some(content),
            _ => None,
        });
    match existing {
        Some(slot) => *slot = content,
        None => state.messages.insert(
            end,
            ChatMessage::Tool {
                call_id: call_id.to_string(),
                content,
            },
        ),
    }
}

/// The key under which a parked call's result announces files the user sent
/// with an answer to it.
pub(crate) const ATTACHMENTS_NOTE_KEY: &str = "user_attachments_note";

/// Announce `count` files the user sent with an answer to the parked call
/// `call_id` (the flow takes no files) in that call's RESULT, with the fixed
/// "not available" sentence: a count only, no name or id. The note lives in
/// the tool result, so history truncation removes it with its group; a
/// separate persisted system message would never be removed. A non-object
/// result is kept under `result`. Nothing is done for 0 or when the call has
/// no result in history.
pub(crate) fn announce_unread_attachments(
    state: &mut ConversationState,
    call_id: &str,
    count: usize,
) {
    if count == 0 {
        return;
    }
    let Some(at) = latest_assistant_with(&state.messages, call_id) else {
        return;
    };
    let end = tool_run_end(&state.messages, at);
    let Some(content) = state.messages[at + 1..end]
        .iter_mut()
        .find_map(|m| match m {
            ChatMessage::Tool {
                call_id: id,
                content,
            } if id == call_id => Some(content),
            _ => None,
        })
    else {
        return;
    };
    let note = Value::String(
        crate::attachments_materialize::unreadable_note(count)
            .trim()
            .to_string(),
    );
    match content {
        Value::Object(map) => {
            map.insert(ATTACHMENTS_NOTE_KEY.to_string(), note);
        }
        other => {
            let result = other.take();
            *other = json!({ "result": result, ATTACHMENTS_NOTE_KEY: note });
        }
    }
}

/// A parked flow tool that this turn will resume.
pub(crate) struct Resume {
    pub(crate) pending: PendingToolCall,
    pub(crate) payload: Value,
}

/// What the start of a step found on the conversation's parked flow tool.
pub(crate) enum Taken {
    /// Nothing was parked.
    Nothing,
    /// The turn carries the user's answer: resume the flow.
    Resume(Resume),
    /// The park was cancelled (typed message, or expired); the trail step
    /// records it. The turn proceeds as an ordinary message.
    Cancelled(AgentStep),
    /// The park STAYS and this turn answers the typed message as a side turn.
    Side,
}

/// The note added to the system prompt of the turn that RESUMES a flow tool
/// after one or more side turns, so the model answers about the finished step
/// and not the last side question.
pub(crate) const RESUME_AFTER_SIDE_TURNS_NOTE: &str = "The form step the user was asked to \
complete has now finished; its result is the tool result in the conversation. \
Reply about that result, not about the earlier side questions.";

/// The note added to the system prompt of a side turn.
pub(crate) const SIDE_TURN_NOTE: &str = "A form step is waiting for the user to \
fill it in. Do not start that step again. Answer the user's question, then \
mention that the form can be continued.";

/// Take the conversation's pending flow tool, if any.
///
/// Returns the resume to run when the suspension is live and the turn carries
/// the user's answer. With `policy == SideTurn`, a typed message on a live,
/// re-offerable park leaves the park in place ([`Taken::Side`]). Otherwise the
/// pending call is CANCELLED here — its `call_id` answered with
/// `{"status": "cancelled", "reason": ...}` — and the returned trail step
/// records it; the turn then proceeds as an ordinary message.
pub(crate) fn take_pending(
    state: &mut ConversationState,
    resume_payload: Option<Value>,
    now: DateTime<Utc>,
    observer: &dyn StepObserver,
    policy: ParkedTextPolicy,
) -> Taken {
    let Some(pending) = state.pending_tool.as_ref() else {
        return Taken::Nothing;
    };
    if policy == ParkedTextPolicy::SideTurn
        && resume_payload.is_none()
        && side_turn_allowed(pending, now)
        && ensure_placeholder(state)
    {
        if let Some(pending) = state.pending_tool.as_mut() {
            begin_side_turn(pending, now);
        }
        return Taken::Side;
    }
    let Some(pending) = state.pending_tool.take() else {
        return Taken::Nothing;
    };
    let reason = if pending.is_expired(now) {
        "the step waiting for the user expired before they answered"
    } else if let Some(payload) = resume_payload {
        return Taken::Resume(Resume { pending, payload });
    } else {
        "the user sent a message instead of completing this step"
    };
    let result = json!({ "status": "cancelled", "reason": reason });
    observer.on_tool_result(&pending.tool_name, &pending.call_id, &result);
    patch_tool_result(state, &pending.call_id, result.clone());
    announce_unread_attachments(
        state,
        &pending.call_id,
        pending.unannounced_attachments as usize,
    );
    Taken::Cancelled(AgentStep::ToolCall {
        name: pending.tool_name,
        call_id: pending.call_id,
        args: pending.args,
        result,
        duration_ms: 0,
    })
}

/// Whether a typed message may be answered while `pending` stays parked: the
/// park is live (idle expiry and the 24h age cap), under the side-turn cap,
/// and carries the card to re-offer afterwards.
fn side_turn_allowed(pending: &PendingToolCall, now: DateTime<Utc>) -> bool {
    let within_age = pending
        .parked_at
        .is_none_or(|at| now < at + chrono::Duration::seconds(PENDING_TOOL_MAX_AGE_SECS));
    !pending.is_expired(now)
        && within_age
        && pending.side_turns < MAX_SIDE_TURNS
        && pending.presentation.is_some()
}

/// Count a side turn and refresh the idle expiry, never beyond
/// `parked_at + PENDING_TOOL_MAX_AGE_SECS`.
fn begin_side_turn(pending: &mut PendingToolCall, now: DateTime<Utc>) {
    pending.side_turns += 1;
    let idle = PendingToolCall::expiry_from(now);
    pending.expires_at = match pending.parked_at {
        Some(at) => idle.min(at + chrono::Duration::seconds(PENDING_TOOL_MAX_AGE_SECS)),
        None => idle,
    };
}

/// Make sure the parked call has a `Tool` result in history (a park recorded
/// without a placeholder, e.g. under the cancel policy), inserting one right
/// after the assistant turn that made the call. `false` when that turn is no
/// longer in history, in which case the park cannot take a side turn.
fn ensure_placeholder(state: &mut ConversationState) -> bool {
    let Some(call_id) = state.pending_tool.as_ref().map(|p| p.call_id.clone()) else {
        return false;
    };
    let Some(at) = latest_assistant_with(&state.messages, &call_id) else {
        return false;
    };
    let end = tool_run_end(&state.messages, at);
    if state.messages[at + 1..end]
        .iter()
        .any(|m| matches!(m, ChatMessage::Tool { call_id: id, .. } if *id == call_id))
    {
        return true;
    }
    state.messages.insert(
        at + 1,
        ChatMessage::Tool {
            call_id,
            content: json!({ "status": AWAITING_PLACEHOLDER }),
        },
    );
    true
}

/// After a side turn's loop: re-offer the stored card. Turns the step's end
/// into `AwaitingToolInput` carrying that card, whatever stopped the loop
/// (a final reply, the iteration cap, the timeout). Returns whether it did.
/// If the model ended the conversation instead, the park is cancelled so no
/// call stays pending behind a finished segment.
pub(crate) fn finish_side_turn(
    state: &mut ConversationState,
    terminated_by: &mut TerminationReason,
    suspension: &mut Option<Value>,
) -> bool {
    if matches!(terminated_by, TerminationReason::ConversationEnded) {
        if let Some(pending) = state.pending_tool.take() {
            patch_tool_result(
                state,
                &pending.call_id,
                json!({ "status": "cancelled", "reason": "the conversation ended" }),
            );
        }
        return false;
    }
    let Some(card) = state
        .pending_tool
        .as_ref()
        .and_then(|p| p.presentation.clone())
    else {
        return false;
    };
    *terminated_by = TerminationReason::AwaitingToolInput;
    *suspension = Some(card);
    true
}

/// The most recent user message's text. A resumed turn carries a card submit
/// rather than a new message, so retrieval and recall key off the message the
/// turn was originally about.
pub(crate) fn last_user_text(state: &ConversationState) -> String {
    state
        .messages
        .iter()
        .rev()
        .find_map(|m| match m {
            ChatMessage::User { content, .. } => Some(content.clone()),
            _ => None,
        })
        .unwrap_or_default()
}

/// Resume the parked flow with the user's answer.
///
/// On completion the flow's output becomes the suspended call's tool result
/// (recorded in the ledger like any successful dispatch) and `None` is
/// returned so the loop continues. If the flow parks AGAIN (the card asked
/// once more), the suspension is re-armed with the fresh snapshot and a
/// refreshed expiry, and the new presentation is returned.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn resume_pending(
    runtime: &AgentRuntime,
    lock: &crate::state::SessionLock,
    tenant: &TenantContext,
    session_id: &str,
    catalogs: &ToolCatalogs,
    state: &mut ConversationState,
    trail: &mut Vec<AgentStep>,
    observer: &dyn StepObserver,
    resume: Resume,
) -> Option<Value> {
    let Resume {
        mut pending,
        payload,
    } = resume;
    let t0 = Instant::now();
    let outcome = match catalogs.flows.as_deref() {
        Some(cat) => {
            // The parked call's binding; a call parked by a runtime older than
            // `extension_id` (`#[serde(default)]`) has it empty. The policy is
            // the RESUMING step's (set by `run_step`), so a sidecar changed
            // between park and resume applies to the resume.
            let binding = if pending.extension_id.is_empty() {
                format!("flow:{}", pending.flow_ref)
            } else {
                pending.extension_id.clone()
            };
            crate::run_trace::under_binding(
                &binding,
                // Same frame as the call that parked (same call id), so the
                // host resumes a nested agent under the session it parked in.
                // The resume can run a nested agent turn too: keep the
                // caller's lock alive while it is pending.
                crate::lock_keepalive::keep_alive(
                    lock,
                    crate::lock_keepalive::LOCK_KEEPALIVE_INTERVAL,
                    crate::tool_call_frame::within(
                        crate::tool_call_frame::ToolCallFrame::new(
                            Some(session_id),
                            &pending.call_id,
                        )
                        .with_caller(tenant.caller_or_anonymous()),
                        cat.resume(&pending.flow_ref, pending.flow_snapshot.clone(), payload),
                    ),
                ),
            )
            .await
        }
        None => FlowInvokeOutcome::Completed(json!({
            "error": format!("unknown flow tool '{}'", pending.flow_ref)
        })),
    };
    match outcome {
        FlowInvokeOutcome::Completed(result) => {
            let duration_ms = t0.elapsed().as_millis() as u64;
            observer.on_tool_result(&pending.tool_name, &pending.call_id, &result);
            if let Err(e) = runtime
                .ledger
                .record(tenant, session_id, &pending.call_id, result.clone())
                .await
            {
                warn!(error = %e, "ledger record failed; continuing");
            }
            patch_tool_result(state, &pending.call_id, result.clone());
            trail.push(AgentStep::ToolCall {
                name: pending.tool_name,
                call_id: pending.call_id,
                args: pending.args,
                result,
                duration_ms,
            });
            None
        }
        FlowInvokeOutcome::Waiting {
            snapshot,
            presentation,
        } => {
            pending.flow_snapshot = snapshot;
            pending.expires_at = PendingToolCall::expiry_from(Utc::now());
            pending.presentation = Some(presentation.clone());
            pending.side_turns = 0;
            state.pending_tool = Some(pending);
            Some(presentation)
        }
    }
}

/// Answer a tool call that sits behind a suspended one in the same batch,
/// without running it.
pub(crate) fn refuse_behind_suspension(
    state: &mut ConversationState,
    trail: &mut Vec<AgentStep>,
    observer: &dyn StepObserver,
    call: &ToolCallRecord,
) {
    let result = json!({ "error": BEHIND_SUSPENSION_ERROR });
    observer.on_tool_call(&call.tool_name, &call.call_id, &call.args);
    observer.on_tool_failed(&call.tool_name, &call.call_id, &result);
    state.messages.push(ChatMessage::Tool {
        call_id: call.call_id.clone(),
        content: result.clone(),
    });
    trail.push(AgentStep::ToolCall {
        name: call.tool_name.clone(),
        call_id: call.call_id.clone(),
        args: call.args.clone(),
        result,
        duration_ms: 0,
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tool(id: &str, content: Value) -> ChatMessage {
        ChatMessage::Tool {
            call_id: id.into(),
            content,
        }
    }

    fn state_with(messages: Vec<ChatMessage>) -> ConversationState {
        let mut s = ConversationState::empty(&TenantContext::new("a", "b"), "x");
        s.messages = messages;
        s
    }

    #[test]
    fn patch_replaces_the_placeholder_in_place() {
        let mut s = state_with(vec![
            asst(&["c1"]),
            tool("c1", json!({ "status": AWAITING_PLACEHOLDER })),
            ChatMessage::User {
                content: "q".into(),
                attachments: Vec::new(),
            },
        ]);
        patch_tool_result(&mut s, "c1", json!({ "ok": true }));
        assert_eq!(s.messages.len(), 3);
        assert!(matches!(&s.messages[1],
            ChatMessage::Tool { content, .. } if content == &json!({ "ok": true })));
    }

    fn asst(ids: &[&str]) -> ChatMessage {
        ChatMessage::Assistant {
            content: String::new(),
            tool_calls: ids
                .iter()
                .map(|id| ToolCallRecord {
                    call_id: (*id).into(),
                    extension_id: "flow:form".into(),
                    tool_name: "form".into(),
                    args: json!({}),
                })
                .collect(),
        }
    }

    #[test]
    fn patch_adds_the_result_after_its_assistant_turn_when_there_is_no_placeholder() {
        let mut s = state_with(vec![asst(&["c1"])]);
        patch_tool_result(&mut s, "c1", json!({ "status": "cancelled" }));
        assert_eq!(s.messages.len(), 2);
    }

    #[test]
    fn patch_never_adds_an_orphan_when_the_assistant_turn_is_gone() {
        let mut s = state_with(vec![]);
        patch_tool_result(&mut s, "c1", json!({ "status": "cancelled" }));
        assert!(s.messages.is_empty());
    }

    /// A backend that reuses call ids: the result must land on the Tool that
    /// answers the LATEST assistant turn carrying the id, not an older one.
    #[test]
    fn patch_targets_the_latest_assistant_turn_when_a_call_id_is_reused() {
        let old = json!({ "old": true });
        let mut s = state_with(vec![
            asst(&["c1"]),
            tool("c1", old.clone()),
            ChatMessage::User {
                content: "next".into(),
                attachments: Vec::new(),
            },
            asst(&["c1"]),
            tool("c1", json!({ "status": AWAITING_PLACEHOLDER })),
        ]);
        patch_tool_result(&mut s, "c1", json!({ "new": true }));
        assert!(matches!(&s.messages[1], ChatMessage::Tool { content, .. } if content == &old));
        assert!(matches!(&s.messages[4],
            ChatMessage::Tool { content, .. } if content == &json!({ "new": true })));
    }

    #[test]
    fn ensure_placeholder_answers_the_latest_assistant_turn_when_a_call_id_is_reused() {
        let mut s = state_with(vec![
            asst(&["c1"]),
            tool("c1", json!({ "old": true })),
            ChatMessage::User {
                content: "next".into(),
                attachments: Vec::new(),
            },
            asst(&["c1"]),
        ]);
        s.pending_tool = Some(crate::state::PendingToolCall {
            call_id: "c1".into(),
            tool_name: "form".into(),
            extension_id: "flow:form".into(),
            args: json!({}),
            flow_ref: "form".into(),
            flow_snapshot: json!({}),
            iterations_used: 0,
            expires_at: Utc::now() + chrono::Duration::hours(1),
            presentation: Some(json!({})),
            parked_at: None,
            side_turns: 0,
            unannounced_attachments: 0,
        });
        assert!(ensure_placeholder(&mut s));
        assert_eq!(s.messages.len(), 5);
        assert!(matches!(&s.messages[4], ChatMessage::Tool { .. }));
    }
}
