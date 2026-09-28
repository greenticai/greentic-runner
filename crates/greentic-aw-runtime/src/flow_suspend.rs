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

use crate::flow_source::FlowInvokeOutcome;
use crate::state::{ChatMessage, ConversationState, PendingToolCall, ToolCallRecord};
use crate::tenant::TenantContext;
use crate::tool_session::ToolCatalogs;
use crate::{AgentRuntime, AgentStep, StepObserver};

/// The error value every tool call queued behind a suspended one receives.
pub(crate) const BEHIND_SUSPENSION_ERROR: &str = "another step is waiting for the user";

/// A parked flow tool that this turn will resume.
pub(crate) struct Resume {
    pub(crate) pending: PendingToolCall,
    pub(crate) payload: Value,
}

/// Take the conversation's pending flow tool, if any.
///
/// Returns the resume to run when the suspension is live and the turn carries
/// the user's answer. Otherwise the pending call is CANCELLED here — its
/// `call_id` answered with `{"status": "cancelled", "reason": ...}` — and the
/// returned trail step records it; the turn then proceeds as an ordinary
/// message.
pub(crate) fn take_pending(
    state: &mut ConversationState,
    resume_payload: Option<Value>,
    now: DateTime<Utc>,
    observer: &dyn StepObserver,
) -> (Option<Resume>, Option<AgentStep>) {
    let Some(pending) = state.pending_tool.take() else {
        return (None, None);
    };
    let reason = if pending.is_expired(now) {
        "the step waiting for the user expired before they answered"
    } else if let Some(payload) = resume_payload {
        return (Some(Resume { pending, payload }), None);
    } else {
        "the user sent a message instead of completing this step"
    };
    let result = json!({ "status": "cancelled", "reason": reason });
    observer.on_tool_result(&pending.tool_name, &pending.call_id, &result);
    state.messages.push(ChatMessage::Tool {
        call_id: pending.call_id.clone(),
        content: result.clone(),
    });
    let step = AgentStep::ToolCall {
        name: pending.tool_name,
        call_id: pending.call_id,
        args: pending.args,
        result,
        duration_ms: 0,
    };
    (None, Some(step))
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
            ChatMessage::User { content } => Some(content.clone()),
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
            cat.resume(&pending.flow_ref, pending.flow_snapshot.clone(), payload)
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
            state.messages.push(ChatMessage::Tool {
                call_id: pending.call_id.clone(),
                content: result.clone(),
            });
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
