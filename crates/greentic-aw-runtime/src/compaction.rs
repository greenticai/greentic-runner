//! History compaction: instead of discarding the messages that overflow
//! `AgentLimits::max_history_turns`, fold them into one system message.
//!
//! The summary is a `System` message, and system messages are exempt from
//! truncation and not counted against the limit, so it survives later turns
//! and is itself re-summarised (with the next overflow) rather than growing.
//!
//! Compaction never fails a turn. If the summarising call errors or returns
//! nothing, the overflow is dropped exactly as plain truncation would have.

use tracing::warn;

use crate::config::AgentConfig;
use crate::llm::LlmRequest;
use crate::state::{ChatMessage, ConversationState};
use crate::{AgentRuntime, TenantContext};

/// Marks the system message holding the running summary.
pub const SUMMARY_PREFIX: &str = "Summary of the earlier conversation (older messages were \
removed to save space):\n";

/// Longest the stored summary may be, in characters.
const MAX_SUMMARY_CHARS: usize = 4_000;
/// Longest a single folded message may be, in characters.
const MAX_MESSAGE_CHARS: usize = 1_500;
/// Longest the whole material sent to the summariser may be, in characters.
const MAX_INPUT_CHARS: usize = 24_000;

const SYSTEM_PROMPT: &str = "You compress a conversation between a user and an AI assistant. \
Write a faithful, compact summary that keeps: facts the user stated, decisions made, open \
questions, identifiers (names, ids, numbers, dates), and what tools returned that matters. \
Drop pleasantries and repetition. Never invent anything. Reply with the summary only.";

/// Tokens spent by a compaction call, for the caller's usage totals.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CompactionUsage {
    pub tokens_in: u64,
    pub tokens_out: u64,
}

/// Trim `state` to the configured window, summarising what overflows when
/// `config.limits.compact_history` is on.
pub async fn truncate_with_compaction(
    runtime: &AgentRuntime,
    tenant: &TenantContext,
    agent_id: &str,
    config: &AgentConfig,
    state: &mut ConversationState,
) -> CompactionUsage {
    let max = config.limits.max_history_turns;
    if !config.limits.compact_history {
        state.truncate_history(max);
        return CompactionUsage::default();
    }
    let overflow = state.take_overflow(max);
    if overflow.is_empty() {
        return CompactionUsage::default();
    }
    let previous = existing_summary(state);
    let material = render_material(previous.as_deref(), &overflow);
    let request = LlmRequest {
        system_prompt: SYSTEM_PROMPT.to_string(),
        history: vec![ChatMessage::User {
            content: material,
            attachments: Vec::new(),
        }],
        tools: Vec::new(),
        provider: config.llm.clone(),
        turn_attachments: Default::default(),
        attachment_text_guard: None,
    };
    let response = match runtime.llm.complete(request).await {
        Ok(r) => r,
        Err(e) => {
            warn!(error = %e, "history compaction failed; older messages were dropped");
            return CompactionUsage::default();
        }
    };
    let usage = CompactionUsage {
        tokens_in: u64::from(response.tokens_in),
        tokens_out: u64::from(response.tokens_out),
    };
    let step_tokens = usage.tokens_in + usage.tokens_out;
    if let Err(e) = runtime.token_meter.add(tenant, step_tokens).await {
        warn!(error = %e, "token meter add failed for compaction; continuing");
    }
    if let Err(e) = runtime
        .billing_meter
        .emit(
            tenant,
            usage.tokens_in,
            usage.tokens_out,
            agent_id,
            &config.llm.model,
        )
        .await
    {
        warn!(error = %e, "billing meter emit failed for compaction; continuing");
    }
    let summary = response.content.unwrap_or_default();
    let summary = summary.trim();
    if summary.is_empty() {
        return usage;
    }
    set_summary(state, summary);
    usage
}

/// The running summary's text, without its marker, when one exists.
fn existing_summary(state: &ConversationState) -> Option<String> {
    state.messages.iter().find_map(|m| match m {
        ChatMessage::System { content } => content.strip_prefix(SUMMARY_PREFIX).map(str::to_string),
        _ => None,
    })
}

/// Replace the running summary (or add one) right after the leading system
/// messages, so it reads before any conversational message.
fn set_summary(state: &mut ConversationState, summary: &str) {
    state.messages.retain(
        |m| !matches!(m, ChatMessage::System { content } if content.starts_with(SUMMARY_PREFIX)),
    );
    let at = state
        .messages
        .iter()
        .take_while(|m| matches!(m, ChatMessage::System { .. }))
        .count();
    let body: String = summary.chars().take(MAX_SUMMARY_CHARS).collect();
    state.messages.insert(
        at,
        ChatMessage::System {
            content: format!("{SUMMARY_PREFIX}{body}"),
        },
    );
}

fn clip(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        return text.to_string();
    }
    let head: String = text.chars().take(max).collect();
    format!("{head}…")
}

fn render_message(m: &ChatMessage) -> Option<String> {
    match m {
        ChatMessage::System { .. } => None,
        ChatMessage::User { content, .. } => {
            Some(format!("User: {}", clip(content, MAX_MESSAGE_CHARS)))
        }
        ChatMessage::Assistant {
            content,
            tool_calls,
        } => {
            let mut line = format!("Assistant: {}", clip(content, MAX_MESSAGE_CHARS));
            if !tool_calls.is_empty() {
                let names: Vec<&str> = tool_calls.iter().map(|c| c.tool_name.as_str()).collect();
                line.push_str(&format!(" [called tools: {}]", names.join(", ")));
            }
            Some(line)
        }
        ChatMessage::Tool { content, .. } => Some(format!(
            "Tool result: {}",
            clip(&content.to_string(), MAX_MESSAGE_CHARS)
        )),
    }
}

/// The text handed to the summariser: the previous summary, then the messages
/// being folded in. When the folded messages exceed the input budget the
/// oldest are cut, since the previous summary already stands for older still.
fn render_material(previous: Option<&str>, overflow: &[ChatMessage]) -> String {
    let lines: Vec<String> = overflow.iter().filter_map(render_message).collect();
    let mut kept: Vec<&str> = Vec::new();
    let mut used = 0usize;
    for line in lines.iter().rev() {
        let n = line.chars().count() + 1;
        if used + n > MAX_INPUT_CHARS && !kept.is_empty() {
            break;
        }
        used += n;
        kept.push(line);
    }
    kept.reverse();
    let mut out = String::new();
    if let Some(prev) = previous {
        out.push_str("Existing summary:\n");
        out.push_str(prev);
        out.push_str("\n\n");
    }
    out.push_str("Messages to fold into the summary, oldest first:\n");
    out.push_str(&kept.join("\n"));
    out.push_str("\n\nWrite the updated summary now.");
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::ToolCallRecord;

    fn user(t: &str) -> ChatMessage {
        ChatMessage::User {
            content: t.into(),
            attachments: Vec::new(),
        }
    }

    fn assistant(t: &str) -> ChatMessage {
        ChatMessage::Assistant {
            content: t.into(),
            tool_calls: Vec::new(),
        }
    }

    fn state_with(messages: Vec<ChatMessage>) -> ConversationState {
        let mut s = ConversationState::empty(&TenantContext::new("t", "e"), "s");
        s.messages = messages;
        s
    }

    #[test]
    fn the_summary_sits_after_leading_system_messages_and_replaces_the_old_one() {
        let mut s = state_with(vec![
            ChatMessage::System {
                content: "sys".into(),
            },
            user("u"),
        ]);
        set_summary(&mut s, "first");
        set_summary(&mut s, "second");
        assert!(matches!(&s.messages[0], ChatMessage::System { content } if content == "sys"));
        assert!(matches!(&s.messages[1], ChatMessage::System { content }
            if content == &format!("{SUMMARY_PREFIX}second")));
        assert_eq!(
            s.messages
                .iter()
                .filter(|m| matches!(m, ChatMessage::System { content } if content.starts_with(SUMMARY_PREFIX)))
                .count(),
            1
        );
        assert!(matches!(&s.messages[2], ChatMessage::User { .. }));
    }

    #[test]
    fn existing_summary_strips_the_marker() {
        let mut s = state_with(vec![user("u")]);
        assert_eq!(existing_summary(&s), None);
        set_summary(&mut s, "facts");
        assert_eq!(existing_summary(&s).as_deref(), Some("facts"));
    }

    #[test]
    fn material_includes_the_previous_summary_and_every_role() {
        let call = ToolCallRecord {
            call_id: "c".into(),
            extension_id: "x".into(),
            tool_name: "lookup".into(),
            args: serde_json::json!({}),
        };
        let overflow = vec![
            user("my name is Ana"),
            ChatMessage::Assistant {
                content: "checking".into(),
                tool_calls: vec![call],
            },
            ChatMessage::Tool {
                call_id: "c".into(),
                content: serde_json::json!({"ok": true}),
            },
            assistant("done"),
        ];
        let m = render_material(Some("earlier facts"), &overflow);
        assert!(m.contains("Existing summary:\nearlier facts"));
        assert!(m.contains("User: my name is Ana"));
        assert!(m.contains("[called tools: lookup]"));
        assert!(m.contains("Tool result:"));
        assert!(m.contains("Assistant: done"));
    }

    #[test]
    fn an_oversized_overflow_keeps_the_newest_messages() {
        let big = "x".repeat(MAX_MESSAGE_CHARS);
        let mut overflow: Vec<ChatMessage> = (0..40).map(|_| user(&big)).collect();
        overflow.push(user("the newest message"));
        let m = render_material(None, &overflow);
        assert!(m.contains("the newest message"));
        assert!(m.chars().count() < MAX_INPUT_CHARS + 2_000);
    }
}
