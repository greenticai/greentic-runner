//! What one `SendMessage` produced, and the JSON the worker's own model sees
//! for it.
//!
//! # The shape decision
//!
//! Before this, every outcome was flattened into one of two strings:
//! `{"reply": <text>}` or `{"error": <reason>}`. Two things were lost there,
//! and both cost the same way — the model could not tell them from an answer:
//!
//! * `input-required` — the remote agent had asked US a question — came back
//!   as `{"reply": "a2a agent X accepted the request as task t-1
//!   (InputRequired); polling a task to completion is not supported yet"}`.
//!   That is prose about our own implementation, under the key the model
//!   reads as "the agent's answer".
//! * A task still `working` came back the same way, so a model that asked
//!   "is the order shipped?" read an acceptance receipt as a No.
//!
//! So the result is a small tagged object rather than prose, with one rule
//! that is worth more than the rest of the shape put together:
//!
//! **`reply` appears if and only if `status` is `completed`.**
//!
//! Every other outcome carries `question`, `detail` or `error`, never
//! `reply`. A model — and any future parser — can therefore treat the
//! presence of `reply` as "there is an answer here" without reading `status`
//! at all, which is exactly the mistake the old shape forced. The rule is
//! pinned by [`tests::only_a_completed_outcome_carries_a_reply`].
//!
//! `status` is a stable snake_case token, not a `Debug`-formatted enum: these
//! values travel into an LLM prompt and, through the audit trail, into
//! consumers outside this crate.

use greentic_a2a::message::TaskState;
use serde_json::{Value, json};

/// The result of one call to a remote agent, before it is rendered for the
/// model.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum A2aOutcome {
    /// The agent answered. This is the only variant that produces a `reply`.
    Answered { text: String },
    /// The agent needs something from us before it can answer, and the task
    /// it is holding open is recorded in the conversation's continuation, so
    /// the next call to this agent resumes it.
    InputRequired { question: Option<String> },
    /// The agent took the work and has not finished. Not an answer, and
    /// deliberately not an error either: nothing has gone wrong.
    Working {
        state: TaskState,
        detail: Option<String>,
    },
    /// The task is over without an answer, or cannot proceed without action
    /// outside this exchange. Always an error to the model.
    Ended {
        state: TaskState,
        detail: Option<String>,
    },
}

/// The stable wire token for a task state.
///
/// Deliberately hand-written rather than derived from the `TASK_STATE_*`
/// serde names: those are the A2A wire form, and what the model reads here
/// is Greentic's own vocabulary. Keeping them separate means a protocol
/// rename cannot silently reshape a prompt.
fn state_token(state: TaskState) -> &'static str {
    match state {
        TaskState::Unspecified => "unspecified",
        TaskState::Submitted => "submitted",
        TaskState::Working => "working",
        TaskState::Completed => "completed",
        TaskState::Failed => "failed",
        TaskState::Canceled => "canceled",
        TaskState::InputRequired => "input_required",
        TaskState::Rejected => "rejected",
        TaskState::AuthRequired => "auth_required",
    }
}

/// One sentence describing an end state, with the agent's own words appended
/// when it gave any.
fn ended_sentence(agent_id: &str, state: TaskState, detail: Option<&str>) -> String {
    let what = match state {
        TaskState::Failed => "failed",
        TaskState::Canceled => "was canceled",
        TaskState::Rejected => "rejected the request",
        TaskState::AuthRequired => {
            "requires authentication this worker cannot supply in-conversation"
        }
        _ => "ended in a state this client does not understand",
    };
    match detail {
        Some(text) if !text.trim().is_empty() => {
            format!("a2a agent {agent_id} {what}: {}", text.trim())
        }
        _ => format!("a2a agent {agent_id} {what}"),
    }
}

impl A2aOutcome {
    /// Render this outcome as the tool result the model observes.
    ///
    /// `agent_id` is named in every variant on purpose: a worker may be bound
    /// to several agents and the model sees only the result value, so a
    /// result that does not say which agent produced it cannot be acted on.
    pub(super) fn to_value(&self, agent_id: &str) -> Value {
        match self {
            Self::Answered { text } => json!({
                "status": "completed",
                "agent": agent_id,
                "reply": text,
            }),
            Self::InputRequired { question } => json!({
                "status": "input_required",
                "agent": agent_id,
                "question": question.clone().unwrap_or_else(|| format!(
                    "a2a agent {agent_id} needs more information but did not say what"
                )),
                "next_step": format!(
                    "The remote agent {agent_id} is waiting and has NOT answered yet. \
                     Ask the user for what it needs, then call this tool again with \
                     their answer; the same remote task continues."
                ),
            }),
            Self::Working { state, detail } => json!({
                "status": "working",
                "agent": agent_id,
                "state": state_token(*state),
                "detail": detail.clone().unwrap_or_else(|| format!(
                    "a2a agent {agent_id} accepted the request and has not answered yet"
                )),
                "next_step": format!(
                    "There is no answer yet. Do not report this as the agent's answer. \
                     Call this tool again later to ask {agent_id} for progress."
                ),
            }),
            Self::Ended { state, detail } => json!({
                "status": "failed",
                "agent": agent_id,
                "state": state_token(*state),
                "error": ended_sentence(agent_id, *state, detail.as_deref()),
            }),
        }
    }
}

/// The result value for a call that never reached a remote task at all: an
/// unknown agent, an unreachable card, a transport failure, a JSON-RPC error.
///
/// Separate from [`A2aOutcome::Ended`] because the two are different facts.
/// `failed` says the remote agent decided something; `error` says we could
/// not ask it. Collapsing them would tell an operator to go and look at the
/// wrong system.
/// Public because a caller may not reach a catalogue at all — the `a2a` FLOW
/// node in greentic-runner-host has no source to build one from when its pack
/// carries no `assets/a2a-routes.json`. That is the same fact ("we could not
/// ask the agent"), so it must render as the same value rather than as a
/// second, privately-invented shape the routing rules would not recognise.
pub fn call_error_value(agent_id: &str, reason: &str) -> Value {
    json!({
        "status": "error",
        "agent": agent_id,
        "error": reason,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn all_outcomes() -> Vec<A2aOutcome> {
        vec![
            A2aOutcome::Answered {
                text: "an omelette".into(),
            },
            A2aOutcome::InputRequired {
                question: Some("Which city?".into()),
            },
            A2aOutcome::InputRequired { question: None },
            A2aOutcome::Working {
                state: TaskState::Working,
                detail: None,
            },
            A2aOutcome::Working {
                state: TaskState::Submitted,
                detail: Some("queued".into()),
            },
            A2aOutcome::Ended {
                state: TaskState::Failed,
                detail: Some("upstream timeout".into()),
            },
            A2aOutcome::Ended {
                state: TaskState::Canceled,
                detail: None,
            },
            A2aOutcome::Ended {
                state: TaskState::Rejected,
                detail: None,
            },
            A2aOutcome::Ended {
                state: TaskState::AuthRequired,
                detail: None,
            },
            A2aOutcome::Ended {
                state: TaskState::Unspecified,
                detail: None,
            },
        ]
    }

    /// The one invariant the whole shape exists for.
    #[test]
    fn only_a_completed_outcome_carries_a_reply() {
        for outcome in all_outcomes() {
            let value = outcome.to_value("recipe");
            let has_reply = value.get("reply").is_some();
            let completed = value["status"] == "completed";
            assert_eq!(
                has_reply, completed,
                "`reply` must appear exactly when status is completed; got {value}"
            );
        }
    }

    #[test]
    fn a_failure_carries_an_error_and_never_reads_as_an_answer() {
        for state in [
            TaskState::Failed,
            TaskState::Canceled,
            TaskState::Rejected,
            TaskState::AuthRequired,
            TaskState::Unspecified,
        ] {
            let value = A2aOutcome::Ended {
                state,
                detail: None,
            }
            .to_value("recipe");
            assert_eq!(value["status"], "failed", "{state:?}");
            assert_eq!(value["state"], state_token(state));
            assert!(
                value["error"]
                    .as_str()
                    .is_some_and(|e| e.contains("recipe"))
            );
            assert!(value.get("reply").is_none());
        }
    }

    #[test]
    fn auth_required_names_authentication_rather_than_a_generic_failure() {
        // An operator reading "failed" would go looking at the remote agent's
        // logs; this one has a different fix, so it has to say so.
        let value = A2aOutcome::Ended {
            state: TaskState::AuthRequired,
            detail: None,
        }
        .to_value("recipe");
        assert_eq!(value["state"], "auth_required");
        let error = value["error"].as_str().unwrap();
        assert!(error.contains("authentication"), "got: {error}");
    }

    #[test]
    fn input_required_surfaces_the_question_and_says_the_agent_is_waiting() {
        let value = A2aOutcome::InputRequired {
            question: Some("Which city?".into()),
        }
        .to_value("travel");
        assert_eq!(value["status"], "input_required");
        assert_eq!(value["question"], "Which city?");
        let next = value["next_step"].as_str().unwrap();
        assert!(next.contains("waiting") && next.contains("call this tool again"));
    }

    #[test]
    fn input_required_with_no_question_still_says_the_agent_is_waiting() {
        // A conformant agent may park a task without a status message. The
        // model must still learn it is expected to come back with something.
        let value = A2aOutcome::InputRequired { question: None }.to_value("travel");
        assert_eq!(value["status"], "input_required");
        assert!(
            value["question"]
                .as_str()
                .is_some_and(|q| q.contains("travel")),
            "got: {value}"
        );
    }

    #[test]
    fn a_working_task_is_not_an_error_and_not_an_answer() {
        let value = A2aOutcome::Working {
            state: TaskState::Working,
            detail: None,
        }
        .to_value("recipe");
        assert_eq!(value["status"], "working");
        assert!(value.get("reply").is_none());
        assert!(value.get("error").is_none(), "nothing has gone wrong");
    }

    #[test]
    fn a_call_that_never_reached_the_agent_is_error_not_failed() {
        let value = call_error_value("recipe", "card unreachable");
        assert_eq!(value["status"], "error");
        assert_eq!(value["agent"], "recipe");
        assert_eq!(value["error"], "card unreachable");
        assert!(value.get("reply").is_none());
    }

    #[test]
    fn every_status_token_is_stable_snake_case() {
        for state in [
            TaskState::Unspecified,
            TaskState::Submitted,
            TaskState::Working,
            TaskState::Completed,
            TaskState::Failed,
            TaskState::Canceled,
            TaskState::InputRequired,
            TaskState::Rejected,
            TaskState::AuthRequired,
        ] {
            let token = state_token(state);
            assert!(
                token.chars().all(|c| c.is_ascii_lowercase() || c == '_'),
                "{token}"
            );
        }
    }
}
