//! Which agent tool call a nested host computation is running for.
//!
//! A `flow:` tool's flow runs in the HOST (runner-host builds a fresh flow
//! engine per call), and a `dw.agent` inside that flow needs a conversation
//! session of its own that is (a) never the caller's — the caller holds its
//! session lock across the tool await — and (b) the same on the call and on
//! the resume of a parked call. Neither is derivable from the `FlowInvoker`
//! arguments, which carry only the flow ref and the arguments, and adding a
//! parameter would break every implementor. So the agent loop and
//! [`crate::ToolSession`] publish the caller's session and the LLM call id in
//! a task-local for the duration of the dispatch.
//!
//! Visible only across `.await` on the same task: a `tokio::spawn` or
//! `spawn_blocking` between the dispatch and the reader loses it, and the
//! reader must then fall back to something that does not need it.

use std::future::Future;

use crate::tenant::VerifiedCaller;

/// The calling agent's session and the LLM's call id for one tool call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolCallFrame {
    /// The calling agent's conversation session; `None` for a caller that has
    /// none (a `ToolSession` built without `with_session_id`).
    pub session_id: Option<String>,
    /// The LLM's call id, the key the tool result is recorded under.
    pub call_id: String,
    /// The verified caller of the OUTER step, as the host stamped it on the
    /// step's `TenantContext`; never anything the model wrote. Anonymous
    /// (the default) when the outer step had none.
    pub caller: VerifiedCaller,
}

impl ToolCallFrame {
    /// A frame for `call_id`; an empty `session_id` reads as no session.
    #[must_use]
    pub fn new(session_id: Option<&str>, call_id: &str) -> Self {
        Self {
            session_id: session_id.filter(|s| !s.is_empty()).map(str::to_string),
            call_id: call_id.to_string(),
            caller: VerifiedCaller::default(),
        }
    }

    /// The same frame carrying the outer step's verified caller.
    #[must_use]
    pub fn with_caller(mut self, caller: VerifiedCaller) -> Self {
        self.caller = caller;
        self
    }

    /// The outer step's verified caller (anonymous when it had none).
    #[must_use]
    pub fn caller(&self) -> &VerifiedCaller {
        &self.caller
    }
}

tokio::task_local! {
    static CURRENT: ToolCallFrame;
}

/// Run `fut` with `frame` as the current tool call.
pub async fn within<F: Future>(frame: ToolCallFrame, fut: F) -> F::Output {
    CURRENT.scope(frame, fut).await
}

/// The tool call the current task is dispatching, if any.
#[must_use]
pub fn current_tool_call() -> Option<ToolCallFrame> {
    CURRENT.try_with(Clone::clone).ok()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn there_is_no_frame_outside_a_tool_call() {
        assert!(current_tool_call().is_none());
    }

    #[tokio::test]
    async fn a_frame_is_visible_inside_shadowable_and_gone_after() {
        let (outer, nested) = within(ToolCallFrame::new(Some("s"), "c1"), async {
            let outer = current_tool_call();
            let nested = within(ToolCallFrame::new(Some("s2"), "c2"), async {
                current_tool_call()
            })
            .await;
            (outer, nested)
        })
        .await;
        assert_eq!(
            outer,
            Some(ToolCallFrame {
                session_id: Some("s".into()),
                call_id: "c1".into(),
                caller: VerifiedCaller::default(),
            })
        );
        assert_eq!(nested.map(|f| f.call_id), Some("c2".to_string()));
        assert!(current_tool_call().is_none());
    }

    #[test]
    fn the_frame_exposes_the_caller_it_was_built_with_and_defaults_anonymous() {
        let alice = VerifiedCaller {
            user_verified: true,
            sub: Some("alice".into()),
            ..VerifiedCaller::default()
        };
        let f = ToolCallFrame::new(Some("s"), "c").with_caller(alice.clone());
        assert_eq!(f.caller(), &alice);
        let anon = ToolCallFrame::new(Some("s"), "c");
        assert_eq!(anon.caller(), &VerifiedCaller::default());
        assert!(!anon.caller().user_verified);
    }

    #[test]
    fn an_empty_session_is_no_session() {
        assert_eq!(ToolCallFrame::new(Some(""), "c").session_id, None);
    }
}
