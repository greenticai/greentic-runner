//! The executed step path of one turn (run-outcome wire contract v3).
//!
//! The observer appends the id of every node that STARTS during the turn, in
//! execution order. Two rules keep it small and free of content:
//!
//! - **Node ids only.** Never a payload, never an output, never user data.
//! - **Bounded.** Consecutive duplicates collapse into one entry (a node that
//!   routes back to itself is one step), and at most
//!   [`MAX_PATH_STEPS`] entries are kept. A step that would have been appended
//!   past the cap sets [`StepPath::truncated`], which the wire reports as
//!   `path_truncated: true`.
//!
//! What the path covers is exactly what the [`super::turn::RunOutcomeObserver`]
//! sees, which is everything the flow engine walks in the turn: a `flow.call`
//! sub-flow's nodes (the engine hands the same observer to the callee), and a
//! `flow.goto` target's nodes (a goto is a jump inside the SAME walk). It does
//! NOT cover a flow an agentic worker runs as a `flow:` tool — that runs
//! through `PackRuntime::run_flow_for_tool*` with no observer, so such a turn's
//! path shows the agent node and not the tool flow's nodes.
//!
//! # Engine retries
//!
//! `FlowEngine::execute_with_entry` retries a transiently failing walk from
//! its entry node (default 3 attempts), with the same observer. The path must
//! describe the attempt that decided the outcome, not every attempt glued
//! together (`a, b, a, b, c`) — and a concatenation could spend the cap on the
//! failed attempts and cut off the one that mattered. So each walk entered
//! through `execute*` pushes a [`PathMark`] on attempt 1 and rewinds to it on
//! every retry. The marks nest: a retry of the TOP-LEVEL walk rewinds the whole
//! turn, while a retry inside a `flow.call` callee (whose attempt counter
//! restarts at 1, so `FlowContext::attempt` alone cannot tell the two apart)
//! rewinds only the callee's own steps and keeps the caller's prefix.
//!
//! Two documented limits: consecutive-duplicate collapse does not look at
//! which flow a node belongs to (a callee node sharing its caller node's id,
//! adjacent in the walk, collapses into one entry), and a failure raised while
//! rendering a node's input — before the node starts — leaves that node out.

/// Most node ids one event's `path` carries.
pub(crate) const MAX_PATH_STEPS: usize = 32;

/// Where a walk's first attempt began, to rewind to on a retry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PathMark {
    len: usize,
    truncated: bool,
}

/// The bounded, duplicate-collapsed list of node ids executed in one turn.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct StepPath {
    steps: Vec<String>,
    truncated: bool,
    /// One mark per walk currently executing (see the module doc).
    walks: Vec<PathMark>,
}

impl StepPath {
    /// A walk entered through `execute*` starts `attempt` (1-based). Attempt 1
    /// opens the walk; a later attempt is a retry, which discards whatever
    /// the walk's earlier attempts recorded.
    pub(crate) fn attempt(&mut self, attempt: u32) {
        if attempt <= 1 {
            self.walks.push(PathMark {
                len: self.steps.len(),
                truncated: self.truncated,
            });
            return;
        }
        let mark = self.walks.last().copied().unwrap_or(PathMark {
            len: 0,
            truncated: false,
        });
        self.steps.truncate(mark.len);
        self.truncated = mark.truncated;
    }

    /// The innermost walk returned.
    pub(crate) fn exit(&mut self) {
        self.walks.pop();
    }

    /// Record that `node_id` started. A repeat of the last entry is not a new
    /// step; a new step past the cap is dropped and marks the path truncated.
    pub(crate) fn push(&mut self, node_id: &str) {
        if self.steps.last().is_some_and(|last| last == node_id) {
            return;
        }
        if self.steps.len() >= MAX_PATH_STEPS {
            self.truncated = true;
            return;
        }
        self.steps.push(node_id.to_string());
    }

    pub(crate) fn steps(&self) -> &[String] {
        &self.steps
    }

    pub(crate) fn truncated(&self) -> bool {
        self.truncated
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consecutive_duplicates_collapse_but_revisits_are_kept() {
        let mut path = StepPath::default();
        for node in ["a", "a", "b", "b", "b", "a"] {
            path.push(node);
        }
        assert_eq!(path.steps(), ["a", "b", "a"]);
        assert!(!path.truncated());
    }

    #[test]
    fn a_retry_of_the_top_level_walk_keeps_only_the_last_attempt() {
        let mut path = StepPath::default();
        path.attempt(1);
        for node in ["a", "b"] {
            path.push(node);
        }
        path.attempt(2);
        for node in ["a", "b", "c"] {
            path.push(node);
        }
        path.exit();
        assert_eq!(path.steps(), ["a", "b", "c"]);
    }

    #[test]
    fn a_retry_that_follows_a_cut_attempt_is_not_cut() {
        let mut path = StepPath::default();
        path.attempt(1);
        for i in 0..40 {
            path.push(&format!("n{i}"));
        }
        assert!(path.truncated());
        path.attempt(2);
        path.push("ok");
        assert_eq!(path.steps(), ["ok"]);
        assert!(!path.truncated(), "the failed attempt's cut is discarded");
    }

    #[test]
    fn a_callee_retry_rewinds_only_the_callee() {
        let mut path = StepPath::default();
        path.attempt(1); // top-level walk
        path.push("call");
        path.attempt(1); // `flow.call` callee, its own counter
        path.push("hello");
        path.push("flaky");
        path.attempt(2); // callee retry
        path.push("hello");
        path.push("flaky");
        path.exit(); // callee returns
        path.push("ask");
        path.exit();
        assert_eq!(path.steps(), ["call", "hello", "flaky", "ask"]);
    }

    #[test]
    fn the_cap_keeps_the_first_32_steps_and_marks_the_path_truncated() {
        let mut path = StepPath::default();
        for i in 0..MAX_PATH_STEPS {
            path.push(&format!("n{i}"));
        }
        assert_eq!(path.steps().len(), MAX_PATH_STEPS);
        assert!(!path.truncated(), "exactly at the cap is not truncated");

        // A repeat of the last step is not a new step, so it cuts nothing.
        path.push(&format!("n{}", MAX_PATH_STEPS - 1));
        assert!(!path.truncated());

        path.push("one_too_many");
        assert!(path.truncated());
        assert_eq!(path.steps().len(), MAX_PATH_STEPS);
        assert_eq!(path.steps()[0], "n0");
        assert_eq!(path.steps()[MAX_PATH_STEPS - 1], "n31");
    }
}
