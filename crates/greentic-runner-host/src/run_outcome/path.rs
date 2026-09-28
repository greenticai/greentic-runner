//! The executed step path of one turn (run-outcome wire contract v3).
//!
//! The observer appends the id of every node that STARTS during the turn, in
//! execution order. Two rules keep it small and free of content:
//!
//! - **Node ids only.** Never a payload, never an output, never user data.
//! - **Bounded.** Consecutive duplicates collapse into one entry (a node that
//!   loops on itself, or a start/end pair, is one step), and at most
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

/// Most node ids one event's `path` carries.
pub(crate) const MAX_PATH_STEPS: usize = 32;

/// The bounded, duplicate-collapsed list of node ids executed in one turn.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct StepPath {
    steps: Vec<String>,
    truncated: bool,
}

impl StepPath {
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
