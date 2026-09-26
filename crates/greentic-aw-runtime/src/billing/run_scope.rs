//! The run a deployed turn belongs to, for [`super::WorkerUsageMeter`]'s
//! optional `run_id` (audit wire contract v2 §5).
//!
//! The runner host wraps each flow turn in [`with_run_id`]; the meter reads
//! [`current_run_id`] SYNCHRONOUSLY inside `emit`, which the agent loop calls
//! inline on the turn's own task — so the value is the turn's run without
//! threading an id through `BillingMeter::emit` (a trait other meters
//! implement). Work spawned onto another task does not inherit it; its events
//! simply carry no `run_id`, which is what an event outside any run carries.

use std::future::Future;

tokio::task_local! {
    static CURRENT_RUN_ID: String;
}

/// Run `fut` with `run_id` as the current run.
pub async fn with_run_id<F: Future>(run_id: String, fut: F) -> F::Output {
    CURRENT_RUN_ID.scope(run_id, fut).await
}

/// The current task's run, if it runs inside [`with_run_id`].
pub fn current_run_id() -> Option<String> {
    CURRENT_RUN_ID.try_with(Clone::clone).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn the_run_id_is_visible_inside_the_scope_only() {
        assert_eq!(current_run_id(), None);
        let seen = with_run_id("01RUN".into(), async { current_run_id() }).await;
        assert_eq!(seen.as_deref(), Some("01RUN"));
        assert_eq!(current_run_id(), None);
    }
}
