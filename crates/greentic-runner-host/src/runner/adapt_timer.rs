use std::str::FromStr;
use std::sync::{Arc, Weak};
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use cron::Schedule;
use serde_json::json;
use tokio::task::JoinHandle;
use tokio::time::sleep;

use crate::engine::runtime::IngressEnvelope;
use crate::runtime::TenantRuntime;

pub fn spawn_timers(runtime: Arc<TenantRuntime>) -> Result<Vec<JoinHandle<()>>> {
    let mut handles = Vec::new();

    for timer in runtime.config().timers.clone() {
        let cron_expr = timer.cron.clone();
        let normalized = normalize_cron(&cron_expr);
        let schedule = Schedule::from_str(&normalized)
            .with_context(|| format!("invalid cron expression for {}", timer.schedule_id()))?;
        let flow_id = timer.flow_id.clone();
        let schedule_id = timer.schedule_id().to_string();
        let tenant = runtime.config().tenant.clone();
        // Hold the runtime weakly: a task owning an `Arc<TenantRuntime>` would
        // keep it alive forever, so `TenantRuntime::drop` (which aborts the
        // timers) could never run and every reload would duplicate cron firing.
        let runtime_weak: Weak<TenantRuntime> = Arc::downgrade(&runtime);

        let handle = tokio::spawn(async move {
            tracing::info!(
                flow_id = %flow_id,
                schedule_id = %schedule_id,
                cron = %cron_expr,
                normalized_cron = %normalized,
                "registered timer schedule"
            );
            // Re-derive the next tick from "now" each round (rather than walking
            // one `upcoming` iterator) so ticks that elapsed while a slow flow
            // ran are skipped instead of fired back-to-back.
            let mut last_fired: Option<DateTime<Utc>> = None;
            while let Some(next) = schedule
                .after(&search_origin(Utc::now(), last_fired))
                .next()
            {
                last_fired = Some(next);
                if let Some(wait) = duration_until(next) {
                    sleep(wait).await;
                } else {
                    continue;
                }
                let Some(runtime_clone) = runtime_weak.upgrade() else {
                    tracing::info!(
                        flow_id = %flow_id,
                        schedule_id = %schedule_id,
                        "tenant runtime dropped; stopping timer schedule"
                    );
                    return;
                };
                let pack_id = match runtime_clone.engine().flow_by_id(&flow_id) {
                    Some(flow) => flow.pack_id.clone(),
                    None => {
                        tracing::error!(
                            flow_id = %flow_id,
                            schedule_id = %schedule_id,
                            "timer flow is ambiguous; pack_id is required"
                        );
                        continue;
                    }
                };
                let payload = json!({
                    "now": next.to_rfc3339(),
                    "schedule_id": schedule_id.clone(),
                });
                tracing::info!(
                    flow_id = %flow_id,
                    schedule_id = %schedule_id,
                    scheduled_for = %next,
                    "triggering timer flow"
                );
                let envelope = IngressEnvelope {
                    tenant: tenant.clone(),
                    env: None,
                    pack_id: Some(pack_id),
                    flow_id: flow_id.clone(),
                    flow_type: Some("timer".into()),
                    action: Some("timer".into()),
                    session_hint: Some(schedule_id.clone()),
                    provider: Some("timer".into()),
                    // Timers fire without a messaging endpoint by definition.
                    messaging_endpoint_id: None,
                    channel: Some(schedule_id.clone()),
                    conversation: Some(schedule_id.clone()),
                    user: None,
                    // A timer fires the flow from its entrypoint; it carries no card nav.
                    entry_node: None,
                    activity_id: Some(format!("{}@{}", schedule_id, next)),
                    timestamp: Some(next.to_rfc3339()),
                    payload,
                    metadata: None,
                    reply_scope: None,
                }
                .canonicalize();
                match runtime_clone.state_machine().handle(envelope).await {
                    Ok(output) => {
                        tracing::info!(
                            flow_id = %flow_id,
                            schedule_id = %schedule_id,
                            now = %next,
                            response = %output,
                            "timer flow completed"
                        );
                    }
                    Err(err) => {
                        let chain = err.chain().map(|e| e.to_string()).collect::<Vec<_>>();
                        tracing::error!(
                            flow_id = %flow_id,
                            schedule_id = %schedule_id,
                            error.cause_chain = ?chain,
                            "timer flow execution failed"
                        );
                    }
                }
            }
            tracing::info!(
                flow_id = %flow_id,
                schedule_id = %schedule_id,
                "timer schedule completed"
            );
        });

        handles.push(handle);
    }

    Ok(handles)
}

/// Where to search for the next tick: never earlier than the last tick fired,
/// so a wall clock stepped backwards (NTP) cannot make a tick fire twice.
fn search_origin(now: DateTime<Utc>, last_fired: Option<DateTime<Utc>>) -> DateTime<Utc> {
    match last_fired {
        Some(last) if last > now => last,
        _ => now,
    }
}

fn duration_until(next: DateTime<Utc>) -> Option<Duration> {
    let now = Utc::now();
    let duration = next - now;
    if duration.num_milliseconds() <= 0 {
        return Some(Duration::from_secs(0));
    }
    duration.to_std().ok()
}

fn normalize_cron(expr: &str) -> String {
    if expr.split_whitespace().count() == 5 {
        format!("0 {expr}")
    } else {
        expr.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn normalize_cron_adds_seconds_for_five_fields() {
        assert_eq!(normalize_cron("*/5 * * * *"), "0 */5 * * * *");
        assert_eq!(normalize_cron("0 */2 * * * *"), "0 */2 * * * *");
    }

    #[test]
    fn a_clock_stepped_back_does_not_repeat_a_fired_tick() {
        let schedule = Schedule::from_str("0 * * * * *").expect("cron");
        let now = Utc::now();
        let fired = schedule.after(&now).next().expect("tick");
        // Wall clock stepped back to before the tick that just fired.
        let stepped_back = fired - chrono::Duration::seconds(30);
        let naive = schedule.after(&stepped_back).next().expect("tick");
        assert_eq!(naive, fired, "re-deriving from now would repeat the tick");
        let origin = search_origin(stepped_back, Some(fired));
        let next = schedule.after(&origin).next().expect("tick");
        assert!(next > fired);
        assert_eq!(
            search_origin(fired + chrono::Duration::seconds(5), Some(fired)),
            fired + chrono::Duration::seconds(5)
        );
    }

    #[test]
    fn duration_until_returns_zero_for_past_times() {
        let past = Utc::now() - chrono::Duration::seconds(10);
        assert_eq!(duration_until(past).unwrap(), Duration::from_secs(0));
    }

    #[test]
    fn duration_until_returns_positive_duration_for_future_times() {
        let future = Utc::now() + chrono::Duration::milliseconds(150);
        let wait = duration_until(future).unwrap();
        assert!(wait > Duration::from_secs(0));
        assert!(wait <= Duration::from_secs(1));
    }

    #[test]
    fn normalize_cron_preserves_non_five_field_expressions() {
        assert_eq!(normalize_cron("@daily"), "@daily");
        assert_eq!(normalize_cron("0 0 */2 * * *"), "0 0 */2 * * *");
    }

    #[tokio::test]
    async fn spawn_timers_is_empty_when_config_has_no_timers() {
        let (_workspace, runtime) = crate::test_support::build_test_runtime()
            .await
            .expect("runtime");
        let timers = spawn_timers(Arc::clone(&runtime)).expect("spawn timers");
        assert!(timers.is_empty());
    }

    #[tokio::test]
    async fn timer_task_does_not_keep_the_runtime_alive() {
        let (_workspace, runtime) = crate::test_support::build_test_runtime_with_timers(
            r#"[{ flow_id: "flow.main", cron: "* * * * * *" }]"#,
        )
        .await
        .expect("runtime");
        let weak = Arc::downgrade(&runtime);
        let mut timers = spawn_timers(runtime).expect("spawn timers");
        // The caller's Arc was the only strong reference.
        assert!(
            weak.upgrade().is_none(),
            "timer task must hold the runtime weakly"
        );
        let handle = timers.pop().expect("one timer");
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("timer task must stop once the runtime is gone")
            .expect("timer task must not panic");
    }

    #[test]
    fn next_tick_is_always_in_the_future() {
        let schedule = Schedule::from_str("* * * * * *").expect("schedule");
        let next = schedule.after(&Utc::now()).next().expect("next tick");
        assert!(next > Utc::now() - chrono::Duration::milliseconds(50));
    }

    #[tokio::test]
    async fn spawn_timers_rejects_an_invalid_cron_expression() {
        let (_workspace, runtime) = crate::test_support::build_test_runtime_with_timers(
            r#"[{ flow_id: "flow.main", cron: "not a cron" }]"#,
        )
        .await
        .expect("runtime");
        let err = spawn_timers(runtime).expect_err("bad cron must be refused");
        assert!(
            err.to_string().contains("invalid cron expression"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn a_timer_for_an_unknown_flow_keeps_running_instead_of_dying() {
        let (_workspace, runtime) = crate::test_support::build_test_runtime_with_timers(
            r#"[{ flow_id: "flow.does.not.exist", cron: "* * * * * *", schedule_id: "every-second" }]"#,
        )
        .await
        .expect("runtime");
        // Keep the runtime alive: timers only hold it weakly.
        let handles = spawn_timers(Arc::clone(&runtime)).expect("spawn timers");
        assert_eq!(handles.len(), 1);
        // Let at least one tick fire against the missing flow.
        sleep(Duration::from_millis(2300)).await;
        assert!(
            !handles[0].is_finished(),
            "an unresolvable flow must be logged and skipped, not end the schedule"
        );
        for handle in handles {
            handle.abort();
        }
        drop(runtime);
    }
}
