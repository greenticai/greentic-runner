use std::str::FromStr;
use std::sync::Arc;
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
        let runtime_clone = Arc::clone(&runtime);

        let handle = tokio::spawn(async move {
            tracing::info!(
                flow_id = %flow_id,
                schedule_id = %schedule_id,
                cron = %cron_expr,
                normalized_cron = %normalized,
                "registered timer schedule"
            );
            for next in schedule.upcoming(Utc) {
                if let Some(wait) = duration_until(next) {
                    sleep(wait).await;
                } else {
                    continue;
                }
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
}
