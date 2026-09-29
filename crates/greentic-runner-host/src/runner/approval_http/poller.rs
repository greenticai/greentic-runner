//! The one polling task per runtime: fetches decisions, enforces deadlines,
//! and resumes each approval at most once.

use std::sync::Arc;
use std::time::Duration;

use greentic_types::{DispatchError, RuntimeDispatchResponse};
use secrecy::ExposeSecret;
use serde_json::Value;
use tokio::time::Instant;

use super::wire::{self, ApprovalDecisionBody, ApprovalWithdrawBody, WithdrawReason};
use super::{Inner, Pending};
use crate::runner::dispatch_listener::decode_response;
use crate::runner::remote_dispatch::build_timeout_response_message_with_token;

/// Longest the loop sleeps without re-checking (bounds the effect of a lost
/// wake-up).
const MAX_IDLE: Duration = Duration::from_secs(60);

/// What one poll of one id decided.
enum PollOutcome {
    /// `204`: still pending.
    Pending,
    /// `200` with a readable body.
    Decided(ApprovalDecisionBody),
    /// `200` whose body is unreadable. Still ends polling (spec §4.4 rule 1).
    Unreadable,
    /// Transport error, `429`, `5xx`: try again later.
    Transient(String),
    /// `401`/`403`/`404`/`410` or any other answer: stop asking about this id.
    Gone(reqwest::StatusCode),
}

pub(super) async fn run(inner: Arc<Inner>) {
    loop {
        if inner.cancel.is_cancelled() {
            break;
        }
        let now = Instant::now();
        let (expired, due) = collect_due(&inner, now);
        for (correlation_id, pending) in expired {
            if inner.cancel.is_cancelled() {
                break;
            }
            expire(&inner, &correlation_id, pending).await;
        }
        for correlation_id in due {
            if inner.cancel.is_cancelled() {
                break;
            }
            poll_one(&inner, &correlation_id).await;
        }
        let sleep_for = next_wake(&inner, Instant::now());
        tokio::select! {
            () = inner.cancel.cancelled() => break,
            () = inner.wake.notified() => {}
            () = tokio::time::sleep(sleep_for) => {}
        }
    }
    // Release everything this revision held, whoever cancelled it.
    inner.pending.lock().clear();
    inner.resumer.lock().take();
}

/// Split the pending set into ids past their deadline (REMOVED here, so no
/// poll can decide them afterwards) and ids due for a poll.
fn collect_due(inner: &Inner, now: Instant) -> (Vec<(String, Pending)>, Vec<String>) {
    let mut pending = inner.pending.lock();
    let expired_ids: Vec<String> = pending
        .iter()
        .filter(|(_, p)| p.deadline_at <= now)
        .map(|(id, _)| id.clone())
        .collect();
    let expired = expired_ids
        .into_iter()
        .filter_map(|id| pending.remove(&id).map(|p| (id, p)))
        .collect();
    let due = pending
        .iter()
        .filter(|(_, p)| p.next_poll_at <= now)
        .map(|(id, _)| id.clone())
        .collect();
    (expired, due)
}

fn next_wake(inner: &Inner, now: Instant) -> Duration {
    let pending = inner.pending.lock();
    pending
        .values()
        .map(|p| p.next_poll_at.min(p.deadline_at))
        .min()
        .map(|at| at.saturating_duration_since(now))
        .unwrap_or(MAX_IDLE)
        .min(MAX_IDLE)
}

async fn poll_one(inner: &Inner, correlation_id: &str) {
    let outcome = fetch_decision(inner, correlation_id).await;
    let now = Instant::now();
    match outcome {
        PollOutcome::Pending => reschedule(inner, correlation_id, now, None),
        PollOutcome::Transient(reason) => {
            tracing::debug!(%correlation_id, %reason, "approval decision poll failed; backing off");
            reschedule(inner, correlation_id, now, Some(inner.timing.backoff));
        }
        PollOutcome::Gone(status) => {
            if inner.pending.lock().remove(correlation_id).is_some() {
                tracing::warn!(
                    %correlation_id,
                    %status,
                    "approval inbox no longer answers for this approval; stopped waiting for it \
                     (the flow stays parked)"
                );
            }
        }
        PollOutcome::Unreadable => {
            if inner.pending.lock().remove(correlation_id).is_some() {
                tracing::warn!(
                    %correlation_id,
                    "approval inbox answered with a decision this runner cannot read; stopped \
                     waiting for it (the flow stays parked)"
                );
            }
        }
        PollOutcome::Decided(decision) => {
            // First `200` ends polling, whatever happens next: take the entry
            // out BEFORE resuming, so no later tick can resume it again.
            let Some(pending) = inner.pending.lock().remove(correlation_id) else {
                return;
            };
            let mut output = serde_json::json!({
                "decision": decision.decision.as_str(),
                "resolved_by": decision.resolved_by,
                "note": decision.note,
            });
            if let Some(token) = pending.decision_token.as_ref()
                && let Value::Object(map) = &mut output
            {
                map.insert(
                    "decision_token".to_string(),
                    Value::String(token.expose_secret().to_string()),
                );
            }
            let response = RuntimeDispatchResponse {
                ok: true,
                output,
                events: vec![],
                error: None::<DispatchError>,
            };
            let body = serde_json::to_vec(&response).unwrap_or_default();
            resume(inner, correlation_id, &pending, &body).await;
        }
    }
}

fn reschedule(inner: &Inner, correlation_id: &str, now: Instant, after: Option<Duration>) {
    let mut pending = inner.pending.lock();
    if let Some(entry) = pending.get_mut(correlation_id) {
        let interval = after.unwrap_or_else(|| {
            if now.saturating_duration_since(entry.registered_at) < inner.timing.fast_window {
                inner.timing.fast_interval
            } else {
                inner.timing.slow_interval
            }
        });
        entry.next_poll_at = now + interval;
    }
}

async fn fetch_decision(inner: &Inner, correlation_id: &str) -> PollOutcome {
    let mut url = inner.decision_url.clone();
    url.query_pairs_mut()
        .append_pair(wire::DECISION_QUERY_PARAM, correlation_id);
    let sent = inner
        .http
        .get(url)
        .bearer_auth(inner.token.expose_secret())
        .header(wire::APPROVAL_RAIL_HEADER, wire::APPROVAL_RAIL_VERSION)
        .send()
        .await;
    let response = match sent {
        Ok(response) => response,
        Err(error) => return PollOutcome::Transient(error.without_url().to_string()),
    };
    let status = response.status();
    if status == reqwest::StatusCode::NO_CONTENT {
        return PollOutcome::Pending;
    }
    if status == reqwest::StatusCode::OK {
        return match response.json::<ApprovalDecisionBody>().await {
            Ok(decision) => PollOutcome::Decided(decision),
            Err(_) => PollOutcome::Unreadable,
        };
    }
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
        return PollOutcome::Transient(format!("HTTP {status}"));
    }
    PollOutcome::Gone(status)
}

/// Deadline reached: withdraw at the admin, then resume locally with the same
/// `timeout` body the NATS watchdog publishes, token included.
async fn expire(inner: &Inner, correlation_id: &str, pending: Pending) {
    withdraw(inner, correlation_id, pending.deadline_reason).await;
    let (_subject, _headers, body) = build_timeout_response_message_with_token(
        crate::runner::engine::APPROVAL_RUNTIME,
        correlation_id,
        pending.tenant.tenant_id.as_str(),
        &pending.env,
        pending.deadline_ms,
        pending.decision_token.as_ref().map(|t| t.expose_secret()),
    );
    resume(inner, correlation_id, &pending, &body).await;
}

async fn withdraw(inner: &Inner, correlation_id: &str, reason: WithdrawReason) {
    let body = ApprovalWithdrawBody {
        tenant_slug: inner.tenant_slug.clone(),
        correlation_id: correlation_id.to_string(),
        reason,
    };
    let sent = inner
        .http
        .post(&inner.withdraw_url)
        .bearer_auth(inner.token.expose_secret())
        .header(wire::APPROVAL_RAIL_HEADER, wire::APPROVAL_RAIL_VERSION)
        .json(&body)
        .send()
        .await;
    match sent {
        Ok(response) if response.status().is_success() => {}
        Ok(response) => tracing::warn!(
            %correlation_id,
            status = %response.status(),
            "approval inbox refused the withdrawal; the timeout is applied locally anyway"
        ),
        Err(error) => tracing::warn!(
            %correlation_id,
            error = %error.without_url(),
            "could not withdraw the approval; the timeout is applied locally anyway"
        ),
    }
}

/// The ONE resume attempt for this id. Never retried. The resumer is strict,
/// so a gate that is no longer parked under this exact id drops it rather
/// than starting a fresh run.
async fn resume(inner: &Inner, correlation_id: &str, pending: &Pending, body: &[u8]) {
    if inner.cancel.is_cancelled() {
        return;
    }
    let Some(resumer) = inner.resumer.lock().clone() else {
        tracing::warn!(
            %correlation_id,
            "approval decision arrived but this runtime has no resumer attached; dropped"
        );
        return;
    };
    let input = match decode_response(
        Some(correlation_id),
        Some(pending.tenant.tenant_id.as_str()),
        Some(&pending.env),
        body,
    ) {
        Ok(input) => input,
        Err(error) => {
            tracing::error!(%correlation_id, %error, "could not build the approval resume input");
            return;
        }
    };
    // The dispatch's own tenant context, not one rebuilt from strings.
    if let Err(error) = resumer
        .resume(pending.tenant.clone(), &input.correlation_id, input.output)
        .await
    {
        tracing::error!(%correlation_id, %error, "failed to resume the approval gate");
    }
}
