//! JetStream durability for the agentic dispatch consumer: a `greentic-agentic`
//! stream binds `greentic.agentic.request.v1`; a durable pull consumer
//! `agentic-workers` (explicit ack) lets aw-serve replicas share the queue and
//! lets KEDA scale on consumer lag.

use anyhow::{Context, Result};
use async_nats::jetstream::{self, AckKind, consumer, stream};
use futures_util::StreamExt;
use greentic_types::request_topic;
use std::sync::Arc;
use std::time::Duration;

use crate::{AgentDispatchInvoker, Disposition, RUNTIME_NAME};

pub const STREAM_NAME: &str = "greentic-agentic";
pub const DURABLE_CONSUMER: &str = "agentic-workers";

/// Returns the JetStream stream config that binds `greentic.agentic.request.v1`
/// with work-queue retention (each message is removed once any consumer acks it).
#[must_use]
pub fn agentic_stream_config() -> stream::Config {
    stream::Config {
        name: STREAM_NAME.to_string(),
        subjects: vec![request_topic(RUNTIME_NAME)],
        retention: stream::RetentionPolicy::WorkQueue,
        ..Default::default()
    }
}

/// Returns the durable pull consumer config (`agentic-workers`) with explicit
/// ack and a max-deliver cap of 5 to prevent poison-pill messages from looping
/// indefinitely.
#[must_use]
pub fn agentic_consumer_config() -> consumer::pull::Config {
    consumer::pull::Config {
        durable_name: Some(DURABLE_CONSUMER.to_string()),
        ack_policy: consumer::AckPolicy::Explicit,
        max_deliver: 5,
        ack_wait: ACK_WAIT,
        ..Default::default()
    }
}

/// Ensure the `greentic-agentic` stream and `agentic-workers` durable pull
/// consumer both exist, then return a handle to the consumer.
///
/// Idempotent: uses `get_or_create_stream` / `get_or_create_consumer`, so
/// calling this on every process start is safe.
pub async fn ensure_consumer(
    client: &async_nats::Client,
) -> Result<consumer::Consumer<consumer::pull::Config>> {
    let js = jetstream::new(client.clone());
    let stream = js
        .get_or_create_stream(agentic_stream_config())
        .await
        .context("get_or_create greentic-agentic stream")?;
    let consumer = match stream
        .get_or_create_consumer(DURABLE_CONSUMER, agentic_consumer_config())
        .await
    {
        Ok(consumer) => consumer,
        Err(create_err) => {
            // A durable created by an older build (default 30s ack_wait) has a
            // differing config; `ack_wait` is updatable in place.
            tracing::warn!(error = %create_err, "agentic-workers consumer config differs; updating");
            stream
                .update_consumer(agentic_consumer_config())
                .await
                .context("update agentic-workers consumer")?;
            stream
                .get_consumer(DURABLE_CONSUMER)
                .await
                .map_err(|e| anyhow::anyhow!("get agentic-workers consumer: {e}"))?
        }
    };
    Ok(consumer)
}

/// JetStream `ack_wait`: how long the server waits for an ack before
/// redelivering. Must exceed the agent step timeout (default 60s) plus the
/// session-lock wait, otherwise a slow-but-healthy turn is redelivered and run
/// twice. In-flight handlers also send periodic progress acks ([`PROGRESS_EVERY`]).
pub const ACK_WAIT: Duration = Duration::from_secs(300);

/// Interval of `in_progress` acks while a message is being handled; a fraction
/// of [`ACK_WAIT`] so a turn longer than `ack_wait` is still not redelivered.
pub const PROGRESS_EVERY: Duration = Duration::from_secs(30);

/// Backoff bounds for re-opening the message stream after an error.
const BACKOFF_BASE: Duration = Duration::from_millis(500);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

/// Exponential backoff (`500ms * 2^attempt`, capped at 30s).
#[must_use]
pub fn backoff_delay(attempt: u32) -> Duration {
    BACKOFF_BASE
        .saturating_mul(1u32.checked_shl(attempt.min(16)).unwrap_or(u32::MAX))
        .min(BACKOFF_MAX)
}

/// Whether a handled message should be acked.  Errors are left un-acked so
/// JetStream redelivers (the invoker is idempotent — PR2).
#[must_use]
pub fn should_ack(handle_result: &Result<()>) -> bool {
    handle_result.is_ok()
}

/// The JetStream settlement for a handled message: `None` leaves it un-acked
/// (redelivery), `Term` stops redelivery of a request that can never succeed.
#[must_use]
pub fn settlement(result: &Result<Disposition>) -> Option<AckKind> {
    match result {
        Ok(Disposition::Handled | Disposition::Skipped) => Some(AckKind::Ack),
        Ok(Disposition::Rejected) => Some(AckKind::Term),
        Err(_) => None,
    }
}

/// Pull messages from the `agentic-workers` durable consumer, dispatch each
/// via [`crate::handle_message_outcome`], and settle it (ack / term).
///
/// Un-acked messages are redelivered up to `max_deliver` times (5) before
/// JetStream marks them as dead-lettered.  Each message is processed in its
/// own `tokio::spawn` so one slow agent does not stall the pull loop, with
/// `in_progress` acks sent while it runs. A stream error is logged and the
/// message stream re-opened with backoff rather than ending the consumer.
pub async fn run_bridge_jetstream(
    client: async_nats::Client,
    invoker: Arc<dyn AgentDispatchInvoker>,
) -> Result<()> {
    let consumer = ensure_consumer(&client).await?;
    let mut attempt: u32 = 0;
    loop {
        match consumer.messages().await {
            Ok(mut messages) => {
                while let Some(item) = messages.next().await {
                    let js_msg = match item {
                        Ok(m) => m,
                        Err(error) => {
                            tracing::warn!(%error, "jetstream message error; continuing");
                            tokio::time::sleep(backoff_delay(attempt)).await;
                            attempt = attempt.saturating_add(1);
                            continue;
                        }
                    };
                    attempt = 0;
                    let client = client.clone();
                    let invoker = invoker.clone();
                    tokio::spawn(handle_one(client, invoker, js_msg));
                }
                tracing::warn!("jetstream message stream ended; re-opening");
            }
            Err(error) => tracing::warn!(%error, "open jetstream messages stream failed"),
        }
        tokio::time::sleep(backoff_delay(attempt)).await;
        attempt = attempt.saturating_add(1);
    }
}

async fn handle_one(
    client: async_nats::Client,
    invoker: Arc<dyn AgentDispatchInvoker>,
    js_msg: jetstream::Message,
) {
    // jetstream::Message has a `.message: async_nats::Message` field and
    // also implements Deref<Target = async_nats::Message>.  We extract the
    // inner core message via the public `.message` field so that
    // handle_message receives the owned async_nats::Message it expects.
    let core = js_msg.message.clone();
    let handling = crate::handle_message_outcome(&client, invoker, core);
    tokio::pin!(handling);
    let mut ticker = tokio::time::interval(PROGRESS_EVERY);
    ticker.tick().await; // first tick fires immediately
    let result = loop {
        tokio::select! {
            result = &mut handling => break result,
            _ = ticker.tick() => {
                if let Err(error) = js_msg.ack_with(AckKind::Progress).await {
                    tracing::warn!(%error, "jetstream in-progress ack failed");
                }
            }
        }
    };
    match settlement(&result) {
        Some(kind) => {
            if let Err(ack_err) = js_msg.ack_with(kind).await {
                tracing::error!(error = %ack_err, "jetstream ack failed");
            }
        }
        None => {
            if let Err(handle_err) = result {
                tracing::error!(
                    error = %handle_err,
                    "agentic handler failed; leaving unacked for redelivery"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ack_only_on_success() {
        assert!(should_ack(&Ok(())));
        assert!(!should_ack(&Err(anyhow::anyhow!("boom"))));
    }

    #[test]
    fn stream_config_binds_agentic_request_subject() {
        let cfg = agentic_stream_config();
        assert_eq!(cfg.name, "greentic-agentic");
        assert!(
            cfg.subjects
                .iter()
                .any(|s| s == "greentic.agentic.request.v1"),
            "expected greentic.agentic.request.v1 in subjects, got {:?}",
            cfg.subjects
        );
    }

    #[test]
    fn stream_config_uses_work_queue_retention() {
        let cfg = agentic_stream_config();
        assert!(
            matches!(cfg.retention, stream::RetentionPolicy::WorkQueue),
            "expected WorkQueue retention, got {:?}",
            cfg.retention
        );
    }

    #[test]
    fn consumer_config_is_durable_explicit_ack() {
        let cfg = agentic_consumer_config();
        assert_eq!(
            cfg.durable_name.as_deref(),
            Some("agentic-workers"),
            "durable_name must be agentic-workers"
        );
        assert!(
            matches!(cfg.ack_policy, consumer::AckPolicy::Explicit),
            "ack_policy must be Explicit, got {:?}",
            cfg.ack_policy
        );
    }

    #[test]
    fn consumer_config_caps_max_deliver() {
        let cfg = agentic_consumer_config();
        assert_eq!(
            cfg.max_deliver, 5,
            "max_deliver should be 5 to prevent poison-pill loops"
        );
    }

    #[test]
    fn ack_wait_exceeds_progress_interval_and_step_timeout() {
        assert!(ACK_WAIT > PROGRESS_EVERY * 2);
        assert!(ACK_WAIT > Duration::from_secs(60), "default step timeout");
        assert_eq!(agentic_consumer_config().ack_wait, ACK_WAIT);
    }

    #[test]
    fn settlement_acks_skips_and_terms_but_never_acks_errors() {
        assert!(matches!(
            settlement(&Ok(Disposition::Handled)),
            Some(AckKind::Ack)
        ));
        // Lock contention: acked, no failure response published.
        assert!(matches!(
            settlement(&Ok(Disposition::Skipped)),
            Some(AckKind::Ack)
        ));
        assert!(matches!(
            settlement(&Ok(Disposition::Rejected)),
            Some(AckKind::Term)
        ));
        assert!(settlement(&Err(anyhow::anyhow!("publish failed"))).is_none());
    }

    #[test]
    fn backoff_grows_and_is_capped() {
        assert_eq!(backoff_delay(0), Duration::from_millis(500));
        assert_eq!(backoff_delay(1), Duration::from_secs(1));
        assert_eq!(backoff_delay(100), BACKOFF_MAX);
    }
}
