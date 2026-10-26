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

use crate::{AgentDispatchInvoker, BusyPolicy, Disposition, RUNTIME_NAME};

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
/// ack and a hard max-deliver backstop ([`CONSUMER_MAX_DELIVER`]). The tight
/// poison-pill bound for handler errors is application-level ([`MAX_DELIVER`]);
/// a busy session is bounded by message AGE instead (see [`busy_budget`]).
#[must_use]
pub fn agentic_consumer_config() -> consumer::pull::Config {
    consumer::pull::Config {
        durable_name: Some(DURABLE_CONSUMER.to_string()),
        ack_policy: consumer::AckPolicy::Explicit,
        max_deliver: CONSUMER_MAX_DELIVER,
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
            // The server refused because the existing durable's config differs.
            tracing::warn!(error = %create_err, "agentic-workers consumer config differs; updating");
            return stream
                .update_consumer(agentic_consumer_config())
                .await
                .context("update agentic-workers consumer");
        }
    };
    // `get_or_create_consumer` can hand back an existing durable unchanged: one
    // created by an older build keeps its 30s `ack_wait`, which is shorter than
    // a turn and would redeliver (and double-run) it. Converge it in place.
    if ack_wait_is_stale(consumer.cached_info().config.ack_wait)
        || consumer.cached_info().config.max_deliver != CONSUMER_MAX_DELIVER
    {
        tracing::warn!(
            existing = ?consumer.cached_info().config.ack_wait,
            wanted = ?ACK_WAIT,
            "agentic-workers consumer ack_wait or max_deliver is stale; updating"
        );
        return stream
            .update_consumer(agentic_consumer_config())
            .await
            .context("update agentic-workers consumer ack_wait");
    }
    Ok(consumer)
}

/// Whether an existing durable's `ack_wait` differs from the one we require.
#[must_use]
pub fn ack_wait_is_stale(existing: Duration) -> bool {
    existing != ACK_WAIT
}

/// Application-level cap on HANDLER-ERROR attempts (poison-pill guard): on this
/// failed attempt an erroring request is answered and terminated.
///
/// It counts handler ERRORS only (see [`ErrorAttempts`]), not deliveries: a busy
/// Nak is also a delivery, and a request that waited out a long turn used to
/// arrive at its first handler error with its whole budget already spent. This is
/// not the consumer's `max_deliver` either ([`CONSUMER_MAX_DELIVER`]); busy
/// redelivery is bounded by message age ([`busy_budget`]).
pub const MAX_DELIVER: i64 = 6;

/// Server-side `max_deliver` of the durable consumer: an absolute backstop
/// above anything the age-based busy budget plus [`MAX_DELIVER`] error attempts
/// can reach. The longest busy budget (30 min handler ceiling + 60s margin =
/// 31 min) is about 40 deliveries at the 60s delay cap.
pub const CONSUMER_MAX_DELIVER: i64 = 100;

/// Most stream sequences [`ErrorAttempts`] tracks at once. When full, the
/// OLDEST tracked sequence is evicted (the counts are best effort,
/// [`CONSUMER_MAX_DELIVER`] still bounds); the rest keep their counts, so a
/// poison pill mid-way through its budget is not reset by unrelated traffic.
const ERROR_ATTEMPTS_CAP: usize = 4096;

/// Per-process count of handler-error attempts, keyed by stream sequence.
///
/// Process-local on purpose: JetStream only reports total deliveries, and a Nak
/// cannot carry a counter. A redelivery that lands on another replica starts
/// from zero, which grants extra error retries but never more than
/// [`CONSUMER_MAX_DELIVER`] deliveries in total.
#[derive(Debug, Default)]
pub struct ErrorAttempts {
    inner: std::sync::Mutex<AttemptTable>,
}

#[derive(Debug, Default)]
struct AttemptTable {
    counts: std::collections::HashMap<u64, i64>,
    /// Tracked sequences, oldest first (every key of `counts` appears once).
    order: std::collections::VecDeque<u64>,
}

impl ErrorAttempts {
    fn table(&self) -> std::sync::MutexGuard<'_, AttemptTable> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Record one more handler error for `sequence`; returns the new count.
    pub fn record(&self, sequence: u64) -> i64 {
        let mut table = self.table();
        if !table.counts.contains_key(&sequence) {
            while table.counts.len() >= ERROR_ATTEMPTS_CAP {
                match table.order.pop_front() {
                    Some(oldest) => {
                        table.counts.remove(&oldest);
                    }
                    None => break,
                }
            }
            table.order.push_back(sequence);
        }
        let count = table.counts.entry(sequence).or_insert(0);
        *count = count.saturating_add(1);
        *count
    }

    /// Forget `sequence` (the message was acked or terminated).
    pub fn clear(&self, sequence: u64) {
        let mut table = self.table();
        // Most settlements are successes that never recorded anything; only a
        // tracked sequence pays for the scan of `order`.
        if table.counts.remove(&sequence).is_some() {
            table.order.retain(|tracked| *tracked != sequence);
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.table().counts.len()
    }

    #[cfg(test)]
    fn count(&self, sequence: u64) -> Option<i64> {
        self.table().counts.get(&sequence).copied()
    }
}

/// Count the outcome of one delivery and decide how to settle it: record a
/// handler error against `sequence`, ask [`settlement`], and forget the sequence
/// unless the message will be redelivered (a Nak keeps its count). Returns the
/// settlement and the error attempt number (`0` when the result was not an
/// error). Apart from `handle_one` so the record -> settle -> clear wiring is
/// testable without NATS.
fn settle_delivery(
    attempts: &ErrorAttempts,
    result: &Result<Disposition>,
    sequence: Option<u64>,
    delivered: i64,
) -> (AckKind, i64) {
    let error_attempt = match (result, sequence) {
        (Err(_), Some(sequence)) => attempts.record(sequence),
        // No sequence to key on: this is the last delivery.
        (Err(_), None) => MAX_DELIVER,
        _ => 0,
    };
    let kind = settlement(result, delivered, error_attempt);
    if let Some(sequence) = sequence
        && !matches!(kind, AckKind::Nak(_))
    {
        attempts.clear(sequence);
    }
    (kind, error_attempt)
}

fn error_attempts() -> &'static ErrorAttempts {
    static ATTEMPTS: std::sync::OnceLock<ErrorAttempts> = std::sync::OnceLock::new();
    ATTEMPTS.get_or_init(ErrorAttempts::default)
}

/// Extra time on top of the handle timeout before a busy request is answered
/// `session_busy`: covers the holder finishing right at its bound plus the
/// redelivery delay and the lock wait.
pub const BUSY_BUDGET_MARGIN: Duration = Duration::from_secs(60);

/// Delay before the first redelivery of a request whose session was busy;
/// doubles on each further delivery up to [`BUSY_REDELIVER_MAX_DELAY`].
pub const BUSY_REDELIVER_DELAY: Duration = Duration::from_secs(5);

/// Cap of the busy redelivery delay.
pub const BUSY_REDELIVER_MAX_DELAY: Duration = Duration::from_secs(60);

/// Delay before a request whose handler failed (e.g. the response could not be
/// published) is redelivered.
pub const ERROR_REDELIVER_DELAY: Duration = Duration::from_secs(2);

/// Session-lock wait the runtime uses per attempt (see `acquire_lock`).
const LOCK_WAIT: Duration = Duration::from_secs(5);

/// Margin added to an agent's step timeout when sizing the handler bound.
const HANDLE_MARGIN: Duration = Duration::from_secs(30);

/// Hard ceiling of the derived handler bound.
const HANDLE_TIMEOUT_CEILING: Duration = Duration::from_secs(30 * 60);

/// Busy redelivery delay after `delivered` deliveries: 5s, 10s, 20s, 40s, 60s.
///
/// Exponential so the total busy budget (delays plus the per-attempt lock
/// wait) outlasts a legitimate turn (default step timeout 60s) that is still
/// running when the user's next message arrives.
#[must_use]
pub fn busy_redeliver_delay(delivered: i64) -> Duration {
    let exp = u32::try_from(delivered.saturating_sub(1).clamp(0, 16)).unwrap_or(16);
    BUSY_REDELIVER_DELAY
        .saturating_mul(1u32.checked_shl(exp).unwrap_or(u32::MAX))
        .min(BUSY_REDELIVER_MAX_DELAY)
}

/// How long (from the message's publish time) a request may keep being
/// redelivered because its session is busy before it is answered `session_busy`.
///
/// The session holder is a turn of the same agent, which may run for up to
/// [`handle_timeout_for`] the agent's step timeout (default 4 minutes, at most
/// 30), so the budget is that bound plus [`BUSY_BUDGET_MARGIN`]. It is finite by
/// construction (`HANDLE_TIMEOUT_CEILING` + margin), so a stuck lock cannot make
/// a poison loop.
#[must_use]
pub fn busy_budget(step_timeout: Option<Duration>) -> Duration {
    handle_timeout_for(step_timeout).saturating_add(BUSY_BUDGET_MARGIN)
}

/// Per-message handler bound: [`HANDLE_TIMEOUT`], raised when the agent's own
/// `limits.timeout` (+ lock wait + margin) needs longer, up to a ceiling.
/// `ack_wait` is not a limit here: in-progress acks keep the message alive.
#[must_use]
pub fn handle_timeout_for(step_timeout: Option<Duration>) -> Duration {
    match step_timeout {
        Some(step) => step
            .saturating_add(LOCK_WAIT)
            .saturating_add(HANDLE_MARGIN)
            .clamp(HANDLE_TIMEOUT, HANDLE_TIMEOUT_CEILING),
        None => HANDLE_TIMEOUT,
    }
}

/// Upper bound on handling ONE message (step timeout + session-lock wait +
/// margin). The default agent step timeout is 60s and the lock wait 5s; agents
/// configured with a longer `limits.timeout` up to ~3 minutes still fit. Kept
/// under [`ACK_WAIT`]. Without it a hung handler would be `in_progress`
/// heartbeated forever.
pub const HANDLE_TIMEOUT: Duration = Duration::from_secs(240);

/// JetStream `ack_wait`: how long the server waits for an ack before
/// redelivering. Must exceed the agent step timeout (default 60s) plus the
/// session-lock wait, otherwise a slow-but-healthy turn is redelivered and run
/// twice. In-flight handlers also send periodic progress acks ([`PROGRESS_EVERY`]).
pub const ACK_WAIT: Duration = Duration::from_secs(300);

/// Interval of `in_progress` acks while a message is being handled; a fraction
/// of [`ACK_WAIT`] so a turn longer than `ack_wait` is still not redelivered.
pub const PROGRESS_EVERY: Duration = Duration::from_secs(10);

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

/// Busy policy for a delivery: redeliver while the request is younger than its
/// [`busy_budget`] (and the consumer backstop has deliveries left), answer
/// `session_busy` once either is exhausted.
///
/// `age` is the time since the message was published; the next redelivery is
/// only worth scheduling when it can still land inside the budget.
#[must_use]
pub fn busy_policy(delivered: i64, age: Duration, budget: Duration) -> BusyPolicy {
    let next_delay = busy_redeliver_delay(delivered);
    if delivered >= CONSUMER_MAX_DELIVER - 1 || age.saturating_add(next_delay) > budget {
        BusyPolicy::Respond
    } else {
        BusyPolicy::Redeliver
    }
}

/// Time since the message was published (zero when the clocks disagree).
///
/// Measured from the PUBLISH time, so time spent in the stream backlog counts
/// against the busy budget. That is deliberate: the caller's own wait started at
/// publish, so a request that already queued for longer than the budget is
/// answered `session_busy` on its first busy delivery instead of being parked
/// even longer.
fn message_age(info: &jetstream::message::Info<'_>) -> Duration {
    let published_secs = u64::try_from(info.published.unix_timestamp()).unwrap_or(0);
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0);
    Duration::from_secs(now_secs.saturating_sub(published_secs))
}

/// Whether a handled message should be acked.  Errors are left un-acked so
/// JetStream redelivers (the invoker is idempotent — PR2).
#[must_use]
pub fn should_ack(handle_result: &Result<()>) -> bool {
    handle_result.is_ok()
}

/// The JetStream settlement for a handled message after `delivered` deliveries.
/// `Term` stops redelivery of a request that can never succeed (or has no
/// delivery left). A handler error is Nak'd with a short delay rather than left
/// to wait out `ack_wait`.
///
/// `delivered` is the TOTAL delivery count (busy Naks included; it sizes the busy
/// delay and drives the [`CONSUMER_MAX_DELIVER`] backstop). `error_attempts` is
/// the number of handler errors so far including this one; only it is held
/// against [`MAX_DELIVER`].
#[must_use]
pub fn settlement(result: &Result<Disposition>, delivered: i64, error_attempts: i64) -> AckKind {
    match result {
        Ok(Disposition::Handled) => AckKind::Ack,
        // Session busy and no response sent: redeliver later, never ack.
        Ok(Disposition::Busy) => AckKind::Nak(Some(busy_redeliver_delay(delivered))),
        Ok(Disposition::Rejected) => AckKind::Term,
        Err(_) if error_attempts >= MAX_DELIVER || delivered >= CONSUMER_MAX_DELIVER => {
            AckKind::Term
        }
        Err(_) => AckKind::Nak(Some(ERROR_REDELIVER_DELAY)),
    }
}

/// Pull messages from the `agentic-workers` durable consumer, dispatch each
/// via [`crate::handle_message_outcome`], and settle it (ack / term).
///
/// Un-acked messages are redelivered up to `max_deliver` times
/// ([`CONSUMER_MAX_DELIVER`]) before JetStream gives up on them; handler errors
/// are terminated after [`MAX_DELIVER`] deliveries, a busy session after its
/// age budget.  Each message is processed in its
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
    // On the last allowed delivery there is no redelivery left to lean on, so a
    // busy session must be answered instead of Nak'd into the void.
    let (delivered, sequence, age) = match js_msg.info() {
        Ok(info) => (
            info.delivered,
            Some(info.stream_sequence),
            message_age(&info),
        ),
        Err(error) => {
            // Without delivery metadata neither backstop can count, so a Nak
            // could redeliver forever. Treat the budget as spent: a busy
            // session is answered and a handler error is terminated.
            tracing::warn!(
                %error,
                "jetstream message info unavailable; settling this delivery as the last"
            );
            (CONSUMER_MAX_DELIVER, None, Duration::ZERO)
        }
    };
    let step_timeout = step_timeout_of(&invoker, &core).await;
    let busy = busy_policy(delivered, age, busy_budget(step_timeout));
    let handle_timeout = handle_timeout_for(step_timeout);
    let handling = tokio::time::timeout(
        handle_timeout,
        crate::handle_message_outcome(&client, invoker, core.clone(), busy),
    );
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
    let result = match result {
        Ok(result) => result,
        Err(_elapsed) => {
            tracing::error!(
                timeout = ?handle_timeout,
                "agentic handler exceeded its time bound; terminating the request"
            );
            if let Err(error) = crate::publish_error_for_message(
                &client,
                &core,
                "timeout",
                format!("agent request handling exceeded {handle_timeout:?}"),
            )
            .await
            {
                tracing::warn!(%error, "could not publish timeout response");
            }
            // The step may have side effects; do not run it again.
            Ok(Disposition::Rejected)
        }
    };
    let (kind, error_attempt) = settle_delivery(error_attempts(), &result, sequence, delivered);
    if let Err(handle_err) = &result {
        tracing::error!(
            error = %handle_err,
            delivered,
            error_attempt,
            "agentic handler failed; settling for redelivery"
        );
        if matches!(kind, AckKind::Term) {
            // Last delivery: nothing will retry, so do not leave the caller
            // waiting for a response that never comes.
            publish_final_error(&client, &core, &handle_err.to_string()).await;
        }
    }
    if let Err(ack_err) = js_msg.ack_with(kind).await {
        tracing::error!(error = %ack_err, "jetstream ack failed");
    }
}

/// Best-effort error response on the last delivery, retried once.
async fn publish_final_error(client: &async_nats::Client, core: &async_nats::Message, why: &str) {
    for attempt in 0..2u8 {
        match crate::publish_error_for_message(
            client,
            core,
            "invoke_failed",
            format!("agent request could not be completed: {why}"),
        )
        .await
        {
            Ok(_) => return,
            Err(error) => {
                tracing::warn!(%error, attempt, "could not publish final error response");
                tokio::time::sleep(ERROR_REDELIVER_DELAY).await;
            }
        }
    }
}

/// The agent's configured step timeout for the request in `core`, if the
/// invoker knows it. An unparsable request yields `None` (default bound).
pub(crate) async fn step_timeout_of(
    invoker: &Arc<dyn AgentDispatchInvoker>,
    core: &async_nats::Message,
) -> Option<Duration> {
    let headers = core.headers.as_ref();
    let parsed = crate::parse_request(
        |name| {
            headers
                .and_then(|h| h.get(name))
                .map(|v| v.as_str().to_string())
        },
        &core.payload,
    )
    .ok()?;
    invoker
        .step_timeout(&parsed.tenant, &parsed.env, &parsed.request.target)
        .await
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
    fn consumer_config_keeps_a_finite_max_deliver_backstop() {
        let cfg = agentic_consumer_config();
        assert_eq!(cfg.max_deliver, CONSUMER_MAX_DELIVER);
        assert!(cfg.max_deliver > 0, "never unlimited: poison-pill backstop");
        assert!(cfg.max_deliver > MAX_DELIVER);
    }

    #[test]
    fn ack_wait_exceeds_progress_interval_and_step_timeout() {
        assert!(ACK_WAIT > PROGRESS_EVERY * 2);
        assert!(ACK_WAIT > Duration::from_secs(60), "default step timeout");
        assert_eq!(agentic_consumer_config().ack_wait, ACK_WAIT);
    }

    #[test]
    fn settlement_acks_naks_and_terms() {
        assert!(matches!(
            settlement(&Ok(Disposition::Handled), 1, 0),
            AckKind::Ack
        ));
        // Lock contention: redelivered with a growing delay, never acked.
        assert!(matches!(
            settlement(&Ok(Disposition::Busy), 1, 0),
            AckKind::Nak(Some(delay)) if delay == BUSY_REDELIVER_DELAY
        ));
        assert!(matches!(
            settlement(&Ok(Disposition::Busy), 2, 0),
            AckKind::Nak(Some(delay)) if delay == BUSY_REDELIVER_DELAY * 2
        ));
        assert!(matches!(
            settlement(&Ok(Disposition::Rejected), 1, 0),
            AckKind::Term
        ));
    }

    #[test]
    fn handler_errors_are_nakd_quickly_then_termed_on_the_last_delivery() {
        let err = || Err(anyhow::anyhow!("publish failed"));
        assert!(matches!(
            settlement(&err(), 1, 1),
            AckKind::Nak(Some(d)) if d == ERROR_REDELIVER_DELAY
        ));
        assert!(matches!(
            settlement(&err(), MAX_DELIVER, MAX_DELIVER),
            AckKind::Term
        ));
    }

    #[test]
    fn busy_deliveries_do_not_spend_the_handler_error_budget() {
        let err = || Err(anyhow::anyhow!("publish failed"));
        // 30 busy Naks later this is delivery 31 but only the first error.
        assert!(matches!(
            settlement(&err(), 31, 1),
            AckKind::Nak(Some(d)) if d == ERROR_REDELIVER_DELAY
        ));
        // Consumer backstop still terminates regardless of the error count.
        assert!(matches!(
            settlement(&err(), CONSUMER_MAX_DELIVER, 1),
            AckKind::Term
        ));
    }

    #[test]
    fn error_attempts_count_per_sequence_and_clear() {
        let attempts = ErrorAttempts::default();
        assert_eq!(attempts.record(7), 1);
        assert_eq!(attempts.record(7), 2);
        assert_eq!(attempts.record(8), 1);
        attempts.clear(7);
        assert_eq!(attempts.record(7), 1);
        // Clearing something never recorded is a no-op.
        attempts.clear(999);
        assert_eq!(attempts.len(), 2);
    }

    #[test]
    fn a_full_table_evicts_only_the_oldest_sequence() {
        let attempts = ErrorAttempts::default();
        // Sequence 1 is the oldest (3 attempts spent); 2 has spent 2.
        for _ in 0..3 {
            attempts.record(1);
        }
        for _ in 0..2 {
            attempts.record(2);
        }
        for sequence in 3..=(ERROR_ATTEMPTS_CAP as u64) {
            attempts.record(sequence);
        }
        assert_eq!(attempts.len(), ERROR_ATTEMPTS_CAP);
        // One more new sequence evicts exactly the oldest (1)...
        let newcomer = ERROR_ATTEMPTS_CAP as u64 + 1;
        assert_eq!(attempts.record(newcomer), 1);
        assert_eq!(attempts.len(), ERROR_ATTEMPTS_CAP);
        assert_eq!(attempts.count(1), None, "oldest entry evicted");
        // ...and every other poison pill keeps its count.
        assert_eq!(attempts.count(2), Some(2));
        assert_eq!(attempts.count(3), Some(1));
        assert_eq!(attempts.count(newcomer), Some(1));
        // An already-tracked sequence never evicts anything.
        assert_eq!(attempts.record(2), 3);
        assert_eq!(attempts.len(), ERROR_ATTEMPTS_CAP);
        assert_eq!(attempts.count(3), Some(1));
    }

    #[test]
    fn cleared_then_rerecorded_sequence_is_not_evicted_early() {
        let attempts = ErrorAttempts::default();
        attempts.record(1);
        attempts.record(2);
        attempts.clear(1);
        attempts.record(1); // now the newest: order = [2, 1]
        for sequence in 3..(ERROR_ATTEMPTS_CAP as u64 + 2) {
            attempts.record(sequence);
        }
        // The table overflowed by one: 2 (oldest) goes, 1 stays.
        assert_eq!(attempts.count(2), None);
        assert_eq!(attempts.count(1), Some(1));
    }

    #[test]
    fn settle_delivery_counts_errors_until_the_budget_then_terms_and_clears() {
        let attempts = ErrorAttempts::default();
        let failing = || -> Result<Disposition> { Err(anyhow::anyhow!("publish failed")) };
        for attempt in 1..MAX_DELIVER {
            let (kind, counted) = settle_delivery(&attempts, &failing(), Some(42), attempt);
            assert_eq!(counted, attempt);
            assert!(matches!(kind, AckKind::Nak(_)), "attempt {attempt}");
            assert_eq!(attempts.count(42), Some(attempt), "a Nak keeps the count");
        }
        let (kind, counted) = settle_delivery(&attempts, &failing(), Some(42), MAX_DELIVER);
        assert_eq!(counted, MAX_DELIVER);
        assert!(matches!(kind, AckKind::Term));
        assert_eq!(
            attempts.count(42),
            None,
            "a terminated message is forgotten"
        );
    }

    #[test]
    fn settle_delivery_clears_on_success_and_busy_does_not_spend_the_budget() {
        let attempts = ErrorAttempts::default();
        let failing = || -> Result<Disposition> { Err(anyhow::anyhow!("boom")) };
        settle_delivery(&attempts, &failing(), Some(5), 1);
        assert_eq!(attempts.count(5), Some(1));
        let (kind, counted) = settle_delivery(&attempts, &Ok(Disposition::Busy), Some(5), 2);
        assert_eq!(counted, 0);
        assert!(matches!(kind, AckKind::Nak(_)));
        assert_eq!(attempts.count(5), Some(1));
        let (kind, _) = settle_delivery(&attempts, &Ok(Disposition::Handled), Some(5), 3);
        assert!(matches!(kind, AckKind::Ack));
        assert_eq!(attempts.count(5), None);
        // No sequence on an error: last delivery, terminated.
        let (kind, counted) = settle_delivery(&attempts, &failing(), None, 1);
        assert_eq!(counted, MAX_DELIVER);
        assert!(matches!(kind, AckKind::Term));
    }

    #[test]
    fn busy_delay_schedule_is_exponential_and_capped() {
        let secs: Vec<u64> = (1..=8).map(|d| busy_redeliver_delay(d).as_secs()).collect();
        assert_eq!(secs, vec![5, 10, 20, 40, 60, 60, 60, 60]);
        assert_eq!(busy_redeliver_delay(0), BUSY_REDELIVER_DELAY);
        assert_eq!(busy_redeliver_delay(i64::MAX), BUSY_REDELIVER_MAX_DELAY);
    }

    #[test]
    fn busy_budget_tracks_the_handle_timeout_and_is_finite() {
        assert_eq!(busy_budget(None), HANDLE_TIMEOUT + BUSY_BUDGET_MARGIN);
        // A 20 minute step: the budget must outlast it, not stop at ~160s.
        let long = busy_budget(Some(Duration::from_secs(20 * 60)));
        assert!(long > Duration::from_secs(20 * 60), "got {long:?}");
        // Absolute max age: ceiling + margin, however large the step timeout.
        assert_eq!(
            busy_budget(Some(Duration::from_secs(u64::MAX / 4))),
            HANDLE_TIMEOUT_CEILING + BUSY_BUDGET_MARGIN
        );
    }

    #[test]
    fn busy_redelivery_outlasts_a_healthy_long_turn_without_a_poison_loop() {
        let budget = busy_budget(Some(Duration::from_secs(20 * 60)));
        // Walk the redelivery schedule: deliveries stop (answered) at the
        // budget, well before the consumer backstop, and well after the old
        // ~160s that 6 deliveries bought.
        let mut age = Duration::ZERO;
        let mut delivered = 1;
        while busy_policy(delivered, age, budget) == BusyPolicy::Redeliver {
            age += busy_redeliver_delay(delivered);
            delivered += 1;
        }
        assert!(age > Duration::from_secs(20 * 60), "stopped at {age:?}");
        assert!(age <= budget, "never waits past the budget: {age:?}");
        assert!(
            delivered < CONSUMER_MAX_DELIVER - 1,
            "delivered {delivered}"
        );
    }

    #[test]
    fn handle_timeout_follows_the_agent_step_timeout() {
        assert_eq!(handle_timeout_for(None), HANDLE_TIMEOUT);
        assert_eq!(
            handle_timeout_for(Some(Duration::from_secs(30))),
            HANDLE_TIMEOUT
        );
        assert_eq!(
            handle_timeout_for(Some(Duration::from_secs(600))),
            Duration::from_secs(635)
        );
        assert_eq!(
            handle_timeout_for(Some(Duration::from_secs(86_400))),
            HANDLE_TIMEOUT_CEILING
        );
    }

    #[test]
    fn busy_is_redelivered_inside_the_budget_then_answered() {
        let budget = busy_budget(None);
        assert_eq!(
            busy_policy(1, Duration::ZERO, budget),
            BusyPolicy::Redeliver
        );
        // Past the old 6th delivery, still young: keep redelivering.
        assert_eq!(
            busy_policy(MAX_DELIVER + 1, Duration::from_secs(170), budget),
            BusyPolicy::Redeliver
        );
        // Out of budget: answer session_busy.
        assert_eq!(busy_policy(3, budget, budget), BusyPolicy::Respond);
        // Consumer backstop reached: answer regardless of age.
        assert_eq!(
            busy_policy(CONSUMER_MAX_DELIVER - 1, Duration::ZERO, budget),
            BusyPolicy::Respond
        );
    }

    #[test]
    fn stale_ack_wait_is_detected() {
        assert!(ack_wait_is_stale(Duration::from_secs(30)));
        assert!(!ack_wait_is_stale(ACK_WAIT));
    }

    #[test]
    fn handle_timeout_fits_inside_ack_wait_and_covers_a_default_step() {
        assert!(HANDLE_TIMEOUT < ACK_WAIT);
        assert!(HANDLE_TIMEOUT > Duration::from_secs(60 + 5));
        assert!(PROGRESS_EVERY <= Duration::from_secs(10));
    }

    #[test]
    fn backoff_grows_and_is_capped() {
        assert_eq!(backoff_delay(0), Duration::from_millis(500));
        assert_eq!(backoff_delay(1), Duration::from_secs(1));
        assert_eq!(backoff_delay(100), BACKOFF_MAX);
    }
}
