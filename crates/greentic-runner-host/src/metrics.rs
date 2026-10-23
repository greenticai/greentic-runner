//! Lightweight wrappers around the global OpenTelemetry meter so engine code
//! can record metrics without each call site having to know about
//! [`opentelemetry`] types.
//!
//! When no meter provider is installed (e.g. unit tests, or
//! `gtc start` without a `telemetry:` block) the no-op global meter
//! short-circuits each call, so these helpers are safe to invoke
//! unconditionally.

use std::collections::HashSet;
use std::sync::OnceLock;

use parking_lot::Mutex;

use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram, Meter};

static METER: OnceLock<Meter> = OnceLock::new();
static FLOW_EXEC_COUNT: OnceLock<Counter<u64>> = OnceLock::new();
static FLOW_EXEC_DURATION: OnceLock<Histogram<f64>> = OnceLock::new();
static PROVIDER_INVOKE_COUNT: OnceLock<Counter<u64>> = OnceLock::new();
static PROVIDER_OP_DURATION: OnceLock<Histogram<f64>> = OnceLock::new();

/// Explicit bucket boundaries (milliseconds) for every duration histogram; the
/// SDK default boundaries are tuned for seconds-scale values and would pile
/// almost every sample into the first bucket.
const DURATION_BOUNDARIES_MS: [f64; 14] = [
    1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1_000.0, 2_500.0, 5_000.0, 10_000.0,
    30_000.0,
];

/// Longest label value emitted for a guest- or config-derived identifier.
const LABEL_MAX_CHARS: usize = 64;
/// Distinct provider / operation label values tracked before everything new is
/// folded into `"other"`, bounding metric cardinality.
const LABEL_MAX_DISTINCT: usize = 256;

static SEEN_PROVIDERS: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
static SEEN_OPS: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();

/// Normalise an unbounded identifier into a safe, bounded metric label:
/// restricted charset, capped length, `"unknown"` when empty, and `"other"`
/// once `LABEL_MAX_DISTINCT` distinct values have been seen.
fn bounded_label(seen: &OnceLock<Mutex<HashSet<String>>>, raw: &str) -> String {
    let cleaned: String = raw
        .trim()
        .chars()
        .take(LABEL_MAX_CHARS)
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | ':') {
                c
            } else {
                '_'
            }
        })
        .collect();
    if cleaned.is_empty() {
        return "unknown".to_string();
    }
    let mut guard = seen.get_or_init(|| Mutex::new(HashSet::new())).lock();
    if guard.contains(&cleaned) {
        return cleaned;
    }
    if guard.len() >= LABEL_MAX_DISTINCT {
        return "other".to_string();
    }
    guard.insert(cleaned.clone());
    cleaned
}

fn meter() -> &'static Meter {
    METER.get_or_init(|| global::meter("greentic-runner-host"))
}

fn flow_count() -> &'static Counter<u64> {
    FLOW_EXEC_COUNT.get_or_init(|| {
        meter()
            .u64_counter("greentic.flow.executions")
            .with_description("Total greentic flow executions")
            .build()
    })
}

fn flow_duration() -> &'static Histogram<f64> {
    FLOW_EXEC_DURATION.get_or_init(|| {
        meter()
            .f64_histogram("greentic.flow.duration_ms")
            .with_description("Duration of greentic flow executions")
            .with_unit("ms")
            .with_boundaries(DURATION_BOUNDARIES_MS.to_vec())
            .build()
    })
}

fn provider_count() -> &'static Counter<u64> {
    PROVIDER_INVOKE_COUNT.get_or_init(|| {
        meter()
            .u64_counter("greentic.provider.invocations")
            .with_description("Total greentic provider invocations")
            .build()
    })
}

fn provider_op_duration() -> &'static Histogram<f64> {
    PROVIDER_OP_DURATION.get_or_init(|| {
        meter()
            .f64_histogram("greentic.provider.op_duration_ms")
            .with_description("Duration of greentic provider operations")
            .with_unit("ms")
            .with_boundaries(DURATION_BOUNDARIES_MS.to_vec())
            .build()
    })
}

/// Record one flow execution: increments the executions counter and emits a
/// duration sample. `status` is `"ok"` or `"err"`.
pub fn record_flow_execution(tenant: &str, flow_id: &str, status: &str, duration_ms: f64) {
    let attrs = [
        KeyValue::new("tenant", tenant.to_string()),
        KeyValue::new("flow_id", flow_id.to_string()),
        KeyValue::new("status", status.to_string()),
    ];
    flow_count().add(1, &attrs);
    flow_duration().record(duration_ms, &attrs);
}

/// Record one provider invocation along with its duration. `status` is
/// `"ok"` or `"err"`. `provider` and `op` are bounded (see [`bounded_label`])
/// so a misbehaving pack cannot create unbounded metric series; callers should
/// pass the provider *type* rather than a per-instance id where they have one.
pub fn record_provider_invocation(
    tenant: &str,
    provider: &str,
    op: &str,
    status: &str,
    duration_ms: f64,
) {
    let attrs = [
        KeyValue::new("tenant", tenant.to_string()),
        KeyValue::new("provider", bounded_label(&SEEN_PROVIDERS, provider)),
        KeyValue::new("op", bounded_label(&SEEN_OPS, op)),
        KeyValue::new("status", status.to_string()),
    ];
    provider_count().add(1, &attrs);
    provider_op_duration().record(duration_ms, &attrs);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_label_sanitises_and_caps_length() {
        let seen = OnceLock::new();
        assert_eq!(bounded_label(&seen, "  "), "unknown");
        assert_eq!(bounded_label(&seen, "a b/c"), "a_b_c");
        assert_eq!(
            bounded_label(&seen, &"x".repeat(500)).len(),
            LABEL_MAX_CHARS
        );
    }

    #[test]
    fn bounded_label_folds_overflow_into_other() {
        let seen = OnceLock::new();
        for i in 0..LABEL_MAX_DISTINCT {
            assert_eq!(bounded_label(&seen, &format!("p{i}")), format!("p{i}"));
        }
        assert_eq!(bounded_label(&seen, "brand-new"), "other");
        // Already-seen values keep their own series.
        assert_eq!(bounded_label(&seen, "p0"), "p0");
    }

    #[test]
    fn recording_is_safe_without_a_meter_provider() {
        record_flow_execution("t", "f", "ok", 1.0);
        record_provider_invocation("t", "p", "op", "ok", 1.0);
    }
}
