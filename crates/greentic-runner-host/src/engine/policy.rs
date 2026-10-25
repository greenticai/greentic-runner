use super::error::{GResult, RunnerError};
use rand::{RngExt, rng};
use std::time::Duration;
use tokio::time::sleep;

#[derive(Clone, Debug)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(5),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Policy {
    pub retry: RetryPolicy,
    pub max_egress_adapters: usize,
    pub max_payload_bytes: usize,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            retry: RetryPolicy::default(),
            max_egress_adapters: 32,
            max_payload_bytes: 512 * 1024,
        }
    }
}

pub async fn retry_with_jitter<F, Fut, T>(policy: &RetryPolicy, mut op: F) -> GResult<T>
where
    F: FnMut() -> Fut + Send,
    Fut: std::future::Future<Output = GResult<T>> + Send,
    T: Send,
{
    let mut attempt = 0u32;
    loop {
        match op().await {
            Ok(value) => return Ok(value),
            Err(err) => {
                // The effects already happened (or the failure is deterministic);
                // running `op` again repeats them / fails the same way.
                if err.is_terminal() {
                    return Err(err);
                }
                attempt += 1;
                if attempt >= policy.max_attempts {
                    return Err(err);
                }
                let backoff = backoff_with_jitter(policy, attempt);
                sleep(backoff).await;
            }
        }
    }
}

fn backoff_with_jitter(policy: &RetryPolicy, attempt: u32) -> Duration {
    let capped_attempt = attempt.min(10);
    let initial_ms = policy.initial_backoff.as_millis().max(1) as u64;
    let multiplier = 1u64 << capped_attempt;
    let mut base_ms = initial_ms.saturating_mul(multiplier);
    let max_ms = policy.max_backoff.as_millis().max(1) as u64;
    if base_ms > max_ms {
        base_ms = max_ms;
    }
    let base = Duration::from_millis(base_ms);
    let mut rng = rng();
    let jitter_cap = base.as_millis().max(1) as u64;
    let jitter_ms = rng.random_range(0..=jitter_cap);
    base + Duration::from_millis(jitter_ms)
}

pub fn policy_violation(reason: impl Into<String>) -> RunnerError {
    RunnerError::Policy {
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    };

    #[test]
    fn default_policy_sets_budget_and_retry_limits() {
        let policy = Policy::default();

        assert_eq!(policy.retry.max_attempts, 5);
        assert_eq!(policy.retry.initial_backoff, Duration::from_millis(100));
        assert_eq!(policy.retry.max_backoff, Duration::from_secs(5));
        assert_eq!(policy.max_egress_adapters, 32);
        assert_eq!(policy.max_payload_bytes, 512 * 1024);
    }

    #[test]
    fn policy_violation_preserves_reason() {
        let err = policy_violation("too many adapters");

        match err {
            RunnerError::Policy { reason } => assert_eq!(reason, "too many adapters"),
            other => panic!("expected policy error, got {other:?}"),
        }
    }

    #[test]
    fn backoff_uses_minimum_nonzero_bounds() {
        let policy = RetryPolicy {
            max_attempts: 3,
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
        };

        let backoff = backoff_with_jitter(&policy, 1);

        assert!(backoff >= Duration::from_millis(1));
        assert!(backoff <= Duration::from_millis(2));
    }

    #[test]
    fn backoff_caps_large_attempts_at_max_backoff_plus_jitter() {
        let policy = RetryPolicy {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_millis(250),
        };

        let backoff = backoff_with_jitter(&policy, 99);

        assert!(backoff >= Duration::from_millis(250));
        assert!(backoff <= Duration::from_millis(500));
    }

    #[tokio::test]
    async fn retry_with_jitter_returns_immediate_success() {
        let attempts = Arc::new(AtomicU32::new(0));
        let seen = Arc::clone(&attempts);
        let policy = RetryPolicy {
            max_attempts: 3,
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
        };

        let value = retry_with_jitter(&policy, move || {
            let seen = Arc::clone(&seen);
            async move {
                seen.fetch_add(1, Ordering::SeqCst);
                Ok::<_, RunnerError>("ok")
            }
        })
        .await
        .expect("retry should succeed");

        assert_eq!(value, "ok");
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn retry_with_jitter_retries_until_success() {
        let attempts = Arc::new(AtomicU32::new(0));
        let seen = Arc::clone(&attempts);
        let policy = RetryPolicy {
            max_attempts: 3,
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
        };

        let value = retry_with_jitter(&policy, move || {
            let seen = Arc::clone(&seen);
            async move {
                let attempt = seen.fetch_add(1, Ordering::SeqCst) + 1;
                if attempt < 2 {
                    Err(policy_violation("transient"))
                } else {
                    Ok("ok")
                }
            }
        })
        .await
        .expect("retry should eventually succeed");

        assert_eq!(value, "ok");
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn retry_with_jitter_returns_last_error_after_limit() {
        let attempts = Arc::new(AtomicU32::new(0));
        let seen = Arc::clone(&attempts);
        let policy = RetryPolicy {
            max_attempts: 2,
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
        };

        let err = retry_with_jitter::<_, _, ()>(&policy, move || {
            let seen = Arc::clone(&seen);
            async move {
                let attempt = seen.fetch_add(1, Ordering::SeqCst) + 1;
                Err(policy_violation(format!("attempt {attempt}")))
            }
        })
        .await
        .expect_err("retry should fail");

        match err {
            RunnerError::Policy { reason } => assert_eq!(reason, "attempt 2"),
            other => panic!("expected policy error, got {other:?}"),
        }
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn post_effect_persistence_failures_are_never_retried() {
        let attempts = Arc::new(AtomicU32::new(0));
        let seen = Arc::clone(&attempts);
        let policy = RetryPolicy {
            max_attempts: 5,
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
        };

        let result = retry_with_jitter::<_, _, ()>(&policy, move || {
            let seen = Arc::clone(&seen);
            async move {
                seen.fetch_add(1, Ordering::SeqCst);
                Err(RunnerError::PostEffectPersistence {
                    reason: "store down".into(),
                })
            }
        })
        .await;

        assert!(matches!(
            result,
            Err(RunnerError::PostEffectPersistence { .. })
        ));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }
}
