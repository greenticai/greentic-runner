//! NATS subscriber that eagerly pre-caches a local-wasm MCP component when the
//! admin publishes a warm event on `greentic.mcp.warm.v1`.
//!
//! ## Purpose
//!
//! This is a **pure optimization**: the Slice-2 lazy pull in `mcp_local` already
//! downloads and caches components on first use.  This subscriber fires earlier —
//! as soon as the admin pins a new component — so that the cache is warm before
//! the next agentic step probes it.
//!
//! ## Failure semantics
//!
//! Both JSON parse errors and `ensure_cached` failures are logged with
//! `tracing::warn!` and silently dropped.  The listener **never** crashes or
//! returns an error from the inner loop; only a NATS subscription failure (before
//! the loop starts) propagates up to the caller.

use anyhow::Result;
use futures::StreamExt as _;

/// Payload published by the admin on `greentic.mcp.warm.v1`.
///
/// Field names must match the admin's JSON exactly.
#[derive(Debug, serde::Deserialize)]
struct WarmEvent {
    tenant: String,
    component_ref: String,
    version: String,
    digest: String,
}

/// Components this process has warmed or is warming, so N listeners in one
/// process pull a given component once.
static WARMED: std::sync::LazyLock<parking_lot::Mutex<std::collections::HashSet<String>>> =
    std::sync::LazyLock::new(|| parking_lot::Mutex::new(std::collections::HashSet::new()));

fn warm_key(event: &WarmEvent) -> String {
    format!(
        "{}\u{1f}{}\u{1f}{}",
        event.component_ref, event.version, event.digest
    )
}

/// `true` for the first caller to claim `key`; later callers skip the pull.
fn claim_warm(key: &str) -> bool {
    WARMED.lock().insert(key.to_string())
}

fn release_warm(key: &str) {
    WARMED.lock().remove(key);
}

/// Run `pull` under the in-flight claim for `key`, then ALWAYS release it.
///
/// The claim only dedupes listeners racing on the same event while a pull is
/// running. It must not outlive the pull: the claim set is process-lifetime, so
/// a key kept after success would make a repeated warm event (after the cache
/// entry was evicted) be skipped forever. A repeat for an already cached
/// component is a cheap no-op in `ensure_cached`. `None` = another listener
/// holds the claim and the pull was skipped.
async fn warm_once<F, Fut, T>(key: &str, pull: F) -> Option<T>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    if !claim_warm(key) {
        return None;
    }
    let result = pull().await;
    release_warm(key);
    Some(result)
}

/// Subscribe to `greentic.mcp.warm.v1` and eagerly pull each pinned component
/// into the local-wasm cache via
/// [`greentic_aw_runtime::mcp_store_pull::ensure_cached`].
///
/// # Errors
///
/// Returns an error only if subscribing to the NATS subject fails (i.e. NATS
/// connection is unavailable at startup).  All per-message errors are logged and
/// dropped so the subscription never terminates due to a single bad message or a
/// transient store outage.
pub async fn run_mcp_warm_listener(client: async_nats::Client) -> Result<()> {
    // No queue group: the warm cache is per PROCESS, so every replica must see
    // the event (a global queue group would warm only one of N). Listeners in
    // the same process (one per tenant runtime, or reloads) dedupe through
    // `claim_warm` instead.
    let mut subscription = client.subscribe("greentic.mcp.warm.v1").await?;

    while let Some(msg) = subscription.next().await {
        let event = match serde_json::from_slice::<WarmEvent>(&msg.payload) {
            Ok(event) => event,
            Err(parse_error) => {
                tracing::warn!(
                    %parse_error,
                    "mcp_warm_listener: ignoring malformed warm event"
                );
                continue;
            }
        };

        let warm_key = warm_key(&event);
        let Some(result) = warm_once(&warm_key, || {
            greentic_aw_runtime::mcp_store_pull::ensure_cached(
                &event.component_ref,
                &event.version,
                &event.digest,
            )
        })
        .await
        else {
            continue;
        };
        match result {
            Ok(()) => {
                tracing::info!(
                    tenant = %event.tenant,
                    component_ref = %event.component_ref,
                    version = %event.version,
                    "mcp_warm_listener: component pre-cached successfully"
                );
            }
            Err(pull_error) => {
                // A failed pull may be retried by a later event (the claim was
                // already released by `warm_once`).
                tracing::warn!(
                    %pull_error,
                    tenant = %event.tenant,
                    component_ref = %event.component_ref,
                    version = %event.version,
                    "mcp_warm_listener: ensure_cached failed; lazy pull remains the fallback"
                );
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{WarmEvent, claim_warm, release_warm, warm_once};

    #[tokio::test]
    async fn a_repeated_event_after_success_pulls_again_but_inflight_dedups() {
        let key = "test-ref\u{1f}1.0.0\u{1f}digest-repeat";
        // In flight: a second listener on the same event is skipped.
        let inner = warm_once(key, || async {
            let skipped = warm_once(key, || async { "second" }).await;
            assert_eq!(skipped, None, "in-flight pull is deduped");
            "first"
        })
        .await;
        assert_eq!(inner, Some("first"));
        // After success the claim is gone: a later event (cache evicted) pulls.
        assert_eq!(warm_once(key, || async { "again" }).await, Some("again"));
    }

    #[test]
    fn a_component_is_claimed_once_per_process_until_released() {
        let key = "test-ref\u{1f}1.0.0\u{1f}digest-claim";
        assert!(claim_warm(key));
        assert!(!claim_warm(key), "second listener skips the pull");
        release_warm(key);
        assert!(claim_warm(key), "a failed pull can be retried");
        release_warm(key);
    }

    #[test]
    fn warm_event_parses() {
        let raw = br#"{"tenant":"acme","component_ref":"greentic.mcp.weather","version":"1.0.0","digest":"abc"}"#;
        let event: WarmEvent = serde_json::from_slice(raw).expect("parse should succeed");
        assert_eq!(event.tenant, "acme");
        assert_eq!(event.component_ref, "greentic.mcp.weather");
        assert_eq!(event.version, "1.0.0");
        assert_eq!(event.digest, "abc");
    }
}
