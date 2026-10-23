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
    // Queue group: the warm cache is process-global, so one delivery per event
    // is enough however many tenant runtimes (or reloads of one) spawn a listener.
    let mut subscription = client
        .queue_subscribe("greentic.mcp.warm.v1", "runner-mcp-warm".to_string())
        .await?;

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

        match greentic_aw_runtime::mcp_store_pull::ensure_cached(
            &event.component_ref,
            &event.version,
            &event.digest,
        )
        .await
        {
            Ok(()) => {
                tracing::info!(
                    tenant = %event.tenant,
                    component_ref = %event.component_ref,
                    version = %event.version,
                    "mcp_warm_listener: component pre-cached successfully"
                );
            }
            Err(pull_error) => {
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
    use super::WarmEvent;

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
