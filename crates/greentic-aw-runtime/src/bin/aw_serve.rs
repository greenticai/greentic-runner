//! `aw-serve` — run the agentic-worker runtime as a NATS-consuming service.
//!
//! This is the credit-free, dependency-light serve entry: it builds a
//! `test-mock` [`AgentRuntime`] (canned LLM reply, in-memory state, no tools)
//! and serves `greentic.agentic.request.v1` so a live cross-process e2e
//! (runner `agentic.call` -> aw-serve over NATS) can run without LLM credits or
//! Redis. The production serve path (real LLM/Redis/extensions) lives in the
//! runner host, which reuses its `build_agent_runtime` construction — see
//! `greentic-runner-host` `runner::agent_node`.
//!
//! Usage:
//! ```text
//! GREENTIC_EVENTS_NATS_URL=nats://127.0.0.1:4222 \
//!   cargo run -p greentic-aw-runtime --features serve,test-mock --bin aw-serve
//! ```
//! Optional env:
//! * `AW_SERVE_AGENT_ID` — agent id to register (default `greeter`).
//! * `AW_SERVE_REPLY`    — canned reply text (default `Hello from aw-serve`).

use std::process::ExitCode;

use greentic_aw_runtime::serve::{build_test_mock_runtime, serve};

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber_init();

    let nats_url = match std::env::var("GREENTIC_EVENTS_NATS_URL") {
        Ok(url) if !url.trim().is_empty() => url,
        _ => {
            eprintln!(
                "aw-serve: GREENTIC_EVENTS_NATS_URL is unset; nothing to serve. \
                 Set it to a NATS url (e.g. nats://127.0.0.1:4222)."
            );
            return ExitCode::FAILURE;
        }
    };

    let agent_id = std::env::var("AW_SERVE_AGENT_ID").unwrap_or_else(|_| "greeter".to_string());
    let reply =
        std::env::var("AW_SERVE_REPLY").unwrap_or_else(|_| "Hello from aw-serve".to_string());

    let nats_url_display = greentic_aw_runtime::url_redact::redact_url(&nats_url);
    eprintln!(
        "aw-serve: starting test-mock agentic runtime (agent_id='{agent_id}', \
         reply='{reply}') on {nats_url_display}"
    );

    let runtime = build_test_mock_runtime(&agent_id, &reply);
    match serve(&nats_url, runtime).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("aw-serve: bridge stopped with error: {error:#}");
            ExitCode::FAILURE
        }
    }
}

/// Minimal tracing init that is silent if no subscriber crate is available.
fn tracing_subscriber_init() {
    // The runtime depends on `tracing` (facade only). We avoid pulling a
    // subscriber dependency into the library; stderr `eprintln!` carries the
    // operator-facing lifecycle messages. This is intentionally a no-op so the
    // bin stays dependency-light.
}
