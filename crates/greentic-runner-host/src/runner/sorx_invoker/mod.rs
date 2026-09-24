//! Runner-host implementations of `greentic_aw_runtime::SorxInvoker`.
//!
//! Split across three modules:
//! - [`http`] — the HTTP transport pieces: SoRX capability discovery
//!   (`discover`), capability invocation (`invoke_sor`), and
//!   [`http::SorxHttpInvoker`], the single-SoR `GREENTIC_AW_SORX_URL`
//!   development override built on top of them.
//! - [`routed`] — [`routed::SorxRoutedInvoker`], which resolves each bound
//!   SoR's own route document (`sorla_route::resolve_route`) and dispatches
//!   through the same [`http::discover`]/[`http::invoke_sor`] pair.
//! - [`tests`] — shared test coverage for both.
//!
//! The aw-runtime half (`SorlaToolSource`/`SorlaToolCatalog`) depends only on
//! the `SorxInvoker` trait + JSON, so this is where `reqwest` enters.

#![cfg(feature = "agentic-worker")]

mod http;
// `pub(crate)`, not re-exported: nothing in this crate builds a
// `SorxRoutedInvoker` yet (that lands with its agent-node wiring), so a
// `use routed::SorxRoutedInvoker;` re-export here would be an unused import
// until that caller exists. Reach it as `sorx_invoker::routed::SorxRoutedInvoker`.
pub(crate) mod routed;
#[cfg(test)]
mod tests;

use std::sync::OnceLock;
use std::time::Duration;

pub(crate) use http::SorxHttpInvoker;

/// Caller identity stamped on every capability invocation. SP1 has no
/// per-agent caller identity to thread through `SorxInvoker::invoke` (the
/// trait carries only `(pack, action, args_json)`), so a fixed identity is
/// used; the deployed worker path can carry a real caller/tenant once the
/// trait grows a context parameter.
const SORLA_CALLER_ID: &str = "dw-agent";

/// One process-wide HTTP client for every SoRX request this module makes: a
/// 30s timeout (a hung SoR must not hang an agent step forever) and
/// redirects disabled (a SoR that redirects is a misconfiguration to surface,
/// never something to silently follow). Falls back to `reqwest::Client::new`
/// (logged) on the unlikely event the configured builder fails.
pub(crate) fn shared_client() -> reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT
        .get_or_init(|| {
            reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap_or_else(|err| {
                    tracing::warn!(
                        error = %err,
                        "sorla: failed to build a configured HTTP client; falling back to reqwest defaults"
                    );
                    reqwest::Client::new()
                })
        })
        .clone()
}
