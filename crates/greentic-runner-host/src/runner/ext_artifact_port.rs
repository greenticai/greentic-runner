//! The extension-runtime `ArtifactPort` backed by the admin `artifacts` door
//! (contract C3 `put`): the CREATE path of attachments.
//!
//! An extension that creates a file (`greentic.media`) calls `host.artifact.put`;
//! `greentic-ext-runtime` validates size, name and media type, refuses a call
//! with no tenant (`tenant-required`), then calls this port. The port POSTs
//! `put` and returns the `artifact://` id. The TENANT is decided by the door
//! from the token, never from the body; the tenant check below only refuses a
//! call that reaches the port without one, so a misconfigured embedder cannot
//! write anonymously.
//!
//! Where the port comes from is decided at the HOST, never below it (same
//! rules as the attachment reader, `crate::runner::artifact_reader_wiring`):
//! a deployed unit gets ONLY the port passed with
//! `RevisionHostOptions::with_ext_artifact_port`; a single-tenant
//! `HostBuilder` host gets its injected port, else, only when it opted in with
//! `HostBuilder::with_artifact_env_fallback(true)`, the env fallback
//! ([`artifact_port_from_env`]); a multi-tenant host gets none.
//! `build_ext_runtime` installs exactly the port it is handed.
//!
//! `ArtifactPort::put` is synchronous (host functions are linked on the sync
//! wasmtime linker) and the door is async: the bridge is `block_in_place` +
//! `Handle::block_on`. `block_in_place` panics on a current-thread runtime, so
//! that flavour is refused with a typed error instead.
//!
//! The token is a credential: redacted from `Debug`, never in a log line, an
//! error or anything the guest sees; redirects are refused so it never leaves
//! the door host.

use std::sync::Arc;
use std::time::Duration;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use greentic_aw_runtime::ArtifactClientError;
use greentic_ext_runtime::host_ports::{
    ArtifactPort, ArtifactPortError, ArtifactPutRequest, HostCallContext,
};

use crate::host::ExtArtifactPort;

/// Connect budget of one put.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Total budget of one put (the body is at most a 10 MiB file, base64-encoded).
const TOTAL_TIMEOUT: Duration = Duration::from_secs(30);

pub struct HttpArtifactPort {
    client: reqwest::Client,
    endpoint: String,
    token: String,
    handle: tokio::runtime::Handle,
}

impl std::fmt::Debug for HttpArtifactPort {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpArtifactPort")
            .field("endpoint", &self.endpoint)
            .field("token", &"<redacted>")
            .finish()
    }
}

impl HttpArtifactPort {
    /// `endpoint` is the door base (`.../api/v1/ingest/artifacts`); `put` posts
    /// to `<endpoint>/put`. `handle` is the multi-thread runtime the blocking
    /// call parks on.
    ///
    /// Fallible, mirroring `HttpArtifactReader::new`: an empty token, a token
    /// with control characters, or a client that cannot be built is an error
    /// (never a default client, which has no redirect policy and no timeouts).
    /// The error names neither the endpoint nor the token.
    pub fn new(
        endpoint: String,
        token: String,
        handle: tokio::runtime::Handle,
    ) -> Result<Self, ArtifactClientError> {
        if token.trim().is_empty() {
            return Err(ArtifactClientError::EmptyToken);
        }
        if token.chars().any(char::is_control) {
            return Err(ArtifactClientError::InvalidToken);
        }
        // A redirect would resend the bearer token to wherever it points.
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(TOTAL_TIMEOUT)
            .build()
            .map_err(|_| ArtifactClientError::Client)?;
        Ok(Self {
            client,
            endpoint,
            token,
            handle,
        })
    }
}

#[derive(serde::Deserialize)]
struct PutBody {
    id: String,
}

/// Unavailable with a fixed, credential-free reason.
fn unavailable(reason: &str) -> ArtifactPortError {
    ArtifactPortError::Unavailable(reason.to_string())
}

/// Whether `block_in_place` + `handle.block_on` is safe from the CALLING
/// context. `block_in_place` panics when the caller runs on a current-thread
/// runtime, whatever the stored handle is, so the caller's runtime is checked
/// first; a caller outside any runtime (a plain or blocking thread) is fine.
/// The stored handle must also be multi-thread: a current-thread runtime is
/// not driven from another thread.
fn can_block_here(stored: &tokio::runtime::Handle) -> bool {
    use tokio::runtime::RuntimeFlavor::CurrentThread;
    let caller_ok = tokio::runtime::Handle::try_current()
        .map(|current| current.runtime_flavor() != CurrentThread)
        .unwrap_or(true);
    caller_ok && stored.runtime_flavor() != CurrentThread
}

impl ArtifactPort for HttpArtifactPort {
    fn put(
        &self,
        extension_id: &str,
        ctx: &HostCallContext,
        request: ArtifactPutRequest,
    ) -> Result<String, ArtifactPortError> {
        if ctx
            .tenant
            .as_deref()
            .map(str::trim)
            .is_none_or(str::is_empty)
        {
            return Err(unavailable("call carries no tenant"));
        }
        if !can_block_here(&self.handle) {
            return Err(unavailable("artifact put needs a multi-thread runtime"));
        }
        // The same endpoint rule as the reader: https, or loopback http only,
        // never userinfo. A refused endpoint never receives the token.
        let Some(url) = greentic_aw_runtime::door_url(&self.endpoint, "put") else {
            return Err(unavailable("artifact endpoint is not usable"));
        };
        let body = serde_json::json!({
            "name": request.name,
            "mime_type": request.mime_type,
            "data_base64": STANDARD.encode(&request.bytes),
            "derived_from": null,
            // An extension call belongs to no conversation; the door then
            // applies only the per-tenant byte quota.
            "conversation_id": null,
        });
        let send = self
            .client
            .post(url)
            .bearer_auth(&self.token)
            .json(&body)
            .send();
        // Errors carry fixed reasons only: a reqwest error can embed the URL.
        let response = tokio::task::block_in_place(|| self.handle.block_on(send))
            .map_err(|_| unavailable("artifact door unreachable"))?;
        match response.status().as_u16() {
            200 => {}
            415 => return Err(ArtifactPortError::InvalidMediaType),
            422 => return Err(ArtifactPortError::QuotaExceeded),
            // 401/403 (token or purpose), 413, 429, 503, a redirect and
            // anything else: not stored. The body is never read or returned.
            other => {
                tracing::warn!(
                    extension = %extension_id,
                    status = other,
                    code = "artifact_put_refused",
                    "artifact door refused a put"
                );
                return Err(ArtifactPortError::Unavailable(format!(
                    "door answered {other}"
                )));
            }
        }
        let parsed =
            tokio::task::block_in_place(|| self.handle.block_on(response.json::<PutBody>()))
                .map_err(|_| unavailable("artifact door answered an unreadable body"))?;
        Ok(parsed.id)
    }
}

/// Env fallback, consulted ONLY by `crate::host::host_ext_artifact_port` for a
/// single-tenant `HostBuilder` host with no injected port that opted in with
/// `HostBuilder::with_artifact_env_fallback(true)` (the standalone runner). Needs both `GREENTIC_ARTIFACT_ENDPOINT` and
/// `GREENTIC_ARTIFACT_TOKEN`, and a running multi-thread tokio runtime to block
/// on; anything missing means no port, never a panic.
pub fn artifact_port_from_env() -> Option<ExtArtifactPort> {
    let endpoint = std::env::var("GREENTIC_ARTIFACT_ENDPOINT")
        .ok()
        .filter(|v| !v.trim().is_empty())?;
    let token = std::env::var("GREENTIC_ARTIFACT_TOKEN")
        .ok()
        .filter(|v| !v.trim().is_empty())?;
    let handle = tokio::runtime::Handle::try_current().ok()?;
    match HttpArtifactPort::new(endpoint, token, handle) {
        Ok(port) => Some(Arc::new(port)),
        Err(error) => {
            // `ArtifactClientError` never carries the endpoint or the token.
            tracing::warn!(
                code = "artifact_port_unavailable",
                %error,
                "artifact port from the environment could not be built; \
                 host.artifact.put answers unsupported"
            );
            None
        }
    }
}

#[cfg(test)]
#[path = "ext_artifact_port_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "ext_artifact_port_dispatch_tests.rs"]
mod dispatch_tests;
