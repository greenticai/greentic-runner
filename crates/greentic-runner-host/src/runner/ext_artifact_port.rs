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
// The door helper speaks greentic-aw-runtime's `reqwest`, so the port's
// client is built from that one too.
use greentic_aw_runtime::door_reqwest as reqwest;
use greentic_aw_runtime::{ArtifactClientError, DoorRetry};
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
    /// At most two puts in flight per port, and a busy or failing door retried
    /// (`greentic_aw_runtime::artifact_door`, the reader's own rules).
    door: DoorRetry,
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
        // The token must reach the door only: never via an env-named proxy.
        let client = reqwest::Client::builder()
            .no_proxy()
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
            door: DoorRetry::new(),
        })
    }
}

/// Largest put reply read: the reply is a small JSON object.
const MAX_REPLY_BYTES: usize = 16 * 1024;

/// Read the body while it arrives and stop past the cap (`None`).
async fn read_capped(response: reqwest::Response) -> Option<Vec<u8>> {
    use futures::StreamExt;
    if response
        .content_length()
        .is_some_and(|n| n > MAX_REPLY_BYTES as u64)
    {
        return None;
    }
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.ok()?;
        if body.len() + chunk.len() > MAX_REPLY_BYTES {
            return None;
        }
        body.extend_from_slice(&chunk);
    }
    Some(body)
}

#[derive(serde::Deserialize)]
struct PutBody {
    id: String,
}

/// What one put brought back: a refusal status, or a 200's capped body
/// (`None` when unreadable or over the cap).
enum PutOutcome {
    Refused(u16),
    Body(Option<Vec<u8>>),
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
        // One blocking section for the request AND the reply, so this port's
        // door slot is held until the reply is read (the transfer lasts that
        // long on the door). A busy door (408/429/502/503/504) or a transport
        // failure is retried before anything is reported.
        let exchange = async {
            let reply = self
                .door
                .send(|| {
                    self.client
                        .post(url.clone())
                        .bearer_auth(&self.token)
                        .json(&body)
                })
                .await?;
            Ok::<_, greentic_aw_runtime::DoorSendError>(match reply.response.status().as_u16() {
                200 => PutOutcome::Body(read_capped(reply.response).await),
                other => PutOutcome::Refused(other),
            })
        };
        // Errors carry fixed reasons only: a reqwest error can embed the URL.
        let outcome = tokio::task::block_in_place(|| self.handle.block_on(exchange))
            .map_err(|_| unavailable("artifact door unreachable"))?;
        let raw = match outcome {
            PutOutcome::Body(raw) => raw,
            PutOutcome::Refused(415) => return Err(ArtifactPortError::InvalidMediaType),
            PutOutcome::Refused(422) => return Err(ArtifactPortError::QuotaExceeded),
            // 401/403 (token or purpose), 413, a 429/503 still refused after
            // every retry, a redirect and anything else: not stored. The body
            // is never read or returned.
            PutOutcome::Refused(other) => {
                // Distinct fixed codes for the host log; the extension sees
                // the same `unavailable` whatever the cause.
                let reason = match other {
                    401 => "unauthorized",
                    403 => "purpose_not_granted",
                    413 => "too_large",
                    429 => "rate_limited",
                    _ => "refused",
                };
                tracing::debug!(
                    extension = %extension_id,
                    status = other,
                    reason,
                    "artifact put refused"
                );
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
        };
        // The reply is untrusted: at most MAX_REPLY_BYTES of it were read, and
        // only a well-formed artifact id is accepted.
        let raw = raw.ok_or_else(|| unavailable("artifact door answered an unreadable body"))?;
        let parsed: PutBody = serde_json::from_slice(&raw)
            .map_err(|_| unavailable("artifact door answered an unreadable body"))?;
        if !greentic_aw_runtime::is_artifact_ref(&parsed.id) {
            return Err(unavailable("artifact door answered an invalid id"));
        }
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
