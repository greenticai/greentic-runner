//! Production [`SessionResumer`] that resumes a paused flow by feeding a
//! synthesized [`IngressEnvelope`] back through the runtime ingress entry
//! ([`StateMachineRuntime::handle`]).
//!
//! ## How a resume is keyed (verified against `engine/runtime.rs`)
//!
//! When a `sorla.call await` node pauses, `PackFlowAdapter::call` persists the
//! wait via `FlowResumeStore::save(envelope, wait)`. The wait is keyed by
//! `build_store_ctx`, which derives:
//!   * a **store hint** = `session_hint` (else `canonical_session_hint`), and
//!     when `pack_id` is present, suffixed with `::pack=<pack_id>`;
//!   * a **user id** = `sha256(store_hint)` (see `derive_user_id`);
//!   * a **reply scope** = the envelope's `ReplyScope` (whose `scope_hash`
//!     covers `conversation`/`thread`/`reply_to`).
//!
//! To resume, `FlowResumeStore::fetch(envelope)` re-derives the same triplet,
//! so the synthesized envelope MUST reproduce the *exact* store hint and reply
//! scope used at save time. The dispatch correlation id echoed in the response
//! is `ctx.session_id` — the canonical session hint (optionally carrying the
//! `::pack=<id>` suffix). This resumer splits that suffix off, sets the BARE
//! hint as `session_hint` (so `build_store_ctx` re-appends the pack suffix
//! exactly once), carries the recovered `pack_id`, and rebuilds the reply scope
//! by parsing the conversation out of the canonical hint
//! (`tenant:provider:channel:conversation:user`).
//!
//! ### Keying + routing caveat (IMPORTANT — see PR notes)
//!
//! Resuming through `handle` requires the synthesized envelope to (a) ROUTE via
//! `StateMachine::step`, which needs a *registered* `(pack_id, flow_id)`, and
//! (b) KEY the saved wait via `FlowResumeStore::fetch`, which needs the store
//! hint (`<bare hint>::pack=<pack_id>`) and the reply scope (`scope_hash` over
//! `conversation`/`thread`/`reply_to`).
//!
//! The dispatch wire contract ([`greentic_types::RuntimeDispatchResponse`] plus
//! the `Greentic-*` headers) carries only `(tenant, env, correlation_id)`. The
//! correlation id is the canonical session hint, which embeds the conversation
//! (4th `:`-segment) but NOT the `pack_id` or `flow_id` needed to route.
//!
//! This resumer recovers `pack_id` from a `::pack=<id>` suffix (the convention
//! pinned by `tests/sorla_node.rs`, where `ctx.session_id` carries it) and
//! `flow_id` from an additional `::flow=<id>` marker. For production resume to
//! work, the dispatch side must therefore emit a correlation id of the form
//! `<bare hint>::pack=<pack_id>::flow=<flow_id>` (or the wire contract must be
//! extended to carry pack/flow, or a server-side `correlation_id → (pack, flow,
//! scope)` map must exist). A bare canonical hint alone is NOT resumable.
//!
//! Waits whose original inbound used a non-empty `thread`/`reply_to` ARE
//! resumable: `execute_sorla_call` appends `::thread=<t>`/`::reply=<r>` markers
//! (each omitted when empty), and this resumer strips them and feeds them back
//! into the synthesized `ReplyScope` so `fetch` recomputes the same
//! `scope_hash` `save` used. The no-thread case is unchanged (no markers
//! emitted → `thread`/`reply_to` stay `None`).
//!
//! ### The per-dispatch nonce (`::n=`, approval runtime only)
//!
//! An `approval.call` correlation id ends with `::n=<32 lowercase hex>`, minted
//! per dispatch, so two approvals in one conversation are two ids at the
//! responder (greentic-designer-admin UNIQUE-indexes the id). It is always the
//! LAST segment and is stripped FIRST here, before any other marker, so the
//! store hint and reply scope — and therefore the saved wait's key — are
//! exactly what they were without it. The park stays keyed per conversation.
//!
//! The nonce is what lets a response be matched to its OWN gate: the parked
//! snapshot records the id each pending approval was published under, and
//! [`RuntimeSessionResumer::resume`] refuses a nonced response that names a
//! different one (an earlier gate's late decision, or its watchdog `timeout`
//! arriving after the conversation moved on) instead of feeding it to
//! whichever gate is parked now. An id with no nonce (every other runtime, and
//! any id minted before this segment existed) parses and resumes exactly as
//! before and is never verified.

use anyhow::Result;
use async_trait::async_trait;
use greentic_types::{ReplyScope, TenantCtx};
use serde_json::Value;
use std::sync::Arc;

use super::dispatch_listener::SessionResumer;
use crate::engine::runtime::{IngressEnvelope, StateMachineRuntime};
use crate::runner::engine::{ParkedApproval, response_authenticates};

/// Marker appended to a store hint when a pack id is known (mirrors
/// `build_store_ctx` in `engine/runtime.rs`).
const PACK_HINT_MARKER: &str = "::pack=";

/// Marker carrying the routing flow id in the correlation id. Unlike `::pack=`,
/// this is NOT part of the store hint — it is consumed only to route the resume
/// envelope through `StateMachine::step`, which requires a registered
/// `(pack_id, flow_id)`. It is stripped before the store hint is derived.
const FLOW_HINT_MARKER: &str = "::flow=";

/// Marker carrying the originating inbound `ReplyScope.thread`. The store key
/// (`FlowResumeStore::save`) hashes the reply scope's `thread`/`reply_to`, so a
/// wait saved against a non-empty thread is only re-keyable if the resumer
/// reproduces that thread. The dispatch side (`execute_sorla_call`) appends this
/// marker (omitting it when the thread is empty). It is stripped before the
/// store hint is derived and fed back into the synthesized `ReplyScope`.
const THREAD_HINT_MARKER: &str = "::thread=";

/// Marker carrying the originating inbound `ReplyScope.reply_to`. See
/// [`THREAD_HINT_MARKER`]; same contract for `reply_to`.
const REPLY_HINT_MARKER: &str = "::reply=";

/// Marker for the per-dispatch nonce an `approval` correlation id ends with.
/// Always the LAST segment; see the module docs.
pub(crate) const NONCE_HINT_MARKER: &str = "::n=";

/// Length of a nonce value: 16 random bytes, hex-encoded (the width of a v4
/// UUID in its `simple` form).
const NONCE_HEX_LEN: usize = 32;

/// Mint a fresh per-dispatch nonce: 32 lowercase hex characters.
pub(crate) fn new_dispatch_nonce() -> String {
    use rand::{RngExt, rng};
    let mut bytes = [0u8; NONCE_HEX_LEN / 2];
    rng().fill(&mut bytes);
    hex::encode(bytes)
}

/// Split a trailing `::n=<nonce>` off `correlation_id`.
///
/// Strict on purpose: only a LAST segment of exactly 32 lowercase hex
/// characters is a nonce. Anything else — including a thread or reply value
/// that happens to contain `::n=` — leaves the id unchanged, so an id without
/// a nonce parses exactly as it always did.
pub fn split_dispatch_nonce(correlation_id: &str) -> (&str, Option<&str>) {
    match correlation_id.rsplit_once(NONCE_HINT_MARKER) {
        Some((prefix, nonce))
            if nonce.len() == NONCE_HEX_LEN
                && nonce
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)) =>
        {
            (prefix, Some(nonce))
        }
        _ => (correlation_id, None),
    }
}

/// Resumes paused flow sessions by driving the runtime ingress entry.
pub struct RuntimeSessionResumer {
    runtime: Arc<StateMachineRuntime>,
    mode: AdmissionMode,
}

/// How strictly a NONCED response must match the park before it resumes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AdmissionMode {
    /// The NATS rail's rule: a nonced response with NOTHING parked is left to
    /// the runtime (which then starts a fresh run), and a park recorded before
    /// ids were stored may take any nonced response.
    Lenient,
    /// The HTTP approval inbox's rule (`runner::approval_http`): a nonced
    /// response resumes ONLY a parked approval recorded under exactly that id.
    /// Nothing parked, or a park without a recorded id, is
    /// [`Admission::WrongGate`] — a decision fetched for a gate that is gone
    /// must never start a fresh run from the flow's entrypoint.
    Strict,
}

impl RuntimeSessionResumer {
    /// Build a resumer over the live runtime ingress handle.
    ///
    /// `runtime` is the same [`StateMachineRuntime`] that serves inbound
    /// ingress; resuming routes through its `handle` so the existing
    /// `FlowResumeStore::fetch` + `FlowEngine::resume` path runs unchanged.
    pub fn new(runtime: Arc<StateMachineRuntime>) -> Self {
        Self {
            runtime,
            mode: AdmissionMode::Lenient,
        }
    }

    /// A resumer for the HTTP approval inbox: a nonced response resumes only
    /// the approval parked under exactly that id, and NEVER falls through to a
    /// fresh run when nothing is parked (see [`AdmissionMode::Strict`]).
    pub fn strict(runtime: Arc<StateMachineRuntime>) -> Self {
        Self {
            runtime,
            mode: AdmissionMode::Strict,
        }
    }

    /// Build the [`IngressEnvelope`] that re-keys the saved wait.
    ///
    /// Split out (and `pub(crate)`) so it can be unit-tested without a broker.
    ///
    /// * `session_hint` is set to the bare canonical hint so
    ///   `FlowResumeStore::fetch` re-derives the same store key (with the pack
    ///   suffix re-appended exactly once by `build_store_ctx`).
    /// * `pack_id` is recovered from the `::pack=<id>` suffix when present so
    ///   `StateMachineRuntime::handle` (which requires a `pack_id`) succeeds and
    ///   the store hint is reproduced identically.
    /// * `flow_id` is recovered from the `::flow=<id>` marker when present so the
    ///   resume routes through `StateMachine::step` (which requires a registered
    ///   `(pack_id, flow_id)`). The saved wait's snapshot then supplies the real
    ///   resume flow/node. When absent, the bare pack id is used as a fallback
    ///   flow id (only relevant when no wait is found).
    /// * the reply scope's `conversation` is parsed from the canonical hint so
    ///   the scope hash matches the saved wait.
    /// * `payload` carries the runtime `output` as the resume flow input.
    ///
    /// NOTE: the dispatch wire contract ([`greentic_types::RuntimeDispatchResponse`]
    /// plus headers) carries only `(tenant, env, correlation_id)`. Routing a
    /// resume requires `pack_id` AND `flow_id`; this resumer recovers them from
    /// the `::pack=`/`::flow=` markers in the correlation id. Until those markers
    /// are emitted at dispatch time (or the response carries the fields), a bare
    /// canonical hint cannot be resumed — see the module docs.
    pub(crate) fn build_resume_envelope(
        tenant: &TenantCtx,
        correlation_id: &str,
        output: Value,
    ) -> IngressEnvelope {
        // Strip markers in the REVERSE of the order they were appended at
        // dispatch (`pack`, `flow`, `thread`, `reply`, `n`) so each
        // `rsplit_once` sees its own marker last. Each marker is optional —
        // absent markers leave the string unchanged (back-compat with the
        // no-thread and no-nonce cases). The nonce is NOT part of the store
        // key: dropping it here is what keeps the park keyed per conversation.
        let (without_nonce, _nonce) = split_dispatch_nonce(correlation_id);
        let (without_reply, reply_to) = split_marker(without_nonce, REPLY_HINT_MARKER);
        let (without_thread, thread) = split_marker(&without_reply, THREAD_HINT_MARKER);
        let (without_flow, flow_marker) = split_marker(&without_thread, FLOW_HINT_MARKER);
        let (bare_hint, pack_id) = split_pack_suffix(&without_flow);
        let parts: Vec<&str> = bare_hint.splitn(5, ':').collect();
        let provider = parts.get(1).map(|segment| segment.to_string());
        let channel = parts.get(2).map(|segment| segment.to_string());
        let conversation = parts.get(3).map(|segment| segment.to_string());
        let user = parts.get(4).map(|segment| segment.to_string());

        let flow_id = flow_marker
            .or_else(|| pack_id.clone())
            .unwrap_or_else(|| "resume.flow".to_string());

        IngressEnvelope {
            tenant: tenant.tenant_id.to_string(),
            env: Some(tenant.env.to_string()),
            pack_id,
            // Routes the resume into `PackFlowAdapter::call`; the saved wait's
            // snapshot overrides the actual resume flow/node from there.
            flow_id,
            flow_type: None,
            action: None,
            // The store hint is `session_hint` (+ `::pack=<pack_id>` re-appended
            // by `build_store_ctx`). We set the BARE hint here and carry the pack
            // id separately so the suffix is reproduced exactly once — setting
            // the suffixed correlation id here AND `pack_id` would double-suffix
            // and miss the saved wait.
            session_hint: Some(bare_hint.clone()),
            provider,
            channel,
            conversation: conversation.clone(),
            user,
            // A session resume re-enters through its snapshot, not a card nav target.
            entry_node: None,
            activity_id: None,
            timestamp: None,
            messaging_endpoint_id: None,
            payload: output,
            metadata: None,
            // Reproduce the EXACT reply scope used at save time so
            // `FlowResumeStore::fetch` recomputes the same `scope_hash`. The
            // conversation comes from the bare hint; thread/reply_to are
            // recovered from the `::thread=`/`::reply=` markers (absent markers
            // → `None`, matching the no-thread case).
            reply_scope: conversation.map(|conversation| ReplyScope {
                conversation,
                thread,
                reply_to,
                correlation: None,
            }),
        }
    }
}

#[async_trait]
impl SessionResumer for RuntimeSessionResumer {
    async fn resume(&self, tenant: TenantCtx, correlation_id: &str, output: Value) -> Result<()> {
        let nonced = split_dispatch_nonce(correlation_id).1.is_some();
        let envelope = Self::build_resume_envelope(&tenant, correlation_id, output.clone());
        // Read for EVERY response, not only nonced ones: a sender that omits
        // the nonce must not skip the token check. A read failure is fatal for
        // a nonced (approval) response, as before; for any other response it
        // falls through to the runtime, where the approval gate re-checks the
        // token itself before deciding anything.
        let parked = match self.runtime.parked_approvals(&envelope).await {
            Ok(parked) => parked,
            Err(error) if !nonced => {
                tracing::warn!(%error, %correlation_id, "could not read the parked flow before resuming; the gate re-checks the response itself");
                None
            }
            Err(error) => return Err(error),
        };
        match admit_response_with(self.mode, correlation_id, &output, parked.as_deref()) {
            Admission::Resume => {}
            Admission::WrongGate => {
                // Not an error: the response is real, it just no longer has a
                // gate to land on. Resuming would hand an earlier gate's
                // decision (or its watchdog's late `timeout`) to a different
                // gate — or to whatever the conversation is parked on now.
                tracing::warn!(
                    %correlation_id,
                    "dropping approval response: it does not match the approval this conversation is parked on"
                );
                return Ok(());
            }
            Admission::BadToken => {
                // A security event: whoever sent this does not hold the token
                // the gate was issued (or is replaying a spent one). The gate
                // stays parked. The token is never logged.
                tracing::warn!(
                    %correlation_id,
                    "dropping approval response: its decision_token is missing or does not match the one the parked approval was issued"
                );
                return Ok(());
            }
        }
        self.runtime.handle(envelope).await.map(|_| ())
    }
}

/// What the resumer does with a dispatch response, decided before the runtime
/// sees it.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Admission {
    /// Hand it to the runtime.
    Resume,
    /// A NONCED approval response for an approval this conversation is not
    /// parked on (greentic-runner#793).
    WrongGate,
    /// The approval it would land on was issued a `decision_token` and the
    /// response does not carry it (greentic-runner#794).
    BadToken,
}

/// Decide whether `response`, published under `correlation_id`, may resume
/// the conversation's park.
///
/// `parked` is `None` when nothing is parked: that case is left to the runtime
/// exactly as before these checks existed (a spent token cannot resurrect a
/// gate — its mark is gone — and a fresh run re-dispatches rather than
/// deciding).
///
/// Otherwise the candidates are the parked approvals the response could land
/// on: for a nonced id, those published under this exact id (or recorded
/// before ids were, which cannot be told apart); for an id with no nonce, all
/// of them. A nonced response with no candidate is [`Admission::WrongGate`].
/// A response resumes when some candidate was issued no token (a legacy park)
/// or its token matches one; otherwise it is [`Admission::BadToken`]. An id
/// with no nonce and no parked approval (the conversation waits on something
/// else) is left to the runtime, as before.
#[cfg(test)]
pub(crate) fn admit_response(
    correlation_id: &str,
    response: &Value,
    parked: Option<&[ParkedApproval]>,
) -> Admission {
    admit_response_with(AdmissionMode::Lenient, correlation_id, response, parked)
}

/// [`admit_response`] under an explicit [`AdmissionMode`]. `Strict` changes
/// only the NONCED cases: nothing parked is [`Admission::WrongGate`] rather
/// than [`Admission::Resume`], and a park must have recorded exactly this id.
pub(crate) fn admit_response_with(
    mode: AdmissionMode,
    correlation_id: &str,
    response: &Value,
    parked: Option<&[ParkedApproval]>,
) -> Admission {
    let nonced = split_dispatch_nonce(correlation_id).1.is_some();
    let strict = nonced && mode == AdmissionMode::Strict;
    let Some(parked) = parked else {
        return if strict {
            Admission::WrongGate
        } else {
            Admission::Resume
        };
    };
    let candidates: Vec<&ParkedApproval> = parked
        .iter()
        .filter(|park| {
            if !nonced {
                return true;
            }
            match park.correlation_id.as_deref() {
                Some(recorded) => recorded == correlation_id,
                None => !strict,
            }
        })
        .collect();
    if candidates.is_empty() {
        return if nonced {
            Admission::WrongGate
        } else {
            Admission::Resume
        };
    }
    if candidates
        .iter()
        .any(|park| response_authenticates(response, park))
    {
        Admission::Resume
    } else {
        Admission::BadToken
    }
}

/// Split a store hint into its bare canonical hint and the pack id encoded in
/// the trailing `::pack=<id>` marker (if any). Mirrors the suffix added by
/// `build_store_ctx`.
fn split_pack_suffix(hint: &str) -> (String, Option<String>) {
    split_marker(hint, PACK_HINT_MARKER)
}

/// Split `value` on the LAST occurrence of `marker`, returning the prefix and
/// the captured (non-empty) trailing segment. When the marker is absent or the
/// captured value is empty, the input is returned unchanged with `None`.
fn split_marker(value: &str, marker: &str) -> (String, Option<String>) {
    match value.rsplit_once(marker) {
        Some((prefix, captured)) if !captured.is_empty() => {
            (prefix.to_string(), Some(captured.to_string()))
        }
        _ => (value.to_string(), None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use greentic_types::{EnvId, TenantId};
    use serde_json::json;
    use std::str::FromStr;

    fn tenant_ctx() -> TenantCtx {
        TenantCtx::new(
            EnvId::from_str("local").unwrap(),
            TenantId::from_str("demo").unwrap(),
        )
    }

    #[test]
    fn build_resume_envelope_sets_session_hint_to_correlation_id() {
        let envelope = RuntimeSessionResumer::build_resume_envelope(
            &tenant_ctx(),
            "demo:provider:chan:conv:user",
            json!({ "ok": true }),
        );
        assert_eq!(
            envelope.session_hint.as_deref(),
            Some("demo:provider:chan:conv:user")
        );
        assert_eq!(envelope.payload, json!({ "ok": true }));
        assert_eq!(envelope.tenant, "demo");
        assert_eq!(envelope.env.as_deref(), Some("local"));
    }

    #[test]
    fn build_resume_envelope_parses_conversation_into_reply_scope() {
        let envelope = RuntimeSessionResumer::build_resume_envelope(
            &tenant_ctx(),
            "demo:provider:chan:conv:user",
            json!(null),
        );
        let scope = envelope.reply_scope.expect("reply scope from conversation");
        assert_eq!(scope.conversation, "conv");
        assert!(scope.thread.is_none());
        assert!(scope.correlation.is_none());
        assert_eq!(envelope.conversation.as_deref(), Some("conv"));
        assert_eq!(envelope.provider.as_deref(), Some("provider"));
        assert_eq!(envelope.channel.as_deref(), Some("chan"));
        assert_eq!(envelope.user.as_deref(), Some("user"));
    }

    #[test]
    fn build_resume_envelope_recovers_pack_id_from_suffix() {
        let envelope = RuntimeSessionResumer::build_resume_envelope(
            &tenant_ctx(),
            "demo:provider:chan:conv:user::pack=greentic.demo",
            json!(null),
        );
        assert_eq!(envelope.pack_id.as_deref(), Some("greentic.demo"));
        // session_hint is the BARE hint; `build_store_ctx` re-appends the pack
        // suffix exactly once, so it must not be carried here.
        assert_eq!(
            envelope.session_hint.as_deref(),
            Some("demo:provider:chan:conv:user")
        );
        // conversation is still parsed from the bare hint, not the suffix.
        assert_eq!(envelope.conversation.as_deref(), Some("conv"));
    }

    #[test]
    fn build_resume_envelope_recovers_pack_and_flow_for_routing() {
        let envelope = RuntimeSessionResumer::build_resume_envelope(
            &tenant_ctx(),
            "demo:provider:chan:conv:user::pack=greentic.demo::flow=wait.flow",
            json!(null),
        );
        // flow id routes the resume; pack id keys it.
        assert_eq!(envelope.flow_id, "wait.flow");
        assert_eq!(envelope.pack_id.as_deref(), Some("greentic.demo"));
        // the flow marker is stripped before deriving the store hint.
        assert_eq!(
            envelope.session_hint.as_deref(),
            Some("demo:provider:chan:conv:user")
        );
        assert_eq!(envelope.conversation.as_deref(), Some("conv"));
    }

    #[test]
    fn build_resume_envelope_recovers_thread_and_reply_into_scope() {
        let envelope = RuntimeSessionResumer::build_resume_envelope(
            &tenant_ctx(),
            "demo:provider:chan:conv:user::pack=greentic.demo::flow=wait.flow::thread=topic-7::reply=msg-42",
            json!(null),
        );
        // routing markers still recovered with thread/reply present.
        assert_eq!(envelope.flow_id, "wait.flow");
        assert_eq!(envelope.pack_id.as_deref(), Some("greentic.demo"));
        // bare hint is clean of every marker.
        assert_eq!(
            envelope.session_hint.as_deref(),
            Some("demo:provider:chan:conv:user")
        );
        let scope = envelope.reply_scope.expect("reply scope with thread/reply");
        assert_eq!(scope.conversation, "conv");
        assert_eq!(scope.thread.as_deref(), Some("topic-7"));
        assert_eq!(scope.reply_to.as_deref(), Some("msg-42"));
    }

    #[test]
    fn build_resume_envelope_recovers_thread_without_reply() {
        let envelope = RuntimeSessionResumer::build_resume_envelope(
            &tenant_ctx(),
            "demo:provider:chan:conv:user::pack=greentic.demo::flow=wait.flow::thread=topic-7",
            json!(null),
        );
        let scope = envelope.reply_scope.expect("reply scope with thread");
        assert_eq!(scope.thread.as_deref(), Some("topic-7"));
        assert!(scope.reply_to.is_none());
        assert_eq!(
            envelope.session_hint.as_deref(),
            Some("demo:provider:chan:conv:user")
        );
    }

    #[test]
    fn build_resume_envelope_without_markers_leaves_thread_and_reply_none() {
        // Back-compat: the no-thread gold path must keep thread/reply_to None.
        let envelope = RuntimeSessionResumer::build_resume_envelope(
            &tenant_ctx(),
            "demo:provider:chan:conv:user::pack=greentic.demo::flow=wait.flow",
            json!(null),
        );
        let scope = envelope.reply_scope.expect("reply scope from conversation");
        assert!(scope.thread.is_none());
        assert!(scope.reply_to.is_none());
    }

    #[test]
    fn split_pack_suffix_handles_missing_and_empty_marker() {
        assert_eq!(
            split_pack_suffix("a:b:c:d:e"),
            ("a:b:c:d:e".to_string(), None)
        );
        assert_eq!(
            split_pack_suffix("a:b:c:d:e::pack=p"),
            ("a:b:c:d:e".to_string(), Some("p".to_string()))
        );
        assert_eq!(
            split_pack_suffix("a:b:c:d:e::pack="),
            ("a:b:c:d:e::pack=".to_string(), None)
        );
    }

    const NONCE: &str = "0123456789abcdef0123456789abcdef";

    #[test]
    fn new_dispatch_nonce_is_32_lowercase_hex_and_fresh() {
        let a = new_dispatch_nonce();
        let b = new_dispatch_nonce();
        assert_eq!(a.len(), 32);
        assert!(
            a.bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
        );
        assert_ne!(a, b, "two dispatches must not share a nonce");
        let id = format!("h::pack=p::flow=f{NONCE_HINT_MARKER}{a}");
        assert_eq!(
            split_dispatch_nonce(&id),
            ("h::pack=p::flow=f", Some(a.as_str()))
        );
    }

    #[test]
    fn split_dispatch_nonce_leaves_ids_without_a_nonce_unchanged() {
        // No marker at all: the old format.
        let old = "demo:provider:chan:conv:user::pack=greentic.demo::flow=wait.flow";
        assert_eq!(split_dispatch_nonce(old), (old, None));
        // A reply value that happens to contain `::n=` is not a nonce.
        let reply = "a:b:c:d:e::pack=p::flow=f::reply=x::n=not-hex";
        assert_eq!(split_dispatch_nonce(reply), (reply, None));
        // Wrong length / upper case are not nonces either.
        let short = "h::pack=p::n=abc";
        assert_eq!(split_dispatch_nonce(short), (short, None));
        let upper = format!("h::pack=p::n={}", NONCE.to_uppercase());
        assert_eq!(split_dispatch_nonce(&upper), (upper.as_str(), None));
    }

    #[test]
    fn a_nonce_does_not_change_the_resume_key() {
        // The park is keyed per conversation: with or without the nonce the
        // synthesized envelope must be identical, or the saved wait is missed.
        let base = "demo:provider:chan:conv:user::pack=greentic.demo::flow=wait.flow::thread=topic-7::reply=msg-42";
        let with_nonce = format!("{base}::n={NONCE}");
        let plain = RuntimeSessionResumer::build_resume_envelope(&tenant_ctx(), base, json!(1));
        let nonced =
            RuntimeSessionResumer::build_resume_envelope(&tenant_ctx(), &with_nonce, json!(1));
        assert_eq!(nonced.session_hint, plain.session_hint);
        assert_eq!(
            nonced.session_hint.as_deref(),
            Some("demo:provider:chan:conv:user")
        );
        assert_eq!(nonced.pack_id, plain.pack_id);
        assert_eq!(nonced.flow_id, "wait.flow");
        let (scope, plain_scope) = (nonced.reply_scope.unwrap(), plain.reply_scope.unwrap());
        assert_eq!(scope.thread.as_deref(), Some("topic-7"));
        assert_eq!(
            scope.reply_to.as_deref(),
            Some("msg-42"),
            "the nonce must not leak into reply_to"
        );
        assert_eq!(scope.scope_hash(), plain_scope.scope_hash());
    }

    #[test]
    fn a_nonced_response_resumes_only_its_own_parked_approval() {
        let mine = format!("h::pack=p::flow=f::n={NONCE}");
        let other = format!("h::pack=p::flow=f::n={}", "f".repeat(32));
        let untokened = |id: Option<&str>| ParkedApproval {
            correlation_id: id.map(str::to_string),
            token_fingerprint: None,
        };
        let r = json!({"ok": true, "output": {"decision": "approved"}});
        // Nothing parked: left to the runtime, unchanged.
        assert_eq!(admit_response(&mine, &r, None), Admission::Resume);
        // Parked on this very approval.
        assert_eq!(
            admit_response(&mine, &r, Some(&[untokened(Some(&mine))])),
            Admission::Resume
        );
        // Parked on a DIFFERENT approval in the same conversation.
        assert_eq!(
            admit_response(&mine, &r, Some(&[untokened(Some(&other))])),
            Admission::WrongGate
        );
        // Parked, but not on any approval (e.g. a card after the gate passed).
        assert_eq!(admit_response(&mine, &r, Some(&[])), Admission::WrongGate);
        // A legacy mark with no recorded id cannot be verified: not refused.
        assert_eq!(
            admit_response(&mine, &r, Some(&[untokened(None)])),
            Admission::Resume
        );
    }

    fn tokened(id: &str, fingerprint: &str) -> ParkedApproval {
        ParkedApproval {
            correlation_id: Some(id.to_string()),
            token_fingerprint: Some(fingerprint.to_string()),
        }
    }

    fn response(token: Option<&str>) -> Value {
        match token {
            Some(token) => {
                json!({"ok": true, "output": {"decision": "approved", "decision_token": token}})
            }
            None => json!({"ok": true, "output": {"decision": "approved"}}),
        }
    }

    #[test]
    fn a_tokened_park_resumes_only_on_the_right_token() {
        use crate::runner::approval_token::issue;
        let mine = format!("h::pack=p::flow=f::n={NONCE}");
        let issued = issue();
        let parks = [tokened(&mine, &issued.fingerprint)];
        assert_eq!(
            admit_response(&mine, &response(Some(&issued.token)), Some(&parks)),
            Admission::Resume
        );
        assert_eq!(
            admit_response(&mine, &response(Some(&issue().token)), Some(&parks)),
            Admission::BadToken,
            "a wrong token is refused"
        );
        assert_eq!(
            admit_response(&mine, &response(None), Some(&parks)),
            Admission::BadToken,
            "a missing token is refused"
        );
    }

    #[test]
    fn dropping_the_nonce_does_not_skip_the_token_check() {
        // The derived, guessable id with no `::n=`: the attack from #794.
        use crate::runner::approval_token::issue;
        let issued = issue();
        let parks = [tokened(
            &format!("h::pack=p::flow=f::n={NONCE}"),
            &issued.fingerprint,
        )];
        let guessed = "h::pack=p::flow=f";
        assert_eq!(
            admit_response(guessed, &response(None), Some(&parks)),
            Admission::BadToken
        );
        assert_eq!(
            admit_response(guessed, &response(Some(&issued.token)), Some(&parks)),
            Admission::Resume
        );
    }

    /// The HTTP approval inbox's rule: a decision it fetched for a gate that is
    /// no longer parked must never fall through to a fresh run.
    #[test]
    fn strict_mode_never_resumes_a_nonced_response_with_nothing_parked() {
        use crate::runner::approval_token::issue;
        let mine = format!("h::pack=p::flow=f::n={NONCE}");
        let issued = issue();
        let r = response(Some(&issued.token));
        assert_eq!(
            admit_response_with(AdmissionMode::Strict, &mine, &r, None),
            Admission::WrongGate,
            "nothing parked: a fresh run would re-execute the flow and raise a second approval"
        );
        assert_eq!(
            admit_response_with(AdmissionMode::Strict, &mine, &r, Some(&[])),
            Admission::WrongGate
        );
        // A park recorded without an id cannot be proven to be this gate.
        let legacy = ParkedApproval {
            correlation_id: None,
            token_fingerprint: None,
        };
        assert_eq!(
            admit_response_with(AdmissionMode::Strict, &mine, &r, Some(&[legacy])),
            Admission::WrongGate
        );
        // Parked under exactly this id, with the right token: resumes.
        let parks = [tokened(&mine, &issued.fingerprint)];
        assert_eq!(
            admit_response_with(AdmissionMode::Strict, &mine, &r, Some(&parks)),
            Admission::Resume
        );
        // Parked under another nonce: dropped.
        let other = format!("h::pack=p::flow=f::n={}", "f".repeat(32));
        let parks = [tokened(&other, &issued.fingerprint)];
        assert_eq!(
            admit_response_with(AdmissionMode::Strict, &mine, &r, Some(&parks)),
            Admission::WrongGate
        );
    }

    /// Strict mode changes nothing for an id without a nonce, and lenient mode
    /// (NATS) keeps today's fall-through.
    #[test]
    fn strict_mode_leaves_unnonced_ids_and_lenient_mode_unchanged() {
        let r = json!({"ok": true});
        assert_eq!(
            admit_response_with(AdmissionMode::Strict, "h::pack=p::flow=f", &r, None),
            Admission::Resume
        );
        let mine = format!("h::pack=p::flow=f::n={NONCE}");
        assert_eq!(
            admit_response_with(AdmissionMode::Lenient, &mine, &r, None),
            Admission::Resume
        );
    }
}
