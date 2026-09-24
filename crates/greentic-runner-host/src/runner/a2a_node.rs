//! Flow-execution A2A node (LOCKED ENCODING v1: `component == "a2a"`).
//!
//! A plain flow can ask an external A2A agent one question. The node carries
//! `agent`, `message` and an optional `output` state key in its
//! payload/config; the payload is the source of truth, exactly as
//! [`super::mcp_node`]'s `server`/`tool` are.
//!
//! ```text
//! nodes:
//!   ask_recipe:
//!     a2a:
//!       agent: 8d2f0c1e-recipe     # the admin row id, as the sidecar names it
//!       message: "{{ entry.text }}"
//!       output: recipe             # optional state key to bind the result under
//!     routing: [ ... ]
//! ```
//!
//! # This is the flow lane, and it reuses the worker lane whole
//!
//! Everything below the route record is [`greentic_aw_runtime::a2a_source`]:
//! the card fetch, the same-host rule, the per-call credential read, the
//! `SendMessage` exchange, the continuation, and — most importantly — the
//! rendering of a remote task's state into a value. Nothing here classifies an
//! outcome. `A2aToolCatalog::dispatch_in_conversation` is called verbatim, so
//! the rule **`reply` appears if and only if `status` is `completed`** is the
//! same rule in both lanes, pinned by one test
//! (`a2a_source::outcome::tests::only_a_completed_outcome_carries_a_reply`).
//!
//! What a flow node adds is only the two things a worker already had and a
//! flow did not: where the agents come from (this pack's own
//! `assets/a2a-routes.json`, never another pack's) and where the remote
//! context is remembered (below).
//!
//! # Five statuses, and a flow ROUTES on them
//!
//! A flow cannot stop mid-node to ask a human, so a remote agent that answers
//! `input-required` is not an error and must not read as an answer either.
//! The node's value always carries `status`, one of `completed`,
//! `input_required`, `working`, `failed` (the remote decided) or `error` (we
//! could not ask it) — so a `conditional_branch` on
//! `{{node.<id>.payload.status}}` is how a flow handles each one. Only
//! `completed` carries `reply`.
//!
//! # Continuation: one remote context per (tenant, env, session, agent)
//!
//! A worker keeps its remote `contextId` in `ConversationState`. A flow has no
//! such thing, so this node keeps the same [`A2aContinuations`] map — the type
//! itself, not a copy of its rules — in the flow's own durable state store
//! under prefix [`aw::CONTINUATION_PREFIX`], keyed by the session hint.
//!
//! The rule, stated so it can be relied on: **every call this session makes to
//! one agent continues the same remote conversation.** A flow that routes
//! `input_required` to a card, collects the answer and comes back to this node
//! therefore ANSWERS the remote's question instead of asking it again — which
//! is the only shape in which a flow can use `input-required` at all. Two
//! different sessions never share a context (the key differs, and
//! [`A2aContinuations::resume`] re-checks the tenant and env recorded in the
//! value itself).
//!
//! **A run with no session hint gets no continuation**, because there is no
//! conversation to belong to: such a call is its own context and nothing is
//! stored. Same when the runtime has no state store configured. Both are
//! degradations to "a fresh remote context", never a node failure.

use serde_json::Value;

/// The agent id an A2A node names, from its payload/config object (LOCKED
/// ENCODING v1). `None` when the key is absent, not a string, or blank, in
/// which case the caller falls back to the `operation` / `a2a:<agent_id>`
/// component-ref form.
pub(crate) fn agent_from_payload(payload: &Value) -> Option<String> {
    super::mcp_node::str_field(payload, "agent")
}

/// Parse an `a2a:<agent_id>` component ref, or a bare `<agent_id>` carried as
/// the node `operation`. Returns `None` for an empty id.
pub(crate) fn agent_from_ref(reference: &str) -> Option<String> {
    let id = reference.strip_prefix("a2a:").unwrap_or(reference).trim();
    (!id.is_empty()).then(|| id.to_string())
}

#[cfg(feature = "agentic-worker")]
pub mod aw {
    use std::sync::Arc;

    use chrono::Utc;
    use greentic_aw_runtime::TenantContext;
    use greentic_aw_runtime::a2a_source::{
        A2aContinuations, A2aToolSource, CONTINUATION_IDLE_TTL_SECS, call_error_value,
    };
    use greentic_types::{StateKey, TenantCtx};
    use serde_json::Value;

    use crate::pack::PackRuntime;
    use crate::storage::DynStateStore;

    /// The state-store namespace the per-session continuation map lives in.
    ///
    /// Note both accessors below call the SYNCHRONOUS [`greentic_state::StateStore`]
    /// from inside an `async fn`, which is what the `state.get` / `state.set`
    /// nodes already do (`FlowEngine::execute_state_get`). Following that
    /// precedent keeps one story for flow-state access; moving this one call
    /// site onto `spawn_blocking` alone would make the two disagree without
    /// fixing anything.
    ///
    /// Deliberately NOT [`crate::storage::state::STATE_PREFIX`], which is what
    /// the `state.get` / `state.set` nodes write under: a flow author owns
    /// every key in that namespace, so sharing it would let an ordinary
    /// `state.set` overwrite — or read — this node's remote references.
    pub const CONTINUATION_PREFIX: &str = "runner-a2a";

    /// The store key one session's continuations live at. The tenant and env
    /// are not in it: [`greentic_state::fqn`] already scopes every key by
    /// both.
    fn continuation_key(session_id: &str) -> StateKey {
        StateKey::new(format!("continuations/{session_id}"))
    }

    /// The A2A agents THIS pack declares, or `None`.
    ///
    /// Scoped to the flow's own pack rather than to every pack the runtime
    /// loaded, which is the fail-closed reading of decision 4: a flow may ask
    /// only the agents its own `assets/a2a-routes.json` names. The worker lane
    /// unions a revision's packs because a worker's bindings are resolved
    /// against the whole unit; a flow node names one agent in one pack's flow.
    ///
    /// `None` for a pack with no sidecar, a sidecar declaring no agent, or
    /// `GREENTIC_AW_A2A=0` — the operator opt-out, which a pack must not be
    /// able to override.
    pub(crate) fn source_for_pack(
        pack: &Arc<PackRuntime>,
        tenant: &str,
        secrets: Option<crate::secrets::DynSecretsManager>,
        unit: Option<&str>,
    ) -> Option<Arc<A2aToolSource>> {
        crate::runner::a2a_pack_source::a2a_source_from_packs(
            std::slice::from_ref(pack),
            tenant,
            secrets,
            unit,
        )
    }

    /// Read this session's continuation map.
    ///
    /// Every failure — no store, no session, an unreadable entry, a blob this
    /// build cannot decode — yields an empty map, i.e. a fresh remote context.
    /// That is the safe direction: the cost is one new conversation with the
    /// remote agent, whereas failing the node would take down a call that has
    /// nothing wrong with it.
    fn load_continuations(
        store: Option<&DynStateStore>,
        state_ctx: Option<&TenantCtx>,
        session_id: Option<&str>,
    ) -> A2aContinuations {
        let (Some(store), Some(ctx), Some(session)) = (store, state_ctx, session_id) else {
            return A2aContinuations::default();
        };
        let stored = match store.get_json(
            ctx,
            CONTINUATION_PREFIX,
            &continuation_key(session),
            None,
        ) {
            Ok(value) => value,
            Err(error) => {
                tracing::warn!(
                    %error,
                    "a2a node could not read its continuation state; this call starts a fresh remote context"
                );
                return A2aContinuations::default();
            }
        };
        let Some(stored) = stored else {
            return A2aContinuations::default();
        };
        serde_json::from_value(stored).unwrap_or_else(|error| {
            tracing::warn!(
                %error,
                "a2a node could not decode its continuation state; this call starts a fresh remote context"
            );
            A2aContinuations::default()
        })
    }

    /// Write this session's continuation map back.
    ///
    /// The store TTL is refreshed to the continuation's own idle window, so a
    /// session that goes quiet does not leave a blob behind forever. It is a
    /// housekeeping measure and NOT the authority: expiry is decided by
    /// [`A2aContinuations::resume`] against the timestamp inside the value, so
    /// a backend that ignores TTLs still starts a fresh context after the same
    /// hour.
    ///
    /// A write failure is a `warn` and nothing else. The call has already
    /// happened; refusing the node afterwards would report a failure for work
    /// the remote agent really did, and the only consequence of a lost write
    /// is a fresh context next time.
    fn save_continuations(
        store: Option<&DynStateStore>,
        state_ctx: Option<&TenantCtx>,
        session_id: Option<&str>,
        continuations: &A2aContinuations,
    ) {
        let (Some(store), Some(ctx), Some(session)) = (store, state_ctx, session_id) else {
            return;
        };
        let value = match serde_json::to_value(continuations) {
            Ok(value) => value,
            Err(error) => {
                tracing::warn!(%error, "a2a node could not serialise its continuation state");
                return;
            }
        };
        let ttl = u32::try_from(CONTINUATION_IDLE_TTL_SECS).ok();
        if let Err(error) = store.set_json(
            ctx,
            CONTINUATION_PREFIX,
            &continuation_key(session),
            None,
            &value,
            ttl,
        ) {
            tracing::warn!(
                %error,
                "a2a node could not persist its continuation state; the next call to this agent \
                 will start a fresh remote context"
            );
        }
    }

    /// Everything one `a2a` node dispatch needs that is not the message.
    ///
    /// A struct rather than eight parameters because half of them are
    /// `Option<&str>` and a transposed pair would compile into a node that
    /// talks to the right agent under the wrong tenant.
    pub struct A2aCall<'a> {
        /// The agents this flow's pack declares. `None` is the "not
        /// configured" refusal below.
        pub source: Option<&'a Arc<A2aToolSource>>,
        /// The flow's durable state store, for the continuation map.
        pub store: Option<&'a DynStateStore>,
        /// The scope that store is keyed by. `None` disables continuation.
        pub state_ctx: Option<&'a TenantCtx>,
        /// The session hint this run belongs to. `None` disables continuation.
        pub session_id: Option<&'a str>,
        /// The runtime tenant and env, for the credential scope and for the
        /// ownership check inside a stored continuation.
        pub tenant: &'a str,
        pub env: &'a str,
        /// The pack whose sidecar `source` was built from, named in the
        /// "not configured" refusal so an operator knows which pack to rebuild.
        pub pack_id: &'a str,
    }

    /// Ask `agent_id` one question and return the value the node binds.
    ///
    /// Infallible by contract, like [`super::super::mcp_node::aw::invoke_with_secrets`]:
    /// an unconfigured pack, an unknown agent, a missing credential, an
    /// unreachable card and a transport failure all come back as a
    /// `status: "error"` value. The caller binds it as-is, so a flow routes on
    /// `status` whatever happened.
    ///
    /// `message` is handed to the catalogue untouched so
    /// `a2a_source`'s own `args_to_text` is the single converter from a node's
    /// JSON to the text an A2A agent receives — a string is sent verbatim,
    /// anything else as its JSON text, so no configured content is dropped.
    pub async fn invoke(call: A2aCall<'_>, agent_id: &str, message: &Value) -> Value {
        let Some(source) = call.source else {
            return call_error_value(
                agent_id,
                &format!(
                    "a2a is not available to this flow: pack '{}' declares no agents \
                     (no assets/a2a-routes.json, or GREENTIC_AW_A2A=0)",
                    call.pack_id
                ),
            );
        };

        let catalog = source.catalog().await;
        let tenant_ctx = TenantContext::new(call.tenant, call.env);
        let mut continuations = load_continuations(call.store, call.state_ctx, call.session_id);
        let before = continuations.clone();

        let result = catalog
            .dispatch_in_conversation(
                agent_id,
                message,
                &tenant_ctx,
                &mut continuations,
                Utc::now(),
            )
            .await;

        // Only on a change: an agent that neither opened nor closed anything
        // would otherwise make every turn of every flow a store write.
        if continuations != before {
            save_continuations(call.store, call.state_ctx, call.session_id, &continuations);
        }

        // Every failure travels as a value, and a node with no error route
        // still reports `ok: true` (see `a2a_node_output`), so without this an
        // operator sees a clean run whose agent was never asked. Mirrors the
        // MCP node's warn, and for the same reason.
        if let Some(error) = result.get("error") {
            // `status` is read out here rather than inside the macro: the
            // `tracing` macros bring their own `Value` trait into scope, so
            // `Value::as_str` there resolves to the wrong `Value`.
            let status = result
                .get("status")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("error");
            tracing::warn!(
                tenant = call.tenant,
                agent = agent_id,
                status,
                error = %error,
                "a2a node did not get an answer"
            );
        }
        result
    }

    #[cfg(test)]
    #[allow(clippy::unwrap_used, clippy::expect_used)]
    mod tests {
        use super::*;

        #[test]
        fn the_continuation_namespace_is_not_the_one_flow_authors_write_to() {
            // A flow author owns every key under the `state.set` namespace. If
            // this node shared it, an ordinary `state.set` could overwrite a
            // remote task reference — or read one out.
            assert_ne!(CONTINUATION_PREFIX, crate::storage::state::STATE_PREFIX);
        }

        #[test]
        fn the_key_is_per_session_so_two_sessions_never_share_a_remote_context() {
            assert_ne!(
                continuation_key("demo:web:c1:u1").as_str(),
                continuation_key("demo:web:c2:u1").as_str()
            );
        }

        /// With no store, no scope or no session there is nothing to read and
        /// nothing to write: the call is its own context and neither path may
        /// panic or error.
        #[test]
        fn without_a_session_there_is_no_continuation_and_nothing_is_stored() {
            let empty = load_continuations(None, None, None);
            assert!(empty.is_empty());
            // A no-op rather than a panic; there is no store to observe.
            save_continuations(None, None, None, &empty);
        }
    }
}
