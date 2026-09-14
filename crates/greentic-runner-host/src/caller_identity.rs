//! Where a verified caller enters the runtime, and why this is the only place
//! it may.
//!
//! [`greentic_aw_runtime::VerifiedCaller`] is what a tool eventually receives.
//! This module is the other end: it names the ONE position in an inbound
//! activity that may establish one, and refuses every other.
//!
//! ## The trust boundary is the provider, and that is not an accident of
//! implementation
//!
//! An inbound activity's payload is `serde_json::to_value(envelope)` where the
//! envelope is what the messaging PROVIDER COMPONENT produced from its
//! `ingest_http` op (`greentic-start`'s `revision_serve`). The provider is the
//! thing that verified the caller's bearer token, and it is WASM the operator
//! installed, so it sits inside the trust boundary.
//!
//! That is the whole basis for believing this block, and it is exactly as
//! strong as the provider — no more. `VerifiedCaller::user_verified` therefore
//! means "the provider vouched", never "this runtime proved it". A deployment
//! that does not trust its own provider cannot recover trust here.
//!
//! ## Why it is read from the envelope EXTENSIONS and not from the flow
//!
//! A `dw.agent` node receives the input its flow mapped for it, and a flow's
//! mapping is authorable. Reading identity from there would mean trusting
//! whatever the flow — and, one step further on, the model — put in it, which
//! is the failure this whole mechanism exists to end: a worker once sent
//! `sub: "user@example.com"`, `principals: ["employee"]` for a caller who had
//! sent neither.
//!
//! So the block is read ONCE, at the run's entry, out of the provider's own
//! envelope, and travels beside the flow's data on `FlowContext` — never
//! through it.
//!
//! ## Absent is not a failure
//!
//! A provider that stamps nothing yields `None`, which becomes an anonymous
//! caller downstream (`user_verified: false`). Every provider that predates
//! this contract is in that state, and so is every autonomous turn. That must
//! stay an ordinary answer rather than an error: a runtime that refused to run
//! an unauthenticated turn would break every existing deployment, and this
//! feature is about telling a TOOL what is known, not about gating ingress.

use serde_json::Value;

/// Envelope extension key a provider stamps a verified caller under.
///
/// Lives on `ChannelMessageEnvelope::extensions`, beside `channel_data` and
/// `rag`. A new key rather than a field on `Actor` because `Actor` is
/// `{id, kind}` in provider space — a Slack user id, a Webex person id — and
/// carries no notion of having been verified against anything.
pub const CALLER_EXT_KEY: &str = "caller";

/// The caller block a provider stamped on this activity payload, if any.
///
/// `payload` is the whole serialized envelope, so the block is looked for at
/// `extensions.caller` and nowhere else. In particular it is NOT read from the
/// payload root: a root-level `caller` would be indistinguishable from one an
/// upstream producer put in the activity by hand, and the extensions map is
/// what the provider component owns.
///
/// Returns the raw JSON rather than a typed value because this crate's core
/// cannot name [`greentic_aw_runtime::VerifiedCaller`] — that crate is optional
/// behind the `agentic-worker` feature, while `FlowContext` is not. The typed
/// parse happens at the agent node, which is already feature-gated.
pub fn caller_block(payload: &Value) -> Option<&Value> {
    payload
        .get("extensions")?
        .get(CALLER_EXT_KEY)
        .filter(|block| block.is_object())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn envelope_with(extensions: Value) -> Value {
        json!({ "id": "m-1", "text": "hello", "extensions": extensions })
    }

    #[test]
    fn a_provider_stamped_block_is_found() {
        let payload = envelope_with(json!({
            "caller": { "user_verified": true, "sub": "u-1@acme" }
        }));
        let block = caller_block(&payload).expect("block");
        assert_eq!(block["sub"], "u-1@acme");
    }

    /// Every provider predating this contract, and every autonomous turn.
    /// An ordinary answer, not an error.
    #[test]
    fn an_envelope_without_one_yields_none() {
        assert!(caller_block(&envelope_with(json!({}))).is_none());
        assert!(caller_block(&json!({ "text": "hello" })).is_none());
    }

    /// The payload ROOT is not a position a provider owns — a root-level key
    /// is indistinguishable from one an upstream producer added by hand, so it
    /// must not be mistaken for a verified block.
    #[test]
    fn a_root_level_caller_is_ignored() {
        let payload = json!({
            "text": "hello",
            "caller": { "user_verified": true, "sub": "root@evil" }
        });
        assert!(
            caller_block(&payload).is_none(),
            "only the provider-owned extensions map establishes a caller"
        );
    }

    /// A non-object under the key is not a caller. Passing a scalar or array
    /// through would hand the typed parse downstream something it would have
    /// to reject anyway, one layer further from where the shape is known.
    #[test]
    fn a_non_object_block_is_not_a_caller() {
        for bad in [json!("yes"), json!(true), json!(["a"]), json!(7)] {
            let payload = envelope_with(json!({ "caller": bad }));
            assert!(caller_block(&payload).is_none());
        }
    }
}
