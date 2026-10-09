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

/// `TenantCtx.attributes` key carrying the verified subject verbatim.
///
/// The typed `user` slot of `greentic_types::TenantCtx` only accepts an id made
/// of ASCII letters, digits, `.`, `-` and `_`, so a subject such as
/// `u-1@acme` cannot live there. The attribute always carries it unchanged.
pub const ATTR_CALLER_SUB: &str = "caller.sub";
/// `TenantCtx.attributes` key carrying the verified team.
pub const ATTR_CALLER_TEAM: &str = "caller.team";
/// `TenantCtx.attributes` key carrying the verified groups as a JSON array.
pub const ATTR_CALLER_GROUPS: &str = "caller.groups";
/// `TenantCtx.attributes` key carrying the verified role.
pub const ATTR_CALLER_ROLE: &str = "caller.role";
/// `TenantCtx.attributes` key present (value `"true"`) exactly when the
/// runtime is presenting a provider-verified caller.
pub const ATTR_CALLER_VERIFIED: &str = "caller.user_verified";

/// The verified caller as a `component.exec` invocation is told about it.
///
/// Built only from a block whose `user_verified` is `true`: an unverified or
/// undecodable block yields no caller at all, so a component never mistakes a
/// claim for a verified identity.
///
/// This is what the component SEES. It deliberately does not change the scope
/// the host itself uses for a component's secrets and state
/// (`ExecCtx.tenant.user` / `.team`): moving those to the caller's team would
/// make every tenant-wide secret unreachable for a verified caller, and moving
/// the state user would re-key existing component state.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ComponentCaller {
    pub sub: Option<String>,
    pub team: Option<String>,
    pub groups: Vec<String>,
    pub role: Option<String>,
}

/// Mirror of `greentic_aw_runtime::VerifiedCaller`'s wire shape, which this
/// crate's core cannot name (see [`caller_block`]).
#[derive(serde::Deserialize)]
struct CallerWire {
    #[serde(default)]
    user_verified: bool,
    #[serde(default)]
    sub: Option<String>,
    #[serde(default)]
    groups: Vec<String>,
    #[serde(default)]
    team: Option<String>,
    #[serde(default)]
    role: Option<String>,
}

fn non_empty(value: Option<String>) -> Option<String> {
    value
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

impl ComponentCaller {
    /// The caller a component may be told about, from the block the run entry
    /// established (`FlowContext::caller`).
    ///
    /// `None` for an anonymous turn, an unverified block, a block that does not
    /// decode (warned, same restrictive direction the agent node takes), and a
    /// verified block that names neither a subject nor a team.
    pub fn from_block(block: &Value) -> Option<Self> {
        let wire = match serde_json::from_value::<CallerWire>(block.clone()) {
            Ok(wire) => wire,
            Err(error) => {
                tracing::warn!(
                    %error,
                    "provider stamped a caller block this runtime cannot decode; \
                     the component invocation runs without a caller"
                );
                return None;
            }
        };
        if !wire.user_verified {
            return None;
        }
        let caller = Self {
            sub: non_empty(wire.sub),
            team: non_empty(wire.team),
            groups: wire
                .groups
                .into_iter()
                .map(|g| g.trim().to_string())
                .filter(|g| !g.is_empty())
                .collect(),
            role: non_empty(wire.role),
        };
        if caller.sub.is_none() && caller.team.is_none() {
            return None;
        }
        Some(caller)
    }

    /// The caller as `TenantCtx.attributes` entries, in a stable order.
    ///
    /// This is where `groups` and `role` travel: no component WIT world has a
    /// typed slot for them, and only the attribute map is free-form.
    pub fn attributes(&self) -> Vec<(String, String)> {
        let mut attrs = vec![(ATTR_CALLER_VERIFIED.to_string(), "true".to_string())];
        if let Some(sub) = &self.sub {
            attrs.push((ATTR_CALLER_SUB.to_string(), sub.clone()));
        }
        if let Some(team) = &self.team {
            attrs.push((ATTR_CALLER_TEAM.to_string(), team.clone()));
        }
        if !self.groups.is_empty() {
            attrs.push((
                ATTR_CALLER_GROUPS.to_string(),
                Value::from(self.groups.clone()).to_string(),
            ));
        }
        if let Some(role) = &self.role {
            attrs.push((ATTR_CALLER_ROLE.to_string(), role.clone()));
        }
        attrs
    }

    /// The same attributes, CBOR-encoded for a world that has no typed slot
    /// for them.
    ///
    /// # Why this exists
    ///
    /// `greentic:component@0.6.0`'s `tenant-ctx` carries ten identity fields
    /// and no free-form map — `attributes` was not argued out of the 0.6
    /// design, it was simply never listed when 0.6 was rewritten onto a
    /// minimal `types-core`. So a 0.6 component can be handed `user_id` and
    /// `team_id` and has nowhere to receive `role`, `groups` or the
    /// verification flag, while the identical 0.5 component receives all five.
    ///
    /// A partner hit this and worked around it by reading the raw
    /// `{{in}}.extensions.caller` out of the flow template instead — which
    /// works, but only for a node whose input they control, and not at all for
    /// a tool or a component someone else wrote.
    ///
    /// `invocation-envelope.metadata_cbor` is already in the published 0.6 WIT
    /// and the host has always sent `None`. Filling it needs no WIT change, no
    /// new world version and no component rebuild: a component that does not
    /// read the field is unaffected, and one that wants `caller.*` can have it
    /// without leaving 0.6. The alternative — a typed slot — means
    /// `types-core@0.6.1`, a new `component@0.6.1`, eight repositories moving
    /// in order and roughly forty components rebuilt, re-signed and
    /// republished, because a published OCI WIT tag is immutable and WIT
    /// records are structural.
    ///
    /// **The vocabulary is deliberately identical to [`Self::attributes`]** —
    /// same keys, same value encoding, `groups` as a JSON array string. Two
    /// spellings of one fact is how the two transports would drift, and the
    /// one that drifted would be the one nobody was looking at.
    ///
    /// `None` when there is nothing to say, so the envelope keeps the exact
    /// bytes it had before for every uncallered invocation.
    pub fn metadata_cbor(&self) -> Option<Vec<u8>> {
        let attrs = self.attributes();
        if attrs.is_empty() {
            return None;
        }
        let map: std::collections::BTreeMap<String, String> = attrs.into_iter().collect();
        serde_cbor::to_vec(&map).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_verified_block_becomes_a_component_caller() {
        let caller = ComponentCaller::from_block(&json!({
            "user_verified": true,
            "sub": "u-1@acme",
            "team": "sales",
            "groups": ["employee", " ", "admins"],
            "role": "member"
        }))
        .expect("caller");
        assert_eq!(caller.sub.as_deref(), Some("u-1@acme"));
        assert_eq!(caller.team.as_deref(), Some("sales"));
        assert_eq!(caller.groups, vec!["employee", "admins"]);
        assert_eq!(caller.role.as_deref(), Some("member"));
        assert_eq!(
            caller.attributes(),
            vec![
                ("caller.user_verified".to_string(), "true".to_string()),
                ("caller.sub".to_string(), "u-1@acme".to_string()),
                ("caller.team".to_string(), "sales".to_string()),
                (
                    "caller.groups".to_string(),
                    r#"["employee","admins"]"#.to_string()
                ),
                ("caller.role".to_string(), "member".to_string()),
            ]
        );
    }

    /// A claim is not an identity: only `user_verified: true` is presented.
    #[test]
    fn an_unverified_or_malformed_block_is_no_caller() {
        assert!(ComponentCaller::from_block(&json!({ "sub": "u-1@acme" })).is_none());
        assert!(
            ComponentCaller::from_block(&json!({ "user_verified": false, "sub": "u-1" })).is_none()
        );
        assert!(
            ComponentCaller::from_block(&json!({ "user_verified": "yes", "sub": "u-1" })).is_none()
        );
        assert!(ComponentCaller::from_block(&json!({ "user_verified": true })).is_none());
        assert!(
            ComponentCaller::from_block(&json!({ "user_verified": true, "sub": "  " })).is_none()
        );
    }

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
