use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// High-level activity payload exchanged with Greentic hosts.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Activity {
    #[serde(default)]
    pub(crate) kind: ActivityKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tenant: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pack_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    flow_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    flow_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    provider_id: Option<String>,
    /// Multi-instance messaging endpoint id (M1.4). Disambiguates provider
    /// instances of the same `provider_type` so sessions/traces partition
    /// per-endpoint. Producer-set; the runner threads it into
    /// `IngressEnvelope.messaging_endpoint_id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    messaging_endpoint_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    user_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    channel_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    conversation_id: Option<String>,
    /// M1.5 welcome-flow override hint. Producer-supplied — the runner uses
    /// it as a one-way override of the resolved `(pack_id, flow_id,
    /// flow_type)` when **all** of: a messaging endpoint is asserted AND
    /// this hint is present AND no active wait snapshot exists in this
    /// pack's session bucket. See [`WelcomeFlowHint`] for the contract —
    /// the **producer** decides when this is actually first contact; the
    /// runner-host only refuses to override on top of an active wait.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    welcome_flow_hint: Option<WelcomeFlowHint>,
    #[serde(default)]
    payload: Value,
}

/// M1.5 welcome-flow override hint: the `(pack_id, flow_id)` a producer wants
/// the runner to dispatch when this activity is the user's first contact on
/// the asserted messaging endpoint. Encodes both axes because welcome flows
/// can live in a different pack from the resolved one (e.g. greentic-start
/// reads the endpoint's `welcome_flow` from `Environment.messaging_endpoints`
/// and attaches it here).
///
/// # First-contact ownership is on the producer
///
/// The runner-host's first-contact probe checks `FlowResumeStore::fetch`,
/// which only finds **active wait snapshots**. A flow that completed (or
/// completed without ever calling `session.wait`) leaves NO marker, so a
/// post-completion turn would also see "no wait" and re-fire welcome. To
/// avoid welcome-loop behaviour the producer is responsible for **only
/// attaching this hint when the activity is actually first contact** — for
/// greentic-start that means consulting a durable welcome-seen marker
/// (planned follow-up) before attaching.
///
/// The runner-host's role is narrowly:
/// 1. Honour the producer's hint when no active wait exists (override
///    `(pack_id, flow_id, flow_type)` before the state machine runs).
/// 2. Refuse to override when an active wait exists, even if the hint is
///    present — that's the safety net against accidentally re-routing a
///    mid-conversation turn.
///
/// Attaching the hint on every turn is **NOT** safe under this contract —
/// it would route every no-wait turn through welcome.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct WelcomeFlowHint {
    pub pack_id: String,
    pub flow_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ActivityKind {
    /// Messaging-style activity (default).
    #[default]
    Message,
    /// Custom activity with user-specified action + optional flow type override.
    Custom {
        action: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        flow_type: Option<String>,
    },
}

impl Activity {
    /// Create a text messaging activity payload.
    pub fn text(text: impl Into<String>) -> Self {
        Self {
            kind: ActivityKind::Message,
            tenant: None,
            pack_id: None,
            flow_id: None,
            flow_type: Some("messaging".into()),
            session_id: None,
            provider_id: None,
            messaging_endpoint_id: None,
            user_id: None,
            channel_id: None,
            conversation_id: None,
            welcome_flow_hint: None,
            payload: json!({ "text": text.into() }),
        }
    }

    /// Build a custom activity with a raw payload body.
    pub fn custom(action: impl Into<String>, payload: Value) -> Self {
        Self {
            kind: ActivityKind::Custom {
                action: action.into(),
                flow_type: None,
            },
            tenant: None,
            pack_id: None,
            flow_id: None,
            flow_type: None,
            session_id: None,
            provider_id: None,
            messaging_endpoint_id: None,
            user_id: None,
            channel_id: None,
            conversation_id: None,
            welcome_flow_hint: None,
            payload,
        }
    }

    /// Attach the M1.5 welcome-flow override hint. See [`WelcomeFlowHint`]
    /// for the contract — the **producer** is responsible for only calling
    /// this when the activity is actually first contact on the asserted
    /// messaging endpoint. Attaching on every turn would route every
    /// no-active-wait turn through the welcome flow.
    pub fn with_welcome_flow_hint(mut self, hint: WelcomeFlowHint) -> Self {
        self.welcome_flow_hint = Some(hint);
        self
    }

    /// Return the welcome-flow override hint, if any.
    pub fn welcome_flow_hint(&self) -> Option<&WelcomeFlowHint> {
        self.welcome_flow_hint.as_ref()
    }

    /// Attach a tenant identifier to the activity.
    pub fn with_tenant(mut self, tenant: impl Into<String>) -> Self {
        self.tenant = Some(tenant.into());
        self
    }

    /// Target a specific flow identifier.
    pub fn with_flow(mut self, flow_id: impl Into<String>) -> Self {
        self.flow_id = Some(flow_id.into());
        self
    }

    /// Target a specific pack identifier.
    pub fn with_pack(mut self, pack_id: impl Into<String>) -> Self {
        self.pack_id = Some(pack_id.into());
        self
    }

    /// Hint which flow type should handle the activity.
    pub fn with_flow_type(mut self, flow_type: impl Into<String>) -> Self {
        let flow_type = flow_type.into();
        self.flow_type = Some(flow_type.clone());
        if let ActivityKind::Custom {
            flow_type: inner, ..
        } = &mut self.kind
        {
            *inner = Some(flow_type);
        }
        self
    }

    /// Merge a namespaced field into the activity payload. The only caller is the
    /// fast2flow dispatch path (feature `greentic-x-provider`), so gate the helper
    /// to that feature — this keeps it (and its unit test) out of builds that
    /// don't compile fast2flow, avoiding a dead-code warning there.
    #[cfg(feature = "greentic-x-provider")]
    pub(crate) fn with_payload_field(mut self, key: impl Into<String>, value: Value) -> Self {
        match &mut self.payload {
            Value::Object(object) => {
                object.insert(key.into(), value);
            }
            existing => {
                let original = std::mem::replace(existing, Value::Null);
                *existing = json!({
                    key.into(): value,
                    "value": original,
                });
            }
        }
        self
    }

    /// Replace the payload body, keeping the activity's kind and routing. The
    /// agent chat ingress uses it to carry a card submit (the answer to a
    /// parked `flow:` tool) as a message activity.
    pub fn with_payload(mut self, payload: Value) -> Self {
        self.payload = payload;
        self
    }

    /// Attach a session identifier used for retries/idempotency.
    pub fn with_session(mut self, session_id: impl Into<String>) -> Self {
        self.session_id = Some(session_id.into());
        self
    }

    /// Attach a provider identifier for telemetry scoping.
    pub fn with_provider(mut self, provider: impl Into<String>) -> Self {
        self.provider_id = Some(provider.into());
        self
    }

    /// Attach the receiving messaging endpoint id (M1.4). Distinguishes
    /// provider instances of the same `provider_type` (e.g. `teams-legal`
    /// vs `teams-accounting`); the runner threads it into the envelope so
    /// session keys and telemetry partition per-endpoint.
    pub fn with_messaging_endpoint(mut self, endpoint_id: impl Into<String>) -> Self {
        self.messaging_endpoint_id = Some(endpoint_id.into());
        self
    }

    /// Attach the originating user for messaging activities.
    pub fn from_user(mut self, user: impl Into<String>) -> Self {
        self.user_id = Some(user.into());
        self
    }

    /// Attach a channel identifier (chat, room, or queue) for canonical session keys.
    pub fn in_channel(mut self, channel: impl Into<String>) -> Self {
        self.channel_id = Some(channel.into());
        self
    }

    /// Attach a conversation/thread identifier for canonical session keys.
    pub fn in_conversation(mut self, conversation: impl Into<String>) -> Self {
        self.conversation_id = Some(conversation.into());
        self
    }

    /// Return the resolved tenant identifier, if any.
    pub fn tenant(&self) -> Option<&str> {
        self.tenant.as_deref()
    }

    /// Return the resolved pack identifier, if any.
    pub fn pack_id(&self) -> Option<&str> {
        self.pack_id.as_deref()
    }

    /// Return the resolved flow identifier hint.
    pub fn flow_id(&self) -> Option<&str> {
        self.flow_id.as_deref()
    }

    /// Return the resolved flow type hint.
    pub fn flow_type(&self) -> Option<&str> {
        self.flow_type
            .as_deref()
            .or_else(|| self.kind.flow_type_hint())
    }

    /// Return the originating session identifier, if supplied.
    pub fn session_id(&self) -> Option<&str> {
        self.session_id.as_deref()
    }

    /// Return the originating provider identifier, if supplied.
    pub fn provider_id(&self) -> Option<&str> {
        self.provider_id.as_deref()
    }

    /// Return the receiving messaging endpoint id (M1.4), if supplied.
    pub fn messaging_endpoint_id(&self) -> Option<&str> {
        self.messaging_endpoint_id.as_deref()
    }

    /// Return the originating user identifier, if supplied.
    pub fn user(&self) -> Option<&str> {
        self.user_id.as_deref()
    }

    /// Return the channel identifier, if supplied.
    pub fn channel(&self) -> Option<&str> {
        self.channel_id.as_deref()
    }

    /// Return the conversation identifier, if supplied.
    pub fn conversation(&self) -> Option<&str> {
        self.conversation_id.as_deref()
    }

    /// Underlying payload body.
    pub fn payload(&self) -> &Value {
        &self.payload
    }

    pub(crate) fn action(&self) -> Option<&str> {
        self.kind.action_hint()
    }

    pub(crate) fn into_payload(self) -> Value {
        self.payload
    }

    pub(crate) fn ensure_tenant(mut self, tenant: &str) -> Self {
        if self.tenant.is_none() {
            self.tenant = Some(tenant.to_string());
        }
        self
    }

    pub(crate) fn from_output(payload: Value, tenant: &str) -> Self {
        Activity::custom("response", payload).ensure_tenant(tenant)
    }
}

impl ActivityKind {
    fn flow_type_hint(&self) -> Option<&str> {
        match self {
            ActivityKind::Message => Some("messaging"),
            ActivityKind::Custom { flow_type, .. } => flow_type.as_deref(),
        }
    }

    fn action_hint(&self) -> Option<&str> {
        match self {
            ActivityKind::Message => Some("messaging"),
            ActivityKind::Custom { action, .. } => Some(action.as_str()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn with_messaging_endpoint_sets_field() {
        let activity = Activity::text("hi").with_messaging_endpoint("teams-legal");
        assert_eq!(activity.messaging_endpoint_id(), Some("teams-legal"));
    }

    #[cfg(feature = "greentic-x-provider")]
    #[test]
    fn with_payload_field_adds_field_to_object_payload() {
        let activity = Activity::text("show traffic")
            .with_payload_field("fast2flow", json!({"entities": [{"kind": "date"}]}));

        assert_eq!(
            activity.payload()["fast2flow"]["entities"][0]["kind"],
            "date"
        );
        assert_eq!(activity.payload()["text"], "show traffic");
    }

    #[test]
    fn messaging_endpoint_id_defaults_to_none() {
        let activity = Activity::text("hi");
        assert!(activity.messaging_endpoint_id().is_none());
    }

    #[test]
    fn messaging_endpoint_id_round_trips_through_serde() {
        let original = Activity::text("hi").with_messaging_endpoint("teams-legal");
        let encoded = serde_json::to_string(&original).expect("serialize");
        assert!(encoded.contains("\"messaging_endpoint_id\":\"teams-legal\""));
        let decoded: Activity = serde_json::from_str(&encoded).expect("deserialize");
        assert_eq!(decoded.messaging_endpoint_id(), Some("teams-legal"));
    }

    #[test]
    fn messaging_endpoint_id_serde_skips_when_unset() {
        // Combined wire-compat + skip-if-none proof: an Activity without
        // the field omits it on the wire AND decodes back with the field
        // unset, so pre-M1.4 producers/consumers interop cleanly.
        let encoded = serde_json::to_string(&Activity::text("hi")).expect("serialize");
        assert!(!encoded.contains("messaging_endpoint_id"));
        let decoded: Activity = serde_json::from_str(&encoded).expect("deserialize");
        assert!(decoded.messaging_endpoint_id().is_none());
    }
}
