//! The A2A agent card: what an agent publishes at
//! `/.well-known/agent-card.json` so another agent can address it.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// What an agent publishes about itself. Field names are the proto's, with
/// `rename_all = "camelCase"` producing the wire form.
///
/// Required by the spec: `name`, `description`, `supported_interfaces`,
/// `version`, `capabilities`, `default_input_modes`, `default_output_modes`,
/// `skills`. Everything else is optional and is modelled as such.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentCard {
    pub name: String,
    pub description: String,
    /// Ordered; the first entry is the agent's preferred interface.
    pub supported_interfaces: Vec<AgentInterface>,
    pub version: String,
    pub capabilities: AgentCapabilities,
    pub default_input_modes: Vec<String>,
    pub default_output_modes: Vec<String>,
    pub skills: Vec<AgentSkill>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<AgentProvider>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub documentation_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icon_url: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub security_schemes: BTreeMap<String, SecurityScheme>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub security_requirements: Vec<SecurityRequirement>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub signatures: Vec<AgentCardSignature>,
}

/// One address the agent answers on.
///
/// `tenant` is not decoration: when the server sets it, the client **MUST**
/// echo it in the `tenant` field of every request (spec §8.3.2). It is how one
/// endpoint serves many agents, and it is what our own `(tenant, team)` scoping
/// maps onto.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentInterface {
    pub url: String,
    /// `JSONRPC`, `GRPC` or `HTTP+JSON`. Open-form on purpose, so it is a
    /// `String` rather than an enum.
    pub protocol_binding: String,
    /// `Major.Minor` only — spec §3.6 says patch numbers MUST NOT be
    /// considered when a client and server negotiate.
    pub protocol_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentCapabilities {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub streaming: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub push_notifications: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extended_agent_card: Option<bool>,
}

/// An ability the agent claims. **There is no input schema here** — that is
/// the whole reason an A2A agent cannot become a typed LLM tool the way an MCP
/// server does, and why C3 resolves the schema from the author's contract
/// instead.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentSkill {
    pub id: String,
    pub name: String,
    pub description: String,
    pub tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub examples: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub input_modes: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub output_modes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentProvider {
    pub url: String,
    pub organization: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentCardSignature {
    pub protected: String,
    pub signature: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub header: Option<serde_json::Value>,
}

/// A named set of scheme names the caller must satisfy.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecurityRequirement {
    #[serde(default)]
    pub schemes: BTreeMap<String, Vec<String>>,
}

/// How to authenticate to an agent.
///
/// **This looks like OpenAPI and is not.** The proto models it as a `oneof`,
/// so the JSON carries **no `type` field** — the member name is the
/// discriminator. Writing `{"type":"http","scheme":"bearer"}` from OpenAPI
/// habit produces a card the protocol rejects.
///
/// Externally tagged on purpose: serde then requires the JSON object to carry
/// exactly one key, and that key to be a variant name. That is what makes the
/// OpenAPI-shaped `{"type":"http","scheme":"bearer"}` fail — `type` is not a
/// variant. No `deny_unknown_fields` is needed (and serde does not accept it
/// on an enum container anyway).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SecurityScheme {
    #[serde(rename = "apiKeySecurityScheme")]
    ApiKey(ApiKeySecurityScheme),
    #[serde(rename = "httpAuthSecurityScheme")]
    HttpAuth(HttpAuthSecurityScheme),
    #[serde(rename = "oauth2SecurityScheme")]
    OAuth2(serde_json::Value),
    #[serde(rename = "openIdConnectSecurityScheme")]
    OpenIdConnect(serde_json::Value),
    #[serde(rename = "mtlsSecurityScheme")]
    MutualTls(serde_json::Value),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiKeySecurityScheme {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// `query`, `header` or `cookie`.
    pub location: String,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HttpAuthSecurityScheme {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub scheme: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bearer_format: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A card in the shape a real agent serves, including the security-scheme
    /// trap: the JSON has NO `type` field — the member name discriminates.
    const CARD: &str = r#"{
      "name": "Recipe Agent",
      "description": "Helps with recipes and cooking.",
      "version": "1.0.0",
      "supportedInterfaces": [
        {
          "url": "https://api.example.com/a2a",
          "protocolBinding": "JSONRPC",
          "protocolVersion": "1.0",
          "tenant": "acme"
        }
      ],
      "capabilities": { "streaming": false, "pushNotifications": false },
      "defaultInputModes": ["text/plain"],
      "defaultOutputModes": ["text/plain"],
      "skills": [
        {
          "id": "suggest",
          "name": "Suggest a recipe",
          "description": "Given ingredients, suggests a dish.",
          "tags": ["cooking"],
          "examples": ["what can I make with eggs?"]
        }
      ],
      "securitySchemes": {
        "main": { "httpAuthSecurityScheme": { "scheme": "bearer", "bearerFormat": "gti_" } }
      }
    }"#;

    #[test]
    fn a_real_shaped_card_deserializes() {
        let card: AgentCard = serde_json::from_str(CARD).expect("card should parse");
        assert_eq!(card.name, "Recipe Agent");
        assert_eq!(card.version, "1.0.0");
        assert_eq!(card.supported_interfaces.len(), 1);
        assert_eq!(card.supported_interfaces[0].protocol_binding, "JSONRPC");
        assert_eq!(card.supported_interfaces[0].tenant.as_deref(), Some("acme"));
        assert_eq!(card.skills[0].id, "suggest");
        assert_eq!(card.capabilities.streaming, Some(false));
    }

    #[test]
    fn the_security_scheme_is_discriminated_by_member_name_not_a_type_field() {
        let card: AgentCard = serde_json::from_str(CARD).expect("card should parse");
        let scheme = card.security_schemes.get("main").expect("scheme present");
        match scheme {
            SecurityScheme::HttpAuth(http) => {
                assert_eq!(http.scheme, "bearer");
                assert_eq!(http.bearer_format.as_deref(), Some("gti_"));
            }
            other => panic!("expected httpAuthSecurityScheme, got {other:?}"),
        }
    }

    #[test]
    fn an_openapi_shaped_scheme_is_rejected_rather_than_silently_accepted() {
        // `{"type":"http","scheme":"bearer"}` is what OpenAPI habit produces.
        // A2A has no `type` field, so this must NOT parse — if it ever does,
        // we are accepting cards the real protocol rejects.
        let wrong = r#"{ "type": "http", "scheme": "bearer" }"#;
        assert!(serde_json::from_str::<SecurityScheme>(wrong).is_err());
    }

    #[test]
    fn a_card_round_trips_through_camel_case() {
        let card: AgentCard = serde_json::from_str(CARD).unwrap();
        let out = serde_json::to_string(&card).unwrap();
        assert!(
            out.contains("\"supportedInterfaces\""),
            "must emit camelCase"
        );
        assert!(
            !out.contains("supported_interfaces"),
            "must not emit snake_case"
        );
        let back: AgentCard = serde_json::from_str(&out).unwrap();
        assert_eq!(card, back);
    }

    #[test]
    fn no_struct_emits_a_snake_case_key() {
        let card: AgentCard = serde_json::from_str(CARD).unwrap();
        let value = serde_json::to_value(&card).expect("serialises");
        assert_eq!(
            crate::testutil::first_snake_case_key(&value),
            None,
            "a struct lost rename_all = \"camelCase\""
        );
    }
}
