//! The A2A agent card: what an agent publishes at
//! `/.well-known/agent-card.json` so another agent can address it.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// What an agent publishes about itself. Field names are the proto's, with
/// `rename_all = "camelCase"` producing the wire form.
///
/// `name`, `description`, `supported_interfaces`, `version`, `capabilities`,
/// `default_input_modes`, `default_output_modes` and `skills` are required —
/// but the proto says so with `google.api.field_behavior`, an annotation
/// **proto3 JSON does not enforce.** A field holding its default (an empty
/// string, an empty list, an unset message) is simply OMITTED from the JSON a
/// conformant proto3 serialiser emits. So a real agent with no skills, or with
/// every capability false, serves a card missing those keys, and a struct that
/// demands them rejects it.
///
/// Every field but `name` therefore carries `#[serde(default)]`. `name` is the
/// one whose emptiness means "this is not an agent card at all" — keeping it
/// required is what stops an unrelated JSON document (an error body, another
/// API's payload) parsing into an empty card that then looks usable.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentCard {
    pub name: String,
    #[serde(default)]
    pub description: String,
    /// Ordered; the first entry is the agent's preferred interface.
    #[serde(default)]
    pub supported_interfaces: Vec<AgentInterface>,
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub capabilities: AgentCapabilities,
    #[serde(default)]
    pub default_input_modes: Vec<String>,
    #[serde(default)]
    pub default_output_modes: Vec<String>,
    #[serde(default)]
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
    #[serde(default)]
    pub url: String,
    /// `JSONRPC`, `GRPC` or `HTTP+JSON`. Open-form on purpose, so it is a
    /// `String` rather than an enum.
    #[serde(default)]
    pub protocol_binding: String,
    /// `Major.Minor` only — spec §3.6 says patch numbers MUST NOT be
    /// considered when a client and server negotiate.
    #[serde(default)]
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
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
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
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub organization: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentCardSignature {
    #[serde(default)]
    pub protected: String,
    #[serde(default)]
    pub signature: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub header: Option<serde_json::Value>,
}

/// A named set of scheme names the caller must satisfy, each with the scopes
/// it requires.
///
/// **The proto wraps the scopes in a message, and the obvious JSON does not.**
/// `SecurityRequirement` is `map<string, StringList> schemes`, and `StringList`
/// is `{ repeated string list }`, so the spec form is
///
/// ```json
/// { "schemes": { "bearer": { "list": ["read"] } } }
/// ```
///
/// — the scopes arrive under a `list` key, not as a bare array. Writing the
/// map value as a bare `["read"]` is what OpenAPI habit produces, and it is
/// the only shape this type accepted until now.
///
/// Both are read onto the one representation below; serialisation emits the
/// spec form. That matters more than a field nothing in this workspace reads
/// would suggest: `serde` fails the WHOLE [`AgentCard`] on one unreadable
/// field, and a card that fails to parse takes every tool that agent offers
/// with it — the agent simply disappears from the worker's tool list, with a
/// warn line as the only signal.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SecurityRequirement {
    pub schemes: BTreeMap<String, Vec<String>>,
}

/// The scopes for one scheme, in either shape.
#[derive(Deserialize)]
#[serde(untagged)]
enum ScopeList {
    /// The spec form, `{"list": ["read"]}`. Tried first; a bare array cannot
    /// match a struct variant, so the ordering costs the other arm nothing.
    Wrapped {
        #[serde(default)]
        list: Vec<String>,
    },
    /// The OpenAPI-shaped form, `["read"]`.
    Bare(Vec<String>),
}

impl From<ScopeList> for Vec<String> {
    fn from(value: ScopeList) -> Self {
        match value {
            ScopeList::Wrapped { list } | ScopeList::Bare(list) => list,
        }
    }
}

/// A whole requirement, in either shape.
#[derive(Deserialize)]
#[serde(untagged)]
enum RequirementShape {
    /// `{"schemes": {…}}` — the proto's own field name around the map.
    Wrapped {
        schemes: BTreeMap<String, ScopeList>,
    },
    /// `{…}` — the map inlined, which is the 0.x `security` shape. Reached
    /// only when the object carries no `schemes` key that reads as a scheme
    /// map, so the two cannot be confused; a scheme genuinely NAMED `schemes`
    /// still lands here, because the wrapped arm fails on its value first.
    Flat(BTreeMap<String, ScopeList>),
}

impl<'de> Deserialize<'de> for SecurityRequirement {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let schemes = match RequirementShape::deserialize(deserializer)? {
            RequirementShape::Wrapped { schemes } | RequirementShape::Flat(schemes) => schemes,
        };
        Ok(Self {
            schemes: schemes
                .into_iter()
                .map(|(name, scopes)| (name, scopes.into()))
                .collect(),
        })
    }
}

impl Serialize for SecurityRequirement {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Scopes<'a> {
            list: &'a [String],
        }
        #[derive(Serialize)]
        struct Wire<'a> {
            schemes: BTreeMap<&'a str, Scopes<'a>>,
        }
        Wire {
            schemes: self
                .schemes
                .iter()
                .map(|(name, scopes)| (name.as_str(), Scopes { list: scopes }))
                .collect(),
        }
        .serialize(serializer)
    }
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
    /// Any scheme this crate does not model. Kept rather than rejected: a
    /// scheme we do not use must not make the whole card — and therefore the
    /// whole agent — unreachable.
    #[serde(untagged)]
    Other(serde_json::Value),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiKeySecurityScheme {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// `query`, `header` or `cookie`.
    ///
    /// The proto calls this `location`, so `location` is what we emit. OpenAPI
    /// — which this scheme is modelled on, and which the 0.x JSON schema
    /// followed — calls it `in`, and that spelling is read as an alias. It
    /// costs one attribute and the alternative is not a parse failure but a
    /// silently EMPTY location, which would read as "no location was stated"
    /// on a card that stated one.
    #[serde(default, alias = "in")]
    pub location: String,
    #[serde(default)]
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HttpAuthSecurityScheme {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default)]
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
    fn an_openapi_shaped_scheme_lands_in_other_never_in_http_auth() {
        // `{"type":"http","scheme":"bearer"}` is what OpenAPI habit produces.
        // A2A has no `type` field, so this must NOT be mistaken for a real
        // `httpAuthSecurityScheme` — that mistake would authenticate wrong,
        // silently. It is preserved in the `Other` catch-all (Important 5)
        // rather than rejected outright, so ONE unrecognised scheme does not
        // make the whole card — and therefore the whole agent — unreachable.
        let wrong = r#"{ "type": "http", "scheme": "bearer" }"#;
        match serde_json::from_str::<SecurityScheme>(wrong).expect("kept, not rejected") {
            SecurityScheme::Other(_) => {}
            other => panic!("must not be mistaken for a real scheme, got {other:?}"),
        }
    }

    #[test]
    fn a_scheme_whose_body_is_the_wrong_shape_is_kept_rather_than_failing_the_card() {
        // External tagging picks the variant by member name and then parses
        // the body. A body that is not an object cannot be that variant — and
        // the `Other` catch-all is what keeps that from failing the whole
        // card, which would take every one of the agent's tools with it.
        let wrong = r#"{ "httpAuthSecurityScheme": "bearer" }"#;
        match serde_json::from_str::<SecurityScheme>(wrong).expect("kept, not rejected") {
            SecurityScheme::Other(value) => assert_eq!(value["httpAuthSecurityScheme"], "bearer"),
            other => panic!("expected the catch-all, got {other:?}"),
        }
    }

    #[test]
    fn a_scheme_stating_only_its_member_name_reads_as_that_scheme() {
        // proto3 JSON omits a field holding its default, so a `oneof` member
        // whose every field is empty serialises as a bare `{}`. Demanding the
        // REQUIRED fields would push such a scheme into the catch-all, losing
        // which scheme it is.
        let json = r#"{ "httpAuthSecurityScheme": {} }"#;
        match serde_json::from_str::<SecurityScheme>(json).expect("parses") {
            SecurityScheme::HttpAuth(http) => assert_eq!(http.scheme, ""),
            other => panic!("expected httpAuthSecurityScheme, got {other:?}"),
        }
    }

    #[test]
    fn the_openapi_spelling_of_the_api_key_location_is_read_as_location() {
        // The proto says `location`; OpenAPI and the 0.x JSON schema say `in`.
        // Without the alias this parses with an EMPTY location rather than
        // failing — a card that stated one reading as if it had not.
        let json = r#"{ "apiKeySecurityScheme": { "in": "header", "name": "X-Key" } }"#;
        match serde_json::from_str::<SecurityScheme>(json).expect("parses") {
            SecurityScheme::ApiKey(key) => {
                assert_eq!(key.location, "header");
                assert_eq!(key.name, "X-Key");
            }
            other => panic!("expected apiKeySecurityScheme, got {other:?}"),
        }
    }

    #[test]
    fn a_card_with_an_unknown_security_scheme_still_parses() {
        // The motivating scenario for Important 5: a single future auth
        // scheme this crate does not model must not make the whole card —
        // and therefore the whole agent — unreachable.
        let json = r#"{
          "main": { "pasetoSecurityScheme": { "version": "v4" } }
        }"#;
        let schemes: BTreeMap<String, SecurityScheme> = serde_json::from_str(json).unwrap();
        match schemes.get("main").expect("scheme present") {
            SecurityScheme::Other(value) => {
                assert_eq!(value["pasetoSecurityScheme"]["version"], "v4");
            }
            other => panic!("expected the unknown scheme to land in Other, got {other:?}"),
        }
    }

    // --- securityRequirements: two shapes, one meaning ---------------------

    /// The proto's own shape: `map<string, StringList> schemes`, where
    /// `StringList` is `{ repeated string list }`.
    const REQUIREMENT_SPEC_FORM: &str = r#"{
      "schemes": {
        "bearer": { "list": ["read", "write"] },
        "mtls": { "list": [] }
      }
    }"#;

    /// What OpenAPI habit produces, and the only shape this crate read before.
    const REQUIREMENT_BARE_FORM: &str = r#"{
      "schemes": {
        "bearer": ["read", "write"],
        "mtls": []
      }
    }"#;

    #[test]
    fn the_spec_form_and_the_bare_form_parse_to_the_same_requirement() {
        let spec: SecurityRequirement =
            serde_json::from_str(REQUIREMENT_SPEC_FORM).expect("the spec form must parse");
        let bare: SecurityRequirement =
            serde_json::from_str(REQUIREMENT_BARE_FORM).expect("the bare form must parse");

        assert_eq!(spec, bare, "the two shapes carry the same requirement");
        assert_eq!(
            spec.schemes.get("bearer").map(Vec::as_slice),
            Some(["read".to_string(), "write".to_string()].as_slice()),
            "the scopes must survive the unwrapping, not just the scheme names"
        );
        assert_eq!(
            spec.schemes.get("mtls").map(Vec::as_slice),
            Some([].as_slice()),
            "a scheme requiring no scopes is still a scheme the caller must satisfy"
        );
    }

    #[test]
    fn a_requirement_serializes_in_the_spec_form_and_round_trips() {
        let requirement: SecurityRequirement =
            serde_json::from_str(REQUIREMENT_BARE_FORM).expect("parses");
        let value = serde_json::to_value(&requirement).expect("serialises");

        assert_eq!(
            value,
            serde_json::json!({
              "schemes": {
                "bearer": { "list": ["read", "write"] },
                "mtls": { "list": [] }
              }
            }),
            "we emit the proto's shape, scopes wrapped in `list`"
        );

        let back: SecurityRequirement = serde_json::from_value(value).expect("re-parses");
        assert_eq!(requirement, back);
    }

    #[test]
    fn the_0_x_shape_with_the_map_inlined_still_parses() {
        // `{"bearer": []}` with no `schemes` wrapper is the 0.x `security`
        // shape. It parsed before only because the unknown key was ignored
        // and `schemes` defaulted to empty — i.e. it silently lost every
        // requirement rather than failing.
        let flat: SecurityRequirement =
            serde_json::from_str(r#"{ "bearer": { "list": ["read"] }, "apiKey": [] }"#)
                .expect("parses");
        assert_eq!(
            flat.schemes.get("bearer").map(Vec::as_slice),
            Some(["read".to_string()].as_slice())
        );
        assert!(flat.schemes.contains_key("apiKey"));
    }

    #[test]
    fn a_card_whose_requirements_use_the_spec_form_parses_whole() {
        // The regression this fix exists for: `serde` fails the WHOLE card on
        // one unreadable field, so a spec-shaped `securityRequirements` took
        // the name, the interfaces and every skill down with it.
        let json = CARD.replace(
            "\"securitySchemes\": {",
            "\"securityRequirements\": [{ \"schemes\": { \"main\": { \"list\": [\"read\"] } } }],\n      \"securitySchemes\": {",
        );
        let card: AgentCard = serde_json::from_str(&json).expect("the spec form must not fail");

        assert_eq!(card.name, "Recipe Agent", "the rest of the card survives");
        assert_eq!(card.skills.len(), 1);
        assert_eq!(
            card.security_requirements[0]
                .schemes
                .get("main")
                .map(Vec::as_slice),
            Some(["read".to_string()].as_slice())
        );
    }

    // --- tolerance ---------------------------------------------------------

    #[test]
    fn unknown_fields_anywhere_in_a_card_are_ignored_not_fatal() {
        let json = r#"{
          "name": "Recipe Agent",
          "description": "Helps with recipes and cooking.",
          "version": "1.0.0",
          "protocolVersion": "1.0",
          "preferredTransport": "JSONRPC",
          "supportedInterfaces": [
            {
              "url": "https://api.example.com/a2a",
              "protocolBinding": "JSONRPC",
              "protocolVersion": "1.0",
              "somethingNewInTheNextMinorVersion": { "a": 1 }
            }
          ],
          "capabilities": {
            "streaming": true,
            "extensions": [{ "uri": "https://example.com/ext", "required": false }]
          },
          "defaultInputModes": ["text/plain"],
          "defaultOutputModes": ["text/plain"],
          "skills": [
            {
              "id": "suggest",
              "name": "Suggest a recipe",
              "description": "Suggests a dish.",
              "tags": ["cooking"],
              "securityRequirements": [{ "schemes": { "main": { "list": [] } } }]
            }
          ]
        }"#;
        let card: AgentCard = serde_json::from_str(json).expect("unknown fields must be ignored");
        assert_eq!(card.capabilities.streaming, Some(true));
        assert_eq!(card.skills[0].id, "suggest");
    }

    #[test]
    fn a_card_omitting_every_proto3_default_field_still_parses() {
        // proto3 JSON omits a field holding its default, and `field_behavior
        // = REQUIRED` does not change that. An agent with no skills, no
        // declared modes and all-false capabilities emits exactly this.
        let card: AgentCard =
            serde_json::from_str(r#"{ "name": "Minimal Agent" }"#).expect("must parse");
        assert_eq!(card.name, "Minimal Agent");
        assert!(card.skills.is_empty());
        assert!(card.supported_interfaces.is_empty());
        assert_eq!(card.capabilities, AgentCapabilities::default());
    }

    #[test]
    fn a_document_that_is_not_a_card_at_all_is_still_refused() {
        // `name` stays required precisely so this does not parse into an
        // empty card that then reads as a usable agent.
        assert!(serde_json::from_str::<AgentCard>("{}").is_err());
        assert!(serde_json::from_str::<AgentCard>(r#"{ "error": "not found" }"#).is_err());
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
        // Built in Rust with every field populated, including the five
        // `skip_serializing_if` fields `CARD` above never sets
        // (`provider`, `documentationUrl`, `iconUrl`, `securityRequirements`,
        // `signatures`) — those never reach the walker when they are absent,
        // so parsing the partial `CARD` constant would leave them unchecked.
        let card = AgentCard {
            name: "Recipe Agent".into(),
            description: "Helps with recipes and cooking.".into(),
            supported_interfaces: vec![AgentInterface {
                url: "https://api.example.com/a2a".into(),
                protocol_binding: "JSONRPC".into(),
                protocol_version: "1.0".into(),
                tenant: Some("acme".into()),
            }],
            version: "1.0.0".into(),
            capabilities: AgentCapabilities {
                streaming: Some(false),
                push_notifications: Some(false),
                extended_agent_card: Some(false),
            },
            default_input_modes: vec!["text/plain".into()],
            default_output_modes: vec!["text/plain".into()],
            skills: vec![AgentSkill {
                id: "suggest".into(),
                name: "Suggest a recipe".into(),
                description: "Given ingredients, suggests a dish.".into(),
                tags: vec!["cooking".into()],
                examples: vec!["what can I make with eggs?".into()],
                input_modes: vec!["text/plain".into()],
                output_modes: vec!["text/plain".into()],
            }],
            provider: Some(AgentProvider {
                url: "https://example.com".into(),
                organization: "Example Org".into(),
            }),
            documentation_url: Some("https://example.com/docs".into()),
            icon_url: Some("https://example.com/icon.png".into()),
            security_schemes: BTreeMap::from([(
                "main".to_string(),
                SecurityScheme::HttpAuth(HttpAuthSecurityScheme {
                    description: Some("bearer auth".into()),
                    scheme: "bearer".into(),
                    bearer_format: Some("gti_".into()),
                }),
            )]),
            security_requirements: vec![SecurityRequirement {
                schemes: BTreeMap::from([("main".to_string(), vec!["read".to_string()])]),
            }],
            signatures: vec![AgentCardSignature {
                protected: "protected-header".into(),
                signature: "signature-bytes".into(),
                header: Some(serde_json::json!({"kid": "1"})),
            }],
        };
        let value = serde_json::to_value(&card).expect("serialises");
        assert_eq!(
            crate::testutil::first_snake_case_key(&value),
            None,
            "a struct lost rename_all = \"camelCase\""
        );
    }
}
