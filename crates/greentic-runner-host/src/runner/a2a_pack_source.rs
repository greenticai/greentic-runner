//! Build the agent loop's [`greentic_aw_runtime::A2aToolSource`] from the
//! `assets/a2a-routes.json` sidecars of the loaded packs (cross-repo contract
//! 2026-09-21 §5).
//!
//! It lives beside `agent_node::aw::mcp_source_from_packs` in spirit but in its
//! own file, because `agent_node.rs` is already over 4000 lines. Unlike MCP
//! there is no admin source to prefer: the pack is the only A2A source, so
//! there is no `or_else`.

use std::collections::HashSet;
use std::sync::Arc;

/// The A2A tool source for one runtime, or `None`.
///
/// - `GREENTIC_AW_A2A=0` disables it, as `GREENTIC_AW_MCP=0` does for MCP. An
///   operator who disabled outbound A2A must not have it re-enabled by a pack.
/// - Agent ids are deduplicated across packs, first pack wins. `packs` is one
///   revision's pack list (the runtime is built per deployed unit), so the
///   dedup never mixes units.
/// - `tenant` and `unit` are captured for the credential read; `unit` is the
///   revision's `bundle_id` (`None` on the legacy tenant-only runtime).
/// - `None` when disabled, when no pack is loaded, or when no pack declares an
///   agent. `a2a:` refs are then dropped from the LLM tool list and reported
///   by the preflight check.
/// - A failure to build the HTTP client is a `warn` and `None`, never a panic:
///   one missing tool source must not take the worker down.
pub(crate) fn a2a_source_from_packs(
    packs: &[Arc<crate::pack::PackRuntime>],
    tenant: &str,
    secrets: Option<crate::secrets::DynSecretsManager>,
    unit: Option<&str>,
) -> Option<Arc<greentic_aw_runtime::A2aToolSource>> {
    if std::env::var("GREENTIC_AW_A2A").ok().as_deref() == Some("0") {
        tracing::info!("GREENTIC_AW_A2A=0; pack-backed A2A tool source disabled");
        return None;
    }
    if packs.is_empty() {
        return None;
    }

    let mut seen = HashSet::new();
    let mut routes = Vec::new();
    for pack in packs {
        let Some(declared) = pack.a2a_routes() else {
            continue;
        };
        for route in declared.iter() {
            if !seen.insert(route.agent_id.clone()) {
                continue;
            }
            routes.push(greentic_aw_runtime::A2aRoute {
                agent_id: route.agent_id.clone(),
                base_url: route.base_url.clone(),
                auth_header_name: route.auth_header_name.clone(),
                auth_team: route.auth_team.clone(),
                requires_auth: route.requires_auth,
            });
        }
    }
    if routes.is_empty() {
        return None;
    }

    let agents = routes.len();
    match greentic_aw_runtime::A2aToolSource::from_routes(
        routes,
        secrets,
        tenant,
        unit.map(str::to_string),
    ) {
        Ok(source) => {
            tracing::info!(tenant = %tenant, agents, "pack-backed A2A tool source constructed");
            Some(Arc::new(source))
        }
        Err(error) => {
            tracing::warn!(
                tenant = %tenant,
                error = %error,
                "A2A tool source could not be built; a2a: tools are unavailable"
            );
            None
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::collections::HashMap;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use greentic_aw_runtime::cost::MockTokenMeter;
    use greentic_aw_runtime::error::{LlmError, TerminationReason};
    use greentic_aw_runtime::llm::{LlmBackend, LlmRequest, LlmResponse};
    use greentic_aw_runtime::mock::{
        MockAgentStateStore, MockConfigProvider, MockTelemetry, NoopToolLedger,
    };
    use greentic_aw_runtime::state::ToolCallRecord;
    use greentic_aw_runtime::{
        AgentConfig, AgentInput, AgentLimits, AgentRuntime, AgentStep, LlmProviderRef,
        TenantContext, ToolRef,
    };
    use serde_json::json;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::a2a_source_from_packs;

    /// A hyphenated id, as the admin mints them: the secret name must use it
    /// verbatim.
    const AGENT_ID: &str = "8d2f0c1e-recipe";

    /// A real-shaped agent card; `DESCRIPTION` and `INTERFACE` are replaced.
    const CARD: &str = r#"{
    "name": "Recipe Agent",
    "description": "DESCRIPTION",
    "version": "1.0.0",
    "supportedInterfaces": [
      { "url": "INTERFACE", "protocolBinding": "JSONRPC", "protocolVersion": "1.0" }
    ],
    "capabilities": { "streaming": false, "pushNotifications": false },
    "defaultInputModes": ["text/plain"],
    "defaultOutputModes": ["text/plain"],
    "skills": []
  }"#;

    async fn serve_agent(server: &MockServer, description: &str, reply: &str) {
        let card = CARD
            .replace("DESCRIPTION", description)
            .replace("INTERFACE", &format!("{}/a2a", server.uri()));
        Mock::given(method("GET"))
            .and(path("/.well-known/agent-card.json"))
            .respond_with(ResponseTemplate::new(200).set_body_string(card))
            .mount(server)
            .await;
        Mock::given(method("POST"))
            .and(path("/a2a"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0", "id": 1,
                "result": {"message": {"messageId": "m-2", "role": "ROLE_AGENT",
                                       "parts": [{"text": reply}]}}
            })))
            .mount(server)
            .await;
    }

    /// A pack directory carrying `sidecar` as `assets/a2a-routes.json`.
    fn pack_with_sidecar(sidecar: &str) -> (tempfile::TempDir, Arc<crate::pack::PackRuntime>) {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("assets")).unwrap();
        std::fs::write(dir.path().join("assets/a2a-routes.json"), sidecar).unwrap();
        let pack = Arc::new(crate::pack::tests::pack_runtime_for_dir(dir.path()));
        (dir, pack)
    }

    struct MapSecrets(HashMap<String, Vec<u8>>);

    #[async_trait::async_trait]
    impl greentic_secrets_lib::SecretsManager for MapSecrets {
        async fn read(&self, path: &str) -> greentic_secrets_lib::Result<Vec<u8>> {
            self.0
                .get(path)
                .cloned()
                .ok_or_else(|| greentic_secrets_lib::SecretError::NotFound(path.to_string()))
        }
        async fn write(&self, _: &str, _: &[u8]) -> greentic_secrets_lib::Result<()> {
            Ok(())
        }
        async fn delete(&self, _: &str) -> greentic_secrets_lib::Result<()> {
            Ok(())
        }
    }

    fn secrets(pairs: &[(&str, &str)]) -> crate::secrets::DynSecretsManager {
        Arc::new(MapSecrets(
            pairs
                .iter()
                .map(|(k, v)| ((*k).to_string(), v.as_bytes().to_vec()))
                .collect(),
        ))
    }

    #[allow(unsafe_code)]
    fn clear_gate() {
        // SAFETY: every caller is #[serial] (crate convention).
        unsafe {
            std::env::remove_var("GREENTIC_AW_A2A");
        }
    }

    #[test]
    #[serial_test::serial]
    #[allow(unsafe_code)]
    fn the_env_gate_disables_even_a_pack_that_carries_routes() {
        let (_dir, pack) =
            pack_with_sidecar(r#"[{"agent_id":"a","base_url":"https://a.example"}]"#);
        // SAFETY: #[serial] serializes env-mutating tests (crate convention).
        unsafe {
            std::env::set_var("GREENTIC_AW_A2A", "0");
        }
        let gated = a2a_source_from_packs(std::slice::from_ref(&pack), "acme", None, None);
        clear_gate();
        assert!(
            gated.is_none(),
            "an operator who disabled outbound A2A must not have it re-enabled by a pack"
        );
        assert!(
            a2a_source_from_packs(&[pack], "acme", None, None).is_some(),
            "control: the same pack builds a source with the gate unset"
        );
    }

    #[test]
    #[serial_test::serial]
    fn no_packs_or_no_sidecar_or_an_empty_sidecar_yields_no_source() {
        clear_gate();
        assert!(a2a_source_from_packs(&[], "acme", None, None).is_none());
        let bare = tempfile::tempdir().unwrap();
        let bare_pack = Arc::new(crate::pack::tests::pack_runtime_for_dir(bare.path()));
        assert!(a2a_source_from_packs(&[bare_pack], "acme", None, None).is_none());
        let (_dir, empty) = pack_with_sidecar("[]");
        assert!(a2a_source_from_packs(&[empty], "acme", None, None).is_none());
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn the_first_pack_wins_per_agent_id() {
        clear_gate();
        let first = MockServer::start().await;
        let second = MockServer::start().await;
        serve_agent(&first, "from the first pack", "one").await;
        serve_agent(&second, "from the second pack", "two").await;
        let (_d1, pack_one) = pack_with_sidecar(&format!(
            r#"[{{"agent_id":"recipe","base_url":"{}"}}]"#,
            first.uri()
        ));
        let (_d2, pack_two) = pack_with_sidecar(&format!(
            r#"[{{"agent_id":"recipe","base_url":"{}"}}]"#,
            second.uri()
        ));

        let source = a2a_source_from_packs(&[pack_one, pack_two], "acme", None, None)
            .expect("a source is built");
        let catalog = source.catalog().await;

        assert_eq!(
            catalog.tool_entry("recipe").unwrap().description,
            "from the first pack"
        );
        assert!(
            second.received_requests().await.unwrap().is_empty(),
            "the losing pack's agent must never be dialled"
        );
    }

    /// LLM backend recording the tools offered on each turn, then replaying a
    /// script.
    struct RecordingLlm {
        responses: Mutex<Vec<LlmResponse>>,
        offered: Mutex<Vec<Vec<(String, String)>>>,
    }

    impl LlmBackend for RecordingLlm {
        fn complete<'a>(
            &'a self,
            req: LlmRequest,
        ) -> Pin<Box<dyn Future<Output = Result<LlmResponse, LlmError>> + Send + 'a>> {
            self.offered.lock().unwrap().push(
                req.tools
                    .iter()
                    .map(|t| (t.extension_id.clone(), t.tool_name.clone()))
                    .collect(),
            );
            let next = {
                let mut queue = self.responses.lock().unwrap();
                if queue.is_empty() {
                    Err(LlmError::Transport("script exhausted".into()))
                } else {
                    Ok(queue.remove(0))
                }
            };
            Box::pin(async move { next })
        }
    }

    /// End to end. A pack carrying the sidecar, with a team-scoped credential
    /// in the store, yields an `a2a:` tool the LLM is offered. Calling it
    /// POSTs with the bearer token, the card fetch carries none, and the
    /// reply lands in the trail.
    #[tokio::test]
    #[serial_test::serial]
    async fn a_pack_carrying_the_sidecar_yields_an_a2a_tool_the_llm_is_offered_and_can_call() {
        clear_gate();
        let server = MockServer::start().await;
        serve_agent(&server, "Suggests dishes.", "an omelette").await;
        let (_dir, pack) = pack_with_sidecar(&format!(
            r#"[{{"agent_id":"{AGENT_ID}","name":"Recipe agent","base_url":"{}",
               "auth_header_name":null,"auth_team":"sales","requires_auth":true}}]"#,
            server.uri()
        ));
        let token_uri = format!("secrets://default/acme/sales/a2a/{AGENT_ID}");
        let store = secrets(&[(token_uri.as_str(), "tok-1")]);

        let source = a2a_source_from_packs(&[pack], "acme", Some(store), None);
        assert!(source.is_some(), "the sidecar must yield a source");

        let tool_ref = format!("a2a:{AGENT_ID}");
        let config = AgentConfig {
            agent_id: "worker".into(),
            system_prompt: "sys".into(),
            tools: vec![ToolRef {
                extension_id: tool_ref.clone(),
                tool_name: "ask".into(),
                description: Some("Ask the Recipe agent agent.".into()),
                input_schema: Some(json!({
                    "type": "object",
                    "properties": {"message": {"type": "string",
                                               "description": "What to ask the agent"}},
                    "required": ["message"]
                })),
                usage_note: None,
            }],
            llm: LlmProviderRef {
                provider: "mock".into(),
                model: "m".into(),
                credential_ref: None,
            },
            limits: AgentLimits {
                max_iter: 4,
                timeout: Duration::from_secs(60),
                ..AgentLimits::default()
            },
            memory: None,
            knowledge: None,
            guardrails: vec![],
            conversational: false,
            opening_message: None,
        };
        let llm = Arc::new(RecordingLlm {
            responses: Mutex::new(vec![
                LlmResponse {
                    content: None,
                    tool_calls: vec![ToolCallRecord {
                        call_id: "c1".into(),
                        extension_id: tool_ref.clone(),
                        tool_name: "ask".into(),
                        args: json!({"message": "eggs?"}),
                    }],
                    tokens_in: 5,
                    tokens_out: 5,
                },
                LlmResponse {
                    content: Some("Try an omelette.".into()),
                    tool_calls: vec![],
                    tokens_in: 5,
                    tokens_out: 5,
                },
            ]),
            offered: Mutex::new(Vec::new()),
        });
        let tc = TenantContext::new("acme", "prod");
        let cp = MockConfigProvider::new();
        cp.insert(&tc, "worker", config);
        let runtime = AgentRuntime::new(
            Arc::new(cp),
            Arc::new(MockAgentStateStore::new()),
            Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().unwrap()),
            llm.clone(),
            Arc::new(MockTelemetry::new()),
            Arc::new(MockTokenMeter::new(0)),
            Arc::new(NoopToolLedger),
            None,
        )
        .with_a2a_source(source);

        let out = runtime
            .step(
                tc,
                "s",
                "worker",
                AgentInput {
                    text: "go".into(),
                    conversational: false,
                },
            )
            .await
            .unwrap();

        assert!(
            llm.offered.lock().unwrap()[0].contains(&(tool_ref.clone(), "ask".to_string())),
            "the a2a tool must be offered to the LLM"
        );
        let requests = server.received_requests().await.unwrap();
        for get in requests.iter().filter(|r| r.method.as_str() == "GET") {
            assert!(
                get.headers.get("authorization").is_none(),
                "card fetch is public"
            );
        }
        let post = requests
            .iter()
            .find(|r| r.method.as_str() == "POST")
            .expect("the agent must have been called");
        assert_eq!(
            post.headers
                .get("authorization")
                .and_then(|v| v.to_str().ok()),
            Some("Bearer tok-1")
        );
        let result = out.trail.iter().find_map(|step| match step {
            AgentStep::ToolCall { name, result, .. } if name == "ask" => Some(result.clone()),
            _ => None,
        });
        assert_eq!(result, Some(json!({"reply": "an omelette"})));
        assert_eq!(out.terminated_by, TerminationReason::FinalReply);
    }
}
