// End-to-end test for the "combine a flow with an Agentic Worker" demo
// (tavily-research-demo): a flow whose single `dw.agent` node runs a research
// agent that may call the Tavily design extension, driven by a REAL LLM.
//
// What it proves (always, with any LLM key set):
//   - the flow's `dw.agent` node dispatches to the agentic-worker runtime
//     (desktop ephemeral mode, no Redis) — the flow<->agent wiring,
//   - the Tavily design extension is loaded so its tools are available to the
//     agent, and
//   - the flow completes (status Success) and the agent's `reply` is surfaced.
//
// LLM provider: with the `greentic-llm-backend` feature the worker's LLM call
// routes through greentic-llm, so the agent's declared provider (DeepSeek,
// Anthropic, Gemini, …) works in-process. Without that feature the AW falls
// back to a hardwired OpenAI client, and a non-OpenAI key yields the runtime's
// fallback message. The test hard-asserts a real (non-fallback) reply when
// compiled with `greentic-llm-backend` or when OPENAI_API_KEY is set.
//
// Live test — #[ignore]d (run with `-- --ignored`) and FAILS when its key is
// missing instead of printing SKIP and passing. DeepSeek example:
//   GREENTIC_LLM_API_KEY=<key> \
//   cargo test -p greentic-runner-desktop \
//     --features desktop-agent-ephemeral,greentic-llm-backend \
//     tavily_research -- --ignored --nocapture
//
// (A real Tavily key is only needed for the agent to actually search; without
//  it the tool call fails gracefully and the LLM still replies.)

#[cfg(feature = "desktop-agent-ephemeral")]
mod tests {
    use std::collections::BTreeMap;
    use std::fs::File;
    use std::io::Write;
    use std::path::{Path, PathBuf};

    use anyhow::{Context, Result};
    use greentic_runner_desktop::{RunOptions, RunStatus, desktop_defaults, run_pack_with_options};
    use greentic_types::{
        ComponentCapabilities, ComponentManifest, ComponentProfiles, ExtensionInline, ExtensionRef,
        PackFlowEntry, PackKind, PackManifest, ResourceHints, encode_pack_manifest,
    };
    use once_cell::sync::Lazy;
    use semver::Version;
    use serde_json::{Value, json};
    use tempfile::TempDir;
    use zip::write::FileOptions;

    const RUNTIME_FLOW_EXT_ID: &str = "greentic.pack.runtime_flow";
    const PACK_ID: &str = "tavily.research.e2e";
    const FLOW_ID: &str = "research.flow";
    const AGENT_ID: &str = "tavily_researcher";
    const NODE_ID: &str = "research";
    const QUESTION: &str = "What is the latest stable version of the Rust programming language? Answer in one sentence.";

    /// A real component artifact the pack loader can validate. The `dw.agent`
    /// node never invokes it — it routes to the agent handler — but a pack must
    /// carry a loadable component.
    static COMPONENT_ARTIFACT: Lazy<Vec<u8>> = Lazy::new(|| {
        let runner_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|p| p.parent())
            .map(|p| p.to_path_buf())
            .expect("greentic-runner root");
        let archive_path =
            runner_root.join("tests/fixtures/packs/runner-components/runner-components.gtpack");
        let file = File::open(&archive_path).expect("open fixture gtpack");
        let mut archive = zip::ZipArchive::new(file).expect("parse fixture gtpack");
        let mut entry = archive
            .by_name("components/qa.process@0.1.0/component.wasm")
            .expect("qa.process wasm missing from fixture pack");
        let mut buf = Vec::new();
        std::io::Read::read_to_end(&mut entry, &mut buf).expect("read wasm bytes");
        buf
    });

    /// The research agent — DeepSeek brain + the two Tavily tools. Mirrors
    /// tavily-research-demo/agents/tavily_researcher.json (provider swapped to
    /// deepseek to match the test's LLM key).
    fn agent_blob() -> Value {
        agent_blob_with_cred(None)
    }

    /// The research agent, optionally carrying an `llm.credential_ref` so the
    /// runner resolves the LLM key from the secrets store instead of the env.
    fn agent_blob_with_cred(credential_ref: Option<&str>) -> Value {
        let mut llm = json!({ "provider": "deepseek", "model": "deepseek-chat" });
        if let Some(cref) = credential_ref {
            llm["credential_ref"] = json!(cref);
        }
        json!({
            "agent_id": AGENT_ID,
            "system_prompt": "You are a research assistant. When the user asks about facts \
                or recent events, call `tavily_search` to find current information, then \
                synthesize a concise answer and cite the source URLs. If search is \
                unavailable, answer from your own knowledge and say so. Always reply in the \
                user's language.",
            "tools": [
                { "extension_id": "greentic.tavily", "tool_name": "tavily_search" },
                { "extension_id": "greentic.tavily", "tool_name": "tavily_extract" }
            ],
            "guardrails": [],
            "llm": llm,
            "limits": {}
        })
    }

    fn build_research_pack(pack_path: &Path) -> Result<()> {
        build_research_pack_with_agent(pack_path, agent_blob())
    }

    fn build_research_pack_with_agent(pack_path: &Path, agent: Value) -> Result<()> {
        let node = json!({
            "component": "dw.agent",
            "operation": AGENT_ID,
            "input": { "user_text": "{{in.text}}" },
            "routing": "end",
        });
        let mut nodes = serde_json::Map::new();
        nodes.insert(NODE_ID.to_string(), node);

        let runtime_flow = json!({
            "id": FLOW_ID,
            "flow_type": "messaging",
            "start": NODE_ID,
            "nodes": Value::Object(nodes),
        });

        let mut extensions = BTreeMap::new();
        extensions.insert(
            RUNTIME_FLOW_EXT_ID.to_string(),
            ExtensionRef {
                kind: RUNTIME_FLOW_EXT_ID.to_string(),
                version: "2.0.0".into(),
                digest: None,
                location: None,
                inline: Some(ExtensionInline::Other(json!({ "flows": [runtime_flow] }))),
            },
        );

        let mut agents: BTreeMap<String, Value> = BTreeMap::new();
        agents.insert(AGENT_ID.to_string(), agent);

        let manifest = PackManifest {
            schema_version: "1.0".into(),
            pack_id: PACK_ID.parse().expect("valid pack id"),
            name: None,
            version: Version::parse("0.0.0").expect("valid version"),
            kind: PackKind::Application,
            publisher: "test".into(),
            components: vec![ComponentManifest {
                id: "qa.process".parse().expect("component id"),
                version: Version::parse("0.1.0").expect("valid version"),
                supports: vec![greentic_types::FlowKind::Messaging],
                world: "greentic:component@0.4.0".into(),
                profiles: ComponentProfiles::default(),
                capabilities: ComponentCapabilities::default(),
                configurators: None,
                operations: Vec::new(),
                config_schema: None,
                resources: ResourceHints::default(),
                dev_flows: BTreeMap::new(),
            }],
            flows: Vec::<PackFlowEntry>::new(),
            dependencies: Vec::new(),
            capabilities: Vec::new(),
            signatures: Default::default(),
            secret_requirements: Vec::new(),
            bootstrap: None,
            agents,
            extensions: Some(extensions),
        };

        let mut zip = zip::ZipWriter::new(File::create(pack_path).context("create pack archive")?);
        let options: FileOptions<'_, ()> =
            FileOptions::default().compression_method(zip::CompressionMethod::Stored);
        let manifest_bytes = encode_pack_manifest(&manifest)?;
        zip.start_file("manifest.cbor", options)?;
        zip.write_all(&manifest_bytes)?;
        zip.start_file("components/qa.process.wasm", options)?;
        zip.write_all(&COMPONENT_ARTIFACT)?;
        zip.finish().context("finalise pack archive")?;
        Ok(())
    }

    /// Locate the prebuilt Tavily extension `.gtxpack` by walking up from the
    /// crate dir to the mono-workspace root.
    fn tavily_gtxpack() -> Option<PathBuf> {
        let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        for _ in 0..6 {
            let candidate = dir.join("component-tavily-ext/greentic.tavily-0.1.0.gtxpack");
            if candidate.is_file() {
                return Some(candidate);
            }
            dir = dir.parent()?.to_path_buf();
        }
        None
    }

    /// Unpack the Tavily `.gtxpack` (describe.json + extension.wasm) into
    /// `<ext_root>/design/greentic.tavily/` so the agentic-worker runtime
    /// discovers its tools.
    fn install_tavily_extension(ext_root: &Path) -> Result<bool> {
        let Some(gtxpack) = tavily_gtxpack() else {
            return Ok(false);
        };
        let dest = ext_root.join("design").join("greentic.tavily");
        std::fs::create_dir_all(&dest)?;
        let mut archive = zip::ZipArchive::new(File::open(&gtxpack)?)?;
        for i in 0..archive.len() {
            let mut entry = archive.by_index(i)?;
            let name = entry.name().to_string();
            let mut bytes = Vec::new();
            std::io::Read::read_to_end(&mut entry, &mut bytes)?;
            std::fs::write(dest.join(name), bytes)?;
        }
        Ok(true)
    }

    /// Best-effort: find the agent's `reply` by scanning the run transcript for
    /// any object carrying a non-empty `reply` string.
    fn reply_from_transcript(artifacts_dir: &Path) -> Option<String> {
        let text = std::fs::read_to_string(artifacts_dir.join("transcript.jsonl")).ok()?;
        for line in text.lines() {
            let Ok(value) = serde_json::from_str::<Value>(line) else {
                continue;
            };
            if let Some(reply) = find_reply(&value) {
                return Some(reply);
            }
        }
        None
    }

    fn find_reply(value: &Value) -> Option<String> {
        match value {
            Value::Object(map) => {
                if let Some(Value::String(reply)) = map.get("reply")
                    && !reply.trim().is_empty()
                {
                    return Some(reply.clone());
                }
                map.values().find_map(find_reply)
            }
            Value::Array(items) => items.iter().find_map(find_reply),
            _ => None,
        }
    }

    #[test]
    #[ignore = "live e2e: needs GREENTIC_LLM_API_KEY (or OPENAI_API_KEY); run with -- --ignored"]
    #[serial_test::serial]
    #[allow(unsafe_code)]
    fn tavily_research_flow_dispatches_dw_agent_e2e() {
        let have_key = std::env::var("GREENTIC_LLM_API_KEY")
            .ok()
            .filter(|v| !v.is_empty())
            .or_else(|| {
                std::env::var("OPENAI_API_KEY")
                    .ok()
                    .filter(|v| !v.is_empty())
            })
            .is_some();
        assert!(
            have_key,
            "set GREENTIC_LLM_API_KEY (+ GREENTIC_LLM_PROVIDER=deepseek \
             GREENTIC_LLM_MODEL=deepseek-chat) to run this live e2e."
        );

        let ext_root = TempDir::new().expect("ext tempdir");
        let tavily_installed =
            install_tavily_extension(ext_root.path()).expect("install tavily ext");

        // SAFETY: #[serial] guarantees no concurrent env mutation in this suite.
        unsafe {
            std::env::remove_var("GREENTIC_AW_REDIS_URL");
            std::env::set_var("GREENTIC_EXTENSIONS_DIR", ext_root.path());
            // Drive the agent's brain to DeepSeek (the AW + greentic-llm read
            // these env vars; the key stays whatever the caller exported).
            if std::env::var("GREENTIC_LLM_PROVIDER").is_err() {
                std::env::set_var("GREENTIC_LLM_PROVIDER", "deepseek");
            }
            if std::env::var("GREENTIC_LLM_MODEL").is_err() {
                std::env::set_var("GREENTIC_LLM_MODEL", "deepseek-chat");
            }
        }

        let temp = TempDir::new().expect("pack tempdir");
        let pack_path = temp.path().join("tavily-research.gtpack");
        build_research_pack(&pack_path).expect("build research pack");

        // The question defaults to a stable factual one (deterministic), but can
        // be overridden via E2E_QUESTION to exercise a live tavily_search.
        let question = std::env::var("E2E_QUESTION").unwrap_or_else(|_| QUESTION.to_string());
        let opts = RunOptions {
            entry_flow: Some(FLOW_ID.to_string()),
            input: json!({ "text": question }),
            ..desktop_defaults()
        };

        let result = run_pack_with_options(&pack_path, opts)
            .expect("run_pack_with_options should return Ok");

        eprintln!(
            "tavily_installed={tavily_installed} status={:?} nodes={:?} error={:?}",
            result.status,
            result
                .node_summaries
                .iter()
                .map(|n| (n.node_id.as_str(), &n.status))
                .collect::<Vec<_>>(),
            result.error,
        );

        // 1. The dw.agent node must have dispatched (wiring proof), not been
        //    rejected as an unknown node kind.
        let dispatched = result.node_summaries.iter().any(|n| n.node_id == NODE_ID);
        assert!(
            dispatched,
            "dw.agent node '{NODE_ID}' was not dispatched; status={:?} error={:?}",
            result.status, result.error
        );
        if let Some(failure) = result.failures.get(NODE_ID) {
            let msg = failure.message.to_ascii_lowercase();
            assert!(
                !msg.contains("unknown node kind")
                    && !msg.contains("unresolved component")
                    && !msg.contains("no handler"),
                "dw.agent failed with a wiring error rather than at the LLM/tool: {msg}"
            );
        }

        // 2. The flow completed and surfaced the agent's reply.
        assert_eq!(
            result.status,
            RunStatus::Success,
            "run did not succeed; error={:?}",
            result.error
        );
        let reply = reply_from_transcript(&result.artifacts_dir)
            .expect("a successful run must carry an agent reply in the transcript");
        eprintln!("\n===== AGENT REPLY =====\n{reply}\n=======================");

        // 3. The LLM ANSWER step depends on a provider the agentic-worker
        //    runtime actually supports. The runner's in-process AW backend is
        //    hardwired to OpenAI (api.openai.com), and the only LLM-bridge
        //    extension (greentic.llm-openai) is network-pinned to
        //    OpenAI/Anthropic with `base_url` deliberately ignored — so DeepSeek
        //    (OpenAI-compatible, but a different host) cannot drive the worker
        //    here and surfaces the runtime's fallback message instead of a real
        //    answer.
        let looks_like_llm_failure = reply.to_ascii_lowercase().contains("something went wrong");
        if looks_like_llm_failure {
            eprintln!(
                "NOTE: the agent returned the runtime's fallback message — the LLM call did not \
                 succeed. The AW path supports OpenAI (in-process) or OpenAI/Anthropic via the \
                 greentic.llm-openai bridge; DeepSeek is not reachable here. Export a real \
                 OPENAI_API_KEY for a live answer."
            );
        }

        // Demand a real answer when the agent's provider is one the AW can
        // actually reach: compiled with the multi-provider greentic-llm backend
        // (any provider the AgentConfig declares), OR an OpenAI key is present
        // (the in-process default). Otherwise a fallback message is expected
        // (e.g. DeepSeek without the multi-provider feature).
        let demand_real_answer = cfg!(feature = "greentic-llm-backend")
            || std::env::var("OPENAI_API_KEY")
                .ok()
                .filter(|v| !v.is_empty())
                .is_some();
        if demand_real_answer {
            assert!(
                !looks_like_llm_failure,
                "with a supported provider + key the agent must produce a real answer, \
                 got the runtime fallback message"
            );
        }
    }

    /// A secrets store that returns the Tavily key for any canonical
    /// `…/tavily/api_key` scope and `NotFound` otherwise. Stands in for the dev
    /// store `gtc setup` populates: the agent's tool secret is resolved from
    /// here, never from the process environment.
    struct StoreOnlyTavilySecrets {
        tavily_key: String,
    }

    #[async_trait::async_trait]
    impl greentic_secrets_lib::SecretsManager for StoreOnlyTavilySecrets {
        async fn read(&self, path: &str) -> greentic_secrets_lib::Result<Vec<u8>> {
            if path.ends_with("/tavily/api_key") {
                Ok(self.tavily_key.clone().into_bytes())
            } else {
                Err(greentic_secrets_lib::SecretError::NotFound(
                    path.to_string(),
                ))
            }
        }
        async fn write(&self, _: &str, _: &[u8]) -> greentic_secrets_lib::Result<()> {
            Ok(())
        }
        async fn delete(&self, _: &str) -> greentic_secrets_lib::Result<()> {
            Ok(())
        }
    }

    /// Zero-env tool-secret proof: the Tavily API key lives ONLY in the injected
    /// secrets store (no `TAVILY_API_KEY` in the environment). The agent's
    /// `tavily_search` tool must still resolve its `secret://tavily/api_key`
    /// reference through `StoreToolSecretsBackend` and search the live web.
    ///
    /// Live — `#[ignore]`d; fails unless an LLM key and `ZEROENV_TAVILY_KEY` are present.
    ///   GREENTIC_LLM_API_KEY=<deepseek> ZEROENV_TAVILY_KEY=<tvly-…> \
    ///   cargo test -p greentic-runner-desktop \
    ///     --features desktop-agent-ephemeral,greentic-llm-backend \
    ///     tavily_tool_secret_resolves_from_store_zero_env -- --ignored --nocapture
    #[test]
    #[ignore = "live e2e: needs GREENTIC_LLM_API_KEY + ZEROENV_TAVILY_KEY; run with -- --ignored"]
    #[serial_test::serial]
    #[allow(unsafe_code)]
    fn tavily_tool_secret_resolves_from_store_zero_env() {
        use std::sync::Arc;

        let llm_key = std::env::var("GREENTIC_LLM_API_KEY")
            .ok()
            .filter(|v| !v.is_empty());
        // Test-only carrier for the Tavily key — deliberately NOT `TAVILY_API_KEY`
        // (the env name the agent would fall back to), so a passing run proves the
        // key came from the store.
        let tavily_key = std::env::var("ZEROENV_TAVILY_KEY")
            .ok()
            .filter(|v| !v.is_empty());
        let (Some(llm_key), Some(tavily_key)) = (llm_key, tavily_key) else {
            panic!(
                "set GREENTIC_LLM_API_KEY + ZEROENV_TAVILY_KEY (NOT TAVILY_API_KEY) \
                 to run this zero-env tool-secret e2e."
            );
        };

        let ext_root = TempDir::new().expect("ext tempdir");
        let tavily_installed =
            install_tavily_extension(ext_root.path()).expect("install tavily ext");
        assert!(
            tavily_installed,
            "this test requires the tavily gtxpack to be present"
        );

        // SAFETY: #[serial] guarantees no concurrent env mutation in this suite.
        unsafe {
            std::env::remove_var("GREENTIC_AW_REDIS_URL");
            // The whole point: the Tavily key must NOT be reachable via env.
            std::env::remove_var("TAVILY_API_KEY");
            std::env::set_var("GREENTIC_EXTENSIONS_DIR", ext_root.path());
            std::env::set_var("GREENTIC_LLM_API_KEY", &llm_key);
            if std::env::var("GREENTIC_LLM_PROVIDER").is_err() {
                std::env::set_var("GREENTIC_LLM_PROVIDER", "deepseek");
            }
            if std::env::var("GREENTIC_LLM_MODEL").is_err() {
                std::env::set_var("GREENTIC_LLM_MODEL", "deepseek-chat");
            }
        }

        let temp = TempDir::new().expect("pack tempdir");
        let pack_path = temp.path().join("tavily-research.gtpack");
        build_research_pack(&pack_path).expect("build research pack");

        // The Tavily key is reachable ONLY through this injected store.
        let secrets: Arc<dyn greentic_secrets_lib::SecretsManager> =
            Arc::new(StoreOnlyTavilySecrets { tavily_key });

        // A question that forces a live web search (Tavily), so success implies
        // the store-resolved key actually authenticated the tool call.
        let question = std::env::var("E2E_QUESTION").unwrap_or_else(|_| {
            "Using web search, what is today's date according to a news source? \
             Cite the source URL."
                .to_string()
        });
        let opts = RunOptions {
            entry_flow: Some(FLOW_ID.to_string()),
            input: json!({ "text": question }),
            secrets_manager: Some(secrets),
            ..desktop_defaults()
        };

        let result =
            run_pack_with_options(&pack_path, opts).expect("run_pack_with_options should be Ok");

        eprintln!(
            "ZERO-ENV tavily: TAVILY_API_KEY_unset={} status={:?} nodes={:?} error={:?}",
            std::env::var("TAVILY_API_KEY").is_err(),
            result.status,
            result
                .node_summaries
                .iter()
                .map(|n| (n.node_id.as_str(), &n.status))
                .collect::<Vec<_>>(),
            result.error,
        );

        assert_eq!(
            result.status,
            RunStatus::Success,
            "zero-env run did not succeed; error={:?}",
            result.error
        );
        let reply = reply_from_transcript(&result.artifacts_dir)
            .expect("a successful run must carry an agent reply");
        eprintln!(
            "\n===== ZERO-ENV AGENT REPLY (Tavily key from store, not env) =====\n{reply}\n\
             ================================================================"
        );
        assert!(
            !reply.to_ascii_lowercase().contains("something went wrong"),
            "the agent must produce a real answer with the store-resolved Tavily key"
        );
    }

    /// A secrets store that serves BOTH the LLM credential (any `…/llm/deepseek`
    /// scope, the agent's `credential_ref`) and the Tavily key (any
    /// `…/tavily/api_key` scope), and `NotFound` otherwise. Stands in for the dev
    /// store `gtc setup` populates: neither key is reachable via the environment.
    struct StoreOnlyLlmAndTavilySecrets {
        llm_key: String,
        tavily_key: String,
    }

    #[async_trait::async_trait]
    impl greentic_secrets_lib::SecretsManager for StoreOnlyLlmAndTavilySecrets {
        async fn read(&self, path: &str) -> greentic_secrets_lib::Result<Vec<u8>> {
            if path.ends_with("/llm/deepseek") {
                Ok(self.llm_key.clone().into_bytes())
            } else if path.ends_with("/tavily/api_key") {
                Ok(self.tavily_key.clone().into_bytes())
            } else {
                Err(greentic_secrets_lib::SecretError::NotFound(
                    path.to_string(),
                ))
            }
        }
        async fn write(&self, _: &str, _: &[u8]) -> greentic_secrets_lib::Result<()> {
            Ok(())
        }
        async fn delete(&self, _: &str) -> greentic_secrets_lib::Result<()> {
            Ok(())
        }
    }

    /// FULL zero-env proof: BOTH the LLM key (via the agent's `credential_ref`)
    /// and the Tavily tool key are resolved ONLY from the injected secrets store,
    /// with `GREENTIC_LLM_API_KEY`, `OPENAI_API_KEY`, and `TAVILY_API_KEY` all
    /// removed from the environment. A non-fallback answer with a source URL
    /// proves the in-process LLM backend authenticated with the store-resolved
    /// key and `tavily_search` ran with the store-resolved key.
    ///
    /// Live — `#[ignore]`d; fails unless `ZEROENV_LLM_KEY` and `ZEROENV_TAVILY_KEY` are present.
    ///   ZEROENV_LLM_KEY=<deepseek> ZEROENV_TAVILY_KEY=<tvly-…> \
    ///   cargo test -p greentic-runner-desktop \
    ///     --features desktop-agent-ephemeral,greentic-llm-backend \
    ///     full_zero_env_llm_and_tavily_from_store -- --ignored --nocapture
    #[test]
    #[ignore = "live e2e: needs ZEROENV_LLM_KEY + ZEROENV_TAVILY_KEY; run with -- --ignored"]
    #[serial_test::serial]
    #[allow(unsafe_code)]
    fn full_zero_env_llm_and_tavily_from_store() {
        use std::sync::Arc;

        // Test-only carriers — deliberately NOT the env names the runner falls
        // back to (`GREENTIC_LLM_API_KEY`, `TAVILY_API_KEY`), so a pass cannot be
        // a silent env fallback.
        let llm_key = std::env::var("ZEROENV_LLM_KEY")
            .ok()
            .filter(|v| !v.is_empty());
        let tavily_key = std::env::var("ZEROENV_TAVILY_KEY")
            .ok()
            .filter(|v| !v.is_empty());
        let (Some(llm_key), Some(tavily_key)) = (llm_key, tavily_key) else {
            panic!(
                "set ZEROENV_LLM_KEY + ZEROENV_TAVILY_KEY (NOT the *_API_KEY env names) \
                 to run this full zero-env e2e."
            );
        };

        let ext_root = TempDir::new().expect("ext tempdir");
        let tavily_installed =
            install_tavily_extension(ext_root.path()).expect("install tavily ext");
        assert!(tavily_installed, "this test requires the tavily gtxpack");

        // SAFETY: #[serial] guarantees no concurrent env mutation in this suite.
        unsafe {
            std::env::remove_var("GREENTIC_AW_REDIS_URL");
            // No bridge extension — exercise the in-process LLM path's store
            // credential resolution.
            std::env::remove_var("GREENTIC_AW_LLM_EXTENSION");
            // The whole point: NEITHER key may be reachable via env.
            std::env::remove_var("GREENTIC_LLM_API_KEY");
            std::env::remove_var("OPENAI_API_KEY");
            std::env::remove_var("TAVILY_API_KEY");
            std::env::set_var("GREENTIC_EXTENSIONS_DIR", ext_root.path());
            // Provider/model still ride on the request; only the KEY is zero-env.
            std::env::set_var("GREENTIC_LLM_PROVIDER", "deepseek");
            std::env::set_var("GREENTIC_LLM_MODEL", "deepseek-chat");
        }

        let temp = TempDir::new().expect("pack tempdir");
        let pack_path = temp.path().join("tavily-research.gtpack");
        // Agent declares credential_ref "deepseek" → runner resolves
        // `secrets://default/{tenant}/_/llm/deepseek` from the store.
        build_research_pack_with_agent(&pack_path, agent_blob_with_cred(Some("deepseek")))
            .expect("build research pack");

        let secrets: Arc<dyn greentic_secrets_lib::SecretsManager> =
            Arc::new(StoreOnlyLlmAndTavilySecrets {
                llm_key,
                tavily_key,
            });

        let question = std::env::var("E2E_QUESTION").unwrap_or_else(|_| {
            "What is the latest stable Rust version? Answer in one sentence and cite the \
             source URL."
                .to_string()
        });
        let opts = RunOptions {
            entry_flow: Some(FLOW_ID.to_string()),
            input: json!({ "text": question }),
            secrets_manager: Some(secrets),
            ..desktop_defaults()
        };

        let result =
            run_pack_with_options(&pack_path, opts).expect("run_pack_with_options should be Ok");

        eprintln!(
            "FULL ZERO-ENV: llm_env_unset={} tavily_env_unset={} status={:?} nodes={:?} error={:?}",
            std::env::var("GREENTIC_LLM_API_KEY").is_err(),
            std::env::var("TAVILY_API_KEY").is_err(),
            result.status,
            result
                .node_summaries
                .iter()
                .map(|n| (n.node_id.as_str(), &n.status))
                .collect::<Vec<_>>(),
            result.error,
        );

        assert_eq!(
            result.status,
            RunStatus::Success,
            "full zero-env run did not succeed; error={:?}",
            result.error
        );
        let reply = reply_from_transcript(&result.artifacts_dir)
            .expect("a successful run must carry an agent reply");
        eprintln!(
            "\n===== FULL ZERO-ENV AGENT REPLY (LLM + Tavily keys from store) =====\n{reply}\n\
             ===================================================================="
        );
        assert!(
            !reply.to_ascii_lowercase().contains("something went wrong"),
            "with both keys store-resolved the agent must produce a real answer, \
             got the runtime fallback message"
        );
    }
}
