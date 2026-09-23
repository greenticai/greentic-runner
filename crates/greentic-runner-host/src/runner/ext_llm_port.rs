//! The extension-runtime LLM port built over the WORKER's own resolved
//! backend.
//!
//! `build_ext_runtime`'s fallback used to be the env-keyed `EnvLlmPort`, whose
//! provider defaults to `deepseek`. So in every deployed lane an extension's
//! `host.llm.complete` ran on a provider the operator never selected — or, with
//! no env key at all, failed with "llm not configured for this runtime" — while
//! the correct backend was already resolved a few lines below in the same
//! function. This wraps THAT backend.
//!
//! The port ignores `role`, as `EnvLlmPort` does: a deployed runtime has no
//! per-role provider map, and inventing one would be a configuration surface an
//! operator must fill in before their extension works at all. The gate that
//! matters is upstream, in `host_state_llm`, which refuses any role the
//! extension did not declare.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use greentic_aw_runtime::AgentConfig;
use greentic_ext_runtime::host_ports::{
    HostCallContext, LlmPort, LlmPortError, LlmPortRequest, LlmPortResponse,
};

/// An extension-runtime LLM port over the worker's own resolved backend.
pub(crate) struct AgentLlmPort {
    backend: Arc<dyn greentic_aw_runtime::llm::LlmBackend>,
    provider: String,
    model: String,
    // Named in `LlmPort::complete`'s tracing line below, and by the
    // `#[cfg(test)]` accessor the deterministic-tie-break tests use to
    // assert WHICH agent won.
    agent_id: String,
}

impl AgentLlmPort {
    /// Build a port over `backend`, taking provider and model from the agent
    /// this runtime serves.
    ///
    /// The agent is chosen deterministically by
    /// [`crate::runner::agent_node::first_declared_llm_agent`] — sorted id
    /// order, first one declaring a non-empty provider — the same
    /// agent-selection rule (`configured_llm_provider` also builds on it, to
    /// decide keylessness rather than the per-request provider).
    ///
    /// Only that ONE candidate is ever examined — this does not fall through
    /// to a later agent when the first one fails a check below. That is
    /// deliberate: it is what keeps this port and the in-process agent
    /// backend looking at the same agent's declaration, rather than this
    /// port silently picking a second, differently-configured agent behind
    /// the backend's back. So an unparseable provider or a missing model on
    /// the sorted-first agent yields no port at all, even when a later agent
    /// would have qualified.
    ///
    /// Two checks on top of that selection, either of which returns `None`:
    /// the declared provider must be one [`greentic_llm::ProviderKind`] can
    /// parse, and the declared model must be non-empty (`first_declared_llm_agent`
    /// only guarantees a non-empty PROVIDER — an empty model would build
    /// exactly the kind of port that first check exists to prevent: one that
    /// fails on every call). In every `None` case the caller falls back to
    /// the env-keyed port instead of getting one that fails on every call.
    pub(crate) fn from_agents(
        backend: Arc<dyn greentic_aw_runtime::llm::LlmBackend>,
        agents: &HashMap<String, AgentConfig>,
    ) -> Option<Self> {
        let (id, agent) = crate::runner::agent_node::first_declared_llm_agent(agents)?;
        let provider = agent.llm.provider.trim();
        // Refuse a provider the backend cannot resolve. Building a port for
        // one produces an opaque failure on every single call instead of one
        // legible line here.
        if greentic_llm::ProviderKind::from_str(provider).is_err() {
            tracing::warn!(
                agent_id = %id,
                %provider,
                "extension runtime LLM port: agent declares an unknown provider; \
                 no worker-backed ext LLM port (falling back to the env-keyed port)"
            );
            return None;
        }
        let model = agent.llm.model.trim();
        if model.is_empty() {
            // Same failure mode as an unparseable provider: a port with an
            // empty model is exactly the opaque-failure-on-every-call shape
            // the provider check above exists to prevent.
            tracing::warn!(
                agent_id = %id,
                %provider,
                "extension runtime LLM port: agent declares a provider but no model; \
                 no worker-backed ext LLM port (falling back to the env-keyed port)"
            );
            return None;
        }
        tracing::info!(
            agent_id = %id,
            %provider,
            %model,
            "extension runtime LLM port wired (the worker's own agent LLM)"
        );
        Some(Self {
            backend,
            provider: provider.to_string(),
            model: model.to_string(),
            agent_id: id.clone(),
        })
    }

    #[cfg(test)]
    pub(crate) fn provider(&self) -> &str {
        &self.provider
    }

    #[cfg(test)]
    pub(crate) fn model(&self) -> &str {
        &self.model
    }

    #[cfg(test)]
    pub(crate) fn agent_id(&self) -> &str {
        &self.agent_id
    }
}

impl LlmPort for AgentLlmPort {
    fn complete(
        &self,
        extension_id: &str,
        _ctx: &HostCallContext,
        role: &str,
        request: LlmPortRequest,
    ) -> Result<LlmPortResponse, LlmPortError> {
        // The single most useful diagnostic this port can offer: which
        // agent's declaration decided the provider an extension's
        // completion actually ran on.
        tracing::debug!(
            agent_id = %self.agent_id,
            extension_id = %extension_id,
            role = %role,
            provider = %self.provider,
            model = %self.model,
            "extension runtime LLM port: dispatching to the worker's own agent LLM"
        );
        let llm_request = crate::runner::agent_node::port_request_to_llm_request(
            request,
            &self.provider,
            &self.model,
        );
        crate::runner::agent_node::complete_on_thread(self.backend.clone(), llm_request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn agent_with(provider: &str, model: &str) -> AgentConfig {
        let mut agent = crate::runner::agent_node::test_support::sample_agent_config("a");
        agent.llm.provider = provider.into();
        agent.llm.model = model.into();
        agent
    }

    /// An [`greentic_aw_runtime::llm::LlmBackend`] whose `complete` is never
    /// meant to run — every test here only inspects the resolved provider,
    /// model and agent id, never actually calls out to a backend.
    struct NeverCalledBackend;

    impl greentic_aw_runtime::llm::LlmBackend for NeverCalledBackend {
        fn complete<'a>(
            &'a self,
            _request: greentic_aw_runtime::llm::LlmRequest,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<
                            greentic_aw_runtime::llm::LlmResponse,
                            greentic_aw_runtime::error::LlmError,
                        >,
                    > + Send
                    + 'a,
            >,
        > {
            unreachable!("these tests never drive a completion")
        }
    }

    fn fake_backend() -> Arc<dyn greentic_aw_runtime::llm::LlmBackend> {
        Arc::new(NeverCalledBackend)
    }

    #[test]
    fn the_sorted_first_agent_that_declares_a_provider_decides() {
        let mut agents = HashMap::new();
        agents.insert("zz".to_string(), agent_with("ollama", "llama3"));
        agents.insert("aa".to_string(), agent_with("anthropic", "claude-x"));

        for _ in 0..50 {
            let port = AgentLlmPort::from_agents(fake_backend(), &agents)
                .expect("an agent declares a provider");
            assert_eq!(port.provider(), "anthropic");
            assert_eq!(port.model(), "claude-x");
            assert_eq!(port.agent_id(), "aa");
        }
    }

    #[test]
    fn an_agent_declaring_no_provider_is_skipped() {
        let mut agents = HashMap::new();
        agents.insert("aa".to_string(), agent_with("", ""));
        agents.insert("bb".to_string(), agent_with("groq", "llama-fast"));

        let port =
            AgentLlmPort::from_agents(fake_backend(), &agents).expect("bb declares a provider");
        assert_eq!(port.provider(), "groq");
        assert_eq!(port.agent_id(), "bb");
    }

    #[test]
    fn a_provider_no_provider_kind_knows_builds_no_port() {
        let mut agents = HashMap::new();
        agents.insert("aa".to_string(), agent_with("not-a-real-provider", "m"));

        assert!(
            AgentLlmPort::from_agents(fake_backend(), &agents).is_none(),
            "a provider the backend cannot parse must not produce a port that \
             fails on every call instead"
        );
    }

    #[test]
    fn an_agent_declaring_a_provider_but_no_model_builds_no_port() {
        let mut agents = HashMap::new();
        agents.insert("aa".to_string(), agent_with("anthropic", ""));

        assert!(
            AgentLlmPort::from_agents(fake_backend(), &agents).is_none(),
            "a provider without a model must not produce a port that fails on \
             every call instead — first_declared_llm_agent only guarantees a \
             non-empty provider, not a non-empty model"
        );
    }

    #[test]
    fn only_the_sorted_first_agent_is_ever_examined() {
        let mut agents = HashMap::new();
        // "aa" sorts first and declares a provider ProviderKind cannot parse;
        // "bb" would qualify on its own. The port must not fall through to
        // "bb" — that would let this port disagree with whichever agent the
        // in-process backend was built for.
        agents.insert("aa".to_string(), agent_with("not-a-real-provider", "m"));
        agents.insert("bb".to_string(), agent_with("anthropic", "claude-x"));

        assert!(
            AgentLlmPort::from_agents(fake_backend(), &agents).is_none(),
            "a disqualified sorted-first candidate must not fall through to a \
             later, otherwise-qualifying agent"
        );
    }

    #[test]
    fn no_agents_at_all_builds_no_port() {
        assert!(AgentLlmPort::from_agents(fake_backend(), &HashMap::new()).is_none());
    }
}
