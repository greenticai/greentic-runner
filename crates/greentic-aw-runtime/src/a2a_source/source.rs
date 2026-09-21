//! [`A2aToolSource`] — card fetch, catalog build, and `SendMessage` dispatch
//! for the A2A agents an agentic worker is bound to.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use greentic_a2a::fetch::CardCache;
use greentic_a2a::message::{Message, Part, Role};
use greentic_a2a::rpc::{
    JsonRpcRequest, JsonRpcResponse, METHOD_SEND_MESSAGE, SendMessageParams, SendMessageResult,
};

use super::types::{A2aToolCatalog, A2aToolEntry};

/// How long a fetched agent card is trusted before a refetch is considered.
/// Mirrors [`crate::mcp_source`]'s catalog TTL: a card changes rarely, so
/// re-fetching it on every agent step would be pure network cost.
const CARD_TTL: Duration = Duration::from_secs(5 * 60);

/// Per-request budget for a `SendMessage` call. An A2A agent may do real work
/// before replying, so this is deliberately longer than a mere connectivity
/// probe would need.
const CALL_TIMEOUT: Duration = Duration::from_secs(30);

/// The A2A agents one worker may call, plus the transport to reach them.
///
/// Holds the configured `(agent_id, base)` pairs, a [`CardCache`] (which
/// enforces the https-except-loopback rule on every fetch), and a
/// `reqwest::Client` built with `redirect::Policy::none()` and a timeout —
/// the same posture `greentic-a2a`'s own client uses, and for the same
/// reason: an agent must not be able to redirect a credentialed call
/// elsewhere.
pub struct A2aToolSource {
    agents: HashMap<String, String>,
    cards: CardCache,
    client: reqwest::Client,
}

impl A2aToolSource {
    /// `agents` is `(agent_id, base_url)` pairs — the bindings a worker's
    /// tool list resolved to `a2a:<agent_id>`.
    ///
    /// Fails only if the HTTP client cannot be built. That failure is
    /// returned rather than papered over: falling back to a default client
    /// would silently drop the no-redirect policy and the timeout, which are
    /// the two properties this type exists to guarantee.
    pub fn new(agents: Vec<(String, String)>) -> Result<Self, reqwest::Error> {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(CALL_TIMEOUT)
            .build()?;
        Ok(Self {
            agents: agents.into_iter().collect(),
            cards: CardCache::new(CARD_TTL),
            client,
        })
    }

    /// Build a catalog by fetching every configured agent's card.
    ///
    /// Infallible by contract, mirroring [`crate::mcp_source::McpToolSource`]:
    /// one dead or malformed agent must not remove a worker's OTHER tools, so
    /// a fetch failure is recorded in [`A2aToolCatalog::error_for`] instead of
    /// failing the whole build.
    pub async fn catalog(&self) -> Arc<A2aToolCatalog> {
        let mut tools = HashMap::new();
        let mut errors = HashMap::new();
        for (agent_id, base) in &self.agents {
            match self.cards.get(base).await {
                Ok(card) => {
                    tools.insert(
                        agent_id.clone(),
                        A2aToolEntry {
                            description: card.description.clone(),
                        },
                    );
                }
                Err(err) => {
                    errors.insert(agent_id.clone(), err.to_string());
                }
            }
        }
        Arc::new(A2aToolCatalog { tools, errors })
    }

    /// Call one agent with a plain-text prompt and return its reply text.
    ///
    /// Resolves the agent's card, takes its first `supported_interfaces`
    /// entry, and posts a `SendMessage` request to that interface's URL. The
    /// interface's `tenant`, when the card set one, is echoed into
    /// `SendMessageParams.tenant` — the A2A spec makes that a MUST for
    /// clients, not a courtesy.
    pub async fn call(&self, agent_id: &str, text: &str) -> Result<String, String> {
        let base = self
            .agents
            .get(agent_id)
            .ok_or_else(|| format!("unknown a2a agent: {agent_id}"))?;

        let card = self
            .cards
            .get(base)
            .await
            .map_err(|err| format!("fetching card for a2a agent {agent_id} failed: {err}"))?;

        let interface = card
            .supported_interfaces
            .first()
            .ok_or_else(|| format!("a2a agent {agent_id} advertises no interfaces"))?;

        let message = Message {
            message_id: uuid::Uuid::new_v4().to_string(),
            context_id: None,
            task_id: None,
            role: Role::User,
            parts: vec![Part::text(text)],
            metadata: None,
        };
        let params = SendMessageParams {
            message,
            tenant: interface.tenant.clone(),
            metadata: None,
        };
        let request = JsonRpcRequest::new(1, METHOD_SEND_MESSAGE, params);

        let response = self
            .client
            .post(interface.url.as_str())
            .json(&request)
            .send()
            .await
            .map_err(|err| format!("calling a2a agent {agent_id} failed: {err}"))?;

        // A JSON-RPC server reports method errors inside a 200 body, so a
        // non-success status is a transport-level failure (a proxy, a crash,
        // a refused redirect). Name the status: parsing an HTML error page as
        // JSON would otherwise report only "unreadable reply".
        let status = response.status();
        if !status.is_success() {
            return Err(format!("a2a agent {agent_id} answered HTTP {status}"));
        }

        let body: JsonRpcResponse<SendMessageResult> = response
            .json()
            .await
            .map_err(|err| format!("a2a agent {agent_id} sent an unreadable reply: {err}"))?;

        if let Some(error) = body.error {
            return Err(error.message);
        }

        match body.result {
            Some(SendMessageResult::Message(reply)) => Ok(reply
                .parts
                .iter()
                .filter_map(|part| part.text.as_deref())
                .collect::<String>()),
            // This slice does not poll a `Task` to completion — that is a
            // later slice. Returning `Ok` with a sentence naming the task's
            // id and state gives an operator something actionable rather
            // than silence.
            Some(SendMessageResult::Task(task)) => Ok(format!(
                "the agent started a task instead of replying directly (id: {}, state: {:?}); \
                 polling a task to completion is not supported yet",
                task.id, task.status.state
            )),
            None => Err(format!(
                "a2a agent {agent_id} returned neither a result nor an error"
            )),
        }
    }
}
