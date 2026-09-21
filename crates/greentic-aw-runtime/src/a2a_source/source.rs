//! [`A2aToolSource`] — card fetch, catalog build, and `SendMessage` dispatch
//! for the A2A agents an agentic worker is bound to.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use greentic_a2a::fetch::{CardCache, require_secure_interface};
use greentic_a2a::message::{Message, Part, Role, Task, TaskState};
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
    transport: Arc<Transport>,
}

/// The agents plus the means to reach them, shared between the source and
/// every catalogue it builds so a catalogue can dispatch a call — the same
/// arrangement as `FlowToolCatalog` holding its invoker.
pub(super) struct Transport {
    agents: HashMap<String, String>,
    cards: CardCache,
    client: reqwest::Client,
}

impl std::fmt::Debug for Transport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Transport")
            .field("agents", &self.agents.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
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
            transport: Arc::new(Transport {
                agents: agents.into_iter().collect(),
                cards: CardCache::new(CARD_TTL)?,
                client,
            }),
        })
    }

    /// Build a catalog by fetching every configured agent's card.
    ///
    /// Infallible by contract, mirroring [`crate::mcp_source::McpToolSource`]:
    /// one dead or malformed agent must not remove a worker's OTHER tools, so
    /// a fetch failure is recorded in [`A2aToolCatalog::error_for`] instead of
    /// failing the whole build. The catalogue can dispatch calls itself.
    pub async fn catalog(&self) -> Arc<A2aToolCatalog> {
        let mut catalog = self.transport.fetch_cards().await;
        catalog.caller = Some(Arc::clone(&self.transport));
        Arc::new(catalog)
    }

    /// Call one agent with a plain-text prompt and return its reply text.
    pub async fn call(&self, agent_id: &str, text: &str) -> Result<String, String> {
        self.transport.call(agent_id, text).await
    }
}

impl Transport {
    /// Fetch every configured agent's card into a catalogue with no caller.
    async fn fetch_cards(&self) -> A2aToolCatalog {
        // Concurrently: fetched one after another, N hanging agents would cost
        // N card timeouts before the model is even called, every step.
        let fetched = futures::future::join_all(
            self.agents
                .iter()
                .map(|(agent_id, base)| async move { (agent_id, self.cards.get(base).await) }),
        )
        .await;

        let mut tools = HashMap::new();
        let mut errors = HashMap::new();
        for (agent_id, result) in fetched {
            match result {
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
        A2aToolCatalog {
            tools,
            errors,
            caller: None,
        }
    }

    /// Call one agent with a plain-text prompt and return its reply text.
    ///
    /// Resolves the agent's card, takes its first `supported_interfaces`
    /// entry, and posts a `SendMessage` request to that interface's URL. The
    /// interface's `tenant`, when the card set one, is echoed into
    /// `SendMessageParams.tenant` — the A2A spec makes that a MUST for
    /// clients, not a courtesy.
    pub(super) async fn call(&self, agent_id: &str, text: &str) -> Result<String, String> {
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

        // The card was fetched over https (or loopback), but the interface URL
        // is whatever the card SAYS. Apply the same rule to it, or an https
        // card could route the message — and any credential sent with it —
        // over plaintext to anywhere.
        let target = require_secure_interface(base, &interface.url)
            .map_err(|err| format!("a2a agent {agent_id} names an unusable interface: {err}"))?;

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
            .post(target)
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
            return Err(format!(
                "a2a agent {agent_id} returned an error: {}",
                error.message
            ));
        }

        match body.result {
            Some(SendMessageResult::Message(reply)) => {
                reply_text(agent_id, &reply.parts, "replied with no text part")
            }
            Some(SendMessageResult::Task(task)) => task_outcome(agent_id, &task),
            None => Err(format!(
                "a2a agent {agent_id} returned neither a result nor an error"
            )),
        }
    }
}

/// The text of `parts`, one part per line.
///
/// A reply with no text part at all is an error rather than an empty result:
/// an empty tool result tells the model nothing, and it would carry on as if
/// the agent had answered.
fn reply_text(agent_id: &str, parts: &[Part], empty: &str) -> Result<String, String> {
    let texts: Vec<&str> = parts
        .iter()
        .filter_map(|part| part.text.as_deref())
        .collect();
    if texts.is_empty() {
        return Err(format!("a2a agent {agent_id} {empty}"));
    }
    Ok(texts.join("\n"))
}

/// What the model should see when an agent answered with a `Task`.
///
/// This slice does not poll. A task that has already finished is answered
/// from its artifacts; one that failed is an error, so the model does not read
/// a failure as a successful result; one still in flight is reported as
/// pending, which is true.
fn task_outcome(agent_id: &str, task: &Task) -> Result<String, String> {
    let detail = task
        .status
        .message
        .as_ref()
        .and_then(|m| reply_text(agent_id, &m.parts, "").ok())
        .map(|text| format!(": {text}"))
        .unwrap_or_default();
    match task.status.state {
        TaskState::Completed => {
            let parts: Vec<Part> = task
                .artifacts
                .iter()
                .flat_map(|artifact| artifact.parts.iter().cloned())
                .collect();
            reply_text(agent_id, &parts, "completed a task with no text artifact")
        }
        TaskState::Submitted | TaskState::Working | TaskState::InputRequired => Ok(format!(
            "a2a agent {agent_id} accepted the request as task {} ({:?}); \
             polling a task to completion is not supported yet{detail}",
            task.id, task.status.state
        )),
        TaskState::Failed
        | TaskState::Rejected
        | TaskState::Canceled
        | TaskState::AuthRequired
        | TaskState::Unspecified => Err(format!(
            "a2a agent {agent_id} ended task {} as {:?}{detail}",
            task.id, task.status.state
        )),
    }
}
