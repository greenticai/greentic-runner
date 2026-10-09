//! [`A2aToolSource`] — card fetch, catalog build, and `SendMessage` dispatch
//! for the A2A agents an agentic worker is bound to.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use greentic_a2a::fetch::{CardCache, require_secure_interface};
use greentic_a2a::message::{Message, Part, Role, Task, TaskProgress, TaskState};
use greentic_a2a::rpc::{
    JsonRpcRequest, JsonRpcResponse, METHOD_SEND_MESSAGE, SendMessageParams, SendMessageResult,
};
use greentic_secrets_lib::SecretsManager;

use super::auth::{CredentialScope, ensure_same_host};
use super::continuation::A2aContinuation;
use super::outcome::A2aOutcome;
use super::types::{A2aRoute, A2aToolCatalog, A2aToolEntry};

/// How long a fetched agent card is trusted before a refetch is considered.
/// Mirrors [`crate::mcp_source`]'s catalog TTL: a card changes rarely, so
/// re-fetching it on every agent step would be pure network cost.
const CARD_TTL: Duration = Duration::from_secs(5 * 60);

/// Per-request budget for a `SendMessage` call. An A2A agent may do real work
/// before replying, so this is deliberately longer than a mere connectivity
/// probe would need.
const CALL_TIMEOUT: Duration = Duration::from_secs(30);

/// The A2A protocol extensions this client implements.
///
/// **Empty, and that is the fact the check below turns on.** We implement
/// none, so every extension an agent marks `required` is one we cannot
/// honour. The list exists rather than the check being hardcoded to "refuse
/// any required extension" so that implementing one is an entry here and
/// nothing else.
const SUPPORTED_EXTENSIONS: &[&str] = &[];

/// Why this agent may not be used, when its card requires an extension we do
/// not implement.
///
/// `required` means the client "must understand and comply with the
/// extension's requirements". An agent declaring one has said its wire
/// behaviour is not the plain protocol, so calling it as though it were is a
/// violation it warned us about in advance — and one that would surface as
/// wrong answers rather than as an error, since the request is well-formed
/// plain A2A and the agent may well reply to it.
///
/// Both the catalogue build and [`Transport::call`] go through this, so an
/// agent cannot be withheld from the tool list and still be callable. The
/// catalogue's own entry is advisory: `A2aToolCatalog::dispatch` deliberately
/// attempts a call for any CONFIGURED agent, including one that had no entry,
/// so the listing alone would not have stopped the call.
fn unsupported_required_extensions(card: &greentic_a2a::card::AgentCard) -> Option<String> {
    let missing: Vec<&str> = card
        .capabilities
        .required_extension_uris()
        .into_iter()
        .filter(|uri| !SUPPORTED_EXTENSIONS.contains(uri))
        .collect();
    (!missing.is_empty()).then(|| {
        format!(
            "requires protocol extension(s) this client does not implement: {}",
            missing.join(", ")
        )
    })
}

/// The A2A agents one worker may call, plus the transport to reach them.
///
/// Holds the configured [`A2aRoute`]s, a [`CardCache`] (which enforces the
/// https-except-loopback rule on every fetch), the scope a credentialed
/// route's token is read from, and a `reqwest::Client` built with
/// `redirect::Policy::none()` and a timeout. That is the same posture
/// `greentic-a2a`'s own client uses, for the same reason: an agent must not be
/// able to redirect a credentialed call elsewhere.
pub struct A2aToolSource {
    transport: Arc<Transport>,
}

/// The agents plus the means to reach them, shared between the source and
/// every catalogue it builds so a catalogue can dispatch a call — the same
/// arrangement as `FlowToolCatalog` holding its invoker.
pub(super) struct Transport {
    agents: HashMap<String, A2aRoute>,
    cards: CardCache,
    client: reqwest::Client,
    credentials: CredentialScope,
}

impl std::fmt::Debug for Transport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Transport")
            .field("agents", &self.agents.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl A2aToolSource {
    /// `agents` is `(agent_id, base_url)` pairs with no credential: the no-auth
    /// form, kept for callers (and tests) that bind public agents only.
    ///
    /// Fails only if the HTTP client cannot be built. That failure is
    /// returned rather than papered over: falling back to a default client
    /// would silently drop the no-redirect policy and the timeout, which are
    /// the two properties this type exists to guarantee.
    pub fn new(agents: Vec<(String, String)>) -> Result<Self, reqwest::Error> {
        let routes = agents
            .into_iter()
            .map(|(agent_id, base)| A2aRoute::unauthenticated(agent_id, base))
            .collect();
        Self::build(routes, CredentialScope::none())
    }

    /// Build from sidecar routes, any of which may require a credential.
    ///
    /// `tenant` and `unit` are captured here, as the flow and component
    /// sources capture theirs, so [`Self::catalog`] stays tenant-less. The
    /// token is NOT read here: it is read on every call, so a rotated secret
    /// takes effect without a restart. A route with `requires_auth` and no
    /// `secrets` builds fine and refuses at call time, naming the scope.
    ///
    /// Routes are deduplicated by `agent_id`, first wins, matching the
    /// first-pack-wins rule of the caller that assembles them.
    pub fn from_routes(
        routes: Vec<A2aRoute>,
        secrets: Option<Arc<dyn SecretsManager>>,
        tenant: impl Into<String>,
        unit: Option<String>,
    ) -> Result<Self, reqwest::Error> {
        Self::build(routes, CredentialScope::new(secrets, tenant.into(), unit))
    }

    fn build(routes: Vec<A2aRoute>, credentials: CredentialScope) -> Result<Self, reqwest::Error> {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(CALL_TIMEOUT)
            .build()?;
        let mut agents = HashMap::with_capacity(routes.len());
        for route in routes {
            agents.entry(route.agent_id.clone()).or_insert(route);
        }
        Ok(Self {
            transport: Arc::new(Transport {
                agents,
                cards: CardCache::new(CARD_TTL)?,
                client,
                credentials,
            }),
        })
    }

    /// Build a catalog by fetching every configured agent's card.
    ///
    /// Infallible by contract, mirroring [`crate::mcp_source::McpToolSource`]:
    /// one dead or malformed agent must not remove a worker's OTHER tools, so
    /// a fetch failure is recorded in [`A2aToolCatalog::error_for`] instead of
    /// failing the whole build. The catalogue can dispatch calls itself. Card
    /// fetches are unauthenticated: public cards are unauthenticated in A2A.
    pub async fn catalog(&self) -> Arc<A2aToolCatalog> {
        let mut catalog = self.transport.fetch_cards().await;
        catalog.caller = Some(Arc::clone(&self.transport));
        Arc::new(catalog)
    }

    /// Call one agent with a plain-text prompt and return its reply text,
    /// starting a NEW remote conversation.
    ///
    /// Kept for callers that hold no conversation state of their own. It
    /// collapses every non-answer — a task still working, a task asking us a
    /// question — into an `Err`, because a `Result<String, String>` has
    /// nowhere honest to put them; the shape that does is
    /// [`A2aToolCatalog::dispatch_in_conversation`], which is what the agent
    /// loop uses.
    pub async fn call(&self, agent_id: &str, text: &str) -> Result<String, String> {
        match self.transport.send(agent_id, text, None).await? {
            A2aReply {
                outcome: A2aOutcome::Answered { text },
                ..
            } => Ok(text),
            other => Err(flatten_non_answer(agent_id, &other.outcome)),
        }
    }
}

/// One `SendMessage` exchange: what to tell the model, and what to remember
/// about the remote conversation.
///
/// The two are returned together and decided in one place so they cannot
/// disagree — a `task_id` kept alive for a task reported as finished would
/// make the next call fail against a task the remote has closed.
pub(super) struct A2aReply {
    pub(super) outcome: A2aOutcome,
    /// The remote's id for this conversation, when it named one. `None`
    /// leaves whatever the caller already held in place: an agent that
    /// answers a plain `Message` with no `contextId` has not ended the
    /// conversation, it just did not restate its name for it.
    pub(super) context_id: Option<String>,
    /// The remote task still awaiting us. `Some` ONLY while the task is
    /// open — see [`super::continuation`].
    pub(super) task_id: Option<String>,
}

/// One sentence for an outcome that [`A2aToolSource::call`] cannot return as
/// a reply. Never used on the catalogue path, which renders the outcome
/// structurally instead.
fn flatten_non_answer(agent_id: &str, outcome: &A2aOutcome) -> String {
    match outcome.to_value(agent_id) {
        serde_json::Value::Object(map) => map
            .get("error")
            .or_else(|| map.get("question"))
            .or_else(|| map.get("detail"))
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .unwrap_or_else(|| format!("a2a agent {agent_id} did not answer")),
        _ => format!("a2a agent {agent_id} did not answer"),
    }
}

impl Transport {
    /// Fetch every configured agent's card into a catalogue with no caller.
    async fn fetch_cards(&self) -> A2aToolCatalog {
        // Concurrently: fetched one after another, N hanging agents would cost
        // N card timeouts before the model is even called, every step.
        let fetched =
            futures::future::join_all(self.agents.iter().map(|(agent_id, route)| async move {
                (agent_id, self.cards.get(&route.base_url).await)
            }))
            .await;

        let mut tools = HashMap::new();
        let mut errors = HashMap::new();
        for (agent_id, result) in fetched {
            match result {
                Ok(card) => match unsupported_required_extensions(&card) {
                    Some(reason) => {
                        tracing::warn!(
                            agent = %agent_id,
                            reason = %reason,
                            "a2a agent requires an unimplemented protocol extension"
                        );
                        errors.insert(agent_id.clone(), reason);
                    }
                    None => {
                        tools.insert(
                            agent_id.clone(),
                            A2aToolEntry {
                                description: card.description.clone(),
                            },
                        );
                    }
                },
                Err(err) => {
                    tracing::warn!(agent = %agent_id, error = %err, "a2a agent card unavailable");
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

    /// Send one message to `agent_id`, continuing `prior` when there is one.
    ///
    /// `prior` is the conversation's remembered reference for this agent. Its
    /// `context_id` and `task_id` travel on the outgoing [`Message`], which is
    /// the whole of the multi-turn fix: without them every call opened a new
    /// remote task, so an agent that answered `input-required` could never be
    /// answered.
    ///
    /// Resolves the agent's (public, unauthenticated) card, takes its first
    /// `supported_interfaces` entry, and checks that interface is secure. For
    /// a credentialed route it then requires the interface to share the
    /// configured base's host and port, and only then reads the credential;
    /// a missing or unusable one refuses the call with nothing sent. The
    /// `SendMessage` POST is the only request that carries the credential.
    /// The interface's `tenant`, when the card set one, is echoed into
    /// `SendMessageParams.tenant`; the A2A spec makes that a MUST for clients.
    ///
    /// An agent requiring a protocol extension we do not implement is refused
    /// HERE as well as withheld from the catalogue, because a dispatch is
    /// attempted for every configured agent whether it had an entry or not.
    pub(super) async fn send(
        &self,
        agent_id: &str,
        text: &str,
        prior: Option<&A2aContinuation>,
    ) -> Result<A2aReply, String> {
        let route = self
            .agents
            .get(agent_id)
            .ok_or_else(|| format!("unknown a2a agent: {agent_id}"))?;

        let card = self
            .cards
            .get(&route.base_url)
            .await
            .map_err(|err| format!("fetching card for a2a agent {agent_id} failed: {err}"))?;

        if let Some(reason) = unsupported_required_extensions(&card) {
            return Err(format!("a2a agent {agent_id} {reason}"));
        }

        let interface = card
            .supported_interfaces
            .first()
            .ok_or_else(|| format!("a2a agent {agent_id} advertises no interfaces"))?;

        // The card was fetched over https (or loopback), but the interface URL
        // is whatever the card SAYS. Apply the same rule to it, or an https
        // card could route the message — and the credential sent with it —
        // over plaintext to anywhere.
        let target = require_secure_interface(&route.base_url, &interface.url)
            .map_err(|err| format!("a2a agent {agent_id} names an unusable interface: {err}"))?;
        // Userinfo in the interface URL makes the HTTP client add its own
        // `Authorization: Basic` header, so a credentialed POST would carry two
        // competing credentials. No legitimate card needs it.
        if !target.username().is_empty() || target.password().is_some() {
            return Err(format!(
                "a2a agent {agent_id} names an interface URL carrying userinfo; refusing it"
            ));
        }

        // A credential goes only to the host and port the admin configured.
        // Checked before the secret is read, so a card naming another host
        // never causes a secrets lookup, and nothing is sent: there is no
        // unauthenticated retry.
        if route.requires_auth {
            ensure_same_host(agent_id, &route.base_url, &target)?;
        }
        let auth = self.credentials.header_for(route).await?;

        // The two fields this whole slice exists to populate. An absent
        // `prior` is a first turn and correctly sends neither; a `prior`
        // holding only a context resumes the conversation but starts a new
        // task, which is what a follow-up question after a COMPLETED task is.
        let message = Message {
            message_id: uuid::Uuid::new_v4().to_string(),
            context_id: prior.map(|c| c.context_id.clone()),
            task_id: prior.and_then(|c| c.task_id.clone()),
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

        let mut post = self.client.post(target).json(&request);
        if let Some((name, value)) = auth {
            post = post.header(name, value);
        }
        let response = post
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
            Some(SendMessageResult::Message(reply)) => Ok(A2aReply {
                outcome: A2aOutcome::Answered {
                    text: reply_text(agent_id, &reply.parts, "replied with no text part")?,
                },
                context_id: reply.context_id,
                // A bare `Message` reply is not a task, so there is nothing
                // open to continue. Any task we were holding has been
                // answered outside the task model; dropping its id is the
                // honest record.
                task_id: None,
            }),
            Some(SendMessageResult::Task(task)) => Ok(task_reply(agent_id, task)),
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

/// The answer a completed task's artifacts carry: its prose, then the
/// structured results beside it.
///
/// Reading only the `text` parts — which is what this did until 2026-09-25 —
/// drops the half of the answer a program asked for. greentic-start answers a
/// turn that produced a structured output with a `Task` whose artifacts are
/// the turn's prose AND one `application/json` `data` part per output
/// (worker-interop contract D13), so a Greentic worker calling another
/// Greentic worker received the sentence and never the object the callee
/// deliberately produced. Nothing was red: the call succeeded, the model
/// answered from the prose alone, and the missing result looked like the
/// callee not having produced one.
///
/// The media type is matched EXACTLY, never by a `+json` suffix. An Adaptive
/// Card is `application/vnd.microsoft.card.adaptive+json`, and contract D10
/// says a card reaches an agent caller only when it asked for one — this
/// client never does, so a conformant server never sends one, and an exact
/// match means a non-conformant one cannot get a card in front of the model
/// through this door either. A vendor `+json` type is a rendering, not a
/// result.
///
/// Serialized compactly on its own line, because the destination is an LLM
/// tool result: a `Value` has to become text somewhere, and doing it here
/// keeps the object beside the words that explain it.
fn artifact_answer(agent_id: &str, parts: &[Part]) -> Result<String, String> {
    let mut lines: Vec<String> = parts.iter().filter_map(|part| part.text.clone()).collect();
    lines.extend(parts.iter().filter_map(structured_text));
    if lines.is_empty() {
        return Err(format!(
            "a2a agent {agent_id} completed a task with no text artifact"
        ));
    }
    Ok(lines.join("\n"))
}

/// One artifact part's structured value as compact JSON, or `None` when the
/// part is not one.
fn structured_text(part: &Part) -> Option<String> {
    let value = part.data.as_ref()?;
    if part.media_type.as_deref() != Some(STRUCTURED_OUTPUT_MEDIA_TYPE) {
        return None;
    }
    // A value that cannot be re-serialized came off the wire as JSON, so this
    // is unreachable in practice; dropping it is still better than failing a
    // call over a part the prose may already have explained.
    serde_json::to_string(value).ok()
}

/// The media type greentic-start stamps on a structured-output artifact part
/// (`interop::a2a::STRUCTURED_OUTPUT_MEDIA_TYPE`). Declared here rather than
/// imported because the two repositories share a protocol, not a crate.
const STRUCTURED_OUTPUT_MEDIA_TYPE: &str = "application/json";

/// What the model should see when an agent answered with a `Task`, and what
/// the conversation should remember about it.
///
/// This slice does not poll. What it does instead — and what the old code
/// could not — is keep the reference that lets the NEXT call continue the
/// same task, which is the only thing `input-required` needs to become
/// answerable.
///
/// The `task_id` is carried forward ONLY while
/// [`TaskState::progress`] says the task is open. A completed or failed task
/// will not accept another message, so keeping its id would make the next
/// call fail against a task the remote has closed; the `context_id` survives
/// either way, so a follow-up question still lands in the same remote
/// conversation.
fn task_reply(agent_id: &str, task: Task) -> A2aReply {
    let detail = task
        .status
        .message
        .as_ref()
        .and_then(|m| reply_text(agent_id, &m.parts, "").ok());
    let state = task.status.state;
    let open = matches!(state.progress(), TaskProgress::Open);
    let outcome = match state.progress() {
        TaskProgress::Done => {
            let parts: Vec<Part> = task
                .artifacts
                .iter()
                .flat_map(|artifact| artifact.parts.iter().cloned())
                .collect();
            match artifact_answer(agent_id, &parts) {
                Ok(text) => A2aOutcome::Answered { text },
                // A completed task with nothing to read is not an answer.
                // Reported as an end state rather than as an empty reply, so
                // the model does not relay silence as the agent's response.
                Err(reason) => A2aOutcome::Ended {
                    state: TaskState::Completed,
                    detail: Some(reason),
                },
            }
        }
        TaskProgress::Open if state == TaskState::InputRequired => {
            A2aOutcome::InputRequired { question: detail }
        }
        TaskProgress::Open => A2aOutcome::Working { state, detail },
        TaskProgress::Ended => A2aOutcome::Ended { state, detail },
    };
    A2aReply {
        outcome,
        context_id: task.context_id,
        task_id: open.then_some(task.id),
    }
}
