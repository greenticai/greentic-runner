//! Playbook-as-agent-tool source. A playbook is a stateless typed procedure —
//! instructions, an allow-list of tools, guardrails, declared inputs — executed
//! by an LLM, so it is the whole tool unit and the catalog is keyed by a single
//! `playbook_id`. Contract: `docs/playbook-contract-v1.md` in
//! greentic-designer.
//!
//! # Two halves, two different shapes, and the reason
//!
//! The LISTING half mirrors [`crate::flow_source`] exactly: a host boundary
//! enumerates what the pack carries and this crate depends only on the trait
//! plus JSON.
//!
//! The EXECUTION half cannot. A `flow:`, `mcp:` or `a2a:` call is an outbound
//! request, so a catalog is all any of them needs; a playbook turn is an LLM
//! loop, which is [`crate::AgentRuntime`] itself, and the dispatch path is
//! reached FROM that loop — passing the runtime into the function its own loop
//! calls is circular. So the turn is an injected effect, exactly as
//! [`crate::graph::executor::AgentTurnFn`] already is: this crate declares
//! [`PlaybookTurnRequest`] / [`PlaybookTurnResult`] / [`PlaybookTurnFn`] and the
//! host wires the closure to `AgentRuntime::step`.
//!
//! # Resilience contract (a playbook tool must never break an agent step)
//!
//! - Building a catalog is infallible: a [`PlaybookSource`] that surfaces no
//!   playbooks simply yields an empty catalog, and
//!   [`PlaybookToolSource::catalog`] never returns or propagates an error.
//! - [`PlaybookToolCatalog::dispatch`] always returns a JSON
//!   [`serde_json::Value`] and never panics — an unknown `playbook_id`, a
//!   playbook with no callable tools left after narrowing, or a turn failure
//!   becomes `{"error": "..."}` so the LLM observes it as a normal tool result.

use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use serde_json::json;

use crate::config::{GuardrailRef, ToolRef};
use crate::tenant::TenantContext;

/// How long a built catalog is reused before a rebuild is considered.
/// Mirrors [`crate::flow_source`]'s own TTL.
const CATALOG_TTL: Duration = Duration::from_secs(5 * 60);

/// What a playbook needs from an LLM — **never which LLM**.
///
/// A playbook document deliberately names no model: its publisher's
/// `validate_publishable` rejects a pinned one so the document stays
/// installable in a tenant that does not have it. The host resolves the tier
/// and the capabilities against the tenant's own provider, and must FAIL rather
/// than downgrade — a `Reasoning` playbook served by a `Fast` model answers
/// plausibly and reports nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlaybookLlmTier {
    Fast,
    Balanced,
    Reasoning,
}

/// A capability the playbook cannot run without.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum PlaybookLlmCapability {
    ToolCalling,
    StructuredOutput,
    Vision,
    LongContext,
}

/// The playbook's LLM requirement, unresolved.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlaybookLlmRequirement {
    pub tier: PlaybookLlmTier,
    pub requires: Vec<PlaybookLlmCapability>,
}

impl Default for PlaybookLlmRequirement {
    fn default() -> Self {
        Self {
            tier: PlaybookLlmTier::Balanced,
            requires: Vec::new(),
        }
    }
}

/// One playbook offered as an agent tool, with everything a turn needs.
/// Produced by [`PlaybookSource::list_playbooks`] — the host reads the pack's
/// own document once, here, rather than on every call.
#[derive(Clone, Debug)]
pub struct PlaybookOperation {
    pub playbook_id: String,
    /// LLM-facing description (the document's `summary`).
    pub description: String,
    /// LLM-facing JSON schema, projected from the document's `inputs[]`.
    pub parameters: serde_json::Value,
    /// The turn's system prompt (the document's `instructions`).
    pub instructions: String,
    pub llm: PlaybookLlmRequirement,
    /// The document's own `tools` — an ALLOW-LIST to intersect against the
    /// calling worker's set, never a grant. See [`PlaybookToolCatalog`].
    pub allow_list: Vec<ToolRef>,
    pub guardrails: Vec<GuardrailRef>,
}

/// A resolved catalog entry. Stored inside [`PlaybookToolCatalog`], keyed by
/// `playbook_id`, and carrying the narrowed tool set the turn will run with.
#[derive(Clone, Debug)]
pub struct PlaybookToolEntry {
    pub description: String,
    pub parameters: serde_json::Value,
    pub instructions: String,
    pub llm: PlaybookLlmRequirement,
    /// `intersect(calling worker's tools, the document's allow-list)`, computed
    /// once when the catalog is built.
    pub tools: Vec<ToolRef>,
    pub guardrails: Vec<GuardrailRef>,
}

/// Host boundary: enumerate the playbooks the loaded packs carry. The concrete
/// implementation lives in runner-host, which is the only layer that knows what
/// a pack is; this crate depends only on the trait plus JSON.
///
/// Total by contract: it returns whatever is currently exposed, possibly empty.
/// A document that does not parse, or whose `descriptor_version` this build
/// does not know, is omitted by the host rather than surfaced broken — a
/// malformed playbook costs its own tool and nothing else.
pub trait PlaybookSource: Send + Sync {
    fn list_playbooks(&self) -> Vec<PlaybookOperation>;
}

/// One playbook turn, as the host must run it.
///
/// `tools` is already narrowed: the host runs the turn with exactly this set
/// and must not widen it back to the calling worker's.
#[derive(Clone, Debug)]
pub struct PlaybookTurnRequest {
    pub playbook_id: String,
    pub instructions: String,
    pub llm: PlaybookLlmRequirement,
    pub tools: Vec<ToolRef>,
    pub guardrails: Vec<GuardrailRef>,
    /// The caller's arguments, as the LLM produced them.
    pub input: serde_json::Value,
}

/// What a playbook turn produced.
#[derive(Clone, Debug)]
pub struct PlaybookTurnResult {
    pub reply: String,
}

/// One playbook turn: the host wires this to `AgentRuntime::step`.
///
/// Mirrors [`crate::graph::executor::AgentTurnFn`], and for the same reason —
/// see this module's header.
pub type PlaybookTurnFn = Arc<
    dyn Fn(
            PlaybookTurnRequest,
        )
            -> Pin<Box<dyn Future<Output = Result<PlaybookTurnResult, String>> + Send + 'static>>
        + Send
        + Sync,
>;

/// Immutable per-turn view of the playbook-tool surface: the LLM-facing schemas
/// (list seam) plus the injected turn effect needed to run one.
pub struct PlaybookToolCatalog {
    /// `playbook_id` → entry, with `tools` already intersected.
    tools: HashMap<String, PlaybookToolEntry>,
    turn: PlaybookTurnFn,
    fetched_at: Instant,
}

impl PlaybookToolCatalog {
    /// Build a catalog by enumerating `source` once and narrowing every
    /// playbook's allow-list against `caller_tools`.
    ///
    /// **The narrowing is a security boundary, not an optimisation.** A
    /// playbook may be called by any worker that binds it, so inheriting the
    /// caller's set unfiltered would let one published skill reach every tool
    /// of every caller it ever has; and a playbook naming a tool its caller
    /// does not hold must not gain it. Hence INTERSECT, never union — and it is
    /// computed here, once, rather than left to each host to remember.
    pub fn from_source(
        source: &dyn PlaybookSource,
        turn: PlaybookTurnFn,
        caller_tools: &[ToolRef],
    ) -> Self {
        let held: BTreeSet<(&str, &str)> = caller_tools
            .iter()
            .map(|t| (t.extension_id.as_str(), t.tool_name.as_str()))
            .collect();

        let mut tools = HashMap::new();
        for op in source.list_playbooks() {
            let narrowed: Vec<ToolRef> = op
                .allow_list
                .iter()
                .filter(|t| held.contains(&(t.extension_id.as_str(), t.tool_name.as_str())))
                .cloned()
                .collect();
            if narrowed.len() != op.allow_list.len() {
                tracing::warn!(
                    playbook = %op.playbook_id,
                    declared = op.allow_list.len(),
                    callable = narrowed.len(),
                    "playbook binds tools the calling worker does not hold; they are not granted"
                );
            }
            tools.insert(
                op.playbook_id,
                PlaybookToolEntry {
                    description: op.description,
                    parameters: op.parameters,
                    instructions: op.instructions,
                    llm: op.llm,
                    tools: narrowed,
                    guardrails: op.guardrails,
                },
            );
        }
        Self {
            tools,
            turn,
            fetched_at: Instant::now(),
        }
    }

    /// Iterate every `playbook_id` key with its entry.
    pub fn tools(&self) -> impl Iterator<Item = (&String, &PlaybookToolEntry)> {
        self.tools.iter()
    }

    /// Number of playbooks in the catalog.
    pub fn len(&self) -> usize {
        self.tools.len()
    }

    /// Whether the catalog exposes no playbooks.
    pub fn is_empty(&self) -> bool {
        self.tools.is_empty()
    }

    /// Entry for one playbook, if present.
    pub fn tool_entry(&self, playbook_id: &str) -> Option<&PlaybookToolEntry> {
        self.tools.get(playbook_id)
    }

    /// Run one playbook, always returning a JSON value. An unknown
    /// `playbook_id`, unreadable arguments or a turn failure is surfaced as
    /// `{"error": "..."}` so the LLM observes it as a normal tool result.
    pub async fn dispatch(&self, playbook_id: &str, args_json: &str) -> serde_json::Value {
        let Some(entry) = self.tool_entry(playbook_id) else {
            return json!({ "error": format!("unknown playbook tool '{playbook_id}'") });
        };
        let input: serde_json::Value = match serde_json::from_str(args_json) {
            Ok(v) => v,
            Err(e) => {
                return json!({
                    "error": format!("invalid JSON args for playbook '{playbook_id}': {e}")
                });
            }
        };
        let request = PlaybookTurnRequest {
            playbook_id: playbook_id.to_string(),
            instructions: entry.instructions.clone(),
            llm: entry.llm.clone(),
            tools: entry.tools.clone(),
            guardrails: entry.guardrails.clone(),
            input,
        };
        match (self.turn)(request).await {
            Ok(result) => json!({ "reply": result.reply }),
            Err(e) => json!({ "error": e }),
        }
    }
}

/// Per-turn, TTL-gated source of playbook tool catalogs.
///
/// **The cache key carries the caller's tool set, unlike
/// [`crate::flow_source::FlowToolSource`]'s.** A flow catalog is the same for
/// every worker in a tenant; a playbook catalog is narrowed against the worker
/// that is calling, so two workers in one tenant legitimately get different
/// entries for the same document. Keying on the tenant alone would hand one
/// worker another's narrowing — which, since the narrowing is what stops a
/// skill reaching tools its caller does not hold, would be exactly the
/// disclosure [`PlaybookToolCatalog::from_source`] exists to prevent.
pub struct PlaybookToolSource {
    source: Arc<dyn PlaybookSource>,
    turn: PlaybookTurnFn,
    cache: DashMap<String, Arc<PlaybookToolCatalog>>,
}

impl PlaybookToolSource {
    /// Construct a source over a host-provided pack reader and turn effect.
    pub fn new(source: Arc<dyn PlaybookSource>, turn: PlaybookTurnFn) -> Self {
        Self {
            source,
            turn,
            cache: DashMap::new(),
        }
    }

    /// `(tenant_id, env_id)` plus the caller's tool set, order-independent.
    /// A `BTreeSet` is what makes two spellings of one set share an entry.
    fn cache_key(tenant: &TenantContext, caller_tools: &[ToolRef]) -> String {
        let held: BTreeSet<String> = caller_tools
            .iter()
            .map(|t| format!("{}/{}", t.extension_id, t.tool_name))
            .collect();
        format!(
            "{}:{}:{}",
            tenant.tenant_id,
            tenant.env_id,
            held.into_iter().collect::<Vec<_>>().join(",")
        )
    }

    /// Return the catalog for this tenant and caller, rebuilding when stale or
    /// absent. Infallible by contract.
    pub async fn catalog(
        &self,
        tenant: &TenantContext,
        caller_tools: &[ToolRef],
    ) -> Arc<PlaybookToolCatalog> {
        let key = Self::cache_key(tenant, caller_tools);

        if let Some(entry) = self.cache.get(&key) {
            let snap = entry.value();
            if snap.fetched_at.elapsed() < CATALOG_TTL {
                return snap.clone();
            }
        }

        let built = Arc::new(PlaybookToolCatalog::from_source(
            self.source.as_ref(),
            self.turn.clone(),
            caller_tools,
        ));
        self.cache.insert(key, built.clone());
        built
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn tool(ext: &str, name: &str) -> ToolRef {
        ToolRef {
            extension_id: ext.to_string(),
            tool_name: name.to_string(),
            description: None,
            input_schema: None,
            usage_note: None,
        }
    }

    /// Declares one playbook binding two tools.
    struct FakeSource;
    impl PlaybookSource for FakeSource {
        fn list_playbooks(&self) -> Vec<PlaybookOperation> {
            vec![PlaybookOperation {
                playbook_id: "refund".into(),
                description: "Issue a refund".into(),
                parameters: json!({ "type": "object" }),
                instructions: "Follow the refund policy.".into(),
                llm: PlaybookLlmRequirement {
                    tier: PlaybookLlmTier::Reasoning,
                    requires: vec![PlaybookLlmCapability::ToolCalling],
                },
                allow_list: vec![
                    tool("greentic.billing", "refund"),
                    tool("greentic.crm", "note"),
                ],
                guardrails: vec![],
            }]
        }
    }

    /// Records the request it was handed, so a test can assert on the narrowing.
    fn recording_turn() -> (
        PlaybookTurnFn,
        Arc<std::sync::Mutex<Vec<PlaybookTurnRequest>>>,
    ) {
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = seen.clone();
        let turn: PlaybookTurnFn = Arc::new(move |req: PlaybookTurnRequest| {
            let sink = sink.clone();
            Box::pin(async move {
                sink.lock().expect("lock").push(req);
                Ok(PlaybookTurnResult {
                    reply: "done".into(),
                })
            })
        });
        (turn, seen)
    }

    fn failing_turn(message: &'static str) -> PlaybookTurnFn {
        Arc::new(move |_req| Box::pin(async move { Err(message.to_string()) }))
    }

    #[tokio::test]
    async fn catalog_lists_and_runs_a_playbook() {
        let (turn, _) = recording_turn();
        let cat = PlaybookToolCatalog::from_source(
            &FakeSource,
            turn,
            &[
                tool("greentic.billing", "refund"),
                tool("greentic.crm", "note"),
            ],
        );
        assert_eq!(cat.len(), 1);
        let entry = cat.tool_entry("refund").expect("entry");
        assert_eq!(entry.description, "Issue a refund");
        assert_eq!(entry.llm.tier, PlaybookLlmTier::Reasoning);
        let out = cat.dispatch("refund", "{\"order\":7}").await;
        assert_eq!(out["reply"], "done");
    }

    #[tokio::test]
    async fn the_turn_receives_the_playbooks_instructions_and_arguments() {
        let (turn, seen) = recording_turn();
        let cat = PlaybookToolCatalog::from_source(
            &FakeSource,
            turn,
            &[
                tool("greentic.billing", "refund"),
                tool("greentic.crm", "note"),
            ],
        );
        cat.dispatch("refund", "{\"order\":7}").await;
        let calls = seen.lock().expect("lock");
        let req = calls.first().expect("one turn");
        assert_eq!(req.instructions, "Follow the refund policy.");
        assert_eq!(req.input["order"], 7);
    }

    /// The security boundary: a playbook may not gain a tool its caller lacks.
    #[tokio::test]
    async fn a_tool_the_caller_does_not_hold_is_not_granted() {
        let (turn, seen) = recording_turn();
        let cat = PlaybookToolCatalog::from_source(
            &FakeSource,
            turn,
            // The caller holds only ONE of the two the playbook binds.
            &[tool("greentic.billing", "refund")],
        );
        let entry = cat.tool_entry("refund").expect("entry");
        assert_eq!(
            entry.tools,
            vec![tool("greentic.billing", "refund")],
            "the unheld tool must be dropped, never granted"
        );
        cat.dispatch("refund", "{}").await;
        let calls = seen.lock().expect("lock");
        assert_eq!(
            calls.first().expect("one turn").tools.len(),
            1,
            "the turn must run with the narrowed set"
        );
    }

    /// Intersect, never union — a caller's extra tools do not reach the skill.
    #[tokio::test]
    async fn a_callers_other_tools_do_not_reach_the_playbook() {
        let (turn, _) = recording_turn();
        let cat = PlaybookToolCatalog::from_source(
            &FakeSource,
            turn,
            &[
                tool("greentic.billing", "refund"),
                tool("greentic.crm", "note"),
                tool("greentic.payroll", "pay"),
            ],
        );
        let entry = cat.tool_entry("refund").expect("entry");
        assert_eq!(entry.tools.len(), 2, "union would have given three");
        assert!(
            !entry
                .tools
                .iter()
                .any(|t| t.extension_id == "greentic.payroll"),
            "a tool the document does not bind must not reach the turn"
        );
    }

    #[tokio::test]
    async fn dispatch_unknown_playbook_returns_an_error_value_not_err() {
        let (turn, _) = recording_turn();
        let cat = PlaybookToolCatalog::from_source(&FakeSource, turn, &[]);
        let out = cat.dispatch("no_such_playbook", "{}").await;
        assert!(out.get("error").is_some(), "must yield an error value");
        assert!(
            out["error"]
                .as_str()
                .unwrap_or("")
                .contains("no_such_playbook"),
            "the error must name the playbook, got: {out}"
        );
    }

    #[tokio::test]
    async fn unreadable_arguments_are_an_error_value_and_no_turn_runs() {
        let (turn, seen) = recording_turn();
        let cat = PlaybookToolCatalog::from_source(&FakeSource, turn, &[]);
        let out = cat.dispatch("refund", "not json").await;
        assert!(out.get("error").is_some());
        assert!(
            seen.lock().expect("lock").is_empty(),
            "a turn must not run on arguments that could not be read"
        );
    }

    #[tokio::test]
    async fn a_failed_turn_is_an_error_value_not_err() {
        let cat = PlaybookToolCatalog::from_source(
            &FakeSource,
            failing_turn("no provider satisfies tier reasoning"),
            &[],
        );
        let out = cat.dispatch("refund", "{}").await;
        assert_eq!(out["error"], "no provider satisfies tier reasoning");
    }

    #[tokio::test]
    async fn an_empty_source_yields_an_empty_catalog() {
        struct Empty;
        impl PlaybookSource for Empty {
            fn list_playbooks(&self) -> Vec<PlaybookOperation> {
                Vec::new()
            }
        }
        let (turn, _) = recording_turn();
        let cat = PlaybookToolCatalog::from_source(&Empty, turn, &[]);
        assert!(cat.is_empty());
    }

    #[tokio::test]
    async fn source_caches_within_ttl_for_the_same_caller() {
        let (turn, _) = recording_turn();
        let source = PlaybookToolSource::new(Arc::new(FakeSource), turn);
        let tenant = TenantContext::new("acme", "prod");
        let held = [tool("greentic.billing", "refund")];
        let first = source.catalog(&tenant, &held).await;
        let second = source.catalog(&tenant, &held).await;
        assert!(
            Arc::ptr_eq(&first, &second),
            "second call must hit the cache"
        );
    }

    /// The key difference from the flow source, and why it exists.
    #[tokio::test]
    async fn two_callers_with_different_tools_get_different_catalogs() {
        let (turn, _) = recording_turn();
        let source = PlaybookToolSource::new(Arc::new(FakeSource), turn);
        let tenant = TenantContext::new("acme", "prod");
        let narrow = source
            .catalog(&tenant, &[tool("greentic.billing", "refund")])
            .await;
        let wide = source
            .catalog(
                &tenant,
                &[
                    tool("greentic.billing", "refund"),
                    tool("greentic.crm", "note"),
                ],
            )
            .await;
        assert!(
            !Arc::ptr_eq(&narrow, &wide),
            "one worker must not inherit another's narrowing"
        );
        assert_eq!(narrow.tool_entry("refund").expect("entry").tools.len(), 1);
        assert_eq!(wide.tool_entry("refund").expect("entry").tools.len(), 2);
    }

    #[tokio::test]
    async fn the_cache_key_ignores_the_order_of_the_callers_tools() {
        let (turn, _) = recording_turn();
        let source = PlaybookToolSource::new(Arc::new(FakeSource), turn);
        let tenant = TenantContext::new("acme", "prod");
        let one = source
            .catalog(
                &tenant,
                &[
                    tool("greentic.billing", "refund"),
                    tool("greentic.crm", "note"),
                ],
            )
            .await;
        let other = source
            .catalog(
                &tenant,
                &[
                    tool("greentic.crm", "note"),
                    tool("greentic.billing", "refund"),
                ],
            )
            .await;
        assert!(
            Arc::ptr_eq(&one, &other),
            "two spellings of one set must share an entry"
        );
    }

    #[tokio::test]
    async fn a_different_env_does_not_share_a_catalog() {
        let (turn, _) = recording_turn();
        let source = PlaybookToolSource::new(Arc::new(FakeSource), turn);
        let prod = source
            .catalog(&TenantContext::new("acme", "prod"), &[])
            .await;
        let staging = source
            .catalog(&TenantContext::new("acme", "staging"), &[])
            .await;
        assert!(!Arc::ptr_eq(&prod, &staging));
    }
}
