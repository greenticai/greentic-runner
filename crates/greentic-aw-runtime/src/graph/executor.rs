//! Durable graph executor — the node-visiting drive loop.
//!
//! Ports the semantics of the greentic-designer spike
//! (`src/orchestrate/agent_graph/executor.rs`, origin/spike/agent-graph-engine-slice)
//! with the following adaptations:
//!
//! - Uses [`GraphConfig`] / [`GraphRunState`] / [`CheckpointStore`] from this
//!   crate rather than the designer's SQLite-backed checkpoint module.
//! - Effect closures are `Arc<dyn Fn(…) -> BoxFut<…>>` (shared, cloneable)
//!   instead of owned `Box<dyn Fn…>`.
//! - `start`/`resume` return `Result<GraphRunOutcome, GraphExecError>` (not
//!   `Result<()>`), carrying the final reply and visit trail.
//!
//! ## Record-then-checkpoint ordering (replayable resume)
//!
//! Each side-effect node (Agent, Tool) follows a strict two-write ordering:
//! the effect's result is recorded into the node-visit store
//! (`CheckpointStore::record_node_visit`) *immediately after the effect
//! returns and before the checkpoint update*. The checkpoint update then
//! commits the new cursor, state, and per-node `visits` counts atomically.
//!
//! On resume at cursor node N, the next attempt is `visits[N] + 1`. If a
//! `(run, N, attempt)` visit row already exists, the effect ran but the
//! process crashed before the checkpoint committed — so the recorded result
//! is **replayed** instead of re-invoking the effect.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex as AsyncMutex;

use crate::tenant::TenantContext;

use super::checkpoint::{CheckpointError, CheckpointStore, GraphRunRecord, RunStatus};
use super::model::{GraphConfig, GraphError, NodeKind};
use super::router::route;
use super::state::{GraphRole, GraphRunState};

// ---------------------------------------------------------------------------
// BoxFut alias — shared with checkpoint.rs pattern
// ---------------------------------------------------------------------------

/// Owned heap-allocated future, `Send + 'static`.
///
/// Kept `pub` (not `pub(crate)`) because external crates that construct
/// [`AgentTurnFn`] or [`ToolFn`] closures must be able to name this type as
/// their return type.  The Task-7 `DwAgentGraph` handler in `greentic-runner-host`
/// is the primary consumer.
pub type BoxFut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

// ---------------------------------------------------------------------------
// Effect request / result types
// ---------------------------------------------------------------------------

/// Request payload delivered to an injected agent-turn closure.
#[derive(Debug, Clone)]
pub struct AgentTurnRequest {
    /// Graph node id (for telemetry / routing context).
    pub node_id: String,
    /// System prompt from the node's configuration.
    pub system_prompt: String,
    /// Model identifier from the node's configuration.
    pub model: String,
    /// Current run state at the time of the turn.
    pub state: GraphRunState,
    /// LLM provider override from the node's `provider` field.
    /// `None` when the field is absent (existing graphs) — the host maps
    /// `None` to `"openai"` for backward compatibility.
    pub provider: Option<String>,
    /// Tool references declared on the node's `tools` field, in
    /// `"<extension_id>/<tool_name>"` form. Empty when the field is absent
    /// (existing graphs) — the host resolves these against the registered
    /// design extensions / MCP tool sources.
    pub tools: Vec<String>,
    /// Referenced published-agent id (from the node's `agent_ref`); when Some,
    /// the host runs that agent's full config instead of the inline fields.
    pub agent_ref: Option<String>,
    /// Referenced agent id (from the node's `inherit_from`); when Some, the
    /// host keeps this node's own prompt/model but inherits that agent's
    /// tools, memory, knowledge and guardrails. Ignored when `agent_ref` is
    /// set — that already takes everything.
    pub inherit_from: Option<String>,
}

/// Result returned by an injected agent-turn closure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentTurnResult {
    /// The assistant's reply text.
    pub reply: String,
    /// `true` when the agent considers the issue fully resolved.
    pub resolved: bool,
}

/// Request payload delivered to an injected tool closure.
#[derive(Debug, Clone)]
pub struct ToolCallRequest {
    /// Graph node id.
    pub node_id: String,
    /// Tool name from the node's configuration.
    pub tool_name: String,
    /// Current run state at the time of the call.
    pub state: GraphRunState,
}

// ---------------------------------------------------------------------------
// Injected effect types
// ---------------------------------------------------------------------------

/// One agent turn: the host wires this to `AgentRuntime::step`.
pub type AgentTurnFn = Arc<
    dyn Fn(AgentTurnRequest) -> BoxFut<'static, Result<AgentTurnResult, GraphExecError>>
        + Send
        + Sync,
>;

/// One deterministic tool call.
pub type ToolFn = Arc<
    dyn Fn(ToolCallRequest) -> BoxFut<'static, Result<serde_json::Value, GraphExecError>>
        + Send
        + Sync,
>;

// ---------------------------------------------------------------------------
// Supervisor effect types
// ---------------------------------------------------------------------------

/// Request payload delivered to an injected supervisor closure.
#[derive(Debug, Clone)]
pub struct SupervisorRequest {
    /// Graph node id.
    pub node_id: String,
    /// System prompt from the supervisor node's configuration.
    pub system_prompt: String,
    /// Model identifier from the supervisor node's configuration.
    pub model: String,
    /// The declared routes for this supervisor node.
    pub routes: Vec<crate::graph::model::SupervisorRoute>,
    /// Current run state at the time of the routing decision.
    pub state: GraphRunState,
    /// LLM provider override from the node's `provider` field.
    /// `None` when the field is absent (existing graphs) — the host maps
    /// `None` to `"openai"` for backward compatibility.
    pub provider: Option<String>,
    /// Referenced agent id (from the node's `agent_ref`); when `Some`, the host
    /// inherits that agent's **guardrails** for this routing turn — not its
    /// tools, memory or knowledge. See [`crate::graph::model::NodeKind`].
    pub agent_ref: Option<String>,
}

/// Result returned by an injected supervisor closure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorResult {
    /// The branch label chosen by the supervisor.
    pub branch: String,
    /// Raw reply text from the LLM (stored in the message log).
    pub raw_reply: String,
}

/// One supervisor routing decision: the host wires this to `AgentRuntime::step`
/// with a generated routing prompt containing the route menu.
pub type SupervisorFn = Arc<
    dyn Fn(SupervisorRequest) -> BoxFut<'static, Result<SupervisorResult, GraphExecError>>
        + Send
        + Sync,
>;

// ---------------------------------------------------------------------------
// Approval effect types
// ---------------------------------------------------------------------------

/// Request payload delivered to an injected approval closure.
#[derive(Debug, Clone)]
pub struct ApprovalRequest {
    /// Graph run id.
    pub run_id: String,
    /// Graph node id (the approval node).
    pub node_id: String,
    /// Tenant id the run belongs to.
    pub tenant: String,
    /// Human-readable title from the node's configuration.
    pub title: String,
    /// Gate mode: `"always"` | `"above_risk"` | `"above_confidence"`.
    pub mode: String,
    /// Risk threshold used when `mode == "above_risk"`.
    pub risk_threshold: Option<f64>,
    /// Confidence threshold used when `mode == "above_confidence"`.
    pub confidence_threshold: Option<f64>,
    /// Optional decision deadline, in milliseconds.
    pub deadline_ms: Option<u64>,
    /// Current run state at the time of the approval check.
    pub state: GraphRunState,
}

/// Result returned by an injected approval closure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ApprovalOutcome {
    /// No decision has arrived yet — the executor parks the run
    /// (`RunStatus::AwaitingInput`) at this node without advancing the
    /// cursor or recording a node visit.
    Awaiting,
    /// A human decision has arrived; `branch` selects the outgoing edge the
    /// executor advances along (e.g. `"approved"`, `"denied"`, `"timeout"`).
    Decided { branch: String },
}

/// One approval-gate check: the host wires this to its approval-request
/// transport (e.g. the `greentic.approval.request.v1` / `.response.v1`
/// NATS subjects) and reports whether a decision has arrived yet.
pub type ApprovalFn = Arc<
    dyn Fn(ApprovalRequest) -> BoxFut<'static, Result<ApprovalOutcome, GraphExecError>>
        + Send
        + Sync,
>;

// ---------------------------------------------------------------------------
// GraphExecError
// ---------------------------------------------------------------------------

/// Errors that may surface from [`GraphExecutor::start`] or
/// [`GraphExecutor::resume`].
#[derive(Debug, thiserror::Error)]
pub enum GraphExecError {
    #[error("graph run {run_id} exceeded the node-visit cap")]
    IterationCap { run_id: String },

    #[error("unknown node `{0}` (cursor corrupt or graph changed)")]
    UnknownNode(String),

    #[error("unknown run `{0}`")]
    UnknownRun(String),

    #[error("run `{0}` already completed")]
    AlreadyCompleted(String),

    #[error(transparent)]
    Graph(#[from] GraphError),

    #[error(transparent)]
    Checkpoint(#[from] CheckpointError),

    #[error("agent turn failed: {0}")]
    AgentTurn(String),

    #[error("tool call failed: {0}")]
    Tool(String),

    #[error("supervisor routing failed: {0}")]
    Supervisor(String),
}

// ---------------------------------------------------------------------------
// GraphRunOutcome
// ---------------------------------------------------------------------------

/// The final result of a drive-loop execution.
#[derive(Debug, Clone)]
pub struct GraphRunOutcome {
    /// Terminal status (`Succeeded` or `Failed`).
    pub status: RunStatus,
    /// Last assistant message emitted (what the Respond node returns), or an
    /// empty string if the run never produced an assistant reply.
    pub reply: String,
    /// One JSON entry per node visit.
    ///
    /// **Drive-loop shape** (normal execution or active resume):
    /// `{"node": id, "kind": "agent|tool|router|respond", "attempt": n, "replayed": bool}`.
    ///
    /// **Terminal-resume shape** (returned by [`GraphExecutor::resume`] when the run
    /// is already in a terminal state — built by `rebuild_trail_from_state`):
    /// `{"kind": "user|agent|tool", "content": "…"}`.  The node id and attempt
    /// count are not available from the stored message log, so this shape is
    /// intentionally narrower.  Callers that need both shapes must handle both.
    pub trail: Vec<serde_json::Value>,
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Hard upper bound on node visits per `drive` call, independent of the
/// per-router `maxIterations` cap. Prevents infinite loops on malformed
/// graphs.
pub const MAX_NODE_VISITS: u32 = 64;

// ---------------------------------------------------------------------------
// BranchCursor — durable per-branch frontier slot
// ---------------------------------------------------------------------------

/// One branch's position inside an in-flight parallel region.
///
/// The frontier is `Vec<BranchCursor>` (one entry per branch, ordered by
/// branch label lexicographically). It is serialised into
/// [`GraphRunRecord::frontier_json`] after every per-node visit so a crash
/// mid-parallel resumes each branch from its last good node.
///
/// - `cursor`: the node id this branch will visit next. When the branch
///   reaches the join node it stops without executing the join and records
///   the join id here with `parked = true`.
/// - `state_json`: a serialised [`GraphRunState`] private to this branch
///   (a clone of the trunk state taken at fan-out, advanced by this branch's
///   visits only — branches never observe each other's messages mid-flight).
/// - `parked`: `true` once the branch has reached the join.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct BranchCursor {
    branch: String,
    cursor: String,
    state_json: String,
    parked: bool,
}

// ---------------------------------------------------------------------------
// Concurrency & durability model for parallel regions
// ---------------------------------------------------------------------------
//
// Branches execute concurrently via `futures::future::join_all` over a
// `Vec<BoxFut>` (the spec's named primitive; `futures` is already a dep).
// `join_all` needs no `spawn`, so each branch future may borrow `&self`,
// `tenant`, `run_id`, and the shared `GraphConfig` — all futures settle
// before the await returns, so nothing escapes.
//
// Two pieces of shared mutable state, each with a single race-free owner:
//
//  1. The visits map. Each branch owns a PRIVATE `HashMap<String, u32>`
//     (cloned from the trunk's). Because validation guarantees branch paths
//     are node-disjoint until the join, the per-branch maps have disjoint key
//     sets and merge without conflict after all branches park. Attempt
//     numbering (for ledger replay) is therefore correct without any locking.
//
//  2. The global visit cap. `MAX_NODE_VISITS` is GLOBAL across all branches
//     plus the trunk. A shared `Arc<AtomicU32>`, seeded with the trunk's
//     visit count at fan-out, is incremented (fetch_add) by every branch
//     immediately BEFORE each node visit; if the post-increment total exceeds
//     the cap the branch returns `IterationCap`. This enforces the sum bound
//     without the branches needing to see each other's private maps.
//
//  3. The durable frontier. A single `tokio::Mutex<Vec<BranchCursor>>` is the
//     canonical frontier. After each branch node visit, the branch locks the
//     mutex, mutates ONLY its own slot (cursor + state_json + parked), builds
//     the whole record, and `save`s it while still holding the lock. Because
//     `save` overwrites the entire record, serialising every writer behind one
//     mutex is what prevents a lost update: no two branches ever build a
//     record from a stale frontier copy. Lock hold time spans the `save`
//     await, but contention is low (N branches, one short critical section per
//     node visit) and correctness beats throughput here. Each branch only ever
//     writes its own slot, so the held-across-await design has no logical
//     conflict — the mutex purely linearises the blob writes.

/// Shared, lock-guarded checkpoint coordinator for a parallel region.
///
/// Holds the canonical frontier plus the immutable trunk fields needed to
/// rebuild a full [`GraphRunRecord`]. Every branch checkpoints through
/// [`FrontierCheckpoint::save_slot`], which is serialised by the inner mutex.
struct FrontierCheckpoint {
    run_id: String,
    graph_json: String,
    parallel_node: String,
    /// Trunk state + trunk visits, frozen at fan-out (the trunk is parked).
    trunk_state_json: String,
    trunk_visits: HashMap<String, u32>,
    inner: AsyncMutex<Vec<BranchCursor>>,
    /// Accumulates every branch's private visit counts (disjoint keys by
    /// validation) so the merge can fold them into the trunk visits map.
    branch_visits: AsyncMutex<HashMap<String, u32>>,
}

// ---------------------------------------------------------------------------
// GraphExecutor
// ---------------------------------------------------------------------------

/// Drives agent-graph runs to completion, persisting checkpoints after every
/// node so that a killed process can resume mid-loop.
pub struct GraphExecutor {
    store: Arc<dyn CheckpointStore>,
    agent_turn: AgentTurnFn,
    tool: ToolFn,
    supervisor: SupervisorFn,
    approval: ApprovalFn,
}

impl GraphExecutor {
    /// Construct a new executor.
    pub fn new(
        store: Arc<dyn CheckpointStore>,
        agent_turn: AgentTurnFn,
        tool: ToolFn,
        supervisor: SupervisorFn,
        approval: ApprovalFn,
    ) -> Self {
        Self {
            store,
            agent_turn,
            tool,
            supervisor,
            approval,
        }
    }

    /// Start a **new** run.
    ///
    /// Validates that `run_id` is fresh:
    /// - If a record already exists with status `Running` → delegates to
    ///   resume logic.
    /// - If a record exists in a terminal state → returns
    ///   [`GraphExecError::AlreadyCompleted`].
    ///
    /// Otherwise, seeds [`GraphRunState`] with the user message, snapshots the
    /// graph JSON, saves the initial `Running` record, and drives the loop.
    pub async fn start(
        &self,
        tenant: &TenantContext,
        run_id: &str,
        cfg: &GraphConfig,
        user_text: &str,
    ) -> Result<GraphRunOutcome, GraphExecError> {
        // Check if a record already exists.
        if let Some(existing) = self.store.load(tenant, run_id).await? {
            return match existing.status {
                // `AwaitingInput` means the run is parked at an approval
                // node. Re-driving from the parked cursor is the correct
                // resume path: the Approval arm re-asks the `ApprovalFn`
                // closure, which re-parks (`Awaiting`) if still undecided or
                // advances (`Decided`) once a decision has arrived — so
                // `AwaitingInput` is handled identically to `Running` here.
                RunStatus::Running | RunStatus::AwaitingInput => {
                    // Resume the in-flight run.
                    self.drive_from_record(tenant, run_id, existing).await
                }
                RunStatus::Succeeded | RunStatus::Failed => {
                    Err(GraphExecError::AlreadyCompleted(run_id.to_owned()))
                }
            };
        }

        // Fresh run — seed state.
        let mut state = GraphRunState::default();
        state.push_message(GraphRole::User, user_text);

        let graph_json = serde_json::to_string(cfg)
            .map_err(|e| GraphExecError::Checkpoint(CheckpointError::Serde(e)))?;
        let cursor = cfg.graph.entry.clone();
        let visits: HashMap<String, u32> = HashMap::new();

        let rec = build_record(
            run_id,
            &graph_json,
            &cursor,
            &state,
            &visits,
            RunStatus::Running,
        )?;
        self.store.save(tenant, &rec).await?;

        self.drive(tenant, run_id, cfg.clone(), cursor, state, visits)
            .await
    }

    /// Resume an **existing** run.
    ///
    /// - If the run does not exist → [`GraphExecError::UnknownRun`].
    /// - If the run is already in a terminal state → return the stored outcome
    ///   WITHOUT re-driving.
    pub async fn resume(
        &self,
        tenant: &TenantContext,
        run_id: &str,
    ) -> Result<GraphRunOutcome, GraphExecError> {
        let rec = self
            .store
            .load(tenant, run_id)
            .await?
            .ok_or_else(|| GraphExecError::UnknownRun(run_id.to_owned()))?;

        match rec.status {
            RunStatus::Succeeded | RunStatus::Failed => {
                // Terminal — rebuild outcome from stored state without re-driving.
                let state: GraphRunState =
                    serde_json::from_str(&rec.state_json).map_err(CheckpointError::Serde)?;
                let reply = last_assistant_message(&state);
                let trail = rebuild_trail_from_state(&state);
                Ok(GraphRunOutcome {
                    status: rec.status,
                    reply,
                    trail,
                })
            }
            // Same rationale as `start` above — re-drive from the parked
            // cursor; the Approval arm re-asks `ApprovalFn` and either
            // re-parks or advances based on the current answer.
            RunStatus::Running | RunStatus::AwaitingInput => {
                self.drive_from_record(tenant, run_id, rec).await
            }
        }
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    /// Deserialise a stored record and call [`drive`].
    async fn drive_from_record(
        &self,
        tenant: &TenantContext,
        run_id: &str,
        rec: GraphRunRecord,
    ) -> Result<GraphRunOutcome, GraphExecError> {
        let cfg: GraphConfig = GraphConfig::from_json(&rec.graph_json)?;
        let state: GraphRunState =
            serde_json::from_str(&rec.state_json).map_err(CheckpointError::Serde)?;
        let visits: HashMap<String, u32> =
            serde_json::from_str(&rec.visits_json).map_err(CheckpointError::Serde)?;

        // A persisted frontier means the run crashed mid-parallel. Reconstruct
        // the branch cursors and re-drive only the non-parked branches; the
        // ledger replays completed branch visits. After the merge, control
        // returns to the trunk drive loop at the join's successor.
        if let Some(frontier_json) = &rec.frontier_json {
            let frontier: Vec<BranchCursor> =
                serde_json::from_str(frontier_json).map_err(CheckpointError::Serde)?;
            return self
                .resume_parallel(tenant, run_id, &cfg, &rec.cursor, state, visits, frontier)
                .await;
        }

        let cursor = rec.cursor.clone();
        self.drive(tenant, run_id, cfg, cursor, state, visits).await
    }

    /// The core node-visiting loop. Persists a checkpoint after every node.
    ///
    /// Semantics are ported faithfully from the designer spike:
    /// - Record-before-checkpoint ordering for side-effect nodes.
    /// - `iterations` increments AFTER `visits.insert` and message push (same
    ///   placement as the spike).
    /// - Respond node: saves Succeeded, returns immediately.
    /// - Cap exhausted: saves Failed, returns `Err(IterationCap)`.
    /// - Effect error: does NOT mark the run Failed (it stays Running so
    ///   `resume` can retry the failed node).
    async fn drive(
        &self,
        tenant: &TenantContext,
        run_id: &str,
        cfg: GraphConfig,
        mut cursor: String,
        mut state: GraphRunState,
        mut visits: HashMap<String, u32>,
    ) -> Result<GraphRunOutcome, GraphExecError> {
        let mut trail: Vec<serde_json::Value> = Vec::new();

        for _ in 0..MAX_NODE_VISITS {
            let node = cfg
                .graph
                .node(&cursor)
                .ok_or_else(|| GraphExecError::UnknownNode(cursor.clone()))?
                .clone();

            match &node.kind {
                NodeKind::Agent {
                    system_prompt,
                    model,
                    provider,
                    tools,
                    agent_ref,
                    inherit_from,
                } => {
                    let attempt = *visits.get(&cursor).unwrap_or(&0) + 1;

                    // Pre-clone so the closure can own the values it needs.
                    let node_id_for_err = cursor.clone();
                    let provider_clone = provider.clone();
                    let tools_clone = tools.clone();
                    let agent_ref_clone = agent_ref.clone();
                    let inherit_from_clone = inherit_from.clone();
                    let (raw, replayed) = self
                        .visit_effect(tenant, run_id, &cursor, attempt, || {
                            let req = AgentTurnRequest {
                                node_id: node_id_for_err.clone(),
                                system_prompt: system_prompt.clone(),
                                model: model.clone(),
                                state: state.clone(),
                                provider: provider_clone,
                                tools: tools_clone,
                                agent_ref: agent_ref_clone,
                                inherit_from: inherit_from_clone,
                            };
                            let fut = (self.agent_turn)(req);
                            Box::pin(async move {
                                let r = fut.await.map_err(|e| {
                                    GraphExecError::AgentTurn(format!(
                                        "node '{}' attempt {}: {}",
                                        node_id_for_err, attempt, e
                                    ))
                                })?;
                                serde_json::to_value(&r)
                                    .map_err(CheckpointError::Serde)
                                    .map_err(GraphExecError::Checkpoint)
                            })
                        })
                        .await?;

                    let result: AgentTurnResult =
                        serde_json::from_value(raw).map_err(CheckpointError::Serde)?;

                    trail.push(serde_json::json!({
                        "node": cursor,
                        "kind": "agent",
                        "attempt": attempt,
                        "replayed": replayed,
                    }));

                    // Update state — in the same order as the spike.
                    visits.insert(cursor.clone(), attempt);
                    state.iterations += 1;
                    state.push_message(GraphRole::Assistant, &result.reply);
                    if result.resolved {
                        state.resolved = true;
                    }

                    // Advance cursor (single outgoing edge).
                    cursor = next_linear(&cfg, &cursor)?;

                    // Checkpoint: cursor already advanced; state reflects this visit.
                    let rec = build_record(
                        run_id,
                        &serde_json::to_string(&cfg).map_err(CheckpointError::Serde)?,
                        &cursor,
                        &state,
                        &visits,
                        RunStatus::Running,
                    )?;
                    self.store.save(tenant, &rec).await?;
                }

                NodeKind::Tool { tool_name } => {
                    let attempt = *visits.get(&cursor).unwrap_or(&0) + 1;

                    // Pre-clone so the closure can own the values it needs.
                    let node_id_for_err = cursor.clone();
                    let (result, replayed) = self
                        .visit_effect(tenant, run_id, &cursor, attempt, || {
                            let req = ToolCallRequest {
                                node_id: node_id_for_err.clone(),
                                tool_name: tool_name.clone(),
                                state: state.clone(),
                            };
                            let fut = (self.tool)(req);
                            Box::pin(async move {
                                fut.await.map_err(|e| {
                                    GraphExecError::Tool(format!(
                                        "node '{}' attempt {}: {}",
                                        node_id_for_err, attempt, e
                                    ))
                                })
                            })
                        })
                        .await?;

                    trail.push(serde_json::json!({
                        "node": cursor,
                        "kind": "tool",
                        "attempt": attempt,
                        "replayed": replayed,
                    }));

                    visits.insert(cursor.clone(), attempt);
                    state.push_message(GraphRole::Tool, result.to_string());

                    cursor = next_linear(&cfg, &cursor)?;

                    let rec = build_record(
                        run_id,
                        &serde_json::to_string(&cfg).map_err(CheckpointError::Serde)?,
                        &cursor,
                        &state,
                        &visits,
                        RunStatus::Running,
                    )?;
                    self.store.save(tenant, &rec).await?;
                }

                NodeKind::Router { .. } => {
                    let attempt = *visits.get(&cursor).unwrap_or(&0) + 1;

                    let next = route(&cfg.graph, &cursor, &state)?;

                    trail.push(serde_json::json!({
                        "node": cursor,
                        "kind": "router",
                        "attempt": attempt,
                        "replayed": false,
                    }));

                    visits.insert(cursor.clone(), attempt);
                    cursor = next;

                    let rec = build_record(
                        run_id,
                        &serde_json::to_string(&cfg).map_err(CheckpointError::Serde)?,
                        &cursor,
                        &state,
                        &visits,
                        RunStatus::Running,
                    )?;
                    self.store.save(tenant, &rec).await?;
                }

                NodeKind::Respond => {
                    let attempt = *visits.get(&cursor).unwrap_or(&0) + 1;

                    trail.push(serde_json::json!({
                        "node": cursor,
                        "kind": "respond",
                        "attempt": attempt,
                        "replayed": false,
                    }));

                    visits.insert(cursor.clone(), attempt);

                    // Save terminal checkpoint.
                    let rec = build_record(
                        run_id,
                        &serde_json::to_string(&cfg).map_err(CheckpointError::Serde)?,
                        &cursor,
                        &state,
                        &visits,
                        RunStatus::Succeeded,
                    )?;
                    self.store.save(tenant, &rec).await?;

                    let reply = last_assistant_message(&state);
                    return Ok(GraphRunOutcome {
                        status: RunStatus::Succeeded,
                        reply,
                        trail,
                    });
                }

                NodeKind::Supervisor {
                    system_prompt,
                    model,
                    routes,
                    provider,
                    agent_ref,
                } => {
                    let attempt = *visits.get(&cursor).unwrap_or(&0) + 1;
                    let node_id_for_err = cursor.clone();
                    let routes_clone = routes.clone();
                    let system_prompt_clone = system_prompt.clone();
                    let model_clone = model.clone();
                    let provider_clone = provider.clone();
                    let agent_ref_clone = agent_ref.clone();

                    let (raw, replayed) = self
                        .visit_effect(tenant, run_id, &cursor, attempt, || {
                            let req = SupervisorRequest {
                                node_id: node_id_for_err.clone(),
                                system_prompt: system_prompt_clone,
                                model: model_clone,
                                routes: routes_clone.clone(),
                                state: state.clone(),
                                provider: provider_clone,
                                agent_ref: agent_ref_clone,
                            };
                            let fut = (self.supervisor)(req);
                            Box::pin(async move {
                                let r = fut.await.map_err(|e| {
                                    GraphExecError::Supervisor(format!(
                                        "node '{}' attempt {}: {}",
                                        node_id_for_err, attempt, e
                                    ))
                                })?;
                                serde_json::to_value(&r)
                                    .map_err(CheckpointError::Serde)
                                    .map_err(GraphExecError::Checkpoint)
                            })
                        })
                        .await?;

                    let result: SupervisorResult =
                        serde_json::from_value(raw).map_err(CheckpointError::Serde)?;

                    // Validate that the chosen branch is one of the declared routes
                    // AND has a matching outgoing edge.
                    let branch = &result.branch;
                    let branch_is_valid_route = routes.iter().any(|r| &r.branch == branch);
                    let matching_edge = cfg
                        .graph
                        .edges_from(&cursor)
                        .find(|e| e.branch.as_deref() == Some(branch.as_str()));

                    let next_cursor = match (branch_is_valid_route, matching_edge) {
                        (true, Some(edge)) => edge.to.clone(),
                        _ => {
                            return Err(GraphExecError::Graph(super::model::GraphError::Invalid(
                                format!(
                                    "supervisor node '{}': branch '{}' returned by supervisor \
                                     does not match any declared route or outgoing edge",
                                    cursor, branch
                                ),
                            )));
                        }
                    };

                    trail.push(serde_json::json!({
                        "node": cursor,
                        "kind": "supervisor",
                        "attempt": attempt,
                        "replayed": replayed,
                        "branch": result.branch,
                    }));

                    visits.insert(cursor.clone(), attempt);
                    state.push_message(GraphRole::Assistant, &result.raw_reply);

                    // Supervisor does NOT increment iterations.
                    cursor = next_cursor;

                    let rec = build_record(
                        run_id,
                        &serde_json::to_string(&cfg).map_err(CheckpointError::Serde)?,
                        &cursor,
                        &state,
                        &visits,
                        RunStatus::Running,
                    )?;
                    self.store.save(tenant, &rec).await?;
                }

                // v2 Parallel — fan out into a concurrent frontier, drive every
                // branch to the join, deterministically merge, and resume the
                // trunk at the join's successor. The whole region runs inside
                // `run_parallel_region`, which advances `cursor`, `state`, and
                // `visits` past the join.
                NodeKind::Parallel => {
                    let (next_cursor, merged_state, merged_visits, region_trail) = self
                        .run_parallel_region(tenant, run_id, &cfg, &cursor, &state, &visits)
                        .await?;
                    trail.extend(region_trail);
                    cursor = next_cursor;
                    state = merged_state;
                    visits = merged_visits;
                    // Continue the trunk loop from the join's successor.
                }

                // v2 Join — the join node itself is consumed by the parallel
                // arm (the trunk resumes at the join's OUTGOING target after the
                // merge), so the single-cursor loop should never land here. If a
                // malformed graph routes here directly, treat it as a defensive
                // pass-through: advance to its single outgoing edge. Validation
                // guarantees a join has exactly one outgoing edge.
                NodeKind::Join => {
                    tracing::warn!(
                        node = %cursor,
                        "trunk drive loop reached a join node directly; \
                         passing through to its successor (expected to be \
                         consumed by the parallel arm)"
                    );
                    cursor = next_linear(&cfg, &cursor)?;
                }

                // Human-in-the-loop approval gate. Ask the host's `ApprovalFn`
                // whether a decision has arrived yet:
                // - `Awaiting`: park the run (`RunStatus::AwaitingInput`) with
                //   the cursor still AT this node (not advanced) and return
                //   the outcome cleanly, without recording a node visit — the
                //   next `drive` pass (via `resume`) re-asks the same
                //   question.
                // - `Decided { branch }`: select the outgoing edge whose
                //   label matches `branch`, record the decision as this
                //   node's visit, and advance the loop.
                NodeKind::Approval {
                    title,
                    mode,
                    risk_threshold,
                    confidence_threshold,
                    deadline_ms,
                } => {
                    let req = ApprovalRequest {
                        run_id: run_id.to_string(),
                        node_id: cursor.clone(),
                        tenant: tenant.tenant_id.clone(),
                        title: title.clone(),
                        mode: mode.clone(),
                        risk_threshold: *risk_threshold,
                        confidence_threshold: *confidence_threshold,
                        deadline_ms: *deadline_ms,
                        state: state.clone(),
                    };

                    match (self.approval)(req).await? {
                        ApprovalOutcome::Awaiting => {
                            // Park: persist AwaitingInput with cursor still at
                            // THIS node (not advanced). Do NOT record a node
                            // visit for the park itself — only a `Decided`
                            // outcome produces a ledgered visit.
                            let rec = build_record(
                                run_id,
                                &serde_json::to_string(&cfg).map_err(CheckpointError::Serde)?,
                                &cursor,
                                &state,
                                &visits,
                                RunStatus::AwaitingInput,
                            )?;
                            self.store.save(tenant, &rec).await?;

                            let reply = last_assistant_message(&state);
                            return Ok(GraphRunOutcome {
                                status: RunStatus::AwaitingInput,
                                reply,
                                trail,
                            });
                        }
                        ApprovalOutcome::Decided { branch } => {
                            // Select the outgoing edge whose label matches the
                            // decided branch — same lookup idiom the
                            // Supervisor arm uses for its branch label.
                            let matching_edge = cfg
                                .graph
                                .edges_from(&cursor)
                                .find(|e| e.branch.as_deref() == Some(branch.as_str()));
                            let next_cursor = match matching_edge {
                                Some(edge) => edge.to.clone(),
                                None => {
                                    return Err(GraphExecError::Graph(GraphError::Invalid(
                                        format!(
                                            "approval node '{}': decision branch '{}' does not \
                                             match any outgoing edge",
                                            cursor, branch
                                        ),
                                    )));
                                }
                            };

                            let attempt = *visits.get(&cursor).unwrap_or(&0) + 1;
                            self.store
                                .record_node_visit(
                                    tenant,
                                    run_id,
                                    &cursor,
                                    attempt,
                                    &serde_json::json!({"decision": branch}),
                                )
                                .await?;

                            trail.push(serde_json::json!({
                                "node": cursor,
                                "kind": "approval",
                                "attempt": attempt,
                                "replayed": false,
                                "branch": branch,
                            }));

                            visits.insert(cursor.clone(), attempt);
                            cursor = next_cursor;

                            let rec = build_record(
                                run_id,
                                &serde_json::to_string(&cfg).map_err(CheckpointError::Serde)?,
                                &cursor,
                                &state,
                                &visits,
                                RunStatus::Running,
                            )?;
                            self.store.save(tenant, &rec).await?;
                        }
                    }
                }
            }
        }

        // Cap exhausted — mark as Failed first, then propagate the error.
        let graph_json = serde_json::to_string(&cfg).map_err(CheckpointError::Serde)?;
        let rec = build_record(
            run_id,
            &graph_json,
            &cursor,
            &state,
            &visits,
            RunStatus::Failed,
        )?;
        self.store.save(tenant, &rec).await?;

        Err(GraphExecError::IterationCap {
            run_id: run_id.to_owned(),
        })
    }

    /// Load-or-invoke-and-record a single side-effect node visit.
    ///
    /// Returns `(serialised_result, replayed)`:
    /// - `replayed = true` means the result was already stored (crash recovery);
    ///   the `invoke` closure was NOT called.
    /// - `replayed = false` means `invoke` was called and its result has been
    ///   durably recorded before returning.
    ///
    /// The `invoke` closure is responsible for wrapping any effect-level error
    /// with `node_id`/`attempt` context **before** returning it here, so that
    /// `?`-propagation in the caller carries the full diagnostic.
    async fn visit_effect(
        &self,
        tenant: &TenantContext,
        run_id: &str,
        node_id: &str,
        attempt: u32,
        invoke: impl FnOnce() -> BoxFut<'static, Result<serde_json::Value, GraphExecError>>,
    ) -> Result<(serde_json::Value, bool), GraphExecError> {
        if let Some(cached) = self
            .store
            .load_node_visit(tenant, run_id, node_id, attempt)
            .await?
        {
            return Ok((cached, true));
        }

        // Effect not yet recorded — invoke and record before returning.
        let value = invoke().await?;
        // Record BEFORE checkpoint (replayable resume ordering).
        self.store
            .record_node_visit(tenant, run_id, node_id, attempt, &value)
            .await?;
        Ok((value, false))
    }

    // ------------------------------------------------------------------
    // Parallel region driver
    // ------------------------------------------------------------------

    /// Fan out a [`NodeKind::Parallel`] node into concurrent branch drives,
    /// merge deterministically at the join, and return the trunk continuation.
    ///
    /// Returns `(next_cursor, merged_state, merged_visits, trail)` where
    /// `next_cursor` is the join's single outgoing target.
    #[allow(clippy::type_complexity)]
    async fn run_parallel_region(
        &self,
        tenant: &TenantContext,
        run_id: &str,
        cfg: &GraphConfig,
        parallel_node: &str,
        trunk_state: &GraphRunState,
        trunk_visits: &HashMap<String, u32>,
    ) -> Result<
        (
            String,
            GraphRunState,
            HashMap<String, u32>,
            Vec<serde_json::Value>,
        ),
        GraphExecError,
    > {
        // Enumerate branch edges sorted by label (deterministic ordering).
        let mut branch_edges: Vec<(String, String)> = cfg
            .graph
            .edges_from(parallel_node)
            .filter_map(|e| e.branch.clone().map(|b| (b, e.to.clone())))
            .collect();
        branch_edges.sort_by(|a, b| a.0.cmp(&b.0));

        // Resolve the single join node for this region.
        let join_id = find_join_for_parallel(cfg, parallel_node)?;

        // Snapshot the trunk state once per branch (isolated clones).
        let trunk_state_json =
            serde_json::to_string(trunk_state).map_err(CheckpointError::Serde)?;
        let frontier: Vec<BranchCursor> = branch_edges
            .iter()
            .map(|(branch, target)| BranchCursor {
                branch: branch.clone(),
                cursor: target.clone(),
                state_json: trunk_state_json.clone(),
                parked: false,
            })
            .collect();

        self.drive_frontier(
            tenant,
            run_id,
            cfg,
            parallel_node,
            &join_id,
            trunk_state,
            trunk_visits,
            frontier,
            /* persist_before_driving = */ true,
        )
        .await
    }

    /// Resume an in-flight parallel region from a reconstructed frontier.
    ///
    /// Parked branches are already done; non-parked branches re-drive from
    /// their last good cursor, replaying ledgered visits.
    #[allow(clippy::too_many_arguments, clippy::type_complexity)]
    async fn resume_parallel(
        &self,
        tenant: &TenantContext,
        run_id: &str,
        cfg: &GraphConfig,
        parallel_node: &str,
        trunk_state: GraphRunState,
        trunk_visits: HashMap<String, u32>,
        frontier: Vec<BranchCursor>,
    ) -> Result<GraphRunOutcome, GraphExecError> {
        let join_id = find_join_for_parallel(cfg, parallel_node)?;

        let (next_cursor, merged_state, merged_visits, _region_trail) = self
            .drive_frontier(
                tenant,
                run_id,
                cfg,
                parallel_node,
                &join_id,
                &trunk_state,
                &trunk_visits,
                frontier,
                /* persist_before_driving = */ false,
            )
            .await?;

        // The merge is committed; continue the trunk drive loop from the
        // join's successor.
        self.drive(
            tenant,
            run_id,
            cfg.clone(),
            next_cursor,
            merged_state,
            merged_visits,
        )
        .await
    }

    /// Drive the supplied `frontier` of branches concurrently to the join, then
    /// deterministically merge. Shared between fresh fan-out and resume.
    ///
    /// `persist_before_driving` checkpoints the initial frontier before driving
    /// (fresh fan-out path); on resume the frontier is already durable.
    #[allow(clippy::too_many_arguments, clippy::type_complexity)]
    async fn drive_frontier(
        &self,
        tenant: &TenantContext,
        run_id: &str,
        cfg: &GraphConfig,
        parallel_node: &str,
        join_id: &str,
        trunk_state: &GraphRunState,
        trunk_visits: &HashMap<String, u32>,
        frontier: Vec<BranchCursor>,
        persist_before_driving: bool,
    ) -> Result<
        (
            String,
            GraphRunState,
            HashMap<String, u32>,
            Vec<serde_json::Value>,
        ),
        GraphExecError,
    > {
        let graph_json = serde_json::to_string(cfg).map_err(CheckpointError::Serde)?;
        let trunk_state_json =
            serde_json::to_string(trunk_state).map_err(CheckpointError::Serde)?;

        let coord = Arc::new(FrontierCheckpoint {
            run_id: run_id.to_owned(),
            graph_json: graph_json.clone(),
            parallel_node: parallel_node.to_owned(),
            trunk_state_json,
            trunk_visits: trunk_visits.clone(),
            inner: AsyncMutex::new(frontier.clone()),
            branch_visits: AsyncMutex::new(HashMap::new()),
        });

        // Persist the initial frontier BEFORE driving (fresh fan-out only).
        if persist_before_driving {
            coord.checkpoint(self.store.as_ref(), tenant).await?;
        }

        // Global visit counter, seeded with the trunk's current visit total.
        let trunk_visit_total: u32 = trunk_visits.values().copied().sum();
        let global_visits = Arc::new(AtomicU32::new(trunk_visit_total));

        // Build one future per branch (skipping already-parked branches).
        let mut futs: Vec<BoxFut<'_, Result<BranchOutcome, GraphExecError>>> = Vec::new();
        for (slot, bc) in frontier.iter().enumerate() {
            if bc.parked {
                continue;
            }
            let bc = bc.clone();
            let coord = coord.clone();
            let global_visits = global_visits.clone();
            futs.push(Box::pin(self.drive_branch(
                tenant,
                run_id,
                cfg,
                join_id,
                slot,
                bc,
                trunk_visits.clone(),
                coord,
                global_visits,
            )));
        }

        // Settle ALL branch futures, THEN inspect results. We never cancel an
        // in-flight branch: cancelling mid-effect could drop an effect after it
        // ran but before it was recorded, breaking crash-replay. So we let every
        // branch run its current await to completion (each branch's per-node
        // checkpoint persists its last good cursor), collect outcomes, and only
        // afterwards propagate the FIRST error. This "settle then propagate"
        // policy guarantees every completed visit is durably recorded; the run
        // stays Running so resume re-drives just the non-parked branches.
        let results = futures::future::join_all(futs).await;

        let mut first_err: Option<GraphExecError> = None;
        let mut trail: Vec<serde_json::Value> = Vec::new();
        let mut merged_visits = trunk_visits.clone();
        for r in results {
            match r {
                Ok(outcome) => {
                    trail.extend(outcome.trail);
                    // Disjoint keys by validation — no conflict on insert.
                    for (k, v) in outcome.visits {
                        merged_visits.insert(k, v);
                    }
                }
                Err(e) if first_err.is_none() => first_err = Some(e),
                Err(_) => { /* keep only the first error; others already settled */ }
            }
        }
        if let Some(e) = first_err {
            // A global visit-cap breach is terminal: write Failed exactly once,
            // now that every branch has settled, so no concurrent slot save can
            // clobber the terminal status. Effect errors (agent/tool/supervisor)
            // are NOT terminal — leave the run Running (the last good frontier is
            // already persisted) so resume re-drives only the non-parked branches.
            if matches!(e, GraphExecError::IterationCap { .. }) {
                let last_frontier = coord.inner.lock().await.clone();
                let frontier_json =
                    Some(serde_json::to_string(&last_frontier).map_err(CheckpointError::Serde)?);
                let rec = build_record_with_frontier(
                    run_id,
                    &graph_json,
                    parallel_node,
                    trunk_state,
                    trunk_visits,
                    RunStatus::Failed,
                    frontier_json,
                )?;
                self.store.save(tenant, &rec).await?;
            }
            return Err(e);
        }

        // All branches parked → deterministic merge in branch-label order.
        // Re-read the durable frontier (covers branches whose work was wholly
        // replayed on resume and thus produced no fresh BranchOutcome visits).
        let mut ordered = coord.inner.lock().await.clone();
        ordered.sort_by(|a, b| a.branch.cmp(&b.branch));

        let snapshot_len = trunk_state.messages.len();
        let mut merged_state = trunk_state.clone();

        for bc in &ordered {
            let branch_state: GraphRunState =
                serde_json::from_str(&bc.state_json).map_err(CheckpointError::Serde)?;

            // Append the branch's NEW messages (delta after the snapshot).
            for msg in branch_state.messages.iter().skip(snapshot_len) {
                merged_state.messages.push(msg.clone());
            }
            // resolved = any(branch.resolved).
            if branch_state.resolved {
                merged_state.resolved = true;
            }
            // iterations = max across branches and the trunk.
            merged_state.iterations = merged_state.iterations.max(branch_state.iterations);
            // scratchpad.branches.<label> = branch scratchpad.
            if !branch_state.scratchpad.is_null() {
                if !merged_state.scratchpad.is_object() {
                    merged_state.scratchpad = serde_json::json!({});
                }
                if let Some(obj) = merged_state.scratchpad.as_object_mut() {
                    let branches = obj
                        .entry("branches")
                        .or_insert_with(|| serde_json::json!({}));
                    if let Some(branches_obj) = branches.as_object_mut() {
                        branches_obj.insert(bc.branch.clone(), branch_state.scratchpad.clone());
                    }
                }
            }
        }

        // Also fold in any visit counts the coordinator accumulated (covers the
        // resume case where a branch parked via pure replay).
        for (k, v) in coord.collect_branch_visits().await {
            merged_visits.entry(k).or_insert(v);
        }

        // The join's single outgoing target is the trunk continuation.
        let next_cursor = next_linear(cfg, join_id)?;

        // Clear the frontier and checkpoint the merged trunk as Running before
        // returning control to the trunk loop.
        let rec = build_record(
            run_id,
            &graph_json,
            &next_cursor,
            &merged_state,
            &merged_visits,
            RunStatus::Running,
        )?;
        self.store.save(tenant, &rec).await?;

        Ok((next_cursor, merged_state, merged_visits, trail))
    }

    /// Drive ONE branch from its cursor to the join (exclusive). Mirrors the
    /// trunk node-execution logic (agent/tool/router/supervisor via
    /// `visit_effect`) but parks instead of executing the join.
    #[allow(clippy::too_many_arguments)]
    fn drive_branch<'a>(
        &'a self,
        tenant: &'a TenantContext,
        run_id: &'a str,
        cfg: &'a GraphConfig,
        join_id: &'a str,
        slot: usize,
        mut bc: BranchCursor,
        mut visits: HashMap<String, u32>,
        coord: Arc<FrontierCheckpoint>,
        global_visits: Arc<AtomicU32>,
    ) -> BoxFut<'a, Result<BranchOutcome, GraphExecError>> {
        Box::pin(async move {
            let mut state: GraphRunState =
                serde_json::from_str(&bc.state_json).map_err(CheckpointError::Serde)?;
            let mut trail: Vec<serde_json::Value> = Vec::new();
            let branch_label = bc.branch.clone();

            loop {
                // Reached the join → park without executing it.
                if bc.cursor == join_id {
                    bc.parked = true;
                    bc.state_json =
                        serde_json::to_string(&state).map_err(CheckpointError::Serde)?;
                    coord
                        .save_slot(self.store.as_ref(), tenant, slot, &bc)
                        .await?;
                    return Ok(BranchOutcome {
                        branch: branch_label,
                        visits,
                        trail,
                    });
                }

                // Global visit cap — count this visit BEFORE executing.
                // Return the error WITHOUT writing a terminal record here: with
                // concurrent branches, a Failed write from one branch could be
                // clobbered by another branch's later Running slot save. The
                // caller (`drive_frontier`) writes Failed exactly once after all
                // branches have settled, so the terminal status can't be lost.
                let total = global_visits.fetch_add(1, Ordering::SeqCst) + 1;
                if total > MAX_NODE_VISITS {
                    return Err(GraphExecError::IterationCap {
                        run_id: run_id.to_owned(),
                    });
                }

                let node = cfg
                    .graph
                    .node(&bc.cursor)
                    .ok_or_else(|| GraphExecError::UnknownNode(bc.cursor.clone()))?
                    .clone();

                match &node.kind {
                    NodeKind::Agent {
                        system_prompt,
                        model,
                        provider,
                        tools,
                        agent_ref,
                        inherit_from,
                    } => {
                        let attempt = *visits.get(&bc.cursor).unwrap_or(&0) + 1;
                        let node_id = bc.cursor.clone();
                        let sp = system_prompt.clone();
                        let md = model.clone();
                        let pv = provider.clone();
                        let tl = tools.clone();
                        let ar = agent_ref.clone();
                        let ih = inherit_from.clone();
                        let state_for_call = state.clone();
                        let (raw, replayed) = self
                            .visit_effect(tenant, run_id, &bc.cursor, attempt, || {
                                let req = AgentTurnRequest {
                                    node_id: node_id.clone(),
                                    system_prompt: sp,
                                    model: md,
                                    state: state_for_call,
                                    provider: pv,
                                    tools: tl,
                                    agent_ref: ar,
                                    inherit_from: ih,
                                };
                                let fut = (self.agent_turn)(req);
                                Box::pin(async move {
                                    let r = fut.await.map_err(|e| {
                                        GraphExecError::AgentTurn(format!(
                                            "node '{}' attempt {}: {}",
                                            node_id, attempt, e
                                        ))
                                    })?;
                                    serde_json::to_value(&r)
                                        .map_err(CheckpointError::Serde)
                                        .map_err(GraphExecError::Checkpoint)
                                })
                            })
                            .await?;
                        let result: AgentTurnResult =
                            serde_json::from_value(raw).map_err(CheckpointError::Serde)?;
                        trail.push(serde_json::json!({
                            "node": bc.cursor, "kind": "agent",
                            "attempt": attempt, "replayed": replayed, "branch": branch_label,
                        }));
                        visits.insert(bc.cursor.clone(), attempt);
                        state.iterations += 1;
                        state.push_message(GraphRole::Assistant, &result.reply);
                        if result.resolved {
                            state.resolved = true;
                        }
                        bc.cursor = next_linear(cfg, &bc.cursor)?;
                    }
                    NodeKind::Tool { tool_name } => {
                        let attempt = *visits.get(&bc.cursor).unwrap_or(&0) + 1;
                        let node_id = bc.cursor.clone();
                        let tn = tool_name.clone();
                        let state_for_call = state.clone();
                        let (result, replayed) = self
                            .visit_effect(tenant, run_id, &bc.cursor, attempt, || {
                                let req = ToolCallRequest {
                                    node_id: node_id.clone(),
                                    tool_name: tn,
                                    state: state_for_call,
                                };
                                let fut = (self.tool)(req);
                                Box::pin(async move {
                                    fut.await.map_err(|e| {
                                        GraphExecError::Tool(format!(
                                            "node '{}' attempt {}: {}",
                                            node_id, attempt, e
                                        ))
                                    })
                                })
                            })
                            .await?;
                        trail.push(serde_json::json!({
                            "node": bc.cursor, "kind": "tool",
                            "attempt": attempt, "replayed": replayed, "branch": branch_label,
                        }));
                        visits.insert(bc.cursor.clone(), attempt);
                        state.push_message(GraphRole::Tool, result.to_string());
                        bc.cursor = next_linear(cfg, &bc.cursor)?;
                    }
                    NodeKind::Router { .. } => {
                        let attempt = *visits.get(&bc.cursor).unwrap_or(&0) + 1;
                        let next = route(&cfg.graph, &bc.cursor, &state)?;
                        trail.push(serde_json::json!({
                            "node": bc.cursor, "kind": "router",
                            "attempt": attempt, "replayed": false, "branch": branch_label,
                        }));
                        visits.insert(bc.cursor.clone(), attempt);
                        bc.cursor = next;
                    }
                    NodeKind::Supervisor {
                        system_prompt,
                        model,
                        routes,
                        provider,
                        agent_ref,
                    } => {
                        let attempt = *visits.get(&bc.cursor).unwrap_or(&0) + 1;
                        let node_id = bc.cursor.clone();
                        let sp = system_prompt.clone();
                        let md = model.clone();
                        let pv = provider.clone();
                        let ar = agent_ref.clone();
                        let routes_clone = routes.clone();
                        let state_for_call = state.clone();
                        let (raw, replayed) = self
                            .visit_effect(tenant, run_id, &bc.cursor, attempt, || {
                                let req = SupervisorRequest {
                                    node_id: node_id.clone(),
                                    system_prompt: sp,
                                    model: md,
                                    routes: routes_clone.clone(),
                                    state: state_for_call,
                                    provider: pv,
                                    agent_ref: ar,
                                };
                                let fut = (self.supervisor)(req);
                                Box::pin(async move {
                                    let r = fut.await.map_err(|e| {
                                        GraphExecError::Supervisor(format!(
                                            "node '{}' attempt {}: {}",
                                            node_id, attempt, e
                                        ))
                                    })?;
                                    serde_json::to_value(&r)
                                        .map_err(CheckpointError::Serde)
                                        .map_err(GraphExecError::Checkpoint)
                                })
                            })
                            .await?;
                        let result: SupervisorResult =
                            serde_json::from_value(raw).map_err(CheckpointError::Serde)?;
                        let branch = &result.branch;
                        let matching_edge = cfg
                            .graph
                            .edges_from(&bc.cursor)
                            .find(|e| e.branch.as_deref() == Some(branch.as_str()));
                        let next_cursor =
                            match (routes.iter().any(|r| &r.branch == branch), matching_edge) {
                                (true, Some(edge)) => edge.to.clone(),
                                _ => {
                                    return Err(GraphExecError::Graph(GraphError::Invalid(
                                        format!(
                                            "supervisor node '{}': branch '{}' does not match any \
                                     declared route or outgoing edge",
                                            bc.cursor, branch
                                        ),
                                    )));
                                }
                            };
                        trail.push(serde_json::json!({
                            "node": bc.cursor, "kind": "supervisor",
                            "attempt": attempt, "replayed": replayed,
                            "branch": result.branch, "branch_path": branch_label,
                        }));
                        visits.insert(bc.cursor.clone(), attempt);
                        state.push_message(GraphRole::Assistant, &result.raw_reply);
                        bc.cursor = next_cursor;
                    }
                    NodeKind::Respond => {
                        // Validation forbids respond inside a parallel branch.
                        return Err(GraphExecError::Graph(GraphError::Invalid(format!(
                            "respond node '{}' inside parallel branch '{}' is not allowed",
                            bc.cursor, branch_label
                        ))));
                    }
                    NodeKind::Parallel | NodeKind::Join => {
                        // Validation forbids nested parallel; a join other than
                        // the region's join is unreachable. Guard defensively.
                        return Err(GraphExecError::Graph(GraphError::Invalid(format!(
                            "branch '{}' reached unexpected '{}' node '{}'",
                            branch_label,
                            node.kind.kind_name(),
                            bc.cursor
                        ))));
                    }
                    // Approval nodes inside a parallel branch are UNSUPPORTED
                    // in v1: parking a single branch mid-fan-out would need
                    // per-branch `AwaitingInput` semantics (which branch is
                    // parked vs. running, how the frontier round-trips a
                    // decision back into ONE slot) that the durable-frontier
                    // design does not model yet. Rather than silently
                    // mis-executing an approval gate (e.g. skipping it, or
                    // parking the whole region), fail loudly so a malformed
                    // graph is caught at drive time instead of producing a
                    // wrong decision. Revisit if/when parallel-region
                    // approval becomes a real requirement.
                    NodeKind::Approval { .. } => {
                        return Err(GraphExecError::Graph(GraphError::Invalid(format!(
                            "approval node '{}' inside parallel branch '{}': not supported in v1 (parallel-branch approval parking is unimplemented)",
                            bc.cursor, branch_label
                        ))));
                    }
                }

                // Durably persist this branch's progress after every node.
                bc.state_json = serde_json::to_string(&state).map_err(CheckpointError::Serde)?;
                coord
                    .save_slot(self.store.as_ref(), tenant, slot, &bc)
                    .await?;
                // Record this branch's visit counts in the coordinator so the
                // merge can fold them into the trunk visits map.
                coord.record_branch_visits(&visits).await;
            }
        })
    }
}

// ---------------------------------------------------------------------------
// BranchOutcome — what a single branch drive returns
// ---------------------------------------------------------------------------

/// Result of driving one branch to its join.
struct BranchOutcome {
    #[allow(dead_code)]
    branch: String,
    /// This branch's private visits map (disjoint keys; merged into trunk).
    visits: HashMap<String, u32>,
    /// Trail entries produced by this branch.
    trail: Vec<serde_json::Value>,
}

impl FrontierCheckpoint {
    /// Update one branch slot and persist the whole record under the mutex.
    ///
    /// Holding the lock across the `save` await is what linearises the
    /// otherwise-concurrent blob writes (see the module-level concurrency note).
    async fn save_slot(
        &self,
        store: &dyn CheckpointStore,
        tenant: &TenantContext,
        slot: usize,
        bc: &BranchCursor,
    ) -> Result<(), GraphExecError> {
        let mut guard = self.inner.lock().await;
        if let Some(existing) = guard.get_mut(slot) {
            *existing = bc.clone();
        }
        let frontier_json = Some(serde_json::to_string(&*guard).map_err(CheckpointError::Serde)?);
        let trunk_state: GraphRunState =
            serde_json::from_str(&self.trunk_state_json).map_err(CheckpointError::Serde)?;
        let rec = build_record_with_frontier(
            &self.run_id,
            &self.graph_json,
            &self.parallel_node,
            &trunk_state,
            &self.trunk_visits,
            RunStatus::Running,
            frontier_json,
        )?;
        store.save(tenant, &rec).await?;
        Ok(())
    }

    /// Persist the current frontier without changing any slot (initial save).
    async fn checkpoint(
        &self,
        store: &dyn CheckpointStore,
        tenant: &TenantContext,
    ) -> Result<(), GraphExecError> {
        let guard = self.inner.lock().await;
        let frontier_json = Some(serde_json::to_string(&*guard).map_err(CheckpointError::Serde)?);
        let trunk_state: GraphRunState =
            serde_json::from_str(&self.trunk_state_json).map_err(CheckpointError::Serde)?;
        let rec = build_record_with_frontier(
            &self.run_id,
            &self.graph_json,
            &self.parallel_node,
            &trunk_state,
            &self.trunk_visits,
            RunStatus::Running,
            frontier_json,
        )?;
        store.save(tenant, &rec).await?;
        Ok(())
    }

    /// Fold a branch's private visit counts into the shared collector.
    async fn record_branch_visits(&self, visits: &HashMap<String, u32>) {
        let mut guard = self.branch_visits.lock().await;
        for (k, v) in visits {
            guard.insert(k.clone(), *v);
        }
    }

    /// Snapshot the merged branch visit counts (disjoint keys by validation).
    async fn collect_branch_visits(&self) -> HashMap<String, u32> {
        self.branch_visits.lock().await.clone()
    }
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// Serialise all fields into a [`GraphRunRecord`] (no parallel frontier).
fn build_record(
    run_id: &str,
    graph_json: &str,
    cursor: &str,
    state: &GraphRunState,
    visits: &HashMap<String, u32>,
    status: RunStatus,
) -> Result<GraphRunRecord, GraphExecError> {
    build_record_with_frontier(run_id, graph_json, cursor, state, visits, status, None)
}

/// Serialise all fields into a [`GraphRunRecord`], including an optional
/// parallel `frontier_json`.
fn build_record_with_frontier(
    run_id: &str,
    graph_json: &str,
    cursor: &str,
    state: &GraphRunState,
    visits: &HashMap<String, u32>,
    status: RunStatus,
    frontier_json: Option<String>,
) -> Result<GraphRunRecord, GraphExecError> {
    let state_json = serde_json::to_string(state).map_err(CheckpointError::Serde)?;
    let visits_json = serde_json::to_string(visits).map_err(CheckpointError::Serde)?;
    Ok(GraphRunRecord {
        run_id: run_id.to_owned(),
        graph_json: graph_json.to_owned(),
        cursor: cursor.to_owned(),
        state_json,
        status,
        visits_json,
        frontier_json,
    })
}

/// Return the single outgoing edge target, or an error if none.
fn next_linear(cfg: &GraphConfig, id: &str) -> Result<String, GraphExecError> {
    cfg.graph
        .edges_from(id)
        .next()
        .map(|e| e.to.clone())
        .ok_or_else(|| {
            GraphExecError::Graph(super::model::GraphError::Invalid(format!(
                "node '{id}' has no outgoing edge"
            )))
        })
}

/// Resolve the single join node for a parallel region by forward BFS from the
/// parallel node, returning the first [`NodeKind::Join`] reached.
///
/// Validation guarantees every branch converges on the SAME join and that no
/// nested parallel exists, so the first join found is THE region's join.
fn find_join_for_parallel(
    cfg: &GraphConfig,
    parallel_node: &str,
) -> Result<String, GraphExecError> {
    use std::collections::HashSet;
    let mut visited: HashSet<String> = HashSet::new();
    let mut queue: Vec<String> = cfg
        .graph
        .edges_from(parallel_node)
        .map(|e| e.to.clone())
        .collect();
    while let Some(current) = queue.pop() {
        if !visited.insert(current.clone()) {
            continue;
        }
        match cfg.graph.node(&current) {
            Some(n) if matches!(n.kind, NodeKind::Join) => return Ok(current),
            Some(_) => {
                for e in cfg.graph.edges_from(&current) {
                    queue.push(e.to.clone());
                }
            }
            None => {
                return Err(GraphExecError::UnknownNode(current));
            }
        }
    }
    Err(GraphExecError::Graph(GraphError::Invalid(format!(
        "parallel node '{parallel_node}' has no reachable join node"
    ))))
}

/// The most recent assistant message content, or empty string if none.
fn last_assistant_message(state: &GraphRunState) -> String {
    state
        .messages
        .iter()
        .rev()
        .find(|m| m.role == GraphRole::Assistant)
        .map(|m| m.content.clone())
        .unwrap_or_default()
}

/// Rebuild a minimal trail from the state message log (used when returning
/// a stored terminal outcome without re-driving).
fn rebuild_trail_from_state(state: &GraphRunState) -> Vec<serde_json::Value> {
    state
        .messages
        .iter()
        .map(|m| {
            let kind = match m.role {
                GraphRole::User => "user",
                GraphRole::Assistant => "agent",
                GraphRole::Tool => "tool",
            };
            serde_json::json!({"kind": kind, "content": m.content})
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;
    use crate::graph::test_fixtures::{parallel_json, supervisor_json, triage_json};
    use crate::graph::{GraphConfig, InMemoryCheckpointStore};
    use crate::tenant::TenantContext;

    // -----------------------------------------------------------------------
    // Test helpers
    // -----------------------------------------------------------------------

    fn tenant() -> TenantContext {
        TenantContext::new("test", "dev")
    }

    fn triage_cfg() -> GraphConfig {
        GraphConfig::from_json(&triage_json()).expect("fixture is valid")
    }

    fn supervisor_cfg() -> GraphConfig {
        GraphConfig::from_json(&supervisor_json()).expect("supervisor fixture is valid")
    }

    fn parallel_cfg() -> GraphConfig {
        GraphConfig::from_json(&parallel_json()).expect("parallel fixture is valid")
    }

    /// Build an [`AgentTurnFn`] that resolves on the n-th call (1-indexed).
    /// `counter` is incremented on every (non-replayed) invocation.
    fn agent_fn_resolves_on(counter: Arc<AtomicU32>, resolve_on_call: u32) -> AgentTurnFn {
        Arc::new(move |req: AgentTurnRequest| {
            let n = counter.fetch_add(1, Ordering::SeqCst) + 1; // 1-indexed
            let resolved = n >= resolve_on_call;
            let reply = format!("reply-{n} from {}", req.node_id);
            Box::pin(async move { Ok(AgentTurnResult { reply, resolved }) })
        })
    }

    /// Always-succeed tool fn with a counter.
    fn tool_fn_counting(counter: Arc<AtomicU32>) -> ToolFn {
        Arc::new(move |_req: ToolCallRequest| {
            counter.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move { Ok(serde_json::json!({"found": true})) })
        })
    }

    /// A no-op supervisor fn that always returns an error (for v1 tests that
    /// never reach a supervisor node).
    fn supervisor_fn_unreachable() -> SupervisorFn {
        Arc::new(|_req: SupervisorRequest| {
            Box::pin(async move {
                Err(GraphExecError::Supervisor(
                    "supervisor fn should not be called in this test".into(),
                ))
            })
        })
    }

    /// Build a supervisor fn that always routes to `branch`, counting calls.
    fn supervisor_fn_always_routes_to(
        counter: Arc<AtomicU32>,
        branch: &'static str,
    ) -> SupervisorFn {
        Arc::new(move |req: SupervisorRequest| {
            counter.fetch_add(1, Ordering::SeqCst);
            let node = req.node_id.clone();
            Box::pin(async move {
                Ok(SupervisorResult {
                    branch: branch.to_string(),
                    raw_reply: format!("[[ROUTE:{branch}]] from supervisor at {node}"),
                })
            })
        })
    }

    /// A trivial approval fn that always reports `Awaiting` — a safe default
    /// for tests that never route through a `NodeKind::Approval` node.
    fn approval_fn_awaiting() -> ApprovalFn {
        Arc::new(|_req: ApprovalRequest| Box::pin(async move { Ok(ApprovalOutcome::Awaiting) }))
    }

    /// Build an approval fn that always reports `Decided { branch }`,
    /// counting calls.
    fn approval_fn_decides(counter: Arc<AtomicU32>, branch: &'static str) -> ApprovalFn {
        Arc::new(move |_req: ApprovalRequest| {
            counter.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                Ok(ApprovalOutcome::Decided {
                    branch: branch.to_string(),
                })
            })
        })
    }

    // -----------------------------------------------------------------------
    // Test 1: happy path — resolves on first agent pass
    // -----------------------------------------------------------------------

    /// Path: agent → lookup → router → respond
    /// Agent resolves on attempt 1 → router takes "resolved" branch → Succeed.
    #[tokio::test]
    async fn happy_path_resolves_first_pass() {
        let store = Arc::new(InMemoryCheckpointStore::default());
        let agent_count = Arc::new(AtomicU32::new(0));
        let tool_count = Arc::new(AtomicU32::new(0));

        let exec = GraphExecutor::new(
            store.clone(),
            agent_fn_resolves_on(agent_count.clone(), 1),
            tool_fn_counting(tool_count.clone()),
            supervisor_fn_unreachable(),
            approval_fn_awaiting(),
        );

        let outcome = exec
            .start(&tenant(), "run-happy", &triage_cfg(), "help me")
            .await
            .expect("should succeed");

        assert_eq!(outcome.status, RunStatus::Succeeded, "status");
        assert!(
            outcome.reply.contains("reply-1"),
            "reply should contain agent output: {:?}",
            outcome.reply
        );
        assert_eq!(agent_count.load(Ordering::SeqCst), 1, "agent invoked once");
        assert_eq!(tool_count.load(Ordering::SeqCst), 1, "tool invoked once");
        // Trail: agent, tool, router, respond = 4 entries
        assert_eq!(outcome.trail.len(), 4, "trail: {:?}", outcome.trail);
    }

    // -----------------------------------------------------------------------
    // Test 2: loops until router cap, then resolves
    // -----------------------------------------------------------------------

    /// triage_json has maxIterations=3.  Agent never self-resolves.
    /// After 3 iterations, router takes "resolved" branch → Succeeded.
    #[tokio::test]
    async fn loops_until_router_cap_then_resolves_via_cap() {
        let store = Arc::new(InMemoryCheckpointStore::default());
        let agent_count = Arc::new(AtomicU32::new(0));

        // resolve_on_call = u32::MAX → never resolves on its own
        let exec = GraphExecutor::new(
            store.clone(),
            agent_fn_resolves_on(agent_count.clone(), u32::MAX),
            tool_fn_counting(Arc::new(AtomicU32::new(0))),
            supervisor_fn_unreachable(),
            approval_fn_awaiting(),
        );

        let outcome = exec
            .start(&tenant(), "run-cap", &triage_cfg(), "loop me")
            .await
            .expect("should succeed via iteration cap");

        assert_eq!(outcome.status, RunStatus::Succeeded);
        assert_eq!(
            agent_count.load(Ordering::SeqCst),
            3,
            "agent should be invoked exactly 3 times (maxIterations=3)"
        );
    }

    // -----------------------------------------------------------------------
    // Test 3: resume on a succeeded run returns stored outcome, no re-invoke
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn resume_on_succeeded_run_returns_stored_outcome_without_reinvoking() {
        let store = Arc::new(InMemoryCheckpointStore::default());
        let agent_count = Arc::new(AtomicU32::new(0));
        let tool_count = Arc::new(AtomicU32::new(0));

        let exec = GraphExecutor::new(
            store.clone(),
            agent_fn_resolves_on(agent_count.clone(), 1),
            tool_fn_counting(tool_count.clone()),
            supervisor_fn_unreachable(),
            approval_fn_awaiting(),
        );

        // Drive to completion.
        exec.start(&tenant(), "run-resume-done", &triage_cfg(), "hi")
            .await
            .expect("first run succeeds");

        let after_start_agent = agent_count.load(Ordering::SeqCst);
        let after_start_tool = tool_count.load(Ordering::SeqCst);

        // resume should return Succeeded without calling agent or tool again.
        let outcome = exec
            .resume(&tenant(), "run-resume-done")
            .await
            .expect("resume should succeed");

        assert_eq!(outcome.status, RunStatus::Succeeded);
        assert_eq!(
            agent_count.load(Ordering::SeqCst),
            after_start_agent,
            "agent must NOT be called again on resume of terminal run"
        );
        assert_eq!(
            tool_count.load(Ordering::SeqCst),
            after_start_tool,
            "tool must NOT be called again on resume of terminal run"
        );
    }

    // -----------------------------------------------------------------------
    // Test 4: global visit cap fails the run
    // -----------------------------------------------------------------------

    /// Build a variant of triage with maxIterations=1000 (far above MAX_NODE_VISITS).
    /// The global cap of 64 should fire first, saving the record as Failed.
    #[tokio::test]
    async fn global_visit_cap_fails_run() {
        let store = Arc::new(InMemoryCheckpointStore::default());

        // Clone triage fixture and patch maxIterations on the router node.
        let mut v: serde_json::Value = serde_json::from_str(&triage_json()).expect("fixture JSON");
        for node in v["nodes"].as_array_mut().expect("nodes array") {
            if node["kind"] == "router" {
                node["maxIterations"] = serde_json::json!(1000);
            }
        }
        let cfg = GraphConfig::from_json(&v.to_string()).expect("patched graph valid");

        let exec = GraphExecutor::new(
            store.clone(),
            // never resolves
            agent_fn_resolves_on(Arc::new(AtomicU32::new(0)), u32::MAX),
            tool_fn_counting(Arc::new(AtomicU32::new(0))),
            supervisor_fn_unreachable(),
            approval_fn_awaiting(),
        );

        let err = exec
            .start(&tenant(), "run-global-cap", &cfg, "infinite loop")
            .await
            .expect_err("should fail with IterationCap");

        assert!(
            matches!(err, GraphExecError::IterationCap { .. }),
            "expected IterationCap, got {err:?}"
        );

        // The stored record must be Failed.
        let rec = store
            .load(&tenant(), "run-global-cap")
            .await
            .expect("store accessible")
            .expect("record must exist");
        assert_eq!(
            rec.status,
            RunStatus::Failed,
            "stored status must be Failed"
        );
    }

    // -----------------------------------------------------------------------
    // Test 5: effect error leaves run resumable; replay prevents re-invoke
    // -----------------------------------------------------------------------

    /// Agent succeeds (unresolved) on attempt 1, then fails on attempt 2.
    /// start() returns Err; record stays Running.
    /// resume() with a closure that resolves on attempt 1 (= attempt 2 of the
    /// node, but attempt 1 of the fresh closure) → Succeeded.
    /// Replay must prevent attempt-1 from being re-executed.
    #[tokio::test]
    async fn effect_error_leaves_run_resumable() {
        let store = Arc::new(InMemoryCheckpointStore::default());
        let t = tenant();

        // Phase 1: agent call #1 → ok/unresolved; call #2 → error.
        let phase1_count = Arc::new(AtomicU32::new(0));
        {
            let pc = phase1_count.clone();
            let agent_phase1: AgentTurnFn = Arc::new(move |req: AgentTurnRequest| {
                let n = pc.fetch_add(1, Ordering::SeqCst) + 1;
                let node = req.node_id.clone();
                Box::pin(async move {
                    if n == 1 {
                        Ok(AgentTurnResult {
                            reply: format!("pass-{n} from {node}"),
                            resolved: false,
                        })
                    } else {
                        Err(GraphExecError::AgentTurn(
                            "simulated failure on attempt 2".into(),
                        ))
                    }
                })
            });

            let tool_count = Arc::new(AtomicU32::new(0));
            let store_ref = store.clone();
            let exec = GraphExecutor::new(
                store_ref,
                agent_phase1,
                tool_fn_counting(tool_count.clone()),
                supervisor_fn_unreachable(),
                approval_fn_awaiting(),
            );

            let err = exec
                .start(&t, "run-resumable", &triage_cfg(), "retry me")
                .await
                .expect_err("should fail on agent attempt 2");

            assert!(
                matches!(err, GraphExecError::AgentTurn(_)),
                "expected AgentTurn error, got {err:?}"
            );

            // Record must still be Running.
            let rec = store
                .load(&t, "run-resumable")
                .await
                .expect("store ok")
                .expect("record exists");
            assert_eq!(
                rec.status,
                RunStatus::Running,
                "run should stay Running after effect error"
            );
        }

        // Phase 2: resume with a closure that resolves on its first call
        // (= attempt 2 of the "agent" node, but the phase2 closure only sees
        // calls that were NOT replayed).
        let phase2_count = Arc::new(AtomicU32::new(0));
        let tool_phase2_count = Arc::new(AtomicU32::new(0));
        {
            let pc2 = phase2_count.clone();
            let agent_phase2: AgentTurnFn = Arc::new(move |_req: AgentTurnRequest| {
                pc2.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move {
                    Ok(AgentTurnResult {
                        reply: "resolved!".into(),
                        resolved: true,
                    })
                })
            });

            let exec2 = GraphExecutor::new(
                store.clone(),
                agent_phase2,
                tool_fn_counting(tool_phase2_count.clone()),
                supervisor_fn_unreachable(),
                approval_fn_awaiting(),
            );

            let outcome = exec2
                .resume(&t, "run-resumable")
                .await
                .expect("resume should succeed");

            assert_eq!(outcome.status, RunStatus::Succeeded, "outcome status");
        }

        // Attempt 1 was replayed → phase2 agent called exactly once.
        assert_eq!(
            phase2_count.load(Ordering::SeqCst),
            1,
            "phase2 agent must be called exactly once (attempt-1 was replayed)"
        );
        // Attempt 1 of the tool node was replayed (already recorded in phase 1).
        // Attempt 2 of the tool node (second loop pass) is a new invocation.
        // So phase2 tool is called exactly once — for attempt 2, not for the
        // replayed attempt 1.
        assert_eq!(
            tool_phase2_count.load(Ordering::SeqCst),
            1,
            "phase2 tool must be called once (attempt-1 replayed, attempt-2 is fresh)"
        );
    }

    // -----------------------------------------------------------------------
    // Test 6: start twice with same run id after completion → AlreadyCompleted
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn start_twice_with_same_run_id_after_completion_errors() {
        let store = Arc::new(InMemoryCheckpointStore::default());

        let exec = GraphExecutor::new(
            store.clone(),
            agent_fn_resolves_on(Arc::new(AtomicU32::new(0)), 1),
            tool_fn_counting(Arc::new(AtomicU32::new(0))),
            supervisor_fn_unreachable(),
            approval_fn_awaiting(),
        );

        exec.start(&tenant(), "run-dup", &triage_cfg(), "first")
            .await
            .expect("first start succeeds");

        let err = exec
            .start(&tenant(), "run-dup", &triage_cfg(), "second attempt")
            .await
            .expect_err("second start must fail");

        assert!(
            matches!(err, GraphExecError::AlreadyCompleted(_)),
            "expected AlreadyCompleted, got {err:?}"
        );
    }

    // -----------------------------------------------------------------------
    // Supervisor tests (Task 2)
    // -----------------------------------------------------------------------
    //
    // Supervisor fixture topology (from test_fixtures::supervisor_json):
    //
    //   sup (supervisor: routes=[billing, tech])
    //    ├─[billing]─► agent_billing ─► router_billing ─┬─[loop]──► sup
    //    │                                               └─[resolved]─► respond
    //    └─[tech]────► agent_tech    ─► router_tech    ─┬─[loop]──► sup
    //                                                    └─[resolved]─► respond
    //
    // The mock supervisor always routes to a fixed branch; the agent under that
    // branch resolves on call 1 → router takes "resolved" → respond.

    /// Test 7: supervisor routes to "billing" branch.
    /// Expected path: sup → agent_billing → router_billing → respond.
    /// Supervisor invoked once.
    #[tokio::test]
    async fn supervisor_routes_to_billing_branch() {
        let store = Arc::new(InMemoryCheckpointStore::default());
        let sup_count = Arc::new(AtomicU32::new(0));
        let agent_count = Arc::new(AtomicU32::new(0));

        let exec = GraphExecutor::new(
            store.clone(),
            agent_fn_resolves_on(agent_count.clone(), 1),
            tool_fn_counting(Arc::new(AtomicU32::new(0))),
            supervisor_fn_always_routes_to(sup_count.clone(), "billing"),
            approval_fn_awaiting(),
        );

        let outcome = exec
            .start(
                &tenant(),
                "run-sup-billing",
                &supervisor_cfg(),
                "I have a billing question",
            )
            .await
            .expect("supervisor billing run should succeed");

        assert_eq!(outcome.status, RunStatus::Succeeded, "status");
        assert_eq!(
            sup_count.load(Ordering::SeqCst),
            1,
            "supervisor invoked once"
        );
        assert_eq!(
            agent_count.load(Ordering::SeqCst),
            1,
            "agent invoked once on billing branch"
        );

        // Trail should contain a supervisor entry with branch="billing".
        let sup_entry = outcome.trail.iter().find(|e| e["kind"] == "supervisor");
        assert!(
            sup_entry.is_some(),
            "trail must contain a supervisor entry: {:?}",
            outcome.trail
        );
        let sup_entry = sup_entry.unwrap();
        assert_eq!(
            sup_entry["branch"], "billing",
            "supervisor trail branch must be 'billing'"
        );
        assert_eq!(
            sup_entry["replayed"], false,
            "fresh run: supervisor not replayed"
        );
    }

    /// Test 8: supervisor routes to "tech" branch.
    #[tokio::test]
    async fn supervisor_routes_to_tech_branch() {
        let store = Arc::new(InMemoryCheckpointStore::default());
        let sup_count = Arc::new(AtomicU32::new(0));
        let agent_count = Arc::new(AtomicU32::new(0));

        let exec = GraphExecutor::new(
            store.clone(),
            agent_fn_resolves_on(agent_count.clone(), 1),
            tool_fn_counting(Arc::new(AtomicU32::new(0))),
            supervisor_fn_always_routes_to(sup_count.clone(), "tech"),
            approval_fn_awaiting(),
        );

        let outcome = exec
            .start(
                &tenant(),
                "run-sup-tech",
                &supervisor_cfg(),
                "I have a tech issue",
            )
            .await
            .expect("supervisor tech run should succeed");

        assert_eq!(outcome.status, RunStatus::Succeeded, "status");
        assert_eq!(
            sup_count.load(Ordering::SeqCst),
            1,
            "supervisor invoked once"
        );
        assert_eq!(
            agent_count.load(Ordering::SeqCst),
            1,
            "agent invoked once on tech branch"
        );

        let sup_entry = outcome.trail.iter().find(|e| e["kind"] == "supervisor");
        assert!(sup_entry.is_some(), "trail must have supervisor entry");
        assert_eq!(sup_entry.unwrap()["branch"], "tech");
    }

    /// Test 9: replay determinism — complete a supervisor run, then resume the
    /// same run_id. The supervisor closure must NOT be invoked again; the recorded
    /// routing decision is replayed from the ledger.
    #[tokio::test]
    async fn supervisor_replay_determinism() {
        let store = Arc::new(InMemoryCheckpointStore::default());
        let sup_count = Arc::new(AtomicU32::new(0));
        let agent_count = Arc::new(AtomicU32::new(0));

        let exec = GraphExecutor::new(
            store.clone(),
            agent_fn_resolves_on(agent_count.clone(), 1),
            tool_fn_counting(Arc::new(AtomicU32::new(0))),
            supervisor_fn_always_routes_to(sup_count.clone(), "billing"),
            approval_fn_awaiting(),
        );

        // Drive to completion.
        let first = exec
            .start(
                &tenant(),
                "run-sup-replay",
                &supervisor_cfg(),
                "billing question",
            )
            .await
            .expect("first run succeeds");
        assert_eq!(first.status, RunStatus::Succeeded);

        let after_first_sup = sup_count.load(Ordering::SeqCst);

        // Resume the completed run — should return the terminal outcome without
        // re-invoking the supervisor or agent.
        let second = exec
            .resume(&tenant(), "run-sup-replay")
            .await
            .expect("resume should succeed");
        assert_eq!(second.status, RunStatus::Succeeded);
        assert_eq!(
            sup_count.load(Ordering::SeqCst),
            after_first_sup,
            "supervisor must NOT be called again on resume of a terminal run"
        );
    }

    /// Test 10: trail has the supervisor entry with branch and replayed=false on
    /// fresh run, and replayed=true in a mid-flight crash-recovery scenario.
    #[tokio::test]
    async fn supervisor_trail_entry_has_branch_and_replayed_flag() {
        let store = Arc::new(InMemoryCheckpointStore::default());
        let sup_count = Arc::new(AtomicU32::new(0));

        let exec = GraphExecutor::new(
            store.clone(),
            agent_fn_resolves_on(Arc::new(AtomicU32::new(0)), 1),
            tool_fn_counting(Arc::new(AtomicU32::new(0))),
            supervisor_fn_always_routes_to(sup_count.clone(), "tech"),
            approval_fn_awaiting(),
        );

        let outcome = exec
            .start(&tenant(), "run-sup-trail", &supervisor_cfg(), "need help")
            .await
            .expect("run should succeed");

        // Find the supervisor entry in the trail.
        let sup_entry = outcome
            .trail
            .iter()
            .find(|e| e["kind"] == "supervisor")
            .expect("trail must contain a supervisor entry");

        assert_eq!(sup_entry["node"], "sup", "supervisor node id");
        assert_eq!(sup_entry["kind"], "supervisor");
        assert_eq!(sup_entry["attempt"], 1u32);
        assert_eq!(sup_entry["replayed"], false);
        assert_eq!(sup_entry["branch"], "tech");
    }

    // -----------------------------------------------------------------------
    // Provider threading tests
    // -----------------------------------------------------------------------

    /// Test: when an agent node carries `"provider": "anthropic"`, the
    /// `AgentTurnRequest` delivered to the closure must have
    /// `provider = Some("anthropic")`.
    #[tokio::test]
    async fn agent_turn_request_carries_node_provider_when_set() {
        use std::sync::Mutex;

        let store = Arc::new(InMemoryCheckpointStore::default());
        let captured: Arc<Mutex<Option<Option<String>>>> = Arc::new(Mutex::new(None));
        let cap = captured.clone();

        let agent: AgentTurnFn = Arc::new(move |req: AgentTurnRequest| {
            *cap.lock().unwrap() = Some(req.provider.clone());
            Box::pin(async move {
                Ok(AgentTurnResult {
                    reply: "ok".into(),
                    resolved: true,
                })
            })
        });

        // Graph with provider set on the agent node.
        let cfg_json = serde_json::json!({
            "schemaVersion": 1,
            "entry": "agent",
            "nodes": [
                {
                    "id": "agent",
                    "kind": "agent",
                    "systemPrompt": "You help.",
                    "model": "claude-3-5-sonnet",
                    "provider": "anthropic"
                },
                {"id": "respond", "kind": "respond"}
            ],
            "edges": [
                {"from": "agent", "to": "respond"}
            ]
        })
        .to_string();
        let cfg = GraphConfig::from_json(&cfg_json).expect("fixture valid");

        let exec = GraphExecutor::new(
            store.clone(),
            agent,
            Arc::new(|_| Box::pin(async { Ok(serde_json::json!({})) })),
            supervisor_fn_unreachable(),
            approval_fn_awaiting(),
        );
        exec.start(&tenant(), "run-provider-set", &cfg, "hi")
            .await
            .expect("run should succeed");

        let got = captured.lock().unwrap().take().expect("agent was called");
        assert_eq!(
            got,
            Some("anthropic".to_string()),
            "AgentTurnRequest.provider must be Some(\"anthropic\") when set on the node"
        );
    }

    /// Test: when an agent node has NO `"provider"` field, the
    /// `AgentTurnRequest` must have `provider = None` (backward compat).
    #[tokio::test]
    async fn agent_turn_request_provider_is_none_when_absent() {
        use std::sync::Mutex;

        let store = Arc::new(InMemoryCheckpointStore::default());
        let captured: Arc<Mutex<Option<Option<String>>>> = Arc::new(Mutex::new(None));
        let cap = captured.clone();

        let agent: AgentTurnFn = Arc::new(move |req: AgentTurnRequest| {
            *cap.lock().unwrap() = Some(req.provider.clone());
            Box::pin(async move {
                Ok(AgentTurnResult {
                    reply: "ok".into(),
                    resolved: true,
                })
            })
        });

        // Graph WITHOUT provider — uses the triage fixture (no provider field).
        let exec = GraphExecutor::new(
            store.clone(),
            agent,
            Arc::new(|_| Box::pin(async { Ok(serde_json::json!({})) })),
            supervisor_fn_unreachable(),
            approval_fn_awaiting(),
        );
        let cfg_json = serde_json::json!({
            "schemaVersion": 1,
            "entry": "agent",
            "nodes": [
                {
                    "id": "agent",
                    "kind": "agent",
                    "systemPrompt": "You help.",
                    "model": "gpt-4o-mini"
                },
                {"id": "respond", "kind": "respond"}
            ],
            "edges": [{"from": "agent", "to": "respond"}]
        })
        .to_string();
        let cfg = GraphConfig::from_json(&cfg_json).expect("fixture valid");
        exec.start(&tenant(), "run-provider-absent", &cfg, "hi")
            .await
            .expect("run should succeed");

        let got = captured.lock().unwrap().take().expect("agent was called");
        assert_eq!(
            got, None,
            "AgentTurnRequest.provider must be None when the node has no provider field"
        );
    }

    // -----------------------------------------------------------------------
    // Tools threading tests (graph-node-agent-tools Task 1)
    // -----------------------------------------------------------------------

    /// Test: when an agent node declares `"tools": ["myext/dothing"]`, the
    /// `AgentTurnRequest` delivered to the closure must carry
    /// `tools == vec!["myext/dothing"]`.
    #[tokio::test]
    async fn agent_turn_request_carries_node_tools_when_set() {
        use std::sync::Mutex;

        let store = Arc::new(InMemoryCheckpointStore::default());
        let captured: Arc<Mutex<Option<Vec<String>>>> = Arc::new(Mutex::new(None));
        let cap = captured.clone();

        let agent: AgentTurnFn = Arc::new(move |req: AgentTurnRequest| {
            *cap.lock().unwrap() = Some(req.tools.clone());
            Box::pin(async move {
                Ok(AgentTurnResult {
                    reply: "ok".into(),
                    resolved: true,
                })
            })
        });

        // Graph with tools declared on the agent node.
        let cfg_json = serde_json::json!({
            "schemaVersion": 1,
            "entry": "agent",
            "nodes": [
                {
                    "id": "agent",
                    "kind": "agent",
                    "systemPrompt": "You help.",
                    "model": "gpt-4o-mini",
                    "tools": ["myext/dothing"]
                },
                {"id": "respond", "kind": "respond"}
            ],
            "edges": [
                {"from": "agent", "to": "respond"}
            ]
        })
        .to_string();
        let cfg = GraphConfig::from_json(&cfg_json).expect("fixture valid");

        let exec = GraphExecutor::new(
            store.clone(),
            agent,
            Arc::new(|_| Box::pin(async { Ok(serde_json::json!({})) })),
            supervisor_fn_unreachable(),
            approval_fn_awaiting(),
        );
        exec.start(&tenant(), "run-tools-set", &cfg, "hi")
            .await
            .expect("run should succeed");

        let got = captured.lock().unwrap().take().expect("agent was called");
        assert_eq!(
            got,
            vec!["myext/dothing".to_string()],
            "AgentTurnRequest.tools must equal the node's declared tools"
        );
    }

    /// Test: when an agent node has NO `"tools"` field, the
    /// `AgentTurnRequest.tools` must be an empty vec (backward compat).
    #[tokio::test]
    async fn agent_turn_request_tools_is_empty_when_absent() {
        use std::sync::Mutex;

        let store = Arc::new(InMemoryCheckpointStore::default());
        let captured: Arc<Mutex<Option<Vec<String>>>> = Arc::new(Mutex::new(None));
        let cap = captured.clone();

        let agent: AgentTurnFn = Arc::new(move |req: AgentTurnRequest| {
            *cap.lock().unwrap() = Some(req.tools.clone());
            Box::pin(async move {
                Ok(AgentTurnResult {
                    reply: "ok".into(),
                    resolved: true,
                })
            })
        });

        // Graph WITHOUT tools — no tools field on the agent node.
        let exec = GraphExecutor::new(
            store.clone(),
            agent,
            Arc::new(|_| Box::pin(async { Ok(serde_json::json!({})) })),
            supervisor_fn_unreachable(),
            approval_fn_awaiting(),
        );
        let cfg_json = serde_json::json!({
            "schemaVersion": 1,
            "entry": "agent",
            "nodes": [
                {
                    "id": "agent",
                    "kind": "agent",
                    "systemPrompt": "You help.",
                    "model": "gpt-4o-mini"
                },
                {"id": "respond", "kind": "respond"}
            ],
            "edges": [{"from": "agent", "to": "respond"}]
        })
        .to_string();
        let cfg = GraphConfig::from_json(&cfg_json).expect("fixture valid");
        exec.start(&tenant(), "run-tools-absent", &cfg, "hi")
            .await
            .expect("run should succeed");

        let got = captured.lock().unwrap().take().expect("agent was called");
        assert!(
            got.is_empty(),
            "AgentTurnRequest.tools must be empty when the node has no tools field"
        );
    }

    // -----------------------------------------------------------------------
    // Parallel / Join tests (Task 3)
    // -----------------------------------------------------------------------
    //
    // parallel_json topology:
    //   entry(agent) → fan(parallel) ─[a]─► agent_a(agent) ─┐
    //                                 ─[b]─► tool_b(tool)  ─┤
    //                                                        ▼
    //                                                  meet(join) → respond
    //
    // After the trunk visits `entry`, the trunk state holds: [user, entry-reply].
    // Branch "a" appends agent_a's reply; branch "b" appends tool_b's result.

    /// Test 11: parallel happy path → Succeeded, both branch messages present
    /// in deterministic (label) merge order.
    #[tokio::test]
    async fn parallel_happy_path_merges_both_branches() {
        let store = Arc::new(InMemoryCheckpointStore::default());

        // Agent fn replies "agent-reply from <node>"; resolves so the run can end.
        let agent: AgentTurnFn = Arc::new(|req: AgentTurnRequest| {
            let node = req.node_id.clone();
            Box::pin(async move {
                Ok(AgentTurnResult {
                    reply: format!("agent-reply from {node}"),
                    resolved: true,
                })
            })
        });
        let tool: ToolFn = Arc::new(|_req: ToolCallRequest| {
            Box::pin(async move { Ok(serde_json::json!({"branch_b": "done"})) })
        });

        let exec = GraphExecutor::new(
            store.clone(),
            agent,
            tool,
            supervisor_fn_unreachable(),
            approval_fn_awaiting(),
        );

        let outcome = exec
            .start(&tenant(), "run-par-happy", &parallel_cfg(), "go")
            .await
            .expect("parallel run should succeed");

        assert_eq!(outcome.status, RunStatus::Succeeded, "status");

        // Reconstruct the final state from the store to inspect merged messages.
        let rec = store
            .load(&tenant(), "run-par-happy")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(rec.frontier_json, None, "frontier cleared after merge");
        let state: GraphRunState = serde_json::from_str(&rec.state_json).unwrap();
        let contents: Vec<&str> = state.messages.iter().map(|m| m.content.as_str()).collect();

        // Both branch contributions must be present.
        let a_idx = contents
            .iter()
            .position(|c| c.contains("agent-reply from agent_a"))
            .expect("branch a message present");
        let b_idx = contents
            .iter()
            .position(|c| c.contains("branch_b"))
            .expect("branch b message present");
        // Deterministic merge order: branch "a" before branch "b".
        assert!(
            a_idx < b_idx,
            "branch a must merge before branch b: {contents:?}"
        );
    }

    /// Test 12: branch isolation — branch B (tool_b) must NOT see branch A's
    /// message mid-flight. The tool closure probes its received state.
    #[tokio::test]
    async fn parallel_branch_isolation_no_cross_bleed() {
        let store = Arc::new(InMemoryCheckpointStore::default());

        let agent: AgentTurnFn = Arc::new(|req: AgentTurnRequest| {
            let node = req.node_id.clone();
            Box::pin(async move {
                // Only agent_a yields the distinctive message branch B must never
                // see; entry (trunk) yields a neutral reply so the probe targets
                // cross-branch bleed specifically, not the shared trunk snapshot.
                let reply = if node == "agent_a" {
                    "SECRET-A-MESSAGE from agent_a".to_string()
                } else {
                    format!("neutral reply from {node}")
                };
                Ok(AgentTurnResult {
                    reply,
                    resolved: true,
                })
            })
        });

        let saw_secret = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let saw_secret_probe = saw_secret.clone();
        let tool: ToolFn = Arc::new(move |req: ToolCallRequest| {
            // Probe: does branch B's received state contain branch A's message?
            let leaked = req
                .state
                .messages
                .iter()
                .any(|m| m.content.contains("SECRET-A-MESSAGE"));
            if leaked {
                saw_secret_probe.store(true, Ordering::SeqCst);
            }
            Box::pin(async move { Ok(serde_json::json!({"branch_b": "done"})) })
        });

        let exec = GraphExecutor::new(
            store.clone(),
            agent,
            tool,
            supervisor_fn_unreachable(),
            approval_fn_awaiting(),
        );
        exec.start(&tenant(), "run-par-iso", &parallel_cfg(), "go")
            .await
            .expect("run should succeed");

        assert!(
            !saw_secret.load(Ordering::SeqCst),
            "branch B observed branch A's message — isolation violated"
        );
    }

    /// Test 13: deterministic merge under injected delay. Branch "a" (agent_a)
    /// is SLOW; branch "b" (tool_b) is FAST. The merge must still be a-then-b
    /// (by label), not b-then-a (by completion).
    #[tokio::test]
    async fn parallel_merge_is_deterministic_under_delay() {
        for _ in 0..3 {
            let store = Arc::new(InMemoryCheckpointStore::default());

            // Slow agent (branch a). entry is also an agent but runs in the
            // trunk before fan-out, so its latency does not affect ordering.
            let agent: AgentTurnFn = Arc::new(|req: AgentTurnRequest| {
                let node = req.node_id.clone();
                Box::pin(async move {
                    if node == "agent_a" {
                        tokio::time::sleep(std::time::Duration::from_millis(40)).await;
                    }
                    Ok(AgentTurnResult {
                        reply: format!("reply from {node}"),
                        resolved: true,
                    })
                })
            });
            // Fast tool (branch b).
            let tool: ToolFn = Arc::new(|_req: ToolCallRequest| {
                Box::pin(async move { Ok(serde_json::json!({"branch_b_fast": true})) })
            });

            let exec = GraphExecutor::new(
                store.clone(),
                agent,
                tool,
                supervisor_fn_unreachable(),
                approval_fn_awaiting(),
            );
            exec.start(&tenant(), "run-par-det", &parallel_cfg(), "go")
                .await
                .expect("run should succeed");

            let rec = store.load(&tenant(), "run-par-det").await.unwrap().unwrap();
            let state: GraphRunState = serde_json::from_str(&rec.state_json).unwrap();
            let contents: Vec<&str> = state.messages.iter().map(|m| m.content.as_str()).collect();
            let a_idx = contents
                .iter()
                .position(|c| c.contains("reply from agent_a"))
                .expect("branch a present");
            let b_idx = contents
                .iter()
                .position(|c| c.contains("branch_b_fast"))
                .expect("branch b present");
            assert!(
                a_idx < b_idx,
                "slow branch a must still merge before fast branch b: {contents:?}"
            );
        }
    }

    /// Test 14: global visit cap across branches. A parallel whose branches
    /// would collectively exceed MAX_NODE_VISITS → Failed + IterationCap.
    #[tokio::test]
    async fn parallel_global_visit_cap_across_branches_fails() {
        // Build a parallel graph where each branch loops via a router with a
        // huge maxIterations, so the only thing that can stop it is the global
        // MAX_NODE_VISITS cap counted across BOTH branches.
        let v = serde_json::json!({
            "schemaVersion": 2,
            "entry": "fan",
            "nodes": [
                {"id": "fan", "kind": "parallel"},
                {"id": "agent_a", "kind": "agent", "systemPrompt": "a", "model": "m", "tools": []},
                {"id": "router_a", "kind": "router", "maxIterations": 1000},
                {"id": "agent_b", "kind": "agent", "systemPrompt": "b", "model": "m", "tools": []},
                {"id": "router_b", "kind": "router", "maxIterations": 1000},
                {"id": "meet", "kind": "join"},
                {"id": "respond", "kind": "respond"}
            ],
            "edges": [
                {"from": "fan", "to": "agent_a", "branch": "a"},
                {"from": "fan", "to": "agent_b", "branch": "b"},
                {"from": "agent_a", "to": "router_a"},
                {"from": "router_a", "to": "agent_a", "branch": "loop"},
                {"from": "router_a", "to": "meet", "branch": "resolved"},
                {"from": "agent_b", "to": "router_b"},
                {"from": "router_b", "to": "agent_b", "branch": "loop"},
                {"from": "router_b", "to": "meet", "branch": "resolved"},
                {"from": "meet", "to": "respond"}
            ]
        });
        let cfg = GraphConfig::from_json(&v.to_string()).expect("graph valid");
        let store = Arc::new(InMemoryCheckpointStore::default());

        // Agents never resolve → branches loop until the global cap fires.
        let agent: AgentTurnFn = Arc::new(|req: AgentTurnRequest| {
            let node = req.node_id.clone();
            Box::pin(async move {
                Ok(AgentTurnResult {
                    reply: format!("loop from {node}"),
                    resolved: false,
                })
            })
        });
        let exec = GraphExecutor::new(
            store.clone(),
            agent,
            tool_fn_counting(Arc::new(AtomicU32::new(0))),
            supervisor_fn_unreachable(),
            approval_fn_awaiting(),
        );

        let err = exec
            .start(&tenant(), "run-par-cap", &cfg, "loop forever")
            .await
            .expect_err("should hit global cap");
        assert!(
            matches!(err, GraphExecError::IterationCap { .. }),
            "expected IterationCap, got {err:?}"
        );

        let rec = store.load(&tenant(), "run-par-cap").await.unwrap().unwrap();
        assert_eq!(rec.status, RunStatus::Failed, "run must be Failed on cap");
    }

    /// Test 15: mid-branch effect error → Err returned, record stays Running,
    /// frontier persisted with the failed branch's cursor at its last good node.
    #[tokio::test]
    async fn parallel_mid_branch_error_keeps_run_running_with_frontier() {
        let store = Arc::new(InMemoryCheckpointStore::default());

        // Branch a (agent_a) errors; branch b (tool_b) succeeds and parks.
        let agent: AgentTurnFn = Arc::new(|req: AgentTurnRequest| {
            let node = req.node_id.clone();
            Box::pin(async move {
                if node == "agent_a" {
                    Err(GraphExecError::AgentTurn("branch a boom".into()))
                } else {
                    Ok(AgentTurnResult {
                        reply: format!("ok from {node}"),
                        resolved: true,
                    })
                }
            })
        });
        let tool: ToolFn = Arc::new(|_req: ToolCallRequest| {
            Box::pin(async move { Ok(serde_json::json!({"branch_b": "ok"})) })
        });

        let exec = GraphExecutor::new(
            store.clone(),
            agent,
            tool,
            supervisor_fn_unreachable(),
            approval_fn_awaiting(),
        );
        let err = exec
            .start(&tenant(), "run-par-err", &parallel_cfg(), "go")
            .await
            .expect_err("branch a error should propagate");
        assert!(
            matches!(err, GraphExecError::AgentTurn(_)),
            "expected AgentTurn error, got {err:?}"
        );

        let rec = store.load(&tenant(), "run-par-err").await.unwrap().unwrap();
        assert_eq!(
            rec.status,
            RunStatus::Running,
            "run must stay Running after a mid-branch error"
        );
        let frontier_json = rec
            .frontier_json
            .as_ref()
            .expect("frontier must be persisted mid-parallel");
        let frontier: Vec<BranchCursor> = serde_json::from_str(frontier_json).unwrap();
        // Branch a never advanced past agent_a (cursor stays at agent_a, not parked).
        let a = frontier
            .iter()
            .find(|b| b.branch == "a")
            .expect("branch a slot present");
        assert_eq!(
            a.cursor, "agent_a",
            "failed branch cursor at last good node"
        );
        assert!(!a.parked, "failed branch must not be parked");
    }

    /// Test 16: resume after a mid-branch error completes the run without
    /// re-running the already-parked branch (in-process resume).
    #[tokio::test]
    async fn parallel_resume_after_branch_error_completes() {
        let store = Arc::new(InMemoryCheckpointStore::default());
        let t = tenant();

        // Phase 1: branch a errors.
        {
            let agent: AgentTurnFn = Arc::new(|req: AgentTurnRequest| {
                let node = req.node_id.clone();
                Box::pin(async move {
                    if node == "agent_a" {
                        Err(GraphExecError::AgentTurn("boom".into()))
                    } else {
                        Ok(AgentTurnResult {
                            reply: format!("ok from {node}"),
                            resolved: true,
                        })
                    }
                })
            });
            let tool: ToolFn =
                Arc::new(|_r| Box::pin(async move { Ok(serde_json::json!({"branch_b": "ok"})) }));
            let exec = GraphExecutor::new(
                store.clone(),
                agent,
                tool,
                supervisor_fn_unreachable(),
                approval_fn_awaiting(),
            );
            exec.start(&t, "run-par-resume", &parallel_cfg(), "go")
                .await
                .expect_err("phase 1 errors");
        }

        // Phase 2: resume with a healthy agent → branch a now completes.
        let branch_b_calls = Arc::new(AtomicU32::new(0));
        {
            let agent: AgentTurnFn = Arc::new(|req: AgentTurnRequest| {
                let node = req.node_id.clone();
                Box::pin(async move {
                    Ok(AgentTurnResult {
                        reply: format!("recovered from {node}"),
                        resolved: true,
                    })
                })
            });
            let bcalls = branch_b_calls.clone();
            let tool: ToolFn = Arc::new(move |_r| {
                bcalls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move { Ok(serde_json::json!({"branch_b": "ok"})) })
            });
            let exec = GraphExecutor::new(
                store.clone(),
                agent,
                tool,
                supervisor_fn_unreachable(),
                approval_fn_awaiting(),
            );
            let outcome = exec
                .resume(&t, "run-par-resume")
                .await
                .expect("resume should complete");
            assert_eq!(outcome.status, RunStatus::Succeeded, "resumed run succeeds");
        }

        // Branch b was already parked + recorded in phase 1; resume must replay
        // it from the ledger and NOT re-invoke the tool.
        assert_eq!(
            branch_b_calls.load(Ordering::SeqCst),
            0,
            "already-parked branch b must replay, not re-invoke its tool"
        );

        // Final merged state must contain both branches' contributions.
        let rec = store.load(&t, "run-par-resume").await.unwrap().unwrap();
        assert_eq!(rec.status, RunStatus::Succeeded);
        assert_eq!(rec.frontier_json, None, "frontier cleared after merge");
        let state: GraphRunState = serde_json::from_str(&rec.state_json).unwrap();
        let contents: Vec<&str> = state.messages.iter().map(|m| m.content.as_str()).collect();
        assert!(
            contents
                .iter()
                .any(|c| c.contains("recovered from agent_a")),
            "branch a recovered: {contents:?}"
        );
        assert!(
            contents.iter().any(|c| c.contains("branch_b")),
            "branch b present: {contents:?}"
        );
    }

    // -----------------------------------------------------------------------
    // Approval tests (Task C2)
    // -----------------------------------------------------------------------
    //
    // approval_cfg topology: start(agent) → approval → respond
    // (edge label "approved" on the approval node's only outgoing edge).

    fn approval_json() -> String {
        serde_json::json!({
            "schemaVersion": 2,
            "entry": "start",
            "nodes": [
                {"id": "start", "kind": "agent", "systemPrompt": "greet the user", "model": "gpt-4o-mini"},
                {"id": "approval", "kind": "approval", "title": "Approve refund?", "mode": "always"},
                {"id": "respond", "kind": "respond"}
            ],
            "edges": [
                {"from": "start", "to": "approval"},
                {"from": "approval", "to": "respond", "branch": "approved"}
            ]
        })
        .to_string()
    }

    fn approval_cfg() -> GraphConfig {
        GraphConfig::from_json(&approval_json()).expect("approval fixture is valid")
    }

    /// Test 17: an approval node with an `Awaiting` `ApprovalFn` parks the run
    /// (`RunStatus::AwaitingInput`, cursor at the approval node); resuming with
    /// a `Decided` `ApprovalFn` advances along the matching edge to `respond`.
    #[tokio::test]
    async fn approval_parks_then_resumes() {
        let store = Arc::new(InMemoryCheckpointStore::default());
        let t = tenant();
        let agent_count = Arc::new(AtomicU32::new(0));

        // Phase 1: park.
        {
            let exec = GraphExecutor::new(
                store.clone(),
                agent_fn_resolves_on(agent_count.clone(), 1),
                tool_fn_counting(Arc::new(AtomicU32::new(0))),
                supervisor_fn_unreachable(),
                approval_fn_awaiting(),
            );

            let outcome = exec
                .start(&t, "run-approval", &approval_cfg(), "please approve this")
                .await
                .expect("start should return cleanly when parked, not error");

            assert_eq!(
                outcome.status,
                RunStatus::AwaitingInput,
                "start() outcome must report AwaitingInput"
            );

            let rec = store
                .load(&t, "run-approval")
                .await
                .expect("store accessible")
                .expect("record must exist");
            assert_eq!(
                rec.status,
                RunStatus::AwaitingInput,
                "persisted record must be AwaitingInput"
            );
            assert_eq!(
                rec.cursor, "approval",
                "cursor must stay at the approval node while parked"
            );
        }

        // Phase 2: resume with a decision.
        let decision_count = Arc::new(AtomicU32::new(0));
        {
            let exec = GraphExecutor::new(
                store.clone(),
                agent_fn_resolves_on(agent_count.clone(), 1),
                tool_fn_counting(Arc::new(AtomicU32::new(0))),
                supervisor_fn_unreachable(),
                approval_fn_decides(decision_count.clone(), "approved"),
            );

            let outcome = exec
                .resume(&t, "run-approval")
                .await
                .expect("resume with a decision should succeed");

            assert_eq!(
                outcome.status,
                RunStatus::Succeeded,
                "resumed run must reach Succeeded at respond"
            );
        }

        assert_eq!(
            decision_count.load(Ordering::SeqCst),
            1,
            "approval fn must be called exactly once on resume"
        );

        let rec = store
            .load(&t, "run-approval")
            .await
            .unwrap()
            .expect("record must exist after resume");
        assert_eq!(rec.status, RunStatus::Succeeded);
        assert_eq!(rec.cursor, "respond", "cursor must land on respond node");

        // The visit ledger must have recorded the decision payload.
        let visit = store
            .load_node_visit(&t, "run-approval", "approval", 1)
            .await
            .unwrap()
            .expect("approval node visit must be recorded");
        assert_eq!(visit["decision"], "approved");
    }
}
