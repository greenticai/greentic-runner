//! Data model for durable agent-graph execution.
//!
//! Ported from the greentic-designer engine spike (PR #436,
//! `src/orchestrate/agent_graph/model.rs`) and extended with a
//! schema-versioned `GraphConfig` envelope for the runner sidecar wire
//! format.
//!
//! See `docs/superpowers/specs/2026-06-06-runtime-agent-graph-execution-design.md`
//! for the full design, and
//! `docs/superpowers/specs/2026-06-07-agent-graph-v2-node-kinds-design.md`
//! for the v2 additions (supervisor, parallel, join).

use std::collections::HashSet;
use std::ops::RangeInclusive;

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Node kinds
// ---------------------------------------------------------------------------

/// The role a node plays in the graph, plus its per-kind configuration.
///
/// Serialised with `"kind"` as the tag field; variant names are lowercase.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum NodeKind {
    /// An LLM agent that may invoke tools.
    #[serde(rename_all = "camelCase")]
    Agent {
        system_prompt: String,
        model: String,
        #[serde(default)]
        tools: Vec<String>,
        /// LLM provider override (e.g. `"anthropic"`, `"openai"`).
        /// Absent in existing graphs — defaults to `None`, which the executor
        /// maps to `"openai"` for backward compatibility.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provider: Option<String>,
        /// When set, run the referenced published agent's FULL config
        /// (memory/knowledge/guardrails) resolved from the pack's merged
        /// agents, instead of the inline system_prompt/model/tools.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        agent_ref: Option<String>,
        /// When set, keep this node's OWN `system_prompt` and `model` but
        /// inherit the referenced agent's tools, memory, knowledge and
        /// guardrails.
        ///
        /// This is the specialist case, and it is why `agent_ref` alone is not
        /// enough: `agent_ref` adopts the referenced config wholesale, prompt
        /// included, which would make every specialist in a generated graph an
        /// identical copy of its parent worker. `inherit_from` keeps what makes
        /// a specialist a specialist and inherits only the capability surface.
        ///
        /// Ignored when `agent_ref` is set — that already takes everything.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        inherit_from: Option<String>,
    },
    /// A deterministic tool call node.
    #[serde(rename_all = "camelCase")]
    Tool { tool_name: String },
    /// A routing decision node (loop vs. resolved).
    #[serde(rename_all = "camelCase")]
    Router {
        #[serde(default = "default_max_iterations")]
        max_iterations: u32,
    },
    /// Terminal node that emits the final reply.
    Respond,
    /// LLM-routing supervisor — picks a branch from its route list.
    ///
    /// Requires `schemaVersion: 2`.
    #[serde(rename_all = "camelCase")]
    Supervisor {
        system_prompt: String,
        model: String,
        routes: Vec<SupervisorRoute>,
        /// LLM provider override (e.g. `"anthropic"`, `"openai"`).
        /// Absent in existing graphs — defaults to `None`, which the executor
        /// maps to `"openai"` for backward compatibility.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provider: Option<String>,
        /// When set, inherit the referenced agent's **guardrails** from the
        /// pack's merged agents.
        ///
        /// Deliberately narrower than [`NodeKind::Agent`]'s `agent_ref`, which
        /// adopts the referenced agent's full config: a supervisor routes, it
        /// does not work, so it stays tool-free and gains no memory or
        /// knowledge. Guardrails are the exception because a supervisor reads
        /// the operator's raw input — it is the first place sensitive data
        /// arrives, so inbound content-safety hooks must run there.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        agent_ref: Option<String>,
    },
    /// Fan-out node — spawns one branch per outgoing edge.
    ///
    /// Requires `schemaVersion: 2`.
    Parallel,
    /// Fan-in node — waits for all parallel branches to arrive.
    ///
    /// Requires `schemaVersion: 2`.
    Join,
    /// Human-in-the-loop approval node. Parks the run awaiting a human decision
    /// (`RunStatus::AwaitingInput`) and advances along the `approved`/`denied`/
    /// `timeout` edge once decided. The decision itself is supplied by the host's
    /// `ApprovalFn` closure.
    ///
    /// Requires `schemaVersion: 2`.
    Approval {
        title: String,
        #[serde(default = "default_approval_mode")]
        mode: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        risk_threshold: Option<f64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        confidence_threshold: Option<f64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        deadline_ms: Option<u64>,
    },
}

/// Default `mode` for an [`NodeKind::Approval`] node when omitted from the
/// wire document — always require a human decision.
fn default_approval_mode() -> String {
    "always".to_string()
}

impl NodeKind {
    /// Returns the lowercase string name of this kind (matches the serde tag).
    pub fn kind_name(&self) -> &'static str {
        match self {
            NodeKind::Agent { .. } => "agent",
            NodeKind::Tool { .. } => "tool",
            NodeKind::Router { .. } => "router",
            NodeKind::Respond => "respond",
            NodeKind::Supervisor { .. } => "supervisor",
            NodeKind::Parallel => "parallel",
            NodeKind::Join => "join",
            NodeKind::Approval { .. } => "approval",
        }
    }

    /// Returns `true` if this kind requires schemaVersion 2.
    fn requires_v2(&self) -> bool {
        matches!(
            self,
            NodeKind::Supervisor { .. }
                | NodeKind::Parallel
                | NodeKind::Join
                | NodeKind::Approval { .. }
        )
    }
}

/// One routing choice on a [`NodeKind::Supervisor`] node.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorRoute {
    pub branch: String,
    pub description: String,
}

fn default_max_iterations() -> u32 {
    4
}

// ---------------------------------------------------------------------------
// Graph primitives
// ---------------------------------------------------------------------------

/// A single node in the graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub id: String,
    #[serde(flatten)]
    pub kind: NodeKind,
}

/// A directed edge between two nodes. `branch` discriminates router / supervisor / parallel exits.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Edge {
    pub from: String,
    pub to: String,
    /// Router uses this to pick an outgoing edge (`"loop"` | `"resolved"`).
    /// Supervisor and Parallel use it to name the branch.
    #[serde(default)]
    pub branch: Option<String>,
}

// ---------------------------------------------------------------------------
// Graph (bare, no schema-version envelope)
// ---------------------------------------------------------------------------

/// The bare execution graph: an entry node, a node list, and an edge list.
///
/// Deserialised via `serde` and validated via [`Graph::validate`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Graph {
    pub entry: String,
    pub nodes: Vec<Node>,
    pub edges: Vec<Edge>,
}

impl Graph {
    /// Look up a node by id.
    pub fn node(&self, id: &str) -> Option<&Node> {
        self.nodes.iter().find(|n| n.id == id)
    }

    /// Outgoing edges for a node, in declaration order.
    pub fn edges_from(&self, id: &str) -> impl Iterator<Item = &Edge> {
        self.edges.iter().filter(move |e| e.from == id)
    }

    /// Incoming edges for a node.
    pub fn edges_to(&self, id: &str) -> impl Iterator<Item = &Edge> {
        self.edges.iter().filter(move |e| e.to == id)
    }

    /// Validate the graph.
    ///
    /// `schema_version` is passed in so that v2-only node kinds can be
    /// rejected when the document declares `schemaVersion: 1`.
    ///
    /// ## Rules (v1 + v2)
    ///
    /// 1. Node ids must be unique.
    /// 2. `entry` names an existing node.
    /// 3. Every edge endpoint names an existing node.
    /// 4. Agent and Tool nodes have exactly one outgoing edge.
    /// 5. Router nodes have both a `"loop"` and a `"resolved"` branch edge.
    /// 6. Respond nodes have zero outgoing edges.
    /// 7. (v2) v2-kind nodes in a schemaVersion-1 document are rejected.
    /// 8. (v2) Supervisor: ≥2 routes; unique branch labels; every route has
    ///    exactly one matching outgoing edge; no extra outgoing edges.
    /// 9. (v2) Parallel: ≥2 outgoing edges, all with unique branch labels;
    ///    every branch reaches the same join node; branches are node-disjoint
    ///    before the join; no nested parallel; no respond inside a branch.
    /// 10. (v2) Join: ≥2 incoming edges; exactly 1 outgoing edge; only
    ///     reachable via a parallel node.
    /// 11. (v2) Approval: ≥1 outgoing edge (branch labels are validated
    ///     leniently — any of `approved`/`denied`/`timeout` is accepted).
    pub fn validate(&self, schema_version: u32) -> Result<(), String> {
        // Rule 1: node ids must be unique.
        let mut seen_ids: HashSet<&str> = HashSet::new();
        for node in &self.nodes {
            if !seen_ids.insert(node.id.as_str()) {
                return Err(format!("duplicate node id '{}'", node.id));
            }
        }

        // Rule 2: entry exists.
        if self.node(&self.entry).is_none() {
            return Err(format!("entry node '{}' not found", self.entry));
        }

        // Rule 3: all edge endpoints exist.
        for e in &self.edges {
            if self.node(&e.from).is_none() {
                return Err(format!("edge from unknown node '{}'", e.from));
            }
            if self.node(&e.to).is_none() {
                return Err(format!("edge to unknown node '{}'", e.to));
            }
        }

        // Rules 4–10: per-kind constraints.
        for node in &self.nodes {
            // Rule 7: kind-gating — v2 kinds not allowed in schemaVersion 1.
            if node.kind.requires_v2() && schema_version < 2 {
                return Err(format!(
                    "node kind `{}` requires schemaVersion 2",
                    node.kind.kind_name()
                ));
            }

            let out: Vec<&Edge> = self.edges_from(&node.id).collect();
            match &node.kind {
                NodeKind::Agent { .. } | NodeKind::Tool { .. } => {
                    // Rule 4.
                    if out.len() != 1 {
                        return Err(format!(
                            "node '{}' must have exactly 1 outgoing edge, found {}",
                            node.id,
                            out.len()
                        ));
                    }
                }
                NodeKind::Router { .. } => {
                    // Rule 5.
                    let has = |b: &str| out.iter().any(|e| e.branch.as_deref() == Some(b));
                    if !has("loop") || !has("resolved") {
                        return Err(format!(
                            "router '{}' must have both a 'loop' and a 'resolved' branch edge",
                            node.id
                        ));
                    }
                }
                NodeKind::Respond => {
                    // Rule 6.
                    if !out.is_empty() {
                        return Err(format!(
                            "respond node '{}' cannot have outgoing edges",
                            node.id
                        ));
                    }
                }
                NodeKind::Supervisor { routes, .. } => {
                    // Rule 8.
                    self.validate_supervisor(node, routes, &out)?;
                }
                NodeKind::Parallel => {
                    // Rule 9.
                    self.validate_parallel(node, &out)?;
                }
                NodeKind::Join => {
                    // Rule 10.
                    let inc: Vec<&Edge> = self.edges_to(&node.id).collect();
                    if inc.len() < 2 {
                        return Err(format!(
                            "join node '{}' must have at least 2 incoming edges, found {}",
                            node.id,
                            inc.len()
                        ));
                    }
                    if out.len() != 1 {
                        return Err(format!(
                            "join node '{}' must have exactly 1 outgoing edge, found {}",
                            node.id,
                            out.len()
                        ));
                    }
                    // Reachable-via-parallel check: every incoming source must
                    // lie on a parallel branch path. We check that each source
                    // has at least one parallel ancestor.
                    for edge in &inc {
                        if !self.has_parallel_ancestor(edge.from.as_str()) {
                            return Err(format!(
                                "join node '{}' is reachable from node '{}' which is not on a parallel branch path",
                                node.id, edge.from
                            ));
                        }
                    }
                }
                NodeKind::Approval { .. } => {
                    // Rule 11. Branch labels (approved/denied/timeout) are
                    // validated leniently — the executor (Task C2) resolves
                    // the decision to whichever outgoing edge matches, so we
                    // only require that the node isn't a dead end.
                    if out.is_empty() {
                        return Err(format!(
                            "approval node '{}' must have at least 1 outgoing edge, found 0",
                            node.id
                        ));
                    }
                }
            }
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Supervisor validation helper
    // -----------------------------------------------------------------------

    fn validate_supervisor(
        &self,
        node: &Node,
        routes: &[SupervisorRoute],
        out: &[&Edge],
    ) -> Result<(), String> {
        // ≥2 routes.
        if routes.len() < 2 {
            return Err(format!(
                "supervisor '{}' must have at least 2 routes, found {}",
                node.id,
                routes.len()
            ));
        }

        // Unique branch labels.
        let mut seen: HashSet<&str> = HashSet::new();
        for r in routes {
            if !seen.insert(r.branch.as_str()) {
                return Err(format!(
                    "supervisor '{}' has duplicate route branch label '{}'",
                    node.id, r.branch
                ));
            }
        }

        // Every route must have exactly one matching outgoing edge.
        for r in routes {
            let matching: Vec<&&Edge> = out
                .iter()
                .filter(|e| e.branch.as_deref() == Some(r.branch.as_str()))
                .collect();
            match matching.len() {
                1 => {}
                0 => {
                    return Err(format!(
                        "supervisor '{}' route '{}' has no matching outgoing edge",
                        node.id, r.branch
                    ));
                }
                n => {
                    return Err(format!(
                        "supervisor '{}' route '{}' has {} matching outgoing edges (expected 1)",
                        node.id, r.branch, n
                    ));
                }
            }
        }

        // No extra outgoing edges beyond the declared routes.
        let declared_branches: HashSet<&str> = routes.iter().map(|r| r.branch.as_str()).collect();
        for e in out {
            let branch = e.branch.as_deref().unwrap_or("");
            if !declared_branches.contains(branch) {
                return Err(format!(
                    "supervisor '{}' has outgoing edge with undeclared branch label '{}'",
                    node.id, branch
                ));
            }
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Parallel validation helper
    // -----------------------------------------------------------------------

    fn validate_parallel(&self, node: &Node, out: &[&Edge]) -> Result<(), String> {
        // ≥2 outgoing edges.
        if out.len() < 2 {
            return Err(format!(
                "parallel node '{}' must have at least 2 outgoing edges, found {}",
                node.id,
                out.len()
            ));
        }

        // All outgoing edges must have unique branch labels.
        let mut seen_branches: HashSet<&str> = HashSet::new();
        for e in out {
            let label = e.branch.as_deref().unwrap_or("");
            if label.is_empty() {
                return Err(format!(
                    "parallel node '{}' has an outgoing edge without a branch label",
                    node.id
                ));
            }
            if !seen_branches.insert(label) {
                return Err(format!(
                    "parallel node '{}' has duplicate branch label '{}'",
                    node.id, label
                ));
            }
        }

        // Walk each branch and collect the set of visited node ids.
        // Then verify they all converge on one common join node.
        let mut branch_paths: Vec<(String, HashSet<String>)> = Vec::new();
        let mut join_targets: Vec<String> = Vec::new();

        for edge in out {
            let branch_label = edge.branch.as_deref().unwrap_or("").to_owned();
            let (join_id, visited) =
                self.walk_branch_to_join(node.id.as_str(), edge.to.as_str(), node.id.as_str())?;
            join_targets.push(join_id);
            branch_paths.push((branch_label, visited));
        }

        // All branches must reach the same join node.
        let common_join = &join_targets[0];
        for (i, j) in join_targets.iter().enumerate() {
            if j != common_join {
                return Err(format!(
                    "parallel node '{}': branches do not converge on the same join node (branch 0 → '{}', branch {} → '{}')",
                    node.id, common_join, i, j
                ));
            }
        }

        // Branch paths must be node-disjoint.
        // Build the union and check for overlap across branches.
        for i in 0..branch_paths.len() {
            for j in (i + 1)..branch_paths.len() {
                let shared: Vec<&String> =
                    branch_paths[i].1.intersection(&branch_paths[j].1).collect();
                if !shared.is_empty() {
                    return Err(format!(
                        "parallel node '{}': branches '{}' and '{}' share node(s) {:?} before the join (branches must be node-disjoint)",
                        node.id, branch_paths[i].0, branch_paths[j].0, shared
                    ));
                }
            }
        }

        Ok(())
    }

    /// Walk from `start_id` towards a join node. Follows each node's single
    /// outgoing edge, branching at Router (both), Supervisor (all routes),
    /// until a `Join` node is reached.
    ///
    /// Returns `(join_node_id, set_of_visited_node_ids_before_join)`.
    ///
    /// Errors if:
    /// - a nested `Parallel` is encountered
    /// - a `Respond` is encountered before a `Join`
    /// - no `Join` is reachable (reaches a dead end or loops)
    fn walk_branch_to_join(
        &self,
        parallel_id: &str,
        start_id: &str,
        _origin: &str,
    ) -> Result<(String, HashSet<String>), String> {
        // DFS — keep a stack of (node_id, path_so_far).
        // Use an iteration limit to detect cycles.
        const WALK_LIMIT: usize = 128;

        let mut visited: HashSet<String> = HashSet::new();
        let mut stack: Vec<String> = vec![start_id.to_owned()];
        let mut found_join: Option<String> = None;
        let mut iterations = 0_usize;

        while let Some(current) = stack.pop() {
            iterations += 1;
            if iterations > WALK_LIMIT {
                return Err(format!(
                    "parallel node '{}': branch walk exceeded limit (possible cycle)",
                    parallel_id
                ));
            }

            if visited.contains(&current) {
                // Cycle — already processed.
                continue;
            }

            let node = self.node(&current).ok_or_else(|| {
                format!(
                    "parallel node '{}': branch references unknown node '{}'",
                    parallel_id, current
                )
            })?;

            match &node.kind {
                NodeKind::Join => {
                    // Record which join we hit; don't add to visited (join is shared).
                    match &found_join {
                        None => {
                            found_join = Some(current.clone());
                        }
                        Some(j) if j != &current => {
                            return Err(format!(
                                "parallel node '{}': branch path reaches multiple join nodes ('{}' and '{}')",
                                parallel_id, j, current
                            ));
                        }
                        _ => {}
                    }
                    // Don't recurse past join.
                }
                NodeKind::Parallel => {
                    return Err(format!(
                        "parallel node '{}': nested parallel '{}' is not allowed",
                        parallel_id, current
                    ));
                }
                NodeKind::Respond => {
                    return Err(format!(
                        "parallel node '{}': respond node '{}' inside a parallel branch is not allowed",
                        parallel_id, current
                    ));
                }
                _ => {
                    visited.insert(current.clone());
                    // Push all successors.
                    for e in self.edges_from(&current) {
                        stack.push(e.to.clone());
                    }
                }
            }
        }

        found_join
            .ok_or_else(|| {
                format!(
                    "parallel node '{}': branch starting at '{}' never reaches a join node",
                    parallel_id, start_id
                )
            })
            .map(|j| (j, visited))
    }

    /// Returns `true` if `node_id` has at least one `Parallel` ancestor in the
    /// graph (i.e. it lies on a parallel branch path).
    fn has_parallel_ancestor(&self, node_id: &str) -> bool {
        // BFS backwards through incoming edges.
        let mut visited: HashSet<&str> = HashSet::new();
        let mut queue: Vec<&str> = vec![node_id];
        while let Some(current) = queue.pop() {
            if visited.contains(current) {
                continue;
            }
            visited.insert(current);
            for e in self.edges_to(current) {
                if let Some(n) = self.node(&e.from) {
                    if matches!(n.kind, NodeKind::Parallel) {
                        return true;
                    }
                    queue.push(e.from.as_str());
                }
            }
        }
        false
    }
}

// ---------------------------------------------------------------------------
// GraphConfig — schema-versioned envelope
// ---------------------------------------------------------------------------

/// Wire envelope for an agent-graph document (`agent-graph.json` sidecar or
/// the admin-registry graph document). `schema_version` gates forward
/// evolution; versions 1 and 2 are accepted.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GraphConfig {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    #[serde(flatten)]
    pub graph: Graph,
}

fn default_schema_version() -> u32 {
    1
}

/// The range of `schema_version` values this build accepts.
pub const SUPPORTED_SCHEMA_VERSIONS: RangeInclusive<u32> = 1..=2;

/// Deprecated alias kept for backward compatibility; external callers should
/// migrate to `SUPPORTED_SCHEMA_VERSIONS`.
#[deprecated(since = "0.0.0", note = "use SUPPORTED_SCHEMA_VERSIONS instead")]
pub const SUPPORTED_SCHEMA_VERSION: u32 = 1;

// ---------------------------------------------------------------------------
// GraphError
// ---------------------------------------------------------------------------

/// Errors produced when parsing or validating a [`GraphConfig`].
#[derive(Debug, thiserror::Error)]
pub enum GraphError {
    #[error("invalid graph: {0}")]
    Invalid(String),
    #[error("graph JSON parse error: {0}")]
    Parse(#[from] serde_json::Error),
    #[error("unsupported graph schemaVersion {0}")]
    UnsupportedSchemaVersion(u32),
}

// ---------------------------------------------------------------------------
// GraphConfig impl
// ---------------------------------------------------------------------------

impl GraphConfig {
    /// Parse and validate a JSON string into a [`GraphConfig`].
    ///
    /// Returns [`GraphError::UnsupportedSchemaVersion`] when `schema_version`
    /// is outside [`SUPPORTED_SCHEMA_VERSIONS`], and
    /// [`GraphError::Invalid`] when the graph fails structural validation.
    pub fn from_json(raw: &str) -> Result<Self, GraphError> {
        let cfg: GraphConfig = serde_json::from_str(raw)?;
        if !SUPPORTED_SCHEMA_VERSIONS.contains(&cfg.schema_version) {
            return Err(GraphError::UnsupportedSchemaVersion(cfg.schema_version));
        }
        cfg.graph
            .validate(cfg.schema_version)
            .map_err(GraphError::Invalid)?;
        Ok(cfg)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::graph::test_fixtures;

    /// Parse the shared fixture JSON back to a `serde_json::Value` so the
    /// mutation-based tests can alter individual fields.
    fn triage_value() -> serde_json::Value {
        serde_json::from_str(&test_fixtures::triage_json()).expect("fixture is valid JSON")
    }

    // -----------------------------------------------------------------------
    // Existing v1 tests (must remain green, unchanged)
    // -----------------------------------------------------------------------

    #[test]
    fn parses_and_validates_triage_graph() {
        let cfg = GraphConfig::from_json(&test_fixtures::triage_json()).expect("valid graph");
        assert_eq!(cfg.schema_version, 1);
        assert_eq!(cfg.graph.entry, "agent");
        assert_eq!(cfg.graph.nodes.len(), 4);
    }

    #[test]
    fn rejects_unknown_entry() {
        let mut v = triage_value();
        v["entry"] = "missing".into();
        let err = GraphConfig::from_json(&v.to_string()).unwrap_err();
        assert!(matches!(err, GraphError::Invalid(_)), "got {err:?}");
    }

    #[test]
    fn rejects_router_without_resolved_branch() {
        let mut v = triage_value();
        v["edges"]
            .as_array_mut()
            .unwrap()
            .retain(|e| e["branch"] != "resolved");
        assert!(GraphConfig::from_json(&v.to_string()).is_err());
    }

    #[test]
    fn rejects_agent_with_two_outgoing_edges() {
        let mut v = triage_value();
        v["edges"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({"from": "agent", "to": "router"}));
        assert!(GraphConfig::from_json(&v.to_string()).is_err());
    }

    #[test]
    fn rejects_respond_with_outgoing_edge() {
        let mut v = triage_value();
        v["edges"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({"from": "respond", "to": "agent"}));
        assert!(GraphConfig::from_json(&v.to_string()).is_err());
    }

    #[test]
    fn unknown_schema_version_is_rejected() {
        let mut v = triage_value();
        v["schemaVersion"] = 99.into();
        let err = GraphConfig::from_json(&v.to_string()).unwrap_err();
        assert!(matches!(err, GraphError::UnsupportedSchemaVersion(99)));
    }

    #[test]
    fn rejects_duplicate_node_ids() {
        let mut v = triage_value();
        v["nodes"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({"id": "agent", "kind": "respond"}));
        assert!(GraphConfig::from_json(&v.to_string()).is_err());
    }

    // -----------------------------------------------------------------------
    // V2 fixture tests
    // -----------------------------------------------------------------------

    #[test]
    fn parses_and_validates_supervisor_graph() {
        let cfg = GraphConfig::from_json(&test_fixtures::supervisor_json())
            .expect("valid supervisor graph");
        assert_eq!(cfg.schema_version, 2);
        assert_eq!(cfg.graph.entry, "sup");
        // nodes: sup, agent_billing, router_billing, agent_tech, router_tech, respond
        assert!(cfg.graph.nodes.len() >= 3, "expected at least 3 nodes");
    }

    #[test]
    fn parses_and_validates_parallel_graph() {
        let cfg =
            GraphConfig::from_json(&test_fixtures::parallel_json()).expect("valid parallel graph");
        assert_eq!(cfg.schema_version, 2);
        assert_eq!(cfg.graph.entry, "entry");
        // Nodes: entry agent, parallel, agent_a, tool_b, join, respond
        assert!(cfg.graph.nodes.len() >= 5, "expected at least 5 nodes");
    }

    #[test]
    fn approval_kind_serde_tag_and_validate() {
        let n: NodeKind = serde_json::from_value(serde_json::json!({
            "kind":"approval","title":"Send refund","mode":"always"
        }))
        .unwrap();
        assert_eq!(n.kind_name(), "approval");

        // A minimal graph: start -> approval -> respond(approved) must validate.
        let v = serde_json::json!({
            "schemaVersion": 2,
            "entry": "gate",
            "nodes": [
                {"id": "gate", "kind": "approval", "title": "Send refund"},
                {"id": "respond", "kind": "respond"}
            ],
            "edges": [
                {"from": "gate", "to": "respond", "branch": "approved"}
            ]
        });
        GraphConfig::from_json(&v.to_string()).expect("valid approval graph");
    }

    // -----------------------------------------------------------------------
    // V2 kind-gating: v2 kinds in a v1 document must be rejected
    // -----------------------------------------------------------------------

    #[test]
    fn v2_supervisor_kind_in_v1_doc_rejected() {
        let v = serde_json::json!({
            "schemaVersion": 1,
            "entry": "sup",
            "nodes": [
                {"id": "sup", "kind": "supervisor", "systemPrompt": "route",
                 "model": "gpt-4o-mini",
                 "routes": [
                     {"branch": "a", "description": "A"},
                     {"branch": "b", "description": "B"}
                 ]},
                {"id": "respond_a", "kind": "respond"},
                {"id": "respond_b", "kind": "respond"}
            ],
            "edges": [
                {"from": "sup", "to": "respond_a", "branch": "a"},
                {"from": "sup", "to": "respond_b", "branch": "b"}
            ]
        });
        let err = GraphConfig::from_json(&v.to_string()).unwrap_err();
        assert!(
            matches!(&err, GraphError::Invalid(msg) if msg.contains("supervisor") && msg.contains("schemaVersion 2")),
            "expected Invalid with 'supervisor requires schemaVersion 2', got {err:?}"
        );
    }

    #[test]
    fn v2_parallel_kind_in_v1_doc_rejected() {
        let v = serde_json::json!({
            "schemaVersion": 1,
            "entry": "fan",
            "nodes": [
                {"id": "fan", "kind": "parallel"},
                {"id": "meet", "kind": "join"},
                {"id": "a", "kind": "respond"},
                {"id": "b", "kind": "respond"}
            ],
            "edges": [
                {"from": "fan", "to": "a", "branch": "a"},
                {"from": "fan", "to": "b", "branch": "b"}
            ]
        });
        let err = GraphConfig::from_json(&v.to_string()).unwrap_err();
        assert!(
            matches!(&err, GraphError::Invalid(msg) if msg.contains("parallel") && msg.contains("schemaVersion 2")),
            "expected Invalid with 'parallel requires schemaVersion 2', got {err:?}"
        );
    }

    #[test]
    fn v2_join_kind_in_v1_doc_rejected() {
        let v = serde_json::json!({
            "schemaVersion": 1,
            "entry": "meet",
            "nodes": [
                {"id": "meet", "kind": "join"}
            ],
            "edges": []
        });
        let err = GraphConfig::from_json(&v.to_string()).unwrap_err();
        assert!(
            matches!(&err, GraphError::Invalid(msg) if msg.contains("join") && msg.contains("schemaVersion 2")),
            "expected Invalid with 'join requires schemaVersion 2', got {err:?}"
        );
    }

    // -----------------------------------------------------------------------
    // Supervisor validation errors
    // -----------------------------------------------------------------------

    #[test]
    fn supervisor_fewer_than_two_routes_rejected() {
        let v = serde_json::json!({
            "schemaVersion": 2,
            "entry": "sup",
            "nodes": [
                {"id": "sup", "kind": "supervisor", "systemPrompt": "route",
                 "model": "gpt-4o-mini",
                 "routes": [{"branch": "a", "description": "A"}]},
                {"id": "resp", "kind": "respond"}
            ],
            "edges": [
                {"from": "sup", "to": "resp", "branch": "a"}
            ]
        });
        let err = GraphConfig::from_json(&v.to_string()).unwrap_err();
        assert!(
            matches!(&err, GraphError::Invalid(msg) if msg.contains("at least 2 routes")),
            "got {err:?}"
        );
    }

    #[test]
    fn supervisor_duplicate_route_labels_rejected() {
        let v = serde_json::json!({
            "schemaVersion": 2,
            "entry": "sup",
            "nodes": [
                {"id": "sup", "kind": "supervisor", "systemPrompt": "route",
                 "model": "gpt-4o-mini",
                 "routes": [
                     {"branch": "a", "description": "A"},
                     {"branch": "a", "description": "A duplicate"}
                 ]},
                {"id": "resp_a1", "kind": "respond"},
                {"id": "resp_a2", "kind": "respond"}
            ],
            "edges": [
                {"from": "sup", "to": "resp_a1", "branch": "a"},
                {"from": "sup", "to": "resp_a2", "branch": "a"}
            ]
        });
        let err = GraphConfig::from_json(&v.to_string()).unwrap_err();
        assert!(
            matches!(&err, GraphError::Invalid(msg) if msg.contains("duplicate route branch label")),
            "got {err:?}"
        );
    }

    #[test]
    fn supervisor_route_without_matching_edge_rejected() {
        let v = serde_json::json!({
            "schemaVersion": 2,
            "entry": "sup",
            "nodes": [
                {"id": "sup", "kind": "supervisor", "systemPrompt": "route",
                 "model": "gpt-4o-mini",
                 "routes": [
                     {"branch": "billing", "description": "Billing"},
                     {"branch": "tech", "description": "Tech"}
                 ]},
                {"id": "resp_billing", "kind": "respond"}
                // No node for tech branch on purpose
            ],
            "edges": [
                {"from": "sup", "to": "resp_billing", "branch": "billing"}
                // No edge for "tech" branch
            ]
        });
        let err = GraphConfig::from_json(&v.to_string()).unwrap_err();
        assert!(
            matches!(&err, GraphError::Invalid(msg) if msg.contains("no matching outgoing edge")),
            "got {err:?}"
        );
    }

    #[test]
    fn supervisor_extra_outgoing_edge_rejected() {
        let v = serde_json::json!({
            "schemaVersion": 2,
            "entry": "sup",
            "nodes": [
                {"id": "sup", "kind": "supervisor", "systemPrompt": "route",
                 "model": "gpt-4o-mini",
                 "routes": [
                     {"branch": "billing", "description": "Billing"},
                     {"branch": "tech", "description": "Tech"}
                 ]},
                {"id": "resp_billing", "kind": "respond"},
                {"id": "resp_tech", "kind": "respond"},
                {"id": "resp_extra", "kind": "respond"}
            ],
            "edges": [
                {"from": "sup", "to": "resp_billing", "branch": "billing"},
                {"from": "sup", "to": "resp_tech", "branch": "tech"},
                {"from": "sup", "to": "resp_extra", "branch": "unknown_branch"}
            ]
        });
        let err = GraphConfig::from_json(&v.to_string()).unwrap_err();
        assert!(
            matches!(&err, GraphError::Invalid(msg) if msg.contains("undeclared branch label")),
            "got {err:?}"
        );
    }

    // -----------------------------------------------------------------------
    // Parallel validation errors
    // -----------------------------------------------------------------------

    #[test]
    fn parallel_single_branch_rejected() {
        let v = serde_json::json!({
            "schemaVersion": 2,
            "entry": "fan",
            "nodes": [
                {"id": "fan", "kind": "parallel"},
                {"id": "agent_a", "kind": "agent", "systemPrompt": "a", "model": "gpt-4o-mini"},
                {"id": "meet", "kind": "join"},
                {"id": "respond", "kind": "respond"}
            ],
            "edges": [
                {"from": "fan", "to": "agent_a", "branch": "a"},
                {"from": "agent_a", "to": "meet"},
                {"from": "meet", "to": "respond"}
            ]
        });
        let err = GraphConfig::from_json(&v.to_string()).unwrap_err();
        assert!(
            matches!(&err, GraphError::Invalid(msg) if msg.contains("at least 2 outgoing edges")),
            "got {err:?}"
        );
    }

    #[test]
    fn parallel_duplicate_branch_labels_rejected() {
        let v = serde_json::json!({
            "schemaVersion": 2,
            "entry": "fan",
            "nodes": [
                {"id": "fan", "kind": "parallel"},
                {"id": "agent_a", "kind": "agent", "systemPrompt": "a", "model": "gpt-4o-mini"},
                {"id": "agent_b", "kind": "agent", "systemPrompt": "b", "model": "gpt-4o-mini"},
                {"id": "meet", "kind": "join"},
                {"id": "respond", "kind": "respond"}
            ],
            "edges": [
                {"from": "fan", "to": "agent_a", "branch": "same"},
                {"from": "fan", "to": "agent_b", "branch": "same"},
                {"from": "agent_a", "to": "meet"},
                {"from": "agent_b", "to": "meet"},
                {"from": "meet", "to": "respond"}
            ]
        });
        let err = GraphConfig::from_json(&v.to_string()).unwrap_err();
        assert!(
            matches!(&err, GraphError::Invalid(msg) if msg.contains("duplicate branch label")),
            "got {err:?}"
        );
    }

    #[test]
    fn parallel_branches_sharing_node_before_join_rejected() {
        // agent_shared is on both branches — must be rejected.
        let v = serde_json::json!({
            "schemaVersion": 2,
            "entry": "fan",
            "nodes": [
                {"id": "fan", "kind": "parallel"},
                {"id": "agent_shared", "kind": "agent", "systemPrompt": "shared", "model": "gpt-4o-mini"},
                {"id": "meet", "kind": "join"},
                {"id": "respond", "kind": "respond"}
            ],
            "edges": [
                {"from": "fan", "to": "agent_shared", "branch": "a"},
                {"from": "fan", "to": "agent_shared", "branch": "b"},
                {"from": "agent_shared", "to": "meet"},
                {"from": "meet", "to": "respond"}
            ]
        });
        let err = GraphConfig::from_json(&v.to_string()).unwrap_err();
        assert!(
            matches!(&err, GraphError::Invalid(msg) if msg.contains("share node") || msg.contains("node-disjoint")),
            "got {err:?}"
        );
    }

    #[test]
    fn nested_parallel_rejected() {
        let v = serde_json::json!({
            "schemaVersion": 2,
            "entry": "outer",
            "nodes": [
                {"id": "outer", "kind": "parallel"},
                {"id": "inner", "kind": "parallel"},
                {"id": "agent_a", "kind": "agent", "systemPrompt": "a", "model": "gpt-4o-mini"},
                {"id": "inner_a", "kind": "agent", "systemPrompt": "ia", "model": "gpt-4o-mini"},
                {"id": "inner_b", "kind": "agent", "systemPrompt": "ib", "model": "gpt-4o-mini"},
                {"id": "inner_join", "kind": "join"},
                {"id": "outer_join", "kind": "join"},
                {"id": "respond", "kind": "respond"}
            ],
            "edges": [
                {"from": "outer", "to": "agent_a", "branch": "a"},
                {"from": "outer", "to": "inner", "branch": "b"},
                {"from": "inner", "to": "inner_a", "branch": "x"},
                {"from": "inner", "to": "inner_b", "branch": "y"},
                {"from": "inner_a", "to": "inner_join"},
                {"from": "inner_b", "to": "inner_join"},
                {"from": "inner_join", "to": "outer_join"},
                {"from": "agent_a", "to": "outer_join"},
                {"from": "outer_join", "to": "respond"}
            ]
        });
        let err = GraphConfig::from_json(&v.to_string()).unwrap_err();
        assert!(
            matches!(&err, GraphError::Invalid(msg) if msg.contains("nested parallel")),
            "got {err:?}"
        );
    }

    #[test]
    fn respond_inside_parallel_branch_rejected() {
        let v = serde_json::json!({
            "schemaVersion": 2,
            "entry": "fan",
            "nodes": [
                {"id": "fan", "kind": "parallel"},
                {"id": "agent_a", "kind": "agent", "systemPrompt": "a", "model": "gpt-4o-mini"},
                {"id": "early_respond", "kind": "respond"},
                {"id": "agent_b", "kind": "agent", "systemPrompt": "b", "model": "gpt-4o-mini"},
                {"id": "meet", "kind": "join"},
                {"id": "respond", "kind": "respond"}
            ],
            "edges": [
                {"from": "fan", "to": "agent_a", "branch": "a"},
                {"from": "fan", "to": "agent_b", "branch": "b"},
                {"from": "agent_a", "to": "early_respond"},
                {"from": "agent_b", "to": "meet"},
                {"from": "meet", "to": "respond"}
            ]
        });
        let err = GraphConfig::from_json(&v.to_string()).unwrap_err();
        assert!(
            matches!(&err, GraphError::Invalid(msg) if msg.contains("respond") && msg.contains("branch")),
            "got {err:?}"
        );
    }

    // -----------------------------------------------------------------------
    // Join validation errors
    // -----------------------------------------------------------------------

    #[test]
    fn join_with_one_incoming_edge_rejected() {
        // Build a minimal valid parallel graph and then remove one incoming edge to join.
        let v = serde_json::json!({
            "schemaVersion": 2,
            "entry": "fan",
            "nodes": [
                {"id": "fan", "kind": "parallel"},
                {"id": "agent_a", "kind": "agent", "systemPrompt": "a", "model": "gpt-4o-mini"},
                {"id": "agent_b", "kind": "agent", "systemPrompt": "b", "model": "gpt-4o-mini"},
                {"id": "meet", "kind": "join"},
                {"id": "respond", "kind": "respond"}
            ],
            "edges": [
                {"from": "fan", "to": "agent_a", "branch": "a"},
                {"from": "fan", "to": "agent_b", "branch": "b"},
                {"from": "agent_a", "to": "meet"},
                // Omit agent_b → meet (only 1 incoming to join)
                {"from": "meet", "to": "respond"}
            ]
        });
        let err = GraphConfig::from_json(&v.to_string()).unwrap_err();
        // Either join incoming-edge count failure or parallel branch never reaching join
        assert!(matches!(&err, GraphError::Invalid(_)), "got {err:?}");
    }

    // -----------------------------------------------------------------------
    // v1 fixtures still parse (backward compatibility guard)
    // -----------------------------------------------------------------------

    #[test]
    fn v1_triage_fixture_still_parses_after_v2_changes() {
        let cfg = GraphConfig::from_json(&test_fixtures::triage_json())
            .expect("v1 triage must still parse");
        assert_eq!(cfg.schema_version, 1);
    }

    // -----------------------------------------------------------------------
    // schemaVersion 2 is now accepted (previously unknown_schema_version test
    // used 2 — updated the old test to use 99)
    // -----------------------------------------------------------------------
    #[test]
    fn schema_version_2_is_accepted_for_v2_graphs() {
        // supervisor_json uses schemaVersion 2 — parsing it is the acceptance test.
        GraphConfig::from_json(&test_fixtures::supervisor_json()).expect("v2 graph must parse");
    }

    // -----------------------------------------------------------------------
    // Provider field: backward-compat + round-trip tests
    // -----------------------------------------------------------------------

    /// Existing agent-node JSON without a `"provider"` key must deserialize to
    /// `provider: None`. This is the backward-compatibility guarantee.
    #[test]
    fn agent_node_without_provider_deserializes_to_none() {
        let json = serde_json::json!({
            "id": "agent",
            "kind": "agent",
            "systemPrompt": "You help users.",
            "model": "gpt-4o-mini"
        });
        let node: Node = serde_json::from_value(json).expect("should deserialize");
        match node.kind {
            NodeKind::Agent { provider, .. } => {
                assert_eq!(provider, None, "absent provider must deserialize to None");
            }
            other => panic!("expected Agent, got {other:?}"),
        }
    }

    /// A JSON agent node with `"provider": "anthropic"` must deserialize to
    /// `provider: Some("anthropic")`.
    #[test]
    fn agent_node_with_provider_deserializes_to_some() {
        let json = serde_json::json!({
            "id": "agent",
            "kind": "agent",
            "systemPrompt": "You help users.",
            "model": "claude-3-5-sonnet",
            "provider": "anthropic"
        });
        let node: Node = serde_json::from_value(json).expect("should deserialize");
        match node.kind {
            NodeKind::Agent { provider, .. } => {
                assert_eq!(
                    provider,
                    Some("anthropic".to_string()),
                    "explicit provider must round-trip"
                );
            }
            other => panic!("expected Agent, got {other:?}"),
        }
    }

    /// When `provider` is `None`, the serialized JSON must NOT contain a
    /// `"provider"` key (`skip_serializing_if = "Option::is_none"`).
    #[test]
    fn agent_node_provider_none_is_omitted_from_json() {
        let node = Node {
            id: "agent".to_string(),
            kind: NodeKind::Agent {
                system_prompt: "You help users.".to_string(),
                model: "gpt-4o-mini".to_string(),
                tools: vec![],
                provider: None,
                agent_ref: None,
                inherit_from: None,
            },
        };
        let value = serde_json::to_value(&node).expect("serialization must succeed");
        assert!(
            value.get("provider").is_none(),
            "provider: None must be omitted from JSON, got: {value}"
        );
    }

    #[test]
    fn agent_node_carries_optional_agent_ref() {
        let j = serde_json::json!({
            "schemaVersion": 1,
            "entry": "a",
            "nodes": [
                {"id":"a","kind":"agent","systemPrompt":"","model":"gpt-4","agentRef":"support"},
                {"id":"r","kind":"respond"}
            ],
            "edges": [{"from":"a","to":"r"}]
        });
        let cfg = GraphConfig::from_json(&j.to_string()).unwrap();
        let a = cfg.graph.nodes.iter().find(|n| n.id == "a").unwrap();
        match &a.kind {
            NodeKind::Agent { agent_ref, .. } => assert_eq!(agent_ref.as_deref(), Some("support")),
            other => panic!("expected Agent, got {other:?}"),
        }
        // a present agent_ref re-serializes to the camelCase `agentRef` key
        let back = serde_json::to_value(&a.kind).unwrap();
        assert_eq!(back["agentRef"], serde_json::json!("support"));
    }

    /// SP1 Task 4: an `agent_ref` node is subject to the same "exactly 1
    /// outgoing edge" rule as any other Agent node (Rule 4) — no separate
    /// `agent_ref`-specific validation is needed. Confirms the existing rule
    /// covers it both ways (valid with 1 edge, rejected with 0).
    #[test]
    fn agent_ref_node_validates_with_one_edge() {
        let ok = serde_json::json!({
            "schemaVersion": 1,
            "entry": "a",
            "nodes": [
                {"id":"a","kind":"agent","systemPrompt":"","model":"gpt-4","agentRef":"support"},
                {"id":"r","kind":"respond"}
            ],
            "edges": [{"from":"a","to":"r"}]
        });
        assert!(GraphConfig::from_json(&ok.to_string()).is_ok());

        let bad = serde_json::json!({
            "schemaVersion": 1,
            "entry": "a",
            "nodes": [
                {"id":"a","kind":"agent","systemPrompt":"","model":"gpt-4","agentRef":"support"},
                {"id":"r","kind":"respond"}
            ],
            "edges": []
        });
        let err = GraphConfig::from_json(&bad.to_string())
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("exactly 1 outgoing edge"),
            "unexpected error: {err}"
        );
    }
}
