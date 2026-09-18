//! Durable agent-graph execution: model, router, executor, checkpointing.
//! Ported from the greentic-designer engine slice (PR #436); see
//! docs/superpowers/specs/2026-06-06-runtime-agent-graph-execution-design.md.
pub mod checkpoint;
pub mod executor;
pub mod http_provider;
pub mod kv_checkpoint;
pub mod model;
pub mod redis_checkpoint;
pub mod router;
pub mod state;

#[cfg(test)]
pub(crate) mod test_fixtures;

pub use checkpoint::{
    CheckpointError, CheckpointStore, GraphRunRecord, InMemoryCheckpointStore, NodeVisitOutcome,
    RunStatus,
};
pub use executor::{
    AgentTurnFn, AgentTurnRequest, AgentTurnResult, ApprovalFn, ApprovalOutcome, ApprovalRequest,
    BoxFut, GraphExecError, GraphExecutor, GraphRunOutcome, SupervisorFn, SupervisorRequest,
    SupervisorResult, ToolCallRequest, ToolFn,
};
pub use http_provider::{CachingGraphProvider, HttpGraphProvider};
pub use kv_checkpoint::KvCheckpointStore;
pub use model::{
    Edge, Graph, GraphConfig, GraphError, Node, NodeKind, SUPPORTED_SCHEMA_VERSIONS,
    SupervisorRoute,
};
pub use redis_checkpoint::RedisCheckpointStore;
pub use router::route;
pub use state::{GraphMessage, GraphRole, GraphRunState};
