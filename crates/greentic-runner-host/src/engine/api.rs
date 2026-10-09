use super::error::GResult;
use greentic_types::TenantCtx;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FlowSummary {
    pub pack_id: String,
    pub id: String,
    pub name: String,
    pub version: String,
    pub description: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct FlowSchema {
    pub pack_id: String,
    pub id: String,
    pub schema_json: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RunFlowRequest {
    pub tenant: TenantCtx,
    pub pack_id: String,
    pub flow_id: String,
    pub input: serde_json::Value,
    pub session_hint: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RunFlowResult {
    /// Outcome is expressed using standard greentic-types semantics (Done/Pending/Error).
    pub outcome: serde_json::Value,
    /// Per-step node outputs for this turn, keyed by the step's `operation`
    /// (for a `dw.agent` step that is the agent_id). Observability only — this
    /// never affects `outcome`. Defaults to `Null` for back-compat.
    #[serde(default)]
    pub node_outputs: serde_json::Value,
}

#[async_trait::async_trait]
pub trait RunnerApi: Send + Sync {
    async fn list_flows(&self, tenant: &TenantCtx) -> GResult<Vec<FlowSummary>>;
    async fn get_flow_schema(
        &self,
        tenant: &TenantCtx,
        pack_id: &str,
        flow_id: &str,
    ) -> GResult<FlowSchema>;
    async fn run_flow(&self, req: RunFlowRequest) -> GResult<RunFlowResult>;
}
