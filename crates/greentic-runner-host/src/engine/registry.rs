use super::error::GResult;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdapterCall {
    pub adapter: String,
    pub operation: String,
    pub payload: Value,
}

/// What an adapter that drives a flow reports about the nodes it ran.
/// Observability only — never affects the adapter's response.
#[derive(Clone, Debug, Default)]
pub struct NodeIo {
    /// `node_id → node_output_view(payload)`.
    pub outputs: serde_json::Map<String, Value>,
    /// `node_id → the node's rendered input`, for the nodes that ran in this
    /// call (a node restored from a parked snapshot has none).
    pub inputs: serde_json::Map<String, Value>,
}

#[async_trait]
pub trait Adapter: Send + Sync {
    async fn call(&self, call: &AdapterCall) -> GResult<Value>;

    /// Like [`Adapter::call`], but also returns the flow's per-node output map
    /// (`node_id → node_output_view(payload)`) when the adapter drives a flow.
    /// Default: delegate to `call` and contribute no node map. Observability
    /// only — the returned `Value` is identical to `call`.
    async fn call_traced(
        &self,
        call: &AdapterCall,
    ) -> GResult<(Value, serde_json::Map<String, Value>)> {
        Ok((self.call(call).await?, serde_json::Map::new()))
    }

    /// Like [`Adapter::call_traced`], but also reports what each node was
    /// actually run with. Default: delegate to `call_traced` and contribute no
    /// inputs. Observability only — the returned `Value` is identical to `call`.
    async fn call_with_node_io(&self, call: &AdapterCall) -> GResult<(Value, NodeIo)> {
        let (response, outputs) = self.call_traced(call).await?;
        Ok((
            response,
            NodeIo {
                outputs,
                inputs: serde_json::Map::new(),
            },
        ))
    }
}

#[derive(Default, Clone)]
pub struct AdapterRegistry {
    inner: HashMap<String, Arc<dyn Adapter>>,
}

impl AdapterRegistry {
    pub fn register(&mut self, name: impl Into<String>, adapter: Box<dyn Adapter>) {
        self.inner.insert(name.into(), Arc::from(adapter));
    }

    pub fn register_arc(&mut self, name: impl Into<String>, adapter: Arc<dyn Adapter>) {
        self.inner.insert(name.into(), adapter);
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn Adapter>> {
        self.inner.get(name).cloned()
    }
}
