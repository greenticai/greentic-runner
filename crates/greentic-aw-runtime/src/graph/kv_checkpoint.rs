//! `CheckpointStore` over [`crate::kv::AwKv`] (Redis-free graph checkpoints).
//! Run records and node-visit results are JSON blobs under the `aw:*`
//! namespace with the 7-day checkpoint TTL. Node visits use `set_nx` for the
//! insert-if-absent (replay) semantics.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use crate::graph::checkpoint::{
    CheckpointError, CheckpointStore, GraphRunRecord, NodeVisitOutcome, check_segment,
};
use crate::kv::AwKv;
use crate::tenant::TenantContext;

type BoxFut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

const CHECKPOINT_TTL: Duration = Duration::from_secs(7 * 24 * 60 * 60);

fn run_key(t: &TenantContext, run_id: &str) -> String {
    format!("{}:{run_id}:graph", t.key_prefix())
}

fn visit_key(t: &TenantContext, run_id: &str, node_id: &str, attempt: u32) -> String {
    format!(
        "{}:{run_id}:graph:visit:{node_id}:{attempt}",
        t.key_prefix()
    )
}

fn backend(msg: impl std::fmt::Display) -> CheckpointError {
    CheckpointError::Backend(msg.to_string())
}

/// Graph checkpoint store backed by an [`AwKv`].
pub struct KvCheckpointStore {
    kv: Arc<dyn AwKv>,
}

impl KvCheckpointStore {
    /// Wrap a shared key-value backend.
    pub fn new(kv: Arc<dyn AwKv>) -> Self {
        Self { kv }
    }
}

impl CheckpointStore for KvCheckpointStore {
    fn load<'a>(
        &'a self,
        tenant: &'a TenantContext,
        run_id: &'a str,
    ) -> BoxFut<'a, Result<Option<GraphRunRecord>, CheckpointError>> {
        Box::pin(async move {
            let key = run_key(tenant, run_id);
            match self.kv.get(&key).await.map_err(backend)? {
                Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
                None => Ok(None),
            }
        })
    }

    fn save<'a>(
        &'a self,
        tenant: &'a TenantContext,
        rec: &'a GraphRunRecord,
    ) -> BoxFut<'a, Result<(), CheckpointError>> {
        Box::pin(async move {
            check_segment("run_id", &rec.run_id)?;
            let key = run_key(tenant, &rec.run_id);
            let bytes = serde_json::to_vec(rec)?;
            self.kv
                .set_ex(&key, bytes, CHECKPOINT_TTL)
                .await
                .map_err(backend)
        })
    }

    fn record_node_visit<'a>(
        &'a self,
        tenant: &'a TenantContext,
        run_id: &'a str,
        node_id: &'a str,
        attempt: u32,
        result: &'a serde_json::Value,
    ) -> BoxFut<'a, Result<NodeVisitOutcome, CheckpointError>> {
        Box::pin(async move {
            check_segment("run_id", run_id)?;
            check_segment("node_id", node_id)?;
            let key = visit_key(tenant, run_id, node_id, attempt);
            let bytes = serde_json::to_vec(result)?;
            // Insert-if-absent: set_nx returns false when a value already exists.
            if self
                .kv
                .set_nx(&key, bytes, CHECKPOINT_TTL)
                .await
                .map_err(backend)?
            {
                Ok(NodeVisitOutcome::Recorded)
            } else {
                let existing = self
                    .kv
                    .get(&key)
                    .await
                    .map_err(backend)?
                    .ok_or_else(|| backend("visit vanished after set_nx"))?;
                Ok(NodeVisitOutcome::Replayed(serde_json::from_slice(
                    &existing,
                )?))
            }
        })
    }

    fn load_node_visit<'a>(
        &'a self,
        tenant: &'a TenantContext,
        run_id: &'a str,
        node_id: &'a str,
        attempt: u32,
    ) -> BoxFut<'a, Result<Option<serde_json::Value>, CheckpointError>> {
        Box::pin(async move {
            let key = visit_key(tenant, run_id, node_id, attempt);
            match self.kv.get(&key).await.map_err(backend)? {
                Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
                None => Ok(None),
            }
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::graph::checkpoint::RunStatus;
    use crate::kv::MemoryKv;
    use std::sync::Arc;

    fn store() -> KvCheckpointStore {
        KvCheckpointStore::new(Arc::new(MemoryKv::new()))
    }

    fn record(run_id: &str) -> GraphRunRecord {
        GraphRunRecord {
            run_id: run_id.into(),
            graph_json: "{}".into(),
            cursor: "agent".into(),
            state_json: "{}".into(),
            status: RunStatus::Running,
            visits_json: "{}".into(),
            frontier_json: None,
        }
    }

    #[tokio::test]
    async fn save_then_load_run() {
        let s = store();
        let t = TenantContext::new("acme", "prod");
        assert!(s.load(&t, "run1").await.unwrap().is_none());
        s.save(&t, &record("run1")).await.unwrap();
        assert_eq!(s.load(&t, "run1").await.unwrap().unwrap().cursor, "agent");
    }

    #[tokio::test]
    async fn record_node_visit_is_insert_if_absent() {
        let s = store();
        let t = TenantContext::new("acme", "prod");
        let v1 = serde_json::json!({"n": 1});
        assert_eq!(
            s.record_node_visit(&t, "run1", "node", 0, &v1)
                .await
                .unwrap(),
            NodeVisitOutcome::Recorded
        );
        let v2 = serde_json::json!({"n": 2});
        assert_eq!(
            s.record_node_visit(&t, "run1", "node", 0, &v2)
                .await
                .unwrap(),
            NodeVisitOutcome::Replayed(v1.clone())
        );
        assert_eq!(
            s.load_node_visit(&t, "run1", "node", 0).await.unwrap(),
            Some(v1)
        );
    }
}
