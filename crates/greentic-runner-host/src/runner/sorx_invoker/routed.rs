//! [`SorxRoutedInvoker`]: one `SorxInvoker` over every SoR a worker binds,
//! each reached through its own route document
//! (`sorla_route::resolve_route`) rather than a single
//! `GREENTIC_AW_SORX_URL`.
//!
//! Discovery is a one-time fetch per bound SoR, exactly like
//! [`super::SorxHttpInvoker`]. Invocation is not: the route document is
//! re-read on every call (never cached on `self`), because a local SoR
//! child restarted on a new port must be followed without rebuilding the
//! invoker — only the discovered capability map is reused.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use greentic_aw_runtime::{SorxInvoker, SorxOperation};
use serde_json::Value;

use super::SORLA_CALLER_ID;
use super::http::{discover, invoke_sor};
use crate::runner::sorla_route::resolve_route;
use crate::secrets::DynSecretsManager;

/// `SorxInvoker` over every SoR named in `sors`, each resolved through its
/// own route document at `secrets://default/<tenant>/<team|_>/sorla/<sor>`.
///
/// `#[allow(dead_code)]`: exercised today only by [`super::tests`]. Nothing
/// outside this module constructs one yet — that lands with its agent-node
/// wiring — so the plain (non-test) build sees no caller.
#[allow(dead_code)]
pub(crate) struct SorxRoutedInvoker {
    client: reqwest::Client,
    secrets: DynSecretsManager,
    tenant: String,
    unit: Option<String>,
    ops: Vec<SorxOperation>,
    cap_by_key: HashMap<(String, String), String>,
}

#[allow(dead_code)]
impl SorxRoutedInvoker {
    /// Resolve and discover every SoR in `sors`. A SoR whose route document
    /// is missing or invalid is skipped — logged by name and reason, never
    /// by token or document value — so one misconfigured SoR does not blank
    /// a worker's whole tool surface.
    pub(crate) async fn discover_all(
        sors: Vec<String>,
        secrets: DynSecretsManager,
        tenant: String,
        unit: Option<String>,
    ) -> Self {
        let client = super::shared_client();
        let mut ops = Vec::new();
        let mut cap_by_key = HashMap::new();
        for sor in &sors {
            match resolve_route(&*secrets, &tenant, unit.as_deref(), sor).await {
                Ok(route) => {
                    let (sor_ops, sor_caps) = discover(&client, &route).await;
                    ops.extend(sor_ops);
                    cap_by_key.extend(sor_caps);
                }
                Err(err) => {
                    tracing::warn!(
                        sor = %sor,
                        error = %err,
                        "sorla: skipping SoR with no usable route document"
                    );
                }
            }
        }
        Self {
            client,
            secrets,
            tenant,
            unit,
            ops,
            cap_by_key,
        }
    }
}

impl SorxInvoker for SorxRoutedInvoker {
    fn list_operations(&self) -> Vec<SorxOperation> {
        self.ops.clone()
    }

    fn invoke<'a>(
        &'a self,
        pack: &'a str,
        action: &'a str,
        args_json: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            let cap = self
                .cap_by_key
                .get(&(pack.to_string(), action.to_string()))
                .ok_or_else(|| format!("no capability for sorla tool '{pack}/{action}'"))?;
            let input: Value = serde_json::from_str(args_json)
                .map_err(|e| format!("invalid sorla tool arguments: {e}"))?;
            // Re-read on every call, never cached: a restarted local SoR
            // child on a new port must be followed without rebuilding the
            // invoker (only `cap_by_key`, built at discovery time, is
            // reused).
            let route = resolve_route(&*self.secrets, &self.tenant, self.unit.as_deref(), pack)
                .await
                .map_err(|err| err.to_string())?;
            invoke_sor(&self.client, &route, cap, input, SORLA_CALLER_ID).await
        })
    }
}
