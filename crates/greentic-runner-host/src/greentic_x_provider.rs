use std::sync::Arc;

use crate::component_api::node::{ExecCtx as ComponentExecCtx, TenantCtx as ComponentTenantCtx};
use crate::pack::PackRuntime;
use greentic_x_runtime::{
    ComponentInvocationEnvelope, ComponentInvocationResultEnvelope, ComponentProvider,
    Fast2FlowRouteRequest, Fast2FlowRouteResult, Fast2FlowRoutingProvider, InMemoryComponentCache,
    RuntimeError, execute_component_with_strategies,
};
use serde_json::Value;
use tokio::runtime::{Builder, Runtime};

/// Greentic-X component provider backed by a materialized Greentic runner pack.
///
/// The provider intentionally invokes components that are already present in the
/// loaded pack runtime. OCI/component resolution remains the responsibility of
/// pack materialization; the Greentic-X envelope supplies the stable invocation
/// contract used by descriptor-driven solutions such as Telco-X.
pub struct RunnerPackComponentProvider {
    pack: Arc<PackRuntime>,
    runtime: Runtime,
    default_operation: String,
    tenant: String,
    flow_id: String,
    strategy_cache: Arc<InMemoryComponentCache>,
}

impl RunnerPackComponentProvider {
    pub fn new(pack: Arc<PackRuntime>) -> Result<Self, RuntimeError> {
        let runtime = Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|err| RuntimeError::ComponentInvocationFailed {
                component_id: "runner-pack-provider".to_owned(),
                message: format!("failed to create component invocation runtime: {err}"),
            })?;
        Ok(Self {
            pack,
            runtime,
            default_operation: "invoke".to_owned(),
            tenant: "default".to_owned(),
            flow_id: "greentic-x.component-invocation".to_owned(),
            strategy_cache: Arc::new(InMemoryComponentCache::new()),
        })
    }

    pub fn with_default_operation(mut self, operation: impl Into<String>) -> Self {
        self.default_operation = operation.into();
        self
    }

    pub fn with_tenant(mut self, tenant: impl Into<String>) -> Self {
        self.tenant = tenant.into();
        self
    }

    pub fn with_flow_id(mut self, flow_id: impl Into<String>) -> Self {
        self.flow_id = flow_id.into();
        self
    }

    fn component_ref(envelope: &ComponentInvocationEnvelope) -> String {
        metadata_string(envelope, "component_ref").unwrap_or_else(|| envelope.component_id.clone())
    }

    fn operation(&self, envelope: &ComponentInvocationEnvelope) -> String {
        metadata_string(envelope, "operation").unwrap_or_else(|| self.default_operation.clone())
    }

    pub fn with_strategy_cache(mut self, cache: Arc<InMemoryComponentCache>) -> Self {
        self.strategy_cache = cache;
        self
    }

    fn exec_ctx(&self, envelope: &ComponentInvocationEnvelope) -> ComponentExecCtx {
        ComponentExecCtx {
            tenant: ComponentTenantCtx {
                tenant: self.tenant.clone(),
                team: None,
                user: Some(envelope.provenance.actor.actor_id.to_string()),
                trace_id: envelope.provenance.trace_id.clone(),
                i18n_id: None,
                correlation_id: envelope
                    .provenance
                    .correlation_id
                    .clone()
                    .or_else(|| envelope.run_id.clone()),
                deadline_unix_ms: None,
                attempt: 1,
                idempotency_key: Some(envelope.invocation_id.clone()),
            },
            i18n_id: None,
            flow_id: envelope
                .run_id
                .clone()
                .unwrap_or_else(|| self.flow_id.clone()),
            node_id: Some(envelope.component_id.clone()),
        }
    }
}

impl RunnerPackComponentProvider {
    fn invoke_component_once(
        &self,
        envelope: ComponentInvocationEnvelope,
    ) -> Result<ComponentInvocationResultEnvelope, RuntimeError> {
        let component_ref = Self::component_ref(&envelope);
        let operation = self.operation(&envelope);
        let ctx = self.exec_ctx(&envelope);
        let input_json = serde_json::to_string(&envelope.input).map_err(|err| {
            RuntimeError::ComponentInvocationFailed {
                component_id: envelope.component_id.clone(),
                message: format!("failed to serialize component input: {err}"),
            }
        })?;
        let invocation_id = envelope.invocation_id.clone();
        let component_id = envelope.component_id.clone();
        let pack = Arc::clone(&self.pack);

        let output = self
            .runtime
            .block_on(async move {
                pack.invoke_component(&component_ref, ctx, &operation, None, input_json)
                    .await
            })
            .map_err(|err| RuntimeError::ComponentInvocationFailed {
                component_id: component_id.clone(),
                message: err.to_string(),
            })?;

        Ok(ComponentInvocationResultEnvelope::success(
            invocation_id,
            component_id,
            output,
        ))
    }
}

struct RunnerPackComponentInvocation<'a> {
    provider: &'a RunnerPackComponentProvider,
}

impl ComponentProvider for RunnerPackComponentInvocation<'_> {
    fn invoke_component(
        &self,
        envelope: ComponentInvocationEnvelope,
    ) -> Result<ComponentInvocationResultEnvelope, RuntimeError> {
        self.provider.invoke_component_once(envelope)
    }
}

impl ComponentProvider for RunnerPackComponentProvider {
    fn invoke_component(
        &self,
        envelope: ComponentInvocationEnvelope,
    ) -> Result<ComponentInvocationResultEnvelope, RuntimeError> {
        let one_shot_provider = RunnerPackComponentInvocation { provider: self };
        execute_component_with_strategies(
            &one_shot_provider,
            envelope,
            Some(self.strategy_cache.as_ref()),
        )
        .map(|outcome| outcome.result)
    }
}

/// Fast2Flow routing provider backed by a materialized Greentic runner pack.
///
/// The routed component is expected to accept `Fast2FlowRouteRequest` JSON and
/// return `Fast2FlowRouteResult` JSON. This keeps Greentic runner on the
/// Greentic-X routing boundary while allowing the concrete Fast2Flow
/// implementation to live in a pack/component.
pub struct RunnerPackFast2FlowRoutingProvider {
    pack: Arc<PackRuntime>,
    runtime: Runtime,
    component_ref: String,
    operation: String,
    tenant: String,
    flow_id: String,
}

impl RunnerPackFast2FlowRoutingProvider {
    pub fn new(pack: Arc<PackRuntime>) -> Result<Self, RuntimeError> {
        let runtime = Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|err| RuntimeError::Fast2FlowRoutingFailed {
                scope: "runner-pack-routing-provider".to_owned(),
                message: format!("failed to create routing invocation runtime: {err}"),
            })?;
        Ok(Self {
            pack,
            runtime,
            component_ref: "fast2flow-routing".to_owned(),
            operation: "route".to_owned(),
            tenant: "default".to_owned(),
            flow_id: "greentic-x.fast2flow-routing".to_owned(),
        })
    }

    pub fn with_component_ref(mut self, component_ref: impl Into<String>) -> Self {
        self.component_ref = component_ref.into();
        self
    }

    pub fn with_operation(mut self, operation: impl Into<String>) -> Self {
        self.operation = operation.into();
        self
    }

    pub fn with_tenant(mut self, tenant: impl Into<String>) -> Self {
        self.tenant = tenant.into();
        self
    }

    pub fn with_flow_id(mut self, flow_id: impl Into<String>) -> Self {
        self.flow_id = flow_id.into();
        self
    }

    fn exec_ctx(&self, request: &Fast2FlowRouteRequest) -> ComponentExecCtx {
        ComponentExecCtx {
            tenant: ComponentTenantCtx {
                tenant: self.tenant.clone(),
                team: None,
                user: None,
                trace_id: None,
                i18n_id: Some(request.input_locale.clone()),
                correlation_id: None,
                deadline_unix_ms: None,
                attempt: 1,
                idempotency_key: Some(format!(
                    "fast2flow:{}:{}:{}",
                    request.scope,
                    request.envelope.channel.as_deref().unwrap_or(""),
                    request.now_unix_ms
                )),
            },
            i18n_id: Some(request.input_locale.clone()),
            flow_id: self.flow_id.clone(),
            node_id: Some(self.component_ref.clone()),
        }
    }
}

impl Fast2FlowRoutingProvider for RunnerPackFast2FlowRoutingProvider {
    fn route_intent(
        &self,
        request: Fast2FlowRouteRequest,
    ) -> Result<Fast2FlowRouteResult, RuntimeError> {
        let scope = request.scope.clone();
        let ctx = self.exec_ctx(&request);
        let input_json = serde_json::to_string(&request).map_err(|err| {
            RuntimeError::Fast2FlowRoutingFailed {
                scope: scope.clone(),
                message: format!("failed to serialize Fast2Flow route request: {err}"),
            }
        })?;
        let pack = Arc::clone(&self.pack);
        let component_ref = self.component_ref.clone();
        let operation = self.operation.clone();

        let output = self
            .runtime
            .block_on(async move {
                pack.invoke_component(&component_ref, ctx, &operation, None, input_json)
                    .await
            })
            .map_err(|err| RuntimeError::Fast2FlowRoutingFailed {
                scope: scope.clone(),
                message: err.to_string(),
            })?;

        serde_json::from_value(output).map_err(|err| RuntimeError::Fast2FlowRoutingFailed {
            scope,
            message: format!("failed to decode Fast2Flow route result: {err}"),
        })
    }
}

fn metadata_string(envelope: &ComponentInvocationEnvelope, key: &str) -> Option<String> {
    envelope
        .metadata
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests {
    use greentic_x_runtime::{ComponentDescriptor, ComponentRuntimeKind};
    use greentic_x_types::{ActorRef, Provenance};

    use super::*;

    #[test]
    fn component_ref_prefers_metadata_mapping() {
        let mut descriptor = ComponentDescriptor::new(
            "zain.analyser.rca",
            "analyser",
            ComponentRuntimeKind::WasmWasi,
            "oci://example/zain-analyser:latest",
        );
        descriptor.metadata.insert(
            "component_ref".to_owned(),
            Value::String("zain-analyser".to_owned()),
        );
        let envelope = ComponentInvocationEnvelope::new(
            "invoke-1",
            &descriptor,
            Value::Null,
            Provenance::new(ActorRef::service("test").expect("actor")),
        );

        assert_eq!(
            RunnerPackComponentProvider::component_ref(&envelope),
            "zain-analyser"
        );
    }
}
