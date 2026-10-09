use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use greentic_runner_host::config::{
    FlowRetryConfig, HostConfig, OperatorPolicy, RateLimits, SecretsPolicy, StateStorePolicy,
    WebhookPolicy,
};
use greentic_runner_host::engine::host::SessionKey;
use greentic_runner_host::pack::PackRuntime;
use greentic_runner_host::runner::engine::{FlowContext, FlowEngine, FlowStatus};
use greentic_runner_host::runner::flow_adapter::{FlowIR, NodeIR, RouteIR};
use greentic_runner_host::storage::{new_state_store, state_host_from};
use greentic_runner_host::trace::TraceConfig;
use greentic_runner_host::validate::ValidationConfig;
use greentic_types::{EnvId, TenantCtx, TenantId};
use indexmap::IndexMap;
use serde_json::json;

fn host_config(tenant: &str) -> HostConfig {
    HostConfig {
        tenant: tenant.to_string(),
        bindings_path: PathBuf::from("/tmp/bindings.yaml"),
        flow_type_bindings: HashMap::new(),
        rate_limits: RateLimits::default(),
        retry: FlowRetryConfig::default(),
        http_enabled: false,
        secrets_policy: SecretsPolicy::allow_all(),
        state_store_policy: StateStorePolicy::default(),
        webhook_policy: WebhookPolicy::default(),
        timers: Vec::new(),
        oauth: None,
        mocks: None,
        pack_bindings: Vec::new(),
        env_passthrough: Vec::new(),
        trace: TraceConfig::from_env(),
        validation: ValidationConfig::from_env(),
        operator_policy: OperatorPolicy::allow_all(),
        fast2flow: Default::default(),
        #[cfg(feature = "agentic-worker")]
        agents: std::collections::HashMap::new(),
        #[cfg(feature = "agentic-worker")]
        graphs: std::collections::HashMap::new(),
    }
}

fn fixture_component() -> PathBuf {
    let workspace = workspace_root().join("tests/fixtures/runner-components");
    let target_root = workspace.join("target-test");
    target_root.join("wasm32-wasip2/release/qa_process.wasm")
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .map(PathBuf::from)
        .expect("workspace root")
}

fn ensure_fixture_component() -> Result<PathBuf> {
    let artifact = fixture_component();
    if artifact.exists() {
        return Ok(artifact);
    }

    let workspace = workspace_root().join("tests/fixtures/runner-components");
    let target_root = workspace.join("target-test");
    let manifest = workspace.join("qa_process/Cargo.toml");
    let offline = std::env::var("CARGO_NET_OFFLINE").ok();
    let mut cmd = Command::new("cargo");
    if let Some(val) = &offline {
        cmd.env("CARGO_NET_OFFLINE", val);
    }
    let mut args: Vec<String> = vec![
        "build".into(),
        "--manifest-path".into(),
        manifest
            .to_str()
            .ok_or_else(|| anyhow!("fixture manifest path not valid utf-8"))?
            .into(),
        "--target".into(),
        "wasm32-wasip2".into(),
        "--release".into(),
    ];
    if matches!(offline.as_deref(), Some("true")) {
        args.insert(1, "--offline".into());
    }

    let status = cmd
        .env("CARGO_TARGET_DIR", &target_root)
        .current_dir(&workspace)
        .args(args)
        .status()
        .context("failed to build qa_process fixture component")?;
    if !status.success() {
        anyhow::bail!("qa_process fixture component build failed");
    }

    if artifact.exists() {
        Ok(artifact)
    } else {
        Err(anyhow!(
            "component artifact missing: {}",
            artifact.display()
        ))
    }
}

fn flow_ir(flow_id: &str) -> FlowIR {
    let mut nodes = IndexMap::new();
    nodes.insert(
        "start".to_string(),
        NodeIR {
            component: "qa.process".to_string(),
            payload_expr: json!({
                "operation": "process",
                "input": { "text": "hello" }
            }),
            routes: Vec::<RouteIR>::new(),
        },
    );
    FlowIR {
        id: flow_id.to_string(),
        flow_type: "messaging".to_string(),
        start: Some("start".to_string()),
        parameters: json!({}),
        nodes,
    }
}

#[tokio::test]
async fn state_store_key_is_tenant_scoped() -> Result<()> {
    let store = new_state_store();
    let host = state_host_from(store);
    let env = EnvId::new("local")?;
    let tenant_a = TenantId::new("tenant-a")?;
    let tenant_b = TenantId::new("tenant-b")?;
    let ctx_a = TenantCtx::new(env.clone(), tenant_a);
    let ctx_b = TenantCtx::new(env, tenant_b);

    let key_a = SessionKey::new(&ctx_a, "pack-a", "main", Some("session".into()));
    let key_b = SessionKey::new(&ctx_b, "pack-a", "main", Some("session".into()));

    host.set_json(&key_a, json!({"pack": "a"})).await?;
    let value = host.get_json(&key_b).await?;
    assert!(value.is_none(), "tenant scopes must not share state");
    Ok(())
}

#[tokio::test]
async fn state_store_key_includes_pack_id() -> Result<()> {
    let store = new_state_store();
    let host = state_host_from(store);
    let env = EnvId::new("local")?;
    let tenant = TenantId::new("tenant-a")?;
    let ctx = TenantCtx::new(env, tenant);

    let key_a = SessionKey::new(&ctx, "pack-a", "main", Some("session".into()));
    let key_b = SessionKey::new(&ctx, "pack-b", "main", Some("session".into()));

    host.set_json(&key_a, json!({"pack": "a"})).await?;
    let value = host.get_json(&key_b).await?;
    assert!(value.is_none(), "pack-scoped state should not collide");
    Ok(())
}

#[tokio::test]
async fn flow_engine_allows_duplicate_flow_ids_across_packs() -> Result<()> {
    let config = Arc::new(host_config("tenant-a"));
    let component = ensure_fixture_component()?;
    let pack_a = PackRuntime::for_component_test(
        vec![("qa".to_string(), component.clone())],
        HashMap::from([("main".to_string(), flow_ir("main"))]),
        "pack-a",
        Arc::clone(&config),
    )?;
    let pack_b = PackRuntime::for_component_test(
        vec![("qa".to_string(), component)],
        HashMap::from([("main".to_string(), flow_ir("main"))]),
        "pack-b",
        Arc::clone(&config),
    )?;

    let engine = FlowEngine::new(vec![Arc::new(pack_a), Arc::new(pack_b)], config).await?;
    assert_eq!(engine.flows().len(), 2);
    assert!(engine.flow_by_key("pack-a", "main").is_some());
    assert!(engine.flow_by_key("pack-b", "main").is_some());

    let retry_config = FlowRetryConfig::default().into();
    let ctx_a = FlowContext {
        tenant: "tenant-a",
        pack_id: "pack-a",
        flow_id: "main",
        node_id: None,
        tool: None,
        action: None,
        session_id: None,
        provider_id: None,
        reply_scope: None,
        retry_config,
        attempt: 1,
        observer: None,
        mocks: None,
        caller: None,
    };
    let ctx_b = FlowContext {
        tenant: "tenant-a",
        pack_id: "pack-b",
        flow_id: "main",
        node_id: None,
        tool: None,
        action: None,
        session_id: None,
        provider_id: None,
        reply_scope: None,
        retry_config,
        attempt: 1,
        observer: None,
        mocks: None,
        caller: None,
    };

    let exec_a = engine.execute(ctx_a, json!({})).await?;
    let exec_b = engine.execute(ctx_b, json!({})).await?;
    assert!(matches!(exec_a.status, FlowStatus::Completed));
    assert!(matches!(exec_b.status, FlowStatus::Completed));
    Ok(())
}
