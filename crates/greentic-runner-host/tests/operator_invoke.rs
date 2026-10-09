#![recursion_limit = "256"]
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Write, copy};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use anyhow::{Context, Result};
use greentic_runner_host::{
    RunnerWasiPolicy,
    config::{HostConfig, OperatorPolicy, SecretsPolicy},
    runner::operator::{
        OperatorErrorCode, OperatorPayload, OperatorRequest, OperatorStatus, invoke_operator,
    },
    runtime::TenantRuntime,
    secrets::default_manager,
    storage::{new_session_store, new_state_store, session_host_from, state_host_from},
    trace::TraceConfig,
    validate::ValidationConfig,
};
use greentic_types::{
    ComponentCapabilities, ComponentManifest, ComponentProfiles, EnvId, ExtensionInline,
    ExtensionRef, PROVIDER_EXTENSION_ID, PackKind, PackManifest, ProviderDecl,
    ProviderExtensionInline, ProviderRuntimeRef, ResourceHints, StateKey, TenantCtx, TenantId,
    encode_pack_manifest,
};
use semver::Version;
use serde_json::{Value, json};
use tempfile::TempDir;
use zip::ZipWriter;
use zip::write::FileOptions;

const PROVIDER_TYPE: &str = "example.dummy";
const PROVIDER_OP: &str = "echo";

#[tokio::test]
async fn invoke_operator_api_returns_provider_output() -> Result<()> {
    let workspace = TempDir::new()?;
    let config = minimal_config(workspace.path())?;
    let pack_path = workspace.path().join("operator-provider.gtpack");
    let component_path = build_provider_component()?;
    build_provider_pack(&component_path, &pack_path)?;
    let runtime = setup_runtime(&pack_path, Arc::clone(&config)).await?;

    let payload = serde_cbor::to_vec(&json!({"message": "ping"}))?;
    let request = OperatorRequest {
        tenant_id: Some("demo".into()),
        provider_id: None,
        provider_type: Some(PROVIDER_TYPE.to_string()),
        pack_id: None,
        op_id: PROVIDER_OP.to_string(),
        trace_id: None,
        correlation_id: None,
        timeout: None,
        flags: Vec::new(),
        op_version: None,
        schema_hash: None,
        locale: None,
        payload: OperatorPayload {
            cbor_input: payload,
            attachments: Vec::new(),
        },
    };

    let response = invoke_operator(&runtime, request).await;
    assert!(
        matches!(response.status, OperatorStatus::Ok),
        "unexpected response: {response:?}"
    );
    assert!(response.error.is_none());
    let output = response
        .cbor_output
        .as_deref()
        .context("expected CBOR output for success")?;
    let value: Value = serde_cbor::from_slice(output)?;
    assert_eq!(value, json!({"message": "ping"}));
    let stats = runtime.contract_cache_stats();
    assert_eq!(stats.entries, 1);
    assert_eq!(stats.misses, 1);
    assert_eq!(stats.hits, 0);

    let payload = serde_cbor::to_vec(&json!({"message": "ping"}))?;
    let request = OperatorRequest {
        tenant_id: Some("demo".into()),
        provider_id: None,
        provider_type: Some(PROVIDER_TYPE.to_string()),
        pack_id: None,
        op_id: PROVIDER_OP.to_string(),
        trace_id: None,
        correlation_id: None,
        timeout: None,
        flags: Vec::new(),
        op_version: None,
        schema_hash: None,
        locale: None,
        payload: OperatorPayload {
            cbor_input: payload,
            attachments: Vec::new(),
        },
    };
    let response = invoke_operator(&runtime, request).await;
    assert!(matches!(response.status, OperatorStatus::Ok));
    let stats = runtime.contract_cache_stats();
    assert_eq!(stats.entries, 1);
    assert_eq!(stats.misses, 1);
    assert_eq!(stats.hits, 1);
    Ok(())
}

#[tokio::test]
async fn invoke_operator_api_missing_operation_errors() -> Result<()> {
    let workspace = TempDir::new()?;
    let config = minimal_config(workspace.path())?;
    let pack_path = workspace.path().join("operator-provider.gtpack");
    let component_path = build_provider_component()?;
    build_provider_pack(&component_path, &pack_path)?;
    let runtime = setup_runtime(&pack_path, Arc::clone(&config)).await?;

    let payload = serde_cbor::to_vec(&json!({"message": "ping"}))?;
    let request = OperatorRequest {
        tenant_id: Some("demo".into()),
        provider_id: None,
        provider_type: Some(PROVIDER_TYPE.to_string()),
        pack_id: None,
        op_id: "unknown".to_string(),
        trace_id: None,
        correlation_id: None,
        timeout: None,
        flags: Vec::new(),
        op_version: None,
        schema_hash: None,
        locale: None,
        payload: OperatorPayload {
            cbor_input: payload,
            attachments: Vec::new(),
        },
    };

    let response = invoke_operator(&runtime, request).await;
    assert!(matches!(response.status, OperatorStatus::Error));
    let error = response.error.context("expected error response")?;
    assert!(matches!(error.code, OperatorErrorCode::OpNotFound));
    let details = error
        .details_cbor
        .as_deref()
        .context("expected deterministic diagnostics details")?;
    let diagnostics: Vec<greentic_runner_host::runner::operator::Diagnostic> =
        serde_cbor::from_slice(details)?;
    assert_eq!(diagnostics.len(), 1);
    assert_eq!(diagnostics[0].code, "op_not_found");
    assert_eq!(diagnostics[0].path, "/op_id");
    assert_eq!(diagnostics[0].message_key, "runner.operator.op_not_found");
    assert_eq!(diagnostics[0].operation_id.as_deref(), Some("unknown"));
    assert!(response.cbor_output.is_none());
    Ok(())
}

#[tokio::test]
async fn invoke_operator_api_rejects_schema_hash_mismatch() -> Result<()> {
    let workspace = TempDir::new()?;
    let config = minimal_config(workspace.path())?;
    let pack_path = workspace.path().join("operator-provider.gtpack");
    let component_path = build_provider_component()?;
    build_provider_pack(&component_path, &pack_path)?;
    let runtime = setup_runtime(&pack_path, Arc::clone(&config)).await?;

    let payload = serde_cbor::to_vec(&json!({"message": "ping"}))?;
    let request = OperatorRequest {
        tenant_id: Some("demo".into()),
        provider_id: None,
        provider_type: Some(PROVIDER_TYPE.to_string()),
        pack_id: None,
        op_id: PROVIDER_OP.to_string(),
        trace_id: None,
        correlation_id: None,
        timeout: None,
        flags: Vec::new(),
        op_version: None,
        schema_hash: Some("sha256:deadbeef".to_string()),
        locale: None,
        payload: OperatorPayload {
            cbor_input: payload,
            attachments: Vec::new(),
        },
    };

    let response = invoke_operator(&runtime, request).await;
    assert!(matches!(response.status, OperatorStatus::Error));
    let error = response.error.context("expected schema mismatch error")?;
    assert!(matches!(error.code, OperatorErrorCode::TypeMismatch));
    let details = error
        .details_cbor
        .as_deref()
        .context("expected deterministic diagnostics details")?;
    let diagnostics: Vec<greentic_runner_host::runner::operator::Diagnostic> =
        serde_cbor::from_slice(details)?;
    assert_eq!(diagnostics.len(), 1);
    assert_eq!(diagnostics[0].code, "schema_hash_mismatch");
    assert_eq!(diagnostics[0].path, "/schema_hash");
    assert_eq!(
        diagnostics[0].message_key,
        "runner.operator.schema_hash_mismatch"
    );
    assert_eq!(diagnostics[0].operation_id.as_deref(), Some(PROVIDER_OP));
    Ok(())
}

#[tokio::test]
async fn invoke_operator_api_rejects_invalid_input_against_schema() -> Result<()> {
    let workspace = TempDir::new()?;
    let config = minimal_config(workspace.path())?;
    let pack_path = workspace.path().join("operator-provider.gtpack");
    let component_path = build_provider_component()?;
    build_provider_pack(&component_path, &pack_path)?;
    let runtime = setup_runtime(&pack_path, Arc::clone(&config)).await?;

    let payload = serde_cbor::to_vec(&json!({"message": 42}))?;
    let request = OperatorRequest {
        tenant_id: Some("demo".into()),
        provider_id: None,
        provider_type: Some(PROVIDER_TYPE.to_string()),
        pack_id: None,
        op_id: PROVIDER_OP.to_string(),
        trace_id: None,
        correlation_id: None,
        timeout: None,
        flags: Vec::new(),
        op_version: None,
        schema_hash: None,
        locale: None,
        payload: OperatorPayload {
            cbor_input: payload,
            attachments: Vec::new(),
        },
    };

    let response = invoke_operator(&runtime, request).await;
    assert!(matches!(response.status, OperatorStatus::Error));
    let error = response.error.context("expected schema validation error")?;
    assert!(matches!(error.code, OperatorErrorCode::TypeMismatch));
    let details = error
        .details_cbor
        .as_deref()
        .context("expected deterministic diagnostics details")?;
    let diagnostics: Vec<greentic_runner_host::runner::operator::Diagnostic> =
        serde_cbor::from_slice(details)?;
    assert!(!diagnostics.is_empty());
    assert_eq!(diagnostics[0].code, "schema_validation");
    assert_eq!(
        diagnostics[0].message_key,
        "runner.schema.validation_failed"
    );
    Ok(())
}

#[tokio::test]
async fn invoke_operator_api_strict_rejects_unsupported_schema_constraints() -> Result<()> {
    let workspace = TempDir::new()?;
    let config = minimal_config(workspace.path())?;
    let pack_path = workspace
        .path()
        .join("operator-provider-unsupported.gtpack");
    let component_path = build_provider_component()?;
    build_provider_pack_with_schemas(
        &component_path,
        &pack_path,
        r#"{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "message": {
      "type": "string",
      "pattern": "^[a-z]+$"
    }
  }
}"#,
        None,
    )?;
    let runtime = setup_runtime(&pack_path, Arc::clone(&config)).await?;

    let payload = serde_cbor::to_vec(&json!({"message": "ping"}))?;
    let request = OperatorRequest {
        tenant_id: Some("demo".into()),
        provider_id: None,
        provider_type: Some(PROVIDER_TYPE.to_string()),
        pack_id: None,
        op_id: PROVIDER_OP.to_string(),
        trace_id: None,
        correlation_id: None,
        timeout: None,
        flags: Vec::new(),
        op_version: None,
        schema_hash: None,
        locale: None,
        payload: OperatorPayload {
            cbor_input: payload,
            attachments: Vec::new(),
        },
    };

    let response = invoke_operator(&runtime, request).await;
    assert!(matches!(response.status, OperatorStatus::Error));
    let error = response
        .error
        .context("expected unsupported schema error")?;
    assert!(matches!(error.code, OperatorErrorCode::TypeMismatch));
    let details = error
        .details_cbor
        .as_deref()
        .context("expected deterministic diagnostics details")?;
    let diagnostics: Vec<greentic_runner_host::runner::operator::Diagnostic> =
        serde_cbor::from_slice(details)?;
    assert!(!diagnostics.is_empty());
    assert_eq!(diagnostics[0].code, "unsupported_schema_constraint");
    assert_eq!(
        diagnostics[0].message_key,
        "runner.schema.unsupported_constraint"
    );
    Ok(())
}

#[tokio::test]
async fn invoke_operator_api_rejects_invalid_output_against_output_schema() -> Result<()> {
    let workspace = TempDir::new()?;
    let config = minimal_config(workspace.path())?;
    let pack_path = workspace
        .path()
        .join("operator-provider-output-schema.gtpack");
    let component_path = build_provider_component()?;
    build_provider_pack_with_schemas(
        &component_path,
        &pack_path,
        r#"{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["message"],
  "properties": {
    "message": { "type": "string" }
  },
  "additionalProperties": false
}"#,
        Some(
            r#"{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["result"],
  "properties": {
    "result": { "type": "string" }
  },
  "additionalProperties": false
}"#,
        ),
    )?;
    let runtime = setup_runtime(&pack_path, Arc::clone(&config)).await?;

    let payload = serde_cbor::to_vec(&json!({"message": "ping"}))?;
    let request = OperatorRequest {
        tenant_id: Some("demo".into()),
        provider_id: None,
        provider_type: Some(PROVIDER_TYPE.to_string()),
        pack_id: None,
        op_id: PROVIDER_OP.to_string(),
        trace_id: None,
        correlation_id: None,
        timeout: None,
        flags: Vec::new(),
        op_version: None,
        schema_hash: None,
        locale: None,
        payload: OperatorPayload {
            cbor_input: payload,
            attachments: Vec::new(),
        },
    };

    let response = invoke_operator(&runtime, request).await;
    assert!(matches!(response.status, OperatorStatus::Error));
    let error = response
        .error
        .context("expected output schema validation error")?;
    assert!(matches!(error.code, OperatorErrorCode::TypeMismatch));
    let details = error
        .details_cbor
        .as_deref()
        .context("expected deterministic diagnostics details")?;
    let diagnostics: Vec<greentic_runner_host::runner::operator::Diagnostic> =
        serde_cbor::from_slice(details)?;
    assert!(!diagnostics.is_empty());
    assert_eq!(diagnostics[0].code, "schema_validation");
    assert_eq!(diagnostics[0].path, "/output");
    Ok(())
}

#[tokio::test]
async fn invoke_operator_api_skip_output_validate_bypasses_output_schema_errors() -> Result<()> {
    let workspace = TempDir::new()?;
    let config = minimal_config(workspace.path())?;
    let pack_path = workspace
        .path()
        .join("operator-provider-output-schema-skip.gtpack");
    let component_path = build_provider_component()?;
    build_provider_pack_with_schemas(
        &component_path,
        &pack_path,
        r#"{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["message"],
  "properties": {
    "message": { "type": "string" }
  },
  "additionalProperties": false
}"#,
        Some(
            r#"{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["result"],
  "properties": {
    "result": { "type": "string" }
  },
  "additionalProperties": false
}"#,
        ),
    )?;
    let runtime = setup_runtime(&pack_path, Arc::clone(&config)).await?;

    let payload = serde_cbor::to_vec(&json!({"message": "ping"}))?;
    let request = OperatorRequest {
        tenant_id: Some("demo".into()),
        provider_id: None,
        provider_type: Some(PROVIDER_TYPE.to_string()),
        pack_id: None,
        op_id: PROVIDER_OP.to_string(),
        trace_id: None,
        correlation_id: None,
        timeout: None,
        flags: vec!["skip-output-validate".to_string()],
        op_version: None,
        schema_hash: None,
        locale: None,
        payload: OperatorPayload {
            cbor_input: payload,
            attachments: Vec::new(),
        },
    };

    let response = invoke_operator(&runtime, request).await;
    assert!(matches!(response.status, OperatorStatus::Ok));
    assert!(response.error.is_none());
    Ok(())
}

#[tokio::test]
async fn invoke_operator_api_preserves_provider_instance_config_for_component_runtime() -> Result<()>
{
    let workspace = TempDir::new()?;
    let config = minimal_config(workspace.path())?;
    let pack_path = workspace.path().join("operator-component-config.gtpack");
    let component_path = build_qa_component()?;
    build_component_provider_pack(&component_path, &pack_path)?;
    let (runtime, state_store) = setup_runtime_with_state(&pack_path, Arc::clone(&config)).await?;

    let tenant = TenantCtx::new(EnvId::new("local")?, TenantId::new("demo")?);
    let key = StateKey::from(format!("providers/instances/{PROVIDER_TYPE}.json"));
    state_store.set_json(
        &tenant,
        "runner",
        &key,
        None,
        &json!({
            "provider_id": PROVIDER_TYPE,
            "provider_type": PROVIDER_TYPE,
            "enabled": true,
            "component_ref": "qa.process",
            "export": "node",
            "world": "greentic:component@0.4.0",
            "config": {
                "provider": "ollama",
                "default_model": "llama3.2",
                "base_url": "http://127.0.0.1:11434/v1"
            }
        }),
        None,
    )?;

    let payload = serde_cbor::to_vec(&json!({"message": "ping"}))?;
    // M1.1b: id-only dispatch for state-store-backed instances. The inline
    // pack decl no longer synthesizes `provider_id = Some(provider_type)`,
    // so per_provider_id has no entry. invoke_operator probes the state
    // store via main_pack.resolve_provider to derive provider_type and
    // retries the operator lookup.
    let request = OperatorRequest {
        tenant_id: Some("demo".into()),
        provider_id: Some(PROVIDER_TYPE.to_string()),
        provider_type: None,
        pack_id: None,
        op_id: "process".to_string(),
        trace_id: None,
        correlation_id: None,
        timeout: None,
        flags: Vec::new(),
        op_version: None,
        schema_hash: None,
        locale: None,
        payload: OperatorPayload {
            cbor_input: payload,
            attachments: Vec::new(),
        },
    };

    let response = invoke_operator(&runtime, request).await;
    assert!(
        matches!(response.status, OperatorStatus::Ok),
        "unexpected response: {response:?}"
    );
    let output = response
        .cbor_output
        .as_deref()
        .context("expected CBOR output for success")?;
    let value: Value = serde_cbor::from_slice(output)?;
    assert_eq!(
        value,
        json!({
            "config": {
                "provider": "ollama",
                "default_model": "llama3.2",
                "base_url": "http://127.0.0.1:11434/v1"
            },
            "input": {
                "message": "ping"
            }
        })
    );
    Ok(())
}

fn minimal_config(workspace: &Path) -> Result<Arc<HostConfig>> {
    let bindings_path = workspace.join("bindings.yaml");
    std::fs::write(
        &bindings_path,
        r#"
tenant: demo
flow_type_bindings: {}
rate_limits: {}
retry: {}
timers: []
"#,
    )?;
    let mut config =
        HostConfig::load_from_path(&bindings_path).context("load minimal host bindings")?;
    config.secrets_policy = SecretsPolicy::allow_all();
    config.operator_policy = OperatorPolicy::allow_all();
    config.trace = TraceConfig::from_env();
    config.validation = ValidationConfig::from_env();
    Ok(Arc::new(config))
}

async fn setup_runtime(pack_path: &Path, config: Arc<HostConfig>) -> Result<Arc<TenantRuntime>> {
    let (runtime, _) = setup_runtime_with_state(pack_path, config).await?;
    Ok(runtime)
}

async fn setup_runtime_with_state(
    pack_path: &Path,
    config: Arc<HostConfig>,
) -> Result<(
    Arc<TenantRuntime>,
    greentic_runner_host::storage::DynStateStore,
)> {
    let session_store = new_session_store();
    let session_host = session_host_from(Arc::clone(&session_store));
    let state_store = new_state_store();
    let state_host = state_host_from(Arc::clone(&state_store));
    let secrets = default_manager()?;
    let runtime = TenantRuntime::load(
        pack_path,
        config,
        None,
        Some(pack_path),
        None,
        Arc::new(RunnerWasiPolicy::new()),
        session_host,
        Arc::clone(&session_store),
        Arc::clone(&state_store),
        state_host,
        secrets,
        #[cfg(feature = "agentic-worker")]
        None,
        #[cfg(feature = "agentic-worker")]
        None,
        #[cfg(feature = "agentic-worker")]
        None,
    )
    .await?;
    Ok((runtime, state_store))
}

fn build_provider_pack(component_path: &Path, pack_path: &Path) -> Result<()> {
    build_provider_pack_with_schemas(
        component_path,
        pack_path,
        r#"{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["message"],
  "properties": {
    "message": { "type": "string" }
  },
  "additionalProperties": false
}"#,
        None,
    )
}

fn build_component_provider_pack(component_path: &Path, pack_path: &Path) -> Result<()> {
    let mut extensions = BTreeMap::new();
    let inline = ProviderExtensionInline {
        providers: vec![ProviderDecl {
            provider_type: PROVIDER_TYPE.to_string(),

            capabilities: Vec::new(),
            ops: vec!["process".to_string()],
            config_schema_ref: "schemas/config.schema.json".into(),
            state_schema_ref: Some("schemas/state.schema.json".into()),
            runtime: ProviderRuntimeRef {
                component_ref: "qa.process".into(),
                export: "node".into(),
                world: "greentic:component@0.4.0".into(),
            },
            docs_ref: None,
        }],
        ..Default::default()
    };
    extensions.insert(
        PROVIDER_EXTENSION_ID.to_string(),
        ExtensionRef {
            kind: PROVIDER_EXTENSION_ID.to_string(),
            version: "1.0.0".into(),
            digest: None,
            location: None,
            inline: Some(ExtensionInline::Provider(inline)),
        },
    );

    let manifest = PackManifest {
        schema_version: "1.0".into(),
        pack_id: "operator.component".parse()?,
        name: Some("operator.component".into()),
        version: Version::parse("0.1.0")?,
        kind: PackKind::Application,
        publisher: "test".into(),
        components: vec![ComponentManifest {
            id: "qa.process".parse()?,
            version: Version::parse("0.1.0")?,
            supports: Vec::new(),
            world: "greentic:component@0.4.0".into(),
            profiles: ComponentProfiles::default(),
            capabilities: ComponentCapabilities::default(),
            configurators: None,
            operations: Vec::new(),
            config_schema: None,
            resources: ResourceHints::default(),
            dev_flows: BTreeMap::new(),
        }],
        flows: Vec::new(),
        dependencies: Vec::new(),
        capabilities: Vec::new(),
        signatures: Default::default(),
        secret_requirements: Vec::new(),
        bootstrap: None,
        agents: BTreeMap::new(),
        extensions: Some(extensions),
    };

    let mut writer =
        ZipWriter::new(File::create(pack_path).context("create component provider pack archive")?);
    let options: FileOptions<'_, ()> =
        FileOptions::default().compression_method(zip::CompressionMethod::Stored);
    let manifest_bytes = encode_pack_manifest(&manifest)?;
    writer.start_file("manifest.cbor", options)?;
    writer.write_all(&manifest_bytes)?;

    writer.start_file("components/qa.process.wasm", options)?;
    let mut component_file =
        File::open(component_path).with_context(|| format!("Open {:?}", component_path))?;
    copy(&mut component_file, &mut writer)?;

    writer.start_file("schemas/config.schema.json", options)?;
    writer.write_all(
        br#"{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["message"],
  "properties": {
    "message": { "type": "string" }
  },
  "additionalProperties": false
}"#,
    )?;
    writer.start_file("schemas/output.schema.json", options)?;
    writer.write_all(
        br#"{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["config", "input"],
  "properties": {
    "config": {
      "type": "object",
      "required": ["provider", "default_model", "base_url"],
      "properties": {
        "provider": { "type": "string" },
        "default_model": { "type": "string" },
        "base_url": { "type": "string" }
      },
      "additionalProperties": true
    },
    "input": {
      "type": "object",
      "required": ["message"],
      "properties": {
        "message": { "type": "string" }
      },
      "additionalProperties": true
    }
  },
  "additionalProperties": false
}"#,
    )?;
    writer.start_file("schemas/state.schema.json", options)?;
    writer.write_all(br#"{ "type": "object", "additionalProperties": true }"#)?;
    writer
        .finish()
        .context("finalise component provider pack")?;
    Ok(())
}

fn build_provider_pack_with_schemas(
    component_path: &Path,
    pack_path: &Path,
    config_schema_json: &str,
    output_schema_json: Option<&str>,
) -> Result<()> {
    let mut extensions = BTreeMap::new();
    let inline = ProviderExtensionInline {
        providers: vec![ProviderDecl {
            provider_type: PROVIDER_TYPE.to_string(),

            capabilities: Vec::new(),
            ops: vec![PROVIDER_OP.to_string()],
            config_schema_ref: "schemas/config.schema.json".into(),
            state_schema_ref: Some("schemas/state.schema.json".into()),
            runtime: ProviderRuntimeRef {
                component_ref: "provider.dummy".into(),
                export: "provider-core".into(),
                world: "greentic:provider-core@1.0.0".into(),
            },
            docs_ref: None,
        }],
        ..Default::default()
    };
    extensions.insert(
        PROVIDER_EXTENSION_ID.to_string(),
        ExtensionRef {
            kind: PROVIDER_EXTENSION_ID.to_string(),
            version: "1.0.0".into(),
            digest: None,
            location: None,
            inline: Some(ExtensionInline::Provider(inline)),
        },
    );

    let manifest = PackManifest {
        agents: Default::default(),
        schema_version: "1.0".into(),
        pack_id: "operator.provider".parse()?,
        name: Some("operator.provider".into()),
        version: Version::parse("0.1.0")?,
        kind: PackKind::Application,
        publisher: "test".into(),
        components: vec![ComponentManifest {
            id: "provider.dummy".parse()?,
            version: Version::parse("0.1.0")?,
            supports: Vec::new(),
            world: "greentic:provider-core@1.0.0".into(),
            profiles: ComponentProfiles::default(),
            capabilities: ComponentCapabilities::default(),
            configurators: None,
            operations: Vec::new(),
            config_schema: None,
            resources: ResourceHints::default(),
            dev_flows: BTreeMap::new(),
        }],
        flows: Vec::new(),
        dependencies: Vec::new(),
        capabilities: Vec::new(),
        signatures: Default::default(),
        secret_requirements: Vec::new(),
        bootstrap: None,
        extensions: Some(extensions),
    };

    let mut writer =
        ZipWriter::new(File::create(pack_path).context("create provider pack archive")?);
    let options: FileOptions<'_, ()> =
        FileOptions::default().compression_method(zip::CompressionMethod::Stored);
    let manifest_bytes = encode_pack_manifest(&manifest)?;
    writer.start_file("manifest.cbor", options)?;
    writer.write_all(&manifest_bytes)?;

    writer.start_file("components/provider.dummy.wasm", options)?;
    let mut component_file =
        File::open(component_path).with_context(|| format!("Open {:?}", component_path))?;
    copy(&mut component_file, &mut writer)?;

    writer.start_file("schemas/config.schema.json", options)?;
    writer.write_all(config_schema_json.as_bytes())?;
    if let Some(output_schema) = output_schema_json {
        writer.start_file("schemas/output.schema.json", options)?;
        writer.write_all(output_schema.as_bytes())?;
    }
    writer.start_file("schemas/state.schema.json", options)?;
    writer.write_all(
        br#"{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "cursor": { "type": "string" }
  },
  "additionalProperties": true
}"#,
    )?;
    writer.finish().context("finalise provider pack")?;
    Ok(())
}

fn build_provider_component() -> Result<PathBuf> {
    let root = fixture_path("tests/assets/provider-core-dummy");
    let wasm = root.join("target/wasm32-wasip2/release/provider_core_dummy.wasm");
    if !wasm.exists() {
        let offline = std::env::var("CARGO_NET_OFFLINE").ok();
        let mut cmd = Command::new("cargo");
        let mut args: Vec<String> = vec![
            "build".into(),
            "--release".into(),
            "--target".into(),
            "wasm32-wasip2".into(),
            "--manifest-path".into(),
            root.join("Cargo.toml")
                .to_str()
                .expect("manifest path")
                .into(),
        ];
        if matches!(offline.as_deref(), Some("true")) {
            args.insert(1, "--offline".into());
        }
        if let Some(val) = &offline {
            cmd.env("CARGO_NET_OFFLINE", val);
        }
        cmd.args(&args)
            .status()
            .context("build provider component")?;
    }
    Ok(wasm)
}

fn build_qa_component() -> Result<PathBuf> {
    let root = fixture_path("tests/fixtures/runner-components/qa_process");
    let wasm = fixture_path(
        "tests/fixtures/runner-components/target/wasm32-wasip2/release/qa_process.wasm",
    );
    if !wasm.exists() {
        let offline = std::env::var("CARGO_NET_OFFLINE").ok();
        let mut cmd = Command::new("cargo");
        let mut args: Vec<String> = vec![
            "build".into(),
            "--release".into(),
            "--target".into(),
            "wasm32-wasip2".into(),
            "--manifest-path".into(),
            root.join("Cargo.toml")
                .to_str()
                .expect("manifest path")
                .into(),
        ];
        if matches!(offline.as_deref(), Some("true")) {
            args.insert(1, "--offline".into());
        }
        if let Some(val) = &offline {
            cmd.env("CARGO_NET_OFFLINE", val);
        }
        let status = cmd.args(&args).status().context("build qa component")?;
        if !status.success() {
            anyhow::bail!("failed to build qa_process fixture");
        }
    }
    Ok(wasm)
}

fn fixture_path(relative: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .map(PathBuf::from)
        .expect("workspace root")
        .join(relative)
}
