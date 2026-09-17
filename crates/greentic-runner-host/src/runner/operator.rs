use axum::{
    body::{Body, to_bytes},
    http::{HeaderMap, Response, StatusCode},
};
use serde::{Deserialize, Serialize};
use serde_cbor;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tracing::{Level, span};

use crate::component_api::node::{ExecCtx as ComponentExecCtx, TenantCtx as ComponentTenantCtx};
use crate::operator_registry::{OperatorBinding, OperatorResolveError};
use crate::provider::ProviderBinding;
use crate::routing::TenantRuntimeHandle;
use crate::runner::contract_cache::ContractSnapshot;
use crate::runner::contract_introspection::introspect_component_contract;
use crate::runner::i18n::{I18nText, resolve_text, select_locale};
use crate::runner::schema_validator::validate_json_instance;
use crate::runtime::TenantRuntime;

const CONTENT_TYPE_CBOR: &str = "application/cbor";
const FLAG_SKIP_OUTPUT_VALIDATE: &str = "skip-output-validate";
const FLAG_PERMISSIVE_SCHEMA: &str = "permissive-schema";

/// Operator-facing invocation payload (CBOR envelope).
#[derive(Debug, Deserialize)]
pub struct OperatorRequest {
    #[serde(default)]
    pub tenant_id: Option<String>,
    #[serde(default)]
    pub provider_id: Option<String>,
    #[serde(default)]
    pub provider_type: Option<String>,
    #[serde(default)]
    pub pack_id: Option<String>,
    pub op_id: String,
    #[serde(default)]
    pub trace_id: Option<String>,
    #[serde(default)]
    pub correlation_id: Option<String>,
    #[serde(default)]
    pub timeout: Option<u64>,
    #[serde(default)]
    pub flags: Vec<String>,
    #[serde(default)]
    pub op_version: Option<String>,
    #[serde(default)]
    pub schema_hash: Option<String>,
    #[serde(default)]
    pub locale: Option<String>,
    pub payload: OperatorPayload,
}

impl OperatorRequest {
    pub fn from_cbor(bytes: &[u8]) -> Result<Self, serde_cbor::Error> {
        serde_cbor::from_slice(bytes)
    }
}

#[derive(Debug, Deserialize)]
pub struct OperatorPayload {
    #[serde(default)]
    #[serde(rename = "cbor_input")]
    pub cbor_input: Vec<u8>,
    #[serde(default)]
    pub attachments: Vec<AttachmentRef>,
}

#[derive(Debug, Deserialize)]
pub struct AttachmentRef {
    pub id: String,
    #[serde(default)]
    pub metadata: Option<Value>,
}

/// Operator response envelope serialized back to CBOR.
#[derive(Debug, Serialize)]
pub struct OperatorResponse {
    pub status: OperatorStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cbor_output: Option<Vec<u8>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<OperatorError>,
}

impl OperatorResponse {
    pub fn ok(output: Vec<u8>) -> Self {
        Self {
            status: OperatorStatus::Ok,
            cbor_output: Some(output),
            error: None,
        }
    }

    pub fn error(code: OperatorErrorCode, message: impl Into<String>) -> Self {
        Self {
            status: OperatorStatus::Error,
            cbor_output: None,
            error: Some(OperatorError {
                code,
                message: message.into(),
                details_cbor: None,
            }),
        }
    }

    pub fn error_with_diagnostics(
        code: OperatorErrorCode,
        message: impl Into<String>,
        diagnostics: Vec<Diagnostic>,
    ) -> Self {
        let details_cbor = serde_cbor::to_vec(&diagnostics).ok();
        Self {
            status: OperatorStatus::Error,
            cbor_output: None,
            error: Some(OperatorError {
                code,
                message: message.into(),
                details_cbor,
            }),
        }
    }

    pub fn to_cbor(&self) -> Result<Vec<u8>, serde_cbor::Error> {
        serde_cbor::ser::to_vec_packed(self)
    }
}

#[derive(Debug, Serialize)]
pub struct OperatorError {
    pub code: OperatorErrorCode,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details_cbor: Option<Vec<u8>>,
}

#[derive(Debug, Serialize)]
pub enum OperatorStatus {
    Ok,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticSeverity {
    Error,
    Warning,
    Info,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Diagnostic {
    pub code: String,
    pub path: String,
    pub severity: DiagnosticSeverity,
    pub message_key: String,
    pub fallback: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub component_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OperatorErrorCode {
    OpNotFound,
    ProviderNotFound,
    TenantNotAllowed,
    InvalidRequest,
    CborDecode,
    TypeMismatch,
    ComponentLoad,
    InvokeTrap,
    Timeout,
    PolicyDenied,
    HostFailure,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
struct ExecutionValidationOptions {
    validate_output: bool,
    strict: bool,
}

impl Default for ExecutionValidationOptions {
    fn default() -> Self {
        Self {
            validate_output: true,
            strict: true,
        }
    }
}

fn validation_options_from_flags(flags: &[String]) -> ExecutionValidationOptions {
    let mut options = ExecutionValidationOptions::default();
    for flag in flags {
        match flag.trim().to_ascii_lowercase().as_str() {
            FLAG_SKIP_OUTPUT_VALIDATE => options.validate_output = false,
            FLAG_PERMISSIVE_SCHEMA => options.strict = false,
            _ => {}
        }
    }
    options
}

fn normalize_operation_id(op_id: &str) -> String {
    let normalized = op_id.trim();
    if normalized.is_empty() {
        "run".to_string()
    } else {
        normalized.to_string()
    }
}

fn normalize_sha256_hash(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.starts_with("sha256:") {
        trimmed.to_string()
    } else {
        format!("sha256:{trimmed}")
    }
}

fn sha256_prefixed(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    format!("sha256:{}", to_hex(&digest))
}

fn to_hex(digest: &[u8]) -> String {
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[derive(Serialize)]
struct SchemaHashMaterial<'a> {
    resolved_digest: &'a str,
    component_ref: &'a str,
    operation_id: &'a str,
    world: &'a str,
    export: &'a str,
    input_schema: &'a Value,
    output_schema: &'a Value,
    config_schema: &'a Value,
    state_schema_ref: Option<&'a str>,
}

#[derive(Serialize)]
struct DescribeHashMaterial<'a> {
    resolved_digest: &'a str,
    component_ref: &'a str,
    world: &'a str,
    export: &'a str,
    pack_ref: &'a str,
    input_schema: &'a Value,
    output_schema: &'a Value,
}

#[allow(clippy::too_many_arguments)]
fn compute_contract_hashes(
    resolved_digest: &str,
    component_ref: &str,
    operation_id: &str,
    world: &str,
    export: &str,
    input_schema: &Value,
    output_schema: &Value,
    config_schema: &Value,
    state_schema_ref: Option<&str>,
    pack_ref: &str,
) -> (String, String) {
    let input_schema = canonicalize_json_value(input_schema.clone());
    let output_schema = canonicalize_json_value(output_schema.clone());
    let config_schema = canonicalize_json_value(config_schema.clone());
    let describe_material = DescribeHashMaterial {
        resolved_digest,
        component_ref,
        world,
        export,
        pack_ref,
        input_schema: &input_schema,
        output_schema: &output_schema,
    };
    let describe_bytes =
        serde_cbor::to_vec(&describe_material).expect("describe hash material serialization");
    let describe_hash = sha256_prefixed(&describe_bytes);

    let schema_material = SchemaHashMaterial {
        resolved_digest,
        component_ref,
        operation_id,
        world,
        export,
        input_schema: &input_schema,
        output_schema: &output_schema,
        config_schema: &config_schema,
        state_schema_ref,
    };
    let schema_bytes =
        serde_cbor::to_vec(&schema_material).expect("schema hash material serialization");
    let schema_hash = sha256_prefixed(&schema_bytes);
    (describe_hash, schema_hash)
}

fn canonicalize_json_value(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut ordered = serde_json::Map::new();
            let mut keys = map.keys().cloned().collect::<Vec<_>>();
            keys.sort();
            for key in keys {
                let normalized = map
                    .get(&key)
                    .cloned()
                    .map(canonicalize_json_value)
                    .unwrap_or(Value::Null);
                ordered.insert(key, normalized);
            }
            Value::Object(ordered)
        }
        Value::Array(items) => {
            Value::Array(items.into_iter().map(canonicalize_json_value).collect())
        }
        other => other,
    }
}

fn derive_output_schema_ref(config_schema_ref: Option<&str>) -> Option<String> {
    let config_ref = config_schema_ref?;
    let candidate = config_ref.replace("config", "output");
    if candidate == config_ref {
        None
    } else {
        Some(candidate)
    }
}

fn schema_issues_to_diagnostics(
    issues: Vec<crate::runner::schema_validator::SchemaValidationIssue>,
    path_prefix: &str,
    component_ref: &str,
    resolved_digest: &str,
    op_id: &str,
    locale: &str,
) -> Vec<Diagnostic> {
    issues
        .into_iter()
        .map(|issue| {
            let text = I18nText::new(issue.message_key, issue.fallback);
            let path = if path_prefix.is_empty() {
                issue.path
            } else if issue.path == "/" {
                path_prefix.to_string()
            } else {
                format!("{path_prefix}{}", issue.path)
            };
            Diagnostic {
                code: issue.code,
                path,
                severity: DiagnosticSeverity::Error,
                message_key: text.message_key.clone(),
                fallback: text.fallback.clone(),
                message: resolve_text(&text, locale),
                hint: None,
                component_id: Some(component_ref.to_string()),
                digest: Some(resolved_digest.to_string()),
                operation_id: Some(op_id.to_string()),
            }
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn diagnostic_error(
    code: &str,
    path: &str,
    message_key: &str,
    fallback: String,
    operation_id: Option<&str>,
    component_id: Option<&str>,
    digest: Option<&str>,
    locale: &str,
) -> Diagnostic {
    let text = I18nText::new(message_key, fallback);
    let message = resolve_text(&text, locale);
    Diagnostic {
        code: code.to_string(),
        path: path.to_string(),
        severity: DiagnosticSeverity::Error,
        message_key: text.message_key,
        message,
        fallback: text.fallback,
        hint: None,
        component_id: component_id.map(ToString::to_string),
        digest: digest.map(ToString::to_string),
        operation_id: operation_id.map(ToString::to_string),
    }
}

impl OperatorErrorCode {
    pub fn reason(&self) -> &'static str {
        match self {
            OperatorErrorCode::OpNotFound => "op not found",
            OperatorErrorCode::ProviderNotFound => "provider not found",
            OperatorErrorCode::TenantNotAllowed => "tenant not allowed",
            OperatorErrorCode::InvalidRequest => "invalid operator request",
            OperatorErrorCode::CborDecode => "failed to decode CBOR payload",
            OperatorErrorCode::TypeMismatch => "type mismatch between CBOR and operation",
            OperatorErrorCode::ComponentLoad => "failed to load component",
            OperatorErrorCode::InvokeTrap => "component trapped during invoke",
            OperatorErrorCode::Timeout => "invocation timed out",
            OperatorErrorCode::PolicyDenied => "policy denied the operation",
            OperatorErrorCode::HostFailure => "internal host failure",
        }
    }
}

/// Invoke an operator request without assuming HTTP transport.
pub async fn invoke_operator(
    runtime: &TenantRuntime,
    request: OperatorRequest,
) -> OperatorResponse {
    let op_id = normalize_operation_id(&request.op_id);
    let validation_options = validation_options_from_flags(&request.flags);
    let locale = select_locale(request.locale.as_deref());
    if let Some(request_tenant) = request.tenant_id.as_deref()
        && request_tenant != runtime.tenant()
    {
        let message = format!(
            "tenant mismatch: routing resolved `{}` but request wants `{request_tenant}`",
            runtime.tenant(),
        );
        return OperatorResponse::error_with_diagnostics(
            OperatorErrorCode::TenantNotAllowed,
            message.clone(),
            vec![diagnostic_error(
                "tenant_mismatch",
                "/tenant_id",
                "runner.operator.tenant_mismatch",
                message,
                Some(op_id.as_str()),
                None,
                runtime.digest(),
                &locale,
            )],
        );
    }

    if request.provider_id.is_none() && request.provider_type.is_none() {
        let message = "operator invoke requires provider_id or provider_type".to_string();
        return OperatorResponse::error_with_diagnostics(
            OperatorErrorCode::InvalidRequest,
            message.clone(),
            vec![diagnostic_error(
                "missing_provider_selector",
                "/provider_id",
                "runner.operator.missing_provider_selector",
                message,
                Some(op_id.as_str()),
                None,
                runtime.digest(),
                &locale,
            )],
        );
    }

    let tenant = runtime.tenant();
    let root_span = span!(
        Level::INFO,
        "operator.invoke",
        tenant = %tenant,
        op_id = %op_id,
        provider_id = ?request.provider_id,
        provider_type = ?request.provider_type
    );
    let _root_guard = root_span.enter();

    let provider_id = request.provider_id.as_deref();
    let provider_type = request.provider_type.as_deref();
    runtime
        .operator_metrics()
        .resolve_attempts
        .fetch_add(1, Ordering::Relaxed);
    let resolve_span = span!(Level::DEBUG, "resolve_op");
    let _resolve_guard = resolve_span.enter();

    let emit_resolve_error = |err: OperatorResolveError| {
        let (code, message) = match err {
            OperatorResolveError::ProviderNotFound => {
                let label = provider_id.or(provider_type).unwrap_or("unknown");
                (
                    OperatorErrorCode::ProviderNotFound,
                    format!("provider `{label}` not registered"),
                )
            }
            OperatorResolveError::OpNotFound => {
                let label = provider_id.or(provider_type).unwrap_or("unknown provider");
                (
                    OperatorErrorCode::OpNotFound,
                    format!("op `{}` not found for provider `{label}`", &op_id),
                )
            }
        };
        runtime
            .operator_metrics()
            .resolve_errors
            .fetch_add(1, Ordering::Relaxed);
        let response = OperatorResponse::error(code, message);
        let diagnostic = diagnostic_error(
            match code {
                OperatorErrorCode::ProviderNotFound => "provider_not_found",
                OperatorErrorCode::OpNotFound => "op_not_found",
                _ => "resolve_error",
            },
            "/op_id",
            match code {
                OperatorErrorCode::ProviderNotFound => "runner.operator.provider_not_found",
                OperatorErrorCode::OpNotFound => "runner.operator.op_not_found",
                _ => "runner.operator.resolve_error",
            },
            response
                .error
                .as_ref()
                .map(|e| e.message.clone())
                .unwrap_or_else(|| "operator resolve failed".to_string()),
            Some(op_id.as_str()),
            binding_component_ref_hint(provider_id, provider_type),
            runtime.digest(),
            &locale,
        );
        OperatorResponse::error_with_diagnostics(
            code,
            response
                .error
                .as_ref()
                .map(|e| e.message.clone())
                .unwrap_or_else(|| "operator resolve failed".to_string()),
            vec![diagnostic],
        )
    };

    // M1.1b: state-store-backed provider instances are not in
    // OperatorRegistry's per_provider_id index (the registry is built from
    // inline pack decls at load time, instances live in the tenant state
    // store at providers/instances/{id}.json). For id-only dispatch, probe
    // the state store via main_pack to derive the provider_type, then re-key
    // the operator lookup by type. The probe's ProviderBinding is carried
    // forward so the second-stage pack.resolve_provider can be skipped — one
    // state-store read covers both the type-derivation and the runtime
    // binding. This also makes the snapshot atomic (no probe-vs-dispatch
    // TOCTOU on the instance file).
    let initial = runtime
        .operator_registry()
        .resolve(provider_id, provider_type, &op_id);
    let (binding, probe_binding): (OperatorBinding, Option<ProviderBinding>) = match initial {
        Ok(b) => (b.clone(), None),
        Err(OperatorResolveError::ProviderNotFound)
            if let Some(id) = provider_id
                && provider_type.is_none() =>
        {
            match runtime.main_pack().resolve_provider(Some(id), None) {
                Ok(pb) => {
                    let derived = pb.provider_type.clone();
                    // Retry by type only: the probe proved this id maps to
                    // that type. The strict OperatorRegistry contract would
                    // reject (Some(id), _) again because the id is not in
                    // per_provider_id.
                    match runtime
                        .operator_registry()
                        .resolve(None, Some(derived.as_str()), &op_id)
                    {
                        Ok(b) => (b.clone(), Some(pb)),
                        Err(err) => return emit_resolve_error(err),
                    }
                }
                Err(probe_err) => {
                    // Don't swallow — disabled instance, malformed JSON, or
                    // state-store I/O errors are all actionable diagnostics
                    // that get rolled up as a generic ProviderNotFound to
                    // the caller but surface in logs for operators.
                    tracing::warn!(
                        provider_id = %id,
                        error = %probe_err,
                        "state-store probe failed for id-only operator dispatch"
                    );
                    return emit_resolve_error(OperatorResolveError::ProviderNotFound);
                }
            }
        }
        Err(err) => return emit_resolve_error(err),
    };
    drop(_resolve_guard);

    let policy = &runtime.config().operator_policy;
    if !policy.allows_provider(provider_id, binding.provider_type.as_str()) {
        return OperatorResponse::error(
            OperatorErrorCode::PolicyDenied,
            format!(
                "provider `{}` not allowed for tenant {}",
                binding
                    .provider_id
                    .as_deref()
                    .unwrap_or(&binding.provider_type),
                runtime.config().tenant
            ),
        );
    }
    if !policy.allows_op(provider_id, binding.provider_type.as_str(), &binding.op_id) {
        return OperatorResponse::error(
            OperatorErrorCode::PolicyDenied,
            format!(
                "op `{}` is not permitted for provider `{}` on tenant {}",
                binding.op_id,
                binding
                    .provider_id
                    .as_deref()
                    .unwrap_or(&binding.provider_type),
                runtime.config().tenant
            ),
        );
    }

    if let Some(req_pack) = request.pack_id.as_deref() {
        let binding_pack = binding
            .pack_ref
            .split('@')
            .next()
            .unwrap_or(&binding.pack_ref);
        if binding_pack != req_pack {
            return OperatorResponse::error(
                OperatorErrorCode::PolicyDenied,
                format!(
                    "request bound to pack `{req_pack}`, but op lives in `{}`",
                    binding.pack_ref
                ),
            );
        }
    }

    let attachments = match resolve_attachments(&request.payload, runtime) {
        Ok(map) => map,
        Err(response) => return response,
    };

    let decode_span = span!(Level::DEBUG, "decode_cbor");
    let _decode_guard = decode_span.enter();
    let input_value = match decode_request_payload(&request.payload.cbor_input) {
        Ok(value) => value,
        Err(err) => {
            runtime
                .operator_metrics()
                .cbor_decode_errors
                .fetch_add(1, Ordering::Relaxed);
            return OperatorResponse::error(OperatorErrorCode::CborDecode, format!("{err}"));
        }
    };
    drop(_decode_guard);

    let input_value = merge_input_with_attachments(input_value, attachments);

    let registry_component_ref = &binding.runtime.component_ref;
    let resolved = match runtime.resolve_component(registry_component_ref) {
        Some(resolved) => resolved,
        None => {
            return OperatorResponse::error(
                OperatorErrorCode::ComponentLoad,
                format!(
                    "component `{}` not found in tenant packs",
                    registry_component_ref
                ),
            );
        }
    };
    let pack = resolved.pack;
    // When the probe above produced a ProviderBinding (id-only dispatch
    // against a state-store instance), reuse it — it's already a coherent
    // snapshot from the same load_instance call that derived the type, so
    // there's nothing to drift against and no second state-store read is
    // needed. For all other paths the second-stage resolve runs as before
    // and ProviderRegistry's defense-in-depth check (binding.provider_type
    // == requested) fires on the (Some(id), Some(type)) shape.
    let provider_binding = match probe_binding {
        Some(pb) => pb,
        None => match pack.resolve_provider(provider_id, provider_type) {
            Ok(binding) => binding,
            Err(err) => {
                return OperatorResponse::error(
                    OperatorErrorCode::HostFailure,
                    format!("failed to resolve provider runtime: {err}"),
                );
            }
        },
    };

    let component_ref = &provider_binding.component_ref;
    let resolved = match runtime.resolve_component(component_ref) {
        Some(resolved) => resolved,
        None => {
            return OperatorResponse::error(
                OperatorErrorCode::ComponentLoad,
                format!("component `{}` not found in tenant packs", component_ref),
            );
        }
    };
    let pack = resolved.pack;
    let resolved_digest = if resolved.digest == "unknown" {
        binding
            .pack_digest
            .clone()
            .unwrap_or_else(|| resolved.digest.clone())
    } else {
        resolved.digest.clone()
    };
    let introspected_contract =
        match introspect_component_contract(pack.as_ref(), component_ref.as_str(), &op_id) {
            Ok(value) => value,
            Err(err) => {
                let message = format!("failed to introspect component contract: {err}");
                return OperatorResponse::error_with_diagnostics(
                    OperatorErrorCode::TypeMismatch,
                    message.clone(),
                    vec![diagnostic_error(
                        "contract_introspection_failed",
                        "/operation",
                        "runner.operator.contract_introspection_failed",
                        message,
                        Some(op_id.as_str()),
                        Some(component_ref.as_str()),
                        Some(resolved_digest.as_str()),
                        &locale,
                    )],
                );
            }
        };
    let invoke_op_id = introspected_contract
        .as_ref()
        .map(|contract| contract.selected_operation.clone())
        .unwrap_or_else(|| op_id.clone());
    let loaded_config_schema = introspected_contract
        .as_ref()
        .map(|contract| contract.config_schema.clone())
        .filter(|value| !value.is_null())
        .or_else(|| {
            binding
                .config_schema_ref
                .as_deref()
                .and_then(|schema_ref| pack.load_schema_json(schema_ref).ok().flatten())
        })
        .unwrap_or(Value::Null);
    let loaded_output_schema = introspected_contract
        .as_ref()
        .map(|contract| contract.output_schema.clone())
        .filter(|value| !value.is_null())
        .or_else(|| {
            derive_output_schema_ref(binding.config_schema_ref.as_deref())
                .and_then(|schema_ref| pack.load_schema_json(&schema_ref).ok().flatten())
        })
        .unwrap_or(Value::Null);
    let loaded_input_schema = introspected_contract
        .as_ref()
        .map(|contract| contract.input_schema.clone())
        .filter(|value| !value.is_null())
        .or_else(|| loaded_config_schema.is_null().then_some(Value::Null))
        .or_else(|| Some(loaded_config_schema.clone()))
        .unwrap_or(Value::Null);
    let loaded_config_schema = binding
        .config_schema_ref
        .as_deref()
        .and_then(|schema_ref| pack.load_schema_json(schema_ref).ok().flatten())
        .unwrap_or_else(|| loaded_config_schema.clone());
    let contract_key = format!(
        "{}::{component_ref}::{op_id}::validate_output={}::strict={}",
        resolved_digest, validation_options.validate_output, validation_options.strict
    );
    let _contract_snapshot = if let Some(snapshot) = runtime.contract_cache().get(&contract_key) {
        snapshot
    } else {
        let (describe_hash, schema_hash) = introspected_contract
            .as_ref()
            .map(|contract| (contract.describe_hash.clone(), contract.schema_hash.clone()))
            .unwrap_or_else(|| {
                compute_contract_hashes(
                    &resolved_digest,
                    component_ref,
                    &invoke_op_id,
                    &provider_binding.world,
                    &provider_binding.export,
                    &loaded_input_schema,
                    &loaded_output_schema,
                    &loaded_config_schema,
                    binding.state_schema_ref.as_deref(),
                    &binding.pack_ref,
                )
            });
        let mut snapshot = ContractSnapshot::new(
            resolved_digest.clone(),
            component_ref.clone(),
            invoke_op_id.clone(),
            validation_options.validate_output,
            validation_options.strict,
        );
        snapshot.describe_hash = Some(describe_hash);
        snapshot.schema_hash = Some(schema_hash);
        let snapshot = Arc::new(snapshot);
        runtime
            .contract_cache()
            .insert(contract_key, Arc::clone(&snapshot));
        snapshot
    };
    if !loaded_input_schema.is_null() {
        let issues = validate_json_instance(
            &loaded_input_schema,
            &input_value,
            validation_options.strict,
        );
        if !issues.is_empty() {
            let diagnostics = schema_issues_to_diagnostics(
                issues,
                "/input",
                component_ref,
                &resolved_digest,
                &op_id,
                &locale,
            );
            return OperatorResponse::error_with_diagnostics(
                OperatorErrorCode::TypeMismatch,
                "input failed schema validation".to_string(),
                diagnostics,
            );
        }
    } else if validation_options.strict && binding.config_schema_ref.is_some() {
        let message = format!(
            "schema `{}` referenced by op `{}` was not found in pack",
            binding.config_schema_ref.as_deref().unwrap_or("unknown"),
            op_id
        );
        return OperatorResponse::error_with_diagnostics(
            OperatorErrorCode::TypeMismatch,
            message.clone(),
            vec![diagnostic_error(
                "schema_ref_not_found",
                "/schema_hash",
                "runner.operator.schema_ref_not_found",
                message,
                Some(op_id.as_str()),
                Some(component_ref.as_str()),
                Some(resolved_digest.as_str()),
                &locale,
            )],
        );
    }

    if let Some(request_schema_hash) = request.schema_hash.as_deref()
        && let Some(expected_schema_hash) = _contract_snapshot.schema_hash.as_deref()
    {
        let expected = normalize_sha256_hash(expected_schema_hash);
        let provided = normalize_sha256_hash(request_schema_hash);
        if expected != provided {
            let message = format!(
                "schema_hash mismatch for op `{}`: expected `{}`, got `{}`",
                op_id, expected, provided
            );
            return OperatorResponse::error_with_diagnostics(
                OperatorErrorCode::TypeMismatch,
                message.clone(),
                vec![diagnostic_error(
                    "schema_hash_mismatch",
                    "/schema_hash",
                    "runner.operator.schema_hash_mismatch",
                    message,
                    Some(op_id.as_str()),
                    Some(component_ref.as_str()),
                    Some(resolved_digest.as_str()),
                    &locale,
                )],
            );
        }
    }

    let input_json = match serde_json::to_string(&input_value) {
        Ok(json) => json,
        Err(err) => {
            return OperatorResponse::error(
                OperatorErrorCode::TypeMismatch,
                format!("failed to serialise input JSON: {err}"),
            );
        }
    };

    let exec_ctx = build_exec_ctx(&request, runtime, &op_id);
    runtime
        .operator_metrics()
        .invoke_attempts
        .fetch_add(1, Ordering::Relaxed);
    let invoke_span = span!(Level::INFO, "invoke_component", component = %component_ref);
    let _invoke_guard = invoke_span.enter();
    let result = if provider_binding.world.starts_with("greentic:provider-core") {
        let input_bytes = input_json.clone().into_bytes();
        match pack
            .invoke_provider(&provider_binding, exec_ctx, &invoke_op_id, input_bytes)
            .await
        {
            Ok(value) => value,
            Err(err) => {
                runtime
                    .operator_metrics()
                    .invoke_errors
                    .fetch_add(1, Ordering::Relaxed);
                return OperatorResponse::error(
                    OperatorErrorCode::HostFailure,
                    format!("provider invoke failed: {err}"),
                );
            }
        }
    } else {
        match pack
            .invoke_component(
                component_ref,
                exec_ctx,
                &invoke_op_id,
                provider_binding.config_json.clone(),
                input_json.clone(),
            )
            .await
        {
            Ok(value) => value,
            Err(err) => {
                runtime
                    .operator_metrics()
                    .invoke_errors
                    .fetch_add(1, Ordering::Relaxed);
                return OperatorResponse::error(
                    OperatorErrorCode::HostFailure,
                    format!("component invoke failed: {err}"),
                );
            }
        }
    };
    drop(_invoke_guard);

    if validation_options.validate_output
        && let Some(output_ref) = derive_output_schema_ref(binding.config_schema_ref.as_deref())
        && let Ok(Some(output_schema)) = pack.load_schema_json(&output_ref)
    {
        let output_value = result
            .as_object()
            .and_then(|obj| obj.get("output"))
            .unwrap_or(&result);
        let issues =
            validate_json_instance(&output_schema, output_value, validation_options.strict);
        if !issues.is_empty() {
            let diagnostics = schema_issues_to_diagnostics(
                issues,
                "/output",
                component_ref,
                &resolved_digest,
                &op_id,
                &locale,
            );
            return OperatorResponse::error_with_diagnostics(
                OperatorErrorCode::TypeMismatch,
                "output failed schema validation".to_string(),
                diagnostics,
            );
        }
    }

    if let Some(new_state) = result.as_object().and_then(|obj| obj.get("new_state")) {
        if let Some(config_ref) = binding.config_schema_ref.as_deref() {
            let config_schema = match pack.load_schema_json(config_ref) {
                Ok(Some(schema)) => schema,
                Ok(None) => {
                    let message = format!(
                        "config schema `{}` required for new_state validation was not found",
                        config_ref
                    );
                    return OperatorResponse::error_with_diagnostics(
                        OperatorErrorCode::TypeMismatch,
                        message.clone(),
                        vec![diagnostic_error(
                            "new_state_schema_missing",
                            "/new_state",
                            "runner.operator.new_state_schema_missing",
                            message,
                            Some(op_id.as_str()),
                            Some(component_ref.as_str()),
                            Some(resolved_digest.as_str()),
                            &locale,
                        )],
                    );
                }
                Err(err) => {
                    let message = format!(
                        "failed to load config schema `{}` for new_state validation: {}",
                        config_ref, err
                    );
                    return OperatorResponse::error_with_diagnostics(
                        OperatorErrorCode::TypeMismatch,
                        message.clone(),
                        vec![diagnostic_error(
                            "new_state_schema_load_failed",
                            "/new_state",
                            "runner.operator.new_state_schema_load_failed",
                            message,
                            Some(op_id.as_str()),
                            Some(component_ref.as_str()),
                            Some(resolved_digest.as_str()),
                            &locale,
                        )],
                    );
                }
            };
            let issues =
                validate_json_instance(&config_schema, new_state, validation_options.strict);
            if !issues.is_empty() {
                let diagnostics = schema_issues_to_diagnostics(
                    issues,
                    "/new_state",
                    component_ref,
                    &resolved_digest,
                    &op_id,
                    &locale,
                );
                return OperatorResponse::error_with_diagnostics(
                    OperatorErrorCode::TypeMismatch,
                    "new_state failed schema validation".to_string(),
                    diagnostics,
                );
            }
        } else if validation_options.strict {
            let message = "new_state returned but no config_schema_ref is available".to_string();
            return OperatorResponse::error_with_diagnostics(
                OperatorErrorCode::TypeMismatch,
                message.clone(),
                vec![diagnostic_error(
                    "new_state_schema_unavailable",
                    "/new_state",
                    "runner.operator.new_state_schema_unavailable",
                    message,
                    Some(op_id.as_str()),
                    Some(component_ref.as_str()),
                    Some(resolved_digest.as_str()),
                    &locale,
                )],
            );
        }
    }

    let encode_span = span!(Level::DEBUG, "encode_cbor");
    let _encode_guard = encode_span.enter();
    let output_bytes = match serde_cbor::to_vec(&result) {
        Ok(bytes) => bytes,
        Err(err) => {
            return OperatorResponse::error(
                OperatorErrorCode::HostFailure,
                format!("failed to encode CBOR output: {err}"),
            );
        }
    };
    drop(_encode_guard);

    OperatorResponse::ok(output_bytes)
}

fn binding_component_ref_hint<'a>(
    provider_id: Option<&'a str>,
    provider_type: Option<&'a str>,
) -> Option<&'a str> {
    provider_id.or(provider_type)
}

/// Convenience helper that takes CBOR bytes and reuses `invoke_operator`.
pub async fn invoke_operator_cbor(
    runtime: &TenantRuntime,
    req_cbor: &[u8],
) -> Result<Vec<u8>, serde_cbor::Error> {
    let request = OperatorRequest::from_cbor(req_cbor)?;
    let response = invoke_operator(runtime, request).await;
    response.to_cbor()
}

/// Axum handler stub for `/operator/op/invoke`.
pub async fn invoke(
    TenantRuntimeHandle { runtime, .. }: TenantRuntimeHandle,
    _headers: HeaderMap,
    body: Body,
) -> Result<Response<Body>, Response<Body>> {
    let bytes = match to_bytes(body, usize::MAX).await {
        Ok(bytes) => bytes,
        Err(err) => {
            return Err(bad_request(format!("failed to read body: {err}")));
        }
    };

    let request = match OperatorRequest::from_cbor(&bytes) {
        Ok(request) => request,
        Err(err) => {
            return Err(bad_request(format!("failed to decode request CBOR: {err}")));
        }
    };

    let response = invoke_operator(&runtime, request).await;
    build_cbor_response(response)
}

fn bad_request(message: String) -> Response<Body> {
    let payload = json!({ "error": message });
    Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .header("content-type", "application/json")
        .body(Body::from(payload.to_string()))
        .expect("building JSON error response must succeed")
}

#[allow(clippy::result_large_err)]
fn build_cbor_response(response: OperatorResponse) -> Result<Response<Body>, Response<Body>> {
    match response.to_cbor() {
        Ok(bytes) => Ok(Response::builder()
            .status(StatusCode::OK)
            .header("content-type", CONTENT_TYPE_CBOR)
            .body(Body::from(bytes))
            .expect("building CBOR response must succeed")),
        Err(err) => Err(bad_request(format!(
            "failed to serialize response CBOR: {err}"
        ))),
    }
}

fn decode_request_payload(bytes: &[u8]) -> Result<Value, serde_cbor::Error> {
    if bytes.is_empty() {
        return Ok(Value::Null);
    }
    serde_cbor::from_slice(bytes)
}

fn build_exec_ctx(
    request: &OperatorRequest,
    runtime: &TenantRuntime,
    operation_id: &str,
) -> ComponentExecCtx {
    let deadline_unix_ms = request.timeout.and_then(|timeout_ms| {
        SystemTime::now()
            .checked_add(Duration::from_millis(timeout_ms))
            .and_then(|deadline| deadline.duration_since(UNIX_EPOCH).ok())
            .map(|duration| duration.as_millis() as u64)
    });

    let tenant_ctx = ComponentTenantCtx {
        tenant: runtime.config().tenant.clone(),
        team: None,
        user: None,
        trace_id: request.trace_id.clone(),
        i18n_id: None,
        correlation_id: request.correlation_id.clone(),
        deadline_unix_ms,
        attempt: 1,
        idempotency_key: request.correlation_id.clone(),
    };

    ComponentExecCtx {
        tenant: tenant_ctx,
        i18n_id: None,
        flow_id: format!("operator/{operation_id}"),
        node_id: None,
    }
}

fn resolve_attachments(
    payload: &OperatorPayload,
    runtime: &TenantRuntime,
) -> Result<Map<String, Value>, OperatorResponse> {
    let mut attachments = Map::new();
    for attachment in &payload.attachments {
        if let Some(kind) = AttachmentKind::from_metadata(attachment.metadata.as_ref()) {
            match kind {
                AttachmentKind::Secret { key, alias } => {
                    let secret = runtime.get_secret(&key).map_err(|err| {
                        OperatorResponse::error(
                            OperatorErrorCode::PolicyDenied,
                            format!("secret `{key}` access denied: {err}"),
                        )
                    })?;
                    attachments.insert(alias, Value::String(secret));
                }
            }
        }
    }
    Ok(attachments)
}

fn merge_input_with_attachments(input: Value, attachments: Map<String, Value>) -> Value {
    if attachments.is_empty() {
        return input;
    }
    match input {
        Value::Object(mut map) => {
            map.insert("_attachments".into(), Value::Object(attachments));
            Value::Object(map)
        }
        other => {
            let mut map = Map::new();
            map.insert("input".into(), other);
            map.insert("_attachments".into(), Value::Object(attachments));
            Value::Object(map)
        }
    }
}

enum AttachmentKind {
    Secret { key: String, alias: String },
}

impl AttachmentKind {
    fn from_metadata(metadata: Option<&Value>) -> Option<Self> {
        let metadata = metadata?.as_object()?;
        let attachment_type = metadata.get("type")?.as_str()?;
        match attachment_type {
            "secret" => {
                let key = metadata.get("key")?.as_str()?.to_string();
                let alias = metadata
                    .get("alias")
                    .and_then(Value::as_str)
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| key.clone());
                Some(AttachmentKind::Secret { key, alias })
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Map, Value, json};

    #[test]
    fn merge_input_with_attachments_preserves_map_fields() {
        let mut attachments = Map::new();
        attachments.insert("secret".into(), json!("value"));
        let mut input_map = Map::new();
        input_map.insert("foo".into(), json!("bar"));
        let merged = merge_input_with_attachments(Value::Object(input_map), attachments.clone());
        let obj = merged.as_object().expect("should be object");
        assert_eq!(obj.get("foo"), Some(&json!("bar")));
        assert_eq!(obj.get("_attachments"), Some(&Value::Object(attachments)));
    }

    #[test]
    fn merge_input_with_attachments_wraps_scalar() {
        let mut attachments = Map::new();
        attachments.insert("secret".into(), json!("value"));
        let merged =
            merge_input_with_attachments(Value::String("text".into()), attachments.clone());
        let obj = merged.as_object().expect("should be object");
        assert_eq!(obj.get("input"), Some(&Value::String("text".into())));
        assert_eq!(obj.get("_attachments"), Some(&Value::Object(attachments)));
    }

    #[test]
    fn attachment_kind_secret_requires_type_lock() {
        let metadata = json!({
            "type": "secret",
            "key": "TOKEN"
        });
        if let Some(AttachmentKind::Secret { key, alias }) =
            AttachmentKind::from_metadata(Some(&metadata))
        {
            assert_eq!(key, "TOKEN");
            assert_eq!(alias, "TOKEN");
        } else {
            panic!("expected secret attachment");
        }
    }

    #[test]
    fn attachment_kind_secret_with_alias() {
        let metadata = json!({
            "type": "secret",
            "key": "TOKEN",
            "alias": "api_token"
        });
        if let Some(AttachmentKind::Secret { key, alias }) =
            AttachmentKind::from_metadata(Some(&metadata))
        {
            assert_eq!(key, "TOKEN");
            assert_eq!(alias, "api_token");
        } else {
            panic!("expected secret attachment");
        }
    }

    #[test]
    fn error_with_diagnostics_encodes_details_cbor() {
        let diagnostics = vec![Diagnostic {
            code: "op_not_found".to_string(),
            path: "/op_id".to_string(),
            severity: DiagnosticSeverity::Error,
            message_key: "runner.operator.op_not_found".to_string(),
            fallback: "op `echo` not found".to_string(),
            message: "op `echo` not found".to_string(),
            hint: None,
            component_id: Some("provider.demo".to_string()),
            digest: Some("sha256:abc123".to_string()),
            operation_id: Some("echo".to_string()),
        }];

        let response = OperatorResponse::error_with_diagnostics(
            OperatorErrorCode::OpNotFound,
            "op not found",
            diagnostics.clone(),
        );
        let details = response
            .error
            .as_ref()
            .and_then(|err| err.details_cbor.as_ref())
            .expect("details_cbor must exist");
        let decoded: Vec<Diagnostic> =
            serde_cbor::from_slice(details).expect("diagnostics should decode");
        assert_eq!(decoded, diagnostics);
    }

    #[test]
    fn validation_options_default_to_strict_with_output_validation() {
        let options = validation_options_from_flags(&[]);
        assert!(options.validate_output);
        assert!(options.strict);
    }

    #[test]
    fn validation_options_apply_known_flags() {
        let options = validation_options_from_flags(&[
            FLAG_SKIP_OUTPUT_VALIDATE.to_string(),
            FLAG_PERMISSIVE_SCHEMA.to_string(),
        ]);
        assert!(!options.validate_output);
        assert!(!options.strict);
    }

    #[test]
    fn normalize_operation_defaults_to_run_when_blank() {
        assert_eq!(normalize_operation_id(""), "run");
        assert_eq!(normalize_operation_id("   "), "run");
        assert_eq!(normalize_operation_id("render"), "render");
    }

    #[test]
    fn compute_contract_hashes_is_deterministic() {
        let input_schema = json!({
            "type": "object",
            "properties": {
                "message": { "type": "string" }
            }
        });
        let output_schema = json!({
            "type": "object",
            "properties": {
                "result": { "type": "string" }
            }
        });
        let one = compute_contract_hashes(
            "sha256:abc",
            "provider.dummy",
            "echo",
            "greentic:provider-core@1.0.0",
            "provider-core",
            &input_schema,
            &output_schema,
            &input_schema,
            Some("schemas/state.schema.json"),
            "operator.provider@0.1.0",
        );
        let two = compute_contract_hashes(
            "sha256:abc",
            "provider.dummy",
            "echo",
            "greentic:provider-core@1.0.0",
            "provider-core",
            &input_schema,
            &output_schema,
            &input_schema,
            Some("schemas/state.schema.json"),
            "operator.provider@0.1.0",
        );
        assert_eq!(one, two);
    }
}
