use crate::gtbind::PackBinding;
use crate::gtbind::TenantBindings;
use crate::oauth::OAuthBrokerConfig;
use crate::runner::mocks::MocksConfig;
use crate::trace::TraceConfig;
use crate::validate::ValidationConfig;
use anyhow::{Context, Result};
use parking_lot::RwLock;
use serde::Deserialize;
use serde_json::Value;
use serde_yaml_bw as serde_yaml;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct HostConfig {
    pub tenant: String,
    pub bindings_path: PathBuf,
    pub flow_type_bindings: HashMap<String, FlowBinding>,
    pub rate_limits: RateLimits,
    pub retry: FlowRetryConfig,
    pub http_enabled: bool,
    pub secrets_policy: SecretsPolicy,
    pub state_store_policy: StateStorePolicy,
    pub webhook_policy: WebhookPolicy,
    pub timers: Vec<TimerBinding>,
    pub oauth: Option<OAuthConfig>,
    pub mocks: Option<MocksConfig>,
    pub pack_bindings: Vec<PackBinding>,
    pub env_passthrough: Vec<String>,
    pub trace: TraceConfig,
    pub validation: ValidationConfig,
    pub operator_policy: OperatorPolicy,
    pub fast2flow: Fast2FlowRoutingConfig,
    /// Operator-declared Digital Worker agent configs, keyed by `agent_id`.
    /// Sourced from the `agents:` section of the bindings YAML and consumed
    /// by the production `ConfigProvider` (Task 4.3).
    /// Only populated when the `agentic-worker` feature is enabled.
    #[cfg(feature = "agentic-worker")]
    pub agents: HashMap<String, greentic_aw_runtime::AgentConfig>,
    /// Operator/producer-declared agent-graph configs, keyed by `graph_id`.
    /// The pack loader (Task 8) reads `agent-graph.json` sidecars directly, but
    /// this map is the contract that external producers (e.g. greentic-start's
    /// env-file path, the admin registry hand-off) populate to supply graphs
    /// that do not ship as a pack sidecar. Merged with pack-sidecar graphs at
    /// runtime construction (producer entries win on `graph_id` collision).
    /// Only populated when the `agentic-worker` feature is enabled.
    #[cfg(feature = "agentic-worker")]
    pub graphs: HashMap<String, greentic_aw_runtime::graph::GraphConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BindingsFile {
    pub tenant: String,
    #[serde(default)]
    pub flow_type_bindings: HashMap<String, FlowBinding>,
    #[serde(default)]
    pub rate_limits: RateLimits,
    #[serde(default)]
    pub retry: FlowRetryConfig,
    #[serde(default)]
    pub timers: Vec<TimerBinding>,
    #[serde(default)]
    pub oauth: Option<OAuthConfig>,
    #[serde(default)]
    pub mocks: Option<MocksConfig>,
    #[serde(default)]
    pub state_store: StateStorePolicy,
    #[serde(default)]
    pub operator: OperatorPolicyConfig,
    #[serde(default)]
    pub fast2flow: Fast2FlowRoutingConfig,
    /// Digital Worker agent configs keyed by `agent_id`. The whole section is
    /// optional; each value must be a complete `AgentConfig` (all `limits`
    /// fields are required — `AgentLimits` has no serde defaults).
    /// Only present when the `agentic-worker` feature is enabled.
    #[cfg(feature = "agentic-worker")]
    #[serde(default)]
    pub agents: HashMap<String, greentic_aw_runtime::AgentConfig>,
    /// Operator-declared agent-graph configs keyed by `graph_id`. The whole
    /// section is optional; each value must be a complete `GraphConfig`
    /// (camelCase fields, same wire shape as the `agent-graph.json` sidecar).
    /// Only present when the `agentic-worker` feature is enabled.
    #[cfg(feature = "agentic-worker")]
    #[serde(default)]
    pub graphs: HashMap<String, greentic_aw_runtime::graph::GraphConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FlowBinding {
    pub adapter: String,
    #[serde(default)]
    pub config: serde_yaml::Value,
    #[serde(default)]
    pub secrets: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RateLimits {
    #[serde(default = "default_messaging_qps")]
    pub messaging_send_qps: u32,
    #[serde(default = "default_messaging_burst")]
    pub messaging_burst: u32,
}

#[derive(Debug, Clone)]
pub struct SecretsPolicy {
    /// Secret names allowed by the bindings file.
    binding_allowed: HashSet<String>,
    /// Secret names discovered at flow-load time from node configs that
    /// reference secrets via fields ending in `_secret` (e.g.
    /// `api_key_secret: "llm-api-key"`). Shared across all clones so that
    /// flow loading can register names after policy construction.
    flow_discovered: Arc<RwLock<HashSet<String>>>,
    allow_all: bool,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct OperatorPolicyConfig {
    #[serde(default)]
    pub allowed_providers: Vec<String>,
    #[serde(default)]
    pub allowed_ops: HashMap<String, Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct OperatorPolicy {
    allow_all: bool,
    allowed_providers: HashSet<String>,
    allowed_ops: HashMap<String, HashSet<String>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Fast2FlowRoutingConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_fast2flow_component_ref")]
    pub component_ref: String,
    #[serde(default = "default_fast2flow_operation")]
    pub operation: String,
    #[serde(default)]
    pub scope: Option<String>,
    #[serde(default)]
    pub registry_path: String,
    #[serde(default)]
    pub indexes_path: String,
    #[serde(default = "default_fast2flow_time_budget_ms")]
    pub time_budget_ms: u64,
}

impl Default for Fast2FlowRoutingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            component_ref: default_fast2flow_component_ref(),
            operation: default_fast2flow_operation(),
            scope: None,
            registry_path: String::new(),
            indexes_path: String::new(),
            time_budget_ms: default_fast2flow_time_budget_ms(),
        }
    }
}

fn default_fast2flow_component_ref() -> String {
    "fast2flow-routing".to_owned()
}

fn default_fast2flow_operation() -> String {
    "route".to_owned()
}

fn default_fast2flow_time_budget_ms() -> u64 {
    250
}

#[derive(Debug, Clone, Deserialize)]
pub struct FlowRetryConfig {
    #[serde(default = "default_retry_attempts")]
    pub max_attempts: u32,
    #[serde(default = "default_retry_base_delay_ms")]
    pub base_delay_ms: u64,
}

#[derive(Debug, Clone, Default)]
pub struct WebhookPolicy {
    allow_paths: Vec<String>,
    deny_paths: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StateStorePolicy {
    #[serde(default = "default_state_store_allow")]
    pub allow: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WebhookBindingConfig {
    #[serde(default)]
    pub allow_paths: Vec<String>,
    #[serde(default)]
    pub deny_paths: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TimerBinding {
    pub flow_id: String,
    pub cron: String,
    #[serde(default)]
    pub schedule_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OAuthConfig {
    pub http_base_url: String,
    pub nats_url: String,
    pub provider: String,
    #[serde(default)]
    pub env: Option<String>,
    #[serde(default)]
    pub team: Option<String>,
    #[serde(default)]
    pub shared_secret: Option<String>,
}

impl HostConfig {
    pub fn load_from_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let content = fs::read_to_string(path)
            .with_context(|| format!("failed to read bindings file {path:?}"))?;
        let bindings: BindingsFile = serde_yaml::from_str(&content)
            .with_context(|| format!("failed to parse bindings file {path:?}"))?;

        let secrets_policy = SecretsPolicy::from_bindings(&bindings);
        let http_enabled = bindings.flow_type_bindings.contains_key("messaging");
        let webhook_policy = webhook_policy_from_bindings(&bindings)?;

        // Reject unusable tenant ids up front instead of silently running as
        // the `local` tenant later.
        tenant_ctx_for(&bindings.tenant, greentic_env().as_deref())
            .with_context(|| format!("invalid tenant in bindings file {path:?}"))?;

        Ok(Self {
            tenant: bindings.tenant.clone(),
            bindings_path: path.to_path_buf(),
            flow_type_bindings: bindings.flow_type_bindings.clone(),
            rate_limits: bindings.rate_limits.clone(),
            retry: bindings.retry.clone(),
            http_enabled,
            secrets_policy,
            state_store_policy: bindings.state_store.clone(),
            webhook_policy,
            timers: bindings.timers.clone(),
            oauth: bindings.oauth.clone(),
            mocks: bindings.mocks.clone(),
            pack_bindings: Vec::new(),
            env_passthrough: Vec::new(),
            trace: TraceConfig::from_env(),
            validation: ValidationConfig::from_env(),
            operator_policy: OperatorPolicy::from_config(bindings.operator.clone()),
            fast2flow: bindings.fast2flow.clone(),
            #[cfg(feature = "agentic-worker")]
            agents: bindings.agents.clone(),
            #[cfg(feature = "agentic-worker")]
            graphs: bindings.graphs.clone(),
        })
    }

    pub fn from_gtbind(bindings: TenantBindings) -> Self {
        Self {
            tenant: bindings.tenant,
            bindings_path: PathBuf::from("<gtbind>"),
            flow_type_bindings: HashMap::new(),
            rate_limits: RateLimits::default(),
            retry: FlowRetryConfig::default(),
            // GTBind-backed embedded hosts do not populate flow_type_bindings,
            // so the YAML-path heuristic cannot be used here. Keep outbound
            // host HTTP enabled so components like component-llm-openai can
            // reach their configured providers.
            http_enabled: true,
            secrets_policy: SecretsPolicy::allow_all(),
            state_store_policy: StateStorePolicy::default(),
            webhook_policy: WebhookPolicy::default(),
            timers: Vec::new(),
            oauth: None,
            mocks: None,
            pack_bindings: bindings.packs,
            env_passthrough: bindings.env_passthrough,
            trace: TraceConfig::from_env(),
            validation: ValidationConfig::from_env(),
            operator_policy: OperatorPolicy::allow_all(),
            fast2flow: Fast2FlowRoutingConfig::default(),
            // TODO(phase-4): TenantBindings (gtbind) has no agents section yet.
            // When embedded gtbind hosts need Digital Worker agents, extend
            // TenantBindings to carry them and populate this map here.
            #[cfg(feature = "agentic-worker")]
            agents: HashMap::new(),
            // TODO(phase-4): likewise, gtbind carries no agent-graph section;
            // pack sidecars remain the local source of graphs for now.
            #[cfg(feature = "agentic-worker")]
            graphs: HashMap::new(),
        }
    }

    pub fn messaging_binding(&self) -> Option<&FlowBinding> {
        self.flow_type_bindings.get("messaging")
    }

    pub fn retry_config(&self) -> FlowRetryConfig {
        self.retry.clone()
    }

    pub fn oauth_broker_config(&self) -> Option<OAuthBrokerConfig> {
        let env_secret = std::env::var("GREENTIC_OAUTH_BROKER_SHARED_SECRET").ok();
        self.oauth_broker_config_with_env(env_secret.as_deref())
    }

    /// Internal builder that accepts the env-var value explicitly so that
    /// tests can exercise the priority logic without mutating the process
    /// environment (which is unsafe in Rust 2024).
    fn oauth_broker_config_with_env(&self, env_secret: Option<&str>) -> Option<OAuthBrokerConfig> {
        let oauth = self.oauth.as_ref()?;
        let mut cfg = OAuthBrokerConfig::new(&oauth.http_base_url, &oauth.nats_url);
        if !oauth.provider.is_empty() {
            cfg.default_provider = Some(oauth.provider.clone());
        }
        if let Some(team) = &oauth.team
            && !team.is_empty()
        {
            cfg.team = Some(team.clone());
        }
        // Prefer env var; fall back to yaml field. Never log the value.
        cfg.shared_secret = env_secret
            .map(str::to_owned)
            .or_else(|| oauth.shared_secret.clone());
        Some(cfg)
    }

    /// Derive a tenant context for the current configuration. This is used when
    /// evaluating scoped requirements (e.g. secrets).
    ///
    /// Fails when the tenant id or `GREENTIC_ENV` is not a valid identifier;
    /// it never substitutes the `local` tenant/env.
    pub fn try_tenant_ctx(&self) -> Result<greentic_types::TenantCtx> {
        tenant_ctx_for(&self.tenant, greentic_env().as_deref())
    }

    /// Infallible variant kept for callers that cannot propagate errors.
    ///
    /// Configs are validated with [`Self::try_tenant_ctx`] when loaded
    /// (`load_from_path`, `run`), so the fallback below is unreachable for
    /// validated configs; it logs loudly if it is ever taken.
    pub fn tenant_ctx(&self) -> greentic_types::TenantCtx {
        match self.try_tenant_ctx() {
            Ok(ctx) => ctx,
            Err(err) => {
                tracing::error!(
                    tenant = %self.tenant,
                    error = %err,
                    "invalid tenant/env identifier; falling back to local context"
                );
                local_fallback_ctx()
            }
        }
    }
}

/// Last-resort context for the infallible [`HostConfig::tenant_ctx`] shim.
///
/// `local` is a compile-time literal that satisfies the identifier grammar, so
/// the `expect`s below cannot fire; validated configs never reach this path.
#[allow(clippy::expect_used)]
fn local_fallback_ctx() -> greentic_types::TenantCtx {
    greentic_types::TenantCtx::new(
        greentic_types::EnvId::new("local").expect("`local` is a valid env id"),
        greentic_types::TenantId::new("local").expect("`local` is a valid tenant id"),
    )
}

/// A raw `GREENTIC_ENV` / `DEFAULT_TENANT` style value: trimmed, and `None` when
/// absent OR blank. A blank value is "not configured", exactly like an unset one.
pub(crate) fn non_blank(raw: Option<&str>) -> Option<String> {
    raw.map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_string)
}

/// The deployment env from `GREENTIC_ENV`, `None` when unset or blank. Every
/// reader goes through this so an empty value cannot behave differently per
/// call site (previously: `""` in one place, an error in another).
pub(crate) fn greentic_env() -> Option<String> {
    non_blank(std::env::var("GREENTIC_ENV").ok().as_deref())
}

/// [`greentic_env`] with the documented `local` default.
pub(crate) fn greentic_env_or_local() -> String {
    greentic_env().unwrap_or_else(|| "local".to_string())
}

fn tenant_ctx_for(tenant: &str, env: Option<&str>) -> Result<greentic_types::TenantCtx> {
    // An empty `GREENTIC_ENV` is "not configured", like an unset one.
    let env = env
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .unwrap_or("local");
    let env_id = greentic_types::EnvId::from_str(env)
        .map_err(|err| anyhow::anyhow!("invalid GREENTIC_ENV `{env}`: {err}"))?;
    let tenant_id = greentic_types::TenantId::from_str(tenant)
        .map_err(|err| anyhow::anyhow!("invalid tenant id `{tenant}`: {err}"))?;
    // The strictest downstream rule (secrets path), enforced once here.
    crate::secrets_broker::validate_tenant_segment(tenant)?;
    Ok(greentic_types::TenantCtx::new(env_id, tenant_id))
}

fn webhook_policy_from_bindings(bindings: &BindingsFile) -> Result<WebhookPolicy> {
    match bindings.flow_type_bindings.get("webhook") {
        Some(binding) => serde_yaml::from_value::<WebhookBindingConfig>(binding.config.clone())
            .map(WebhookPolicy::from)
            .context("failed to parse webhook binding config"),
        None => Ok(WebhookPolicy::default()),
    }
}

impl SecretsPolicy {
    fn from_bindings(bindings: &BindingsFile) -> Self {
        let binding_allowed = bindings
            .flow_type_bindings
            .values()
            .flat_map(|binding| binding.secrets.iter().cloned())
            .collect::<HashSet<_>>();
        Self {
            binding_allowed,
            flow_discovered: Arc::new(RwLock::new(HashSet::new())),
            allow_all: false,
        }
    }

    pub fn is_allowed(&self, key: &str) -> bool {
        if self.allow_all || self.binding_allowed.contains(key) {
            return true;
        }
        self.flow_discovered.read().contains(key)
    }

    pub fn allow_all() -> Self {
        Self {
            binding_allowed: HashSet::new(),
            flow_discovered: Arc::new(RwLock::new(HashSet::new())),
            allow_all: true,
        }
    }

    /// Register a secret name discovered while loading a flow's node config.
    ///
    /// Components are gated by [`is_allowed`]; the bindings file is the
    /// authoritative source, but flows that reference secrets directly via
    /// node config fields (`api_key_secret: "llm-api-key"`) would otherwise
    /// have their lookups denied even when the secret is provisioned. Calling
    /// this from the pack flow loader closes that gap without changing the
    /// component manifest contract.
    pub fn register_flow_secret(&self, name: &str) {
        if name.is_empty() {
            return;
        }
        self.flow_discovered.write().insert(name.to_string());
    }

    /// Walk a JSON value (typically a flow node's config block) and register
    /// any string-valued field whose key ends in `_secret`. Recurses through
    /// nested objects and arrays.
    pub fn register_flow_secret_refs(&self, value: &Value) {
        match value {
            Value::Object(map) => {
                for (key, val) in map {
                    if key.ends_with("_secret")
                        && let Value::String(name) = val
                    {
                        self.register_flow_secret(name);
                    } else {
                        self.register_flow_secret_refs(val);
                    }
                }
            }
            Value::Array(items) => {
                for item in items {
                    self.register_flow_secret_refs(item);
                }
            }
            _ => {}
        }
    }
}

impl OperatorPolicy {
    pub fn from_config(config: OperatorPolicyConfig) -> Self {
        let allowed_providers = config.allowed_providers.into_iter().collect::<HashSet<_>>();
        let allowed_ops = config
            .allowed_ops
            .into_iter()
            .map(|(provider, ops)| (provider, ops.into_iter().collect::<HashSet<_>>()))
            .collect::<HashMap<_, _>>();
        let allow_all = allowed_providers.is_empty() && allowed_ops.is_empty();
        Self {
            allow_all,
            allowed_providers,
            allowed_ops,
        }
    }

    pub fn allow_all() -> Self {
        Self {
            allow_all: true,
            allowed_providers: HashSet::new(),
            allowed_ops: HashMap::new(),
        }
    }

    pub fn allows_provider(&self, provider_id: Option<&str>, provider_type: &str) -> bool {
        if self.allow_all {
            return true;
        }
        provider_id
            .map(|id| self.allowed_providers.contains(id))
            .unwrap_or(false)
            || self.allowed_providers.contains(provider_type)
    }

    pub fn allows_op(&self, provider_id: Option<&str>, provider_type: &str, op_id: &str) -> bool {
        if self.allow_all {
            return true;
        }
        if let Some(ops) = provider_id.and_then(|id| self.allowed_ops.get(id)) {
            return ops.contains(op_id);
        }
        if let Some(ops) = self.allowed_ops.get(provider_type) {
            return ops.contains(op_id);
        }
        self.allows_provider(provider_id, provider_type)
    }
}

impl Default for RateLimits {
    fn default() -> Self {
        Self {
            messaging_send_qps: default_messaging_qps(),
            messaging_burst: default_messaging_burst(),
        }
    }
}

impl Default for StateStorePolicy {
    fn default() -> Self {
        Self {
            allow: default_state_store_allow(),
        }
    }
}

fn default_messaging_qps() -> u32 {
    10
}

fn default_messaging_burst() -> u32 {
    20
}

fn default_state_store_allow() -> bool {
    true
}

impl From<WebhookBindingConfig> for WebhookPolicy {
    fn from(value: WebhookBindingConfig) -> Self {
        Self {
            allow_paths: value.allow_paths,
            deny_paths: value.deny_paths,
        }
    }
}

impl WebhookPolicy {
    pub fn is_allowed(&self, path: &str) -> bool {
        if self
            .deny_paths
            .iter()
            .any(|prefix| path.starts_with(prefix))
        {
            return false;
        }

        if self.allow_paths.is_empty() {
            return true;
        }

        self.allow_paths
            .iter()
            .any(|prefix| path.starts_with(prefix))
    }
}

impl TimerBinding {
    pub fn schedule_id(&self) -> &str {
        self.schedule_id.as_deref().unwrap_or(self.flow_id.as_str())
    }
}

impl Default for FlowRetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: default_retry_attempts(),
            base_delay_ms: default_retry_base_delay_ms(),
        }
    }
}

#[cfg(test)]
mod operator_policy_tests {
    use super::{OperatorPolicy, OperatorPolicyConfig};
    use std::collections::HashMap;

    #[test]
    fn policy_allows_configured_provider_op() {
        let mut allowed_ops = HashMap::new();
        allowed_ops.insert("provider.allowed".into(), vec!["op1".into(), "op2".into()]);
        let config = OperatorPolicyConfig {
            allowed_providers: vec!["provider.allowed".into()],
            allowed_ops,
        };
        let policy = OperatorPolicy::from_config(config);
        assert!(policy.allows_provider(Some("provider.allowed"), "provider.allowed"));
        assert!(policy.allows_op(Some("provider.allowed"), "provider.allowed", "op1"));
        assert!(!policy.allows_op(Some("provider.allowed"), "provider.allowed", "other"));
        assert!(!policy.allows_provider(Some("provider.denied"), "provider.denied"));
    }

    #[test]
    fn policy_allow_all_defaults_true() {
        let policy = OperatorPolicy::allow_all();
        assert!(policy.allows_provider(None, "any"));
        assert!(policy.allows_op(None, "any", "op"));
    }
}

fn default_retry_attempts() -> u32 {
    3
}

fn default_retry_base_delay_ms() -> u64 {
    250
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;

    #[test]
    fn blank_greentic_env_is_treated_as_unset() {
        for blank in [None, Some(""), Some("  ")] {
            let ctx = tenant_ctx_for("tenant-a", blank).expect("blank env means local");
            assert_eq!(ctx.env.as_str(), "local");
        }
        assert!(tenant_ctx_for("tenant-a", Some("bad env!")).is_err());
    }

    use crate::gtbind::{PackBinding, TenantBindings};
    use std::collections::HashMap;
    use std::path::PathBuf;

    fn host_config_with_oauth(oauth: Option<OAuthConfig>) -> HostConfig {
        HostConfig {
            tenant: "tenant-a".to_string(),
            bindings_path: PathBuf::from("/tmp/bindings.yaml"),
            flow_type_bindings: HashMap::new(),
            rate_limits: RateLimits::default(),
            retry: FlowRetryConfig::default(),
            http_enabled: false,
            secrets_policy: SecretsPolicy::allow_all(),
            state_store_policy: StateStorePolicy::default(),
            webhook_policy: WebhookPolicy::default(),
            timers: Vec::new(),
            oauth,
            mocks: None,
            pack_bindings: Vec::new(),
            env_passthrough: Vec::new(),
            trace: TraceConfig::from_env(),
            validation: ValidationConfig::from_env(),
            operator_policy: OperatorPolicy::allow_all(),
            fast2flow: Fast2FlowRoutingConfig::default(),
            #[cfg(feature = "agentic-worker")]
            agents: HashMap::new(),
            #[cfg(feature = "agentic-worker")]
            graphs: HashMap::new(),
        }
    }

    #[cfg(feature = "agentic-worker")]
    #[test]
    fn load_from_path_parses_agents_section() {
        // All seven `limits` fields are required: AgentLimits has no serde
        // defaults, so operator-authored YAML must spell each one out.
        let yaml = r#"
tenant: acme
agents:
  greeter:
    agent_id: greeter
    system_prompt: "You are a greeter."
    tools: []
    llm:
      provider: openai
      model: gpt-4o-mini
    limits:
      max_iter: 8
      timeout: 60
      max_history_turns: 20
      llm_retry_attempts: 3
      llm_retry_backoff: 250
      provider_failure_message: null
      daily_token_cap_per_tenant: null
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bindings.yaml");
        std::fs::write(&path, yaml).unwrap();
        let cfg = HostConfig::load_from_path(&path).unwrap();
        assert!(cfg.agents.contains_key("greeter"));
        let greeter = &cfg.agents["greeter"];
        assert_eq!(greeter.system_prompt, "You are a greeter.");
        assert_eq!(greeter.limits.max_iter, 8);
        // duration_secs / duration_ms custom serde maps the integers above.
        assert_eq!(greeter.limits.timeout, std::time::Duration::from_secs(60));
        assert_eq!(
            greeter.limits.llm_retry_backoff,
            std::time::Duration::from_millis(250)
        );
    }

    #[cfg(feature = "agentic-worker")]
    #[test]
    fn load_from_path_omitted_agents_section_yields_empty_map() {
        let yaml = "tenant: acme\n";
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bindings.yaml");
        std::fs::write(&path, yaml).unwrap();
        let cfg = HostConfig::load_from_path(&path).unwrap();
        assert!(cfg.agents.is_empty());
    }

    #[test]
    fn host_config_loads_fast2flow_routing_block() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let path = temp.path().join("bindings.yaml");
        std::fs::write(
            &path,
            r#"
tenant: demo
fast2flow:
  enabled: true
  component_ref: router.fast2flow
  operation: handle-hook
  scope: tenant-a
  registry_path: /mnt/registry
  indexes_path: /mnt/indexes
  time_budget_ms: 750
"#,
        )
        .expect("write bindings");

        let cfg = HostConfig::load_from_path(&path).expect("load config");

        assert!(cfg.fast2flow.enabled);
        assert_eq!(cfg.fast2flow.component_ref, "router.fast2flow");
        assert_eq!(cfg.fast2flow.operation, "handle-hook");
        assert_eq!(cfg.fast2flow.scope.as_deref(), Some("tenant-a"));
        assert_eq!(cfg.fast2flow.registry_path, "/mnt/registry");
        assert_eq!(cfg.fast2flow.indexes_path, "/mnt/indexes");
        assert_eq!(cfg.fast2flow.time_budget_ms, 750);
    }

    #[test]
    fn secrets_policy_register_flow_secret_refs_walks_nested_objects() {
        let policy = SecretsPolicy {
            binding_allowed: HashSet::new(),
            flow_discovered: Arc::new(RwLock::new(HashSet::new())),
            allow_all: false,
        };

        // Bare bindings deny anything by default.
        assert!(!policy.is_allowed("llm-api-key"));

        // Simulate a flow node config block carrying both a top-level
        // *_secret reference and a nested one inside an arbitrary subtree.
        let node_config = serde_json::json!({
            "api_key_secret": "llm-api-key",
            "provider": "openai",
            "fallback": {
                "secondary_api_key_secret": "openrouter-key",
                "model": "gpt-4o",
            },
            "list": [
                { "tertiary_secret": "another-key" },
                { "non_secret_field": "ignored" },
            ],
            "ignored_field": ""
        });

        policy.register_flow_secret_refs(&node_config);

        assert!(policy.is_allowed("llm-api-key"));
        assert!(policy.is_allowed("openrouter-key"));
        assert!(policy.is_allowed("another-key"));
        assert!(!policy.is_allowed("non_secret_field"));
        assert!(!policy.is_allowed("ignored"));
    }

    #[test]
    fn secrets_policy_ignores_empty_or_non_string_secret_values() {
        let policy = SecretsPolicy {
            binding_allowed: HashSet::new(),
            flow_discovered: Arc::new(RwLock::new(HashSet::new())),
            allow_all: false,
        };

        let node_config = serde_json::json!({
            "api_key_secret": "",
            "fallback_secret": null,
            "numeric_secret": 42,
            "real_secret": "good-key",
        });

        policy.register_flow_secret_refs(&node_config);

        assert!(policy.is_allowed("good-key"));
        assert!(!policy.is_allowed(""));
    }

    #[test]
    fn oauth_broker_config_absent_without_block() {
        let cfg = host_config_with_oauth(None);
        assert!(cfg.oauth_broker_config().is_none());
    }

    #[test]
    fn oauth_broker_config_maps_fields() {
        let cfg = host_config_with_oauth(Some(OAuthConfig {
            http_base_url: "https://oauth.example/".into(),
            nats_url: "nats://broker:4222".into(),
            provider: "demo".into(),
            env: None,
            team: Some("ops".into()),
            shared_secret: None,
        }));
        // Use `_with_env(None)` so the test is not perturbed by a set
        // GREENTIC_OAUTH_BROKER_SHARED_SECRET environment variable.
        let broker = cfg
            .oauth_broker_config_with_env(None)
            .expect("missing broker config");
        assert_eq!(broker.http_base_url, "https://oauth.example/");
        assert_eq!(broker.nats_url, "nats://broker:4222");
        assert_eq!(broker.default_provider.as_deref(), Some("demo"));
        assert_eq!(broker.team.as_deref(), Some("ops"));
        assert!(broker.shared_secret.is_none());
    }

    #[test]
    fn oauth_broker_config_maps_shared_secret_from_yaml() {
        let cfg = host_config_with_oauth(Some(OAuthConfig {
            http_base_url: "https://oauth.example/".into(),
            nats_url: "nats://broker:4222".into(),
            provider: "demo".into(),
            env: None,
            team: None,
            shared_secret: Some("yaml-secret".into()),
        }));
        // Pass None as the env var to exercise the yaml-field fallback path.
        let broker = cfg
            .oauth_broker_config_with_env(None)
            .expect("missing broker config");
        assert_eq!(broker.shared_secret.as_deref(), Some("yaml-secret"));
    }

    #[test]
    fn oauth_broker_config_env_overrides_yaml_shared_secret() {
        let cfg = host_config_with_oauth(Some(OAuthConfig {
            http_base_url: "https://oauth.example/".into(),
            nats_url: "nats://broker:4222".into(),
            provider: "demo".into(),
            env: None,
            team: None,
            shared_secret: Some("yaml-secret".into()),
        }));
        let broker = cfg
            .oauth_broker_config_with_env(Some("env-secret"))
            .expect("missing broker config");
        assert_eq!(broker.shared_secret.as_deref(), Some("env-secret"));
    }

    #[test]
    fn oauth_broker_config_env_provides_secret_when_yaml_absent() {
        let cfg = host_config_with_oauth(Some(OAuthConfig {
            http_base_url: "https://oauth.example/".into(),
            nats_url: "nats://broker:4222".into(),
            provider: "demo".into(),
            env: None,
            team: None,
            shared_secret: None,
        }));
        let broker = cfg
            .oauth_broker_config_with_env(Some("env-only-secret"))
            .expect("missing broker config");
        assert_eq!(broker.shared_secret.as_deref(), Some("env-only-secret"));
    }

    #[test]
    fn invalid_webhook_binding_config_is_an_error_not_allow_all() {
        let bindings: BindingsFile = serde_yaml::from_str(
            "tenant: demo\nflow_type_bindings:\n  webhook:\n    adapter: webhook\n    config:\n      deny_paths: not-a-list\n",
        )
        .expect("bindings parse");
        let err = webhook_policy_from_bindings(&bindings).expect_err("must fail");
        assert!(format!("{err:#}").contains("webhook binding config"));
    }

    #[test]
    fn valid_webhook_binding_config_keeps_deny_paths() {
        let bindings: BindingsFile = serde_yaml::from_str(
            "tenant: demo\nflow_type_bindings:\n  webhook:\n    adapter: webhook\n    config:\n      deny_paths: [/admin]\n",
        )
        .expect("bindings parse");
        let policy = webhook_policy_from_bindings(&bindings).expect("policy");
        assert!(!policy.is_allowed("/admin/x"));
        assert!(policy.is_allowed("/hook"));
    }

    #[test]
    fn tenant_ids_a_secret_read_would_refuse_are_rejected_at_load() {
        for bad in [".hidden", "a..b", "..", ".", &"a".repeat(129)] {
            let err = tenant_ctx_for(bad, None).expect_err(bad);
            assert!(
                err.to_string().contains("invalid tenant id"),
                "{bad:?}: {err}"
            );
        }
        for good in ["demo", "acme-1.eu", "T_1", "a.b"] {
            assert!(tenant_ctx_for(good, None).is_ok(), "{good:?}");
        }
    }

    #[test]
    fn tenant_ctx_for_rejects_invalid_ids_instead_of_falling_back() {
        assert!(tenant_ctx_for("bad tenant!", None).is_err());
        assert!(tenant_ctx_for("demo", Some("bad env!")).is_err());
        let ctx = tenant_ctx_for("demo", None).expect("valid");
        assert_eq!(ctx.tenant.as_str(), "demo");
        assert_eq!(ctx.env.as_str(), "local");
    }

    #[test]
    fn gtbind_configs_enable_outbound_http() {
        let cfg = HostConfig::from_gtbind(TenantBindings {
            tenant: "demo".into(),
            packs: vec![PackBinding {
                pack_id: "deep-research-demo".into(),
                pack_ref: "deep-research-demo@0.1.0".into(),
                pack_locator: None,
                flows: vec!["main".into()],
            }],
            env_passthrough: Vec::new(),
        });

        assert!(
            cfg.http_enabled,
            "gtbind-backed tenants should allow outbound component HTTP"
        );
    }
}

#[cfg(test)]
mod non_blank_tests {
    use super::*;

    #[test]
    fn blank_and_whitespace_are_unset() {
        assert_eq!(non_blank(None), None);
        assert_eq!(non_blank(Some("")), None);
        assert_eq!(non_blank(Some("  \t")), None);
        assert_eq!(non_blank(Some(" prod ")), Some("prod".to_string()));
    }
}
