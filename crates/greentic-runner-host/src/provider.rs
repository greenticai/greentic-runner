use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use greentic_types::{
    EnvId, PackManifest, ProviderRuntimeRef, StateKey as StoreStateKey, TenantCtx, TenantId,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::storage::DynStateStore;
use crate::storage::state::STATE_PREFIX;

#[derive(Clone, Debug, Serialize)]
pub struct ProviderBinding {
    pub provider_id: Option<String>,
    pub provider_type: String,
    pub component_ref: String,
    pub export: String,
    pub world: String,
    pub config_json: Option<String>,
    pub pack_ref: Option<String>,
}

#[derive(Clone, Debug)]
pub struct OperatorProviderMetadata {
    pub provider_id: Option<String>,
    pub provider_type: String,
    pub capabilities: Vec<String>,
    pub ops: Vec<String>,
    pub config_schema_ref: Option<String>,
    pub state_schema_ref: Option<String>,
    pub runtime: ProviderRuntimeRef,
    pub docs_ref: Option<String>,
    pub pack_ref: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct ProviderInstance {
    provider_id: String,
    provider_type: String,
    pack_ref: Option<String>,
    component_ref: String,
    export: String,
    world: String,
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    config: Value,
}

#[derive(Clone, Debug, Deserialize)]
struct ProviderExtRuntime {
    component_ref: String,
    export: String,
    world: String,
}

#[derive(Clone, Debug, Deserialize)]
#[allow(dead_code)]
struct ProviderExtDecl {
    #[serde(default)]
    provider_id: Option<String>,
    provider_type: String,
    #[serde(default)]
    capabilities: Vec<String>,
    #[serde(default)]
    ops: Vec<String>,
    #[serde(default)]
    config_schema_ref: Option<String>,
    #[serde(default)]
    state_schema_ref: Option<String>,
    runtime: ProviderExtRuntime,
    #[serde(default)]
    docs_ref: Option<String>,
}

#[derive(Clone)]
pub struct ProviderRegistry {
    pack_ref: Option<String>,
    inline: Vec<ProviderExtDecl>,
    state_store: Option<DynStateStore>,
    tenant: TenantCtx,
}

impl ProviderRegistry {
    pub fn new(
        manifest: &PackManifest,
        state_store: Option<DynStateStore>,
        tenant: &str,
        env: &str,
    ) -> Result<Self> {
        let inline = extract_inline_providers(manifest)?;
        let tenant_ctx = TenantCtx::new(
            EnvId::from_str(env).with_context(|| format!("invalid env id `{env}`"))?,
            TenantId::from_str(tenant).with_context(|| format!("invalid tenant id `{tenant}`"))?,
        );
        let pack_ref = Some(format!(
            "{}@{}",
            manifest.pack_id.as_str(),
            manifest.version
        ));
        Ok(Self {
            pack_ref,
            inline,
            state_store,
            tenant: tenant_ctx,
        })
    }

    pub fn operator_metadata(&self) -> Vec<OperatorProviderMetadata> {
        self.inline
            .iter()
            .map(|decl| OperatorProviderMetadata {
                provider_id: decl.provider_id.clone(),
                provider_type: decl.provider_type.clone(),
                capabilities: decl.capabilities.clone(),
                ops: decl.ops.clone(),
                config_schema_ref: decl.config_schema_ref.clone(),
                state_schema_ref: decl.state_schema_ref.clone(),
                runtime: ProviderRuntimeRef {
                    component_ref: decl.runtime.component_ref.clone(),
                    export: decl.runtime.export.clone(),
                    world: decl.runtime.world.clone(),
                },
                docs_ref: decl.docs_ref.clone(),
                pack_ref: self.pack_ref.clone(),
            })
            .collect()
    }

    /// Resolve `(provider_id, provider_type)` to a binding, returning
    /// `Ok(None)` when no provider runtime is registered for the type
    /// (the "missing binding is OK" case for fan-out probes) and `Err`
    /// only on hard failures (multi-binding collision, id/type mismatch,
    /// instance-file load errors).
    ///
    /// Encapsulates the brittle "no provider runtime found for type"
    /// string discrimination that fan-out callers (identify-instance,
    /// describe-identify-instance) need to skip-vs-fail-closed.
    pub fn try_resolve(
        &self,
        provider_id: Option<&str>,
        provider_type: Option<&str>,
    ) -> Result<Option<ProviderBinding>> {
        match self.resolve(provider_id, provider_type) {
            Ok(binding) => Ok(Some(binding)),
            Err(err)
                if err
                    .to_string()
                    .starts_with("no provider runtime found for type") =>
            {
                Ok(None)
            }
            Err(err) => Err(err),
        }
    }

    /// Like [`try_resolve`] but also returns the declared `ops` list from
    /// the matching inline `provider-extension.v1` declaration.
    ///
    /// Used by Phase D revision-aware host APIs to gate component
    /// invocations against the declared op allowlist before crossing the
    /// WASM call boundary — a defense-in-depth check that ensures a
    /// caller bug or misrouted URL can't run an undeclared op even if
    /// the binding itself resolves.
    ///
    /// `ops` is sourced from the manifest decl whose `provider_type`
    /// matches the resolved binding. For instance-file-loaded bindings
    /// (where the registry has no inline decl for that exact
    /// `provider_id`) the type-matched decl's ops are used; if no inline
    /// decl exposes the type, the returned list is empty and any op
    /// allowlist check will fail closed.
    ///
    /// [`try_resolve`]: ProviderRegistry::try_resolve
    pub fn try_resolve_with_ops(
        &self,
        provider_id: Option<&str>,
        provider_type: Option<&str>,
    ) -> Result<Option<(ProviderBinding, Vec<String>)>> {
        let Some(binding) = self.try_resolve(provider_id, provider_type)? else {
            return Ok(None);
        };
        let ops = self
            .inline
            .iter()
            .find(|decl| decl.provider_type == binding.provider_type)
            .map(|decl| decl.ops.clone())
            .unwrap_or_default();
        Ok(Some((binding, ops)))
    }

    pub fn resolve(
        &self,
        provider_id: Option<&str>,
        provider_type: Option<&str>,
    ) -> Result<ProviderBinding> {
        if provider_id.is_none() && provider_type.is_none() {
            bail!("provider.invoke requires provider_id or provider_type");
        }

        if let Some(id) = provider_id {
            let binding = if let Some(binding) = self.load_instance(id)? {
                binding
            } else if let Some(ext) = self
                .inline
                .iter()
                .find(|decl| decl.provider_id.as_deref() == Some(id))
            {
                binding_from_decl(ext, self.pack_ref.clone(), None)
            } else {
                bail!("provider_id `{id}` not found");
            };

            // Defense-in-depth: when caller supplies both, the resolved
            // binding's provider_type must match the requested provider_type.
            // Catches drift between an instance file's provider_type and the
            // caller's expectation (e.g. an instance retyped from Teams→Slack
            // while flows still target it by id).
            if let Some(ty) = provider_type
                && binding.provider_type != ty
            {
                bail!(
                    "provider_id `{id}` resolved to provider_type `{}`, but caller requested `{ty}`",
                    binding.provider_type
                );
            }
            return Ok(binding);
        }

        let Some(provider_type) = provider_type else {
            bail!("provider selection requires a provider_id or a provider_type");
        };
        let matches: Vec<_> = self
            .inline
            .iter()
            .filter(|decl| decl.provider_type == provider_type)
            .collect();
        match matches.as_slice() {
            [] => bail!("no provider runtime found for type `{provider_type}`"),
            [decl] => Ok(binding_from_decl(
                decl,
                self.pack_ref.clone(),
                Some(provider_type.to_string()),
            )),
            _ => bail!("multiple providers found for type `{provider_type}`, specify provider_id"),
        }
    }

    fn load_instance(&self, provider_id: &str) -> Result<Option<ProviderBinding>> {
        let store = match &self.state_store {
            Some(store) => Arc::clone(store),
            None => return Ok(None),
        };
        let key = StoreStateKey::from(format!("providers/instances/{provider_id}.json"));
        let value = store
            .get_json(&self.tenant, STATE_PREFIX, &key, None)
            .map_err(|err| anyhow!(err.to_string()))
            .with_context(|| format!("failed to load provider instance `{provider_id}`"))?;
        let Some(doc) = value else {
            return Ok(None);
        };
        let instance: ProviderInstance = serde_json::from_value(doc)
            .with_context(|| format!("invalid provider instance `{provider_id}`"))?;
        if !instance.enabled {
            bail!("provider `{provider_id}` is disabled");
        }
        Ok(Some(binding_from_instance(instance)))
    }
}

fn extract_inline_providers(manifest: &PackManifest) -> Result<Vec<ProviderExtDecl>> {
    let Some(inline) = manifest.provider_extension_inline() else {
        return Ok(Vec::new());
    };

    // Trust boundary: validate the wire payload before building any runtime
    // registry from it. validate_basic enforces non-empty/unique provider_type,
    // unique non-empty provider_id, no cross-namespace collisions, and
    // populated runtime fields. Without this gate, OperatorRegistry::build
    // (last-wins HashMap) and ProviderRegistry::resolve (first-wins Vec scan)
    // can diverge on a pack with duplicate provider_ids, routing op metadata
    // to one decl and the actual runtime invocation to another.
    inline
        .validate_basic()
        .context("provider extension inline failed validation")?;

    let providers = inline
        .providers
        .iter()
        .map(|provider| ProviderExtDecl {
            // ProviderDecl.provider_id was removed upstream (greentic-types
            // 1.1.0-dev.27836473437). Inline providers from the manifest no
            // longer carry a separate identity; the provider_type is the sole
            // identifier. Instance-file-loaded providers still carry an id.
            provider_id: None,
            provider_type: provider.provider_type.clone(),
            capabilities: provider.capabilities.clone(),
            ops: provider.ops.clone(),
            config_schema_ref: Some(provider.config_schema_ref.clone()),
            state_schema_ref: provider.state_schema_ref.clone(),
            runtime: ProviderExtRuntime {
                component_ref: provider.runtime.component_ref.clone(),
                export: provider.runtime.export.clone(),
                world: provider.runtime.world.clone(),
            },
            docs_ref: provider.docs_ref.clone(),
        })
        .collect();

    Ok(providers)
}

fn binding_from_decl(
    decl: &ProviderExtDecl,
    pack_ref: Option<String>,
    default_provider_id: Option<String>,
) -> ProviderBinding {
    ProviderBinding {
        provider_id: decl.provider_id.clone().or(default_provider_id),
        provider_type: decl.provider_type.clone(),
        component_ref: decl.runtime.component_ref.clone(),
        export: decl.runtime.export.clone(),
        world: decl.runtime.world.clone(),
        config_json: None,
        pack_ref,
    }
}

fn binding_from_instance(instance: ProviderInstance) -> ProviderBinding {
    ProviderBinding {
        config_json: if instance.config.is_null() {
            None
        } else {
            Some(instance.config.to_string())
        },
        provider_id: Some(instance.provider_id),
        provider_type: instance.provider_type,
        component_ref: instance.component_ref,
        export: instance.export,
        world: instance.world,
        pack_ref: instance.pack_ref,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ext_decl(provider_type: &str, ops: &[&str]) -> ProviderExtDecl {
        ProviderExtDecl {
            provider_id: None,
            provider_type: provider_type.to_string(),
            capabilities: Vec::new(),
            ops: ops.iter().map(|s| s.to_string()).collect(),
            config_schema_ref: None,
            state_schema_ref: None,
            runtime: ProviderExtRuntime {
                component_ref: format!("components/{provider_type}.wasm"),
                export: "greentic:provider/schema-core@1.0.0".to_string(),
                world: "greentic:provider/schema-core@1.0.0".to_string(),
            },
            docs_ref: None,
        }
    }

    fn registry(inline: Vec<ProviderExtDecl>) -> ProviderRegistry {
        let tenant = TenantCtx::new(
            EnvId::from_str("local").expect("env"),
            TenantId::from_str("demo").expect("tenant"),
        );
        ProviderRegistry {
            pack_ref: Some("test-pack@0.0.0".to_string()),
            inline,
            state_store: None,
            tenant,
        }
    }

    fn empty_manifest() -> PackManifest {
        PackManifest {
            agents: Default::default(),
            schema_version: "1.0".into(),
            pack_id: greentic_types::PackId::new("vendor.providers").expect("pack id"),
            name: None,
            version: semver::Version::new(0, 1, 0),
            kind: greentic_types::PackKind::Application,
            publisher: "test".into(),
            components: Vec::new(),
            flows: Vec::new(),
            dependencies: Vec::new(),
            capabilities: Vec::new(),
            secret_requirements: Vec::new(),
            signatures: Default::default(),
            bootstrap: None,
            extensions: None,
        }
    }

    #[test]
    fn invalid_env_is_rejected_not_silently_local() {
        let manifest = empty_manifest();
        assert!(ProviderRegistry::new(&manifest, None, "demo", "local").is_ok());
        let err = ProviderRegistry::new(&manifest, None, "demo", "not a valid env!")
            .err()
            .expect("invalid env must fail");
        assert!(err.to_string().contains("invalid env id"));
    }

    #[test]
    fn try_resolve_with_ops_returns_none_for_unknown_type() {
        let reg = registry(vec![ext_decl("messaging.telegram.bot", &["ingest_http"])]);
        let result = reg
            .try_resolve_with_ops(None, Some("messaging.teams.bot"))
            .expect("try_resolve_with_ops");
        assert!(
            result.is_none(),
            "unknown provider_type must surface as Ok(None) so fan-out callers can fail-closed at the host layer"
        );
    }

    #[test]
    fn try_resolve_with_ops_returns_declared_ops_for_matching_type() {
        let reg = registry(vec![ext_decl(
            "messaging.telegram.bot",
            &["ingest_http", "send_message"],
        )]);
        let (binding, ops) = reg
            .try_resolve_with_ops(None, Some("messaging.telegram.bot"))
            .expect("try_resolve_with_ops")
            .expect("match");
        assert_eq!(binding.provider_type, "messaging.telegram.bot");
        assert_eq!(
            ops,
            vec!["ingest_http".to_string(), "send_message".to_string()],
            "declared ops must round-trip in manifest order so the host's allowlist check matches the wire contract"
        );
    }

    #[test]
    fn try_resolve_with_ops_empty_ops_disables_allowlist_pass() {
        // A decl with no declared ops produces an empty allowlist; the
        // host's `declared_ops.contains(op)` check therefore fails closed
        // for every op. This guards the "decl with missing/empty ops"
        // edge case that would otherwise admit any op silently.
        let reg = registry(vec![ext_decl("messaging.telegram.bot", &[])]);
        let (_, ops) = reg
            .try_resolve_with_ops(None, Some("messaging.telegram.bot"))
            .expect("try_resolve_with_ops")
            .expect("match");
        assert!(
            ops.is_empty(),
            "empty declared ops must remain empty so the host-side allowlist rejects every op"
        );
    }
}
