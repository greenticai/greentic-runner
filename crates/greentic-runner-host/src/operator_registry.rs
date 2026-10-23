use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;

use greentic_types::provider::ProviderRuntimeRef;

use crate::pack::PackRuntime;

#[derive(Clone, Debug)]
pub struct OperatorBinding {
    pub provider_id: Option<String>,
    pub provider_type: String,
    pub op_id: String,
    pub runtime: ProviderRuntimeRef,
    pub pack_ref: String,
    pub pack_digest: Option<String>,
    pub config_schema_ref: Option<String>,
    pub state_schema_ref: Option<String>,
    pub docs_ref: Option<String>,
    pub capabilities: Vec<String>,
    pub pack_priority: usize,
}

#[derive(Debug)]
pub enum OperatorResolveError {
    ProviderNotFound,
    OpNotFound,
}

pub struct OperatorRegistry {
    per_provider_id: HashMap<String, HashMap<String, OperatorBinding>>,
    per_provider_type: HashMap<String, HashMap<String, OperatorBinding>>,
}

/// Insert `binding` for `op_id`, resolving duplicates deterministically.
///
/// Two packs (or two providers of one pack) can declare the same
/// `(provider, op)`. The binding from the pack with the higher `pack_priority`
/// (the later pack in load order) wins; on a tie the later declaration wins.
/// The conflict is logged so an operator can see which binding was shadowed.
fn insert_binding(
    ops: &mut HashMap<String, OperatorBinding>,
    op_id: &str,
    binding: OperatorBinding,
) {
    match ops.get(op_id) {
        Some(existing) if existing.pack_priority > binding.pack_priority => {
            tracing::warn!(
                op = %op_id,
                provider_type = %binding.provider_type,
                kept_pack = %existing.pack_ref,
                ignored_pack = %binding.pack_ref,
                "duplicate operator binding: keeping the higher-priority pack"
            );
        }
        Some(existing) => {
            tracing::warn!(
                op = %op_id,
                provider_type = %binding.provider_type,
                replaced_pack = %existing.pack_ref,
                winning_pack = %binding.pack_ref,
                "duplicate operator binding: later declaration replaces the earlier one"
            );
            ops.insert(op_id.to_string(), binding);
        }
        None => {
            ops.insert(op_id.to_string(), binding);
        }
    }
}

impl OperatorRegistry {
    pub fn build(packs: &[(Arc<PackRuntime>, Option<String>)]) -> Result<OperatorRegistry> {
        let mut per_provider_id: HashMap<String, HashMap<String, OperatorBinding>> = HashMap::new();
        let mut per_provider_type: HashMap<String, HashMap<String, OperatorBinding>> =
            HashMap::new();

        for (pack_priority, (pack, digest)) in packs.iter().enumerate() {
            let pack_meta = pack.metadata();
            let computed_ref = format!("{}@{}", pack_meta.pack_id, pack_meta.version);
            let registry = match pack.provider_registry_optional() {
                Ok(Some(registry)) => registry,
                Ok(None) => continue,
                Err(err) => {
                    return Err(err.context(format!(
                        "failed to build provider registry for pack {}",
                        pack_meta.pack_id
                    )));
                }
            };
            for provider in registry.operator_metadata() {
                let pack_ref = provider
                    .pack_ref
                    .clone()
                    .unwrap_or_else(|| computed_ref.clone());
                for op_id in &provider.ops {
                    let binding = OperatorBinding {
                        provider_id: provider.provider_id.clone(),
                        provider_type: provider.provider_type.clone(),
                        op_id: op_id.clone(),
                        runtime: provider.runtime.clone(),
                        pack_ref: pack_ref.clone(),
                        pack_digest: digest.clone(),
                        config_schema_ref: provider.config_schema_ref.clone(),
                        state_schema_ref: provider.state_schema_ref.clone(),
                        docs_ref: provider.docs_ref.clone(),
                        capabilities: provider.capabilities.clone(),
                        pack_priority,
                    };
                    if let Some(provider_id) = binding.provider_id.clone() {
                        insert_binding(
                            per_provider_id.entry(provider_id).or_default(),
                            op_id,
                            binding.clone(),
                        );
                    }
                    insert_binding(
                        per_provider_type
                            .entry(binding.provider_type.clone())
                            .or_default(),
                        op_id,
                        binding,
                    );
                }
            }
        }

        Ok(OperatorRegistry {
            per_provider_id,
            per_provider_type,
        })
    }

    pub fn resolve(
        &self,
        provider_id: Option<&str>,
        provider_type: Option<&str>,
        op_id: &str,
    ) -> Result<&OperatorBinding, OperatorResolveError> {
        // Strict identity semantics: a caller supplying `provider_id` opts
        // into id-keyed lookup. If the id is unknown to this registry, fail
        // loudly rather than silently rebinding to the type index (which
        // would route a stale/typo'd instance id to a generic type-level
        // binding with different config). State-store-backed instances —
        // which never appear in `per_provider_id` because the registry is
        // built from inline pack decls only — are resolved by the
        // invoke_operator probe (operator.rs), which retries with
        // (None, Some(derived_type)) once it learns the type from the
        // instance file.
        if let Some(id) = provider_id {
            let ops = self
                .per_provider_id
                .get(id)
                .ok_or(OperatorResolveError::ProviderNotFound)?;
            return ops.get(op_id).ok_or(OperatorResolveError::OpNotFound);
        }
        if let Some(ty) = provider_type {
            let ops = self
                .per_provider_type
                .get(ty)
                .ok_or(OperatorResolveError::ProviderNotFound)?;
            return ops.get(op_id).ok_or(OperatorResolveError::OpNotFound);
        }
        Err(OperatorResolveError::ProviderNotFound)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding(pack_ref: &str, pack_priority: usize) -> OperatorBinding {
        OperatorBinding {
            provider_id: None,
            provider_type: "t".to_string(),
            op_id: "send".to_string(),
            runtime: ProviderRuntimeRef {
                component_ref: "c".to_string(),
                export: "e".to_string(),
                world: "w".to_string(),
            },
            pack_ref: pack_ref.to_string(),
            pack_digest: None,
            config_schema_ref: None,
            state_schema_ref: None,
            docs_ref: None,
            capabilities: Vec::new(),
            pack_priority,
        }
    }

    #[test]
    fn higher_priority_pack_wins_regardless_of_insertion_order() {
        let mut ops = HashMap::new();
        insert_binding(&mut ops, "send", binding("high@1", 2));
        insert_binding(&mut ops, "send", binding("low@1", 1));
        assert_eq!(ops["send"].pack_ref, "high@1");

        let mut ops = HashMap::new();
        insert_binding(&mut ops, "send", binding("low@1", 1));
        insert_binding(&mut ops, "send", binding("high@1", 2));
        assert_eq!(ops["send"].pack_ref, "high@1");
    }

    #[test]
    fn equal_priority_keeps_later_declaration() {
        let mut ops = HashMap::new();
        insert_binding(&mut ops, "send", binding("first@1", 0));
        insert_binding(&mut ops, "send", binding("second@1", 0));
        assert_eq!(ops["send"].pack_ref, "second@1");
    }
}
