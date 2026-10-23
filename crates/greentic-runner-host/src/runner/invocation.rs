use anyhow::{Context, Result};
use greentic_types::{EnvId, InvocationEnvelope, TeamId, TenantCtx, TenantId, UserId};
use serde_json::Value;
use std::str::FromStr;

/// Runtime-owned invocation metadata builder.
///
/// Runtime owns `ctx` and flow/node metadata, so flows must not embed their own context fields.
#[derive(Clone, Copy)]
pub struct InvocationMeta<'a> {
    pub env: &'a str,
    pub tenant: &'a str,
    pub flow_id: &'a str,
    pub node_id: Option<&'a str>,
    pub provider_id: Option<&'a str>,
    pub session_id: Option<&'a str>,
    pub attempt: u32,
}

/// Present a verified caller on the envelope's `TenantCtx`.
///
/// The typed `user`/`team` slots only take identifier-shaped values, so a
/// subject such as `u-1@acme` is left out of them rather than mangled; the
/// attribute map always carries the caller verbatim, groups and role included.
fn stamp_caller(mut ctx: TenantCtx, caller: &crate::caller_identity::ComponentCaller) -> TenantCtx {
    if let Some(user) = caller
        .sub
        .as_deref()
        .and_then(|sub| UserId::from_str(sub).ok())
    {
        ctx = ctx.with_user(Some(user));
    }
    if let Some(team) = caller
        .team
        .as_deref()
        .and_then(|team| TeamId::from_str(team).ok())
    {
        ctx = ctx.with_team(Some(team));
    }
    ctx.attributes.extend(caller.attributes());
    ctx
}

pub fn build_invocation_envelope(
    meta: InvocationMeta<'_>,
    operation: &str,
    payload: Value,
) -> Result<InvocationEnvelope> {
    build_invocation_envelope_as(meta, None, operation, payload)
}

/// [`build_invocation_envelope`] on behalf of a provider-verified caller,
/// stamped onto the envelope `ctx`. Runtime owned like everything else here; a
/// flow cannot supply it. A separate argument rather than an `InvocationMeta`
/// field so that struct keeps the shape downstream crates construct.
pub fn build_invocation_envelope_as(
    meta: InvocationMeta<'_>,
    caller: Option<&crate::caller_identity::ComponentCaller>,
    operation: &str,
    payload: Value,
) -> Result<InvocationEnvelope> {
    let parsed = InvocationPayload::parse(payload);
    // An invalid id is refused, not mapped onto a shared default tenant/env:
    // that would run the invocation (and bill and audit it) as someone else.
    let env_id = EnvId::from_str(meta.env)
        .map_err(|err| anyhow::anyhow!("invalid env id `{}` in invocation: {err}", meta.env))?;
    let tenant_id = TenantId::from_str(meta.tenant).map_err(|err| {
        anyhow::anyhow!("invalid tenant id `{}` in invocation: {err}", meta.tenant)
    })?;
    let mut ctx = TenantCtx::new(env_id, tenant_id).with_flow(meta.flow_id.to_string());
    if let Some(provider) = meta.provider_id {
        ctx = ctx.with_provider(provider.to_string());
    }
    if let Some(session) = meta.session_id {
        ctx = ctx.with_session(session.to_string());
    }
    if let Some(node) = meta.node_id {
        ctx = ctx.with_node(node.to_string());
    }
    ctx = ctx.with_attempt(meta.attempt);
    if let Some(caller) = caller {
        ctx = stamp_caller(ctx, caller);
    }

    let payload_bytes =
        to_binary_payload(&parsed.payload).context("serialize payload for invocation envelope")?;
    let metadata_bytes = to_binary_payload(&parsed.metadata)
        .context("serialize metadata for invocation envelope")?;

    let op = parsed.op.unwrap_or_else(|| operation.to_string());
    tracing::trace!(
        flow_id = %meta.flow_id,
        node_id = ?meta.node_id,
        op = %op,
        payload = %parsed.payload,
        "built invocation envelope; runtime owns ctx; flows must not embed ctx"
    );

    Ok(InvocationEnvelope {
        ctx,
        flow_id: meta.flow_id.to_string(),
        node_id: meta.node_id.map(str::to_string),
        op,
        payload: payload_bytes,
        metadata: metadata_bytes,
    })
}

fn to_binary_payload(value: &Value) -> Result<Vec<u8>> {
    serde_json::to_vec(value).context("failed to encode payload for envelope")
}

struct InvocationPayload {
    op: Option<String>,
    payload: Value,
    metadata: Value,
}

impl InvocationPayload {
    fn parse(value: Value) -> Self {
        if let Value::Object(mut map) = value {
            if let Some(envelope) = map.remove("envelope") {
                return Self::from_envelope(envelope);
            }
            // Accept both canonical keys (`op`, `payload`, `metadata`) and
            // compatibility aliases used by older flow payloads (`operation`,
            // `input`, `config`) without dropping sibling fields such as `tool`.
            let op = extract_string(map.remove("op").or_else(|| map.remove("operation")));
            let payload = map
                .remove("payload")
                .or_else(|| map.remove("input"))
                .unwrap_or(Value::Null);
            let metadata = map
                .remove("metadata")
                .or_else(|| map.remove("config"))
                .unwrap_or(Value::Null);
            if op.is_some() || !payload.is_null() || !metadata.is_null() {
                if payload.is_null() && !map.is_empty() {
                    return Self {
                        op,
                        payload: Value::Object(map),
                        metadata,
                    };
                }
                return Self {
                    op,
                    payload,
                    metadata,
                };
            }
            return Self {
                op: None,
                payload: Value::Object(map),
                metadata: Value::Null,
            };
        }
        InvocationPayload {
            op: None,
            payload: value,
            metadata: Value::Null,
        }
    }

    fn from_envelope(value: Value) -> Self {
        if let Value::Object(map) = value {
            let op = extract_string(map.get("op").cloned());
            let payload = map.get("payload").cloned().unwrap_or(Value::Null);
            let metadata = map.get("metadata").cloned().unwrap_or(Value::Null);
            return Self {
                op,
                payload,
                metadata,
            };
        }
        InvocationPayload {
            op: None,
            payload: value,
            metadata: Value::Null,
        }
    }
}

fn extract_string(value: Option<Value>) -> Option<String> {
    value.and_then(|v| v.as_str().map(|s| s.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn meta<'a>(env: &'a str, tenant: &'a str) -> InvocationMeta<'a> {
        InvocationMeta {
            env,
            tenant,
            flow_id: "flow.main",
            node_id: None,
            provider_id: None,
            session_id: None,
            attempt: 1,
        }
    }

    #[test]
    fn invalid_tenant_or_env_is_refused_not_defaulted() {
        let bad_tenant =
            build_invocation_envelope(meta("local", "Not A Tenant!"), "run", json!({}));
        assert!(bad_tenant.is_err());
        let bad_env = build_invocation_envelope(meta("bad env", "acme"), "run", json!({}));
        assert!(bad_env.is_err());
    }

    #[test]
    fn valid_ids_build_an_envelope_under_the_real_tenant() {
        let envelope =
            build_invocation_envelope(meta("local", "acme"), "run", json!({})).expect("valid ids");
        assert_eq!(envelope.ctx.tenant_id.as_str(), "acme");
    }
}
