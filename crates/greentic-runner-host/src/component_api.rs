use anyhow::Result;

pub mod v0_4 {
    wasmtime::component::bindgen!({
        inline: r#"
        package greentic:component@0.4.0;

        interface control {
          should-cancel: func() -> bool;
          yield-now: func();
        }

        interface node {
          type json = string;

          record tenant-ctx {
            tenant: string,
            team: option<string>,
            user: option<string>,
            trace-id: option<string>,
            correlation-id: option<string>,
            deadline-unix-ms: option<u64>,
            attempt: u32,
            idempotency-key: option<string>,
          }

          record exec-ctx {
            tenant: tenant-ctx,
            flow-id: string,
            node-id: option<string>,
          }

          record node-error {
            code: string,
            message: string,
            retryable: bool,
            backoff-ms: option<u64>,
            details: option<json>,
          }

          variant invoke-result {
            ok(json),
            err(node-error),
          }

          variant stream-event {
            data(json),
            progress(u8),
            done,
            error(string),
          }

          enum lifecycle-status { ok }

          get-manifest: func() -> json;
          on-start: func(ctx: exec-ctx) -> result<lifecycle-status, string>;
          on-stop: func(ctx: exec-ctx, reason: string) -> result<lifecycle-status, string>;
          invoke: func(ctx: exec-ctx, op: string, input: json) -> invoke-result;
          invoke-stream: func(ctx: exec-ctx, op: string, input: json) -> list<stream-event>;
        }

        world component {
          import control;
          export node;
        }
        "#,
        world: "component",
    });
}

pub mod v0_5 {
    wasmtime::component::bindgen!({
        inline: r#"
        package greentic:component@0.5.0;

        interface control {
          should-cancel: func() -> bool;
          yield-now: func();
        }

        interface node {
          type json = string;

          record impersonation {
            actor-id: string,
            reason: option<string>,
          }

          record tenant-ctx {
            env: string,
            tenant: string,
            tenant-id: string,
            team: option<string>,
            team-id: option<string>,
            user: option<string>,
            user-id: option<string>,
            trace-id: option<string>,
            i18n-id: option<string>,
            correlation-id: option<string>,
            attributes: list<tuple<string, string>>,
            session-id: option<string>,
            flow-id: option<string>,
            node-id: option<string>,
            provider-id: option<string>,
            deadline-ms: option<s64>,
            attempt: u32,
            idempotency-key: option<string>,
            impersonation: option<impersonation>,
          }

          record exec-ctx {
            tenant: tenant-ctx,
            i18n-id: option<string>,
            flow-id: string,
            node-id: option<string>,
          }

          record node-error {
            code: string,
            message: string,
            retryable: bool,
            backoff-ms: option<u64>,
            details: option<json>,
          }

          variant invoke-result {
            ok(json),
            err(node-error),
          }

          variant stream-event {
            data(json),
            progress(u8),
            done,
            error(string),
          }

          enum lifecycle-status { ok }

          get-manifest: func() -> json;
          on-start: func(ctx: exec-ctx) -> result<lifecycle-status, string>;
          on-stop: func(ctx: exec-ctx, reason: string) -> result<lifecycle-status, string>;
          invoke: func(ctx: exec-ctx, op: string, input: json) -> invoke-result;
          invoke-stream: func(ctx: exec-ctx, op: string, input: json) -> list<stream-event>;
        }

        world component {
          import control;
          export node;
        }
        "#,
        world: "component",
    });
}

pub mod v0_6_descriptor {
    wasmtime::component::bindgen!({
        inline: r#"
        package greentic:component@0.6.0;

        interface component-descriptor {
          describe: func() -> list<u8>;
        }

        world component-v0-v6-v0 {
          export component-descriptor;
        }
        "#,
        world: "component-v0-v6-v0",
    });
}

// Component ABI bridge for runner-host: adds greentic:component/node@0.6 invoke envelope/result adapters and
// keeps backward compatibility with v0.5/v0.4.
pub mod v0_6 {
    wasmtime::component::bindgen!({
        inline: r#"
        package greentic:component@0.6.0;

        interface node {
          type capability-id = string;
          type component-id = string;
          type flow-id = string;
          type step-id = string;
          type tenant-id = string;
          type team-id = string;
          type user-id = string;
          type env-id = string;
          type trace-id = string;
          type correlation-id = string;

          record node-error {
            code: string,
            message: string,
            retryable: bool,
            backoff-ms: option<u64>,
            details: option<list<u8>>,
          }

          record tenant-ctx {
            tenant-id: tenant-id,
            team-id: option<team-id>,
            user-id: option<user-id>,
            env-id: env-id,
            trace-id: trace-id,
            correlation-id: correlation-id,
            deadline-ms: u64,
            attempt: u32,
            idempotency-key: option<string>,
            i18n-id: string,
          }

          record invocation-envelope {
            ctx: tenant-ctx,
            flow-id: flow-id,
            step-id: step-id,
            component-id: component-id,
            attempt: u32,
            payload-cbor: list<u8>,
            metadata-cbor: option<list<u8>>,
          }

          record invocation-result {
            ok: bool,
            output-cbor: list<u8>,
            output-metadata-cbor: option<list<u8>>,
          }

          invoke: func(op: string, envelope: invocation-envelope) -> result<invocation-result, node-error>;
        }

        world component {
          export node;
        }
        "#,
        world: "component",
    });
}

pub mod v0_6_runtime {
    wasmtime::component::bindgen!({
        inline: r#"
        package greentic:component@0.6.0;

        interface component-runtime {
          record run-result {
            output: list<u8>,
            new-state: list<u8>,
          }
          run: func(input: list<u8>, state: list<u8>) -> run-result;
        }

        world component-v0-v6-runtime {
          export component-runtime;
        }
        "#,
        world: "component-v0-v6-runtime",
    });
}

pub mod node {
    pub type Json = String;

    #[derive(Clone, Debug)]
    pub struct TenantCtx {
        pub tenant: String,
        pub team: Option<String>,
        pub user: Option<String>,
        pub trace_id: Option<String>,
        pub i18n_id: Option<String>,
        pub correlation_id: Option<String>,
        pub deadline_unix_ms: Option<u64>,
        pub attempt: u32,
        pub idempotency_key: Option<String>,
    }

    #[derive(Clone, Debug)]
    pub struct ExecCtx {
        pub tenant: TenantCtx,
        pub i18n_id: Option<String>,
        pub flow_id: String,
        pub node_id: Option<String>,
    }

    #[derive(Clone, Debug)]
    pub struct NodeError {
        pub code: String,
        pub message: String,
        pub retryable: bool,
        pub backoff_ms: Option<u64>,
        pub details: Option<Json>,
    }

    #[derive(Clone, Debug)]
    pub enum InvokeResult {
        Ok(Json),
        Err(NodeError),
    }
}

// The provider-verified caller, as the component-world conversions present it.
//
// Deliberately NOT a field of `node::ExecCtx`: downstream crates
// (greentic-start, greentic-setup) build that struct with literal
// initializers, so a new public field is a breaking change. The caller
// travels beside it as an argument instead (the `*_as` conversions below),
// and `ExecCtx.tenant` stays the HOST's scope for the invocation's secrets
// and state.

/// Environment variable that restores the pre-2026-09 presentation, in which a
/// component invoked with no verified caller was told the PROVIDER id as its
/// `user` (see [`presented_user`]).
///
/// Default: unset, i.e. the provider is NOT presented as the user. A truthy
/// value (`1`, `true`, `yes`, `on`, case-insensitive) opts back into the
/// legacy value for a component that keyed behaviour on it. It changes only
/// what a component is TOLD; the host scope that keys its state and secrets
/// is the same either way.
pub const PRESENT_PROVIDER_AS_USER_ENV: &str = "GREENTIC_PRESENT_PROVIDER_AS_USER";

/// Parse [`PRESENT_PROVIDER_AS_USER_ENV`]. Pure so it can be tested without
/// mutating the process environment.
fn legacy_provider_as_user_from(raw: Option<&str>) -> bool {
    raw.map(|value| value.trim().to_ascii_lowercase())
        .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on"))
}

fn legacy_provider_as_user() -> bool {
    legacy_provider_as_user_from(std::env::var(PRESENT_PROVIDER_AS_USER_ENV).ok().as_deref())
}

/// The `user` a component is told.
///
/// * A verified caller with a subject: that subject.
/// * Otherwise, when the host scope's user IS the provider that delivered the
///   invocation (`provider_id`) — which is what the flow engine's
///   `component_exec_ctx` stores there, so that component state keyed on it is
///   never re-keyed — nobody: `None`. An unverified or anonymous caller of a
///   deployed environment must not look like a user named after the provider,
///   and in particular must not look like Run Demo (`greentic-run-demo`).
///   The provider is presented in its own slot instead (0.5 `provider_id`).
///   This matches the JSON `InvocationEnvelope` path, which has always left
///   `user` empty for an unverified caller.
/// * Otherwise, the host scope's user, unchanged (e.g. an embedder that set a
///   real user on the `ExecCtx`).
///
/// [`PRESENT_PROVIDER_AS_USER_ENV`] restores the legacy fallback to the
/// provider id.
fn presented_user(
    ctx: &node::ExecCtx,
    caller: Option<&crate::caller_identity::ComponentCaller>,
    provider_id: Option<&str>,
) -> Option<String> {
    presented_user_with(ctx, caller, provider_id, legacy_provider_as_user())
}

fn presented_user_with(
    ctx: &node::ExecCtx,
    caller: Option<&crate::caller_identity::ComponentCaller>,
    provider_id: Option<&str>,
    legacy_provider_as_user: bool,
) -> Option<String> {
    if let Some(sub) = caller.and_then(|caller| caller.sub.clone()) {
        return Some(sub);
    }
    let host_user = ctx.tenant.user.as_deref()?;
    let host_user_is_the_provider = provider_id == Some(host_user);
    if host_user_is_the_provider && !legacy_provider_as_user {
        return None;
    }
    Some(host_user.to_string())
}

/// The `team` a component is told: the verified team when there is one,
/// otherwise the host scope's team.
///
/// Unlike [`presented_user`] there is nothing to strip here: the flow engine's
/// host scope never carries the provider in `team` (`component_exec_ctx` sets
/// it to `None` so tenant-wide secrets resolve), so an unverified caller
/// already reaches a component with no team.
fn presented_team(
    ctx: &node::ExecCtx,
    caller: Option<&crate::caller_identity::ComponentCaller>,
) -> Option<String> {
    caller
        .and_then(|caller| caller.team.clone())
        .or_else(|| ctx.tenant.team.clone())
}

pub fn exec_ctx_v0_4(ctx: &node::ExecCtx) -> v0_4::exports::greentic::component::node::ExecCtx {
    exec_ctx_v0_4_as(ctx, None)
}

/// [`exec_ctx_v0_4`], presenting `caller` as the component's user and team.
pub fn exec_ctx_v0_4_as(
    ctx: &node::ExecCtx,
    caller: Option<&crate::caller_identity::ComponentCaller>,
) -> v0_4::exports::greentic::component::node::ExecCtx {
    exec_ctx_v0_4_for(ctx, caller, None)
}

/// [`exec_ctx_v0_4_as`] for an invocation delivered by `provider_id`.
///
/// The 0.4 world has no provider slot, so the provider is not presented at
/// all; `provider_id` only decides whether the host scope's user is the
/// provider and therefore must not be presented as the user
/// ([`presented_user`]).
pub fn exec_ctx_v0_4_for(
    ctx: &node::ExecCtx,
    caller: Option<&crate::caller_identity::ComponentCaller>,
    provider_id: Option<&str>,
) -> v0_4::exports::greentic::component::node::ExecCtx {
    v0_4::exports::greentic::component::node::ExecCtx {
        tenant: v0_4::exports::greentic::component::node::TenantCtx {
            tenant: ctx.tenant.tenant.clone(),
            team: presented_team(ctx, caller),
            user: presented_user(ctx, caller, provider_id),
            trace_id: ctx.tenant.trace_id.clone(),
            correlation_id: ctx.tenant.correlation_id.clone(),
            deadline_unix_ms: ctx.tenant.deadline_unix_ms,
            attempt: ctx.tenant.attempt,
            idempotency_key: ctx.tenant.idempotency_key.clone(),
        },
        flow_id: ctx.flow_id.clone(),
        node_id: ctx.node_id.clone(),
    }
}

pub fn exec_ctx_v0_5(ctx: &node::ExecCtx) -> v0_5::exports::greentic::component::node::ExecCtx {
    exec_ctx_v0_5_as(ctx, None)
}

/// [`exec_ctx_v0_5`], presenting `caller` as the component's user, team and
/// `caller.*` attributes.
pub fn exec_ctx_v0_5_as(
    ctx: &node::ExecCtx,
    caller: Option<&crate::caller_identity::ComponentCaller>,
) -> v0_5::exports::greentic::component::node::ExecCtx {
    exec_ctx_v0_5_for(ctx, caller, None)
}

/// [`exec_ctx_v0_5_as`] for an invocation delivered by `provider_id`, which
/// is presented in the world's `provider_id` slot — the place a component
/// reads which provider (or `greentic-run-demo`, for the designer's Run Demo)
/// delivered the turn, instead of inferring it from `user_id`.
pub fn exec_ctx_v0_5_for(
    ctx: &node::ExecCtx,
    caller: Option<&crate::caller_identity::ComponentCaller>,
    provider_id: Option<&str>,
) -> v0_5::exports::greentic::component::node::ExecCtx {
    let env = std::env::var("GREENTIC_ENV").unwrap_or_else(|_| "local".to_string());
    let team_id = presented_team(ctx, caller);
    let user_id = presented_user(ctx, caller, provider_id);
    let deadline_ms = ctx
        .tenant
        .deadline_unix_ms
        .and_then(|value| i64::try_from(value).ok());
    v0_5::exports::greentic::component::node::ExecCtx {
        tenant: v0_5::exports::greentic::component::node::TenantCtx {
            env,
            tenant: ctx.tenant.tenant.clone(),
            tenant_id: ctx.tenant.tenant.clone(),
            team: team_id.clone(),
            team_id,
            user: user_id.clone(),
            user_id,
            trace_id: ctx.tenant.trace_id.clone(),
            i18n_id: ctx.tenant.i18n_id.clone(),
            correlation_id: ctx.tenant.correlation_id.clone(),
            attributes: caller
                .map(crate::caller_identity::ComponentCaller::attributes)
                .unwrap_or_default(),
            session_id: ctx.tenant.correlation_id.clone(),
            flow_id: Some(ctx.flow_id.clone()),
            node_id: ctx.node_id.clone(),
            provider_id: provider_id.map(str::to_string),
            deadline_ms,
            attempt: ctx.tenant.attempt,
            idempotency_key: ctx.tenant.idempotency_key.clone(),
            impersonation: None,
        },
        i18n_id: ctx.i18n_id.clone(),
        flow_id: ctx.flow_id.clone(),
        node_id: ctx.node_id.clone(),
    }
}

pub fn envelope_v0_6(
    ctx: &node::ExecCtx,
    component_id: &str,
    payload_json: &str,
) -> Result<v0_6::exports::greentic::component::node::InvocationEnvelope> {
    envelope_v0_6_as(ctx, None, component_id, payload_json)
}

/// [`envelope_v0_6`], presenting `caller` as the component's user and team.
pub fn envelope_v0_6_as(
    ctx: &node::ExecCtx,
    caller: Option<&crate::caller_identity::ComponentCaller>,
    component_id: &str,
    payload_json: &str,
) -> Result<v0_6::exports::greentic::component::node::InvocationEnvelope> {
    envelope_v0_6_for(ctx, caller, None, component_id, payload_json)
}

/// [`envelope_v0_6_as`] for an invocation delivered by `provider_id`.
///
/// The 0.6 `TenantCtx` has no provider slot, so the provider is not presented;
/// `provider_id` only decides whether the host scope's user is the provider
/// and therefore must not be presented as the user ([`presented_user`]).
pub fn envelope_v0_6_for(
    ctx: &node::ExecCtx,
    caller: Option<&crate::caller_identity::ComponentCaller>,
    provider_id: Option<&str>,
    component_id: &str,
    payload_json: &str,
) -> Result<v0_6::exports::greentic::component::node::InvocationEnvelope> {
    let env = std::env::var("GREENTIC_ENV").unwrap_or_else(|_| "local".to_string());
    let step_id = ctx
        .node_id
        .clone()
        .unwrap_or_else(|| "component.exec".to_string());
    let i18n_id = ctx
        .i18n_id
        .clone()
        .or_else(|| ctx.tenant.i18n_id.clone())
        .unwrap_or_default();
    let trace_id = ctx.tenant.trace_id.clone().unwrap_or_default();
    let correlation_id = ctx.tenant.correlation_id.clone().unwrap_or_default();
    let deadline_ms = ctx.tenant.deadline_unix_ms.unwrap_or(u64::MAX);
    let payload_cbor = if let Ok(invocation) =
        serde_json::from_str::<greentic_types::InvocationEnvelope>(payload_json)
    {
        match serde_json::from_slice::<serde_json::Value>(&invocation.payload) {
            Ok(value) => serde_cbor::to_vec(&value)?,
            Err(_) => serde_cbor::to_vec(&serde_cbor::Value::Bytes(invocation.payload))?,
        }
    } else {
        let payload_value = serde_json::from_str::<serde_json::Value>(payload_json)
            .unwrap_or_else(|_| serde_json::Value::String(payload_json.to_string()));
        serde_cbor::to_vec(&payload_value)?
    };

    Ok(
        v0_6::exports::greentic::component::node::InvocationEnvelope {
            ctx: v0_6::exports::greentic::component::node::TenantCtx {
                tenant_id: ctx.tenant.tenant.clone(),
                team_id: presented_team(ctx, caller),
                user_id: presented_user(ctx, caller, provider_id),
                env_id: env,
                trace_id,
                correlation_id,
                deadline_ms,
                attempt: ctx.tenant.attempt,
                idempotency_key: ctx.tenant.idempotency_key.clone(),
                i18n_id,
            },
            flow_id: ctx.flow_id.clone(),
            step_id,
            component_id: component_id.to_string(),
            attempt: ctx.tenant.attempt,
            payload_cbor,
            // The 0.6 world has no typed slot for `role`, `groups` or the
            // verification flag — see `ComponentCaller::metadata_cbor`. This
            // is the carrier that already exists in the published WIT, so a
            // 0.6 component can read `caller.*` without a world bump. `None`
            // whenever there is no caller, which keeps every existing
            // invocation byte-identical.
            metadata_cbor: caller.and_then(crate::caller_identity::ComponentCaller::metadata_cbor),
        },
    )
}

pub fn invoke_result_from_v0_4(
    result: v0_4::exports::greentic::component::node::InvokeResult,
) -> node::InvokeResult {
    match result {
        v0_4::exports::greentic::component::node::InvokeResult::Ok(body) => {
            node::InvokeResult::Ok(body)
        }
        v0_4::exports::greentic::component::node::InvokeResult::Err(err) => {
            node::InvokeResult::Err(node::NodeError {
                code: err.code,
                message: err.message,
                retryable: err.retryable,
                backoff_ms: err.backoff_ms,
                details: err.details,
            })
        }
    }
}

pub fn invoke_result_from_v0_6(
    result: std::result::Result<
        v0_6::exports::greentic::component::node::InvocationResult,
        v0_6::exports::greentic::component::node::NodeError,
    >,
) -> Result<node::InvokeResult> {
    match result {
        Ok(ok) => {
            let body = cbor_to_json_string(&ok.output_cbor);
            if ok.ok {
                Ok(node::InvokeResult::Ok(body))
            } else {
                Ok(node::InvokeResult::Err(node::NodeError {
                    code: "COMPONENT_INVOCATION_FAILED".to_string(),
                    message: body,
                    retryable: false,
                    backoff_ms: None,
                    details: None,
                }))
            }
        }
        Err(err) => Ok(node::InvokeResult::Err(node::NodeError {
            code: err.code,
            message: err.message,
            retryable: err.retryable,
            backoff_ms: err.backoff_ms,
            details: err.details.map(|bytes| cbor_to_json_string(bytes.as_ref())),
        })),
    }
}

pub fn invoke_result_from_v0_5(
    result: v0_5::exports::greentic::component::node::InvokeResult,
) -> node::InvokeResult {
    match result {
        v0_5::exports::greentic::component::node::InvokeResult::Ok(body) => {
            node::InvokeResult::Ok(body)
        }
        v0_5::exports::greentic::component::node::InvokeResult::Err(err) => {
            node::InvokeResult::Err(node::NodeError {
                code: err.code,
                message: err.message,
                retryable: err.retryable,
                backoff_ms: err.backoff_ms,
                details: err.details,
            })
        }
    }
}

fn cbor_to_json_string(bytes: &[u8]) -> String {
    match serde_cbor::from_slice::<serde_json::Value>(bytes)
        .ok()
        .and_then(|value| serde_json::to_string(&value).ok())
    {
        Some(json) => json,
        None => String::from_utf8_lossy(bytes).to_string(),
    }
}

/// Convert v0.6 `component-runtime::run()` output to the canonical InvokeResult.
/// Decodes CBOR output bytes to a JSON string.
pub fn invoke_result_from_v0_6_run(
    result: v0_6_runtime::exports::greentic::component::component_runtime::RunResult,
) -> node::InvokeResult {
    let json_str = match serde_cbor::from_slice::<serde_json::Value>(&result.output) {
        Ok(value) => serde_json::to_string(&value).unwrap_or_default(),
        Err(_) => {
            // Fallback: try as UTF-8 string
            String::from_utf8(result.output).unwrap_or_default()
        }
    };
    node::InvokeResult::Ok(json_str)
}

#[cfg(test)]
mod tests {
    use super::{
        PRESENT_PROVIDER_AS_USER_ENV, cbor_to_json_string, envelope_v0_6, envelope_v0_6_as,
        envelope_v0_6_for, exec_ctx_v0_4, exec_ctx_v0_4_as, exec_ctx_v0_4_for, exec_ctx_v0_5,
        exec_ctx_v0_5_as, exec_ctx_v0_5_for, invoke_result_from_v0_4, invoke_result_from_v0_5,
        invoke_result_from_v0_6, invoke_result_from_v0_6_run, legacy_provider_as_user_from, node,
        presented_user_with,
    };
    use greentic_types::{EnvId, InvocationEnvelope, TenantCtx, TenantId};
    use std::str::FromStr;

    fn sample_exec_ctx() -> node::ExecCtx {
        node::ExecCtx {
            tenant: node::TenantCtx {
                tenant: "tenant.demo".to_string(),
                team: Some("team.demo".to_string()),
                user: Some("user.demo".to_string()),
                trace_id: Some("trace.demo".to_string()),
                i18n_id: Some("en-US".to_string()),
                correlation_id: Some("corr.demo".to_string()),
                deadline_unix_ms: Some(123),
                attempt: 2,
                idempotency_key: Some("idem.demo".to_string()),
            },
            i18n_id: Some("en-US".to_string()),
            flow_id: "flow.demo".to_string(),
            node_id: Some("node.demo".to_string()),
        }
    }

    /// A verified caller is what the component is told, in every world, while
    /// the host scope (`ExecCtx.tenant`) that keys secrets and state is left
    /// exactly as it was.
    #[test]
    fn a_verified_caller_is_presented_in_every_world() {
        let ctx = sample_exec_ctx();
        let verified = crate::caller_identity::ComponentCaller {
            sub: Some("u-1@acme".to_string()),
            team: Some("sales".to_string()),
            groups: vec!["employee".to_string()],
            role: None,
        };
        let caller = Some(&verified);

        let v04 = exec_ctx_v0_4_as(&ctx, caller);
        assert_eq!(v04.tenant.user.as_deref(), Some("u-1@acme"));
        assert_eq!(v04.tenant.team.as_deref(), Some("sales"));

        let v05 = exec_ctx_v0_5_as(&ctx, caller);
        assert_eq!(v05.tenant.user.as_deref(), Some("u-1@acme"));
        assert_eq!(v05.tenant.user_id.as_deref(), Some("u-1@acme"));
        assert_eq!(v05.tenant.team_id.as_deref(), Some("sales"));
        assert!(
            v05.tenant
                .attributes
                .contains(&("caller.groups".to_string(), r#"["employee"]"#.to_string()))
        );

        let envelope = envelope_v0_6_as(&ctx, caller, "component.demo", "{}").expect("envelope");
        assert_eq!(envelope.ctx.user_id.as_deref(), Some("u-1@acme"));
        assert_eq!(envelope.ctx.team_id.as_deref(), Some("sales"));

        assert_eq!(ctx.tenant.user.as_deref(), Some("user.demo"));
        assert_eq!(ctx.tenant.team.as_deref(), Some("team.demo"));
    }

    /// The 0.6 world has no typed slot for `role`, `groups` or the
    /// verification flag, so they ride in `metadata_cbor` — the field the
    /// published 0.6 WIT already carries and the host used to leave empty.
    ///
    /// This is what lets a 0.6 component read `caller.*` without a world bump,
    /// which is the alternative: eight repositories moving in order and ~40
    /// components rebuilt.
    #[test]
    fn a_v0_6_envelope_carries_the_caller_in_metadata() {
        let ctx = sample_exec_ctx();
        let verified = crate::caller_identity::ComponentCaller {
            sub: Some("u-1@acme".to_string()),
            team: Some("sales".to_string()),
            groups: vec!["employee".to_string(), "admins".to_string()],
            role: Some("approver".to_string()),
        };

        let envelope =
            envelope_v0_6_as(&ctx, Some(&verified), "component.demo", "{}").expect("envelope");
        let bytes = envelope.metadata_cbor.expect("a caller must reach 0.6");
        let map: std::collections::BTreeMap<String, String> =
            serde_cbor::from_slice(&bytes).expect("metadata decodes as a string map");

        assert_eq!(map.get("caller.sub").map(String::as_str), Some("u-1@acme"));
        assert_eq!(map.get("caller.team").map(String::as_str), Some("sales"));
        assert_eq!(map.get("caller.role").map(String::as_str), Some("approver"));
        assert_eq!(
            map.get("caller.groups").map(String::as_str),
            Some(r#"["employee","admins"]"#)
        );
        assert_eq!(
            map.get("caller.user_verified").map(String::as_str),
            Some("true")
        );
    }

    /// One vocabulary, two transports. A component reading `caller.*` off a
    /// 0.5 `attributes` list and one reading it out of 0.6 `metadata_cbor`
    /// must see the same keys and the same values — otherwise the two drift
    /// and the one that drifts is the one nobody is looking at.
    #[test]
    fn the_v0_5_attributes_and_the_v0_6_metadata_say_the_same_thing() {
        let ctx = sample_exec_ctx();
        let verified = crate::caller_identity::ComponentCaller {
            sub: Some("u-1@acme".to_string()),
            team: Some("sales".to_string()),
            groups: vec!["employee".to_string()],
            role: Some("approver".to_string()),
        };

        let v05: std::collections::BTreeMap<String, String> =
            exec_ctx_v0_5_as(&ctx, Some(&verified))
                .tenant
                .attributes
                .into_iter()
                .collect();
        let envelope =
            envelope_v0_6_as(&ctx, Some(&verified), "component.demo", "{}").expect("envelope");
        let v06: std::collections::BTreeMap<String, String> =
            serde_cbor::from_slice(&envelope.metadata_cbor.expect("metadata")).expect("decodes");

        assert_eq!(v05, v06);
    }

    /// An invocation with no caller keeps the exact envelope it had before
    /// this field was ever populated. Every deployed 0.6 component sees no
    /// change at all.
    #[test]
    fn an_uncallered_v0_6_envelope_carries_no_metadata() {
        let ctx = sample_exec_ctx();
        let envelope = envelope_v0_6_as(&ctx, None, "component.demo", "{}").expect("envelope");
        assert!(envelope.metadata_cbor.is_none());
    }

    #[test]
    fn envelope_v0_6_preserves_binary_payload_when_envelope_payload_is_not_json() {
        let envelope = InvocationEnvelope {
            ctx: TenantCtx::new(
                EnvId::from_str("local").expect("valid env id"),
                TenantId::from_str("tenant.default").expect("valid tenant id"),
            ),
            flow_id: "flow.demo".to_string(),
            node_id: Some("node.demo".to_string()),
            op: "on_message".to_string(),
            payload: vec![255, 0, 1, 42],
            metadata: Vec::new(),
        };
        let payload_json = serde_json::to_string(&envelope).expect("serialize invocation envelope");
        let envelope = envelope_v0_6(&sample_exec_ctx(), "component.demo", &payload_json)
            .expect("envelope conversion");
        let decoded: serde_cbor::Value =
            serde_cbor::from_slice(&envelope.payload_cbor).expect("decode cbor");
        assert_eq!(
            decoded,
            serde_cbor::Value::Bytes(vec![255, 0, 1, 42]),
            "binary payload must not be dropped when not valid json bytes"
        );
    }

    #[test]
    fn exec_ctx_conversions_preserve_identity_fields() {
        let ctx = sample_exec_ctx();

        let v04 = exec_ctx_v0_4(&ctx);
        assert_eq!(v04.tenant.tenant, "tenant.demo");
        assert_eq!(v04.flow_id, "flow.demo");

        let v05 = exec_ctx_v0_5(&ctx);
        assert_eq!(v05.tenant.tenant_id, "tenant.demo");
        assert_eq!(v05.tenant.team_id.as_deref(), Some("team.demo"));
        assert_eq!(v05.tenant.user_id.as_deref(), Some("user.demo"));
        assert_eq!(v05.tenant.deadline_ms, Some(123));
        assert_eq!(v05.tenant.flow_id.as_deref(), Some("flow.demo"));
        // No provider was named, so none is presented, and a host user that is
        // not a provider id is still presented as the user.
        assert_eq!(v05.tenant.provider_id, None);
    }

    /// The flow engine's host scope: `user` is the provider id (a state key),
    /// no team.
    fn provider_scoped_exec_ctx(provider: &str) -> node::ExecCtx {
        let mut ctx = sample_exec_ctx();
        ctx.tenant.user = Some(provider.to_string());
        ctx.tenant.team = None;
        ctx
    }

    /// Partner blocker #3, per world: with no verified caller, a host user that
    /// is the provider is not presented as the component's user in 0.4, 0.5
    /// or 0.6. 0.5 names the provider in its own slot.
    #[test]
    fn an_unverified_invocation_presents_no_user_in_every_world() {
        for provider in ["messaging-webchat", "greentic-run-demo", "provider"] {
            let ctx = provider_scoped_exec_ctx(provider);

            let v04 = exec_ctx_v0_4_for(&ctx, None, Some(provider));
            assert_eq!(v04.tenant.user, None, "0.4 user for {provider}");
            assert_eq!(v04.tenant.team, None);

            let v05 = exec_ctx_v0_5_for(&ctx, None, Some(provider));
            assert_eq!(v05.tenant.user, None, "0.5 user for {provider}");
            assert_eq!(v05.tenant.user_id, None, "0.5 user_id for {provider}");
            assert_eq!(v05.tenant.provider_id.as_deref(), Some(provider));

            let v06 = envelope_v0_6_for(&ctx, None, Some(provider), "c", "{}").expect("envelope");
            assert_eq!(v06.ctx.user_id, None, "0.6 user_id for {provider}");
            assert!(v06.metadata_cbor.is_none());

            // The host scope is untouched: it still keys state on the provider.
            assert_eq!(ctx.tenant.user.as_deref(), Some(provider));
        }
    }

    /// A verified caller that names a team but no subject presents no user
    /// either, rather than falling back to the provider.
    #[test]
    fn a_verified_caller_without_a_subject_presents_no_user() {
        let ctx = provider_scoped_exec_ctx("messaging-webchat");
        let verified = crate::caller_identity::ComponentCaller {
            sub: None,
            team: Some("sales".to_string()),
            groups: Vec::new(),
            role: None,
        };
        let v05 = exec_ctx_v0_5_for(&ctx, Some(&verified), Some("messaging-webchat"));
        assert_eq!(v05.tenant.user_id, None);
        assert_eq!(v05.tenant.team_id.as_deref(), Some("sales"));
    }

    /// Only the provider id is withheld. A host user that is something else —
    /// an embedder that set a real user on the `ExecCtx` — is still presented.
    #[test]
    fn a_host_user_that_is_not_the_provider_is_still_presented() {
        let ctx = sample_exec_ctx();
        let v05 = exec_ctx_v0_5_for(&ctx, None, Some("messaging-webchat"));
        assert_eq!(v05.tenant.user_id.as_deref(), Some("user.demo"));
        assert_eq!(v05.tenant.provider_id.as_deref(), Some("messaging-webchat"));
    }

    /// `GREENTIC_PRESENT_PROVIDER_AS_USER` restores the legacy value; a
    /// verified subject still wins over it.
    #[test]
    fn the_opt_out_restores_the_provider_as_the_user() {
        let ctx = provider_scoped_exec_ctx("messaging-webchat");
        assert_eq!(
            presented_user_with(&ctx, None, Some("messaging-webchat"), true).as_deref(),
            Some("messaging-webchat")
        );
        assert_eq!(
            presented_user_with(&ctx, None, Some("messaging-webchat"), false),
            None
        );
        let verified = crate::caller_identity::ComponentCaller {
            sub: Some("u-1@acme".to_string()),
            ..Default::default()
        };
        assert_eq!(
            presented_user_with(&ctx, Some(&verified), Some("messaging-webchat"), true).as_deref(),
            Some("u-1@acme")
        );
    }

    #[test]
    fn the_opt_out_flag_parses_truthy_values_only() {
        assert_eq!(
            PRESENT_PROVIDER_AS_USER_ENV,
            "GREENTIC_PRESENT_PROVIDER_AS_USER"
        );
        for on in ["1", "true", "TRUE", "yes", "On", " 1 "] {
            assert!(legacy_provider_as_user_from(Some(on)), "{on:?}");
        }
        for off in ["0", "false", "no", "off", "", "2"] {
            assert!(!legacy_provider_as_user_from(Some(off)), "{off:?}");
        }
        assert!(!legacy_provider_as_user_from(None));
    }

    #[test]
    fn invoke_result_adapters_preserve_error_details() {
        let v04 = invoke_result_from_v0_4(
            super::v0_4::exports::greentic::component::node::InvokeResult::Err(
                super::v0_4::exports::greentic::component::node::NodeError {
                    code: "E04".into(),
                    message: "bad request".into(),
                    retryable: true,
                    backoff_ms: Some(10),
                    details: Some("{\"detail\":true}".into()),
                },
            ),
        );
        match v04 {
            node::InvokeResult::Err(err) => {
                assert_eq!(err.code, "E04");
                assert!(err.retryable);
            }
            node::InvokeResult::Ok(_) => panic!("expected error"),
        }

        let v05 = invoke_result_from_v0_5(
            super::v0_5::exports::greentic::component::node::InvokeResult::Err(
                super::v0_5::exports::greentic::component::node::NodeError {
                    code: "E05".into(),
                    message: "still bad".into(),
                    retryable: false,
                    backoff_ms: None,
                    details: Some("{\"reason\":\"x\"}".into()),
                },
            ),
        );
        match v05 {
            node::InvokeResult::Err(err) => {
                assert_eq!(err.code, "E05");
                assert_eq!(err.details.as_deref(), Some("{\"reason\":\"x\"}"));
            }
            node::InvokeResult::Ok(_) => panic!("expected error"),
        }
    }

    #[test]
    fn invoke_result_v0_6_handles_component_failures_and_run_output() {
        let ok = invoke_result_from_v0_6(Ok(
            super::v0_6::exports::greentic::component::node::InvocationResult {
                ok: false,
                output_cbor: serde_cbor::to_vec(&serde_json::json!({"error":"boom"}))
                    .expect("cbor"),
                output_metadata_cbor: None,
            },
        ))
        .expect("adapt result");
        match ok {
            node::InvokeResult::Err(err) => {
                assert_eq!(err.code, "COMPONENT_INVOCATION_FAILED");
                assert!(err.message.contains("boom"));
            }
            node::InvokeResult::Ok(_) => panic!("expected component failure"),
        }

        let err = invoke_result_from_v0_6(Err(
            super::v0_6::exports::greentic::component::node::NodeError {
                code: "E06".into(),
                message: "bad".into(),
                retryable: true,
                backoff_ms: Some(25),
                details: Some(serde_cbor::to_vec(&serde_json::json!({"kind":"timeout"})).unwrap()),
            },
        ))
        .expect("adapt error");
        match err {
            node::InvokeResult::Err(err) => {
                assert_eq!(err.code, "E06");
                assert_eq!(err.backoff_ms, Some(25));
                assert!(err.details.expect("details").contains("timeout"));
            }
            node::InvokeResult::Ok(_) => panic!("expected node error"),
        }

        let run = invoke_result_from_v0_6_run(
            super::v0_6_runtime::exports::greentic::component::component_runtime::RunResult {
                output: serde_cbor::to_vec(&serde_json::json!({"ok":true})).expect("cbor"),
                new_state: Vec::new(),
            },
        );
        match run {
            node::InvokeResult::Ok(body) => assert!(body.contains("\"ok\":true")),
            node::InvokeResult::Err(_) => panic!("expected ok"),
        }
    }

    #[test]
    fn cbor_to_json_string_falls_back_to_utf8() {
        assert_eq!(cbor_to_json_string(b"plain-text"), "plain-text");
    }
}
