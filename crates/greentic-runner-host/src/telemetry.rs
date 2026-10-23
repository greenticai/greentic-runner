use greentic_types::telemetry::set_current_tenant_ctx;
use greentic_types::{EnvId, TenantCtx, TenantId};

/// Canonical `TenantCtx.attributes` keys for deploy-spec rollout identifiers
/// (B11/C5) and the messaging endpoint discriminator (M1.4).
///
/// Previously exported by `greentic_types::telemetry::attr_keys`; the upstream
/// module was removed in greentic-types 1.1.0-dev.27836473437 and the new
/// `set_current_tenant_ctx` no longer projects these from the attributes map.
/// The runner-host is the sole producer of these values, so the constants live
/// here now. The string values are unchanged to keep existing telemetry
/// dashboards and queries stable.
pub(crate) mod attr_keys {
    pub const CUSTOMER_ID: &str = "gt.customer_id";
    pub const DEPLOYMENT_ID: &str = "gt.deployment_id";
    pub const BUNDLE_ID: &str = "gt.bundle_id";
    pub const REVISION_ID: &str = "gt.revision_id";
    pub const PACK_ID: &str = "gt.pack_id";
    pub const MESSAGING_ENDPOINT_ID: &str = "gt.messaging_endpoint_id";
}
use rand::{RngExt, rng};
use std::str::FromStr;
use tracing::Span;

pub const PROVIDER_ID: &str = "greentic-runner";

#[derive(Debug, Clone)]
pub struct FlowSpanAttributes<'a> {
    pub tenant: &'a str,
    pub flow_id: &'a str,
    pub node_id: Option<&'a str>,
    pub tool: Option<&'a str>,
    pub action: Option<&'a str>,
}

pub fn annotate_span(span: &Span, attrs: &FlowSpanAttributes<'_>) {
    span.record("tenant", attrs.tenant);
    span.record("flow_id", attrs.flow_id);
    if let Some(node) = attrs.node_id {
        span.record("node_id", node);
    }
    if let Some(tool) = attrs.tool {
        span.record("tool", tool);
    }
    if let Some(action) = attrs.action {
        span.record("action", action);
    }
}

pub fn tenant_context(
    env: &str,
    tenant: &str,
    flow_id: Option<&str>,
    node_id: Option<&str>,
    provider_id: Option<&str>,
    session_id: Option<&str>,
) -> anyhow::Result<TenantCtx> {
    use anyhow::Context as _;
    let env_id = EnvId::from_str(env).with_context(|| format!("invalid env id `{env}`"))?;
    let tenant_id =
        TenantId::from_str(tenant).with_context(|| format!("invalid tenant id `{tenant}`"))?;
    let mut ctx = TenantCtx::new(env_id, tenant_id);
    let provider = provider_id.unwrap_or(PROVIDER_ID);
    ctx = ctx.with_provider(provider.to_string());
    if let Some(flow) = flow_id {
        ctx = ctx.with_flow(flow.to_string());
    }
    if let Some(node) = node_id {
        ctx = ctx.with_node(node.to_string());
    }
    if let Some(session) = session_id {
        ctx = ctx.with_session(session.to_string());
    }
    Ok(ctx)
}

/// Build the per-invocation tenant context for a flow execution, stamping the
/// resolved `pack_id` and any rollout identifiers onto `attributes`.
///
/// `pack_id` is always live (the engine knows it per invocation). The rollout
/// IDs come from the engine's owning revision-keyed runtime and are empty until
/// the Phase-D revision dispatcher constructs revision runtimes. Pure so the
/// stamping can be unit-tested without the task-local slot.
#[allow(clippy::too_many_arguments)]
fn flow_tenant_ctx(
    env: &str,
    tenant: &str,
    flow_id: &str,
    node_id: Option<&str>,
    provider_id: Option<&str>,
    session_id: Option<&str>,
    pack_id: &str,
    rollout: &RolloutIds,
) -> anyhow::Result<TenantCtx> {
    let mut ctx = tenant_context(env, tenant, Some(flow_id), node_id, provider_id, session_id)?;
    if !pack_id.is_empty() {
        ctx.attributes
            .insert(attr_keys::PACK_ID.to_string(), pack_id.to_string());
    }
    stamp_rollout_ids(&mut ctx, rollout);
    Ok(ctx)
}

/// Project a [`TenantCtx`] into a [`greentic_telemetry::TelemetryCtx`] for span
/// annotation export.
///
/// This replaces the removed `greentic_types::telemetry::tenant_ctx_to_telemetry`.
/// The new `set_current_tenant_ctx` only copies the core identity fields into the
/// task-local slot; it no longer projects rollout/pack/messaging attributes. The
/// `annotate_span` export path still needs the full projection so that `gt.*`
/// attributes reach exported OTLP spans.
#[cfg(feature = "telemetry")]
fn tenant_ctx_to_telemetry(ctx: &TenantCtx) -> greentic_telemetry::TelemetryCtx {
    let mut t =
        greentic_telemetry::TelemetryCtx::new(ctx.tenant_id.as_ref()).with_env(ctx.env.as_str());
    if let Some(team) = ctx.team_id.as_ref().or(ctx.team.as_ref()) {
        t = t.with_team(team.as_str());
    }
    if let Some(session) = ctx.session_id() {
        t = t.with_session(session);
    }
    if let Some(flow) = ctx.flow_id() {
        t = t.with_flow(flow);
    }
    if let Some(node) = ctx.node_id() {
        t = t.with_node(node);
    }
    if let Some(provider) = ctx.provider_id() {
        t = t.with_provider(provider);
    }
    if let Some(v) = ctx.attributes.get(attr_keys::CUSTOMER_ID) {
        t = t.with_customer_id(v);
    }
    if let Some(v) = ctx.attributes.get(attr_keys::DEPLOYMENT_ID) {
        t = t.with_deployment_id(v);
    }
    if let Some(v) = ctx.attributes.get(attr_keys::BUNDLE_ID) {
        t = t.with_bundle_id(v);
    }
    if let Some(v) = ctx.attributes.get(attr_keys::REVISION_ID) {
        t = t.with_revision_id(v);
    }
    if let Some(v) = ctx.attributes.get(attr_keys::PACK_ID) {
        t = t.with_pack_id(v);
    }
    if let Some(v) = ctx.attributes.get(attr_keys::MESSAGING_ENDPOINT_ID) {
        t = t.with_messaging_endpoint_id(v);
    }
    t
}

/// Install per-invocation telemetry for `span`: stamp the tenant context (live
/// `pack_id` + rollout IDs) into the task-local slot, and export the `gt.*`
/// attribution as real OpenTelemetry attributes on `span`.
///
/// The export step is load-bearing: greentic-telemetry's `ContextLayer` only
/// stashes the task-local context in span extensions for capture layers — it
/// does NOT record `gt.*` onto exported spans (`Span::record` is a no-op for
/// callsite-unknown fields). The supported export primitive is
/// `greentic_telemetry::annotate_span`, which sets the attributes directly on
/// the owned span handle; without it the IDs never reach exported telemetry.
#[allow(clippy::too_many_arguments)]
pub fn set_flow_context(
    span: &Span,
    env: &str,
    tenant: &str,
    flow_id: &str,
    node_id: Option<&str>,
    provider_id: Option<&str>,
    session_id: Option<&str>,
    pack_id: &str,
    rollout: &RolloutIds,
) {
    let ctx = match flow_tenant_ctx(
        env,
        tenant,
        flow_id,
        node_id,
        provider_id,
        session_id,
        pack_id,
        rollout,
    ) {
        Ok(ctx) => ctx,
        Err(err) => {
            // Telemetry attribution must never take a flow down: skip stamping
            // the context for an id that does not validate.
            tracing::warn!(error = %err, "skipping telemetry context for invalid env/tenant id");
            return;
        }
    };
    set_current_tenant_ctx(&ctx);
    #[cfg(feature = "telemetry")]
    greentic_telemetry::annotate_span(span, &tenant_ctx_to_telemetry(&ctx));
    #[cfg(not(feature = "telemetry"))]
    let _ = span;
}

/// Deploy-spec rollout identifiers stamped onto the per-invocation
/// [`TenantCtx`] for telemetry attribution (B11). All optional — the producer
/// (the revision dispatcher resolving a deployment/revision) is Phase D, so
/// today these are `None` and [`stamp_rollout_ids`] is a no-op.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RolloutIds {
    pub customer_id: Option<String>,
    pub deployment_id: Option<String>,
    pub bundle_id: Option<String>,
    pub revision_id: Option<String>,
}

impl RolloutIds {
    /// True when no identifier is set (the common case until Phase D wires the
    /// dispatcher producer).
    pub fn is_empty(&self) -> bool {
        self.customer_id.is_none()
            && self.deployment_id.is_none()
            && self.bundle_id.is_none()
            && self.revision_id.is_none()
    }
}

/// Stamp the rollout IDs onto `ctx.attributes` under the canonical
/// [`attr_keys`], so the local [`tenant_ctx_to_telemetry`] projection copies
/// them into `TelemetryCtx` for span export.
///
/// Authoritative over these four keys: a present ID is written, an absent one
/// is cleared. Stamping is therefore safe to re-run on a reused `TenantCtx`
/// (e.g. a session that migrates between revisions) — an ID dropped on a
/// re-stamp won't linger as a stale attribute from an earlier stamp.
pub fn stamp_rollout_ids(ctx: &mut TenantCtx, ids: &RolloutIds) {
    set_or_clear(ctx, attr_keys::CUSTOMER_ID, ids.customer_id.as_deref());
    set_or_clear(ctx, attr_keys::DEPLOYMENT_ID, ids.deployment_id.as_deref());
    set_or_clear(ctx, attr_keys::BUNDLE_ID, ids.bundle_id.as_deref());
    set_or_clear(ctx, attr_keys::REVISION_ID, ids.revision_id.as_deref());
}

fn set_or_clear(ctx: &mut TenantCtx, key: &str, value: Option<&str>) {
    match value {
        Some(v) => {
            ctx.attributes.insert(key.to_string(), v.to_string());
        }
        None => {
            ctx.attributes.remove(key);
        }
    }
}

pub fn backoff_delay_ms(base: u64, attempt: u32) -> u64 {
    let multiplier = 1_u64 << attempt.min(10);
    let exp = base.saturating_mul(multiplier);
    let mut rng = rng();
    let jitter = rng.random_range(0..=exp.min(1000));
    exp.saturating_add(jitter)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> TenantCtx {
        tenant_context("prod-eu", "acme", None, None, None, None).expect("valid ids")
    }

    #[test]
    fn backoff_delay_saturates_instead_of_overflowing() {
        assert_eq!(backoff_delay_ms(u64::MAX, 8), u64::MAX);
    }

    #[test]
    fn invalid_ids_are_an_error_not_a_panic() {
        assert!(tenant_context("bad env!", "acme", None, None, None, None).is_err());
        assert!(tenant_context("prod", "bad tenant!", None, None, None, None).is_err());
        let span = Span::none();
        set_flow_context(
            &span,
            "bad env!",
            "acme",
            "flow",
            None,
            None,
            None,
            "",
            &RolloutIds::default(),
        );
    }

    #[test]
    fn stamp_sets_present_ids_under_canonical_keys() {
        let mut c = ctx();
        let ids = RolloutIds {
            customer_id: Some("cust-acme".into()),
            deployment_id: Some("01JTKS".into()),
            bundle_id: Some("customer.support".into()),
            revision_id: Some("01JTKR".into()),
        };
        stamp_rollout_ids(&mut c, &ids);
        assert_eq!(
            c.attributes.get(attr_keys::CUSTOMER_ID).map(String::as_str),
            Some("cust-acme")
        );
        assert_eq!(
            c.attributes
                .get(attr_keys::DEPLOYMENT_ID)
                .map(String::as_str),
            Some("01JTKS")
        );
        assert_eq!(
            c.attributes.get(attr_keys::BUNDLE_ID).map(String::as_str),
            Some("customer.support")
        );
        assert_eq!(
            c.attributes.get(attr_keys::REVISION_ID).map(String::as_str),
            Some("01JTKR")
        );
    }

    #[test]
    fn stamp_empty_is_noop() {
        let mut c = ctx();
        let before = c.attributes.len();
        stamp_rollout_ids(&mut c, &RolloutIds::default());
        assert_eq!(c.attributes.len(), before);
        assert!(RolloutIds::default().is_empty());
    }

    #[test]
    fn stamp_only_sets_present_subset() {
        let mut c = ctx();
        stamp_rollout_ids(
            &mut c,
            &RolloutIds {
                deployment_id: Some("01JTKS".into()),
                ..Default::default()
            },
        );
        assert!(c.attributes.contains_key(attr_keys::DEPLOYMENT_ID));
        assert!(!c.attributes.contains_key(attr_keys::CUSTOMER_ID));
    }

    #[test]
    fn flow_ctx_stamps_pack_id_live() {
        let ctx = flow_tenant_ctx(
            "prod-eu",
            "acme",
            "support",
            None,
            None,
            None,
            "customer.support@1.2.0",
            &RolloutIds::default(),
        )
        .expect("valid ids");
        assert_eq!(
            ctx.attributes.get(attr_keys::PACK_ID).map(String::as_str),
            Some("customer.support@1.2.0")
        );
        // No revision runtime today → rollout IDs absent.
        assert!(!ctx.attributes.contains_key(attr_keys::REVISION_ID));
    }

    #[test]
    fn flow_ctx_stamps_pack_id_and_rollout_ids() {
        let ctx = flow_tenant_ctx(
            "prod-eu",
            "acme",
            "support",
            None,
            None,
            None,
            "customer.support@1.2.0",
            &RolloutIds {
                customer_id: Some("cust-acme".into()),
                deployment_id: Some("01JTKS".into()),
                bundle_id: Some("customer.support".into()),
                revision_id: Some("01JTKR".into()),
            },
        )
        .expect("valid ids");
        assert_eq!(
            ctx.attributes.get(attr_keys::PACK_ID).map(String::as_str),
            Some("customer.support@1.2.0")
        );
        assert_eq!(
            ctx.attributes
                .get(attr_keys::REVISION_ID)
                .map(String::as_str),
            Some("01JTKR")
        );
        assert_eq!(
            ctx.attributes
                .get(attr_keys::DEPLOYMENT_ID)
                .map(String::as_str),
            Some("01JTKS")
        );
    }

    #[test]
    fn flow_ctx_skips_empty_pack_id() {
        let ctx = flow_tenant_ctx(
            "prod-eu",
            "acme",
            "support",
            None,
            None,
            None,
            "",
            &RolloutIds::default(),
        )
        .expect("valid ids");
        assert!(!ctx.attributes.contains_key(attr_keys::PACK_ID));
    }

    #[test]
    fn stamp_clears_stale_ids_on_restamp() {
        let mut c = ctx();
        stamp_rollout_ids(
            &mut c,
            &RolloutIds {
                customer_id: Some("cust-acme".into()),
                deployment_id: Some("01JTKS".into()),
                bundle_id: Some("customer.support".into()),
                revision_id: Some("01JTKR".into()),
            },
        );
        // Re-stamp with only a new revision (e.g. a session migrating revisions):
        // the other three IDs must be cleared, not left stale from the first stamp.
        stamp_rollout_ids(
            &mut c,
            &RolloutIds {
                revision_id: Some("01JTKZ".into()),
                ..Default::default()
            },
        );
        assert_eq!(
            c.attributes.get(attr_keys::REVISION_ID).map(String::as_str),
            Some("01JTKZ")
        );
        assert!(!c.attributes.contains_key(attr_keys::CUSTOMER_ID));
        assert!(!c.attributes.contains_key(attr_keys::DEPLOYMENT_ID));
        assert!(!c.attributes.contains_key(attr_keys::BUNDLE_ID));
    }
}

/// End-to-end export regression (C5.4): proves the stamped `gt.*` attribution
/// actually reaches an **exported** OTLP span — not just the task-local slot.
/// Guards against the failure mode where `set_current_tenant_ctx` is called but
/// no `annotate_span` export happens, so production spans omit pack/rollout
/// attribution while pure-context tests still pass.
#[cfg(all(test, feature = "telemetry"))]
mod export_tests {
    use super::*;
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::error::OTelSdkResult;
    use opentelemetry_sdk::trace::{SdkTracerProvider, SpanData, SpanExporter};
    use std::sync::{Arc, Mutex};
    use tracing::subscriber;
    use tracing_subscriber::prelude::*;

    #[derive(Clone, Debug)]
    struct TestExporter {
        spans: Arc<Mutex<Vec<SpanData>>>,
    }

    impl SpanExporter for TestExporter {
        fn export(
            &self,
            batch: Vec<SpanData>,
        ) -> impl std::future::Future<Output = OTelSdkResult> + Send {
            let spans = self.spans.clone();
            async move {
                spans
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .extend(batch);
                Ok(())
            }
        }
    }

    fn attr_value(span: &SpanData, key: &str) -> Option<String> {
        span.attributes
            .iter()
            .find(|kv| kv.key.as_str() == key)
            .map(|kv| kv.value.to_string())
    }

    #[test]
    fn set_flow_context_exports_pack_id_and_rollout_ids() {
        let exported = Arc::new(Mutex::new(Vec::new()));
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(TestExporter {
                spans: exported.clone(),
            })
            .build();
        let tracer = provider.tracer("c5.4-flow-export");
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
        let _guard = subscriber::set_default(subscriber);

        // A span whose callsite declares NONE of the gt.* fields — exactly the
        // `flow.execute` situation. `set_flow_context` must export them anyway.
        let span = tracing::info_span!("flow.execute");
        set_flow_context(
            &span,
            "prod-eu",
            "acme",
            "support",
            None,
            None,
            Some("sess-1"),
            "customer.support@1.2.0",
            &RolloutIds {
                customer_id: Some("cust-acme".into()),
                deployment_id: Some("01JTKS".into()),
                bundle_id: Some("customer.support".into()),
                revision_id: Some("01JTKR".into()),
            },
        );
        {
            let _enter = span.enter();
        }
        drop(span);

        let _ = provider.force_flush();
        let _ = provider.shutdown();

        let spans = exported.lock().unwrap_or_else(|e| e.into_inner());
        // Assert on the named span, not just the last one — a future extra span
        // from the subscriber stack would otherwise yield confusing failures.
        let span = spans
            .iter()
            .find(|s| s.name == "flow.execute")
            .expect("flow.execute span exported");
        assert_eq!(
            attr_value(span, attr_keys::PACK_ID).as_deref(),
            Some("customer.support@1.2.0"),
            "pack_id must reach the exported span"
        );
        assert_eq!(
            attr_value(span, attr_keys::REVISION_ID).as_deref(),
            Some("01JTKR")
        );
        assert_eq!(
            attr_value(span, attr_keys::DEPLOYMENT_ID).as_deref(),
            Some("01JTKS")
        );
        assert_eq!(
            attr_value(span, attr_keys::BUNDLE_ID).as_deref(),
            Some("customer.support")
        );
        assert_eq!(
            attr_value(span, attr_keys::CUSTOMER_ID).as_deref(),
            Some("cust-acme")
        );
    }
}
