use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use greentic_session::{SessionData, SessionKey as StoreSessionKey};
use greentic_types::{
    EnvId, FlowId, GreenticError, PackId, ReplyScope, SessionCursor as TypesSessionCursor,
    TenantCtx, TenantId, UserId,
};

use crate::telemetry::attr_keys;
use rand::{RngExt, rng};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use super::api::{RunFlowRequest, RunnerApi};
use super::builder::{Runner, RunnerBuilder};
use super::error::{GResult, RunnerError};
use super::glue::{FnSecretsHost, FnTelemetryHost};
use super::host::{HostBundle, SecretsHost, SessionHost, StateHost};
use super::policy::Policy;
use super::registry::{Adapter, AdapterCall, AdapterRegistry};
use super::shims::{InMemorySessionHost, InMemoryStateHost};
use super::state_machine::{FlowDefinition, FlowStep, PAYLOAD_FROM_LAST_INPUT};

use crate::config::{HostConfig, SecretsPolicy};
use crate::pack::FlowDescriptor;
use crate::run_outcome::RunOutcomeSink;
use crate::run_outcome::turn::{RunMarker, RunOutcomeObserver, RunOutcomeReporter, TurnContext};
use crate::runner::engine::{
    FlowContext, FlowEngine, FlowExecution, FlowSnapshot, FlowStatus, FlowWait,
};
use crate::runner::mocks::MockLayer;
use crate::secrets::{DynSecretsManager, read_secret_blocking};
use crate::storage::session::DynSessionStore;
use crate::trace::audit_sink::AuditSink;
use crate::trace::{PackTraceInfo, TraceContext, TraceMode, TraceRecorder};

const DEFAULT_ENV: &str = "local";
const PACK_FLOW_ADAPTER: &str = "pack_flow";

/// Reads and writes the parked-flow wait that makes a conversation resumable.
///
/// Every method is `async` because the backing `SessionStore` may be Redis, and
/// `SessionStore` is a SYNCHRONOUS trait whose Redis implementation blocks on a
/// socket. The store round-trips run on [`tokio::task::spawn_blocking`]; the key
/// derivation around them is pure and stays on the caller's thread.
///
/// These three were synchronous before durable storage was configurable, which
/// was harmless while the only backend was an in-process `HashMap`. It is not
/// harmless now: this is the hot path of every parked turn, so leaving it
/// blocking would park one tokio worker per concurrent resume — invisible in a
/// test and a throughput collapse in a deployment.
#[derive(Clone)]
pub struct FlowResumeStore {
    store: DynSessionStore,
}

impl FlowResumeStore {
    pub fn new(store: DynSessionStore) -> Self {
        Self { store }
    }

    pub async fn fetch(&self, envelope: &IngressEnvelope) -> GResult<Option<FlowSnapshot>> {
        Ok(self
            .fetch_with_run(envelope)
            .await?
            .map(|(snapshot, _)| snapshot))
    }

    /// [`fetch`](Self::fetch), plus the run-audit marker stored with the
    /// snapshot (absent on a wait parked before markers existed, or by a host
    /// with no run-outcome sink).
    pub(crate) async fn fetch_with_run(
        &self,
        envelope: &IngressEnvelope,
    ) -> GResult<Option<(FlowSnapshot, Option<RunMarker>)>> {
        let (mut ctx, user, _, scope) = build_store_ctx(envelope)?;
        ctx = ctx.with_user(Some(user.clone()));

        let mut scopes = vec![scope.clone()];
        if scope.correlation.is_some() {
            let mut base = scope.clone();
            base.correlation = None;
            scopes.push(base);
        }

        let expected_pack = envelope.pack_id.clone();
        let store = Arc::clone(&self.store);
        offload(move || {
            for lookup in scopes {
                let Some(key) = store.find_wait_by_scope(&ctx, &user, &lookup)? else {
                    continue;
                };
                let Some(data) = store.get_session(&key)? else {
                    continue;
                };
                let record: FlowResumeRecord =
                    serde_json::from_str(&data.context_json).map_err(|err| {
                        decode_failure(format!("failed to decode flow resume snapshot: {err}"))
                    })?;
                if let Some(pack_id) = expected_pack.as_deref()
                    && record.snapshot.pack_id != pack_id
                {
                    return Err(decode_failure(format!(
                        "resume pack mismatch: expected {pack_id}, found {}",
                        record.snapshot.pack_id
                    )));
                }
                return Ok(Some((record.snapshot, record.run)));
            }
            Ok(None)
        })
        .await
    }

    pub async fn save(&self, envelope: &IngressEnvelope, wait: &FlowWait) -> GResult<ReplyScope> {
        self.save_with_run(envelope, wait, None).await
    }

    /// [`save`](Self::save), persisting `run` beside the snapshot so the next
    /// resume reports under the same run id. `None` stores exactly what
    /// [`save`](Self::save) always stored.
    pub(crate) async fn save_with_run(
        &self,
        envelope: &IngressEnvelope,
        wait: &FlowWait,
        run: Option<RunMarker>,
    ) -> GResult<ReplyScope> {
        let (ctx, user, hint, scope) = build_store_ctx(envelope)?;
        let record = FlowResumeRecord {
            snapshot: wait.snapshot.clone(),
            reason: wait.reason.clone(),
            run,
        };
        let data = record_to_session_data(&record, ctx.clone(), &user, &hint)?;
        let mut reply_scope = scope.clone();
        if reply_scope.correlation.is_none() {
            reply_scope.correlation = Some(generate_correlation_id());
        }
        let mut store_scope = scope;
        store_scope.correlation = None;
        let session_key = StoreSessionKey::new(format!("{hint}::{}", store_scope.scope_hash()));
        let store = Arc::clone(&self.store);
        offload(move || store.register_wait(&ctx, &user, &store_scope, &session_key, data, None))
            .await?;
        Ok(reply_scope)
    }

    pub async fn clear(&self, envelope: &IngressEnvelope) -> GResult<()> {
        let (ctx, user, _, scope) = build_store_ctx(envelope)?;
        let mut scopes = vec![scope.clone()];
        if scope.correlation.is_some() {
            let mut base = scope;
            base.correlation = None;
            scopes.push(base);
        }
        let store = Arc::clone(&self.store);
        offload(move || {
            for lookup in scopes {
                store.clear_wait(&ctx, &user, &lookup)?;
            }
            Ok(())
        })
        .await
    }

    /// Returns the `(tenant_ctx, user_id)` pair this store derives from
    /// `envelope` for wait bucketing.
    ///
    /// Exposed so siblings — currently the M1.5 welcome-seen marker — can
    /// partition by the SAME identity instead of re-deriving the digest and
    /// risking drift.
    pub(crate) fn contact_identity(envelope: &IngressEnvelope) -> GResult<(TenantCtx, UserId)> {
        let (ctx, user, _, _) = build_store_ctx(envelope)?;
        Ok((ctx, user))
    }
}

#[derive(Serialize, Deserialize)]
struct FlowResumeRecord {
    snapshot: FlowSnapshot,
    #[serde(default)]
    reason: Option<String>,
    /// Run-audit marker (`crate::run_outcome`). Skipped when absent, so a host
    /// without a run-outcome sink persists byte-identical records, and a
    /// record written before this field existed decodes as `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    run: Option<RunMarker>,
}

fn build_store_ctx(envelope: &IngressEnvelope) -> GResult<(TenantCtx, UserId, String, ReplyScope)> {
    let base_hint = envelope
        .session_hint
        .clone()
        .unwrap_or_else(|| envelope.canonical_session_hint());
    let hint = if let Some(pack_id) = envelope.pack_id.as_deref() {
        format!("{base_hint}::pack={pack_id}")
    } else {
        base_hint.clone()
    };
    let user = derive_user_id(&hint)?;
    let scope = envelope
        .reply_scope
        .clone()
        .ok_or_else(|| RunnerError::Session {
            reason: "Cannot suspend: reply_scope missing; provider plugin must supply ReplyScope"
                .to_string(),
        })?;
    let mut ctx = envelope.tenant_ctx();
    ctx = ctx.with_session(hint.clone());
    ctx = ctx.with_user(Some(user.clone()));
    Ok((ctx, user, hint, scope))
}

fn record_to_session_data(
    record: &FlowResumeRecord,
    ctx: TenantCtx,
    user: &UserId,
    session_hint: &str,
) -> GResult<SessionData> {
    let flow = FlowId::from_str(record.snapshot.flow_id.as_str()).map_err(map_store_error)?;
    let pack = PackId::from_str(record.snapshot.pack_id.as_str()).map_err(map_store_error)?;
    let mut cursor = TypesSessionCursor::new(record.snapshot.next_node.clone());
    if let Some(reason) = record.reason.clone() {
        cursor = cursor.with_wait_reason(reason);
    }
    let context_json = serde_json::to_string(record).map_err(|err| RunnerError::Session {
        reason: format!("failed to encode flow resume snapshot: {err}"),
    })?;
    let ctx = ctx
        .with_user(Some(user.clone()))
        .with_session(session_hint.to_string())
        .with_flow(record.snapshot.flow_id.clone());
    Ok(SessionData {
        tenant_ctx: ctx,
        flow_id: flow,
        pack_id: Some(pack),
        cursor,
        context_json,
    })
}

fn derive_user_id(hint: &str) -> GResult<UserId> {
    let digest = Sha256::digest(hint.as_bytes());
    let slug = format!("sess{}", hex::encode(&digest[..8]));
    UserId::from_str(&slug).map_err(map_store_error)
}

fn map_store_error(err: GreenticError) -> RunnerError {
    RunnerError::Session {
        reason: err.to_string(),
    }
}

/// A decode/consistency failure raised from inside an offloaded closure.
///
/// The closure's error channel is the store's own `GreenticError`, so a reason
/// that did not come from the store still travels as one and is mapped back to
/// `RunnerError::Session` by [`offload`] — the same variant the caller saw when
/// these methods were synchronous.
fn decode_failure(reason: impl Into<String>) -> GreenticError {
    GreenticError::new(greentic_types::ErrorCode::Internal, reason)
}

/// Run a blocking session-store call on the blocking pool.
///
/// `spawn_blocking` rather than `block_in_place`: the latter panics on a
/// `current_thread` runtime and inside a `LocalSet`, and this is a library —
/// the embedder picks the runtime flavour, not us.
async fn offload<F, T>(f: F) -> GResult<T>
where
    F: FnOnce() -> Result<T, GreenticError> + Send + 'static,
    T: Send + 'static,
{
    match tokio::task::spawn_blocking(f).await {
        Ok(result) => result.map_err(map_store_error),
        Err(err) => Err(RunnerError::Session {
            reason: format!("session store call panicked or was cancelled: {err}"),
        }),
    }
}

fn generate_correlation_id() -> String {
    let mut bytes = [0u8; 16];
    rng().fill(&mut bytes);
    hex::encode(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runner::engine::ExecutionState;
    use crate::storage::session::new_session_store;
    use serde_json::json;

    fn sample_envelope() -> IngressEnvelope {
        IngressEnvelope {
            tenant: "demo".into(),
            env: Some("local".into()),
            pack_id: Some("pack.demo".into()),
            flow_id: "flow.main".into(),
            flow_type: None,
            action: Some("messaging".into()),
            session_hint: Some("demo:provider:chan:conv:user".into()),
            provider: Some("provider".into()),
            messaging_endpoint_id: None,
            channel: Some("chan".into()),
            conversation: Some("conv".into()),
            user: Some("user".into()),
            entry_node: None,
            activity_id: Some("act-1".into()),
            timestamp: None,
            payload: json!({ "text": "hi" }),
            metadata: None,
            reply_scope: Some(ReplyScope {
                conversation: "conv".into(),
                thread: None,
                reply_to: None,
                correlation: None,
            }),
        }
    }

    fn sample_wait() -> FlowWait {
        let state: ExecutionState = serde_json::from_value(json!({
            "input": { "text": "hi" },
            "nodes": {},
            "egress": []
        }))
        .expect("state");
        FlowWait {
            reason: Some("await-user".into()),
            snapshot: FlowSnapshot {
                pack_id: "pack.demo".into(),
                flow_id: "flow.main".into(),
                next_flow: None,
                next_node: "node-2".into(),
                awaiting_submit: false,
                state,
            },
        }
    }

    #[test]
    fn derive_user_id_is_stable() {
        let hint = "some-tenant::session-key";
        let a = derive_user_id(hint).unwrap();
        let b = derive_user_id(hint).unwrap();
        assert_eq!(a, b);
        assert!(a.as_str().starts_with("sess"));
    }

    #[tokio::test]
    async fn resume_store_roundtrip() -> GResult<()> {
        let store = FlowResumeStore::new(new_session_store());
        let envelope = sample_envelope();
        assert!(store.fetch(&envelope).await?.is_none());

        let wait = sample_wait();
        let _ = store.save(&envelope, &wait).await?;
        let snapshot = store.fetch(&envelope).await?.expect("snapshot missing");
        assert_eq!(snapshot.flow_id, wait.snapshot.flow_id);
        assert_eq!(snapshot.next_node, wait.snapshot.next_node);

        store.clear(&envelope).await?;
        assert!(store.fetch(&envelope).await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn resume_store_round_trips_the_run_marker() -> GResult<()> {
        let store = FlowResumeStore::new(new_session_store());
        let envelope = sample_envelope();
        let marker = RunMarker::mint();
        let _ = store
            .save_with_run(&envelope, &sample_wait(), Some(marker.clone()))
            .await?;
        let (_, run) = store
            .fetch_with_run(&envelope)
            .await?
            .expect("snapshot missing");
        assert_eq!(run, Some(marker));
        Ok(())
    }

    #[tokio::test]
    async fn a_wait_saved_without_a_marker_resumes_without_one() -> GResult<()> {
        let store = FlowResumeStore::new(new_session_store());
        let envelope = sample_envelope();
        let _ = store.save(&envelope, &sample_wait()).await?;
        let (_, run) = store
            .fetch_with_run(&envelope)
            .await?
            .expect("snapshot missing");
        assert_eq!(run, None);
        Ok(())
    }

    #[test]
    fn a_record_written_before_run_markers_decodes_and_a_markerless_one_is_unchanged() {
        let wait = sample_wait();
        let legacy = json!({ "snapshot": wait.snapshot, "reason": "await-user" });
        let record: FlowResumeRecord = serde_json::from_value(legacy.clone()).expect("decode");
        assert!(record.run.is_none());
        // No marker ⇒ the exact bytes a pre-audit runtime wrote.
        assert_eq!(serde_json::to_value(&record).expect("encode"), legacy);
    }

    #[tokio::test]
    async fn resume_store_overwrites_existing() -> GResult<()> {
        let store = FlowResumeStore::new(new_session_store());
        let envelope = sample_envelope();
        let mut wait = sample_wait();
        let _ = store.save(&envelope, &wait).await?;

        wait.snapshot.next_node = "node-3".into();
        wait.reason = Some("retry".into());
        let _ = store.save(&envelope, &wait).await?;

        let snapshot = store.fetch(&envelope).await?.expect("snapshot missing");
        assert_eq!(snapshot.next_node, "node-3");
        store.clear(&envelope).await?;
        Ok(())
    }

    #[tokio::test]
    async fn resume_store_uses_snapshot_even_if_envelope_flow_differs() -> GResult<()> {
        let store = FlowResumeStore::new(new_session_store());
        let envelope = sample_envelope();
        let wait = sample_wait();
        let _ = store.save(&envelope, &wait).await?;

        let mut redirected = envelope.clone();
        redirected.flow_id = "flow.other".into();
        let snapshot = store.fetch(&redirected).await?.expect("snapshot missing");
        assert_eq!(snapshot.flow_id, wait.snapshot.flow_id);

        store.clear(&envelope).await?;
        Ok(())
    }

    #[test]
    fn canonicalize_populates_defaults() {
        let envelope = IngressEnvelope {
            tenant: "demo".into(),
            env: None,
            pack_id: None,
            flow_id: "flow.main".into(),
            flow_type: None,
            action: None,
            session_hint: None,
            provider: None,
            messaging_endpoint_id: None,
            channel: None,
            conversation: None,
            user: None,
            entry_node: None,
            activity_id: Some("activity-1".into()),
            timestamp: None,
            payload: json!({}),
            metadata: None,
            reply_scope: None,
        }
        .canonicalize();

        assert_eq!(envelope.provider.as_deref(), Some("provider"));
        assert_eq!(envelope.channel.as_deref(), Some("flow.main"));
        assert_eq!(envelope.conversation.as_deref(), Some("flow.main"));
        assert_eq!(envelope.user.as_deref(), Some("activity-1"));
        assert!(envelope.session_hint.is_some());
    }

    #[test]
    fn canonical_session_hint_is_pure_structured_form() {
        // canonical_session_hint() does NOT embed messaging_endpoint_id —
        // namespacing happens at the canonicalize() layer so explicit
        // producer-supplied hints get the same treatment.
        let mut envelope = sample_envelope();
        envelope.session_hint = None;
        assert_eq!(
            envelope.canonical_session_hint(),
            "demo:provider:chan:conv:user"
        );
        envelope.messaging_endpoint_id = Some("teams-legal".into());
        assert_eq!(
            envelope.canonical_session_hint(),
            "demo:provider:chan:conv:user"
        );
    }

    #[test]
    fn canonicalize_unchanged_when_endpoint_id_none() {
        // Backward compat: pre-M1.4 envelopes (endpoint_id = None) must
        // produce the same session_hint bytes, or existing Redis sessions
        // orphan. Both derived and explicit hints stay untouched.
        let mut derived = sample_envelope();
        derived.session_hint = None;
        let derived = derived.canonicalize();
        assert_eq!(
            derived.session_hint.as_deref(),
            Some("demo:provider:chan:conv:user")
        );

        let explicit = sample_envelope().canonicalize();
        assert_eq!(
            explicit.session_hint.as_deref(),
            Some("demo:provider:chan:conv:user")
        );
    }

    #[test]
    fn canonicalize_namespaces_derived_hint_when_endpoint_id_set() {
        let mut envelope = sample_envelope();
        envelope.session_hint = None;
        envelope.messaging_endpoint_id = Some("teams-legal".into());
        let envelope = envelope.canonicalize();
        assert_eq!(
            envelope.session_hint.as_deref(),
            Some("ep=teams-legal::demo:provider:chan:conv:user")
        );
    }

    #[test]
    fn canonicalize_partitions_explicit_hints_across_endpoints() {
        // Codex M1.4b-iii regression: producer-supplied hints must still
        // partition by endpoint id, not collapse to the same session key.
        // Two endpoints with IDENTICAL producer-supplied session_hints must
        // resolve to distinct effective session keys.
        let raw = "shared-session-key";

        let mut a = sample_envelope();
        a.session_hint = Some(raw.into());
        a.messaging_endpoint_id = Some("teams-legal".into());
        let a = a.canonicalize();

        let mut b = sample_envelope();
        b.session_hint = Some(raw.into());
        b.messaging_endpoint_id = Some("teams-accounting".into());
        let b = b.canonicalize();

        assert_ne!(a.session_hint, b.session_hint);
        assert_eq!(
            a.session_hint.as_deref(),
            Some("ep=teams-legal::shared-session-key")
        );
        assert_eq!(
            b.session_hint.as_deref(),
            Some("ep=teams-accounting::shared-session-key")
        );
    }

    #[test]
    fn canonicalize_is_idempotent_for_endpoint_prefix() {
        // Re-canonicalizing must not double-prefix the namespace marker.
        let mut envelope = sample_envelope();
        envelope.session_hint = Some("raw-key".into());
        envelope.messaging_endpoint_id = Some("teams-legal".into());
        let once = envelope.canonicalize();
        let twice = once.clone().canonicalize();
        assert_eq!(once.session_hint, twice.session_hint);
        assert_eq!(
            twice.session_hint.as_deref(),
            Some("ep=teams-legal::raw-key")
        );
    }

    #[test]
    fn canonicalize_drops_invalid_endpoint_id_to_none() {
        // Defensive depth: an eid that bypassed the http-layer validator
        // (embedded caller, future WIT-derived `identify_instance`) must
        // not corrupt the `ep=<eid>::<base>` namespace prefix or the
        // telemetry attribute. Drop to None ⇒ run unscoped.
        let cases = [
            ("empty", ""),
            ("colon embedded", "teams:legal"), // collides prefix delimiter
            ("space", "teams legal"),
            ("control char", "teams\nlegal"),
            ("oversized", &"a".repeat(129)),
        ];
        for (label, bad) in cases {
            let mut envelope = sample_envelope();
            envelope.session_hint = Some("raw-key".into());
            envelope.messaging_endpoint_id = Some(bad.into());
            let canon = envelope.canonicalize();
            assert!(
                canon.messaging_endpoint_id.is_none(),
                "{label}: invalid eid {bad:?} must drop to None"
            );
            // Session hint must be the un-prefixed form when eid was dropped.
            assert_eq!(
                canon.session_hint.as_deref(),
                Some("raw-key"),
                "{label}: dropped eid must leave hint un-namespaced"
            );
        }
    }

    #[test]
    fn canonicalize_preserves_valid_endpoint_id_forms() {
        // The validator must accept both the M1.2 ULID form and a
        // hand-typeable slug, and the dot variant used in display ids.
        for valid in ["teams-legal", "01HA1ABCDE", "teams_legal.v2"] {
            let mut envelope = sample_envelope();
            envelope.session_hint = Some("raw-key".into());
            envelope.messaging_endpoint_id = Some(valid.into());
            let canon = envelope.canonicalize();
            assert_eq!(
                canon.messaging_endpoint_id.as_deref(),
                Some(valid),
                "valid eid {valid:?} must be preserved"
            );
            assert_eq!(
                canon.session_hint.as_deref(),
                Some(format!("ep={valid}::raw-key").as_str()),
                "valid eid {valid:?} must still prefix the hint"
            );
        }
    }

    #[test]
    fn tenant_ctx_stamps_messaging_endpoint_id() {
        let mut envelope = sample_envelope();
        envelope.messaging_endpoint_id = Some("teams-legal".into());
        let ctx = envelope.tenant_ctx();
        assert_eq!(
            ctx.attributes.get(attr_keys::MESSAGING_ENDPOINT_ID),
            Some(&"teams-legal".to_string())
        );
    }

    #[test]
    fn tenant_ctx_omits_messaging_endpoint_id_when_unset() {
        let envelope = sample_envelope();
        let ctx = envelope.tenant_ctx();
        assert!(
            !ctx.attributes
                .contains_key(attr_keys::MESSAGING_ENDPOINT_ID)
        );
    }
}

pub struct StateMachineRuntime {
    runner: Runner,
    /// Reports a turn that failed after every retry, once — see
    /// `run_outcome::turn::TurnScope`. `None` without a sink.
    run_outcome: Option<RunOutcomeReporter>,
}

impl StateMachineRuntime {
    /// Construct a runtime from explicit flow definitions (legacy entrypoint used by tests/examples).
    pub fn new(flows: Vec<FlowDefinition>) -> GResult<Self> {
        let secrets = Arc::new(FnSecretsHost::new(|name| {
            Err(RunnerError::Secrets {
                reason: format!("secret {name} unavailable (noop host)"),
            })
        }));
        let telemetry = Arc::new(FnTelemetryHost::new(|_, _| Ok(())));
        let session = Arc::new(InMemorySessionHost::new());
        let state = Arc::new(InMemoryStateHost::new());
        let host = HostBundle::new(secrets, telemetry, session, state);

        let adapters = AdapterRegistry::default();
        let policy = Policy::default();

        let mut builder = RunnerBuilder::new()
            .with_host(host)
            .with_adapters(adapters)
            .with_policy(policy);
        for flow in flows {
            builder = builder.with_flow(flow);
        }
        let runner = builder.build()?;
        Ok(Self {
            runner,
            run_outcome: None,
        })
    }

    /// Build a state-machine runtime that proxies pack flows through the legacy FlowEngine.
    #[allow(clippy::too_many_arguments)]
    pub fn from_flow_engine(
        config: Arc<HostConfig>,
        engine: Arc<FlowEngine>,
        pack_trace: HashMap<String, PackTraceInfo>,
        session_host: Arc<dyn SessionHost>,
        session_store: DynSessionStore,
        state_host: Arc<dyn StateHost>,
        secrets_manager: DynSecretsManager,
        mocks: Option<Arc<MockLayer>>,
        audit_nats_client: Option<async_nats::Client>,
    ) -> Result<Self> {
        Self::from_flow_engine_with_run_outcome_sink(
            config,
            engine,
            pack_trace,
            session_host,
            session_store,
            state_host,
            secrets_manager,
            mocks,
            audit_nats_client,
            None,
        )
    }

    /// [`from_flow_engine`](Self::from_flow_engine), additionally reporting one
    /// [`crate::run_outcome::RunOutcome`] per flow turn to `run_outcome_sink`.
    /// `None` is exactly [`from_flow_engine`](Self::from_flow_engine).
    #[allow(clippy::too_many_arguments)]
    pub fn from_flow_engine_with_run_outcome_sink(
        config: Arc<HostConfig>,
        engine: Arc<FlowEngine>,
        pack_trace: HashMap<String, PackTraceInfo>,
        session_host: Arc<dyn SessionHost>,
        session_store: DynSessionStore,
        state_host: Arc<dyn StateHost>,
        secrets_manager: DynSecretsManager,
        mocks: Option<Arc<MockLayer>>,
        audit_nats_client: Option<async_nats::Client>,
        run_outcome_sink: Option<Arc<dyn RunOutcomeSink>>,
    ) -> Result<Self> {
        let run_outcome = run_outcome_sink.map(RunOutcomeReporter::new);
        let policy = Arc::new(config.secrets_policy.clone());
        let tenant_ctx = config.tenant_ctx();
        let secrets = Arc::new(PolicySecretsHost::new(policy, secrets_manager, tenant_ctx));
        let telemetry = Arc::new(FnTelemetryHost::new(|span, fields| {
            tracing::debug!(?span, ?fields, "telemetry emit");
            Ok(())
        }));
        let host = HostBundle::new(secrets, telemetry, session_host, state_host);
        let resume_store = FlowResumeStore::new(session_store);

        let mut adapters = AdapterRegistry::default();
        adapters.register(
            PACK_FLOW_ADAPTER,
            Box::new(PackFlowAdapter::new(
                Arc::clone(&config),
                Arc::clone(&engine),
                pack_trace,
                resume_store,
                mocks,
                audit_nats_client,
                run_outcome.clone(),
            )),
        );

        let flows = build_flow_definitions(engine.flows());
        let mut builder = RunnerBuilder::new()
            .with_host(host)
            .with_adapters(adapters)
            .with_policy(Policy::default());
        for flow in flows {
            builder = builder.with_flow(flow);
        }
        let runner = builder
            .build()
            .map_err(|err| anyhow!("state machine init failed: {err}"))?;
        Ok(Self {
            runner,
            run_outcome,
        })
    }

    /// Execute the flow associated with the provided ingress event.
    pub async fn handle(&self, envelope: IngressEnvelope) -> Result<Value> {
        let result = self.run_flow_result(envelope).await?;
        let outcome = result.outcome;
        Ok(outcome.get("response").cloned().unwrap_or(outcome))
    }

    /// Like [`Self::handle`], but also returns this turn's per-step node-output
    /// map (keyed by each step's `operation`; for a `dw.agent` step that is the
    /// agent_id). Observability only — the returned response `Value` is computed
    /// byte-identically to [`Self::handle`].
    pub async fn handle_traced(&self, envelope: IngressEnvelope) -> Result<(Value, Value)> {
        let result = self.run_flow_result(envelope).await?;
        let node_outputs = result.node_outputs;
        let outcome = result.outcome;
        let response = outcome.get("response").cloned().unwrap_or(outcome);
        Ok((response, node_outputs))
    }

    /// Shared body for [`Self::handle`] / [`Self::handle_traced`]: build the
    /// ingress request and run the flow. Keeping this single seam guarantees the
    /// traced and untraced paths can never diverge on the outcome.
    async fn run_flow_result(
        &self,
        envelope: IngressEnvelope,
    ) -> Result<super::api::RunFlowResult> {
        let tenant_ctx = envelope.tenant_ctx();
        let session_hint = envelope
            .session_hint
            .clone()
            .unwrap_or_else(|| envelope.canonical_session_hint());
        let pack_id = envelope.pack_id.clone().ok_or_else(|| {
            anyhow!("pack_id missing; ingress must specify pack_id for multi-pack flows")
        })?;
        let input =
            serde_json::to_value(&envelope).context("failed to serialise ingress envelope")?;
        let request = RunFlowRequest {
            tenant: tenant_ctx,
            pack_id,
            flow_id: envelope.flow_id.clone(),
            input,
            session_hint: Some(session_hint),
        };
        let run = self.runner.run_flow(request);
        let result = match self.run_outcome.as_ref() {
            Some(reporter) => reporter.scoped(run).await,
            None => run.await,
        };
        result.map_err(|err| anyhow!("flow execution failed: {err}"))
    }
}

struct PolicySecretsHost {
    policy: Arc<SecretsPolicy>,
    manager: DynSecretsManager,
    tenant_ctx: TenantCtx,
}

impl PolicySecretsHost {
    fn new(policy: Arc<SecretsPolicy>, manager: DynSecretsManager, tenant_ctx: TenantCtx) -> Self {
        Self {
            policy,
            manager,
            tenant_ctx,
        }
    }
}

/// Pseudo-pack segment for [`PolicySecretsHost`].
///
/// **Deliberately NOT unit-scoped**, for a blunter reason than
/// `TenantRuntime::get_secret`'s: this host is unreachable. It is installed on
/// `HostBundle.secrets`, a field nothing in the workspace ever reads — every
/// live secret read goes through `HostState` (`pack.rs`) or
/// `TenantRuntime::get_secret`. Threading a unit through dead code would be
/// one more thing to keep in step with the live path for no behaviour; removing
/// it means removing `HostBundle.secrets`, the `SecretsHost` trait and
/// `FnSecretsHost` with it, which is a wider cleanup than this change.
const POLICY_SECRETS_PACK_ID: &str = "_runner";

#[async_trait]
impl SecretsHost for PolicySecretsHost {
    async fn get(&self, name: &str) -> GResult<String> {
        if !self.policy.is_allowed(name) {
            return Err(RunnerError::Secrets {
                reason: format!("secret {name} denied by policy"),
            });
        }
        let bytes = read_secret_blocking(
            &self.manager,
            &self.tenant_ctx,
            POLICY_SECRETS_PACK_ID,
            name,
        )
        .map_err(|err| RunnerError::Secrets {
            reason: format!("secret {name} unavailable: {err}"),
        })?;
        String::from_utf8(bytes).map_err(|err| RunnerError::Secrets {
            reason: format!("secret {name} not valid UTF-8: {err}"),
        })
    }
}

fn build_flow_definitions(flows: &[FlowDescriptor]) -> Vec<FlowDefinition> {
    flows
        .iter()
        .map(|descriptor| {
            FlowDefinition::new(
                super::api::FlowSummary {
                    pack_id: descriptor.pack_id.clone(),
                    id: descriptor.id.clone(),
                    name: descriptor
                        .description
                        .clone()
                        .unwrap_or_else(|| descriptor.id.clone()),
                    version: descriptor.version.clone(),
                    description: descriptor.description.clone(),
                },
                serde_json::json!({
                    "type": "object"
                }),
                vec![FlowStep::Adapter(AdapterCall {
                    adapter: PACK_FLOW_ADAPTER.into(),
                    operation: descriptor.id.clone(),
                    payload: Value::String(PAYLOAD_FROM_LAST_INPUT.into()),
                })],
            )
        })
        .collect()
}

struct PackFlowAdapter {
    tenant: String,
    config: Arc<HostConfig>,
    engine: Arc<FlowEngine>,
    pack_trace: HashMap<String, PackTraceInfo>,
    resume: FlowResumeStore,
    mocks: Option<Arc<MockLayer>>,
    /// NATS client backing the per-node audit-event sink; `None` (the
    /// default, off path) when `GREENTIC_EVENTS_NATS_URL` is unset or NATS
    /// could not be reached — see `docs/superpowers/specs/2026-07-03-runner-audit-emitter-design.md`.
    audit_nats_client: Option<async_nats::Client>,
    /// Run-audit reporter; `None` (the default) installs no observer, mints no
    /// run id and changes nothing — see `crate::run_outcome`.
    run_outcome: Option<RunOutcomeReporter>,
}

impl PackFlowAdapter {
    fn new(
        config: Arc<HostConfig>,
        engine: Arc<FlowEngine>,
        pack_trace: HashMap<String, PackTraceInfo>,
        resume: FlowResumeStore,
        mocks: Option<Arc<MockLayer>>,
        audit_nats_client: Option<async_nats::Client>,
        run_outcome: Option<RunOutcomeReporter>,
    ) -> Self {
        Self {
            tenant: config.tenant.clone(),
            config,
            engine,
            pack_trace,
            resume,
            mocks,
            audit_nats_client,
            run_outcome,
        }
    }
}

#[async_trait::async_trait]
impl Adapter for PackFlowAdapter {
    async fn call(&self, call: &AdapterCall) -> GResult<Value> {
        Ok(self.call_traced(call).await?.0)
    }

    async fn call_traced(
        &self,
        call: &AdapterCall,
    ) -> GResult<(Value, serde_json::Map<String, Value>)> {
        let envelope: IngressEnvelope =
            serde_json::from_value(call.payload.clone()).map_err(|err| {
                RunnerError::AdapterCall {
                    reason: format!("invalid ingress payload: {err}"),
                }
            })?;
        let envelope = envelope.canonicalize();
        let flow_id = call.operation.clone();
        let action_owned = envelope.action.clone();
        let session_owned = envelope
            .session_hint
            .clone()
            .unwrap_or_else(|| envelope.canonical_session_hint());
        let provider_owned = envelope.provider.clone();
        let payload = envelope.payload.clone();
        let retry_config = self.config.retry_config().into();
        let (resume_snapshot, resume_run) = match self.resume.fetch_with_run(&envelope).await? {
            Some((snapshot, run)) => (Some(snapshot), run),
            None => (None, None),
        };
        let resume_flow_id = resume_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.next_flow.clone())
            .or_else(|| {
                resume_snapshot
                    .as_ref()
                    .map(|snapshot| snapshot.flow_id.clone())
            });
        let effective_flow_id = resume_flow_id.clone().unwrap_or_else(|| flow_id.clone());
        let effective_pack_id = if let Some(snapshot) = resume_snapshot.as_ref() {
            snapshot.pack_id.clone()
        } else if let Some(pack_id) = envelope.pack_id.as_deref() {
            let found = self
                .engine
                .flow_by_key(pack_id, effective_flow_id.as_str())
                .is_some();
            if !found {
                return Err(RunnerError::AdapterCall {
                    reason: format!(
                        "flow {} not registered for pack {pack_id}",
                        effective_flow_id
                    ),
                });
            }
            pack_id.to_string()
        } else if let Some(flow) = self.engine.flow_by_id(effective_flow_id.as_str()) {
            flow.pack_id.clone()
        } else {
            return Err(RunnerError::AdapterCall {
                reason: format!(
                    "flow {} is ambiguous; pack_id is required",
                    effective_flow_id
                ),
            });
        };

        let trace_config = self.config.trace.clone();
        let flow_version = self
            .engine
            .flow_by_key(effective_pack_id.as_str(), effective_flow_id.as_str())
            .map(|desc| desc.version.clone())
            .unwrap_or_else(|| "unknown".to_string());
        let pack_trace = self
            .pack_trace
            .get(effective_pack_id.as_str())
            .cloned()
            .unwrap_or_else(|| PackTraceInfo {
                pack_ref: effective_pack_id.clone(),
                resolved_digest: None,
            });
        let trace_ctx = TraceContext {
            pack_ref: pack_trace.pack_ref,
            resolved_digest: pack_trace.resolved_digest,
            flow_id: effective_flow_id.clone(),
            flow_version,
        };
        let trace = if trace_config.mode == TraceMode::Off {
            None
        } else {
            // Build the best-effort audit sink from the threaded NATS client
            // (EPIC-B B-2). `audit_tenant` is derived from the same
            // condition as the sink so the two stay coupled: both `Some`
            // (audit enabled) or both `None` (default, file-trace-only path,
            // zero behaviour change). `TraceRecorder` construction happens
            // on the async execution path, so `AuditSink::new`'s
            // `tokio::spawn` runs inside a live tokio runtime context.
            let sink = self.audit_nats_client.clone().map(AuditSink::new);
            let audit_tenant = sink.is_some().then(|| envelope.tenant_ctx());
            Some(TraceRecorder::new_with_audit(
                trace_config,
                trace_ctx,
                sink,
                audit_tenant,
            ))
        };

        let mocks = self.mocks.as_deref();
        // Read ONCE, here, from the provider's own envelope — see
        // `crate::caller_identity`. Everything downstream propagates this
        // value; nothing downstream may establish one from a node payload.
        // Cloned rather than borrowed: `payload` is moved into the run below,
        // and the context must outlive that move.
        let caller_block = crate::caller_identity::caller_block(&payload).cloned();
        // Run audit: only when a sink is installed. The wrapper observer
        // forwards to the trace recorder, so trace output is unchanged.
        let turn = self.run_outcome.as_ref().map(|reporter| {
            // A retried attempt of the same turn reuses its first attempt's
            // run rather than minting another (see `run_outcome::turn`).
            let resumed = resume_run.or_else(|| reporter.retried_marker());
            let turn = TurnContext::begin(
                &envelope,
                caller_block.as_ref(),
                resumed,
                effective_flow_id.as_str(),
            );
            reporter.remember(&turn.marker);
            turn
        });
        let outcome_observer = turn.as_ref().map(|_| {
            RunOutcomeObserver::new(
                trace
                    .as_ref()
                    .map(|recorder| recorder as &dyn crate::runner::engine::ExecutionObserver),
            )
        });
        let observer: Option<&dyn crate::runner::engine::ExecutionObserver> =
            match outcome_observer.as_ref() {
                Some(observer) => Some(observer),
                None => trace
                    .as_ref()
                    .map(|recorder| recorder as &dyn crate::runner::engine::ExecutionObserver),
            };
        let ctx = FlowContext {
            tenant: &self.tenant,
            pack_id: effective_pack_id.as_str(),
            flow_id: effective_flow_id.as_str(),
            node_id: None,
            tool: None,
            action: action_owned.as_deref(),
            session_id: Some(session_owned.as_str()),
            provider_id: provider_owned.as_deref(),
            // Carry the inbound reply scope so async-dispatch nodes can encode
            // the originating thread/reply_to into the dispatch correlation id
            // (so a threaded wait can be re-keyed on resume).
            reply_scope: envelope.reply_scope.as_ref(),
            retry_config,
            attempt: 1,
            observer,
            mocks,
            caller: caller_block.as_ref(),
        };

        let execution = if let Some(snapshot) = resume_snapshot {
            let resume_pack_id = snapshot.pack_id.clone();
            let resume_flow_id = snapshot
                .next_flow
                .clone()
                .unwrap_or_else(|| snapshot.flow_id.clone());
            let resume_ctx = FlowContext {
                pack_id: resume_pack_id.as_str(),
                flow_id: resume_flow_id.as_str(),
                ..ctx
            };
            self.engine.resume(resume_ctx, snapshot, payload).await
        } else if let Some(entry) = envelope.entry_node.as_deref() {
            self.engine.execute_from(ctx, payload, entry).await
        } else {
            self.engine.execute(ctx, payload).await
        };
        let execution = match execution {
            Ok(execution) => {
                if let Some(recorder) = trace.as_ref()
                    && let Err(err) = recorder.flush_success()
                {
                    tracing::warn!(error = %err, "failed to write trace");
                }
                execution
            }
            Err(err) => {
                if let Some(recorder) = trace.as_ref()
                    && let Err(write_err) = recorder.flush_error(err.as_ref())
                {
                    tracing::warn!(error = %write_err, "failed to write trace");
                }
                // The error text is read only to pick a class; it is never
                // part of the reported outcome.
                if let (Some(reporter), Some(turn), Some(observer)) = (
                    self.run_outcome.as_ref(),
                    turn.as_ref(),
                    outcome_observer.as_ref(),
                ) {
                    reporter.record_failure(turn.failed(&observer.observed(), &format!("{err:#}")));
                }
                return Err(RunnerError::AdapterCall {
                    // `{:#}` flattens the full anyhow source chain; a bare
                    // `to_string()` keeps only the outermost context and drops
                    // the real root cause (e.g. the underlying LLM/dispatch
                    // error behind "in-process operala dispatch … failed").
                    reason: format!("{err:#}"),
                });
            }
        };

        let FlowExecution {
            output,
            status,
            node_outputs,
        } = execution;
        let observed = outcome_observer
            .as_ref()
            .map(RunOutcomeObserver::observed)
            .unwrap_or_default();
        match status {
            FlowStatus::Completed => {
                if let (Some(reporter), Some(turn)) = (self.run_outcome.as_ref(), turn.as_ref()) {
                    reporter.record(turn.completed(&observed, &output));
                }
                self.resume.clear(&envelope).await?;
                Ok((output, node_outputs))
            }
            FlowStatus::Waiting(wait) => {
                let marker = turn.as_ref().map(|turn| turn.marker_after(&observed));
                let reply_scope = self.resume.save_with_run(&envelope, &wait, marker).await?;
                if let (Some(reporter), Some(turn)) = (self.run_outcome.as_ref(), turn.as_ref()) {
                    reporter.record(turn.waiting(&observed, &wait.snapshot.next_node));
                }
                let outcome = json!({
                    "status": "pending",
                    "reason": wait.reason,
                    "resume": wait.snapshot,
                    "reply_scope": reply_scope,
                    "response": output,
                });
                Ok((outcome, node_outputs))
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IngressEnvelope {
    pub tenant: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pack_id: Option<String>,
    pub flow_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flow_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_hint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    /// Multi-instance messaging endpoint discriminator (M1.4).
    /// Distinguishes provider instances of the same `provider_type` —
    /// e.g. `teams-legal` vs `teams-accounting` — so sessions and traces
    /// don't collide across endpoints that share provider/channel/user.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub messaging_endpoint_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conversation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    /// Flow node this run must START at, lifted from a card-navigation target
    /// that names a node rather than a card asset — see
    /// [`crate::runner::card_nav`]. `None` starts at the flow's entrypoint.
    ///
    /// A parked snapshot still outranks it: resuming a waiting run is never
    /// the same thing as re-entering the flow at a node.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry_node: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activity_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,
    #[serde(default)]
    pub payload: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_scope: Option<ReplyScope>,
}

/// Validate a producer-asserted messaging endpoint id at the runner-host
/// boundary. Mirrors the http-layer validator in
/// `greentic-start::revision_serve::validate_endpoint_id` so the
/// canonicalize-layer `ep=<eid>::<base>` session prefix is safe regardless
/// of how the eid reached the envelope — http header (already validated),
/// embedded callers (`start_embedded_host`), or the upcoming WIT-derived
/// `identify_instance` path. Invalid → drop to None (run unscoped instead
/// of corrupting the namespace prefix).
///
/// The threat the grammar defends against: a value containing `:` collides
/// the prefix delimiter (`eid="a"+base="b::c"` and `eid="a::b"+base="c"`
/// both produce `ep=a::b::c`); empty/whitespace-only values collapse all
/// malformed traffic into one namespace; control characters or unbounded
/// length corrupt downstream session-store keys and telemetry attribute
/// values.
fn endpoint_id_is_valid(raw: &str) -> bool {
    if raw.is_empty() || raw.len() > 128 {
        return false;
    }
    raw.bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
}

impl IngressEnvelope {
    pub fn canonicalize(mut self) -> Self {
        if self.provider.is_none() {
            self.provider = Some("provider".into());
        }
        if self.channel.is_none() {
            self.channel = Some(self.flow_id.clone());
        }
        if self.conversation.is_none() {
            self.conversation = self.channel.clone();
        }
        if self.user.is_none() {
            if let Some(ref hint) = self.session_hint {
                self.user = Some(hint.clone());
            } else if let Some(ref activity) = self.activity_id {
                self.user = Some(activity.clone());
            } else {
                self.user = Some("user".into());
            }
        }
        // Defensive: drop an invalid eid BEFORE it reaches the prefix step
        // or telemetry stamping. See `endpoint_id_is_valid` for why.
        //
        // The drop is silent at the http boundary (greentic-start validates
        // and drops there too — see Codex review note: that path is fronted
        // by request validation, so an invalid eid means the operator never
        // asserted one). Embedded callers and the future WIT-derived
        // `identify_instance` path have no such validator, so a downgrade
        // here usually signals a producer bug. Emit at WARN so operators can
        // spot it, then continue with the same drop-to-None semantics the
        // http layer uses — staying consistent across boundaries.
        if let Some(eid) = self.messaging_endpoint_id.as_deref()
            && !endpoint_id_is_valid(eid)
        {
            tracing::warn!(
                tenant = %self.tenant,
                messaging_endpoint_id = %eid,
                "M1.4: invalid messaging_endpoint_id dropped at runner-host canonicalize \
                 — request will run unscoped (legacy session bucket). \
                 Producer bug or non-HTTP caller bypassing the boundary validator."
            );
            self.messaging_endpoint_id = None;
        }
        // Endpoint isolation: prefix the hint (explicit OR derived) with
        // `ep=<eid>::` so two endpoints reusing the same producer-supplied
        // session key never collide. Idempotent — re-canonicalize is a no-op.
        let base = self
            .session_hint
            .clone()
            .unwrap_or_else(|| self.canonical_session_hint());
        self.session_hint = Some(match &self.messaging_endpoint_id {
            Some(eid) => {
                let prefix = format!("ep={eid}::");
                if base.starts_with(&prefix) {
                    base
                } else {
                    format!("{prefix}{base}")
                }
            }
            None => base,
        });
        if self.reply_scope.is_none()
            && let Some(conversation) = self.conversation.clone()
        {
            self.reply_scope = Some(ReplyScope {
                conversation,
                thread: None,
                reply_to: None,
                correlation: None,
            });
        }
        self
    }

    pub fn canonical_session_hint(&self) -> String {
        // Pre-M1.4 structured form. Endpoint isolation is applied uniformly
        // at the `canonicalize()` layer so explicit producer-supplied hints
        // get the same namespacing as derived ones — keeping this function
        // pure and bytes-identical to pre-M1.4 for `endpoint_id = None`.
        format!(
            "{}:{}:{}:{}:{}",
            self.tenant,
            self.provider.as_deref().unwrap_or("provider"),
            self.channel.as_deref().unwrap_or("channel"),
            self.conversation.as_deref().unwrap_or("conversation"),
            self.user.as_deref().unwrap_or("user")
        )
    }

    pub fn tenant_ctx(&self) -> TenantCtx {
        let env_raw = self.env.clone().unwrap_or_else(|| DEFAULT_ENV.into());
        let env = EnvId::from_str(env_raw.as_str())
            .unwrap_or_else(|_| EnvId::from_str(DEFAULT_ENV).expect("default env must be valid"));
        let tenant_id = TenantId::from_str(self.tenant.as_str()).unwrap_or_else(|_| {
            TenantId::from_str("tenant.default").expect("tenant fallback must be valid")
        });
        let mut ctx = TenantCtx::new(env, tenant_id).with_flow(self.flow_id.clone());
        if let Some(provider) = &self.provider {
            ctx = ctx.with_provider(provider.clone());
        }
        if let Some(session) = &self.session_hint {
            ctx = ctx.with_session(session.clone());
        }
        // M1.4: ride the attributes map so the runner-host local
        // `tenant_ctx_to_telemetry` projection copies it onto
        // `TelemetryCtx::messaging_endpoint_id` for OTel export.
        if let Some(eid) = &self.messaging_endpoint_id {
            ctx.attributes
                .insert(attr_keys::MESSAGING_ENDPOINT_ID.to_string(), eid.clone());
        }
        ctx
    }
}
