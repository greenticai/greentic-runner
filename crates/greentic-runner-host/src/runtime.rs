use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use arc_swap::ArcSwap;
use parking_lot::Mutex;
use reqwest::Client;
use serde_json::Value;
use tokio::runtime::{Handle, Runtime};
use tokio::task::JoinHandle;

use crate::config::HostConfig;
use crate::engine::host::{SessionHost, StateHost};
use crate::engine::runtime::StateMachineRuntime;
use crate::oauth::{OAuthBrokerConfig, request_resource_token};
use crate::operator_metrics::OperatorMetrics;
use crate::operator_registry::OperatorRegistry;
use crate::pack::{ComponentResolution, PackRuntime};
use crate::runner::adapt_events_email::{
    EmailExecutionPlan, EmailSendRequest, build_email_execution_plan, execute_email_request,
};
use crate::runner::contract_cache::{ContractCache, ContractCacheStats};
use crate::runner::engine::FlowEngine;
use crate::runner::mocks::MockLayer;
use crate::secrets::{DynSecretsManager, canonicalize_secret_key, read_secret_blocking};
use crate::storage::session::DynSessionStore;
use crate::storage::state::DynStateStore;
use crate::telemetry::RolloutIds;
use crate::trace::PackTraceInfo;
use crate::wasi::RunnerWasiPolicy;
use greentic_deploy_spec::ids::{BundleId, DeploymentId, RevisionId};
use greentic_types::SecretRequirement;
use runner_core::packs::PackDigest;

const RUNTIME_SECRETS_PACK_ID: &str = "_runner";

/// Key identifying a live runtime in [`ActivePacks`].
///
/// Tenant-only entries use [`RuntimeKey::legacy`] (all id fields `None`);
/// fully-qualified entries use [`RuntimeKey::revision`] (all `Some`). The two
/// forms never collide, so both can coexist in the same map.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RuntimeKey {
    pub tenant: String,
    pub deployment_id: Option<DeploymentId>,
    pub bundle_id: Option<BundleId>,
    pub revision_id: Option<RevisionId>,
}

impl RuntimeKey {
    /// Tenant-only key for the pre-revision-routing path.
    pub fn legacy(tenant: impl Into<String>) -> Self {
        Self {
            tenant: tenant.into(),
            deployment_id: None,
            bundle_id: None,
            revision_id: None,
        }
    }

    /// Fully-qualified key for a specific deployment/bundle/revision.
    pub fn revision(
        tenant: impl Into<String>,
        deployment_id: DeploymentId,
        bundle_id: BundleId,
        revision_id: RevisionId,
    ) -> Self {
        Self {
            tenant: tenant.into(),
            deployment_id: Some(deployment_id),
            bundle_id: Some(bundle_id),
            revision_id: Some(revision_id),
        }
    }

    /// `true` for the tenant-only key produced by [`legacy`](Self::legacy).
    pub fn is_legacy(&self) -> bool {
        self.deployment_id.is_none() && self.bundle_id.is_none() && self.revision_id.is_none()
    }
}

/// Build the next runtime map for a legacy (tenant-only) reload: install the
/// freshly-resolved `legacy` entries and carry over every revision-keyed entry
/// untouched. Revision runtimes are owned by the deployment lifecycle, not the
/// pack watcher, so a tenant-pack reload must not evict them.
fn merge_legacy_reload<V: Clone>(
    prev: &HashMap<RuntimeKey, V>,
    mut legacy: HashMap<RuntimeKey, V>,
) -> HashMap<RuntimeKey, V> {
    for (key, value) in prev {
        if !key.is_legacy() {
            legacy.insert(key.clone(), value.clone());
        }
    }
    legacy
}

/// Pure swap helper for [`ActivePacks::remove_revision`]: clone the prev map
/// minus `key`, return it alongside the removed value. `None` when the key was
/// absent so the caller can skip the `ArcSwap` store. Generic over `V` so the
/// swap logic is testable without standing up a real `TenantRuntime`.
fn remove_keyed_entry<V: Clone>(
    prev: &HashMap<RuntimeKey, V>,
    key: &RuntimeKey,
) -> Option<(HashMap<RuntimeKey, V>, V)> {
    let removed = prev.get(key)?.clone();
    let mut next = prev.clone();
    next.remove(key);
    Some((next, removed))
}

/// Pure swap helper for [`ActivePacks::insert_revision`]: clone the prev map
/// and insert `value` under `key`, returning the next map. Generic over `V` so
/// the swap logic is testable without standing up a real `TenantRuntime`.
fn insert_keyed_entry<V: Clone>(
    prev: &HashMap<RuntimeKey, V>,
    key: RuntimeKey,
    value: V,
) -> HashMap<RuntimeKey, V> {
    let mut next = prev.clone();
    next.insert(key, value);
    next
}

/// Atomically swapped view of live tenant runtimes.
///
/// Reads are lock-free via `ArcSwap`. Mutations serialize on `write_lock` so a
/// read-modify-write swap (e.g. a watcher reload preserving revision entries)
/// cannot interleave with a concurrent insert and clobber the other's update.
pub struct ActivePacks {
    inner: ArcSwap<HashMap<RuntimeKey, Arc<TenantRuntime>>>,
    write_lock: Mutex<()>,
}

impl ActivePacks {
    pub fn new() -> Self {
        Self {
            inner: ArcSwap::from_pointee(HashMap::new()),
            write_lock: Mutex::new(()),
        }
    }

    /// Look up the tenant-only (legacy) runtime. Compatibility helper for the
    /// pre-revision-routing path; see [`load_revision`](Self::load_revision).
    pub fn load_pack(&self, tenant: &str) -> Option<Arc<TenantRuntime>> {
        self.inner.load().get(&RuntimeKey::legacy(tenant)).cloned()
    }

    /// Look up the runtime for a specific deployment/bundle/revision.
    pub fn load_revision(
        &self,
        tenant: &str,
        deployment_id: DeploymentId,
        bundle_id: BundleId,
        revision_id: RevisionId,
    ) -> Option<Arc<TenantRuntime>> {
        self.inner
            .load()
            .get(&RuntimeKey::revision(
                tenant,
                deployment_id,
                bundle_id,
                revision_id,
            ))
            .cloned()
    }

    pub fn snapshot(&self) -> Arc<HashMap<RuntimeKey, Arc<TenantRuntime>>> {
        self.inner.load_full()
    }

    /// Insert (or replace) a single tenant-only runtime, preserving all other
    /// entries — including revision-keyed ones.
    pub fn insert_pack(&self, tenant: &str, runtime: Arc<TenantRuntime>) {
        let _guard = self.write_lock.lock();
        let mut next = (*self.inner.load_full()).clone();
        next.insert(RuntimeKey::legacy(tenant), runtime);
        self.inner.store(Arc::new(next));
    }

    /// Insert (or replace) a single revision-keyed runtime, preserving every
    /// other entry — the tenant-only legacy entry and sibling revisions alike.
    /// This is the producer the deployment warm path calls once a revision's
    /// packs are loaded; the pack watcher's [`replace_legacy`](Self::replace_legacy)
    /// then carries the entry across tenant-pack reloads untouched.
    ///
    /// Fails closed when the runtime's identity does not match the key it would
    /// be stored under: a wiring bug that files one tenant's runtime under
    /// another tenant's revision (breaking isolation) or stores a runtime whose
    /// telemetry reports a different revision than it routes (breaking rollout
    /// attribution) is rejected here rather than silently serving traffic under
    /// the wrong identity. Pairs with [`TenantRuntime::load_revision`], which
    /// derives the runtime's rollout identity from these same ids.
    pub fn insert_revision(
        &self,
        tenant: &str,
        deployment_id: DeploymentId,
        bundle_id: BundleId,
        revision_id: RevisionId,
        runtime: Arc<TenantRuntime>,
    ) -> Result<()> {
        if runtime.tenant() != tenant {
            bail!(
                "revision runtime tenant `{}` does not match key tenant `{tenant}`",
                runtime.tenant()
            );
        }
        let ids = runtime.engine().rollout_ids();
        let key_deployment = deployment_id.to_string();
        let key_bundle = bundle_id.as_str();
        let key_revision = revision_id.to_string();
        if ids.deployment_id.as_deref() != Some(key_deployment.as_str())
            || ids.bundle_id.as_deref() != Some(key_bundle)
            || ids.revision_id.as_deref() != Some(key_revision.as_str())
        {
            bail!(
                "revision runtime rollout identity (deployment={:?}, bundle={:?}, revision={:?}) \
                 does not match key (deployment=`{key_deployment}`, bundle=`{key_bundle}`, \
                 revision=`{key_revision}`)",
                ids.deployment_id,
                ids.bundle_id,
                ids.revision_id
            );
        }
        let _guard = self.write_lock.lock();
        let key = RuntimeKey::revision(tenant, deployment_id, bundle_id, revision_id);
        let next = insert_keyed_entry(&self.inner.load_full(), key, runtime);
        self.inner.store(Arc::new(next));
        Ok(())
    }

    /// Swap in a freshly-resolved set of tenant-only (legacy) runtimes while
    /// carrying over every revision-keyed entry. Used by the pack watcher, whose
    /// index is authoritative for tenant packs but not for deployment revisions.
    pub fn replace_legacy(&self, legacy: HashMap<RuntimeKey, Arc<TenantRuntime>>) {
        let _guard = self.write_lock.lock();
        let prev = self.inner.load_full();
        let next = merge_legacy_reload(&prev, legacy);
        self.inner.store(Arc::new(next));
    }

    /// Remove and return the runtime for a single revision-keyed entry,
    /// preserving every other entry (legacy and revision alike). Used by the
    /// drain coordinator (`gtc op revisions drain`) after the drain window
    /// closes to tear down exactly the one revision being retired. `None` if
    /// no such entry was present — idempotent for safe re-runs.
    pub fn remove_revision(
        &self,
        tenant: &str,
        deployment_id: DeploymentId,
        bundle_id: BundleId,
        revision_id: RevisionId,
    ) -> Option<Arc<TenantRuntime>> {
        let _guard = self.write_lock.lock();
        let prev = self.inner.load_full();
        let key = RuntimeKey::revision(tenant, deployment_id, bundle_id, revision_id);
        let (next, removed) = remove_keyed_entry(&prev, &key)?;
        self.inner.store(Arc::new(next));
        Some(removed)
    }

    /// Replace the entire map, dropping every entry (legacy and revision alike).
    /// Used for full host stop.
    pub fn replace(&self, next: HashMap<RuntimeKey, Arc<TenantRuntime>>) {
        let _guard = self.write_lock.lock();
        self.inner.store(Arc::new(next));
    }

    pub fn len(&self) -> usize {
        self.inner.load().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for ActivePacks {
    fn default() -> Self {
        Self::new()
    }
}

/// Runtime bundle for a tenant pack.
pub struct TenantRuntime {
    tenant: String,
    config: Arc<HostConfig>,
    packs: Vec<Arc<PackRuntime>>,
    digests: Vec<Option<String>>,
    engine: Arc<FlowEngine>,
    state_machine: Arc<StateMachineRuntime>,
    /// The session store this runtime was loaded with. Shared with the inner
    /// state machine — re-exposed here so callers outside the flow run loop
    /// (M1.5 welcome-flow first-contact probe) can query the SAME bucket the
    /// state machine will read/write. For revision mode each revision passes
    /// its own store into `load_revision`; querying the RunnerHost-level
    /// store would key the wrong bucket.
    session_store: DynSessionStore,
    http_client: Client,
    mocks: Option<Arc<MockLayer>>,
    timer_handles: Mutex<Vec<JoinHandle<()>>>,
    secrets: DynSecretsManager,
    operator_registry: OperatorRegistry,
    operator_metrics: Arc<OperatorMetrics>,
    contract_cache: ContractCache,
}

#[derive(Clone)]
pub struct ResolvedComponent {
    pub digest: String,
    pub component_ref: String,
    pub pack: Arc<PackRuntime>,
}

/// One pinned pack of a deployment revision: the on-disk path plus the
/// `algo:value` content digest the deployment staged it under.
/// [`TenantRuntime::load_revision`] fails closed when the file no longer
/// matches the digest, defending the stage→warm window against a swapped or
/// stale cache path.
#[derive(Clone, Debug)]
pub struct RevisionPackRef {
    pub path: PathBuf,
    pub digest: String,
}

/// Block on a future whether or not we're already inside a tokio runtime.
pub fn block_on<F: Future<Output = R>, R>(future: F) -> R {
    if let Ok(handle) = Handle::try_current() {
        handle.block_on(future)
    } else {
        Runtime::new()
            .expect("failed to create tokio runtime")
            .block_on(future)
    }
}

impl TenantRuntime {
    #[allow(clippy::too_many_arguments)]
    pub async fn load(
        pack_path: &Path,
        config: Arc<HostConfig>,
        mocks: Option<Arc<MockLayer>>,
        archive_source: Option<&Path>,
        digest: Option<String>,
        wasi_policy: Arc<RunnerWasiPolicy>,
        session_host: Arc<dyn SessionHost>,
        session_store: DynSessionStore,
        state_store: DynStateStore,
        state_host: Arc<dyn StateHost>,
        secrets_manager: DynSecretsManager,
        #[cfg(feature = "agentic-worker")] ext_llm_port: Option<crate::host::ExtLlmPort>,
        #[cfg(feature = "agentic-worker")] mcp_source: Option<crate::host::McpSource>,
        #[cfg(feature = "agentic-worker")] stream_observers: Option<
            crate::http::agent_stream::StreamObserverRegistry,
        >,
    ) -> Result<Arc<Self>> {
        let pack = Self::load_pack_runtime(
            pack_path,
            &config,
            mocks.clone(),
            archive_source,
            &wasi_policy,
            &session_store,
            &state_store,
            &secrets_manager,
            &BTreeMap::new(),
            &BTreeMap::new(),
            None,
            // Tenant-only path: no deployed unit, so pack secrets resolve the
            // bare pack scope exactly as before.
            None,
        )
        .await?;
        Self::from_packs(
            config,
            vec![(pack, digest)],
            mocks,
            session_host,
            session_store,
            state_store,
            state_host,
            secrets_manager,
            #[cfg(feature = "agentic-worker")]
            ext_llm_port,
            #[cfg(feature = "agentic-worker")]
            mcp_source,
            #[cfg(feature = "agentic-worker")]
            stream_observers,
        )
        .await
    }

    /// Build a revision-keyed runtime from its pinned pack list (the resolved
    /// `pack_list` of a deployment revision). The first entry is the main pack;
    /// the rest are overlays.
    ///
    /// Fails closed if any pack file no longer matches the digest the
    /// deployment pinned it under — defending the stage→warm window against a
    /// swapped or stale cache path. The verified digests are threaded into the
    /// runtime so admin status, traces, and contract hashes report the real
    /// content (parity with the legacy index path). The rollout telemetry
    /// identity is **derived from** `deployment_id` / `bundle_id` /
    /// `revision_id` / `customer_id`, so the engine's attribution cannot drift
    /// from the key the runtime is later inserted under.
    #[allow(clippy::too_many_arguments)]
    pub async fn load_revision(
        pack_refs: &[RevisionPackRef],
        config: Arc<HostConfig>,
        mocks: Option<Arc<MockLayer>>,
        wasi_policy: Arc<RunnerWasiPolicy>,
        session_host: Arc<dyn SessionHost>,
        session_store: DynSessionStore,
        state_store: DynStateStore,
        state_host: Arc<dyn StateHost>,
        secrets_manager: DynSecretsManager,
        deployment_id: DeploymentId,
        bundle_id: BundleId,
        revision_id: RevisionId,
        customer_id: Option<String>,
        runtime_configs_by_pack_id: &BTreeMap<String, Arc<BTreeMap<String, Value>>>,
        runtime_refs_by_pack_id: &BTreeMap<String, Arc<BTreeMap<String, String>>>,
        runtime_ref_resolver: Option<Arc<dyn crate::runtime_refs::RuntimeRefResolver>>,
    ) -> Result<Arc<Self>> {
        if pack_refs.is_empty() {
            bail!(
                "revision runtime for tenant {} requires at least one pack",
                config.tenant
            );
        }
        let mut packs = Vec::with_capacity(pack_refs.len());
        let mut seen_pack_ids = HashSet::with_capacity(pack_refs.len());
        for pack_ref in pack_refs {
            let expected = PackDigest::parse(&pack_ref.digest).with_context(|| {
                format!(
                    "revision pack `{}` has an invalid digest `{}`",
                    pack_ref.path.display(),
                    pack_ref.digest
                )
            })?;
            // `matches_file` always hashes with SHA-256, so a digest pinned under
            // any other algorithm could never match — reject it with a clear
            // message rather than the misleading "does not match" below.
            if expected.algorithm() != "sha256" {
                bail!(
                    "revision pack `{}` pins unsupported digest algorithm `{}`; only sha256 is supported",
                    pack_ref.path.display(),
                    expected.algorithm()
                );
            }
            if !expected.matches_file(&pack_ref.path).with_context(|| {
                format!(
                    "hashing revision pack `{}` for digest verification",
                    pack_ref.path.display()
                )
            })? {
                bail!(
                    "revision pack `{}` does not match pinned digest `{}`",
                    pack_ref.path.display(),
                    pack_ref.digest
                );
            }
            let pack = Self::load_pack_runtime(
                &pack_ref.path,
                &config,
                mocks.clone(),
                None,
                &wasi_policy,
                &session_store,
                &state_store,
                &secrets_manager,
                runtime_configs_by_pack_id,
                runtime_refs_by_pack_id,
                runtime_ref_resolver.as_ref(),
                // The unit every extension credential in this pack is scoped
                // to. Same value `FlowEngine::mcp_credential_unit` reads off
                // `rollout_ids.bundle_id` below, so the flow-node MCP lane and
                // the component lane cannot disagree about which unit is running.
                Some(bundle_id.as_str()),
            )
            .await?;
            // Reject duplicate pack_id within a single revision — two refs
            // resolving to the same pack_id would silently share config/routing
            // entries and produce an ambiguous runtime.
            let pack_id = pack.metadata().pack_id.clone();
            if !seen_pack_ids.insert(pack_id.clone()) {
                bail!(
                    "revision for tenant {} contains duplicate pack_id `{}` (path `{}`)",
                    config.tenant,
                    pack_id,
                    pack_ref.path.display(),
                );
            }
            packs.push((pack, Some(expected.raw_string())));
        }
        let rollout = RolloutIds {
            customer_id,
            deployment_id: Some(deployment_id.to_string()),
            bundle_id: Some(bundle_id.as_str().to_string()),
            revision_id: Some(revision_id.to_string()),
        };
        Self::from_packs_with_rollout(
            config,
            packs,
            mocks,
            session_host,
            session_store,
            state_store,
            state_host,
            secrets_manager,
            // No host-injected ports on the revision-keyed path: it is
            // constructed from a pinned pack list rather than from a
            // `RunnerHost`, so there is none to read them from. Matches what
            // `ext_llm_port` already does here — an engine built this way
            // falls back to env for both.
            #[cfg(feature = "agentic-worker")]
            None,
            #[cfg(feature = "agentic-worker")]
            None,
            #[cfg(feature = "agentic-worker")]
            None,
            rollout,
        )
        .await
    }

    /// Load a single [`PackRuntime`] from a path, sharing the tenant's session /
    /// state / secrets backends. Shared by [`load`](Self::load) (one pack) and
    /// [`load_revision`](Self::load_revision) (the revision's pack list).
    ///
    /// `runtime_configs_by_pack_id` is consulted AFTER the pack loads (the
    /// `pack_id` is only known after the manifest read) and BEFORE the
    /// `Arc<PackRuntime>` is created. A matching entry is injected via
    /// [`PackRuntime::set_runtime_config_non_secret`] so the C4.3 producer
    /// plumbing requires no post-hoc `Arc::get_mut` dance — the single-pack
    /// [`load`](Self::load) path just passes an empty map.
    ///
    /// `runtime_refs_by_pack_id` mirrors the same shape for the C5
    /// `pack-config.v1.runtime_refs` channel; a matching entry is injected
    /// via [`PackRuntime::set_runtime_refs`] alongside `runtime_ref_resolver`.
    ///
    /// `unit_id` is the deployed unit this pack belongs to — the revision's
    /// `bundle_id` on the [`load_revision`](Self::load_revision) path, `None`
    /// on the legacy tenant-only [`load`](Self::load) path. It scopes every
    /// extension credential the pack's components read, so two units of the
    /// SAME pack in one environment can hold different values. It is taken as
    /// a parameter rather than read from a process environment variable
    /// because one greentic-start process serves every revision of an
    /// environment, so an env var could not differ per unit.
    #[allow(clippy::too_many_arguments)]
    async fn load_pack_runtime(
        pack_path: &Path,
        config: &Arc<HostConfig>,
        mocks: Option<Arc<MockLayer>>,
        archive_source: Option<&Path>,
        wasi_policy: &Arc<RunnerWasiPolicy>,
        session_store: &DynSessionStore,
        state_store: &DynStateStore,
        secrets_manager: &DynSecretsManager,
        runtime_configs_by_pack_id: &BTreeMap<String, Arc<BTreeMap<String, Value>>>,
        runtime_refs_by_pack_id: &BTreeMap<String, Arc<BTreeMap<String, String>>>,
        runtime_ref_resolver: Option<&Arc<dyn crate::runtime_refs::RuntimeRefResolver>>,
        unit_id: Option<&str>,
    ) -> Result<Arc<PackRuntime>> {
        let oauth_config = config.oauth_broker_config();
        let mut pack = PackRuntime::load(
            pack_path,
            Arc::clone(config),
            mocks,
            archive_source,
            Some(Arc::clone(session_store)),
            Some(Arc::clone(state_store)),
            Arc::clone(wasi_policy),
            Arc::clone(secrets_manager),
            oauth_config,
            true,
            ComponentResolution::default(),
        )
        .await
        .with_context(|| {
            format!(
                "failed to load pack {} for tenant {}",
                pack_path.display(),
                config.tenant
            )
        })?;
        let pack_id = pack.metadata().pack_id.clone();
        if let Some(non_secret) = runtime_configs_by_pack_id.get(pack_id.as_str()) {
            pack.set_runtime_config_non_secret(Some(Arc::clone(non_secret)));
        }
        pack.set_unit_id(unit_id.map(str::to_string));
        if let Some(refs) = runtime_refs_by_pack_id.get(pack_id.as_str()) {
            let resolver = runtime_ref_resolver.ok_or_else(|| {
                anyhow!(
                    "pack `{}` has runtime_refs bound but no RuntimeRefResolver was provided",
                    pack_id,
                )
            })?;
            pack.set_runtime_refs(Some(crate::runtime_refs::RuntimeRefsInjection {
                refs: Arc::clone(refs),
                resolver: Arc::clone(resolver),
            }));
        }
        Ok(Arc::new(pack))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn from_packs(
        config: Arc<HostConfig>,
        packs: Vec<(Arc<PackRuntime>, Option<String>)>,
        mocks: Option<Arc<MockLayer>>,
        session_host: Arc<dyn SessionHost>,
        session_store: DynSessionStore,
        state_store: DynStateStore,
        state_host: Arc<dyn StateHost>,
        secrets_manager: DynSecretsManager,
        #[cfg(feature = "agentic-worker")] ext_llm_port: Option<crate::host::ExtLlmPort>,
        #[cfg(feature = "agentic-worker")] mcp_source: Option<crate::host::McpSource>,
        #[cfg(feature = "agentic-worker")] stream_observers: Option<
            crate::http::agent_stream::StreamObserverRegistry,
        >,
    ) -> Result<Arc<Self>> {
        Self::from_packs_with_rollout(
            config,
            packs,
            mocks,
            session_host,
            session_store,
            state_store,
            state_host,
            secrets_manager,
            #[cfg(feature = "agentic-worker")]
            ext_llm_port,
            #[cfg(feature = "agentic-worker")]
            mcp_source,
            #[cfg(feature = "agentic-worker")]
            stream_observers,
            RolloutIds::default(),
        )
        .await
    }

    /// Like [`from_packs`](Self::from_packs) but stamps `rollout` onto the flow
    /// engine so every span this runtime emits carries the deployment / bundle /
    /// revision / customer identity. [`from_packs`](Self::from_packs) is the
    /// legacy (tenant-only) path and passes [`RolloutIds::default`].
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn from_packs_with_rollout(
        config: Arc<HostConfig>,
        packs: Vec<(Arc<PackRuntime>, Option<String>)>,
        mocks: Option<Arc<MockLayer>>,
        session_host: Arc<dyn SessionHost>,
        session_store: DynSessionStore,
        _state_store: DynStateStore,
        state_host: Arc<dyn StateHost>,
        secrets_manager: DynSecretsManager,
        #[cfg(feature = "agentic-worker")] ext_llm_port: Option<crate::host::ExtLlmPort>,
        #[cfg(feature = "agentic-worker")] mcp_source: Option<crate::host::McpSource>,
        #[cfg(feature = "agentic-worker")] stream_observers: Option<
            crate::http::agent_stream::StreamObserverRegistry,
        >,
        rollout: RolloutIds,
    ) -> Result<Arc<Self>> {
        let operator_registry = OperatorRegistry::build(&packs)?;
        let operator_metrics = Arc::new(OperatorMetrics::default());
        let pack_runtimes = packs
            .iter()
            .map(|(pack, _)| Arc::clone(pack))
            .collect::<Vec<_>>();
        let digests = packs
            .iter()
            .map(|(_, digest)| digest.clone())
            .collect::<Vec<_>>();
        let mut pack_trace = HashMap::new();
        for (pack, digest) in &packs {
            let pack_id = pack.metadata().pack_id.clone();
            let pack_ref = config
                .pack_bindings
                .iter()
                .find(|binding| binding.pack_id == pack_id)
                .map(|binding| binding.pack_ref.clone())
                .unwrap_or_else(|| pack_id.clone());
            pack_trace.insert(
                pack_id,
                PackTraceInfo {
                    pack_ref,
                    resolved_digest: digest.clone(),
                },
            );
        }
        // The deployed unit's identity, used as the `project_id` billing
        // dimension for this runtime's agentic-worker spend. It is exactly what
        // greentic-designer records as `pack_name`, so authoring-time and
        // runtime spend join on it. Captured BEFORE `rollout` moves into the
        // engine; `None` on the legacy tenant-only path (no bundle is pinned
        // there), which makes billing omit the dimension rather than fall back
        // to a non-unique in-pack agent id.
        #[cfg(feature = "agentic-worker")]
        let agent_project_id = rollout.bundle_id.clone();
        let engine = FlowEngine::new(pack_runtimes.clone(), Arc::clone(&config))
            .await
            .context("failed to prime flow engine")?
            .with_rollout_ids(rollout);
        // A host-injected MCP source replaces the one `FlowEngine::new` derives
        // from env; `None` leaves that env-derived source alone, so a
        // standalone runner is unaffected.
        #[cfg(feature = "agentic-worker")]
        let engine = engine.with_mcp_source(mcp_source);
        // An `mcp` flow node resolves its credential through the manager this
        // runtime already holds — dev-store in a Cloud Run or Kubernetes
        // deployment, which is the only backend there that can read a
        // `secrets://` URI. Without this the node built its own manager from
        // `SECRETS_BACKEND`, which no remote deployment sets, and every lookup
        // missed. An explicit `SECRETS_BACKEND` still wins; see
        // `mcp_node::aw::choose_mcp_secrets`.
        #[cfg(feature = "agentic-worker")]
        let engine = engine.with_mcp_secrets(Some(Arc::clone(&secrets_manager)));
        #[cfg_attr(not(feature = "agentic-worker"), allow(unused_mut))]
        let mut engine = engine;

        // Wire Sorla remote-dispatch (NATS) into the flow engine BEFORE it is
        // moved behind an `Arc`. `set_remote_dispatch_handler` takes `&mut self`,
        // so the dispatcher must be attached while `engine` is still owned and
        // mutable. The response listener (which needs the post-build ingress
        // handle) is spawned further below, after the runtime is constructed.
        //
        // Gated on `GREENTIC_EVENTS_NATS_URL`: when unset, `sorla.call` stays
        // disabled and existing behaviour is unchanged. When set but NATS cannot
        // be reached we log a warning and continue (the engine simply has no
        // dispatch handler, so `sorla.call` nodes fail fast at execution time).
        //
        // Connected here (BEFORE the agentic-worker block below) rather than
        // its original post-block position so the agent-node handler
        // construction below can also thread the (possibly connected) client
        // into an `AuditSink` for `dw.agent` step audit events (EPIC-B B-3);
        // pure reordering, no behaviour change to the dispatch wiring itself.
        let dispatch_nats_client = match std::env::var("GREENTIC_EVENTS_NATS_URL") {
            Ok(nats_url) => match async_nats::connect(&nats_url).await {
                Ok(client) => {
                    engine.set_remote_dispatch_handler(Arc::new(
                        crate::runner::remote_dispatch::NatsDispatcher::new(client.clone()),
                    ));
                    Some(client)
                }
                Err(error) => {
                    tracing::warn!(
                        %error,
                        "GREENTIC_EVENTS_NATS_URL set but NATS connect failed; sorla.call disabled"
                    );
                    None
                }
            },
            Err(_) => None,
        };

        // Clone the (possibly connected) client for the audit sink (EPIC-B
        // B-2/B-3): threaded into `StateMachineRuntime::from_flow_engine` so
        // `TraceRecorder` can publish best-effort audit events over NATS, and
        // (as an `AuditSink`) into the `dw.agent` node handler below so agent
        // tool-call/tool-result steps are audited too. Cloned BEFORE the
        // response-listener loop further below moves `dispatch_nats_client`.
        // `None` when NATS is unset/unreachable, which keeps both audit paths
        // off by default (zero behaviour change).
        let audit_nats_client = dispatch_nats_client.clone();

        #[cfg(feature = "agentic-worker")]
        {
            use crate::runner::agent_node::{
                agent_configs_from_manifest, merge_agent_sources, merge_sidecar_into,
            };
            use std::collections::HashMap;

            // Collect agent configs from all New-manifest packs. When the same
            // agent_id appears in multiple packs the last pack wins (packs are
            // ordered: first = primary, rest = overlays). Collisions are logged
            // so operators can audit cross-pack conflicts.
            let mut pack_agents: HashMap<String, greentic_aw_runtime::AgentConfig> = HashMap::new();
            for pack in &pack_runtimes {
                let mut blobs = pack.manifest_agent_blobs();
                // Bridge: designer-built packs cannot populate `manifest.agents`
                // (old greentic-pack); they embed a `dw-agents.json` sidecar.
                // Fill any agent_id the manifest lacked (manifest stays authoritative).
                merge_sidecar_into(&mut blobs, pack.dw_agents_sidecar_blobs());
                if blobs.is_empty() {
                    continue;
                }
                let pack_id = pack.metadata().pack_id.clone();
                let configs = agent_configs_from_manifest(&pack_id, &blobs);
                for (agent_id, agent_config) in configs {
                    if let Some(existing) = pack_agents.get(&agent_id) {
                        tracing::warn!(
                            agent_id,
                            prior_pack = existing.agent_id.as_str(),
                            new_pack = pack_id.as_str(),
                            "agent_id collision across packs; last pack wins"
                        );
                    }
                    pack_agents.insert(agent_id, agent_config);
                }
            }

            // Operator config overrides pack-provided agents on collision.
            let merged_agents = merge_agent_sources(pack_agents, config.agents.clone());

            // First-boot ingest of any pack-baked knowledge corpus (W4 4c). Runs
            // BEFORE the agent runtime mounts its serving knowledge connection:
            // embedded SurrealDB allows one handle per store directory, so the
            // temporary ingest connection must open and drop before the serving
            // mount (inside build_agent_node_handler) opens its own. No-op without
            // the `knowledge-chronicle` feature or when no pack carries a corpus.
            #[cfg(feature = "knowledge-chronicle")]
            {
                // The expectation comes from the same config `ingest_corpus` will
                // build its embedder from, so precomputed vectors are validated
                // against the embedder that actually runs.
                let expectation = crate::runner::knowledge_mount::embedding_expectation();
                let corpus = crate::runner::knowledge_corpus::collect(
                    &pack_runtimes,
                    expectation.as_ref().map(|(model, dim)| {
                        crate::runner::knowledge_corpus::EmbeddingExpectation { model, dim: *dim }
                    }),
                );
                crate::runner::knowledge_mount::ingest_corpus(&config.tenant_ctx(), corpus).await;
            }

            // DwAgent state-store selection. With GREENTIC_AW_REDIS_URL set, use the
            // Redis-backed stores (production multi-process default). Without it, when
            // built with `desktop-agent-ephemeral`, fall back to the process-global
            // in-memory stores so a single-process runner (e.g. the designer's
            // loopback test-chat sidecar) runs agentic-worker turns with NO external
            // infra. Otherwise DwAgent nodes stay disabled — unchanged server
            // behaviour (build_agent_node_handler returns None when Redis is unset).
            let redis_set = std::env::var("GREENTIC_AW_REDIS_URL")
                .map(|v| !v.is_empty())
                .unwrap_or(false);
            // Best-effort agent-step audit sink (EPIC-B B-3), built from the
            // same (possibly connected) NATS client the flow-level audit sink
            // (B-2) uses. `None` when NATS is unset/unreachable, which keeps
            // `dw.agent` execution on the plain `AgentRuntime::step` path
            // (zero behaviour change).
            let agent_audit_sink = audit_nats_client
                .clone()
                .map(crate::trace::audit_sink::AuditSink::new);
            // `merged_agents` is MOVED into `build_agent_node_handler` below
            // (redis/ephemeral branches); clone for the graph handler first so
            // it can resolve a graph node's `agent_ref` (SP1) against the same
            // process-level merged config map.
            let graph_agents = merged_agents.clone();
            // Also needed after `merged_agents` is moved into
            // `build_agent_node_handler`/`_ephemeral` below, to resolve the
            // in-process operala.call LLM key the same way (see the
            // `operala-in-process` block after the DwAgent wiring).
            #[cfg(feature = "operala-in-process")]
            let operala_agents = merged_agents.clone();
            let agent_handler = if redis_set {
                crate::runner::agent_node::build_agent_node_handler(
                    merged_agents,
                    config.tenant.clone(),
                    Arc::clone(&secrets_manager),
                    ext_llm_port.clone(),
                    pack_runtimes.clone(),
                    agent_audit_sink.clone(),
                    stream_observers.clone(),
                    agent_project_id.clone(),
                )
                .await
            } else {
                #[cfg(feature = "desktop-agent-ephemeral")]
                {
                    crate::runner::agent_node::build_agent_node_handler_ephemeral(
                        merged_agents,
                        config.tenant.clone(),
                        Arc::clone(&secrets_manager),
                        ext_llm_port.clone(),
                        pack_runtimes.clone(),
                        agent_audit_sink.clone(),
                        stream_observers.clone(),
                        agent_project_id.clone(),
                    )
                    .await
                }
                #[cfg(not(feature = "desktop-agent-ephemeral"))]
                {
                    crate::runner::agent_node::build_agent_node_handler(
                        merged_agents,
                        config.tenant.clone(),
                        Arc::clone(&secrets_manager),
                        ext_llm_port.clone(),
                        pack_runtimes.clone(),
                        agent_audit_sink.clone(),
                        stream_observers.clone(),
                        agent_project_id.clone(),
                    )
                    .await
                }
            };
            if let Some(handler) = agent_handler {
                engine.set_agent_node_handler(handler);
                tracing::info!("DwAgent runtime wired into FlowEngine");
            }

            // In-process deep-worker runtime for `operala.call` nodes
            // (`operala-in-process`; `desktop-agent-ephemeral` implies it, so
            // the designer's offline Test-chat sidecar keeps it). Reuses the
            // exact key-resolution policy the
            // in-process dw.agent LLM backend uses (env key wins; otherwise
            // the first agent's `llm.credential_ref` resolved from the
            // per-tenant secrets store), then builds a
            // `greentic_llm::RigBackend` — the same OpenAI-compatible,
            // multi-provider client `GreenticLlmBackend` uses for dw.agent —
            // and wraps it in `DeepWorkerInvoker`. No handler is wired (and
            // `operala.call` falls back to the NATS `RemoteDispatchHandler`,
            // failing without it) when no LLM key resolves or the provider
            // fails to build.
            #[cfg(feature = "operala-in-process")]
            {
                use crate::runner::operala_node::{
                    OPERALA_DISPATCH_ENV, OperalaSelection, select_operala_handler,
                };

                // Provider/model are resolved PER WORKER from each
                // `operala.call` node's `input.llm` binding (stamped by
                // greentic-dw-authoring for authored deep-worker packs); the
                // handler builds the LLM per dispatch. For a node that carries
                // NO `input.llm` — e.g. an operala.call synthesized at runtime
                // for a dw.agent's chronicle knowledge/memory retrieval — fall
                // back to the agent's OWN configured provider/model (the same
                // `operala_agents` the api_key is resolved from). The
                // process-level env still OVERRIDES both. Only when neither the
                // node, the agent config, nor the env carries a provider/model
                // does the dispatch error explicitly.
                let agent_llm = operala_agents
                    .values()
                    .map(|agent| &agent.llm)
                    .find(|llm| !llm.provider.trim().is_empty() && !llm.model.trim().is_empty());
                let fallback_provider = std::env::var("GREENTIC_LLM_PROVIDER")
                    .ok()
                    .filter(|value| !value.trim().is_empty())
                    .or_else(|| agent_llm.map(|llm| llm.provider.clone()));
                let fallback_model = std::env::var("GREENTIC_LLM_MODEL")
                    .ok()
                    .filter(|value| !value.trim().is_empty())
                    .or_else(|| agent_llm.map(|llm| llm.model.clone()));
                let base_url = std::env::var("GREENTIC_LLM_BASE_URL")
                    .ok()
                    .filter(|value| !value.trim().is_empty());

                // Same key policy the in-process dw.agent backend uses: env key
                // wins; otherwise the first agent's `llm.credential_ref` from the
                // per-tenant secrets store.
                let key_secrets = &secrets_manager;
                let key_tenant = config.tenant.as_str();
                let key_agents = &operala_agents;
                let resolve_key = move || async move {
                    let env_key = std::env::var("GREENTIC_LLM_API_KEY")
                        .ok()
                        .filter(|value| !value.trim().is_empty())
                        .or_else(|| {
                            std::env::var("OPENAI_API_KEY")
                                .ok()
                                .filter(|value| !value.trim().is_empty())
                        });
                    match env_key {
                        Some(key) => Some(key),
                        None => {
                            crate::runner::agent_node::resolve_in_process_llm_key(
                                key_secrets,
                                key_tenant,
                                key_agents,
                            )
                            .await
                        }
                    }
                };

                let dispatch_env = std::env::var(OPERALA_DISPATCH_ENV).ok();
                match select_operala_handler(
                    dispatch_env.as_deref(),
                    resolve_key,
                    base_url,
                    fallback_provider,
                    fallback_model,
                )
                .await
                {
                    OperalaSelection::InProcess(handler) => {
                        engine.set_operala_node_handler(handler);
                        tracing::info!(
                            dispatch = "in-process",
                            "operala.call in-process deep-worker runtime wired into FlowEngine \
                             (provider/model resolved per-worker from node input.llm)"
                        );
                    }
                    OperalaSelection::Nats => {
                        tracing::info!(
                            dispatch = "nats",
                            "GREENTIC_OPERALA_DISPATCH=nats: operala.call dispatches over NATS; \
                             in-process deep-worker handler not wired"
                        );
                        if std::env::var("GREENTIC_EVENTS_NATS_URL")
                            .ok()
                            .filter(|value| !value.trim().is_empty())
                            .is_none()
                        {
                            tracing::warn!(
                                "GREENTIC_OPERALA_DISPATCH=nats but GREENTIC_EVENTS_NATS_URL is \
                                 unset; operala.call nodes will fail (no remote dispatch \
                                 handler). Set the NATS URL or unset the flag."
                            );
                        }
                    }
                    OperalaSelection::NoKey => {
                        tracing::warn!(
                            dispatch = "nats",
                            "no LLM API key resolved (env or store) for operala.call \
                             in-process wiring; operala.call nodes will fall back to NATS \
                             (and fail without it)"
                        );
                    }
                }
            }

            // Collect agent-graph sidecars from each pack. Unlike agents (a
            // manifest.cbor map), graphs arrive as a pack FILE (`agent-graph.json`)
            // — one sidecar per pack — so the graph is keyed by `pack_id`. A
            // sidecar that fails UTF-8 / JSON / schema validation is logged and
            // skipped (lenient, mirroring `agent_configs_from_manifest`) so a bad
            // graph never blocks the rest of pack loading. When the same pack_id
            // appears twice (overlay), the last pack wins.
            let mut graphs: HashMap<String, greentic_aw_runtime::graph::GraphConfig> =
                HashMap::new();
            for pack in &pack_runtimes {
                let Some(bytes) = pack.read_agent_graph_sidecar() else {
                    continue;
                };
                let pack_id = pack.metadata().pack_id.clone();
                if let Some(config) =
                    crate::runner::graph_node::graph_config_from_sidecar(&pack_id, &bytes)
                    && graphs.insert(pack_id.clone(), config).is_some()
                {
                    tracing::warn!(
                        pack_id = pack_id.as_str(),
                        "agent-graph sidecar for pack_id seen twice; last pack wins"
                    );
                }
            }

            // Producer/operator-declared graphs (HostConfig.graphs) override
            // pack-sidecar graphs on `graph_id` collision — mirroring the
            // operator-wins merge for agents.
            for (graph_id, graph_config) in config.graphs.clone() {
                graphs.insert(graph_id, graph_config);
            }

            // Tenant, secrets manager and deployed unit are the same three
            // values the `dw.agent` handler above receives, so a graph turn's
            // `a2a:` tools resolve the same credentials a single-turn worker's
            // would.
            if let Some(handler) = crate::runner::graph_node::build_graph_node_handler(
                graphs,
                agent_audit_sink.clone(),
                Arc::new(pack_runtimes.clone()),
                graph_agents,
                config.tenant.clone(),
                Arc::clone(&secrets_manager),
                agent_project_id.clone(),
            )
            .await
            {
                engine.set_graph_node_handler(handler);
                tracing::info!("DwAgentGraph runtime wired into FlowEngine");
            }
        }

        // Resolve how `dw.agent` nodes dispatch. When `GREENTIC_AW_DISPATCH=nats`
        // is set, the node is rerouted over the durable agentic NATS path instead
        // of the in-process handler. Must be wired while `engine` is still owned.
        #[cfg(feature = "agentic-worker")]
        {
            let dw_dispatch =
                crate::runner::agent_node::dw_agent_dispatch_mode(|k| std::env::var(k).ok());
            engine.set_dw_agent_dispatch(dw_dispatch);
            if matches!(
                dw_dispatch,
                crate::runner::agent_node::DwAgentDispatch::Nats
            ) && std::env::var("GREENTIC_EVENTS_NATS_URL")
                .ok()
                .filter(|s| !s.is_empty())
                .is_none()
            {
                tracing::warn!(
                    "GREENTIC_AW_DISPATCH=nats but GREENTIC_EVENTS_NATS_URL is unset; \
                     dw.agent nodes will fail (no remote dispatch handler). \
                     Set the NATS URL or unset the flag."
                );
            }
        }

        let engine = Arc::new(engine);
        let state_machine = Arc::new(
            StateMachineRuntime::from_flow_engine(
                Arc::clone(&config),
                Arc::clone(&engine),
                pack_trace,
                session_host,
                Arc::clone(&session_store),
                state_host,
                Arc::clone(&secrets_manager),
                mocks.clone(),
                audit_nats_client,
            )
            .context("failed to initialise state machine runtime")?,
        );

        // Spawn the response listeners now that the ingress handle
        // (`state_machine`) exists. Each listener resumes paused flows by feeding
        // a synthesized ingress envelope through `StateMachineRuntime::handle`
        // (see `RuntimeSessionResumer`). The resumer is runtime-agnostic (it
        // resumes by correlation id), so one shared resumer serves all runtimes;
        // we run one listener per runtime so every `*.call` node's responses
        // (`greentic.<runtime>.response.v1`) are consumed. Only started when the
        // dispatcher above connected successfully.
        if let Some(client) = dispatch_nats_client {
            let resumer = Arc::new(
                crate::runner::runtime_session_resumer::RuntimeSessionResumer::new(Arc::clone(
                    &state_machine,
                )),
            );
            for runtime_name in ["sorla", "operala", "agentic", "telco-x", "approval"] {
                tokio::spawn(crate::runner::dispatch_listener::run_response_listener(
                    client.clone(),
                    runtime_name.to_string(),
                    Arc::clone(&resumer)
                        as Arc<dyn crate::runner::dispatch_listener::SessionResumer>,
                ));
            }
            // Eagerly pre-cache local-wasm MCP components when the admin publishes
            // a warm event. Feature-gated behind `agentic-worker` because
            // `mcp_store_pull` lives in `greentic-aw-runtime` which is only
            // present when that feature is enabled.
            #[cfg(feature = "agentic-worker")]
            tokio::spawn(crate::runner::mcp_warm_listener::run_mcp_warm_listener(
                client.clone(),
            ));
            tracing::info!(
                "Remote-dispatch (NATS) wired into runtime: sorla.call / operala.call / agentic.call / telco-x.call"
            );
        }
        let http_client = Client::builder().build()?;
        Ok(Arc::new(Self {
            tenant: config.tenant.clone(),
            config,
            packs: pack_runtimes,
            digests,
            engine,
            state_machine,
            session_store,
            http_client,
            mocks,
            timer_handles: Mutex::new(Vec::new()),
            secrets: secrets_manager,
            operator_registry,
            operator_metrics,
            contract_cache: ContractCache::from_env(),
        }))
    }

    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    pub fn config(&self) -> &Arc<HostConfig> {
        &self.config
    }

    pub fn operator_registry(&self) -> &OperatorRegistry {
        &self.operator_registry
    }

    pub fn operator_metrics(&self) -> &OperatorMetrics {
        &self.operator_metrics
    }

    pub fn contract_cache(&self) -> &ContractCache {
        &self.contract_cache
    }

    pub fn contract_cache_stats(&self) -> ContractCacheStats {
        self.contract_cache.stats()
    }

    pub fn main_pack(&self) -> &Arc<PackRuntime> {
        self.packs
            .first()
            .expect("tenant runtime must contain at least one pack")
    }

    pub fn pack(&self) -> Arc<PackRuntime> {
        Arc::clone(self.main_pack())
    }

    pub fn overlays(&self) -> Vec<Arc<PackRuntime>> {
        self.packs.iter().skip(1).cloned().collect()
    }

    /// All packs in declaration order: main pack at index 0, overlays after.
    /// Borrowed slice — no `Arc` clones, no allocation. Use this when you only
    /// need to iterate the full pack list; prefer [`pack`]/[`overlays`] when
    /// you need to hand out owned `Arc`s downstream.
    ///
    /// [`pack`]: TenantRuntime::pack
    /// [`overlays`]: TenantRuntime::overlays
    pub fn all_packs(&self) -> &[Arc<PackRuntime>] {
        &self.packs
    }

    /// Resolved content digest of each loaded pack, index-aligned with the pack
    /// list. `Some` for revision runtimes (verified at load) and the legacy
    /// index path; `None` only when a digest was unavailable.
    pub fn pack_digests(&self) -> &[Option<String>] {
        &self.digests
    }

    pub fn engine(&self) -> &Arc<FlowEngine> {
        &self.engine
    }

    pub fn state_machine(&self) -> &Arc<StateMachineRuntime> {
        &self.state_machine
    }

    /// Shared session store. M1.5: lets `apply_welcome_flow_override`
    /// build a `FlowResumeStore` against the SAME bucket the state machine
    /// will read/write — important under revision mode where each revision
    /// is given its own store via `load_revision`.
    pub fn session_store(&self) -> &DynSessionStore {
        &self.session_store
    }

    pub fn http_client(&self) -> &Client {
        &self.http_client
    }

    pub fn oauth_config(&self) -> Option<OAuthBrokerConfig> {
        self.config.oauth_broker_config()
    }

    pub fn digest(&self) -> Option<&str> {
        self.digests.first().and_then(|d| d.as_deref())
    }

    pub fn overlay_digests(&self) -> Vec<Option<String>> {
        self.digests.iter().skip(1).cloned().collect()
    }

    pub fn required_secrets(&self) -> Vec<SecretRequirement> {
        self.packs
            .iter()
            .flat_map(|pack| pack.required_secrets().iter().cloned())
            .collect()
    }

    pub fn missing_secrets(&self) -> Vec<SecretRequirement> {
        self.packs
            .iter()
            .flat_map(|pack| pack.missing_secrets(&self.config.tenant_ctx()))
            .collect()
    }

    pub fn mocks(&self) -> Option<&Arc<MockLayer>> {
        self.mocks.as_ref()
    }

    pub fn register_timers(&self, handles: Vec<JoinHandle<()>>) {
        self.timer_handles.lock().extend(handles);
    }

    /// Read a RUNTIME-level secret, at the `_runner` pseudo-pack segment.
    ///
    /// **Deliberately NOT unit-scoped**, unlike every pack secret a component
    /// reads (see [`crate::secrets::unit_pack_segment`]). Its only caller is
    /// the operator HTTP attachments path (`runner::operator::resolve_attachments`),
    /// and `_runner` is not a deployed pack: nothing stages a per-unit value
    /// there, in any lane. Scoping it per unit would move an address no writer
    /// produces, so every currently-working `_runner` secret would resolve only
    /// through the compatibility fallback — a strictly worse address for no
    /// isolation gained. Revisit only if a per-unit writer for `_runner`
    /// appears.
    pub fn get_secret(&self, key: &str) -> Result<String> {
        if crate::provider_core_only::is_enabled() {
            bail!(crate::provider_core_only::blocked_message("secrets"))
        }
        if !self.config.secrets_policy.is_allowed(key) {
            bail!("secret {key} is not permitted by bindings policy");
        }
        let ctx = self.config.tenant_ctx();
        let canonical_key = canonicalize_secret_key(key);
        let bytes =
            read_secret_blocking(&self.secrets, &ctx, RUNTIME_SECRETS_PACK_ID, &canonical_key)
                .context("failed to read secret from manager")?;
        let value = String::from_utf8(bytes).context("secret value is not valid UTF-8")?;
        Ok(value)
    }

    pub fn build_events_email_execution_plan(
        &self,
        tenant: &greentic_types::TenantCtx,
        request: &EmailSendRequest,
    ) -> Result<EmailExecutionPlan> {
        let oauth = self
            .oauth_config()
            .ok_or_else(|| anyhow!("oauth broker config is not configured for tenant runtime"))?;
        build_email_execution_plan(&oauth, tenant, request)
    }

    pub async fn execute_events_email_request(
        &self,
        access_token: &str,
        request: &EmailSendRequest,
    ) -> Result<()> {
        execute_email_request(self.http_client(), access_token, request).await
    }

    pub async fn execute_events_email_with_oauth(
        &self,
        tenant: &greentic_types::TenantCtx,
        request: &EmailSendRequest,
    ) -> Result<()> {
        let plan = self.build_events_email_execution_plan(tenant, request)?;
        let token = request_resource_token(self.http_client(), &plan.token_request).await?;
        self.execute_events_email_request(&token.access_token, request)
            .await
    }

    pub fn pack_for_component(&self, component_ref: &str) -> Option<Arc<PackRuntime>> {
        self.packs
            .iter()
            .find(|pack| pack.contains_component(component_ref))
            .cloned()
    }

    pub fn pack_for_component_with_digest(
        &self,
        component_ref: &str,
    ) -> Option<(Arc<PackRuntime>, Option<String>)> {
        self.packs
            .iter()
            .zip(self.digests.iter())
            .find(|(pack, _)| pack.contains_component(component_ref))
            .map(|(pack, digest)| (Arc::clone(pack), digest.clone()))
    }

    pub fn resolve_component(&self, component_ref: &str) -> Option<ResolvedComponent> {
        self.pack_for_component_with_digest(component_ref)
            .map(|(pack, digest)| ResolvedComponent {
                digest: digest
                    .or_else(|| self.digest().map(ToString::to_string))
                    .unwrap_or_else(|| "unknown".to_string()),
                component_ref: component_ref.to_string(),
                pack,
            })
    }
}

impl Drop for TenantRuntime {
    fn drop(&mut self) {
        for handle in self.timer_handles.lock().drain(..) {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod runtime_key_tests {
    use super::*;

    #[test]
    fn legacy_keys_match_only_on_tenant() {
        assert_eq!(RuntimeKey::legacy("acme"), RuntimeKey::legacy("acme"));
        assert_ne!(RuntimeKey::legacy("acme"), RuntimeKey::legacy("other"));
    }

    #[test]
    fn legacy_and_revision_keys_never_collide() {
        let key = RuntimeKey::revision(
            "acme",
            DeploymentId::new(),
            BundleId::from("bundle-a"),
            RevisionId::new(),
        );
        assert_ne!(RuntimeKey::legacy("acme"), key);
    }

    #[test]
    fn revision_keys_distinguish_revision_id() {
        let deployment = DeploymentId::new();
        let bundle = BundleId::from("bundle-a");
        let rev_a = RevisionId::new();
        let rev_b = RevisionId::new();
        let key_a = RuntimeKey::revision("acme", deployment, bundle.clone(), rev_a);
        let key_b = RuntimeKey::revision("acme", deployment, bundle.clone(), rev_b);
        let same = RuntimeKey::revision("acme", deployment, bundle, rev_a);
        assert_ne!(key_a, key_b);
        assert_eq!(key_a, same);
    }

    #[test]
    fn legacy_reload_preserves_revision_entries() {
        let revision_key = RuntimeKey::revision(
            "acme",
            DeploymentId::new(),
            BundleId::from("bundle-a"),
            RevisionId::new(),
        );
        let mut prev: HashMap<RuntimeKey, u32> = HashMap::new();
        prev.insert(RuntimeKey::legacy("acme"), 1); // stale legacy, refreshed below
        prev.insert(RuntimeKey::legacy("retired"), 2); // dropped: not in new index
        prev.insert(revision_key.clone(), 99); // revision runtime: must survive

        let mut legacy: HashMap<RuntimeKey, u32> = HashMap::new();
        legacy.insert(RuntimeKey::legacy("acme"), 10);
        legacy.insert(RuntimeKey::legacy("newcomer"), 20);

        let next = merge_legacy_reload(&prev, legacy);

        assert_eq!(next.get(&RuntimeKey::legacy("acme")), Some(&10));
        assert_eq!(next.get(&RuntimeKey::legacy("newcomer")), Some(&20));
        assert_eq!(next.get(&RuntimeKey::legacy("retired")), None);
        assert_eq!(next.get(&revision_key), Some(&99));
    }

    #[test]
    fn remove_keyed_entry_pops_only_the_targeted_key() {
        let deployment = DeploymentId::new();
        let bundle = BundleId::from("bundle-a");
        let rev_a = RevisionId::new();
        let rev_b = RevisionId::new();
        let key_a = RuntimeKey::revision("acme", deployment, bundle.clone(), rev_a);
        let key_b = RuntimeKey::revision("acme", deployment, bundle, rev_b);

        let mut prev: HashMap<RuntimeKey, u32> = HashMap::new();
        prev.insert(RuntimeKey::legacy("acme"), 1);
        prev.insert(key_a.clone(), 10);
        prev.insert(key_b.clone(), 20);

        let (next, removed) = remove_keyed_entry(&prev, &key_a).expect("present");
        assert_eq!(removed, 10);
        assert_eq!(next.get(&key_a), None);
        assert_eq!(next.get(&key_b), Some(&20));
        assert_eq!(next.get(&RuntimeKey::legacy("acme")), Some(&1));
    }

    #[test]
    fn remove_keyed_entry_returns_none_for_missing_key() {
        let prev: HashMap<RuntimeKey, u32> = HashMap::new();
        let ghost = RuntimeKey::revision(
            "acme",
            DeploymentId::new(),
            BundleId::from("bundle-a"),
            RevisionId::new(),
        );
        assert!(remove_keyed_entry(&prev, &ghost).is_none());
    }

    #[test]
    fn remove_keyed_entry_leaves_other_deployments_alone() {
        let bundle = BundleId::from("bundle-a");
        let dep_a = DeploymentId::new();
        let dep_b = DeploymentId::new();
        let rev = RevisionId::new();
        let key_a = RuntimeKey::revision("acme", dep_a, bundle.clone(), rev);
        let key_b = RuntimeKey::revision("acme", dep_b, bundle, rev);

        let mut prev: HashMap<RuntimeKey, u32> = HashMap::new();
        prev.insert(key_a.clone(), 100);
        prev.insert(key_b.clone(), 200);

        let (next, removed) = remove_keyed_entry(&prev, &key_a).expect("present");
        assert_eq!(removed, 100);
        assert_eq!(next.get(&key_b), Some(&200));
        assert_eq!(next.len(), 1);
    }

    #[test]
    fn map_lookup_separates_legacy_from_revision() {
        let deployment = DeploymentId::new();
        let bundle = BundleId::from("bundle-a");
        let revision = RevisionId::new();

        let mut map: HashMap<RuntimeKey, u32> = HashMap::new();
        map.insert(RuntimeKey::legacy("acme"), 1);
        map.insert(
            RuntimeKey::revision("acme", deployment, bundle.clone(), revision),
            2,
        );

        assert_eq!(map.get(&RuntimeKey::legacy("acme")), Some(&1));
        assert_eq!(
            map.get(&RuntimeKey::revision("acme", deployment, bundle, revision)),
            Some(&2)
        );
        assert_eq!(map.get(&RuntimeKey::legacy("ghost")), None);
    }

    #[test]
    fn insert_keyed_entry_adds_revision_preserving_legacy_and_siblings() {
        let deployment = DeploymentId::new();
        let bundle = BundleId::from("bundle-a");
        let rev_a = RevisionId::new();
        let rev_b = RevisionId::new();
        let key_a = RuntimeKey::revision("acme", deployment, bundle.clone(), rev_a);
        let key_b = RuntimeKey::revision("acme", deployment, bundle, rev_b);

        let mut prev: HashMap<RuntimeKey, u32> = HashMap::new();
        prev.insert(RuntimeKey::legacy("acme"), 1);
        prev.insert(key_a.clone(), 10);

        let next = insert_keyed_entry(&prev, key_b.clone(), 20);

        // New revision lands; legacy entry and the sibling revision survive.
        assert_eq!(next.get(&key_b), Some(&20));
        assert_eq!(next.get(&key_a), Some(&10));
        assert_eq!(next.get(&RuntimeKey::legacy("acme")), Some(&1));
        assert_eq!(next.len(), 3);
    }

    #[test]
    fn insert_keyed_entry_replaces_existing_revision() {
        let key = RuntimeKey::revision(
            "acme",
            DeploymentId::new(),
            BundleId::from("bundle-a"),
            RevisionId::new(),
        );
        let mut prev: HashMap<RuntimeKey, u32> = HashMap::new();
        prev.insert(key.clone(), 10);

        let next = insert_keyed_entry(&prev, key.clone(), 99);

        assert_eq!(next.get(&key), Some(&99));
        assert_eq!(next.len(), 1);
    }
}
