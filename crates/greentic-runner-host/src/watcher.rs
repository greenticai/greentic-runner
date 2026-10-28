use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use parking_lot::Mutex;
use runner_core::{Index, PackConfig, PackManager, PackPolicy};
use tokio::sync::mpsc;
use tokio::task;

use crate::HostConfig;
use crate::engine::host::{SessionHost, StateHost};
use crate::host::RunnerHost;
use crate::http::health::HealthState;
use crate::pack::{ComponentResolution, PackRuntime};
use crate::runner::adapt_timer;
use crate::runtime::{ActivePacks, RuntimeKey, TenantRuntime};
use crate::secrets::DynSecretsManager;
use crate::storage::session::DynSessionStore;
use crate::storage::state::DynStateStore;
use crate::wasi::RunnerWasiPolicy;

/// Digests (main + overlays) last applied per tenant, used to skip rebuilding
/// tenants whose packs did not change.
type AppliedDigests = Arc<Mutex<HashMap<String, Vec<String>>>>;
/// Default first retry delay after a failed tenant rebuild.
const REBUILD_RETRY_BASE: Duration = Duration::from_secs(60);
/// Upper bound for the exponential rebuild retry delay.
const REBUILD_RETRY_CAP: Duration = Duration::from_secs(15 * 60);

/// A failed rebuild of one tenant's pack digests.
struct FailedRebuild {
    digests: Vec<String>,
    last_attempt: Instant,
    attempts: u32,
}

/// Per tenant, the pack digests whose runtime build last failed, with an
/// exponential retry backoff. `PackRuntime::load` can fail transiently
/// (network fetch of remote components, secrets, IO), so a failed rebuild is
/// retried after `base`, `2*base`, ... capped at `cap`, instead of being
/// skipped until the digests change. A manual reload trigger clears all
/// markers and retries immediately.
struct FailedRebuilds {
    base: Duration,
    cap: Duration,
    entries: HashMap<String, FailedRebuild>,
}

impl FailedRebuilds {
    fn new(base: Duration, cap: Duration) -> Self {
        Self {
            base,
            cap,
            entries: HashMap::new(),
        }
    }

    fn delay_for(&self, attempts: u32) -> Duration {
        let exp = attempts.saturating_sub(1).min(31);
        self.base
            .checked_mul(1u32 << exp)
            .unwrap_or(self.cap)
            .min(self.cap)
    }

    /// Whether a rebuild of `digests` for `tenant` failed and is still inside
    /// its backoff window at `now`.
    fn should_skip(&self, tenant: &str, digests: &[String], now: Instant) -> bool {
        self.entries.get(tenant).is_some_and(|known| {
            known.digests.as_slice() == digests
                && now.saturating_duration_since(known.last_attempt)
                    < self.delay_for(known.attempts)
        })
    }

    fn record_failure(&mut self, tenant: &str, digests: &[String], now: Instant) {
        let attempts = match self.entries.get(tenant) {
            Some(known) if known.digests.as_slice() == digests => known.attempts.saturating_add(1),
            _ => 1,
        };
        self.entries.insert(
            tenant.to_string(),
            FailedRebuild {
                digests: digests.to_vec(),
                last_attempt: now,
                attempts,
            },
        );
    }

    fn clear_tenant(&mut self, tenant: &str) {
        self.entries.remove(tenant);
    }

    fn clear_all(&mut self) {
        self.entries.clear();
    }
}

type FailedDigests = Arc<Mutex<FailedRebuilds>>;

pub struct PackWatcher {
    handle: tokio::task::JoinHandle<()>,
}

impl Drop for PackWatcher {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

#[derive(Clone)]
pub struct PackReloadHandle {
    trigger: mpsc::Sender<()>,
}

impl PackReloadHandle {
    pub async fn trigger(&self) -> Result<()> {
        self.trigger
            .send(())
            .await
            .map_err(|_| anyhow!("pack watcher task stopped"))
    }
}

/// Starts the watcher with a policy taken from the environment only
/// (`PackPolicy::from_env`). This wrapper has no access to the resolved config,
/// so `packs.trust.require_signatures` is NOT honoured here; callers that need
/// it must build the policy themselves and use
/// [`start_pack_watcher_with_policy`] (as `run()` does via `pack_policy_from`).
pub async fn start_pack_watcher(
    host: Arc<RunnerHost>,
    cfg: PackConfig,
    refresh: Duration,
) -> Result<(PackWatcher, PackReloadHandle)> {
    start_pack_watcher_with_policy(host, cfg, PackPolicy::from_env(), refresh).await
}

/// Like [`start_pack_watcher`], with an explicit pack verification policy
/// (strict signature enforcement, filesystem locator confinement).
pub async fn start_pack_watcher_with_policy(
    host: Arc<RunnerHost>,
    cfg: PackConfig,
    policy: PackPolicy,
    refresh: Duration,
) -> Result<(PackWatcher, PackReloadHandle)> {
    let cfg_clone = cfg.clone();
    let manager = task::spawn_blocking(move || {
        PackManager::with_policy(cfg_clone, policy)
            .map(|manager| manager.with_refresh_interval(refresh))
    })
    .await
    .context("pack manager init task failed")??;
    let manager = Arc::new(manager);
    let configs = Arc::new(host.tenant_configs());
    let active = host.active_packs();
    let health = host.health_state();
    let session_host = host.session_host();
    let session_store = host.session_store();
    let state_store = host.state_store();
    let state_host = host.state_host();
    let wasi_policy = host.wasi_policy();
    let secrets_manager = host.secrets_manager();
    #[cfg(feature = "agentic-worker")]
    let ext_llm_port = host.ext_llm_port();
    #[cfg(feature = "agentic-worker")]
    let mcp_source = host.mcp_source();
    #[cfg(feature = "agentic-worker")]
    let artifact_reader = host.artifact_reader();
    #[cfg(feature = "agentic-worker")]
    let ext_artifact_port = host.ext_artifact_port();
    #[cfg(feature = "agentic-worker")]
    let stream_observers = host.stream_observers();

    let applied: AppliedDigests = Arc::new(Mutex::new(HashMap::new()));
    let failed_digests: FailedDigests = Arc::new(Mutex::new(FailedRebuilds::new(
        REBUILD_RETRY_BASE,
        REBUILD_RETRY_CAP,
    )));
    reload_once(
        configs.as_ref(),
        &manager,
        &cfg,
        &applied,
        &failed_digests,
        &active,
        &health,
        session_host.clone(),
        session_store.clone(),
        state_store.clone(),
        state_host.clone(),
        Arc::clone(&wasi_policy),
        secrets_manager.clone(),
        #[cfg(feature = "agentic-worker")]
        ext_llm_port.clone(),
        #[cfg(feature = "agentic-worker")]
        mcp_source.clone(),
        #[cfg(feature = "agentic-worker")]
        artifact_reader.clone(),
        #[cfg(feature = "agentic-worker")]
        ext_artifact_port.clone(),
        #[cfg(feature = "agentic-worker")]
        stream_observers.clone(),
    )
    .await?;

    let (tx, mut rx) = mpsc::channel::<()>(4);
    let index_cfg = cfg.clone();
    let manager_clone = Arc::clone(&manager);
    let applied_clone = Arc::clone(&applied);
    let failed_clone = Arc::clone(&failed_digests);
    let health_clone = Arc::clone(&health);
    let active_clone = Arc::clone(&active);
    let configs_clone = Arc::clone(&configs);
    let state_store_clone = Arc::clone(&state_store);
    let wasi_policy_clone = Arc::clone(&wasi_policy);
    let secrets_manager_clone = secrets_manager.clone();
    #[cfg(feature = "agentic-worker")]
    let ext_llm_port_clone = ext_llm_port.clone();
    #[cfg(feature = "agentic-worker")]
    let mcp_source_clone = mcp_source.clone();
    #[cfg(feature = "agentic-worker")]
    let artifact_reader_clone = artifact_reader.clone();
    #[cfg(feature = "agentic-worker")]
    let ext_artifact_port_clone = ext_artifact_port.clone();
    #[cfg(feature = "agentic-worker")]
    let stream_observers_clone = stream_observers.clone();
    let handle = tokio::spawn(async move {
        // The initial load already ran above; start ticking one period from
        // now so the first tick does not trigger an immediate second load.
        let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + refresh, refresh);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = ticker.tick() => {},
                recv = rx.recv() => {
                    if recv.is_none() {
                        break;
                    }
                    // A manual trigger bypasses the failed-rebuild backoff.
                    failed_clone.lock().clear_all();
                }
            }
            if let Err(err) = reload_once(
                configs_clone.as_ref(),
                &manager_clone,
                &index_cfg,
                &applied_clone,
                &failed_clone,
                &active_clone,
                &health_clone,
                session_host.clone(),
                session_store.clone(),
                state_store_clone.clone(),
                state_host.clone(),
                Arc::clone(&wasi_policy_clone),
                secrets_manager_clone.clone(),
                #[cfg(feature = "agentic-worker")]
                ext_llm_port_clone.clone(),
                #[cfg(feature = "agentic-worker")]
                mcp_source_clone.clone(),
                #[cfg(feature = "agentic-worker")]
                artifact_reader_clone.clone(),
                #[cfg(feature = "agentic-worker")]
                ext_artifact_port_clone.clone(),
                #[cfg(feature = "agentic-worker")]
                stream_observers_clone.clone(),
            )
            .await
            {
                tracing::error!(error = %redact_chain(&err), "pack reload failed");
                health_clone.record_reload_error(&err);
            }
        }
    });

    let watcher = PackWatcher { handle };
    let handle = PackReloadHandle { trigger: tx };
    Ok((watcher, handle))
}

/// An error chain for a log line, the reload failure list or `/admin/packs/status`
/// `last_error`. A pack locator may be a presigned S3 / Azure SAS URL, so URL
/// userinfo and queries (and credential pairs) are redacted.
pub(crate) fn redact_chain(err: &anyhow::Error) -> String {
    crate::run_outcome::error::redact(&format!("{err:#}"))
}

/// Refresh the cached archives of a runtime kept although its republish failed
/// to build. Its old digest is no longer resolved, so nothing else would bump
/// the archive mtime and the next successful store could prune it from under
/// the live runtime.
fn keep_archives_alive(manager: &PackManager, runtime: &TenantRuntime) {
    for pack in runtime.all_packs() {
        if let Some(path) = pack.archive_path() {
            manager.touch_cached_archive(path);
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn reload_once(
    configs: &HashMap<String, Arc<HostConfig>>,
    manager: &Arc<PackManager>,
    cfg: &PackConfig,
    applied: &AppliedDigests,
    failed_digests: &FailedDigests,
    active: &Arc<ActivePacks>,
    health: &Arc<HealthState>,
    session_host: Arc<dyn SessionHost>,
    session_store: DynSessionStore,
    state_store: DynStateStore,
    state_host: Arc<dyn StateHost>,
    wasi_policy: Arc<RunnerWasiPolicy>,
    secrets_manager: DynSecretsManager,
    #[cfg(feature = "agentic-worker")] ext_llm_port: Option<crate::host::ExtLlmPort>,
    #[cfg(feature = "agentic-worker")] mcp_source: Option<crate::host::McpSource>,
    #[cfg(feature = "agentic-worker")] artifact_reader: Option<crate::host::ArtifactReaderPort>,
    #[cfg(feature = "agentic-worker")] ext_artifact_port: Option<crate::host::ExtArtifactPort>,
    #[cfg(feature = "agentic-worker")]
    stream_observers: crate::http::agent_stream::StreamObserverRegistry,
) -> Result<()> {
    // Index fetch, pack download and hashing are blocking; keep them off the
    // async workers.
    let manager_for_resolve = Arc::clone(manager);
    let index_location = cfg.index_location.clone();
    let resolved = task::spawn_blocking(move || {
        let index = Index::load(&index_location)?;
        Ok::<_, anyhow::Error>(manager_for_resolve.resolve_each_for_index(&index))
    })
    .await
    .context("pack resolve task failed")??;

    let mut next = HashMap::new();
    let mut failures: Vec<String> = Vec::new();
    let mut new_applied: HashMap<String, Vec<String>> = HashMap::new();
    // (new runtime, the runtime it supersedes) for every tenant rebuilt this pass.
    let mut swapped: Vec<(Arc<TenantRuntime>, Option<Arc<TenantRuntime>>)> = Vec::new();
    for (tenant, record) in resolved {
        // A failing tenant keeps its previous runtime; others still reload.
        let previous = active.load_pack(&tenant);
        let record = match record {
            Ok(record) => record,
            Err(err) => {
                tracing::error!(tenant = %tenant, error = %redact_chain(&err), "pack resolve failed; keeping previous runtime");
                failures.push(format!("{tenant}: {}", redact_chain(&err)));
                if let Some(prev) = previous {
                    // Like the other keep-previous branches: the kept runtime
                    // re-opens its archive lazily, so a long resolve outage must
                    // not let the cache prune its (idle-looking) directory.
                    keep_archives_alive(manager, &prev);
                    next.insert(RuntimeKey::legacy(tenant.clone()), prev);
                    if let Some(digests) = applied.lock().get(&tenant).cloned() {
                        new_applied.insert(tenant.clone(), digests);
                    }
                }
                continue;
            }
        };
        let digests: Vec<String> = std::iter::once(record.main.digest.as_str().to_string())
            .chain(
                record
                    .overlays
                    .iter()
                    .map(|overlay| overlay.digest.as_str().to_string()),
            )
            .collect();
        // Tenants whose digests are unchanged are NOT rebuilt: load-time inputs
        // the digest does not cover (e.g. floating component tags resolved at
        // load) stay frozen at the first load. A restart, or a digest change
        // (a manual `PackReloadHandle::trigger` re-resolves the index and
        // picks that up immediately), is needed to refresh them.
        if let Some(prev) = previous.clone()
            && applied.lock().get(&tenant) == Some(&digests)
        {
            tracing::debug!(tenant = %tenant, "packs unchanged; keeping runtime");
            next.insert(RuntimeKey::legacy(tenant.clone()), prev);
            new_applied.insert(tenant.clone(), digests);
            continue;
        }
        if failed_digests
            .lock()
            .should_skip(&tenant, &digests, Instant::now())
        {
            tracing::debug!(tenant = %tenant, "pack digests unchanged since failed rebuild; waiting for retry backoff");
            failures.push(format!(
                "{tenant}: rebuild previously failed for these pack digests"
            ));
            if let Some(prev) = previous {
                keep_archives_alive(manager, &prev);
                next.insert(RuntimeKey::legacy(tenant.clone()), prev);
                if let Some(old) = applied.lock().get(&tenant).cloned() {
                    new_applied.insert(tenant.clone(), old);
                }
            }
            continue;
        }
        let built: Result<Arc<TenantRuntime>> = async {
            let config = configs
                .get(&tenant)
                .cloned()
                .with_context(|| format!("no host config registered for tenant {tenant}"))?;
            let oauth_config = config.oauth_broker_config();
            let mut packs = Vec::new();
            let main_runtime = Arc::new(
                PackRuntime::load(
                    &record.main.path,
                    Arc::clone(&config),
                    None,
                    Some(&record.main.path),
                    Some(Arc::clone(&session_store)),
                    Some(Arc::clone(&state_store)),
                    Arc::clone(&wasi_policy),
                    Arc::clone(&secrets_manager),
                    oauth_config.clone(),
                    true,
                    ComponentResolution::default(),
                )
                .await
                .with_context(|| format!("failed to load pack for tenant {tenant}"))?,
            );
            packs.push((main_runtime, Some(record.main.digest.as_str().to_string())));

            for overlay in &record.overlays {
                let runtime = Arc::new(
                    PackRuntime::load(
                        &overlay.path,
                        Arc::clone(&config),
                        None,
                        Some(&overlay.path),
                        Some(Arc::clone(&session_store)),
                        Some(Arc::clone(&state_store)),
                        Arc::clone(&wasi_policy),
                        Arc::clone(&secrets_manager),
                        oauth_config.clone(),
                        true,
                        ComponentResolution::default(),
                    )
                    .await
                    .with_context(|| {
                        format!(
                            "failed to load overlay {} for tenant {tenant}",
                            overlay.reference.name
                        )
                    })?,
                );
                packs.push((runtime, Some(overlay.digest.as_str().to_string())));
            }

            let runtime = TenantRuntime::from_packs_with_artifact_reader(
                Arc::clone(&config),
                packs,
                None,
                Arc::clone(&session_host),
                Arc::clone(&session_store),
                Arc::clone(&state_store),
                Arc::clone(&state_host),
                Arc::clone(&secrets_manager),
                #[cfg(feature = "agentic-worker")]
                ext_llm_port.clone(),
                #[cfg(feature = "agentic-worker")]
                mcp_source.clone(),
                #[cfg(feature = "agentic-worker")]
                artifact_reader.clone(),
                #[cfg(feature = "agentic-worker")]
                ext_artifact_port.clone(),
                #[cfg(feature = "agentic-worker")]
                Some(stream_observers.clone()),
            )
            .await?;
            // Validate only: the timers are started AFTER the swap below, so
            // the old and new runtime never both fire a cron tick.
            adapt_timer::validate_timers(&runtime)?;
            Ok(runtime)
        }
        .await;
        match built {
            Ok(runtime) => {
                failed_digests.lock().clear_tenant(&tenant);
                swapped.push((Arc::clone(&runtime), previous.clone()));
                next.insert(RuntimeKey::legacy(tenant.clone()), runtime);
                new_applied.insert(tenant.clone(), digests);
            }
            Err(err) => {
                tracing::error!(tenant = %tenant, error = %redact_chain(&err), "tenant reload failed; keeping previous runtime");
                failures.push(format!("{tenant}: {}", redact_chain(&err)));
                failed_digests
                    .lock()
                    .record_failure(&tenant, &digests, Instant::now());
                if let Some(prev) = previous {
                    keep_archives_alive(manager, &prev);
                    next.insert(RuntimeKey::legacy(tenant.clone()), prev);
                    if let Some(old) = applied.lock().get(&tenant).cloned() {
                        new_applied.insert(tenant.clone(), old);
                    }
                }
            }
        }
    }
    active.replace_legacy(next);
    *applied.lock() = new_applied;
    // Hand over cron timers: stop the superseded runtime's first, then start
    // the new one's, so a tick is fired by exactly one of them.
    for (runtime, previous) in swapped {
        if let Some(previous) = previous {
            previous.abort_timers();
        }
        match adapt_timer::spawn_timers(Arc::clone(&runtime)) {
            Ok(timers) => runtime.register_timers(timers),
            Err(err) => {
                let tenant = runtime.config().tenant.clone();
                tracing::error!(tenant = %tenant, error = %redact_chain(&err), "failed to start timers after reload");
                failures.push(format!("{tenant}: timers: {}", redact_chain(&err)));
            }
        }
    }
    if !failures.is_empty() {
        return Err(anyhow!(
            "pack reload failed for {} tenant(s): {}",
            failures.len(),
            failures.join("; ")
        ));
    }
    health.record_reload_success();
    tracing::info!("pack reload completed successfully");
    Ok(())
}

#[cfg(test)]
mod failed_digest_tests {
    use super::*;

    fn d(s: &str) -> Vec<String> {
        vec![s.to_string()]
    }

    #[test]
    fn failed_rebuild_is_skipped_within_backoff_then_retried() {
        let base = Duration::from_secs(10);
        let mut f = FailedRebuilds::new(base, Duration::from_secs(60));
        let t0 = Instant::now();
        assert!(!f.should_skip("t", &d("a"), t0));
        f.record_failure("t", &d("a"), t0);
        assert!(f.should_skip("t", &d("a"), t0 + Duration::from_secs(5)));
        assert!(!f.should_skip("t", &d("a"), t0 + base));
        assert!(!f.should_skip("t", &d("b"), t0));
        assert!(!f.should_skip("other", &d("a"), t0));
    }

    #[test]
    fn backoff_doubles_and_caps() {
        let mut f = FailedRebuilds::new(Duration::from_secs(10), Duration::from_secs(50));
        let t0 = Instant::now();
        f.record_failure("t", &d("a"), t0);
        f.record_failure("t", &d("a"), t0);
        assert!(f.should_skip("t", &d("a"), t0 + Duration::from_secs(19)));
        assert!(!f.should_skip("t", &d("a"), t0 + Duration::from_secs(20)));
        for _ in 0..40 {
            f.record_failure("t", &d("a"), t0);
        }
        assert_eq!(f.delay_for(41), Duration::from_secs(50));
        assert!(!f.should_skip("t", &d("a"), t0 + Duration::from_secs(50)));
    }

    #[test]
    fn manual_trigger_clear_bypasses_backoff() {
        let mut f = FailedRebuilds::new(Duration::from_secs(60), Duration::from_secs(900));
        let t0 = Instant::now();
        f.record_failure("t", &d("a"), t0);
        assert!(f.should_skip("t", &d("a"), t0));
        f.clear_all();
        assert!(!f.should_skip("t", &d("a"), t0));
        f.record_failure("t", &d("a"), t0);
        f.clear_tenant("t");
        assert!(!f.should_skip("t", &d("a"), t0));
    }
}
