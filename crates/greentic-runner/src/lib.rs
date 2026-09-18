#![forbid(unsafe_code)]
// The merged `run_http_host` future nests the research agentic-dispatch path and
// main's revision/fast2flow path in one async body, whose layout exceeds the
// default query depth (128). 256 covers the combined nesting.
#![recursion_limit = "256"]
//! Canonical entrypoint for embedding the Greentic runner.
//!
//! This crate provides two supported integration paths:
//! - [`run_http_host`] mirrors the CLI and starts the HTTP server that exposes
//!   ingress adapters, admin endpoints, and the pack watcher.
//! - [`start_embedded_host`] constructs a [`RunnerHost`] without spinning up the
//!   HTTP server so callers can drive `handle_activity` manually (desktop/dev
//!   harnesses, tests, etc.).

use anyhow::Result;

pub use greentic_runner_host::{
    self as host, Activity, ActivityKind, HostBuilder, HostServer, RunnerConfig, RunnerHost,
    TenantHandle, config, http, pack, routing, runner, runtime, runtime_wasmtime, telemetry,
    verify, watcher,
};

pub mod desktop {
    pub use greentic_runner_desktop::*;
}

pub mod gen_bindings;
pub mod info;

/// Launch the canonical HTTP host. This is equivalent to running the
/// `greentic-runner` binary with the provided [`RunnerConfig`].
pub async fn run_http_host(cfg: RunnerConfig) -> Result<()> {
    greentic_runner_host::run(cfg).await
}

/// Build and start a [`RunnerHost`] without wiring the HTTP ingress server.
/// Callers are responsible for loading packs via [`RunnerHost::load_pack`] and
/// invoking [`RunnerHost::handle_activity`] directly.
pub async fn start_embedded_host(builder: HostBuilder) -> Result<RunnerHost> {
    let host = builder.build()?;
    host.start().await?;
    Ok(host)
}

#[cfg(test)]
mod tests {
    use super::*;
    use greentic_runner_host::config::{
        FlowRetryConfig, HostConfig, OperatorPolicy, RateLimits, SecretsPolicy, StateStorePolicy,
        WebhookPolicy,
    };
    use greentic_runner_host::trace::TraceConfig;
    use greentic_runner_host::validate::ValidationConfig;
    use std::collections::HashMap;
    use std::path::PathBuf;

    fn host_config() -> HostConfig {
        HostConfig {
            tenant: "embedded".into(),
            bindings_path: PathBuf::from("<test>"),
            flow_type_bindings: HashMap::new(),
            rate_limits: RateLimits::default(),
            retry: FlowRetryConfig::default(),
            http_enabled: false,
            secrets_policy: SecretsPolicy::allow_all(),
            state_store_policy: StateStorePolicy::default(),
            webhook_policy: WebhookPolicy::default(),
            timers: Vec::new(),
            oauth: None,
            mocks: None,
            pack_bindings: Vec::new(),
            env_passthrough: Vec::new(),
            trace: TraceConfig::from_env(),
            validation: ValidationConfig::from_env(),
            operator_policy: OperatorPolicy::allow_all(),
            fast2flow: Default::default(),
            #[cfg(feature = "agentic-worker")]
            agents: HashMap::new(),
            #[cfg(feature = "agentic-worker")]
            graphs: HashMap::new(),
        }
    }

    fn resolved_config() -> greentic_config::ResolvedConfig {
        greentic_config::ResolvedConfig {
            config: greentic_config_types::GreenticConfig {
                schema_version: greentic_config_types::ConfigVersion::v1(),
                environment: greentic_config_types::EnvironmentConfig {
                    env_id: greentic_types::EnvId::new("local").expect("env"),
                    deployment: None,
                    connection: None,
                    region: None,
                },
                paths: greentic_config_types::PathsConfig {
                    greentic_root: PathBuf::from(".greentic"),
                    state_dir: PathBuf::from(".greentic/state"),
                    cache_dir: PathBuf::from(".greentic/cache"),
                    logs_dir: PathBuf::from(".greentic/logs"),
                },
                packs: None,
                services: None,
                events: None,
                runtime: greentic_config_types::RuntimeConfig::default(),
                telemetry: greentic_config_types::TelemetryConfig::default(),
                network: greentic_config_types::NetworkConfig::default(),
                deployer: None,
                secrets: greentic_config_types::SecretsBackendRefConfig::default(),
                dev: None,
            },
            provenance: HashMap::new(),
            warnings: Vec::new(),
        }
    }

    #[tokio::test]
    async fn start_embedded_host_rejects_empty_builder() {
        assert!(start_embedded_host(HostBuilder::new()).await.is_err());
    }

    #[tokio::test]
    async fn start_embedded_host_starts_with_minimal_config() {
        let host = start_embedded_host(HostBuilder::new().with_config(host_config()))
            .await
            .expect("embedded host");
        host.stop().await.expect("stop");
    }

    #[tokio::test]
    async fn run_http_host_rejects_empty_runner_config() {
        let cfg = RunnerConfig {
            tenant_bindings: HashMap::new(),
            pack: runner_core::env::PackConfig {
                source: runner_core::env::PackSource::Fs,
                index_location: runner_core::env::IndexLocation::File(PathBuf::from("index.json")),
                cache_dir: PathBuf::from("cache"),
                public_key: None,
                network: None,
            },
            port: 0,
            refresh_interval: std::time::Duration::from_secs(1),
            routing: greentic_runner_host::RoutingConfig::default(),
            admin: greentic_runner_host::AdminAuth::default(),
            telemetry: None,
            secrets_backend: greentic_runner_host::secrets::SecretsBackend::Env,
            wasi_policy: greentic_runner_host::RunnerWasiPolicy::default(),
            resolved_config: resolved_config(),
            trace: greentic_runner_host::trace::TraceConfig::from_env(),
            validation: greentic_runner_host::validate::ValidationConfig::from_env(),
        };

        assert!(run_http_host(cfg).await.is_err());
    }
}
