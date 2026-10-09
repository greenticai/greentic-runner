//! `InfoReport` — the canonical capability/version surface of `greentic-runner`.
//!
//! The `info` subcommand (E5, separate commit) serialises this to JSON or
//! renders it via the sibling `human` module. Fields stay stable across patch
//! releases; add new ones with `#[serde(default)]` and bump
//! `info_schema_version` only on breaking changes.

use serde::{Deserialize, Serialize};

/// Top-level report emitted by `greentic-runner info`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfoReport {
    pub info_schema_version: u32,
    pub runner_version: String,
    pub wasmtime_version: String,
    pub target_triple: String,
    pub build_profile: String,
    pub build_timestamp_utc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub git_sha: Option<String>,
    pub pack_format_versions: Vec<u32>,
    pub features: Features,
    pub wasi_imports: Vec<InterfaceBinding>,
    pub greentic_imports: Vec<InterfaceBinding>,
}

/// Compile-time Cargo feature partition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Features {
    pub enabled: Vec<String>,
    pub disabled: Vec<String>,
}

/// A single WASI or Greentic interface the host links into guest components.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InterfaceBinding {
    pub interface: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub versions: Vec<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub opt_in_per_pack: bool,
}

fn is_false(b: &bool) -> bool {
    !*b
}

/// Pack format versions the runner understands. Single-pinned today — sourced
/// from `greentic_pack::builder::PACK_VERSION` so bumping the pack spec flows
/// through without touching this file.
pub const PACK_FORMAT_VERSIONS: &[u32] = &[greentic_pack::builder::PACK_VERSION];

/// WASI preview-2 interfaces the host registers via `register_all` in
/// `crates/greentic-runner-host/src/pack.rs`.
pub const WASI_IMPORTS: &[(&str, &str)] = &[
    ("wasi:io", "0.2.0"),
    ("wasi:clocks", "0.2.0"),
    ("wasi:filesystem", "0.2.0"),
    ("wasi:random", "0.2.0"),
    ("wasi:sockets", "0.2.0"),
    ("wasi:http", "0.2.0"),
    ("wasi:tls", "0.2.0"),
];

/// Hand-maintained description of a Greentic host-provided interface.
pub struct GreenticImport {
    pub interface: &'static str,
    pub versions: &'static [&'static str],
    pub opt_in_per_pack: bool,
}

/// Greentic-custom imports the host registers. Must stay in sync with
/// `register_all` at `crates/greentic-runner-host/src/pack.rs:1277`. A
/// consistency test below guards against drift.
pub const GREENTIC_IMPORTS: &[GreenticImport] = &[
    GreenticImport {
        interface: "greentic:component/control",
        versions: &["0.4.0", "0.5.0"],
        opt_in_per_pack: false,
    },
    GreenticImport {
        interface: "greentic:http/client",
        versions: &["1.0.0", "1.1.0"],
        opt_in_per_pack: false,
    },
    GreenticImport {
        interface: "greentic:interfaces/host",
        versions: &["0.6.0"],
        opt_in_per_pack: false,
    },
    GreenticImport {
        interface: "greentic:interfaces/telemetry",
        versions: &["0.6.0"],
        opt_in_per_pack: false,
    },
    GreenticImport {
        interface: "greentic:interfaces/state-store",
        versions: &["0.6.0"],
        opt_in_per_pack: true,
    },
    GreenticImport {
        interface: "greentic:interfaces/secrets",
        versions: &["1.1.0"],
        opt_in_per_pack: false,
    },
];

/// Assemble the [`InfoReport`] from compile-time metadata.
pub fn collect() -> InfoReport {
    let mut enabled = Vec::new();
    let mut disabled = Vec::new();
    for (feat, on) in [
        ("verify", cfg!(feature = "verify")),
        ("telemetry", true),
        ("session-redis", cfg!(feature = "session-redis")),
        ("fault-injection", cfg!(feature = "fault-injection")),
        (
            "component-v0-6-introspection",
            cfg!(feature = "component-v0-6-introspection"),
        ),
        ("operala-in-process", cfg!(feature = "operala-in-process")),
    ] {
        if on {
            enabled.push(feat.to_string());
        } else {
            disabled.push(feat.to_string());
        }
    }

    InfoReport {
        info_schema_version: 1,
        runner_version: env!("CARGO_PKG_VERSION").to_string(),
        wasmtime_version: wasmtime_environ::VERSION.to_string(),
        target_triple: env!("TARGET").to_string(),
        build_profile: env!("BUILD_PROFILE").to_string(),
        build_timestamp_utc: env!("BUILD_TIMESTAMP_UTC").to_string(),
        git_sha: option_env!("GIT_SHA").map(String::from),
        pack_format_versions: PACK_FORMAT_VERSIONS.to_vec(),
        features: Features { enabled, disabled },
        wasi_imports: WASI_IMPORTS
            .iter()
            .map(|(iface, ver)| InterfaceBinding {
                interface: (*iface).to_string(),
                version: Some((*ver).to_string()),
                versions: Vec::new(),
                opt_in_per_pack: false,
            })
            .collect(),
        greentic_imports: GREENTIC_IMPORTS
            .iter()
            .map(|g| InterfaceBinding {
                interface: g.interface.to_string(),
                version: None,
                versions: g.versions.iter().map(|v| (*v).to_string()).collect(),
                opt_in_per_pack: g.opt_in_per_pack,
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_version_is_one() {
        let r = collect();
        assert_eq!(r.info_schema_version, 1);
    }

    #[test]
    fn features_are_disjoint() {
        let r = collect();
        for f in &r.features.enabled {
            assert!(
                !r.features.disabled.contains(f),
                "feature {f} appears in both enabled and disabled"
            );
        }
    }

    #[test]
    fn runner_version_is_non_empty() {
        let r = collect();
        assert!(!r.runner_version.is_empty());
        assert!(!r.wasmtime_version.is_empty());
    }

    /// Guards against drift between the hand-maintained `GREENTIC_IMPORTS`
    /// list and the `register_all()` call site in pack.rs. Reads pack.rs as
    /// text and asserts each interface name here actually appears somewhere
    /// in that source. A failure means the list is stale — update it or
    /// update register_all.
    #[test]
    fn greentic_imports_listed_here_are_registered() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../greentic-runner-host/src/pack.rs"
        );
        let source = std::fs::read_to_string(path).expect("read pack.rs");
        for g in GREENTIC_IMPORTS {
            let found_explicit = g
                .versions
                .iter()
                .any(|v| source.contains(&format!("{}@{}", g.interface, v)));
            // Some entries are registered via macro-generated bindings where
            // the literal "interface@version" string never appears verbatim
            // (e.g. `state_store`, `telemetry_logger`, `runner_host_http`,
            // `secrets_store_v1_1`). For those, look for the underscored
            // binding tag instead.
            let fallback = match g.interface {
                "greentic:interfaces/host" => {
                    source.contains("runner_host_http") || source.contains("runner_host_kv")
                }
                "greentic:interfaces/telemetry" => source.contains("telemetry_logger"),
                "greentic:interfaces/state-store" => source.contains("state_store"),
                "greentic:interfaces/secrets" => source.contains("secrets_store_v1_1"),
                _ => false,
            };
            assert!(
                found_explicit || fallback,
                "GREENTIC_IMPORTS entry {} not referenced in register_all() source — \
                 either the interface was renamed/removed upstream (update this list) \
                 or the test is looking at the wrong file.",
                g.interface,
            );
        }
    }
}
