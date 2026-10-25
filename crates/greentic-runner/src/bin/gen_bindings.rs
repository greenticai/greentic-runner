#![allow(clippy::missing_errors_doc)]

use anyhow::{Context, Result, bail};
use clap::Parser;
use greentic_runner::gen_bindings::input::resolve_pack_root;
use greentic_runner::gen_bindings::{self, GeneratorOptions, component};
use serde_yaml_bw as serde_yaml;
use std::{
    fs,
    path::{Path, PathBuf},
};

#[derive(Debug, Parser)]
#[command(
    name = "greentic-gen-bindings",
    about = "Generate bindings hints from a .gtpack, pack directory, or component",
    long_about = "Quick start: greentic-gen-bindings <pack>.gtpack\n\nUse --pack-dir for unpacked pack directories or --component to inspect a compiled component."
)]
struct Cli {
    /// Pack archive (.gtpack)
    #[arg(value_name = "PACK", help_heading = "Options", conflicts_with_all = ["pack_dir"])]
    pack: Option<PathBuf>,

    /// Pack directory that exposes pack.yaml + flow annotations
    #[arg(long = "pack-dir", value_name = "DIR", help_heading = "Options", conflicts_with_all = ["pack"])]
    pack_dir: Option<PathBuf>,

    /// Compiled component (.wasm) to inspect. On its own it only prints the detected
    /// capabilities; together with a pack it enables the `--strict` capability checks
    #[arg(long, value_name = "FILE", help_heading = "Advanced options")]
    component: Option<PathBuf>,

    /// Output path for the generated bindings (defaults to <PACK>.gtbind)
    #[arg(long, value_name = "FILE", help_heading = "Options")]
    out: Option<PathBuf>,

    /// Ensure the base env passthrough variables are present (they are already
    /// included by default)
    #[arg(long, help_heading = "Advanced options")]
    complete: bool,

    /// Fail if information is missing instead of inferring (unlisted MCP servers; with
    /// --component also HTTP/secrets capabilities without matching hints)
    #[arg(long, help_heading = "Advanced options")]
    strict: bool,

    /// Accepted for compatibility; the emitted YAML is always block-style
    #[arg(long, help_heading = "Advanced options")]
    pretty: bool,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    if cli.pack.is_none() && cli.pack_dir.is_none() && cli.component.is_none() {
        bail!("provide a .gtpack, --pack-dir, or --component");
    }

    let component_features = if let Some(component_path) = cli.component.clone() {
        let features = component::analyze_component(&component_path)?;
        println!("component features: {:?}", features);
        Some(features)
    } else {
        None
    };

    let common_opts = GeneratorOptions {
        strict: cli.strict,
        complete: cli.complete,
        component: component_features.clone(),
        pack_locator: None,
    };

    if let Some(pack_path) = cli.pack {
        let pack_locator = Some(format!(
            "fs://{}",
            pack_path
                .canonicalize()
                .with_context(|| {
                    format!("failed to resolve pack path {}", pack_path.display())
                })?
                .display()
        ));
        let common_opts = GeneratorOptions {
            pack_locator,
            ..common_opts
        };
        let input_is_dir = pack_path.is_dir();
        let (pack_root, _temp_dir) = resolve_pack_root(&pack_path)?;
        let metadata = gen_bindings::load_pack_root(&pack_root)?;
        let bindings = gen_bindings::generate_bindings(&metadata, common_opts)?;
        let serialized = serde_yaml::to_string(&bindings)?;
        let out_path = cli.out.unwrap_or_else(|| {
            if input_is_dir {
                pack_root.join("bindings.generated.yaml")
            } else {
                default_out_path_for_gtpack(&pack_path)
            }
        });
        if let Some(parent) = out_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create directory {}", parent.display()))?;
        }
        fs::write(&out_path, serialized)
            .with_context(|| format!("failed to write {}", out_path.display()))?;
        println!("generated bindings → {}", out_path.display());
    } else if let Some(pack_dir) = cli.pack_dir {
        if !pack_dir.is_dir() {
            bail!("pack directory {} does not exist", pack_dir.display());
        }
        let metadata = gen_bindings::load_pack_root(&pack_dir)?;
        let bindings = gen_bindings::generate_bindings(&metadata, common_opts)?;
        let serialized = serde_yaml::to_string(&bindings)?;
        let out_path = cli
            .out
            .unwrap_or_else(|| pack_dir.join("bindings.generated.yaml"));
        if let Some(parent) = out_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create directory {}", parent.display()))?;
        }
        fs::write(&out_path, serialized)
            .with_context(|| format!("failed to write {}", out_path.display()))?;
        println!("generated bindings → {}", out_path.display());
    } else if let Some(component_path) = cli.component {
        if component_features.is_some() {
            println!("component-only analysis complete");
            return Ok(());
        }
        bail!(
            "component inspection is not supported yet (tried: {})",
            component_path.display()
        );
    }

    Ok(())
}

fn default_out_path_for_gtpack(pack_path: &Path) -> PathBuf {
    let mut out = pack_path.to_path_buf();
    out.set_extension("gtbind");
    out
}
