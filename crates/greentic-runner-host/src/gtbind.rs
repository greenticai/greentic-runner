use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct PackBinding {
    pub pack_id: String,
    pub pack_ref: String,
    pub pack_locator: Option<String>,
    pub flows: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct TenantBindings {
    pub tenant: String,
    pub packs: Vec<PackBinding>,
    pub env_passthrough: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct GtBindFile {
    tenant: String,
    pack_id: String,
    pack_ref: String,
    #[serde(default)]
    pack_locator: Option<String>,
    #[serde(default)]
    flows: Vec<GtBindFlow>,
    #[serde(default)]
    env_passthrough: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct GtBindFlow {
    id: String,
}

pub fn collect_gtbind_paths(paths: &[PathBuf], dirs: &[PathBuf]) -> Result<Vec<PathBuf>> {
    let mut resolved = Vec::new();
    for path in paths {
        if path.is_dir() {
            resolved.extend(scan_dir(path)?);
        } else if path.is_file() {
            resolved.push(path.to_path_buf());
        } else {
            bail!("bindings path does not exist: {}", path.display());
        }
    }
    for dir in dirs {
        if !dir.is_dir() {
            bail!("bindings dir does not exist: {}", dir.display());
        }
        resolved.extend(scan_dir(dir)?);
    }
    resolved.sort();
    resolved.dedup();
    Ok(resolved)
}

pub fn load_gtbinds(paths: &[PathBuf]) -> Result<HashMap<String, TenantBindings>> {
    let mut tenants: HashMap<String, TenantBindings> = HashMap::new();
    for path in paths {
        let content = fs::read_to_string(path)
            .with_context(|| format!("failed to read gtbind {}", path.display()))?;
        let raw: GtBindFile = serde_yaml_bw::from_str(&content)
            .with_context(|| format!("failed to parse gtbind {}", path.display()))?;
        if raw.pack_id.trim().is_empty() {
            bail!("gtbind {} missing pack_id", path.display());
        }
        if raw.pack_ref.trim().is_empty() {
            bail!("gtbind {} missing pack_ref", path.display());
        }
        if raw.tenant.trim().is_empty() {
            bail!("gtbind {} missing tenant", path.display());
        }
        let flows = raw
            .flows
            .into_iter()
            .map(|flow| flow.id)
            .filter(|id| !id.trim().is_empty())
            .collect::<Vec<_>>();
        // `" demo"` and `"demo"` are the same tenant; key on the trimmed name.
        let tenant_key = raw.tenant.trim().to_string();
        let pack = PackBinding {
            pack_id: raw.pack_id,
            pack_ref: raw.pack_ref,
            pack_locator: raw.pack_locator,
            flows,
        };
        let entry = tenants
            .entry(tenant_key.clone())
            .or_insert_with(|| TenantBindings {
                tenant: tenant_key.clone(),
                packs: Vec::new(),
                env_passthrough: Vec::new(),
            });
        merge_pack(entry, pack)?;
        merge_env(entry, raw.env_passthrough);
    }
    Ok(tenants)
}

fn scan_dir(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut entries = Vec::new();
    for entry in fs::read_dir(dir).with_context(|| format!("failed to read {}", dir.display()))? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("gtbind") {
            entries.push(path);
        }
    }
    Ok(entries)
}

fn merge_pack(tenant: &mut TenantBindings, pack: PackBinding) -> Result<()> {
    if let Some(existing) = tenant
        .packs
        .iter_mut()
        .find(|entry| entry.pack_id == pack.pack_id)
    {
        if existing.pack_ref != pack.pack_ref {
            bail!(
                "pack_ref mismatch for tenant {} pack {}",
                tenant.tenant,
                pack.pack_id
            );
        }
        match (&existing.pack_locator, &pack.pack_locator) {
            (Some(existing), Some(incoming)) if existing != incoming => {
                bail!(
                    "pack_locator mismatch for tenant {} pack {}",
                    tenant.tenant,
                    pack.pack_id
                );
            }
            (None, Some(incoming)) => {
                existing.pack_locator = Some(incoming.clone());
            }
            _ => {}
        }
        let mut flows = HashSet::new();
        flows.extend(existing.flows.iter().cloned());
        flows.extend(pack.flows);
        existing.flows = flows.into_iter().collect();
        existing.flows.sort();
        return Ok(());
    }
    tenant.packs.push(pack);
    tenant.packs.sort_by(|a, b| a.pack_id.cmp(&b.pack_id));
    Ok(())
}

fn merge_env(tenant: &mut TenantBindings, envs: Vec<String>) {
    let mut merged = HashSet::new();
    merged.extend(tenant.env_passthrough.iter().cloned());
    merged.extend(envs);
    tenant.env_passthrough = merged.into_iter().collect();
    tenant.env_passthrough.sort();
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn write_file(path: &Path, body: &str) {
        fs::write(path, body).expect("write file");
    }

    #[test]
    fn tenant_key_is_trimmed() {
        let dir = std::env::temp_dir().join(format!("gtbind-trim-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("dir");
        let a = dir.join("a.gtbind");
        let b = dir.join("b.gtbind");
        std::fs::write(&a, "tenant: \" demo \"\npack_id: pack\npack_ref: pack@1\n").expect("a");
        std::fs::write(&b, "tenant: demo\npack_id: pack2\npack_ref: pack2@1\n").expect("b");
        let tenants = load_gtbinds(&[a, b]).expect("load");
        std::fs::remove_dir_all(&dir).ok();
        assert_eq!(tenants.len(), 1);
        let demo = tenants.get("demo").expect("trimmed key");
        assert_eq!(demo.tenant, "demo");
        assert_eq!(demo.packs.len(), 2);
    }

    #[test]
    fn collect_gtbind_paths_scans_dirs_and_dedups() {
        let temp = TempDir::new().expect("tempdir");
        let a = temp.path().join("a.gtbind");
        let b = temp.path().join("b.gtbind");
        let ignored = temp.path().join("ignored.txt");
        write_file(&a, "tenant: demo\npack_id: pack\npack_ref: pack@1\n");
        write_file(&b, "tenant: demo\npack_id: pack2\npack_ref: pack2@1\n");
        write_file(&ignored, "ignore");

        let paths = collect_gtbind_paths(std::slice::from_ref(&a), &[temp.path().to_path_buf()])
            .expect("collect paths");

        assert_eq!(paths, vec![a, b]);
    }

    #[test]
    fn load_gtbinds_merges_flows_locators_and_env() {
        let temp = TempDir::new().expect("tempdir");
        let one = temp.path().join("one.gtbind");
        let two = temp.path().join("two.gtbind");
        write_file(
            &one,
            r#"
tenant: demo
pack_id: pack.main
pack_ref: pack.main@1.0.0
pack_locator: fs:///packs/main.gtpack
flows:
  - id: flow-a
  - id: ""
env_passthrough: [TOKEN, API_KEY]
"#,
        );
        write_file(
            &two,
            r#"
tenant: demo
pack_id: pack.main
pack_ref: pack.main@1.0.0
flows:
  - id: flow-b
env_passthrough: [API_KEY, SECRET]
"#,
        );

        let tenants = load_gtbinds(&[one, two]).expect("load gtbinds");
        let tenant = tenants.get("demo").expect("tenant");
        assert_eq!(tenant.packs.len(), 1);
        assert_eq!(tenant.packs[0].flows, vec!["flow-a", "flow-b"]);
        assert_eq!(
            tenant.packs[0].pack_locator.as_deref(),
            Some("fs:///packs/main.gtpack")
        );
        assert_eq!(tenant.env_passthrough, vec!["API_KEY", "SECRET", "TOKEN"]);
    }

    #[test]
    fn load_gtbinds_rejects_missing_required_fields() {
        let temp = TempDir::new().expect("tempdir");
        let file = temp.path().join("broken.gtbind");
        write_file(&file, "tenant: demo\npack_id: ''\npack_ref: pack@1\n");

        assert!(load_gtbinds(&[file]).is_err());
    }

    #[test]
    fn merge_pack_rejects_conflicting_refs_and_locators() {
        let mut tenant = TenantBindings {
            tenant: "demo".into(),
            packs: vec![PackBinding {
                pack_id: "pack.main".into(),
                pack_ref: "pack.main@1.0.0".into(),
                pack_locator: Some("fs:///packs/a.gtpack".into()),
                flows: vec!["flow-a".into()],
            }],
            env_passthrough: Vec::new(),
        };

        assert!(
            merge_pack(
                &mut tenant,
                PackBinding {
                    pack_id: "pack.main".into(),
                    pack_ref: "pack.main@2.0.0".into(),
                    pack_locator: Some("fs:///packs/a.gtpack".into()),
                    flows: vec![],
                }
            )
            .is_err()
        );

        assert!(
            merge_pack(
                &mut tenant,
                PackBinding {
                    pack_id: "pack.main".into(),
                    pack_ref: "pack.main@1.0.0".into(),
                    pack_locator: Some("fs:///packs/b.gtpack".into()),
                    flows: vec![],
                }
            )
            .is_err()
        );
    }
}
