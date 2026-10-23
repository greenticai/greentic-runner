use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_yaml_bw::{self as serde_yaml, Value};
use std::{collections::HashSet, fs, path::Path};
use url::Url;

use self::component::ComponentFeatures;
use greentic_types::cbor::decode_pack_manifest;

pub mod component;
pub mod input;

fn yaml_string(value: impl Into<String>) -> Value {
    Value::String(value.into(), None)
}

#[derive(Debug)]
pub struct PackMetadata {
    pub tenant: String,
    pub pack_id: String,
    pub pack_ref: String,
    pub flows: Vec<FlowMetadata>,
    pub hints: BindingsHints,
}

#[derive(Debug)]
pub struct FlowMetadata {
    pub name: String,
    pub document: Value,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct BindingsHints {
    #[serde(default)]
    pub network: NetworkHints,
    #[serde(default)]
    pub secrets: SecretsHints,
    #[serde(default)]
    pub env: EnvHints,
    #[serde(default)]
    pub mcp: McpHints,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct NetworkHints {
    #[serde(default)]
    pub allow: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct SecretsHints {
    #[serde(default)]
    pub required: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct EnvHints {
    #[serde(default)]
    pub passthrough: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct McpHints {
    #[serde(default)]
    pub servers: Vec<McpServer>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct McpServer {
    pub name: String,
    pub transport: String,
    pub endpoint: String,
    #[serde(default)]
    pub caps: Vec<String>,
}

pub fn load_pack(pack_dir: &Path) -> Result<PackMetadata> {
    // The path comes straight from the user (CLI argument), so there is no
    // trust boundary to enforce: canonicalize it directly. This accepts
    // relative paths containing `..` that a root-confined check would reject.
    let pack_dir = pack_dir
        .canonicalize()
        .with_context(|| format!("pack directory {} does not exist", pack_dir.display()))?;
    if !pack_dir.is_dir() {
        anyhow::bail!("pack directory {} does not exist", pack_dir.display());
    }

    let manifest = pack_dir.join("pack.yaml");
    let content = fs::read_to_string(&manifest)
        .with_context(|| format!("failed to read {}", manifest.display()))?;
    let pack_manifest: PackManifest =
        serde_yaml::from_str(&content).with_context(|| "failed to parse pack manifest")?;

    let hints_path = pack_dir.join("bindings.hints.yaml");
    let hints = if hints_path.exists() {
        serde_yaml::from_reader(fs::File::open(&hints_path)?)
            .with_context(|| format!("failed to read hints {}", hints_path.display()))?
    } else {
        BindingsHints::default()
    };

    let flows_dir = pack_dir.join("flows");
    let mut flows = Vec::new();
    if flows_dir.is_dir() {
        let mut paths = Vec::new();
        for entry in fs::read_dir(&flows_dir)? {
            let path = entry?.path();
            if path.extension().and_then(|s| s.to_str()) == Some("yaml") {
                paths.push(path);
            }
        }
        // read_dir order is filesystem-dependent; sort for deterministic output.
        paths.sort();
        for path in paths {
            flows.push(load_flow(&path)?);
        }
    }

    let pack_id = pack_manifest
        .pack_id
        .filter(|id| !id.trim().is_empty())
        .ok_or_else(|| anyhow::anyhow!("pack.yaml missing pack_id"))?;
    let version = pack_manifest
        .version
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| anyhow::anyhow!("pack.yaml missing version"))?;
    let pack_ref = format!("{}@{}", pack_id, version);

    let tenant = pack_manifest
        .name
        .or_else(|| {
            pack_dir
                .file_name()
                .and_then(|n| n.to_str())
                .map(|s| s.to_string())
        })
        .unwrap_or_else(|| pack_id.clone());

    Ok(PackMetadata {
        tenant,
        pack_id,
        pack_ref,
        flows,
        hints,
    })
}

pub fn load_pack_root(pack_root: &Path) -> Result<PackMetadata> {
    let manifest = pack_root.join("pack.yaml");
    if manifest.is_file() {
        return load_pack(pack_root);
    }
    let cbor_path = pack_root.join("manifest.cbor");
    if cbor_path.is_file() {
        return load_pack_manifest_cbor(pack_root, &cbor_path);
    }
    bail!(
        "pack directory {} does not include pack.yaml or manifest.cbor",
        pack_root.display()
    );
}

fn load_flow(path: &Path) -> Result<FlowMetadata> {
    let content = fs::read_to_string(path)?;
    let fallback = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("<unknown>");
    load_flow_content(&content, fallback, &path.display().to_string())
}

fn load_flow_content(content: &str, fallback_name: &str, label: &str) -> Result<FlowMetadata> {
    let parsed: Value =
        serde_yaml::from_str(content).with_context(|| format!("failed to parse flow {}", label))?;
    let name = parsed
        .get("name")
        .and_then(Value::as_str)
        .map(|s| s.to_string())
        .unwrap_or_else(|| fallback_name.to_string());
    Ok(FlowMetadata {
        name,
        document: parsed,
    })
}

fn load_pack_manifest_cbor(pack_root: &Path, cbor_path: &Path) -> Result<PackMetadata> {
    let bytes =
        fs::read(cbor_path).with_context(|| format!("failed to read {}", cbor_path.display()))?;
    let manifest = decode_pack_manifest(&bytes)
        .with_context(|| format!("failed to decode {}", cbor_path.display()))?;
    let flows = manifest
        .flows
        .iter()
        .map(|entry| {
            let serialized = serde_yaml::to_string(&entry.flow)
                .context("failed to serialize flow from manifest")?;
            let parsed: Value =
                serde_yaml::from_str(&serialized).context("failed to parse flow from manifest")?;
            Ok(FlowMetadata {
                name: entry.id.to_string(),
                document: parsed,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let hints_path = pack_root.join("bindings.hints.yaml");
    let hints = if hints_path.exists() {
        serde_yaml::from_reader(fs::File::open(&hints_path)?)
            .with_context(|| format!("failed to read hints {}", hints_path.display()))?
    } else {
        BindingsHints::default()
    };

    let pack_id = manifest.pack_id.to_string();
    let pack_ref = format!("{}@{}", pack_id, manifest.version);
    let tenant = pack_id.clone();

    Ok(PackMetadata {
        tenant,
        pack_id,
        pack_ref,
        flows,
        hints,
    })
}

#[derive(Debug, Deserialize)]
struct PackManifest {
    name: Option<String>,
    pack_id: Option<String>,
    version: Option<String>,
}

#[derive(Clone, Default)]
pub struct GeneratorOptions {
    pub strict: bool,
    pub complete: bool,
    pub component: Option<ComponentFeatures>,
    pub pack_locator: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct GeneratedBindings {
    pub tenant: String,
    pub pack_id: String,
    pub pack_ref: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pack_locator: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub env_passthrough: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub network_allow: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub secrets_required: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub flows: Vec<FlowHint>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub mcp_servers: Vec<McpServer>,
}

#[derive(Debug, Serialize)]
pub struct FlowHint {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub urls: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub secrets: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub env: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub mcp_components: Vec<String>,
}

impl FlowHint {
    fn from_flow(flow: &FlowMetadata) -> Self {
        let mut bindings = collect_flow_bindings(&flow.document);
        let meta = find_meta_bindings(&flow.document);
        bindings.urls.extend(meta.urls);
        bindings.secrets.extend(meta.secrets);
        bindings.env.extend(meta.env);
        bindings.mcp_components.extend(meta.mcp_components);
        FlowHint {
            id: flow.name.clone(),
            name: Some(flow.name.clone()),
            urls: bindings.urls.clone(),
            secrets: bindings.secrets.clone(),
            env: bindings.env.clone(),
            mcp_components: bindings.mcp_components.clone(),
        }
    }
}

#[derive(Default)]
struct FlowBindings {
    urls: Vec<String>,
    secrets: Vec<String>,
    env: Vec<String>,
    mcp_components: Vec<String>,
}

fn collect_flow_bindings(doc: &Value) -> FlowBindings {
    let mut bindings = FlowBindings::default();
    scan_value_for_placeholders(doc, &mut bindings);
    collect_mcp_components(doc, &mut bindings);
    bindings
}

fn scan_value_for_placeholders(value: &Value, bindings: &mut FlowBindings) {
    match value {
        Value::String(s, _) => {
            bindings.secrets.extend(extract_placeholders(s, "secrets."));
            bindings.env.extend(extract_placeholders(s, "env."));
            if let Some(origin) = extract_origin(s) {
                bindings.urls.push(origin);
            }
        }
        Value::Sequence(seq) => {
            for item in seq {
                scan_value_for_placeholders(item, bindings);
            }
        }
        Value::Mapping(map) => {
            for (_, item) in map {
                scan_value_for_placeholders(item, bindings);
            }
        }
        _ => {}
    }
}

fn collect_mcp_components(value: &Value, bindings: &mut FlowBindings) {
    match value {
        Value::Mapping(map) => {
            let exec_key = yaml_string("mcp.exec");
            let component_key = yaml_string("component");
            if let Some(Value::Mapping(exec_map)) = map.get(&exec_key)
                && let Some(Value::String(component, _)) = exec_map.get(&component_key)
            {
                bindings.mcp_components.push(component.clone());
            }
            for (_, v) in map {
                collect_mcp_components(v, bindings);
            }
        }
        Value::Sequence(seq) => {
            for item in seq {
                collect_mcp_components(item, bindings);
            }
        }
        _ => {}
    }
}

/// Extract identifiers following `prefix` (e.g. `secrets.`) but only when they
/// appear inside a `{{ ... }}` template expression, so plain text such as
/// "see env.docs" is not mistaken for a placeholder.
fn extract_placeholders(value: &str, prefix: &str) -> Vec<String> {
    let mut results = Vec::new();
    let mut remaining = value;
    while let Some(open) = remaining.find("{{") {
        let after_open = &remaining[open + 2..];
        let Some(close) = after_open.find("}}") else {
            break;
        };
        collect_prefixed_identifiers(&after_open[..close], prefix, &mut results);
        remaining = &after_open[close + 2..];
    }
    results
}

fn collect_prefixed_identifiers(expr: &str, prefix: &str, results: &mut Vec<String>) {
    let mut start = 0;
    while let Some(idx) = expr[start..].find(prefix) {
        let at = start + idx;
        let after = at + prefix.len();
        let preceded_by_ident = expr[..at]
            .chars()
            .next_back()
            .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.');
        let rest = &expr[after..];
        let end = rest
            .find(|c: char| !c.is_ascii_alphanumeric() && c != '_')
            .unwrap_or(rest.len());
        if !preceded_by_ident && end > 0 {
            results.push(rest[..end].to_string());
        }
        start = after + end;
    }
}

fn extract_origin(text: &str) -> Option<String> {
    if let Ok(url) = Url::parse(text)
        && url.scheme().starts_with("http")
        && let Some(host) = url.host_str()
    {
        let port = match url.port() {
            Some(p) => format!(":{}", p),
            None => "".to_string(),
        };
        return Some(format!("{}://{}{}", url.scheme(), host, port));
    }
    None
}

pub fn generate_bindings(
    metadata: &PackMetadata,
    opts: GeneratorOptions,
) -> Result<GeneratedBindings> {
    let mut env = base_env_passthrough();
    env.extend(metadata.hints.env.passthrough.iter().cloned());
    let flows: Vec<FlowHint> = metadata.flows.iter().map(FlowHint::from_flow).collect();
    for flow in &flows {
        env.extend(flow.env.clone());
    }

    let mut env = unique_sorted(env);

    let mut secrets = metadata.hints.secrets.required.clone();
    for flow in &flows {
        secrets.extend(flow.secrets.clone());
    }
    let secrets = unique_sorted(secrets);

    let mut network = metadata.hints.network.allow.clone();
    for flow in &flows {
        network.extend(flow.urls.clone());
    }
    let network = unique_sorted(network);

    if opts.complete {
        for value in base_env_passthrough() {
            if !env.contains(&value) {
                env.push(value);
            }
        }
    }

    let mut mcp_servers = metadata.hints.mcp.servers.clone();
    let mut referenced_components = Vec::new();
    for flow in &flows {
        referenced_components.extend(flow.mcp_components.clone());
    }
    let referenced_components = unique_sorted(referenced_components);
    for component in referenced_components {
        if mcp_servers.iter().any(|server| server.name == component) {
            continue;
        }
        if opts.strict {
            bail!(
                "MCP component '{}' referenced but no server hint; add hints or rerun with --complete",
                component
            );
        }
        mcp_servers.push(McpServer {
            name: component.clone(),
            transport: "websocket".into(),
            endpoint: "ws://localhost:9000".into(),
            caps: Vec::new(),
        });
    }

    if opts.strict {
        if opts.component.as_ref().map(|c| c.http).unwrap_or(false) && network.is_empty() {
            bail!(
                "HTTP capability detected but no network allow rules; add hints or use --complete"
            );
        }
        if opts.component.as_ref().map(|c| c.secrets).unwrap_or(false) && secrets.is_empty() {
            bail!(
                "Secrets capability detected but no secrets.required hints; add hints or use --complete"
            );
        }
    }

    Ok(GeneratedBindings {
        tenant: metadata.tenant.clone(),
        pack_id: metadata.pack_id.clone(),
        pack_ref: metadata.pack_ref.clone(),
        pack_locator: opts.pack_locator.clone(),
        env_passthrough: env,
        network_allow: network,
        secrets_required: secrets,
        flows,
        mcp_servers,
    })
}

fn base_env_passthrough() -> Vec<String> {
    vec![
        "RUST_LOG".into(),
        "OTEL_EXPORTER_OTLP_ENDPOINT".into(),
        "OTEL_RESOURCE_ATTRIBUTES".into(),
    ]
}

fn unique_sorted(values: Vec<String>) -> Vec<String> {
    let mut set = HashSet::new();
    let mut result = Vec::new();
    for v in values {
        if set.insert(v.clone()) {
            result.push(v);
        }
    }
    result.sort_unstable();
    result
}

fn find_meta_bindings(meta: &Value) -> FlowBindings {
    let mut hints = FlowBindings::default();
    if let Some(bindings) = find_bindings_value(meta) {
        hints.urls.extend(extract_string_list(bindings, "urls"));
        hints
            .secrets
            .extend(extract_string_list(bindings, "secrets"));
        hints.env.extend(extract_string_list(bindings, "env"));
        hints
            .mcp_components
            .extend(extract_string_list(bindings, "mcp_components"));
    }
    hints
}

fn find_bindings_value(meta: &Value) -> Option<&Value> {
    if let Value::Mapping(map) = meta {
        let bindings_key = yaml_string("bindings");
        if let Some(bindings) = map.get(&bindings_key) {
            return Some(bindings);
        }
        let meta_key = yaml_string("meta");
        if let Some(Value::Mapping(inner_map)) = map.get(&meta_key) {
            let inner_bindings_key = yaml_string("bindings");
            return inner_map.get(&inner_bindings_key);
        }
    }
    None
}

fn extract_string_list(bindings: &Value, key: &str) -> Vec<String> {
    if let Value::Mapping(map) = bindings {
        let key_value = yaml_string(key);
        if let Some(value) = map.get(&key_value) {
            return value_to_list(value);
        }
    }
    Vec::new()
}

fn value_to_list(value: &Value) -> Vec<String> {
    match value {
        Value::Sequence(seq) => seq
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect(),
        Value::String(s, _) => vec![s.clone()],
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholders_are_only_read_inside_template_braces() {
        assert_eq!(
            extract_placeholders("a {{ env.FOO }} b {{secrets.BAR}}", "env."),
            vec!["FOO".to_string()]
        );
        assert!(extract_placeholders("see env.DOCS for details", "env.").is_empty());
        assert!(extract_placeholders("{{ myenv.X }}", "env.").is_empty());
        assert!(extract_placeholders("{{ env.UNCLOSED", "env.").is_empty());
    }

    #[test]
    fn complete_mode_does_not_add_wildcard_egress() {
        let metadata = PackMetadata {
            tenant: "t".into(),
            pack_id: "p".into(),
            pack_ref: "p@1".into(),
            flows: Vec::new(),
            hints: BindingsHints::default(),
        };
        let out = generate_bindings(
            &metadata,
            GeneratorOptions {
                complete: true,
                ..Default::default()
            },
        )
        .expect("generate");
        assert!(out.network_allow.is_empty());
    }

    #[test]
    fn relative_pack_dir_with_parent_components_loads_and_flows_are_sorted() {
        let temp = tempfile::tempdir().expect("tmp");
        let pack = temp.path().join("pack");
        fs::create_dir_all(pack.join("flows")).expect("flows dir");
        fs::write(pack.join("pack.yaml"), "pack_id: p\nversion: 0.1.0\n").expect("manifest");
        for name in ["c", "a", "b"] {
            fs::write(
                pack.join("flows").join(format!("{name}.yaml")),
                "nodes: {}\n",
            )
            .expect("flow");
        }
        let with_dotdot = pack.join("..").join("pack");
        let meta = load_pack(&with_dotdot).expect("load pack with ..");
        let names: Vec<_> = meta.flows.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, vec!["a", "b", "c"]);
    }
}
