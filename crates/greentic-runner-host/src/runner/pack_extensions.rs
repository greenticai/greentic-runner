//! Design extensions carried inside a `.gtpack`.
//!
//! # Why this exists
//!
//! A `.gtxpack` design extension bound as a tool on an agentic worker is
//! declared to the pack build as a dependency, and greentic-pack reads its
//! `describe.json` to generate `setup.yaml` + `secret-requirements.json` — so a
//! cloud operator is asked for that tool's API key. Until now nothing carried
//! the archive any further: `agent_node::extension_discovery_dir` resolves
//! `GREENTIC_EXTENSIONS_DIR` or `$HOME/.greentic/extensions`, nothing writes
//! either inside a container, and `discovery::scan_kind_dir` returned an empty
//! list. The tool was dropped with a `warn`, the deploy reported success, the
//! worker booted, and `list_tools` came back empty — after the operator had
//! already supplied the credential.
//!
//! This module closes the last hop: the extensions travel in the pack, and the
//! runner loads them from there. It follows [`component_source_from_packs`] and
//! [`mcp_source_from_packs`], which already moved resolution out of "the
//! environment of this lane" and into "the contents of the pack". Design
//! extensions were the last of the three still on the old model.
//!
//! The rejected alternative — unpacking to `GREENTIC_EXTENSIONS_DIR` at deploy
//! time — would make the filesystem and the environment part of the deploy
//! contract, so every target needs its own unpack step and forgetting one fails
//! silently. That is what left the `mcp:` lane needing an env-projection seam
//! that still does not exist.
//!
//! # CONTRACT — the in-pack extension layout, shared with greentic-pack
//!
//! A `.gtpack` in a customer's hands cannot be renamed later, so this layout is
//! fixed at the first release that emits it and is not ours alone to change. It
//! is recorded verbatim at the producing site (greentic-pack
//! `crates/packc/src/build.rs`) and in greentic-designer's external-RAG design
//! spec.
//!
//!   1. A tool extension's `.gtxpack` travels at `extensions/<name>.gtxpack`,
//!      one flat level. There is no kind partition — NOT `extensions/design/`.
//!   2. A consumer enumerates extensions by FILTERING ON THE `.gtxpack`
//!      SUFFIX. It must not treat `extensions/` as homogeneous.
//!   3. Rule 2 is load-bearing, not defensive: `extensions/` is ALREADY an
//!      authoring convention in a pack source tree — the wizard writes
//!      `extensions/*.json` manifest sidecars there and they are walked into the
//!      archive verbatim. Those `.json` files are not extensions and predate
//!      this feature.
//!
//! Flat rather than kind-partitioned because placing by kind would make the
//! producer read and trust `describe.json`'s `kind` for a path decision baked
//! into shipped bytes. The consumer opens each archive anyway and classifies
//! from the inside, where the answer is authoritative.
//!
//! # What is verified
//!
//! Nothing here is a second loader. Each archive is unpacked into a staging
//! directory and handed to [`ExtensionRuntime::register_loaded_from_dir`] — the
//! same entry point the on-disk scan uses — so the pack path runs the identical
//! gate: `verify_dir_signature` (describe self-consistency, then the TOFU
//! publisher-key anchor) and `verify_dir_manifest` (the `manifest.json`
//! whole-archive integrity ledger, bound into the signed describe through
//! `manifestSha256`). A pack-carried archive is therefore not a way around
//! signing; it is the same bytes reaching the same check by a different road.
//!
//! Unpacking adds one check of its own, before the loader sees anything: an
//! archive entry whose path escapes the staging directory is refused outright
//! rather than sanitised, so a hostile `.gtxpack` cannot write over the host.
//!
//! [`component_source_from_packs`]: crate::runner::agent_node::component_source_from_packs
//! [`mcp_source_from_packs`]: crate::runner::agent_node::mcp_source_from_packs

use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};

use crate::pack::PackRuntime;

/// The single directory level the contract places extensions under.
pub const EXTENSIONS_PREFIX: &str = "extensions/";

/// The suffix that distinguishes an extension from the `.json` sidecars that
/// share the directory with it (contract rule 2).
pub const ARCHIVE_SUFFIX: &str = ".gtxpack";

/// Operator opt-out, mirroring `GREENTIC_AW_MCP` / `GREENTIC_AW_COMPONENT_TOOLS`
/// / `GREENTIC_AW_FLOW_TOOLS`. Set to `0` to ignore pack-carried extensions and
/// keep the on-disk scan as the only source.
pub const OPT_OUT_ENV: &str = "GREENTIC_AW_PACK_EXTENSIONS";

/// Written last into a staged directory, only after every archive entry landed.
///
/// The directory appears at the first write, so its existence proves an unpack
/// STARTED. Two tenants of one process stage the same digest, and a half-written
/// tree that a later tenant reused as "already staged" would load an extension
/// missing files — silently, since the manifest ledger would then fail with a
/// hash mismatch rather than the truthful "the unpack was interrupted".
const COMPLETE_MARKER: &str = ".staged";

/// Is `entry` one of the extension archives the contract describes?
///
/// Rejects the `.json` sidecars that predate this feature, anything nested
/// deeper than the one flat level, and a bare `extensions/.gtxpack`.
#[must_use]
pub fn is_extension_archive_entry(entry: &str) -> bool {
    let Some(name) = entry.strip_prefix(EXTENSIONS_PREFIX) else {
        return false;
    };
    if name.contains('/') {
        return false;
    }
    let Some(stem) = name.strip_suffix(ARCHIVE_SUFFIX) else {
        return false;
    };
    !stem.is_empty()
}

/// One `.gtxpack` unpacked out of a pack and ready for the verified loader.
#[derive(Debug, Clone)]
pub(crate) struct StagedExtension {
    /// The archive-relative entry it came from, for logs an operator can act on.
    pub(crate) entry_name: String,
    /// The staging directory, shaped exactly like an installed extension dir.
    pub(crate) dir: PathBuf,
    /// `describe.json`'s `metadata.id`, when it could be read.
    ///
    /// `None` is not fatal: the id is only needed to decide precedence against
    /// the on-disk set, and an archive whose describe cannot be read will be
    /// refused by the loader moments later with a far better message than
    /// anything this module could invent.
    pub(crate) extension_id: Option<String>,
}

/// One `.gtxpack` this host could not present to the loader at all.
///
/// Kept rather than dropped: an extension that vanishes without a trace is the
/// exact failure this whole module exists to end.
#[derive(Debug, Clone)]
pub(crate) struct StagingFailure {
    pub(crate) entry_name: String,
    pub(crate) reason: String,
}

/// The outcome of staging every archive a set of packs carries.
#[derive(Debug, Default)]
pub(crate) struct PackExtensions {
    pub(crate) staged: Vec<StagedExtension>,
    pub(crate) failures: Vec<StagingFailure>,
}

/// Process-wide staging root for extensions unpacked out of a pack.
///
/// Process-scoped rather than per-call for two reasons. A [`LoadedExtension`]
/// retains its `source_dir` for the life of the runtime and `ExtensionRuntime`
/// offers no seam to hang a lifetime guard on, so a per-call `TempDir` would be
/// dropped out from under a loaded extension. And every tenant of one process
/// stages the same archives, so a shared, digest-keyed root turns N tenants
/// into one unpack rather than N.
///
/// A `TempDir` inside a `static` never runs its `Drop`, so the tree survives
/// until the OS temp reaper takes it. That is the deliberate trade: the
/// alternative deletes a directory the runtime is still pointing at.
///
/// [`LoadedExtension`]: greentic_ext_runtime::LoadedExtension
static STAGING_ROOT: OnceLock<Option<tempfile::TempDir>> = OnceLock::new();

fn staging_root() -> Option<&'static Path> {
    STAGING_ROOT
        .get_or_init(|| {
            match tempfile::Builder::new()
                .prefix("greentic-pack-extensions-")
                .tempdir()
            {
                Ok(dir) => Some(dir),
                Err(error) => {
                    tracing::warn!(
                        error = %error,
                        "cannot create a staging directory; pack-carried extensions are unavailable"
                    );
                    None
                }
            }
        })
        .as_ref()
        .map(tempfile::TempDir::path)
}

/// Unpack `bytes` into `dest`, refusing any entry that escapes it.
///
/// `enclosed_name` returning `None` is the refusal: an entry naming `..` or an
/// absolute path is a hostile archive, and sanitising the path would silently
/// load an extension whose contents are not what it claims.
fn unpack_archive(bytes: &[u8], dest: &Path) -> Result<()> {
    let mut archive =
        zip::ZipArchive::new(Cursor::new(bytes)).context("read the extension archive")?;
    for index in 0..archive.len() {
        let mut entry = archive.by_index(index)?;
        let raw_name = entry.name().to_string();
        let Some(relative) = entry.enclosed_name() else {
            bail!("archive entry escapes the staging directory: {raw_name}");
        };
        let out_path = dest.join(&relative);
        if entry.is_dir() {
            std::fs::create_dir_all(&out_path)?;
            continue;
        }
        if let Some(parent) = out_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut contents = Vec::new();
        entry.read_to_end(&mut contents)?;
        std::fs::write(&out_path, &contents)
            .with_context(|| format!("write {}", out_path.display()))?;
    }
    Ok(())
}

/// Read `describe.json`'s `metadata.id` out of a staged directory.
///
/// Deliberately a shallow `serde_json::Value` read rather than a typed decode:
/// the only thing needed here is the identity used for the precedence decision,
/// and refusing a describe this module cannot fully model would take the
/// refusal away from the loader, which does it properly.
fn staged_extension_id(dir: &Path) -> Option<String> {
    let bytes = std::fs::read(dir.join("describe.json")).ok()?;
    let value: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    value
        .get("metadata")?
        .get("id")?
        .as_str()
        .map(str::to_string)
}

/// Stage one archive under the process staging root, keyed by its own digest.
///
/// Digest-keyed so two tenants carrying the same extension unpack it once, and
/// so a re-run within one process is idempotent.
fn stage_one(entry_name: &str, bytes: &[u8]) -> Result<StagedExtension> {
    let root = staging_root().context("no staging directory is available")?;
    let digest = hex::encode(Sha256::digest(bytes));
    let dir = root.join(&digest[..32]);

    if !dir.join(COMPLETE_MARKER).exists() {
        // A previous attempt may have died partway through. Clear rather than
        // unpack over the top: a stale truncated file the current archive no
        // longer lists would survive and be hashed against the ledger.
        if dir.exists() {
            std::fs::remove_dir_all(&dir)
                .with_context(|| format!("clear partial staging at {}", dir.display()))?;
        }
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("create staging dir {}", dir.display()))?;
        unpack_archive(bytes, &dir)?;
        std::fs::write(dir.join(COMPLETE_MARKER), b"")
            .with_context(|| format!("mark {} staged", dir.display()))?;
    }

    let extension_id = staged_extension_id(&dir);
    Ok(StagedExtension {
        entry_name: entry_name.to_string(),
        dir,
        extension_id,
    })
}

/// Stage every `(entry_name, bytes)` pair, recording per-archive failures
/// instead of aborting on the first one.
///
/// One unreadable archive must not cost an operator the other tools they bound,
/// and it must not vanish either — hence two lists rather than a `Result`.
pub(crate) fn stage_archives<I>(entries: I) -> PackExtensions
where
    I: IntoIterator<Item = (String, Vec<u8>)>,
{
    let mut out = PackExtensions::default();
    for (entry_name, bytes) in entries {
        match stage_one(&entry_name, &bytes) {
            Ok(staged) => out.staged.push(staged),
            Err(error) => out.failures.push(StagingFailure {
                entry_name,
                reason: format!("{error:#}"),
            }),
        }
    }
    out
}

/// Collect and stage every extension archive the loaded packs carry.
///
/// Entry names are deduplicated across packs, FIRST PACK WINS — the same rule
/// [`mcp_source_from_packs`] applies to server ids, and for the same reason: a
/// deterministic winner beats a load order nobody controls.
///
/// [`mcp_source_from_packs`]: crate::runner::agent_node::mcp_source_from_packs
pub(crate) fn stage_from_packs(packs: &[Arc<PackRuntime>]) -> PackExtensions {
    let mut seen = std::collections::HashSet::new();
    let mut entries = Vec::new();
    let mut failures = Vec::new();
    for pack in packs {
        for entry_name in pack.extension_archive_entries() {
            if !seen.insert(entry_name.clone()) {
                continue;
            }
            match pack.read_pack_file(&entry_name) {
                Some(bytes) => entries.push((entry_name, bytes)),
                None => failures.push(StagingFailure {
                    entry_name,
                    reason: "the pack lists this entry but it could not be read".to_string(),
                }),
            }
        }
    }
    let mut out = stage_archives(entries);
    out.failures.extend(failures);
    out
}

/// What [`register_from_packs`] did, so the caller can log one honest line.
#[cfg(feature = "agentic-worker")]
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct PackExtensionLoad {
    /// Registered into the runtime from a pack.
    pub(crate) loaded: usize,
    /// Present in a pack but already supplied by the on-disk scan.
    pub(crate) shadowed: usize,
    /// Could not be staged, or the verified loader refused them.
    pub(crate) failed: usize,
}

/// Does the on-disk set already supply this extension?
///
/// # Precedence: DISK WINS, the pack is the fallback
///
/// This is the same direction as the agent loop's MCP source
/// (`mcp_source_from_env().or_else(mcp_source_from_packs(..))`) and the OPPOSITE
/// of the flow MCP node, whose pack route wins. The asymmetry between those two
/// is deliberate upstream, and the tie-breaker here is which choice can only
/// improve things:
///
/// - An on-disk extension is one an operator installed or updated deliberately
///   (`gtdx install`, the designer's admin auto-sync, the bundled unpack). It is
///   the newer artefact by construction in every lane that has one.
/// - A pack-carried archive is frozen at pack-build time. Preferring it would
///   silently pin every designer and desktop lane to whatever version was
///   current when the pack was built — downgrading environments that work today
///   in order to fix one that does not.
///
/// The cost, stated plainly: a host that has a STALE on-disk copy keeps it, even
/// when the pack carries a newer one. That is invisible, and it is accepted
/// because in the lane this closes — a k8s or Cloud Run container — the
/// directory is empty, so the pack is the only source and the rule never fires.
/// Making the newer of the two win needs a version comparison the ids do not
/// carry (`ExtensionId` is `metadata.id`, version-free), and would have to
/// decide what "newer" means for two unrelated publishers.
#[cfg(feature = "agentic-worker")]
fn is_shadowed(staged: &StagedExtension, on_disk: &std::collections::HashSet<String>) -> bool {
    staged
        .extension_id
        .as_deref()
        .is_some_and(|id| on_disk.contains(id))
}

/// Register every extension the loaded packs carry into `runtime`, on top of
/// whatever the on-disk scan already loaded.
///
/// Must be called AFTER the on-disk scan: `register_loaded_from_dir` inserts by
/// `ExtensionId` and would otherwise overwrite a disk-loaded extension with the
/// pack's copy, inverting the precedence documented on [`is_shadowed`].
///
/// Never fails. A refused archive is one tool an operator has to be told about;
/// it is not a reason to take a worker down.
#[cfg(feature = "agentic-worker")]
pub(crate) fn register_from_packs(
    runtime: &mut greentic_ext_runtime::ExtensionRuntime,
    packs: &[Arc<PackRuntime>],
) -> PackExtensionLoad {
    let mut report = PackExtensionLoad::default();
    if packs.is_empty() {
        return report;
    }
    if std::env::var(OPT_OUT_ENV).ok().as_deref() == Some("0") {
        tracing::info!("{OPT_OUT_ENV}=0; pack-carried design extensions disabled");
        return report;
    }

    let PackExtensions { staged, failures } = stage_from_packs(packs);
    for failure in &failures {
        report.failed += 1;
        tracing::warn!(
            entry = %failure.entry_name,
            reason = %failure.reason,
            "pack-carried extension could not be staged; its tools will not resolve"
        );
    }

    let on_disk: std::collections::HashSet<String> = runtime
        .loaded()
        .keys()
        .map(|id| id.as_str().to_string())
        .collect();

    for extension in staged {
        if is_shadowed(&extension, &on_disk) {
            report.shadowed += 1;
            tracing::info!(
                entry = %extension.entry_name,
                extension_id = extension.extension_id.as_deref().unwrap_or_default(),
                "pack-carried extension is already installed on disk; keeping the installed copy"
            );
            continue;
        }
        match runtime.register_loaded_from_dir(&extension.dir) {
            Ok(()) => {
                report.loaded += 1;
                tracing::info!(
                    entry = %extension.entry_name,
                    extension_id = extension.extension_id.as_deref().unwrap_or_default(),
                    "loaded design extension from the pack"
                );
            }
            Err(error) => {
                report.failed += 1;
                tracing::warn!(
                    entry = %extension.entry_name,
                    extension_id = extension.extension_id.as_deref().unwrap_or_default(),
                    error = %error,
                    "pack-carried extension failed to load; its tools will not resolve"
                );
            }
        }
    }
    report
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    /// Build a `.gtxpack`-shaped zip in memory from `(name, bytes)` entries.
    fn zip_bytes(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
        let options: zip::write::FileOptions<'_, ()> =
            zip::write::FileOptions::default().compression_method(zip::CompressionMethod::Stored);
        for (name, bytes) in entries {
            writer.start_file(*name, options).expect("start entry");
            writer.write_all(bytes).expect("write entry");
        }
        writer.finish().expect("finish zip").into_inner()
    }

    fn describe_bytes(id: &str) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "apiVersion": "v2",
            "kind": "design",
            "metadata": { "id": id, "version": "0.1.0" }
        }))
        .expect("serialise describe")
    }

    /// A well-formed extension archive: a describe naming `id`, plus a payload.
    fn extension_archive(id: &str) -> Vec<u8> {
        zip_bytes(&[
            ("describe.json", describe_bytes(id).as_slice()),
            ("extension.wasm", b"not-a-real-component".as_slice()),
        ])
    }

    #[test]
    fn only_gtxpack_entries_at_one_flat_level_are_extensions() {
        // Rule 2: the suffix decides. The wizard's `.json` manifest sidecars
        // share this directory and predate the feature.
        assert!(is_extension_archive_entry("extensions/acme.tool.gtxpack"));
        assert!(!is_extension_archive_entry(
            "extensions/wizard-answers.json"
        ));
        assert!(!is_extension_archive_entry(
            "extensions/design/acme.gtxpack"
        ));
        assert!(!is_extension_archive_entry("extensions/.gtxpack"));
        assert!(!is_extension_archive_entry("assets/acme.gtxpack"));
        assert!(!is_extension_archive_entry("extensions/acme.gtxpack.bak"));
    }

    #[test]
    fn a_staged_archive_is_unpacked_whole_and_carries_its_extension_id() {
        let staged = stage_archives([(
            "extensions/acme.tool.gtxpack".to_string(),
            extension_archive("acme.tool"),
        )]);

        assert!(staged.failures.is_empty(), "{:?}", staged.failures);
        assert_eq!(staged.staged.len(), 1);
        let extension = &staged.staged[0];
        assert_eq!(extension.extension_id.as_deref(), Some("acme.tool"));
        assert_eq!(
            std::fs::read(extension.dir.join("describe.json")).expect("staged describe"),
            describe_bytes("acme.tool"),
            "the staged describe must be the archive's own bytes"
        );
        assert_eq!(
            std::fs::read(extension.dir.join("extension.wasm")).expect("staged wasm"),
            b"not-a-real-component",
        );
    }

    #[test]
    fn a_corrupt_archive_is_reported_and_a_sibling_still_stages() {
        let staged = stage_archives([
            (
                "extensions/broken.gtxpack".to_string(),
                b"this is not a zip".to_vec(),
            ),
            (
                "extensions/good.gtxpack".to_string(),
                extension_archive("acme.good"),
            ),
        ]);

        assert_eq!(staged.failures.len(), 1);
        assert_eq!(staged.failures[0].entry_name, "extensions/broken.gtxpack");
        assert!(
            !staged.failures[0].reason.is_empty(),
            "a refused archive must say why"
        );
        assert_eq!(
            staged
                .staged
                .iter()
                .map(|e| e.entry_name.as_str())
                .collect::<Vec<_>>(),
            vec!["extensions/good.gtxpack"],
            "one broken archive must not cost the operator the others"
        );
    }

    #[test]
    fn an_archive_entry_that_escapes_the_staging_dir_is_refused() {
        let hostile = zip_bytes(&[("../escaped.txt", b"pwned".as_slice())]);
        let staged = stage_archives([("extensions/hostile.gtxpack".to_string(), hostile)]);

        assert!(
            staged.staged.is_empty(),
            "a traversing archive must not be presented to the loader"
        );
        assert_eq!(staged.failures.len(), 1);
        assert!(
            staged.failures[0].reason.contains("escapes"),
            "reason was {:?}",
            staged.failures[0].reason
        );
        let root = staging_root().expect("staging root");
        assert!(
            !root.join("escaped.txt").exists() && !root.join("../escaped.txt").exists(),
            "nothing may be written outside the staged directory"
        );
    }

    #[test]
    fn a_describe_without_an_id_still_stages_for_the_loader_to_refuse() {
        // The loader owns the refusal, and says far more about why than a
        // shallow read here could. Dropping it silently is the one behaviour
        // this module must never have.
        let archive = zip_bytes(&[("describe.json", b"{}".as_slice())]);
        let staged = stage_archives([("extensions/anonymous.gtxpack".to_string(), archive)]);

        assert!(staged.failures.is_empty());
        assert_eq!(staged.staged.len(), 1);
        assert_eq!(staged.staged[0].extension_id, None);
    }

    /// The whole hop this module exists for: a pack carries an extension, the
    /// on-disk extensions directory is empty, and the extension is nonetheless
    /// unpacked and identified — which is what makes its tools resolvable.
    ///
    /// Driven through a real `PackRuntime` over a real `.gtpack` ZIP rather than
    /// through `stage_archives` directly, because the half that was missing was
    /// never the unpacking: it was that nothing looked inside the pack at all.
    #[test]
    fn a_pack_carried_extension_is_staged_when_the_extensions_directory_is_empty() {
        use std::io::Write;

        let empty_dir = tempfile::tempdir().expect("an empty extensions directory");
        assert_eq!(
            std::fs::read_dir(empty_dir.path())
                .expect("read the empty dir")
                .count(),
            0,
            "the on-disk source must be empty for this test to mean anything"
        );

        let holder = tempfile::tempdir().expect("tempdir");
        let pack_path = holder.path().join("worker.gtpack");
        let mut writer =
            zip::ZipWriter::new(std::fs::File::create(&pack_path).expect("create pack"));
        let options: zip::write::FileOptions<'_, ()> =
            zip::write::FileOptions::default().compression_method(zip::CompressionMethod::Stored);
        writer
            .start_file("extensions/acme.tool.gtxpack", options)
            .expect("start entry");
        writer
            .write_all(&extension_archive("acme.tool"))
            .expect("write entry");
        writer
            .start_file("extensions/wizard-answers.json", options)
            .expect("start sidecar");
        writer.write_all(b"{}").expect("write sidecar");
        writer.finish().expect("finish pack");

        let pack = Arc::new(crate::pack::tests::pack_runtime_for_dir(&pack_path));
        let staged = stage_from_packs(&[pack]);

        assert!(staged.failures.is_empty(), "{:?}", staged.failures);
        assert_eq!(
            staged.staged.len(),
            1,
            "the `.json` sidecar must not be staged as an extension"
        );
        assert_eq!(staged.staged[0].entry_name, "extensions/acme.tool.gtxpack");
        assert_eq!(
            staged.staged[0].extension_id.as_deref(),
            Some("acme.tool"),
            "a staged extension must be identifiable, or precedence cannot be decided"
        );
        assert!(staged.staged[0].dir.join("extension.wasm").is_file());
    }

    /// The pack path is not a way around signing.
    ///
    /// A pack-carried archive that the verified loader refuses must be COUNTED
    /// and logged, and must not end up in the runtime. Both halves matter: an
    /// artefact that loaded here without passing `verify_dir_signature` /
    /// `verify_dir_manifest` would make "declared in a pack" a substitute for
    /// "signed", and one that was refused without being counted would be the
    /// original silent disappearance wearing a different hat.
    #[cfg(feature = "agentic-worker")]
    #[test]
    fn an_unverifiable_pack_carried_extension_is_refused_and_counted() {
        use std::io::Write;

        use greentic_ext_runtime::{DiscoveryPaths, ExtensionRuntime, RuntimeConfig};

        let empty_root = tempfile::tempdir().expect("an empty discovery root");
        let config =
            RuntimeConfig::from_paths(DiscoveryPaths::new(empty_root.path().to_path_buf()));
        let mut runtime = ExtensionRuntime::new(config).expect("build an extension runtime");
        assert!(runtime.loaded().is_empty(), "nothing is installed on disk");

        let holder = tempfile::tempdir().expect("tempdir");
        let pack_path = holder.path().join("worker.gtpack");
        let mut writer =
            zip::ZipWriter::new(std::fs::File::create(&pack_path).expect("create pack"));
        let options: zip::write::FileOptions<'_, ()> =
            zip::write::FileOptions::default().compression_method(zip::CompressionMethod::Stored);
        writer
            .start_file("extensions/acme.tool.gtxpack", options)
            .expect("start entry");
        writer
            .write_all(&extension_archive("acme.tool"))
            .expect("write entry");
        writer.finish().expect("finish pack");

        let pack = Arc::new(crate::pack::tests::pack_runtime_for_dir(&pack_path));
        let report = register_from_packs(&mut runtime, &[pack]);

        assert_eq!(
            report,
            PackExtensionLoad {
                loaded: 0,
                shadowed: 0,
                failed: 1,
            },
            "an unsigned, unverifiable archive must be refused and reported"
        );
        assert!(
            runtime.loaded().is_empty(),
            "a refused archive must not reach the runtime"
        );
    }

    #[cfg(feature = "agentic-worker")]
    #[test]
    fn an_extension_already_loaded_from_disk_is_not_replaced_by_the_pack() {
        let staged = StagedExtension {
            entry_name: "extensions/acme.tool.gtxpack".to_string(),
            dir: PathBuf::from("/nonexistent"),
            extension_id: Some("acme.tool".to_string()),
        };
        let on_disk: std::collections::HashSet<String> =
            ["acme.tool".to_string()].into_iter().collect();
        assert!(is_shadowed(&staged, &on_disk));

        let other: std::collections::HashSet<String> =
            ["acme.other".to_string()].into_iter().collect();
        assert!(!is_shadowed(&staged, &other));

        let anonymous = StagedExtension {
            extension_id: None,
            ..staged
        };
        assert!(
            !is_shadowed(&anonymous, &on_disk),
            "an unreadable id must reach the loader, not be assumed shadowed"
        );
    }
}
