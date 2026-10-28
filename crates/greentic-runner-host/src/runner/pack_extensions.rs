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

/// Written last, only after every archive entry landed, BESIDE the extension
/// directory — never inside it.
///
/// The directory appears at the first write, so its existence proves an unpack
/// STARTED. Two tenants of one process stage the same digest, and a half-written
/// tree that a later tenant reused as "already staged" would load an extension
/// missing files — silently, since the manifest ledger would then fail with a
/// hash mismatch rather than the truthful "the unpack was interrupted".
///
/// # Why the marker lives at `<digest>/`, not in the extension directory
///
/// The extension directory is handed to the verified loader, whose ledger check
/// refuses ANY file present on disk but absent from the signed `manifest.json`.
/// A marker written inside it made every signed pack-carried extension fail
/// with "`.staged` is present in the extension directory but absent from
/// manifest.json", so no pack-carried extension ever loaded. The layout is
/// therefore:
///
/// ```text
/// <staging root>/<digest>/.ext-staged   <- this marker
/// <staging root>/<digest>/ext/          <- exactly the archive's files; loaded
/// ```
///
/// The marker is deliberately NOT named `.staged`: the previous layout wrote
/// `<digest>/.staged` with the archive unpacked directly into `<digest>/`, so
/// reusing that name would read an old-layout tree as a complete new-layout
/// one with no `ext/` beneath it. Under a new name an old-layout tree simply
/// reads as "not staged" and is cleared and re-staged.
const COMPLETE_MARKER: &str = ".ext-staged";

/// The subdirectory of `<digest>/` that holds the unpacked archive and is the
/// directory handed to the loader.
const EXTENSION_SUBDIR: &str = "ext";

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
    /// Decompressed size of the staged tree, as recorded when it was unpacked.
    /// Counted against the per-pass unpacked-bytes budget (`stage_bounded`).
    pub(crate) unpacked_bytes: u64,
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
/// A `TempDir` inside a `static` never runs its `Drop`, so the root survives
/// until the OS temp reaper takes it. That is the deliberate trade: the
/// alternative deletes a directory the runtime is still pointing at. Within the
/// root, superseded digests of an extension and abandoned partial unpacks are
/// reclaimed by `sweep_stale_slots` after a grace period.
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

/// Per-entry cap on the decompressed size of one file in an extension archive.
const MAX_EXT_ENTRY_BYTES: u64 = 64 * 1024 * 1024;
/// Cap on the total decompressed size of one extension archive.
const MAX_EXT_TOTAL_BYTES: u64 = 256 * 1024 * 1024;
/// Cap on the number of entries (files and directories) in one archive.
const MAX_EXT_ENTRIES: usize = 4096;

/// Unpack `bytes` into `dest`, refusing any entry that escapes it.
///
/// `enclosed_name` returning `None` is the refusal: an entry naming `..` or an
/// absolute path is a hostile archive, and sanitising the path would silently
/// load an extension whose contents are not what it claims.
///
/// This runs BEFORE signature / ledger verification (the archive comes from a
/// tenant pack), so it must be safe against a decompression bomb: the entry
/// count, each declared size and the running total are capped, and the real
/// decompressed bytes are streamed to disk through a `take` because a declared
/// size can lie.
#[cfg(test)]
fn unpack_archive(bytes: &[u8], dest: &Path) -> Result<u64> {
    unpack_archive_within(bytes, dest, MAX_EXT_TOTAL_BYTES)
}

/// [`unpack_archive`] with the total cap lowered to `max_total_bytes` (never
/// raised above [`MAX_EXT_TOTAL_BYTES`]). Returns the decompressed bytes written.
fn unpack_archive_within(bytes: &[u8], dest: &Path, max_total_bytes: u64) -> Result<u64> {
    unpack_archive_with_limits(
        bytes,
        dest,
        ExtractLimits {
            max_entry_bytes: MAX_EXT_ENTRY_BYTES,
            max_total_bytes: max_total_bytes.min(MAX_EXT_TOTAL_BYTES),
            max_entries: MAX_EXT_ENTRIES,
        },
    )
}

#[derive(Debug, Clone, Copy)]
struct ExtractLimits {
    max_entry_bytes: u64,
    max_total_bytes: u64,
    max_entries: usize,
}

fn unpack_archive_with_limits(bytes: &[u8], dest: &Path, limits: ExtractLimits) -> Result<u64> {
    let mut archive =
        zip::ZipArchive::new(Cursor::new(bytes)).context("read the extension archive")?;
    if archive.len() > limits.max_entries {
        bail!(
            "extension archive has too many entries ({}, limit {})",
            archive.len(),
            limits.max_entries
        );
    }
    let mut total_bytes = 0u64;
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
        if entry.size() > limits.max_entry_bytes {
            bail!(
                "extension archive entry `{raw_name}` is too large ({} bytes, limit {})",
                entry.size(),
                limits.max_entry_bytes
            );
        }
        if let Some(parent) = out_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut outfile = std::fs::File::create(&out_path)
            .with_context(|| format!("create {}", out_path.display()))?;
        // Bound the real decompressed size, not just the declared one.
        let mut limited = (&mut entry).take(limits.max_entry_bytes.saturating_add(1));
        let copied = std::io::copy(&mut limited, &mut outfile)
            .with_context(|| format!("write {}", out_path.display()))?;
        if copied > limits.max_entry_bytes {
            bail!(
                "extension archive entry `{raw_name}` exceeds the size limit ({})",
                limits.max_entry_bytes
            );
        }
        total_bytes = total_bytes.saturating_add(copied);
        if total_bytes > limits.max_total_bytes {
            bail!(
                "extension archive exceeds the total size limit ({} bytes)",
                limits.max_total_bytes
            );
        }
    }
    Ok(total_bytes)
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
fn stage_one(entry_name: &str, bytes: &[u8], unpack_budget: u64) -> Result<StagedExtension> {
    let root = staging_root().context("no staging directory is available")?;
    stage_one_budgeted(root, entry_name, bytes, unpack_budget)
}

/// [`stage_one`] against an explicit root, so tests can inspect the layout
/// without sharing the process-wide staging root.
#[cfg(test)]
fn stage_one_in(root: &Path, entry_name: &str, bytes: &[u8]) -> Result<StagedExtension> {
    stage_one_budgeted(root, entry_name, bytes, MAX_EXT_TOTAL_BYTES)
}

/// [`stage_one_in`] refusing to unpack more than `unpack_budget` bytes. An
/// already-staged slot is reused only if its recorded size fits the budget too.
fn stage_one_budgeted(
    root: &Path,
    entry_name: &str,
    bytes: &[u8],
    unpack_budget: u64,
) -> Result<StagedExtension> {
    // Serialise staging process-wide. Two callers staging the same bytes share
    // one slot; without this, the second sees no marker mid-unpack and clears
    // the slot under the first (`Directory not empty`), or the sweep races an
    // unpack. Staging is rare and short, so one lock costs nothing.
    static STAGING_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _guard = STAGING_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);

    let digest = hex::encode(Sha256::digest(bytes));
    let slot = root.join(&digest[..32]);
    let dir = slot.join(EXTENSION_SUBDIR);
    let marker = slot.join(COMPLETE_MARKER);

    if !marker.exists() {
        // A previous attempt may have died partway through, or the slot may
        // hold a tree from the old layout (archive unpacked straight into
        // `<digest>/` with the marker inside it). Clear the whole slot rather
        // than unpack over the top: a stale truncated file the current archive
        // no longer lists would survive and be hashed against the ledger.
        if slot.exists() {
            std::fs::remove_dir_all(&slot)
                .with_context(|| format!("clear partial staging at {}", slot.display()))?;
        }
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("create staging dir {}", dir.display()))?;
        let unpacked = match unpack_archive_within(bytes, &dir, unpack_budget) {
            Ok(unpacked) => unpacked,
            Err(error) => {
                // Do not leave a refused (possibly huge) partial tree on disk
                // until the sweep's grace expires.
                if let Err(clean) = std::fs::remove_dir_all(&slot) {
                    tracing::debug!(error = %clean, "cannot remove refused staging slot");
                }
                return Err(error);
            }
        };
        std::fs::write(&marker, unpacked.to_string())
            .with_context(|| format!("mark {} staged", dir.display()))?;
    }
    let unpacked_bytes = recorded_unpacked_bytes(&slot, &dir);
    if unpacked_bytes > unpack_budget {
        bail!(
            "extension archive unpacks to {unpacked_bytes} bytes, over the {unpack_budget} bytes left for this pass"
        );
    }

    let extension_id = staged_extension_id(&dir);
    record_slot_use(&slot, extension_id.as_deref());
    sweep_stale_slots(root, &slot, SUPERSEDED_GRACE);
    Ok(StagedExtension {
        entry_name: entry_name.to_string(),
        dir,
        extension_id,
        unpacked_bytes,
    })
}

/// Decompressed size of a staged slot: the number its marker records, else (a
/// marker from a build that wrote none) the sum of the files under `dir`.
fn recorded_unpacked_bytes(slot: &Path, dir: &Path) -> u64 {
    std::fs::read_to_string(slot.join(COMPLETE_MARKER))
        .ok()
        .and_then(|text| text.trim().parse::<u64>().ok())
        .unwrap_or_else(|| dir_size(dir))
}

fn dir_size(dir: &Path) -> u64 {
    let Ok(read_dir) = std::fs::read_dir(dir) else {
        return 0;
    };
    read_dir
        .flatten()
        .map(|entry| match entry.metadata() {
            Ok(meta) if meta.is_dir() => dir_size(&entry.path()),
            Ok(meta) => meta.len(),
            Err(_) => 0,
        })
        .fold(0u64, u64::saturating_add)
}

/// File in a slot naming the extension id staged there, so a later digest of
/// the same extension can find the slots it supersedes.
const SLOT_ID_FILE: &str = ".ext-id";

/// How long a superseded or abandoned slot is left alone before the sweep
/// removes it. A [`LoadedExtension`](greentic_ext_runtime::LoadedExtension)
/// keeps pointing into its slot, and nothing tells this module when a runtime
/// drops, so a slot is only reclaimed once nothing has staged it for this
/// long. Every stage of a digest re-touches its slot (`record_slot_use`), so a
/// slot some pack still carries never ages out while that pack is rebuilt.
const SUPERSEDED_GRACE: std::time::Duration = std::time::Duration::from_secs(60 * 60);

/// Refresh the slot's last-use time and remember which extension it holds.
/// Best effort: failing to record only means the slot is swept less precisely.
fn record_slot_use(slot: &Path, extension_id: Option<&str>) {
    if let Some(id) = extension_id
        && let Err(error) = std::fs::write(slot.join(SLOT_ID_FILE), id)
    {
        tracing::debug!(error = %error, "cannot record extension id for staging slot");
    }
    let touched = std::fs::File::options()
        .write(true)
        .open(slot.join(COMPLETE_MARKER))
        .and_then(|file| file.set_modified(std::time::SystemTime::now()));
    if let Err(error) = touched {
        tracing::debug!(error = %error, "cannot refresh staging slot use time");
    }
}

/// Remove staging slots nothing has used for `grace`: incomplete slots (a crash
/// mid-unpack) and complete slots of ANY extension id (an older digest of the
/// same extension, a different id, or one with no readable id). `keep` itself
/// is never removed, and every stage re-touches its slot, so a slot some pack
/// still carries never ages out while that pack is staged again; it is the age
/// test that keeps tenants' different extensions from evicting each other.
/// Known limit: a runtime that registered a slot and is not rebuilt for longer
/// than `grace` while another extension stages loses its slot, the same
/// exposure superseded digests always had. Best effort: a failure is logged,
/// never propagated.
fn sweep_stale_slots(root: &Path, keep: &Path, grace: std::time::Duration) {
    let Ok(read_dir) = std::fs::read_dir(root) else {
        return;
    };
    let now = std::time::SystemTime::now();
    for candidate in read_dir.flatten() {
        let path = candidate.path();
        if path == keep || !path.is_dir() {
            continue;
        }
        let last_used = match std::fs::metadata(path.join(COMPLETE_MARKER)) {
            Ok(meta) => meta.modified().ok(),
            // No marker: an unpack that died partway.
            Err(_) => std::fs::metadata(&path).and_then(|m| m.modified()).ok(),
        };
        let aged = last_used
            .and_then(|t| now.duration_since(t).ok())
            .is_some_and(|age| age >= grace);
        if aged && let Err(error) = std::fs::remove_dir_all(&path) {
            tracing::debug!(
                error = %error,
                slot = %path.display(),
                "cannot remove stale extension staging slot"
            );
        }
    }
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
    let mut candidates: Vec<(String, &Arc<PackRuntime>)> = Vec::new();
    for pack in packs {
        for entry_name in pack.extension_archive_entries() {
            if seen.insert(entry_name.clone()) {
                candidates.push((entry_name, pack));
            }
        }
    }
    stage_bounded(
        candidates,
        |name, pack, limit| pack.read_pack_file_capped(name, limit),
        MAX_PACK_EXT_ENTRIES,
        MAX_PACK_EXT_TOTAL_BYTES,
        MAX_PACK_EXT_UNPACKED_BYTES,
    )
}

/// Most `extensions/*.gtxpack` entries staged from the loaded packs in one pass.
const MAX_PACK_EXT_ENTRIES: usize = 64;
/// Most archive bytes read from the loaded packs in one pass (each entry is
/// also bounded by what the unpacker accepts). Reading is one entry at a time,
/// so this bounds work and disk, while resident memory stays at one archive.
const MAX_PACK_EXT_TOTAL_BYTES: u64 = 512 * 1024 * 1024;
/// Most DECOMPRESSED bytes unpacked to disk from the loaded packs in one pass.
/// The compressed cap above does not bound this: each archive may unpack to
/// [`MAX_EXT_TOTAL_BYTES`], so 64 of them were 16 GiB.
const MAX_PACK_EXT_UNPACKED_BYTES: u64 = 1024 * 1024 * 1024;

/// Stage candidates one at a time: read, stage, release the bytes, then read
/// the next. The previous shape collected every archive into a `Vec` first, so
/// resident memory grew with the number of entries. Entries past `max_entries`
/// or past the `max_total_bytes` aggregate are recorded as failures (visible,
/// never silently dropped) and not staged.
///
/// `read` gets the bytes still available in the aggregate budget and must not
/// return more than that: an entry over budget is refused WHILE it is read (at
/// most budget + 1 bytes resident), not after it was read whole. The total is
/// checked again on what came back, so a reader that ignores the limit still
/// cannot overspend. Each archive may unpack only into what is left of
/// `max_unpacked_bytes` (decompressed, across the pass); a refused unpack is a
/// failure and its partial tree is removed. This is the one staging path: the tests drive it too.
fn stage_bounded<T>(
    candidates: Vec<(String, T)>,
    read: impl Fn(&str, &T, u64) -> Option<Vec<u8>>,
    max_entries: usize,
    max_total_bytes: u64,
    max_unpacked_bytes: u64,
) -> PackExtensions {
    let mut out = PackExtensions::default();
    let mut total: u64 = 0;
    let mut unpacked_total: u64 = 0;
    for (index, (entry_name, source)) in candidates.into_iter().enumerate() {
        if index >= max_entries {
            out.failures.push(StagingFailure {
                entry_name,
                reason: format!("more than {max_entries} extension archives in the loaded packs"),
            });
            continue;
        }
        let remaining = max_total_bytes.saturating_sub(total);
        let Some(bytes) = read(&entry_name, &source, remaining) else {
            out.failures.push(StagingFailure {
                entry_name,
                reason: format!(
                    "the pack lists this entry but it could not be read within the {remaining} bytes left for this pass"
                ),
            });
            continue;
        };
        let len = bytes.len() as u64;
        if total.saturating_add(len) > max_total_bytes {
            out.failures.push(StagingFailure {
                entry_name,
                reason: format!(
                    "extension archives from the loaded packs exceed {max_total_bytes} bytes in total"
                ),
            });
            continue;
        }
        total += len;
        let unpack_budget = max_unpacked_bytes.saturating_sub(unpacked_total);
        match stage_one(&entry_name, &bytes, unpack_budget) {
            Ok(staged) => {
                unpacked_total = unpacked_total.saturating_add(staged.unpacked_bytes);
                out.staged.push(staged);
            }
            Err(error) => out.failures.push(StagingFailure {
                entry_name,
                reason: format!("{error:#}"),
            }),
        }
        // `bytes` is dropped here, before the next entry is read.
    }
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

    /// Stage in-memory archives through the PRODUCTION path ([`stage_bounded`],
    /// the same function `stage_from_packs` runs), so the tests below cannot
    /// drift from it. Only the byte source differs.
    fn stage_archives<I>(entries: I) -> PackExtensions
    where
        I: IntoIterator<Item = (String, Vec<u8>)>,
    {
        stage_bounded(
            entries.into_iter().collect(),
            |_, bytes, limit| (bytes.len() as u64 <= limit).then(|| bytes.clone()),
            usize::MAX,
            u64::MAX,
            u64::MAX,
        )
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

    #[test]
    fn staging_caps_the_entry_count_and_never_reads_past_it() {
        use std::cell::Cell;

        let reads = Cell::new(0usize);
        let candidates: Vec<(String, String)> = (0..5)
            .map(|n| (format!("extensions/e{n}.gtxpack"), format!("acme.e{n}")))
            .collect();
        let out = stage_bounded(
            candidates,
            |_, id, _| {
                reads.set(reads.get() + 1);
                Some(extension_archive(id))
            },
            3,
            u64::MAX,
            u64::MAX,
        );
        assert_eq!(out.staged.len(), 3);
        assert_eq!(out.failures.len(), 2, "over-cap entries are reported");
        assert_eq!(reads.get(), 3, "over-cap entries are never read");
    }

    #[test]
    fn staging_stops_at_the_aggregate_byte_budget() {
        let archive = extension_archive("acme.big");
        let one = archive.len() as u64;
        let candidates: Vec<(String, ())> = (0..3)
            .map(|n| (format!("extensions/b{n}.gtxpack"), ()))
            .collect();
        let out = stage_bounded(
            candidates,
            |_, (), _| Some(archive.clone()),
            10,
            one * 2,
            u64::MAX,
        );
        assert_eq!(out.staged.len(), 2);
        assert_eq!(out.failures.len(), 1);
        assert!(out.failures[0].reason.contains("in total"));
    }

    #[test]
    fn the_reader_is_given_the_remaining_budget_not_the_whole_one() {
        use std::cell::RefCell;

        let archive = extension_archive("acme.budget");
        let one = archive.len() as u64;
        let seen = RefCell::new(Vec::new());
        let candidates: Vec<(String, ())> = (0..3)
            .map(|n| (format!("extensions/r{n}.gtxpack"), ()))
            .collect();
        let out = stage_bounded(
            candidates,
            |_, (), limit| {
                seen.borrow_mut().push(limit);
                (archive.len() as u64 <= limit).then(|| archive.clone())
            },
            10,
            one * 2 + 1,
            u64::MAX,
        );
        assert_eq!(*seen.borrow(), vec![one * 2 + 1, one + 1, 1]);
        assert_eq!(out.staged.len(), 2);
        assert_eq!(out.failures.len(), 1, "the over-budget read is refused");
    }

    #[test]
    fn an_over_budget_pack_entry_is_refused_while_it_is_read() {
        let dir = tempfile::tempdir().expect("tmp");
        let extensions = dir.path().join("extensions");
        std::fs::create_dir_all(&extensions).expect("mkdir");
        std::fs::write(extensions.join("big.gtxpack"), vec![0u8; 4096]).expect("write");
        let pack = crate::pack::tests::pack_runtime_for_dir(dir.path());
        assert_eq!(
            pack.read_pack_file_capped("extensions/big.gtxpack", 4096)
                .map(|bytes| bytes.len()),
            Some(4096)
        );
        assert_eq!(
            pack.read_pack_file_capped("extensions/big.gtxpack", 4095),
            None,
            "one byte over the limit must not be returned"
        );
    }

    #[test]
    fn an_unreadable_listed_entry_is_a_failure_not_a_gap() {
        let out = stage_bounded(
            vec![("extensions/gone.gtxpack".to_string(), ())],
            |_, (), _| None,
            10,
            u64::MAX,
            u64::MAX,
        );
        assert!(out.staged.is_empty());
        assert_eq!(out.failures.len(), 1);
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
            unpacked_bytes: 0,
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

    /// Every regular file under `dir`, as `/`-joined relative paths, sorted.
    fn files_under(dir: &Path) -> Vec<String> {
        fn walk(base: &Path, current: &Path, out: &mut Vec<String>) {
            for entry in std::fs::read_dir(current).expect("read staged dir") {
                let path = entry.expect("dir entry").path();
                if path.is_dir() {
                    walk(base, &path, out);
                } else {
                    let rel = path.strip_prefix(base).expect("under base");
                    out.push(rel.to_string_lossy().replace('\\', "/"));
                }
            }
        }
        let mut out = Vec::new();
        walk(dir, dir, &mut out);
        out.sort();
        out
    }

    /// The regression: the directory handed to the loader must contain EXACTLY
    /// the archive's files. The loader's ledger refuses any file the signed
    /// manifest does not list, so a completion marker written inside it made
    /// every signed pack-carried extension unloadable.
    #[test]
    fn the_loader_directory_holds_only_the_archives_files_and_the_marker_sits_beside_it() {
        let root = tempfile::tempdir().expect("staging root");
        let archive = zip_bytes(&[
            ("describe.json", describe_bytes("acme.tool").as_slice()),
            ("manifest.json", b"{}".as_slice()),
            ("extension.wasm", b"wasm".as_slice()),
            ("assets/icon.svg", b"<svg/>".as_slice()),
        ]);

        let staged =
            stage_one_in(root.path(), "extensions/acme.tool.gtxpack", &archive).expect("stage");

        assert_eq!(
            files_under(&staged.dir),
            vec![
                "assets/icon.svg",
                "describe.json",
                "extension.wasm",
                "manifest.json",
            ],
            "nothing but the archive's own entries may appear in the loader's directory"
        );
        let slot = staged.dir.parent().expect("slot");
        assert_eq!(slot.parent(), Some(root.path()));
        assert!(
            slot.join(COMPLETE_MARKER).is_file(),
            "the completion marker must live beside the extension directory"
        );
        assert!(!staged.dir.join(COMPLETE_MARKER).exists());
        assert!(!staged.dir.join(".staged").exists());
    }

    #[test]
    fn a_completed_stage_is_reused_and_not_unpacked_again() {
        let root = tempfile::tempdir().expect("staging root");
        let archive = extension_archive("acme.tool");

        let first = stage_one_in(root.path(), "extensions/a.gtxpack", &archive).expect("first");
        // A sentinel the archive does not contain: if the second call re-unpacked
        // (which clears the slot first), it would be gone.
        let sentinel = first.dir.parent().expect("slot").join("sentinel");
        std::fs::write(&sentinel, b"kept").expect("write sentinel");

        let second = stage_one_in(root.path(), "extensions/a.gtxpack", &archive).expect("second");

        assert_eq!(first.dir, second.dir);
        assert!(sentinel.exists(), "a completed stage must be reused as-is");
        assert_eq!(second.extension_id.as_deref(), Some("acme.tool"));
    }

    fn small_limits() -> ExtractLimits {
        ExtractLimits {
            max_entry_bytes: 1024,
            max_total_bytes: 3 * 1024,
            max_entries: 4,
        }
    }

    /// Flip the declared uncompressed size in every central-directory header
    /// (and local header) of a stored zip, producing an archive that lies
    /// about how big its entries are.
    fn lie_about_size(mut bytes: Vec<u8>, declared: u32) -> Vec<u8> {
        let patch = |bytes: &mut Vec<u8>, sig: &[u8; 4], offset: usize| {
            let mut at = 0;
            while at + 4 <= bytes.len() {
                if &bytes[at..at + 4] == sig && at + offset + 4 <= bytes.len() {
                    bytes[at + offset..at + offset + 4].copy_from_slice(&declared.to_le_bytes());
                    at += 4;
                } else {
                    at += 1;
                }
            }
        };
        // Central directory header: uncompressed size at +24.
        patch(&mut bytes, b"PK\x01\x02", 24);
        bytes
    }

    #[test]
    fn a_normal_archive_unpacks_under_the_limits() {
        let dir = tempfile::tempdir().expect("dest");
        let archive = extension_archive("acme.tool");
        unpack_archive_with_limits(&archive, dir.path(), small_limits()).expect("unpack");
        assert!(dir.path().join("describe.json").is_file());
        assert!(dir.path().join("extension.wasm").is_file());
        // And the production limits accept it too.
        let dir = tempfile::tempdir().expect("dest");
        unpack_archive(&archive, dir.path()).expect("unpack with default limits");
    }

    #[test]
    fn an_entry_declared_over_the_cap_is_refused() {
        let dir = tempfile::tempdir().expect("dest");
        let payload = vec![0u8; 2048];
        let archive = zip_bytes(&[("big.bin", payload.as_slice())]);
        let err = unpack_archive_with_limits(&archive, dir.path(), small_limits())
            .expect_err("oversized entry must be refused");
        assert!(err.to_string().contains("too large"), "{err}");
    }

    #[test]
    fn an_entry_that_lies_about_its_size_is_stopped_by_the_streaming_cap() {
        let dir = tempfile::tempdir().expect("dest");
        let payload = vec![0u8; 4096];
        let archive = lie_about_size(zip_bytes(&[("big.bin", payload.as_slice())]), 10);
        let err = unpack_archive_with_limits(&archive, dir.path(), small_limits())
            .expect_err("a lying declared size must not bypass the cap");
        // Either the zip reader rejects the inconsistent entry, or our streaming
        // cap does; in no case may the full payload be written out.
        let written = std::fs::metadata(dir.path().join("big.bin"))
            .map(|m| m.len())
            .unwrap_or(0);
        assert!(written <= small_limits().max_entry_bytes + 1, "{err}");
    }

    #[test]
    fn the_total_size_cap_is_enforced_across_entries() {
        let dir = tempfile::tempdir().expect("dest");
        let chunk = vec![1u8; 1000];
        let archive = zip_bytes(&[
            ("a.bin", chunk.as_slice()),
            ("b.bin", chunk.as_slice()),
            ("c.bin", chunk.as_slice()),
            ("d.bin", chunk.as_slice()),
        ]);
        let limits = ExtractLimits {
            max_entry_bytes: 1024,
            max_total_bytes: 2500,
            max_entries: 10,
        };
        let err = unpack_archive_with_limits(&archive, dir.path(), limits)
            .expect_err("total cap must trip");
        assert!(err.to_string().contains("total size"), "{err}");
    }

    #[test]
    fn the_entry_count_cap_is_enforced() {
        let dir = tempfile::tempdir().expect("dest");
        let names: Vec<String> = (0..5).map(|i| format!("f{i}.txt")).collect();
        let entries: Vec<(&str, &[u8])> = names.iter().map(|n| (n.as_str(), &b"x"[..])).collect();
        let archive = zip_bytes(&entries);
        let err = unpack_archive_with_limits(&archive, dir.path(), small_limits())
            .expect_err("too many entries must be refused");
        assert!(err.to_string().contains("too many entries"), "{err}");
    }

    fn age_slot(slot: &Path, age: std::time::Duration) {
        let past = std::time::SystemTime::now() - age;
        let marker = std::fs::File::options()
            .write(true)
            .open(slot.join(COMPLETE_MARKER))
            .expect("open marker");
        marker.set_modified(past).expect("age marker");
    }

    #[test]
    fn a_superseded_digest_is_swept_after_the_grace_but_not_before() {
        let root = tempfile::tempdir().expect("staging root");
        let old = stage_one_in(
            root.path(),
            "extensions/a.gtxpack",
            &zip_bytes(&[
                ("describe.json", describe_bytes("acme.tool").as_slice()),
                ("v", b"1".as_slice()),
            ]),
        )
        .expect("old");
        let old_slot = old.dir.parent().expect("slot").to_path_buf();
        let other = stage_one_in(
            root.path(),
            "extensions/b.gtxpack",
            &extension_archive("other.tool"),
        )
        .expect("other");
        let other_slot = other.dir.parent().expect("slot").to_path_buf();

        // A fresh superseding digest does not remove the old one inside the grace.
        let new = stage_one_in(
            root.path(),
            "extensions/a.gtxpack",
            &zip_bytes(&[
                ("describe.json", describe_bytes("acme.tool").as_slice()),
                ("v", b"2".as_slice()),
            ]),
        )
        .expect("new");
        let new_slot = new.dir.parent().expect("slot").to_path_buf();
        assert!(old_slot.exists(), "inside the grace the old slot stays");

        age_slot(
            &old_slot,
            SUPERSEDED_GRACE + std::time::Duration::from_secs(60),
        );
        age_slot(
            &other_slot,
            SUPERSEDED_GRACE + std::time::Duration::from_secs(60),
        );
        sweep_stale_slots(root.path(), &new_slot, SUPERSEDED_GRACE);

        assert!(!old_slot.exists(), "an aged superseded slot is removed");
        assert!(new_slot.exists(), "the current slot is kept");
        assert!(
            !other_slot.exists(),
            "an aged slot of another extension id is reclaimed too"
        );
    }

    #[test]
    fn a_recently_used_slot_of_another_extension_is_never_evicted() {
        let root = tempfile::tempdir().expect("staging root");
        let other = stage_one_in(
            root.path(),
            "extensions/b.gtxpack",
            &extension_archive("other.tool"),
        )
        .expect("other");
        let other_slot = other.dir.parent().expect("slot").to_path_buf();
        let anonymous = stage_one_in(
            root.path(),
            "extensions/c.gtxpack",
            &zip_bytes(&[("extension.wasm", b"no describe".as_slice())]),
        )
        .expect("anonymous");
        let anonymous_slot = anonymous.dir.parent().expect("slot").to_path_buf();
        let current = stage_one_in(
            root.path(),
            "extensions/a.gtxpack",
            &extension_archive("acme.tool"),
        )
        .expect("current");
        let current_slot = current.dir.parent().expect("slot").to_path_buf();

        sweep_stale_slots(root.path(), &current_slot, SUPERSEDED_GRACE);
        assert!(other_slot.exists() && anonymous_slot.exists());

        // Aged: the id-less slot goes as well as the other id's; a re-stage
        // (a runtime still using it) refreshes the age first.
        for slot in [&other_slot, &anonymous_slot] {
            age_slot(slot, SUPERSEDED_GRACE + std::time::Duration::from_secs(60));
        }
        record_slot_use(&other_slot, other.extension_id.as_deref());
        sweep_stale_slots(root.path(), &current_slot, SUPERSEDED_GRACE);
        assert!(other_slot.exists(), "a re-touched slot survives");
        assert!(
            !anonymous_slot.exists(),
            "an aged id-less slot is reclaimed"
        );
        assert!(current_slot.exists());
    }

    #[test]
    fn the_unpacked_byte_budget_bounds_a_pass_and_cleans_up() {
        let big = zip_bytes(&[
            ("describe.json", describe_bytes("acme.big").as_slice()),
            ("blob.bin", vec![7u8; 4000].as_slice()),
        ]);
        let small = extension_archive("acme.small");
        let candidates = vec![
            ("extensions/big.gtxpack".to_string(), big.clone()),
            ("extensions/small.gtxpack".to_string(), small.clone()),
        ];
        // Budget fits the small archive but not the big one, whatever their
        // (tiny) compressed size.
        let out = stage_bounded(
            candidates,
            |_, bytes, _| Some(bytes.clone()),
            10,
            u64::MAX,
            1000,
        );
        assert_eq!(out.staged.len(), 1, "{:?}", out.failures);
        assert_eq!(out.staged[0].entry_name, "extensions/small.gtxpack");
        assert_eq!(out.failures.len(), 1);
        assert_eq!(out.failures[0].entry_name, "extensions/big.gtxpack");
        assert!(
            out.failures[0].reason.contains("total size limit"),
            "{:?}",
            out.failures
        );
    }

    #[test]
    fn the_unpacked_budget_is_spent_across_entries() {
        let a = extension_archive("acme.a");
        let b = extension_archive("acme.b");
        let one = out_unpacked(&a);
        let out = stage_bounded(
            vec![
                ("extensions/a.gtxpack".to_string(), a),
                ("extensions/b.gtxpack".to_string(), b),
            ],
            |_, bytes, _| Some(bytes.clone()),
            10,
            u64::MAX,
            one + one / 2,
        );
        assert_eq!(out.staged.len(), 1, "{:?}", out.failures);
        assert_eq!(out.failures.len(), 1);
    }

    fn out_unpacked(archive: &[u8]) -> u64 {
        let dir = tempfile::tempdir().expect("dest");
        unpack_archive(archive, dir.path()).expect("unpack")
    }

    #[test]
    fn a_staged_slot_records_its_unpacked_size_and_is_charged_again_on_reuse() {
        let root = tempfile::tempdir().expect("staging root");
        let archive = extension_archive("acme.tool");
        let expected = out_unpacked(&archive);
        let first = stage_one_in(root.path(), "extensions/a.gtxpack", &archive).expect("first");
        assert_eq!(first.unpacked_bytes, expected);
        // Reusing the slot reports the same size...
        let again = stage_one_in(root.path(), "extensions/a.gtxpack", &archive).expect("again");
        assert_eq!(again.unpacked_bytes, expected);
        // ...and a budget below it refuses even though nothing is unpacked.
        let err = stage_one_budgeted(root.path(), "extensions/a.gtxpack", &archive, expected - 1)
            .expect_err("over budget");
        assert!(err.to_string().contains("over the"), "{err}");
    }

    #[test]
    fn an_aged_incomplete_slot_is_swept() {
        let root = tempfile::tempdir().expect("staging root");
        let orphan = root.path().join("deadbeefdeadbeefdeadbeefdeadbeef");
        std::fs::create_dir_all(orphan.join(EXTENSION_SUBDIR)).expect("orphan");
        let staged = stage_one_in(
            root.path(),
            "extensions/a.gtxpack",
            &extension_archive("acme.tool"),
        )
        .expect("stage");
        let keep = staged.dir.parent().expect("slot").to_path_buf();

        sweep_stale_slots(root.path(), &keep, SUPERSEDED_GRACE);
        assert!(orphan.exists(), "a fresh incomplete slot may be mid-unpack");

        sweep_stale_slots(root.path(), &keep, std::time::Duration::ZERO);
        assert!(!orphan.exists());
        assert!(keep.exists());
    }

    #[test]
    fn a_partial_unpack_without_the_marker_is_redone() {
        let root = tempfile::tempdir().expect("staging root");
        let archive = extension_archive("acme.tool");
        let digest = hex::encode(Sha256::digest(&archive));
        let slot = root.path().join(&digest[..32]);
        let dir = slot.join(EXTENSION_SUBDIR);
        std::fs::create_dir_all(&dir).expect("partial dir");
        std::fs::write(dir.join("describe.json"), b"trunc").expect("truncated describe");
        std::fs::write(dir.join("stale.bin"), b"left behind").expect("stale file");

        let staged = stage_one_in(root.path(), "extensions/a.gtxpack", &archive).expect("stage");

        assert_eq!(
            files_under(&staged.dir),
            vec!["describe.json", "extension.wasm"]
        );
        assert_eq!(
            std::fs::read(staged.dir.join("describe.json")).expect("describe"),
            describe_bytes("acme.tool"),
        );
        assert!(slot.join(COMPLETE_MARKER).is_file());
    }

    /// A slot written by the previous layout — archive unpacked straight into
    /// `<digest>/` with `.staged` inside it — must not be mistaken for a
    /// completed new-layout stage; it is cleared and re-staged.
    #[test]
    fn a_slot_left_by_the_old_layout_is_restaged_into_the_new_one() {
        let root = tempfile::tempdir().expect("staging root");
        let archive = extension_archive("acme.tool");
        let digest = hex::encode(Sha256::digest(&archive));
        let slot = root.path().join(&digest[..32]);
        unpack_archive(&archive, &{
            std::fs::create_dir_all(&slot).expect("old slot");
            slot.clone()
        })
        .expect("old-layout unpack");
        std::fs::write(slot.join(".staged"), b"").expect("old marker");

        let staged = stage_one_in(root.path(), "extensions/a.gtxpack", &archive).expect("stage");

        assert_eq!(staged.dir, slot.join(EXTENSION_SUBDIR));
        assert_eq!(
            files_under(&staged.dir),
            vec!["describe.json", "extension.wasm"]
        );
        assert_eq!(
            files_under(&slot),
            vec![
                SLOT_ID_FILE.to_string(),
                COMPLETE_MARKER.to_string(),
                format!("{EXTENSION_SUBDIR}/describe.json"),
                format!("{EXTENSION_SUBDIR}/extension.wasm"),
            ],
            "the old-layout files and marker must be gone"
        );
    }

    /// The test whose absence let the in-directory marker ship: a SIGNED
    /// extension carried in a pack goes through the same verified loader the
    /// runtime uses (`register_from_packs` → `register_loaded_from_dir`) and
    /// registers. Signature, manifest ledger (including its "no file outside the
    /// ledger" rule) and TOFU anchor are all real; only the trust root is a
    /// temp dir.
    #[cfg(feature = "agentic-worker")]
    #[test]
    fn a_signed_pack_carried_extension_is_staged_and_registers_through_the_loader() {
        use std::io::Write;

        use greentic_ext_runtime::{DiscoveryPaths, ExtensionRuntime, RuntimeConfig};

        let archive = signed_extension_archive("acme.signed-tool");

        let empty_root = tempfile::tempdir().expect("an empty discovery root");
        let trust = tempfile::tempdir().expect("a temp trust root");
        let config =
            RuntimeConfig::from_paths(DiscoveryPaths::new(empty_root.path().to_path_buf()))
                .with_trust_root(trust.path().to_path_buf());
        let mut runtime = ExtensionRuntime::new(config).expect("build an extension runtime");
        assert!(runtime.loaded().is_empty(), "nothing is installed on disk");

        let holder = tempfile::tempdir().expect("tempdir");
        let pack_path = holder.path().join("worker.gtpack");
        let mut writer =
            zip::ZipWriter::new(std::fs::File::create(&pack_path).expect("create pack"));
        let options: zip::write::FileOptions<'_, ()> =
            zip::write::FileOptions::default().compression_method(zip::CompressionMethod::Stored);
        writer
            .start_file("extensions/acme.signed-tool.gtxpack", options)
            .expect("start entry");
        writer.write_all(&archive).expect("write entry");
        writer.finish().expect("finish pack");

        let pack = Arc::new(crate::pack::tests::pack_runtime_for_dir(&pack_path));
        let report = register_from_packs(&mut runtime, &[pack]);

        assert_eq!(
            report,
            PackExtensionLoad {
                loaded: 1,
                shadowed: 0,
                failed: 0,
            },
            "a correctly signed pack-carried extension must pass the load gate"
        );
        assert!(
            runtime
                .loaded()
                .keys()
                .any(|id| id.as_str() == "acme.signed-tool"),
            "the extension must be registered in the runtime"
        );
    }

    /// A minimal, correctly signed `.gtxpack`: an empty wasm component, a
    /// whole-archive manifest bound into the describe, and an ed25519 signature
    /// over the JCS-canonical describe — produced bind → sign, as the SDK does.
    #[cfg(feature = "agentic-worker")]
    fn signed_extension_archive(id: &str) -> Vec<u8> {
        use greentic_extension_sdk_contract::{
            DescribeJson, artifact_sha256, bind_manifest, build_manifest, sign_describe,
        };

        // `(component)` in binary form: the component-model preamble alone.
        let wasm: Vec<u8> = vec![0x00, 0x61, 0x73, 0x6d, 0x0d, 0x00, 0x01, 0x00];
        let wasm_sha = artifact_sha256(&wasm);

        let mut describe: DescribeJson = serde_json::from_value(serde_json::json!({
            "apiVersion": "greentic.ai/v2",
            "kind": "DesignExtension",
            "compat": {
                "min_designer_version": ">=1.0.0",
                "min_runner_version": "^0.12.0",
                "contract_version": "1.2.0"
            },
            "metadata": {
                "id": id,
                "name": id,
                "version": "0.1.0",
                "summary": "test",
                "author": { "name": "test" },
                "license": "MIT"
            },
            "engine": { "greenticDesigner": "*", "extRuntime": "*" },
            "capabilities": {
                "offered": [{ "id": "greentic:test/ping", "version": "1.0.0" }],
                "required": []
            },
            "runtime": {
                "memoryLimitMB": 64,
                "permissions": {},
                "components": {
                    "stub": {
                        "gtpack": {
                            "file": "extension.wasm",
                            "sha256": wasm_sha,
                            "pack_id": id,
                            "component_version": "0.1.0"
                        },
                        "sha256": wasm_sha,
                        "world": "greentic:component/stub@0.1.0"
                    }
                }
            },
            "contributions": {}
        }))
        .expect("a valid describe");

        let manifest = build_manifest(vec![("extension.wasm", wasm.as_slice())]);
        let manifest_json = serde_json::to_vec(&manifest).expect("serialise manifest");
        bind_manifest(&mut describe, &manifest_json);
        let key = ed25519_dalek::SigningKey::from_bytes(&[7u8; 32]);
        sign_describe(&mut describe, &key).expect("sign describe");
        let describe_json = serde_json::to_vec_pretty(&describe).expect("serialise describe");

        zip_bytes(&[
            ("describe.json", describe_json.as_slice()),
            ("manifest.json", manifest_json.as_slice()),
            ("extension.wasm", wasm.as_slice()),
        ])
    }
}
