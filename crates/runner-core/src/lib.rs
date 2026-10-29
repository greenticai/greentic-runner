//! Pack runtime core for Greentic runner.
//!
//! This crate provides the building blocks required to ingest pack indexes,
//! download pack artifacts from multiple backends, verify their integrity, and
//! maintain an on-disk cache that other runtimes can consume. It is not meant
//! to be embedded directly; use `greentic-runner`/`greentic-runner-host`, which
//! wrap these helpers with the canonical runtime.

pub mod env;
pub mod net_guard;
pub mod packs;
pub mod path_safety;
pub mod url_split;

pub use env::{IndexLocation, PackConfig, PackSource, join_public_keys};
pub use packs::{
    Index, PackDigest, PackManager, PackPolicy, PackRef, PackVersion, ResolvedPack, ResolvedSet,
    TenantPacks,
};
pub use path_safety::normalize_under_root;
