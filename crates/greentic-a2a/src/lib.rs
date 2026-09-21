//! A2A (Agent2Agent) v1.0 wire types and agent-card retrieval.
//!
//! The canonical specification is proto-first — `specification/a2a.proto` in
//! `github.com/a2aproject/A2A`, release v1.0.1. The generated `a2a.json` is a
//! non-normative build artifact and is not committed there; where the prose
//! and the proto disagree, the proto is authoritative.
//!
//! **The proto is snake_case and the wire form is camelCase.** Every type here
//! carries `#[serde(rename_all = "camelCase")]` for that reason.
//!
//! This crate is deliberately types plus one HTTP call: no client loop, no
//! server, no projection of skills into LLM tools. Those belong to the
//! consumers, so that a worker calling an agent and a worker exposed as one
//! share these definitions rather than growing two.

pub mod card;
