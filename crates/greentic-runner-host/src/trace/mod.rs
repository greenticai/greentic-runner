#[cfg(feature = "agentic-worker")]
pub mod agent_audit;
pub mod audit_event;
pub mod audit_sink;
mod model;
mod recorder;

pub use model::{TraceEnvelope, TraceError, TraceFlow, TraceHash, TracePack, TraceStep};
// Only re-exported for `trace::agent_audit` and `runner::agent_node`'s `mod
// aw`, both gated behind `agentic-worker`; `recorder.rs` itself calls the
// function directly (same module), so this re-export is otherwise unused when
// that feature is off.
#[cfg(feature = "agentic-worker")]
pub(crate) use recorder::generate_audit_event_id;
pub use recorder::{PackTraceInfo, TraceConfig, TraceContext, TraceMode, TraceRecorder};
