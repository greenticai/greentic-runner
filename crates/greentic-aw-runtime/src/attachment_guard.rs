//! Inbound guardrails over attachment DOCUMENT text.
//!
//! The inbound chain runs on the user's message text in the agent loop, but a
//! document's text is fetched later, inside the LLM backend. The loop therefore
//! hands the backend a guard ([`AttachmentTextGuard`]) built from the SAME
//! chain, context and evaluator it ran over the message text, and the backend
//! calls it on every document's text before that text is put in the prompt.
//!
//! The verdicts mirror the message-text path ([`crate::guardrail::run_chain`]):
//! `Accept` keeps the text, `Update` replaces it (a redaction), a `Deny` in
//! Enforce mode blocks it and a `Deny` in Monitor mode is only observed. What
//! differs is what "blocked" does: a blocked message fails the turn, a blocked
//! document is WITHHELD (the model gets a fixed sentence instead, and the turn
//! goes on). And an evaluator ERROR withholds the document whatever the
//! guardrail is: the message-text path fails open for an agent-level guardrail,
//! but a file nobody could check is not shown.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::StepObserver;
use crate::guardrail::{
    ChainOutcome, GuardrailDirection, GuardrailEvaluator, GuardrailInput, GuardrailInvokeError,
    GuardrailRunCtx, GuardrailVerdict, ResolvedGuardrail, run_chain,
};

/// What a guard decided about one document's text.
#[derive(Clone, PartialEq, Eq)]
pub enum AttachmentTextVerdict {
    /// Put this text in the prompt (the input unchanged, or a redaction).
    Allow(String),
    /// Do not show the document: the model gets a fixed sentence instead.
    Withhold,
}

impl fmt::Debug for AttachmentTextVerdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Never the document text.
        match self {
            AttachmentTextVerdict::Allow(text) => f
                .debug_struct("Allow")
                .field("text_len", &text.len())
                .finish(),
            AttachmentTextVerdict::Withhold => f.write_str("Withhold"),
        }
    }
}

/// Checks a document's text before it reaches the prompt. Called once per
/// document per turn (inside the per-turn memo), synchronously, like the
/// message-text chain.
pub trait AttachmentTextGuard: Send + Sync + fmt::Debug {
    fn check(&self, text: &str) -> AttachmentTextVerdict;
}

/// The inbound guardrail chain of one turn, applied to document text.
pub struct InboundChainGuard {
    chain: Vec<ResolvedGuardrail>,
    ctx: GuardrailRunCtx,
    evaluator: Arc<dyn GuardrailEvaluator>,
    observer: Arc<dyn StepObserver>,
}

impl fmt::Debug for InboundChainGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InboundChainGuard")
            .field("guardrails", &self.chain.len())
            .finish()
    }
}

impl InboundChainGuard {
    /// The guard for one turn, or `None` when the chain is empty (nothing to
    /// check: the backend then behaves exactly as without a guard).
    pub fn for_turn(
        chain: &[ResolvedGuardrail],
        ctx: &GuardrailRunCtx,
        evaluator: Arc<dyn GuardrailEvaluator>,
        observer: Arc<dyn StepObserver>,
    ) -> Option<Arc<dyn AttachmentTextGuard>> {
        if chain.is_empty() {
            return None;
        }
        Some(Arc::new(Self {
            chain: chain.to_vec(),
            ctx: ctx.clone(),
            evaluator,
            observer,
        }))
    }
}

impl AttachmentTextGuard for InboundChainGuard {
    fn check(&self, text: &str) -> AttachmentTextVerdict {
        let errored = AtomicBool::new(false);
        let tracking = ErrorTracking {
            inner: self.evaluator.as_ref(),
            errored: &errored,
        };
        match run_chain(
            &self.chain,
            GuardrailDirection::Inbound,
            text.to_string(),
            &self.ctx,
            &tracking,
        ) {
            ChainOutcome::Pass {
                content,
                observations,
            } => {
                for obs in &observations {
                    self.observer.on_guardrail(obs);
                }
                if errored.load(Ordering::SeqCst) {
                    // `run_chain` let an agent-level guardrail fail open; a
                    // document nobody could check is withheld instead.
                    tracing::warn!(
                        code = "attachment_guardrail_error",
                        "a guardrail could not check an attachment's text; withholding it"
                    );
                    AttachmentTextVerdict::Withhold
                } else {
                    AttachmentTextVerdict::Allow(content)
                }
            }
            ChainOutcome::Denied { observation, .. } => {
                // The deny reason is dropped: it may quote the text.
                self.observer.on_guardrail(&observation);
                AttachmentTextVerdict::Withhold
            }
        }
    }
}

/// Records whether any evaluation failed, so an error the chain tolerated
/// (agent-level, fail-open) can still withhold the document.
struct ErrorTracking<'a> {
    inner: &'a dyn GuardrailEvaluator,
    errored: &'a AtomicBool,
}

impl GuardrailEvaluator for ErrorTracking<'_> {
    fn evaluate(
        &self,
        extension_id: &str,
        input: &GuardrailInput,
    ) -> Result<GuardrailVerdict, GuardrailInvokeError> {
        let out = self.inner.evaluate(extension_id, input);
        if out.is_err() {
            self.errored.store(true, Ordering::SeqCst);
        }
        out
    }
}

#[cfg(test)]
#[path = "attachment_guard_tests.rs"]
mod tests;
