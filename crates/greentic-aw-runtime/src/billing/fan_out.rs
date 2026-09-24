//! [`FanOutBillingMeter`]: one sink that is the credit gate, one that only
//! records, both handed every usage event.
//!
//! Exists so installing a recorder (a deployed unit's
//! [`super::WorkerUsageMeter`]) can never switch the cloud-commerce credit
//! gate off: `over_budget` asks the GATE only, and the recorder — which has no
//! wallet — is never consulted for it.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use super::{BillingError, BillingMeter};
use crate::tenant::TenantContext;

/// Emits every event to `gate` and to `recorder`; `over_budget` is the
/// gate's answer alone.
pub struct FanOutBillingMeter {
    gate: Arc<dyn BillingMeter>,
    recorder: Arc<dyn BillingMeter>,
}

impl FanOutBillingMeter {
    /// `gate` answers `over_budget` and receives every event; `recorder`
    /// receives every event and is never asked for a budget.
    pub fn new(gate: Arc<dyn BillingMeter>, recorder: Arc<dyn BillingMeter>) -> Self {
        Self { gate, recorder }
    }
}

impl BillingMeter for FanOutBillingMeter {
    fn emit<'a>(
        &'a self,
        tenant: &'a TenantContext,
        input_tokens: u64,
        output_tokens: u64,
        agent_id: &'a str,
        model: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), BillingError>> + Send + 'a>> {
        Box::pin(async move {
            // Both are fire-and-forget by contract, so this adds no latency
            // worth parallelising. A failure of one never stops the other.
            let gate = self
                .gate
                .emit(tenant, input_tokens, output_tokens, agent_id, model)
                .await;
            let recorder = self
                .recorder
                .emit(tenant, input_tokens, output_tokens, agent_id, model)
                .await;
            gate.and(recorder)
        })
    }

    fn over_budget<'a>(
        &'a self,
        tenant: &'a TenantContext,
    ) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>> {
        self.gate.over_budget(tenant)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    struct Probe {
        emits: Mutex<Vec<(u64, u64, String, String)>>,
        budget_asks: AtomicUsize,
        over: bool,
        fail_emit: bool,
    }

    impl Probe {
        fn new(over: bool, fail_emit: bool) -> Arc<Self> {
            Arc::new(Self {
                emits: Mutex::new(Vec::new()),
                budget_asks: AtomicUsize::new(0),
                over,
                fail_emit,
            })
        }
    }

    impl BillingMeter for Probe {
        fn emit<'a>(
            &'a self,
            _tenant: &'a TenantContext,
            input_tokens: u64,
            output_tokens: u64,
            agent_id: &'a str,
            model: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<(), BillingError>> + Send + 'a>> {
            self.emits.lock().unwrap().push((
                input_tokens,
                output_tokens,
                agent_id.into(),
                model.into(),
            ));
            let fail = self.fail_emit;
            Box::pin(async move {
                if fail {
                    Err(BillingError::Transport("down".into()))
                } else {
                    Ok(())
                }
            })
        }

        fn over_budget<'a>(
            &'a self,
            _tenant: &'a TenantContext,
        ) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>> {
            self.budget_asks.fetch_add(1, Ordering::SeqCst);
            Box::pin(std::future::ready(self.over))
        }
    }

    #[tokio::test]
    async fn every_event_reaches_both_sinks() {
        let (gate, recorder) = (Probe::new(false, false), Probe::new(false, false));
        let fan = FanOutBillingMeter::new(gate.clone(), recorder.clone());
        let t = TenantContext::new("acme", "prod");
        fan.emit(&t, 7, 3, "a", "m").await.unwrap();
        let expected = vec![(7, 3, "a".to_string(), "m".to_string())];
        assert_eq!(*gate.emits.lock().unwrap(), expected);
        assert_eq!(*recorder.emits.lock().unwrap(), expected);
    }

    #[tokio::test]
    async fn the_credit_gate_is_the_gates_alone() {
        // The recorder says "over budget" and must not be believed — nor asked.
        let (gate, recorder) = (Probe::new(true, false), Probe::new(false, false));
        let fan = FanOutBillingMeter::new(gate.clone(), recorder.clone());
        assert!(fan.over_budget(&TenantContext::new("acme", "prod")).await);
        assert_eq!(gate.budget_asks.load(Ordering::SeqCst), 1);
        assert_eq!(recorder.budget_asks.load(Ordering::SeqCst), 0);

        let (gate, recorder) = (Probe::new(false, false), Probe::new(true, false));
        let fan = FanOutBillingMeter::new(gate, recorder);
        assert!(!fan.over_budget(&TenantContext::new("acme", "prod")).await);
    }

    #[tokio::test]
    async fn a_failing_gate_emit_still_reaches_the_recorder() {
        let (gate, recorder) = (Probe::new(false, true), Probe::new(false, false));
        let fan = FanOutBillingMeter::new(gate, recorder.clone());
        let result = fan
            .emit(&TenantContext::new("acme", "prod"), 1, 1, "a", "m")
            .await;
        assert!(result.is_err(), "the gate's error is surfaced");
        assert_eq!(recorder.emits.lock().unwrap().len(), 1);
    }
}
