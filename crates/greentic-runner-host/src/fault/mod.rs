//! Fault injection for resilience testing.
//!
//! The injection machinery (env-var driven: `GREENTIC_FAIL_*`) is compiled only
//! with the `fault-injection` feature, so a production build cannot be steered
//! into dropping state writes or failing asset fetches by an environment
//! variable. Without the feature this module exposes the same two entry points
//! as inert pass-throughs, which keeps call sites (`storage::state`, `pack`)
//! free of `cfg` noise.

#[cfg(feature = "fault-injection")]
mod enabled;
#[cfg(feature = "fault-injection")]
pub use enabled::*;

#[cfg(not(feature = "fault-injection"))]
mod disabled {
    use anyhow::Result;

    use crate::storage::state::DynStateStore;

    /// Pass-through: fault injection is not compiled in.
    pub fn wrap_state_store(store: DynStateStore) -> DynStateStore {
        store
    }

    /// Always succeeds: fault injection is not compiled in.
    pub async fn maybe_fail_asset(_reference: &str) -> Result<()> {
        Ok(())
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[tokio::test]
        async fn hooks_are_inert_without_the_feature() {
            assert!(maybe_fail_asset("oci://example/x:1").await.is_ok());
            let store = crate::storage::state::new_state_store();
            let wrapped = wrap_state_store(store.clone());
            assert!(std::sync::Arc::ptr_eq(&store, &wrapped));
        }
    }
}
#[cfg(not(feature = "fault-injection"))]
pub use disabled::*;
