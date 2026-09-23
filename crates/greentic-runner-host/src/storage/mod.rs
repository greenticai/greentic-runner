//! Session and flow-state stores, and how a host chooses between them.
//!
//! See [`config`] for the backend choices and for what the two backing crates
//! do about namespacing.

pub mod config;
mod offload;
pub mod session;
pub mod state;

use std::sync::Arc;

use anyhow::{Context, Result};

use crate::engine::host::{SessionHost, StateHost};
pub use config::{SessionBackend, StateBackend, StorageConfig, StorageConfigError};
pub use session::DynSessionStore;
pub use state::DynStateStore;

pub fn new_session_store() -> DynSessionStore {
    session::new_session_store()
}

pub fn new_state_store() -> DynStateStore {
    state::new_state_store()
}

/// Build the session store named by `backend`.
///
/// A configured-but-unreachable backend is an `Err` here, before the host
/// exists — never a quiet fall back to memory, which would look like a healthy
/// host that silently restarts every conversation.
pub fn session_store_from_config(backend: &SessionBackend) -> Result<DynSessionStore> {
    session::store_from_config(backend)
}

/// Build the flow-state store named by `backend`. Same failure contract as
/// [`session_store_from_config`].
pub fn state_store_from_config(backend: &StateBackend) -> Result<DynStateStore> {
    state::store_from_config(backend)
}

/// Build both stores, probing each durable one for reachability.
///
/// The probe is the reason this is fallible and eager. `redis::Client::open`
/// only parses the URL — it opens no socket — so without a probe an unreachable
/// Redis would build a perfectly healthy-looking host whose first parked flow
/// fails minutes later, at request time, in front of a user.
pub fn stores_from_config(config: &StorageConfig) -> Result<(DynSessionStore, DynStateStore)> {
    let session = session_store_from_config(&config.session)
        .context("failed to build the runner session store")?;
    let state =
        state_store_from_config(&config.state).context("failed to build the runner state store")?;
    Ok((session, state))
}

pub fn session_host_from(store: DynSessionStore) -> Arc<dyn SessionHost> {
    session::session_host_from(store)
}

pub fn state_host_from(store: DynStateStore) -> Arc<dyn StateHost> {
    state::state_host_from(store)
}
