//! In-runtime SQL gateway: serves `/sql/<conn>/schema` + `/sql/<conn>/query`
//! over the runner's axum server, backed by read-only sqlx pools, so the
//! `greentic.sql` design extension can run text-to-data inside one bundle.

pub mod config;
pub mod execute;
pub mod introspect;
pub mod pool;
pub mod routes;

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use introspect::Schema;
use pool::ConnectionPool;

/// One configured connection's live pool + engine.
pub struct SqlConnection {
    pub engine: config::Engine,
    pub pool: ConnectionPool,
}

/// Runtime state for the SQL gateway: connections, the cached schema per
/// connection, and the shared bearer token expected on `/sql` requests.
#[derive(Clone)]
pub struct SqlGateway {
    inner: Arc<SqlGatewayInner>,
}

struct SqlGatewayInner {
    connections: HashMap<String, SqlConnection>,
    // Populated and read by the /schema route (Task 5); suppress dead_code until wired.
    #[allow(dead_code)]
    schema_cache: RwLock<HashMap<String, Schema>>,
    auth_token: String,
}

impl SqlGateway {
    #[must_use]
    pub fn new(connections: HashMap<String, SqlConnection>, auth_token: String) -> Self {
        if auth_token.is_empty() {
            tracing::warn!(
                "sql gateway has an empty auth token: all /sql requests will be rejected"
            );
        }
        Self {
            inner: Arc::new(SqlGatewayInner {
                connections,
                schema_cache: RwLock::new(HashMap::new()),
                auth_token,
            }),
        }
    }

    #[must_use]
    pub fn connection(&self, name: &str) -> Option<&SqlConnection> {
        self.inner.connections.get(name)
    }

    #[must_use]
    pub fn check_token(&self, presented: &str) -> bool {
        // Fail closed: a gateway built with an empty token (the placeholder
        // used when no SQL connections are configured) authenticates nobody,
        // not even an empty bearer.
        if self.inner.auth_token.is_empty() {
            return false;
        }
        // Constant-time compare over fixed-size digests: no length leak and no
        // timing-based token enumeration.
        use sha2::{Digest, Sha256};
        use subtle::ConstantTimeEq;
        let presented = Sha256::digest(presented.as_bytes());
        let expected = Sha256::digest(self.inner.auth_token.as_bytes());
        presented.ct_eq(&expected).into()
    }

    // Used by the /schema route handler (Task 5).
    #[allow(dead_code)]
    pub(crate) fn cache(&self) -> &RwLock<HashMap<String, Schema>> {
        &self.inner.schema_cache
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_token_rejects_everything() {
        let gw = SqlGateway::new(HashMap::new(), String::new());
        assert!(!gw.check_token(""));
        assert!(!gw.check_token("anything"));
    }

    #[test]
    fn configured_token_is_checked() {
        let gw = SqlGateway::new(HashMap::new(), "tok".to_string());
        assert!(gw.check_token("tok"));
        assert!(!gw.check_token("tok2"));
        assert!(!gw.check_token(""));
    }
}
