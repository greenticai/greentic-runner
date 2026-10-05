//! HTTP client for the admin's user-ledger door. The client itself lands in
//! Phase C Task 2; this file starts with the target type the host passes in.

/// Where and as whom one unit reads and writes the user ledger. greentic-start
/// builds it from the unit's staged `metering {endpoint, token}` block:
/// `base_url` = the worker-usage endpoint with its last segment swapped for
/// `ledger` (`{admin}/api/v1/ingest/ledger`).
#[derive(Clone)]
pub struct UserLedgerTarget {
    /// `{admin}/api/v1/ingest/ledger` — the client appends `/read`, `/append`.
    pub base_url: String,
    /// The unit's `gtm_` worker-usage token. Only ever sent as a header.
    pub token: secrecy::SecretString,
    /// The workspace slug the token belongs to; the door refuses any other.
    pub tenant_slug: String,
}

impl std::fmt::Debug for UserLedgerTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UserLedgerTarget")
            .field("base_url", &self.base_url)
            .field("token", &"<redacted>")
            .field("tenant_slug", &self.tenant_slug)
            .finish()
    }
}
