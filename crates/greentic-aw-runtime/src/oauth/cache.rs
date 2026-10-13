//! Per-key token cache state. One `tokio` mutex per key gives single-flight
//! refresh: concurrent callers for the same route queue behind the one that is
//! fetching, then read what it stored.

use std::time::Instant;

use secrecy::SecretString;

use super::provider::OAuthError;

/// What the cache remembers for one key.
pub(super) enum Slot {
    /// A live token, valid until `valid_until` (already net of the skew).
    Token {
        fingerprint: [u8; 32],
        token: SecretString,
        valid_until: Instant,
    },
    /// The endpoint failed; do not ask again until `until`.
    Failed {
        fingerprint: [u8; 32],
        until: Instant,
        error: OAuthError,
    },
}

impl Slot {
    /// A token that is still good for `fingerprint` at `now`.
    pub(super) fn live_token(&self, fingerprint: &[u8; 32], now: Instant) -> Option<&SecretString> {
        match self {
            Slot::Token {
                fingerprint: held,
                token,
                valid_until,
            } if held == fingerprint && now < *valid_until => Some(token),
            _ => None,
        }
    }

    /// A recent failure for the same inputs. A changed credential or endpoint
    /// (a different fingerprint) is a fresh question and is never held back.
    pub(super) fn recent_failure(
        &self,
        fingerprint: &[u8; 32],
        now: Instant,
    ) -> Option<&OAuthError> {
        match self {
            Slot::Failed {
                fingerprint: held,
                until,
                error,
            } if held == fingerprint && now < *until => Some(error),
            _ => None,
        }
    }
}
