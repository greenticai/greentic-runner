//! The single-use `decision_token` an `approval.call` dispatch is issued
//! (greentic-runner#794; approval rail contract v2 §4, which lives in
//! greentic-designer as `docs/approval-rail-contract-v2.md`).
//!
//! Before this, the resume path accepted any response whose correlation id was
//! merely present — and the id is derived from the session, pack and flow, so
//! anyone able to publish on `greentic.approval.response.v1` could construct a
//! plausible one and approve a parked flow.
//!
//! # Wire placement
//!
//! * Request: `routing.decision_token`, a top-level `routing` object beside
//!   `target` / `operation` / `input` (contract §2). greentic-designer-admin
//!   seals it from exactly that path and greentic-start relays it verbatim.
//! * Response: `output.decision_token` (contract §3). greentic-designer-admin
//!   echoes it there.
//!
//! # What is stored
//!
//! Only `sha256(token)`, hex-encoded, in the parked snapshot beside the
//! correlation id (`ExecutionState::pending_approval_token`). The plaintext
//! exists in memory for the publish and — when a `deadline_ms` is set — for the
//! runner's own watchdog, which must authenticate its `timeout` response like
//! any other sender. The token is never logged, at any level, in any form.
//!
//! # Rules
//!
//! * A park that HAS a fingerprint resumes only on a response carrying the
//!   matching token. Missing or wrong: the response is dropped with a `warn`
//!   and the gate stays parked.
//! * Single use: the fingerprint is removed with the park when the decision is
//!   applied, so a replay has nothing left to match.
//! * A park WITHOUT a fingerprint was parked by a runner predating this module
//!   and is resumed exactly as before. That exemption drains by itself: every
//!   park written from now on carries one.

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde_json::Value;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

/// Entropy per token: 32 bytes (contract §4), well above the 128-bit floor.
const TOKEN_BYTES: usize = 32;

/// A freshly minted token and the fingerprint to store for it.
pub(crate) struct IssuedToken {
    /// The bearer credential. Goes on the wire and nowhere else.
    pub(crate) token: String,
    /// `sha256(token)`, lowercase hex. The only form that is persisted.
    pub(crate) fingerprint: String,
}

/// Mint a single-use token: 32 random bytes, base64url without padding
/// (43 characters, `[A-Za-z0-9_-]`).
pub(crate) fn issue() -> IssuedToken {
    use rand::{RngExt, rng};
    let mut bytes = [0u8; TOKEN_BYTES];
    rng().fill(&mut bytes);
    let token = URL_SAFE_NO_PAD.encode(bytes);
    let fingerprint = fingerprint(&token);
    IssuedToken { token, fingerprint }
}

/// `sha256(token)` as lowercase hex.
pub(crate) fn fingerprint(token: &str) -> String {
    hex::encode(Sha256::digest(token.as_bytes()))
}

/// Whether `presented` is the token `stored` is the fingerprint of.
///
/// The comparison is over the two digests, in constant time. An absent or
/// blank token never matches.
pub(crate) fn verify(presented: Option<&str>, stored: &str) -> bool {
    let Some(presented) = presented.map(str::trim).filter(|t| !t.is_empty()) else {
        return false;
    };
    let Ok(stored) = hex::decode(stored) else {
        return false;
    };
    let presented = Sha256::digest(presented.as_bytes());
    stored.len() == presented.len() && bool::from(presented.as_slice().ct_eq(stored.as_slice()))
}

/// The token a response envelope (`{ok, output, events, error}`) carries at
/// `output.decision_token`, if any.
pub(crate) fn from_response(response: &Value) -> Option<&str> {
    response
        .pointer("/output/decision_token")
        .and_then(Value::as_str)
}

/// Remove `output.decision_token` from a response before it becomes a node
/// output, so a spent credential does not travel on through flow state,
/// templates and traces.
pub(crate) fn strip_from_response(response: &mut Value) {
    if let Some(output) = response.get_mut("output").and_then(Value::as_object_mut) {
        output.remove("decision_token");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn an_issued_token_is_43_url_safe_chars_and_fresh_per_dispatch() {
        let a = issue();
        let b = issue();
        assert_eq!(a.token.len(), 43);
        assert!(
            a.token
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
        );
        assert_ne!(a.token, b.token, "two dispatches must not share a token");
        assert_ne!(a.fingerprint, b.fingerprint);
        assert_eq!(a.fingerprint, fingerprint(&a.token));
        assert_ne!(a.fingerprint, a.token, "only the digest is stored");
    }

    #[test]
    fn verify_accepts_only_the_issued_token() {
        let issued = issue();
        assert!(verify(Some(&issued.token), &issued.fingerprint));
        assert!(!verify(Some(&issue().token), &issued.fingerprint));
        assert!(!verify(None, &issued.fingerprint));
        assert!(!verify(Some(""), &issued.fingerprint));
        assert!(!verify(Some("   "), &issued.fingerprint));
        assert!(!verify(Some(&issued.token), "not-hex"));
        assert!(!verify(Some(&issued.token), "abcd"));
    }

    #[test]
    fn the_token_is_read_from_output_and_stripped_from_it() {
        let mut response = json!({
            "ok": true,
            "output": {"decision": "approved", "decision_token": "tok"},
            "events": [],
            "error": null,
        });
        assert_eq!(from_response(&response), Some("tok"));
        strip_from_response(&mut response);
        assert_eq!(from_response(&response), None);
        assert_eq!(response["output"]["decision"], "approved");
        // A top-level token is not where the contract puts it.
        assert_eq!(from_response(&json!({"decision_token": "tok"})), None);
    }
}
