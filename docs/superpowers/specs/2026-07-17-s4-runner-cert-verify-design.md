# S4 — the runner verifies the embedded publisher certificate

**Date:** 2026-07-17
**Status:** Design, approved for planning
**Epic:** did:web trust root. S1 (verify lib), S2 (ceremony), S3a (mint-cert) are merged in `greenticai/greentic-trust`; S3b (store-server embeds the cert) is merged. S4 is the last slice — the runner verifying the embedded cert closes the chain.
**Repo:** `greentic-runner` (**greenticai** remote — same org as greentic-trust; toolchain 1.95.0). Default branch `main`.

## Problem

The runner's `mcp_store_pull` path pulls a `.gtxpack`, checks its sha256, then verifies the `describe.json` Ed25519 signature — today against `GREENTIC_MCP_TRUSTED_SIGNERS`, a flat env allowlist (`mcp_store_pull/mod.rs:154`, `verify_describe_signature` + `trusted_signers()`). That allowlist is set in **zero deployments**, so `trusted_signers()` returns empty and verification fails closed — the check is effectively dormant on this path in production. It gives no attribution and no root anchoring: any key in the (empty) list would be trusted for any component.

S3b made store-server embed a root-signed `PublisherCert` into `signature.certificate`. S4 makes the runner verify it: the describe signature must be made by the key a `PublisherCert` vouches for, and that cert must chain to the did:web trust root. This replaces "trust a flat allowlist" with "trust a key the Greentic root vouched for" — attributable, anchored, and the authoritative enforcement point (store-server's attach-time check in S3b is only defense-in-depth).

## The crux: S1 already wrote the verification

`greentic_trust::verify_describe` (S1's `chain.rs`, `pub`) IS the whole check:

```rust
pub async fn verify_describe(
    describe: &serde_json::Value,
    trusted_did: &DidWeb,
    resolver: &dyn RootResolver,
    now: DateTime<Utc>,
) -> Result<VerifyingKey, TrustError>;
```

It runs the six-step chain: parse the signature block; require the `certificate` (absent → `TrustError::CertMissing`); resolve the trusted DID to its root keys; verify the cert chains to a root and has not expired; bind (the cert vouches for the key that signed); and verify the describe signature with that certified key. The runner writes **none** of this — S4 wires it in. The heavy lifting was S1.

## Decisions

### D1 — DID-gated: cert enforcement is on iff a trusted DID is configured

A new config `GREENTIC_RUNNER_TRUST_DID: Option<String>` (e.g. `did:web:trust.greentic.cloud`).

- **DID set** → at the verify step, call `greentic_trust::verify_describe(&describe, &did, &resolver, Utc::now())`. A cert-less, foreign-root, expired, mis-bound, or bad-signature artifact is rejected with a clear error.
- **DID unset** → today's behaviour, unchanged: `verify_describe_signature(&describe, &trusted_signers())` (the flat allowlist, empty/dormant in current deployments).

This is a clean, opt-in cutover per deployment. Because the production trust root does not exist yet (the ceremony is org-blocked), no deployment sets the DID today, so **prod behaviour is unchanged until the DID + ceremony land** — the machinery ships inert, like every prior slice.

**Accepted consequence:** once a deployment sets the DID, artifacts *without* a cert are rejected. That is intentional — you set the DID only after store-server (S3b) is emitting certs and the relevant artifacts have been re-published with one. There is no soft "permissive" ramp; the gate is the config flag itself. (A permissive→enforce mode was considered and rejected as more machinery than this slice needs — the DID-unset path already gives a safe "off" state.)

### D2 — the trust root is resolved from the DID, not hard-configured

Same as store-server S3b: the runner constructs a `greentic_trust::HttpResolver` and resolves the configured DID at verify time, so a root rotation is picked up automatically. The resolver has a TTL cache and is constructed once (when the DID is set) and shared across pulls, not rebuilt per pull.

### D3 — depend on greentic-trust as a rev-pinned git dependency (same-org — no cross-org gate)

`greentic-trust` is private and unpublished, so a git dependency pinned to a rev:

```toml
greentic-trust = { git = "https://github.com/greenticai/greentic-trust", rev = "725fb4c" }
```

Unlike S3b (store-server was greentic-biz, a cross-org access prerequisite), **the runner is greenticai — the same org as greentic-trust — so its CI already has read access.** No cross-org infra gate. The toolchains match (both 1.95.0). Locally the dep resolves via the existing git-fetch-with-cli + ssh config.

## Architecture

| Unit | Change |
|---|---|
| workspace `Cargo.toml` + `greentic-aw-runtime/Cargo.toml` | add the `greentic-trust` git dep |
| runner config | `GREENTIC_RUNNER_TRUST_DID: Option<String>`, read from env, threaded to where the pull path is invoked |
| `mcp_store_pull` | construct/hold an `HttpResolver` when the DID is set; at `mod.rs:154`, branch: DID set → `verify_describe`; unset → the existing `verify_describe_signature(&describe, &trusted_signers())` |
| `StorePullError` | map `greentic_trust::TrustError` into the signature-failure variant with the specific reason (cert-missing / foreign-root / expired / key-mismatch / bad-signature), so failures are legible |

The verification is `greentic_trust::verify_describe` — six steps, cert-required, cert→root, bind, signature. The runner owns only the wiring: the dep, the config, the resolver lifecycle, the gated branch, and the error mapping.

**Resolver lifecycle detail for the plan:** `pull` is async and currently takes no resolver. The plan threads an `Option<&HttpResolver>` (or a small trust context) into the pull path from wherever it is invoked, or holds it in the surrounding store-pull state. Constructing the resolver per pull would work (pulls are infrequent — one per component install) but discards the TTL cache across pulls; construct-once-and-share is preferred. The plan picks the threading that fits the existing call graph with the least churn.

`now` is `chrono::Utc::now()` — the runner is a live process with a clock (greentic-trust itself forbids `Utc::now()` internally, which is why the caller supplies it).

## Testing

Every behaviour that matters has a test, and each security invariant carries the mutation that must turn it red. Tests build a fixture root, serve a fixture did:web document, and construct a describe carrying an embedded cert — all via greentic-trust's own `ceremony::{generate_root, build_document, mint_cert}` and a plain-HTTP stub resolver (`HttpResolver::allow_http()` under greentic-trust's `testing` feature), the same pattern S1's resolver tests and S3b use.

| Invariant | Test | Mutation that must fail it |
|---|---|---|
| **Cert-anchored accept (the star)** | with the DID set, a describe carrying a valid cert (minted by S3a's fn, embedded as S3b does) verifies through the pull path | — |
| Cert-less rejected when DID set | describe with no `signature.certificate` → the pull fails (maps `CertMissing`) | remove the DID-gated branch → cert-less artifact wrongly accepted |
| Foreign-root rejected | a cert signed by a root NOT served by the DID → rejected | — (covered inside `verify_describe`; pinned at the runner boundary) |
| DID-unset = legacy, unchanged | with the DID unset, the pull uses `verify_describe_signature(&trusted_signers())` exactly as today | invert the gate → the cert path runs with no DID configured |

The star test proves the epic end to end within the runner's boundary: a cert minted by S3a's `mint-cert`, embedded the way S3b embeds it, is verified by the runner using the same greentic-trust `verify_describe`. Because mint, embed, and verify all go through one crate, the wire format cannot drift — if it did, this test is red, not production.

## Out of scope for S4

- The production ceremony and a real root/DID config (org-blocked; owner still TBD → Maarten). S4 ships the machinery; prod stays on the legacy path until the DID is set.
- Removing the legacy `trusted_signers()` / `GREENTIC_MCP_TRUSTED_SIGNERS` path — kept as the DID-unset fallback. A later cleanup can retire it once every deployment sets a DID.
- The other verification surfaces (`packc verify`, `greentic-distributor-client`'s dormant DSSE policy) — separate consumers, not this pull path.
- Custody model B (publishers signing client-side) — a separate future epic.

## What this does not buy

S4 makes the runner enforce cert → root on the store-pull path, replacing a flat allowlist that was set nowhere. It does not, by itself, make any production runner verify anything — that needs the DID configured, which needs the root to exist, which is the ceremony that remains the epic's one true blocker. What S4 delivers is the last piece of code: with S1–S4 merged, the moment a real root is published and the DID is set, the whole chain — mint, embed, verify — is live end to end.
