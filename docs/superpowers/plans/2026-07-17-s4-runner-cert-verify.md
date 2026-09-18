# S4 — Runner verifies the embedded publisher certificate — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** When a trusted did:web DID is configured, the runner's store-pull path verifies a pulled artifact's `describe.json` against the root-signed `PublisherCert` embedded in `signature.certificate` (cert → did:web root → binding → signature) instead of the flat `GREENTIC_MCP_TRUSTED_SIGNERS` allowlist; with no DID configured, behaviour is unchanged.

**Architecture:** The whole six-step check already lives in `greentic_trust::verify_describe` (S1). S4 only wires it into `mcp_store_pull`: add the crate as a rev-pinned git dependency, read a new `GREENTIC_RUNNER_TRUST_DID` env var, hold a process-shared `HttpResolver`, and replace the single verify call at `mcp_store_pull/mod.rs:154` with a gated seam — DID set → `verify_describe`; DID unset → the existing `verify_describe_signature(&trusted_signers())`. The runner writes none of the cryptography.

**Tech Stack:** Rust 1.95.0, `greentic-trust` (git dep, `greenticai` org — same org as the runner), `reqwest` 0.12, `chrono` 0.4, `ed25519-dalek` 2.1, `tokio`, `wiremock` 0.6 (test), `serial_test` (test).

## Global Constraints

- Toolchain **1.95.0** (`rust-toolchain.toml`, do not edit). CI runs `bash ci/local_check.sh` (fmt + clippy `-D warnings` + test).
- `#![forbid(unsafe_code)]` at crate roots. **No `unwrap()` / `panic!()` / `expect()` in production (non-test) paths** — use `?` and typed errors.
- **No Claude co-authorship trailers on commits or PRs** (runner CLAUDE.md forbids it). Do not add `Co-Authored-By: Claude…` or "Generated with Claude Code".
- Conventional Commits (`feat:`, `fix:`, `test:`).
- `Cargo.lock` is committed; CI builds `--locked`. Every task that touches `Cargo.toml` commits the updated `Cargo.lock` too.
- **graphify is mandatory and hook-enforced in this repo.** Before reading or grepping any runner source file, run `graphify query "<question>"` (or `graphify explain` / `graphify path`) from the **main** repo root (`/home/bima-pangestu/projects/Works/greentic/greentic-runner`, not the worktree — `graphify-out/` is git-ignored and absent from the worktree) to orient. Only read raw files after graphify has oriented you, or to quote exact lines. This applies to every subagent.
- The `greentic-trust` dependency is pinned to `rev = "725fb4c"` (its `main` HEAD, the S3a merge). Do not float the rev.
- **greentic-trust's `testing` feature is a dev-dependency only.** It gates `HttpResolver::allow_http()` — a production build must not even have that method. Never move `testing` into `[dependencies]`.

---

## File Structure

| File | Responsibility | Change |
|---|---|---|
| workspace root `Cargo.toml` | shared dep table | add `greentic-trust` git dep under `[workspace.dependencies]` |
| `crates/greentic-aw-runtime/Cargo.toml` | crate deps | add `greentic-trust` to `[dependencies]` and (with `testing`) to `[dev-dependencies]` |
| `crates/greentic-aw-runtime/src/mcp_store_pull/mod.rs` | store-pull + verification wiring | new `TRUST_DID_ENV` const, `trust_did()`, `map_trust_error()`, `shared_resolver()`, `verify_certified()`, `verify_authenticity()`; the call at `:154` becomes `verify_authenticity(&describe).await?` |
| `crates/greentic-aw-runtime/src/mcp_store_pull/tests/mod.rs` | test-module wiring | register `mod cert_verify;` |
| `crates/greentic-aw-runtime/src/mcp_store_pull/tests/unit.rs` | no-network unit tests | add dependency smoke test + `trust_did`/`map_trust_error` unit tests |
| `crates/greentic-aw-runtime/src/mcp_store_pull/tests/cert_verify.rs` | cert-path integration tests | new file: star + cert-less + foreign-root + DID-unset-legacy |

**Existing anchors (verify with graphify + Read before editing):**
- `mcp_store_pull/mod.rs:47` — `const TRUSTED_SIGNERS_ENV: &str = "GREENTIC_MCP_TRUSTED_SIGNERS";` (place the new const beside it).
- `mcp_store_pull/mod.rs:70-89` — `pub enum StorePullError { Config, Network, Integrity, Signature, Archive, Io }`. The authenticity variant is `Signature(String)`.
- `mcp_store_pull/mod.rs:112-116` — `pub async fn ensure_cached(component_ref, component_version, component_digest) -> Result<(), StorePullError>`.
- `mcp_store_pull/mod.rs:150-154` — `let (describe, wasm) = unzip_describe_and_wasm(&archive_bytes)?;` then `verify_describe_signature(&describe, &trusted_signers())?;`. `describe` is `serde_json::Value`.
- `mcp_store_pull/mod.rs:227-281` — `pub fn verify_describe_signature(describe: &serde_json::Value, trusted: &[VerifyingKey]) -> Result<(), StorePullError>`.
- `mcp_store_pull/mod.rs:290-298` — `pub fn trusted_signers() -> Vec<VerifyingKey>` (reads `GREENTIC_MCP_TRUSTED_SIGNERS`).
- Test helpers in `tests/mod.rs`: `sample_describe() -> serde_json::Value` (unsigned) and `sign_describe_like_store(&serde_json::Value, &SigningKey) -> Vec<u8>` (JCS-canonicalizes describe-minus-signature, signs, injects `signature { algorithm:"ed25519", publicKey, value }`).

**`greentic_trust` API this plan calls (verified against the pinned rev):**
- `greentic_trust::verify_describe(describe: &serde_json::Value, trusted_did: &DidWeb, resolver: &dyn RootResolver, now: DateTime<Utc>) -> Result<VerifyingKey, TrustError>` (async).
- `greentic_trust::DidWeb::parse(&str) -> Result<DidWeb, TrustError>`.
- `greentic_trust::HttpResolver::new(ttl: Duration, capacity: u64) -> Result<HttpResolver, TrustError>` (production, https-only).
- `greentic_trust::HttpResolver::allow_http(self) -> Result<HttpResolver, TrustError>` — **`testing` feature only** (tests).
- `greentic_trust::TrustError` — `pub` enum; `TrustError::CertMissing` is a unit variant (constructible in tests).
- `greentic_trust::ceremony::mint_cert(root: &SigningKey, publisher: &VerifyingKey, key_id: &str, not_after: &str) -> Result<PublisherCert, TrustError>` (always compiled).
- `greentic_trust::ceremony::build_document(did: &DidWeb, roots: &[VerifyingKey]) -> Result<serde_json::Value, TrustError>` (always compiled) — emits exactly the `did.json` the resolver parses.

---

### Task 1: Add the `greentic-trust` git dependency

**Files:**
- Modify: workspace root `Cargo.toml` (`[workspace.dependencies]`)
- Modify: `crates/greentic-aw-runtime/Cargo.toml` (`[dependencies]`, `[dev-dependencies]`)
- Test: `crates/greentic-aw-runtime/src/mcp_store_pull/tests/unit.rs`

**Interfaces:**
- Produces: the `greentic_trust` crate available to `greentic-aw-runtime` (normal dep) and to its tests (dev-dep with `testing` feature). Later tasks call `greentic_trust::{verify_describe, DidWeb, HttpResolver, TrustError, ceremony}`.

- [ ] **Step 1: Orient with graphify, then read the two Cargo files**

Run from the main repo root: `graphify query "greentic-aw-runtime crate dependencies and workspace dependency table"`. Then Read the workspace root `Cargo.toml` `[workspace.dependencies]` section and `crates/greentic-aw-runtime/Cargo.toml` `[dependencies]` + `[dev-dependencies]`. Confirm `greentic-trust` is absent (it should be).

- [ ] **Step 2: Add the workspace dependency**

In the workspace root `Cargo.toml`, under `[workspace.dependencies]`, add (keep the table's existing ordering/style):

```toml
greentic-trust = { git = "https://github.com/greenticai/greentic-trust", rev = "725fb4c" }
```

- [ ] **Step 3: Add the crate dependency and dev-dependency**

In `crates/greentic-aw-runtime/Cargo.toml`, add to `[dependencies]`:

```toml
greentic-trust = { workspace = true }
```

and to `[dev-dependencies]`:

```toml
greentic-trust = { workspace = true, features = ["testing"] }
```

The `testing` feature (which exposes `HttpResolver::allow_http`) is thus enabled only for test/bench builds; a plain `cargo build --release` never compiles it. Do not add `testing` to `[dependencies]`.

- [ ] **Step 4: Write the dependency smoke test**

Add to `crates/greentic-aw-runtime/src/mcp_store_pull/tests/unit.rs` (match the file's existing `use`/attribute style):

```rust
#[test]
fn greentic_trust_dependency_links() {
    // Smoke: the cross-org git dependency resolves, builds, and links.
    assert!(greentic_trust::DidWeb::parse("did:web:example.com").is_ok());
}
```

- [ ] **Step 5: Run the smoke test (fetches the git dep on first build)**

Run: `cargo test -p greentic-aw-runtime greentic_trust_dependency_links -- --nocapture`
Expected: PASS. First run fetches `greenticai/greentic-trust@725fb4c` via git (local git-fetch-with-cli + ssh `insteadOf` handles auth). If the fetch fails with an auth error, stop and report — this is the cross-org access prerequisite, not a code bug.

- [ ] **Step 6: Confirm the workspace still builds and Cargo.lock updated**

Run: `cargo build -p greentic-aw-runtime --locked` — if this fails with a lock mismatch, run `cargo build -p greentic-aw-runtime` once to update `Cargo.lock`, then re-run `--locked`.
Expected: builds clean; `git status` shows `Cargo.lock` modified.

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml Cargo.lock crates/greentic-aw-runtime/Cargo.toml crates/greentic-aw-runtime/src/mcp_store_pull/tests/unit.rs
git commit -m "feat: add greentic-trust as a rev-pinned dependency for cert verification"
```

---

### Task 2: Config reader and error mapping

**Files:**
- Modify: `crates/greentic-aw-runtime/src/mcp_store_pull/mod.rs` (near `:47` for the const; add helpers alongside `trusted_signers`)
- Test: `crates/greentic-aw-runtime/src/mcp_store_pull/tests/unit.rs`

**Interfaces:**
- Consumes: `StorePullError` (existing, `mod.rs:70-89`), `greentic_trust::TrustError` (Task 1).
- Produces:
  - `pub(crate) const TRUST_DID_ENV: &str = "GREENTIC_RUNNER_TRUST_DID";`
  - `pub(crate) fn trust_did() -> Option<String>` — the configured DID, or `None` when the env var is unset **or empty**.
  - `pub(crate) fn map_trust_error(err: greentic_trust::TrustError) -> StorePullError` — every `TrustError` folds into `StorePullError::Signature` with the underlying reason in the message.

- [ ] **Step 1: Orient with graphify, then read the anchors**

Run: `graphify query "mcp_store_pull trusted_signers and StorePullError variants"`. Read `mod.rs:47` (the `TRUSTED_SIGNERS_ENV` const), `mod.rs:70-89` (`StorePullError`), and `mod.rs:290-298` (`trusted_signers`). Confirm the `Signature(String)` variant name.

- [ ] **Step 2: Write the failing unit tests**

Add to `tests/unit.rs`. The `trust_did` tests mutate a process env var, so mark them `#[serial_test::serial]` (the crate already uses `serial_test`). Reference the helpers by the same crate path the existing tests use for `verify_describe_signature` (mirror an existing `use` line in `unit.rs`; the module path is `crate::mcp_store_pull::{trust_did, map_trust_error, TRUST_DID_ENV}`).

```rust
#[test]
#[serial_test::serial]
fn trust_did_is_none_when_unset() {
    std::env::remove_var(crate::mcp_store_pull::TRUST_DID_ENV);
    assert_eq!(crate::mcp_store_pull::trust_did(), None);
}

#[test]
#[serial_test::serial]
fn trust_did_is_none_when_empty() {
    std::env::set_var(crate::mcp_store_pull::TRUST_DID_ENV, "");
    assert_eq!(crate::mcp_store_pull::trust_did(), None);
    std::env::remove_var(crate::mcp_store_pull::TRUST_DID_ENV);
}

#[test]
#[serial_test::serial]
fn trust_did_returns_the_configured_did() {
    std::env::set_var(crate::mcp_store_pull::TRUST_DID_ENV, "did:web:trust.greentic.cloud");
    assert_eq!(
        crate::mcp_store_pull::trust_did(),
        Some("did:web:trust.greentic.cloud".to_string())
    );
    std::env::remove_var(crate::mcp_store_pull::TRUST_DID_ENV);
}

#[test]
fn map_trust_error_folds_into_signature() {
    let mapped = crate::mcp_store_pull::map_trust_error(greentic_trust::TrustError::CertMissing);
    match mapped {
        crate::mcp_store_pull::StorePullError::Signature(msg) => {
            assert!(msg.contains("trust verification"), "reason should be legible: {msg}");
        }
        other => panic!("expected Signature, got {other:?}"),
    }
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test -p greentic-aw-runtime trust_did map_trust_error 2>&1 | head -40`
Expected: FAIL to compile — `trust_did`, `map_trust_error`, `TRUST_DID_ENV` not found. (Compile failure counts as the failing state here; the symbols do not exist yet.)

- [ ] **Step 4: Implement the const and helpers**

In `mod.rs`, beside `TRUSTED_SIGNERS_ENV` (`:47`):

```rust
/// did:web DID whose published root anchors publisher-certificate verification.
/// When set, the store-pull path verifies the embedded `signature.certificate`
/// (cert -> root -> binding -> signature) instead of the flat allowlist.
pub(crate) const TRUST_DID_ENV: &str = "GREENTIC_RUNNER_TRUST_DID";
```

Alongside `trusted_signers` (`:290`):

```rust
/// The configured trusted DID, or `None` when unset or empty. An empty value
/// is treated as unset so a blank deployment override does not half-enable the
/// cert path.
pub(crate) fn trust_did() -> Option<String> {
    match std::env::var(TRUST_DID_ENV) {
        Ok(value) if !value.trim().is_empty() => Some(value),
        _ => None,
    }
}

/// Fold a `greentic_trust::TrustError` into the store-pull authenticity error,
/// preserving the specific reason (cert-missing, foreign-root, expired,
/// key-mismatch, bad-signature, unreachable DID) in the message.
pub(crate) fn map_trust_error(err: greentic_trust::TrustError) -> StorePullError {
    StorePullError::Signature(format!("did:web trust verification failed: {err}"))
}
```

If `StorePullError` does not already derive `Debug`, the `map_trust_error` test's `{other:?}` needs it — it is an error type so it already will; confirm and leave as-is.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p greentic-aw-runtime trust_did map_trust_error`
Expected: PASS (4 tests).

- [ ] **Step 6: Mutation check — the empty-value guard must be load-bearing**

`git add` the changed files first (an untracked file yields a vacuously-empty diff). Then temporarily change the `trust_did` guard from `if !value.trim().is_empty()` to `if true`, and run `cargo test -p greentic-aw-runtime trust_did_is_none_when_empty`.
Expected: `trust_did_is_none_when_empty` FAILS. Revert the mutation; re-run to confirm PASS. Verify the revert with `git diff -- crates/greentic-aw-runtime/src/mcp_store_pull/mod.rs` is empty.

- [ ] **Step 7: Commit**

```bash
git add crates/greentic-aw-runtime/src/mcp_store_pull/mod.rs crates/greentic-aw-runtime/src/mcp_store_pull/tests/unit.rs
git commit -m "feat: read GREENTIC_RUNNER_TRUST_DID and map TrustError to StorePullError"
```

---

### Task 3: Gated certificate verification in the pull path

**Files:**
- Modify: `crates/greentic-aw-runtime/src/mcp_store_pull/mod.rs` (the `:154` call site; new `shared_resolver`, `verify_certified`, `verify_authenticity`)
- Modify: `crates/greentic-aw-runtime/src/mcp_store_pull/tests/mod.rs` (register `mod cert_verify;`)
- Create: `crates/greentic-aw-runtime/src/mcp_store_pull/tests/cert_verify.rs`

**Interfaces:**
- Consumes: `trust_did()`, `map_trust_error()`, `TRUST_DID_ENV` (Task 2); `verify_describe_signature`, `trusted_signers`, `StorePullError` (existing); `greentic_trust::{verify_describe, DidWeb, HttpResolver}` (Task 1); test helpers `sample_describe`, `sign_describe_like_store` (existing, `tests/mod.rs`).
- Produces:
  - `async fn verify_authenticity(describe: &serde_json::Value) -> Result<(), StorePullError>` — the seam `ensure_cached` calls; routes on `trust_did()`.
  - `pub(crate) async fn verify_certified(describe: &serde_json::Value, did: &str, resolver: &greentic_trust::HttpResolver) -> Result<(), StorePullError>` — the DID-path core (tested directly with an `allow_http` resolver).
  - `fn shared_resolver() -> Result<&'static greentic_trust::HttpResolver, StorePullError>` — process-shared https-only resolver (TTL cache reused across pulls).

- [ ] **Step 1: Orient with graphify, then read the anchors and test wiring**

Run: `graphify query "mcp_store_pull ensure_cached verify_describe_signature call site"`. Read `mod.rs:140-160` (the unzip + verify lines) and `tests/mod.rs` (confirm how the test submodule is declared and how `unit.rs`/`pull.rs` are registered — you will register `cert_verify` the same way). Note the exact `use` path the tests use to reach crate-private items.

- [ ] **Step 2: Write the failing integration tests**

Create `crates/greentic-aw-runtime/src/mcp_store_pull/tests/cert_verify.rs`. Adjust the crate paths in the `use` block to match what Step 1 found (mirror `unit.rs`). This mints a cert with `ceremony::mint_cert`, serves the DID document with `ceremony::build_document`, and verifies through `verify_certified` with an `allow_http` resolver — mint, embed, and verify all through the one crate, so a wire-format drift turns this red, not production.

```rust
use std::time::Duration;

use ed25519_dalek::SigningKey;
use greentic_trust::{DidWeb, HttpResolver};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::mcp_store_pull::{verify_certified, StorePullError};
use super::{sample_describe, sign_describe_like_store};

/// A far-future expiry so the runner's real `Utc::now()` is always before it.
const NOT_AFTER: &str = "2999-01-01T00:00:00Z";

/// did:web pointing at the mock server (plain HTTP, so the resolver must
/// allow it explicitly). Mirrors greentic-trust's own resolver-test `did_for`.
fn did_for(server: &MockServer) -> String {
    let authority = server.uri().trim_start_matches("http://").replace(':', "%3A");
    format!("did:web:{authority}")
}

fn allow_http_resolver() -> HttpResolver {
    HttpResolver::new(Duration::from_secs(300), 16)
        .expect("client builds")
        .allow_http()
        .expect("client builds")
}

/// Build a describe signed by `publisher`, then embed `certificate` into its
/// signature block (the order store-server uses at publish: sign over
/// describe-minus-signature, then attach the cert).
fn signed_describe_with_cert(
    publisher: &SigningKey,
    cert: Option<&greentic_trust::PublisherCert>,
) -> serde_json::Value {
    let signed_bytes = sign_describe_like_store(&sample_describe(), publisher);
    let mut describe: serde_json::Value = serde_json::from_slice(&signed_bytes).unwrap();
    if let Some(cert) = cert {
        describe["signature"]["certificate"] = serde_json::to_value(cert).unwrap();
    }
    describe
}

async fn mount_did_document(server: &MockServer, did: &str, root: &SigningKey) {
    let doc = greentic_trust::ceremony::build_document(
        &DidWeb::parse(did).expect("did parses"),
        &[root.verifying_key()],
    )
    .expect("document builds");
    Mock::given(method("GET"))
        .and(path("/.well-known/did.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(doc))
        .mount(server)
        .await;
}

#[tokio::test]
async fn certified_describe_verifies_through_the_pull_path() {
    // The star: cert minted (S3a), embedded (S3b-style), verified (S4) — all
    // through greentic-trust, end to end within the runner's boundary.
    let server = MockServer::start().await;
    let did = did_for(&server);
    let root = SigningKey::from_bytes(&[7u8; 32]);
    let publisher = SigningKey::from_bytes(&[9u8; 32]);
    mount_did_document(&server, &did, &root).await;

    let cert = greentic_trust::ceremony::mint_cert(
        &root,
        &publisher.verifying_key(),
        "store-key-1",
        NOT_AFTER,
    )
    .expect("cert mints");
    let describe = signed_describe_with_cert(&publisher, Some(&cert));

    verify_certified(&describe, &did, &allow_http_resolver())
        .await
        .expect("a root-vouched cert must verify");
}

#[tokio::test]
async fn certless_describe_is_rejected_when_did_set() {
    let server = MockServer::start().await;
    let did = did_for(&server);
    let root = SigningKey::from_bytes(&[7u8; 32]);
    let publisher = SigningKey::from_bytes(&[9u8; 32]);
    mount_did_document(&server, &did, &root).await;

    let describe = signed_describe_with_cert(&publisher, None);

    let err = verify_certified(&describe, &did, &allow_http_resolver())
        .await
        .expect_err("a describe with no certificate must be rejected");
    assert!(matches!(err, StorePullError::Signature(_)), "got {err:?}");
}

#[tokio::test]
async fn foreign_root_cert_is_rejected() {
    let server = MockServer::start().await;
    let did = did_for(&server);
    let served_root = SigningKey::from_bytes(&[7u8; 32]);
    let foreign_root = SigningKey::from_bytes(&[8u8; 32]);
    let publisher = SigningKey::from_bytes(&[9u8; 32]);
    // The DID publishes served_root, but the cert is signed by foreign_root.
    mount_did_document(&server, &did, &served_root).await;

    let cert = greentic_trust::ceremony::mint_cert(
        &foreign_root,
        &publisher.verifying_key(),
        "store-key-1",
        NOT_AFTER,
    )
    .expect("cert mints");
    let describe = signed_describe_with_cert(&publisher, Some(&cert));

    let err = verify_certified(&describe, &did, &allow_http_resolver())
        .await
        .expect_err("a cert not chaining to the published root must be rejected");
    assert!(matches!(err, StorePullError::Signature(_)), "got {err:?}");
}
```

- [ ] **Step 3: Register the test module and run to verify it fails**

Add `mod cert_verify;` to `tests/mod.rs` (beside the existing `mod unit;` / `mod pull;`).
Run: `cargo test -p greentic-aw-runtime cert_verify 2>&1 | head -40`
Expected: FAIL to compile — `verify_certified` not found.

- [ ] **Step 4: Implement the resolver, the DID-path core, and the seam**

Add to `mod.rs` (imports at the top: `use std::sync::OnceLock; use std::time::Duration; use chrono::Utc; use greentic_trust::DidWeb;` — match the file's existing import grouping):

```rust
/// TTL and capacity for the shared did:web resolver cache.
const RESOLVER_TTL: Duration = Duration::from_secs(300);
const RESOLVER_CAPACITY: u64 = 16;

/// Process-shared https-only resolver, so its TTL cache is reused across pulls
/// rather than rebuilt per artifact. Built lazily the first time a DID is set.
fn shared_resolver() -> Result<&'static greentic_trust::HttpResolver, StorePullError> {
    static RESOLVER: OnceLock<greentic_trust::HttpResolver> = OnceLock::new();
    if let Some(resolver) = RESOLVER.get() {
        return Ok(resolver);
    }
    let built = greentic_trust::HttpResolver::new(RESOLVER_TTL, RESOLVER_CAPACITY)
        .map_err(|e| StorePullError::Config(format!("failed to build trust resolver: {e}")))?;
    // If another thread initialised first, `set` returns the value back; either
    // way `get` is populated afterwards.
    let _ = RESOLVER.set(built);
    RESOLVER
        .get()
        .ok_or_else(|| StorePullError::Config("trust resolver unavailable after init".into()))
}

/// Verify a describe against a configured trusted DID: the embedded
/// `signature.certificate` must chain to the DID's published root, vouch for
/// the signing key, and cover a valid describe signature. The whole check is
/// `greentic_trust::verify_describe` (S1); the runner only wires it.
pub(crate) async fn verify_certified(
    describe: &serde_json::Value,
    did: &str,
    resolver: &greentic_trust::HttpResolver,
) -> Result<(), StorePullError> {
    let did = DidWeb::parse(did)
        .map_err(|e| StorePullError::Config(format!("invalid {TRUST_DID_ENV}: {e}")))?;
    greentic_trust::verify_describe(describe, &did, resolver, Utc::now())
        .await
        .map(|_key| ())
        .map_err(map_trust_error)
}

/// Authenticity gate: with a trusted DID configured, verify the embedded
/// publisher certificate; without one, fall back to the flat allowlist exactly
/// as before.
async fn verify_authenticity(describe: &serde_json::Value) -> Result<(), StorePullError> {
    match trust_did() {
        Some(did) => verify_certified(describe, &did, shared_resolver()?).await,
        None => verify_describe_signature(describe, &trusted_signers()),
    }
}
```

Then replace the call at `mod.rs:154`:

```rust
// 4. Authenticity: embedded publisher cert -> did:web root when a trusted DID
//    is configured; otherwise the flat allowlist.
verify_authenticity(&describe).await?;
```

- [ ] **Step 5: Run the cert-path tests to verify they pass**

Run: `cargo test -p greentic-aw-runtime cert_verify`
Expected: PASS (3 tests: star, cert-less, foreign-root).

- [ ] **Step 6: Add the DID-unset legacy test**

Append to `tests/cert_verify.rs` (env-mutating → `#[serial_test::serial]`):

```rust
#[tokio::test]
#[serial_test::serial]
async fn did_unset_uses_the_legacy_allowlist() {
    use crate::mcp_store_pull::{verify_authenticity_for_test, TRUST_DID_ENV};

    std::env::remove_var(TRUST_DID_ENV);
    let publisher = SigningKey::from_bytes(&[9u8; 32]);
    // A describe with NO certificate, signed by `publisher`; the legacy path
    // must accept it purely on the allowlist.
    let describe = signed_describe_with_cert(&publisher, None);
    let public_b64 = base64::engine::general_purpose::STANDARD
        .encode(publisher.verifying_key().as_bytes());
    std::env::set_var(
        "GREENTIC_MCP_TRUSTED_SIGNERS",
        format!("ed25519:{public_b64}"),
    );

    verify_authenticity_for_test(&describe)
        .await
        .expect("legacy allowlist accepts the cert-less describe when no DID is set");

    std::env::remove_var("GREENTIC_MCP_TRUSTED_SIGNERS");
}
```

`verify_authenticity` is private; expose a thin test hook next to it in `mod.rs` so the test can drive the real routing without making the seam `pub`:

```rust
#[cfg(test)]
pub(crate) async fn verify_authenticity_for_test(
    describe: &serde_json::Value,
) -> Result<(), StorePullError> {
    verify_authenticity(describe).await
}
```

Confirm the allowlist entry format: read `parse_trusted_signer` (referenced by `trusted_signers`, `mod.rs` near `:290`) with graphify/Read and match its exact expected string (the `ed25519:<base64>` shape above is the assumption — **use whatever `parse_trusted_signer` actually parses**). Also confirm `base64::engine::general_purpose::STANDARD` is the encoding `sign_describe_like_store` and `parse_trusted_signer` use; mirror them.

- [ ] **Step 7: Run the legacy test**

Run: `cargo test -p greentic-aw-runtime did_unset_uses_the_legacy_allowlist`
Expected: PASS.

- [ ] **Step 8: Mutation check — the gate must be load-bearing**

`git add` all changed files first. Then apply each mutation, run the named test, confirm it fails, and revert:

1. **Cert requirement / error propagation.** In `verify_certified`, change `.map_err(map_trust_error)` to `.map_or(Ok(()), |_| Ok(()))` (swallow the result). Run `cargo test -p greentic-aw-runtime certless_describe_is_rejected_when_did_set foreign_root_cert_is_rejected`. Expected: both FAIL. Revert.
2. **The gate direction.** In `verify_authenticity`, swap the arms so `Some(did) => verify_describe_signature(describe, &trusted_signers())` and `None => verify_certified(...)` — if this does not compile (the `None` arm has no `did`), that itself proves the arms are not interchangeable; instead mutate `trust_did()`'s body to `return None;` unconditionally and run `cargo test -p greentic-aw-runtime cert_verify` — the star test `certified_describe_verifies_through_the_pull_path` still passes (it calls `verify_certified` directly), but `did_unset_uses_the_legacy_allowlist` is unaffected too, so instead assert the gate via: mutate `trust_did()` to `Some("did:web:example.com".to_string())` unconditionally and run `cargo test -p greentic-aw-runtime did_unset_uses_the_legacy_allowlist`. Expected: FAIL (routing now hits the cert path with an unreachable DID). Revert.

Verify each revert with `git diff -- crates/greentic-aw-runtime/src/mcp_store_pull/mod.rs` empty before moving on.

- [ ] **Step 9: Full crate gate**

Run:
```bash
cargo fmt --all -- --check
cargo clippy -p greentic-aw-runtime --all-targets --all-features -- -D warnings
cargo test -p greentic-aw-runtime
```
Expected: all clean; the existing `pull.rs`/`unit.rs` legacy tests still pass unchanged (DID unset by default), proving backward compatibility.

- [ ] **Step 10: Commit**

```bash
git add crates/greentic-aw-runtime/src/mcp_store_pull/mod.rs crates/greentic-aw-runtime/src/mcp_store_pull/tests/mod.rs crates/greentic-aw-runtime/src/mcp_store_pull/tests/cert_verify.rs
git commit -m "feat: verify embedded publisher cert on store pull when a trusted DID is set"
```

---

### Task 4: Whole-repo gate and docs touch-up

**Files:**
- Possibly modify: a runner config/env doc if one enumerates `GREENTIC_MCP_*` env vars (find via graphify; add `GREENTIC_RUNNER_TRUST_DID`). Skip if none exists — do not invent a doc.

- [ ] **Step 1: Find any env-var documentation**

Run: `graphify query "GREENTIC_MCP_TRUSTED_SIGNERS documentation and runner environment variables"`. If a doc/table lists the store-pull env vars, add a row for `GREENTIC_RUNNER_TRUST_DID` (one line: "did:web DID anchoring publisher-certificate verification on store pull; unset = legacy allowlist"). If no such doc exists, note that and skip.

- [ ] **Step 2: Run the repo-canonical local CI**

Run: `bash ci/local_check.sh`
Expected: green. If it fails outside this change's scope, capture the failure for the PR summary rather than hiding it.

- [ ] **Step 3: Commit any doc change**

```bash
git add <doc-file>
git commit -m "docs: document GREENTIC_RUNNER_TRUST_DID for store-pull cert verification"
```

(Skip this commit if Step 1 found no doc to update.)

---

## Self-Review

**Spec coverage:**
- D1 (DID-gated) → Task 2 (`trust_did`) + Task 3 (`verify_authenticity` branch, DID-unset legacy test). ✅
- D2 (resolve DID via `HttpResolver`, construct-once-and-share) → Task 3 (`shared_resolver` `OnceLock`). ✅
- D3 (rev-pinned same-org git dep) → Task 1. ✅
- Architecture table: workspace+crate Cargo (Task 1), config env (Task 2), `mcp_store_pull` branch (Task 3), `TrustError`→`StorePullError` mapping (Task 2 `map_trust_error`). ✅
- Testing table: star (Task 3 Step 2), cert-less + mutation (Task 3 Steps 2/8), foreign-root (Task 3 Step 2), DID-unset legacy + mutation (Task 3 Steps 6/8). ✅

**Placeholder scan:** No TBD/TODO. Two deliberate "confirm against real code" instructions (Task 3 Step 6: `parse_trusted_signer`'s exact allowlist string format and base64 engine; Task 3 Step 1: test-submodule access path) — these are verification steps, not placeholders, because the exact allowlist-entry syntax lives in `parse_trusted_signer`, which was not quoted verbatim in recon. The implementer must read it and match, rather than trust the plan's `ed25519:<base64>` assumption.

**Type consistency:** `verify_certified(&Value, &str, &HttpResolver) -> Result<(), StorePullError>`, `verify_authenticity(&Value) -> Result<(), StorePullError>`, `trust_did() -> Option<String>`, `map_trust_error(TrustError) -> StorePullError`, `shared_resolver() -> Result<&'static HttpResolver, StorePullError>` — used consistently across Tasks 2–3. `greentic_trust::verify_describe` returns `Result<VerifyingKey, TrustError>`; `verify_certified` discards the key with `.map(|_key| ())`. ✅

**Known soft spot (flagged for the reviewer, not blocking):** `map_trust_error` folds *every* `TrustError` — including a genuinely unreachable did:web (transport) — into `StorePullError::Signature`. This is spec-literal ("map into the signature-failure variant") and correct for fail-closed behaviour (the three `ensure_cached` callers only warn on error), but a future refinement could route resolver-transport errors to `StorePullError::Network` for retry semantics. Out of scope for S4.
