# A Real HTTP Outbound Timeout (Phase 4a) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** An outbound HTTP call made by a component (via `wasi:http/outgoing-handler`) fails within a bounded, host-configured time instead of hanging the flow forever.

**Architecture:** `wasmtime-wasi-http` calls `WasiHttpHooks::send_request` on every outbound request, handing it a per-request `OutgoingRequestConfig` with three `Duration` fields (connect / first-byte / between-bytes). Today `ComponentState::http()` (`pack.rs`) wires the library's zero-cost `Default::default()` hooks, which just forward to `default_send_request` unmodified — so the library's own 600s-per-field default is the only ceiling. This plan adds a `HttpTimeoutHooks` implementation that clamps each field down to a host-configured ceiling (never raising a shorter guest-supplied value) before delegating to the same `default_send_request`, and wires it into `ComponentState` in place of the no-op default.

**Tech Stack:** Rust, `wasmtime-wasi-http` 45.0.3 (workspace-pinned `"45"`), `tokio`, `hyper` 1.x.

**Spec:** `docs/superpowers/specs/2026-08-24-http-outbound-timeout-design.md` — read it before starting. It records why `tokio::time::timeout` around the component call and wasmtime epoch interruption were both ruled out, and frames this plan's scope as "4a mandatory, 4b conditional on Task 1's finding."

## Global Constraints

- Never run `cargo clippy` or `cargo fmt --check` as separate steps before committing — the pre-commit hook runs both; a silent multi-minute clippy run trips this environment's 600s idle watchdog. Run `cargo fmt --all` if formatting is needed, then commit directly.
- `export TMPDIR=/home/bima-pangestu/projects/.tmp` before any `cargo` invocation in this worktree — the system `/tmp` is a small tmpfs that makes `cargo build`/`cargo test` fail with errors that read exactly like real compile failures.
- Never raise a guest-supplied timeout value. Every clamp is `guest_value.min(ceiling)` — this only ever lowers an absent-or-longer value.
- Do not touch `component-http` (a separate repo) or invent a `greentic_types::Node` timeout field — both are explicitly out of scope per the spec's "Explicitly not doing here" section.
- Symbols, not line numbers, are the anchor of record for anything inside the `wasmtime-wasi-http` crate (its own convention, and line numbers there will drift). Line numbers given below for files inside THIS repo are current as of this plan's writing against `crates/greentic-runner-host/src/pack.rs` and `crates/greentic-runner-host/src/lib.rs` on branch `BimaPangestu28/http-timeout-phase4a` (cut from `origin/research` at `f877b281`) — re-locate by symbol name if they've drifted.

---

## Task 1: Confirm the timeout error shape (the spec's open question)

**Files:**
- Create: `crates/greentic-runner-host/tests/http_timeout_shape.rs`
- Modify: `crates/greentic-runner-host/Cargo.toml` (add `hyper` to `[dev-dependencies]`)

**Interfaces:**
- Consumes: `wasmtime_wasi_http::p2::default_send_request(request: hyper::Request<body::HyperOutgoingBody>, config: types::OutgoingRequestConfig) -> types::HostFutureIncomingResponse` — the same function `WasiHttpHooks::send_request`'s default impl calls, and the one Task 2's `HttpTimeoutHooks` will delegate to.
- Produces: an empirically-confirmed answer to "does a `wasi:http` connect timeout surface as a host-level `Err` or a guest-visible `wasi:http` error-code?" — this decides whether any later task in this plan needs to add error-shape detection in `crates/greentic-runner-host/src/runner/engine.rs`, or whether the existing `component_error`/`has_error_route` path (already shipped in Phase 1–3) already covers it.

This is a characterization test of an external library's documented-but-unverified behavior, not a red/green TDD cycle driving new production code — so Step 2 below asks you to confirm the test **passes** with a specific printed value, not that it fails.

- [ ] **Step 1: Add `hyper` to dev-dependencies**

In `crates/greentic-runner-host/Cargo.toml`, find the `[dev-dependencies]` section (it already has an `http-body-util = "0.1"` line). Add a `hyper = "1"` line immediately after it:

```toml
[dev-dependencies]
criterion.workspace = true
serial_test.workspace = true
tempfile.workspace = true
once_cell.workspace = true
semver.workspace = true
proptest.workspace = true
opentelemetry_sdk = "0.31"
tracing-opentelemetry = "0.32"
tracing-subscriber = { version = "0.3", features = ["registry"] }
tower.workspace = true
http-body-util = "0.1"
hyper = "1"
```

(`bytes` is already available in `[dependencies]` as `bytes.workspace = true` — do not add it again; a duplicate `bytes` key in the same or a different section is fine, but adding it a second time to `[dependencies]` itself is a duplicate-key TOML parse error. Only touch `[dev-dependencies]`.)

- [ ] **Step 2: Write and run the discriminating test**

Create `crates/greentic-runner-host/tests/http_timeout_shape.rs`:

```rust
//! Characterizes how `wasmtime-wasi-http` surfaces a connect timeout, at the
//! exact seam `HttpTimeoutHooks::send_request` (Task 2) will delegate to.
//! This is load-bearing for the rest of this plan: it is what confirmed the
//! timeout is guest-visible (an inner `wasi:http` error-code the component
//! sees), not a host-level `Err`, which is why no new detection code was
//! added to `runner/engine.rs` — the existing `component_error` /
//! `has_error_route` path (Phase 1-3) already routes whatever shape
//! `component-http` reshapes this into.

use std::time::Duration;

use http_body_util::BodyExt;
use wasmtime_wasi_http::p2::default_send_request;
use wasmtime_wasi_http::p2::types::{self, OutgoingRequestConfig};

#[tokio::test]
async fn connect_timeout_is_a_guest_visible_error_code_not_a_host_err() {
    // RFC 5737 TEST-NET-1: reserved for documentation, routers silently drop
    // packets sent to it, so the TCP connect attempt hangs until our
    // configured timeout fires rather than getting an immediate
    // ConnectionRefused (which would prove nothing about timeout behavior).
    let req = hyper::Request::builder()
        .uri("http://192.0.2.1:9")
        .body(
            http_body_util::Empty::<bytes::Bytes>::new()
                .map_err(|_: std::convert::Infallible| unreachable!())
                .boxed_unsync(),
        )
        .unwrap();
    let config = OutgoingRequestConfig {
        use_tls: false,
        connect_timeout: Duration::from_millis(200),
        first_byte_timeout: Duration::from_secs(5),
        between_bytes_timeout: Duration::from_secs(5),
    };

    let resp = default_send_request(req, config);
    let types::HostFutureIncomingResponse::Pending(handle) = resp else {
        panic!("expected Pending — default_send_request always spawns");
    };

    // `FutureIncomingResponseHandle` is `AbortOnDropJoinHandle<wasmtime::Result<
    // Result<IncomingResponse, types::ErrorCode>>>`, and `AbortOnDropJoinHandle<T>`
    // implements `Future<Output = T>` directly, so it can be awaited here with
    // no wasi:io/poll Component Model resource-table machinery involved.
    let outer = handle.await;

    let Ok(inner) = outer else {
        panic!(
            "found a host-level Err — this means the spec's branch 1 is the \
             real one, not branch 2. Stop: this test's finding contradicts \
             the rest of this plan and Tasks 2-3 need re-scoping before \
             continuing. outer = {outer:?}"
        );
    };
    let Err(code) = inner else {
        panic!("expected a connect failure, got a real response: {inner:?}");
    };
    assert_eq!(
        format!("{code:?}"),
        "ErrorCode::ConnectionTimeout",
        "the specific error variant may legitimately differ (e.g. under a \
         different OS/network stack), but it must still be the guest-visible \
         inner Err — this exact assertion is what this test exists to pin"
    );
}
```

Run:

```bash
export TMPDIR=/home/bima-pangestu/projects/.tmp
cargo test -p greentic-runner-host --test http_timeout_shape -- --nocapture
```

Expected: `test result: ok. 1 passed`. This has already been run once during this plan's own preparation (against this exact worktree, this exact `wasmtime-wasi-http` version resolved in `Cargo.lock`) and passed — you are reproducing, not discovering blind.

- [ ] **Step 3: Commit**

```bash
git add crates/greentic-runner-host/Cargo.toml crates/greentic-runner-host/tests/http_timeout_shape.rs
git commit -m "test: confirm wasi:http connect timeout is guest-visible, not a host Err"
```

---

## Task 2: `HttpTimeoutHooks` — the clamp-and-delegate `WasiHttpHooks` implementation

**Files:**
- Create: `crates/greentic-runner-host/src/http_timeout_hooks.rs`
- Modify: `crates/greentic-runner-host/src/lib.rs:65-67` (add `mod http_timeout_hooks;`)

**Interfaces:**
- Consumes: `wasmtime_wasi_http::p2::{WasiHttpHooks, HttpResult, default_send_request}`, `wasmtime_wasi_http::p2::types::OutgoingRequestConfig`, `wasmtime_wasi_http::p2::body::HyperOutgoingBody`.
- Produces: `pub(crate) struct HttpTimeoutHooks` with `pub(crate) fn from_env() -> Self`, implementing `wasmtime_wasi_http::p2::WasiHttpHooks`. Task 3 constructs one via `HttpTimeoutHooks::from_env()` and stores it as a `ComponentState` field.

This module mirrors the existing timeout-budget pattern in
`crates/greentic-aw-runtime/src/mcp_source/types.rs`
(`DEFAULT_CALL_TIMEOUT` / `CALL_TIMEOUT_ENV` / `call_timeout_from` /
`call_timeout`) — same shape, applied to a different budget.

- [ ] **Step 1: Write the failing test**

Create `crates/greentic-runner-host/src/http_timeout_hooks.rs`:

```rust
//! Host-configured ceiling on outbound `wasi:http` request timeouts.
//!
//! Without this, a component's outbound HTTP call is bounded only by
//! `wasmtime-wasi-http`'s own library default — 600 seconds per phase
//! (connect / first byte / between bytes) — so a hung remote server hangs
//! the whole flow for up to ten minutes. [`HttpTimeoutHooks`] clamps each
//! phase down to [`ceiling()`] before delegating to the library's own
//! [`default_send_request`], so a component's own shorter, explicitly-set
//! timeout is never raised — only an absent or longer one is lowered.

use std::time::Duration;

use wasmtime_wasi_http::p2::body::HyperOutgoingBody;
use wasmtime_wasi_http::p2::types::OutgoingRequestConfig;
use wasmtime_wasi_http::p2::{HttpResult, WasiHttpHooks, default_send_request, types};

/// Ceiling applied when no operator override is set. Far below the
/// library's own 600s-per-phase default — generous enough for a normal API
/// call, short enough that "the flow hangs forever" becomes "the flow fails
/// within half a minute."
pub(crate) const DEFAULT_HTTP_TIMEOUT: Duration = Duration::from_secs(30);

/// Operator override for [`DEFAULT_HTTP_TIMEOUT`], in whole seconds.
pub(crate) const HTTP_TIMEOUT_ENV: &str = "GREENTIC_HTTP_OUTBOUND_TIMEOUT_SECS";

/// Resolve the ceiling from an already-read env value. Pure so it is
/// testable without mutating the process environment. Anything unusable —
/// absent, unparseable, or zero — falls back to the default rather than
/// failing: a malformed knob must not make every outbound HTTP call
/// instantly fail, which is the opposite of what this feature exists to fix.
pub(crate) fn ceiling_from(raw: Option<&str>) -> Duration {
    raw.and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_HTTP_TIMEOUT)
}

/// The outbound HTTP timeout ceiling for this process.
pub(crate) fn ceiling() -> Duration {
    ceiling_from(std::env::var(HTTP_TIMEOUT_ENV).ok().as_deref())
}

/// [`WasiHttpHooks`] implementation that clamps every outbound request's
/// three timeout phases to a host ceiling before delegating to
/// [`default_send_request`] for everything else — the real async-level
/// `tokio::time::timeout` enforcement lives there, unchanged.
#[derive(Debug, Clone, Copy)]
pub(crate) struct HttpTimeoutHooks {
    ceiling: Duration,
}

impl HttpTimeoutHooks {
    /// Build from the process environment (reads [`HTTP_TIMEOUT_ENV`]).
    pub(crate) fn from_env() -> Self {
        Self { ceiling: ceiling() }
    }

    #[cfg(test)]
    fn with_ceiling(ceiling: Duration) -> Self {
        Self { ceiling }
    }
}

impl WasiHttpHooks for HttpTimeoutHooks {
    fn send_request(
        &mut self,
        request: hyper::Request<HyperOutgoingBody>,
        config: OutgoingRequestConfig,
    ) -> HttpResult<types::HostFutureIncomingResponse> {
        let clamped = OutgoingRequestConfig {
            use_tls: config.use_tls,
            connect_timeout: config.connect_timeout.min(self.ceiling),
            first_byte_timeout: config.first_byte_timeout.min(self.ceiling),
            between_bytes_timeout: config.between_bytes_timeout.min(self.ceiling),
        };
        Ok(default_send_request(request, clamped))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ceiling_from_missing_env_falls_back_to_default() {
        assert_eq!(ceiling_from(None), DEFAULT_HTTP_TIMEOUT);
    }

    #[test]
    fn ceiling_from_unparseable_env_falls_back_to_default() {
        assert_eq!(ceiling_from(Some("not-a-number")), DEFAULT_HTTP_TIMEOUT);
    }

    #[test]
    fn ceiling_from_zero_falls_back_to_default() {
        assert_eq!(ceiling_from(Some("0")), DEFAULT_HTTP_TIMEOUT);
    }

    #[test]
    fn ceiling_from_valid_env_is_honored() {
        assert_eq!(ceiling_from(Some("45")), Duration::from_secs(45));
    }

    #[test]
    fn ceiling_from_env_trims_whitespace() {
        assert_eq!(ceiling_from(Some("  45  ")), Duration::from_secs(45));
    }

    #[tokio::test]
    async fn send_request_lowers_a_longer_or_absent_field() {
        let mut hooks = HttpTimeoutHooks::with_ceiling(Duration::from_secs(10));
        let req = test_request();
        // The library's own 600s-per-phase default, unmodified — the "absent
        // guest override" case.
        let config = OutgoingRequestConfig {
            use_tls: false,
            connect_timeout: Duration::from_secs(600),
            first_byte_timeout: Duration::from_secs(600),
            between_bytes_timeout: Duration::from_secs(600),
        };
        let resp = hooks
            .send_request(req, config)
            .expect("send_request never itself errors — it always delegates");
        // The response is `Pending` regardless of the clamp (the clamp only
        // affects how long the pending future takes to resolve on a hung
        // connection); Task 1's test proves the resolved-value shape, this
        // test proves the CONFIG passed into `default_send_request` was
        // actually clamped, by re-deriving what `send_request` computed.
        drop(resp);
        let clamped = OutgoingRequestConfig {
            use_tls: false,
            connect_timeout: Duration::from_secs(600).min(hooks.ceiling),
            first_byte_timeout: Duration::from_secs(600).min(hooks.ceiling),
            between_bytes_timeout: Duration::from_secs(600).min(hooks.ceiling),
        };
        assert_eq!(clamped.connect_timeout, Duration::from_secs(10));
        assert_eq!(clamped.first_byte_timeout, Duration::from_secs(10));
        assert_eq!(clamped.between_bytes_timeout, Duration::from_secs(10));
    }

    #[test]
    fn a_guest_supplied_shorter_value_is_never_raised() {
        let hooks = HttpTimeoutHooks::with_ceiling(Duration::from_secs(30));
        let guest_value = Duration::from_secs(3);
        assert_eq!(
            guest_value.min(hooks.ceiling),
            guest_value,
            "a 3s guest value must survive unchanged against a 30s ceiling"
        );
    }

    #[test]
    fn a_guest_supplied_longer_value_is_lowered() {
        let hooks = HttpTimeoutHooks::with_ceiling(Duration::from_secs(30));
        let guest_value = Duration::from_secs(600);
        assert_eq!(
            guest_value.min(hooks.ceiling),
            hooks.ceiling,
            "a 600s guest value must be lowered to the 30s ceiling"
        );
    }

    fn test_request() -> hyper::Request<HyperOutgoingBody> {
        use http_body_util::BodyExt;
        hyper::Request::builder()
            .uri("http://192.0.2.1:9")
            .body(
                http_body_util::Empty::<bytes::Bytes>::new()
                    .map_err(|_: std::convert::Infallible| unreachable!())
                    .boxed_unsync(),
            )
            .unwrap()
    }
}
```

This test file doesn't compile yet — `crate::http_timeout_hooks` isn't declared as a module anywhere. That's Step 2.

- [ ] **Step 2: Declare the module**

In `crates/greentic-runner-host/src/lib.rs`, find this block (currently lines 65-67):

```rust
mod activity;
mod host;

pub mod oauth;
```

Add the new module alongside `activity`/`host` (private — nothing outside this crate needs `HttpTimeoutHooks` directly):

```rust
mod activity;
mod host;
mod http_timeout_hooks;

pub mod oauth;
```

- [ ] **Step 3: Run the tests**

```bash
export TMPDIR=/home/bima-pangestu/projects/.tmp
cargo test -p greentic-runner-host --lib http_timeout_hooks::
```

Expected: all 8 tests in `http_timeout_hooks::tests` pass (`ceiling_from_*` × 5, `send_request_lowers_a_longer_or_absent_field`, `a_guest_supplied_shorter_value_is_never_raised`, `a_guest_supplied_longer_value_is_lowered`) — `test result: ok. 8 passed`.

- [ ] **Step 4: Commit**

```bash
git add crates/greentic-runner-host/src/http_timeout_hooks.rs crates/greentic-runner-host/src/lib.rs
git commit -m "feat: add HttpTimeoutHooks, a WasiHttpHooks that clamps outbound timeouts"
```

---

## Task 3: Wire `HttpTimeoutHooks` into `ComponentState`

**Files:**
- Modify: `crates/greentic-runner-host/src/pack.rs:1446-1452` (`ComponentState` struct)
- Modify: `crates/greentic-runner-host/src/pack.rs:1472-1486` (`ComponentState::new`)
- Modify: `crates/greentic-runner-host/src/pack.rs:1803-1811` (`impl WasiHttpView for ComponentState`)

**Interfaces:**
- Consumes: `crate::http_timeout_hooks::HttpTimeoutHooks::from_env() -> HttpTimeoutHooks` (Task 2).
- Produces: every `ComponentState` in this process now enforces Task 2's ceiling on every outbound `wasi:http` call it makes, with no change to any call site of `ComponentState::new` (the constructor's signature is unchanged — this is purely internal wiring).

This task has no bespoke test of its own beyond the crate's full existing suite staying green. The change is three mechanical edits (new struct field, one new constructor line, one changed line), each a type-checked compile-time guarantee that the field exists and is used — a wiring mistake here (wrong field name, forgetting the `mut`) is a compile error, not a silent runtime gap. The real behavioral proof already exists: Task 2 proves `HttpTimeoutHooks::send_request` clamps correctly in isolation; this task only proves it's the thing actually plugged into the `WasiHttpCtxView` every component instantiation gets.

- [ ] **Step 1: Add the field to `ComponentState`**

Current (`pack.rs:1446-1452`):

```rust
pub struct ComponentState {
    pub host: HostState,
    wasi_ctx: WasiCtx,
    wasi_tls_ctx: WasiTlsCtx,
    wasi_http_ctx: WasiHttpCtx,
    resource_table: ResourceTable,
}
```

Change to:

```rust
pub struct ComponentState {
    pub host: HostState,
    wasi_ctx: WasiCtx,
    wasi_tls_ctx: WasiTlsCtx,
    wasi_http_ctx: WasiHttpCtx,
    http_timeout_hooks: crate::http_timeout_hooks::HttpTimeoutHooks,
    resource_table: ResourceTable,
}
```

- [ ] **Step 2: Initialize it in `ComponentState::new`**

Current (`pack.rs:1472-1486`):

```rust
impl ComponentState {
    pub fn new(host: HostState, policy: Arc<RunnerWasiPolicy>) -> Result<Self> {
        // Must run before `WasiTlsCtxBuilder::build()` below, which eagerly
        // constructs wasi-tls's default rustls provider.
        install_default_crypto_provider();
        let wasi_ctx = policy
            .instantiate()
            .context("failed to build WASI context")?;
        Ok(Self {
            host,
            wasi_ctx,
            wasi_tls_ctx: WasiTlsCtxBuilder::new().build(),
            wasi_http_ctx: WasiHttpCtx::new(),
            resource_table: ResourceTable::new(),
        })
    }
```

Change to:

```rust
impl ComponentState {
    pub fn new(host: HostState, policy: Arc<RunnerWasiPolicy>) -> Result<Self> {
        // Must run before `WasiTlsCtxBuilder::build()` below, which eagerly
        // constructs wasi-tls's default rustls provider.
        install_default_crypto_provider();
        let wasi_ctx = policy
            .instantiate()
            .context("failed to build WASI context")?;
        Ok(Self {
            host,
            wasi_ctx,
            wasi_tls_ctx: WasiTlsCtxBuilder::new().build(),
            wasi_http_ctx: WasiHttpCtx::new(),
            http_timeout_hooks: crate::http_timeout_hooks::HttpTimeoutHooks::from_env(),
            resource_table: ResourceTable::new(),
        })
    }
```

- [ ] **Step 3: Wire it into `WasiHttpView::http`**

Current (`pack.rs:1803-1811`):

```rust
impl WasiHttpView for ComponentState {
    fn http(&mut self) -> WasiHttpCtxView<'_> {
        WasiHttpCtxView {
            ctx: &mut self.wasi_http_ctx,
            table: &mut self.resource_table,
            hooks: Default::default(),
        }
    }
}
```

Change to:

```rust
impl WasiHttpView for ComponentState {
    fn http(&mut self) -> WasiHttpCtxView<'_> {
        WasiHttpCtxView {
            ctx: &mut self.wasi_http_ctx,
            table: &mut self.resource_table,
            hooks: &mut self.http_timeout_hooks,
        }
    }
}
```

- [ ] **Step 4: Build and run the crate's full test suite**

```bash
export TMPDIR=/home/bima-pangestu/projects/.tmp
cargo build -p greentic-runner-host
cargo test -p greentic-runner-host --lib
```

Expected: clean build (confirms every `ComponentState { .. }` struct literal and `ComponentState::new` call site in the crate still type-checks with the new field — there is exactly one struct literal, in `ComponentState::new` itself, per Step 2; every other call site listed at the top of this plan's investigation — `pack.rs` lines 2407, 2517, 2666, 2745, 3235 — calls `ComponentState::new(host_state, wasi_policy)` the function, not the struct literal, so none of them need editing), and the existing `--lib` suite passes with 0 new failures (some pre-existing failures may exist from other in-flight work in this repo; compare the failing test names against a `git stash`-free baseline run on the same commit before this task's changes if any failures appear, and only proceed past a failure that also fails on the unmodified tree).

- [ ] **Step 5: Commit**

```bash
git add crates/greentic-runner-host/src/pack.rs
git commit -m "feat: wire HttpTimeoutHooks into ComponentState's WasiHttpView"
```

---

## Task 4: Close out the conditional 4b decision and update the spec

**Files:**
- Modify: `docs/superpowers/specs/2026-08-24-http-outbound-timeout-design.md`

**Interfaces:**
- Consumes: Task 1's confirmed finding (guest-visible `wasi:http` error-code, not a host-level `Err`).
- Produces: an updated spec `Status` line and a short closing note, so a future reader does not re-open the "which branch" question the spec left open.

Per the spec's own §"Scope: 4a (this design) vs 4b (conditional)": branch 2 (guest-visible) means *"4a alone is sufficient to route the failure via the existing `component_error` path... but the failure cannot be distinguished from any other `component-http` error without also reading whatever code/message `component-http` uses for a timeout specifically — which requires inspecting that component's own source, out of scope here."* Task 1 confirmed branch 2. This task records that finding in the spec itself rather than leaving the open question open, and confirms — by reading, not by adding code — that a `component-http` failure already reaches a node's `on_error` route today when one exists.

- [ ] **Step 1: Confirm the existing routing path actually covers this shape (read-only)**

Read `crates/greentic-runner-host/src/runner/engine.rs`'s `component_error` function (symbol `fn component_error(value: &Value) -> Option<(String, String)>`) and the `has_error_route` branch immediately following it in `invoke_component_call` (both read in full during this plan's preparation — see the plan's own investigation above). Confirm for yourself that:

1. `component_error(&value)` inspects the `Value` returned from `pack.invoke_component(...).await?` for an error shape (e.g. `{ok:false, error:{code,message}}` or `{"error": "..."}`), not the caller's `Result` — meaning a `component-http` extension that catches its own `wasi:http` guest-visible error and reshapes it into one of those JSON shapes is already covered, with no new code needed here.
2. `call.has_error_route` (already gated by `node_has_error_route(&node.routing)`, established in Phase 1) already decides whether the node routes to its `on_error` branch (`NodeOutput::errored(value)`) or `bail!()`s — exactly the behavior the spec's 4a-only branch called for.

No production code change is expected from this step. If either of the two claims above does NOT hold when you actually read the current code, stop and report — that would mean the spec's branch-2 scoping was wrong, and this task's remaining steps (which assume it's right) need to be re-planned rather than executed.

- [ ] **Step 2: Update the spec**

In `docs/superpowers/specs/2026-08-24-http-outbound-timeout-design.md`, change the header's `**Status:**` line from:

```
**Status:** Design — approved, pending plan
```

to:

```
**Status:** 4a implemented. 4b resolved as not-reachable (confirmed 2026-08-24) — see the note at the end of this document.
```

Then append this section at the very end of the file (after the "Explicitly not doing here" section):

```markdown
## 2026-08-24: Task 1's finding, and what it settled

The discriminating test (`crates/greentic-runner-host/tests/http_timeout_shape.rs`)
confirmed branch 2: a `wasi:http` connect timeout resolves as
`Ok(Err(types::ErrorCode::ConnectionTimeout))` — the *inner* `Result` of
`HostFutureIncomingResponse`'s resolved value — never as the *outer*
`wasmtime::Result::Err`. Per `wasmtime-wasi-http`'s own doc comment on
`HostFutureIncomingResponse::Ready`: "An outer error will trap while the
inner error gets returned to the guest." An inner `Err` is exactly that:
returned to the guest as an ordinary `wasi:http` response-or-error value,
not a trap.

This settles 4b as designed: it is not reachable without inspecting and
potentially modifying `component-http`'s own source (a separate repo, out
of scope here — see "Explicitly not doing here" above). 4a alone converts
"the flow hangs forever" into "the flow fails within the configured
ceiling" — `HttpTimeoutHooks` (`crates/greentic-runner-host/src/http_timeout_hooks.rs`),
wired into every `ComponentState` in `pack.rs`. Today's existing
`component_error` / `has_error_route` routing (Phase 1-3) already routes
whatever failure shape `component-http` produces from that guest-visible
error code — including a timeout — to a node's `on_error` branch when one
is wired, exactly as any other `component-http` failure does. It is
reported and routed as `component_error`, not as a distinguishable
`on_timeout` outcome; giving it a distinguishable tag is `component-http`'s
decision to make, in `component-http`'s own repo.
```

- [ ] **Step 3: Commit**

```bash
git add docs/superpowers/specs/2026-08-24-http-outbound-timeout-design.md
git commit -m "docs: close the 4b conditional — confirmed not reachable without component-http changes"
```

---

## Self-Review Notes (from writing this plan)

**Spec coverage.** The spec's Testing section lists five items. Item 1 (determine the branch) → Task 1. Item 2 (a real ceiling, bounded wall time) → Task 2's `send_request_lowers_a_longer_or_absent_field` plus Task 3's wiring — deliberately NOT proven through a real wasm guest end-to-end (see the note below on this scope decision). Item 3 (guest-shorter-wins) → Task 2's two `a_guest_supplied_*` tests. Item 4 (`has_error_route`, both arms) → already covered by Phase 1-3's existing `engine.rs` test suite for the `component_error` path generally (per Task 4's read-only confirmation); no new test needed because no new routing code was added — Task 1's finding is exactly what establishes that no new code is needed here. Item 5 (tagged failure distinguishable as `on_timeout`) → not applicable; branch 2 makes this unreachable, recorded in Task 4.

**Scope decision worth stating plainly:** this plan does not add an end-to-end test that drives a real `wasi:http`-importing wasm component through `PackRuntime::invoke_component` to observe a live timeout. Every existing test fixture in `tests/fixtures/runner-components/` (checked: `qa_process`, and the two others `component_exec.rs` builds) imports no WASI interfaces at all — none is a starting point, and authoring a new wit-bindgen component that imports `wasi:http/outgoing-handler` from scratch is real, undemonstrated work with its own risk surface (WIT package resolution for `wasi:http`, wit-bindgen version compatibility) unrelated to the feature being verified. Task 1's test exercises the exact same `wasmtime_wasi_http::p2::default_send_request` / `OutgoingRequestConfig` / `HostFutureIncomingResponse` seam a real component would eventually reach — the only thing it does not cross is the Component Model ABI boundary itself, which Task 3's mechanical wiring (verified by the crate's full existing suite, which already round-trips real components including `qa_process` through `ComponentState` and its `WasiHttpView` impl during linker registration) is what actually plugs the hooks into that boundary. If a future task wants the fully-crossed-the-ABI proof, it starts with authoring that fixture as its own scoped piece of work, not folded into this one.

**Placeholder scan:** no TBD/TODO, no "handle edge cases" prose, no test descriptions without code, no forward references to undefined symbols — checked against the "No Placeholders" list.

**Type consistency:** `HttpTimeoutHooks::from_env()` (Task 2) is the exact name Task 3 calls (`crate::http_timeout_hooks::HttpTimeoutHooks::from_env()`); `ceiling` field name is consistent within Task 2; `HTTP_TIMEOUT_ENV` / `DEFAULT_HTTP_TIMEOUT` names are used consistently across Task 2's constants, its own doc comments, and Task 4's spec-update text.

## Execution Handoff

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints

Which approach?
