# A real HTTP timeout for outbound component calls

**Date:** 2026-08-24
**Status:** 4a implemented. 4b resolved as not-reachable (confirmed 2026-08-24) — see the note at the end of this document.
**Repo:** `greentic-runner` (runner-only)
**Area:** `crates/greentic-runner-host/src/pack.rs` (WASI HTTP wiring), `crates/greentic-runner-host/src/runner/engine.rs` (failure detection)

> **Line references.** Verified against the pinned research checkout at
> `~/.cargo/git/checkouts/greentic-runner-6497141b371c7d36/09fa6a0` (rev `09fa6a01`,
> which already carries Phase 2 — R1/R2 — and Phase 3 — R3). Re-verify against
> whatever branch this plan actually starts from; this repo's own convention is
> that line numbers drift and the symbol is the anchor of record, not the number.

## This is Phase 4a of a larger effort

`docs/superpowers/specs/2026-08-23-flow-error-routing-design.md` in
`greentic-designer` is the parent spec for canvas error routing. Its §6 sized
"R4 — a real timeout signal" as large and structurally blocked: no deadline
wraps component execution, and the two mechanisms that looked available —
`tokio::time::timeout` around the call, and wasmtime epoch interruption —
both turned out not to reach this case. This document supersedes that
sizing for the HTTP case specifically, with a mechanism the parent spec did
not know about.

## Problem

An HTTP node's outbound call has no enforced deadline. If the remote server
never responds, the component hangs, and so does the flow — indefinitely.
There is no code path that produces a timeout error for a flow author to
route on, and no way for an operator to bound how long they are willing to
wait.

Two prior candidates, both investigated and ruled out for this specific case:

**`tokio::time::timeout` around the call.** `PackRuntime::invoke_component`
delegates to `run_on_wasi_thread`, which spawns a plain `std::thread` and
blocks the calling async task on a synchronous `handle.join()`. A `timeout`
wrapped around that future does nothing — the future never yields until the
thread finishes, so the deadline is never observed.

**wasmtime epoch interruption.** Genuinely feasible to wire up — the
`Engine` in `PackRuntime` is already shared (`Arc` internally) across calls,
so a single process-wide ticker thread calling `engine.increment_epoch()`
would suffice; each call would only need `store.set_epoch_deadline(n)`
before running. But wasmtime's own documentation
(`wasmtime-45.0.3/src/config.rs:683-706`, "Interaction with blocking host
calls") is explicit: *"Epochs (and fuel) do not assist in handling
WebAssembly code blocked in a call to the host … it's left to the embedder
to determine how best to wake up indefinitely blocking code in the host."*
Epoch checks fire only at wasm bytecode checkpoints (function entry, loop
backedges). An HTTP component blocked waiting on a network response is not
executing wasm bytecode at all — it is suspended inside a host import — so
the epoch check is never reached. Confirmed structurally: `PackRuntime`
builds its `Engine` via `Engine::default()` (`pack.rs:1602,1913,3360,4557`),
without `async_support(true)`; host imports run as ordinary synchronous Rust
calls on the same thread as the guest, not as wasm bytecode the epoch
mechanism can interrupt.

## The mechanism that actually reaches this case

`greentic-runner-host` already wires `wasmtime-wasi-http`'s
`wasi:http/outgoing-handler` for component network access
(`add_only_http_to_linker_sync`, `pack.rs:82-84`), and constructs a fresh
`WasiHttpCtx::new()` per component instantiation at two call sites
(`pack.rs:1450`, `pack.rs:1483`), both currently using the library's
`default_hooks()`.

`wasmtime-wasi-http` computes an `OutgoingRequestConfig` per request
(`connect_timeout`, `first_byte_timeout`, `between_bytes_timeout`, all
`Duration`) — defaulting each to 600 seconds
(`wasmtime-wasi-http-45.0.3/src/http_impl.rs:26-35`) unless the guest
supplies shorter values via its own `wasi:http` `RequestOptions` — and
passes that config to `WasiHttpHooks::send_request(&mut self, request,
config)` (`p2/mod.rs:285-306`) **before** executing the request via
`default_send_request`. That execution genuinely wraps the connection and
response wait in `tokio::time::timeout`
(`wasmtime-wasi-http-45.0.3/src/p2/mod.rs:603,701`) — real async-level
enforcement, not a wasm bytecode checkpoint. `send_request` is a clean
override point: a hook can clamp the config's three fields down to a
host-chosen ceiling and delegate to `default_send_request` for everything
else, without reimplementing the `outgoing_handler::Host` trait.

600 seconds is today's *effective* ceiling for a hung outbound call — no
component in this repo currently sets it lower, and nothing in
`greentic-runner-host` overrides `default_hooks()`.

## Scope: 4a (this design) vs 4b (conditional)

**4a — enforce a real ceiling.** A custom `WasiHttpHooks` implementation
clamps `connect_timeout` / `first_byte_timeout` / `between_bytes_timeout` to
a host-configured default (never *raising* a guest-supplied shorter value —
only lowering an absent or longer one) and wires it in at both
`WasiHttpCtx::new()` call sites in place of `default_hooks()`. This alone
converts "the flow hangs forever" into "the flow fails within N seconds" —
a real correctness improvement whether or not the resulting failure is
tagged specifically as a timeout. Mandatory; the rest of this spec depends
on it existing.

**4b — tag the failure as `on_timeout`, conditionally.** Whether this is
possible without also touching the `component-http` extension (a separate
repo, not inspected as part of this design) depends on a fact not yet
established: what shape does a `wasi:http` timeout take by the time it
reaches `pack.invoke_component`'s `Result<Value, Error>`? Two branches:

- **If it surfaces as a distinguishable host-level `Err`** (the hooked
  `send_request`'s own `tokio::time::timeout` firing, propagating up through
  the linker call boundary as an error before the guest ever sees a
  `wasi:http` response) — then `invoke_component_call` can match on it at
  the same seam Phase 1–3 already established (alongside
  `component_error`/`mcp_tool_error`), and construct
  `NodeOutput::errored(json!({"outcome": "on_timeout", "error": {...}}))`.
  `build_routing_context` already prefers `meta["outcome"]` over the
  ok-derived default (established by Phase 2/R2) — no change to routing
  logic is needed, only tagging the outcome correctly at the point of
  detection.

- **If it surfaces as a guest-visible `wasi:http` error-code** that
  `component-http`'s own wasm reshapes into whatever failure shape it
  already returns (most likely the same `{ok:false, error:{code,message}}`
  or `{"error": "..."}` shapes Phase 1–3 already detect) — then 4a alone is
  sufficient to route the failure via the *existing* `component_error` path
  (an `on_error` branch fires, correctly, today), but the failure cannot be
  distinguished from any other `component-http` error without also reading
  whatever code/message `component-http` uses for a timeout specifically —
  which requires inspecting that component's own source, out of scope here.

**Determining which branch is true is the first implementation task**, not
an assumption baked into this design. See Testing.

## Routing-shape decision: the no-error-route fallback

Mirrors R1/R3's `has_error_route` gate, with one deliberate difference from
R3's MCP case. R3 chose "stay `ok:true`, unchanged" for a node with no error
route, because MCP failure used to *look like success* — introducing an
unconditional `ok:false` there would have turned a silent wrong answer into
a silently parked run, which is worse for an operator who wired nothing.

Timeout has no such history. Before this change, a hung HTTP call did not
look like success — it looked like nothing, forever. There is no
"unchanged" behaviour to preserve. So: **with an error route, route to it
(4b) or to `on_error` generically (4a-only); without one, `bail!`** —
identical to `component_error`'s existing gate (`engine.rs`, the
`has_error_route` check immediately preceding its own `bail!`). This turns
an indefinite hang into a clear flow failure even for an operator who never
wired error handling — strictly better than today, and introduces no new
parking behaviour, because parking was never the prior state for this case.

## Data flow

```
HTTP node (canvas) → component.exec (ygtc) → component-http wasm
  → wasi:http outgoing-handler
    → host's HttpTimeoutHooks::send_request
      → clamp OutgoingRequestConfig's three Duration fields
      → default_send_request (real tokio::time::timeout inside)
        → (fires) → [open question: host Err, or guest wasi:http error-code?]
          → pack.invoke_component's Result<Value, Error>
            → invoke_component_call (existing seam: component_error /
              mcp_tool_error / — new — the timeout detector)
```

## Testing

1. **First task, not a TDD unit in the usual sense: determine which branch
   the open question resolves to.** Build a minimal component (or reuse an
   existing test fixture component with an HTTP-shaped host import) that
   blocks past a short configured timeout, run it through
   `PackRuntime::invoke_component` with `HttpTimeoutHooks` set to a short
   duration (e.g. 100ms), and inspect the actual `Result` / error type that
   reaches the call site. This determines whether 4b is reachable as
   designed or needs a different detection point.
2. **4a discriminating test:** an outbound call to an address that never
   responds (a TCP listener that accepts and never writes, or a
   `tokio::net::TcpListener` bound and never `.accept()`ed) times out within
   the configured ceiling, not the library's 600s default. Assert via clock
   (bounded wall time), not just an eventual `Err`.
3. **Guest-shorter-wins test:** a guest-configured `RequestOptions` shorter
   than the host default must NOT be raised by the hook — assert the
   effective timeout is `min(guest, host-default)`, not always the host
   value.
4. **`has_error_route` gate, both arms**, mirroring R1/R3's pattern: a node
   with an `on_error`/`on_timeout` route reaches it; a node with none
   `bail!`s rather than hanging or silently succeeding. Revert-and-fail each
   before accepting, per this repo's own convention (Phase 2/3 both did
   this and it caught real defects both times).
5. **If 4b is reachable:** a test that the tagged failure's `meta.outcome`
   is `"on_timeout"` specifically, and that it is distinguishable from a
   `component_error`-shaped non-timeout failure on the same node type.

## Explicitly not doing here

- **Epoch interruption.** Real, and useful for a different problem (a pure
  compute loop in guest bytecode that never calls a host import), but
  documented by wasmtime itself as not reaching this one. Not part of this
  design; a future spec for CPU-bound guest hangs would revisit it
  independently.
- **`greentic_types::Node` timeout field.** The deadline is a single
  host-configured default (const or env-var override, mirroring
  `GREENTIC_AW_MCP_CALL_TIMEOUT_SECS`'s existing pattern), not a per-node
  canvas setting. Per-node timeout configuration is a separate, later
  decision, and would need a crates.io-publish-gated field on a
  cross-repo-pinned type — out of scope for closing the "flow hangs
  forever" defect.
- **Changing `component-http`'s own error shape.** That component is a
  separate repo. If 4b's investigation finds the timeout is only
  guest-visible and `component-http` does not currently distinguish it from
  other failures, closing that gap belongs to that repo, not this one.

## 2026-08-24: Task 1's finding, and what it settled

The discriminating test (`crates/greentic-runner-host/tests/http_timeout_shape.rs`)
confirmed branch 2: a `wasi:http` connection failure resolves as
`Ok(Err(types::ErrorCode::_))` — the *inner* `Result` of
`HostFutureIncomingResponse`'s resolved value — never as the *outer*
`wasmtime::Result::Err`. Per `wasmtime-wasi-http`'s own doc comment on
`HostFutureIncomingResponse::Ready`: "An outer error will trap while the
inner error gets returned to the guest." An inner `Err` is exactly that:
returned to the guest as an ordinary `wasi:http` response-or-error value,
not a trap. (The SPECIFIC variant is environment-dependent — both
`ErrorCode::ConnectionTimeout` and `ErrorCode::ConnectionRefused` were
observed for the identical RFC 5737 test address across different runs of
Task 1's test; what is guaranteed, and what this finding rests on, is the
SHAPE — inner, guest-visible `Err` — not which variant fires.)

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
