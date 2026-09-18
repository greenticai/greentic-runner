# External-knowledge chain, end to end

Date: 2026-08-26. Branch `BimaPangestu28/knowledge-e2e`, off `origin/research`
at `5c48e217`.

**The chain runs.** Adapter (`runner/knowledge_ext.rs`) → real
`greentic.rag-http` WASM design extension → real HTTP retrieval service, one
retrieval, real ranked passages back. Nothing in the contract had to move for
the three to meet.

New file: `crates/greentic-runner-host/tests/knowledge_ext_e2e.rs`, four
`#[ignore]`d tests.

---

## 1. How the retrieval service is stood up, and why

**Decision: the service is served from inside the test, in Rust — not by
shelling out to `scripts/rag_contract_check.py --serve-stub`.**

Two reasons, and one of them bit during this session:

1. **The script lives in another repository.** It is
   `greentic-designer/…/scripts/rag_contract_check.py`. A test committed to
   `greentic-runner` cannot depend on a path inside a `greentic-designer`
   worktree; shelling out would have made the test unrunnable for anyone whose
   checkouts are laid out differently, and there is no honest default to fall
   back to.
2. **Serving it here buys an assertion the script cannot give.** The in-test
   service records the request the WASM guest actually sent. That is what lets
   `a_bound_extension_retrieves_real_chunks_over_http` assert on the auth header
   and `the_querys_limit_reaches_the_service_as_top_k` assert on the `top_k`
   field *in the outgoing body* — i.e. test the hop, not the answer. Against the
   Python stub both tests could only have inspected the reply, which the
   extension could in principle have produced without ever leaving the process.

**Third reason, discovered by accident:** the brief's suggested `--port 8099`
is already occupied on this box by a running `greentic-designer-admin` dev
server. A `curl` aimed at it returned that app's `index.html`, and the
conformance checker duly reported `response body is not JSON (text/html…)` and
`a request with no auth header was accepted`. A fixed port is a live hazard
here; the in-test service binds `127.0.0.1:0` and uses whatever it is given.

The in-test service is a behavioural mirror of `StubHandler`: same two
passages, same `top_k` slicing, same 401 when no `Authorization` header arrives.
It differs in one way, deliberately: it also emits `returned` and `truncated`,
which the extension's own declared `output_schema` lists as required and the
Python stub omits.

### Cross-check that the mirror is faithful

```
$ python3 …/scripts/rag_contract_check.py --selftest
selftest: OK

$ curl -s -X POST -H 'Authorization: e2e-secret-token' -H 'Content-Type: application/json' \
    -d '{"query":"what is the refund window","top_k":5,"tenant":"acme","env":"prod"}' \
    http://127.0.0.1:8199/v1/search
{"chunks": [{"text": "first chunk", "score": 0.95}, {"text": "second chunk", "score": 0.85}]}

$ curl -s -X POST -H 'Authorization: e2e-secret-token' -H 'Content-Type: application/json' \
    -d '{"query":"escalation contacts","top_k":1}' http://127.0.0.1:8199/v1/search
{"chunks": [{"text": "first chunk", "score": 0.95}]}

$ curl -s -w ' <- HTTP %{http_code}\n' -X POST -H 'Content-Type: application/json' \
    -d '{"query":"x","top_k":1}' http://127.0.0.1:8199/v1/search
{"error":"unauthorized"} <- HTTP 401

$ python3 …/scripts/rag_contract_check.py http://127.0.0.1:8199/v1/search \
    --token e2e-secret-token --top-k 5
contract: OK
```

Those are the exact passages, scores, ordering and refusal the in-test service
reproduces and the four tests assert on.

---

## 2. Running the tests

```
$ GREENTIC_EXT_ALLOW_UNSIGNED=1 cargo test -p greentic-runner-host \
      --features dev-allow-unsigned --test knowledge_ext_e2e -- --ignored --test-threads=1

running 4 tests
test a_blank_tool_name_is_a_backend_error_naming_the_key ... ok
test a_bound_extension_retrieves_real_chunks_over_http ... ok
test a_stopped_service_is_a_backend_error_never_an_empty_result ... ok
test the_querys_limit_reaches_the_service_as_top_k ... ok

test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.41s
```

**`--features dev-allow-unsigned` and `GREENTIC_EXT_ALLOW_UNSIGNED=1` are not
optional, and that is a fact about the chain rather than about the test.** The
installed `greentic.rag-http-0.1.0` describe carries no `signature` field
(`describe.json` keys: `$schema`, `apiVersion`, `kind`, `compat`, `metadata`,
`capabilities`, `runtime`, `contributions`, `manifestSha256`,
`requiredSecrets`). A default runner build refuses it — see negative control 1
below. Anyone reading "the chain works" should read it as "the chain works for a
signed extension, or for an unsigned one on a dev build that has explicitly
opened the gate".

---

## 3. Which hop each assertion exercises

| Test | Hops it proves |
|---|---|
| `a_bound_extension_retrieves_real_chunks_over_http` | binding `params` read by `claim_of` → `invoke_tool_ctx` instantiates the real component → guest resolves all three `secret://client-rag/…` URIs through the host secrets port → guest's HTTP call passes the runtime's URL allow-list → request arrives at the service with the token in the declared header → response parsed by `parse_chunks` into ranked chunks, service order preserved, scores intact, credential not echoed back |
| `the_querys_limit_reaches_the_service_as_top_k` | `KnowledgeQuery.limit` → adapter's `args` JSON → guest → `top_k` **in the outgoing request body** (asserted on what the service saw, not on the reply), and the reply honouring it |
| `a_stopped_service_is_a_backend_error_never_an_empty_result` | guest's transport failure → `RuntimeError` → `KnowledgeError::Backend`, never `Ok(vec![])`; and the message carries no credential |
| `a_blank_tool_name_is_a_backend_error_naming_the_key` | `Claim::Malformed` fires *before* any invocation — asserted by the service having seen zero requests — and the message names the param the operator must fix |

Real error text, captured from the runs (via a temporary `panic!`, since the
tests assert rather than print; the mutation was reverted):

```
knowledge retrieval tool 'greentic.rag-http/search_knowledge' failed:
extension error (internal): internal: search_knowledge: no response from
127.0.0.1:45919 within the 5s budget (not retried): http transport error:
error sending request for url (http://127.0.0.1:45919/v1/search)
```

```
knowledge binding names `provider.knowledge.extension` but its
`provider.knowledge.extension.tool_name` param is missing or blank, so there is
no retrieval target to invoke
```

Note the extension bounds its own HTTP call at **5 s**, not at the host's 30 s.
`knowledge_ext.rs`'s module comment reasons about a hung service pinning a
blocking-pool thread for up to 30 s because `extension-host/http@0.1.0` carries
no timeout field. That is still true of the *port*, but this extension does not
in fact ride the host ceiling — its own budget is 5 s. The comment is not wrong
about the mechanism; it is describing a worst case this particular extension
does not reach.

---

## 4. Negative controls — because 4 tests passing in 0.41 s is not evidence

A green run this fast could equally mean nothing ran. Two deliberate breakages,
both showing the chain really is load-bearing.

### Control 1 — close the signature gate (drop `GREENTIC_EXT_ALLOW_UNSIGNED`)

```
$ cargo test -p greentic-runner-host --features dev-allow-unsigned \
      --test knowledge_ext_e2e -- --ignored --test-threads=1 a_bound_extension

test a_bound_extension_retrieves_real_chunks_over_http ... FAILED

panicked at crates/greentic-runner-host/tests/knowledge_ext_e2e.rs:314:29:
loading /home/…/.greentic/extensions/design/greentic.rag-http-0.1.0 failed:
signature verification failed for extension 'greentic.rag-http': signature
verification failed: missing signature field
```

The extension really is loaded through the real gate, from the real install
directory.

### Control 2 — name the wrong auth header in the secret

Changed `secret://client-rag/auth_header` from `Authorization` to
`X-Wrong-Header`, so the service sees no auth header and refuses:

```
panicked at …/knowledge_ext_e2e.rs:369:10:
the whole chain must complete: Backend("knowledge retrieval tool
'greentic.rag-http/search_knowledge' failed: extension error
(permission-denied): permission denied: search_knowledge: 127.0.0.1:34657
refused the credential (HTTP 401): {\"error\":\"unauthorized\"}")
```

This is the decisive one. The **WASM guest** is reporting my in-test service's
own 401 body, from my in-test service's own ephemeral port. There is no way to
produce that string without the guest having resolved a secret, built a request,
crossed the URL allow-list, and reached the socket. Reverted afterwards.

---

## 5. Things that did not work first time

- **`--port 8099` is occupied.** See §1. Cost one confusing round of curl output
  that was HTML from `greentic-designer-admin`.
- **`cd /tmp && S=… && python3 … &` backgrounds the whole compound**, so `S`
  never landed in the foreground shell and the follow-up checker invocation was
  handed a URL as a filename. Re-run with the assignment outside the background
  job.
- **`rustfmt` reflowed one `panic!` arm** in the last test; `cargo fmt --all`
  applied it and the check is clean.

Nothing in the chain itself needed adjusting. No test was weakened to make it
pass.

---

## 6. Verification status

```
$ cargo fmt --all -- --check          # FMT EXIT: 0
$ cargo clippy --workspace --all-targets --all-features -- -D warnings   # CLIPPY EXIT: 0
```

The full Rust test suite was **not** run locally — CI's job, per this repo's
own guidance. What was run locally is the four new `#[ignore]`d tests above,
explicitly with `--ignored`.

## 7. Concerns worth carrying forward

1. **The installed extension is unsigned.** Every lane that is not a
   `dev-allow-unsigned` build will refuse it as shipped. Signing is a
   publishing step nobody has taken for this artifact yet.
2. **`runtime.permissions.network` still lists
   `https://REPLACE-WITH-YOUR-RAG-HOST.invalid/*`.** Loopback works, which is
   why this test passes; no real customer origin is reachable until the
   deployment replaces that placeholder and republishes. That is the
   per-customer-template property §7.2 of the design spec records, and it means
   a green run here says nothing about a green run against a real service.
3. **`knowledge_ext.rs`'s 30 s worst-case comment overstates this extension.**
   See §3 — the extension self-bounds at 5 s.
