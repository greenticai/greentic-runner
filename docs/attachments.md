# Attachments in the runner

How files sent with a message reach a flow and an agentic worker, how an
extension creates a file, and which paths do NOT serve attachments in v1.

The envelope shape (contract C1) is owned by the cross-repo attachments plan:
`attachments[i]` is `{ mime_type, url, name, size_bytes }`, where `url` is an
`artifact://<id>` (or `null` when the host could not store the file), and
`extensions.artifacts[i]` (parallel by index) is `{ sha256, kind, text_ref }`.
`extensions.attachment_notes[i]` is `{ code, message }` for a file the host
could not store.

## Flow templates

No engine change is involved: the envelope root is flattened into `entry`, and
the template engine resolves `a.b[0].c` paths. So a flow reads the C1 fields:

| expression | value |
|---|---|
| `{{entry.attachments}}` | the whole array (an exact expression keeps the JSON type) |
| `{{entry.attachments[0].url}}` | the first file's `artifact://` id, or `null` |
| `{{entry.attachments[0].name}}` / `.mime_type` / `.size_bytes` | the first file's metadata |
| `{{in.extensions.artifacts}}` | the parallel metadata array (`sha256`, `kind`, `text_ref`) |
| `{{in.extensions.attachment_notes}}` | the notes for files that could not be stored |

The document TEXT is not in the envelope: only a `text_ref` (another artifact
id) is, and only the agent's LLM backend reads it. A path that does not
resolve (no attachments, a missing index, a missing `extensions` key) renders
as the empty string, like any missing path, and the `dw.agent` node reads an
empty string as "no attachments". Pinned by the `templating::tests::*attachment*`
tests in `greentic-runner-host` (their fixtures also carry a non-contract `text`
field, only to show that any field resolves).

## Who reads attachments: the `dw.agent` node only

A `dw.agent` node reads three keys from its flow-node input, which
greentic-dw-authoring maps from the envelope (contract C4b):
`attachments` (`{{entry.attachments}}`), `attachment_meta`
(`{{in.extensions.artifacts}}`) and `attachment_notes`
(`{{in.extensions.attachment_notes}}`). Absent, `null`, `""` and non-arrays all
mean "no attachments"; nothing there can fail the turn. Sender-chosen names are
cleaned when parsed (controls, bidi, zero-width and line separators removed;
at most 120 characters). A file the host could not store becomes a notice in
the user text, taken from a fixed table of codes; a reason never carries the
sender's name or the host's free text.

The bytes are fetched only by the multi-provider backend (`GreenticLlmBackend`,
feature `greentic-llm-backend`), once per turn, through an `ArtifactReader`
(contract C3 `get`), at most 4 reads in flight per process (a read waits for a
permit; an acquire timeout is future work).

If the model refuses the images it was sent (vision is advertised per
PROVIDER, but some of its models take no images), the turn is retried ONCE
without them and the agent gets the fixed note "The user attached N image(s)
that you cannot see…". If the retry also fails, the original error is returned.
A text-only failure is never retried here.

### Where the reader comes from (decided at the host, never below it)

- **Deployed unit** (`TenantRuntime::load_revision_with`, used by
  greentic-start): ONLY `RevisionHostOptions::with_artifact_reader`, built from
  that unit's own door and token. No option means no reader; the
  `GREENTIC_ARTIFACT_*` variables are never read on this path.
- **Single-tenant `HostBuilder` host** (the designer's Run Demo, the
  standalone runner): `HostBuilder::with_artifact_reader`. The env fallback,
  `GREENTIC_ARTIFACT_ENDPOINT` (the door base, ending in `/artifacts`) +
  `GREENTIC_ARTIFACT_TOKEN` (both required), is OPT-IN:
  `HostBuilder::with_artifact_env_fallback(true)`, default off. It is for
  single-tenant processes only: the standalone runner (`greentic_runner_host::run`,
  i.e. the `greentic-runner` binary, which the Test chat sidecar runs) opts in;
  the designer must never enable it.
- **Multi-tenant `HostBuilder` host**: none. An injected reader is dropped with
  the warning `artifact_reader_multi_tenant_host`, and the env is not used in
  its place, even when opted in.

Without a reader every turn still runs, and each attachment becomes the fixed
notice "The user attached N file(s), but attachments are not available in this
deployment…".

Both door clients (this reader and the extension artifact port below) share
one endpoint rule (`greentic_aw_runtime::door_url`): `https`, or `http` to a
loopback host only, and never userinfo in the URL. Any other endpoint gets no
request at all, so the bearer token is never sent in clear text.

## Creating files (extensions)

An extension that creates a file (`greentic.media`) calls `host.artifact.put`.
The extension runtime that answers it is the one `build_ext_runtime` builds,
and it carries an `ArtifactPort` only when the host decided one, with the same
rules as the reader:

- deployed unit: `RevisionHostOptions::with_ext_artifact_port` (greentic-start
  builds it per unit, over that unit's door and token);
- single-tenant `HostBuilder` host: `HostBuilder::with_ext_artifact_port`, else
  the same env fallback, only when opted in with
  `with_artifact_env_fallback(true)` (and `build()` must run inside a
  multi-thread tokio runtime);
- multi-tenant host: none (warning `artifact_port_multi_tenant_host`).

Without a port an extension's `put` answers `unsupported`. The door-backed port
is `runner::ext_artifact_port::HttpArtifactPort`: no redirects, a 5 s connect /
30 s total timeout, a reply capped at 16 KiB and accepted only with an
`artifact://<64 hex>` id, a refusal when the CALLING thread runs on a
current-thread runtime, and a token that never appears in a log, an error or
anything the extension sees. A door refusal reaches the extension as the same
`unavailable` whatever the cause; the host log carries a fixed debug-level
reason (`unauthorized`, `purpose_not_granted`, `too_large`, `rate_limited`,
`refused`).

## Inbound guardrails see document text (not images)

The inbound guardrail chain that checks the user's message text in the agent
loop (`crates/greentic-aw-runtime/src/loop.rs`) also checks every attachment
document's text. The loop hands the backend a guard built from the SAME chain,
context and evaluator (`LlmRequest.attachment_text_guard`,
`crates/greentic-aw-runtime/src/attachment_guard.rs`), and
`attachments_materialize.rs` `push_document` calls it before the text is put
in the prompt:

- The guard checks the text AFTER sanitising and capping, i.e. exactly what the
  model would read, so no sanitiser trick can separate what is checked from
  what is shown.
- Accept: the text is shown unchanged. A redaction (`Update`) is what the model
  reads, re-sanitised and re-capped, so a guardrail's output cannot write a
  block marker either. A Monitor-mode deny is recorded and the text is shown.
- An Enforce-mode deny WITHHOLDS the document: the model gets the fixed
  sentence "... was withheld by a content policy." (no name, no reason, no
  text) and the turn goes on. A withheld document spends none of the
  per-message text budget.
- An evaluator ERROR withholds the document whatever the guardrail is. This
  differs from the message text, where an agent-level guardrail fails open: a
  file nobody could check is not shown.
- Denials (blocked or monitored) reach the step observer like the message
  text's, as `inbound`.
- It runs once per document per turn (inside the per-turn memo), not once per
  tool iteration or retry. A turn that resumes a parked flow tool pushes no new
  user message, so the backend materialises the previous message's documents
  again: the guard runs on that turn too (only the message-text chain is
  skipped on a resume). No guardrail configured means no guard, and the
  prompt is byte-identical to before.
- Only the multi-provider backend (`GreenticLlmBackend`) reads document text,
  so it is the only backend that calls the guard.

**Image content is not guardrail-inspected in v1**: an image is not text, and
it reaches a vision model as sent.

## Paths NOT served in v1

These paths do not read attachments. Where they receive some, the agent is told
with a fixed notice (a count only, never a name or an id); the "silent" ones are
marked.

- **Other LLM backends.** `OpenAiLlmBackend` (`llm_openai.rs`) and the bridge
  `ExtensionLlmBackend` (`llm_extension.rs`) cannot open attachments. They
  append the SAME fixed "not available in this deployment" notice to the user
  message and send no `artifact://` id or file name to the provider.
- **No reader or port on these entry points.** `greentic-runner-desktop`, the
  public `TenantRuntime::load` / `TenantRuntime::from_packs`, and the public
  `build_agent_node_handler` / `build_agent_node_wiring` /
  `build_agent_node_wiring_ephemeral` (and `build_agent_node_handler_ephemeral`)
  build no reader and no extension artifact port, and never read the env.
  Attachments become the fixed notice; an extension's `put` answers
  `unsupported`.
- **Process-level extension runtimes** (`runner/mod.rs`, `graph_node.rs`): no
  artifact port, so `put` answers `unsupported`.
- **Answer to a parked flow tool** (a `resume_payload`): the answer goes to the
  flow, which takes no files; the agent gets the fixed "not available" notice
  after the resumed tool's result.
- **Agent-graph turns** (`dw.agent_graph`, `graph_node.rs`): the graph runs on
  text; attachments on the node are announced to its agents with the fixed
  "not available" notice appended to the user text.
- **NATS serve path** (`greentic-aw-runtime/src/serve.rs`): `extract_user_text`
  builds the turn from `user_text` / `text` only. Silent: this path never sees
  the attachment keys.
- **Agent chat HTTP** (`greentic-runner-host/src/http/agent_chat.rs`,
  `turn_payload`): the turn carries the text or the card submit only. Silent.
- **Deep worker (`operala.call`) turns**: text only. Silent.
- **Provider-edge drops.** A file a provider could not hand over (too large,
  an unsupported type, a failed fetch, an exhausted quota, the door
  unavailable) is not silently dropped: the HOST leaves `url = null` and writes
  an `attachment_notes` entry (contract C1), which the `dw.agent` node turns
  into a notice for the agent. The runner never fetches from a provider itself.
- **Email and Teams Graph** channels emit no attachments in v1.
- **Test chat's runner sidecar** is a separately installed binary
  (`RUNNER_REINSTALL_REV`); it gets none of this until that pin moves.
