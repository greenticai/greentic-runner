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
the template engine resolves `a.b[0].c` paths. So a flow reads:

| expression | value |
|---|---|
| `{{entry.attachments}}` | the whole array (an exact expression keeps the JSON type) |
| `{{entry.attachments[0].url}}` | the first file's `artifact://` id |
| `{{entry.attachments[0].text}}` | a field on the first attachment, when the envelope carries it |
| `{{in.extensions.artifacts}}` | the parallel metadata array |
| `{{in.extensions.attachment_notes}}` | the notes for files that could not be stored |

A path that does not resolve (no attachments, a missing index, a missing
`extensions` key) renders as the empty string, like any missing path, and the
`dw.agent` node reads an empty string as "no attachments".
Pinned by the `templating::tests::*attachment*` tests in `greentic-runner-host`.

## Who reads attachments: the `dw.agent` node only

A `dw.agent` node reads three keys from its flow-node input, which
greentic-dw-authoring maps from the envelope (contract C4b):
`attachments` (`{{entry.attachments}}`), `attachment_meta`
(`{{in.extensions.artifacts}}`) and `attachment_notes`
(`{{in.extensions.attachment_notes}}`). Absent, `null`, `""` and non-arrays all
mean "no attachments"; nothing there can fail the turn. A file the host could
not store becomes a notice in the user text, taken from a fixed table of codes.

The bytes are fetched only by the multi-provider backend (`GreenticLlmBackend`,
feature `greentic-llm-backend`), once per turn, through an `ArtifactReader`
(contract C3 `get`), at most 4 reads in flight per process.

### Where the reader comes from (decided at the host, never below it)

- **Deployed unit** (`TenantRuntime::load_revision_with`, used by
  greentic-start): ONLY `RevisionHostOptions::with_artifact_reader`, built from
  that unit's own door and token. No option means no reader; the
  `GREENTIC_ARTIFACT_*` variables are never read on this path.
- **Single-tenant `HostBuilder` host** (local runs, the Test chat sidecar, the
  designer's Run Demo): `HostBuilder::with_artifact_reader`, else the env
  fallback `GREENTIC_ARTIFACT_ENDPOINT` (the door base, ending in `/artifacts`)
  + `GREENTIC_ARTIFACT_TOKEN` (both required).
- **Multi-tenant `HostBuilder` host**: none. An injected reader is dropped with
  the warning `artifact_reader_multi_tenant_host`, and the env is not used in
  its place.

Without a reader every turn still runs, and each attachment becomes the fixed
notice "The user attached N file(s), but attachments are not available in this
deployment…".

## Creating files (extensions)

An extension that creates a file (`greentic.media`) calls `host.artifact.put`.
The extension runtime that answers it is the one `build_ext_runtime` builds,
and it carries an `ArtifactPort` only when the host decided one, with the same
rules as the reader:

- deployed unit: `RevisionHostOptions::with_ext_artifact_port` (greentic-start
  builds it per unit, over that unit's door and token);
- single-tenant `HostBuilder` host: `HostBuilder::with_ext_artifact_port`, else
  the same two env variables (and `build()` must run inside a multi-thread
  tokio runtime);
- multi-tenant host: none (warning `artifact_port_multi_tenant_host`).

Without a port an extension's `put` answers `unsupported`. The door-backed port
is `runner::ext_artifact_port::HttpArtifactPort`: no redirects, a 5 s connect /
30 s total timeout, and a token that never appears in a log, an error or
anything the extension sees.

## Paths NOT served in v1

These paths do not receive attachments. Unless noted, nothing warns beyond a
debug log.

- **Other LLM backends.** `OpenAiLlmBackend` (`llm_openai.rs`) and the bridge
  `ExtensionLlmBackend` (`llm_extension.rs`) cannot open attachments. They
  append the SAME fixed "not available in this deployment" notice to the user
  message and send no `artifact://` id or file name to the provider.
- **No reader on these entry points.** `greentic-runner-desktop`, the public
  `TenantRuntime::load` / `TenantRuntime::from_packs`, and the public
  `build_agent_node_handler` / `build_agent_node_wiring` /
  `build_agent_node_wiring_ephemeral` (and their `_ephemeral` handler) build no
  reader and no extension artifact port. Attachments become the fixed notice,
  and an extension's `put` answers `unsupported`. The env fallback does not
  apply there: it is a single-tenant `HostBuilder` rule only.
- **Process-level extension runtimes** (`runner/mod.rs`, `graph_node.rs`): no
  artifact port.
- **NATS serve path** (`greentic-aw-runtime/src/serve.rs`): `extract_user_text`
  builds the turn from `user_text` / `text` only.
- **Agent chat HTTP** (`greentic-runner-host/src/http/agent_chat.rs`,
  `turn_payload`): the turn carries the text or the card submit only.
- **Agent-graph turns** (`greentic-aw-runtime/src/graph/executor.rs`,
  `start(.., user_text: &str)`): text only.
- **Deep worker (`operala.call`) turns**: text only.
- **Provider-edge drops.** A file a provider could not hand over (too large,
  an unsupported type, a failed fetch, an exhausted quota, the door
  unavailable) is not silently dropped: the HOST leaves `url = null` and writes
  an `attachment_notes` entry (contract C1), which the `dw.agent` node turns
  into a notice for the agent. The runner never fetches from a provider itself.
- **Email and Teams Graph** channels emit no attachments in v1.
- **Test chat's runner sidecar** is a separately installed binary
  (`RUNNER_REINSTALL_REV`); it gets none of this until that pin moves.
