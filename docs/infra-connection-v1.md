# Infrastructure connection contract v1

How a deployed worker's design-extension tool obtains the connection of a
platform-provided resource (Qdrant `vector-store/qdrant`, Redis `cache/redis`).
Cross-repo: greentic-designer stages it, greentic-deployer writes it at the
right env segment, this repo serves it, greentic-start ships the runtime.

## 1. Staged document (written by the designer)

Store address (written with `op secrets put`):

```
secrets://default/<tenant>/_/infra/<canonical kind>
```

- category `infra`, env segment **`default`** (not the env id `local`), team `_`,
  no unit scope: one entry per kind per environment;
- tenant is `default` on remote lanes; the reader tries the served tenant first,
  then `default`;
- `<canonical kind>` is `greentic_secrets_lib::canonical_secret_name(kind)`:
  `vector-store/qdrant` -> `vector_store_qdrant`, `cache/redis` -> `cache_redis`.

Value (JSON):

```json
{"host":"q.example.com","port":6333,"scheme":"https","credential":"...","extras":{"collection_prefix":"acme__"}}
```

`v` is optional; absent is read as 1; `v > 1` is refused by name. Unknown
top-level fields are tolerated and not echoed. IPv6 hosts are staged
unbracketed.

Validation (mirrors admin/designer): host non-empty, <= 253 bytes, bare
DNS/IPv4/IPv6 (no scheme, userinfo, path, port, whitespace); port 1..=65535 and
a JSON number; scheme alphanumeric, <= 16 (default `https`); credential 1..=4096
bytes; extras <= 16 string entries, keys and values <= 256 bytes.

Reader: `greentic_aw_runtime::infra_connection` (`read`, `InfraConnection::parse`).

## 2. Guest-facing read

The extension declares, in `describe.json`,
`runtime.permissions.secrets: ["secret://infra/<canonical name>"]` and calls
`host.secrets.get("secret://infra/<canonical name>")`. The extension runtime
gates the URI against that declaration (prefix on a `/` boundary, wildcards
refused), then `StoreToolSecretsBackend` (runner-host `agent_node.rs`) routes
`secret://infra/<name>` to `runner/infra_secret.rs`. A nested path
(`secret://infra/a/b`) is not an infra URI.

The guest receives a host-normalised document (golden fixture:
`crates/greentic-aw-runtime/tests/fixtures/infra_connection_v1.json`):

```json
{"v":1,"kind":"vector-store/qdrant","host":"q.example.com","port":6333,
 "scheme":"https","base_url":"https://q.example.com:6333",
 "credential":"...","extras":{"collection_prefix":"acme__"}}
```

`kind` is echoed as the guest asked (the canonical name). `base_url` brackets
IPv6.

## 3. Errors

All surface as `SecretsError::Backend(msg)` (never `NotFound`, whose rendering
names no remedy). Messages never contain a value; serde errors are mapped to a
field name.

| Condition | Message shape |
|---|---|
| no entry at any candidate | `infra resource '<kind>' is not connected to this deployment (looked at <uris>); add it in workspace settings and deploy again` |
| store error other than not-found | `... could not be read from the secret store: secret store error` |
| malformed field | `... is malformed: host|port|scheme|credential|extras|document|v` |
| `v` above 1 | `... uses schema version N, this runtime reads up to 1` |
| `require_extra` blank/absent | `... has no '<key>' setting; refusing to use an unscoped namespace` |

A missing entry never falls back to an environment variable (that would
resurrect a localhost default), and the reader does no caching (a rotated
credential must not linger).

## 4. Extras

The runtime treats `extras` as opaque strings. The admin provisioners use
`collection_prefix` (Qdrant) and `key_prefix` (Redis) for isolation inside a
shared instance. The runtime cannot enforce a prefix on arbitrary traffic: an
extension must use `InfraConnection::require_extra` / refuse when absent.

## 5. Lanes

- Served: tools run by the per-tenant `TenantRuntime` path (Cloud Run / k8s
  worker, in-process `dw.agent`).
- Not served: flow `component.exec` nodes (they read the pack-scoped
  `host.secrets-store` and cannot reach a unit-less entry); the out-of-process
  serve path (`EnvSecretsBackend`, NATS), which resolves one credential for
  every tenant and therefore refuses infra with a named error; designer Run Demo
  and Test chat (no infra overlay).

## 6. Credential handling

Host side: `SecretString`, redacted `Debug`, no `tracing` of the document. Once
returned, the credential is in the guest's memory and **the guest owns what it
logs**; the host cannot stop a guest writing it to `host.logging`.

## 7. UNVERIFIED

- End-to-end with a real guest extension and a real `host.secrets.get` was not
  run; only unit tests against fake secret managers.
- Read in `greentic-ext-runtime` 1.2.38697752260: `secrets::get` logs the URI on
  denial and the backend's `Backend(msg)` at `error`; other call paths were not
  exhaustively read.
- Whether a deployed Cloud Run / k8s worker serves tenant `default` was not
  checked; the `default` fallback covers it.
- The deployer writing `infra` at env segment `default`, and start's reader,
  belong to other repos and are not exercised here.
