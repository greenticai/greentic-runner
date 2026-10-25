# greentic-runner-host

`greentic-runner-host` packages the Greentic runner as a standalone crate. It owns tenant bindings, the pack watcher, Wasmtime glue, canonical ingress adapters (Telegram, Teams, Slack, WebChat, Webex, WhatsApp, generic webhook, timers), the state machine (pause/resume, session/state persistence), and admin/health endpoints. Binaries such as `greentic-runner` or `greentic-demo` embed this crate instead of vendoring runtime internals.

Provider execution is **provider-core only**: packs declare provider runtimes via the `greentic.ext.provider` extension and invoke them with the `provider.invoke` node, instantiating `greentic:provider/schema-core@1.0.0` components. Legacy typed provider worlds are not supported.

## Architecture highlights

- **Pack ingestion** – consumes a JSON index (local path, HTTPS, or cloud bucket) via `runner-core`, verifies signatures/digests (trust keys from `packs.trust.public_keys`, `PACK_VERIFY_STRICT`), caches artifacts under the configured `packs.cache_dir`, and supports ordered overlays per tenant.
- **Hot reload** – `PACK_REFRESH_INTERVAL` drives a watcher that resolves the index, preloads packs, and swaps tenant runtimes atomically; `/admin/packs/reload` triggers the same path on demand. Overlays can be added/removed without touching the base pack.
- **Canonical ingress** – all adapters normalize raw provider payloads into the shared schema:
  ```json
  {
    "tenant": "demo",
    "provider": "slack",
    "provider_ids": {
      "workspace_id": "T123",
      "channel_id": "C789",
      "thread_id": "1731315600.000100",
      "user_id": "U456",
      "message_id": "1731315600.000100",
      "event_id": "Ev01ABC"
    },
    "session": {
      "key": "demo:slack:1731315600.000100:U456",
      "scopes": ["chat","attachments","buttons"]
    },
    "timestamp": "2025-11-11T09:00:00Z",
    "text": "Hi",
    "attachments": [],
    "buttons": [],
    "entities": { "mentions": [], "urls": [] },
    "metadata": { "raw_headers": {}, "ip": null },
    "channel_data": { "type": "message" },
    "raw": { "...": "original provider payload" }
  }
  ```
  Canonical session keys follow `{tenant}:{provider}:{conversation-or-thread-or-channel}:{user}`, ensuring pause/resume and dedupe behave consistently per adapter.
- **Sessions & state** – the host bundles `greentic-session`/`greentic-state`. Multi-turn flows pause via `session.wait`; the runtime stores `FlowSnapshot`s keyed by the canonical session, resumes on the next ingress event, and clears the entry on completion. Packs can access `greentic:state/store@1.0.0` when the component declares state capability and the policy allows it.
- **Telemetry & admin** – optional OTLP bootstrapping (`greentic-telemetry`), `/healthz`, and bearer-protected `/admin` endpoints (loopback-only when `ADMIN_TOKEN` is unset).

### Pack index format

Pack ingestion is driven by a JSON index (see `examples/index.json`). Each tenant entry declares a `main_pack` plus optional `overlays`, letting you layer overrides without rebuilding the base artifact:

```json
{
  "tenants": {
    "demo": {
      "main_pack": {
        "reference": { "name": "demo-pack", "version": "1.2.3" },
        "locator": "fs:///packs/demo.gtpack",
        "digest": "sha256:abcd...",
        "signature": "ed25519:...",
        "path": "./packs/demo.gtpack"
      },
      "overlays": [
        {
          "reference": { "name": "demo-overlay", "version": "1.2.3" },
          "path": "./packs/demo_overlay.gtpack",
          "digest": "sha256:efgh..."
        }
      ]
    }
  }
}
```

During a reload the watcher resolves each locator (filesystem, HTTPS, OCI, S3, GCS, Azure blob), verifies digests/signatures, caches artifacts, and constructs a `TenantRuntime` that loads the main pack plus overlays in order. Overlay changes are safe to deploy independently—`crates/tests/tests/host_integration.rs` includes regression coverage.

### Materialized packs

Runner can also execute a materialized pack directory (contains `manifest.cbor`, flows/templates, and `components/<id>.wasm`) or a `.gtpack` paired with local component files. Component resolution now prefers explicit overrides, then the materialized directory, and finally embedded archive entries; missing components raise a clear error. The desktop CLI exposes `--components-dir` / `--components-map` so distributor-produced layouts can run without the runner fetching OCI components itself.

### Component cache (preview)

The runner exposes a cache module for compiled component artifacts. Each cache entry is scoped by an **EngineProfile** (Wasmtime version, target triple, CPU policy, and a config fingerprint) and an **ArtifactKey** (`engine_profile_id` + `wasm_digest`). Disk entries are namespaced under `<cache_root>/v1/<engine_profile_id>/...` to prevent cross-version contamination.

The cache manager is wired into component loading with disk + memory tiers. Disk entries are serialized Wasmtime components with metadata; memory entries hold `Arc<Component>` with bounded eviction. The API surface (`CacheManager::get_component`, `warmup`, `doctor`, `prune_disk`) is available for future CLI extensions and diagnostics.

### Pause & resume semantics

Packs can pause mid-flow by emitting the `session.wait` component. The host persists the `FlowSnapshot` (current node pointer + execution state) into `greentic-session`. The next inbound activity for the same canonical session key (`tenant:provider:channel:conversation:user`) automatically resumes the stored snapshot, continues execution, and clears the entry when the flow completes. This makes multi-message LLM flows and human-in-the-loop approvals idempotent without bespoke session wiring.

### OAuth broker integration

Each tenant bindings file may optionally declare an `oauth` block:

```yaml
oauth:
  http_base_url: https://oauth.api.greentic.net/
  nats_url: nats://oauth-broker:4222
  provider: greentic.oauth.default
  env: prod        # optional, falls back to GREENTIC_ENV/local
  team: platform   # optional logical scope
```

When present, the host wires `greentic-oauth-host`, keeps a tenant-scoped
client configuration, and registers the `greentic:oauth-broker@1.0.0/world
broker` WIT world in Wasmtime. Packs that import the world can ask the host for
consent URLs, exchange codes for tokens, or fetch stored tokens, while
environments without the block behave exactly as before (no broker world is
wired).

For OAuth-backed `events-email` execution, the host now has the first explicit
execution helpers in:

- [adapt_events_email.rs](/home/vgrishkyan/greentic/greentic-runner/crates/greentic-runner-host/src/runner/adapt_events_email.rs)
- [oauth.rs](/home/vgrishkyan/greentic/greentic-runner/crates/greentic-runner-host/src/oauth.rs)

The intended host-side flow is:

1. parse `EmailSendRequest`
2. validate the embedded OAuth hint
3. build the resource-token request
4. build the provider HTTP execution target
5. perform the live HTTP call with the resolved access token

`TenantRuntime` now exposes the first host-side primitives for this path:

- `build_events_email_execution_plan(...)`
- `execute_events_email_request(...)`
- `execute_events_email_with_oauth(...)`

## Quick start

```rust
use greentic_runner_host::{Activity, HostBuilder, HostConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = HostConfig::load_from_path("./bindings/customera.yaml")?;
    let host = HostBuilder::new().with_config(config).build()?;

    host.start().await?;
    host.load_pack("customera", "./packs/customera/index.ygtc".as_ref()).await?;

    let activity = Activity::text("hello")
        .with_tenant("customera")
        .from_user("u-1");

    let replies = host.handle_activity("customera", activity).await?;
    for reply in replies {
        println!("reply: {}", reply.payload());
    }

    host.stop().await?;
    Ok(())
}
```

## Cargo features

- `verify` *(default)* – validate pack files exist before loading.
- `mcp` – enable tool invocation through the [`mcp-exec`](https://crates.io/crates/mcp-exec) bridge.
- `telemetry` – wire OTLP export via [`greentic-telemetry`](https://crates.io/crates/greentic-telemetry).

## Environment

| Variable | Description | Default |
| --- | --- | --- |
| `PACK_VERIFY_STRICT` | Truthy (`1/true/yes/on`) enables strict verification: a trust key is required, signatures must be canonical, remote packs need a pinned digest, `fs://` locators are confined. It only adds checks; it cannot switch verification off when a trust key is configured | `false` |
| `PACK_FS_ALLOWED_ROOTS` | Extra roots (platform path list) absolute `fs://` locators may point into; setting it turns confinement on | _unset_ |
| `PACK_ALLOW_LEGACY_SIGNATURES` | Accept legacy digest-only signatures under strict mode | `false` |
| `GREENTIC_TRACE_OUT` | Trace file path; setting it enables trace capture (unset = off) | _unset_ |
| `GREENTIC_TRACE_CAPTURE_INPUTS` | `1` also records node inputs in the trace | _unset_ |
| `PACK_REFRESH_INTERVAL` | Interval used by the background watcher (`30s`, `5m`, etc.) | `30s` |
| `TENANT_RESOLVER` | Router mode for HTTP requests (`host`, `header`, `jwt`, `env`) | `env` |
| `DEFAULT_TENANT` | Fallback tenant identifier when the resolver cannot infer one | `demo` |
| `SECRETS_BACKEND` | Secrets provider to initialise (`env`, `aws`, `gcp`, `azure`) | `env` |
| `OTEL_SERVICE_NAME` | Overrides the OTLP service name advertised to the collector | `greentic-runner-host` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Explicit OTLP collector endpoint | provider preset / unset |
| `ADMIN_TOKEN` | Bearer token required for `/admin` endpoints (loopback-only access if unset) | _unset_ |

> Provider secrets such as `SLACK_SIGNING_SECRET`, `WEBEX_WEBHOOK_SECRET`,
> `WHATSAPP_VERIFY_TOKEN` and `WHATSAPP_APP_SECRET` are **not read from process
> env by the host**. Provider credentials are resolved through the secrets
> backend (`SECRETS_BACKEND`; the `env` backend maps secret keys to env vars)
> by the provider-core components that need them.

## Admin API

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/healthz` | Liveness check (telemetry, secrets, active packs) |
| `GET` | `/admin/packs/status` | Lists loaded tenants, versions, and digests plus last reload info |
| `POST` | `/admin/packs/reload` | Triggers an immediate pack refresh via the watcher |

If `ADMIN_TOKEN` is set, clients must send `Authorization: Bearer <token>`; otherwise, admin endpoints are limited to loopback connections.

## Ingress adapters

| Adapter | Route | Session anchor | Notes / Env |
| --- | --- | --- | --- |
| Telegram Bot API | `POST /messaging/telegram/webhook` | `chat.id:user.id` (fallback to `user.id`) | Uses `update_id` for dedupe; outbound path relies on `TELEGRAM_BOT_TOKEN` |
| Microsoft Teams (Bot Framework) | `POST /teams/activities` | `replyToId` → `conversation.id` → channel | Accepts Activities JSON (`channelData`, attachments) |
| Slack Events API | `POST /slack/events` | `thread_ts` → `channel` | Requires `SLACK_SIGNING_SECRET`, dedupes via `event_id`, handles retries |
| Slack Interactive | `POST /slack/interactive` | `channel`/`thread` from payload | Same signing secret; parses `payload=` form body |
| WebChat / Direct Line | `POST /webchat/activities` | `conversation.id` | Mirrors Bot Framework schema; attachments mapped 1:1 |
| Cisco Webex | `POST /webex/webhook` | `parentId` → `roomId` | Optional `WEBEX_WEBHOOK_SECRET`; keeps `requires_auth` metadata for file URLs |
| WhatsApp Cloud API | `GET/POST /whatsapp/webhook` | `messages[].from` | `WHATSAPP_VERIFY_TOKEN` (challenge) + `WHATSAPP_APP_SECRET` (signature); interactive/list replies → canonical buttons |
| Generic Webhook | `ANY /webhook/:flow_id` | `Idempotency-Key` header (if present) | Wraps method/path/headers/body into canonical payload |
| Timers / Cron | Defined in `bindings.yaml` | `schedule_id` | Schedules flows with normalized cron (seconds field injected) |

Each adapter injects the canonical payload (`tenant`, `provider`, `provider_ids`, `session`, `timestamp`, `text`, `attachments`, `buttons`, `entities`, `metadata`, `channel_data`, `raw`) and uses the same session-key policy `{tenant}:{provider}:{conversation-or-thread-or-channel}:{user}` enforced everywhere. Custom adapters can follow the same pattern by translating incoming payloads into an `IngressEnvelope`.

## License

This project is licensed under the [MIT License](./LICENSE).
