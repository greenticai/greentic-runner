# Coding Agents: How To Use `greentic-runner` Correctly

This document is for coding agents and contributors.

It explains how this repository fits into the larger Greentic toolchain so you do not try to solve the right problem in the wrong repo.

## Short Version

Do not treat `greentic-runner` as the whole product.

In the normal Greentic workflow:

- `greentic-pack` builds packs
- `gtc` creates bundles, applies setup, and starts apps
- `greentic-dev` provides developer tooling around the ecosystem
- `greentic-runner` executes flows and components at runtime

If a problem is about runtime execution, this repo may be the right place.
If a problem is about pack creation, setup persistence, or bundle assembly, another repo may own it.

## What This Repo Owns

This repo owns runtime behavior such as:

- flow execution
- node dispatch
- template rendering
- `component.exec`
- `provider.invoke`
- HTTP ingress normalization
- session pause/resume
- state and secrets access during execution
- pack loading and component invocation

If the bug is “the component got the wrong runtime payload”, this repo is often the right place.

## What This Repo Does Not Usually Own

This repo does not usually own:

- pack authoring UX
- setup questionnaires and setup persistence semantics
- bundle creation
- product-level app startup workflows
- wizard flows in `gtc`
- pack publishing or pack resolution authoring rules

If the bug is “setup answers were not written into the bundle at all”, this is usually not a runner bug.

## Which Tool To Use For Which Job

### Use `gtc` when you need:

- to create or set up a bundle
- to apply setup answers
- to start a local app in the normal product workflow
- to verify full app behavior

Typical examples:

- `gtc wizard`
- `gtc setup`
- `gtc start`

### Use `greentic-pack` when you need:

- to create a pack
- to resolve component references
- to build `.gtpack`
- to inspect pack-level packaging issues

Typical examples:

- `greentic-pack new`
- `greentic-pack resolve`
- `greentic-pack build`

### Use `greentic-dev` when you need:

- development helpers
- ecosystem tooling
- diagnostics outside the core runner

### Use `greentic-runner-cli` when you need:

- a low-level direct pack execution test
- to compare live bundle behavior with direct runner behavior
- to isolate whether a problem is in the runtime or in bundle/setup plumbing

This is especially useful for questions like:

- “Does the same pack behave differently under `gtc start` and direct runner execution?”
- “Is the component receiving the right input/config envelope?”

## A Good Debugging Pattern

When you are debugging a runtime problem, use this sequence:

1. Confirm the pack content.
2. Run the same pack directly with `greentic-runner-cli`.
3. Compare that with the full `gtc start` path.
4. Decide whether the mismatch happens:
   - before runtime
   - inside runtime
   - or after runtime

This matters because many issues that look like runner bugs are actually:

- setup answers never persisted
- secrets never provisioned
- bundle wiring not passing expected data into runtime
- pack authoring not emitting expected metadata

## How To Judge Ownership

Use these rules.

### Likely runner bug

It is likely a bug in this repo if:

- the runner receives the needed data but transforms it incorrectly
- `component.exec` or `provider.invoke` drops or rewrites payload incorrectly
- template variables are missing from the runtime template context
- the wrong secret path is computed during runtime lookup
- state/session logic is wrong during execution

### Likely not a runner bug

It is likely outside this repo if:

- setup answers never appear in the persisted bundle state
- setup-derived config is never materialized into the runtime payload
- the bundle never provisions a secret for the app pack
- the built pack is missing required metadata or assets
- `gtc` or another orchestration layer fails to pass the right manager/backend into the runner

## Important Repo-Specific Guidance

### `component.exec`

In this repo, `component.exec` executes whatever `config` is already present on the flow node payload.

It does not automatically invent or fetch setup-derived config unless some upstream layer has already materialized that config into the node payload.

So if a user expects setup answers to magically appear in `component.exec` config, do not assume the runner owns that behavior. First check whether the setup/config data is even available to the runner.

### Secret Errors

Be careful with secret-related error messages.

Some component/runtime layers preserve the original secret reference name in the error text even when lookup is canonicalized internally.

That means:

- seeing `OPENAI_API_KEY` or `OLLAMA_API_KEY` in the message
- does **not**
- automatically prove the runner looked up the uppercase secret path literally

Always inspect the actual persisted secret store and the runner path computation before concluding it is a normalization bug.

## Recommended Commands In This Repo

Basic development:

```bash
cargo build
cargo test
cargo clippy --all-targets --all-features -- -D warnings
```

Direct runner execution:

```bash
cargo run -p greentic-runner --features legacy-gen-bindings --bin greentic-runner-cli -- \
  --pack ./path/to/pack.gtpack \
  --input '{}'
```

HTTP runner host:

```bash
cargo run -p greentic-runner -- \
  --bindings ./bindings/demo.gtbind \
  --port 8080
```

## Documentation Priority

When deciding what to trust:

1. current code
2. focused crate-level docs
3. current task-specific tests
4. historical notes and older docs

Do not use older “behaviour snapshot” documents as the final source of truth if the code clearly differs.
