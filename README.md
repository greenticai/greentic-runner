# greentic-runner

`greentic-runner` is the runtime that executes Greentic packs.

If you are new to Greentic, the shortest useful description is:

- `greentic-pack` builds a pack
- `gtc` creates, sets up, and starts an app or bundle
- `greentic-runner` is the engine that actually runs the flows and components inside that app

This repository is mainly for people who:

- want to understand how Greentic runtime execution works
- need to debug flow execution, component calls, secrets, state, or messaging ingress
- are working on the runner itself

It is not usually the first tool end users should start with.

## Who This README Is For

This README is written for humans first, especially:

- product-minded builders
- technical writers
- non-specialist programmers
- people who need to understand what this repo does before changing code

If you are a coding agent or someone automating development tasks, skip to:

- [docs/coding-agents.md](docs/coding-agents.md)

That document explains how this repo should be used together with `gtc`, `greentic-dev`, `greentic-pack`, and related tools.

## What This Repository Does

This repository contains the runtime that:

- loads `.gtpack` files and materialized pack directories
- runs flow graphs
- invokes WebAssembly components
- handles `component.exec`, `provider.invoke`, `flow.call`, and built-in control nodes
- exposes HTTP ingress for channels such as webchat, Slack, Teams, Telegram, Webex, WhatsApp, generic webhooks, and timers
- manages pause/resume state for multi-step or waiting flows

In plainer language:

- your pack defines what should happen
- this runner is the thing that makes it happen at runtime

## How It Fits With Other Greentic Tools

People often expect this repo to do more than it actually does, so it helps to be explicit.

### `greentic-pack`

Use `greentic-pack` when you want to:

- create a pack
- resolve component references
- build a `.gtpack`

### `gtc`

Use `gtc` when you want to:

- create a runnable bundle
- apply setup answers
- start an app locally
- work with a complete app experience instead of a raw runtime

### `greentic-dev`

Use `greentic-dev` for developer tooling, diagnostics, and helper workflows around the broader Greentic ecosystem.

### `greentic-runner`

Use this repo when you need to:

- debug why a flow behaves a certain way at runtime
- inspect how `component.exec` or `provider.invoke` is executed
- investigate secrets, state, templating, pause/resume, or ingress normalization
- run a pack directly with the runner CLI for low-level troubleshooting

## The Main Things Built Here

This workspace produces a few important binaries and crates:

- `greentic-runner`
  The main HTTP runtime host.

- `greentic-runner-cli`
  A legacy local execution tool for running a pack directly without a full
  bundle. It is kept in the repo, but it is not part of the default published
  runner binary set. Build it explicitly with `--features legacy-gen-bindings`
  if you still need it.

- `greentic-gen-bindings`
  A legacy helper that inspects a pack and emits a bindings seed.
  It is kept in the repo, but it is not part of the default published runner
  binary set. Build it explicitly with `--features legacy-gen-bindings` if you
  still need it.

- `greentic-runner-host`
  The core runtime crate where most of the real execution logic lives.

- `greentic-runner-desktop`
  A local desktop-style harness used by runner-side tools and tests.

## The Simplest Mental Model

If you are not deep in the codebase yet, this model is usually enough:

1. A pack contains flows and components.
2. A flow reaches a node.
3. The runner renders any templates in that node.
4. The runner decides what action to take.
5. If the node is a component call, the runner invokes the component.
6. If the flow pauses, the runner stores the state and waits for the next event.

That is the heart of this repository.

## When You Should Use This Repo Directly

You should probably work in this repo directly if:

- a pack works in theory but fails only at runtime
- a component receives the wrong payload
- setup, secrets, state, or context seem to disappear before invocation
- a messaging adapter normalizes input incorrectly
- a flow pauses or resumes in the wrong place

You probably should not start here if your task is mainly:

- authoring a new pack
- packaging or publishing a pack
- creating a bundle
- answering setup questions
- starting an app in the usual product workflow

Those tasks usually belong to `greentic-pack`, `gtc`, or another repo.

## Quick Local Commands

For people working on the runner itself, the most common commands are:

```bash
cargo build
cargo test
cargo clippy --all-targets --all-features -- -D warnings
```

To run the HTTP runtime directly:

```bash
cargo run -p greentic-runner -- \
  --bindings ./bindings/acme.gtbind \
  --port 8080
```

To run a pack directly with the local runner harness:

```bash
cargo run -p greentic-runner --bin greentic-runner-cli -- \
  --pack ./path/to/pack.gtpack \
  --input '{}'
```

These are runtime/debugging commands. They are not the normal end-user way to run a Greentic app.

## Where To Read Next

Start with the document that matches your goal:

- [docs/coding-agents.md](docs/coding-agents.md)
  For coding agents and contributors who need tool-by-tool workflow guidance.

- [docs/README.md](docs/README.md)
  For the wider documentation map.

- [crates/greentic-runner-host/README.md](crates/greentic-runner-host/README.md)
  For low-level runtime details, host behavior, and crate-specific reference material.

## Notes About Older Documentation

This repository still contains older deep-dive documents and historical notes.

They may still be useful for archaeology, but they are not the best starting point for most readers.

If two documents seem to disagree:

- trust the current code first
- trust focused crate-level docs next
- treat historical or “current behaviour” snapshots as reference material, not product guidance

## License

MIT
