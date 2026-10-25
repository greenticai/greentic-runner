# Runner Snapshot Tests

Snapshot tests live under `tests/snapshots/`:

- `inputs/` holds the test inputs used to drive the runner.
- `expected/` stores the golden snapshots (insta `.snap` files).

## Running and updating snapshots

The harness is the `snapshots` integration test of the `greentic-runner` crate
(`crates/greentic-runner/tests/snapshots.rs`), driven by [insta](https://insta.rs):

```bash
cargo test -p greentic-runner --test snapshots
```

To update snapshots locally, set insta's update mode (there is no `--accept`
test flag; that belongs to `cargo insta`):

```bash
INSTA_UPDATE=always cargo test -p greentic-runner --test snapshots
```

Alternatively, review pending `.snap.new` files with `cargo insta review`
(requires `cargo install cargo-insta`).

Snapshots must **not** auto-update in CI (insta defaults to `INSTA_UPDATE=no`
when `CI` is set).

## Debugging failures

When a snapshot test fails, the harness writes artifacts to:

```
target/snapshot-artifacts/<test>/
```

Each failure directory contains:
- `input.json`
- `actual.json`
- `expected.json`
