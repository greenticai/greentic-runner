# Pack Verification Visibility Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every unverified pack run leave a countable record, without changing what runs.

**Architecture:** One pure function decides what to say about a verification outcome; three call sites in `run_pack_with_options` feed it and record the result. The decision is unit-tested directly, following the existing `helper_functions_preserve_ids_and_transcript_shape` pattern in the same file; the wiring stays one line per site so there is little left to get wrong.

**Tech Stack:** Rust 1.95, `tracing`, `serde_json`, the crate's own `RunRecorder` / `TranscriptWriter`.

**Spec:** `docs/superpowers/specs/2026-08-06-pack-verification-visibility-design.md`

## Global Constraints

- **Do not change enforcement.** `SigningPolicy::DevOk` stays the default, `runner-core` is not touched, `is_signature_error` is not removed, and `greentic-pack` is not modified. This plan adds observability only — if a run succeeds today it must still succeed, byte for byte, after it.
- **Recording must never fail a run.** The three new call sites fire on the success path. A failure to write an observability record is logged and swallowed, never propagated with `?`. The pre-existing error-path call at the `Err` arm keeps its `?` — the run is already failing there.
- English only in source, tests, doc comments and commit messages.
- No `unwrap()` / `panic!()` in production code paths; tests may use `unwrap()` / `expect()`.
- Conventional commit messages. **No Claude co-author attribution.**
- Do not bypass git hooks.

---

## File Structure

**Modified:** `crates/greentic-runner-desktop/src/lib.rs` — adds one enum, one pure function, three call-site edits, and unit tests in the existing `mod tests`.

Nothing else changes. The file is large already; this adds ~40 lines of production code plus tests, which does not justify splitting it, and splitting it would be unrelated churn.

---

## Task 1: The decision function

**Files:**
- Modify: `crates/greentic-runner-desktop/src/lib.rs` (add import, enum, function; tests in the existing `mod tests` near `helper_functions_preserve_ids_and_transcript_shape`)

**Interfaces:**
- Consumes: `greentic_pack::reader::VerifyReport` (fields `signature_ok: bool`, `sbom_ok: bool`, `warnings: Vec<String>`)
- Produces: `enum VerifyOutcome<'a> { Verified, Unverified(&'a VerifyReport), DirectorySkipped, Downgraded(&'a str) }` and `fn verify_event(outcome: VerifyOutcome<'_>) -> Option<(&'static str, String)>`

- [ ] **Step 1: Write the failing tests**

Add to the existing `mod tests` in `crates/greentic-runner-desktop/src/lib.rs`:

```rust
    fn report(signature_ok: bool, warnings: &[&str]) -> VerifyReport {
        VerifyReport {
            signature_ok,
            sbom_ok: true,
            warnings: warnings.iter().map(|w| (*w).to_string()).collect(),
        }
    }

    #[test]
    fn a_verified_pack_has_nothing_to_report() {
        assert!(verify_event(VerifyOutcome::Verified).is_none());
        // A report that verified is equally silent even if it carried unrelated
        // warnings — this function speaks only about signature state.
        assert!(verify_event(VerifyOutcome::Unverified(&report(true, &["noise"]))).is_none());
    }

    #[test]
    fn an_unverified_pack_reports_the_crates_own_diagnosis() {
        // greentic-pack already distinguishes missing / incomplete / invalid.
        // Surfacing its wording rather than re-deriving one keeps the two in step.
        let r = report(false, &["signature files missing; skipping verification"]);
        let (status, message) = verify_event(VerifyOutcome::Unverified(&r)).expect("reports");
        assert_eq!(status, "unverified");
        assert!(
            message.contains("signature files missing"),
            "the crate's own warning must survive verbatim: {message}"
        );
    }

    #[test]
    fn several_warnings_are_all_kept() {
        let r = report(false, &["first problem", "second problem"]);
        let (_, message) = verify_event(VerifyOutcome::Unverified(&r)).expect("reports");
        assert!(message.contains("first problem"), "{message}");
        assert!(message.contains("second problem"), "{message}");
    }

    #[test]
    fn an_unverified_pack_with_no_warnings_still_reports() {
        // Silence here would recreate the very defect this work closes.
        let r = report(false, &[]);
        let (status, message) = verify_event(VerifyOutcome::Unverified(&r)).expect("reports");
        assert_eq!(status, "unverified");
        assert!(!message.is_empty(), "an empty message is as invisible as no event");
    }

    #[test]
    fn a_directory_input_reports_that_it_was_skipped() {
        let (status, message) = verify_event(VerifyOutcome::DirectorySkipped).expect("reports");
        assert_eq!(status, "skipped");
        assert!(message.contains("director"), "{message}");
    }

    #[test]
    fn a_downgraded_error_names_itself_and_how_it_was_matched() {
        let (status, message) =
            verify_event(VerifyOutcome::Downgraded("boom: bad signature")).expect("reports");
        assert_eq!(status, "downgraded");
        assert!(message.contains("boom: bad signature"), "{message}");
        // The substring match is the reason this downgrade happened at all, and
        // it is known to be an unreliable classifier — say so where it is seen.
        assert!(message.contains("substring"), "{message}");
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
export PATH=$(echo "$PATH" | tr ':' '\n' | grep -v '^/tmp/\.mount_orca\.AnIKIz9' | grep -v '^/tmp/\.mount_orca\.Ap8L4yX' | paste -sd:)
export CARGO_BUILD_JOBS=2
cargo test -j 2 -p greentic-runner-desktop --lib verify_event
```
Expected: FAIL to compile — `verify_event`, `VerifyOutcome` and `VerifyReport` are not in scope.

> The `PATH` line is mandatory on this machine: a dead FUSE mount sits in `PATH` ahead of `/usr/bin` and aborts `execvp`'s path scan, so every link fails with `could not exec the linker "cc": Transport endpoint is not connected`. It is a machine fault, not a code fault.

- [ ] **Step 3: Add the import**

`VerifyReport` is not currently imported. Line 2 of `crates/greentic-runner-desktop/src/lib.rs` reads:

```rust
use greentic_pack::reader::open_pack;
```

Change it to:

```rust
use greentic_pack::reader::{VerifyReport, open_pack};
```

Note that `SigningPolicy` is **not** imported from `greentic_pack` — the runner declares its own `pub enum SigningPolicy` at line 171 and converts with `to_reader_policy()` before calling `open_pack`. Leave both alone; this task needs only the report type.

- [ ] **Step 4: Write the decision function**

Place it next to `is_signature_error` so the two read together:

```rust
/// What a verification outcome is worth recording, if anything.
///
/// Kept separate from the call sites so the decision can be tested directly —
/// the call sites live deep inside `run_pack_with_options`, which needs a real
/// pack, a real profile and a temp directory to reach.
enum VerifyOutcome<'a> {
    /// The signature verified. Nothing to say.
    Verified,
    /// The pack loaded but its signature did not verify. Under the default
    /// `DevOk` policy this is what a missing, incomplete or invalid signature
    /// looks like: `open_pack` returns `Ok` with `signature_ok: false` and an
    /// explanatory warning, and until now the whole report was discarded.
    Unverified(&'a VerifyReport),
    /// The input was a directory, so verification never ran at all.
    DirectorySkipped,
    /// An error was downgraded to a warning by `is_signature_error`.
    Downgraded(&'a str),
}

/// `None` when there is nothing worth recording; otherwise the event status and
/// its message.
fn verify_event(outcome: VerifyOutcome<'_>) -> Option<(&'static str, String)> {
    match outcome {
        VerifyOutcome::Verified => None,
        VerifyOutcome::Unverified(report) if report.signature_ok => None,
        VerifyOutcome::Unverified(report) => {
            // Fall back to a fixed sentence rather than an empty message: an
            // empty event is as invisible as no event, which is the defect this
            // exists to close.
            let message = if report.warnings.is_empty() {
                "pack signature did not verify; the loader reported no detail".to_string()
            } else {
                report.warnings.join("; ")
            };
            Some(("unverified", message))
        }
        VerifyOutcome::DirectorySkipped => Some((
            "skipped",
            "pack verification skipped: input is a directory, not a signed archive".to_string(),
        )),
        VerifyOutcome::Downgraded(err) => Some((
            "downgraded",
            format!(
                "continuing despite error, matched as signature-related by substring: {err}"
            ),
        )),
    }
}
```

- [ ] **Step 5: Run the tests to verify they pass**

```bash
cargo test -j 2 -p greentic-runner-desktop --lib verify_event
```
Expected: PASS, 6 tests.

- [ ] **Step 6: Commit**

```bash
git add crates/greentic-runner-desktop/src/lib.rs
git commit -m "feat(verify): decide what an unverified pack run should report"
```

---

## Task 2: Wire the three call sites

**Files:**
- Modify: `crates/greentic-runner-desktop/src/lib.rs` — the `Ok(load)` arm, the `Err(err)` arm, and the `else` (directory) branch of `if pack_path.is_file()`, around lines 318-356

**Interfaces:**
- Consumes: `verify_event(VerifyOutcome<'_>) -> Option<(&'static str, String)>` from Task 1; `RunRecorder::record_verify_event(&self, status: &str, message: &str) -> Result<()>`

- [ ] **Step 1: Add the recording helper**

The same three lines would otherwise be repeated at three call sites. Put this next to `verify_event`:

```rust
/// Record a verification outcome, if there is one worth recording.
///
/// Deliberately swallows a recording failure. All three call sites fire on the
/// success path, and observability added by this change must not be able to turn
/// a run that works into a run that fails. The pre-existing call on the error
/// path keeps its `?` — that run is already failing.
fn note_verify_outcome(recorder: &RunRecorder, outcome: VerifyOutcome<'_>) {
    let Some((status, message)) = verify_event(outcome) else {
        return;
    };
    warn!(status, message = %message, "pack verification");
    if let Err(err) = recorder.record_verify_event(status, &message) {
        warn!(error = %err, "could not record the pack verification event");
    }
}
```

- [ ] **Step 2: Wire the `Ok(load)` arm**

Inside `if pack_path.is_file() { match open_pack(...) { Ok(load) => { … } … } }`, immediately after the existing `recorder.update_pack_metadata(PackMetadata { … });` call, add:

```rust
                note_verify_outcome(&recorder, VerifyOutcome::Unverified(&load.report));
```

`verify_event` returns `None` when `report.signature_ok` is true, so a properly signed pack stays silent — the branch is on the data, not on the call site.

- [ ] **Step 3: Wire the downgrade arm**

In the `Err(err)` arm, inside the `if opts.signing == SigningPolicy::DevOk && is_signature_error(&err.message)` branch, **replace** the existing line:

```rust
                    warn!(error = %err.message, "continuing despite signature error (dev policy)");
```

with:

```rust
                    note_verify_outcome(&recorder, VerifyOutcome::Downgraded(&err.message));
```

Leave the `else` branch — the one that returns `Err(anyhow!("pack verification failed: {}", err.message))` — exactly as it is, and leave the `recorder.record_verify_event("error", &err.message)?;` above the `if` untouched, `?` included.

- [ ] **Step 4: Wire the directory branch**

**Replace** the `else` branch of `if pack_path.is_file()`:

```rust
    } else {
        tracing::debug!(
            path = %pack_path.display(),
            "skipping pack verification for directory input"
        );
```

with:

```rust
    } else {
        note_verify_outcome(&recorder, VerifyOutcome::DirectorySkipped);
```

Keep whatever follows the `debug!` inside that branch unchanged — only the logging line is replaced.

> This raises a normal dev run from `debug!` to `warn!`. That is intended and is recorded as a known cost in §7 of the spec: the bypass is currently invisible to exactly the people best placed to notice it.

- [ ] **Step 5: Verify nothing else changed**

```bash
git diff --stat
```
Expected: one file, roughly 45 insertions and 5 deletions across both tasks. If the diff touches `runner-core`, `SigningPolicy` defaults, or `is_signature_error`'s body, something went wrong — those are out of scope by the Global Constraints.

- [ ] **Step 6: Build and run the crate's tests**

```bash
export PATH=$(echo "$PATH" | tr ':' '\n' | grep -v '^/tmp/\.mount_orca\.AnIKIz9' | grep -v '^/tmp/\.mount_orca\.Ap8L4yX' | paste -sd:)
export CARGO_BUILD_JOBS=2
cargo test -j 2 -p greentic-runner-desktop --lib
```
Expected: PASS, including the 6 tests from Task 1 and every pre-existing test in the crate. Do not run `cargo test --workspace` — it is slow and unnecessary here.

- [ ] **Step 7: Check formatting and lints**

```bash
cargo fmt --all -- --check
cargo clippy -p greentic-runner-desktop --all-targets -- -D warnings
```
Expected: both clean. If clippy objects to the lifetime on `VerifyOutcome<'a>`, satisfy it rather than adding an `allow`.

- [ ] **Step 8: Commit**

```bash
git add crates/greentic-runner-desktop/src/lib.rs
git commit -m "feat(verify): record unverified, skipped and downgraded pack runs

A pack with a missing, incomplete or invalid signature loaded in silence: under
the default DevOk policy open_pack returns Ok with report.signature_ok=false and
an explanatory warning, and the runner never read load.report. A directory input
skipped verification behind a debug! line. Both are now recorded verify events.

No enforcement change: the default policy, runner-core and is_signature_error are
untouched, so every run that succeeded before still succeeds."
```

---

## Self-Review

**Spec coverage:**

| Spec section | Task |
|---|---|
| §3.1 read the report | Task 1 (`Unverified` arm), Task 2 Step 2 |
| §3.2 directory bypass visible | Task 1 (`DirectorySkipped`), Task 2 Step 4 |
| §3.3 downgrade audible | Task 1 (`Downgraded`), Task 2 Step 3 |
| §3 "recorded event, not only a log" | Task 2 Step 1 — `note_verify_outcome` does both |
| §4 error handling asymmetry | Task 2 Step 1 (swallow) and Step 3 (existing `?` left alone) |
| §5 three tests | Task 1 has six, covering the three cases plus the verified, empty-warning and multi-warning edges |
| §6 out of scope | Global Constraints, plus Task 2 Step 5 as an active check |
| §7 known cost | Called out at Task 2 Step 4 so the implementer meets it where the change happens |

**Placeholder scan:** none. Every step carries the literal code or the literal command.

**Type consistency:** `VerifyOutcome<'a>` and `verify_event` are defined in Task 1 and used unchanged in Task 2. `note_verify_outcome(&RunRecorder, VerifyOutcome<'_>)` is defined in Task 2 Step 1 and used in Steps 2-4. `record_verify_event(&self, status: &str, message: &str) -> Result<()>` is the crate's existing signature, matched by the `if let Err(err)` in Step 1.

**One deliberate deviation from the spec's letter:** the spec says three tests; Task 1 writes six. The extra three pin the cases where this change could silently do nothing — a verified pack must stay quiet, an unverified pack with no warnings must still speak, and multiple warnings must all survive. A test suite that only covers the happy shape of each case would pass against a function that returns a constant.
