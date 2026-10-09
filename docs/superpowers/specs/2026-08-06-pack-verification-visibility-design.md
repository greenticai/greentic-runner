# Pack verification visibility — Design Spec

**Status:** Draft — 2026-08-06
**Closes (partly):** B6 from `greentic-designer`'s 2026-07-30 repo-integration
audit, as re-verified on 2026-08-06 against `origin/develop` `88aa3963`.
**Deliberately does not close:** the enforcement half of B6 (default policy,
`runner-core`'s hardcoded policy). See §6.

## 1. Problem

A pack whose signature is missing, incomplete, or invalid runs today **in
complete silence**. Not downgraded to a warning — silent.

The audit described this as "signature errors are downgraded to `warn!` via a
substring match on the word 'signature'". Reading the code, that is not what
happens on the path everyone actually runs.

`greentic-pack`'s `open_pack` handles all three signature faults the same way
under `SigningPolicy::DevOk`, the default:

```rust
// signature present but invalid
Err(err) => {
    if matches!(policy, SigningPolicy::Strict) { return Err(err); }
    warnings.push(format!("signature verification failed: {err}"));
    false
}
// signature files absent
(None, None, _) => match policy {
    SigningPolicy::Strict => bail!("signature file `{}` missing", SIGNATURE_PATH),
    SigningPolicy::DevOk  => { warnings.push("signature files missing; skipping verification".into()); false }
},
// signature files incomplete
_ => { match policy {
    SigningPolicy::Strict => bail!("signature files incomplete; missing chain"),
    SigningPolicy::DevOk  => warnings.push("signature files incomplete; skipping verification".into()),
} false }
```

Every one returns `Ok(PackLoad { report: VerifyReport { signature_ok: false, warnings, .. }, .. })`.
Only `Strict` produces an `Err`.

And `crates/greentic-runner-desktop/src/lib.rs` **never reads `load.report`** —
a repo-wide search for it returns nothing. The `Ok` arm takes `load.manifest.meta`
and `load.gpack_manifest` and discards the rest.

Two consequences follow, and the second is the sharper one:

1. `greentic-pack` already computes exactly the diagnosis we want — which of the
   three faults occurred, in prose — and the runner throws it away.
2. The `is_signature_error` predicate cannot do its stated job. It only runs on
   the `Err` arm, and under `DevOk` a signature fault never produces an `Err`.
   The only thing it can catch is a **non-signature** error whose message happens
   to contain the word "signature" — the exact inverse of its purpose.

There is also a third silence: a directory input skips verification entirely
(`if pack_path.is_file()` … `else { tracing::debug!(…) }`), leaving one debug
line. That is a bypass, not a downgrade.

## 2. Goal, and what is deliberately not the goal

Make every unverified run **countable**, and change nothing about what runs.

Enforcement is out of scope for a specific reason rather than caution: `packc
build` ships packs **unsigned** by default (verified 2026-08-06 — the production
CLI writes `PackSignatures::default()`; `PackBuilder`'s dev-signing path is used
only by tests and one example). Flipping the runner's default to `Strict` today
would reject essentially every pack the platform produces. Enforcement without an
issuing side is the same trap as B21, and it is a product decision, not a bug
fix.

So this spec buys the data that decision needs: when someone later asks "what
breaks if we turn signing on", the answer will be in the artifacts instead of a
guess.

## 3. Design

Three edits, all in `crates/greentic-runner-desktop/src/lib.rs`. No cross-repo
change, no new dependency, no default changed.

**3.1 Read the report that is already in hand.** On the `Ok(load)` arm, when
`load.report.signature_ok == false`, emit a `warn!` and record a verify event
carrying `load.report.warnings`. The event exposes a `status == "unverified"` that
can be counted as a filter; the sub-classification (missing vs incomplete vs
invalid) survives only as the loader's prose text in the warnings, and
distinguishing it requires substring matching — the technique §6 already
notes is not the proper approach. A typed reason would require `greentic-pack`
to expose a `PackVerifyReason` enum, listed in §6 as its own task.

**3.2 Make the directory bypass visible.** Replace the `tracing::debug!` in the
`else` branch with a `warn!` plus a recorded verify event stating that
verification was skipped because the input is a directory.

**3.3 Make the downgrade audible.** When `is_signature_error` downgrades an
error, `warn!` naming the error and recording that it was matched by substring.
The predicate's behaviour is unchanged — see §6 for why it is not removed.

### Why a recorded event and not only a log line

`record_verify_event` writes a structured record (`ts`, `session_id`, `flow_id`)
that persists as a run artifact; a `warn!` disappears with the process. The
entire point of this work is that someone can **count** unverified runs before
enforcement is switched on, and counting needs records.

## 4. Error handling

The existing error path propagates a recording failure:

```rust
recorder.record_verify_event("error", &err.message)?;
```

The three new call sites will **not** use `?`. They fire on the success path, and
a failure to write an observability record must not turn a successful run into a
failed one. They log the recording failure and continue.

This asymmetry is deliberate. On the error path the run is already failing, so
propagating costs nothing; on the success path propagating would mean the
observability added by this spec could itself break runs — the opposite of the
goal.

## 5. Testing

Three tests, one per silence closed:

- a pack whose `report.signature_ok` is `false` produces a verify event carrying
  the crate's own warning text;
- a directory input produces a verify event recording the skip;
- an error downgraded by `is_signature_error` produces a verify event naming it.

Each must fail if its event is removed — assert on the recorded event's presence
and content, not merely that the run succeeded.

## 6. Out of scope, with reasons

- **The default stays `DevOk`.** §2.
- **`runner-core`'s hardcoded `DevOk`** (`crates/runner-core/src/packs/mod.rs:251`)
  is untouched. Note for whoever picks it up: that call site is not as bare as
  the audit implies — when `cfg.public_key` is configured, `PackManager` performs
  a separate digest-signature check before caching (`packs/mod.rs:236-242`). The
  finding stands, but "no enforcement anywhere in runner-core" would overstate it.
- **`is_signature_error` is not removed.** It is provably not doing its stated
  job, so deleting it is tempting. But deleting it changes behaviour: errors it
  currently downgrades would start failing runs, and nobody knows how many there
  are — precisely because they have never been recorded. §3.3 records them; the
  removal becomes a decision backed by counts rather than reasoning.
- **`greentic-pack` is not changed.** Typing `PackVerifyResult` (today
  `{ message: String }`, built by flattening an `anyhow::Error` to a string) would
  let the runner classify errors properly instead of matching substrings. That is
  the real fix for §3.3, and it is a public-API change to a published crate — its
  own task, in its own repo.
- **SBOM verification state is not surfaced.** A pack with a valid signature but
  a failed SBOM check produces no verify event, and SBOM warnings from an invalid
  signature are folded into the `"unverified"` message. Intentional — this spec
  records signature state only — but calls out that the two concerns are not
  independently visible at the event level.
- **`record_verify_event` hardcodes an `error` object even for successful outcomes.**
  The event schema is optimized for the error case (built for `record_verify_event("error",
  &err.message)?` on the failing path). Under `DevOk` the common case is now
  `status: "unverified"` or `"skipped"`, written with a non-null `error` field.
  Nothing in this repo reads it as failure, but a public `TranscriptHook` will see
  events it never did before, and the field name will be misread elsewhere.

## 7. Known cost

Every dev run against an unpacked directory will now emit a `warn!` where it
previously emitted `debug!`. That is intended — the bypass is currently invisible
to the people best placed to notice it — but it is a real change in log noise for
the most common local workflow. If the directory noise proves too loud in practice,
the remedy is per-outcome, not a single-line change: all three outcomes
(`Loaded`, `DirectorySkipped`, `Downgraded`) share a single `warn!` call, so
lowering its log level would silence the important `Loaded` and `Downgraded` cases
along with the noisy `DirectorySkipped` one. A targeted fix would gate the
`DirectorySkipped` case to a lower level while keeping the event recorded.
