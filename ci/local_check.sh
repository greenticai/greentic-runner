#!/usr/bin/env bash
set -euo pipefail

: "${CI:=0}"
: "${RUN_HOST:=never}"

RUST_TOOLCHAIN_VERSION="1.95.0"
echo "==> Local CI mirror (greentic-runner, rustc ${RUST_TOOLCHAIN_VERSION})"
export CARGO_TERM_COLOR=always
export CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse
export GREENTIC_PROVIDER_CORE_ONLY="${GREENTIC_PROVIDER_CORE_ONLY:-1}"

if [[ -z "${RUSTUP_TOOLCHAIN:-}" ]]; then
  export RUSTUP_TOOLCHAIN="$RUST_TOOLCHAIN_VERSION"
elif [[ "$RUSTUP_TOOLCHAIN" != "${RUST_TOOLCHAIN_VERSION}"* ]]; then
  echo "warning: RUSTUP_TOOLCHAIN=$RUSTUP_TOOLCHAIN differs from expected $RUST_TOOLCHAIN_VERSION" >&2
fi

# If you *really* want to prefetch in CI, do it unconditionally here:
# echo "==> Prefetching dependencies (cargo fetch --locked)"
# cargo fetch --locked

if [[ "${CI:-}" == "1" || "${CI:-}" == "true" ]]; then
  set -x
fi

if [[ -z "${LOCAL_CHECK_PACKAGE+x}" ]]; then
  if [[ "${CI:-}" == "1" || "${CI:-}" == "true" ]]; then
    LOCAL_CHECK_PACKAGE=0
  else
    LOCAL_CHECK_PACKAGE=1
  fi
fi

run_fmt() {
  echo "==> cargo fmt --check"
  cargo fmt --all --check
}

run_dependency_sanity() {
  echo "==> dependency sanity"

  local package="${LOCAL_CHECK_HOST_PACKAGE:-greentic-runner-host}"
  local versions=()
  mapfile -t versions < <(
    cargo tree -p "$package" --duplicates \
      | awk '/^wasmtime v/ { sub(/^v/, "", $2); print $2 }' \
      | sort -u
  )

  if (( ${#versions[@]} > 1 )); then
    echo "found multiple wasmtime versions in $package: ${versions[*]}" >&2
    echo "this usually means local workspace crates and published greentic-* crates are built against different wasmtime lines" >&2
    echo >&2
    for version in "${versions[@]}"; do
      echo "==> dependency paths for wasmtime@$version" >&2
      cargo tree -p "$package" --invert "wasmtime@$version" >&2
      echo >&2
    done
    echo "align the greentic-* dependency versions with the workspace wasmtime version before running clippy/tests" >&2
    exit 1
  fi
}

run_wit_sync() {
  echo "==> verify vendored extension-provider WIT matches pinned upstream revs"
  if ! command -v curl >/dev/null 2>&1; then
    echo "curl not found; skipping WIT sync check"
    return 0
  fi
  # The check fetches raw.githubusercontent.com. Offline (or upstream
  # unreachable) it is a soft-skip so the rest of local CI still runs; CI with
  # network enforces it.
  if ! ./scripts/verify-wit-sync.sh; then
    if [[ "${CI:-}" == "1" || "${CI:-}" == "true" ]]; then
      echo "WIT sync check failed in CI"
      return 1
    fi
    echo "WIT sync check failed (possibly offline); continuing local run"
  fi
}

run_clippy() {
  echo "==> cargo clippy (all targets, all features)"
  cargo clippy --all-targets --all-features -- -D warnings
}

run_host_smoke() {
  RUN_HOST="${RUN_HOST:-never}"
  if [[ "${RUN_HOST}" == "always" ]] || { [[ "${RUN_HOST}" == "auto" ]] && [[ -f "./examples/index.json" ]]; }; then
    echo "==> Running host smoke (with safe defaults)"
    export PACK_INDEX_URL="${PACK_INDEX_URL:-./examples/index.json}"
    export PACK_CACHE_DIR="${PACK_CACHE_DIR:-.packs}"
    export DEFAULT_TENANT="${DEFAULT_TENANT:-demo}"

    if ! cargo run -p greentic-runner -- --bindings examples/bindings/demo.yaml --port 0 --once; then
      echo "Host smoke exited non-zero; continuing with tests but failing at end"
      HOST_SMOKE_FAILED=1
    fi
  else
    echo "==> Skipping host smoke (no examples/index.json and RUN_HOST != always)"
  fi
}

run_crate_tests() {
  echo "==> crate tests"
  cargo test -p greentic-runner
  # The agentic-worker unit tests need `test-mock` (the mock LLM/billing/state
  # doubles live behind it), so a bare `cargo test -p greentic-aw-runtime`
  # silently reports 0 tests. `workspace_tests` covers them via --all-features,
  # but that variant also pulls RocksDB and is routinely skipped, which would
  # leave the credit-budget gate in `loop.rs` -- the only thing stopping an
  # empty wallet from spending on LLM calls -- with no gate in any cheap step.
  cargo test -p greentic-aw-runtime --features test-mock
}

run_workspace_tests() {
  echo "==> workspace tests"
  cargo test --workspace --all-targets --all-features
}

run_conformance() {
  if [[ "${RUN_CONFORMANCE:-0}" == "1" ]]; then
    echo "==> conformance harness"
    cargo run -p greentic-runner -- conformance --packs tests/fixtures/packs --level L1
  else
    echo "==> Skipping conformance (RUN_CONFORMANCE != 1)"
  fi
}

run_package() {
  if [[ "${LOCAL_CHECK_PACKAGE}" == "1" ]]; then
    echo "==> package dry-run (serialized)"
    if ! command -v jq >/dev/null 2>&1; then
      echo "jq not found; skipping package dry-run"
    else
      manifests=$(cargo metadata --no-deps --format-version=1 | jq -r '.packages[] | select(.publish != false and .publish != []) | .manifest_path')
      skipped_package=0
      while IFS= read -r manifest; do
        [[ -z "$manifest" ]] && continue
        crate_dir="$(dirname "$manifest")"
        pushd "$crate_dir" >/dev/null
        if ! cargo package --no-verify --allow-dirty --quiet; then
          echo "package failed for $crate_dir"
          popd >/dev/null
          exit 1
        fi
        popd >/dev/null
      done <<< "$manifests"
      if [[ "$skipped_package" -eq 1 ]]; then
        echo "Package dry-run unfinished due to offline mode; rerun with LOCAL_CHECK_ONLINE=1 to verify packaging"
      fi
    fi
  fi
}

default_steps=("fmt" "dependency_sanity" "wit_sync" "clippy" "host_smoke" "crate_tests" "workspace_tests" "conformance" "package")
if [[ -n "${LOCAL_CHECK_STEPS:-}" ]]; then
  steps_list="${LOCAL_CHECK_STEPS//,/ }"
  read -r -a steps <<< "$steps_list"
else
  steps=("${default_steps[@]}")
fi

for step in "${steps[@]}"; do
  case "$step" in
    fmt) run_fmt ;;
    dependency_sanity) run_dependency_sanity ;;
    wit_sync) run_wit_sync ;;
    clippy) run_clippy ;;
    host_smoke) run_host_smoke ;;
    crate_tests) run_crate_tests ;;
    workspace_tests) run_workspace_tests ;;
    conformance) run_conformance ;;
    package) run_package ;;
    *)
      echo "Unknown LOCAL_CHECK_STEPS entry: $step"
      exit 1
      ;;
  esac
done

if [[ "${HOST_SMOKE_FAILED:-0}" == "1" ]]; then
  echo "Host smoke failed (see log above)"
  exit 1
fi

echo "==> OK"
