#!/usr/bin/env bash
# Assert the crates.io publish lanes agree on which crates they publish.
#
# `release-binaries.yml` (tag `v*`), `crates-publish.yml` (tag
# `runnercrates-v*`) and `dev-publish.yml` (develop) all call the shared
# greenticai/.github publish workflow. The list is duplicated in the three
# files with nothing but this check keeping them in sync.
#
# When they drift, the damage is silent and only shows up mid-release: cargo
# strips `path` when packaging, so each crate is verified against its
# dependencies as resolved from crates.io. A workspace sibling missing from the
# list is resolved to whatever older version is already published, which builds
# fine until a release first has a dependent call a newly added API. That is
# exactly how v1.1.10 half-published (#655) — runner-core went out, then
# greentic-runner-host failed to verify against a stale greentic-aw-runtime.
#
# Order matters as much as membership: the shared workflow waits for index
# propagation between crates, so a dependency must appear before every crate
# that needs it. This compares the lists verbatim, order included.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
release="${repo_root}/.github/workflows/release-binaries.yml"
publish="${repo_root}/.github/workflows/crates-publish.yml"
dev="${repo_root}/.github/workflows/dev-publish.yml"

extract_crate_list() {
  local file="$1"
  local found
  found="$(grep -E '^[[:space:]]+crates:[[:space:]]*".*"[[:space:]]*$' "$file" || true)"
  if [[ -z "${found}" ]]; then
    echo "ERROR: no quoted \`crates:\` input found in ${file}" >&2
    echo "       The workflow shape changed; update this check." >&2
    return 1
  fi
  if [[ "$(printf '%s\n' "${found}" | wc -l)" -ne 1 ]]; then
    echo "ERROR: expected exactly one \`crates:\` input in ${file}, found:" >&2
    printf '%s\n' "${found}" >&2
    return 1
  fi
  # Collapse runs of whitespace so pure formatting never trips the comparison.
  printf '%s\n' "${found}" \
    | sed -E 's/^[[:space:]]*crates:[[:space:]]*"(.*)"[[:space:]]*$/\1/' \
    | tr -s '[:space:]' ' ' \
    | sed -E 's/^ //; s/ $//'
}

release_list="$(extract_crate_list "${release}")"
publish_list="$(extract_crate_list "${publish}")"
dev_list="$(extract_crate_list "${dev}")"

if [[ "${release_list}" != "${publish_list}" || "${release_list}" != "${dev_list}" ]]; then
  echo "ERROR: the crates.io publish lanes disagree on the crate list." >&2
  echo >&2
  echo "  release-binaries.yml: ${release_list}" >&2
  echo "  crates-publish.yml:   ${publish_list}" >&2
  echo "  dev-publish.yml:      ${dev_list}" >&2
  echo >&2
  echo "All must list the same crates in the same dependency order — a crate" >&2
  echo "missing from either lane half-publishes a release (see #655)." >&2
  exit 1
fi

echo "OK: all publish lanes agree on the crate list."
echo "    ${release_list}"
