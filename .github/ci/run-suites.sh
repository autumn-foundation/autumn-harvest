#!/usr/bin/env bash
# Manifest-driven CI suite runner. Reads `.github/ci/integration-suites.txt` and
# executes (or `--no-run`-compiles) the per-suite `cargo test` invocations the
# `test` job used to spell out step-by-step. Adding a suite is one manifest line;
# this script and ci.yml never change.
#
# Portability: written for bash 3.2 (macOS system bash) and Git Bash on the
# Windows runner. NO associative arrays, NO mapfile/readarray, NO bash-4 syntax.
# Always invoked as `bash .github/ci/run-suites.sh …` with `shell: bash`, so it
# never depends on the exec bit or on CRLF (`.gitattributes` pins the manifest
# and this script to LF).
#
# Usage:
#   run-suites.sh run linux     # osclass=linux rows, serial (--test-threads=1)
#   run-suites.sh run linuxpart # osclass=linuxpart rows: the same live-DB shape
#                               # as `linux`, but with HARVEST_TEST_PARTITIONED=1
#                               # so the suite re-runs against the opt-in
#                               # partitioned harvest_events layout (issue #958).
#   run-suites.sh run allos     # osclass=allos rows, parallel, no live DB
#   run-suites.sh compile       # plugin rows (osclass linux|compileonly) --no-run
#
# Set DRY_RUN=1 to print the cargo commands instead of executing them.
#
# Sharding: set SEMAPHORE_SHARD_COUNT to an integer N and
# SEMAPHORE_SHARD_INDEX to 0..N-1 to have `run <linux|linuxpart|allos>` process
# only every Nth matching manifest row (row_ordinal % N == SHARD_INDEX,
# row_ordinal counted among rows matching that osclass, in manifest order).
# `--test-threads=1` WITHIN a row's own `cargo test` invocation is unaffected —
# sharding only changes which ROWS a given invocation of this script runs, not
# how a row's own tests are ordered against each other. Unset (the default):
# no filtering, identical to pre-sharding behavior. A CI job runs this script
# once per shard, in parallel, each with a different SEMAPHORE_SHARD_INDEX.
set -euo pipefail

MODE="${1:-}"
if [ -z "$MODE" ]; then
  echo "usage: run-suites.sh <run <linux|linuxpart|allos> | compile>" >&2
  exit 2
fi

DIR="$(cd "$(dirname "$0")" && pwd)"
MANIFEST="$DIR/integration-suites.txt"

fail=0
# Newline-separated list of failed suites (bash 3.2: no arrays needed here).
failed=""

# Emit non-comment, non-blank manifest records (the columnar data lines).
records() {
  grep -vE '^[[:space:]]*(#|$)' "$MANIFEST"
}

note_failure() {
  fail=1
  failed="${failed}  $1
"
}

# Execute (or, under DRY_RUN, print) a cargo invocation.  "$@" = cargo args.
run_cargo() {
  echo "::group::cargo $*"
  local rc=0
  if [ "${DRY_RUN:-}" = "1" ]; then
    echo "+ cargo $*"
  else
    cargo "$@" || rc=$?
  fi
  echo "::endgroup::"
  return "$rc"
}

# run <linux|allos>: one `cargo test` per matching record.
do_run() {
  local want="$1"
  local os crate target feats filter
  # Row ordinal among rows matching `$want`, for sharding (see header comment).
  # Incremented for every matching row regardless of whether sharding is
  # active, so the ordinal a row gets is independent of SEMAPHORE_SHARD_COUNT.
  local row_ordinal=0
  local shard_count="${SEMAPHORE_SHARD_COUNT:-}"
  local shard_index="${SEMAPHORE_SHARD_INDEX:-0}"
  # Process substitution (not a pipe) so `fail`/`failed` mutations persist in
  # this shell.
  while read -r os crate target feats filter || [ -n "$os" ]; do
    [ -n "$os" ] || continue
    [ "$os" = "$want" ] || continue

    if [ -n "$shard_count" ]; then
      local this_ordinal="$row_ordinal"
      row_ordinal=$(( row_ordinal + 1 ))
      [ "$(( this_ordinal % shard_count ))" -eq "$shard_index" ] || continue
    fi

    local a
    a=(test -p "$crate")
    # autumn-harvest has `default = ["db", "unified-dag-execution"]`. The `allos`
    # class means "no live DB", so for its `integration` aggregate target (which
    # connects to Postgres) we strip the defaults with `--no-default-features`;
    # each such row lists the features it actually needs. `linux` rows keep the
    # defaults (they run against Docker Postgres); non-`integration` allos targets
    # (e.g. `property`, pure functions) keep them too.
    if [ "$os" = "allos" ] && [ "$crate" = "autumn-harvest" ] && [ "$target" = "integration" ]; then
      a+=(--no-default-features)
    fi
    if [ "$feats" != "-" ]; then
      a+=(--features "$feats")
    fi
    a+=(--test "$target")

    # Post-separator (test-binary) args: the positional filter first, then
    # `--test-threads=1` (linux only, serial for shared Postgres). Both after
    # `--`, and `--test-threads=1` is emitted ONLY after any filter — correct
    # ordering by construction.
    local post
    post=()
    if [ "$filter" != "-" ]; then
      post+=("$filter")
    fi
    if [ "$os" = "linux" ] || [ "$os" = "linuxpart" ]; then
      post+=(--test-threads=1)
    fi
    if [ "${#post[@]}" -gt 0 ]; then
      a+=(-- "${post[@]}")
    fi

    # The failure label carries the FILTER. Without it every failing row of the
    # same (crate, target, osclass) prints an identical line — and the core rows
    # are all `autumn-harvest/integration (linux)`, so "3 suites failed" named
    # none of them. The per-suite `::group::` output that would identify them
    # is often outside the log window the API will return for a long job, so
    # the summary is the only place the name reliably survives.
    local label
    label="$crate/$target ($want)"
    if [ "$filter" != "-" ]; then
      label="$label -- $filter"
    fi

    # `linuxpart` is `linux` with the layout switch flipped: identical cargo
    # invocation, run against the opt-in partitioned `harvest_events` layout.
    # Exported only for this invocation so a mixed run cannot leak the flag into
    # the plain `linux` pass.
    if [ "$want" = "linuxpart" ]; then
      if ! HARVEST_TEST_PARTITIONED=1 run_cargo "${a[@]}"; then
        note_failure "$label"
      fi
    elif ! run_cargo "${a[@]}"; then
      note_failure "$label"
    fi
  done < <(records)
}

# compile: batch-compile every plugin row with osclass in {linux, compileonly}
# via `--no-run`, grouped by (crate, features). Core compile is the blanket core
# `--no-run` step in ci.yml, so core rows are intentionally excluded here.
do_compile() {
  # Emit "crate<TAB>features<TAB>target" for the relevant plugin rows, sorted so
  # adjacent lines with the same (crate, features) group together — the portable
  # (no associative array) way to batch.
  local grouped
  grouped="$(records | awk '
    ($1 == "linux" || $1 == "compileonly") && $2 == "autumn-harvest-plugin" {
      print $2 "\t" $4 "\t" $3
    }' | sort)"
  [ -n "$grouped" ] || return 0

  local prev_key="" cur_crate="" cur_feats="" targets=""
  local c f t
  while IFS="$(printf '\t')" read -r c f t; do
    [ -n "$c" ] || continue
    local key="$c|$f"
    if [ "$key" != "$prev_key" ]; then
      if [ -n "$prev_key" ]; then
        emit_compile "$cur_crate" "$cur_feats" "$targets"
      fi
      cur_crate="$c"
      cur_feats="$f"
      targets=""
      prev_key="$key"
    fi
    targets="$targets $t"
  done <<< "$grouped"
  # Flush the final group.
  if [ -n "$prev_key" ]; then
    emit_compile "$cur_crate" "$cur_feats" "$targets"
  fi
}

# emit_compile <crate> <features> <space-separated targets>
emit_compile() {
  local crate="$1" feats="$2" targets="$3"
  local a
  a=(test -p "$crate")
  if [ "$feats" != "-" ]; then
    a+=(--features "$feats")
  fi
  local t
  for t in $targets; do
    a+=(--test "$t")
  done
  a+=(--no-run)
  if ! run_cargo "${a[@]}"; then
    note_failure "compile $crate (${feats})"
  fi
}

case "$MODE" in
  run)
    WANT="${2:-}"
    case "$WANT" in
      linux | linuxpart | allos) do_run "$WANT" ;;
      *)
        echo "usage: run-suites.sh run <linux|linuxpart|allos>" >&2
        exit 2
        ;;
    esac
    ;;
  compile) do_compile ;;
  *)
    echo "bad mode: $MODE (expected: run <linux|linuxpart|allos> | compile)" >&2
    exit 2
    ;;
esac

if [ "$fail" -ne 0 ]; then
  printf 'FAILED SUITES:\n%s' "$failed" >&2
  exit 1
fi
