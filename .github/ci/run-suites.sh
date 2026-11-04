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
# The live-DB CI job builds once and runs many times through cargo-nextest:
#   run-suites.sh archive <file>    # build every linux|linuxpart test binary
#                                   # into one nextest archive
#   run-suites.sh extract <file>    # unpack that archive into ./target
#   run-suites.sh nextest linux     # run the linux rows from ./target, serial
#   run-suites.sh nextest linuxpart # run the linuxpart rows the same way, with
#                                   # HARVEST_TEST_PARTITIONED=1
#   run-suites.sh filterset <class> # print the nextest filterset of a class
#
# Set DRY_RUN=1 to print the cargo commands instead of executing them.
# Set SUITES_MANIFEST to read another manifest. Only the runner tests do that.
#
# Sharding: set SEMAPHORE_SHARD_COUNT to an integer N and
# SEMAPHORE_SHARD_INDEX to 0..N-1 to have `run <linux|linuxpart|allos>` or
# `compile` process only every Nth matching row (row_ordinal % N ==
# SHARD_INDEX). For `run`, row_ordinal is counted among rows matching that
# osclass, in manifest order. For `compile`, row_ordinal is counted among the
# (crate, features, target) rows compile draws from (linux|compileonly plugin
# rows), in the same sorted order `do_compile` batches them by — filtering a
# sorted list by row ordinal yields a sorted subsequence, so rows sharing a
# (crate, features) key still land adjacent and batch into one `cargo`
# invocation exactly as they would unsharded, just with fewer targets in it.
# `--test-threads=1` WITHIN a row's own `cargo test` invocation is unaffected —
# sharding only changes which ROWS a given invocation of this script runs, not
# how a row's own tests are ordered against each other. Unset (the default):
# no filtering, identical to pre-sharding behavior. A CI job runs this script
# once per shard, in parallel, each with a different SEMAPHORE_SHARD_INDEX.
#
# `nextest` shards by test, not by row. It passes `--partition
# hash:<index+1>/<count>`, so each test lands on one shard by the hash of its
# name. A large suite then spreads over every shard. The row-ordinal scheme
# above keeps a whole suite on one shard, and the coverage nightly still uses it.
set -euo pipefail

MODE="${1:-}"
if [ -z "$MODE" ]; then
  echo "usage: run-suites.sh <run <linux|linuxpart|allos> | compile | archive <file> | extract <file> | nextest <linux|linuxpart> | filterset <osclass>>" >&2
  exit 2
fi

DIR="$(cd "$(dirname "$0")" && pwd)"
MANIFEST="${SUITES_MANIFEST:-$DIR/integration-suites.txt}"

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

# Execute (or, under DRY_RUN, print) a command.  "$@" = the command and its args.
run_cmd() {
  echo "::group::$*"
  local rc=0
  if [ "${DRY_RUN:-}" = "1" ]; then
    echo "+ $*"
  else
    "$@" || rc=$?
  fi
  echo "::endgroup::"
  return "$rc"
}

# Execute (or, under DRY_RUN, print) a cargo invocation.  "$@" = cargo args.
run_cargo() {
  run_cmd cargo "$@"
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

  # Sharding (see header comment): keep every Nth row, by ordinal in the
  # already-sorted list above. Same row_ordinal % shard_count == shard_index
  # scheme as do_run's, computed independent of whether sharding is active.
  local shard_count="${SEMAPHORE_SHARD_COUNT:-}"
  local shard_index="${SEMAPHORE_SHARD_INDEX:-0}"
  if [ -n "$shard_count" ]; then
    grouped="$(printf '%s\n' "$grouped" | awk -v n="$shard_count" -v i="$shard_index" '((NR - 1) % n) == i')"
    [ -n "$grouped" ] || return 0
  fi

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

# ── Build once, run many: the nextest modes ──────────────────────────────────

# The extracted archive lives where cargo builds, so a path that a test bakes
# in with `env!` resolves. `CARGO_BIN_EXE_*` and `CARGO_TARGET_TMPDIR` are two
# such paths. GitHub checks the repository out at the same absolute path in
# every job, so the build runner and each shard agree on that path.
NEXTEST_DIR="target/nextest"

# live_db_rows: the rows whose test binaries the archive must hold.
live_db_rows() {
  records | awk '$1 == "linux" || $1 == "linuxpart"'
}

# archive <file>: build every live-DB test binary into one nextest archive.
#
# One cargo invocation resolves features once. So the archive takes the union
# of the feature columns, each one qualified by its crate. Every feature in
# that union is additive, so a suite still compiles and runs under it.
#
# Debug info is off by default. A plugin test binary is about 1.4 GB with full
# debug info and about 130 MB without it. The archive holds over a hundred
# such binaries. A panic still reports its file and line without debug info.
do_archive() {
  local out="$1"
  local a
  a=(env
    "CARGO_PROFILE_DEV_DEBUG=${CARGO_PROFILE_DEV_DEBUG:-0}"
    "CARGO_PROFILE_TEST_DEBUG=${CARGO_PROFILE_TEST_DEBUG:-0}"
    cargo nextest archive --archive-file "$out")
  local crate target feats
  for crate in $(live_db_rows | awk '{ print $2 }' | LC_ALL=C sort -u); do
    a+=(-p "$crate")
  done
  for target in $(live_db_rows | awk '{ print $3 }' | LC_ALL=C sort -u); do
    a+=(--test "$target")
  done
  feats="$(live_db_rows | awk '
    $4 != "-" {
      n = split($4, f, ",")
      for (i = 1; i <= n; i++) print $2 "/" f[i]
    }' | LC_ALL=C sort -u | awk '{ s = (NR == 1 ? $0 : s "," $0) } END { print s }')"
  if [ -n "$feats" ]; then
    a+=(--features "$feats")
  fi
  if ! run_cmd "${a[@]}"; then
    note_failure "archive $out"
  fi
}

# extract <file>: unpack the archive into ./target and keep its metadata there.
#
# `binaries-only` lists the binaries without running them, so this step only
# unpacks. `--extract-overwrite` keeps the files already in the checkout.
do_extract() {
  local archive="$1"
  if ! run_cargo nextest list --archive-file "$archive" --workspace-remap . \
    --extract-to . --extract-overwrite --list-type binaries-only; then
    note_failure "extract $archive"
    return 0
  fi
  # `CARGO_TARGET_TMPDIR` names this directory, and cargo makes it only when
  # it builds. A test that writes a log there needs it to exist.
  if [ "${DRY_RUN:-}" != "1" ]; then
    mkdir -p target/tmp
  fi
}

# filterset <osclass>: one nextest filterset that selects the rows of a class.
#
# A row with no filter selects its whole binary. A row with a filter selects
# the tests whose name contains it. That is the substring match the positional
# filter of `cargo test` makes, so a row selects the same tests either way.
# A filter with any other character than [a-z0-9_:] is refused, because the
# filterset grammar would read it.
do_filterset() {
  records | awk -v want="$1" '
    $1 == want {
      if ($5 !~ /^(-|[a-z0-9_:]+)$/) { bad = bad " " $5; next }
      t = "binary_id(=" $2 "::" $3 ")"
      if ($5 != "-") t = "(" t " & test(" $5 "))"
      expr = (expr == "" ? t : expr " | " t)
    }
    END {
      if (bad != "") {
        print "run-suites.sh: unsupported manifest filter:" bad > "/dev/stderr"
        exit 1
      }
      print expr
    }'
}

# nextest <linux|linuxpart>: run the rows of a class from the extracted archive.
#
# Serial, as `run linux` is: the suites start Postgres containers, and the
# runner has room for one at a time. `--no-tests warn` lets a shard that the
# hash gives no test of a small class pass.
do_nextest() {
  local want="$1"
  local expr
  expr="$(do_filterset "$want")"
  [ -n "$expr" ] || return 0

  local a
  a=(env)
  if [ "$want" = "linuxpart" ]; then
    a+=(HARVEST_TEST_PARTITIONED=1)
  fi
  a+=(cargo nextest run
    --binaries-metadata "$NEXTEST_DIR/binaries-metadata.json"
    --cargo-metadata "$NEXTEST_DIR/cargo-metadata.json"
    --workspace-remap . --target-dir-remap target
    --test-threads 1 --no-fail-fast --no-tests warn
    -E "$expr")

  local shard_count="${SEMAPHORE_SHARD_COUNT:-}"
  local shard_index="${SEMAPHORE_SHARD_INDEX:-0}"
  if [ -n "$shard_count" ]; then
    a+=(--partition "hash:$(( shard_index + 1 ))/$shard_count")
  fi

  if ! run_cmd "${a[@]}"; then
    note_failure "nextest $want"
  fi
}

case "$MODE" in
  archive | extract)
    FILE="${2:-}"
    if [ -z "$FILE" ]; then
      echo "usage: run-suites.sh $MODE <archive-file>" >&2
      exit 2
    fi
    if [ "$MODE" = "archive" ]; then do_archive "$FILE"; else do_extract "$FILE"; fi
    ;;
  nextest)
    WANT="${2:-}"
    case "$WANT" in
      linux | linuxpart) do_nextest "$WANT" ;;
      *)
        echo "usage: run-suites.sh nextest <linux|linuxpart>" >&2
        exit 2
        ;;
    esac
    ;;
  filterset)
    WANT="${2:-}"
    if [ -z "$WANT" ]; then
      echo "usage: run-suites.sh filterset <osclass>" >&2
      exit 2
    fi
    do_filterset "$WANT"
    ;;
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
    echo "bad mode: $MODE (expected: run <linux|linuxpart|allos> | compile | archive <file> | extract <file> | nextest <linux|linuxpart> | filterset <osclass>)" >&2
    exit 2
    ;;
esac

if [ "$fail" -ne 0 ]; then
  printf 'FAILED SUITES:\n%s' "$failed" >&2
  exit 1
fi
