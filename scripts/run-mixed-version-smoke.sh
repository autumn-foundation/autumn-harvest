#!/usr/bin/env bash
# Mixed-version smoke (issue #1828). It proves the rolling-deploy contract in
# docs/upgrading/README.md for the current tree (N) and the previous release
# (N-1).
#
# Steps:
#   1. Build scripts/mixed-version-smoke against both trees.
#   2. Apply the migrations of N. Both versions then run on the schema of N.
#   3. Roll forward: N-1 starts the runs, N finishes them.
#   4. Roll back: N starts the runs, N-1 finishes them.
#   5. Mixed fleet: N-1 and N poll the same queue at the same time.
#   6. Steps 3 to 5 again, with a payload codec on both sides, as far as
#      N-1 supports it.
#
# In steps 3 and 4 the first worker stops while each run waits on a timer.
# The check then asserts which version ran each step of each run.
#
# Needs a Postgres database that it can reset, and git tags. Run it from a
# checkout with tags fetched:
#
#   DATABASE_URL=postgres://postgres:postgres@localhost:5432/mvsmoke \
#     ./scripts/run-mixed-version-smoke.sh
#
# Environment:
#   DATABASE_URL         required. The script drops and recreates its schema.
#   MV_SMOKE_PREVIOUS    the previous release tag. Default: the highest
#                        vX.Y.Z tag below the workspace version.
#   MV_SMOKE_RUNS        runs per scenario. Default: 6.
#   MV_SMOKE_TIMEOUT     seconds for each wait. Default: 180.

set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

: "${DATABASE_URL:?set DATABASE_URL to a database that this script may reset}"
runs="${MV_SMOKE_RUNS:-6}"
timeout="${MV_SMOKE_TIMEOUT:-180}"
work="$root/target/mixed-version-smoke"
export CARGO_TARGET_DIR="$work/build"
mkdir -p "$work/bin" "$work/logs"

version="$(grep -m1 '^version = "' Cargo.toml | sed -E 's/version = "([^"]*)"/\1/')"

# Print the highest release tag below version $1. Pre-release tags do not count.
previous_release() {
  git tag -l 'v[0-9]*' | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' \
    | { cat; echo "v$1"; } | sort -uV | grep -B1 -x "v$1" | head -n1 \
    | grep -vx "v$1" || true
}

previous="${MV_SMOKE_PREVIOUS:-$(previous_release "$version")}"
if [ -z "$previous" ]; then
  echo "no release tag below v$version; fetch tags or set MV_SMOKE_PREVIOUS" >&2
  exit 1
fi
echo "mixed-version smoke: N = v$version (this tree), N-1 = $previous"

# The adapter feature for the API of the previous release. See src/compat.rs.
case "$previous" in
  v0.6.*) features="v0_6" ;;
  *) features="" ;;
esac

pids=()
cleanup() {
  status=$?
  for pid in "${pids[@]}"; do
    kill -INT "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  if [ "$status" != "0" ]; then
    for log in "$work"/logs/*.log; do
      [ -f "$log" ] || continue
      echo "--- $log (last 60 lines)" >&2
      tail -n 60 "$log" >&2
    done
  fi
}
trap cleanup EXIT

# Build the smoke crate against tree $1 with features $2. Copy it to bin/$3.
build() {
  local tree="$1" feats="$2" label="$3" src="$1/target/mixed-version-smoke-src"
  rm -rf "$src"
  mkdir -p "$src"
  cp -R "$root/scripts/mixed-version-smoke/." "$src/"
  # Use the dependency versions that this tree shipped with.
  cp "$tree/Cargo.lock" "$src/Cargo.lock"
  echo "building $label from $tree"
  cargo build --manifest-path "$src/Cargo.toml" ${feats:+--features "$feats"}
  cp "$CARGO_TARGET_DIR/debug/mixed-version-smoke" "$work/bin/$label"
}

old_tree="$work/tree-$previous"
if [ ! -d "$old_tree" ]; then
  git worktree add --detach "$old_tree" "$previous"
fi
build "$root" "" current
build "$old_tree" "$features" previous

current="$work/bin/current"

echo "resetting the database schema"
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -q \
  -c 'DROP SCHEMA public CASCADE' -c 'CREATE SCHEMA public'

echo "applying the migrations of v$version"
cargo run -q -p autumn-harvest-cli -- migrate run \
  --database-url "$DATABASE_URL" \
  --include-dir autumn-harvest-plugin/migrations/harvest

# Start a worker of version $1 ("current" or "previous") in the background.
# Set the variable named $2 to its pid. $3 names the log.
start_worker() {
  local label="$1" log="$work/logs/$3-$1.log"
  MV_SMOKE_LABEL="$label" "$work/bin/$label" worker >"$log" 2>&1 &
  local pid=$!
  pids+=("$pid")
  printf -v "$2" '%s' "$pid"
  for _ in $(seq 1 60); do
    if grep -q "mixed-version-smoke worker ready" "$log"; then
      return 0
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "the $label worker exited before it was ready" >&2
      return 1
    fi
    sleep 1
  done
  echo "the $label worker was not ready after 60s" >&2
  return 1
}

# Stop the worker with pid $1. A graceful stop drains in-flight tasks.
stop_worker() {
  kill -INT "$1"
  wait "$1"
}

# Roll from version $1 to version $2 while runs wait on their timers.
roll() {
  local from="$1" to="$2" prefix="$3" from_pid to_pid
  echo "== $prefix: $from starts the runs, $to finishes them"
  start_worker "$from" from_pid "$prefix"
  "$work/bin/$from" start "$prefix" "$runs"
  "$current" wait-first "$prefix" "$runs" "$timeout"
  stop_worker "$from_pid"
  start_worker "$to" to_pid "$prefix"
  "$current" wait-done "$prefix" "$runs" "$timeout"
  "$current" check "$prefix" "$runs" "$from" "$to"
  stop_worker "$to_pid"
}

# Run both versions on one queue. Each version starts half of the runs.
mixed() {
  local prefix="$1" old_pid new_pid
  echo "== $prefix: both versions poll one queue"
  start_worker previous old_pid "$prefix"
  start_worker current new_pid "$prefix"
  "$work/bin/previous" start "$prefix-previous" "$runs"
  "$current" start "$prefix-current" "$runs"
  for p in "$prefix-previous" "$prefix-current"; do
    "$current" wait-done "$p" "$runs" "$timeout"
    "$current" check "$p" "$runs" '*' '*'
  done
  stop_worker "$old_pid"
  stop_worker "$new_pid"
}

# Run scenario $1 with the prefix $2.
scenario() {
  case "$1" in
    roll-forward) roll previous current "$2" ;;
    roll-back) roll current previous "$2" ;;
    mixed) mixed "$2" ;;
  esac
}

all="roll-forward roll-back mixed"
for name in $all; do
  scenario "$name" "$name"
done

# The same scenarios with a payload codec on both sides. The previous
# release limits them. See "Known limits" in docs/upgrading/README.md.
case "$previous" in
  # 0.6 workers do not apply a payload codec. They write plain payloads and
  # cannot read an envelope that 0.7 writes. So only a roll forward works.
  v0.6.*) codec_scenarios="roll-forward" ;;
  *) codec_scenarios="$all" ;;
esac
export MV_SMOKE_CODEC=1
for name in $all; do
  if [[ " $codec_scenarios " == *" $name "* ]]; then
    scenario "$name" "codec-$name"
  else
    echo "== codec-$name: not supported with $previous (see Known limits)"
  fi
done

echo "mixed-version smoke passed: v$version and $previous"
