#!/usr/bin/env bash
# Clean-room reproduction of the documented quickstart journey
# (examples/quickstart/README.md, mirrored by the top-level README's
# "Try it end-to-end" line). Runs the exact commands a newcomer would copy,
# in order, from a pristine checkout — no shortcuts, no pre-warmed caches
# assumed. Logs step boundaries, wall-clock time, and exit status so the
# run is auditable and diffable across commits.
#
# Usage (from the repository root, on a machine with Docker + a Rust
# toolchain — this is literally what the docs ask a newcomer to have):
#
#   ./scripts/clean-room-quickstart.sh
#
# Exit code 0 means the documented golden path (start Postgres, start the
# app, trigger a workflow, watch it complete) succeeded end to end. Any
# other exit code names the failing step.
#
# Known non-goal: this script does NOT exercise `harvest preflight`
# end-to-end because that route sits behind the management API's
# session-based admin guard even in the dev profile (see
# autumn-harvest-plugin/src/api.rs, `has_harvest_admin_access`) — a
# deliberate fail-closed security posture, not a quickstart defect, and
# not something this script should silently work around. It only checks
# that the command *runs* (no ambiguous-binary failure), and reports the
# 401 as a known, separately tracked gap rather than a hard failure.

set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

COMPOSE_FILE="examples/quickstart/compose.yaml"
APP_LOG="$(mktemp)"
CLI_LOG="$(mktemp)"
STEP_LOG="$(mktemp)"
RUN_START=$(date +%s)
FAILED_STEP=""

step_start() {
  local name="$1"
  echo "=== [$( date -u +%H:%M:%S )] STEP: $name ===" | tee -a "$STEP_LOG"
  date +%s
}

step_end() {
  local name="$1" t0="$2" status="$3"
  local elapsed=$(( $(date +%s) - t0 ))
  echo "--- [$( date -u +%H:%M:%S )] $name: $status (${elapsed}s) ---" | tee -a "$STEP_LOG"
}

cleanup() {
  [ -n "${APP_PID:-}" ] && kill "$APP_PID" 2>/dev/null || true
  docker compose -f "$COMPOSE_FILE" down -v >/dev/null 2>&1 || true
  echo
  echo "=== Step log ==="
  cat "$STEP_LOG"
  echo
  echo "Total wall-clock: $(( $(date +%s) - RUN_START ))s"
  if [ -n "$FAILED_STEP" ]; then
    echo "RESULT: FAILED at '$FAILED_STEP'"
  else
    echo "RESULT: PASSED"
  fi
}
trap cleanup EXIT

fail() {
  FAILED_STEP="$1"
  echo "FAIL: $1" >&2
  exit 1
}

# Step 1 — start Postgres (examples/quickstart/README.md "Step 1")
t0=$(step_start "docker compose up")
docker compose -f "$COMPOSE_FILE" up -d || fail "docker compose up"
for _ in $(seq 1 30); do
  status="$(docker compose -f "$COMPOSE_FILE" ps --format '{{.Health}}' 2>/dev/null)"
  [ "$status" = "healthy" ] && break
  sleep 2
done
[ "$status" = "healthy" ] || fail "postgres never became healthy"
step_end "docker compose up" "$t0" "ok"

# Step 2 — start the app, verbatim command from the docs
t0=$(step_start "cargo run -p quickstart")
AUTUMN_MANIFEST_DIR=examples/quickstart AUTUMN_PROFILE=dev cargo run -p quickstart >"$APP_LOG" 2>&1 &
APP_PID=$!
ready=false
for _ in $(seq 1 240); do
  if ! kill -0 "$APP_PID" 2>/dev/null; then
    break
  fi
  if grep -q "could not determine which binary to run" "$APP_LOG"; then
    break
  fi
  if curl -sf -o /dev/null http://localhost:3000/api/harvest/health 2>/dev/null; then
    ready=true
    break
  fi
  sleep 1
done
if grep -q "could not determine which binary to run" "$APP_LOG"; then
  echo "cargo could not resolve a default binary for 'quickstart' — see [[bin]] targets in examples/quickstart/Cargo.toml" >&2
  fail "cargo run -p quickstart (ambiguous binary)"
fi
if [ "$ready" != "true" ]; then
  tail -n 60 "$APP_LOG" >&2
  fail "cargo run -p quickstart (never became healthy)"
fi
step_end "cargo run -p quickstart" "$t0" "ok (healthy)"

# Step 3 — preflight, verbatim command from the docs (best-effort; see the
# header comment on the known admin-auth gap — not this script's job to fix).
t0=$(step_start "cargo run -p autumn-harvest-cli -- preflight")
cargo run -p autumn-harvest-cli -- --base-url http://localhost:3000/api/harvest preflight >"$CLI_LOG" 2>&1
cli_status=$?
if grep -q "could not determine which binary to run" "$CLI_LOG"; then
  echo "cargo could not resolve a default binary for 'autumn-harvest-cli' — see [[bin]] targets in autumn-harvest-cli/Cargo.toml" >&2
  fail "cargo run -p autumn-harvest-cli (ambiguous binary)"
fi
if [ $cli_status -ne 0 ]; then
  echo "preflight exited $cli_status (expected while the admin route's session guard is unconfigured — tracked separately, not an ambiguous-binary failure)" | tee -a "$STEP_LOG"
fi
step_end "cargo run -p autumn-harvest-cli -- preflight" "$t0" "ran (exit $cli_status)"

# Step 4 — trigger a workflow execution, verbatim command from the docs
t0=$(step_start "trigger greeting workflow")
start_resp="$(curl -sf -X POST http://localhost:3000/api/harvest/workflows/greeting/start \
  -H 'Content-Type: application/json' \
  -d '{"workflow_id":"clean-room-demo","input":"World"}')" || fail "trigger workflow"
exec_id="$(echo "$start_resp" | python3 -c 'import json,sys; print(json.load(sys.stdin)["execution_id"])')"
step_end "trigger greeting workflow" "$t0" "ok ($exec_id)"

# Step 5 — observe completion (the 30s durable timer plus two activities)
t0=$(step_start "await workflow completion")
completed=false
for _ in $(seq 1 90); do
  state="$(curl -sf "http://localhost:3000/api/harvest/workflows/$exec_id" | python3 -c 'import json,sys; print(json.load(sys.stdin)["execution"]["state"])' 2>/dev/null)"
  if [ "$state" = "COMPLETED" ]; then
    completed=true
    break
  fi
  sleep 1
done
[ "$completed" = "true" ] || fail "workflow did not reach COMPLETED"
step_end "await workflow completion" "$t0" "ok (COMPLETED)"

echo "Clean-room quickstart journey succeeded."
