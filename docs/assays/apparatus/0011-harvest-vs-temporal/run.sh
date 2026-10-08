#!/usr/bin/env bash
# Runner for the Temporal arm of assay ledger #11.
#
# It exists for one reason: the harvest arm calls `reset_database` before
# EVERY repetition, so the Temporal arm has to start each repetition on an
# empty database too. Without that, repetition 2 runs against a database
# still holding repetition 1's executions and histories, table and index
# sizes grow across the arm, and the samples stop being matched to harvest's.
# Found by review on PR #1617.
#
# Temporal has no in-process reset, so a repetition is one whole process
# lifetime: drop the two databases, let auto-setup rebuild them, run ONE
# repetition, repeat.
#
# Written for bash 3.2 so it runs on a stock macOS shell as well as Linux.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"

REPS="${ASSAY11_REPS:-3}"
WORKFLOWS="${ASSAY11_WORKFLOWS:-2000}"
CAP="${ASSAY11_CAP_SECS:-900}"
IMAGE="${ASSAY11_TEMPORAL_IMAGE:-temporalio/auto-setup:1.25.2}"
CONTAINER="${ASSAY11_CONTAINER:-temporal-bench}"
ADMIN_URL="${ASSAY11_ADMIN_URL:-postgres://postgres@127.0.0.1:5432/postgres}"

reset_temporal() {
  docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
  psql "$ADMIN_URL" -q \
    -c 'DROP DATABASE IF EXISTS temporal WITH (FORCE)' \
    -c 'DROP DATABASE IF EXISTS temporal_visibility WITH (FORCE)'
  docker run -d --name "$CONTAINER" --network host \
    -e DB=postgres12 -e DB_PORT=5432 \
    -e POSTGRES_USER=postgres -e POSTGRES_PWD=postgres -e POSTGRES_SEEDS=127.0.0.1 \
    -e DEFAULT_NAMESPACE=default -e DEFAULT_NAMESPACE_RETENTION=24h \
    "$IMAGE" >/dev/null

  # Wait for the frontend to accept work rather than for the container to
  # exist. auto-setup creates the schema and the default namespace after the
  # container starts, and a repetition that begins before that is not a
  # measurement of a ready server.
  for _ in $(seq 1 120); do
    if docker logs "$CONTAINER" 2>&1 | grep -q "Starting_Worker\|Started Worker"; then
      break
    fi
    sleep 1
  done
  sleep 5
}

echo "# Assay #11 — Temporal arm, $REPS repetition(s)"
echo
for rep in $(seq 1 "$REPS"); do
  echo "## resetting Temporal persistence before repetition $rep"
  reset_temporal
  ASSAY11_WORKFLOWS="$WORKFLOWS" ASSAY11_REPS=1 ASSAY11_CAP_SECS="$CAP" \
    "$HERE/assay11"
  echo
done

docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
echo "==> Temporal container removed"
