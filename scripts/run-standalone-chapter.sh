#!/usr/bin/env bash
# Runs the bash blocks of docs/getting-started/standalone-axum.md as written
# (issue #1614). The two previous defects in the example docs were both
# "the documented command does not work when pasted". This script pastes them.
#
# Each block that the script runs has a marker line before it:
#
#   <!-- chapter-run: serve -->         Start in the background. Wait for health.
#   <!-- chapter-run: expect TEXT -->   Run once. Exit 0. Output contains TEXT.
#   <!-- chapter-run: preflight -->     Run once. Exit 0 (pass) or 2 (warn).
#
# A block without a marker does not run, for example `docker compose up`.
# CI supplies the database as a service container instead.
# docs/audits/standalone-chapter-sync.py checks that the markers exist.
#
# Run it from a clean checkout with the chapter's Postgres up:
#
#   docker compose -f examples/standalone-quickstart/compose.yaml up -d
#   ./scripts/run-standalone-chapter.sh

set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

doc="docs/getting-started/standalone-axum.md"
health_url="http://localhost:3000/api/harvest/health"
health_timeout_secs="${CHAPTER_HEALTH_TIMEOUT_SECS:-600}"

if [ ! -f "$doc" ]; then
  echo "$doc: not found" >&2
  exit 1
fi

work="$(mktemp -d)"
serve_pid=""
cleanup() {
  if [ -n "$serve_pid" ]; then
    # The serve block runs in its own process group. Stop cargo and the app.
    kill -- "-$serve_pid" 2>/dev/null || true
  fi
  rm -rf "$work"
}
trap cleanup EXIT

# Write each marked block to $work/NN and its marker to $work/NN.kind.
awk -v dir="$work" '
  /^<!-- chapter-run: .* -->$/ {
    kind = $0
    sub(/^<!-- chapter-run: /, "", kind)
    sub(/ -->$/, "", kind)
    pending = 1
    next
  }
  pending && /^```bash$/ {
    n++
    file = sprintf("%s/%02d", dir, n)
    print kind > (file ".kind")
    close(file ".kind")
    inblock = 1
    pending = 0
    next
  }
  inblock && /^```$/ { inblock = 0; close(file); next }
  inblock { print > file }
' "$doc"

blocks=("$work"/[0-9][0-9])
if [ ! -f "${blocks[0]}" ]; then
  echo "$doc: no chapter-run blocks found" >&2
  exit 1
fi

wait_for_health() {
  for second in $(seq 1 "$health_timeout_secs"); do
    if curl -sf -o /dev/null "$health_url"; then
      echo "healthy after ${second}s"
      return 0
    fi
    if ! kill -0 "$serve_pid" 2>/dev/null; then
      echo "::error::the serve block exited before it became healthy" >&2
      cat "$work/serve.log" >&2
      return 1
    fi
    sleep 1
  done
  echo "::error::$health_url did not answer in ${health_timeout_secs}s" >&2
  cat "$work/serve.log" >&2
  return 1
}

for block in "${blocks[@]}"; do
  kind="$(cat "$block.kind")"
  echo "--- chapter-run: $kind"
  cat "$block"
  case "$kind" in
    serve)
      setsid bash "$block" >"$work/serve.log" 2>&1 &
      serve_pid=$!
      wait_for_health || exit 1
      ;;
    expect\ *)
      want="${kind#expect }"
      if ! output="$(bash -o pipefail "$block" 2>&1)"; then
        echo "$output"
        echo "::error::the block exited non-zero" >&2
        exit 1
      fi
      echo "$output"
      if ! grep -qF -- "$want" <<<"$output"; then
        echo "::error::the output does not contain '$want'" >&2
        exit 1
      fi
      ;;
    preflight)
      if output="$(bash -o pipefail "$block" 2>&1)"; then
        status=0
      else
        status=$?
      fi
      echo "$output"
      echo "exit=$status"
      if [ "$status" != "0" ] && [ "$status" != "2" ]; then
        echo "::error::preflight must pass (0) or warn (2), got $status" >&2
        exit 1
      fi
      ;;
    *)
      echo "::error::unknown chapter-run kind: $kind" >&2
      exit 1
      ;;
  esac
done

if [ -z "$serve_pid" ]; then
  echo "::error::$doc has no chapter-run: serve block" >&2
  exit 1
fi
echo "every chapter-run block ran as written"
