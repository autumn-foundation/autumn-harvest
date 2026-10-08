#!/usr/bin/env bash
# Fails if the claude-agent-daemon example's default runtime files are not
# git-ignored at the repository root.
#
# Mechanism this guards against: examples/claude-agent-daemon/README.md's
# "Try it in one minute" section tells the reader to run every `agentd`
# command "from the repository root — Cargo needs the workspace manifest".
# The daemon's own defaults — `agentd.db` (examples/claude-agent-daemon/src/main.rs)
# and `agentd.sock` (examples/claude-agent-daemon/src/protocol.rs,
# DEFAULT_SOCKET) — write relative to the current directory, plus the SQLite
# WAL sidecars (`agentd.db-shm`, `agentd.db-wal`) and the socket's lock file
# (`agentd.sock.lock`, examples/claude-agent-daemon/src/guard.rs). Reproduced
# live: running the walkthrough's own `serve` step verbatim from a clean
# checkout's repository root leaves `git status --porcelain` non-empty.
#
# That is not just clutter. The doc says outright that the database "holds
# every prompt, tool input, and tool result, including the content of each
# file the agent read" — so the same walkthrough run against a real
# ANTHROPIC_API_KEY leaves an actual conversation transcript sitting as an
# untracked file, one `git add -A` away from landing in version control.
#
# Usage: ./scripts/check-agentd-runtime-artifacts-ignored.sh

set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

main_rs="examples/claude-agent-daemon/src/main.rs"
protocol_rs="examples/claude-agent-daemon/src/protocol.rs"

# Read the defaults from source rather than hard-coding them, so a renamed
# default fails this check instead of leaving it checking stale names while
# the daemon writes its runtime state to a new, unignored path.
db_default="$(sed -n 's/.*env = "AGENTD_DB", default_value = "\([^"]*\)".*/\1/p' "$main_rs")"
socket_default="$(sed -n 's/.*DEFAULT_SOCKET: &str = "\([^"]*\)";.*/\1/p' "$protocol_rs")"

if [ -z "$db_default" ]; then
  echo "$main_rs: could not find AGENTD_DB's default_value; has the --db" \
    "argument been restructured? Update this guard to match." >&2
  exit 1
fi

if [ -z "$socket_default" ]; then
  echo "$protocol_rs: could not find DEFAULT_SOCKET; has it been renamed or" \
    "moved? Update this guard to match." >&2
  exit 1
fi

missing=""

for name in "$db_default" "$db_default-shm" "$db_default-wal" "$socket_default" "$socket_default.lock"; do
  if ! git check-ignore -q -- "$name"; then
    missing="${missing}${name}\n"
  fi
done

if [ -n "$missing" ]; then
  echo "The following claude-agent-daemon runtime files are NOT git-ignored" \
    "at the repository root:" >&2
  printf '%b' "$missing" >&2
  echo >&2
  echo "Fix: add /agentd.db* and /agentd.sock* to .gitignore, so the" \
    "documented 'Try it in one minute' walkthrough (run from the" \
    "repository root) cannot leave the checkout dirty or park a" \
    "conversation transcript as an untracked file." >&2
  exit 1
fi

echo "OK: the claude-agent-daemon example's default runtime files are git-ignored."
