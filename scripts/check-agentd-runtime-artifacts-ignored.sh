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

missing=""

for name in agentd.db agentd.db-shm agentd.db-wal agentd.sock agentd.sock.lock; do
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
