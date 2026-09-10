#!/usr/bin/env bash
# Fails if any ```bash fenced block in docs/README uses PowerShell's
# `$env:VAR = "value"` env-var assignment syntax.
#
# Mechanism this guards against: examples/standalone-runner/README.md's "Run"
# section fenced its command block ```bash — matching every sibling example
# (examples/quickstart, examples/billing-autumn-web) — but the two lines
# setting DATABASE_URL and AUTUMN_PROFILE used PowerShell's `$env:NAME =
# "value"` assignment instead of the `NAME=value` shape bash requires and
# every other example in the repository uses. Reproduced live: pasted
# verbatim into bash, each line prints
# `bash: line N: :NAME: command not found` and is silently skipped (no
# `set -e` in an interactive shell), so a reader sees two red error lines,
# has no reason to suspect them (the block is labeled bash and looks like an
# env-var assignment), and moves on with neither variable set. DATABASE_URL
# happens to fall back to the same value via a hardcoded default in
# examples/standalone-runner/src/server.rs, masking that half — but
# AUTUMN_PROFILE=dev not being set means the auto-migration branch is skipped,
# so the failure surfaces later and elsewhere: the harvest runner cannot find
# its tables against a database no migration ever ran.
#
# Usage: ./scripts/check-no-powershell-env-syntax.sh

set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

violations=0

while IFS=: read -r file line_no line; do
  echo "$file:$line_no: PowerShell env-var syntax '${line#*:}' in a bash" \
    "code block; bash requires 'NAME=value', not '\$env:NAME = \"value\"'." >&2
  violations=$((violations + 1))
done < <(grep -rnE '^\s*\$env:[A-Za-z_][A-Za-z0-9_]*\s*=' --include="*.md" . 2>/dev/null \
  | grep -vE "^\./target/")

if [ "$violations" -gt 0 ]; then
  echo >&2
  echo "Fix: use the shell's own 'NAME=value' assignment (inline before the" \
    "command, matching every other example in this repository)." >&2
  exit 1
fi

echo "OK: no PowerShell env-var assignment syntax in any documented bash block."
