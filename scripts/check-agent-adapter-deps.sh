#!/usr/bin/env bash
# Fails if autumn-harvest-agent depends on an Autumn crate outside the engine.
#
# Issue #1973 requires the agent adapter to work on the core crate with no
# autumn-web. The adapter also owns its agent primitives, so it must not pull
# in an Autumn plugin either (autumn-plugin-agent, autumn-harvest-plugin).
# One changed dependency line would bring the web stack back, and no compile
# error would show it. This check reads the resolved graph instead.
#
# Usage: ./scripts/check-agent-adapter-deps.sh

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

graph="$(cargo tree --locked -p autumn-harvest-agent --all-features -e normal --prefix none)"

foreign="$(grep -E '^autumn-' <<<"$graph" \
    | grep -vE '^autumn-harvest(-agent|-macros|-sqlite)? ' \
    | sort -u || true)"

if [[ -n "$foreign" ]]; then
    echo "FAIL: autumn-harvest-agent depends on Autumn crates outside the engine:" >&2
    echo "$foreign" >&2
    exit 1
fi

if ! grep -q '^autumn-harvest ' <<<"$graph"; then
    echo "FAIL: autumn-harvest is missing from the graph; the check reads the wrong crate." >&2
    exit 1
fi

echo "OK: autumn-harvest-agent depends on no Autumn crate outside the engine."
