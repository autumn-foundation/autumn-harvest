#!/usr/bin/env bash
# Fails if autumn-web reaches the dependency graph of autumn-harvest-agent.
#
# Issue #1973 requires the agent adapter to work on the core crate with no
# autumn-web. The adapter takes autumn-plugin-agent with no default features,
# and that crate gates autumn-web behind its `autumn` feature. One changed
# feature flag would pull the whole web stack back in, and no compile error
# would show it. This check reads the resolved graph instead.
#
# Usage: ./scripts/check-agent-adapter-no-autumn-web.sh

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

graph="$(cargo tree --locked -p autumn-harvest-agent --all-features -e normal --prefix none)"

if grep -q '^autumn-web ' <<<"$graph"; then
    echo "FAIL: autumn-web is in the autumn-harvest-agent dependency graph:" >&2
    cargo tree --locked -p autumn-harvest-agent --all-features -e normal -i autumn-web >&2
    exit 1
fi

if ! grep -q '^autumn-plugin-agent ' <<<"$graph"; then
    echo "FAIL: autumn-plugin-agent is missing from the graph; the check reads the wrong crate." >&2
    exit 1
fi

echo "OK: autumn-harvest-agent has no autumn-web dependency."
