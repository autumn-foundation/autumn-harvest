#!/usr/bin/env bash
# Fails if any workspace package defines more than one `[[bin]]` target
# without a `default-run` in `[package]`.
#
# Mechanism this guards against: `cargo run -p <pkg>` (no `--bin`) is the
# form every README/quickstart/CLI doc uses. It only works when Cargo can
# pick exactly one binary. Adding a second `[[bin]]` to a package silently
# breaks every documented `cargo run -p <pkg>` invocation for that package —
# `cargo build -p <pkg>` still succeeds (it builds all bin targets), so
# nothing in a build-only CI job catches it. `examples/quickstart` and
# `autumn-harvest-cli` both regressed this way (issue: `cargo run -p
# autumn-harvest-cli -- ...`, the documented command in 50+ places including
# the quickstart's own "Step 3 — Run preflight", failed with "could not
# determine which binary to run" on trunk-dev).
#
# Usage: ./scripts/check-default-run.sh

set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

# `pipefail` alone doesn't abort the script on a failed pipeline inside a
# command substitution — it only sets $?, which nothing would check without
# this explicit `if !`. Without it, a `cargo metadata` or parser failure
# left `violations` empty and this script printed "OK" over a workspace it
# never actually inspected.
if ! violations="$(cargo metadata --no-deps --format-version=1 2>/dev/null | python3 -c '
import json, sys

data = json.load(sys.stdin)
bad = []
for pkg in data["packages"]:
    bins = [t["name"] for t in pkg["targets"] if "bin" in t["kind"]]
    if len(bins) > 1 and not pkg.get("default_run"):
        bad.append((pkg["name"], bins))

for name, bins in bad:
    print(f"{name}: bins={bins}")
')"; then
  echo "check-default-run.sh: failed to inspect the workspace (cargo metadata or the parser errored) — failing closed instead of reporting a false OK." >&2
  exit 1
fi

if [ -n "$violations" ]; then
  echo "The following packages define multiple [[bin]] targets with no default-run," >&2
  echo "so \`cargo run -p <pkg>\` (the form used throughout README.md and docs/) is" >&2
  echo "ambiguous and fails outright:" >&2
  echo "$violations" >&2
  echo >&2
  echo "Fix: add \`default-run = \"<primary-bin-name>\"\` under [package] in the" >&2
  echo "package's Cargo.toml." >&2
  exit 1
fi

echo "OK: every multi-binary package sets default-run."
