#!/usr/bin/env bash
# Fails if README.md's "CI pattern" replay-safety snippet pins a
# `autumn-harvest` dev-dependency version that does not match the current
# workspace minor version.
#
# Mechanism this guards against: README.md's "Testing workflow code changes
# with the replayer" section tells a reader who just finished the
# getting-started chapters (where Chapter 1 correctly pins
# `autumn-harvest = "0.6"`) to add a *second*, hand-typed version requirement
# for the same crate as a dev-dependency. Nothing keeps that second pin in
# sync with the workspace version as releases ship, and Cargo's semver rule
# for a 0.x requirement is narrow: "0.2" resolves only within 0.2.x, it does
# not mean "0.2 or later". Reproduced live against the current tree: the
# workspace version is 0.6.0, yet README.md's snippet pinned
# `autumn-harvest = { version = "0.2", features = ["testing"] }` — four
# minor versions behind the `"0.6"` pin the reader already has in the same
# manifest from Chapter 1, and predating features documented two lines
# below it. Pasting both pins into one Cargo.toml gives the same crate two
# non-overlapping version requirements in [dependencies] and
# [dev-dependencies].
#
# Usage: ./scripts/check-readme-testing-dep-version.sh

set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

full_version=$(grep -m1 '^version = "' Cargo.toml | sed -E 's/version = "([^"]*)"/\1/')

if [ -z "$full_version" ]; then
  echo "Could not read the workspace version from Cargo.toml." >&2
  exit 1
fi

minor_version=$(echo "$full_version" | sed -E 's/^([0-9]+\.[0-9]+)\..*/\1/')

pin_line=$(grep -m1 'autumn-harvest = { version = "[^"]*", features = \["testing"\]' README.md)

if [ -z "$pin_line" ]; then
  echo "Could not find the testing-dependency pin (\"autumn-harvest = {" >&2
  echo "version = ..., features = [\"testing\"] }\") in README.md's CI" >&2
  echo "pattern snippet at all." >&2
  exit 1
fi

pinned_version=$(echo "$pin_line" | sed -E 's/.*version = "([^"]*)".*/\1/')

if [ "$pinned_version" != "$minor_version" ]; then
  echo "README.md's CI-pattern snippet pins autumn-harvest = \"$pinned_version\"" >&2
  echo "as a dev-dependency, but the current workspace version is" >&2
  echo "$full_version (minor $minor_version)." >&2
  echo >&2
  echo "Fix: update the pin in README.md to" >&2
  echo "  autumn-harvest = { version = \"$minor_version\", features = [\"testing\"] }" >&2
  exit 1
fi

echo "OK: README.md's testing-dependency pin matches the current workspace version ($minor_version)."
