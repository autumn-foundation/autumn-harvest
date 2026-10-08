#!/usr/bin/env bash
# Fails if the legal and policy files are missing from the tree or from a
# published crate (issue #1990).
#
# The workspace declares `license = "MIT OR Apache-2.0"`. Corporate review
# rejects a crate that does not ship the license text. Cargo does not copy a
# root `LICENSE-*` file into a crate, so each crate directory holds a symlink
# to it.
#
# The script checks these things:
#
# - The root holds LICENSE-MIT, LICENSE-APACHE, SECURITY.md and CONTRIBUTING.md.
# - Each publishable workspace package declares `MIT OR Apache-2.0`.
# - `cargo package --list` for that package lists both license files.
# - The license files in the package directory match the root bytes. A copy
#   that drifts, or a symlink that a checkout turned into text, fails.
#
# The script reads the package list from `cargo metadata`. A new crate is
# checked with no edit here. A package with `publish = false` is skipped.
#
# Usage: ./scripts/check-license-files.sh

set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

LICENSE_FILES=(LICENSE-MIT LICENSE-APACHE)
ROOT_FILES=("${LICENSE_FILES[@]}" SECURITY.md CONTRIBUTING.md)
EXPECTED_LICENSE="MIT OR Apache-2.0"

failures=()

for f in "${ROOT_FILES[@]}"; do
  if [ ! -s "$f" ]; then
    failures+=("repository root: $f is missing or empty")
  fi
done

# Each line is "<name>\t<license>\t<package dir>". A failed pipeline in a
# command substitution only sets $?, so check it. Otherwise an empty list
# prints OK over a workspace that the script never read.
if ! packages="$(cargo metadata --no-deps --format-version=1 2>/dev/null | python3 -c '
import json, os, sys

data = json.load(sys.stdin)
members = set(data["workspace_members"])
for pkg in data["packages"]:
    if pkg["id"] not in members:
        continue
    # `publish = false` is an empty list. Null means any registry.
    if pkg.get("publish") == []:
        continue
    print("\t".join([
        pkg["name"],
        pkg.get("license") or "",
        os.path.dirname(pkg["manifest_path"]),
    ]))
')" || [ -z "$packages" ]; then
  echo "check-license-files.sh: cargo metadata or the parser failed, or found no publishable package. The check fails closed." >&2
  exit 1
fi

while IFS=$'\t' read -r name license dir; do
  if [ "$license" != "$EXPECTED_LICENSE" ]; then
    failures+=("$name: license is \"$license\", expected \"$EXPECTED_LICENSE\"")
  fi

  if ! listing="$(cargo package --list --allow-dirty -p "$name" 2>&1)"; then
    failures+=("$name: cargo package --list failed: $listing")
    continue
  fi

  for f in "${LICENSE_FILES[@]}"; do
    if ! grep -qxF "$f" <<<"$listing"; then
      failures+=("$name: cargo package --list does not show $f")
    elif ! cmp -s "$f" "$dir/$f"; then
      failures+=("$name: $dir/$f does not match the root $f")
    fi
  done
done <<<"$packages"

if [ "${#failures[@]}" -gt 0 ]; then
  echo "Legal or policy files are missing or wrong:" >&2
  printf '  - %s\n' "${failures[@]}" >&2
  echo >&2
  echo "Fix: add the root file. For a crate, link it from the crate directory:" >&2
  echo "  ln -s ../LICENSE-MIT <crate>/LICENSE-MIT" >&2
  echo "  ln -s ../LICENSE-APACHE <crate>/LICENSE-APACHE" >&2
  exit 1
fi

echo "OK: the root and every publishable crate ship LICENSE-MIT and LICENSE-APACHE."
