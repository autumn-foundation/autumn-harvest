#!/usr/bin/env bash
# Fails if a license or policy file is missing from the repository root, or a
# license file is missing from a publishable package (issue #1990).
#
# The workspace declares `license = "MIT OR Apache-2.0"`. Corporate review
# rejects a crate that does not ship the license text. Cargo does not copy a
# root `LICENSE-*` file into a crate, so each crate directory holds a symlink
# to it.
#
# The script checks these things:
#
# - The root holds LICENSE-MIT, LICENSE-APACHE, SECURITY.md and CONTRIBUTING.md.
# - Each publishable workspace crate declares `MIT OR Apache-2.0`.
# - `cargo package --list` for that crate lists both license files.
# - The license files in the crate directory match the root, byte for byte.
#   A copy with different bytes fails. A symlink that a checkout turned into a
#   text file also fails.
# - The TypeScript client holds copies of both license files with the root
#   bytes. npm does not pack a symlink. `scripts/build-typescript-client.sh`
#   checks that the tarball holds them.
#
# The script reads the crate list from `cargo metadata`. It checks a new crate
# with no edit. It skips a crate with `publish = false`.
#
# Usage: ./scripts/check-license-files.sh

set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

LICENSE_FILES=(LICENSE-MIT LICENSE-APACHE)
ROOT_FILES=("${LICENSE_FILES[@]}" SECURITY.md CONTRIBUTING.md)
EXPECTED_LICENSE="MIT OR Apache-2.0"
NPM_PACKAGE_DIR=clients/typescript
# The unit separator is not whitespace, so `read` keeps an empty field.
SEP=$'\x1f'

failures=()

for f in "${ROOT_FILES[@]}"; do
  if [ ! -s "$f" ]; then
    failures+=("repository root: $f is missing or empty")
  fi
done

for f in "${LICENSE_FILES[@]}"; do
  if ! cmp -s "$f" "$NPM_PACKAGE_DIR/$f"; then
    failures+=("$NPM_PACKAGE_DIR: $f is missing or does not match the root $f")
  fi
done

# Each line is "<name> SEP <license> SEP <crate dir>". A failed pipeline in a
# command substitution only sets $?, so check it. Otherwise the script prints
# OK for a workspace that it never read.
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
    print("\x1f".join([
        pkg["name"],
        pkg.get("license") or "",
        os.path.dirname(pkg["manifest_path"]),
    ]))
')" || [ -z "$packages" ]; then
  echo "check-license-files.sh: cargo metadata or the parser failed, or the workspace has no publishable crate. The check fails closed." >&2
  exit 1
fi

while IFS="$SEP" read -r name license dir; do
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
  echo "Fix: add the missing root file. In a crate directory, add a relative" >&2
  echo "symlink to each root license file, for example:" >&2
  echo "  ln -s ../LICENSE-MIT autumn-harvest/LICENSE-MIT" >&2
  echo "In $NPM_PACKAGE_DIR, copy the root license files. Set the crate" >&2
  echo "license to \"$EXPECTED_LICENSE\"." >&2
  exit 1
fi

echo "OK: the root holds the license and policy files. Each publishable crate and the TypeScript client ship both license texts."
