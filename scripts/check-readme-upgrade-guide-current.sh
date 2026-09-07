#!/usr/bin/env bash
# Fails if README.md's "Upgrading an existing deployment?" pointer does not
# link to the upgrade guide for the current workspace version.
#
# Mechanism this guards against: docs/upgrading/ gains a new dated guide each
# release, and docs/getting-started/README.md's own "Where to go next" list
# is updated to name the new one — but README.md's top-level "Upgrading an
# existing deployment?" pointer is a separate, hand-written link that nothing
# keeps in sync with either. Reproduced live against the current tree: the
# workspace version is 0.6.0, docs/upgrading/0.6.0.md exists and is already
# the guide docs/getting-started/README.md calls "the current" one, yet
# README.md still pointed at docs/upgrading/0.5.0.md — a superseded 0.4.0 →
# 0.5.0 guide. A reader who follows the repository's own front door to
# upgrade an existing deployment would be sent to the wrong guide and could
# miss the release's defining change entirely (0.6.0's move of migration
# ownership to Autumn), rather than a cosmetic staleness.
#
# Usage: ./scripts/check-readme-upgrade-guide-current.sh

set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

version=$(grep -m1 '^version = "' Cargo.toml | sed -E 's/version = "([^"]*)"/\1/')
guide="docs/upgrading/${version}.md"

if [ -z "$version" ]; then
  echo "Could not read the workspace version from Cargo.toml." >&2
  exit 1
fi

if [ ! -f "$guide" ]; then
  echo "Workspace version is $version, but $guide does not exist." >&2
  echo "If this release genuinely needs no upgrade guide, update this" >&2
  echo "script's expectations instead of leaving it red." >&2
  exit 1
fi

# Scoped to the pointer's own paragraph (the anchor sentence through the
# next blank line), not the whole file — a correct link mentioned elsewhere
# in README.md (a historical aside, a release note) must not satisfy this
# check while the pointer sentence itself stays stale.
pointer_block=$(awk '/Upgrading an existing deployment\?/{flag=1} flag{print; if (/^$/) exit}' README.md)

if [ -z "$pointer_block" ]; then
  echo "Could not find the \"Upgrading an existing deployment?\" pointer in" >&2
  echo "README.md at all." >&2
  exit 1
fi

# The paragraph may name more than one guide (the current one, plus a
# pointer to the hop before it for readers further behind). Only the FIRST
# markdown link is the primary pointer readers are told to follow, so check
# that one specifically — a correct current-version link anywhere later in
# the paragraph must not paper over a stale primary link.
primary_link=$(grep -oE '\]\([^)]+\)' <<<"$pointer_block" | head -1 | sed -E 's/^\]\(([^)]+)\)$/\1/')

if [ "$primary_link" != "$guide" ]; then
  echo "README.md's \"Upgrading an existing deployment?\" pointer's primary" >&2
  echo "link does not point to $guide, the guide for the current workspace" >&2
  echo "version ($version)." >&2
  echo "Its primary link currently points to: ${primary_link:-<no markdown link found in that paragraph>}" >&2
  echo >&2
  echo "Fix: make the first link in that paragraph point at $guide." >&2
  exit 1
fi

echo "OK: README.md's upgrade pointer links to the current guide ($guide)."
