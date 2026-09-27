#!/usr/bin/env bash
# Fails if CHANGELOG.md cites the wrong migration version for the broker
# connector's dead-letter table.
#
# Mechanism this guards against: the `[0.6.0]` "Add broker event-source
# connectors" entry (issue #944) names the poison-message sink as "the
# plugin-owned `harvest_connector_dead_letters` table (migration
# `20260719000000`)". That version is wrong: it is the version of a
# completely different, already-shipped core migration,
# `autumn-harvest/migrations/20260719000000_harvest_workflow_logs` (issue
# #790, cited correctly two entries below in the same CHANGELOG section).
# The connector migration's real, on-disk version is `20260719900000` —
# deliberately bumped by the `900000` time component, per that migration's
# own up.sql header, specifically so it would NOT collide with a core
# migration under Diesel's single, source-blind `__diesel_schema_migrations`
# keyspace (see docs/shipped-work.md for the collision this rename fixed).
# docs/upgrading/0.6.0.md sends an upgrading operator straight to
# CHANGELOG.md's `[0.6.0]` section to find this release's migrations; an
# operator who greps disk for the cited `20260719000000` to locate the
# connector migration finds `harvest_workflow_logs` instead, and either
# concludes the connector migration is missing or inspects the wrong table
# under the right feature's name.
#
# Usage: ./scripts/check-changelog-connector-migration-version.sh

set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

actual_dir=$(find autumn-harvest-plugin/migrations/harvest -maxdepth 1 -type d -name '*_harvest_connector_dead_letters' | head -1)

if [ -z "$actual_dir" ]; then
  echo "Could not find a harvest_connector_dead_letters migration directory" >&2
  echo "under autumn-harvest-plugin/migrations/harvest/." >&2
  exit 1
fi

actual_version=$(basename "$actual_dir" | sed -E 's/^([0-9]+)_.*/\1/')

cited_version=$(grep -oE 'harvest_connector_dead_letters. table \(migration .[0-9]+.\)' CHANGELOG.md | grep -oE '[0-9]{14}')

if [ -z "$cited_version" ]; then
  echo "Could not find CHANGELOG.md's migration-version citation for" >&2
  echo "harvest_connector_dead_letters at all." >&2
  exit 1
fi

if [ "$cited_version" != "$actual_version" ]; then
  echo "CHANGELOG.md cites migration \`$cited_version\` for" >&2
  echo "harvest_connector_dead_letters, but the migration directory on disk" >&2
  echo "is $actual_dir (version $actual_version)." >&2
  echo >&2
  echo "Fix: update CHANGELOG.md's citation to \`$actual_version\`." >&2
  exit 1
fi

echo "OK: CHANGELOG.md cites the correct harvest_connector_dead_letters migration version ($actual_version)."
