#!/usr/bin/env bash
# Fails if Chapter 1's `HARVEST_DEV_DATABASE_URL` examples promote the literal
# `postgres://me@localhost:5432/harvest_dev` DSN without explaining that `me`
# is a placeholder that must be replaced with a role that can actually
# authenticate.
#
# Mechanism this guards against: docs/getting-started/01-project-skeleton.md
# gives `HARVEST_DEV_DATABASE_URL=postgres://me@localhost:5432/harvest_dev
# cargo dev` as a directly-runnable command, twice, immediately after the
# zero-config `cargo dev` example. Nothing marks `me` as symbolic (no
# `<user>`-style brackets, no accompanying sentence), so it reads as
# copy-pasteable. Reproduced live against an unmodified checkout: run
# verbatim against an ordinary password-authenticated Postgres (the default
# for an apt/brew install or a fresh `initdb`, not a hypothetical), it fails
# with Postgres's own `fe_sendauth: no password supplied` — an error that
# never mentions `HARVEST_DEV_DATABASE_URL`, never says `me` needs to be
# replaced, and points nowhere. Swapping in real credentials
# (`postgres://postgres:postgres@localhost:5432/harvest_dev`) against the
# same unmodified checkout succeeds end-to-end: migrations apply, the worker
# registers, and the banner prints. So the defect is the undocumented
# placeholder, not the runtime.
#
# Usage: ./scripts/check-dev-database-url-placeholder-caveat.sh

set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

doc="docs/getting-started/01-project-skeleton.md"

if ! grep -q 'is a placeholder' "$doc"; then
  echo "$doc promotes the literal \`postgres://me@localhost:5432/harvest_dev\`" >&2
  echo "DSN without explaining that \`me\` is a placeholder for a role that can" >&2
  echo "actually authenticate." >&2
  echo >&2
  echo "Fix: add a caveat next to the example explaining that \`me\` must be" >&2
  echo "replaced with a real, authenticating role (peer, trust, or a password" >&2
  echo "in the DSN) — copied verbatim against an ordinary password-authenticated" >&2
  echo "Postgres it fails immediately with Postgres's own \"fe_sendauth: no" >&2
  echo "password supplied\", which names neither HARVEST_DEV_DATABASE_URL nor" >&2
  echo "this doc." >&2
  exit 1
fi

echo "OK: the HARVEST_DEV_DATABASE_URL placeholder caveat is documented."
