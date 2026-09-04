#!/usr/bin/env bash
# Fails if the "root" caveat for `cargo dev` is missing from either doc that
# promotes it as the fastest zero-setup path.
#
# Mechanism this guards against: README.md's "Quick example" and
# docs/getting-started/01-project-skeleton.md both bill `cargo dev` as
# needing "no database, no Docker, and nothing to configure" / "only the Rust
# toolchain installed". Neither used to mention that PostgreSQL — and so
# `cargo dev` itself, deliberately, per
# autumn-harvest-plugin/src/dev/postgres.rs's own doc comment — refuses to
# run as `root`. That is not a corner case: Docker devcontainers, many CI
# base images, and some cloud sandboxes default to a root shell, so a
# newcomer following either doc verbatim in one of those hits an
# undocumented hard stop on the very first command. Reproduced live against
# an unmodified checkout: `cargo dev` exits immediately with
# `DevError::RunningAsRoot` ("refusing to provision a cluster as root —
# PostgreSQL will not run as root, by design ..."), which is a real,
# deliberate refusal, not a bug — so the fix is documenting it, not removing
# it.
#
# Usage: ./scripts/check-dev-runtime-root-caveat.sh

set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

missing=""

for doc in README.md docs/getting-started/01-project-skeleton.md; do
  if ! grep -q 'refuses to run as `root`' "$doc"; then
    missing="${missing}${doc}\n"
  fi
done

if [ -n "$missing" ]; then
  echo "The following docs promote \`cargo dev\` without the root caveat:" >&2
  printf '%b' "$missing" >&2
  echo >&2
  echo "Fix: document that PostgreSQL — and so \`cargo dev\` — refuses to run" >&2
  echo "as root (autumn-harvest-plugin/src/dev/postgres.rs, refuse_to_run_as_root)," >&2
  echo "and point to the Postgres-in-its-own-container alternatives that are" >&2
  echo "unaffected (examples/quickstart, the bring-your-own-Postgres path)." >&2
  exit 1
fi

echo "OK: the cargo-dev root caveat is documented in both front-door docs."
