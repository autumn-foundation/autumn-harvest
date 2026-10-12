#!/usr/bin/env bash
# Build and pack the TypeScript client from docs/openapi.json (issue #1616).
#
# No running app is necessary. CI and the release job both run this script.
# Progress goes to stderr. The only line on stdout is the tarball path, so a
# caller can capture it and still see every error in the log.
set -euo pipefail
exec 3>&1 1>&2

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${root}/clients/typescript"

# The registry is the one step here that fails for unrelated reasons. Each
# npm network call gets one retry. A type or test error gets none.
retry() {
  "$@" || { echo "retrying after a failure: $*"; sleep 10; "$@"; }
}

retry npm ci --no-audit --no-fund --loglevel=error
npm run typecheck
npm test
npm run build

rm -f autumn-harvest-client-*.tgz
tarball="$(npm pack --json --loglevel=error | node -e \
  'let s = ""; process.stdin.on("data", (c) => (s += c)).on("end", () => console.log(JSON.parse(s)[0].filename));')"

# The package must hold the build output and nothing from src or test.
listing="$(tar -tzf "${tarball}")"
# The package must also ship both license texts (issue #1990). npm adds a
# file named LICENSE-MIT only when `files` lists it.
for required in package/dist/index.js package/dist/index.d.ts package/dist/harvest-api.d.ts \
  package/LICENSE-MIT package/LICENSE-APACHE; do
  if ! grep -qxF "${required}" <<<"${listing}"; then
    echo "error: ${tarball} has no ${required}"
    exit 1
  fi
done
if grep -qE '^package/(src|test)/' <<<"${listing}"; then
  echo "error: ${tarball} holds source or test files"
  exit 1
fi

# Install the tarball into a clean consumer, then type-check and import it.
consumer="$(mktemp -d)"
trap 'rm -rf "${consumer}"' EXIT
cp consumer-smoke/index.ts consumer-smoke/tsconfig.json "${consumer}/"
echo '{ "name": "consumer-smoke", "private": true, "type": "module" }' > "${consumer}/package.json"
retry npm install --prefix "${consumer}" --no-audit --no-fund --loglevel=error "${PWD}/${tarball}"
"${PWD}/node_modules/.bin/tsc" -p "${consumer}/tsconfig.json"
(cd "${consumer}" && node --input-type=module -e \
  'const m = await import("autumn-harvest-client"); if (typeof m.createHarvestClient !== "function") process.exit(1);')

echo "${root}/clients/typescript/${tarball}" >&3
