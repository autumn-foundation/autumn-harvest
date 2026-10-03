#!/usr/bin/env bash
# Build and pack the TypeScript client from docs/openapi.json (issue #1616).
#
# No running app is necessary. CI and the release job both run this script.
# The last line of output is the path of the packed tarball.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${root}/clients/typescript"

npm ci --no-audit --no-fund
npm run generate
npm run typecheck
npm test
npm run build

rm -f autumn-harvest-client-*.tgz
tarball="$(npm pack --silent | tail -n 1)"

# The package must hold the build output and nothing from src or test.
listing="$(tar -tzf "${tarball}")"
for required in package/dist/index.js package/dist/index.d.ts package/dist/harvest-api.d.ts; do
  if ! grep -qx "${required}" <<<"${listing}"; then
    echo "error: ${tarball} has no ${required}" >&2
    exit 1
  fi
done
if grep -qE '^package/(src|test)/' <<<"${listing}"; then
  echo "error: ${tarball} holds source or test files" >&2
  exit 1
fi

# Install the tarball into a clean consumer, then type-check and import it.
consumer="$(mktemp -d)"
trap 'rm -rf "${consumer}"' EXIT
cp consumer-smoke/index.ts consumer-smoke/tsconfig.json "${consumer}/"
echo '{ "name": "consumer-smoke", "private": true, "type": "module" }' > "${consumer}/package.json"
npm install --prefix "${consumer}" --no-audit --no-fund --silent "${PWD}/${tarball}"
"${PWD}/node_modules/.bin/tsc" -p "${consumer}/tsconfig.json"
(cd "${consumer}" && node --input-type=module -e \
  'const m = await import("autumn-harvest-client"); if (typeof m.createHarvestClient !== "function") process.exit(1);')

echo "${root}/clients/typescript/${tarball}"
