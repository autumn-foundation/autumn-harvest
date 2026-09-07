#!/usr/bin/env bash
# Write both copies of the OpenAPI document from docs/api-contract.json.
#
#   autumn-harvest-plugin/openapi.json  compact, compiled in and served.
#   docs/openapi.json                   pretty-printed, for reading.
#
# `cargo test -p autumn-harvest-plugin --test openapi_spec` fails on drift.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${root}"

contract="docs/api-contract.json"
pretty="docs/openapi.json"
compact="autumn-harvest-plugin/openapi.json"

# The crate compiles in ${compact}, so the example cannot build without it.
# Seed it on a first run; the real content lands below.
[ -f "${compact}" ] || echo '{}' > "${compact}"

cargo build --quiet -p autumn-harvest-plugin --example emit_openapi
emit="${CARGO_TARGET_DIR:-target}/debug/examples/emit_openapi"

"${emit}" "${contract}"           > "${pretty}.tmp"
"${emit}" "${contract}" --compact > "${compact}.tmp"
mv "${pretty}.tmp" "${pretty}"
mv "${compact}.tmp" "${compact}"

echo "wrote ${pretty} and ${compact}"
