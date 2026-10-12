#!/usr/bin/env bash
# Check engine traces against the TLA+ specs with TLC (issue #2003).
#
# Usage: scripts/check-formal-traces.sh <dir>...
#
# Each <dir> holds NDJSON traces. The header line of a trace names its spec
# and the result that each guard setting must give. The chaos suite writes
# traces to HARVEST_TLA_TRACE_DIR. formal/tla/trace/fixtures holds fixtures.
#
# TLA2TOOLS_JAR names a local tla2tools.jar. Without it, the script
# downloads the pinned release and checks its SHA-256.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [ "$#" -eq 0 ]; then
  echo "usage: $0 <trace dir>..." >&2
  exit 2
fi

tlc_version="1.7.4"
tlc_sha256="936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

jar="${TLA2TOOLS_JAR:-}"
if [ -z "${jar}" ]; then
  jar="${work}/tla2tools.jar"
  curl -fsSL --retry 3 -o "${jar}" \
    "https://github.com/tlaplus/tlaplus/releases/download/v${tlc_version}/tla2tools.jar"
  echo "${tlc_sha256}  ${jar}" | sha256sum --check --quiet
fi

python3 -I "${root}/scripts/formal_traces.py" \
  --jar "${jar}" --tla "${root}/formal/tla" --work "${work}/run" "$@"
