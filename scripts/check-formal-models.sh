#!/usr/bin/env bash
# Model-check every row of formal/tla/models.txt with TLC (issue #1819).
#
# A `pass` row must finish with no error. A `violation:<Inv>` row must stop
# with TLC exit code 12 (a safety violation) on that invariant.
#
# TLA2TOOLS_JAR names a local tla2tools.jar. Without it, the script
# downloads the pinned release and checks its SHA-256.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
manifest="${root}/formal/tla/models.txt"

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

failures=0
rows=0
while read -r spec config expect; do
  case "${spec}" in '' | '#'*) continue ;; esac
  rows=$((rows + 1))
  log="${work}/${config}.log"
  set +e
  (cd "${root}/formal/tla" &&
    java -XX:+UseParallelGC -cp "${jar}" tlc2.TLC \
      -config "${config}" -metadir "${work}/meta-${config}" \
      -workers auto -cleanup "${spec}.tla") >"${log}" 2>&1
  code=$?
  set -e

  verdict="ok"
  case "${expect}" in
    pass)
      if [ "${code}" -ne 0 ] || ! grep -q "No error has been found" "${log}"; then
        verdict="FAIL: want no error, TLC exit ${code}"
      fi
      ;;
    violation:*)
      inv="${expect#violation:}"
      if [ "${code}" -ne 12 ]; then
        verdict="FAIL: want a violation of ${inv}, TLC exit ${code}"
      elif ! grep -q "Invariant ${inv} is violated" "${log}"; then
        verdict="FAIL: TLC violated another invariant, not ${inv}"
      fi
      ;;
    *)
      verdict="FAIL: bad expect field '${expect}'"
      ;;
  esac

  echo "${spec} ${config} (${expect}): ${verdict}"
  if [ "${verdict}" != "ok" ]; then
    failures=$((failures + 1))
    cat "${log}"
  fi
done <"${manifest}"

if [ "${rows}" -eq 0 ]; then
  echo "no rows in ${manifest}" >&2
  exit 1
fi
if [ "${failures}" -ne 0 ]; then
  echo "${failures} of ${rows} model checks failed" >&2
  exit 1
fi
echo "all ${rows} model checks passed"
