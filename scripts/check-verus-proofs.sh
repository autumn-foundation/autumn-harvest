#!/usr/bin/env bash
# Verify every Verus proof in formal/verus.
#
# Verus checks the crate root formal/verus/src/lib.rs. It must report 0
# errors. The guard suite verus_proofs_coverage.rs checks that each
# mirrored function matches the engine source.
#
# VERUS names a local verus binary. Without it, the script downloads the
# pinned release and checks its SHA-256. The release names the Rust
# toolchain that it links against, and rustup installs that toolchain.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

verus_release="0.2026.10.12.4bfc2b4"
verus_sha256="af2aeac064df83fd8a6ea34cb98de75df3f477415e10bc177c9911853e057138"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

verus="${VERUS:-}"
if [ -z "${verus}" ]; then
  zip="${work}/verus.zip"
  curl -fsSL --retry 3 -o "${zip}" \
    "https://github.com/verus-lang/verus/releases/download/release%2Frolling%2F${verus_release}/verus-${verus_release}-x86-linux.zip"
  echo "${verus_sha256}  ${zip}" | sha256sum --check --quiet
  unzip -q "${zip}" -d "${work}"
  rm -f "${zip}"
  verus="${work}/verus-x86-linux/verus"
fi

# version.json names the toolchain as `<name> (<note>)`. The name is the
# first word.
toolchain="$(python3 -c 'import json, sys
print(json.load(open(sys.argv[1]))["verus"]["toolchain"].split()[0])' \
  "$(dirname "${verus}")/version.json")"
rustup toolchain install "${toolchain}" --profile minimal

log="${work}/verus.log"
set +e
"${verus}" "${root}/formal/verus/src/lib.rs" --crate-type=lib 2>&1 | tee "${log}"
code="${PIPESTATUS[0]}"
set -e

if [ "${code}" -ne 0 ]; then
  echo "verus exited ${code}" >&2
  exit 1
fi
if ! grep -qE "^verification results:: [1-9][0-9]* verified, 0 errors$" "${log}"; then
  echo "want: verification results:: <n> verified, 0 errors" >&2
  exit 1
fi
echo "all Verus proofs verified"
