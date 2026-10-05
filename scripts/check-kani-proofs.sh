#!/usr/bin/env bash
# Run every Kani proof in autumn-harvest and check the count (issue #1819).
#
# `cargo kani` exits 0 when it finds no proof, and when a `kani::cover!` is
# unsatisfiable. This script fails in both cases. It also fails when Kani
# verifies fewer proofs than the source holds, for example when a feature
# that gates a proof is missing.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${root}"

want="$(grep -rhxE '[[:space:]]*#\[kani::proof\]' autumn-harvest/src | wc -l)"
if [ "${want}" -eq 0 ]; then
  echo "no #[kani::proof] in autumn-harvest/src" >&2
  exit 1
fi

log="$(mktemp)"
trap 'rm -f "${log}"' EXIT

# `chaos` turns on the chaos controller, which holds a proof. `-Z stubbing`
# enables the `mix64` stub of the jitter proofs.
cargo kani -p autumn-harvest --no-default-features --features chaos -Z stubbing \
  2>&1 | tee "${log}"

summary="Complete - ${want} successfully verified harnesses, 0 failures, ${want} total."
if ! grep -qxF "${summary}" "${log}"; then
  echo "want: ${summary}" >&2
  exit 1
fi
if grep -E "^ \*\* [0-9]+ of [0-9]+ cover properties satisfied" "${log}" |
  awk '$2 != $4 { bad = 1 } END { exit !bad }'; then
  echo "a kani::cover! property is not satisfied" >&2
  exit 1
fi
echo "all ${want} Kani proofs verified"
