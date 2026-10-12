#!/usr/bin/env bash
# Clean-room run of the getting-started path (Onramp harness).
#
# Starts from an empty directory. Pastes the Chapter 1 `Cargo.toml` and
# `src/main.rs` and the Chapter 2 workflow, activity and registration blocks
# from the docs, with no edits other than splicing the Chapter 2 blocks into
# the Chapter 1 file. Then runs `cargo check`. Every step logs its command,
# exit code and seconds to a TSV, followed by a summary line.
#
# Modes:
#   (default)      Resolve `autumn-harvest*` from crates.io, as a newcomer does.
#   --checkout     Patch `autumn-harvest*` to this checkout. Use it to test a
#                  docs or API change before release.
#
# Not covered: the Postgres steps (`docker compose up`, `curl`). They need a
# Docker daemon. `run-standalone-chapter.sh` covers that half for the
# standalone chapter.
#
# Usage: ./scripts/clean-room-getting-started.sh [--checkout] [--log FILE]
# Exit code: 0 when every step passes, 1 otherwise.
# For a pristine container: docker run --rm -v "$PWD":/src:ro -w /src \
#   rust:latest ./scripts/clean-room-getting-started.sh

set -uo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mode=crates-io
log=""
while [ $# -gt 0 ]; do
  case "$1" in
    --checkout) mode=checkout ;;
    --log) shift; log="${1:-}" ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
  shift
done

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
log="${log:-$work/steps.tsv}"
printf 'step\texit\tseconds\tcommand\n' > "$log"
steps=0
failures=0

# run_step NAME CMD...: run one step in $work/crate and record it.
run_step() {
  local name="$1"
  shift
  local start end code
  start=$(date +%s)
  (cd "$work/crate" && "$@" > "$work/$name.out" 2>&1)
  code=$?
  end=$(date +%s)
  steps=$((steps + 1))
  printf '%s\t%s\t%s\t%s\n' "$name" "$code" "$((end - start))" "$*" >> "$log"
  if [ "$code" != 0 ]; then
    failures=$((failures + 1))
    echo "--- FAIL: $name (exit $code)" >&2
    tail -30 "$work/$name.out" >&2
  fi
  return "$code"
}

mkdir -p "$work/crate/src"
ch1="$repo/docs/getting-started/01-project-skeleton.md"
ch2="$repo/docs/getting-started/02-first-workflow.md"

# Extract the pasted files. A missing block is a failed step, not a skip.
run_step extract python3 -I - "$ch1" "$ch2" "$work/crate" <<'PY'
import re, sys
ch1, ch2, out = sys.argv[1:4]
def blocks(path):
    return re.findall(r"```(\w+)\n(.*?)```", open(path).read(), re.S)
cargo = main = None
for _, b in blocks(ch1):
    if b.startswith("# Cargo.toml"):
        cargo = b
    if b.startswith("// src/main.rs"):
        main = b
rust2 = [b for l, b in blocks(ch2) if l == "rust"]
if not (cargo and main and len(rust2) >= 2):
    sys.exit("docs no longer contain the expected Cargo.toml/main.rs/Chapter 2 blocks")
workflow, register = rust2[0], rust2[1]
# Chapter 2 imports `Duration` and the prelude; Chapter 1 already has the prelude.
main = main.replace("use autumn_harvest_plugin::HarvestPlugin;",
                    "use std::time::Duration;\nuse autumn_harvest_plugin::HarvestPlugin;")
main, n = re.subn(r"HarvestPlugin::new\(\)\n.*?\.api\(\"/api/harvest\"\),\n",
                  register.strip() + ",\n", main, flags=re.S)
if n != 1:
    sys.exit("could not splice the Chapter 2 registration into Chapter 1 main.rs")
workflow = workflow.replace("use std::time::Duration;\n\nuse autumn_harvest::prelude::*;\n", "")
open(out + "/Cargo.toml", "w").write(cargo)
open(out + "/src/main.rs", "w").write(main + "\n" + workflow)
PY
ex=$?

if [ "$ex" = 0 ] && [ "$mode" = checkout ]; then
  {
    echo
    echo "[patch.crates-io]"
    for c in autumn-harvest autumn-harvest-plugin autumn-harvest-macros; do
      echo "$c = { path = \"$repo/$c\" }"
    done
  } >> "$work/crate/Cargo.toml"
fi

[ "$ex" = 0 ] && run_step cargo-check cargo check --quiet --message-format=short

total=$(awk -F'\t' 'NR>1 {s += $3} END {print s+0}' "$log")
echo "clean-room mode=$mode steps=$steps failures=$failures seconds=$total log=$log"
cat "$log"
[ "$failures" = 0 ]
