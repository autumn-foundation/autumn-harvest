#!/usr/bin/env bash
# Fails if docs/getting-started/13-broker-connectors.md's cutover example
# has drifted from the compiled copy of it in
# autumn-harvest-plugin/src/connector/binding.rs.
#
# Mechanism this guards against: the "Cutting a binding over to a new
# cluster or a recreated topic" section's example used to call
# `.map_json(...)` with the wrong closure signature (PR #1523). Patching a
# shell-script approximation of "does this closure type-check" -- arity,
# parameter types, return type, tail position -- kept acquiring new gaps
# across five rounds of review, because a markdown fence is never compiled
# anywhere else. There is no bound on how many ways a hand-rolled pattern
# match can be wrong about Rust; a real compiler has no such gap.
#
# The fix: the doc's example is now required to be byte-identical (module
# indentation aside) to the block between the `cutover-example-start` /
# `cutover-example-end` markers in
# `cutover_example_from_getting_started_ch13_type_checks`, an ordinary test
# in autumn-harvest-plugin/src/connector/binding.rs. That test is compiled
# and run by CI's `connector` lib-test job (`cargo test -p
# autumn-harvest-plugin --features connectors --lib connector`), so a real
# compile enforces the example's correctness. This script's only job is to
# catch the doc and the compiled copy drifting apart.
#
# Usage: ./scripts/check-broker-connector-cutover-example.sh

set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

doc="docs/getting-started/13-broker-connectors.md"
src="autumn-harvest-plugin/src/connector/binding.rs"

for f in "$doc" "$src"; do
  if [ ! -f "$f" ]; then
    echo "$f: not found" >&2
    exit 1
  fi
done

# The doc's example: its SourceBinding::starts(...) call is unique in the
# chapter, and .key_incarnation("2026-08-cutover") 6 lines below it closes
# the block.
doc_block="$(grep -A 6 -F 'SourceBinding::starts("orders", "orders", "order_flow")' "$doc")"

if [ -z "$doc_block" ]; then
  echo "$doc: could not find the cutover example's" \
    "SourceBinding::starts(\"orders\", \"orders\", \"order_flow\") call;" \
    "has the chapter been restructured? Update this guard (and the" \
    "compiled copy in $src) to match." >&2
  exit 1
fi

if ! grep -qF 'key_incarnation("2026-08-cutover")' <<<"$doc_block"; then
  echo "$doc: found SourceBinding::starts(\"orders\", \"orders\"," \
    "\"order_flow\") but not .key_incarnation(\"2026-08-cutover\") within" \
    "6 lines of it; has the cutover example moved or grown? Update this" \
    "guard (and the compiled copy in $src) to match." >&2
  exit 1
fi

# Pull the compiled copy from between the markers and dedent it: strip
# whatever leading whitespace every non-blank line shares, so the test
# function's extra indentation (nested in a fn, a let, and a block) does
# not itself count as a difference from the doc, which starts at column 0.
src_block="$(python3 -c '
import re
import sys

with open(sys.argv[1]) as f:
    text = f.read()

m = re.search(
    r"// cutover-example-start\n(.*?)\n[ \t]*// cutover-example-end",
    text,
    re.DOTALL,
)
if not m:
    sys.exit(1)

lines = m.group(1).split("\n")
indents = [len(l) - len(l.lstrip(" \t")) for l in lines if l.strip()]
strip = min(indents) if indents else 0
print("\n".join(l[strip:] if len(l) >= strip else l for l in lines))
' "$src")"

if [ -z "$src_block" ]; then
  echo "$src: could not find the" \
    "// cutover-example-start ... // cutover-example-end markers in" \
    "cutover_example_from_getting_started_ch13_type_checks; has the test" \
    "moved or been renamed? Update this guard to match." >&2
  exit 1
fi

if [ "$doc_block" != "$src_block" ]; then
  echo "$doc's cutover example has drifted from the compiled copy in" \
    "$src (between the cutover-example-start/end markers in" \
    "cutover_example_from_getting_started_ch13_type_checks). Only the" \
    "compiled copy is verified to type-check, so the two must read" \
    "identically (module indentation aside)." >&2
  echo >&2
  echo "--- $doc ---" >&2
  echo "$doc_block" >&2
  echo >&2
  echo "--- $src (dedented) ---" >&2
  echo "$src_block" >&2
  echo >&2
  echo "Fix: make one match the other, then re-run 'cargo test -p" \
    "autumn-harvest-plugin --features connectors --lib" \
    "connector::binding::tests::cutover_example_from_getting_started_ch13_type_checks'" \
    "to confirm it still compiles." >&2
  exit 1
fi

echo "OK: the cutover example matches its compiled copy in $src."
