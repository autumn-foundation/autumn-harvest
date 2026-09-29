#!/usr/bin/env bash
# Fails if the flagship snippets of chapter 5 (child workflows) or chapter 6
# (idempotency) stop compiling against the real crate.
#
# Mechanism this guards against: both chapters show code a reader pastes
# verbatim, and both had drifted from the API.
#
#   * Chapter 5 called `spawn_child_workflow_raw(name, id, input)`. The real
#     method takes `(name, input)`. Result: error[E0061] "takes 2 arguments
#     but 3 arguments were supplied".
#   * Chapter 6's `charge_card` returned `HarvestResult<Value>` but ended a
#     chain with `.map_err(|e| e.to_string())?`. `HarvestError` has no
#     `From<String>`. Result: error[E0277] "`?` couldn't convert the error".
#
# Each chapter's first ```rust block is extracted, wrapped with the imports
# and stubs the chapter states or implies, and checked with `cargo check`.
#
# Usage: ./scripts/check-child-idempotency-examples.sh

set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

example_name="_onramp_doc_check_child_idempotency"
example_path="autumn-harvest-plugin/examples/${example_name}.rs"
trap 'rm -f "$example_path"' EXIT
rm -f "$example_path"

# Print the Nth (1-based) ```rust fence body of a file.
rust_block() {
  awk -v want="$2" '
    /^```rust$/ { n++; if (n == want) { p = 1; next } }
    p && /^```$/ { exit }
    p { print }
  ' "$1"
}

ch5="docs/getting-started/05-child-workflows.md"
ch6="docs/getting-started/06-idempotency.md"
b5="$(rust_block "$ch5" 1)"
b6="$(rust_block "$ch6" 1)"

if [ -z "$b5" ] || [ -z "$b6" ]; then
  echo "could not extract the first rust block of $ch5 or $ch6;" \
    "has a chapter been restructured? Update this guard." >&2
  exit 1
fi

{
  echo "#![allow(dead_code, unused)]"
  echo "use std::time::Duration;"
  echo "use autumn_harvest::prelude::*;"
  echo "mod ch5 { use super::*;"
  echo "$b5"
  echo "}"
  echo "mod ch6 { use super::*;"
  echo "async fn stripe_charge(_a: u64, _c: &str, _k: &str) -> Result<String, String> { Ok(String::new()) }"
  echo "$b6"
  echo "}"
  echo "fn main() {}"
} >"$example_path"

if output="$(cargo check --quiet -p autumn-harvest-plugin --example "$example_name" 2>&1)"; then
  echo "OK: chapter 5 and chapter 6 flagship snippets compile."
  exit 0
fi

errors="$(grep -c '^error\[' <<<"$output")"
echo "chapter 5/6 snippets do not compile ($errors error(s)):" >&2
echo "$output" >&2
exit 1
