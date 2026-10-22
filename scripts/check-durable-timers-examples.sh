#!/usr/bin/env bash
# Fails if the flagship snippets of chapter 3 (durable timers) stop compiling
# against the real crate.
#
# Mechanism this guards against: the chapter shows code a reader pastes
# verbatim, and nothing compiled it, so drift in the timer API went unseen.
#
# The first three ```rust blocks (sleep timer, cancellable timer, business-day
# timer) are extracted, wrapped with the imports and type stubs the chapter
# implies, and checked with `cargo check`.
#
# Usage: ./scripts/check-durable-timers-examples.sh

set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

example_name="_onramp_doc_check_durable_timers"
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

ch3="docs/getting-started/03-durable-timers.md"
b1="$(rust_block "$ch3" 1)"
b2="$(rust_block "$ch3" 2)"
b3="$(rust_block "$ch3" 3)"

if [ -z "$b1" ] || [ -z "$b2" ] || [ -z "$b3" ]; then
  echo "could not extract the first three rust blocks of $ch3;" \
    "has the chapter been restructured? Update this guard." >&2
  exit 1
fi

{
  echo "#![allow(dead_code, unused)]"
  echo "use autumn_harvest::prelude::*;"
  echo "use serde::{Deserialize, Serialize};"
  echo "#[derive(Serialize, Deserialize)] struct Order { items: Vec<String>, shipped_early: bool }"
  echo "#[derive(Serialize, Deserialize)] struct Ticket { id: i64 }"
  echo "mod timers { use super::*;"
  echo "$b1"
  echo "$b2"
  echo "$b3"
  echo "}"
  echo "fn main() {}"
} >"$example_path"

if output="$(cargo check --quiet -p autumn-harvest-plugin --example "$example_name" 2>&1)"; then
  echo "OK: chapter 3 flagship snippets compile."
  exit 0
fi

errors="$(grep -c '^error\[' <<<"$output")"
echo "chapter 3 snippets do not compile ($errors error(s)):" >&2
echo "$output" >&2
exit 1
