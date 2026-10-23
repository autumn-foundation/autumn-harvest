#!/usr/bin/env bash
# Fails if the snippets of chapter 9 (worker routing) stop compiling against
# the real crate.
#
# Mechanism this guards against: the chapter shows code a reader pastes
# verbatim, and nothing compiled it, so drift in the `requires` grammar or in
# the `WorkerConfig` label and weight builders went unseen.
#
# The four ```rust blocks (activity `requires`, label map, `with_label`,
# `with_queue_weights`) are extracted, wrapped with the imports and type stubs
# the chapter implies, and checked with `cargo check`.
#
# Usage: ./scripts/check-worker-routing-examples.sh

set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

example_name="_onramp_doc_check_worker_routing"
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

ch9="docs/getting-started/09-worker-routing.md"
b1="$(rust_block "$ch9" 1)"
b2="$(rust_block "$ch9" 2)"
b3="$(rust_block "$ch9" 3)"
b4="$(rust_block "$ch9" 4)"

if [ -z "$b1" ] || [ -z "$b2" ] || [ -z "$b3" ] || [ -z "$b4" ]; then
  echo "could not extract the four rust blocks of $ch9;" \
    "has the chapter been restructured? Update this guard." >&2
  exit 1
fi

{
  echo "#![allow(dead_code, unused)]"
  echo "use autumn_harvest::prelude::*;"
  echo "use serde::{Deserialize, Serialize};"
  echo "#[derive(Serialize, Deserialize)] struct TranscodeInput { url: String }"
  echo "#[derive(Serialize, Deserialize, Default)] struct TranscodeOutput { bytes: u64 }"
  echo "mod activity_block { use super::*;"
  echo "$b1"
  echo "}"
  echo "fn labels_map() {"
  echo "$b2"
  echo "}"
  echo "fn labels_single() {"
  echo "$b3"
  echo "}"
  echo "fn weights() {"
  echo "let _ = $b4;"
  echo "}"
  echo "fn main() {}"
} >"$example_path"

if output="$(cargo check --quiet -p autumn-harvest-plugin --example "$example_name" 2>&1)"; then
  echo "OK: chapter 9 snippets compile."
  exit 0
fi

errors="$(grep -c '^error\[' <<<"$output")"
echo "chapter 9 snippets do not compile ($errors error(s)):" >&2
echo "$output" >&2
exit 1
