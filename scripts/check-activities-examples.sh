#!/usr/bin/env bash
# Fails if the snippets of docs/getting-started/activities.md stop compiling
# against the real crate.
#
# Mechanism this guards against: the page shows 8 ```rust blocks a reader
# pastes verbatim, and nothing compiles them. A changed `ActivityContext`
# method or macro attribute goes unseen until a reader pastes the block.
#
# Each block gets its own module with the imports and stubs the prose implies.
# Block 6 is a builder fragment that needs a full interceptor impl. The
# script does not compile it.
#
# Usage: ./scripts/check-activities-examples.sh

set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

example_name="_onramp_doc_check_activities"
example_path="autumn-harvest-plugin/examples/${example_name}.rs"
trap 'rm -f "$example_path"' EXIT
rm -f "$example_path"

page="docs/getting-started/activities.md"
rust_block() {
  awk -v want="$2" '
    /^```rust$/ { n++; if (n == want) { p = 1; next } }
    p && /^```$/ { exit }
    p { print }
  ' "$1"
}
total="$(grep -c '^```rust$' "$page")"
if [ "$total" -ne 8 ]; then
  echo "$page has $total rust blocks, expected 8;" \
    "update this guard to cover the new layout." >&2
  exit 1
fi

pre='use autumn_harvest::prelude::*;'
# Add the prelude unless the block imports it.
mod() {
  local extra=""
  grep -q 'use autumn_harvest::prelude::\*;' <<<"$2" || extra+="$pre "
  printf 'mod %s {\n#![allow(dead_code, unused)]\n%s\n%s\n}\n' "$1" "$extra" "$2"
}

{
  echo "#![allow(dead_code, unused)]"
  mod b1 "$(rust_block "$page" 1)"
  mod b2 "pub struct Rec { pub id: u64 }
fn fetch_records(_u: &str, _o: u64) -> Vec<Rec> { vec![] }
fn write_record(_r: &Rec) -> Result<(), std::io::Error> { Ok(()) }
$(rust_block "$page" 2)"
  mod b3 "#[derive(serde::Serialize, serde::Deserialize)] pub struct Job;
async fn run_long_blocking_work(_j: &Job) -> Result<(), std::io::Error> { Ok(()) }
$(rust_block "$page" 3)"
  mod b4 "pub struct Item { pub id: u64 }
fn load_batch(_j: &str) -> Vec<Item> { vec![] }
fn process_item(_i: &Item) -> Result<(), std::io::Error> { Ok(()) }
$(rust_block "$page" 4)"
  mod b5 "async fn check_status(_t: &str) -> Result<String, std::io::Error> { Ok(String::new()) }
$(rust_block "$page" 5)"
  mod b7 "$(rust_block "$page" 7)"
  mod b8 "$(rust_block "$page" 8)"
  echo "fn main() {}"
} >"$example_path"

if output="$(cargo run --quiet -p autumn-harvest-plugin --example "$example_name" 2>&1)"; then
  echo "OK: activities.md snippets compile."
  exit 0
fi
errors="$(grep -c '^error' <<<"$output")"
echo "activities.md snippets fail ($errors compile error(s)):" >&2
echo "$output" >&2
exit 1
