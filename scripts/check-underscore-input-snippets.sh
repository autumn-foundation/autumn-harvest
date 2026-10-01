#!/usr/bin/env bash
# Fails if the chapter 8 `last_completion_result` snippet or the chapter 11
# `WorkflowTestEnv` billing-loop snippet stop compiling against the real crate.
#
# Mechanism this guards against: both snippets declare their workflow input as
# `_: ()`. `#[workflow]` builds its dispatch code from the input parameter
# *identifiers*, and `attr_util::param_idents` silently drops a `_` pattern. The
# generated call then passes one argument to a two-argument function, and rustc
# reports `error[E0061]: this function takes 5 arguments but 4 arguments were
# supplied ... argument #5 of type TypedStartOptions is missing` on the
# `#[workflow]` line. The message names neither `_` nor the fix.
#
# The chapter 11 snippet also passed a `Duration` to `ctx.timer`, which takes
# whole seconds (`u64`).
#
# Usage: ./scripts/check-underscore-input-snippets.sh

set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

example_name="_onramp_doc_check_underscore_input"
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

ch8="docs/getting-started/08-dags-and-schedules.md"
ch11="docs/getting-started/11-testing.md"
# The ch8 block is found by content, so an inserted earlier block cannot shift it.
b8="$(for n in $(seq 1 30); do
  blk="$(rust_block "$ch8" "$n")"
  [ -z "$blk" ] && break
  if grep -q 'async fn incremental_etl' <<<"$blk"; then echo "$blk"; break; fi
done)"
b11="$(rust_block "$ch11" 2)"

if [ -z "$b8" ] || ! grep -q 'async fn billing_cycle' <<<"$b11"; then
  echo "could not extract the ch8 incremental_etl block or the ch11 billing_cycle" \
    "block; has a chapter been restructured? Update this guard." >&2
  exit 1
fi

{
  echo "#![allow(dead_code, unused)]"
  echo "mod ch8 {"
  echo "$b8"
  echo "}"
  echo "mod ch11 {"
  echo "use autumn_harvest::testing::WorkflowTestEnv;"
  echo "$b11"
  echo "}"
  echo "fn main() {}"
} >"$example_path"

if output="$(cargo check --quiet -p autumn-harvest-plugin --example "$example_name" 2>&1)"; then
  echo "OK: chapter 8 and chapter 11 snippets compile."
  exit 0
fi

errors="$(grep -c '^error\[' <<<"$output")"
echo "chapter 8/11 snippets do not compile ($errors error(s)):" >&2
echo "$output" >&2
exit 1
