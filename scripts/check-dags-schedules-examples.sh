#!/usr/bin/env bash
# Fails if the snippets of chapter 8 (DAGs and schedules) stop compiling
# against the real crate.
#
# Mechanism this guards against: the chapter shows 15 ```rust blocks a reader
# pastes verbatim, and only a syntax check covers them. A renamed DagBuilder
# method or trigger rule goes unseen until a reader pastes the block.
#
# Each block gets its own module with the imports and stubs the prose implies.
# Blocks 3, 8 and 9 are fragments. The script wraps them in a function with
# the stubs they name. Blocks 11 and 14 are not compiled: they need a
# `refund` helper and a full plugin host.
#
# Usage: ./scripts/check-dags-schedules-examples.sh

set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

example_name="_onramp_doc_check_dags_schedules"
example_path="autumn-harvest-plugin/examples/${example_name}.rs"
trap 'rm -f "$example_path"' EXIT
rm -f "$example_path"

ch8="docs/getting-started/08-dags-and-schedules.md"
rust_block() {
  awk -v want="$2" '
    /^```rust$/ { n++; if (n == want) { p = 1; next } }
    p && /^```$/ { exit }
    p { print }
  ' "$1"
}
total="$(grep -c '^```rust$' "$ch8")"
if [ "$total" -ne 15 ]; then
  echo "$ch8 has $total rust blocks, expected 15;" \
    "update this guard to cover the new layout." >&2
  exit 1
fi

pre='use autumn_harvest::prelude::*;'
stub() { echo "#[activity(start_to_close = \"30s\")] async fn $1(_ctx: &ActivityContext, _i: Value) -> HarvestResult<Value> { Ok(Value::Null) }"; }
# Add the two imports the chapter leaves implicit, unless the block has them.
mod() {
  local extra=""
  grep -q 'use serde_json::Value;' <<<"$2" || extra+='use serde_json::Value; '
  grep -q 'use std::time::Duration;' <<<"$2" || extra+='use std::time::Duration; '
  printf 'mod %s {\n#![allow(dead_code, unused)]\n%s\n%s\n%s\n}\n' "$1" "$pre" "$extra" "$2"
}

{
  echo "#![allow(dead_code, unused)]"
  mod b1 "$(rust_block "$ch8" 1)"
  mod b2 "$(rust_block "$ch8" 2)
$(printf '#[dag] pub fn frag(dag: &mut DagBuilder) {\nlet list = dag.activity(list_partitions);\n%s;\n}' "$(rust_block "$ch8" 3)")"
  mod b4 "$(stub extract_users)$(stub load_warehouse)
$(rust_block "$ch8" 4)"
  mod b5 "$(rust_block "$ch8" 5)
$(stub low_risk_path)$(stub medium_risk_path)$(stub high_risk_path)
$(rust_block "$ch8" 6)"
  mod b7 "$(rust_block "$ch8" 7)"
  mod b8 "$(stub fetch_users)$(stub fetch_orders)$(stub join_report)
#[dag] pub fn frag(dag: &mut DagBuilder) {
$(rust_block "$ch8" 8)
}"
  mod b9 "$(stub root_step)$(stub maybe_step)$(stub finalize)
#[dag] pub fn frag(dag: &mut DagBuilder) {
let root = dag.activity(root_step);
$(rust_block "$ch8" 9)
}"
  mod b10 "$(stub reserve_inventory)$(stub release_inventory)$(stub charge_payment)$(stub refund_payment)
$(stub allocate_shipment)$(stub deallocate_shipment)$(stub print_label)$(stub void_label)$(stub notify_customer)
$(rust_block "$ch8" 10)"
  mod b12 "$(stub extract_order)$(stub load_order)
$(rust_block "$ch8" 12)"
  mod b13 "$(stub extract_order)$(stub load_order)$(stub escalate_to_manager)
use autumn_harvest::dag::GateTimeoutAction;
$(rust_block "$ch8" 13)"
  mod b15 "$(rust_block "$ch8" 15)"
  echo "fn main() {}"
} >"$example_path"

if output="$(cargo run --quiet -p autumn-harvest-plugin --example "$example_name" 2>&1)"; then
  echo "OK: chapter 8 snippets compile."
  exit 0
fi
errors="$(grep -c '^error' <<<"$output")"
echo "chapter 8 snippets fail ($errors compile error(s)):" >&2
echo "$output" >&2
exit 1
