#!/usr/bin/env bash
# Fails if the snippets of chapter 7 (reliability knobs) stop compiling
# against the real crate.
#
# Mechanism this guards against: the chapter shows retry, timeout, SLA,
# continue-as-new and versioning code that a reader pastes verbatim, and
# nothing compiled it. Drift in a builder name or an attribute key then went
# unseen until a reader hit the compile error.
#
# The eleven ```rust blocks are extracted, wrapped with the imports and type
# stubs the chapter implies, and compiled. Nothing is run: the blocks need a
# live context. A `{ /* … */ }` body elides code in the chapter, so the wrapper
# swaps it for `todo!()`. Nothing else in a block is edited.
#
# Usage: ./scripts/check-reliability-knobs-examples.sh

set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

example_name="_onramp_doc_check_reliability_knobs"
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

ch7="docs/getting-started/07-reliability-knobs.md"
blocks=()
for i in 1 2 3 4 5 6 7 8 9 10 11; do
  b="$(rust_block "$ch7" "$i")"
  if [ -z "$b" ]; then
    echo "could not extract rust block $i of $ch7;" \
      "has the chapter been restructured? Update this guard." >&2
    exit 1
  fi
  blocks[$i]="${b//\/\* … \*\//todo!()}"
done
if [ -n "$(rust_block "$ch7" 12)" ]; then
  echo "$ch7 has a twelfth rust block; extend this guard." >&2
  exit 1
fi

{
  echo "#![allow(dead_code, unused)]"
  echo "use autumn_harvest::prelude::*;"
  echo "use autumn_harvest::{HarvestBuilder, WorkerConfig};"
  echo "use autumn_harvest::{AdaptiveLimitConfig, AdaptiveLimitPolicy};"
  echo "use serde::{Deserialize, Serialize};"
  echo "use std::time::Duration;"
  echo "#[derive(Serialize, Deserialize)] struct PaymentRequest;"
  echo "#[derive(Serialize, Deserialize)] struct PaymentId;"
  echo "#[derive(Serialize, Deserialize)] struct ApiRequest;"
  echo "#[derive(Serialize, Deserialize)] struct ApiResponse;"
  echo "#[derive(Serialize, Deserialize)] struct Input;"
  echo "#[derive(Serialize, Deserialize)] struct SubState;"
  echo "#[derive(Serialize, Deserialize)] struct SyncState;"
  echo "struct HttpClient;"
  echo "struct HttpResponse;"
  echo "struct HeaderValue;"
  echo "impl HeaderValue { fn to_str(&self) -> Result<&str, std::io::Error> { Ok(\"1\") } }"
  echo "impl HttpResponse {"
  echo "    fn status(&self) -> u16 { 200 }"
  echo "    fn headers(&self) -> std::collections::HashMap<String, HeaderValue> { Default::default() }"
  echo "    async fn json<T: Default>(self) -> Result<T, std::io::Error> { Ok(T::default()) }"
  echo "}"
  echo "impl HttpClient {"
  echo "    async fn post<T>(&self, _: &T) -> Result<HttpResponse, std::io::Error> { Ok(HttpResponse) }"
  echo "}"
  echo "impl Default for ApiResponse { fn default() -> Self { ApiResponse } }"
  echo "fn b1() -> WorkerConfig {"
  echo "${blocks[1]}"
  echo "}"
  echo "fn b2() -> WorkerConfig {"
  echo "${blocks[2]}"
  echo "}"
  echo "fn b3() -> WorkerConfig {"
  echo "${blocks[3]}"
  echo "}"
  for i in 4 5 6 7 8; do
    echo "mod block_$i { use super::*;"
    echo "${blocks[$i]}"
    echo "}"
  done
  echo "fn b9() {"
  echo "let _ = ${blocks[9]%.build();};"
  echo "}"
  echo "async fn b10(ctx: &WorkflowContext, input: serde_json::Value) -> HarvestResult<()> {"
  echo "${blocks[10]}"
  echo "Ok(())"
  echo "}"
  echo "fn b11(ctx: &WorkflowContext) {"
  echo "${blocks[11]}"
  echo "}"
  echo "fn main() {}"
} >"$example_path"

if output="$(cargo build --quiet -p autumn-harvest-plugin --example "$example_name" 2>&1)"; then
  echo "OK: chapter 7 snippets compile."
  exit 0
fi

errors="$(grep -c '^error\[' <<<"$output")"
echo "chapter 7 snippets fail ($errors compile error(s)):" >&2
echo "$output" >&2
exit 1
