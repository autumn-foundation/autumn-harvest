#!/usr/bin/env bash
# Fails if docs/sqlite-backend.md's §7 "drive model" example regresses into a
# real compile failure: a `match rt.outcome(exec)? { ... }` that does not
# cover every `ExecutionOutcome` variant.
#
# Mechanism: the doc and `ExecutionOutcome::Terminated` were both introduced
# in the same commit (3abb5b6, issue #1348) -- the doc's own author added the
# variant to the enum but never added the matching arm to the guide's own
# `match`, so the snippet was broken from the day it was written. Neither
# `docs/audits/doc-snippet-syntax.py` (its corpus is `docs/getting-started/`
# and `README.md` only -- this file is outside it) nor `rustfmt` (a syntax
# check, not a compiler) can catch a non-exhaustive `match`: E0004 is a
# type-checker property, not a parse error. The crate's own
# `examples/quickstart.rs` already has the correct four-arm match, which is
# why CI's existing `cargo test -p autumn-harvest-sqlite` never surfaced
# this -- nothing ever compiled the doc's copy.
#
# This actually compiles the pinned block with `cargo check`, rather than
# counting match arms textually: `ExecutionOutcome` gaining or losing a
# variant is exactly the case a text-shape heuristic misses and a real
# compile catches for free.
#
# Usage: ./scripts/check-sqlite-outcome-match-example.sh

set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

doc="docs/sqlite-backend.md"
example_name="_onramp_doc_check_sqlite_outcome_match"
example_path="autumn-harvest-sqlite/examples/${example_name}.rs"

cleanup() {
  rm -f "$example_path"
}
trap cleanup EXIT
cleanup # self-heal if a previous run was killed before its own trap ran

if [ ! -f "$doc" ]; then
  echo "$doc: not found" >&2
  exit 1
fi

# From the RunState/ExecutionOutcome import through the fenced block's own
# close (exclusive) -- a generous window that survives either match growing
# or shrinking, since it stops at the next "```" line rather than a fixed
# line count.
extract_md_block() {
  awk '
    /^use autumn_harvest_sqlite::\{RunState, ExecutionOutcome\};$/ { p = 1 }
    p && /^```$/ { exit }
    p { print }
  ' "$doc"
}

code="$(extract_md_block)"
if [ -z "$code" ]; then
  echo "$doc: could not find the §7 drive-model example (looked for its" \
    "\"use autumn_harvest_sqlite::{RunState, ExecutionOutcome};\" import" \
    "line); has the chapter been restructured? Update this guard to" \
    "match." >&2
  exit 1
fi

# The doc's prose establishes `rt` (a running SqliteRuntime) and `exec` (an
# in-flight ExecutionId) in the sections above this one -- this stand-in
# registers a trivial workflow and starts it to supply both, matching the
# sibling guards' approach to chapter-local context.
{
  cat <<'RUST_EOF'
#![allow(clippy::all, clippy::pedantic, clippy::nursery, dead_code)]
use autumn_harvest::prelude::*;
use autumn_harvest_sqlite::SqliteRuntime;

#[workflow]
async fn onramp_check_workflow(_ctx: &WorkflowContext, _input: ()) -> Result<String, String> {
    Ok(String::new())
}

#[tokio::main]
async fn main() -> Result<(), autumn_harvest_sqlite::SqliteError> {
    let mut rt = SqliteRuntime::open_in_memory()?;
    rt.register_workflow(&onramp_check_workflow_info());
    let exec = rt.start_workflow("onramp_check_workflow", serde_json::json!(null))?;

RUST_EOF
  echo "$code"
  echo
  echo "    Ok(())"
  echo "}"
} >"$example_path"

if ! output="$(cargo check --quiet -p autumn-harvest-sqlite --example "$example_name" 2>&1)"; then
  echo "$doc's §7 drive-model example does not compile against the real" \
    "autumn-harvest-sqlite crate." >&2
  echo >&2
  echo "$output" >&2
  exit 1
fi

echo "OK: $doc's §7 drive-model example compiles against the real autumn-harvest-sqlite crate."
