#!/usr/bin/env bash
# Fails if chapter 10's "Catching a forgotten registration before rollout"
# example (the flagship #[workflow(activities = [...], children = [...])]
# snippet) regresses into either of two real compile failures an Onramp
# clean-room pass found:
#
# 1. `ctx.execute_activity(&send_email_info(), user_id).await?` and the
#    `spawn_child_workflow` call below it are `HarvestResult<_>`
#    (`Result<_, HarvestError>`), but the workflow returns
#    `Result<_, String>`. There is no `From<HarvestError> for String`, so
#    `?` needs a real conversion first -- E0277 otherwise. Same defect
#    class as chapter 7's continue_as_new example (PR #1700).
# 2. Even with `.map_err(|e| e.to_string())` added, `execute_activity`'s
#    output type `O` is generic and unconstrained when its result is
#    discarded as a bare statement (unlike `continue_as_new`, whose return
#    type is the concrete `HarvestResult<()>`). Current stable rustc denies
#    that by default (`dependency_on_unit_never_type_fallback`, part of
#    `rust_2024_compatibility`) on every edition, not just 2024 --
#    confirmed live in a standalone edition-2021 crate depending on this
#    workspace's autumn-harvest by path, matching chapter 1's own pinned
#    `edition = "2021"` tutorial setup. Fixed with `execute_activity::<_,
#    serde_json::Value>(...)`, NOT the `()` rustc itself suggests as a
#    generic fallback: `ActivityInfo` is not generic, so `execute_activity`
#    never actually checks that `O` matches what the named activity
#    returns (review on this PR: #1708) -- `charge_card` here is a stand-in,
#    but the identically-named real activity in
#    docs/getting-started/06-idempotency.md returns
#    `HarvestResult<serde_json::Value>`, a JSON object. Pinning `O = ()`
#    would still compile, then fail *at runtime* with
#    `HarvestError::Serialization` the moment a reader wires this example
#    up against that real activity -- a worse failure than the compile
#    error this guard exists to catch. `serde_json::Value` is universally
#    deserializable, so it is the one annotation safe for any activity's
#    real output shape.
#
# Neither is a syntax error, so doc-snippet-syntax.py's rustfmt-only check
# passes this block clean (it only checks parse validity, never compiles).
# This actually compiles the pinned block with `cargo check` rather than
# pattern-matching stand-in text, matching the sibling guards in this
# directory -- whether a `?` conversion or a generic output type actually
# resolves is a type-checker property, not a text shape.
#
# Usage: ./scripts/check-preflight-declared-deps-example.sh

set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

doc="docs/getting-started/10-operations.md"
example_name="_onramp_doc_check_preflight_declared_deps"
example_path="autumn-harvest/examples/${example_name}.rs"

cleanup() {
  rm -f "$example_path"
}
trap cleanup EXIT
cleanup # self-heal if a previous run was killed before its own trap ran

if [ ! -f "$doc" ]; then
  echo "$doc: not found" >&2
  exit 1
fi

# From the attribute line through the fenced block's own close (exclusive)
# -- a generous window that survives the body growing or shrinking, since
# it stops at the next "```" line rather than a fixed line count.
extract_md_block() {
  awk '
    /^#\[workflow\(activities = \[send_email, charge_card\], children = \[generate_report\]\)\]$/ { p = 1 }
    p && /^```$/ { exit }
    p { print }
  ' "$doc"
}

code="$(extract_md_block)"
if [ -z "$code" ]; then
  echo "$doc: could not find the \"Catching a forgotten registration" \
    "before rollout\" example (looked for its" \
    "#[workflow(activities = [...], children = [...])] attribute line);" \
    "has the chapter been restructured? Update this guard to match." >&2
  exit 1
fi

# Neither the doc's prose nor this pinned block defines send_email,
# charge_card, generate_report, or Report -- the doc assumes chapter-local
# context established earlier (Chapter 2's activities, Chapter 5's child
# workflows). Stand-ins with matching names let the pinned block compile
# unmodified. charge_card's return type deliberately matches the real
# activity of that name in 06-idempotency.md (a JSON object, not unit) --
# a stand-in that quietly narrowed it to `()` would hide exactly the
# output-type mismatch this guard exists to catch (review on #1708).
{
  cat <<'RUST_EOF'
#![allow(clippy::all, clippy::pedantic, clippy::nursery, dead_code)]
use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Report {
    ok: bool,
}

#[activity]
async fn send_email(_ctx: &ActivityContext, _user_id: i64) -> Result<serde_json::Value, String> {
    Ok(serde_json::Value::Null)
}

// Mirrors the real charge_card in docs/getting-started/06-idempotency.md:
// HarvestResult<serde_json::Value>, not unit.
#[activity]
async fn charge_card(_ctx: &ActivityContext, _user_id: i64) -> Result<serde_json::Value, String> {
    Ok(serde_json::json!({ "charge_id": "ch_stub" }))
}

#[workflow]
async fn generate_report(_ctx: &WorkflowContext, _user_id: i64) -> Result<Report, String> {
    Ok(Report { ok: true })
}

RUST_EOF
  echo "$code"
  echo
  echo "fn main() {}"
} >"$example_path"

if ! output="$(cargo check --quiet -p autumn-harvest --example "$example_name" 2>&1)"; then
  echo "$doc's \"Catching a forgotten registration before rollout\"" \
    "example does not compile against the real autumn-harvest crate." >&2
  echo >&2
  echo "$output" >&2
  exit 1
fi

echo "OK: $doc's preflight-declared-deps example compiles against the real autumn-harvest crate."
