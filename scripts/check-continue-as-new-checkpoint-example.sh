#!/usr/bin/env bash
# Fails if either copy of the "checkpoint before the execution_timeout
# deadline" continue_as_new example regresses into a real type error: a
# `ctx.continue_as_new(...).await` whose error is not actually converted to
# `String` before the surrounding `Result<_, String>` workflow's `?`.
#
# Mechanism: `WorkflowContext::continue_as_new` returns
# `HarvestResult<()>` (`Result<(), HarvestError>`). There is no
# `From<HarvestError> for String`, so `?` needs a real conversion first --
# E0277 otherwise. An Onramp clean-room pass found this in
# docs/getting-started/07-reliability-knobs.md's own flagship
# deadline-aware-checkpoint snippet; doc-snippet-syntax.py never catches it
# because it only runs `rustfmt` (a syntax check), and this is a type
# error. The identical broken snippet also lived, unenforced, inside
# autumn-harvest/examples/long_lived_entity_deadline.rs's own module-doc
# comment, marked ```rust,ignore``` so nothing ever compiled it either.
#
# This actually compiles each pinned snippet with `cargo check`, rather
# than pattern-matching stand-in text (two rounds of PR #1700 review found
# text heuristics gameable here: first a loose window an unrelated
# `.map_err(` could satisfy, then an `.await.map_err(|e| e)?` identity
# closure -- syntactically "a map_err right after await", but `|e| e`
# keeps `HarvestError` instead of producing `String`, so it still fails to
# compile). Whether a closure's return type actually satisfies `?`'s
# target is a type-checker property, not a text shape, so there is no
# third regex worth writing here -- the compiler already settles it.
# `docs/audits/doc-snippet-syntax.py` deliberately does not do this for the
# whole 93-block corpus (its own docstring explains why); that tradeoff
# does not apply to two pinned, previously-broken blocks checked against
# the crate this same `lint` job already builds for clippy a few steps
# later.
#
# Usage: ./scripts/check-continue-as-new-checkpoint-example.sh

set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

example_name="_onramp_doc_check_continue_as_new_checkpoint"
example_path="autumn-harvest/examples/${example_name}.rs"

cleanup() {
  rm -f "$example_path"
}
trap cleanup EXIT
cleanup # self-heal if a previous run was killed before its own trap ran

# Neither pinned snippet defines SubState or its own `use` -- the doc
# assumes chapter-local context, and the .rs snippet's own `use
# autumn_harvest::prelude::*;` line duplicates this harmlessly (an
# identical repeated glob import is not an error). Matches the real
# SubState in autumn-harvest/examples/long_lived_entity_deadline.rs.
harness_prelude() {
  cat <<'RUST_EOF'
#![allow(clippy::all, clippy::pedantic, clippy::nursery, dead_code)]
use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SubState {
    pub cycles: u32,
}
RUST_EOF
}

compile_check() {
  local label="$1"
  local code="$2"
  local output

  {
    harness_prelude
    echo
    echo "$code"
    echo
    echo "fn main() {"
    echo "    let _wfs = workflows![subscription_entity];"
    echo "}"
  } >"$example_path"

  if ! output="$(cargo check --quiet -p autumn-harvest --example "$example_name" 2>&1)"; then
    echo "$label does not compile against the real autumn-harvest crate." >&2
    echo >&2
    echo "$output" >&2
    echo >&2
    echo "See the real, compiling subscription_entity fn in" \
      "autumn-harvest/examples/long_lived_entity_deadline.rs." >&2
    return 1
  fi
  return 0
}

extract_md_block() {
  awk '
    /^#\[workflow\(execution_timeout = "24h"\)\]$/ { p = 1 }
    p && /^```$/ { exit }
    p { print }
  ' "docs/getting-started/07-reliability-knobs.md"
}

extract_rs_doc_comment_block() {
  awk '
    /^\/\/! ```rust,ignore$/ { p = 1; next }
    p && /^\/\/! ```$/ { exit }
    p {
      line = $0
      sub(/^\/\/! ?/, "", line)
      print line
    }
  ' "autumn-harvest/examples/long_lived_entity_deadline.rs"
}

status=0

md_code="$(extract_md_block)"
if [ -z "$md_code" ]; then
  echo "docs/getting-started/07-reliability-knobs.md: could not find the" \
    "subscription_entity checkpoint example (looked for its" \
    "#[workflow(execution_timeout = \"24h\")] attribute line); has the" \
    "chapter been restructured? Update this guard to match." >&2
  status=1
else
  compile_check "docs/getting-started/07-reliability-knobs.md's checkpoint example" \
    "$md_code" || status=1
fi

rs_code="$(extract_rs_doc_comment_block)"
if [ -z "$rs_code" ]; then
  echo "autumn-harvest/examples/long_lived_entity_deadline.rs: could not" \
    "find its \`\`\`rust,ignore\`\`\` module-doc illustration; has it moved?" \
    "Update this guard to match." >&2
  status=1
else
  compile_check "long_lived_entity_deadline.rs's module-doc illustration" \
    "$rs_code" || status=1
fi

if [ "$status" -eq 0 ]; then
  echo "OK: both continue_as_new checkpoint examples compile against the real autumn-harvest crate."
fi
exit "$status"
