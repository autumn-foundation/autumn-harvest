#!/usr/bin/env bash
# Fails if chapter 12's webhook example regresses, in either of two ways:
#
# 1. Doc drift: the "Enable the feature" note (added by an Onramp
#    clean-room pass) -- which tells the reader that
#    `HarvestPlugin::webhooks(...)` needs `autumn-harvest-plugin`'s
#    `webhooks` Cargo feature -- disappears from the chapter, or moves
#    past step 3.
# 2. Code drift: step 2's `#[webhook]` mapping function and step 3's
#    `HarvestPlugin` wiring stop compiling against the real crate.
#
# Mechanism this guards against: chapter 12 showed the `#[webhook]` mapping
# function and `.webhooks(webhooks![...])` plugin wiring with no mention
# that `autumn-harvest-plugin` needs `features = ["webhooks"]` -- unlike
# the sibling chapter 13 (broker connectors), which states its `connectors`
# feature requirement up front in its own "Features:" table. A newcomer who
# scaffolds a project per chapter 1 (no extra features) and pastes chapter
# 12's code verbatim gets, at the `.webhooks(...)` call:
#
#   error[E0599]: no method named `webhooks` found for struct `HarvestPlugin`
#
# which names neither "webhooks" nor chapter 12. Reproduced live: this
# script's `compile_check` with default features fails with exactly that
# error, and succeeds once `--features webhooks` is added -- confirming the
# feature really is the fix, not a coincidence.
#
# Usage: ./scripts/check-webhooks-feature-example.sh

set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

doc="docs/getting-started/12-webhooks.md"
example_name="_onramp_doc_check_webhooks_feature"
example_path="autumn-harvest-plugin/examples/${example_name}.rs"

cleanup() {
  rm -f "$example_path"
}
trap cleanup EXIT
cleanup # self-heal if a previous run was killed before its own trap ran

if [ ! -f "$doc" ]; then
  echo "$doc: not found" >&2
  exit 1
fi

status=0

# ── 1. Doc drift: the feature note must exist, and must precede step 3 ──────

before_step3="$(awk '/^## 3\. Wire the plugin$/ { exit } { print }' "$doc")"

if [ -z "$before_step3" ]; then
  echo "$doc: could not find '## 3. Wire the plugin'; has the chapter been" \
    "restructured? Update this guard to match." >&2
  status=1
elif ! grep -qF 'features = ["webhooks"]' <<<"$before_step3"; then
  echo "$doc: no mention of \`features = [\"webhooks\"]\` before step 3." \
    "Without it, a reader who scaffolds per chapter 1 and pastes this" \
    "chapter's code verbatim hits 'no method named \`webhooks\` found for" \
    "struct \`HarvestPlugin\`' at the .webhooks(...) call, with nothing" \
    "naming the missing feature. See the 'Enable the feature' section this" \
    "guard expects, and chapter 13's 'Features:' table for the sibling" \
    "convention." >&2
  status=1
fi

# ── 2. Code drift: the doc's own snippets must still compile ────────────────

extract_between() {
  local start_pat="$1"
  local end_pat="$2"
  awk -v start="$start_pat" -v end="$end_pat" '
    $0 ~ start { p = 1; next }
    p && $0 ~ end { exit }
    p { print }
  ' "$doc"
}

mapping_fn="$(extract_between '^## 2\. Write the mapping function$' '^```$')"
mapping_fn="$(printf '%s\n' "$mapping_fn" | awk '/^```rust$/{p=1;next} p')"

plugin_wiring="$(extract_between '^## 3\. Wire the plugin$' '^```$')"
plugin_wiring="$(printf '%s\n' "$plugin_wiring" | awk '/^```rust$/{p=1;next} p')"

if [ -z "$mapping_fn" ] || [ -z "$plugin_wiring" ]; then
  echo "$doc: could not extract the step-2 mapping-function block or the" \
    "step-3 plugin-wiring block; has the chapter been restructured?" \
    "Update this guard to match." >&2
  exit 1
fi

compile_check() {
  local features="$1"
  local expect="$2" # "pass" or "fail"
  local output

  {
    echo "#![allow(clippy::all, clippy::pedantic, clippy::nursery, dead_code)]"
    echo "use autumn_harvest_plugin::HarvestPlugin;"
    echo "$mapping_fn"
    echo
    echo "#[workflow]"
    echo "async fn subscription_flow(_ctx: &WorkflowContext, _input: String) -> HarvestResult<String> {"
    echo '    Ok("done".into())'
    echo "}"
    echo
    echo "#[autumn_web::main]"
    echo "async fn main() {"
    echo "$plugin_wiring"
    echo "    app.run().await;"
    echo "}"
  } >"$example_path"

  local feature_args=(-p autumn-harvest-plugin --example "$example_name")
  if [ -n "$features" ]; then
    feature_args+=(--features "$features")
  fi

  if output="$(cargo check --quiet "${feature_args[@]}" 2>&1)"; then
    if [ "$expect" = "fail" ]; then
      echo "$doc's webhook example compiled WITHOUT --features webhooks," \
        "but this guard expects it to fail there (that's the whole point" \
        "of the 'Enable the feature' note). Either the feature gate moved," \
        "or this guard is stale -- update whichever is now wrong." >&2
      return 1
    fi
    return 0
  fi

  if [ "$expect" = "pass" ]; then
    echo "$doc's webhook example does not compile against the real" \
      "autumn-harvest-plugin crate (features: '${features:-<default>}')." >&2
    echo >&2
    echo "$output" >&2
    return 1
  fi
  return 0
}

compile_check "" "fail" || status=1
compile_check "webhooks" "pass" || status=1

if [ "$status" -eq 0 ]; then
  echo "OK: chapter 12's webhook example compiles with --features webhooks," \
    "fails to compile without it as documented, and the feature is" \
    "documented before step 3."
fi
exit "$status"
