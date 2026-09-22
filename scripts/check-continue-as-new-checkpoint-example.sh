#!/usr/bin/env bash
# Fails if either copy of the "checkpoint before the execution_timeout
# deadline" continue_as_new example regresses into a real type error: a
# bare `?` on `ctx.continue_as_new(...).await` inside a `Result<_, String>`
# workflow, with no `.map_err(...)` converting the error first.
#
# Mechanism: `WorkflowContext::continue_as_new` returns
# `HarvestResult<()>` (`Result<(), HarvestError>`). There is no
# `From<HarvestError> for String`, so `?` alone does not compile inside a
# function whose error type is `String` -- E0277. An Onramp clean-room pass
# found this in docs/getting-started/07-reliability-knobs.md's own flagship
# deadline-aware-checkpoint snippet; doc-snippet-syntax.py never catches it
# because it only runs `rustfmt` (a syntax check), and this is a type error.
#
# The same broken snippet also lived, unenforced, inside
# autumn-harvest/examples/long_lived_entity_deadline.rs's own module-doc
# comment -- marked ```rust,ignore``` so nothing ever compiled it either,
# even though the real, working `subscription_entity` fn a few lines below
# in that same file gets this right with `.map_err(|e| e.to_string())`.
#
# Usage: ./scripts/check-continue-as-new-checkpoint-example.sh

set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

check_file() {
  local file="$1"
  local label="$2"
  local window

  if [ ! -f "$file" ]; then
    echo "$file: not found" >&2
    return 1
  fi

  window="$(grep -A 2 -F 'ctx.continue_as_new(' "$file")"

  if [ -z "$window" ]; then
    echo "$file: could not find a ctx.continue_as_new(...) call in $label;" \
      "has it moved? Update this guard to match." >&2
    return 1
  fi

  if ! grep -qF '.map_err(' <<<"$window"; then
    echo "$file: $label calls ctx.continue_as_new(...).await? with no" \
      ".map_err(...) first. continue_as_new returns HarvestResult<()>, and" \
      "there is no From<HarvestError> for String, so '?' does not compile" \
      "inside a Result<_, String> workflow (E0277)." >&2
    echo >&2
    echo "Fix: .map_err(|e| e.to_string())? after the .await, matching the" \
      "real, compiling subscription_entity fn in" \
      "autumn-harvest/examples/long_lived_entity_deadline.rs." >&2
    return 1
  fi
}

status=0
check_file "docs/getting-started/07-reliability-knobs.md" \
  "the deadline-aware checkpoint example" || status=1
check_file "autumn-harvest/examples/long_lived_entity_deadline.rs" \
  "the module-doc illustration" || status=1

if [ "$status" -eq 0 ]; then
  echo "OK: both continue_as_new checkpoint examples map_err before '?'."
fi
exit "$status"
