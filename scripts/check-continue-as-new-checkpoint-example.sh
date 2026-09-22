#!/usr/bin/env bash
# Fails if either copy of the "checkpoint before the execution_timeout
# deadline" continue_as_new example regresses into a real type error: a
# bare `?` on `ctx.continue_as_new(...).await` inside a `Result<_, String>`
# workflow, with no `.map_err(...)` converting the error immediately after
# that `.await`.
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
# Checked per-statement, not with a loose windowed grep (PR #1700 review):
# a fixed-line-count window around "ctx.continue_as_new(" can be satisfied
# by a `.map_err(` that belongs to something else entirely -- the
# `serde_json::to_value(&state)` conversion nested inside the SAME call's
# own argument list, or, since long_lived_entity_deadline.rs's module-doc
# comment and its real `subscription_entity` fn each call continue_as_new
# once, the real fn's own already-correct `.map_err(` masking a regression
# reintroduced into the doc-comment copy alone. Each `ctx.continue_as_new(
# ... );` statement is isolated first (accumulated from its opening line
# to its own terminating `;`, across line breaks, with whitespace and any
# `//!` doc-comment marker stripped), then checked on its own for the exact
# substring ".await.map_err(" -- present whether the fix is written as
# `.await` / `.map_err(...)` on separate lines or chained on one line, and
# absent from the broken `.await?` (or bare `.await;`) forms, regardless of
# an unrelated `.map_err(` earlier in the same statement's argument list.
#
# Usage: ./scripts/check-continue-as-new-checkpoint-example.sh

set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

check_file() {
  local file="$1"
  local label="$2"
  local statements
  local bad=0

  if [ ! -f "$file" ]; then
    echo "$file: not found" >&2
    return 1
  fi

  # One compacted line per `ctx.continue_as_new( ... );` statement, followed
  # by a "###STMT-END###" marker line -- so a file with more than one call
  # (the module-doc copy AND the real fn, in long_lived_entity_deadline.rs)
  # yields one independently-checkable record per call, not one combined
  # blob where either could satisfy the check for the other.
  statements="$(awk '
    /ctx\.continue_as_new\(/ { collecting = 1 }
    collecting {
      line = $0
      gsub(/\/\/!/, "", line)
      gsub(/[ \t]+/, "", line)
      buf = buf line
      if (line ~ /;$/) {
        print buf
        print "###STMT-END###"
        buf = ""
        collecting = 0
      }
    }
  ' "$file")"

  if [ -z "$statements" ]; then
    echo "$file: could not find a ctx.continue_as_new(...) call in $label;" \
      "has it moved? Update this guard to match." >&2
    return 1
  fi

  while IFS= read -r stmt; do
    [ -z "$stmt" ] && continue
    [ "$stmt" = "###STMT-END###" ] && continue
    if [[ "$stmt" != *".await.map_err("* ]]; then
      echo "$file: $label has a ctx.continue_as_new(...).await with no" \
        ".map_err(...) immediately after that .await. continue_as_new" \
        "returns HarvestResult<()>, and there is no From<HarvestError> for" \
        "String, so an unconverted '?' does not compile inside a" \
        "Result<_, String> workflow (E0277)." >&2
      echo "  offending statement (whitespace stripped): $stmt" >&2
      echo >&2
      echo "Fix: .map_err(|e| e.to_string())? right after .await, matching" \
        "the real, compiling subscription_entity fn in" \
        "autumn-harvest/examples/long_lived_entity_deadline.rs." >&2
      bad=1
    fi
  done <<<"$statements"

  return "$bad"
}

status=0
check_file "docs/getting-started/07-reliability-knobs.md" \
  "the deadline-aware checkpoint example" || status=1
check_file "autumn-harvest/examples/long_lived_entity_deadline.rs" \
  "the module-doc illustration" || status=1

if [ "$status" -eq 0 ]; then
  echo "OK: both continue_as_new checkpoint examples map_err right after '.await'."
fi
exit "$status"
