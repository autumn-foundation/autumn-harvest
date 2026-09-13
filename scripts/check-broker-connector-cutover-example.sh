#!/usr/bin/env bash
# Fails if docs/getting-started/13-broker-connectors.md's cutover example
# calls `.map_json(...)` with the wrong closure signature.
#
# Mechanism this guards against: the "Cutting a binding over to a new
# cluster or a recreated topic" section's example used to read:
#
#   SourceBinding::starts("orders", "orders", "order_flow")
#       .map_json(|order: OrderPlaced| Ok(WorkflowId::new(order.order_id)))
#       .key_incarnation("2026-08-cutover")
#
# `SourceBinding::map_json` requires
# `F: Fn(&MessageCtx, T) -> Result<MappedMessage, E>`
# (autumn-harvest-plugin/src/connector/binding.rs:367-378) -- every other
# mapping closure in this chapter, and both shipped examples
# (autumn-harvest-plugin/examples/kafka_connector_quickstart.rs,
# autumn-harvest-plugin/examples/sqs_connector_quickstart.rs), take the
# message context plus the typed body and return a `MappedMessage`. This
# example took the body alone and returned a bare `WorkflowId` -- wrong
# arity, wrong return type, does not type-check as shown.
#
# A newcomer following the cutover recipe copies this into a real binding
# and hits a compile error with no obvious fix, right at the one moment
# (a production cutover) where getting the binding wrong risks the
# duplicate-execution or silent-loss failure the surrounding prose warns
# about.
#
# Usage: ./scripts/check-broker-connector-cutover-example.sh

set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

doc="docs/getting-started/13-broker-connectors.md"

if [ ! -f "$doc" ]; then
  echo "$doc: not found" >&2
  exit 1
fi

# Grab the cutover example: its SourceBinding::starts(...) call is unique in
# the chapter, and .key_incarnation("2026-08-cutover") a few lines below it
# closes the block.
window="$(grep -A 6 -F 'SourceBinding::starts("orders", "orders", "order_flow")' "$doc")"

if [ -z "$window" ]; then
  echo "$doc: could not find the cutover example's" \
    "SourceBinding::starts(\"orders\", \"orders\", \"order_flow\") call;" \
    "has the chapter been restructured? Update this guard to match." >&2
  exit 1
fi

if ! grep -qF 'key_incarnation("2026-08-cutover")' <<<"$window"; then
  echo "$doc: found SourceBinding::starts(\"orders\", \"orders\"," \
    "\"order_flow\") but not .key_incarnation(\"2026-08-cutover\") within" \
    "6 lines of it; has the cutover example moved or grown? Update this" \
    "guard to match." >&2
  exit 1
fi

# Arity: pull the text between `.map_json(|` and the closure's closing `|`.
# A Rust closure parameter list never contains a bare `|` (bitwise-or has no
# place there), so the first `|` after the opening one is always the close.
# Flagged in PR #1523 review: the prior check
# (`grep -qE '\.map_json\(\|[A-Za-z_][A-Za-z0-9_]*: '`) only rejected a
# single argument when it carried an explicit type annotation, so
# `.map_json(|order| { ... })` -- untyped, still the wrong arity -- slipped
# through.
params="$(grep -oP '(?<=\.map_json\(\|)[^|]*' <<<"$window" | head -n1)"

if [ -z "$params" ]; then
  echo "$doc: could not find a .map_json(|...| closure header in the" \
    "cutover example; has it moved to map_raw or changed shape? Update" \
    "this guard to match." >&2
  exit 1
fi

# Count top-level parameters exactly, splitting on commas only at bracket
# depth 0 -- a plain comma count or "does it start with `(`" heuristic both
# proved unsound in PR #1523 review: a tuple-destructured single argument
# (`|(_ctx, order): (&MessageCtx, OrderPlaced)|`) has an internal comma that
# a bare count mistakes for two arguments, a three-argument closure
# (`|_ctx, order: OrderPlaced, extra|`) has two commas but is not the
# required two-argument form, and a single argument with a tuple *type*
# (`|order: (OrderPlaced, String)|`) has a comma with no leading `(` on the
# parameter list itself. None of those are top-level commas once `()` /
# `<>` / `[]` / `{}` nesting is tracked, which is what this counts instead.
param_count="$(python3 -c '
import sys

s = sys.argv[1]
depth = 0
count = 1
opens = "([{<"
closes = ")]}>"
for ch in s:
    if ch in opens:
        depth += 1
    elif ch in closes:
        depth = max(0, depth - 1)
    elif ch == "," and depth == 0:
        count += 1
print(count if s.strip() else 0)
' "$params")"

if [ "$param_count" -ne 2 ]; then
  echo "$doc: the cutover example's .map_json(...) closure takes" \
    "$param_count top-level parameter(s) (\"$params\"), not the two" \
    "SourceBinding::map_json requires -- Fn(&MessageCtx, T) ->" \
    "Result<MappedMessage, E>. This does not type-check regardless of the" \
    "closure body." >&2
  echo >&2
  echo "Fix: take the message context and the typed body as exactly two" \
    "separate closure parameters, e.g. |_ctx, order: OrderPlaced|, and" \
    "return a MappedMessage (see the chapter's other .map_json examples," \
    "or MappedMessage::new)." >&2
  exit 1
fi

if grep -qF 'Ok(WorkflowId::new(' <<<"$window"; then
  echo "$doc: the cutover example's .map_json(...) closure returns a bare" \
    "WorkflowId. map_json requires Result<MappedMessage, E> -- construct a" \
    "MappedMessage instead (see MappedMessage::new)." >&2
  exit 1
fi

# Return type: the closure must yield Result<MappedMessage, E>, not a bare
# MappedMessage. Also flagged in review: a prior check merely required the
# substring `MappedMessage::new` to appear anywhere in the window, so
# `.map_json(|_ctx, order| MappedMessage::new(...))` -- missing the `Ok(...)`
# wrapper -- passed despite not type-checking against `map_json`'s bound.
if ! grep -qE 'Ok(::<[^)]*>)?\(\s*MappedMessage::new\(' <<<"$window"; then
  echo "$doc: the cutover example's .map_json(...) closure does not return" \
    "Ok(MappedMessage::new(...)) -- map_json requires" \
    "Result<MappedMessage, E>, so a bare MappedMessage (missing the Ok(...)" \
    "wrapper) does not type-check." >&2
  echo >&2
  echo "Fix: wrap the constructed MappedMessage in Ok(...), e.g." \
    "Ok::<_, String>(MappedMessage::new(...))." >&2
  exit 1
fi

echo "OK: the cutover example's .map_json(...) closure has the correct signature."
