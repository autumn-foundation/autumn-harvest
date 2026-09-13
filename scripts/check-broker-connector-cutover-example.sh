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

# A leading `(` means the whole parameter list is one tuple-destructured
# argument (e.g. `|(_ctx, order): (&MessageCtx, OrderPlaced)|`), which
# implements `Fn((&MessageCtx, OrderPlaced))` -- a single tuple parameter --
# not the required `Fn(&MessageCtx, OrderPlaced)`. Flagged in PR #1523
# review: a bare comma count treats this as two arguments because a tuple
# pattern's internal comma still counts, so it passed despite being the
# same wrong arity as the untyped and typed single-argument cases above.
trimmed_params="$(sed -E 's/^[[:space:]]+//' <<<"$params")"

if [ -z "$trimmed_params" ] || ! grep -q ',' <<<"$trimmed_params" \
  || [ "${trimmed_params:0:1}" = "(" ]; then
  echo "$doc: the cutover example's .map_json(...) closure takes a single" \
    "argument (\"$params\") -- either the typed body alone, or both" \
    "parameters destructured together as one tuple pattern. Either way it" \
    "implements Fn(T) or Fn((&MessageCtx, T)), not the" \
    "Fn(&MessageCtx, T) -> Result<MappedMessage, E> that SourceBinding::" \
    "map_json requires, so this closure has the wrong arity and does not" \
    "type-check." >&2
  echo >&2
  echo "Fix: take the message context and the typed body as two separate" \
    "closure parameters, e.g. |_ctx, order: OrderPlaced|, and return a" \
    "MappedMessage (see the chapter's other .map_json examples, or" \
    "MappedMessage::new)." >&2
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
