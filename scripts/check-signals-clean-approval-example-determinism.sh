#!/usr/bin/env bash
# Fails if docs/getting-started/04-signals.md's "Clean Declarative
# await_condition" example (`collect_approvals_clean`) regresses into either
# of two compile failures a newcomer hit copying it verbatim.
#
# Mechanism this guards against, both found by an Onramp clean-room pass
# compiling the block against the real crate (doc-snippet-syntax.py only
# runs `rustfmt`, a syntax check, so neither of these ever surfaced there):
#
# 1. The example races `ctx.wait_for_signal(...)` against
#    `ctx.await_condition_timeout(...)` with `futures::future::select`, the
#    exact combinator HVG010 hard-blocks at compile time (see
#    docs/workflow-determinism-guide.md's HVG010 section). The real,
#    CI-compiled equivalent in autumn-harvest/examples/collect_approvals.rs
#    only builds because it carries `#[workflow(allow_nondeterministic_apis)]`
#    plus a `harvest-suppress: DET011` justification -- a bare `#[workflow]`
#    fails with a HardBlocker compile error naming this exact line.
#
# 2. The closure passed to `await_condition_timeout` keeps a borrow on the
#    approvals counter alive for the loop's whole lifetime. A plain
#    `let mut approvals = 0;` captured that way can never also be reassigned
#    by the loop body's `approvals += 1;` -- E0506, "cannot assign ...
#    because it is borrowed". The real example above sidesteps this by
#    putting the counter behind `Mutex::new(...)`, cloning an `Arc` into
#    the closure.
#
# Usage: ./scripts/check-signals-clean-approval-example-determinism.sh

set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

doc="docs/getting-started/04-signals.md"

if [ ! -f "$doc" ]; then
  echo "$doc: not found" >&2
  exit 1
fi

# The example runs from its leading comment to the fenced block's own
# close -- a generous window that survives the body growing or shrinking,
# since it stops at the next "```" line rather than a fixed line count.
window="$(awk '
  /-- Clean Declarative await_condition --/ { p = 1 }
  p { print }
  p && /^```$/ { exit }
' "$doc")"

if [ -z "$window" ]; then
  echo "$doc: could not find the collect_approvals_clean example;" \
    "has the chapter been restructured? Update this guard to match." >&2
  exit 1
fi

if ! grep -qF '#[workflow(allow_nondeterministic_apis)]' <<<"$window"; then
  echo "$doc: collect_approvals_clean races wait_for_signal against" \
    "await_condition_timeout with futures::future::select, which HVG010" \
    "hard-blocks at compile time without the allow_nondeterministic_apis" \
    "opt-out (see autumn-harvest/examples/collect_approvals.rs)." >&2
  echo >&2
  echo "Fix: put '#[workflow(allow_nondeterministic_apis)]' on" \
    "collect_approvals_clean, matching the real compiling example." >&2
  exit 1
fi

if grep -qE 'let mut approvals[[:space:]]*=[[:space:]]*0' <<<"$window"; then
  echo "$doc: collect_approvals_clean captures a plain 'let mut approvals'" \
    "inside the await_condition_timeout closure, then reassigns it in the" \
    "loop body -- a borrow-check error (E0506), since the closure's borrow" \
    "lives for the whole loop." >&2
  echo >&2
  echo "Fix: put the counter behind Mutex::new(...) (Arc-cloned into the" \
    "closure), matching autumn-harvest/examples/collect_approvals.rs." >&2
  exit 1
fi

if ! grep -qF 'Mutex::new(' <<<"$window"; then
  echo "$doc: collect_approvals_clean no longer wraps its approvals" \
    "counter in a Mutex; update this guard to match the current example." >&2
  exit 1
fi

echo "OK: collect_approvals_clean carries the allow_nondeterministic_apis opt-out and a Mutex-backed counter."
