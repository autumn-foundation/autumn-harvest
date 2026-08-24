#!/usr/bin/env python3
"""Deterministic allocation-site categorizer for `debugger_trace_profile`'s
DHAT capture -- the auditable script that produces the "Allocation-site
attribution" table in `docs/performance-debugger-trace.md`.

Mirrors `autumn-harvest-sqlite/scripts/classify_dhat_allocations.py`'s
approach and methodology: DHAT's JSON output (schema version 2) is a list of
"program points" (`pps`) -- one per unique resolved allocation call stack --
each carrying total bytes/blocks (`tb`/`tbk`) and a list of frame indices
(`fs`, INNERMOST-first) into a flat, already-symbolized frame table
(`ftbl`). This script resolves each `pp`'s stack to frame-name strings and
assigns it to exactly one category via a fixed, PRECEDENCE-ORDERED sequence
of marker-substring checks against the joined stack text -- first match
wins, so the categories are a mutually-exclusive, exhaustive partition of
the capture's total bytes/blocks by construction (asserted before printing).

Requires `--num-callers=30` on the `valgrind --tool=dhat` invocation that
produces the input file: `serde_json`'s recursive-descent deserializer and
`BTreeMap`'s recursive clone routines both nest well past DHAT's default
12-frame capture depth for this workload's ~230-byte nested-object payload
shape, truncating the stack before it reaches the real Rust caller for a
meaningful share of allocations and misattributing them to "other".

# Precedence rationale (checked in this order, first match wins)

1. `harness_fixture_setup` -- `debugger_trace_profile::support::build_history`
   anywhere on the stack. This is the ONE-TIME, `O(n)` construction of the
   synthetic event history the harness feeds to `trace_snapshot` -- it runs
   BEFORE `trace_snapshot` is even called and is not part of what the
   function under test does. Checked first because `build_history` also
   calls `activity_payload(i)` (to build each seeded event's payload), which
   would otherwise be miscategorized into `workflow_reexecution` (bucket 4
   below) -- a genuinely different cost that happens once per activity, not
   once per activity PER STEP.
2. `trace_snapshot_prefix_clone` -- `trace_snapshot` on the stack together
   with a `to_vec`/`Clone`/`clone` marker. This is the per-step
   `snapshot.events[..=step.index].to_vec()` call in `trace_snapshot`'s
   loop: at step `k` it clones `k + 1` `WorkflowEvent`s (many carrying
   `serde_json::Value` payloads, `BTreeMap`-backed with no `preserve_order`
   feature) into a fresh owned `Vec` handed to `replay_prefix`. Checked
   before the matcher-internals and re-execution buckets below because a
   `Value::Object` clone's `BTreeMap::clone_subtree` frames can appear on
   stacks that also happen to pass through frames matched by those buckets'
   markers (e.g. `WorkflowContext::for_replay_canary_with_state`, which
   `trace_snapshot`'s caller -> `replay_prefix` -> that constructor all sit
   between) -- this is fundamentally a *cloning* cost caused by
   `trace_snapshot`, not an *internal-matching* cost caused by
   `HistoryMatcher`, so it is attributed to its true origin.
3. `history_matcher_internals` -- `HistoryMatcher`, `match_history`, or
   `drive_query_replay` on the stack -- checked AHEAD of bucket 4 below,
   not after, because these frames are always *nested inside*
   `replay_prefix`'s own `drive_query_replay_async(...).await` (confirmed
   by reading `replay_prefix`'s body in `debugger.rs`), so any stack
   carrying both markers has this as the more specific, innermost cause --
   the identical precedence principle bucket 2 above already applies to
   `trace_snapshot_prefix_clone` vs. its descendants. This is the actual
   replay-matching machinery that walks the (already-owned,
   already-cloned) prefix and decides what the workflow function does
   next.

   KNOWN LIMITATION, empirically confirmed rather than assumed: in this
   workspace's default `cargo bench` release profile (`debug = false`,
   confirmed unchanged even after rebuilding with `-C debuginfo=2`), every
   function this bucket's markers name is fully inlined away and never
   appears as its own frame in DHAT's resolved call stack -- grepping the
   raw `ftbl` for `HistoryMatcher` / `match_history` / `drive_query_replay`
   / `replay_prefix` / `match_activity` / `scan_activity_terminal` returns
   zero matches at every swept input size, with or without debug info, and
   swapping this bucket's precedence against bucket 4's changes nothing
   (both orderings observe the same empty marker set on every captured
   stack). So a `0` count here means "unobservable by this methodology",
   not "free" -- any allocation this code performs is silently folded into
   whichever caller frame survived inlining (most plausibly
   `trace_snapshot::{{closure}}`, landing in bucket 2, or the
   workflow-driving frames feeding bucket 5). See
   `docs/performance-debugger-trace.md`'s "Allocation-site attribution"
   section for the full investigation this note summarizes.
4. `replay_prefix_or_context_build` -- `replay_prefix`,
   `for_replay_canary_with_state`, or `register_declarative_handlers` on the
   stack (and buckets 2-3 didn't already match). Per-step `WorkflowContext`
   construction and declarative-handler registration overhead, distinct
   from the prefix clone above. In practice this bucket is populated almost
   entirely via `for_replay_canary_with_state`/`register_declarative_handlers`:
   `replay_prefix` itself is also inlined away (see bucket 3's note above)
   and never survives as its own frame either.
5. `workflow_reexecution` -- `sequential_workflow` or `activity_payload` on
   the stack (build_history already excluded by bucket 1's precedence).
   This is the WORKFLOW'S OWN CODE re-running from the top of its `for`
   loop on every step's fresh canary replay -- inherent to "step k is a
   fresh replay of `events[0..=k]`", not debugger overhead. Directly
   parallel to `runtime_drive_profile`'s `workflow_reexecution` category.
6. `fmt_format_machinery` -- `core::fmt`/`alloc::fmt` on the stack (harness
   `println!`/`format!` overhead, and any `Display`/`Debug` formatting
   reachable from the traced call).
7. `tokio_runtime` -- `tokio::` on the stack. Async task/future/waker
   machinery `drive_query_replay_async` runs on.
8. `other` -- everything else. Printed with its own top-N breakdown by
   bytes so nothing is silently swept under an unaudited catch-all.
"""

import json
import sys


def resolve(pp, ftbl):
    return [ftbl[i] for i in pp.get("fs", [])]


def classify(joined: str) -> str:
    if "build_history" in joined:
        return "harness_fixture_setup"
    if "trace_snapshot" in joined and (
        "to_vec" in joined or "Clone" in joined or "clone" in joined
    ):
        return "trace_snapshot_prefix_clone"
    # Checked BEFORE replay_prefix_or_context_build: HistoryMatcher /
    # match_history / drive_query_replay frames are always nested *inside*
    # replay_prefix's own drive_query_replay_async(...).await, so on a
    # stack carrying both markers this is the more specific, innermost
    # cause -- see bucket 3's docstring note above for why this precedence
    # is, empirically, unreachable in a release-profile capture regardless
    # of which way it is ordered (both markers are fully inlined away).
    if (
        "HistoryMatcher" in joined
        or "match_history" in joined
        or "drive_query_replay" in joined
    ):
        return "history_matcher_internals"
    if (
        "replay_prefix" in joined
        or "for_replay_canary_with_state" in joined
        or "register_declarative_handlers" in joined
    ):
        return "replay_prefix_or_context_build"
    if "sequential_workflow" in joined or "activity_payload" in joined:
        return "workflow_reexecution"
    if "core::fmt" in joined or "alloc::fmt" in joined:
        return "fmt_format_machinery"
    if "tokio::" in joined:
        return "tokio_runtime"
    return "other"


def main() -> None:
    path = sys.argv[1] if len(sys.argv) > 1 else "dhat.json"
    with open(path, encoding="utf-8") as fh:
        d = json.load(fh)
    ftbl = d["ftbl"]

    order = [
        "harness_fixture_setup",
        "trace_snapshot_prefix_clone",
        "replay_prefix_or_context_build",
        "history_matcher_internals",
        "workflow_reexecution",
        "fmt_format_machinery",
        "tokio_runtime",
        "other",
    ]
    bytes_by = dict.fromkeys(order, 0)
    blocks_by = dict.fromkeys(order, 0)
    other_stacks: dict[str, int] = {}

    total_b = 0
    total_bk = 0
    for pp in d["pps"]:
        tb = pp.get("tb", 0)
        tbk = pp.get("tbk", 0)
        total_b += tb
        total_bk += tbk
        joined = "|".join(resolve(pp, ftbl))
        key = classify(joined)
        bytes_by[key] += tb
        blocks_by[key] += tbk
        if key == "other":
            other_stacks[joined[:400]] = other_stacks.get(joined[:400], 0) + tb

    # Exhaustive-partition assertion: every pp landed in exactly one bucket,
    # so the category totals must sum to the capture's grand totals exactly.
    assert sum(bytes_by.values()) == total_b, "categories do not sum to total bytes"
    assert sum(blocks_by.values()) == total_bk, "categories do not sum to total blocks"

    print(f"Total: {total_b:,} bytes in {total_bk:,} blocks\n")
    print(f"{'category':32s} {'bytes':>14s} {'%':>7s}   {'blocks':>10s} {'%':>7s}")
    for key in order:
        pb = 100 * bytes_by[key] / total_b if total_b else 0.0
        pk = 100 * blocks_by[key] / total_bk if total_bk else 0.0
        print(
            f"{key:32s} {bytes_by[key]:>14,} {pb:6.2f}%   {blocks_by[key]:>10,} {pk:6.2f}%"
        )

    if other_stacks:
        print("\n--- top 'other' stacks by bytes ---")
        for stack, b in sorted(other_stacks.items(), key=lambda kv: -kv[1])[:8]:
            print(f"{b:>10,} bytes  {stack}")


if __name__ == "__main__":
    main()
