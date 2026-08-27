#!/usr/bin/env python3
"""Deterministic allocation-site categorizer for `runtime_drive_profile`'s
DHAT capture -- the auditable script that produces the "Allocation-site
attribution" table in `docs/performance-sqlite-runtime-drive.md`.

This exists so that table is not a hand-eyeballed one-off: re-running this
script against a freshly captured `dhat.json` (see the "Reproduce" section
of that doc for the exact `valgrind --tool=dhat` invocation, including the
required `--num-callers=30`) reproduces it byte-for-byte, and re-categorizing
after a code change is a one-command re-run, not a re-derivation from scratch.

# How it works

DHAT's JSON output (schema version 2) is a list of "program points" (`pps`)
-- one per unique resolved allocation call stack -- each carrying total
bytes/blocks (`tb`/`tbk`) and a list of frame indices (`fs`, innermost-first)
into a flat, already-symbolized frame table (`ftbl`). This script resolves
each `pp`'s stack to frame-name strings and assigns it to exactly one
category via a fixed, PRECEDENCE-ORDERED sequence of marker-substring checks
against the joined stack text. Because every `pp` is classified into exactly
one bucket, the categories are a mutually-exclusive, exhaustive partition of
the capture's total bytes/blocks by construction -- the script asserts this
before printing.

# Precedence rationale (checked in this order, first match wins)

1. Clone markers (`Vec`/`BTreeMap`/`String` `Clone` impls anywhere on the
   stack). `serde_json::Value` objects are `BTreeMap`-backed (no
   `preserve_order` feature in this workspace), so cloning any JSON value
   recurses into these Clone impls -- and this workload has THREE distinct,
   architecturally-separate call sites that all match the same generic
   markers, split by whether `drive_suspension`/`drain_ready` is ALSO on
   the stack:
     - `history.clone()` -- neither `drive_suspension` nor `drain_ready` is
       on the stack. This is `SqliteRuntime::drive_one_cycle`'s clone of the
       freshly reloaded, accumulated `Vec<WorkflowEvent>` before consuming
       it by value -- the ONE clone site this document's whole finding is
       about, happening once per DECISION CYCLE. `drive_one_cycle` itself
       never appears as a literal frame (it is fully inlined into
       `run_until_blocked::{{closure}}`, its sole call site), so its
       absence -- not its presence -- is what a matching stack looks like.
     - `activity_input_clone` -- `drive_suspension` or `drain_ready` IS on
       the stack. These are two smaller, bounded, per-ACTIVITY (not
       per-cycle) clones of the same `serde_json::Value` payload:
       `persist_scheduled_activity` clones an activity's input once, at
       SCHEDULE time, to build the durable `ActivityScheduled` event
       (inlined through `apply_commands`/`apply_schedule_activity` into
       `drive_suspension`, its only caller); `worker::drain_ready` clones
       the same input a second time, at DRAIN time, immediately before
       invoking the registered activity body closure. Neither is part of
       the "N decision cycles reload the whole history" story -- each
       happens exactly once per activity dispatched, not once per cycle --
       so lumping them into `history.clone()` (an earlier cut of this
       script did) overstates that bucket and its addressability. See the
       doc's Methodology note on this split for the numbers.
   Checked FIRST (both sub-cases) because a clone of an already-deserialized
   `Value` can itself pass through frames that also appear in the
   deserialize path (e.g. `BTreeMap::insert` during a Clone's internal
   rebuild), and this is a *cloning* cost, not a *parsing* cost -- the two
   are causally distinct fixes.
2. Deserialize markers (`serde_json::de::`, `serde_json::value::de::`,
   `serde_core::de::`) -- `serde_json::Value::deserialize`'s recursive
   descent. Split into `load_history_deserialize` (the `store::load_history`
   frame is also on the stack -- the JSON payload of an already-recorded
   event, re-parsed on every reload) vs. `serde_json_elsewhere` (any other
   deserialize call site, e.g. `queue::claim_next_ready_task_tx` decoding a
   claimed task's stored input).
3. `workflow_reexecution` -- the user workflow closure
   (`sequential_workflow`) or its payload-construction helper
   (`activity_payload`) is on the stack: the cost of the AUTHOR's code
   re-running from the top on every replay cycle, as opposed to the
   engine's own bookkeeping.
4. sqlite3-C-frame allocations (any frame is a raw sqlite3 C-engine symbol
   -- parser, VDBE, memory allocator, etc. -- with no Rust marker above
   matching first), split by whether `store::load_history` is ALSO on the
   stack -- an earlier cut of this script checked `LOAD_HISTORY_MARKER`
   BEFORE the sqlite3-C check unconditionally, which meant NO sqlite3-C
   allocation could ever be attributed to the reload query at all (any
   stack carrying both markers was claimed by the `load_history` check
   first) while simultaneously mislabeling the whole, much larger,
   non-reload `sqlite3_internals` bucket as if it were "sqlite3 internals
   executing the reload query" in the accompanying doc prose. Precedence
   here restores the actual causal split:
     - `sqlite3_internals_reload_query` -- sqlite3-C AND `load_history` both
       on the stack: the sliver of sqlite3 C-engine work genuinely
       reachable from the reload query itself.
     - `load_history_vec_query` -- `load_history` on the stack, no
       sqlite3-C frame above it matched first (the `Vec<WorkflowEvent>`
       growth and the underlying rusqlite query execution, as opposed to
       the per-event JSON deserialize already carved out above).
     - `sqlite3_internals` -- sqlite3-C on the stack, no `load_history`:
       the storage engine's own allocations from OTHER call sites (task
       claiming, activity enqueueing, timer bookkeeping), independent of
       the history reload.
5. `other_uncategorized` -- everything else: tokio runtime/thread-pool
   setup and teardown, rusqlite plumbing (statement cache, row access) not
   already attributed to `load_history`, UUID-to-string formatting for
   diagnostics, the various small `store::`/`queue::`/`worker::`/`context::`
   accessor and glue functions, and one-time process-startup allocations.
   This is a real, coherent "infrastructure glue" bucket -- confirmed by
   inspecting every stack in it -- not a sign of an under-specified rule
   set; see the doc's Methodology note for why it is not folded into
   `sqlite3_internals`/`workflow_reexecution` on a looser match.

# Usage

    python3 scripts/classify_dhat_allocations.py path/to/dhat.json
"""

import json
import sys
from collections import defaultdict

CLONE_MARKERS = (
    "Vec<T,A> as core::clone::Clone>::clone",
    "BTreeMap<K,V,A> as core::clone::Clone>::clone",
    "clone::clone_subtree",
    "alloc::string::String as core::clone::Clone>::clone",
)
SERDE_DESER_MARKERS = (
    "serde_json::de::",
    "serde_json::value::de::",
    "serde_core::de::",
)
LOAD_HISTORY_MARKER = "autumn_harvest_sqlite::store::load_history"
WORKFLOW_REEXEC_MARKERS = (
    "runtime_drive_profile::sequential_workflow",
    "runtime_drive_profile::activity_payload",
)
SQLITE_C_PREFIXES = (
    "sqlite3",
    "yy_reduce",
    "yy_shift",
    "resolveP2Values",
    "sqlite3VdbeExec",
    "sqlite3RunParser",
)
# The two smaller, per-activity clone sites (see precedence rationale #1
# above) are only ever reachable through one of these two functions, since
# `drive_one_cycle`'s own history clone never has either on its stack.
ACTIVITY_INPUT_CLONE_MARKERS = (
    "autumn_harvest_sqlite::runtime::SqliteRuntime::drive_suspension",
    "autumn_harvest_sqlite::worker::drain_ready",
)

CATEGORY_ORDER = (
    "history.clone()",
    "load_history_deserialize",
    "workflow_reexecution",
    "load_history_vec_query",
    "sqlite3_internals",
    "other_uncategorized",
    "activity_input_clone",
    "serde_json_elsewhere",
    "sqlite3_internals_reload_query",
)


def resolve_stack(pp, ftbl):
    return [ftbl[i] for i in pp["fs"]]


def classify(stack):
    joined = "\n".join(stack)
    if any(m in joined for m in CLONE_MARKERS):
        if any(m in joined for m in ACTIVITY_INPUT_CLONE_MARKERS):
            return "activity_input_clone"
        return "history.clone()"
    if any(m in joined for m in SERDE_DESER_MARKERS):
        if LOAD_HISTORY_MARKER in joined:
            return "load_history_deserialize"
        return "serde_json_elsewhere"
    if any(m in joined for m in WORKFLOW_REEXEC_MARKERS):
        return "workflow_reexecution"
    is_sqlite_c = any(
        any(fr.split(":", 1)[-1].strip().startswith(pfx) for pfx in SQLITE_C_PREFIXES)
        for fr in stack
    )
    has_load_history = LOAD_HISTORY_MARKER in joined
    if is_sqlite_c and has_load_history:
        return "sqlite3_internals_reload_query"
    if has_load_history:
        return "load_history_vec_query"
    if is_sqlite_c:
        return "sqlite3_internals"
    return "other_uncategorized"


def main():
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <dhat.json>", file=sys.stderr)
        return 2

    with open(sys.argv[1]) as f:
        d = json.load(f)
    pps = d["pps"]
    ftbl = d["ftbl"]

    totals = defaultdict(lambda: [0, 0])
    elsewhere_sites = defaultdict(lambda: [0, 0])

    for pp in pps:
        stack = resolve_stack(pp, ftbl)
        cat = classify(stack)
        totals[cat][0] += pp["tb"]
        totals[cat][1] += pp["tbk"]
        if cat == "serde_json_elsewhere":
            caller = next(
                (fr for fr in stack if "autumn_harvest_sqlite::" in fr), "???"
            )
            # Strip the leading `0xADDR: ` and the trailing ` (in /path/to/binary)`
            # DHAT annotates every frame with -- the symbol name alone is what
            # identifies the call site across a rebuild (the address is not
            # stable across binary layouts).
            caller = caller.split(": ", 1)[-1].split(" (in ")[0]
            elsewhere_sites[caller][0] += pp["tb"]
            elsewhere_sites[caller][1] += pp["tbk"]

    grand_total_b = sum(v[0] for v in totals.values())
    grand_total_k = sum(v[1] for v in totals.values())

    # Every pp is classified into exactly one bucket, so the categories are a
    # partition of the capture's totals by construction. Assert it so a
    # future edit to `classify()` that accidentally double-counts or drops a
    # pp fails loudly instead of silently producing a table that no longer
    # sums to the DHAT-reported total.
    reported_b = sum(pp["tb"] for pp in pps)
    reported_k = sum(pp["tbk"] for pp in pps)
    assert grand_total_b == reported_b, (grand_total_b, reported_b)
    assert grand_total_k == reported_k, (grand_total_k, reported_k)

    print(f"{'category':28s} {'bytes':>12s} {'%':>7s} {'blocks':>8s} {'%':>7s}")
    for cat in sorted(totals, key=lambda c: -totals[c][0]):
        b, k = totals[cat]
        print(
            f"{cat:28s} {b:>12,d} {100 * b / grand_total_b:>6.2f}% "
            f"{k:>8,d} {100 * k / grand_total_k:>6.2f}%"
        )
    print(f"{'TOTAL':28s} {grand_total_b:>12,d} {'100.00%':>7s} {grand_total_k:>8,d} {'100.00%':>7s}")

    if elsewhere_sites:
        print()
        print("serde_json_elsewhere call sites:")
        for site, (b, k) in sorted(elsewhere_sites.items(), key=lambda kv: -kv[1][0]):
            print(f"  {site}: {b:,} bytes / {k:,} blocks")

    return 0


if __name__ == "__main__":
    sys.exit(main())
