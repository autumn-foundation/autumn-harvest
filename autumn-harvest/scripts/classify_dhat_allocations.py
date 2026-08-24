#!/usr/bin/env python3
"""Deterministic allocation-site categorizer for `debugger_trace_profile`'s
DHAT capture -- the auditable script that produces the "Allocation-site
attribution" table in `docs/performance-debugger-trace.md`.

Mirrors `autumn-harvest-sqlite/scripts/classify_dhat_allocations.py`'s
approach and methodology: DHAT's JSON output (schema version 2) is a list of
"program points" (`pps`) -- one per unique resolved allocation call stack --
each carrying total bytes/blocks (`tb`/`tbk`) and a list of frame indices
(`fs`, INNERMOST-first) into a flat, already-symbolized frame table
(`ftbl`). Frame `fs[0]` is always the allocator entry point itself
(`malloc`/`calloc`/`posix_memalign`, intercepted via DHAT's `LD_PRELOAD`
shim) -- `fs[1]` is therefore the *true, proximate* Rust-level call site
that triggered the allocation, and `fs[2:]` its ancestor chain going back up
to `main`. This script resolves each `pp`'s stack to frame-name strings and
assigns it to exactly one category via a fixed, PRECEDENCE-ORDERED sequence
of marker checks -- first match wins, so the categories are a
mutually-exclusive, exhaustive partition of the capture's total bytes/blocks
by construction (asserted before printing).

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
   would otherwise be miscategorized into `workflow_reexecution` (bucket 6
   below) -- a genuinely different cost that happens once per activity, not
   once per activity PER STEP.

2. `history_projection_one_time_build` -- `ReplayTrace::from_history_capped`,
   `resolved_payload`, `HistoryAccumulator` (`::apply`/`::open_awaitables`),
   or `normalized_event_facts` anywhere on the stack. Confirmed by reading
   `trace_snapshot`'s body in `debugger.rs`: `from_history_capped` is called
   EXACTLY ONCE, as the very first statement, *before* the per-step loop
   begins -- it iterates every event once (`O(n)` total, never `O(n^2)`) to
   build the handler-free `DebugStep` skeleton, cloning each event's
   accumulator fields (`version_gates`/`patch_gates`/`side_effects`/
   `markers`) and its `resolved_payload` (a `serde_json::Value` field
   clone) along the way. Checked SECOND -- ahead of bucket 3 below -- because
   `resolved_payload`/`HistoryAccumulator`/`normalized_event_facts`'s
   `serde_json::Value`/`BTreeMap` machinery would otherwise satisfy bucket
   3's "trace_snapshot appears somewhere in the stack + a clone/to_vec
   marker also appears somewhere in the stack" check purely because
   `trace_snapshot::{{closure}}` is `from_history_capped`'s caller and
   therefore always sits higher up in the SAME resolved stack --
   conflating a real, but ONE-TIME, `O(n)` decode cost with the genuinely
   `O(n^2)` per-step mechanism bucket 3 is named for. (Confirmed
   empirically, not just from the doc comment: grepping DHAT's raw `ftbl`
   for these four names finds them as their OWN distinct, un-inlined
   symbols in the release binary -- unlike bucket 4's members below, none
   of this bucket's functions were inlined away, so this separation is a
   precise, load-bearing text match rather than an approximation.)

3. `trace_snapshot_prefix_clone` -- EITHER (a) `fs[1]` (the allocation's
   proximate, immediate caller -- see the module docstring above) is
   `trace_snapshot::{{closure}}` itself, with no further Rust-named callee
   in between -- checked HERE, unconditionally, immediately after bucket
   2 -- OR (b) `trace_snapshot` appears anywhere on the stack together
   with a `to_vec`/`Clone`/`clone` marker, a whole-stack substring
   fallback checked LATER in the code, AFTER buckets 4 and 5 below have
   had a chance to claim the pp first (see the "KNOWN FIX" note at the
   end of this bullet for why). Both conditions identify the same
   mechanism: the per-step `snapshot.events[..=step.index].to_vec()` call
   (plus the per-step `input.clone()` beside it) in `trace_snapshot`'s
   loop -- at step `k` it clones `k + 1` `WorkflowEvent`s (many carrying
   `serde_json::Value` payloads, `BTreeMap`-backed with no
   `preserve_order` feature) into a fresh owned `Vec` handed to
   `replay_prefix`.

   Condition (a) exists because LLVM's release-profile inlining is not
   uniform across call sites of the same generic function (confirmed
   empirically: `<T as ...>::to_vec_in::ConvertVec>::to_vec` DOES survive
   as its own distinct frame for SOME of this loop's `to_vec()` calls, at
   others it is inlined away entirely and the allocation's proximate frame
   is `trace_snapshot::{{closure}}` directly with zero intervening
   Rust-named callee -- at n=80 this specific shape accounts for a single
   161-block/2.19MB program point, ~2 allocations/step across 80 steps,
   consistent with the loop's two per-step allocating statements). Reading
   frame `fs[1]` directly is DIRECT, first-hand evidence for the true
   proximate cause -- stronger than condition (b)'s "somewhere in the
   ancestor chain" substring match -- and without it, this class of
   allocation was previously falling through every other bucket and being
   swept into `tokio_runtime` below purely because `CurrentThread::block_on`
   (an ancestor of literally every allocation in this `rt.block_on(...)`-
   wrapped workload) happens to also appear somewhere in its 30-frame
   capture.

   KNOWN FIX, empirically confirmed and measured (in response to a
   further Codex review comment): condition (b)'s whole-stack substring
   search cannot distinguish "`trace_snapshot` is a distant ANCESTOR of
   this allocation" from "`trace_snapshot::{{closure}}` is this
   allocation's own proximate cause" -- the identical ambiguity already
   fixed for `tokio_runtime` below (bucket 8), and for the same root
   reason: `render_command`/`command_payload` (bucket 5's own named
   descendants -- they run nested inside `replay_prefix`, itself called
   from `trace_snapshot`'s per-step loop, so `trace_snapshot` genuinely IS
   an ancestor of their allocations too, just not the most specific one)
   were being swept into THIS bucket by condition (b) purely because
   `trace_snapshot` also appears somewhere in their ancestor chain.
   Measured directly at n=80 against the marker set this script shipped
   with before this fix: 166,467 bytes (0.37% of the 45,300,388-byte
   capture, 7 program points, every one resolving to
   `render_command`/`command_payload` further up the stack) were
   misattributed this way -- small, but real, and previously undetected.
   Fixed identically to bucket 8's fix: condition (a) stays checked HERE
   (a stack's own immediate proximate caller can only ever be one
   function, so it structurally cannot collide with -- or need to defer
   to -- bucket 4/5's descendant markers), but condition (b) moved, in
   the code, to run AFTER buckets 4 and 5 (see their notes below), so a
   pp naming one of their more specific descendants is claimed there
   first. The remaining 29,110,827 bytes (99.63% of this bucket's total
   after the fix, dominated -- 90.73% of the fallback subset -- by
   `BTreeMap::clone::clone_subtree` sitting directly at the proximate
   frame, with `String::clone` and `to_vec_in::ConvertVec::to_vec` making
   up the rest) are unaffected by this fix and remain correctly
   attributed to this bucket: the review comment that prompted this
   investigation additionally questioned whether this bucket's reported
   share as a whole could be trusted at all without excluding named
   descendants -- it can, for the vast majority of it; only the narrow
   166,467-byte slice above needed correcting.

4. `history_matcher_internals` -- `HistoryMatcher`, `match_history`, or
   `drive_query_replay` on the stack -- checked AHEAD of bucket 5 below
   AND ahead of bucket 3's condition (b) fallback above (see that
   bullet's "KNOWN FIX" note), because these frames are always *nested
   inside* `replay_prefix`'s own `drive_query_replay_async(...).await`
   (confirmed by reading `replay_prefix`'s body in `debugger.rs`), so any
   stack carrying both markers has this as the more specific, innermost
   cause. This is the actual replay-matching machinery that walks the
   (already-owned, already-cloned) prefix and decides what the workflow
   function does next.

   KNOWN LIMITATION, empirically confirmed rather than assumed: in this
   workspace's default `cargo bench` release profile (`debug = false`,
   confirmed unchanged even after rebuilding with `-C debuginfo=2`), every
   function this bucket's markers name is fully inlined away and never
   appears as its own frame in DHAT's resolved call stack -- grepping the
   raw `ftbl` for `HistoryMatcher` / `match_history` / `drive_query_replay`
   / `replay_prefix` / `match_activity` / `scan_activity_terminal` returns
   zero matches at every swept input size, with or without debug info, and
   this bucket observes the same empty marker set (zero pps) regardless of
   its precedence relative to bucket 3's condition (b) or bucket 5 --
   the reordering below fixed a real, measured misattribution for bucket
   5 (see bucket 3's note above), but has no measurable effect on THIS
   bucket specifically, since it never receives any pps either way. So a
   `0` count here means "unobservable by this methodology", not "free" --
   any allocation this code performs is silently folded into whichever
   caller frame survived inlining (most plausibly
   `trace_snapshot::{{closure}}`, landing in bucket 3, or the
   workflow-driving frames feeding bucket 6). See
   `docs/performance-debugger-trace.md`'s "Allocation-site attribution"
   section for the full investigation this note summarizes.

5. `replay_prefix_or_context_build` -- `replay_prefix`,
   `for_replay_canary_with_state`, `register_declarative_handlers`,
   `render_command`, or `command_payload` on the stack (and buckets 2, 4,
   and bucket 3's condition (a) didn't already match). Checked ahead of
   bucket 3's condition (b) fallback above -- see that bullet's "KNOWN
   FIX" note for the empirical misattribution this ordering closes (166
   KB / 7 program points at n=80, all `render_command`/`command_payload`).
   Per-step `WorkflowContext` construction,
   declarative-handler registration, and command-rendering overhead --
   confirmed by reading `replay_prefix`'s body: it is called once per step
   (from `trace_snapshot`'s loop), builds a fresh `WorkflowContext` via
   `for_replay_canary_with_state(...).with_*(...)` chains, then converts
   each drained command to a `CommandSnapshot` via
   `ctx.drain_commands().iter().map(render_command).collect()` --
   `render_command` in turn calls `command_payload` to extract each
   command's JSON payload. All of this is distinct from the prefix clone
   above (a different allocation site, doing different work), but still
   fundamentally `replay_prefix`'s own per-step overhead. In practice this
   bucket is populated mostly via `for_replay_canary_with_state`/
   `render_command`/`command_payload`: `replay_prefix` itself is also
   inlined away (see bucket 4's note above) and never survives as its own
   frame either.

6. `workflow_reexecution` -- `sequential_workflow` or `activity_payload` on
   the stack (build_history already excluded by bucket 1's precedence).
   This is the WORKFLOW'S OWN CODE re-running from the top of its `for`
   loop on every step's fresh canary replay -- inherent to "step k is a
   fresh replay of `events[0..=k]`", not debugger overhead. Directly
   parallel to `runtime_drive_profile`'s `workflow_reexecution` category.

7. `fmt_format_machinery` -- `core::fmt`/`alloc::fmt` on the stack (harness
   `println!`/`format!` overhead, and any `Display`/`Debug` formatting
   reachable from the traced call).

8. `tokio_runtime` -- a `tokio::` frame appears among `fs[1]`/`fs[2]` (the
   allocation's own near-immediate caller), NOT merely anywhere in the
   full 30-deep joined stack. Async runtime construction/task/waker
   machinery genuinely IS the proximate cause here (`Builder::build`,
   `BlockingPool::new`, `Wheel::new`, ...) for a handful of one-time,
   process-startup allocations.

   KNOWN FIX, empirically confirmed and measured (not just theorized):
   `tokio::runtime::scheduler::current_thread::CurrentThread::block_on`
   drives every `.await` point in `main()`'s `rt.block_on(debugger
   .trace_snapshot(snapshot))`, so it is a legitimate ANCESTOR frame of
   literally every allocation the whole program makes while the traced
   future is running -- a distant-ancestor-only check therefore also
   catches genuinely `trace_snapshot`-caused and `serde_json`-serialization
   -caused allocations that merely happen to run "underneath" that
   `.await`. Measured directly at n=80 (before this bucket's members 2-5
   above existed, i.e. against the marker set this script shipped with
   before this precision pass): restricting the check to a whole-stack
   substring search classified 3,042,973 bytes (6.72% of the capture) as
   `tokio_runtime`, of which 99.7% (all but ~8.7KB) resolved, at `fs[1]`,
   to `trace_snapshot::{{closure}}` itself (one single 2,190,888-byte/
   161-block program point -- 72% of the bucket on its own) or to
   `BTreeMap`/`serde_json::Value` serialization frames reached through
   `normalized_event_facts`/`command_payload`/`WorkflowEvent::serialize`
   (the remaining ~28%) -- with the `tokio::` marker only appearing 5-15
   frames up the ancestor chain in every one of those cases. Every
   GENUINELY tokio-caused allocation in the same capture, by contrast, has
   its `tokio::` frame at `fs[1]` (depth 1) with zero exceptions. Buckets 2
   and 5 above now claim most of that misattributed volume by name; this
   bucket's depth-limited check additionally guarantees anything it still
   doesn't name explicitly falls into `other` (bucket 9) rather than being
   mislabeled as async runtime overhead.

9. `other` -- everything else. Printed with its own top-N breakdown by
   bytes so nothing is silently swept under an unaudited catch-all.
"""

import json
import sys


def resolve(pp, ftbl):
    return [ftbl[i] for i in pp.get("fs", [])]


def classify(joined: str, frames: list[str]) -> str:
    if "build_history" in joined:
        return "harness_fixture_setup"
    # Checked before bucket 3: a one-time, O(n) cost that would otherwise be
    # conflated with the genuinely O(n^2) per-step prefix clone below --
    # see this bucket's docstring note above.
    if (
        "from_history_capped" in joined
        or "resolved_payload" in joined
        or "HistoryAccumulator" in joined
        or "normalized_event_facts" in joined
    ):
        return "history_projection_one_time_build"
    # frames[0] is always the allocator entry point; frames[1] is the true,
    # proximate Rust-level call site. Reaching trace_snapshot's own closure
    # directly there (condition (a)) is stronger, first-hand evidence than
    # the whole-stack substring search (condition (b), checked further
    # below, AFTER buckets 4 and 5) -- checked here, unconditionally,
    # because a stack's own immediate caller can only ever be one function
    # and therefore cannot collide with -- or need to defer to -- bucket
    # 4/5's descendant markers below. See this bucket's docstring "KNOWN
    # FIX" note above.
    if len(frames) > 1 and "trace_snapshot::{{closure}}" in frames[1]:
        return "trace_snapshot_prefix_clone"
    # Checked BEFORE replay_prefix_or_context_build (and before bucket 3's
    # condition (b) fallback below): HistoryMatcher / match_history /
    # drive_query_replay frames are always nested *inside* replay_prefix's
    # own drive_query_replay_async(...).await, so on a stack carrying both
    # markers this is the more specific, innermost cause -- see bucket 4's
    # docstring note above for why this precedence is, empirically,
    # unreachable in a release-profile capture regardless of ordering
    # (both markers are fully inlined away).
    if (
        "HistoryMatcher" in joined
        or "match_history" in joined
        or "drive_query_replay" in joined
    ):
        return "history_matcher_internals"
    # Checked BEFORE bucket 3's condition (b) fallback below:
    # render_command / command_payload / replay_prefix /
    # for_replay_canary_with_state / register_declarative_handlers are
    # more specific, nested descendants of trace_snapshot's per-step
    # loop -- condition (b) below cannot tell "trace_snapshot is a
    # distant ancestor" from "trace_snapshot is the proximate cause", so
    # it must defer to a more specific match here first. See bucket 3's
    # docstring "KNOWN FIX" note above for the empirical misattribution
    # (166,467 bytes / 7 program points at n=80, all
    # render_command/command_payload) this ordering closes.
    if (
        "replay_prefix" in joined
        or "for_replay_canary_with_state" in joined
        or "register_declarative_handlers" in joined
        or "render_command" in joined
        or "command_payload" in joined
    ):
        return "replay_prefix_or_context_build"
    # Condition (b): the whole-stack substring fallback for
    # trace_snapshot_prefix_clone, deliberately checked LAST among buckets
    # 3-5 -- see bucket 3's docstring "KNOWN FIX" note above.
    if "trace_snapshot" in joined and (
        "to_vec" in joined or "Clone" in joined or "clone" in joined
    ):
        return "trace_snapshot_prefix_clone"
    if "sequential_workflow" in joined or "activity_payload" in joined:
        return "workflow_reexecution"
    if "core::fmt" in joined or "alloc::fmt" in joined:
        return "fmt_format_machinery"
    # Depth-limited: only the allocation's own near-immediate caller, not a
    # distant ancestor via block_on -- see bucket 8's docstring note above.
    if any("tokio::" in f for f in frames[1:3]):
        return "tokio_runtime"
    return "other"


def main() -> None:
    path = sys.argv[1] if len(sys.argv) > 1 else "dhat.json"
    with open(path, encoding="utf-8") as fh:
        d = json.load(fh)
    ftbl = d["ftbl"]

    order = [
        "harness_fixture_setup",
        "history_projection_one_time_build",
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
        frames = resolve(pp, ftbl)
        joined = "|".join(frames)
        key = classify(joined, frames)
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
