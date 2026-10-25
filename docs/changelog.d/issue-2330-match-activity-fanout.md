## Perf — fan-out replay of `match_activity` is no longer quadratic (issue #2330)

`HistoryMatcher::scan_activity_terminal` walked from each schedule to its
terminal. In a fan-out of N activities, each walk stepped over the later
schedules and the consumed completions, so a replay cost O(N²). The settle
then walked the same range again.

The scan now reads a position index (`activity_scan_index.rs`). The index
lists the stops and progress events of each activity, the interleaved
commands and the barriers. Skip links pass over consumed entries. The scan
still walks its first 16 events one by one, so a sequential history builds no
index.

At N=1000 the `fanout` bench costs 73% fewer instructions, and at N=2000 85%
fewer. A sequential history costs 0.8% more. See
`docs/performance-match-activity-fanout.md`.

Replay semantics do not change. A differential property test compares the
new scan with a copy of the old one over random histories, matcher calls and
cut views. A Verus proof (`formal/verus/src/scan_skip.rs`) shows that the
skip query returns the first unconsumed entry. The Verus mirror guard now
drops loop clauses (`invariant`, `invariant_except_break`, `ensures` and
`decreases`), so a proof can cover a loop.

No migration, no new `WorkflowEvent` variant, and no `harvest_events` change.
