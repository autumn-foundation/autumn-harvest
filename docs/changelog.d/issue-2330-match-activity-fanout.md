## Perf — fan-out replay of `match_activity` is no longer quadratic (issue #2330)

`HistoryMatcher::scan_activity_terminal` walked from each schedule to its
terminal. In a fan-out of N activities, each walk stepped over the later
schedules and the consumed completions, so a replay cost O(N²). The settle
then walked the same range again.

A scan in a wide fan-out now reads a position index
(`activity_scan_index.rs`). The index lists the stops and progress events of
each activity, the interleaved commands and the barriers. Skip links pass
over consumed entries. A scan leaves its walk only after an interleaved
command, and only once the walks cost more than an index build. So a
sequential history and a small fan-out build no index.

At N=1000 the `fanout` bench costs 72% fewer instructions, and at N=2000 84%
fewer. A sequential history costs 0.7% more. A fan-out of about 32 costs 52%
more matcher instructions, about 45K. See
`docs/performance-match-activity-fanout.md`. Issue #2338 tracks two other
quadratic matcher paths.

Replay semantics do not change. A differential property test compares the
new scan with a copy of the old one over random histories, matcher calls and
cut views. A Verus proof (`formal/verus/src/scan_skip.rs`) shows that the
skip query returns the first unconsumed entry. The Verus mirror guard now
drops loop clauses (`invariant`, `invariant_except_break`, `ensures` and
`decreases`) in a loop header, so a proof can cover a loop.

No migration, no new `WorkflowEvent` variant, and no `harvest_events` change.
