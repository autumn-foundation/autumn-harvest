## Perf — the external, timer and child matcher scans are no longer quadratic (issue #2338)

Three `HistoryMatcher` scans walked history one event at a time on each
call, so a fan-out replay cost O(N²):

- `scan_ahead_for_external_signal_terminals` walked to the end of history
  before every `match_*` call while an external signal, cancel or await was
  stashed.
- The fire scan of `match_timer_strict` stepped over the later timer starts
  and the consumed fires.
- The terminal scan of `match_child_workflow` stepped over every later child
  start.

All three now read the scan index of issue #2330 (`activity_scan_index.rs`).
The index keeps the external terminals in one skip list, and the fires and
child terminals per id. The timer scan runs through the activity scan code,
because the rules of the two scans agree on every event class. The
ski-rental rule of #2330 applies, so a sequential history builds no index.

At N=1000, `fanout_extsig` costs AFTER_EXTSIG_PCT fewer instructions,
`timers` AFTER_TIMERS_PCT fewer and `children` AFTER_CHILDREN_PCT fewer. See
`docs/performance-match-activity-fanout.md` and `docs/design/DESIGN-2338.md`.
The local activity scan and the external signal, cancel and await scans are
linear already. A scaling test guards all of them.

Replay semantics do not change. The differential property test of #2330 now
runs copies of the old external, timer and child scans as its oracle.

No migration, no new `WorkflowEvent` variant, and no `harvest_events` change.
