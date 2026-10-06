## Phase — decision-boundary event records build and worker per decision (issue #1833)

A history now shows which build and which worker made each decision.

- **New `WorkflowEvent` variant:** `DecisionCommitted { build_id, worker_id }`,
  added at the end of the enum. No migration.
- **Write path:** the Postgres worker appends one boundary in the transaction
  that persists the decision outcome, after the outcome events. A decision
  that writes no event of its own writes no boundary, even when another
  writer appends meanwhile. The notification counts the boundary in
  `event_count`, and `last_event_type` stays the outcome event. A boundary
  never brings a running history to its event hard cap. `store::append_decision_boundary` shares
  the DR write fence with `append_events_with_codecs`.
- **Replay:** `HistoryMatcher::new` marks every boundary consumed, like pause
  and resume. `terminal_failure_tail_start` skips a boundary, so the issue
  #952 failure tail still holds. A resident workflow skips boundaries in its
  delta. `reset --last-workflow-task` never picks a boundary. A history
  written before this release replays without change.
- **Display:** the history export keeps both fields in redacted mode.
  `harvest debug` shows `decision: build …, worker …`, and `DebugStep` has a
  new `decision` field. `diff_traces` skips boundary steps when it aligns
  two traces, so a recording without boundaries compares clean with one
  that has them. `TraceDivergence::step_index` counts the compared steps.
  Vantage and the Mermaid diagram label each boundary.
- **Opt-in:** boundaries are off by default, as the rolling-deploy
  contract requires. Every process of this release reads them.
  `HarvestBuilder::record_decision_boundaries(true)` and
  `WorkflowHistoryPolicy::with_decision_boundaries(true)` turn them on. Turn
  them on only when no older process reads history. An older worker fails
  an execution that holds one, an older timeout leader cannot enforce its
  activity timeouts, and an older API node or CLI cannot show, export or
  cancel it. A later release turns boundaries on by default. See
  `docs/upgrading/0.8.0.md` §1.1.
- **Storage and limits:** each boundary adds about 131 bytes of
  `event_data`. With indexes, it adds about 438 bytes on disk. A boundary
  counts toward the event hard cap, the continue-as-new threshold, the
  timeout-scanner ceiling, the history-bloat warning and the history byte
  quota. The hard-cap preflight does not count the boundary. The boundary
  write skips the boundary when it would bring a running history to its
  cap. See `docs/decision-boundaries.md`.
- **Scope:** the SQLite backend writes no boundaries.
- **Tests:** `event::tests::decision_committed_*`,
  `replay::tests::decision_boundaries_*` and `trailing_boundary_*`,
  `resident::tests::a_decision_boundary_in_the_delta_is_skipped`,
  `reset::tests::last_workflow_task_skips_a_decision_boundary_after_the_terminal`,
  `decision_boundary_replay_tests` (pre-#1833 fixture),
  `decision_boundary_db_tests` (live worker, default off, storage measurement),
  `debugger_tests`, `debug_cli` and the Vantage label test.
