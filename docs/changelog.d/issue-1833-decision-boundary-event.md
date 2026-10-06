## Phase — decision-boundary event records build and worker per decision (issue #1833)

A history now shows which build and which worker made each decision.

- **New `WorkflowEvent` variant:** `DecisionCommitted { build_id, worker_id }`,
  added at the end of the enum. No migration.
- **Write path:** the Postgres worker appends one boundary in the transaction
  that persists the decision outcome, after the outcome events. A decision
  that appends no event writes no boundary. The row stages no `NOTIFY`, so
  `last_event_type` does not change. `store::append_decision_boundary` shares
  the DR write fence with `append_events_with_codecs`.
- **Replay:** `HistoryMatcher::new` marks every boundary consumed, like pause
  and resume. `terminal_failure_tail_start` skips a boundary, so the issue
  #952 failure tail still holds. A resident workflow skips boundaries in its
  delta. `reset --last-workflow-task` never picks a boundary. A history
  written before this release replays without change.
- **Display:** the history export keeps both fields in redacted mode.
  `harvest debug` shows `decision: build …, worker …`, and `DebugStep` has a
  new `decision` field. `harvest debug diff` ignores boundary attribution.
  Vantage and the Mermaid diagram label each boundary.
- **Rollout:** a worker older than this release fails an execution whose
  history holds a boundary. `HarvestBuilder::record_decision_boundaries(false)`
  and `WorkflowHistoryPolicy::with_decision_boundaries(false)` turn boundaries
  off until every worker runs this release. They are on by default.
- **Storage:** about 131 bytes of `event_data` and 438 bytes on disk with
  indexes per boundary. A boundary counts toward the event hard cap, the
  continue-as-new threshold and the history byte quota. See
  `docs/decision-boundaries.md`.
- **Scope:** the SQLite backend writes no boundaries.
- **Tests:** `event::tests::decision_committed_*`,
  `replay::tests::decision_boundaries_*` and `trailing_boundary_*`,
  `resident::tests::a_decision_boundary_in_the_delta_is_skipped`,
  `reset::tests::last_workflow_task_skips_a_decision_boundary_after_the_terminal`,
  `decision_boundary_replay_tests` (pre-#1833 fixture),
  `decision_boundary_db_tests` (live worker, opt-out, storage measurement),
  `debugger_tests`, `debug_cli` and the Vantage label test.
