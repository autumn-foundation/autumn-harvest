## Engine — retention keeps the source of a recorded fork (issue #2122)

**Problem.** A recorded fork reads its source history while it runs.
Retention did not know about forks. It could delete a terminal source under a
running recorded fork. Each later activity of the fork then failed closed
with `ForkEffectUnavailable`.

**Fix.** Retention keeps each run in the fork lineage of an open recorded
fork. An open fork is not terminal, or it is `FAILED` with a dead letter that
a redrive can reopen. The run becomes a candidate again when each such fork
is closed. A live-mode fork reads no source, so it keeps nothing.

- `fork::is_pinned_by_recorded_fork` walks down from a run through fork rows,
  for at most 65 links. Each step probes `idx_harvest_we_fork_source` (#2000).
  It reads the markers only when an open fork exists. So a run with no fork
  never reads `harvest_events`.
- The last `WorkflowForked` marker of a fork sets its mode. A fork with no
  marker counts as recorded.
- The per-candidate gate `should_skip_candidate` calls it before archival.
- The delete transaction calls it again after its `FOR UPDATE` row lock. For
  a pinned run, it returns `SkippedForkSource`. Each new lineage member locks
  its ancestors, so a fork made after the gate cannot slip past the delete.
- The delete transaction now pins `READ COMMITTED`. A `REPEATABLE READ`
  session default would hide a fork that committed during the lock wait.
- The whole lineage stays, not only the record source. A reset or a new fork
  of the running fork walks that lineage and refuses a gap.
- The `LiveFork` rebalance guard and retention share one predicate,
  `fork::open_fork_sql!`, and one 65-link bound. So the rebalance guard now
  also counts a dead-lettered `FAILED` fork, and it reaches the 65th link.

**Runbook.** `docs/runbooks/fork-a-run.md` drops the workaround for recorded
forks. It keeps the legal-hold advice for live-mode forks.

**No migration. No new `WorkflowEvent` variant. No replay impact.** A source
can now outlive its retention age while a recorded fork runs.

**Tests.** DB tests in `tests/integration/fork_tests.rs` cover each case. A
running recorded fork keeps its source. The source goes once each fork ends.
A live-mode fork keeps nothing. A dead-lettered fork keeps its source. A fork
of a fork and a reset of a fork keep their lineage. A 65-link lineage keeps
its root. Two race tests cover the delete re-check. In one, the archiver
forks the source after the gate. In the other, a fork holds the source lock
until the delete waits, under a `REPEATABLE READ` session default. Design
record: `docs/design/DESIGN-2122.md`.
