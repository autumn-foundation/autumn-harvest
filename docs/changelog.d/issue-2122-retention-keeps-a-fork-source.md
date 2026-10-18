## Engine — retention keeps the source of a recorded fork (issue #2122)

**Problem.** A recorded fork reads its source history while it runs.
Retention did not know about forks. It could delete a terminal source under a
live fork. Each later activity of the fork then failed closed with
`ForkEffectUnavailable`.

**Fix.** Retention keeps each run in the fork lineage of a non-terminal
recorded fork. The run becomes a candidate again when each such fork is
terminal. A live fork reads no source, so it keeps nothing.

- `fork::is_pinned_by_recorded_fork` walks down from a run through fork rows,
  for at most 64 links. Each step probes `idx_harvest_we_fork_source` (#2000).
  The last `WorkflowForked` marker of a fork sets its mode. A fork with no
  marker counts as recorded.
- The per-candidate gate `should_skip_candidate` calls it before archival.
- The delete transaction calls it again after its `FOR UPDATE` row lock and
  returns `SkippedForkSource`. Each new lineage member locks its ancestors
  `FOR SHARE`, so a fork made after the scan cannot slip past the delete.
- The whole lineage stays, not only the record source. A reset or a new fork
  of the live fork walks that lineage and refuses a gap.
- The `LiveFork` rebalance guard and retention read one state list,
  `fork::live_fork_states_sql!`.

**Runbook.** `docs/runbooks/fork-a-run.md` drops the workaround. The check
reads existing rows, so forks made before the upgrade are covered.

**No migration. No new `WorkflowEvent` variant. No replay impact.** A source
can now outlive its retention age while a recorded fork runs.

**Tests.** DB tests in `tests/integration/fork_tests.rs`: a live recorded
fork keeps its source, the source goes once each fork ends, a live-mode fork
keeps nothing, a fork of a fork and a reset of a fork keep their lineage, and
two race tests. In one, the archiver forks the source after the scan. In the
other, a fork holds the source lock while the delete waits. Both race tests
fail without the delete re-check. A unit test pins the shared state list.
Design record: `DESIGN-2122.md`.
