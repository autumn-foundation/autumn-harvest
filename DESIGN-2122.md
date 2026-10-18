# Design — Issue #2122: Retention keeps the source of a recorded fork

A recorded fork reads its source history while it runs. Retention did not
know about forks. It could delete a terminal source under a live fork. Each
later activity of the fork then failed closed with `ForkEffectUnavailable`.

This change makes retention keep a run while a non-terminal recorded fork has
the run in its fork lineage. The run becomes a candidate again when each such
fork is terminal.

**No migration. No new `WorkflowEvent` variant. No replay impact.** The
partial index `idx_harvest_we_fork_source` from #2000 serves the lookup.

---

## 0. Planning record

### 0.1 Facts found before the plan

- Retention deletes a run in one place: `delete_candidate_execution`. It
  locks the row `FOR UPDATE` as its first statement. Each later statement
  takes a new snapshot under `READ COMMITTED`.
- The partition sweeper drops a partition only when no row in it has a live
  owner. A kept execution row therefore keeps its history.
- The candidate scan uses `FOR UPDATE SKIP LOCKED`. A source that a fork
  holds `FOR SHARE` is skipped, not selected.
- Every new member of a fork lineage locks each ancestor `FOR SHARE`: a fork
  through `load_fork_source` and `erased_fork_ancestor`, a reset of a fork
  through `reject_unclean_fork_lineage`.
- `idx_harvest_we_fork_source` indexes `(start_source_ref) WHERE start_source
  = 'fork'`. Migration `20261011200537` already ships it, with the
  `migration_lock_safety` annotation.
- The last `WorkflowForked` event of a fork names its record source and its
  effects mode. A reset of a fork names the sealed fork in `start_source_ref`
  and the original source in its marker.
- The record source is always in the `start_source_ref` lineage of the fork.
- A reset or a new fork of a live fork walks its whole lineage. It refuses a
  gap with `LineageGap`.
- The `LiveFork` rebalance guard pins the lineage of each non-terminal fork.

### 0.2 Brainstorm — how can retention know about a fork?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Walk down from the candidate through fork rows on the index. Pin it when a live recorded fork is below it. | **Adopted.** It reads only fork rows and needs no new column. |
| B2 | Add a `record_source_id` column to the execution row. | Rejected. A hot-table column, a backfill for old forks, and a new write on each fork path. |
| B3 | Add a `NOT EXISTS` to the candidate scan SQL. | Rejected. A correlated recursive query per scanned row. The per-candidate gate is the existing seam for a dependency. |
| B4 | Pin only the source that the marker names. | Rejected. A reset of the live fork walks the whole lineage. A gap above the record source makes that reset fail. |
| B5 | Place a legal hold on the source at fork time. | Rejected. A hold is an operator record. Retention would also need to release it. |
| B6 | Snapshot the source outcomes into the fork. | Rejected in `DESIGN-2000.md` (B3). Nested payloads would skip the codec and erasure. |
| B7 | One SQL list of live fork states for retention and rebalance. | **Adopted.** Both guards then mean the same "non-terminal fork". |

### 0.3 Reverse brainstorm — how could this go wrong?

| Failure | Guard |
|---------|-------|
| A fork commits after the candidate scan. Retention deletes the source. | The delete transaction checks again after its row lock. A test forks from inside the archiver. |
| A fork is in flight at the delete. The re-check reads a snapshot from before its commit. | The re-check is a separate statement after the lock, so it takes a new snapshot. A test holds a fork open across the delete. |
| A live fork pins its source for ever. | The seed takes only forks whose last marker is not `live`. A test covers it. |
| A terminal fork pins its source for ever. | The seed takes only non-terminal forks. A test ends the forks and expects a delete. |
| A reset of a fork pins nothing, because its marker names a run that `start_source_ref` does not name. | The walk follows `start_source_ref` through each fork row. A test resets a fork. |
| The walk scans every execution. | Each step probes `idx_harvest_we_fork_source`. |
| A malformed reference loops the walk. | The walk stops at 64 links, the bound of each fork lineage check. |
| A fork with no readable marker is not seen. | A missing marker counts as recorded, as `history_is_recorded_fork` does. |
| Retention and rebalance drift apart on "live". | One macro holds the state list. A unit test pins it to the terminal list. |

### 0.4 Six thinking hats

- **White (facts).** One delete path. One index exists. Forks lock their
  ancestors `FOR SHARE`. Section 0.1 lists the rest.
- **Red (feel).** The fix must be small. A schema change for a liveness gap
  feels too heavy.
- **Black (risk).** The pin keeps data longer than the retention policy says.
  An operator who sees an old source may think retention is broken. The
  runbook says why the source stays.
- **Yellow (value).** A recorded fork no longer fails closed when its source
  ages out. A reset of a live fork keeps its lineage. Forks made before the
  upgrade are covered, because the check reads existing rows.
- **Green (options).** A later issue can add a metric for pinned candidates,
  or show the pin in the run summary.
- **Blue (process).** Red tests first, then the gate and the re-check, then
  the shared state list and the docs.

## 1. Decision

`fork::is_pinned_by_recorded_fork(conn, exec_id)` answers one question: does
a non-terminal recorded fork have `exec_id` in its fork lineage?

- A fork is recorded when its last `WorkflowForked` marker is not `live`.
  A fork with no marker counts as recorded.
- A fork is non-terminal when its state is in `live_fork_states_sql!`. The
  `LiveFork` rebalance guard reads the same list.
- The query walks down from `exec_id` through fork rows, for at most 64 links.

Retention calls it twice:

1. In `should_skip_candidate`, before archival. A pinned run is a routine
   skip.
2. In `delete_candidate_execution`, after the `FOR UPDATE` row lock. A pinned
   run returns `SkippedForkSource`, and nothing is touched.

## 2. Why the re-check closes the race

A new lineage member locks each ancestor `FOR SHARE` in the transaction that
inserts it. Retention locks the candidate `FOR UPDATE`. The two locks
conflict, so one of two orders holds:

- The fork locks first. Retention waits until the fork commits. The re-check
  is a new statement, so its snapshot holds the fork. Retention skips.
- Retention locks first. The fork waits until the delete commits. It then
  finds no row and refuses with `NotFound` or `LineageGap`.

## 3. Out of scope

- A live fork does not pin its source. Use a legal hold for that.
- A fork whose source was deleted before the upgrade still fails closed.
- The SQLite backend has no forks.

## 4. Tests

DB tests in `tests/integration/fork_tests.rs`:

- `retention_keeps_the_source_of_a_live_recorded_fork`
- `retention_deletes_the_source_once_its_recorded_forks_end`
- `retention_does_not_keep_the_source_of_a_live_mode_fork`
- `retention_keeps_the_lineage_of_a_reset_of_a_recorded_fork`
- `a_fork_committed_after_the_scan_keeps_the_source`
- `a_fork_in_flight_at_the_delete_keeps_the_source`

A unit test pins `live_fork_states_sql!` to the terminal state list.
