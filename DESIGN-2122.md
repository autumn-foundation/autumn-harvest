# Design — Issue #2122: Retention keeps the source of a recorded fork

A recorded fork reads its source history while it runs. Retention did not
know about forks. It could delete a terminal source under a running recorded
fork. Each later activity of the fork then failed closed with
`ForkEffectUnavailable`.

This change makes retention keep a run while an open recorded fork has the
run in its fork lineage. The run becomes a candidate again when each such
fork is closed.

**No migration. No new `WorkflowEvent` variant. No replay impact.** The
partial index `idx_harvest_we_fork_source` from #2000 serves the lookup.

## Terms

- **Recorded fork.** A fork whose last `WorkflowForked` marker is not `live`.
  A fork with no marker counts as recorded.
- **Live-mode fork.** A fork whose last marker is `live`. It reads no source.
- **Open fork.** A fork that can still run. Its state is `RUNNING`, `PAUSED`,
  `MIGRATING` or `MIGRATED`. Or it is `FAILED` with a dead letter that a
  redrive can reopen.
- **Closed fork.** Any other fork.

---

## 0. Planning record

### 0.1 Facts found before the plan

- Retention deletes a run in one place: `delete_candidate_execution`. It
  locks the row `FOR UPDATE` as its first read.
- The partition sweeper drops a partition only when no row in it has a live
  owner. A kept execution row therefore keeps its history.
- The candidate scan uses `FOR UPDATE SKIP LOCKED`. A source that a fork
  holds `FOR SHARE` is skipped, not selected.
- Every new member of a fork lineage locks each ancestor. A fork locks its
  source `FOR SHARE` in `load_fork_source`. A reset locks its source `FOR
  UPDATE`. Both lock the other ancestors `FOR SHARE` in
  `erased_fork_ancestor`.
- `idx_harvest_we_fork_source` indexes `(start_source_ref) WHERE start_source
  = 'fork'`. Migration `20261011200537` already creates it, with the
  `migration_lock_safety` annotation.
- The last `WorkflowForked` event of a fork names its record source and its
  mode. A reset of a fork names the sealed fork in `start_source_ref` and the
  original source in its marker.
- The record source is always in the `start_source_ref` lineage of the fork.
- A reset or a new fork walks the whole lineage of its source. It refuses a
  gap with `LineageGap`.
- `erased_fork_ancestor` reads up to 64 ancestors. The new row adds one link,
  so a stored lineage can hold 65 links.
- A DLQ redrive reopens a `FAILED` run. It deletes the dead letter in the
  same commit.
- The `LiveFork` rebalance guard pins the lineage of each open fork.

### 0.2 Brainstorm — how can retention know about a fork?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Walk down from the candidate through fork rows on the index. Pin it when an open recorded fork is below it. | **Adopted.** It reads only fork rows and needs no new column. |
| B2 | Add a `record_source_id` column to the execution row. | Rejected. A hot-table column, a backfill for old forks, and a new write on each fork path. |
| B3 | Add a `NOT EXISTS` to the candidate scan SQL. | Rejected. A correlated recursive query per scanned row. The per-candidate gate is the existing seam for a dependency. |
| B4 | Pin only the source that the marker names. | Rejected. A reset of the running fork walks the whole lineage. A gap above the record source makes that reset fail. |
| B5 | Place a legal hold on the source at fork time. | Rejected. A hold is an operator record. Retention would also need to release it. |
| B6 | Snapshot the source outcomes into the fork. | Rejected in `DESIGN-2000.md` (B3). Nested payloads would skip the codec and erasure. |
| B7 | One open-fork predicate for retention and rebalance. | **Adopted.** Both guards then mean the same "open fork". |
| B8 | Read the markers in a second query, only for open forks. | **Adopted.** A run with no fork never reads `harvest_events`. |

### 0.3 Reverse brainstorm — how could this go wrong?

| Failure | Guard |
|---------|-------|
| A fork commits after the per-candidate gate. Retention deletes the source. | The delete transaction checks again after its row lock. A test forks from inside the archiver. |
| A fork is in flight at the delete. The re-check reads a snapshot from before its commit. | The re-check is a separate statement after the lock. The delete pins `READ COMMITTED`. A test holds a fork open until the delete waits, under a `REPEATABLE READ` session default. |
| A live-mode fork pins a source that it never reads. | The marker query takes only forks whose last marker is not `live`. A test covers it. |
| A closed fork pins its source for ever. | The open-fork filter takes only open forks. The walk itself passes through closed forks. A test ends the forks and expects a delete. |
| A dead-lettered fork is `FAILED`. Retention deletes its source. A redrive reopens the fork, and it fails closed. | A `FAILED` fork with a non-`CALLBACK` dead letter is open. A test covers it. |
| A reset of a fork pins nothing, because its marker names a run that `start_source_ref` does not name. | The walk follows `start_source_ref` through each fork row. A test resets a fork. |
| The walk stops one link short of the deepest stored fork. | The walk bound is 65 links, one past the read bound. A test builds a 65-link lineage. |
| The walk scans every execution. | Each step probes `idx_harvest_we_fork_source`. |
| The check reads `harvest_events` for each candidate. On the partitioned layout, that probes each partition. | The marker query runs only when an open fork exists. |
| A malformed reference loops the walk. | The walk stops at 65 links. |
| A fork with no readable marker is not seen. | A missing marker counts as recorded, as `history_is_recorded_fork` does. |
| Retention and rebalance drift apart on "open". | One macro holds the predicate. A unit test checks that the open and the closed states cover every state. |

### 0.4 Six thinking hats

- **White (facts).** One delete path. One index exists. Forks lock their
  ancestors. Section 0.1 lists the rest.
- **Red (feel).** The fix must be small. A schema change for a liveness gap
  feels too heavy.
- **Black (risk).** The pin keeps data longer than the retention policy says.
  An operator who sees an old source can think retention is broken. The
  runbook says why the source stays.
- **Yellow (value).** A recorded fork no longer fails closed when its source
  ages out. A reset of a running recorded fork keeps its lineage. Forks made
  by an earlier build are covered, because the check reads existing rows.
- **Green (options).** A later issue can add a metric for pinned candidates,
  or show the pin in the run summary.
- **Blue (process).** Red tests first, then the gate and the re-check. Then
  the review fixes, each with a test that fails first.

## 1. Decision

`fork::is_pinned_by_recorded_fork(conn, exec_id)` answers one question: does
an open recorded fork have `exec_id` in its fork lineage?

1. The first query walks down from `exec_id` through fork rows, for at most
   65 links. It returns the open forks. It reads only execution rows and
   dead letters.
2. When that list is not empty, the second query reads the last marker of
   each fork. One recorded fork pins the run.

`open_fork_sql!` holds the open-fork predicate. The `LiveFork` rebalance
guard reads it too, with the same 65-link bound.

Retention calls the check twice:

1. In `should_skip_candidate`, before archival. A pinned run is a routine
   skip.
2. In `delete_candidate_execution`, after the `FOR UPDATE` row lock. A pinned
   run returns `SkippedForkSource`, and nothing is touched.

## 2. Why the re-check closes the race

A new lineage member locks each ancestor in the transaction that inserts it.
Retention locks the candidate `FOR UPDATE`. The two locks conflict, so one of
two orders holds:

- The fork locks first. Retention waits until the fork commits. The re-check
  is a new statement under `READ COMMITTED`, so its snapshot holds the fork.
  Retention skips.
- Retention locks first. The fork waits until the delete commits. It then
  finds no row and refuses with `NotFound` or `LineageGap`.

A redrive takes no lineage lock. It needs none: it deletes the dead letter
and reopens the fork in one commit. Each snapshot sees an open fork.

## 3. Out of scope

- A live-mode fork does not pin its lineage. A reset or a new fork of it can
  then refuse with `LineageGap`. Use a legal hold for that.
- A fork whose source was deleted before this change still fails closed.
- The SQLite backend has no forks.

## 4. Tests

DB tests in `tests/integration/fork_tests.rs`:

- `retention_keeps_the_source_of_a_running_recorded_fork`
- `retention_deletes_the_source_once_its_recorded_forks_end`
- `retention_does_not_keep_the_source_of_a_live_mode_fork`
- `retention_keeps_the_source_of_a_dead_lettered_recorded_fork`
- `retention_keeps_the_lineage_of_a_recorded_fork`
- `retention_keeps_the_lineage_of_a_reset_of_a_recorded_fork`
- `retention_keeps_the_root_of_the_deepest_lineage`
- `a_fork_committed_after_the_scan_keeps_the_source`
- `a_fork_in_flight_at_the_delete_keeps_the_source`

Each test that keeps a run then closes the forks and expects a delete. So a
kept run is a skip, not a failed delete.

Unit tests in `fork.rs` check that the open and the closed states cover every
state, and that the stored bound is one past the read bound.
