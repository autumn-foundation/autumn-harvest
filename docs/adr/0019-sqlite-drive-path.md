# ADR 0019: The SQLite drive path keeps plans, history and one sync fewer

## Status

Accepted.

## Context

- A profile of `SqliteRuntime` showed that 58% of the instructions of a
  short run compiled SQL. See
  [the hot path page](../performance-sqlite-hot-path.md).
- The bundled `SQLite` compiles a cached statement again when a parameter
  is bound, if its planner read a bound value. Each drive statement binds a
  run id.
- Each decision cycle read and parsed the whole history of its run.
- The backend claims that a committed transaction survives a power loss.
  It opened each file with `synchronous = FULL`, so each commit was synced.
  A run of `k` activities made `3k + 2` synced commits.

## Decision

1. **One plan for each statement.** The runtime connection turns on the
   query planner stability guarantee (`SQLITE_DBCONFIG_ENABLE_QPSG`). The
   drive path sends each statement through `prepare_cached`, and the cache
   holds 128 statements.
2. **A warm history.** The runtime keeps the parsed history of each run
   that it drives, within a byte budget. A cycle reads only the rows after
   the cached tail. The default budget is 16 MiB of stored JSON.
   `set_history_cache_bytes(0)` turns the cache off.
3. **A claim commit is not synced.** The commit that only sets a task
   `RUNNING` runs at `synchronous = NORMAL`. Every other commit stays at
   `FULL`.
4. **The task index follows the claim.** `idx_harvest_tasks_exec_state_seq`
   on `(exec_id, state, seq)` replaces the index on `exec_id` alone. The
   claim seeks to the oldest `PENDING` task of its run.
5. **The executor lends the history back.** The drive calls the new core
   entry point `run_workflow_lending_history`. The cycle reads the history
   and returns it. The drive no longer gives the executor a deep copy. The
   Postgres worker lends the history of each cold decision the same way.
6. **A parked run has one history.** The Postgres worker and a resident
   workflow share the history of the run. The workflow holds no events
   between two decisions. Each decision lends the history and gets it back.

## Why each decision is safe

**The plan guarantee.** It turns off only the plans that depend on a bound
value. Those need `ANALYZE` statistics, a bound `LIKE` pattern or a bound
`LIMIT`. The schema has no statistics, and no statement binds a pattern.

**The warm history.** A cached prefix must equal the stored rows.

- The runtime connection only appends to `harvest_events`. The guard
  triggers refuse an `UPDATE`, and no code outside the crate has that
  connection.
- A commit of another connection changes `PRAGMA data_version`. A load
  reads it after the delta rows, and a changed value drops every entry.
- A load checks the cached tail row by sequence, length and hash. A purge
  removes every event of a run, so its tail row is gone.

The version is read before the rows that it guards. A version read after
them could include a commit that those rows do not show.

A cached event came from a committed row, read outside a transaction. So a
failed or a discarded cycle leaves the cache right.

**The claim commit.** A claim of a frozen task writes one value,
`state = 'RUNNING'`. Each open returns a `RUNNING` row to `PENDING`. So the
file after a reopen is the same with the claim commit or without it.

- In WAL mode a commit that is not synced survives a crash of the process.
  Only a power loss or a crash of the operating system can drop it.
- The WAL is one sequence. The next synced commit also syncs the claim. So
  a heartbeat or a result is never durable without its claim.
- The schedule commit of an activity stays synced. The task row and its
  activity id are thus durable before the body runs.

Three cases keep the sync on a claim:

- The claim of a late-registered task freezes its defaults (issue #1068).
  The relaxed transaction rolls such a claim back, and a synced claim
  takes the task.
- A file opened with `open_replicated`. A replicator ships WAL frames, and
  a frame that the file can lose must not reach a replica.
- A file that is not in WAL mode.

**The task index.** It holds the same rows as the index that it replaces,
and its first column is the same. So each statement that the old index
served still has an index. The schema builds it with
`CREATE INDEX IF NOT EXISTS` on each open, as it builds every other index.

**The lent history.** The history matcher changes its events in three
places, and a lent matcher makes each one safe.

- It moves the output out of a matched `ActivityCompleted`. A lent matcher
  copies the output and leaves the event whole. The workflow owns the
  output that it gets, so that one copy is needed in any design.
- A cancelled scope hides a tail of the events while its body runs
  (`cut_view`). A drop guard puts the tail back. A matcher with an open cut
  gives nothing back.
- A resident workflow appends events (`append_consumed`). A matcher that
  holds more events than it got gives nothing back.

When the matcher gives nothing back, the SQLite drive reads the stored
history again. Every other caller of the owning entry point keeps it, and
its matcher still moves each output.

**The one history of a parked run.** The Postgres worker can keep a
suspended workflow resident (issue #1798). That workflow keeps its matcher.

- A drive gives every event back, parked or not. The matcher records the
  count, and it then holds no events.
- Nothing reads the matcher of a parked workflow. Its future is not
  polled, and the next decision checks the delta against the parked
  awaits, which the workflow keeps apart from the matcher.
- A warm decision lends the history again. The matcher takes it only when
  the count is the one that it gave back. Another count declines the
  resume, and the decision replays cold.
- Each step of a resume takes its events of the delta by a move. The
  matcher counts them, so it gives the longer history back.
- The resume puts every event back in the history of the worker, in
  order, after a success and after a decline.

The check is a count, not a comparison of each event. The worker lends the
history of the same run, which only grows by appends. So the first events
of that history are the events that the matcher gave back.

A drive of the worker always ends with the events that it got. A view cut
is closed when its poll returns, and a warm resume counts each event that
it appends. So the worker treats a history that does not come back as a
broken invariant. It fails the task, and a later attempt loads the history
again.

## Consequences

- A short run costs 65% fewer instructions. A long run costs 76% fewer. A
  run on a file makes `2k + 2` synced commits.
- Each state change of a task moves one index entry. A short run on a file
  makes 12% more `pwrite` calls for that, with no more syncs. A claim no longer
  grows with the tasks of its run.
- The first open of an older file builds the task index once from its
  stored tasks.
- The runtime holds up to the cache budget of parsed history in memory.
- A statement that later binds a `LIKE` pattern on the runtime connection
  gets no index range from that pattern. Give such a statement its own
  connection, or a literal pattern.
- A new place in the history matcher that changes an event must keep the
  event whole when the matcher is lent, or it must change the event count.
- A parked run of the Postgres worker holds one history, not two. A warm
  decision costs 24% to 26% fewer instructions.
- A resident workflow of the worker resumes only through
  `resume_lending_with`. New code that reads the matcher of a parked
  workflow finds no events.
- A new writer of `harvest_events` on the runtime connection must only
  append. A writer that changes or removes a stored row of a live run must
  clear the cache.
- The commit of a result stays synced. A design that syncs a group of
  commits once would cut more, and it would widen the window in which a
  body can run again. It needs its own decision.

## Alternatives

- **A history behind an `Arc`.** The matcher needs to change its events,
  so shared events would need a copy on each change, or a matcher that
  never changes them. Each read of the event list in the matcher would change
  too. A shared history would still copy each matched output. So it would
  cost more work and save no more than a lent one.

- **A partial index on `(exec_id, seq)` for `PENDING` tasks.** It is
  smaller, and a finished task leaves it. The statements that name only
  the run would still need the index on `exec_id`. So the table would
  carry two indexes, and a task would write as many index pages.

- **Compile each statement by hand and keep the handles.** It gives the
  same result as the cache with more code.
- **Skip the claim transaction.** A task would never show `RUNNING`. An
  operator reads that state, and a dropped drive is reclaimed through it.
- **`synchronous = NORMAL` for every commit.** A power loss could then
  drop a committed result. That breaks the durability claim.
- **Check the cache with a row count.** The count reads each index entry
  of the run, so each cycle would still cost time in proportion to the
  history.

## Proof

- `history_cache::tests::every_load_equals_a_full_load_under_mixed_writes`
  mixes appends, foreign rewrites, purges and evictions. Each load equals
  a full load.
- `a_warm_run_sees_an_event_that_another_connection_rewrote` fails when
  the data version check is off.
- `a_deferred_claim_leaves_the_freeze_to_a_synced_claim` shows that the
  relaxed transaction never commits a freeze.
- `a_failed_deferred_claim_restores_the_sync_level` and
  `a_dropped_relaxed_sync_restores_the_sync_level` show that each exit
  restores `FULL`.
- `a_cached_statement_is_not_compiled_again_on_a_bind` guards the plan
  guarantee.
- `the_claim_seeks_the_first_pending_task_and_sorts_nothing` guards the
  plan of the claim.
- `a_lent_matcher_returns_its_events_unchanged` and
  `a_lent_matcher_returns_nothing_under_an_open_cut` cover the matcher.
  `a_lent_cycle_matches_an_owned_cycle_and_returns_the_history` compares
  the two entry points. `a_history_that_does_not_come_back_is_read_again`
  covers the drive when the matcher gives nothing back.
- `a_released_matcher_takes_its_events_again` and
  `restore_refuses_events_that_the_matcher_did_not_release` cover the
  matcher of a parked workflow. `assert_warm_matches_cold` and
  `assert_arrivals_match_cold` drive each resident test workflow with a
  lent history too, and compare each returned history with the stored one.
  `a_workflow_that_shares_its_history_declines_a_resume_with_none` and
  `a_lent_resume_declines_a_history_that_the_workflow_did_not_give_back`
  cover the two refusals.
