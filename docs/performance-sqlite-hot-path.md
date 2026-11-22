# `SqliteRuntime` hot path: compiled statements, warm history, fewer syncs

**Outcome: six changes shipped.** A short run costs 65% fewer
instructions. A long run costs 76% fewer. A run on a file syncs 27% less
often. No stored format or public signature changed. One index of the task
table changed, and the core crate has one new entry point.

| Shape | Measure | Before | After | Change |
|---|---|---:|---:|---:|
| Short run, 3 activities, memory | instructions per run | 5,322,065 | 1,836,334 | −65.5% |
| Short run, 1 activity, memory | instructions per run | 2,351,069 | 788,078 | −66.5% |
| Short run, 10 activities, memory | instructions per run | 18,560,469 | 6,153,532 | −66.8% |
| Short run, 3 activities, file | instructions per run | 5,385,270 | 1,900,745 | −64.7% |
| Short run, 3 activities, file | `fsync` calls per run | 11.18 | 8.20 | −26.7% |
| Short run, 3 activities, file | `pwrite` calls per run | 123.80 | 137.55 | +11.1% |
| Long run, 20 activities | instructions, whole process | 52,273,836 | 23,008,791 | −56.0% |
| Long run, 80 activities | instructions, whole process | 386,055,883 | 105,344,806 | −72.7% |
| Long run, 160 activities | instructions, whole process | 1,310,122,645 | 317,525,918 | −75.8% |

Every number is an instruction count from `valgrind --tool=callgrind` or a
system call count from `strace -c`. Both are deterministic. Wall-clock time
is not evidence on a shared machine, as
[`performance-sqlite-runtime-drive.md`](performance-sqlite-runtime-drive.md)
explains. An instruction count moves under 1% between two runs, because
`HashMap` takes a random seed.

For context only, the in-memory rate on the test machine went from about
940 to about 3,000 runs per second, and the file rate from about 270 to
about 400. The disk of that machine syncs in a fraction of a millisecond.
A disk with a slower sync gains more from the lower sync count.

## Workloads

Two harnesses, both `harness = false` binaries.

- **Short runs:** `benches/runtime_throughput_profile.rs` (new). It opens
  one database and drives `M` runs of `k` activities each. The difference
  between `M = 300` and `M = 100`, divided by 200, is the steady cost of one
  run. The schema setup cancels out. The cost stays flat as the tables grow:
  it is 1,919,770 per run between 900 and 1,500 runs, against 1,836,334
  between 100 and 300.
- **Long run:** `benches/runtime_drive_profile.rs`, unchanged. It drives one
  run of `n` sequential activities on a new database.

## Finding 1: each statement was compiled on each use

`sqlite3LockAndPrepare` was 58.19% of the instructions of a short run. Two
causes added up.

**The drive did not use the statement cache.** Most statements went through
`Connection::execute` and `Connection::query_row`. Those compile the SQL
text on each call. The fix sends each statement of the drive path through
`prepare_cached`, and raises the cache from the default 16 statements to
128, above the number that the path uses. That alone took a run from
5,322,065 to 4,650,815 instructions, far less than the profile promised.

**`SQLite` compiled a cached statement again on each bind.** After the
first fix, `sqlite3Reprepare` was 50.92% of the instructions. The bundled
`SQLite` 3.54 reads a bound value while it plans an index range. It then
marks the statement so that the next bind of that parameter expires it. A
drive binds a run id to each statement, so each cached statement was
compiled again on each use. A probe confirmed it with
`sqlite3_stmt_status(SQLITE_STMTSTATUS_REPREPARE)`: one recompile for each
bind after the first, even when the bound value did not change.

The fix turns on `SQLITE_DBCONFIG_ENABLE_QPSG`, the query planner stability
guarantee, for the runtime connection. With it the planner never reads a
bound value, so a statement keeps one plan. The guarantee gives up plans
that depend on a bound value. Those need `ANALYZE` statistics, a bound
`LIKE` pattern, or a bound `LIMIT`. This schema has no statistics, and no
statement binds a `LIKE` pattern. A run then cost 2,214,132 instructions.

`a_cached_statement_is_not_compiled_again_on_a_bind` guards the setting.
It fails with three recompiles when the setting is off.

The auth store, the overlay log and the agent stores open their own
connections. They are not on the drive path and keep the default.

## Finding 2: each decision cycle parsed the whole history

A cycle read and parsed every stored event of its run. A run of `n`
activities has `n + 1` cycles, so it parsed about `n²` events. The earlier
page measured this and left the fix for a decision.

The runtime now keeps the parsed history of each run that it drives
(`history_cache.rs`). A cycle takes the entry, reads the rows after the
cached tail, and gives the entry back. `read_from` runs that one statement
for a cold load and for a warm load. A run that ends leaves the cache.

Three rules keep a cached prefix equal to the stored rows.

1. The runtime connection only appends. The guard triggers refuse an
   `UPDATE` of an event, and no code outside the crate can use that
   connection.
2. A commit of another connection drops every entry.
   `HistoryCache::same_data_version` reads `PRAGMA data_version` after the
   delta rows. A changed value drops the cache.
3. A load checks that the first row it reads is the cached tail row, by
   sequence, length and hash. A purged run has no tail row.

The cache holds at most 16 MiB of stored event JSON by default, least
recently used first. `SqliteRuntime::set_history_cache_bytes` changes the
budget, and zero turns the cache off.

| n | Before this change | After | Change |
|---:|---:|---:|---:|
| 20 | 36,812,324 | 28,052,198 | −23.8% |
| 80 | 322,437,389 | 178,030,630 | −44.8% |
| 160 | 1,183,734,241 | 603,216,072 | −49.0% |

The "before" column already has the compiled statements of finding 1. A
short run gained 5.9% (2,214,212 to 2,084,538), because it has four cycles.

## Finding 3: a claim commit needs no sync

A run of `k` activities on a file made `3k + 2` synced commits: the start,
the end, and for each activity the schedule, the claim and the result. The
claim of a task writes one value, `state = 'RUNNING'`. Each open returns a
`RUNNING` row to `PENDING`. So the file after a reopen is the same with the
claim commit or without it.

`drain_ready` now commits such a claim with `PRAGMA synchronous = NORMAL`
and restores `FULL` before any other commit. `RelaxedSync::begin` lowers
the level, `RelaxedSync::restore` raises it, and `RelaxedSync::drop` raises
it on an unwind. In WAL mode a commit that is not synced survives a crash
of the process. A power loss can drop it, and then the task is `PENDING`,
as after a reopen. The next synced commit also syncs the claim, because
the WAL is one sequence.

Three cases keep the sync:

- The claim of a late-registered task also freezes its defaults (issue
  #1068). A lost freeze would let a changed registration set the contract
  of a task whose body already ran.
- A replicated file (`open_replicated`). A frame that the file can lose
  must not reach a replica.
- A file that is not in WAL mode, where each sync keeps the file
  consistent.

Measured with `strace -f -c -e trace=fsync`: 11.18 calls per run before and
8.18 after, for `k = 3`. The fraction matches the syncs of a WAL
checkpoint each 1,000 pages.

## Finding 4: seven reads of one row

A cycle read the state, the run settings, the cancel request, the input,
the workflow name, the workflow id and the parent of its run with seven
statements. `cycle_row` reads them with one. Nothing writes the run row
between those reads, so the values are the same. A run went from 2,084,538
to 1,941,874 instructions (−6.8%).

## Finding 5: the claim read every task of its run

`claim_next_ready_task_tx` asks for the oldest ready `PENDING` task of a
run. The only index that could serve it was on `exec_id`. A run keeps its
`DONE` rows until retention. So each claim read every task row of the run
and sorted the ready ones in a temporary B-tree. A run of `n` activities
made `2n` claims over up to `n` rows each.

The schema now has `idx_harvest_tasks_exec_state_seq` on
`(exec_id, state, seq)`, and drops `idx_harvest_tasks_exec`. The claim
seeks to its first row and sorts nothing. The prefix of the new index
serves each statement that names only the run, so the old index had no use
left. The wake read (`next_wake_after`) gains too: it now reads only the
`PENDING` tasks of each running run.

| Long run | Before the index | After | Change |
|---:|---:|---:|---:|
| 20 activities | 27,455,768 | 26,719,025 | −2.7% |
| 80 activities | 175,943,120 | 162,327,796 | −7.7% |
| 160 activities | 599,062,183 | 543,932,748 | −9.2% |
| 320 activities | 2,219,378,375 | 1,991,575,100 | −10.3% |

At 160 activities the claim went from 41,209,731 instructions to
9,874,508. That is 128,780 for each claim before and 30,858 after, and the
cost no longer grows with the run.

**The cost.** The index holds the state, so each state change of a task
moves one index entry. A short run pays that and gains little, because its
run has few task rows:

| Short run | Before the index | After | Change |
|---|---:|---:|---:|
| 1 activity, instructions per run | 804,427 | 808,942 | +0.6% |
| 3 activities, instructions per run | 1,955,426 | 1,936,072 | −1.0% |
| 10 activities, instructions per run | 7,264,673 | 7,093,489 | −2.4% |
| 3 activities, file, `pwrite` calls per run | 124.62 | 138.94 | +11.5% |
| 3 activities, file, `fsync` calls per run | 8.18 | 8.20 | +0.2% |

The claim commit and the result commit each write one more WAL page, the
index leaf. The number of syncs does not change. The small rise in `fsync`
is a checkpoint that comes sooner, because the WAL fills faster.

A fleet drive of 20 runs of 40 activities with `run_until_idle` went from
1,060,883,491 to 982,501,997 instructions (−7.4%).

The first open of an older file builds the index once from its stored
tasks.

`the_claim_seeks_the_first_pending_task_and_sorts_nothing` guards the
plan. `the_schema_replaces_the_task_index_of_an_older_file` covers the
older file.

## Finding 6: each cycle copied the whole history for the executor

The core entry point took the history by value, and the drive needs the
history again after the call. So each cycle gave the executor a deep copy
and then dropped it. At 160 activities the copy and its drop were 39.0% of
the instructions.

The copy bought one thing. The history matcher moves the output out of each
matched `ActivityCompleted`, so the workflow gets it with no copy
([`performance-replay.md`](performance-replay.md)). A shared history, such
as one behind an `Arc`, would lose that move and still copy each output.

The core crate now has `run_workflow_lending_history`. The drive lends its
history, and the cycle returns it in `LentCycle::history`. A lent matcher
copies the output of each matched activity and leaves the event whole. That
is the one copy that the workflow needs. The drive makes no other.

The matcher gives the history back only when it holds exactly the events
that it got. An open view cut of a cancelled scope and a resident append
both change the count. The drive then reads the stored history again. No
test of the crate reaches that path through a workflow, so
`a_history_that_does_not_come_back_is_read_again` calls it directly.

| Long run | Before | After | Change |
|---:|---:|---:|---:|
| 20 activities | 26,716,285 | 23,008,791 | −13.9% |
| 80 activities | 162,332,209 | 105,344,806 | −35.1% |
| 160 activities | 543,921,219 | 317,525,918 | −41.6% |
| 320 activities | 1,991,590,340 | 1,091,424,226 | −45.2% |

| Short run | Before | After | Change |
|---|---:|---:|---:|
| 1 activity | 808,942 | 788,078 | −2.6% |
| 3 activities | 1,936,072 | 1,836,334 | −5.2% |
| 10 activities | 7,093,489 | 6,153,532 | −13.3% |

The fleet drive of 20 runs of 40 activities went from 982,501,997 to
682,043,015 instructions (−30.6%).

The same change compares activity ids as values in
`history_has_activity_scheduled`. It made a text form of each recorded id
on each schedule.

Every other caller keeps `run_workflow_with_state_history_policy_and_caps`,
and its matcher still moves each output. The Postgres worker is one of
them. It builds its context itself and can keep a workflow resident, so it
needs its own change to lend a history.

## What is left

At 160 activities, after these changes:

| Cost | Share |
|---|---:|
| The workflow body runs again from its start in each cycle | 74.0% |
| `SQLite` runs the statements | 11.5% |

- **The replay.** A replay from the start is the execution model. Its cost
  here includes the copy of each recorded output. The resident workflow of
  the Postgres worker (issue #1798) avoids the replay there.

## Reproduce

```bash
cargo bench -p autumn-harvest-sqlite --no-run
BIN=target/release/deps/runtime_throughput_profile-<hash>

# Instructions per run: the difference of two run counts, divided by 200.
for M in 100 300; do
  SQLITE_THROUGHPUT_WORKFLOWS=$M valgrind --tool=callgrind \
    --branch-sim=no --cache-sim=no --callgrind-out-file=cg.$M "$BIN"
done

# Syncs per run on a file database.
for M in 100 300; do
  SQLITE_THROUGHPUT_DB=/var/tmp/harvest-bench SQLITE_THROUGHPUT_WORKFLOWS=$M \
    strace -f -c -e trace=fsync,fdatasync "$BIN"
done

# The long run.
SQLITE_RUNTIME_PROFILE_N=160 valgrind --tool=callgrind --branch-sim=no \
  --cache-sim=no target/release/deps/runtime_drive_profile-<hash>
```

`SQLITE_THROUGHPUT_ACTIVITIES` sets `k`. `SQLITE_THROUGHPUT_MODE=idle`
starts every run first and drives the fleet with `run_until_idle`. That
mode went from 5,417,867 to 2,011,603 instructions per run.

## See also

- [`performance-sqlite-runtime-drive.md`](performance-sqlite-runtime-drive.md):
  the first profile of this path, which found the history reload.
- [ADR 0019](adr/0019-sqlite-drive-path.md): the decisions and their
  limits.
- [`sqlite-backend.md`](sqlite-backend.md), section 10: what a commit
  guarantees.
