## Lean claim tier (follows issue #2680)

A worker claim of one row now sends a lean claim statement in front of the
window statement of issue #2680. A batch claim sends none. See the second
entry of this file. The lean statement drops the plan nodes of
concurrency keys, rate limits, build pins and capability requirements. It
is exact for a seek window whose rows use none of those. When its window
holds a row and it locks none, it claims no row and changes no row, and it
sets a flag. The causes are a row that uses one of those features, a parked
row of a paused or expired run, and rows that peer claims hold. The claim
then sends the unchanged window statement in the same transaction. So a
claim takes the same row with and without the lean statement.

A lean statement that sets its flag takes no bucket lock. It keeps one row
lock in one case: a peer commits a change of a window row between its
snapshot and its row lock. The claim keeps the lock of that skipped row
until its transaction ends, as the window statement does.

- **Text.** `queue::lean_claim_query_for` derives each of the twelve lean
  texts from the final window text of its variant. Each edit replaces an
  exact text and asserts its count, so a changed gate panics at first use.
  No existing claim text changed. The batch claim, the by-id claim and the
  by-execution claim have no lean form.
- **Plan shape.** A second step of the derivation changes how the lean
  text reads the pauses. It reads each pause array through a scalar
  subquery, with no join. It builds each array with `ARRAY(SELECT ...)`,
  with no aggregate. The paused activity array also holds the saturated
  names of `$6`, so each scan has one activity test less. Each step is one
  entry of `LEAN_PLAN_REWRITES`, and it asserts the count of each text that
  it replaces.
- **Dispatch.** `queue::claim_task_with_lean` takes the `LeanSkips` of the
  calling claim loop. A lean flag makes the next claims of that loop send
  the window statement first. Each flag adds 4 claims to the skip, up to
  64. Each lean statement that takes a row takes 1 claim off the next skip.
  So one skip sends one wasted lean statement in 5 of its claims at most. A
  loop of plain and keyed windows in turn moves to the window statement
  first.
  The skip counts claims and reads no clock. Each poll loop and each
  follower holds one skip per shard and per claim kind. A worker with queue
  weights also holds one skip per queue. A caller with no state, such as
  `queue::claim_task`, keeps the window statement first.
- **Switch.** `WorkerConfig::with_claim_lean(false)` turns the lean
  statement off. It is on by default. `GET /admin/config` reports
  `claim_lean`. `WorkerConfig` and `WorkerRuntimeConfig` gain the public
  field `claim_lean`. Add `claim_lean: true` to a struct literal. The
  operator guide is `docs/operations/claim-lean.md`.
- **Census.** The `claim_forms.sql` scripts of
  `docs/perf-artifacts/claim-window-split/` and
  `docs/perf-artifacts/claim-batch/` count the lean form. The lean text
  does not start with the `worker_info` CTE, so the old filter dropped it.
- **Memory.** Each pooled connection keeps one more cached plan for each
  lean variant that it runs, 0.7 MB to 1.0 MB on PostgreSQL 16.
- **No migration, no new event.** The change adds no table, no column and
  no `WorkflowEvent` variant.

Evidence: `formal/verus/src/claim_window.rs` proves the model of the lean
step and the skip bound. `lean_claim_db_tests.rs` compares the lean-first
claim with the window-first claim over random queue states for the twelve
variants, in owner mode and with an open and a closed fence. It commits a
peer claim between the snapshot and the row lock of a lean statement. It
counts the statements of a claim loop on a plain queue, on a queue with a
keyed row at its head, over two claim kinds, and of a worker with queue
weights. It counts the lean claims that a paused queue, a
paused activity and each `$6` name decided. It pins the plan of each lean
text on a deep backlog of one queue, with fresh and with idle statistics.

Measured: `docs/performance-claim-lean-tier.md`. One lean claim alone costs
0.55 ms against 0.80 ms for the window form. On the e2e `throughput` cell,
with one row per claim, the lean form is faster in six of six rounds, by a
median of 1.17 times.

## Batch claim on by default (follows issue #2693)

The batch claim of issue #2693 is now on by default.
`WorkerConfig::default()` sets `claim_batch_max` to the new public const
`builder::DEFAULT_CLAIM_BATCH_MAX`, 4 rows. `worker` re-exports the const.
A claim loop takes up to its share of the free slots in one statement, 2 or
4 rows. A batch claim sends no lean statement. So the lean statement of the
entry above serves the claims of one row.

- **The cap and why.** 4 is the largest batch that the end-to-end bench
  formed. Its worker has 8 workflow permits, 16 activity permits and 2
  claim loops, so a share never passes 4. The worker has no backlog gate
  and no slow start.
- **Switch.** `with_claim_batch_max(1)` keeps one row per claim, and so
  does `0`. `with_claim_batch_max(8)` raises the cap. A value above 8 still
  claims 8 rows at most, and the field keeps the value as given. No
  environment variable or config key sets the cap. A struct literal of
  `WorkerConfig` or `WorkerRuntimeConfig` names its own value:
  `NonZeroU8::new(builder::DEFAULT_CLAIM_BATCH_MAX)` gives the default.
- **Unchanged.** Queue weights, fairness keys, priority ageing, an owned
  workflow claim, an adaptive limit, a cost hold and a dispatch channel
  keep one row per claim. The SQLite backend reads no claim knob. The
  change adds no migration and no `WorkflowEvent` variant.
- **Slot counts.** A Postgres poll holds one permit for each row that it
  may claim, and no task runs on those permits. The slot gauges, the slot
  tuner and the fleet heartbeat read raw `available_permits`, so they
  counted those permits as slots in use. `slot_tuner::PollHeldPermits` now
  counts them, and `slot_tuner::slots_in_use` takes them off each reading.
  `harvest.worker.slots_in_use`, `SlotObservations::in_use` and
  `harvest_workers.in_flight_count` then count the slots that a task holds.
  An idle worker reads 0. `slots_in_use + slots_available` still equals the
  target. A reading keeps the larger of two reads of the count around the
  semaphore read. So a moving poll can hide a task for one sample, and it
  never reads as a task. Admission does not change: the semaphores decide
  what a poll may claim.
- **Public API.** `workers::compute_in_flight` takes a fifth argument,
  `&PollHeldPermits`. `workers::spawn_worker_heartbeat` takes a last
  argument, a `PollHeldPermits`. `slot_tuner::TunedSlot` gains the public
  field `poll_held`. `PollHeldPermits::default()` counts none, for a caller
  that does not poll.
- **Telemetry.** Nothing new is recorded. A batch statement records one
  `harvest.db.query.duration{op="claim"}` sample for all its rows. Its
  `kind` is `workflow` when any row is a workflow task. So the sample count
  is a statement count, and `harvest.queue.dispatched` counts the rows.
- **Bench variable.** `e2e_bench` and the capacity bench keep one row per
  claim when `HARVEST_BENCH_CLAIM_BATCH` is unset, as for `0`, `1` and
  `off`. The published baselines were taken that way, and the reproduction
  check is two-sided. A number above 1 sets that cap. The word `default`
  takes the worker default. The report header prints the claim loops and
  the rows per claim statement. `docs/benchmarks.md` lists the variable and
  the row `rows per claim statement` of the published configuration.
- **Known limits.** One worker can take a burst that single claims spread
  over the fleet: 8 rows in two statements on a default worker. A batch
  whose first row has a rate-limit key holds the row locks of its other
  rows while it waits on the bucket lock. A crash right after a claim gives
  one crash strike to each row of the batch. A claim in flight locks up to
  4 rows of the seek window. Each pooled connection can cache 6 more claim
  plans. `docs/operations/claim-batch.md` states each limit.
- **Supersedes.** `CHANGELOG.md` `[0.11.0]` says of claim slots and the
  batch claim: "Both are off by default." `docs/shipped-work.md` heads the
  entry of issue #2693 "Batch claim, off by default", and it says to add
  `claim_batch_max: None` to a struct literal. The batch claim is now on by
  default. The collation corrects those lines.

Evidence: `claim_batch_db_tests.rs` gains three tests.
`each_batch_text_keeps_a_bounded_plan_on_a_deep_backlog_of_one_queue` pins
the plan of each batch text on a deep backlog, with fresh and with idle
statistics. `random_states_give_a_batch_the_rows_of_a_run_of_single_claims`
compares a batch with a run of single claims over random queue states. A
batch takes a prefix of those rows: each row when the state has no keyed
row, and one keyed row at most, in first place.
`a_default_worker_claims_a_backlog_in_batches_and_one_row_turns_it_off`
runs a worker from `WorkerConfig::default()` on a backlog. In
`lean_claim_db_tests.rs`,
`a_worker_claim_loop_sends_the_lean_form_unless_the_switch_is_off` gains a
leg: an idle default worker sends batch statements only. Unit tests cover
the default (`claim_batch_max_is_on_by_default_and_one_row_is_off`,
`a_worker_from_the_default_config_claims_in_batches`), the kind of a batch
sample and the slot counts.
`an_idle_tuned_worker_with_batch_claims_shows_no_slot_in_use` runs a tuned
worker with batch claims. Some tests count or time single claims, and they
pin one row per claim. They are the claim slot suite, the claim concurrency
suite, two legs of the lean worker test and the owner decider claim count.
The conformance worker takes the default.

Measured: `docs/performance-claim-batch.md`, section "On by default". The
rounds ran on the e2e `throughput` cell: six rotated rounds of one binary
for each load level. A cap of 4 gives 1.09 times the throughput of the lean
single claim at 4 workflows in flight. It gives 1.07 times at 32 and at 64.
It is faster in 4 of 6, 6 of 6 and 6 of 6 rounds. Caps of 2 and 8 measure
the same within noise. Take a queue where every row has a concurrency key.
One batch statement costs 1.16 ms there, against 1.13 ms for the window
statement.
