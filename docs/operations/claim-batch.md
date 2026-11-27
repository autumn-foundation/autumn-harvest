# Batch claim

When almost every claim finds a row, the claim loop sets the throughput of
a worker. A batch claim lets one claim statement take 2, 4 or 8 task rows.

The batch claim is on by default. A claim loop takes up to 8 rows in one
statement (`builder::DEFAULT_CLAIM_BATCH_MAX`). A claim of one row is a
single claim. A worker with the batch claim off sends single claims only.

## Change the cap or turn it off

```rust
use autumn_harvest::builder::{DEFAULT_CLAIM_BATCH_MAX, WorkerConfig};

let rows = |config: WorkerConfig| config.claim_batch_max.map(std::num::NonZeroU8::get);
// The default: up to 8 rows.
assert_eq!(rows(WorkerConfig::default()), Some(DEFAULT_CLAIM_BATCH_MAX));
assert_eq!(DEFAULT_CLAIM_BATCH_MAX, 8);
// Off: one row per claim.
assert_eq!(rows(WorkerConfig::default().with_claim_batch_max(1)), None);
// A lower cap: up to 4 rows, the earlier default.
assert_eq!(rows(WorkerConfig::default().with_claim_batch_max(4)), Some(4));
```

- `with_claim_batch_max(n)` sets the field `claim_batch_max`, an
  `Option<NonZeroU8>`. `0` and `1` set `None`, one row per claim.
- A batch holds 2, 4 or 8 rows. A value above 8 claims at most 8 rows. The
  field keeps the value as given.
- The code is the only switch. No environment variable, CLI flag or
  config-file key sets the cap.
- The knob belongs to each worker. Workers with different values can share
  a shard, and no migration is needed.
- `GET /admin/config` reports the value as `claim_batch_max`.
- `WorkerRuntimeConfig` has the same field. A struct literal of
  `WorkerConfig` or `WorkerRuntimeConfig` names its own value.
  `NonZeroU8::new(builder::DEFAULT_CLAIM_BATCH_MAX)` gives the default, and
  `None` gives one row per claim.
- The SQLite backend reads no claim knob. The value has no effect there.

## Batch size

Each claim loop takes its share of the free permits of each kind that it
may claim. The share is the free permits over `max_concurrent_claims`,
rounded up. The batch is the smallest share over those kinds, capped at
`claim_batch_max`. It then rounds down to 2, 4 or 8. A share under 2 sends
the single claim.

A default worker has 20 workflow permits, 50 activity permits and two claim
loops. Its idle claim loop has a share of 10 rows, so it asks for 8 rows,
the default cap. With a cap of 4 it asks for 4.

An idle default worker therefore sends the batch statement on each poll. It
sends no [lean statement](claim-lean.md). A loop with one free permit of
one kind claims one row, however many permits of the other kind are free.

Under load the share is smaller. A busy default worker has few free permits
per loop, so its share rarely passes 4. Its batches are then no larger than
at a cap of 4. A worker with many free slots per loop forms batches of 8.

The loop holds one permit per row across the claim. So a slot tuner shrink
cannot take the permit of a claimed row. The loop gives back the permits of
the rows that it did not get. At the default cap one loop holds up to 8
permits of each kind, 16 in all. The two loops of a default worker hold up
to 32.

No task runs on a permit that a poll holds. So the slot gauges, the slot
tuner and the `in_flight_count` of the worker row leave those permits out.
See [Telemetry](../telemetry.md).

## Forms that keep one row per claim

The default claim is the claim with no priority ageing, no fairness keys
and no owned gate. The name is older than the batch default. It names a
statement family, not the claim of a default worker. Only the default claim
has a batch form.

A worker claims one row per statement when any of these applies:

- queue weights (`with_queue_weights`);
- fairness keys (`with_fairness_keys`). See
  [`fairness-keys.md`](../fairness-keys.md);
- priority ageing (`with_priority_aging_secs` above zero). See
  [`claim-order.md`](claim-order.md);
- an adaptive limit on any activity type (`with_adaptive_limit`);
- a cost hold (`with_cost_hold`). See [`cost-deferral.md`](../cost-deferral.md);
- a dispatch channel, such as [Redis dispatch](redis-dispatch.md).

In owner mode, an owned workflow claim takes one row. When the owner decider
runs on a shard, the poll beside it claims activities only, and those can
batch.

## How it works

`queue::claim_task_batch_with_slots` sends the batch claim. Its text
derives from the window form of the default claim. Three parts change:

1. `seek_candidate` locks up to 2, 4 or 8 rows with
   `FOR UPDATE SKIP LOCKED`.
2. `candidate` keeps those rows in claim order. It stops before the first
   row after row 1 that carries a concurrency key or a rate-limit key. That
   is the key cut.
3. `run_expired_now` lists the rows whose run passed its deadline. The
   claim takes only the other rows.

**The key cut.** The cap re-check of the claim reads the statement
snapshot. Two rows of one concurrency key would both pass it. One
rate-limit bucket row also takes one debit, however many claims join it. So
only the first kept row of a batch may carry a key. A key or a bucket meets
at most one row of a batch. The cut keeps the longest such prefix.

**An expired run stops only its own row.** The claim reads the clock again
after it takes the rate-limit bucket lock. A run can pass its deadline while
the claim waits on that lock. That row then stays `PENDING`. It spends no
token and takes no concurrency-key lock. The claim still takes the other
rows of the batch. The materialized CTE `claim_live` holds the live rows,
so the claim never tests the key lock of a row that it then drops.

Each claimed row passes the post-claim rechecks of a single claim. The
rechecks of all rows share one pipeline. A row that they give back is
hinted after the commit. When the batch finds no row and sets the fallback
flag, the full statement runs and takes one row at most.

A batch claim takes one [claim slot](claim-slots.md) per statement, as a
single claim does. A claim that gets no slot sends no statement.

`formal/verus/src/claim_batch.rs` proves the batch size bounds and the key
cut. See [`testing/formal-methods.md`](../testing/formal-methods.md).
`claim_batch_db_tests.rs` holds the database tests:

- `a_run_that_expires_during_the_claim_stops_only_its_own_row` and
  `an_expired_row_takes_no_key_lock_and_its_peers_still_claim` check the
  expired-row rule.
- `each_batch_text_keeps_a_bounded_plan_on_a_deep_backlog_of_one_queue`
  pins the plan of each batch text, with fresh and with idle statistics.
- `random_states_give_a_batch_the_rows_of_a_run_of_single_claims` compares
  a batch with a run of single claims over random queue states. A batch
  takes a prefix of the rows that the single claims take. With no keyed row
  in the state, it takes each of them. It holds one keyed row at most, in
  first place.
- `a_default_worker_claims_a_backlog_in_batches_and_one_row_turns_it_off`
  proves the default and the switch.

## Measurements

Each figure is a reference measurement from one 4-core box, not an SLO. See
[`performance-claim-batch.md`](../performance-claim-batch.md).

**Against the lean single claim.** These rounds ran on the `throughput`
cell of the bench: six rotated rounds of one binary for each load level.
The table compares a cap of 4, the first default, with the lean single
claim, which is a worker with `with_claim_batch_max(1)`.

| workflows in flight | throughput, cap of 4 over the lean single claim | rounds faster |
|--:|--:|--:|
| 4 | 1.09x | 4 of 6 |
| 32 | 1.07x | 6 of 6 |
| 64 | 1.07x | 6 of 6 |

Caps of 2 and 8 measure the same as 4, within noise. The bench worker has 8
workflow permits, 16 activity permits and two claim loops. No share passes
4 there, so 4 is the largest batch that the bench formed. That is why the
first default was 4.

**A cap of 8 against a cap of 4.** More rounds gave the bench worker more
slots, with two claim loops. The median of the six per-round ratios of a
cap of 8 over a cap of 4:

| worker slots, workflow / activity | workflows in flight | cap of 8 over cap of 4 | rounds faster | rows per batch statement, cap of 4 to cap of 8 |
|:--|--:|--:|--:|--:|
| 20 / 50, the default | 80 | 0.98x | 2 of 6 | 3.37 to 3.34, batch and window together |
| 20 / 50, the default | 160 | 0.99x | 3 of 6 | not taken |
| 64 / 128 | 256 | 1.16x | 6 of 6 | 3.92 to 7.63 |

On the default worker the share rarely passes 4. So its rows per statement
stay about the same, and a cap of 8 measures the same within noise. On the
64 / 128 worker it forms batches of 8 and is faster. That is why the
default is 8. There the cap-of-8 arm held 206 to 212 of 256 workflows in
flight, under the 90% gate of the bench, so its rate understates it. The
census of the default worker counts the window and batch statements
together. Both texts carry `claim_window_fallback`.

**Against the window-first claim.** An older run compared a cap of 8 with
one row per claim, before the lean statement existed. The median rose from
100.0 to 118.7 workflows/s over six alternating pairs, and every pair was
faster. Claim statements fell 37%, and their execution time fell 29%. One
statement for eight rows costs about 1.19 times one statement for one row.
Its single claim sent the window statement first, so the gain over the lean
single claim is smaller.

**A queue of keyed rows.** On a queue where every row has a concurrency
key, one batch statement costs 1.16 ms against 1.13 ms for the window
statement, as `pgbench` transaction latency. The key cut keeps one row of
such a batch.

**A backlog on a default worker.** In one run of the default worker test,
a backlog of 120 tasks took 43 claim statements that took a row. One run at
the first default cap of 4 took 44. With one row per claim it took 120.

## Choose a value

The default is 8. Keep it unless one of these applies. A small or busy
worker needs no lower cap for its own sake. Its share rarely passes 4, so
it forms batches of 2 or 4 by itself, as at a cap of 4.

- **You compare the two, or you rule the batch out of a problem.** Set
  `with_claim_batch_max(1)`. Each claim then takes one row.
- **A small backlog must spread over the fleet.** One worker can take a
  burst that single claims spread over the claim loops of the fleet. See
  the first known limit. `with_claim_batch_max(4)` halves the burst of one
  statement. One row per claim narrows it most.
- **Each row of a queue has a concurrency key or a rate-limit key.** The
  key cut keeps one row per statement, so the batch gains nothing there.
  The measured cost of the statement is about the same. Set
  `with_claim_batch_max(1)` when the row locks of the rows that the cut
  drops matter.
- **Many workers claim one deep head of one kind.** See the limit on window
  rows below. `with_claim_batch_max(4)` locks up to 4 rows per claim, not 8.
- **A crash strike on the rows beside a poison row matters.** See the limit
  on crash strikes below. `with_claim_batch_max(4)` gives a strike to 3 more
  rows at most, not 7.
- **Memory per connection is tight.** `with_claim_batch_max(4)` caches 6
  batch plans per connection, not 9. See [Memory](#memory).

The batch gains most when most claims find a row and the claim loop is the
limit. It gains little when the queue is often empty, or when a worker has
few free permits per loop.

## Known limits

- **One worker can take a burst.** A share reads the free permits of its
  own worker only. Nothing bounds it by the size of the fleet. The two
  loops of an idle default worker can take up to 16 rows of a burst in two
  statements, 8 for each loop. With one row per claim, each of those rows
  is its own race between the claim loops of the fleet. In the worst case
  one default worker fills its 20 workflow slots and 50 activity slots, 70
  rows, while its peers idle. That takes 9 statements at the least, 70
  over 8 rounded up. At a cap of 4 it takes 18. [Sticky
  routing](../sticky-routing.md) then keeps the next workflow tasks of
  those runs on that worker for the sticky window, 5 s by default.
- **A rate-limit key in first place holds the other rows.** A batch whose
  first row has a rate-limit key can wait on the bucket lock. During the
  wait it holds the row locks of the other rows of the batch. That is up
  to 7 rows at the default cap and 3 at a cap of 4. A peer claim skips
  those rows. Their `started_at` is the start of the claim transaction, so
  their start-to-close and heartbeat clocks run during the wait. The claim
  transactions of the peers on the same bucket bound the wait.
- **A crash right after a claim gives a strike to each row of the batch.**
  The reclaim gives one crash strike to each `RUNNING` row of a dead
  worker, started or not. That is not new, and it covers every running row
  of the worker. Take a row that kills the process when its handler
  starts. The batch adds the rows after it in the same statement: up to 7
  at the default cap and 3 at a cap of 4. The default
  `poison_pill_threshold` is 3 strikes.
- **More window rows are locked per claim in flight.** `seek_candidate`
  locks up to 8 rows of the 32-row seek window at the default cap. A single
  claim locks 1. So fewer claims in flight can hold a whole head of one
  kind: 4 at the default cap of 8, and 8 at a cap of 4. The next claim then
  falls back to the full statement and takes one row at most. On a deep
  head of one kind with several workers, more claims can fall back. On the
  bench, with one worker and the knob at 8, full statements rose from 4 to
  19 per run. No fleet run has measured this.
- **A claim that finds one row costs more than the lean single claim.** A
  batch statement of 4 costs about 1.09 times a window statement: 1.104 ms
  against 1.014 ms. A batch statement of 8 costs about 1.19 times a window
  statement: 1.209 ms. A lean statement costs about 0.69 of a window
  statement: 0.55 ms against 0.80 ms on the harness of
  [its own page](../performance-claim-lean-tier.md). The two pairs come
  from different harnesses, so a ratio across them is an estimate. The
  bench cell at 4 workflows in flight did not show a loss. Its worker never
  forms a batch of 8, so no low-load cell has measured one.
- **Each connection caches more plans.** See [Memory](#memory).
- Rows of equal claim key rank by id inside a batch. A single claim takes
  any of them.
- `seek_candidate` locks rows that the cut then drops. A peer claim skips
  such a row for one statement.
- The batch does not split by kind.

## Memory

Each pooled connection caches one plan for each batch text that it runs.
The batch has one text for each claim kind and each batch size. So one
connection can cache this many batch plans:

| `claim_batch_max` | batch sizes | batch plans per connection |
|--:|:--|--:|
| 2 or 3 | 2 | 3 |
| 4 to 7 | 2 and 4 | 6 |
| 8 or more | 2, 4 and 8 | 9 |

A claim with a DR fence runs the fenced texts, a second set of the same
size. These plans are in addition to the lean, window and full plans of a
claim of one row. A worker still sends that claim when a share is under 2.

The size of a batch plan is not measured. A batch text derives from the
window text, so its plan is at least as large as a window plan.
[Lean claim](claim-lean.md#memory) puts a window plan at about 1.4 MB to
2.0 MB on PostgreSQL 16. By that estimate, a default worker adds 13 MB to
18 MB per connection, 9 plans of 1.4 MB to 2.0 MB. A claim with a DR fence
adds a second set, up to 25 MB to 36 MB in all. `with_claim_batch_max(4)`
caps it at 8 MB to 12 MB, or 17 MB to 24 MB with a fence.
`with_claim_batch_max(1)` keeps a connection from building them.

## See also

- [`claim-slots.md`](claim-slots.md): a fleet-wide cap on the claims that
  run at once on one shard.
- [`claim-lean.md`](claim-lean.md): the lean statement in front of the
  single claim. A batch claim sends no lean statement.
- [`docs/architecture.md`](../architecture.md#sharding): the claim loops of
  a worker.
