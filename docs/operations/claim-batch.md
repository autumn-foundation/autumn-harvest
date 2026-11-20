# Batch claim (issue #2693)

A claim statement takes one task row by default. When almost every claim
finds a row, the claim loop sets the throughput of a worker. A batch claim
lets one statement take up to 2, 4 or 8 rows. It is off by default.

## Turn it on

```rust
use autumn_harvest::builder::WorkerConfig;

let config = WorkerConfig::default().with_claim_batch_max(8);
assert_eq!(config.claim_batch_max.map(std::num::NonZeroU8::get), Some(8));
```

- `with_claim_batch_max(n)` sets the field `claim_batch_max`, an
  `Option<NonZeroU8>`. `0` and `1` set `None`, one row per claim.
- A batch holds 2, 4 or 8 rows. A value above 8 claims at most 8 rows.
- The knob belongs to each worker. Workers with and without it can share a
  shard, and no migration is needed.
- `GET /admin/config` reports the value as `claim_batch_max`.
- `WorkerRuntimeConfig` has the same field. A struct literal of
  `WorkerConfig` or `WorkerRuntimeConfig` needs `claim_batch_max: None`.

## Batch size

Each claim loop takes its share of the free permits of each kind that it
may claim. The share is the free permits over `max_concurrent_claims`,
rounded up. The batch is the smallest share over those kinds, capped at
`claim_batch_max`. It then rounds down to 2, 4 or 8. A share under 2 sends
the single claim.

For example, a worker with 8 free workflow permits, 16 free activity
permits and two claim loops gets a share of 4 rows. A loop with one free
permit of one kind claims one row, however many permits of the other kind
are free.

The loop holds one permit per row across the claim. So a slot tuner shrink
cannot take the permit of a claimed row. The loop gives back the permits of
the rows that it did not get.

## Forms that keep one row per claim

Only the default claim has a batch form. A worker claims one row per
statement when any of these applies:

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
2. `candidate` keeps those rows in claim order, up to the second row with a
   concurrency key or a rate-limit key. That is the key cut.
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
cut. See [`testing/formal-methods.md`](../testing/formal-methods.md). The
tests `a_run_that_expires_during_the_claim_stops_only_its_own_row` and
`an_expired_row_takes_no_key_lock_and_its_peers_still_claim` in
`claim_batch_db_tests.rs` check the expired-row rule.

## Choose a value

On the `throughput` cell of the bench, `8` raised the median from 100.0 to
118.7 workflows/s over six alternating pairs. Every pair was faster. Claim
statements fell 37%, and their execution time fell 29%. One statement for
eight rows costs about 1.19 times one statement for one row. See
[`performance-claim-batch.md`](../performance-claim-batch.md).

Use the batch when most claims find a row and the claim loop is the limit.
It gains little when the queue is often empty, or when a worker has few
free permits per loop.

Known limits:

- Rows of equal claim key rank by id inside a batch. A single claim takes
  any of them.
- `seek_candidate` locks rows that the cut then drops. A peer claim skips
  such a row for one statement.
- The batch does not split by kind.

## See also

- [`claim-slots.md`](claim-slots.md): a fleet-wide cap on the claims that
  run at once on one shard.
- [`docs/architecture.md`](../architecture.md#sharding): the claim loops of
  a worker.
