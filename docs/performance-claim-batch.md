# Batch claim (issue #2693)

A claim statement took one row. On the throughput cell the claim loop is
the bottleneck: almost every claim finds a row, and throughput does not rise
from 32 to 128 workflows in flight. `WorkerConfig::with_claim_batch_max(n)`
lets one statement take up to `n` rows, in batches of 2, 4 or 8.

The batch claim shipped off by default. It is now on by default with a cap
of 8 rows. See [On by default](#on-by-default). The operator guide is
[Batch claim](operations/claim-batch.md).

> **This is a reference measurement, not an SLO.** It comes from one 4-core
> box with the database, the worker and the load generator on it.

## TL;DR

* **Against the window-first claim, throughput rises 18.7%** on the
  `throughput` cell, from a median of 100.0 to 118.7 workflows/s over six
  alternating pairs. Every pair is faster, by 1.09x to 1.36x. Latency by
  Little's law falls from 286 ms to 233 ms. These pairs predate the lean
  claim statement.
* **Claim statements fall 37%** in those pairs, from 10,134 to 6,398 per
  run. Their execution time falls 29%, from 7,092 ms to 5,007 ms.
* **A row costs 15% of a single claim at k = 8.** One statement for eight
  rows costs 1.18x one statement for one row.
* **Against the lean single claim, a cap of 4 gives 1.07 to 1.09 times the
  throughput** at 4, 32 and 64 workflows in flight, over six rotated rounds
  of each level.
* **A cap of 8 gives 1.16 times the throughput of a cap of 4** in 6 of 6
  rounds. The worker has 64 workflow and 128 activity slots. On a default
  worker a cap of 8 measures the same as a cap of 4, within noise.
* **The batch claim is on by default, with a cap of 8 rows**
  (`builder::DEFAULT_CLAIM_BATCH_MAX`). `with_claim_batch_max(1)` turns it
  off. Queue weights, fairness keys, priority ageing, owned workflow
  claims, adaptive limits, cost holds and dispatch channels keep one row
  per claim.

## On by default

**The decision.** `WorkerConfig::default()` sets `claim_batch_max` to
`DEFAULT_CLAIM_BATCH_MAX`, 8 rows. `with_claim_batch_max(4)` keeps the
earlier cap of 4 rows. `with_claim_batch_max(1)` keeps one row per claim. A
value above 8 still claims 8 rows at most. The worker has no backlog gate
and no slow start: a claim loop asks for its share on each claim.

**Why 8.** The batch size is the smallest share of the free permits per
kind over the claim loops, capped at the default. So the cap changes a
batch only where a share reaches 8. On a worker whose share rarely passes
4, a cap of 8 forms batches no larger than a cap of 4 does. It measures the
same within noise there. On a worker with many free slots it forms batches
of 8, and it is faster. Most hazards that grow with the cap grow only where
batches of 8 form. Two of them are the rows that a claim in flight locks
and the crash strikes of a batch. Two more are the cached batch plans of a
connection and the burst of one worker. The permits that a claim holds grow
where a share passes 4. A loop holds its share, up to 8 per kind, and gives
back the rest after the claim.

The first default was 4. The end-to-end bench worker has 8 workflow
permits, 16 activity permits and 2 claim loops, so its share never passes
4. The rounds below at 4, 32 and 64 in flight therefore cover a cap of 4.
[Cap of 8](#cap-of-8) has the rounds on workers with more slots.

**The measurement.** The lean claim statement is newer than the first
measurement of this page. A batch claim sends no lean statement. So the
default must beat the lean single claim, not the window-first claim. The
rounds ran on the `throughput` cell at one shard, on the tree that turns
the default on. Each load level has six rotated rounds of one binary.
`HARVEST_BENCH_CLAIM_BATCH` sets the arm.

| workflows in flight | throughput, cap of 4 over the lean single claim | rounds faster |
|--:|--:|--:|
| 4 | 1.09x | 4 of 6 |
| 32 | 1.07x | 6 of 6 |
| 64 | 1.07x | 6 of 6 |

Caps of 2 and 8 measure the same as 4, within noise, on that worker.

**Low load.** A claim that finds one row costs more as a batch statement
than as a lean statement. The cell at 4 workflows in flight is the low-load
arm, and it shows no loss.

**Keyed queues.** The key cut keeps one row of a batch on a queue where
every row has a concurrency key. There, one batch statement costs 1.16 ms
against 1.13 ms for the window statement, as `pgbench` transaction latency.
So a claim on a keyed queue costs about the same with the default.

The raw files are in `docs/perf-artifacts/claim-batch-default/`:
`paired-4.txt`, `paired-32.txt` and `paired-64.txt` hold the rounds of each
load level, and `pgbench-keyed.txt` holds the keyed statement cost.

**What the bench runs.** `e2e_bench` and the capacity bench keep one row
per claim when `HARVEST_BENCH_CLAIM_BATCH` is unset. The published
baselines were taken that way, and the reproduction check is two-sided. A
number above 1 sets that cap, and the word `default` takes the worker
default. The report header prints the rows per claim statement. See
[the benchmark knobs](benchmarks.md#narrowing-a-run).

**What the default changes beside throughput.** One worker can take a burst
that single claims spread over the fleet. A claim in flight locks more
window rows. Each connection caches more claim plans. A crash right after a
claim gives a strike to each row of the batch.
[Batch claim](operations/claim-batch.md) lists each limit and its worst
case.

**Tests.** Three tests of `claim_batch_db_tests.rs` back the default:

* `each_batch_text_keeps_a_bounded_plan_on_a_deep_backlog_of_one_queue`
  pins the plan of each batch text on a deep backlog, with fresh and with
  idle statistics.
* `random_states_give_a_batch_the_rows_of_a_run_of_single_claims` compares
  a batch with a run of single claims over random queue states. A batch
  takes a prefix of the rows of the single claims. It takes each of them
  when the state has no keyed row.
* `a_default_worker_claims_a_backlog_in_batches_and_one_row_turns_it_off`
  runs a worker from `WorkerConfig::default()` on a backlog.

### Cap of 8

These rounds ran the same cell with three arms: the lean single claim
(`HARVEST_BENCH_CLAIM_BATCH=1`), a cap of 4 and a cap of 8. Each row has six
rotated rounds of one binary, with two claim loops. The medians are in
workflows/s.

| worker slots, workflow / activity | in flight | lean single claim | cap of 4 | cap of 8 | cap of 8 over cap of 4, median of the per-round ratios | rows per batch statement, cap of 4 to cap of 8 |
|:--|--:|--:|--:|--:|--:|--:|
| 20 / 50 | 80 | 114.4 | 157.5 | 154.7 | 0.98, 8 faster in 2 of 6 | 3.37 to 3.34, batch and window together |
| 20 / 50 | 160 | 111.5 | 158.7 | 152.5 | 0.99, 8 faster in 3 of 6 | not taken |
| 64 / 128 | 256 | 110.3 | 159.8 | 186.1 | 1.16, 8 faster in 6 of 6 | 3.92 to 7.63 |

Each ratio is the median of the six per-round ratios, so it can differ from
the ratio of the medians.

On the 20 / 50 worker, a cap of 4 gives 1.38 times the lean single claim at
80 in flight and 1.42 times at 160. It gives 1.45 times on the 64 / 128
worker. A cap of 8 gives 1.70 times the lean single claim there.

**Rows per statement.** One `pg_stat_statements` census per cap counts the
rows of the batch statements. On the 20 / 50 worker, a statement returns
3.37 rows at a cap of 4 and 3.34 rows at a cap of 8. Its share rarely
passes 4, so the batches stay small. On the 64 / 128 worker, a batch
statement returns 3.92 rows at a cap of 4 and 7.63 rows at a cap of 8. Its
calls fall from 2,580 to 1,325 for about the same rows. The census
normalizes the `LIMIT` of a batch text to a bind. So it cannot split the
calls by batch size, only give rows per statement. The 20 / 50 census
counts the batch and window statements together, because both texts carry
`claim_window_fallback`. So its figures are rows per statement of the two
forms.

**The in-flight gate.** On the 64 / 128 worker the cap-of-8 arm held 206 to
212 of 256 workflows in flight. That is under the 90% gate of `e2e_bench`,
so its rate understates it. The arms of a cap of 4 and of the lean claim
passed the gate. [Paired runs](performance-claim-window-split.md#paired-runs)
explains why the gate fails when workflows get faster.

The raw files are in `docs/perf-artifacts/claim-batch-default/`.
`cap-20x50-80.txt`, `cap-20x50-160.txt` and `cap-64x128-256.txt` hold the
rounds.
`census-20x50-80.txt` and `census-64x128-256.txt` hold the census of each
cap. `worker-slots.patch` is a scratch patch to the bench that reads
`SCRATCH_WF_SLOTS` and `SCRATCH_ACT_SLOTS`. It is not part of the tree.
`paired.sh` runs the rounds.

## How a batch claims

`queue::claim_task_batch_with_slots` sends the batch claim. Its text derives
from the window form of the default claim, after the DR fence and the kind
predicate are in it. The default claim is the claim with no priority
ageing, no fairness keys and no owned gate. These CTEs change:

* `seek_candidate` locks up to 2, 4 or 8 rows with `FOR UPDATE SKIP LOCKED`.
  It also returns the claim key of each row: sticky rank, priority and due
  time.
* `candidate` ranks those rows in claim order. It keeps them up to the second
  row with a concurrency key or a rate-limit key. The cap re-check of
  `claimed` reads the statement snapshot, so two rows of one key would both
  pass it. One bucket row takes one debit, however many claims join it.
* `run_expired_now` lists each row whose run passed its deadline by the clock
  that `fresh_now` reads after the bucket lock. `claim_live` holds the other
  rows, and `rate_limit_debit` and `claimed` read it in place of
  `candidate`. The run can pass its deadline while the claim waits on the
  bucket lock. Then that row stays `PENDING`, spends no token and takes no
  concurrency-key lock, and the claim still takes the other rows of the
  batch. `claim_live` is materialized, so `claimed` never tests the key lock
  of a row that it then drops.

`fresh_now` is the same text as in the window form. When the batch finds no
row and sets the fallback flag, the full statement runs and takes one row at
most.

Each claimed row passes the post-claim rechecks of a single claim. The
rechecks of all rows share one pipeline. A row that they give back is
hinted after the commit.

`formal/verus/src/claim_batch.rs` proves two things. A loop never asks for
more than its share of the free permits of an allowed kind. Only the first
kept row may carry a key, and the cut keeps the longest such prefix.

## Batch size

A claim loop takes up to its share of the free permits of each kind that it
may claim: the free permits over `max_concurrent_claims`, rounded up. The
batch is the smallest share over those kinds, capped at `claim_batch_max`,
and rounded down to 2, 4 or 8. A share under 2 sends the single claim. The
loop holds one permit per row across the claim, so a slot tuner shrink
cannot take the permit of a claimed row.

The bench worker has 8 workflow and 16 activity permits and two claim loops,
so most batches hold 2 or 4 rows. In the census, batch statements return
7,481 rows from 3,691 calls. 2,688 claims still take one row, because one
kind had a single free permit.

A default worker has 20 workflow and 50 activity permits and two claim
loops. Its idle claim loop has a share of 10 rows, so it asks for the cap
of 8. Under load its share rarely passes 4: the census of
[Cap of 8](#cap-of-8) has 3.34 rows per batch or window statement.

## Measurements

### Statement cost

[`pgbench.sh`](perf-artifacts/claim-batch/pgbench.sh) times each form on a
24-row backlog, with the claim plan settings and a rollback. The batch
scripts come from the window script through
[`batch_variant.py`](perf-artifacts/claim-batch/batch_variant.py). Means of
three rounds
([`pgbench-results.txt`](perf-artifacts/claim-batch/pgbench-results.txt)):

| k | ms per statement | ms per row |
|--:|--:|--:|
| 1 | 1.049 | 1.049 |
| 2 | 1.077 | 0.538 |
| 4 | 1.149 | 0.287 |
| 8 | 1.233 | 0.154 |

With the per-row expiry check, three more rounds give 1.014, 1.055, 1.104
and 1.209 ms per statement for k = 1, 2, 4 and 8
([`pgbench-results-row-expiry.txt`](perf-artifacts/claim-batch/pgbench-results-row-expiry.txt)).
The k = 8 statement still costs 1.19x the k = 1 statement.

### Claim census

Both arms predate the lean claim statement, so the off arm sends the
window statement first. One run of each arm with `pg_stat_statements` reset
before it
([`off-claim-forms.txt`](perf-artifacts/claim-batch/off-claim-forms.txt),
[`on-claim-forms.txt`](perf-artifacts/claim-batch/on-claim-forms.txt)):

| | Off | On |
|:--|--:|--:|
| batch statements | 0 | 3,691 |
| window statements | 10,130 | 2,688 |
| full statements | 4 | 19 |
| claim execution time | 7,092 ms | 5,007 ms |
| all statements | 296,313 | 264,785 |
| all execution time | 19,724 ms | 18,351 ms |

### Throughput

Six alternating pairs of the `throughput` cell at one shard, one binary, the
knob off and on
([`paired-runs.txt`](perf-artifacts/claim-batch/paired-runs.txt)). The off
arm is the window-first claim. See [On by default](#on-by-default) for the
rounds against the lean single claim.

| pair | off | on | ratio |
|--:|--:|--:|--:|
| 1 | 82.08 | 111.73 | 1.361 |
| 2 | 101.18 | 109.89 | 1.086 |
| 3 | 100.17 | 120.48 | 1.203 |
| 4 | 99.83 | 116.96 | 1.172 |
| 5 | 104.53 | 120.97 | 1.157 |
| 6 | 99.67 | 122.20 | 1.226 |

The pairs ran with
[`claim-window-split/paired.sh`](perf-artifacts/claim-window-split/paired.sh),
through [`off.sh`](perf-artifacts/claim-batch/off.sh) and
[`on.sh`](perf-artifacts/claim-batch/on.sh) with `BENCH_BIN` set to the
`e2e_bench` binary. `HARVEST_BENCH_CLAIM_BATCH` sets the batch size, 8 in
the on arm. Both
arms held about 28 of 32 workflows in flight, so e2e_bench withholds the
headline for both. The rate here is the middle half of the raw completions.

## Known limits

* Rows of equal claim key rank by id inside a batch. A single claim takes
  any of them.
* `seek_candidate` locks rows that the cut then drops. Each lock ends with
  the claim transaction, so a peer claim skips that row for one statement.
* The batch does not split by kind. A loop with one free workflow permit
  takes a single row, even with many free activity permits.
* [Batch claim](operations/claim-batch.md) lists the limits that the
  default brings to a fleet: the burst that one worker takes, the row locks
  of a batch, the crash strikes and the plan memory.
