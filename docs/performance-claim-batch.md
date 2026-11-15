# Batch claim, off by default (issue #2693)

A claim statement took one row. On the throughput cell the claim loop is
the bottleneck: almost every claim finds a row, and throughput does not rise
from 32 to 128 workflows in flight. `WorkerConfig::with_claim_batch_max(n)`
lets one statement take up to `n` rows, in batches of 2, 4 or 8.

> **This is a reference measurement, not an SLO.** It comes from one 4-core
> box with the database, the worker and the load generator on it.

## TL;DR

* **Throughput rises 18.7%** on the `throughput` cell, from a median of
  100.0 to 118.7 workflows/s over six alternating pairs. Every pair is
  faster, by 1.09x to 1.36x. Latency by Little's law falls from 286 ms to
  233 ms.
* **Claim statements fall 37%,** from 10,134 to 6,398 per run. Their
  execution time falls 29%, from 7,092 ms to 5,007 ms.
* **A row costs 15% of a single claim at k = 8.** One statement for eight
  rows costs 1.18x one statement for one row.
* **The knob is off by default.** Queue weights, fairness keys, priority
  ageing, owned workflow claims, adaptive limits, cost holds and dispatch
  channels keep one row per claim.

## How a batch claims

The batch text derives from the window form of the default claim, after the
DR fence and the kind predicate are in it. Only two CTEs change:

* `seek_candidate` locks up to 2, 4 or 8 rows with `FOR UPDATE SKIP LOCKED`.
  It also returns the claim key of each row: sticky rank, priority and due
  time.
* `candidate` ranks those rows in claim order. It keeps them up to the second
  row with a concurrency key or a rate-limit key. The cap re-check of
  `claimed` reads the statement snapshot, so two rows of one key would both
  pass it. One bucket row takes one debit, however many claims join it.

`fresh_now`, `run_expired_now`, `rate_limit_debit` and `claimed` are the
same text as in the window form. An expired run anywhere in a batch stops the
whole batch, as it stops a single claim. When the batch finds no row and sets
the fallback flag, the full statement runs and takes one row at most.

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

### Claim census

One run of each arm with `pg_stat_statements` reset before it
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
([`paired-runs.txt`](perf-artifacts/claim-batch/paired-runs.txt)):

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
`e2e_bench` binary. `HARVEST_BENCH_CLAIM_BATCH` sets the batch size. Both
arms held about 28 of 32 workflows in flight, so e2e_bench withholds the
headline for both. The rate here is the middle half of the raw completions.

## Known limits

* Rows of equal claim key rank by id inside a batch. A single claim takes
  any of them.
* `seek_candidate` locks rows that the cut then drops. Each lock ends with
  the claim transaction, so a peer claim skips that row for one statement.
* The batch does not split by kind. A loop with one free workflow permit
  takes a single row, even with many free activity permits.
