# Group commit: throughput at depth and latency at low load

Issue #2256. An owner commits the decisions of several executions of one
decision partition in one transaction, with a savepoint per decision.
[`DESIGN-2256.md`](../DESIGN-2256.md) section 1 is the design note.
[ADR 0009](adr/0009-decision-owner.md) decision 14 records it. This page
measures the bench item of the issue.

| Done-when item | Result |
|---|---|
| A criterion bench shows a persist throughput gain at depth. | **Pass.** 1.16x at 16 writers and 1.33x at 64 writers, `synchronous_commit = on`. The paired bursts of the bench give the figures. |
| No p50 regression at low load. | **Pass.** At one writer, the group p50 is 0.998 of the solo p50, in a range of 0.987 to 1.010. |

## The bench and the host

[`benches/group_commit_bench.rs`](../autumn-harvest/benches/group_commit_bench.rs)
times a model decision persist, as
[`decision_fence_bench`](performance-decision-owner-baseline.md#3-the-fence-cost)
does. Each decision runs the start fence, two appends, one activity task
enqueue and an execution update. The post-commit wake sender is registered.

All writers share one partition and one owner tenure. Each writer owns one
execution and commits decisions in a loop. The writer count is the queue
depth. Each mode has 4 connections:

- `direct`: each decision is its own transaction, as the persist runs today.
- `solo`: the lane with `max_batch = 1`. Each decision commits alone.
- `group`: the lane with the defaults, `width = 4` and `max_batch = 32`.

At one or four writers, no decision waits for a slot. So `solo` and `group`
then run the same code: a batch of one with no savepoint.

Host: 4 logical CPUs, 15 GB. PostgreSQL 16.15 on the same host, with
`fsync = on` and `max_connections = 300`. On 2026-10-24, 21:17 to 22:09 UTC.
Raw output: [`results/`](perf-artifacts/group-commit/results/). Graded:
[`results/graded.md`](perf-artifacts/group-commit/results/graded.md).

Only ratios measured on one box compare. Do not compare the absolute
numbers with another page.

## 1. Throughput at depth

Criterion runs one mode after another, so slow drift on this host moves
each mode apart. At one writer, `group` read 0.92 of `solo` over 9 rounds,
although both run the same code there. So criterion alone cannot resolve
a gain of 10% to 30% here.

The bench therefore alternates bursts of `solo` and `group` at each depth.
Each burst runs 20 decisions per writer, and each pair swaps which mode runs
first. Drift then hits both modes of a pair alike.

| `synchronous_commit` | writers | pairs | solo decisions/s | group decisions/s | group / solo, median | range | pairs faster |
|:--|--:|--:|--:|--:|--:|--:|--:|
| on | 16 | 30 | 807.6 | 956.9 | 1.16 | 0.82 to 1.39 | 24 of 30 |
| on | 64 | 30 | 620.8 | 785.1 | 1.33 | 1.04 to 1.51 | 30 of 30 |
| off | 16 | 20 | 859.2 | 978.2 | 1.07 | 0.84 to 1.75 | 13 of 20 |
| off | 64 | 20 | 608.7 | 730.7 | 1.20 | 0.99 to 1.31 | 19 of 20 |

- **The gain grows with depth.** At 16 writers the mean batch holds 2.1
  decisions. At 64 writers it holds 5.7.
- **A batch saves work per decision.** Each decision of a batch skips its
  own `BEGIN`, start fence and `COMMIT`. It adds a `SAVEPOINT` and a
  `RELEASE`. So it saves one round trip, one fence statement and one WAL
  flush wait.
- **The WAL flush counts.** With `synchronous_commit = off`, a commit does
  not wait for the flush, and the gain is smaller.
- **Each slot takes a fair share.** A leader takes the queue over `width`,
  rounded up. The first build let one leader take the whole queue. The
  other slots then found no work. In that first run, `group` took about 1.4
  times as long as `solo` per decision at 16 writers.
  `group_commit::share` fixed that.

The criterion rows, the median of 9 rounds with `synchronous_commit = on`:

| writers | solo ms per decision | group ms per decision | mean batch |
|--:|--:|--:|--:|
| 1 | 4.146 | 3.684 | 1.00 |
| 4 | 1.587 | 1.450 | 1.00 |
| 16 | 1.516 | 1.392 | 2.13 |
| 64 | 1.469 | 1.327 | 5.66 |

The rows at 1 and 4 writers show the drift: there the two modes run the
same code.

## 2. Latency at low load

One writer commits through `direct`, `solo` and `group` in turn, 1,000
decisions each per round. No batch forms. The table gives the median over
the rounds.

| `synchronous_commit` | rounds | mode | p50 ms | p50 / solo p50 | range |
|:--|--:|:--|--:|--:|--:|
| on | 12 | direct | 3.498 | 0.999 | 0.991 to 1.026 |
| on | 12 | solo | 3.477 | 1.000 | — |
| on | 12 | group | 3.483 | 0.998 | 0.987 to 1.010 |
| off | 5 | direct | 2.977 | 0.996 | 0.976 to 0.998 |
| off | 5 | solo | 2.987 | 1.000 | — |
| off | 5 | group | 2.974 | 0.995 | 0.985 to 0.998 |

- **No p50 regression.** A lone decision leads a batch of one at once. It
  sends no savepoint and waits for nothing.
- **The lane costs nothing visible.** `direct` takes no lane, and its p50
  matches `solo` and `group`. The bench spawns each `direct` decision, as a
  lane spawns its batch, so both run on a worker thread.

## 3. What this page does not show

- **The worker does not use the lane yet.** Issue #2318 wires it into the
  persist paths. The assay #14 shape then measures workflows/s.
- **One partition.** Every writer here shares one partition. Under real
  load, decisions spread over many partitions, so each lane sees less
  depth. Each lane has its own slots, and all lanes share one pool.
- **One host.** The bench and Postgres share 4 CPUs. A host with the
  database apart, or a disk with a slower flush, changes the absolute
  numbers.
