# Assay #10 — cross-mode throughput

Workflow `harvest_e2e_bench_wf`, 3 activities, 7 dispatches per run.
Backlog 2000 workflows, 3 reps per arm, cap 900 s.

Pool: 1 worker, 8 workflow slots, 16 activity slots, 32 connections, 25 ms poll.

Postgres durability this run: `fsync = off`, `synchronous_commit = off`.
Embedded durability is fixed at `journal_mode = WAL`, `synchronous = FULL`, which fsyncs on every commit and cannot be turned off by a caller.

## arm `postgres`

rep 0: 5.64 workflows/sec (2000 completed in 354.87 s, 6000 activity runs, correctness PASS)
rep 1: 5.38 workflows/sec (2000 completed in 371.61 s, 6000 activity runs, correctness PASS)
rep 2: 5.40 workflows/sec (2000 completed in 370.53 s, 6000 activity runs, correctness PASS)

**mean 5.47 workflows/sec** over 3 valid rep(s); per-dispatch rate 38.30/sec.

## summary

| arm | mean workflows/sec | valid reps | correctness |
|:--|--:|--:|:--|
| `postgres` | 5.47 | 3 | PASS |

## pre-registered lines

* **L1** postgres arm 5.47 workflows/sec against the [7.91, 71.19] band around the published 23.73: **KILL**
* **L2** and **L3** are not graded: L1 failed, and the pre-registration withholds every cross-mode number from this apparatus on an L1 kill.
