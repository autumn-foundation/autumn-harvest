# Assay #10 — cross-mode throughput

Workflow `harvest_e2e_bench_wf`, 3 activities, 7 dispatches per run.
Backlog 1000 workflows, 3 reps per arm, cap 900 s.

Pool: 1 worker, 8 workflow slots, 16 activity slots, 32 connections, 25 ms poll.

Postgres durability this run: `fsync = off`, `synchronous_commit = off`.
Embedded durability is fixed at `journal_mode = WAL`, `synchronous = FULL`, which fsyncs on every commit and cannot be turned off by a caller.

## arm `postgres`

rep 0: 14.94 workflows/sec (1000 completed in 66.92 s, 3000 activity runs, correctness PASS)
rep 1: 24.04 workflows/sec (1000 completed in 41.59 s, 3000 activity runs, correctness PASS)
rep 2: 23.80 workflows/sec (1000 completed in 42.01 s, 3000 activity runs, correctness PASS)

**mean 20.93 workflows/sec** over 3 valid rep(s); per-dispatch rate 146.51/sec.

## arm `redis_pg`

rep 0: 21.24 workflows/sec (1000 completed in 47.08 s, 3000 activity runs, correctness PASS, residue entries=0 pending=0 markers=0)
rep 1: 21.87 workflows/sec (1000 completed in 45.73 s, 3000 activity runs, correctness PASS, residue entries=0 pending=0 markers=0)
rep 2: 22.03 workflows/sec (1000 completed in 45.39 s, 3000 activity runs, correctness PASS, residue entries=0 pending=0 markers=0)

**mean 21.71 workflows/sec** over 3 valid rep(s); per-dispatch rate 151.99/sec.

## summary

| arm | mean workflows/sec | valid reps | correctness |
|:--|--:|--:|:--|
| `postgres` | 20.93 | 3 | PASS |
| `redis_pg` | 21.71 | 3 | PASS |

## pre-registered lines

* **L1** postgres arm 20.93 workflows/sec against the [7.91, 71.19] band around the published 23.73: **PASS**
* **L2** redis_pg / postgres = 1.04x against a 2.0x line: **KILL**
* **L3** **INDETERMINATE**: the sqlite arm has no valid repetition.
