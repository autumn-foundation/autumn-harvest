# Assay #10 — cross-mode throughput

Workflow `harvest_e2e_bench_wf`, 3 activities, 7 dispatches per run.
Backlog 500 workflows, 3 reps per arm, cap 900 s.

Pool: 1 worker, 8 workflow slots, 16 activity slots, 32 connections, 25 ms poll.

Postgres durability this run: `fsync = off`, `synchronous_commit = off`.
Embedded durability is fixed at `journal_mode = WAL`, `synchronous = FULL`, which fsyncs on every commit and cannot be turned off by a caller.

## arm `postgres`

rep 0: 23.86 workflows/sec (500 completed in 20.95 s, 1500 activity runs, correctness PASS)
rep 1: 23.85 workflows/sec (500 completed in 20.96 s, 1500 activity runs, correctness PASS)
rep 2: 23.55 workflows/sec (500 completed in 21.23 s, 1500 activity runs, correctness PASS)

**mean 23.76 workflows/sec** over 3 valid rep(s); per-dispatch rate 166.29/sec.

## arm `redis_pg`

rep 0: 22.10 workflows/sec (500 completed in 22.63 s, 1500 activity runs, correctness PASS, residue entries=0 pending=0 markers=0)
rep 1: 22.29 workflows/sec (500 completed in 22.43 s, 1500 activity runs, correctness PASS, residue entries=0 pending=0 markers=0)
rep 2: 20.06 workflows/sec (500 completed in 24.92 s, 1500 activity runs, correctness PASS, residue entries=0 pending=0 markers=0)

**mean 21.48 workflows/sec** over 3 valid rep(s); per-dispatch rate 150.38/sec.

## summary

| arm | mean workflows/sec | valid reps | correctness |
|:--|--:|--:|:--|
| `postgres` | 23.76 | 3 | PASS |
| `redis_pg` | 21.48 | 3 | PASS |

## pre-registered lines

* **L1** postgres arm 23.76 workflows/sec against the [7.91, 71.19] band around the published 23.73: **PASS**
* **L2** redis_pg / postgres = 0.90x against a 2.0x line: **KILL**
* **L3** **INDETERMINATE**: the sqlite arm has no valid repetition.
