# Assay #10 — cross-mode throughput

Workflow `harvest_e2e_bench_wf`, 3 activities, 7 dispatches per run.
Backlog 2000 workflows, 3 reps per arm, cap 900 s.

Pool: 1 worker, 8 workflow slots, 16 activity slots, 32 connections, 25 ms poll.

Postgres durability this run: `fsync = off`, `synchronous_commit = off`.
Embedded durability is fixed at `journal_mode = WAL`, `synchronous = FULL`, which fsyncs on every commit and cannot be turned off by a caller.

## arm `sqlite`

rep 0: 3.20 workflows/sec (2000 completed in 625.37 s, 6000 activity runs, correctness PASS)
rep 1: 3.20 workflows/sec (2000 completed in 625.27 s, 6000 activity runs, correctness PASS)
rep 2: 3.20 workflows/sec (2000 completed in 625.13 s, 6000 activity runs, correctness PASS)

**mean 3.20 workflows/sec** over 3 valid rep(s); per-dispatch rate 22.39/sec.

## arm `postgres`

rep 0: 5.60 workflows/sec (2000 completed in 357.20 s, 6000 activity runs, correctness PASS)
rep 1: 5.57 workflows/sec (2000 completed in 358.91 s, 6000 activity runs, correctness PASS)
rep 2: 5.58 workflows/sec (2000 completed in 358.44 s, 6000 activity runs, correctness PASS)

**mean 5.58 workflows/sec** over 3 valid rep(s); per-dispatch rate 39.09/sec.

## arm `redis_pg`

rep 0: 21.70 workflows/sec (2000 completed in 92.18 s, 6000 activity runs, correctness PASS, residue entries=0 pending=0 markers=0)
rep 1: 22.16 workflows/sec (2000 completed in 90.27 s, 6000 activity runs, correctness PASS, residue entries=0 pending=0 markers=0)
rep 2: 22.35 workflows/sec (2000 completed in 89.48 s, 6000 activity runs, correctness PASS, residue entries=0 pending=0 markers=0)

**mean 22.07 workflows/sec** over 3 valid rep(s); per-dispatch rate 154.48/sec.

## summary

| arm | mean workflows/sec | valid reps | correctness |
|:--|--:|--:|:--|
| `sqlite` | 3.20 | 3 | PASS |
| `postgres` | 5.58 | 3 | PASS |
| `redis_pg` | 22.07 | 3 | PASS |

## pre-registered lines

* **L1** postgres arm 5.58 workflows/sec against the [7.91, 71.19] band around the published 23.73: **KILL**
* **L2** and **L3** are not graded: L1 failed, and the pre-registration withholds every cross-mode number from this apparatus on an L1 kill.

[exited with code 0]
