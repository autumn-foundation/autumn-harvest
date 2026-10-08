# Assay #10 — cross-mode throughput

Workflow `harvest_e2e_bench_wf`, 3 activities, 7 dispatches per run.
Backlog 2000 workflows, 3 reps per arm, cap 900 s.

Pool: 1 worker, 8 workflow slots, 16 activity slots, 32 connections, 25 ms poll.

Postgres durability this run: `fsync = off`, `synchronous_commit = off`.
Embedded durability is fixed at `journal_mode = WAL`, `synchronous = FULL`, which fsyncs on every commit and cannot be turned off by a caller.

## arm `sqlite`

rep 0: 3.19 workflows/sec (2000 completed in 626.21 s, 6000 activity runs, correctness PASS)
rep 1: 3.19 workflows/sec (2000 completed in 626.06 s, 6000 activity runs, correctness PASS)
rep 2: 3.19 workflows/sec (2000 completed in 626.50 s, 6000 activity runs, correctness PASS)

**mean 3.19 workflows/sec** over 3 valid rep(s); per-dispatch rate 22.36/sec.

## summary

| arm | mean workflows/sec | valid reps | correctness |
|:--|--:|--:|:--|
| `sqlite` | 3.19 | 3 | PASS |

## pre-registered lines

* **L1** the postgres arm produced no valid repetition, so every line here is **INDETERMINATE**.
