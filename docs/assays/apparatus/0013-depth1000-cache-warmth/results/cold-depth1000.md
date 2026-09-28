# Assay #13 — depth-1000 cache warmth

Workflow `harvest_e2e_bench_wf`, 3 activities, 7 dispatches per run.
Backlog 1000 workflows, 6 reps per arm, cap 900 s. Condition: COLD (drop/recreate database every rep).

Pool: 1 worker, 8 workflow slots, 16 activity slots, 32 connections, 25 ms poll.

Postgres durability this run: `fsync = off`, `synchronous_commit = off`.
Embedded durability is fixed at `journal_mode = WAL`, `synchronous = FULL`, which fsyncs on every commit and cannot be turned off by a caller.

## arm `postgres`

rep 0: 20.91 workflows/sec (1000 completed in 47.83 s, 3000 activity runs, correctness PASS)
rep 1: 19.92 workflows/sec (1000 completed in 50.21 s, 3000 activity runs, correctness PASS)
rep 2: 21.19 workflows/sec (1000 completed in 47.20 s, 3000 activity runs, correctness PASS)
rep 3: 20.39 workflows/sec (1000 completed in 49.04 s, 3000 activity runs, correctness PASS)
rep 4: 21.83 workflows/sec (1000 completed in 45.82 s, 3000 activity runs, correctness PASS)
rep 5: 17.53 workflows/sec (1000 completed in 57.05 s, 3000 activity runs, correctness PASS)

**mean 20.29 workflows/sec** over 6 valid rep(s); per-dispatch rate 142.05/sec.

## summary

| arm | mean workflows/sec | valid reps | correctness |
|:--|--:|--:|:--|
| `postgres` | 20.29 | 6 | PASS |

## pre-registered line (assay #13)

* **COLD** condition, n=6: mean 20.29, sample stdev 1.50, **CV = 7.4%** (pre-registered line: expect >= 15%, replicating ledger #12)
