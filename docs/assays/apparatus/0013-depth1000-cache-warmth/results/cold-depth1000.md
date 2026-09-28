# Assay #13 — depth-1000 cache warmth

Workflow `harvest_e2e_bench_wf`, 3 activities, 7 dispatches per run.
Backlog 1000 workflows, 6 reps per arm, cap 900 s. Condition: COLD (drop/recreate database every rep).

Pool: 1 worker, 8 workflow slots, 16 activity slots, 32 connections, 25 ms poll.

Postgres durability this run: `fsync = off`, `synchronous_commit = off`.
Embedded durability is fixed at `journal_mode = WAL`, `synchronous = FULL`, which fsyncs on every commit and cannot be turned off by a caller.

## arm `postgres`

rep 0: 20.42 workflows/sec (1000 completed in 48.97 s, 3000 activity runs, correctness PASS)
rep 1: 13.09 workflows/sec (1000 completed in 76.37 s, 3000 activity runs, correctness PASS)
rep 2: 21.08 workflows/sec (1000 completed in 47.45 s, 3000 activity runs, correctness PASS)
rep 3: 7.96 workflows/sec (1000 completed in 125.63 s, 3000 activity runs, correctness PASS)
rep 4: 12.51 workflows/sec (1000 completed in 79.95 s, 3000 activity runs, correctness PASS)
rep 5: 13.60 workflows/sec (1000 completed in 73.53 s, 3000 activity runs, correctness PASS)

**mean 14.78 workflows/sec** over 6 valid rep(s); per-dispatch rate 103.44/sec.

## summary

| arm | mean workflows/sec | valid reps | correctness |
|:--|--:|--:|:--|
| `postgres` | 14.78 | 6 | PASS |

## pre-registered line (assay #13)

* **COLD** condition, n=6: mean 14.78, sample stdev 5.05, **CV = 34.2%** (pre-registered line: expect >= 15%, replicating ledger #12)
