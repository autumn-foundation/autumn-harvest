# Assay #13 — depth-1000 cache warmth

Workflow `harvest_e2e_bench_wf`, 3 activities, 7 dispatches per run.
Backlog 1000 workflows, 6 reps per arm, cap 900 s. Condition: COLD (drop/recreate database every rep).

Pool: 1 worker, 8 workflow slots, 16 activity slots, 32 connections, 25 ms poll.

Postgres durability this run: `fsync = off`, `synchronous_commit = off`.
Embedded durability is fixed at `journal_mode = WAL`, `synchronous = FULL`, which fsyncs on every commit and cannot be turned off by a caller.

## arm `postgres`

rep 0: 19.44 workflows/sec (1000 completed in 51.44 s, 3000 activity runs, correctness PASS)
rep 1: 13.44 workflows/sec (1000 completed in 74.40 s, 3000 activity runs, correctness PASS)
rep 2: 20.09 workflows/sec (1000 completed in 49.77 s, 3000 activity runs, correctness PASS)
rep 3: 10.44 workflows/sec (1000 completed in 95.79 s, 3000 activity runs, correctness PASS)
rep 4: 12.32 workflows/sec (1000 completed in 81.18 s, 3000 activity runs, correctness PASS)
rep 5: 11.14 workflows/sec (1000 completed in 89.74 s, 3000 activity runs, correctness PASS)

**mean 14.48 workflows/sec** over 6 valid rep(s); per-dispatch rate 101.35/sec.

## summary

| arm | mean workflows/sec | valid reps | correctness |
|:--|--:|--:|:--|
| `postgres` | 14.48 | 6 | PASS |

## pre-registered line (assay #13)

* **COLD** condition, n=6: mean 14.48, sample stdev 4.23, **CV = 29.2%** (pre-registered line: expect >= 15%, replicating ledger #12)
