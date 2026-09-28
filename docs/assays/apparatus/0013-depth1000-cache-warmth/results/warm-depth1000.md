# Assay #13 — depth-1000 cache warmth

Workflow `harvest_e2e_bench_wf`, 3 activities, 7 dispatches per run.
Backlog 1000 workflows, 6 reps per arm, cap 900 s. Condition: WARM (create once, truncate+reseed between reps).

Pool: 1 worker, 8 workflow slots, 16 activity slots, 32 connections, 25 ms poll.

Postgres durability this run: `fsync = off`, `synchronous_commit = off`.
Embedded durability is fixed at `journal_mode = WAL`, `synchronous = FULL`, which fsyncs on every commit and cannot be turned off by a caller.

## arm `postgres`

rep 0: 13.15 workflows/sec (1000 completed in 76.05 s, 3000 activity runs, correctness PASS)
rep 1: 21.08 workflows/sec (1000 completed in 47.44 s, 3000 activity runs, correctness PASS)
rep 2: 14.63 workflows/sec (1000 completed in 68.35 s, 3000 activity runs, correctness PASS)
rep 3: 20.61 workflows/sec (1000 completed in 48.52 s, 3000 activity runs, correctness PASS)
rep 4: 20.91 workflows/sec (1000 completed in 47.82 s, 3000 activity runs, correctness PASS)
rep 5: 21.08 workflows/sec (1000 completed in 47.44 s, 3000 activity runs, correctness PASS)

**mean 18.58 workflows/sec** over 6 valid rep(s); per-dispatch rate 130.04/sec.

## summary

| arm | mean workflows/sec | valid reps | correctness |
|:--|--:|--:|:--|
| `postgres` | 18.58 | 6 | PASS |

## pre-registered line (assay #13)

* **WARM** condition, n=6: mean 18.58, sample stdev 3.66, **CV = 19.7%** (pre-registered lines: <= 5% pursues the cold-cache hypothesis, >= 15% kills it)
