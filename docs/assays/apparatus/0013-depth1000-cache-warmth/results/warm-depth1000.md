# Assay #13 — depth-1000 cache warmth

Workflow `harvest_e2e_bench_wf`, 3 activities, 7 dispatches per run.
Backlog 1000 workflows, 6 reps per arm, cap 900 s. Condition: WARM (create once, DELETE+VACUUM+reseed between reps).

Pool: 1 worker, 8 workflow slots, 16 activity slots, 32 connections, 25 ms poll.

Postgres durability this run: `fsync = off`, `synchronous_commit = off`.
Embedded durability is fixed at `journal_mode = WAL`, `synchronous = FULL`, which fsyncs on every commit and cannot be turned off by a caller.

## arm `postgres`

rep 0: 19.19 workflows/sec (1000 completed in 52.12 s, 3000 activity runs, correctness PASS)
rep 1: 19.43 workflows/sec (1000 completed in 51.47 s, 3000 activity runs, correctness PASS)
rep 2: 18.74 workflows/sec (1000 completed in 53.37 s, 3000 activity runs, correctness PASS)
rep 3: 18.61 workflows/sec (1000 completed in 53.72 s, 3000 activity runs, correctness PASS)
rep 4: 18.33 workflows/sec (1000 completed in 54.56 s, 3000 activity runs, correctness PASS)
rep 5: 19.73 workflows/sec (1000 completed in 50.67 s, 3000 activity runs, correctness PASS)

**mean 19.01 workflows/sec** over 6 valid rep(s); per-dispatch rate 133.04/sec.

## summary

| arm | mean workflows/sec | valid reps | correctness |
|:--|--:|--:|:--|
| `postgres` | 19.01 | 6 | PASS |

## pre-registered line (assay #13)

* **WARM** condition, n=6: mean 19.01, sample stdev 0.53, **CV = 2.8%** (pre-registered lines: <= 5% pursues the cold-cache hypothesis, >= 15% kills it)
