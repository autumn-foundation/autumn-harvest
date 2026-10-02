# Assay #13 — depth-1000 cache warmth

Workflow `harvest_e2e_bench_wf`, 3 activities, 7 dispatches per run.
Backlog 1000 workflows, 6 reps per arm, cap 900 s. Condition: WARM (create once, DELETE+VACUUM+reseed between reps).

Pool: 1 worker, 8 workflow slots, 16 activity slots, 32 connections, 25 ms poll.

Postgres durability this run: `fsync = off`, `synchronous_commit = off`.
Embedded durability is fixed at `journal_mode = WAL`, `synchronous = FULL`, which fsyncs on every commit and cannot be turned off by a caller.

## arm `postgres`

rep 0: 17.17 workflows/sec (1000 completed in 58.23 s, 3000 activity runs, correctness PASS)
rep 1: 17.13 workflows/sec (1000 completed in 58.39 s, 3000 activity runs, correctness PASS)
rep 2: 18.68 workflows/sec (1000 completed in 53.54 s, 3000 activity runs, correctness PASS)
rep 3: 20.64 workflows/sec (1000 completed in 48.45 s, 3000 activity runs, correctness PASS)
rep 4: 20.36 workflows/sec (1000 completed in 49.11 s, 3000 activity runs, correctness PASS)
rep 5: 18.88 workflows/sec (1000 completed in 52.96 s, 3000 activity runs, correctness PASS)

**mean 18.81 workflows/sec** over 6 valid rep(s); per-dispatch rate 131.67/sec.

## summary

| arm | mean workflows/sec | valid reps | correctness |
|:--|--:|--:|:--|
| `postgres` | 18.81 | 6 | PASS |

## pre-registered line (assay #13)

* **WARM** condition, n=6: mean 18.81, sample stdev 1.50, **CV = 8.0%** (pre-registered lines: <= 5% pursues the cold-cache hypothesis, >= 15% kills it)
