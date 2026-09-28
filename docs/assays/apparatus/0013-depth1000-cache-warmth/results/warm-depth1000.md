# Assay #13 — depth-1000 cache warmth

Workflow `harvest_e2e_bench_wf`, 3 activities, 7 dispatches per run.
Backlog 1000 workflows, 6 reps per arm, cap 900 s. Condition: WARM (create once, DELETE+VACUUM+reseed between reps).

Pool: 1 worker, 8 workflow slots, 16 activity slots, 32 connections, 25 ms poll.

Postgres durability this run: `fsync = off`, `synchronous_commit = off`.
Embedded durability is fixed at `journal_mode = WAL`, `synchronous = FULL`, which fsyncs on every commit and cannot be turned off by a caller.

## arm `postgres`

rep 0: 21.16 workflows/sec (1000 completed in 47.25 s, 3000 activity runs, correctness PASS)
rep 1: 20.42 workflows/sec (1000 completed in 48.97 s, 3000 activity runs, correctness PASS)
rep 2: 19.79 workflows/sec (1000 completed in 50.54 s, 3000 activity runs, correctness PASS)
rep 3: 19.21 workflows/sec (1000 completed in 52.05 s, 3000 activity runs, correctness PASS)
rep 4: 19.10 workflows/sec (1000 completed in 52.37 s, 3000 activity runs, correctness PASS)
rep 5: 19.04 workflows/sec (1000 completed in 52.52 s, 3000 activity runs, correctness PASS)

**mean 19.79 workflows/sec** over 6 valid rep(s); per-dispatch rate 138.50/sec.

## summary

| arm | mean workflows/sec | valid reps | correctness |
|:--|--:|--:|:--|
| `postgres` | 19.79 | 6 | PASS |

## pre-registered line (assay #13)

* **WARM** condition, n=6: mean 19.79, sample stdev 0.86, **CV = 4.3%** (pre-registered lines: <= 5% pursues the cold-cache hypothesis, >= 15% kills it)
