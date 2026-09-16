<!-- EDITORIAL HEADER, added after capture. The captured output follows
     unchanged below this block. -->

# Assay #11's harvest arm, at the registered ~40-byte payload

**The trailing `## pre-registered lines` block in the captured output below
does not apply to this run, and should be ignored.**

This run overrides `ASSAY10_INPUT_JSON` with assay #11's registered payload.
Assay #10's L1 compares its Postgres arm against a published figure taken with
the canonical `{}` input, so an overridden run is a different workload and
#10's band cannot grade it. The binary printed the block anyway, because at the
time of this run it graded unconditionally. That is fixed: the apparatus now
refuses to grade any run whose seeded input is not the canonical one, and
prints a "Not graded" notice instead. Found by review on PR #1622.

The capture is kept unedited rather than regenerated, so the archive shows what
the run actually printed. The measurement itself — three repetitions, mean
5.47 workflows/sec, every repetition correctness-PASS — is unaffected by the
defect, which was in the verdict printing and not in the measuring.

---

# Assay #10 — cross-mode throughput

Workflow `harvest_e2e_bench_wf`, 3 activities, 7 dispatches per run.
Backlog 2000 workflows, 3 reps per arm, cap 900 s.

Pool: 1 worker, 8 workflow slots, 16 activity slots, 32 connections, 25 ms poll.

Postgres durability this run: `fsync = off`, `synchronous_commit = off`.
Embedded durability is fixed at `journal_mode = WAL`, `synchronous = FULL`, which fsyncs on every commit and cannot be turned off by a caller.

## arm `postgres`

rep 0: 5.64 workflows/sec (2000 completed in 354.87 s, 6000 activity runs, correctness PASS)
rep 1: 5.38 workflows/sec (2000 completed in 371.61 s, 6000 activity runs, correctness PASS)
rep 2: 5.40 workflows/sec (2000 completed in 370.53 s, 6000 activity runs, correctness PASS)

**mean 5.47 workflows/sec** over 3 valid rep(s); per-dispatch rate 38.30/sec.

## summary

| arm | mean workflows/sec | valid reps | correctness |
|:--|--:|--:|:--|
| `postgres` | 5.47 | 3 | PASS |

## pre-registered lines

* **L1** postgres arm 5.47 workflows/sec against the [7.91, 71.19] band around the published 23.73: **KILL**
* **L2** and **L3** are not graded: L1 failed, and the pre-registration withholds every cross-mode number from this apparatus on an L1 kill.
