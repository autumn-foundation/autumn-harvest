# Assay #14: does a latency-driven limiter beat `DefaultSlotTuner` on the #1836 scenario?

**⛏️ Prospect: latency-driven limiter vs `DefaultSlotTuner` (#1836) — kill: best candidate 18.8% of achievable goodput vs 85% line on S2, ledger #14.**

## 🎯 Question

Issue #1836 proposes an opt-in Gradient2/AIMD limiter per activity type. In a
closed-loop simulation of its own AC1 scenario (downstream latency grows with
concurrency), with noise and a retrograde-collapse variant, does a limiter fed
only handler latency and timeout outcome hold goodput within the registered
lines, while the shipped `DefaultSlotTuner` misses them?

Decider: the maintainer triaging epic #1786. *Pursue* builds #1836 with the
named algorithm. *Kill* closes #1836 as specified.

## ⚖️ Pre-registration

`docs/rnd/2026-10-05-adaptive-concurrency-limiter-preregistration.md`, commit
`8a0d32c`, committed 09:15:23 UTC. The apparatus did not exist until after it.
The lines were not edited afterwards. Summary: L1 window goodput at least 85% of
achievable on the median seed and 70% on the worst seed; L2 median limit in
`[0.7K, 1.6K]` and p95 at most `2.0K`; L3 recovery inside 120 s of a knee step.
Pursue needs a candidate to meet all three in every cell and the incumbent to
miss one. Kill if neither candidate meets L1 and L2 in every cell.

## 🔍 Prior art

No ledger entry (#1-#13) touches the slot tuner. `docs/operations/adaptive-slot-tuner.md`
confirms the default controller reads no handler latency and no error signal.
Netflix `concurrency-limits` numbers are inadmissible here and were not used.

## 🧪 Apparatus

`docs/assays/apparatus/0014-adaptive-concurrency-limiter/`, non-production,
outside the workspace. A deterministic closed-loop simulator, one tick per
second, 1,800 s per run, 4 scenarios x 2 start limits x 6 arms x 5 seeds. Arm B
calls the real `DefaultSlotTuner::decide` and `apply_action` from
`autumn-harvest` at tree `e22a53b`. Arms A, C1 and C2 are written in the
apparatus.

**Stubs list.**

* The plant is a closed-form queueing model, not a real downstream.
* Arm B gets `pool: None`. Two permit-wait models stand in for the unmodelled
  claim gate: B-grow reports 1 s, B-gated reports 0.
* Limiters see one batch per tick (mean, median, timeout count), not one sample
  per request. This changes Gradient2 most. Netflix updates per request.
* Gradient2 is a port from memory of the Netflix defaults. It was not diffed
  against the upstream source.
* In-flight equals the limit. Backlog is unbounded. No retries, no pool, no DB.
* No tests, no CI. Seeds 1-5 fixed. The run is deterministic and was repeated
  with a byte-identical result.

## 📊 Assay

One registered sweep, all runs in `results/run1.csv` (per-seed efficiency in
column 4). Efficiency is window-mean goodput over achievable goodput. Registered
order was S2 first.

| cell | A fixed | B-grow | B-gated | C1 AIMD | C2 Gradient2 |
|:--|--:|--:|--:|--:|--:|
| S2 retrograde, start 20 | 49.5% | 0.0% | 49.5% | 18.6% | 0.0% |
| S2 retrograde, start 150 | 0.0% | 0.0% | 0.0% | 18.8% | 0.0% |
| S1 plateau, start 20 | 49.5% | 55.4% (limit 200) | 49.5% | 18.6% | 55.4% (limit 200) |
| S1 plateau, start 150 | 85.7% | 55.4% | 85.7% | 18.4% | 55.4% |
| S3 K 40 to 20, start 20 | 99.0% | 1.5% | 99.0% | 37.8% | 1.5% |
| S3 K 40 to 20, start 150 | 11.4% | 1.5% | 11.4% | 36.5% | 1.5% |
| S4 K 20 to 40, start 20 | 49.5% | 55.3% | 49.5% | 18.9% | 55.3% |
| S4 K 20 to 40, start 150 | 85.8% | 55.3% | 85.8% | 18.3% | 55.3% |

Pass counts against the registered lines, per arm, out of 8 cells: A 3 on L1,
B-grow 0, B-gated 3, C1 0, C2 0. No candidate meets L1 or L2 in any cell. Worst
seeds track the medians within 2 points everywhere, so variance is not the story.

**Mechanisms, from a trace of C2 on S2 (`results/trace-c2-s2-start20-seed1.txt`).**

* **C2:** goodput peaks at 235/s at t = 60 s and falls to zero by t = 540 s while
  the limit keeps rising to the 200 cap. The long-window RTT baseline follows
  the degraded latency upward, so the long/short ratio returns to 1, the
  gradient clamps at 1.0, and the controller grows again. It reads a permanently
  slow downstream as normal.
* **C1:** the registered rule "any timeout multiplies by 0.9" meets a plant in
  which 1% of requests run 10x slow and so exceed the 500 ms timeout even when
  healthy. The limit decays to about 7. This is a defect in the pre-registration
  (plant and rule interact), not a finding about AIMD.
* **B-grow:** grows to the 200 cap in every cell. This confirms the premise of
  #1836 under the growth-favourable permit-wait model. **B-gated** never moves,
  so it is the fixed arm in practice.

**Post-hoc diagnostic, not graded.** Arm `C1b-diag` gates the timeout signal on a
5% timeout rate instead of "any". It clears L1 in every cell and L2 on S2, but
fails L2 on every plateau cell (median limit 115 against `K = 40`, 2.9x). Its
latency baseline is a 60-tick window minimum, so it also chases degradation.
This was chosen after seeing the C1 numbers, so it earns no verdict. It is the
only lead in the data.

## 🏁 Verdict

**Kill, as registered.** Neither candidate meets L1 or L2 in any cell. Against
the 85% L1 line on the riskiest cell, S2, the best candidate reached 18.8%
(C1) and C2 reached 0.0%. The kill rule fired on its first clause. The
second clause (incumbent passes everything) did not apply: B fails too.

Scope of the kill, stated plainly:

* It kills the two candidates *with the registered parameters and tick-batched
  feeding*. It does not kill the idea of a latency-aware limiter.
* C1's failure is largely a registration defect. Do not cite it as evidence
  against AIMD.
* C2's failure is a mechanism (baseline chasing) but depends on the tick-batched
  stub. Per-request sampling could change it. This run cannot say.
* The plant is idealised. The result transfers to a real downstream only as far
  as "a controller that fails its own clean plant is not ready".

Decision for the maintainer: do not build #1836 as specified. Keep it open only
if someone charters the re-charter below.

## Re-charter named (not run)

A new assay with a timeout-rate gate for AIMD and an absolute latency
reference (a configured target or a slow-decaying minimum, not a window minimum)
for the baseline, per-request sampling for Gradient2, and plateau-scenario L2
re-derived for the chosen tolerance. New pre-registration, new lines. The shelf
trigger is a maintainer asking for #1836 to be re-opened with a downstream whose
latency curve is measured, not modelled.

## 🔬 Reproduce

```sh
cd docs/assays/apparatus/0014-adaptive-concurrency-limiter
cargo build --release
./target/release/adaptive_limiter_assay > results/run1.csv   # about 7 s
TRACE=1 ./target/release/adaptive_limiter_assay              # C2 on S2 trace
```

Toolchain and tree are in `results/versions.txt`.
