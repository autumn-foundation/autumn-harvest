# Pre-registration: does a latency-driven limiter beat `DefaultSlotTuner` on the issue #1836 scenario?

> Committed **before** any apparatus existed. Assay ledger #14. Nothing in this
> file is edited after the first measurement. The report at
> `docs/assays/0014-adaptive-concurrency-limiter.md` appends the apparatus, the
> numbers and the verdict.

Source tree: `e22a53b`. Registered 2026-10-05. Time box: 1 day. Budget: local
CPU only, no spend, no production data.

## 🎯 Question

Issue #1836 (P3, parent epic #1786) proposes an opt-in per-activity-type
limiter that uses Gradient2 or AIMD on handler latency and error rate. Its
premise: `DefaultSlotTuner` grows slots when permit waits rise, which is the
wrong direction when the downstream dependency is the bottleneck.

**Falsifiable question:** in a closed-loop simulation of the issue's own AC1
scenario (a downstream whose latency grows with concurrency), with per-request
latency noise and a retrograde-collapse variant, does a latency/error-driven
limiter fed only with signals the worker already has in process (handler
latency, timeout outcome) hold goodput within the lines below, *and* does the
shipped `DefaultSlotTuner` miss those same lines?

**Decision this feeds:** whether to build #1836 as specified (decider: the
maintainer who triages the #1786 epic). On *pursue* the issue proceeds with the
winning algorithm named. On *kill* the issue closes as "do not build" with this
report as the citation. On *undetermined* the issue stays P3 with the re-charter.

## 🔍 Prior art checked

* `docs/operations/adaptive-slot-tuner.md` and `slot_tuner.rs` module docs: the
  default controller shrinks on pool pressure, grows on saturated-and-waiting,
  else holds. It reads no handler latency and no error signal.
* Assay ledger #1-#13: none touches the slot tuner or any controller. No
  ledger-closed claim is re-assayed here.
* `research_notes/.../failure_handling_overload.md` motivates the issue. It is
  a gap analysis, not a measurement.
* Netflix `concurrency-limits` (Gradient2, AIMD) is the named reference. Its
  numbers come from Netflix workloads and are inadmissible here.

## ⚖️ Pre-registration

### Plant (fixed now)

One worker, unbounded backlog, so in-flight equals the limit. Downstream knee
`K`, base latency `L0 = 100 ms`, request timeout `T = 500 ms`, band
`[5, 200]`, tuner tick 1 s, 1,800 s simulated per run.

* **Plateau** plant: `L(n) = L0 * max(1, n/K)`. Goodput `G(n) = min(n, K)/L0`.
* **Retrograde** plant: `L(n) = L0 * max(1, n/K)^2`. Goodput
  `G(n) = min(n, K^2/n)/L0`. A request with `L > T` times out and yields no goodput.
* **Noise** (all runs): multiplicative lognormal, sigma 0.3, plus 1% of
  requests at 10x latency.

Scenarios, each with start limit 20 and start limit 150 (two cells each):

| id | plant | knee |
|:--|:--|:--|
| S1 | plateau | `K = 40` |
| S2 | retrograde | `K = 40` |
| S3 | plateau | `K` steps 40 to 20 at t = 900 s |
| S4 | plateau | `K` steps 20 to 40 at t = 900 s |

Seeds 1-5, fixed. Every run is reported. Judging window: t in [1200, 1800] for
S1/S2, t in [1500, 1800] for S3/S4. `G*` is the best achievable goodput at the
window's knee, `K/L0 = 400/s` at `K = 40` and `200/s` at `K = 20`.

### Arms

* **A (do-nothing control):** fixed limit at the start value.
* **B (incumbent control):** the real `DefaultSlotTuner::decide` and
  `apply_action` from `autumn-harvest`, defaults unchanged (`grow_step 2`,
  `shrink_step 2`, `permit_wait_grow_threshold 50 ms`), pool pressure `None`
  because the downstream, not the pool, is the bottleneck. Two registered
  observation models, both reported: **B-grow** reports `max_permit_wait = 1 s`
  whenever in-use reaches the target (best case for growth), **B-gated**
  reports 0 (permit-gated claims never wait, issue #1787).
* **C1 (candidate):** AIMD, latency-aware. Per tick, baseline = minimum of the
  last 60 tick-median latencies. Tick mean latency above 1.5x baseline, or any
  timeout, multiplies the limit by 0.9. Otherwise, with in-flight at least half
  the limit, add 1.
* **C2 (candidate):** Gradient2 with the Netflix defaults: tolerance 1.5,
  smoothing 0.2, long window 600, queue size `sqrt(limit)`, drift decay 0.95
  when long/short RTT exceeds 2.

Candidate parameters are fixed here and are not tuned after data exists. A
different parameter set is a new assay.

### Lines

A cell is one (scenario, start) pair. A line is judged on the **median of 5
seeds** unless it names the worst seed.

* **L1 efficiency:** window-mean goodput at least 85% of `G*` on the median
  seed, and at least 70% on the worst seed.
* **L2 stability:** window median limit within `[0.7 K, 1.6 K]`, and window p95
  limit at most `2.0 K`. This reads "converges near the knee, no unbounded
  growth" from AC1.
* **L3 adaptation (S3, S4 only):** within 120 s of the step, a 30 s rolling
  goodput reaches 80% of the new `G*`.

### Verdict rule (fixed now)

* **Pursue** candidate X: X meets L1, L2 and L3 in **every** cell, *and* B
  (best of B-grow and B-gated per cell) fails L1 or L2 in at least one cell.
  If both candidates qualify, the report names both and does not pick.
* **Kill:** neither candidate meets L1 and L2 in every cell (a limiter that
  thrashes on noise or collapses under retrograde load is worse than a fixed
  limit), **or** B meets L1, L2 and L3 in every cell (no gap to close).
* **Undetermined:** anything else, with a re-charter or shelf entry.

A miss against a line is a miss. Lines do not move.

### Riskiest assumption, attacked first

That a latency-based limiter holds up under heavy-tailed noise *and* the
retrograde plant, where overshooting the knee costs goodput instead of only
latency. **S2 runs first.** If both candidates fail L1 or L2 on S2, the kill
is already decided and the other scenarios are run for the ledger only.

### Containment

Branch `claude/happy-gauss-8m937o`. Apparatus under
`docs/assays/apparatus/0014-adaptive-concurrency-limiter/`, marked
non-production. It is a standalone crate outside the workspace, with no
engine-source change. It never merges as engine code.

### Known limits of what this can prove (stated before the run)

The plant is a closed-form queueing model, not a real downstream. A *pursue*
shows the control law is viable on this model. It does not show the limiter
works against a real dependency. A *kill* on S2 is stronger: a controller that
fails its own idealised plant fails a harder one.
