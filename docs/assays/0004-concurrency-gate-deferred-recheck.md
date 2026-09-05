# ⛏️ Prospect: does deferring the concurrency gate to the recheck fix the cardinality blowup? (kill: 313.8ms vs 100ms adversarial-retry line, ledger #4)

> Status: **measured.** The pre-registration
> (`docs/rnd/2026-09-05-concurrency-gate-deferred-recheck-preregistration.md`,
> commit `033ec02`) was committed before the apparatus was built or run;
> nothing in it has been edited since. This report is the Apparatus, Assay,
> Verdict and Reproduce sections appended afterward, with the actual numbers.

## 🎯 Question

`docs/assays/0003-concurrency-gate-cardinality-index.md` (ledger #3) killed
the partial-index rewrite `docs/performance.md` had left open — it fixed the
5,000-key hot-contention blowup but paid ~10,000 real per-candidate-row
index probes at idle, 202.6x over its pre-set line. That report named two
untested shapes. The first — a `LATERAL` join with planner hints — is not
actually open: `docs/performance.md`'s own "three rejected rewrites" section
already tested a `LEFT JOIN LATERAL` + hint variant and closed it as a
categorical planner limitation, not an untried knob. This assay charters the
second: **does removing the concurrency-key predicate from candidate
selection entirely, and enforcing the cap only in the `claimed` CTE's
existing authoritative recheck (retrying against the next candidate on a
failed recheck), simultaneously (1) cost no more than the current fix at
idle, (2) fix the 5,000-key hot-contention blowup, (3) not regress the
256-key case, and (4) keep the number of wasted retries bounded when the
highest-priority rows are themselves the ones blocked by a saturated key?**

**Decision this feeds:** whether to land the deferred-recheck rewrite as the
follow-up fix to the concurrency-key claim gate (issue #247 follow-up).
**Decider:** same as ledger #3 — whoever owns the queue/claim-path
performance work.

## ⚖️ Pre-registration

Committed in full at
[`docs/rnd/2026-09-05-concurrency-gate-deferred-recheck-preregistration.md`](../rnd/2026-09-05-concurrency-gate-deferred-recheck-preregistration.md)
(`033ec02`). Summary of the four lines, all four required for **pursue**,
any one miss is **kill**:

- **L1 (idle, must not regress):** 10,000-row backlog, 4 queues, 256 keys,
  zero RUNNING rows — candidate's total buffer count (candidate-select +
  one recheck) ≤ this apparatus's own same-run control buffer count.
- **L2 (high cardinality, must clear 10x):** 5,000 keys, 2,000 RUNNING rows
  — candidate wall-clock ≤ **160ms** (10x the documented 1,600ms).
- **L3 (256-key hot-contention, must not regress):** same 2,000-RUNNING-row
  shape at 256 keys — candidate wall-clock ≤ 2x this apparatus's own
  same-run control.
- **L4 (adversarial retry depth, registered at 50 — this shape's own novel
  risk, attacked first):** a fixture where the 50 highest-priority PENDING
  rows are keyed to already-saturated concurrency keys, with one claimable
  row immediately behind them: candidate must (a) reach it in **exactly 51
  attempts** and (b) total wall-clock ≤ **100ms**.

## 🔍 Prior art

- `docs/assays/0003-concurrency-gate-cardinality-index.md` (ledger #3) —
  named this shape as an untested, un-re-chartered pit; this assay is that
  re-charter.
- `docs/performance.md:1005-1030` — already closes the `LATERAL`+hints shape;
  not re-tested here.
- `docs/assays/README.md` (#1, #2) — unrelated Redis question, not a re-dig.
- No existing `plpgsql` claim-loop prototype anywhere in the tree; no prior
  measurement of retry behavior under adversarial priority ordering.

## 🧪 Apparatus

`docs/assays/apparatus/0004-concurrency-gate-deferred-recheck/` (archived):
the identical minimal schema and non-adversarial fixture generator ledger #3
used (`schema.sql`, `seed.sql`, unmodified — same `idx_harvest_tq_poll` /
`idx_harvest_tq_running` indexes, no new index added, on purpose: part of
this shape's premise is needing none), plus:

- `control.sql` / `control_raw.sql` — the committed fix's exact query, with
  and without `EXPLAIN` instrumentation (the raw form exists so wall-clock
  comparisons against the candidate's `\timing`-measured function calls
  aren't biased by `EXPLAIN ANALYZE`'s own per-node instrumentation
  overhead — see "Corroboration" below).
- `candidate_select.sql` / `recheck.sql` — the candidate's two building
  blocks in isolation, for a buffer-level breakdown comparable to control's.
- `claim_deferred.sql` — a `plpgsql` function implementing the actual
  shape under test: the retry loop the question requires, which no single
  `EXPLAIN`can represent. Its selection and recheck logic is ported by value
  from `queue.rs:605-779`'s real `candidate`/`claimed` CTEs (same
  `ORDER BY`/`LIMIT`/`FOR UPDATE SKIP LOCKED` shape, same
  `pg_try_advisory_xact_lock` + correlated `COUNT(*)` recheck), not
  reinvented.
- `seed_adversarial.sql` — the L4 fixture: 10,000 normal backlog rows
  (priority 0), 10 concurrency keys pre-saturated at cap=2 with RUNNING
  rows, 50 PENDING rows at priority=100 round-robined across those 10
  poisoned keys, and one claimable row (`concurrency_key IS NULL`) at
  priority=99.

**Stubs list (what was faked/cut, on purpose):**

- Same cuts as ledger #3's apparatus: `worker_info`, `paused_queues`,
  `paused_activities`, build-routing, workflow-pause, rate-limit, and
  capability-label clauses — orthogonal to the concurrency gate.
- `claim_deferred`'s `max_attempts` bailout (200, arbitrary) is itself a
  stub: the real crate has no principled answer yet for "then what" when a
  claim attempt exhausts its retry budget without finding a claimable row
  despite claimable work existing behind the exhausted candidates. This
  assay does not need one — L4 kills the shape before that question
  matters — but it would be the first open design question if this were
  ever re-chartered with a rescue mechanism.
- Single non-adversarial scenario family (10,000-row backlog, 4 queues,
  `NON_BLOCKING_CAP`), matching ledger #3's registered scope exactly, so L1-L3
  are directly comparable across the two assays.
- L4 registered at exactly one adversarial depth (50). Deeper depths are not
  measured (see "Worst case" below on what is and isn't extrapolation).

## 📊 Assay

All measurements from `docs/assays/apparatus/0004-concurrency-gate-deferred-recheck/results/`,
same machine, same apparatus, same session (`run.log`, `results/*.txt`,
`results/*.explain.txt`):

**Buffers (L1, `EXPLAIN (ANALYZE, BUFFERS, ...)`, idle: 256 keys, 0 RUNNING):**

| component | buffers |
|:--|--:|
| control (committed fix, full query) | 132 |
| candidate: `candidate_select.sql` (no concurrency predicate) | 131 |
| candidate: `recheck.sql` (one recheck at the idle key) | 1 |
| **candidate total** | **132** |

**Wall-clock, raw `\timing` (no `EXPLAIN` instrumentation on either side — see Corroboration):**

| scenario | keys | running | control (`control_raw.sql`) | candidate (`claim_deferred()`) | attempts |
|:--|--:|--:|--:|--:|--:|
| idle_256 | 256 | 0 | 7.707 ms | 6.631 ms | 1 |
| hot_256 | 256 | 2,000 | 208.515 ms | 6.853 ms | 1 |
| hot_5000 | 5,000 | 2,000 | 1,484.158 ms | 6.794 ms | 1 |
| **l4_adversarial** | 256 | 20 (2 per poisoned key × 10 keys) | — (not applicable) | **313.792 ms** | **51** |

Equivalence check: in every non-adversarial scenario, control and candidate
claimed the identical row id (e.g. `10001` at hot_256, `22001` at hot_5000)
— expected, since `NON_BLOCKING_CAP` never actually blocks anything in these
scenarios, so both shapes just return the single highest-priority row.

**Against the lines:**

- **L1 — PASS (tie).** 132 buffers vs. 132 — the line is `≤ control`, and a
  tie clears it, but this corrects an assumption in the pre-registration's
  own Conditions section: this shape does not *beat* the current fix's
  already near-zero idle cost, it *matches* it. The current fix's
  `(never executed)` CTE short-circuit and the candidate's plain
  no-predicate scan converge on the same number because both are, at 0
  `RUNNING` rows, doing the same thing: a `10,000`-row `Seq Scan` feeding a
  `Sort`/`LIMIT`, with the concurrency machinery (CTE or recheck) contributing
  1 buffer either way.
- **L2 — PASS, decisively.** Candidate: 6.794ms against a ≤160ms line —
  **23.6x** inside it — and a **218.5x** speedup over this run's own control
  (1,484.158ms). Confirms ledger #3's diagnosis that the current fix's
  blowup is a per-candidate-row cost: removing the per-row check entirely
  removes the blowup entirely, not just shrinks it.
- **L3 — PASS.** Candidate: 6.853ms against a ≤417.03ms line (2x control's
  208.515ms) — **30.4x faster than control outright**, not just inside the
  line.
- **L4 — FAIL.** 51 attempts exactly (the correctness sub-part passes: not
  fewer, which would mean a saturated key got wrongly claimed; not more,
  which would mean the exclusion mechanism was broken) — but **313.792ms**
  against a ≤100ms line, **3.14x over**.

**Riskiest assumption, checked first:** confirmed, and it is exactly what
kills this shape. The retry loop's per-attempt cost is cheap in isolation
(6.6-6.9ms, matching the non-adversarial single-attempt numbers above almost
exactly: 313.792 / 51 ≈ 6.15ms/attempt) — but "cheap per attempt" was never
the risk; the risk was whether *attempt count* stays bounded, and nothing in
this shape bounds it. Each retry re-runs the *entire* candidate-selection
scan (`ORDER BY priority DESC, scheduled_at ASC LIMIT 1 FOR UPDATE SKIP
LOCKED` over the whole 10,000-row backlog) because Postgres has no way to
"resume" an ordered scan past previously-tried rows without either an index
condition or an exclusion filter re-evaluated from scratch each time — so
total cost is *structurally* `attempts × (cost of one full claim attempt)`,
confirmed by the arithmetic above lining up to within run-to-run noise. This
is an architectural fact about the shape (visible in `claim_deferred.sql`'s
loop, not inferred from the curve), not an extrapolation — but only one
adversarial depth (50) was measured; a claim about 500-row or 5,000-row
adversarial depths would be extrapolation and is not made here.

**Worst case:** L4's fixture (50 poisoned rows behind 10 saturated keys) is
already a fairly mild adversarial construction — a real deployment with even
a handful of "hot," frequently-recreated, currently-saturated concurrency
keys that happen to also carry high task priority (a plausible operational
pattern: retried or escalated work often *is* both high-priority and on a
concurrency-capped key) would reproduce this shape, not require an
adversary. Nothing in the design distinguishes "an attacker constructed
this" from "normal priority aging plus normal contention produced this."

## 🏁 Verdict

**Kill**, on L4: 313.792ms vs. a 100ms line for 51 forced attempts. Per the
pre-registered rule, one miss among four is a kill regardless of the other
three clearing by wide, decisive margins.

This is a different failure mode than ledger #3's, and a more concerning one
for the same reason it's newly discovered: ledger #3's killed candidate
failed on a cost that is *fixed and bounded* (10,000 probes, exactly once,
regardless of how contended the system is or isn't). This candidate fails on
a cost that is *unbounded and workload-dependent* — it scales with how many
consecutive high-priority candidates an ordinary (or adversarial) pattern of
concurrency-key saturation can put at the head of the queue, which nothing
in the schema, the query, or the claim protocol limits. Where the committed
fix and ledger #3's rewrite both have a worst case you can name in advance
(bounded by distinct-key cardinality or by candidate-row count,
respectively), this shape's worst case is bounded only by how bad the
concurrent workload happens to be at the moment of the claim — which is
exactly the property a claim-path fix should not introduce, whatever it
does at idle or under ordinary hot contention.

L1-L3 passing decisively is not wasted evidence: it confirms ledger #3's own
mechanism finding (the current fix's cardinality cost is a per-candidate-row
tax; remove the per-row check and the tax disappears, cleanly, in every
non-adversarial shape tested) and rules out an entire further branch of
"maybe a smarter version of this shape survives" investigation — any rescue
of this candidate has to solve the retry-bound problem specifically, not
tune the idle or hot-contention paths further, since those were never in
question.

This is evidence, not a proof no fix exists in this family: a version that
caps retries and falls back to some other strategy on exhaustion, or that
biases candidate selection away from recently-failed concurrency keys, might
clear L4 — but that is a different, more complex shape (with a "then what"
question of its own — see the stubs list) and is an explicitly
un-re-chartered pit, not this assay's finding.

## 🔬 Reproduce

```
sudo -u postgres createdb prospect_assay4   # or any local, non-production Postgres 16
cd docs/assays/apparatus/0004-concurrency-gate-deferred-recheck
PGHOST=/var/run/postgresql PGDATABASE=prospect_assay4 ./run_assay.sh
cat results/*.txt
grep "Buffers: shared hit=13" results/idle_256-*.explain.txt
```

`schema.sql`, `seed.sql`, `seed_adversarial.sql`, `control.sql`,
`control_raw.sql`, `candidate_select.sql`, `recheck.sql`, `claim_deferred.sql`,
and the `driver.sql` session script are archived alongside `run_assay.sh` in
this directory, along with the full `results/*.txt` / `results/*.explain.txt`
output this report's tables are drawn from. No migration was added to
`autumn-harvest/migrations/`; no crate code changed. The prototype does not
merge.
