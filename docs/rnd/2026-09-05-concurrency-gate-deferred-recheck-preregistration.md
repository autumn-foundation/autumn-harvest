# ⛏️ Prospect pre-registration: does deferring the concurrency gate entirely to the authoritative recheck fix the cardinality blowup? (assay ledger #4)

**Committed:** 2026-09-05T09:06:52Z, before any apparatus was built or measurement taken.
This document is the contract; the report that follows it is graded against
these lines, not against whatever the numbers turn out to be.

## 🎯 Question

`docs/assays/0003-concurrency-gate-cardinality-index.md` (ledger #3) killed the
partial-index rewrite `docs/performance.md` had left open: it fixes the
5,000-key hot-contention blowup (~48.8x faster than control) and doesn't
regress the 256-key case, but at zero `RUNNING` rows it pays ~10,000 real
per-candidate-row index probes where the current fix pays ~10,000 near-free
probes of a small, resident, empty CTE — a 200x+ miss on the pre-set idle
line. That report's own closing paragraph names two shapes it did not test:
*"a `LATERAL` join with hints specifically defeating the planner's
Sort-before-Limit choice, or a recheck-only strategy that defers the
expensive gate to the already-cheap `claimed` CTE and accepts more
`pg_try_advisory_xact_lock` contention at the gate itself."*

**The first of those is not actually open.** `docs/performance.md`'s own
"three pure query rewrites... evaluated and rejected" section (lines
1005-1030) already tested a `LEFT JOIN LATERAL` variant with
`enable_hashjoin`/`enable_mergejoin` disabled, and again with
`enable_seqscan`/`enable_bitmapscan` also disabled, and states the negative
result as a categorical planner limitation, not an untried knob: *"PostgreSQL
cannot apply `LIMIT`-pushdown-through-ordered-scan to a join whose filter
references the joined side, independent of which physical join algorithm is
chosen."* Re-running that shape with different hints would be a re-dig of
already-closed ground with no new information — this assay does not attempt
it.

**Falsifiable question, the second (genuinely untested) shape:** does
removing the concurrency-key predicate from candidate selection's `WHERE`
clause entirely — so `candidate` is chosen purely by
`queue_name`/`state = 'PENDING'`/`scheduled_at <= NOW()` and
`ORDER BY priority DESC, scheduled_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED`, no
concurrency CTEs, no per-candidate-row check of any kind — and enforcing the
cap only where `claim_task_query()` already enforces it authoritatively today
(`queue.rs:756-771`: `pg_try_advisory_xact_lock` + a correlated `COUNT(*)`
recheck on the single already-locked candidate row), retrying by excluding
the failed row and re-selecting the next-highest-priority candidate when that
recheck fails, simultaneously:

1. cost no more than the current committed fix at idle (it should cost
   *less* — there is no concurrency CTE to build at all), and
2. fix the 5,000-key hot-contention blowup (1,600ms documented, same 10x line
   ledger #3 used since it is the same source figure), and
3. not regress the already-good 256-key hot-contention case, and
4. **(this shape's own novel risk, not shared by either prior candidate)**
   keep the number of wasted round-trips bounded when the highest-priority
   PENDING rows are themselves the ones blocked by an already-saturated
   concurrency key — since unlike both prior shapes (which filter candidates
   *before* selection), this one discovers a blocked candidate only *after*
   selecting and locking it, and nothing in the shape bounds how many
   consecutive high-priority rows can share a saturated key.

## 👤 Decision this feeds

Whether to land the deferred-recheck rewrite as the follow-up fix to the
concurrency-key claim gate (issue #247 follow-up) — the same "known
limitation" section ledger #3 left open, closing it a different way: not by
making the candidate-side filter cheap at high cardinality, but by removing
the candidate-side filter altogether.

**Decider:** same as ledger #3 — whoever owns the queue/claim-path
performance work (the reviewer thread `docs/performance.md:849-851` points
at). This assay does not decide; it produces the missing numbers.

## ⚖️ Success / kill criteria (numeric, set now)

One apparatus, one session, control and candidate (and the adversarial
fixture) built from the same fixture generator so every comparison is
same-harness. Four lines; **all four** must clear for **pursue**, any one
miss is **kill**:

- **L1 — idle case must not regress.** 10,000-row backlog, 4 queues, 256
  distinct concurrency keys, zero `RUNNING` rows: candidate's total buffer
  count (candidate-select query, isolated via `EXPLAIN (ANALYZE, BUFFERS)`,
  plus one recheck-query invocation at the same cost) must be **≤ this
  apparatus's own same-run control measurement** for the committed fix's
  idle-case buffer count. (Relational, not an absolute figure, precisely
  because the whole premise of this shape is "cheaper than the CTE machinery
  at idle" — a tie or a regression against the thing it's supposed to beat is
  itself a kill.)
- **L2 — high-cardinality hot-contention must clear a 10x margin.** 10,000-row
  backlog, 4 queues, 5,000 distinct concurrency keys, 2,000 `RUNNING` rows
  spread across those keys (same shape ledger #3 used): candidate's
  end-to-end wall-clock (via the retry-loop function, `\timing`) **must be ≤
  160ms** (10x faster than the documented 1,600ms).
- **L3 — 256-key hot-contention must not regress.** Same 2,000-`RUNNING`-row
  shape at 256 keys: candidate's end-to-end wall-clock must be **≤ 2x**
  whatever this apparatus's own same-run control measures.
- **L4 — adversarial retry depth, registered at 50.** A fixture where the 50
  highest-priority PENDING rows in the backlog (priority above every other
  row, so `ORDER BY priority DESC, scheduled_at ASC` always selects them
  first) are keyed to already-saturated concurrency keys (`RUNNING` count ==
  `cap` for each), with one claimable row immediately behind them at the next
  priority tier: candidate must (a) reach the claimable row in **exactly 51
  attempts** (50 forced failures + 1 success — a correctness check, not just
  a cost one: fewer means it wrongly claimed a saturated key, more means the
  exclusion mechanism is broken) and (b) total wall-clock for those 51
  attempts **≤ 100ms**. A miss on either sub-part is a kill on L4.

## ⏱️ Conditions

- Postgres 16, local, default `postgres.conf`, `ANALYZE` after every bulk
  seed, matching ledger #3's apparatus exactly (same schema, same
  `seed.sql`-style fixture generator, same 10,000-row/4-queue backlog depth)
  so this assay is directly comparable to #3, not a fresh, incomparable
  harness.
- Candidate is implemented as a `plpgsql` function (`claim_deferred.sql`)
  because the question is about a *retry loop*, not a single query plan —
  something a single `EXPLAIN` cannot represent. The function's body is
  ported by value from `queue.rs:605-779`'s actual `candidate`/`claimed` CTE
  shapes (the `ORDER BY`/`LIMIT`/`FOR UPDATE SKIP LOCKED` selection, the
  `pg_try_advisory_xact_lock` + correlated `COUNT(*)` recheck), not
  reinvented, so a "pursue" verdict describes the real predicate the crate
  would ship, not a lookalike.
- No new index is added for this candidate (this is itself part of what the
  shape is being measured against: ledger #3's killed candidate needed one,
  this one is being tested on the claim that it needs none).
- L4's fixture is a deliberate adversarial construction, not a random one —
  registered as depth 50 specifically so the result is a reproducible,
  falsifiable number rather than "we tried some random seeds and it seemed
  fine."

## 🧨 Riskiest assumption, attacked first

That the retry loop's *per-attempt* cost stays cheap enough that a
plausible-depth adversarial run of forced retries (L4) doesn't itself become
the dominant cost — because unlike ledger #3's killed candidate (whose
failure mode was a fixed, if expensive, per-candidate-row cost paid on every
row regardless of contention) and unlike the committed fix (whose cost is
bounded by distinct-key cardinality, not by priority-ordering adversarial
placement), this shape's worst case is driven by something neither prior
candidate depends on at all: how many consecutive top-priority rows an
attacker, or an unlucky real workload, can arrange to share a saturated key.
If a mere 50 forced retries already costs more than single-digit
milliseconds, the "no CTE, no new index" cheapness at idle buys nothing once
any real contention touches the head of the priority order — so L4 is
measured before L2/L3, not after.

## 🎛️ Control

The current committed fix's exact query shape (`concurrency_running_counts`
`MATERIALIZED` CTE + correlated subquery against it — the same `control.sql`
ledger #3 archived, reused verbatim), run on the identical schema, fixtures,
and machine as the candidate, in the same session.

## 📦 Containment

Local, non-production Postgres 16 (started as a local dev service for this
assay, matching ledger #3's containment). One scratch database created and
dropped within the assay. Apparatus archived under
`docs/assays/apparatus/0004-concurrency-gate-deferred-recheck/` after the
run. No migration is added to `autumn-harvest/migrations/`; no crate code
changes. The prototype does not merge regardless of verdict.

## 💵 Budget / time box

$0 spend (local only). Time box: same session, target under 3 hours from this
commit to verdict. No extension without an explicit re-charter.

## 🔍 Prior art already checked

- `docs/assays/0003-concurrency-gate-cardinality-index.md` (ledger #3) —
  killed the partial-index candidate, named this shape as an untested,
  un-re-chartered pit. This assay is that re-charter, with new information
  (ledger #3's own mechanism finding: the killed candidate's cost was driven
  by per-candidate-row probes, which is exactly what removing the
  candidate-side predicate avoids).
- `docs/performance.md:1005-1030` — the three previously-rejected rewrites,
  including the `LEFT JOIN LATERAL` + planner-hint variant that already
  closes the *other* shape ledger #3 named. Not re-tested here; cited as the
  reason this assay charters only the recheck-deferred shape.
- `docs/assays/README.md` (#1, #2) — the unrelated Redis question, not a
  re-dig.
- Repo search: no existing `plpgsql` claim-loop prototype under
  `docs/assays/apparatus/` or elsewhere in the tree; no prior measurement of
  retry-count behavior under adversarial priority ordering. Apparatus is
  warranted.
