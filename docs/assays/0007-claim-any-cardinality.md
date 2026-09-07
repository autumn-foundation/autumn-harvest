# ⛏️ Prospect: does `ANY()` itself, not cardinality, defeat sort-elision? (confirmed — operator, not cardinality: 195 buffers/Sort-present at n=1 vs 591 at n=4)

## 🎯 Question

Explicit re-charter of [ledger #6](0006-claim-1177-baseline-queue-count.md),
spawned rather than folded into it, after a Codex review round correctly
found that ledger #6's confirmed verdict — scalar equality gets the cheap
plan a 4-queue `ANY()` doesn't — says nothing about what a *real*
single-queue deployment's plan looks like, because
`autumn-harvest/src/queue.rs:641` always binds `queue_name = ANY($2)`,
never scalar equality, regardless of how many queues a worker polls.

**Falsifiable question:** holding everything ledger #6 held constant,
does `queue_name = ANY(ARRAY['bench-q-0'])` — `ANY()` over a
**single-element** array, the actual production binding shape for a
one-queue worker — still produce `Index Scan using idx_harvest_tq_poll`
with zero `Sort` nodes (matching scalar equality's result), or does a
`Sort` node persist (matching the 4-queue control's result despite
cardinality dropping to 1)?

**Decision this feeds:** the same decider ledger #5 and #6 both name —
whoever next re-charters issue #1340's seek-and-refine work. If a `Sort`
node persists at cardinality 1, single-queue deployments get **no** cost
relief from this defect, contradicting any reading of ledger #6 that
treated cardinality as sufficient.

## ⚖️ Pre-registration

Committed before this record's own confirmatory measurement:
[`docs/rnd/2026-09-07-claim-any-cardinality-preregistration.md`](../rnd/2026-09-07-claim-any-cardinality-preregistration.md)
(commit `09fac5d`).

- **Cardinality-explains-it:** `ANY()` over a single-element array shows
  zero `Sort` nodes (matches scalar equality).
- **Operator-defeats-it:** a `Sort` node is present (matches the 4-queue
  control).

**A third round of Codex review correctly flagged a real gap here:** the
`09fac5d` document disclosed, honestly, that its own query had already
been run once exploratorily before that pre-registration was committed.
For a deterministic `EXPLAIN` against a deterministic, non-concurrent
fixture, re-running the *identical* computation after already knowing its
structural outcome is not a genuine confirmatory trial — there was no
real chance of a different result, so the "fresh run" below, while
accurately reported, didn't carry the evidentiary weight a pre-registered
result is supposed to. This is not "the line moved" (the criteria above
are unchanged and the cardinality-1 result still satisfies
operator-defeats-it), it's that the trial testing them wasn't blind.

**Fixed with a genuinely blind addendum, not a retraction:**
[`docs/rnd/2026-09-07-claim-any-cardinality-blind-confirmation-preregistration.md`](../rnd/2026-09-07-claim-any-cardinality-blind-confirmation-preregistration.md)
(commit `04306dd`) registers a cardinality-2 arm — `ANY(ARRAY['bench-q-0',
'bench-q-1'])` — never run in any form before that commit, as the actual
blind test of whether "operator, not cardinality" generalizes past the
two already-seen endpoints (1 and 4). Same criteria, same apparatus, one
new seed parameter. See the assay section below for its result.

## 🔍 Prior art

Ledger #6's own report and pre-registration, and the Codex review finding
on PR #1418 that prompted this re-charter — verified directly against
`autumn-harvest/src/queue.rs:641` (confirmed: `WHERE queue_name = ANY($2)`
in both the `concurrency_pending_keys` and `candidate` CTEs of
`claim_task_query()`) before this record was written, not taken on faith.

## 🧪 Apparatus

Reuses ledger #6's own apparatus unmodified —
`docs/assays/apparatus/0006-claim-1177-baseline-queue-count/`
(`single_queue_any_diagnostic.sql`, `multi_queue_control.sql`,
`schema.sql`, `run_assay.sh`, all seeded from ledger #5's own `seed.sql`
with only `queues` varied). No new apparatus was built for this
re-charter; the question is answerable entirely from an arm ledger #6's
own review process already produced but had not yet properly chartered.

**Stubs list:** identical to ledger #6's — none beyond what ledger #5
already declared for this query shape.

## 📊 Assay

Fresh run, same database (`prospect_assay6`), reseeded from scratch:

**Control** (4-queue `ANY`, reproduced from ledger #6): `Sort` node
present, `Index Scan` reads all 10,000 matching rows, 591 buffers,
4.933ms.

**Test, cardinality 1** (single-queue `ANY(ARRAY['bench-q-0'])`, same
10,000-row backlog, all in one queue — not blind, see pre-registration
section above): `Sort` node **present** — `Index Scan` reads all 10,000
matching rows (not the `LIMIT`ed 50), 195 buffers, 4.117ms.

**Blind confirmation, cardinality 2** (`ANY(ARRAY['bench-q-0',
'bench-q-1'])`, 10,000 rows split between the two queues, never run
before commit `04306dd`): `Sort` node **present** — `Index Scan` reads
all 10,000 matching rows, 329 buffers, 4.247ms. Buffers scale
roughly with cardinality (195 at n=1, 329 at n=2, 591 at n=4 — each
queue's own share of the index range, scanned in full), but the
structural shape — `Sort` present, no `LIMIT` pushdown — does not change
at any cardinality tested.

For reference, scalar equality (ledger #6's own pre-registered arm, same
seed): no `Sort` node, 53 buffers, bounded scan (50 rows read).

`grep -c "Sort Key"`: control (n=4) 1, `ANY` at n=2 1, `ANY` at n=1 1,
scalar equality 0. Full output archived at
[`results/multi_queue_control.explain.txt`](apparatus/0006-claim-1177-baseline-queue-count/results/multi_queue_control.explain.txt),
[`results/any_cardinality_2.explain.txt`](apparatus/0006-claim-1177-baseline-queue-count/results/any_cardinality_2.explain.txt),
and
[`results/single_queue_any.explain.txt`](apparatus/0006-claim-1177-baseline-queue-count/results/single_queue_any.explain.txt)
(all under ledger #6's apparatus directory — this record reuses it
directly rather than duplicating files).

**Against the line:** both the cardinality-1 result and the blind
cardinality-2 confirmation match the **operator-defeats-it** criterion —
a `Sort` node is present at every cardinality tested (1, 2, and 4),
structurally identical to the 4-queue control in every case.

## 🏁 Verdict

**Confirmed: operator, not cardinality** — now on a genuinely blind
result, not just the cardinality-1 comparison alone. `ANY()` defeats
sort-elision on `idx_harvest_tq_poll` regardless of how many elements the
array holds: cardinalities 1, 2, and 4 all show the identical structural
shape (`Sort` node present, full backlog scan, no `LIMIT` pushdown). Only
scalar equality, a query shape no production code path in this
repository emits, gets the cheap plan.

**This both answers its own question and deepens ledger #5's original,
still-only-partially-resolved discrepancy against issue #1177.**
`docs/performance.md` reports that issue #1177's own baseline, using the
identical `ANY($1)` binding, returned a clean `Index Scan` with no `Sort`
at some undocumented cardinality. This record's own apparatus cannot
reproduce that clean shape via `ANY()` even at cardinality 1 — the
cardinality most favorable to sort-elision working, if cardinality were
the relevant variable at all. Two explanations remain open and this
record cannot distinguish between them without further work:

- This apparatus, this Postgres instance (16.13), or some other
  uncontrolled variable differs from #1177's own original reproduction in
  a way neither ledger #5's, #6's, nor this record's diagnostics have
  isolated.
- `ANY()` never gets sort-elision from this planner regardless of array
  cardinality, and #1177's own reported clean baseline — despite its own
  text describing the binding as `ANY($1)` — came from a scalar-equality
  query, or from some other unstated difference in that reproduction.

**For the named decider — scoped to what was actually measured.** Across
ledgers #6 and #7, cardinalities 1, 2, and 4 were tested; 3 and every
value above 4 were not. The plan shape held identical at all three tested
points and the mechanism (an `ANY()` bind defeating sort-elision
independent of the specific array) gives no principled reason to expect a
different shape at 3 or at higher cardinalities, but that is an inference
from the mechanism, not a measurement — this report does not claim
coverage of the full range a real worker's queue count could take. A
future #1340 re-charter should treat this `ORDER BY`/`LIMIT` cost as
present at every cardinality actually tested here (1, 2, 4), reasonably
expect it to hold at untested points given the shared mechanism, and
re-verify directly (cheap: one more seed parameter and one more
`EXPLAIN`, using this same apparatus) before relying on it at a
cardinality this record didn't check, rather than treating "no
queue-count story" as itself an established universal. Whether Postgres's
planner ever elides sort for `ANY()` over one element under *any*
schema — a general-Postgres-behavior question, not specific to this
repository — remains a further, un-chartered, and likely low-value pit
(the practical answer for this codebase, at the cardinalities checked, is
already established: it doesn't).

## 💰 Cost to productionize

Not applicable — diagnostic question about an existing, cited defect, not
a feature to build.

## 🔬 Reproduce

```sh
sudo -u postgres createdb prospect_assay6   # or any local, non-production Postgres 16
cd docs/assays/apparatus/0006-claim-1177-baseline-queue-count
sudo -u postgres PGDATABASE=prospect_assay6 ./run_assay.sh
grep -c "Sort Key" results/multi_queue_control.explain.txt   # 1
grep -c "Sort Key" results/single_queue_any.explain.txt      # 1 -- not blind, see pre-registration section
grep -c "Sort Key" results/any_cardinality_2.explain.txt     # 1 -- this record's blind confirmatory line
grep -c "Sort Key" results/single_queue_scalar.explain.txt   # 0 -- ledger #6's own line, for reference
```

`run_assay.sh` now runs all four arms (control, single-queue `ANY`,
single-queue scalar, and this record's cardinality-2 arm) in one pass and
writes each result fresh to its own `results/*.explain.txt`, so the
`grep` commands above always validate a just-produced plan, never a
stale checked-in one. One new apparatus file,
`any_cardinality_2_diagnostic.sql`, added under ledger #6's apparatus
directory; everything else reused verbatim. No migration added, no crate
code changed. The prototype does not merge.
