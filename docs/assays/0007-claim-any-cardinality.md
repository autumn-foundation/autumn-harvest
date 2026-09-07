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
(commit `09fac5d`). That document states plainly that an exploratory,
unregistered run of this same query had already been made once while
investigating the Codex finding, and that it is *not* being used as this
record's evidence — the pre-registration was committed first, and the
apparatus re-run fresh (below) as the actual, registered measurement.

- **Cardinality-explains-it:** `ANY()` over a single-element array shows
  zero `Sort` nodes (matches scalar equality).
- **Operator-defeats-it:** a `Sort` node is present (matches the 4-queue
  control).

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

**Test** (single-queue `ANY(ARRAY['bench-q-0'])`, same 10,000-row
backlog, all in one queue): `Sort` node **present** — `Index Scan` reads
all 10,000 matching rows (not the `LIMIT`ed 50), 195 buffers, 4.117ms.
Structurally identical in shape to the control; cheaper only because the
underlying index range scanned is narrower (one queue's rows instead of
four queues').

For reference, scalar equality (ledger #6's own pre-registered arm, same
seed): no `Sort` node, 53 buffers, bounded scan (50 rows read).

`grep -c "Sort Key"`: control 1, `ANY(ARRAY['bench-q-0'])` 1, scalar
equality 0. Full output archived at
[`results/multi_queue_control.explain.txt`](apparatus/0006-claim-1177-baseline-queue-count/results/multi_queue_control.explain.txt)
and
[`results/single_queue_any.explain.txt`](apparatus/0006-claim-1177-baseline-queue-count/results/single_queue_any.explain.txt)
(both under ledger #6's apparatus directory — this record reuses it
directly rather than duplicating files).

**Against the line:** `ANY()` at cardinality 1 matches the
**operator-defeats-it** criterion — a `Sort` node is present, structurally
identical to the 4-queue control.

## 🏁 Verdict

**Confirmed: operator, not cardinality.** `ANY()` defeats sort-elision on
`idx_harvest_tq_poll` regardless of how many elements the array holds —
even a single-element array gets the same `Sort`-plus-full-scan plan a
four-element array does. Only scalar equality, a query shape no
production code path in this repository emits, gets the cheap plan.

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

**For the named decider:** the corrected, load-bearing finding across
ledgers #6 and #7 together is that **no** queue-count story, high or low,
gives single-queue deployments a free pass on this cost under the query
shape `queue.rs` actually emits. Any future #1340 re-charter should treat
this `ORDER BY`/`LIMIT` cost as present at every queue cardinality a real
worker can have, not as a multi-queue-specific tax. Whether Postgres's
planner ever elides sort for `ANY()` over one element under *any*
schema — a general-Postgres-behavior question, not specific to this
repository — remains a further, un-chartered, and likely low-value pit
(the practical answer for this codebase is already established: it
doesn't, here).

## 💰 Cost to productionize

Not applicable — diagnostic question about an existing, cited defect, not
a feature to build.

## 🔬 Reproduce

```sh
sudo -u postgres createdb prospect_assay6   # or any local, non-production Postgres 16
cd docs/assays/apparatus/0006-claim-1177-baseline-queue-count
sudo -u postgres PGDATABASE=prospect_assay6 ./run_assay.sh
grep -c "Sort Key" results/multi_queue_control.explain.txt   # 1
grep -c "Sort Key" results/single_queue_any.explain.txt      # 1 -- this record's own line
grep -c "Sort Key" results/single_queue_scalar.explain.txt   # 0 -- ledger #6's own line, for reference
```

No new apparatus files; this record reuses
`docs/assays/apparatus/0006-claim-1177-baseline-queue-count/` verbatim.
No migration added, no crate code changed. The prototype does not merge.
