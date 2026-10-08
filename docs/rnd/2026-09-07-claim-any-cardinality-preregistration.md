# Pre-registration: does `ANY()` itself, independent of array cardinality, defeat sort-elision on `idx_harvest_tq_poll`?

**Status:** pre-registration, committed before this question's own
confirmatory measurement. This is an explicit re-charter spawned by
[ledger #6](../assays/0006-claim-1177-baseline-queue-count.md), not an
edit to it — per that assay's own pre-registration, "single-queue" there
meant scalar equality (`queue_name = 'bench-q-0'`), a literal criterion
that measurement satisfied. A Codex review finding on the PR (verified
against `autumn-harvest/src/queue.rs:641`) correctly noted that scalar
equality is not a shape any production code path takes — the claim path
always binds `queue_name = ANY($2)`, even for a worker polling exactly one
queue — so ledger #6's confirmed verdict, while accurate to its own
letter, does not establish what a real single-queue deployment's plan
looks like. That is a new, separate question, not a retroactive change to
ledger #6's line.

**Order of events, stated plainly:** while investigating that review
finding, an `ANY(ARRAY['bench-q-0'])` variant was run once, exploratorily,
against ledger #6's own apparatus, and appeared to still show a `Sort`
node — the opposite of scalar equality's result. That exploratory run is
not admissible as this record's evidence (a result checked before its own
line existed is inadmissible on its own terms); this pre-registration
commits the line first, and the apparatus is re-run fresh below as this
question's actual, registered measurement.

## 🎯 Question

Holding everything ledger #6 held constant (10,000-row backlog, index
bias, base predicate, `ORDER BY`, `LIMIT 50`, `FOR UPDATE SKIP LOCKED`,
seed reused unmodified from ledger #5's own `seed.sql` with only `queues`
varied) — does replacing scalar equality (`queue_name = 'bench-q-0'`) with
`ANY()` over a **single-element** array (`queue_name =
ANY(ARRAY['bench-q-0'])`) — the actual shape `queue.rs:641` binds for a
one-queue worker — still produce `Index Scan using idx_harvest_tq_poll`
with **zero** `Sort` nodes, matching scalar equality's result? Or does the
`Sort` node persist, matching the 4-queue control's result despite
cardinality dropping to 1?

**Decision this feeds:** the same decider ledger #6 named — whoever next
re-charters issue #1340's seek-and-refine work. If `ANY()` at cardinality
1 still shows a `Sort` node, single-queue deployments get **no** cost
relief from this specific `ORDER BY`/`LIMIT` defect despite ledger #6's
own confirmed (but production-unrepresentative) result, and #1340's
fixture must not assume otherwise. If it doesn't, ledger #6's confirmed
result generalizes to the real binding shape and the multi-queue
explanation stands as originally framed.

## ⚖️ Pre-registered criteria

- **Cardinality-explains-it (matches scalar equality):** `ANY()` over a
  single-element array shows `Index Scan using idx_harvest_tq_poll`, zero
  `Sort` nodes.
- **Operator-defeats-it (matches the 4-queue control, refutes
  cardinality-as-sufficient-explanation):** a `Sort` node is present,
  regardless of whether the scan itself is a `Seq Scan` or an `Index
  Scan`.
- No partial credit. This is a binary, structural read, same standard
  ledger #6 and ledger #5 both used.

## Conditions

Identical to ledger #6's own conditions: same Postgres 16 instance
(16.13), same `schema.sql`, same seed generation (ledger #5's `seed.sql`,
`queues=1`, `keys=256`, `running_rows=0`), same bias, same query text
apart from the one predicate under test.

## Riskiest assumption, attacked first

That `ANY()`'s failure to elide the sort is genuinely about the operator,
not a residual effect of this apparatus's specific fixture (e.g. planner
statistics from a freshly `TRUNCATE`+reseeded table). The control
(ledger #6's own 4-queue `ANY` result, already measured on this instance)
and this record's fresh single-element `ANY` measurement, run back to
back against freshly reseeded data exactly as ledger #6 did, are the
cheapest test of that.

## Time box

Same session; the apparatus already exists (ledger #6's own
`single_queue_any_diagnostic.sql`, already written and available at
`docs/assays/apparatus/0006-claim-1177-baseline-queue-count/`). This
record's own contribution is the committed line, not new apparatus.

## Containment

Same local, non-production `prospect_assay6` database ledger #6 used. No
migration, no crate code change. Prototype does not merge.

## Prior art already checked

Ledger #6's own report and pre-registration, and the Codex review finding
on PR #1418 that prompted this re-charter (verified directly against
`autumn-harvest/src/queue.rs:641` before this record was written, not
taken on faith).
