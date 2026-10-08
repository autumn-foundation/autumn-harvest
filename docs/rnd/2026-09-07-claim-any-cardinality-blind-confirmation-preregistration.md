# Pre-registration addendum: a genuinely unseen cardinality for ledger #7's `ANY()`-defeats-sort-elision claim

**Status:** pre-registration, committed before this specific query has
been run in any form. This addresses a real gap in
[ledger #7's original pre-registration](2026-09-07-claim-any-cardinality-preregistration.md)
(commit `09fac5d`), caught by a third round of Codex review: that
document disclosed, honestly, that its own `ANY(ARRAY['bench-q-0'])`
query had already been run once, exploratorily, before the
"pre-registration" was committed. The review correctly points out that
for a deterministic `EXPLAIN` against a deterministically-generated
fixture (no randomness in `generate_series`-based seeding, no concurrent
load), re-running the *identical* apparatus after already knowing its
structural outcome is not a genuine confirmatory trial — there was no
real chance of a different result, so committing a line and then
re-running the same computation doesn't carry the evidentiary weight
pre-registration is meant to provide. Ledger #7's own verdict is not
retracted (see below for why), but this addendum supplies the thing that
was actually missing: a condition this apparatus has never been run
against, with its outcome unknown at commit time.

## 🎯 Question

Cardinality 1 (ledger #7) and cardinality 4 (ledger #6's control) have
both been measured; cardinality 2 has not. Does `queue_name =
ANY(ARRAY['bench-q-0', 'bench-q-1'])` — a genuinely untested point between
them — also show a `Sort` node, consistent with "any `ANY()` cardinality
≥1 defeats sort-elision," or does it pattern with scalar equality
instead, which would suggest a threshold effect ledger #7's two-point
comparison could not have detected?

## ⚖️ Pre-registered criteria

- **Consistent with ledger #7 (operator, not cardinality, is the
  driver):** the `EXPLAIN` output contains a `Sort` node.
- **Inconsistent (a cardinality threshold exists somewhere between 1 and
  4, not just "any ANY() at all"):** no `Sort` node.
- No partial credit.

## Conditions

Same apparatus, same instance, same bias, same base predicate/`ORDER
BY`/`LIMIT`/lock clause as every other arm in this family. Seed: ledger
#5's own `seed.sql` with `queues=2` (all 10,000 rows split between
`bench-q-0` and `bench-q-1`), `keys=256`, `running_rows=0` — identical
generation to every prior arm, cardinality is the only varied input.

## Riskiest assumption, attacked first

That the 1-vs-4 comparison ledger #7 already ran actually generalizes to
"any cardinality ≥1," rather than being a coincidence of the two specific
points tested. A genuinely blind third point is the cheapest way to add
real evidence for or against that generalization, reusing apparatus that
already exists.

## Time box

Same session; one new seed parameter, one new diagnostic file, both
trivial given the existing apparatus.

## Containment

Same local, non-production `prospect_assay6` database. No migration, no
crate code change. Prototype does not merge.

## What this does and doesn't change about ledger #7

Ledger #7's verdict is not retracted by this addendum — its comparison of
cardinality 1 against cardinality 4 is real, valid evidence (a `Sort`
node was present at cardinality 1 where scalar equality, a different
operator, was absent) whether or not that specific rerun was blind to
its own outcome; scalar equality vs. `ANY()` at n=1 remains a genuine
head-to-head, and the seed-shape fix from ledger #6's own post-review
still applies to it. What is retracted, or rather never should have been
implied, is treating the *second* run of `ANY(ARRAY['bench-q-0'])` — the
one in ledger #7's own "fresh" section — as if it were an independent
confirmatory trial. It wasn't; it was a deterministic replay of a known
result. This addendum's cardinality-2 result is what actually plays that
role.
