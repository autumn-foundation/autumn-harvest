## Phase X.Y — Correct `docs/performance.md`'s claim-plan attribution: any residual predicate defeats sort-elision, not just the sticky CASE (issue #1177)

`docs/performance.md` (issue #786) measured `claim_task`'s superlinear
backlog scaling and attributed it to one specific mechanism: the claim
query's `ORDER BY` leading with a non-indexable sticky-routing `CASE`
expression, which `idx_harvest_tq_poll` cannot serve. Read on its own, that
framing invites the natural next step — drop the `CASE`, or index the sticky
columns, and the cheap index-ordered plan should return.

Issue #1177 reproduces that this does not happen. With the `CASE` removed
entirely from `ORDER BY` (leaving only `priority DESC, scheduled_at`, an
exact match for `idx_harvest_tq_poll`'s key) and with `idx_harvest_tq_poll`
forced via planner hints so the optimizer has no other index to fall back
to, adding any single one of the query's other residual `WHERE` predicates —
including several with zero actual selectivity (100% of rows pass) —
independently collapses the plan straight back to `Seq Scan` + external-merge
`Sort`. This holds with and without `FOR UPDATE SKIP LOCKED`. Ten predicates
were tested independently against a 255 020-row fixture; all ten reproduce
the collapse, ruling out a selectivity-misestimate explanation. Two separate,
compounding effects are documented: (1) any residual `Filter` on an
otherwise index-order-matching scan defeats sort-elision and `LIMIT`
pushdown regardless of `FOR UPDATE`, and (2) `FOR UPDATE SKIP LOCKED`
additionally disables the bounded Top-N sort once (1) has already forced a
`Sort` node, turning bounded in-memory work into an unbounded external sort
that spills to disk at scale.

This also resolves the "Known limitations" bullet that called
`schedule_to_close` (#378), worker sessions (#606) and sticky routing (#235)
"cheap inline column tests" whose cost was simply unmeasured: measured now,
each is independently sufficient to trigger the same O(backlog) plan
regardless of the value it is tested against — they were never cheap, they
were untested.

**Zero engine impact (AC6-equivalent): no new `WorkflowEvent` variant, no
migration, no schema change, no public API change, and the claim query is
byte-for-byte unchanged.** Per the same measure-before-tune discipline issue
#786 established, this PR is documentation only: `docs/performance.md`
gains a new section ("Any residual predicate defeats sort-elision") laying
out the finding, corrects the TL;DR and "The plan" framing to stop implying
the `CASE` key alone would restore the cheap plan, and corrects the "Known
limitations" bullet. `tests/integration/performance_docs.rs` gains two new
guards — `the_case_key_is_not_published_as_sufficient_to_restore_the_cheap_plan`
and `known_limitations_no_longer_calls_the_unmeasured_predicates_cheap` —
following this file's existing banned-phrase-plus-required-correction
pattern, so the retracted CASE-only reading cannot silently return. Both
guards were written and verified failing (red) against the pre-fix doc text,
then verified passing (green) after the doc edit, alongside all 21
pre-existing guards in the same module (no regressions).

A genuine fix — a seek-and-refine restructuring of `claim_task_query()` —
is architectural, changes claim-fairness/latency guarantees under
contention, and needs sign-off from someone with full context on
`queue.rs`'s documented advisory-lock-ordering, exactly-once-claim and
`SKIP LOCKED`-concurrency-safety invariants. Issue #1177 explicitly scoped
that out, and this PR does not attempt it; it is tracked separately.
