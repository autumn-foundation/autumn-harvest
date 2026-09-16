## Engine — Batched seek-and-refine claim: `queue::claim_task_batched` (issue #1340)

Issue #1340 tracks the architectural fix `docs/performance.md`'s "Any
residual predicate defeats sort-elision" section (issue #1177) names but
declines to attempt: a seek-and-refine restructuring of the claim candidate
scan. `docs/assays/0005-claim-batched-seek-and-refine.md` (ledger #5)
prototyped one version of this shape and killed it on a pre-registration
fencepost bug (its adversarial fixtures resolved one batch later than the
registered line, not a mechanism defect — wall-clock cleared every line by
2.9x-190x). This PR is the real implementation that follows from that
prototype, corrected and DB-tested.

**Scope: the per-key concurrency gate (issue #247), not every residual
predicate.** `queue::claim_task_batched_candidates_query()` fetches an
ordered batch of `B` candidates (default 50) via `FOR UPDATE SKIP LOCKED`
with every gate `claim_task_query()` already applies EXCEPT the
concurrency-key soft filter. `queue::claim_batched_candidate_attempt_query()`
applies the SAME authoritative `pg_try_advisory_xact_lock` +
fresh-`COUNT` recheck the single-row path's `claimed` CTE already uses, to
one already-locked candidate at a time — never a batch-wide snapshot, which
ledger #5's own review caught as a real correctness gap in its first draft.
`queue::claim_task_batched()` walks the batch procedurally in Rust, and
fetches the next batch via a keyset cursor
`(sticky_rank, effective_priority, scheduled_at, id)` — four columns, with
an explicit `OR`-chain comparison, not three: `scheduled_at` and priority
commonly tie in a real backlog, and a three-column cursor silently drops
tied rows past a batch boundary (the second bug ledger #5's review caught).

**Not wired into the default claim path.** `claim_task`/`claim_task_on_shard`
are unchanged. Issue #1340 is explicit that no query change should land
against it without sign-off from someone with full context on `queue.rs`'s
exactly-once-claim and lock-ordering invariants — this PR delivers a real,
tested, measured building block for that review, not a switch of default
production behavior. It also does not implement the cross-region DR fence
(#954) or the by-id claim (#1312) the single-row path carries.

**Test evidence.** `tests/integration/claim_batched_tests.rs` (7 DB-backed
tests, red-then-green): equivalence with the single-row path on a plain
backlog, an adversarial saturated-concurrency-key fixture matching ledger
#4/#5's own shape, a multi-batch fixture with tied sort keys spanning a
batch boundary (regression test for the tiebreak bug above), `max_batches`
exhaustion, rate-limit bucket interaction, and — the gap ledger #5's own
single-session apparatus explicitly could not close — real concurrent
Tokio claimers racing a capped concurrency key, asserting the cap is never
exceeded. `queue.rs`'s `mod tests` gains 7 SQL-shape unit tests pinning the
query text (concurrency gate omitted from the batch scan, every other gate
preserved byte-for-byte, the cursor's four-column `OR`-chain, the
authoritative recheck's exact shape, the shared rate-limit-formula helper
used instead of a fourth hand-copied literal).

**Measurement.** `docs/performance-claim-batched-seek-and-refine.md`:
idle cost is 1.2x the single-row path (339 vs 277 buffers, 10,000-row/4-queue/
256-key fixture); under hot contention (2,000 `RUNNING` rows on the same
keys) the batched path is 2.06x faster end-to-end over 400 real claims each
(mean 874.8ms vs 1,804.9ms per claim). Neither query gets an index-driven
bounded scan at this backlog depth — the win is the concurrency-key
aggregate's cost, not a `LIMIT` pushdown, so the O(backlog)-scaling question
ledger #5 left open remains open. Real concurrent-claimer throughput
(distinct from the correctness this PR's concurrency test proves) also
remains unmeasured — both are named explicitly as what a reviewer still
needs before this becomes the default claim path.

**Zero engine impact on the existing claim path:** no new `WorkflowEvent`
variant, no migration, no schema change, `claim_task_query()` byte-for-byte
unchanged, and no change to the default claim call path.
