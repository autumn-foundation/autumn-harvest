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

A third correctness bug surfaced in THIS PR's own review, not ledger #5's:
the first draft's per-candidate walk debited a rate-limit token for every
candidate tried, including one the concurrency gate always rejected. An
adversarial batch sharing a saturated `concurrency_key` and a
`rate_limit_key` could leak up to `batch_size * max_batches` tokens from
one bucket, instead of the single-row path's documented one-token bound.
`queue::claim_batched_candidate_concurrency_probe_query()` fixes this: a
cheap, read-only concurrency check runs first, so the rate-limit debit
only ever runs for a candidate that already cleared the concurrency gate.

PR review (Codex) caught a fourth bug, in the measurement itself, not the
implementation: the first draft's end-to-end capture shared one fixture
across both the single-row and batched loops, so the batched loop — which
ran second — measured a smaller, differently-shaped backlog than the
single-row loop had (the single-row loop's 400 claims move rows `PENDING`
-> `RUNNING`), silently invalidating the ratio. Fixed by reseeding an
identical fresh fixture before each loop.

Codex's review then caught a fifth bug, back in the implementation: the
batch scan's `schedule_to_close_at > NOW()` filter uses `NOW()`, frozen
at transaction start (load-bearing for the keyset cursor, so it cannot
simply switch to real time). A deadline that passes in REAL wall-clock
time — while an earlier candidate in a long batch search is still being
tried, up to `batch_size * max_batches` attempts with the default
config — would incorrectly still read as live against that frozen
snapshot, letting an already-expired task be claimed and dispatched.
Fixed the same way as the concurrency gate: the batch scan's filter
stays a soft, frozen-`NOW()` pre-filter, and
`claim_batched_candidate_attempt_query()` gains an authoritative
`clock_timestamp()` recheck at claim time.

Codex then caught a sixth bug, against that fifth fix: the new
`clock_timestamp()` recheck only gated `claimed`, the CTE that performs
the state update. `rate_limit_debit` is a separate, data-modifying CTE
in the same query, and Postgres runs it regardless of whether `claimed`
uses its result. An expired-but-rate-limited candidate still spent a
token — the same leak shape the third bug fixed, recreated on the
deadline path instead of the concurrency path. Fixed by adding the same
deadline check to `rate_limit_debit`'s own `WHERE` clause.

Codex then caught a seventh bug, against that sixth fix: `clock_timestamp()`
is volatile, so the two direct calls the sixth fix added — one per CTE —
could return two different real times. A deadline falling between those
reads could let `rate_limit_debit` commit its spend in the same statement
where `claimed` rejects the row for that same deadline. Fixed by adding a
leading `now_ts` CTE that calls `clock_timestamp()` exactly once; both
`rate_limit_debit` and `claimed` now read that one materialized value, so
they always agree on the deadline decision.

**Not wired into the default claim path.** `claim_task`/`claim_task_on_shard`
are unchanged. Issue #1340 is explicit that no query change should land
against it without sign-off from someone with full context on `queue.rs`'s
exactly-once-claim and lock-ordering invariants — this PR delivers a real,
tested, measured building block for that review, not a switch of default
production behavior. It also does not implement the cross-region DR fence
(#954) or the by-id claim (#1312) the single-row path carries.

**Test evidence.** `tests/integration/claim_batched_tests.rs` (12 DB-backed
tests, red-then-green): equivalence with the single-row path on a plain
backlog, an adversarial saturated-concurrency-key fixture matching ledger
#4/#5's own shape, a multi-batch fixture with tied sort keys spanning a
batch boundary (regression test for the tiebreak bug above), `max_batches`
exhaustion, rate-limit bucket interaction, a regression test for the
rate-limit-leak bug above (verified red against the pre-fix code, then
green), a regression test for the deadline-recheck bug above (also
verified red against the pre-fix code -- drives
`claim_batched_candidate_attempt_query()` directly with an injected
`pg_sleep` so the real-time-vs-frozen-`NOW()` gap is deterministic, not
timing-dependent), a regression test for the rate-limit-debit-leak-on-
deadline bug above (same `pg_sleep` technique, asserts the bucket is
untouched, also verified red then green), sticky routing and a
capability-routed-activity gate exercised end-to-end through the real
function (not just SQL-text checks), and — the gap ledger #5's own
single-session apparatus explicitly could not close — real concurrent
Tokio claimers racing a capped concurrency key, asserting the cap is
never exceeded. `queue.rs`'s `mod tests` gains 10 SQL-shape unit tests
pinning the query text (concurrency gate omitted from the batch scan,
every other gate preserved byte-for-byte including both capability-label
branches, the cursor's four-column `OR`-chain, the authoritative
recheck's exact shape, the concurrency probe's shape, the deadline
recheck's use of `clock_timestamp()` and not `NOW()` on both the debit
and the claim, the shared rate-limit-formula helper used instead of a
fourth hand-copied literal, and the seventh-bug fix that
`rate_limit_debit` and `claimed` read one shared `now_ts` value rather
than calling `clock_timestamp()` twice).

**Measurement.** `docs/performance-claim-batched-seek-and-refine.md`,
regenerated from a single run of
`autumn-harvest/scripts/claim_batched_seek_and_refine_perf_repro.sh` (now
includes a committed, `#[ignore]`d capture test for the end-to-end numbers,
not an ad hoc run, and reseeds an identical fixture per loop per the fix
above): idle cost is 1.05x the single-row path (290 vs 275 buffers,
10,000-row/4-queue/256-key fixture); under hot contention (2,000
`RUNNING` rows on the same keys) the batched path is ~2.0x faster
end-to-end over 400 real claims each (mean 850.7ms vs 1,717.3ms per
claim). Neither query gets an index-driven bounded scan at this backlog
depth — the win is the concurrency-key aggregate's cost, not a `LIMIT`
pushdown, so the O(backlog)-scaling question ledger #5 left open remains
open. Real concurrent-claimer throughput (distinct from the correctness
this PR's concurrency test proves) also remains unmeasured — both are
named explicitly as what a reviewer still needs before this becomes the
default claim path.

**Zero engine impact on the existing claim path:** no new `WorkflowEvent`
variant, no migration, no schema change, `claim_task_query()` byte-for-byte
unchanged, and no change to the default claim call path.
