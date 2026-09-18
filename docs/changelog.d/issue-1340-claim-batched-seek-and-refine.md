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

Codex then caught an eighth bug, against the original implementation:
`claimed`'s own `started_at = NOW()` stamps a claim with the
transaction-frozen start time, not the real time of the claim. A batch
walk can spend real wall-clock time probing many candidates inside one
transaction, so `NOW()` can be stale by the whole walk's duration.
`start_to_close` and `heartbeat_timeout` are measured from `started_at`,
so a stale stamp silently steals part of a task's timeout budget before
it starts. Fixed by reusing `now_ts`: `started_at` reads the same
materialized `clock_timestamp()` value the deadline checks already use.

Codex then caught a ninth bug, against the seventh bug's own `now_ts`
fix: `now_ts` had no `FROM` clause of its own, so Postgres could resolve
it before `rate_limit_debit` even attempted its own row lock on the
rate-limit bucket. A concurrent transaction holding that lock would then
let `now_ts` capture a stale, pre-wait value that `rate_limit_debit`,
`claimed`, and `started_at` all reuse — a deadline expiring during the
wait would incorrectly still read as live. This one was confirmed
against a real Postgres instance directly, not just reasoned about: the
same query shape, driven by two genuinely concurrent connections (one
holding the bucket row's lock, one blocked waiting on it), mis-claimed
an expired row before the fix and correctly rejected it after. Fixed by
giving `now_ts` its own `FOR UPDATE` lock attempt on the exact bucket
row `rate_limit_debit` locks next, so `now_ts` cannot resolve before
that same wait ends.

Codex then caught a tenth bug, against the ninth bug's own fix: the
forced lock gated only on `$6::text IS NOT NULL`, missing
`rate_limit_debit`'s own `NOT ($7 = ANY($8))` circuit-breaker exclusion.
`rate_limit_debit` never touches the bucket row for a
circuit-breaker-bypassed activity, so such a claim has no reason to
wait on it — but `now_ts`'s forced lock made it wait anyway, serializing
a claim meant to run at full speed behind an unrelated transaction, and
risking a missed deadline on a lock it never needed. Fixed by adding
the same circuit-breaker exclusion to `now_ts`'s forced lock. Verified
with a regression test proving a bypassed claim completes quickly
despite a real, separately-held lock on the bucket row it never needs.

Codex then caught an eleventh bug (P1), against the seventh bug's own
`now_ts` fix: `rate_limit_debit` wrote `last_refilled_at = NOW()`, the
transaction-frozen time, not the real time of the debit. A long batch
walk can let another, faster transaction refill the same bucket in the
meantime; this transaction's own stale `NOW()` can then persist a
`last_refilled_at` from before that other write, letting a later
claimant re-accrue tokens for an interval already accounted for and
exceed the configured rate limit. Fixed by reusing `now_ts` for every
real-time read the rate-limit formula makes: `rate_limit_available` is
now `effective_available_tokens_expr`'s own formula with every `NOW()`
substituted for `(SELECT ts FROM now_ts)`, scoped to this one call site,
and `last_refilled_at` reads that same substituted value. Verified red
against the pre-fix code with a deterministic `pg_sleep` test (same
technique as the `started_at` test): `last_refilled_at` must land after
a 600ms in-transaction sleep, not near the pre-sleep timestamp a frozen
`NOW()` would give.

This same finding also surfaced a related, genuinely pre-existing (not
introduced by any of these fixes) limitation: the bucket row lock is
retained for the whole claim-attempt transaction, across every distinct
`rate_limit_key` any tried candidate carries. Unlike the already-documented
advisory-lock limitation, this is a real, blocking Postgres lock and can
deadlock two claimers walking overlapping bucket keys in opposite orders.
Postgres resolves that cleanly, by aborting one attempt with a typed
error, not a wedge or a double-claim. A real fix needs a redesign
(canonical lock ordering or per-candidate `SAVEPOINT`s), out of scope
here; documented as a named, pre-production gap instead.

Codex then caught a twelfth bug (P2): a candidate's deadline can expire
DURING the batch walk without ever touching its own bucket lock's wait —
not while waiting on that candidate's own bucket lock (the ninth bug),
but while an EARLIER candidate's bucket lock is being waited on. By the
time the walk reaches the later candidate, its deadline has already
passed, and it can never claim regardless of what its own bucket
protects — so waiting on that bucket lock anyway wastes an entire second
lock wait for nothing. Fixed by rejecting an already-expired candidate
in Rust, against a fresh `Utc::now()`, before ever issuing the attempt
query. Verified with a two-lock regression test: one candidate (funded
with exactly one token, so it survives the batch scan's own soft
rate-limit pre-filter) absorbs a real ~1s wait on its own bucket while
losing its one token to a concurrent locker; a second candidate's
deadline expires during that wait, and its own (still-locked, ~3s)
bucket must never be waited on at all — red at ~2.4s against the
pre-fix code, green at well under 2s after the fix.

Codex then caught a thirteenth bug (P2, a real correctness gap, not a
performance one): the batch scan's own build-routing gate
(`required_build_id`/`harvest_build_compat`) only filters candidates at
SCAN time. This candidate's own attempt is a separate, later statement,
so an operator revoking build compatibility in between was invisible to
`claimed`'s `WHERE`, which checked only the candidate id plus the
concurrency/rate-limit/deadline gates. A worker could therefore claim a
task requiring a build it was no longer compatible with, breaking the
replay-determinism guarantee build routing exists to protect --
`claim_task_query()`'s single atomic statement has no such window, but
this two-phase batched design does. Fixed by re-running the scan's own
build-routing gate, verbatim, in `claimed`'s `WHERE`, adding a tenth
bind (`$10`, worker build id) to `claim_batched_candidate_attempt_query()`.
Verified with a test that drives the attempt query directly: an
undeclared worker/required-build pair is rejected exactly as if
compatibility had just been revoked, and declaring compatibility
(`build_routing::declare_compat`) flips the same query's own re-check to
pass.

The same review round caught two more findings against that fix.
First, `rate_limit_debit` is a data-modifying CTE that runs whether or
not `claimed` uses its result -- the same shape as the original
rate-limit-leak bug and the deadline-leak bug above. The build-routing
gate had been added only to `claimed`'s `WHERE`, so a revoked-build
candidate still spent a token before being rejected; an adversarial
batch of revoked-build candidates sharing one rate-limited key could
drain far more than the documented one-token bound. Fixed by adding the
same gate, via a small `EXISTS` keyed on the candidate id, to
`rate_limit_debit`'s own `WHERE`. Verified with a test mirroring the
deadline-leak test: an undeclared build pair is rejected and the bucket
is left untouched.

Second, the twelfth bug's own fast pre-check compared
`schedule_to_close_at` against `Utc::now()` -- the worker host's clock,
not the database's. If that host clock runs ahead of Postgres, the
pre-check could reject a candidate the database's own
`clock_timestamp()` (the authoritative source every other deadline
check in this module trusts) would still consider live, delaying
dispatch until the skewed host clock caught up. Fixed with a new
`db_now` helper that reads `clock_timestamp()` from the database (no
table access, so it never waits on a lock) instead of the host clock.
Genuine clock skew between the test process and the database is not
practically reproducible in this environment, so this relies on the
existing deadline-recheck regression tests (which exercise the same
pre-check against real elapsed time) rather than a fabricated
skew-specific test.

Codex then caught a fifteenth bug (P2, a real correctness gap, the
same class as the thirteenth): the batch scan's own capability-label
gate (`required_capabilities` against `harvest_workers.labels`) only
filters candidates at SCAN time, the same two-phase staleness window
as build routing. A worker's labels can change between the scan and
this candidate's own attempt (a heartbeat refresh, or the same worker
id re-registering with different capabilities), and the attempt never
re-checked them. Fixed by adding a `worker_info` CTE to the attempt
query (reading `harvest_workers.labels` fresh, not the scan's stale
snapshot) and re-running the scan's own capability check, verbatim, in
`claimed`'s `WHERE`. Applied the SAME leak-prevention pattern already
used for the deadline and build-routing gates to `rate_limit_debit`'s
`WHERE` too, closing the same class of leak for this gate from the
start rather than needing a follow-up finding. Verified with a test
driving the attempt query directly (worker labels that do not satisfy
the requirement are rejected, then updating them flips the same
query's own re-check to pass) and a debit-leak test mirroring the
build-routing one (a capability-mismatched candidate leaves the bucket
untouched).

Codex then caught a sixteenth bug (P2, the same liveness class as the
tenth): `now_ts`'s forced bucket lock gated only on the rate-limit key
and the circuit-breaker exclusion, not on whether this candidate was
still build- or capability-eligible. `claimed` is certain to reject a
build-incompatible or capability-mismatched candidate, yet the forced
lock still waited on an unrelated transaction's bucket row first,
stalling the whole batch walk behind a lock the claim was never going
to use. Fixed by gating the forced lock on the SAME eligibility check
`claimed` and `rate_limit_debit` apply. Extracted the shared predicate
into `build_and_capability_eligibility_predicate` (and a wrapping
`candidate_still_build_and_capability_eligible` for callers with no
`harvest_task_queue` row directly in scope) rather than hand-copying it
a third time. The forced lock's own copy reads the worker's labels with
an inline lookup instead of the `worker_info` CTE: a CTE cannot
reference one defined after it, and `now_ts` must stay the query's
leading CTE for the seventh-bug fix above. Verified with a SQL-shape
test pinning the new predicate inside `now_ts`'s own clause, and a
DB-backed test mirroring the circuit-breaker skip test above (a real
lock held by a separate connection; a build-incompatible claim must
complete quickly despite contention it has no reason to wait on).

**Not wired into the default claim path.** `claim_task`/`claim_task_on_shard`
are unchanged. Issue #1340 is explicit that no query change should land
against it without sign-off from someone with full context on `queue.rs`'s
exactly-once-claim and lock-ordering invariants — this PR delivers a real,
tested, measured building block for that review, not a switch of default
production behavior. It also does not implement the cross-region DR fence
(#954) or the by-id claim (#1312) the single-row path carries.

**Test evidence.** `tests/integration/claim_batched_tests.rs` (23 DB-backed
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
untouched, also verified red then green), a regression test for the
`started_at`-backdating bug above (same `pg_sleep` technique, asserts
`started_at` lands after the sleep rather than near transaction start,
also verified red then green), a regression test for the ninth bug above
that drives two genuinely concurrent connections rather than a
`pg_sleep` stand-in -- one holds the rate-limit bucket row's lock past a
task's deadline, the other attempts the claim blocked on that exact
lock, and the test asserts the resulting claim and debit both correctly
reflect the deadline having passed (verified red against the pre-fix
code against a real Postgres instance, then green), a regression test
for the tenth bug above that holds the bucket row's lock from a separate
connection while a circuit-breaker-bypassed claim runs, asserting it
completes quickly rather than serializing behind a lock it never needed
(also verified red then green), a regression test for the eleventh bug
above using a deterministic `pg_sleep` (same technique as `started_at`)
asserting `last_refilled_at` lands after the sleep, not near the
pre-sleep frozen-`NOW()` value (verified red then green), a regression
test for the twelfth bug above using two real, separately-held locks —
one candidate absorbs a genuine wait, a second candidate's deadline
expires during that wait, and its own still-locked bucket must never be
waited on (verified red at ~2.4s against the pre-fix code, green under
2s), a regression test for the thirteenth bug above that drives
`claim_batched_candidate_attempt_query()` directly with an undeclared
worker/required-build pair, asserting it is rejected exactly as
`build_routing::revoke_compat` would leave it, then that declaring
compatibility flips the same query's own re-check to pass, a regression
test for the build-routing-debit-leak follow-up above (same
never-debits-on-rejection shape as the deadline-leak test, asserting an
undeclared build pair leaves the bucket untouched), a regression test for
the fifteenth bug above that drives
`claim_batched_candidate_attempt_query()` directly with a worker whose
labels do not satisfy `required_capabilities`, asserting rejection, then
that updating those labels flips the same query's own re-check to pass,
a regression test for the matching debit-leak follow-up (same
never-debits-on-rejection shape, asserting a capability-mismatched
candidate leaves the bucket untouched), sticky
routing and a
capability-routed-activity gate exercised end-to-end through the real
function (not just SQL-text checks), and — the gap ledger #5's own
single-session apparatus explicitly could not close — real concurrent
Tokio claimers racing a capped concurrency key, asserting the cap is
never exceeded. `queue.rs`'s `mod tests` gains 20 SQL-shape unit tests
pinning the query text (concurrency gate omitted from the batch scan,
every other gate preserved byte-for-byte including both capability-label
branches, the cursor's four-column `OR`-chain, the authoritative
recheck's exact shape, the concurrency probe's shape, the deadline
recheck's use of `clock_timestamp()` and not `NOW()` on both the debit
and the claim, the shared rate-limit-formula helper used instead of a
fourth hand-copied literal, the seventh-bug fix that `rate_limit_debit`
and `claimed` read one shared `now_ts` value rather than calling
`clock_timestamp()` twice, the eighth-bug fix that `started_at` reads
that same shared value instead of the frozen `NOW()`, the ninth-bug fix
that `now_ts` itself takes a `FOR UPDATE` lock on the same bucket row
`rate_limit_debit` locks next, the tenth-bug fix that this forced
lock also skips circuit-breaker-tracked activities, the eleventh-bug fix
that the rate-limit formula and `last_refilled_at` read `now_ts` instead
of `NOW()`, the twelfth-bug fix that an already-expired candidate is
rejected before any query at all, the thirteenth-bug fix that
`claimed`'s `WHERE` re-runs the scan's own build-routing gate, its
own follow-up fix that `rate_limit_debit`'s `WHERE` carries the same
gate, the fifteenth-bug fix that `claimed`'s `WHERE` re-runs the
scan's own capability-label gate too, with the same leak-prevention
pattern applied to `rate_limit_debit`'s `WHERE` from the start, and the
sixteenth-bug fix that `now_ts`'s own forced lock carries that same
build-and-capability eligibility check).

**Measurement.** `docs/performance-claim-batched-seek-and-refine.md`,
regenerated from a single run of
`autumn-harvest/scripts/claim_batched_seek_and_refine_perf_repro.sh`
against the final code, after all fifteen review findings above (Codex
flagged, twice, that the previously-committed numbers predated later
fixes — first the deadline/`now_ts` fixes, then the build-routing and
capability-label rechecks — and no longer measured the code they were
attributed to).
The script includes a committed, `#[ignore]`d capture test for the
end-to-end numbers, not an ad hoc run, and reseeds an identical fixture
per loop per the fix above: idle cost is 1.05x the single-row path (290
vs 275 buffers, 10,000-row/4-queue/256-key fixture); under hot
contention (2,000 `RUNNING` rows on the same keys) the batched path is
~2.1x faster end-to-end over 400 real claims each (mean 852.9ms vs
1,766.8ms per claim). This fixture sets no `rate_limit_key`, no
deadline, no `required_build_id`, and no `required_capabilities`, so the
ratio is essentially unchanged from the prior captures — none of the
later fixes touch the concurrency-gate path it exercises.
Neither query gets an index-driven bounded scan at this backlog
depth — the win is the concurrency-key aggregate's cost, not a `LIMIT`
pushdown, so the O(backlog)-scaling question ledger #5 left open remains
open. Real concurrent-claimer throughput (distinct from the correctness
this PR's concurrency test proves) also remains unmeasured — both are
named explicitly as what a reviewer still needs before this becomes the
default claim path.

**Zero engine impact on the existing claim path:** no new `WorkflowEvent`
variant, no migration, no schema change, `claim_task_query()` byte-for-byte
unchanged, and no change to the default claim call path.
