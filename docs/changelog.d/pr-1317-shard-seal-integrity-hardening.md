## Fix — Shard-rebalance seal integrity, legal-hold cutover race, and successor minting (issue #1317)

Follow-up to #964 and PR #1305, addressing the highest-severity findings
from that PR's round 6-12 review: several ways the shard-rebalance feature
could strand or destroy the one forwarding seal an execution's id resolves
through, plus several call sites that derived a shard from an
`ExecutionId`'s origin bits instead of the row's current residence.

**Seal-predicate hardening.** `existing_seal` (read before a
reverse-migration restage) matched `state = 'MIGRATED'`, unlike its sibling
`read_forward`, which matches on the forwarding pointer alone with a
documented reason: a sealed row can be force-written to another state. A
resume that re-drives a `stage_copy` call after a crash before the phase
advanced past `PENDING` left the row `MIGRATING` with a carried pointer —
exactly the state `existing_seal`'s old predicate could not see — dropping
the pointer on the next staging pass. `existing_seal` now matches on the
pointer, like `read_forward`.

**Abort-restore hardening.** `discard_staged_copy_restoring_seal`'s
fallback DELETE matched `state IN ('MIGRATING', 'MIGRATED')`. An abort whose
target `stage_copy` never actually touched — because it failed before its
target transaction committed — left the target holding its own untouched
`MIGRATED` seal, which the DELETE then destroyed outright. The function now
checks the row is `MIGRATING` before touching it at all, leaving an
untouched seal and its history alone.

**Legal-hold cutover race (round 6, finding #5).** `stage_copy` snapshots
the execution row with no lock, so an administrator placing or refreshing a
hold between that snapshot and the cutover landed the hold only on the
source; the cutover guard checked only quiescence and history, not hold
state, and could seal past it. Fixed the same way `HISTORY_UNCHANGED_SQL`
guards event drift: `verify_target_copy` re-reads and stamps the source's
current `legal_hold_set_at`, and the cutover's atomic guard
(`LEGAL_HOLD_UNCHANGED_SQL`) requires the live value to still match at seal
time. **Migration**: `harvest_shard_migrations` gains a nullable
`verified_legal_hold_set_at TIMESTAMPTZ` column (additive only).

**Resume-sweep robustness (round 8).** `resume_incomplete_migrations`
checked out its per-record source/target connections with `?`, so one
record naming an unavailable or unconfigured shard aborted the whole sweep,
starving every other record behind it. Checkout failure is now a
per-record outcome, like the step errors the loop already handles.

**Post-cutover audit gap.** `migrate_quiescent_executions`'s batch loop
propagated any error from `migrate_execution` with `?`, which could discard
the one audit record for a migration whose cutover had already committed.
The error is now converted into an auditable `Aborted` outcome before the
loop continues.

**Residence-routing fixes (partial).** Retry and continue-as-new successors
were minted via `ExecutionId::new_for_shard(exec_id.shard())` — the
predecessor id's *origin* shard bits, stale once the row is rebalanced. A
rebalanced predecessor's successor was then minted (and inserted) on the
wrong shard, with no forwarding row for its new id: unreachable by every
id-routed handle and API. Both sites now read the row's current `shard_id`
(via `shard_of_held_row` or the already-loaded execution row) instead of
decoding the id. This is the highest-impact slice of the residence-routing
category the issue catalogs across worker.rs, handle.rs, store.rs, api.rs,
mcp_tools.rs, completion_trigger.rs, and context.rs (19+ call sites across
7 subsystems); the remainder, along with the CLI codec-registry gap, the
candidate-scan cap, the signal-activation gap, the SSE resume cursor, and
the listing duplicate, are tracked in a follow-up issue.

**Test evidence.** `shard_rebalance_db_tests.rs` gains 6 new integration
tests against real two-shard Postgres fixtures, each written and verified
failing (red) against the pre-fix code, then passing (green) after the fix:
seal-carrying resume, abort-before-staging, hold-placed-after-verification,
hold-released-after-verification, and resume-sweep-past-one-bad-target. The
full existing `shard_rebalance_db_tests` suite (43 tests) and the
`terminal_write_ownership_tests`/`cross_type_continue_as_new_tests` suites
(43 tests) pass with no regressions. No new `WorkflowEvent` variant, no
mutation of `harvest_events`.
