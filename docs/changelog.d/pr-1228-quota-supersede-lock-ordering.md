## Phase — Quota-vs-supersede ordering and a detached-child lock-ordering gap (issue #1228)

A seventh automated Codex review round on PR #1221 (issue #946, per-tenant
resource quotas) surfaced two more findings against commit `3bda6fc`, after
all 5 permitted review rounds for that PR were used and round 6's findings
were already tracked in #1227. Filed separately from #1227: these two are
about admission ORDER, not a missing retry backoff.

**Finding 1 (correctness) — `autumn-harvest/src/execution.rs`.** On the
plain fresh-insert admission path,
`start_or_load_workflow_execution_collect` ran `enforce_quota_admission`
BEFORE the issue #811 latest-wins supersede pass
(`run_latest_wins_supersede`). Quota admission counts every non-terminal
row sharing the resolved key, including an incumbent the supersede pass is
about to cancel a few lines later. A workflow declaring both a tight
`max_active_executions` quota cap and `on_conflict = "cancel_running"` on
the SAME key — a natural pairing: "at most one active run per tenant, and a
newer request replaces a stale one" — hit the cap on the incumbent's own
occupancy and was rejected with `QuotaExceeded` before supersede ever ran,
silently defeating `cancel_running` for exactly that configuration.

Fix, attempt 1 (reverted): the issue's own suggested fix — run
`run_latest_wins_supersede` before `enforce_quota_admission` on this path —
regresses `concurrency_supersede_tests::nested_self_referential_admission_
emits_residual_over_limit_counter`. A self-referential completion trigger
can start a NESTED admission synchronously, inside the cancellation this
pass performs. That nested admission's own candidate scan requires a
`harvest_task_queue` row to count the OUTER admission as a protected,
in-flight run (`active_runs_for_key`'s "Protected in-flight admissions"
note) — a row this pass's caller does not insert until AFTER `append_events`
and `queue::enqueue`. Moving supersede earlier runs it before that task row
exists, so the nested admission's scan never sees the outer admission as
protected, undercounting the shed target and silently dropping the residual
metric this test pins.

Fix, as shipped: a dry-run credit instead of a reorder. New
`crate::concurrency::dry_run_supersede_shed_count` takes the same
`lock_concurrency_key` and reads how many runs the pass WOULD shed right
now, without cancelling anything. `start_or_load_workflow_execution_collect`
calls it before `enforce_quota_admission`, which gained a
`pending_supersede_credit: u64` parameter subtracted from
`active_executions` alongside the pre-existing "subtract 1 for our own row"
adjustment. The real `run_latest_wins_supersede` call keeps its original
position, unchanged, after `append_events`/`queue::enqueue` — so the
nested-admission invariant above still holds. The advisory lock the dry run
takes is transaction-scoped and re-entrant, so the later real pass simply
re-acquires it; no new lock order, no double-counting. Every OTHER
`enforce_quota_admission` call site (three in `worker.rs`, one in
`cross_shard_child.rs`, one in `replace_execution`) passes `0` for the new
parameter — byte-for-byte unaffected.

**Finding 2 (correctness / liveness) — `autumn-harvest/src/worker.rs`.**
The round-5 fix for the AWAITED-child fan-out
(`persist_all_started_child_workflows`) pre-acquires every distinct
`(workflow_name, quota_key)` advisory lock this fan-out needs, in one
sorted order, before inserting any child row — closing an ABBA deadlock
where two concurrent parents fanning out to the same quota keys in
opposite command order could each hold one key while waiting on the other.
`create_detached_child_executions`, called earlier in the same enclosing
function, had no equivalent step: it called `enforce_quota_admission` (and
so `lock_quota_key`) once per detached child, in raw command order, inside
a single loop. Two parents each spawning two detached children under keys
`A` and `B`, in opposite order, could deadlock the identical way. Postgres
aborts one side with a raw `deadlock_detected` error — not
`HarvestError::QuotaExceeded`, so `recover_from_child_quota_exceeded`'s
existing catch never sees it, and it terminally fails an otherwise-healthy
parent over a transient conflict.

Fix: `create_detached_child_executions` now collects the distinct
`(workflow_name, quota_key)` pairs across every local (non-cross-shard)
`SpawnDetachedChildWorkflow` command whose target declares a capped,
resolvable quota, into a `BTreeSet`, and locks them all in that sorted
order before the per-child insert loop runs — mirroring the awaited-child
fix exactly. `lock_quota_key`'s `pg_advisory_xact_lock` is re-entrant
within one transaction, so the loop's own `enforce_quota_admission` call
simply re-acquires what is already held. A cross-shard child is excluded
(its own shard locks it, via the relay) and an already-created child
(crash-restart replay) is excluded via a best-effort snapshot query — one
batched lookup, skipped entirely when a decision cycle has no detached
spawns at all (zero default overhead). That snapshot is for lock planning
only: the per-child insert loop's own idempotency check stays a fresh
per-row query, unchanged, so an in-batch duplicate `child_id` the snapshot
might miss is still caught before a duplicate insert.

No new `WorkflowEvent` variant, no migration, no replay impact — both are
pure admission-ordering fixes.

**Tests, red → green.** Finding 1: new
`autumn-harvest/tests/integration/quota_supersede_ordering_tests.rs` drives
the real `start_or_load_workflow_execution` entry point with a
`max_active_executions = 1` quota and `on_conflict = "cancel_running"`
declared on the same key —
`cancel_running_supersede_wins_over_a_tight_quota_cap_on_the_same_key`
(the money test: second admission must cancel the incumbent and succeed,
never reject with `QuotaExceeded`),
`cancel_running_supersede_chain_never_trips_the_quota_cap` (ten admissions
in a row, each superseding the last), and
`defer_policy_still_enforces_the_quota_cap_unchanged` (a `Defer` policy
declares no supersede, so the cap must still reject the 2nd admission, byte
for byte as before). The money test and the chain test both fail against
unmodified `execution.rs` (confirmed by stashing the fix and re-running:
both reject with `QuotaExceeded` instead of superseding) and pass with the
fix restored. Re-running `concurrency_supersede_tests.rs`'s full issue #811
suite after the fix caught attempt 1's regression
(`nested_self_referential_admission_emits_residual_over_limit_counter`)
before it shipped; that suite is green against the dry-run-credit fix, and
was also confirmed green against unmodified `execution.rs` (so the fix
introduces no NEW dependency the suite would need to guard).

Finding 2: `create_detached_child_executions` is private, so it cannot be
driven directly from an integration test. New
`autumn-harvest/tests/integration/quota_lock_ordering_tests.rs` instead
exercises `autumn_harvest::quota::lock_quota_key` — the exact advisory-lock
primitive both fan-outs call — under a deterministic, manually-sequenced
two-connection interleaving (not timing-dependent): two connections locking
the SAME two keys in OPPOSITE order are forced into a genuine ABBA cycle
and Postgres aborts exactly one side with a `deadlock_detected` error
(`opposite_order_lock_acquisition_deadlocks`); two connections locking the
SAME two keys in the SAME (sorted) order never deadlock even under real
concurrency — the later one simply blocks and then proceeds
(`same_order_lock_acquisition_never_deadlocks`). Together these pin the
exact property the `BTreeSet` pre-acquisition pass relies on. The existing
`quota_enforcement_tests.rs` detached-child coverage
(`detached_child_spawn_honors_target_quota_parks_parent_then_succeeds` and
its mixed-batch sibling) and `concurrency_supersede_tests.rs`'s full issue
#811 suite were re-run against the refactored function and stay green,
guarding against a regression in the existing per-child/per-key behavior.
