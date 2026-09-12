## Phase 5.x — Cross-shard child placement: five P1 follow-ups from #1260 (issue #1263)

**Scope.** Issue #1263 tracks 17 follow-ups deliberately deferred from PR
#1260 (opt-in cross-shard child workflow placement) so that PR could land
without widening. This PR closes the six the issue's own author flagged
P1, all introduced by #1260: items 7, 10, 11, 13, 15, and 17.

**Item 15 — the zero-writable-shard fallback returned the default shard,
not the parent's.** `shard::resolve_child_placement`'s `Distributed`
branch degenerated to `router.default_shard()` when nothing was writable.
The two coincide only when the parent already lives on the default shard;
for any other in-flight parent the child was encoded to a different shard,
the persist path classified it remote, and the drain preflight rejected
it — the exact drain deadlock the fallback exists to prevent. Now returns
`parent_shard`, which is always local by construction. The test that had
pinned the bug is rewritten to use a parent on a non-default shard and
assert on `parent_shard`.

**Item 7 — a cross-shard child's chain deadline was anchored at the
parent's decision, not its own creation.** `CrossShardChildSpec` carried
the absolute `chain_deadline_at`, computed at spawn time; a relay running
late (unreachable target shard, backlog, worker restart) could hand the
child a deadline already in the past. `chain_execution_timeout_secs` (a
duration) is now the only thing that travels; `start_child_on_target`
derives the absolute value at the child's own creation, exactly like
`deadline_at`/`sla_deadline_at` already did (round 4 of #956).

**Item 11 — parent locality was derived from the installed router instead
of durable data.** `worker::parent_is_on_another_shard`'s unencoded-parent
branch asked the process-global router for `default_shard()`. Placement
can be resolved by a context-local router
(`WorkflowContext::with_shard_router`), so the router that decided a
parent's shard and the router asked here could disagree. The function is
now `async` and takes `conn`: for an unencoded parent it asks whether the
parent's row is visible on this connection — a durable fact — instead of
asking any router. Zero added cost on the common (encoded-parent) path.

**Item 13 — a child cancelled while `PENDING_START` was still created and
enqueued.** The relay's decision table sends every `PENDING_START` row to
`StartChild` regardless of a pending cancel, because a cancel that lands
mid-creation still needs a row to act on — that part is correct. But
`start_child_on_target` used to create the row and enqueue its task
unconditionally, leaving a real window in which a worker could claim that
task before a later sweep cancelled it. It now checks the row's
`cancel_requested` flag after inserting: if set, it appends
`WorkflowCancelled` right behind `WorkflowStarted` in the same batch, seals
the row `CANCELLED`, and never enqueues a task at all.

**Item 10 — PII erasure did not cascade to cross-shard children
(compliance-sensitive false success).** `erase::collect_child_ids` only
ever queried the parent's own shard, which a cross-shard child's row is
never on — an erasure request against such a parent returned success
while the child's payloads stayed in the clear. `cascade_children` now
also reads `harvest_cross_shard_children` and routes each remote child to
its own target shard, reusing the identical same-shard/summary-only/hold
gating logic (factored into `erase_one_child`) there. Additive: the
existing `erase_workflow_payloads` is unchanged for every caller that has
no `ShardedDbPool`; a new `erase_workflow_payloads_with_pool` is what
`erase_workflow_payloads_all_residences` — already the one production
`/erase-payloads` uses — now calls, so the fix reaches real traffic
without an API change. Scope boundary, documented on the new function: a
cross-shard child's outbox pointer is deleted once fully settled, so this
does not reach a child erased long after both sides are terminal — the
same gap item 14 names for `GET /workflows/{id}/stack`.

A follow-up review of this same fix surfaced two further gaps, both
closed here. First, `harvest_cross_shard_children.child_spec` — the
outbox row's own un-encoded copy of the child's `input`, on the PARENT's
own shard — was never scrubbed at all, a second, always-reachable PII
residence the target-shard erase never touched. `cascade_children` now
scrubs it (`scrub_cross_shard_child_spec`) once the outbox row is
`STARTED`; a `PENDING_START` row is left alone, since the relay still
needs that exact input to create the child. Second, a cross-shard child
not yet visible on its target shard (the relay's own crash-and-retry
window between committing the child's row and marking the outbox row
`STARTED`) fell through `erase_one_child` as `(None, None, None)` and was
silently treated as a clean success. It is now reported as a
`SkippedChild`, not dropped.

**Item 17 — the persist-time preflight validated a placement against the
process-global router even when a context-local one resolved it.**
`WorkflowContext` gained `resolved_placement_router()`, returning the
EXPLICIT router installed via `with_shard_router` (never the global
fallback). The executor's `drive_workflow` and every `run_workflow_with_state*`
entry point now return it as a fourth tuple element, threaded through the
worker's persist chain (`persist_workflow_outcome` →
`handle_suspended_workflow` / `persist_terminal_outcome_commands` →
`persist_all_started_child_workflows` / `persist_child_timeout_race` /
`persist_mixed_suspension_batch` / `create_detached_child_executions` /
`insert_awaited_child_execution` / `DetachedSpawnPersistence`) to the
three `preflight_target_shard` call sites, which now consult
`effective_placement_router` (context-local, else the global asked
fresh) instead of always the global. As the issue itself notes, no public
worker API installs a context-local router on the real dispatch path
today — `with_shard_router` is a test/embedder seam — so this closes a
real, reasoned gap without a reachable end-to-end regression test; the
new `resolved_placement_router` accessor is unit-tested directly, and the
full existing cross-shard suite passed unchanged after the refactor.

**Not in this PR.** Items 8, 9, 12, 14, 16, and 6 (P2s and one
backup-verifier item outside the cross-shard relay's own surface) stay
open on #1263, per that issue's own reasoning for why each was deferred
from #1260 rather than folded in here.

**Test evidence.** New DB-gated regression tests in
`cross_shard_children_tests.rs`: chain-deadline anchoring, born-cancelled
(no task ever enqueued), cross-shard erase (payload tombstoned on the
target shard), parent-locality (durable row check, not the router),
outbox `child_spec` scrubbing, and a `PENDING_START` child reported as
skipped rather than dropped. The zero-writable-shard test is rewritten in
place. A new pure unit test pair on
`WorkflowContext::resolved_placement_router`. Full
`cross_shard_children_tests.rs` and `cross_shard_child_placement_unit.rs`
suites (51 tests) pass, plus `legal_hold_tests.rs` and
`retention_summary_tests.rs` (26 tests) unaffected by the same-shard erase
signature change. No new `WorkflowEvent` variant, no migration.
