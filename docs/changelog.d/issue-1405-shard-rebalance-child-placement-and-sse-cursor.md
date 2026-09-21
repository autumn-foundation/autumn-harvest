## Fix — child placement and SSE resume cursor use residence, not origin (issue #1405)

Follow-up to #964/#1317/#1596. That prior work fixed the residence-routing
bug class (deriving a shard from `ExecutionId`'s origin-encoded bits instead
of a row's live residence after a shard-rebalance migration) at
`handle.rs` (`WorkflowHandle`'s cancel/terminate/result paths),
`completion_trigger.rs`, and ten sites across `api.rs`/`mcp_tools.rs` (SSE
listeners and audit attribution). Two items were explicitly left as
follow-up work: `context.rs`'s child-id minting, and the SSE resume
cursor. This PR fixes both.

**Child placement now inherits the parent's CURRENT shard, not its
origin.** `WorkflowContext::mint_child_id` and `race()`'s inline
child-workflow mint both derived a `ParentShard`-placed child's shard from
`self.exec_id.shard()` — the bits encoded when the parent's id was minted,
not where the parent's row lives today. The consequence was not a safe,
fail-closed unresolvable id: `worker.rs`'s `child_target_shard` classifies
a child as local vs. cross-shard by comparing the id's encoded bits against
the parent's LIVE `shard_id`, so a stale-origin child was classified as
cross-shard and relayed onto the origin shard via the ordinary
cross-shard-child path. `cross_shard_child::preflight_target_shard` only
checks that the target has a live, writable pool — it has no notion of
"is this actually where the parent lives" — so on a normal, healthy origin
shard the relay succeeds and silently writes the child's row there. The
failure mode was silent misplacement onto the wrong shard, not a lookup
that fails closed.

Fixed by threading the parent row's live `shard_id` into `WorkflowContext`
at construction, mirroring the existing `deadline_at`/`with_deadline`
pattern exactly: a new `current_shard_id: Option<ShardId>` field and
`with_current_shard_id` builder, populated by the worker's live executor
entry point (`run_workflow_with_state_history_policy_and_caps`, from
`WorkflowExecuteSpanMeta.shard_id`, itself already read live from the
execution row) and by `WorkflowHandle::execute_query_in_process` (from the
loaded row directly). `mint_child_id` and the race-branch inline mint now
prefer `current_shard_id`, falling back to `self.exec_id.shard()` — the
pre-fix behaviour — only when no live shard was threaded in (replayer /
test-env paths). Safe for replay determinism by construction: per
`mint_child_id`'s own contract, a fresh dispatch is the only caller, and a
replay always reuses the child id recorded in `ChildWorkflowStarted`.

**The SSE resume cursor is `event_id`, not `harvest_events.id`.**
`/executions/{exec_id}/events/stream`'s `Last-Event-ID` cursor was
`harvest_events.id`, a shard-local `BIGSERIAL` a migration deliberately
does not copy (the target assigns fresh values from its own sequence). A
client reconnecting with a pre-migration cursor got a meaningless
comparison against the target's own unrelated `id` sequence — dropping
events, or replaying the whole history as duplicates.

Fixed by switching the wire cursor to `event_id` (copied byte-for-byte by a
migration) everywhere the stream emits an SSE `id:` field, and translating
it to the connection's own `harvest_events.id` at connect time via
`store::row_id_for_event_id` — the same helper the stream's mid-flight
shard-rebind already used for exactly this translation. The `stream-end`
event and the `drop_after_event_id` diagnostic field on a slow-consumer
`409` follow the same cursor now, including their empty-backfill fallback
(echo the client's own translated cursor, not a lost `-1`/`None`, so a
migration landing mid-stream with zero new events does not replay history
on its next rebind).

**No new `WorkflowEvent` variant, no migration, no route/contract change**
beyond the `Last-Event-ID` cursor's value space (an `i32` `event_id`
instead of an `i64` row id). A rolling-deployment window where an old
client reconnects to a new server with a stale row-id-shaped cursor is a
known, accepted edge case, not fully closed: most of the time the stale
number matches no `event_id` for that execution and the client gets one
full backfill (self-healing from there, since every `id:` it receives
afterward is `event_id`-based). On a low-traffic shard the stale row id can
coincidentally equal a real `event_id` for the same execution, in which
case the backfill silently starts from the wrong point in history instead
of from the start. Either way the window is one reconnect during a
rollout, every subsequent reconnect is correct, and full history remains
available via the ordinary read APIs regardless — judged not to need
cursor-format versioning for a live-tail convenience channel.

**Deliberately out of scope**, per #1405's own suggested order and
#1596's PR description: `store.rs`'s 3 `assert_fence` DR-fencing call
sites (same residence-routing category, but needs a `ShardId` threaded
through roughly 15-20 call sites across 8 files in the cross-region
DR-fencing hot path — left for a dedicated follow-up rather than rushed
here). The CLI codec-registry item's stated consequence
(`verify_target_copy` failing outright on a non-identity-codec deployment)
is already mitigated by #1596's degrade-to-raw-fingerprint fix. The
signal-delivery "gap" and workflow-listing "duplicate" items in #1405 both
turned out, on inspection, to be intentional and already-correct behaviour
— not bugs.

**Tests.** `context.rs` gained two unit tests
(`awaited_child_workflow_inherits_the_parents_current_shard_not_its_origin`,
`race_child_workflow_branch_inherits_the_parents_current_shard_not_its_origin`)
proving a parent minted on one shard and rebalanced to another places its
child on the new shard. `autumn-harvest-plugin`'s
`workflow_filter_integration.rs` gained an end-to-end test
(`sse_stream_resume_after_migration_uses_the_translated_event_id_cursor`)
that runs a real two-shard migration and asserts a stale pre-migration
cursor backfills exactly the missed events, neither a duplicate replay nor
a gap. All three were verified red against the pre-fix code and green
against the fix.
