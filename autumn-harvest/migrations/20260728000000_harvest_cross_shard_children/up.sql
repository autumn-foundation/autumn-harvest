-- Cross-shard child workflow placement (issue #956).
--
-- Children are pinned to the parent's shard by default and that default is
-- permanent. When a spawn opts in to a different shard, the parent's decision
-- transaction still may not touch a second database -- per-execution ACID is
-- shard-local by design -- so the spawn instead records one row here, in the
-- SAME transaction as the parent's `ChildWorkflowStarted` /
-- `ChildWorkflowSpawnedDetached` event. A background relay then creates the
-- child on the target shard, delivers its terminal back to the parent, and
-- applies the parent-close cascade.
--
-- This row is not a message; it is the cross-shard child's lifecycle record on
-- the PARENT's side. Start, cancel, terminal delivery and close-cascade are all
-- transitions of this one row, which is also why `SELECT count(*)` over it is a
-- meaningful "in-flight cross-shard children" gauge for an operator.
--
-- No new `WorkflowEvent` variant and no change to the adjacently-tagged JSON
-- event contract: the parent's history records exactly the same
-- `ChildWorkflowStarted` / `ChildWorkflowCompleted` / `ChildWorkflowFailed` /
-- `ChildWorkflowCascadeApplied` events it always did, and the child's shard is
-- recoverable from the `child_id` (an `ExecutionId` encodes its shard in the
-- UUID's first two bytes). A parent replays byte-identically regardless of
-- where its children physically live.
--
-- The table is empty in every deployment that never opts in, so a single-shard
-- (or parent-pinned) deployment pays only the empty relation.

CREATE TABLE IF NOT EXISTS harvest_cross_shard_children (
    -- The child's ExecutionId. Primary key, so the relay's insert on the target
    -- shard and this row are keyed identically and a repeated relay is a no-op.
    child_exec_id UUID PRIMARY KEY,
    -- The parent, which lives on THIS shard (the row is always written on the
    -- parent's database, inside the parent's own decision transaction).
    parent_exec_id UUID NOT NULL,
    -- Denormalised from `child_exec_id`'s encoded shard so the relay can index
    -- its work-list by target shard without decoding every UUID.
    target_shard INTEGER NOT NULL,
    -- 'PENDING_START' until the child row exists on the target shard, then
    -- 'STARTED'. Everything after that is decided by observed facts (the child's
    -- state over there, the parent's state here), never by a status the relay
    -- has to remember to advance.
    status TEXT NOT NULL DEFAULT 'PENDING_START',
    -- Set by the parent's cancel paths (race-loser cancel, child-deadline
    -- cancel, explicit cancel of an awaited cross-shard child). Cleared once the
    -- idempotent cancel has been delivered to the target shard.
    cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
    -- NULL = an awaited child (the parent is parked on its terminal).
    -- Non-NULL = a detached child, and the value is its ParentClosePolicy.
    parent_close_policy TEXT NULL,
    workflow_name TEXT NOT NULL,
    -- Everything the relay needs to create the child on the target shard, with
    -- every default ALREADY RESOLVED at spawn time against the spawning
    -- worker's registry -- the same `resolve_child_workflow_defaults` call the
    -- same-shard path makes, at the same moment. The relay never re-derives
    -- them, so a cross-shard child cannot silently pick up different defaults
    -- than its same-shard twin would have.
    child_spec JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Operability: a row that stops making progress names its own last failure
    -- rather than only showing up as a stuck parent.
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NULL,
    last_attempt_at TIMESTAMPTZ NULL
);

-- The relay runs two work-list queries per sweep, and each gets its own index.
--
-- 1. ACTIONABLE rows -- a child not created yet, or a pending cancel. PARTIAL,
--    because in the steady state of a large fan-out this set is a rounding
--    error next to the in-flight set: the index stays tiny and the start
--    backlog drains at full batch width. The predicate is a superset of the
--    query's (which also carries the retry-backoff due check), so Postgres can
--    still use it.
CREATE INDEX IF NOT EXISTS idx_harvest_cross_shard_children_actionable
    ON harvest_cross_shard_children (target_shard, created_at)
    WHERE status = 'PENDING_START' OR cancel_requested;

-- 2. IN-FLIGHT rows, polled least-recently-swept first. The `last_attempt_at`
--    lead column is what makes the poll ROTATE through a large backlog instead
--    of re-reading the oldest window every tick -- without it, a handful of
--    long-running children at the head of `created_at` would occupy every slot
--    and starve every newer row's terminal delivery indefinitely.
CREATE INDEX IF NOT EXISTS idx_harvest_cross_shard_children_inflight
    ON harvest_cross_shard_children (target_shard, last_attempt_at, created_at)
    WHERE status = 'STARTED' AND NOT cancel_requested;

-- Deliberately NO index on `parent_exec_id`: every parent-side lookup (the
-- cancel request, the "is this child already recorded here?" spawn check) is
-- keyed by `child_exec_id`, which is the primary key. An unused index here
-- would be pure write amplification on the hot spawn path -- one insert and one
-- delete per cross-shard child, which is exactly the 10k-child fan-out.
