-- Shard rebalancing: migrating quiescent workflows across shards (issue #964).
--
-- The sharding contract used to end at "cross-shard rebalancing of existing
-- workflows is out of scope", which meant a hot shard stayed hot for as long as
-- its residents lived -- forever, for a continue-as-new entity workflow -- and a
-- shard could never be decommissioned. This migration ships the durable state
-- behind an operator-initiated copy -> replay-verify -> single atomic cutover
-- -> sealed source primitive. See docs/plans/2026-09-02-shard-rebalancing.md
-- for the design note (issue #964 AC4 requires one) and docs/sharding.md for
-- the operator contract.
--
-- ## No new event variants, no event mutation
--
-- Nothing here appends to or rewrites `harvest_events`. The copy INSERTs new
-- rows on a DIFFERENT database and never touches a stored row on the source, so
-- shard rebalancing is NOT a fourth exception to the append-only invariant in
-- CLAUDE.md -- it is an instance of it. All migration bookkeeping lives in the
-- columns and table below.

-- ── The two new execution states ─────────────────────────────────────────────
--
-- `MIGRATING` -- a staged, inert copy on the TARGET shard. It holds the target's
--   `(workflow_name, workflow_id)` active-uniqueness slot (so nothing else can
--   claim the identity mid-migration) but no scanner will ever dispatch it,
--   because every dispatch path filters `state = 'RUNNING'`.
--
-- `MIGRATED` -- the sealed SOURCE row after cutover. Terminal-shaped and
--   non-claimable, carrying a forwarding pointer to the shard the run now lives
--   on. It is the precedent the reset path set with `TERMINATED`: sealed, never
--   deleted, and released from the active-uniqueness index.
--
-- ## Lock discipline
--
-- Every statement below touches `harvest_workflow_executions`, which on a busy
-- deployment is the hottest table there is. Two rules follow, and both are the
-- precedent `20260901115500_harvest_event_partitioning` set:
--
--   * Bound the lock wait immediately. Postgres queues later conflicting
--     requests behind a WAITER, so behind one idle-in-transaction reader an
--     unbounded ALTER would stall every execution-state write for as long as
--     the blocker lives. Failing fast costs a retry; waiting costs the shard's
--     write availability.
--   * Add CHECK constraints `NOT VALID`. A plain ADD CONSTRAINT validates every
--     existing row under ACCESS EXCLUSIVE -- a full scan of the largest table in
--     the deployment. `NOT VALID` still enforces the constraint on every
--     subsequent INSERT and UPDATE, which is all these two need to do: they
--     describe states and columns that only this feature's code writes, so
--     there is nothing pre-existing to validate.
SET LOCAL lock_timeout = '5s';

ALTER TABLE harvest_workflow_executions
    DROP CONSTRAINT IF EXISTS harvest_workflow_executions_state_check;

-- NOTE the presence of 'PAUSED'. This list must be the FULL set, not the set
-- this migration cares about: `ADD CONSTRAINT` replaces the predicate outright,
-- so any state omitted here becomes unwritable. Dropping 'PAUSED' would break
-- `POST /workflows/{id}/pause` permanently and fail this very migration on any
-- database with a paused execution -- which is exactly what
-- `20260607000002_harvest_workflow_pause` warned about when it added it.
ALTER TABLE harvest_workflow_executions
    ADD CONSTRAINT harvest_workflow_executions_state_check
        CHECK (state IN (
            'RUNNING',
            'PAUSED',
            'COMPLETED',
            'FAILED',
            'CANCELLED',
            'TIMED_OUT',
            'CONTINUED_AS_NEW',
            'TERMINATED',
            'MIGRATING',
            'MIGRATED'
        )) NOT VALID;

-- The forwarding reference (AC3). NULL for every execution that never moved,
-- which is every execution in every deployment that never runs a rebalance.
ALTER TABLE harvest_workflow_executions
    ADD COLUMN IF NOT EXISTS migrated_to_shard INTEGER NULL;

ALTER TABLE harvest_workflow_executions
    ADD COLUMN IF NOT EXISTS migrated_at TIMESTAMPTZ NULL;

-- ## The durable residence history, and why the forwarding pointer is not it
--
-- `migrated_to_shard` answers "where do I write?" and is deliberately
-- *collapsed*: a run migrated A -> B -> C has A's pointer rewritten straight to
-- C so hops do not accumulate past the bounded walk depth. That is the right
-- shape for routing and the wrong shape for anything that must reach every byte
-- the run has ever left behind, because collapsing A -> C erases the only
-- evidence that B ever hosted it -- and B's sealed copy still holds the full
-- payloads until B's own retention collects them.
--
-- Erasure is exactly that second question. This column answers it directly and
-- independently of pointer topology: a JSONB array of every shard that
-- PREVIOUSLY hosted this execution, oldest first, appended to at each
-- activation. The live row's array plus the shard the live row is on is the
-- complete residence set, with no walk and no bound.
--
-- NULL for every execution that never moved.
ALTER TABLE harvest_workflow_executions
    ADD COLUMN IF NOT EXISTS migrated_from_shards JSONB NULL;

-- The residence history has to outlive the execution row it lives on.
--
-- After a migrated run terminates, the TARGET shard's retention janitor
-- eventually deletes its execution row and keeps only a compact
-- `harvest_execution_summaries` row. The sealed source copies are not collected
-- with it — retention deliberately never purges a `MIGRATED` row, because that
-- would destroy the pointer every pre-migration id resolves through — so their
-- payloads are still sitting there. A residence lookup that found only the
-- summary and read "never migrated" from its absence would report a clean
-- erasure over exactly those copies.
--
-- Carrying the array onto the summary keeps the erasure's traversal complete
-- for the whole life of the data, not just the life of the execution row.
ALTER TABLE harvest_execution_summaries
    ADD COLUMN IF NOT EXISTS migrated_from_shards JSONB NULL;

-- ## The active-uniqueness index is deliberately LEFT ALONE
--
-- Issue #964's AC3 points at the reset path's `TERMINATED` sealing as the
-- precedent, "which already releases the uniqueness index". Copying that here
-- would be wrong, and the difference is worth stating because it looks like an
-- omission.
--
-- A reset forks a **successor on the same shard**, so the sealed source must
-- release `(workflow_name, workflow_id)` or the successor could not be
-- inserted. A migration puts the copy on a **different database**, whose index
-- is its own -- so nothing needs releasing, and releasing would actively break
-- the single-active-run guarantee: a later start of the same business key
-- hashes back to the source shard, finds no active row, and inserts a SECOND
-- live run while the migrated one is still running on the target.
--
-- Keeping `MIGRATED` inside the index makes that impossible. A start for a
-- migrated business key fails closed on the source's unique index rather than
-- silently duplicating the run.
--
-- No statement here: the index from `20260503000000_harvest_workflow_reset`
-- already excludes only `CONTINUED_AS_NEW` and `TERMINATED`, which is exactly
-- what this feature wants.

-- A `MIGRATED` row is a forwarding pointer and must carry one; a row that is
-- not `MIGRATED` must not. Expressed as a CHECK rather than left to the
-- application, because an id that resolves to NOWHERE is the one failure this
-- feature must never produce.
ALTER TABLE harvest_workflow_executions
    DROP CONSTRAINT IF EXISTS harvest_we_migrated_forward_check;

--
-- Deliberately ONE-DIRECTIONAL: "a sealed row carries a pointer", not "only a
-- sealed row carries one". The converse would make an operator override that
-- force-writes `state` on a migrated row -- `terminate_workflow_execution`
-- carries no state precondition by design -- fail with an opaque constraint
-- violation instead of the idempotent no-op the contract promises. Keeping the
-- pointer on such a row is also what lets id resolution survive the override:
-- `read_forward` matches on the pointer, not on the state.
ALTER TABLE harvest_workflow_executions
    ADD CONSTRAINT harvest_we_migrated_forward_check
        CHECK (
            state <> 'MIGRATED'
            OR (migrated_to_shard IS NOT NULL AND migrated_at IS NOT NULL)
        ) NOT VALID;

-- ── The durable migration record ─────────────────────────────────────────────
--
-- Lives on the SOURCE shard -- the shard that stays authoritative right up to
-- the cutover commit -- so that a crash at any point leaves a record on the
-- database that still owns the run. `resume_incomplete_migrations` drives it.
CREATE TABLE IF NOT EXISTS harvest_shard_migrations (
    -- The execution being moved. Primary key, so two concurrent operators
    -- cannot open two migrations for the same run: the second insert collides.
    -- The `ExecutionId` is NOT re-minted by a migration (see the design note's
    -- identity decision), so this value is stable across the whole lifecycle
    -- and after it.
    execution_id UUID PRIMARY KEY,
    source_shard INTEGER NOT NULL,
    target_shard INTEGER NOT NULL,
    -- PENDING -> COPIED -> VERIFIED -> COMMITTED -> DONE, or ABORTED from any
    -- pre-cutover phase. Only the VERIFIED -> COMMITTED transition changes who
    -- is authoritative, and it is a single-statement commit on this database.
    phase TEXT NOT NULL DEFAULT 'PENDING',
    -- The replay-verification fingerprint the target copy produced, retained so
    -- an operator can see WHAT was verified rather than only that it passed.
    verified_fingerprint TEXT NULL,
    -- The SOURCE history's high-water mark at the instant verification passed:
    -- the number of event rows and the highest per-execution `event_id`.
    --
    -- Verification proves the copy matches the source *as of the copy*. The
    -- cutover happens later -- immediately in the end-to-end path, but possibly
    -- hours later on a resume after a crash at VERIFIED -- and in between the
    -- run can legitimately wake, execute a whole decision cycle, append events
    -- and park again. It is quiescent once more, so a cutover that re-checked
    -- only quiescence would happily seal it and hand authority to a copy that
    -- is now missing everything the run did in that window: silently lost
    -- progress, the worst outcome this feature can produce.
    --
    -- Recording the mark makes "the history is still the history we verified" a
    -- condition of the seal. Events are append-only with a monotonic
    -- per-execution `event_id`, so any append moves both numbers, and a cutover
    -- whose record predates this column (NULL) declines rather than proceeding
    -- unguarded.
    verified_event_count BIGINT NULL,
    verified_max_event_id INTEGER NULL,
    -- Why an aborted migration aborted, in operator-readable words.
    abort_reason TEXT NULL,
    -- The source's parked workflow task row, captured verbatim at stage time.
    --
    -- It is deliberately NOT copied to the target during staging: the claim
    -- query does not filter on execution state, so a task row sitting next to a
    -- staged `MIGRATING` execution would be claimable -- the run would be live
    -- on two shards at once, which is the one thing this design must never
    -- allow. Holding the row here instead means the target becomes claimable at
    -- exactly one instant: its activation, which is driven by this durable
    -- record after the cutover has already sealed the source.
    staged_task JSONB NULL,
    -- Operability: a row that stops making progress names its own last failure.
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NULL,
    last_attempt_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT harvest_shard_migrations_phase_check
        CHECK (phase IN ('PENDING', 'COPIED', 'VERIFIED', 'COMMITTED', 'DONE', 'ABORTED')),
    -- A migration to the shard it already lives on is a no-op that would seal a
    -- row and forward it to itself -- an infinite hop. Refuse it in the schema.
    CONSTRAINT harvest_shard_migrations_distinct_shards_check
        CHECK (source_shard <> target_shard)
);

-- The resume sweep's work list: rows that are not finished, oldest first.
-- PARTIAL, because in the steady state of a deployment that has finished a
-- drain this set is empty and the index costs nothing.
CREATE INDEX IF NOT EXISTS idx_harvest_shard_migrations_unsettled
    ON harvest_shard_migrations (created_at)
    WHERE phase NOT IN ('DONE', 'ABORTED');

-- The forwarding lookup an id-routed read performs after landing on the origin
-- shard and finding a sealed row. PARTIAL for the same reason: only migrated
-- executions are in it, which is zero rows until an operator rebalances.
CREATE INDEX IF NOT EXISTS idx_harvest_we_migrated_forward
    ON harvest_workflow_executions (id)
    WHERE migrated_to_shard IS NOT NULL;
