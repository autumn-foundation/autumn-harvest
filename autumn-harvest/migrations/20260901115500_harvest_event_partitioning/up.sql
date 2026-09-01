-- autumn-harvest/migrations/20260901115500_harvest_event_partitioning/up.sql
-- Issue #958: opt-in native declarative partitioning for `harvest_events`.
--
-- This migration is deliberately **INERT**. It ships the machinery — the
-- cohort function, the cohort column and the integrity trigger — but does NOT
-- convert `harvest_events`. An existing deployment keeps its ordinary
-- (relkind = 'r') table and byte-for-byte identical behaviour until an operator
-- runs `harvest partition enable` (see docs/partitioned-events.md).
--
-- Inert is a claim about how it APPLIES, too, not only about what it leaves
-- behind. Two things follow from that, and both are load-bearing:
--
--   * It builds no index. See the note at the drop gate below.
--   * It bounds its own lock wait, immediately. `ADD COLUMN` with a constant
--     default rewrites nothing, but it still takes ACCESS EXCLUSIVE to write
--     the catalog row — and Postgres queues later conflicting requests behind a
--     WAITER, not merely behind held locks. So behind one idle-in-transaction
--     reader the ALTER waits, and every append arriving after it queues behind
--     the ALTER. Unbounded, on an upgrade a deployment may never opt into.
--
-- Failing fast is the right answer: the operator clears the blocker and re-runs
-- the migration, which costs a retry. Waiting costs the shard's write
-- availability for as long as the blocker lives.
SET LOCAL lock_timeout = '5s';
--
-- ## The partition key, and why it is not `timestamp`
--
-- The obvious key — `harvest_events.timestamp` — is wrong. Postgres requires
-- the partition key to appear in every UNIQUE constraint, so
-- `UNIQUE (workflow_exec_id, event_id)` would become
-- `UNIQUE (workflow_exec_id, event_id, timestamp)`, silently destroying the
-- per-execution id uniqueness that IS the engine's optimistic-concurrency
-- detector. `timestamp` is also caller-settable and back-datable by the
-- operator tooling, so it is not a safe routing key.
--
-- The key is a dedicated `cohort` column: the row's own APPEND INSTANT, floored
-- to a fixed width by a plain column DEFAULT. Two properties follow, and the
-- whole design rests on them:
--
--   1. `cohort` is functionally dependent on the row, not on the caller — it is
--      supplied by a DEFAULT, never by any statement the engine issues. So
--      `UNIQUE (workflow_exec_id, event_id, cohort)` still rejects exactly what
--      `UNIQUE (workflow_exec_id, event_id)` rejected: a second row for the
--      same (execution, event id). The concurrency contract is preserved.
--
--   2. **Past partitions are sealed.** A cohort's range is a window of wall
--      clock that has already closed, and the DEFAULT can only ever produce a
--      cohort at or after "now", so no future INSERT can route into a partition
--      whose upper bound is in the past. A sweep that proves a closed partition
--      holds no live execution's rows cannot then be raced by an append into
--      it — the safety argument is structural, not a lock.
--
-- ## Why the DEFAULT, and not a trigger
--
-- An earlier iteration stamped `cohort` from the owning execution's
-- `created_at` in a BEFORE INSERT trigger, so that every event of one execution
-- landed in one partition. Postgres forbids it: tuple routing happens BEFORE
-- the row trigger fires, so a trigger that changes the partition key fails with
-- `moving row to another partition during a BEFORE FOR EACH ROW trigger is not
-- supported`. The value must be present before routing, which for a column the
-- engine's SQL never mentions means a DEFAULT.
--
-- That trades whole-execution cohesion for sealed partitions. It is the trade
-- issue #958 anticipates ("an execution's events span time"), and the drop gate
-- below is exact about it: a partition is dropped only once NO row in it
-- belongs to a still-existing execution.
--
-- ## Why the column is invisible to Diesel
--
-- `cohort` is deliberately absent from `src/schema.rs`. Diesel always emits
-- explicit column lists (never `SELECT *`), so a column it does not know about
-- is neither read nor written by any generated statement, and the DEFAULT
-- always applies. That is what makes AC2 true BY CONSTRUCTION rather than by
-- testing luck: every read and write SQL string is byte-for-byte the same in
-- both layouts, and the append path pays nothing but one cheap default
-- expression.

-- ── The cohort function ────────────────────────────────────────────────────
--
-- Epoch-floor arithmetic rather than `date_trunc`: `date_trunc(text,
-- timestamptz)` is STABLE (it depends on the session TimeZone), so two workers
-- with different `TimeZone` settings would compute different cohorts for the
-- same instant and scatter rows across partitions. This form is genuinely
-- IMMUTABLE and TimeZone-independent.
--
-- `floor()` (not integer division) is load-bearing: Postgres integer division
-- truncates toward zero, which would round a pre-1970 timestamp UP into the
-- next cohort.
--
-- The width is a LITERAL baked in by `harvest partition enable`, not a lookup:
-- this function runs once per appended row, so a config-table read here would
-- be a per-row cost on the hot path. The default below matches
-- `partition::DEFAULT_COHORT_WIDTH_SECS` (1 day).
CREATE OR REPLACE FUNCTION harvest_event_cohort(ts TIMESTAMPTZ)
RETURNS TIMESTAMPTZ
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT to_timestamp(floor(extract(epoch FROM $1) / 86400) * 86400)
$$;

COMMENT ON FUNCTION harvest_event_cohort(TIMESTAMPTZ) IS
    'Issue #958: floors an instant to its partition cohort. Regenerated with the '
    'operator''s chosen width by `harvest partition enable`; the width is baked in '
    'as a literal so the append hot path never reads a config table.';

-- ── The cohort column ──────────────────────────────────────────────────────
--
-- Added with a NOT NULL **constant** DEFAULT so it is a METADATA-ONLY operation
-- on PG11+ — no table rewrite, no exclusive lock held for the length of a scan,
-- even on a table with tens of millions of rows. That is what lets the
-- conversion of a large live table (docs/partitioned-events.md) start without a
-- maintenance window. A non-constant default such as `harvest_event_cohort(now())`
-- here would rewrite the whole table instead; `harvest partition enable` swaps
-- the default to that expression later, which is itself metadata-only.
--
-- `-infinity` is the pre-cutover sentinel: every row that predates partitioning
-- sorts below every real cohort, so the legacy table attaches cleanly as the
-- `MINVALUE .. cutover` partition with no row touched.
--
-- On the unpartitioned layout the column is inert: no Diesel statement mentions
-- it and every row keeps the sentinel.
ALTER TABLE harvest_events
    ADD COLUMN IF NOT EXISTS cohort TIMESTAMPTZ NOT NULL DEFAULT '-infinity'::timestamptz;

COMMENT ON COLUMN harvest_events.cohort IS
    'Issue #958: partition key — the row''s append instant floored to the cohort '
    'width. Invisible to Diesel (absent from src/schema.rs) so every generated '
    'statement is identical in both layouts. `-infinity` = pre-partitioning row.';

-- ── The drop gate''s fast path ─────────────────────────────────────────────
--
-- The sweeper asks, for each closed cohort partition, whether any row in it
-- still belongs to a live execution. The cheap sufficient answer comes first:
--
--     SELECT EXISTS (SELECT 1 FROM harvest_workflow_executions
--                     WHERE created_at < <partition upper bound>)
--
-- An execution cannot have appended a row before it existed, so if NO execution
-- predates the partition's upper bound, nothing that could own a row in it
-- survives — the partition is droppable, decided by one index probe. This is
-- the steady state, because retention collects oldest-first.
--
-- When that probe says "maybe", the sweeper falls back to the exact per-row
-- check. Without this index the fast path would be a sequential scan of the
-- executions table per cohort per tick.
-- NOT built here. This migration is inert on apply, and a plain `CREATE INDEX`
-- would not be: it holds `SHARE` on `harvest_workflow_executions` for the whole
-- build, which conflicts with the `ROW EXCLUSIVE` every insert, state update and
-- retention delete takes. On a large deployment every execution-state write
-- stops for the duration of the scan — to build an index that is useless until
-- someone opts in, and that a deployment which never opts in would pay for and
-- never use.
--
-- `harvest partition enable` creates it as part of the conversion, and
-- `harvest partition plan` emits it as a `CREATE INDEX CONCURRENTLY` in the
-- phase that runs with the shard online. Both are the moment it starts being
-- needed.

-- ── The insert-time integrity trigger ──────────────────────────────────────
--
-- Installed on the partitioned layout only (by `harvest partition enable`);
-- defined here so it ships with the migration bundle rather than being conjured
-- at runtime.
--
-- It does two jobs, and the second is the subtler one.
--
-- **1. An event may not be written for an execution that does not exist.**
-- The partitioned layout drops `harvest_events_workflow_exec_id_fkey`, because
-- that FK's `ON DELETE CASCADE` is exactly the row-by-row DELETE storm issue
-- #958 exists to eliminate. This restores the half of the FK that is still
-- wanted, at the same cost and with the same lock: a primary-key probe taking
-- `FOR KEY SHARE`, exactly as the FK did. What is NOT restored is the
-- delete-time cascade -- that is the deliberate trade, and the partition
-- sweeper is the garbage collector for the orphan rows it leaves.
--
-- **2. `(workflow_exec_id, event_id)` stays unique ACROSS partitions.**
-- Postgres requires the partition key in every unique constraint, so the table
-- constraint is `UNIQUE (workflow_exec_id, event_id, cohort)`. Because `cohort`
-- is the row's APPEND INSTANT, that constraint is only unique *within* a
-- cohort -- two appends of the same `event_id` landing in different cohorts
-- would both be accepted. That is not a theoretical gap:
--
--   * Immediately after conversion it is systematic. Every pre-cutover row
--     carries the `-infinity` sentinel, so ANY re-append of an existing
--     `event_id` for a pre-existing execution lands in today's cohort and would
--     be accepted -- silently disabling the engine's split-brain detector for
--     every execution that existed at conversion time.
--   * Later, a stale worker whose task was reclaimed can wake after a cohort
--     boundary and re-append an `event_id` the new owner already wrote.
--
-- Both cases are a re-append against an ALREADY COMMITTED row, which this
-- `EXISTS` sees. It uses the partitioned `idx_harvest_events_exec` index, so it
-- is one two-key index probe per partition per inserted row.
--
-- **Residual window, stated honestly.** Two appends of the same `event_id` that
-- are *simultaneously in flight* -- neither committed when the other's check
-- runs -- and whose inserts land in different cohorts would still both succeed:
-- neither transaction can see the other's uncommitted row, and a partitioned
-- unique index cannot span partitions. Closing it would require serialising the
-- append hot path per execution (a row or advisory lock), which risks lock-order
-- deadlocks against the advisory locks the admission and mutex paths already
-- take. The window is the overlap of two conflicting in-flight appends AND a
-- cohort boundary falling between their insert instants -- microseconds per
-- cohort, against a split-brain that is itself rare. `docs/partitioned-events.md`
-- records this as a known limit of the partitioned layout.
--
-- The trigger deliberately does NOT modify `NEW`. A BEFORE ROW trigger on a
-- partitioned table that changes the partition key is rejected by Postgres,
-- because routing has already happened by the time it fires.
--
-- `SET search_path` is not cosmetic: unlike the FK it replaces (which binds the
-- referenced relation by OID), a trigger body resolves relation names through
-- the session `search_path` at runtime. Without pinning it, a session that puts
-- another schema first could shadow `harvest_workflow_executions` with a table
-- of its own and bypass the check entirely.
-- Created through `format(%I, current_schema())` so `search_path` names the
-- schema Harvest is actually installed in, and the body can then use
-- UNQUALIFIED relation names.
--
-- Hard-coding `public` would be wrong twice over on a deployment installed
-- under any other schema — which the rest of this module supports, discovering
-- everything through `current_schema()`. Every append would either fail because
-- `public.harvest_workflow_executions` does not exist, or, worse, validate
-- against an unrelated table that happens to sit in `public`. Pinning the
-- search_path (rather than leaving it unset) is still what stops a session from
-- shadowing these relations with its own.
DO $harvest_trigger_958$
BEGIN
    EXECUTE format($harvest_trigger_fn$
CREATE OR REPLACE FUNCTION harvest_events_require_execution()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = %I, pg_catalog
AS $harvest_trigger_body$
DECLARE
    owner_exists boolean;
BEGIN
    -- `FOR KEY SHARE`, not a bare EXISTS. Without the lock the probe is not a
    -- guarantee, only an observation: the trigger can see the execution, a
    -- concurrent retention delete can then commit, and this INSERT can commit
    -- afterwards -- producing exactly the orphan the check exists to prevent.
    --
    -- `FOR KEY SHARE` is precisely the lock mode the foreign key took, so this
    -- introduces no lock-ordering hazard the pre-#958 layout did not already
    -- have: it conflicts with the `FOR UPDATE` / `DELETE` that collects an
    -- execution, and with nothing else.
    SELECT true INTO owner_exists
      FROM harvest_workflow_executions
     WHERE id = NEW.workflow_exec_id
       FOR KEY SHARE;

    IF owner_exists IS NULL THEN
        RAISE EXCEPTION
            'harvest_events: no workflow execution %% exists', NEW.workflow_exec_id
            USING ERRCODE = 'foreign_key_violation';
    END IF;

    IF EXISTS (
        SELECT 1 FROM harvest_events
         WHERE workflow_exec_id = NEW.workflow_exec_id
           AND event_id = NEW.event_id
    ) THEN
        -- The message carries the ORIGINAL constraint name so the engine's
        -- existing conflict mapping (error.rs) classifies this exactly as it
        -- classified the unpartitioned unique violation. A caller must not be
        -- able to tell the two layouts apart from the error it gets back.
        RAISE EXCEPTION
            'duplicate key value violates unique constraint "harvest_events_workflow_exec_id_event_id_key" (execution %%, event_id %%)',
            NEW.workflow_exec_id, NEW.event_id
            USING ERRCODE = 'unique_violation';
    END IF;

    RETURN NEW;
END;
$harvest_trigger_body$
$harvest_trigger_fn$, current_schema());
END
$harvest_trigger_958$;

COMMENT ON FUNCTION harvest_events_require_execution() IS
    'Issue #958: rejects events for unknown executions (restoring the insert-time '
    'half of the FK the partitioned layout drops) and rejects a duplicate '
    '(workflow_exec_id, event_id) across partitions, which the partition-key-bearing '
    'UNIQUE constraint can only enforce within one cohort. Never modifies NEW -- '
    'Postgres rejects a BEFORE ROW trigger that changes a partitioned row''s destination.';
