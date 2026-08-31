-- autumn-harvest/migrations/20260727000000_harvest_event_partitioning/up.sql
-- Issue #958: opt-in native declarative partitioning for `harvest_events`.
--
-- This migration is deliberately **INERT**. It ships the machinery — the
-- cohort function, the cohort column, the integrity trigger and the index the
-- drop gate needs — but does NOT convert `harvest_events`. An existing
-- deployment keeps its ordinary (relkind = 'r') table and byte-for-byte
-- identical behaviour until an operator runs `harvest partition enable`
-- (see docs/partitioned-events.md).
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
CREATE INDEX IF NOT EXISTS idx_harvest_we_created_at
    ON harvest_workflow_executions (created_at);

-- ── The referential-integrity trigger ──────────────────────────────────────
--
-- Installed on the partitioned layout only (by `harvest partition enable`);
-- defined here so it ships with the migration bundle rather than being conjured
-- at runtime.
--
-- The partitioned layout drops `harvest_events_workflow_exec_id_fkey`, because
-- that FK's `ON DELETE CASCADE` is exactly the row-by-row DELETE storm issue
-- #958 exists to eliminate. This trigger restores the half of the FK that is
-- still wanted — an event may not be written for an execution that does not
-- exist — at comparable cost (both are a primary-key probe per row; this one
-- takes no `FOR KEY SHARE` lock, so it is cheaper in lock traffic).
--
-- It deliberately does NOT modify `NEW`. A BEFORE ROW trigger on a partitioned
-- table that changes the partition key is rejected by Postgres, because routing
-- has already happened by the time it fires. Validate-only is legal, and is all
-- that is needed.
--
-- What is NOT restored is the delete-time cascade. That is the deliberate
-- trade: deleting an execution row leaves its events behind as orphans, and the
-- partition sweeper is their garbage collector. Orphans are invisible to every
-- read path in the engine — all of them filter by a `workflow_exec_id` the
-- caller already resolved to a live execution.
CREATE OR REPLACE FUNCTION harvest_events_require_execution()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM harvest_workflow_executions WHERE id = NEW.workflow_exec_id
    ) THEN
        RAISE EXCEPTION
            'harvest_events: no workflow execution % exists', NEW.workflow_exec_id
            USING ERRCODE = 'foreign_key_violation';
    END IF;
    RETURN NEW;
END;
$$;

COMMENT ON FUNCTION harvest_events_require_execution() IS
    'Issue #958: rejects events for unknown executions, restoring the insert-time '
    'half of the FK the partitioned layout drops (whose ON DELETE CASCADE is the '
    'delete storm being eliminated). Never modifies NEW — Postgres rejects a '
    'BEFORE ROW trigger that changes a partitioned row''s destination.';
