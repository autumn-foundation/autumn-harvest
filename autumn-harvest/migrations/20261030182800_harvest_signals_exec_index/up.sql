-- Issue #2367. An index on `harvest_signals (workflow_exec_id)`.
--
-- These statements reach every signal of one execution, consumed rows too:
--
-- 1. the signal move of a workflow retry (issue #843);
-- 2. the signal scrub of a PII erasure (issue #495);
-- 3. the signal copy and the staged signal delete of a shard rebalance;
-- 4. each `ON DELETE CASCADE` from an execution, which retention triggers;
-- 5. the unkeyed signal check of a backup verification.
--
-- `idx_harvest_signals_pending` skips consumed rows. `uq_harvest_signals_idem`
-- skips rows with no idempotency key. So neither index can serve these
-- statements, and each one read the whole table.
--
-- The new index adds one entry for each inserted signal. A consume update
-- changes `consumed`, which is in the predicate of the pending index. So no
-- such update is HOT, and the new index adds one entry for each one.
--
-- ## Build cost and the zero-downtime recipe
--
-- `CREATE INDEX` (not `CONCURRENTLY`) takes `SHARE` on `harvest_signals`.
-- That blocks each write to the table until the build ends: each signal send,
-- consume, retry move and erasure scrub. It also blocks each execution delete,
-- because the cascade deletes signals. A blocked delete keeps its execution
-- row locked. Size the build window from the row count of `harvest_signals`.
--
-- On a live, large deployment, build the index out of band first:
--
--     CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_harvest_signals_exec
--         ON harvest_signals (workflow_exec_id);
--
-- Then run this migration. The guard below accepts an index that already
-- exists with the expected definition and is valid. It rejects a same-named
-- index with another definition, and an INVALID index from a failed
-- out-of-band build. Drop that one with `DROP INDEX CONCURRENTLY` and build
-- it again.
--
-- `lock_timeout` bounds the wait for `SHARE` (issue #1810).
SET LOCAL lock_timeout = '5s';

DO $$
DECLARE
    existing_index_oid oid;
    existing_def text;
    existing_valid boolean;
BEGIN
    SELECT pg_class.oid, pg_get_indexdef(pg_class.oid), pg_index.indisvalid
      INTO existing_index_oid, existing_def, existing_valid
    FROM pg_class
    JOIN pg_index ON pg_index.indexrelid = pg_class.oid
    WHERE pg_class.relname = 'idx_harvest_signals_exec'
      AND pg_index.indrelid = 'harvest_signals'::regclass;

    IF existing_index_oid IS NULL THEN
        -- lock-safety: allow blocking-index #2367 operators prebuild it CONCURRENTLY, see the header
        CREATE INDEX idx_harvest_signals_exec
            ON harvest_signals (workflow_exec_id);
    ELSIF regexp_replace(existing_def, '^CREATE INDEX \S+ ON (ONLY )?\S+ ', '') <>
          'USING btree (workflow_exec_id)'
    THEN
        RAISE EXCEPTION
            'idx_harvest_signals_exec already exists with an unexpected definition -- rename or drop it before retrying this migration: %',
            existing_def;
    ELSIF NOT existing_valid THEN
        RAISE EXCEPTION
            'idx_harvest_signals_exec already exists but is INVALID -- DROP INDEX CONCURRENTLY it and retry the out-of-band build before retrying this migration';
    END IF;
END $$;
