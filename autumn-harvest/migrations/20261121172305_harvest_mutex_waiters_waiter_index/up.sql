-- Waiter-leading index for the mutex terminal sweep (Ledger pass).
--
-- Each workflow terminal transition runs `sweep_terminal_holder_and_wake`.
-- Its key read finds the keys that the run waits on by `waiter_exec_id`.
-- The unique key of this table leads with `lock_key`, so no index served
-- that filter, and the read scanned the whole waiter heap. Most runs never
-- take a mutex, so most of those scans found no row.
--
-- This index makes the read one index probe. The query text does not
-- change. `docs/performance-mutex-waiter-holder-scan.md` has the numbers.
--
-- The plain build takes `SHARE` on `harvest_mutex_waiters` for the build.
-- That blocks the waiter inserts and deletes of mutex acquires and
-- releases. Reads continue. On a large waiter table, build the index first:
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_harvest_mutex_waiters_waiter
--       ON harvest_mutex_waiters (waiter_exec_id);
-- The block below accepts a prebuilt index only if its definition matches
-- and it is valid. It raises an error otherwise.
SET LOCAL lock_timeout = '5s';

DO $$
DECLARE
    existing_def text;
    existing_valid boolean;
BEGIN
    SELECT pg_get_indexdef(i.indexrelid), i.indisvalid
      INTO existing_def, existing_valid
      FROM pg_index i
     WHERE i.indexrelid = to_regclass('idx_harvest_mutex_waiters_waiter');
    IF existing_def IS NULL THEN
        CREATE INDEX idx_harvest_mutex_waiters_waiter
            ON harvest_mutex_waiters (waiter_exec_id);
    ELSIF regexp_replace(existing_def, '^CREATE INDEX \S+ ON (ONLY )?\S+ ', '') <>
          'USING btree (waiter_exec_id)'
    THEN
        RAISE EXCEPTION
            'idx_harvest_mutex_waiters_waiter already exists with an unexpected definition -- rename or drop it before retrying this migration: %',
            existing_def;
    ELSIF NOT existing_valid THEN
        RAISE EXCEPTION
            'idx_harvest_mutex_waiters_waiter already exists but is INVALID -- DROP INDEX CONCURRENTLY it and retry the out-of-band build before retrying this migration';
    END IF;
END $$;
