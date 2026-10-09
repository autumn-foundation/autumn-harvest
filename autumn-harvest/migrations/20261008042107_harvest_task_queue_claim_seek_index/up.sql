-- Issue #1971. The claim reads a bounded window from the head of each queue.
-- It no longer scans and sorts every due row on each claim.
--
-- The window reads one ordered range of this index per head: one queue, one
-- task type, and new starts or continuations. A new start is a row with
-- `new_start AND attempt = 0`. Inside one head, the claim due time follows
-- `scheduled_at`: a continuation is due at `scheduled_at`, and a new start
-- 30 s later. So each range is already in claim order, `priority DESC, due
-- ASC`. The task type lets a claim for one kind skip the other kind. See
-- `queue::claim_task_query`.
--
-- The due time itself cannot be an index key. `timestamptz + interval` is
-- not immutable, and Harvest supports Postgres 12 and later.
--
-- ## Build cost and the zero-downtime recipe
--
-- `CREATE INDEX` (not `CONCURRENTLY`) takes `SHARE` on `harvest_task_queue`.
-- That blocks every enqueue, claim and completion until the build ends. The
-- index is partial, but the build still reads every row, terminal rows
-- included. Size the build window from the total row count.
--
-- On a live, large deployment, build the index out of band first:
--
--     CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_harvest_tq_claim_seek
--         ON harvest_task_queue
--         (queue_name, task_type, (new_start AND attempt = 0), priority DESC, scheduled_at)
--         WHERE state = 'PENDING';
--
-- Then run this migration. The guard below accepts an index that already
-- exists with the expected definition and is valid.
--
-- The guard rejects two states. A same-named index with a different
-- definition means this migration never installed the intended index, and
-- down.sql would drop an unrelated one. A matching but INVALID index means an
-- out-of-band CONCURRENTLY build did not finish. Drop it with
-- `DROP INDEX CONCURRENTLY` and build it again.
--
-- `lock_timeout` bounds the wait for `SHARE` (issue #1810). A long
-- transaction on the table then fails this migration after 5 s, instead of
-- queueing every later write behind it. Run the migration again.
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
    WHERE pg_class.relname = 'idx_harvest_tq_claim_seek'
      AND pg_index.indrelid = 'harvest_task_queue'::regclass;

    IF existing_index_oid IS NULL THEN
        -- lock-safety: allow blocking-index #1971 operators prebuild it CONCURRENTLY, see the header
        CREATE INDEX idx_harvest_tq_claim_seek
            ON harvest_task_queue
            (queue_name, task_type, (new_start AND attempt = 0), priority DESC, scheduled_at)
            WHERE state = 'PENDING';
    ELSIF regexp_replace(existing_def, '^CREATE INDEX [^ ]+ ON (ONLY )?[^ ]+ ', '') <>
          'USING btree (queue_name, task_type, ((new_start AND (attempt = 0))), priority DESC, scheduled_at) WHERE (state = ''PENDING''::text)'
    THEN
        RAISE EXCEPTION
            'idx_harvest_tq_claim_seek already exists with an unexpected definition -- resolve the name collision (rename or drop the existing index) before retrying this migration: %',
            existing_def;
    ELSIF NOT existing_valid THEN
        RAISE EXCEPTION
            'idx_harvest_tq_claim_seek already exists with the expected definition but is INVALID -- DROP INDEX CONCURRENTLY and retry the out-of-band build before retrying this migration';
    END IF;
END $$;
