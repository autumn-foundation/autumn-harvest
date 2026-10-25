-- Issue #2267. A list of the agents that are due to wake.
--
-- A durable agent publishes the search attribute `agent_next_wake_at`, in
-- epoch milliseconds. A list asks for the agents that wake before a time:
--
--     filter=workflow_name = 'agent' AND attrs.agent_next_wake_at < 1730000000000
--
-- The GIN index `idx_harvest_we_search` finds only the rows that have the
-- key. Every agent with a wake has it, so that read grows with the fleet.
-- Most agents of a large fleet are idle. This btree index reads only the
-- agents in the range.
--
-- The filter grammar (issue #1982) compares a number as
-- `(search_attrs ->> key)::numeric`, after a test that the value is a
-- number. The index has the same expression, and its predicate is that test.
-- So the planner can use it, and a value that is not a number never reaches
-- the cast.
--
-- ## Build cost and the zero-downtime recipe
--
-- `CREATE INDEX` (not `CONCURRENTLY`) takes `SHARE` on
-- `harvest_workflow_executions`. That blocks every state change until the
-- build ends. The index is partial, but the build still reads every row.
--
-- On a live, large deployment, build the index out of band first:
--
--     CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_harvest_we_agent_wake
--         ON harvest_workflow_executions
--            (((search_attrs ->> 'agent_next_wake_at')::numeric))
--         WHERE jsonb_typeof(search_attrs -> 'agent_next_wake_at') = 'number';
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
    WHERE pg_class.relname = 'idx_harvest_we_agent_wake'
      AND pg_index.indrelid = 'harvest_workflow_executions'::regclass;

    IF existing_index_oid IS NULL THEN
        -- lock-safety: allow blocking-index #2267 operators prebuild it CONCURRENTLY, see the header
        CREATE INDEX idx_harvest_we_agent_wake
            ON harvest_workflow_executions
               (((search_attrs ->> 'agent_next_wake_at')::numeric))
            WHERE jsonb_typeof(search_attrs -> 'agent_next_wake_at') = 'number';
    ELSIF regexp_replace(existing_def, '^CREATE INDEX \S+ ON (ONLY )?\S+ ', '') <>
          'USING btree ((((search_attrs ->> ''agent_next_wake_at''::text))::numeric)) WHERE (jsonb_typeof((search_attrs -> ''agent_next_wake_at''::text)) = ''number''::text)'
    THEN
        RAISE EXCEPTION
            'idx_harvest_we_agent_wake already exists with an unexpected definition -- rename or drop it before retrying this migration: %',
            existing_def;
    ELSIF NOT existing_valid THEN
        RAISE EXCEPTION
            'idx_harvest_we_agent_wake already exists but is INVALID -- DROP INDEX CONCURRENTLY it and retry the out-of-band build before retrying this migration';
    END IF;
END $$;
