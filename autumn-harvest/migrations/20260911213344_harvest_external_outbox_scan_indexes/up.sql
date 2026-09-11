-- Index both sides of the three external-outbox claim queries (issue #1486).
--
-- `timeout::enforce_timeouts_once` runs three sibling scanners on every
-- worker's periodic tick -- `enforce_external_signals_outbox`,
-- `enforce_external_cancels_outbox` and `enforce_external_awaits_outbox`.
-- Each drains its own outbox of pending cross-workflow requests with the same
-- query: claim one candidate row, act on it, loop until empty. Before this
-- migration nothing indexed either side of that claim.
--
-- ## What was unindexed, and what it cost
--
-- The outer scan selects one event type out of the largest table in the
-- engine: `event_type = 'ExternalSignalRequested'` and its two siblings.
-- `harvest_events` already carries partial indexes for exactly this problem
-- on other rare event types (`idx_harvest_events_activity_type_ts`,
-- `idx_harvest_events_reset_terminated`, both in
-- `20260702000000_harvest_usage_report_indexes`). It carried none for these
-- three. On a 1.02M-event fixture the claim read 1,020,000 rows to return
-- one, at 14,572 buffers, on every claim of every drain loop.
--
-- The paired resolution check -- "has this request already been delivered or
-- failed?" -- was unindexed too, and that half is the larger cost. It is
-- correlated per execution, so an unindexed probe re-reads every event of the
-- owning execution. A drain pays it once per already-resolved candidate it
-- steps over, so the cost grows with the square of the backlog. On the same
-- fixture it was 90% of a full 50-request drain.
--
-- ## Why the outer index alone was measured as a regression
--
-- Issue #1486 measured the obvious fix -- the outer partial index by itself
-- -- and reported a 271% regression on the full drain. The two halves are
-- complementary, and each alone is worse than neither:
--
--   * The outer index alone leaves the resolution probe unindexed, and moves
--     the row estimate the planner uses to cost it. Past a crossover point
--     the planner stops correlating that probe and reads the whole table once
--     instead, which a drain loop then pays on every iteration.
--   * The query rewrite alone (`timeout.rs`, same issue) pins three joins to
--     their correlated form. Without these indexes each correlated probe is a
--     scan, measured at 1,604,209 buffers against a 298,967-buffer baseline --
--     5.4x worse than doing nothing.
--
-- So this migration and the `timeout.rs` rewrite ship together, and the
-- measurement that matters is of the pair. On a full 50-request drain
-- (`pg_stat_statements`, 204 statements): 298,967 buffers before, 8,088
-- after, a 97.3% reduction. Held under a deliberately stale row estimate
-- (400x the truth, the shape an outage backlog leaves behind): 49,915
-- before, 10,820 after. Plans, snapshots and the fixture are in
-- `docs/perf-artifacts/external-outbox-scan/`; the writeup is
-- `docs/performance-external-outbox-scan.md`.
--
-- ## The four indexes
--
-- One index serves all three outer scans. Each scanner filters a single
-- `event_type`, which the leading column answers as a prefix, so three
-- scanners cost one index rather than three.
--
-- The remaining two columns, `(timestamp, id)`, supply the `ORDER BY
-- e.timestamp, e.id` the rewritten claim adds, and that order is what pins
-- the plan. No other index on this table can produce it, so every competing
-- plan needs an explicit sort, and a sort under `LIMIT 1` has to read every
-- candidate before it can return the first. The ordered index scan returns
-- after one row instead, and wins whatever the planner's row estimate says.
--
-- Ordering on `id` alone is not enough, and this was measured rather than
-- assumed: `harvest_events_pkey` supplies `id` order too, so under an
-- inflated estimate the planner walked the primary key, filtering every
-- unrelated event out of a full ascending scan. Prefixing the order with
-- `event_type` does not fix it either, because the planner drops a column
-- that the `WHERE` clause pins to a constant from the ordering it has to
-- satisfy, which makes the primary key eligible again. `timestamp` is
-- neither constant nor served by any other index, so it is the column that
-- makes the choice unambiguous.
--
-- Three more index the resolution check, one per outbox family, keyed
-- exactly to that check's own predicates: equality on `workflow_exec_id`,
-- then equality on the JSON-extracted correlation id. Each is partial on the
-- two terminal event types of its family, so only those rows pay for it.
--
-- Expression indexes over `event_data` have precedent on this table:
-- `idx_harvest_events_activity_started_lookup`
-- (`20260905181020_harvest_usage_activity_lookback_index`) keys on
-- `event_data #>> '{data,activity_id}'`. The `->`/`->>` spelling here matches
-- the claim queries character for character, which is what makes the
-- expression index usable at all.
--
-- ## Interaction with the two sanctioned `harvest_events` writers
--
-- `erase.rs` (exception #2) and `codec_rotation.rs` (exception #3) both
-- rewrite `event_data` in place. Neither invalidates these indexes: Postgres
-- maintains an expression index on `UPDATE` like any other. Erasure runs on
-- terminal executions only, and these scanners read RUNNING ones, so an
-- erased row is never a claim candidate. Codec rotation leaves the decoded
-- plaintext byte-identical, so an indexed correlation id is unchanged by it.
--
-- ## Build cost and the zero-downtime recipe
--
-- `CREATE INDEX` (not `CONCURRENTLY`) takes `SHARE` on `harvest_events` for
-- the build, blocking every append, claim and completion touching this table
-- -- the same trade-off `20260702000000_harvest_usage_report_indexes` and
-- `20260905181020_harvest_usage_activity_lookback_index` made and documented.
-- All four indexes are partial on event types that are rare in any history,
-- so both the build and the ongoing write cost fall on those rows alone. No
-- other event type's write path is touched.
--
-- For a live, already-large deployment, build them out of band first, then
-- run this migration: the guard below accepts an index that already exists
-- with the expected definition and is valid. The out-of-band recipe is the
-- one written out in full in
-- `20260905181020_harvest_usage_activity_lookback_index/up.sql`, including
-- the partitioned-`harvest_events` variant (`harvest partition enable`,
-- issue #958), the `indisvalid` cleanup pass, the per-leaf convergence loop
-- and the partition-maintenance freeze it needs. Use `CREATE INDEX
-- CONCURRENTLY IF NOT EXISTS` with each name and definition below in place of
-- that migration's single index.
--
-- The guard rejects two states rather than reporting success over them, for
-- the reasons that migration sets out: a same-named index with a DIFFERENT
-- definition means this migration never installed the intended index, and
-- would make `down.sql` drop an unrelated one; a matching but INVALID index
-- means an out-of-band `CONCURRENTLY` build never finished. The `(ONLY )?` in
-- the fingerprint regexp is what lets a correctly pre-built PARTITIONED
-- parent index pass, since `pg_get_indexdef` renders those as `ON ONLY`.
-- `indrelid = 'harvest_events'::regclass` scopes the lookup to this table,
-- because index names are unique per schema and not per table.
DO $$
DECLARE
    -- name, CREATE statement, expected `pg_get_indexdef` suffix.
    wanted CONSTANT text[] := ARRAY[
        'idx_harvest_events_external_outbox_pending',
        'idx_harvest_events_external_signal_resolved',
        'idx_harvest_events_external_cancel_resolved',
        'idx_harvest_events_external_await_resolved'
    ];
    creates CONSTANT text[] := ARRAY[
        'CREATE INDEX idx_harvest_events_external_outbox_pending ON harvest_events (event_type, timestamp, id) WHERE event_type IN (''ExternalSignalRequested'', ''ExternalCancelRequested'', ''ExternalAwaitRequested'')',
        'CREATE INDEX idx_harvest_events_external_signal_resolved ON harvest_events (workflow_exec_id, (event_data->''data''->>''signal_id'')) WHERE event_type IN (''ExternalSignalDelivered'', ''ExternalSignalFailed'')',
        'CREATE INDEX idx_harvest_events_external_cancel_resolved ON harvest_events (workflow_exec_id, (event_data->''data''->>''cancel_id'')) WHERE event_type IN (''ExternalCancelDelivered'', ''ExternalCancelFailed'')',
        'CREATE INDEX idx_harvest_events_external_await_resolved ON harvest_events (workflow_exec_id, (event_data->''data''->>''await_id'')) WHERE event_type IN (''ExternalAwaitResolved'', ''ExternalAwaitFailed'')'
    ];
    fingerprints CONSTANT text[] := ARRAY[
        'USING btree (event_type, "timestamp", id) WHERE (event_type = ANY (ARRAY[''ExternalSignalRequested''::text, ''ExternalCancelRequested''::text, ''ExternalAwaitRequested''::text]))',
        'USING btree (workflow_exec_id, (((event_data -> ''data''::text) ->> ''signal_id''::text))) WHERE (event_type = ANY (ARRAY[''ExternalSignalDelivered''::text, ''ExternalSignalFailed''::text]))',
        'USING btree (workflow_exec_id, (((event_data -> ''data''::text) ->> ''cancel_id''::text))) WHERE (event_type = ANY (ARRAY[''ExternalCancelDelivered''::text, ''ExternalCancelFailed''::text]))',
        'USING btree (workflow_exec_id, (((event_data -> ''data''::text) ->> ''await_id''::text))) WHERE (event_type = ANY (ARRAY[''ExternalAwaitResolved''::text, ''ExternalAwaitFailed''::text]))'
    ];
    existing_def text;
    existing_valid boolean;
BEGIN
    FOR i IN 1 .. array_length(wanted, 1) LOOP
        SELECT pg_get_indexdef(pg_class.oid), pg_index.indisvalid
          INTO existing_def, existing_valid
        FROM pg_class
        JOIN pg_index ON pg_index.indexrelid = pg_class.oid
        WHERE pg_class.relname = wanted[i]
          AND pg_index.indrelid = 'harvest_events'::regclass;

        IF existing_def IS NULL THEN
            EXECUTE creates[i];
        ELSIF regexp_replace(existing_def, '^CREATE INDEX \S+ ON (ONLY )?\S+ ', '') <> fingerprints[i] THEN
            RAISE EXCEPTION
                '% already exists with an unexpected definition -- resolve the name collision (rename or drop the existing index) before retrying this migration: %',
                wanted[i], existing_def;
        ELSIF NOT existing_valid THEN
            RAISE EXCEPTION
                '% already exists with the expected definition but is INVALID -- DROP INDEX CONCURRENTLY and retry the out-of-band build before retrying this migration',
                wanted[i];
        END IF;
    END LOOP;
END $$;
