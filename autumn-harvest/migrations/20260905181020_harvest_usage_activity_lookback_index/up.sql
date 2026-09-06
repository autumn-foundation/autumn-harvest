-- Index the LATERAL activity-attempt lookback in `usage::usage_sql()`
-- (issue #596, `GET /admin/usage`) -- the one CTE the earlier
-- `20260702000000_harvest_usage_report_indexes` migration did not cover.
--
-- `activity_metrics` resolves each activity terminal event's owning
-- `ActivityStarted` attempt with:
--
--   LEFT JOIN LATERAL (
--       SELECT MAX(e2.timestamp) AS last_started_at
--       FROM harvest_events e2
--       WHERE e2.workflow_exec_id = ae.workflow_exec_id
--         AND e2.event_type = 'ActivityStarted'
--         AND e2.event_data #>> '{data,activity_id}' = ae.activity_id
--         AND e2.timestamp <= ae.timestamp
--   ) s ON true
--
-- The only pre-existing index naming `workflow_exec_id` at all is the
-- initial migration's `idx_harvest_events_exec (workflow_exec_id, event_id)`,
-- which cannot serve an `event_type` + JSON-path equality lookup. Without a
-- matching index, this subquery re-scans every OTHER event belonging to the
-- same execution (all event types, not just its `ActivityStarted` siblings)
-- once per activity terminal event in the report window -- a cost that scales
-- with a workflow's own activity fan-out, not with the report's selectivity.
--
-- Measured on a 40,000-execution / ~570,000-event production-shaped fixture
-- (skewed 1%-of-executions "batch" tail with 50-300 activities each --
-- `tests/integration/usage_report_activity_lookback_tests.rs::zz_capture_usage_report_activity_lookback_evidence`):
-- `GET /admin/usage`'s total buffers (`pg_stat_statements`,
-- shared_blks_hit + shared_blks_read) drop from 4,771,832 to 2,139,119 --
-- -55.2% -- with byte-identical grouped counters before and after. Postgres
-- rewrites the correlated `MAX(...)` into an `Index Scan Backward` + `LIMIT 1`
-- against this index (its standard max-via-index-descent transform), replacing
-- a `Bitmap Heap Scan` that filtered ~2.22M heap blocks' worth of sibling
-- events out of a 529,869-loop LATERAL invocation. Full plans and the
-- `pg_stat_statements` snapshots are committed under
-- `docs/perf-artifacts/usage-report-activity-lookback/`; writeup in
-- `docs/performance-usage-report-activity-lookback.md`.
--
-- Partial (`WHERE event_type = 'ActivityStarted'`) and keyed exactly to the
-- subquery's own predicates -- equality on `workflow_exec_id` and the
-- JSON-extracted `activity_id`, then `timestamp` so the `MAX(...) WHERE
-- timestamp <= $ae.timestamp` bound is answerable by a backward index scan --
-- so only `ActivityStarted` rows (the only event type this lookup ever
-- targets) pay for it, and every other event type on this table is unaffected.
--
-- Measured write cost on the same fixture: the index build itself took 20 MB
-- of WAL for ~273,500 indexed rows (one-time, proportional to the
-- `ActivityStarted` backlog at build time); ongoing, inserting a batch of
-- 10,000 `ActivityStarted` rows took 10,378,888 bytes of WAL with the index
-- present vs. 7,737,416 without -- +34.1% WAL specifically on `ActivityStarted`
-- inserts, no other event type's write path is touched. Index size for the
-- fixture's ~273,500 `ActivityStarted` rows: 23 MB.
--
-- `CREATE INDEX` (not `CONCURRENTLY`) takes `SHARE` on `harvest_events` for
-- the build's duration, blocking every append, claim and completion touching
-- this table -- the same trade-off `20260702000000_harvest_usage_report_indexes`
-- made and documented. `IF NOT EXISTS` makes this migration's own statement a
-- safe no-op if the index was already built ahead of time; for a live,
-- already-large deployment, build it out-of-band first (outside any
-- transaction -- `CONCURRENTLY` cannot run inside Diesel's migration
-- transaction) and this migration becomes that no-op.
--
-- The recipe depends on whether the deployment has opted into the partitioned
-- `harvest_events` layout (`docs/partitioned-events.md`, `harvest partition
-- enable`, issue #958), because Postgres does not support `CREATE INDEX
-- CONCURRENTLY` in a single statement against a partitioned PARENT (Codex
-- review, PR #1381): "concurrent index builds for indexes on partitioned
-- tables are currently not supported" -- see
-- <https://www.postgresql.org/docs/16/sql-createindex.html#SQL-CREATEINDEX-CONCURRENTLY>.
--
-- **Unpartitioned** (the default -- `SELECT relkind FROM pg_class WHERE
-- oid = 'harvest_events'::regclass` reports `'r'`):
--
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_harvest_events_activity_started_lookup
--       ON harvest_events (workflow_exec_id, (event_data #>> '{data,activity_id}'), timestamp)
--       WHERE event_type = 'ActivityStarted';
--
-- **Partitioned** (`relkind` reports `'p'`): build the index `CONCURRENTLY`
-- on every existing leaf partition first (each is an ordinary table, so this
-- IS supported per-partition), then create the parent's index without
-- `CONCURRENTLY` -- Postgres recognizes every partition already carries a
-- matching index and only writes the parent's catalog entry, a metadata-only
-- operation that does not rescan data. One query generates the per-partition
-- statements throughout, rather than hand-listing them (partitions are
-- cohort-named and opened over time -- `partition::partition_name`) --
-- filtered to leaves that don't already have a VALID matching index
-- (verified: it returns only the missing/invalid ones), so the SAME query
-- serves both the initial pass and the convergence loop below.
--
-- Checking `indisvalid`, not just presence, matters (Codex review, PR #1381,
-- round 6): `partition.rs`'s own conversion plan documents this exact trap --
-- a cancelled or failed `CREATE INDEX CONCURRENTLY` leaves the index behind,
-- INVALID, and `IF NOT EXISTS` alone reports success on a re-run without
-- looking at `indisvalid`, so the invalid index survives to the parent step,
-- which cannot reuse it and builds a replacement non-concurrently. Matching
-- by EXACT generated name, not a `LIKE` substring against the index
-- definition, also matters: a `LIKE` match cannot tell this index apart from
-- an unrelated one whose definition text happens to contain the same
-- substring. Run the cleanup pass first, so a lingering invalid index (which
-- `CREATE INDEX ... IF NOT EXISTS` would otherwise treat as already
-- satisfied, by the same name-collision trap) is gone before the build pass
-- tries to replace it:
--
--   -- cleanup: drop any invalid leftover under this index's own name pattern.
--   -- A plain (non-CONCURRENTLY) DROP on an invalid index is a catalog-only
--   -- change with no readers to wait for (see partition.rs's own precedent).
--   SELECT format('DROP INDEX CONCURRENTLY IF EXISTS %I;', ic.relname)
--   FROM pg_index i
--   JOIN pg_class ic ON ic.oid = i.indexrelid
--   JOIN pg_class child ON child.oid = i.indrelid
--   JOIN pg_inherits ON pg_inherits.inhrelid = child.oid
--   WHERE pg_inherits.inhparent = 'harvest_events'::regclass
--     AND ic.relname LIKE 'idx_%_activity_started_lookup'
--     AND NOT i.indisvalid;
--   -- review and run the generated statements, THEN generate the builds:
-- Name and `indisvalid` alone are not enough either (Codex review, PR
-- #1381, round 7): they still accept a same-named index with a DIFFERENT
-- definition -- different columns, expression, predicate, collation or
-- opclass. Postgres attaches a leaf index to the new parent index by
-- DEFINITION, not by name, so this recipe's own name is only a label we
-- chose; a same-named leaf index with a different body would never be
-- attached at the parent step regardless. Verified against a toy
-- partitioned table: `pg_get_indexdef()` on two indexes built from an
-- identical column list, expression and predicate returns byte-identical
-- text after the leading `CREATE INDEX name ON schema.table` clause, even
-- across different names and tables, so that suffix is a valid structural
-- fingerprint. The comparison string below is this migration's own
-- `idx_harvest_events_activity_started_lookup` fingerprint, read via
-- `pg_get_indexdef()` against the already-applied unpartitioned index:
--
--   SELECT format(
--       'CREATE INDEX CONCURRENTLY IF NOT EXISTS %I ON %I ' ||
--       '(workflow_exec_id, (event_data #>> ''{data,activity_id}''), timestamp) ' ||
--       'WHERE event_type = ''ActivityStarted'';',
--       'idx_' || child.relname || '_activity_started_lookup',
--       child.relname
--   )
--   FROM pg_inherits
--   JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
--   JOIN pg_class child ON pg_inherits.inhrelid = child.oid
--   WHERE parent.oid = 'harvest_events'::regclass
--     AND NOT EXISTS (
--         SELECT 1 FROM pg_index i
--         JOIN pg_class ic ON ic.oid = i.indexrelid
--          WHERE i.indrelid = child.oid
--            AND ic.relname = 'idx_' || child.relname || '_activity_started_lookup'
--            AND i.indisvalid
--            AND regexp_replace(pg_get_indexdef(i.indexrelid), '^CREATE INDEX \S+ ON \S+ ', '')
--                = 'USING btree (workflow_exec_id, ((event_data #>> ''{data,activity_id}''::text[])), "timestamp") WHERE (event_type = ''ActivityStarted''::text)'
--     );
--
-- A leaf that fails this stricter check for a reason OTHER than a missing
-- index -- a same-named, differently-defined index already sits there --
-- cannot be fixed by the build statement above: `CREATE INDEX CONCURRENTLY
-- IF NOT EXISTS` skips silently on a name collision, regardless of whether
-- the existing definition matches (verified: no error, just a NOTICE, and
-- the mismatched index is left untouched). The convergence loop below
-- therefore never converges for that one leaf -- it keeps reappearing in
-- the build generator's output every pass. That stuck loop is the intended
-- outcome: it surfaces the conflict to the operator instead of silently
-- building the wrong index or skipping the leaf outright. Resolve it by
-- hand -- rename or drop the conflicting index -- before continuing.
--   -- review and run the generated statements, THEN:
--   CREATE INDEX IF NOT EXISTS idx_harvest_events_activity_started_lookup
--       ON harvest_events (workflow_exec_id, (event_data #>> '{data,activity_id}'), timestamp)
--       WHERE event_type = 'ActivityStarted';
--
-- **Partition-maintenance race (Codex review, PR #1381, rounds 2-3):** a
-- single pass through the cleanup and build generators above narrows the
-- window a new partition can slip through but does not close it. Re-run
-- BOTH generator queries, in that order (cleanup, then build), in a LOOP
-- immediately before the parent statement, with no operator delay in
-- between, until the build generator returns zero rows, THEN run the parent
-- statement right away -- but a partition appearing in that window is NOT
-- guaranteed to be
-- empty, and an earlier draft of this comment claimed it was; that claim was
-- wrong and is retracted here. Two things can create a partition:
--
--   * `partition::ensure_partitions` opens a forward lookahead cohort --
--     genuinely empty.
--   * `partition::drain_default` (round 3's finding) creates a cohort
--     partition and moves rows out of `DEFAULT` into it, up to
--     `DRAIN_MAX_ROWS` (50,000) per pass -- a FLOOR, not a ceiling: "one
--     oversized cohort is irreducible and moves in a single pass" (see that
--     constant's own doc comment), so a newly-created partition can carry
--     50,000+ rows the very moment it appears.
--
-- Both run inside `partition::maintain`, and `maintain` is not merely a CLI
-- action an operator can simply avoid scheduling: `retention.rs` calls it
-- automatically from the background retention janitor whenever
-- `RetentionConfig`'s `PartitionMaintenanceConfig::enabled` is true, which it
-- is **by default**. So on a live deployment this is a real, not theoretical,
-- race, and the convergence loop's "the gap is an instant, so what could
-- possibly land in it" reasoning does not make the drain case safe to ignore.
--
-- The actual mitigation has two parts, because there are two independent
-- callers. Setting `RetentionConfig.partitions.enabled = false`
-- (`PartitionMaintenanceConfig::enabled`), rolled out to every worker on the
-- target shard, closes only the AUTOMATIC path -- `retention.rs`'s
-- background janitor. It does NOT close any `harvest partition` CLI
-- subcommand that mutates `harvest_events`: `enable`, `maintain`, and
-- `disable` each dispatch straight to their own `partition::*` function
-- (`run_partition_enable` / `run_partition_maintain` / `run_partition_disable`
-- in `autumn-harvest-cli`), none of them consulting `RetentionConfig`. A
-- person or a cron running any of those three during the window is
-- unaffected by the config flag -- `disable` is the most destructive
-- instance: it reverts `harvest_events` to a plain table, which would leave
-- the per-leaf `CONCURRENTLY` indexes orphaned on tables the parent no longer
-- has, and make the parent statement a full non-concurrent build over the
-- whole rewritten table, exactly the outage this recipe exists to avoid.
--
-- Both parts must hold for the whole window, from before the convergence
-- loop starts through the parent statement finishing: `enabled = false` on
-- every worker, AND an operational freeze covering every mutating `harvest
-- partition` subcommand (`enable`, `maintain`, `disable` today; any later
-- addition to that enum belongs in this freeze too) and every direct call to
-- `partition::maintain`, `ensure_partitions`, `drain_default`, or
-- `disable_partitioning` against this shard by any other means. The
-- read-only `status` and `plan` subcommands are exempt -- neither touches
-- `harvest_events`. Only with both parts held does the convergence loop's
-- zero-row check become exact rather than probabilistic, and only with both
-- held does the table stay partitioned at all for the parent statement to be
-- meaningful against.
--
-- For an operator who cannot take that config-and-restart round trip: the
-- residual, unmitigated risk is that the parent statement performs a
-- non-concurrent build over at most one drain batch (bounded by
-- `DRAIN_MAX_ROWS`, 50,000 rows) or, in the oversized-cohort edge case, that
-- whole cohort -- smaller than indexing the entire historical dataset (the
-- problem this partitioned recipe exists to avoid), but neither instant nor
-- risk-free. State that trade-off to whoever approves the change; do not
-- assume it away.
--
-- (`CONCURRENTLY` can leave an INVALID index behind on failure/cancellation --
-- check `pg_index.indisvalid` for the index's oid and `DROP INDEX
-- CONCURRENTLY` + retry if it is false before relying on it, on either path.)
--
-- `IF NOT EXISTS` alone (Codex review, PR #1381, round 11) accepts a
-- same-named index with a DIFFERENT definition -- an operator's earlier
-- out-of-band build against a stale or mistaken copy of this recipe, for
-- example. Silently accepting it would report this migration as applied
-- without ever installing the intended lookup index, and would make
-- `down.sql`'s `DROP INDEX IF EXISTS` drop an unrelated index instead. A
-- same-named index that DOES match but is left INVALID is not safe to
-- accept silently either: it means the out-of-band build never finished,
-- so this statement would otherwise report success over a non-functional
-- index. Verified against a toy table (all four cases): no existing index
-- builds fresh; a matching, valid one is silently accepted, unchanged; a
-- mismatched or an invalid one aborts the migration with a clear error
-- instead of completing over it.
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
    WHERE pg_class.relname = 'idx_harvest_events_activity_started_lookup'
      AND pg_class.relkind IN ('i', 'I');

    IF existing_index_oid IS NULL THEN
        CREATE INDEX idx_harvest_events_activity_started_lookup
            ON harvest_events (workflow_exec_id, (event_data #>> '{data,activity_id}'), timestamp)
            WHERE event_type = 'ActivityStarted';
    ELSIF regexp_replace(existing_def, '^CREATE INDEX \S+ ON \S+ ', '') <>
          'USING btree (workflow_exec_id, ((event_data #>> ''{data,activity_id}''::text[])), "timestamp") WHERE (event_type = ''ActivityStarted''::text)'
    THEN
        RAISE EXCEPTION
            'idx_harvest_events_activity_started_lookup already exists with an unexpected definition -- resolve the name collision (rename or drop the existing index) before retrying this migration: %',
            existing_def;
    ELSIF NOT existing_valid THEN
        RAISE EXCEPTION
            'idx_harvest_events_activity_started_lookup already exists with the expected definition but is INVALID -- DROP INDEX CONCURRENTLY and retry the out-of-band build before retrying this migration';
    END IF;
END $$;
