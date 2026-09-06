-- Legal-hold race during shard rebalancing (issue #1317, round 6 finding #5).
--
-- `stage_copy` snapshots the execution row with no lock. An administrator can
-- place or refresh a legal hold on the source between that snapshot and the
-- cutover; the cutover guard checked only quiescence and history, never hold
-- state, so it could seal a source that now correctly holds the run while the
-- target -- the live copy from here on -- keeps the pre-hold columns. The hold
-- API reports success; the data it was meant to protect is not, in fact, held.
--
-- Fixed the same way `verified_event_count`/`verified_max_event_id` already
-- guard against history drift: `verify_target_copy` re-reads the source's
-- current `legal_hold_set_at` and stamps it here, and the cutover's atomic
-- guard requires the live value to still match at seal time. A hold placed or
-- released in between makes the values differ, so the cutover declines and the
-- operator re-runs the migration -- which restages under the fresh hold state.
ALTER TABLE harvest_shard_migrations
    ADD COLUMN IF NOT EXISTS verified_legal_hold_set_at TIMESTAMPTZ NULL;

-- `verified_legal_hold_set_at` is NULL both for "verified, no hold" and for
-- "never verified by code that knows this column exists" -- a rolling
-- deployment can have an old worker run `verify_target_copy` against a
-- database that already carries this migration, leaving the column at its
-- default. A cutover guard comparing NULL to NULL would then authorize a
-- target staged under a hold that was released before an OLD worker verified
-- it, treating an unverified row as verified. `legal_hold_verified` marks
-- that the check actually ran, independent of what it found, so a legacy or
-- foreign-verified record fails the cutover guard closed rather than by
-- coincidence -- the same shape `verified_event_count IS NOT NULL` already
-- gives the history guard.
ALTER TABLE harvest_shard_migrations
    ADD COLUMN IF NOT EXISTS legal_hold_verified BOOLEAN NOT NULL DEFAULT FALSE;

-- `begin_migration` resets both columns above when it reopens a settled
-- (`DONE`/`ABORTED`) record for another attempt. That reset only runs when
-- the code executing `begin_migration` knows these columns exist. During a
-- rolling deployment, a PARENT-version process is schema-compatible and can
-- still perform the reopening -- its `ON CONFLICT` update predates this
-- migration and never touches either column, no matter how new the code is
-- that later runs `verify_target_copy` or `commit_cutover` against the same
-- row. Enforcing the reset only in application code is therefore not
-- version-independent: it protects a reopening driven by new code, not one
-- driven by old code against a database that already carries this migration.
--
-- A trigger closes that gap the same way the column additions themselves are
-- closed against old code: at the schema, not the call site. Any UPDATE that
-- carries this row's `phase` from a settled state back to `PENDING` -- the
-- one transition `begin_migration` performs, regardless of which binary
-- version issued it -- clears both columns as part of that same statement.
-- `SET search_path` pins name resolution against a hostile search_path on
-- the connection that fires the trigger.
CREATE OR REPLACE FUNCTION harvest_shard_migrations_reset_hold_verification()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    IF NEW.phase = 'PENDING' AND OLD.phase IN ('DONE', 'ABORTED') THEN
        NEW.legal_hold_verified := FALSE;
        NEW.verified_legal_hold_set_at := NULL;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS harvest_shard_migrations_reset_hold_verification_trigger
    ON harvest_shard_migrations;
CREATE TRIGGER harvest_shard_migrations_reset_hold_verification_trigger
    BEFORE UPDATE ON harvest_shard_migrations
    FOR EACH ROW
    EXECUTE FUNCTION harvest_shard_migrations_reset_hold_verification();

-- `resume_incomplete_migrations` now orders its unsettled-record scan by
-- `attempts ASC, created_at ASC` (issue #1317), so a record whose checkout
-- or step keeps failing sinks behind less-tried ones. The original
-- `idx_harvest_shard_migrations_unsettled (created_at)` index, added by the
-- #964 migration, cannot serve that ordering: Postgres would sort the whole
-- unsettled set before applying `LIMIT`, which is most expensive exactly
-- when a shard has accumulated the large unsettled backlog the resume sweep
-- exists to drain. Replace it with an index matching the actual ordering.
-- No other query in this codebase orders this table by `created_at` alone.
DROP INDEX IF EXISTS idx_harvest_shard_migrations_unsettled;
CREATE INDEX IF NOT EXISTS idx_harvest_shard_migrations_unsettled
    ON harvest_shard_migrations (attempts, created_at)
    WHERE phase NOT IN ('DONE', 'ABORTED');
