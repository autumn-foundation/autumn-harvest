-- Append-only guard on `harvest_events` (issue #1817).
--
-- History is an append-only log. Replay reads it back in order, so a
-- rewritten row can change what a past run means. This trigger enforces
-- the rule in the database. It stops an ad-hoc fix, a new code path or an
-- operator script that rewrites history.
--
-- The rules, per updated row:
--
-- 1. `event_data` can change only when the transaction sets
--    `harvest.sanctioned_event_rewrite` to `erase` or `codec_rotation`.
--    Only `erase.rs` and `codec_rotation.rs` set it. See CLAUDE.md.
-- 2. The `type` key inside `event_data` never changes.
-- 3. No other column can change. The check compares whole rows, so a
--    column that a later migration adds is guarded too.
--
-- Rule 3 covers `cohort`. The sweeper's fast drop gate assumes that no
-- row's cohort predates its execution. A row moved into an older cohort
-- can be dropped while its run is still live.
--
-- The guard checks who rewrites `event_data`, not what the writer changes
-- under `data`. Each writer's own tests prove its scope.
--
-- This guard stops mistakes. It is not a security boundary: any role can
-- set a custom setting, and a superuser can disable triggers.
--
-- DELETE and TRUNCATE are not guarded. Retention and partition reclaim
-- need them.
--
-- No row is read or written. `CREATE TRIGGER` takes a SHARE ROW EXCLUSIVE
-- lock, so event writes wait behind it. A held lock fails the migration
-- after 5 s, and the operator re-runs it.
SET LOCAL lock_timeout = '5s';

CREATE OR REPLACE FUNCTION harvest_events_guard_append_only()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $harvest_append_only_1817$
DECLARE
    sanction text := coalesce(current_setting('harvest.sanctioned_event_rewrite', true), '');
    new_rest record;
    old_rest record;
BEGIN
    -- Compare every column except `event_data`. Clearing it in copies is
    -- cheaper than serializing a large payload to compare whole rows.
    new_rest := NEW;
    old_rest := OLD;
    new_rest.event_data := NULL;
    old_rest.event_data := NULL;
    IF new_rest IS DISTINCT FROM old_rest THEN
        RAISE EXCEPTION 'harvest_events is append-only: row % changes a column other than event_data', OLD.id
            USING ERRCODE = 'restrict_violation',
                  HINT = 'Only event_data can change. See CLAUDE.md, Engine Invariants.';
    END IF;
    IF (NEW.event_data -> 'type') IS DISTINCT FROM (OLD.event_data -> 'type') THEN
        RAISE EXCEPTION 'harvest_events is append-only: row % changes the event type', OLD.id
            USING ERRCODE = 'restrict_violation',
                  HINT = 'The type key inside event_data never changes. See CLAUDE.md, Engine Invariants.';
    END IF;
    IF NEW.event_data IS DISTINCT FROM OLD.event_data
       AND sanction NOT IN ('erase', 'codec_rotation')
    THEN
        RAISE EXCEPTION 'harvest_events is append-only: row % rewrites event_data without a sanction', OLD.id
            USING ERRCODE = 'restrict_violation',
                  HINT = 'Only erase.rs and codec_rotation.rs rewrite event_data. See CLAUDE.md, Engine Invariants.';
    END IF;
    RETURN NEW;
END
$harvest_append_only_1817$;

COMMENT ON FUNCTION harvest_events_guard_append_only() IS
    'Issue #1817: rejects history rewrites on harvest_events. See CLAUDE.md, Engine Invariants.';

CREATE TRIGGER harvest_events_append_only_trg
    BEFORE UPDATE ON harvest_events
    FOR EACH ROW EXECUTE FUNCTION harvest_events_guard_append_only();
