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
-- 3. `cohort` can change. It is storage placement, not history. Replay
--    does not read it.
-- 4. No other column can change. The check compares whole rows, so a
--    column that a later migration adds is guarded too.
--
-- This guard stops mistakes. It is not a security boundary: any role can
-- set a custom setting, and a superuser can disable triggers.
--
-- DELETE and TRUNCATE are not guarded. Retention and partition reclaim
-- need them.
--
-- Metadata only: no row is read or written. A held lock fails the
-- migration after 5 s, so traffic does not queue behind it.
SET LOCAL lock_timeout = '5s';

CREATE OR REPLACE FUNCTION harvest_events_guard_append_only()
RETURNS trigger
LANGUAGE plpgsql
AS $harvest_append_only_1817$
DECLARE
    sanction text := coalesce(current_setting('harvest.sanctioned_event_rewrite', true), '');
BEGIN
    IF (to_jsonb(NEW) - 'event_data' - 'cohort') IS DISTINCT FROM (to_jsonb(OLD) - 'event_data' - 'cohort')
       OR (NEW.event_data -> 'type') IS DISTINCT FROM (OLD.event_data -> 'type')
    THEN
        RAISE EXCEPTION 'harvest_events is append-only: row % changes an identity column', OLD.id
            USING ERRCODE = 'restrict_violation',
                  HINT = 'Only event_data and cohort can change. See CLAUDE.md, Engine Invariants.';
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
