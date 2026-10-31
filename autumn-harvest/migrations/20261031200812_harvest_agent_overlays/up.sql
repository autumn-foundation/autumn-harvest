-- The versioned store of agent profile overlays (issue #2371, epic #2369).
--
-- Each agent kind has a log of overlay versions and one active pointer. A
-- version row never changes after its insert. A rollback moves the pointer
-- to an earlier version, so it restores the exact bytes of that version.
--
-- `harvest_agent_overlays` holds the versions. `body` is BYTEA, not JSONB,
-- because JSONB reorders keys and a read must return the written bytes.
-- `author_kind` and `author` record who wrote a version: an operator id, or
-- the execution id of a refine run.
--
-- `harvest_agent_overlay_heads` holds one row per kind. `latest` is the
-- last version. A put upserts this row, so two puts of one kind take the
-- same row lock and get two versions. `active` is the pointer. Its foreign
-- key keeps it on a stored version, so the active version cannot be
-- deleted.
--
-- The trigger refuses each UPDATE of a version row, as the `harvest_events`
-- guard of issue #1817 does. It stops mistakes. It is not a security
-- boundary: a superuser can disable triggers. DELETE is not guarded, so a
-- later erasure can remove a version that is not active.
--
-- The tables are new and not hot. No other session holds a lock on them.
-- The timeout bounds the trigger and the foreign key all the same.
SET LOCAL lock_timeout = '5s';

CREATE TABLE harvest_agent_overlays (
    kind        TEXT        NOT NULL CHECK (kind <> ''),
    version     BIGINT      NOT NULL CHECK (version >= 1),
    body        BYTEA       NOT NULL,
    author_kind TEXT        NOT NULL CHECK (author_kind IN ('operator', 'refine')),
    author      TEXT        NOT NULL CHECK (author <> ''),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (kind, version)
);

CREATE TABLE harvest_agent_overlay_heads (
    kind       TEXT        PRIMARY KEY,
    latest     BIGINT      NOT NULL CHECK (latest >= 1),
    active     BIGINT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (kind, active) REFERENCES harvest_agent_overlays (kind, version)
);

CREATE OR REPLACE FUNCTION harvest_agent_overlays_guard_immutable()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $harvest_agent_overlays_2371$
BEGIN
    RAISE EXCEPTION 'harvest_agent_overlays is append-only: version % of kind % never changes', OLD.version, OLD.kind
        USING ERRCODE = 'restrict_violation',
              HINT = 'Put a new version, then activate it. See CLAUDE.md, Engine Invariants.';
END
$harvest_agent_overlays_2371$;

COMMENT ON FUNCTION harvest_agent_overlays_guard_immutable() IS
    'Issue #2371: refuses each UPDATE of an agent overlay version.';

CREATE TRIGGER harvest_agent_overlays_immutable_trg
    BEFORE UPDATE ON harvest_agent_overlays
    FOR EACH ROW EXECUTE FUNCTION harvest_agent_overlays_guard_immutable();

COMMENT ON TABLE harvest_agent_overlays IS
    'Issue #2371: immutable agent profile overlay versions, keyed by (kind, version).';
COMMENT ON TABLE harvest_agent_overlay_heads IS
    'Issue #2371: the last and the active overlay version of each agent kind.';
