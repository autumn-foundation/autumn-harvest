-- The activation log of agent profile overlays (issue #2374, epic #2369).
--
-- Each move of the pointer in `harvest_agent_overlay_heads` adds one row.
-- The row holds the pointer before the move, the version after it and the
-- actor. The move and its row commit in one transaction, under the lock of
-- the head row. A rollback reads the last row and activates its
-- `from_version` over its `to_version`.
--
-- `seq` is the number of the move in its kind. The writer takes the last
-- `seq` plus 1 under the head lock, so two moves never share a number. An
-- activation of the active version moves nothing and adds no row.
--
-- No foreign key points at a version. An erasure can still delete a
-- version that is not active. A rollback to a deleted version then fails
-- with an unknown version, and the pointer does not move.
--
-- A trigger refuses each UPDATE of a row, as the version guard of issue
-- #2371 does. The trigger stops mistakes. It is not a security boundary.
--
-- The table is new and not hot. No other session holds a lock on it. The
-- timeout bounds the trigger all the same.
SET LOCAL lock_timeout = '5s';

CREATE TABLE harvest_agent_overlay_activations (
    kind         TEXT        NOT NULL CHECK (kind <> ''),
    seq          BIGINT      NOT NULL CHECK (seq >= 1),
    from_version BIGINT      CHECK (from_version >= 1),
    to_version   BIGINT      NOT NULL CHECK (to_version >= 1),
    actor_kind   TEXT        NOT NULL CHECK (actor_kind IN ('operator', 'refine')),
    actor        TEXT        NOT NULL CHECK (actor <> ''),
    activated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (kind, seq),
    CHECK (from_version IS DISTINCT FROM to_version)
);

CREATE OR REPLACE FUNCTION harvest_agent_overlay_activations_guard_immutable()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $harvest_agent_overlay_activations_2374$
BEGIN
    RAISE EXCEPTION 'harvest_agent_overlay_activations is append-only: move % of kind % never changes', OLD.seq, OLD.kind
        USING ERRCODE = 'restrict_violation',
              HINT = 'Activate a version to add a move. See CLAUDE.md, Engine Invariants.';
END
$harvest_agent_overlay_activations_2374$;

COMMENT ON FUNCTION harvest_agent_overlay_activations_guard_immutable() IS
    'Issue #2374: refuses each UPDATE of an agent overlay activation row.';

CREATE TRIGGER harvest_agent_overlay_activations_immutable_trg
    BEFORE UPDATE ON harvest_agent_overlay_activations
    FOR EACH ROW EXECUTE FUNCTION harvest_agent_overlay_activations_guard_immutable();

COMMENT ON TABLE harvest_agent_overlay_activations IS
    'Issue #2374: each move of the active agent overlay pointer, keyed by (kind, seq).';
