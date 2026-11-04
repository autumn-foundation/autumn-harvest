-- The operator refine policy of each agent kind (issue #2394, epic #2369).
--
-- `harvest_agent_refine_policies` is the policy log of each kind. Each set
-- or reset adds one row. The last row of a kind is its current policy.
-- `new_body` NULL means the defaults. No row also means the defaults.
-- `old_body` is the body of the row before, so each row is the audit of
-- one change with its old and new value.
--
-- `seq` is the number of the change in its kind. A writer takes the last
-- `seq` plus 1 in the insert. Two writers that read the same last row take
-- the same `seq`, and the primary key refuses the second one. So
-- `old_body` is always the body before.
--
-- A trigger refuses each UPDATE of a row, as the overlay guards of issues
-- #2371 and #2374 do. The trigger stops mistakes. It is not a security
-- boundary.
--
-- An auto adoption of the refine loop records the author kind `auto`. The
-- two overlay tables widen their CHECK to allow it.
--
-- The tables are not hot. The timeout bounds each lock all the same.
SET LOCAL lock_timeout = '5s';

CREATE TABLE harvest_agent_refine_policies (
    kind       TEXT        NOT NULL CHECK (kind <> ''),
    seq        BIGINT      NOT NULL CHECK (seq >= 1),
    old_body   BYTEA,
    new_body   BYTEA,
    actor_kind TEXT        NOT NULL CHECK (actor_kind = 'operator'),
    actor      TEXT        NOT NULL CHECK (actor <> ''),
    changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (kind, seq)
);

CREATE OR REPLACE FUNCTION harvest_agent_refine_policies_guard_immutable()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $harvest_agent_refine_policies_2394$
BEGIN
    RAISE EXCEPTION 'harvest_agent_refine_policies is append-only: change % of kind % never changes', OLD.seq, OLD.kind
        USING ERRCODE = 'restrict_violation',
              HINT = 'Set or reset the policy to add a change. See CLAUDE.md, Engine Invariants.';
END
$harvest_agent_refine_policies_2394$;

COMMENT ON FUNCTION harvest_agent_refine_policies_guard_immutable() IS
    'Issue #2394: refuses each UPDATE of an agent refine policy change.';

CREATE TRIGGER harvest_agent_refine_policies_immutable_trg
    BEFORE UPDATE ON harvest_agent_refine_policies
    FOR EACH ROW EXECUTE FUNCTION harvest_agent_refine_policies_guard_immutable();

COMMENT ON TABLE harvest_agent_refine_policies IS
    'Issue #2394: each change of the refine policy of an agent kind, keyed by (kind, seq).';

ALTER TABLE harvest_agent_overlays
    DROP CONSTRAINT IF EXISTS harvest_agent_overlays_author_kind_check;
ALTER TABLE harvest_agent_overlays
    ADD CONSTRAINT harvest_agent_overlays_author_kind_check
    CHECK (author_kind IN ('operator', 'refine', 'auto'));

ALTER TABLE harvest_agent_overlay_activations
    DROP CONSTRAINT IF EXISTS harvest_agent_overlay_activations_actor_kind_check;
ALTER TABLE harvest_agent_overlay_activations
    ADD CONSTRAINT harvest_agent_overlay_activations_actor_kind_check
    CHECK (actor_kind IN ('operator', 'refine', 'auto'));
