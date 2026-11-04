-- Issue #2394: drop the refine policy log, and narrow the author kinds.
--
-- The drop removes every stored policy, so each kind reads the defaults.
-- The narrow CHECK fails while a row has the author kind `auto`. Delete or
-- keep those rows first: an active `auto` version cannot be deleted.
SET LOCAL lock_timeout = '5s';

DROP TABLE IF EXISTS harvest_agent_refine_policies;
DROP FUNCTION IF EXISTS harvest_agent_refine_policies_guard_immutable();

ALTER TABLE harvest_agent_overlay_activations
    DROP CONSTRAINT IF EXISTS harvest_agent_overlay_activations_actor_kind_check;
ALTER TABLE harvest_agent_overlay_activations
    ADD CONSTRAINT harvest_agent_overlay_activations_actor_kind_check
    CHECK (actor_kind IN ('operator', 'refine'));

ALTER TABLE harvest_agent_overlays
    DROP CONSTRAINT IF EXISTS harvest_agent_overlays_author_kind_check;
ALTER TABLE harvest_agent_overlays
    ADD CONSTRAINT harvest_agent_overlays_author_kind_check
    CHECK (author_kind IN ('operator', 'refine'));
