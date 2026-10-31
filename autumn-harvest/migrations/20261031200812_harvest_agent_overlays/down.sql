-- Issue #2371: drop the agent overlay store.
--
-- The drop removes every stored overlay version. An agent then runs its
-- code profile.
DROP TABLE IF EXISTS harvest_agent_overlay_heads;
DROP TABLE IF EXISTS harvest_agent_overlays;
DROP FUNCTION IF EXISTS harvest_agent_overlays_guard_immutable();
