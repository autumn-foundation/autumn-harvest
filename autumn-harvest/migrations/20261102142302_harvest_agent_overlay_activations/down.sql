-- Issue #2374: drop the activation log of agent overlays.
--
-- The drop removes the record of each move. The active pointers stay, so
-- each agent keeps its version. A rollback then has no earlier activation.
DROP TABLE IF EXISTS harvest_agent_overlay_activations;
DROP FUNCTION IF EXISTS harvest_agent_overlay_activations_guard_immutable();
