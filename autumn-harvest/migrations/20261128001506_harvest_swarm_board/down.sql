-- Reverts 20261128001506_harvest_swarm_board.
SET LOCAL lock_timeout = '5s';

DROP TABLE IF EXISTS harvest_swarm_merges;
DROP TABLE IF EXISTS harvest_swarm_mains;
DROP TABLE IF EXISTS harvest_swarm_posts;
DROP TABLE IF EXISTS harvest_swarm_counters;
DROP TABLE IF EXISTS harvest_swarm_items;
DROP FUNCTION IF EXISTS harvest_swarm_guard_immutable();
