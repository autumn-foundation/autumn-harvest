-- Reverts 20261102032158_harvest_agent_policies (issue #2439).
SET LOCAL lock_timeout = '5s';

DROP TABLE IF EXISTS harvest_agent_policy_audit;
DROP TABLE IF EXISTS harvest_agent_policies;
