-- Revert: drop the decision owner leases (issue #2251).
DROP TABLE IF EXISTS harvest_decision_owner;
DROP FUNCTION IF EXISTS harvest_decision_owner_fence(INTEGER, TEXT);
DROP FUNCTION IF EXISTS harvest_decision_owner_guard();
