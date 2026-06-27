-- Support O(log n) keyset pagination on GET /workflows/{id}/history (issue #529).
-- The (workflow_exec_id, id) composite index lets each page fetch walk in id order
-- within an execution, so LIMIT is applied before materializing all rows rather
-- than after sorting the full execution history.  The existing
-- idx_harvest_events_exec covers (workflow_exec_id, event_id) and is used by
-- load_history / load_history_since; this index covers the id-based cursor path.
CREATE INDEX IF NOT EXISTS idx_harvest_events_history_page
    ON harvest_events (workflow_exec_id, id);
