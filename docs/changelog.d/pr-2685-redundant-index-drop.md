## Redundant index drop (issue #2684, PR #2685)

Two migrations drop five indexes. `20261114213148` drops
`idx_harvest_events_exec`, a duplicate of the `(workflow_exec_id, event_id)`
constraint. `20261114213149` drops `idx_harvest_we_state`,
`idx_harvest_we_shard`, `idx_harvest_we_quota_reconcile_candidates` and
`harvest_wf_exec_detached_running`, each covered by another index or used by
no query. Each drop bounds its lock wait and gives an out-of-band
`DROP INDEX CONCURRENTLY` recipe. The upgrade guide lists both.

Evidence: WAL per bench run falls 4.1% and an event insert 7.1%. Throughput
does not move. All 73 indexes of the three hot tables were audited, and
each drop survived two adversarial checks. See
`docs/performance-redundant-index-drop.md`.
