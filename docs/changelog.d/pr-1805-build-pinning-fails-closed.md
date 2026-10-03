## Phase 8.x — Build pinning fails closed (issue #1805)

A worker with an empty `build_id` can no longer claim a run pinned to a build.
The `OR $n = ''` bypass is gone from all claim queries and from
`BuildCompatibilitySet::is_eligible`. Unpinned rows keep today's behaviour, so
a queue with no build policy is unchanged.

A worker with an empty `build_id` on a queue that has a build policy logs a
warning at registration. It also sets the gauge
`harvest.worker.empty_build_policy{queue}`.

No migration. No `WorkflowEvent` variant. No replay impact.

Tests: `no_claim_query_lets_an_empty_build_worker_bypass_pinning`,
`empty_build_worker_cannot_claim_a_pinned_task`,
`empty_build_worker_claims_unpinned_task_without_policy`,
`empty_build_worker_is_flagged_only_on_queues_with_a_policy`.
