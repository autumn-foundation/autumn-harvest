## Phase — Redis dispatch follow-ups: latency, coverage, and operability (issue #1429)

Issue #1312 shipped Redis Streams as an opt-in dispatch channel in front of the
Postgres task queue. It shipped with ten known gaps, each named in its own doc
comment or tracking note. This closes all ten in one PR, per the issue's own
scope.

**1. Tail latency under a saturated pool.** `run_dispatch_iteration` slept a
full `poll_interval` when both the workflow and activity semaphores were full,
even if a permit freed moments later. It now races the sleep against
`workflow_semaphore.acquire()` / `activity_semaphore.acquire()` via
`tokio::select!`, so a freed permit wakes the loop immediately. Broader
sampler-dependent tail-latency work stays deferred to issue #1428, per the
issue text.

**2. Buffering-scope gaps.** `dispatch::buffered`/`buffered_settled` ties hint
publication to transaction commit, so a hint never names a row a reader could
still lose to rollback. `context::run_transactional`, `poison_pill.rs`'s
`quarantine_orphan` (via `fail_owning_workflow`), and nine call sites in
`timeout.rs` (activity/workflow timeout enforcement, external task/signal/
cancel/await outbox sweeps, history-ceiling enforcement) now wrap their
`conn.transaction` calls with `buffered_settled`, matching every other
transaction owner already covered.

**3. Multi-shard runtimes.** v1 rejected Redis dispatch outright on any worker
spanning more than one shard. A worker may now install a per-shard channel
(`dispatch::install_for_shard`) for every one of its `shard_assignments`
instead of the single global slot; `Worker::new` requires full coverage before
accepting either shape, and the multi-shard poll loop reads and claims each
shard through its own matching channel. An immediate hint still cannot name
its shard (the `queue.rs` publish hooks that raise it run with no shard
context), so on a multi-shard runtime it falls through to Postgres; only the
reconcile sweep — which already runs once per shard against that shard's own
pool — publishes into the per-shard channel. That costs one reconcile
interval of latency on a row's first dispatch, never a lost or duplicated one,
matching the durability floor every other dispatch path already relies on.

**4. Priority and sticky affinity, best effort.** Redis Streams deliver in
arrival order; `COUNT` caps what a read returns before any application-side
sort can act. `RedisDispatch::next_inner` now collects every candidate across
the queues a read spans and sorts by priority before splitting leases from
surplus, so a read that spans queues favors the highest-priority candidates
among whatever surplus it already has, with no extra round trip. A read sized
to exactly one queue's ready backlog has no surplus to favor — `COUNT` already
capped it — so this is a real but narrow improvement, documented as such
rather than oversold. Sticky affinity stays best effort: any consumer group
member may read any reference, and the Postgres claim predicate is still the
enforcement point.

**5. Redis Cluster hash tags.** The key family (stream, delayed set, payload
hash, markers) is now hash-tagged per queue (`{prefix:dispatch:queue}`), so
the multi-key Lua scripts (`PUBLISH_LUA`, `REQUEUE_LUA`, `PROMOTE_MARKED_LUA`)
stay in one Cluster slot and the `CROSSSLOT` failure is gone. This crate still
connects with a single-node `redis::Client`/`ConnectionManager`, not a
cluster-aware client, and does not follow `MOVED`/`ASK` redirects — that
remains a separate follow-up, stated honestly in the crate doc rather than
implied as done.

**6. `[harvest.redis]` in `EffectiveConfigView`.** New `DispatchConfigView`
(installed state, endpoint, key prefix, consumer group, visibility timeout,
poll/reconcile intervals, reconcile batch size) on `EffectiveConfigView`,
covered by a test that also pins that no connection URL ever leaks into it.

**7. `dispatch::dropped_hints()` metrics.** New `harvest_dispatch_dropped_hints`
gauge, sampled by a dedicated background task and rendered by both the
`metrics-rs` adapter and the built-in Prometheus scrape endpoint. New alert
row in `docs/alerts/starter-pack-v0.1.0.json` and a runbook section in
`docs/runbooks/harvest-alerts.md`.

**8. Batched acks.** `TaskDispatch` gained `ack_many`/`release_many`, with a
default sequential implementation and a pipelined Redis implementation.
`run_dispatch_iteration`'s shutdown and no-permit paths now batch their
releases instead of acking/releasing one lease per round trip.

**9. Recovery round trips.** `RedisDispatch` recovery now pipelines `XPENDING`
across every queue in one round trip (`pending_pipeline`) before claiming idle
entries per queue, instead of one `XPENDING` per queue in sequence.

**10. Reconcile sweep fan-out.** `reconcile_batch` is now an operator-tunable
setting (`[harvest.redis] reconcile_batch`, env
`AUTUMN_HARVEST_REDIS__RECONCILE_BATCH`, default matches the prior hardcoded
value), validated `>= 1`, so an operator can size the sweep to their fleet
instead of N workers each duplicating a fixed-size scan.

**No new `WorkflowEvent` variant, no migration, no replay-determinism impact.**
Every change is confined to the dispatch channel, its configuration surface,
and its metrics; the Postgres claim path and event log are untouched.

**Review.** Four agents reviewed this diff from independent angles
(concurrency/crash-safety, Redis Cluster/Lua correctness,
config/metrics/operability, test-coverage/doc-consistency). Confirmed
findings, all fixed: `HarvestRunner::stop` uninstalled only the
single-shard dispatch slot, leaking a multi-shard install's per-shard
channels; a shard connect failure mid-install left earlier shards'
channels installed with no guard to unwind them; `ack_many_inner` batched
leases from any queue into one `MULTI`/`EXEC` pipeline, undercutting this
PR's own per-queue Cluster hash-tag scheme; `requeue_batch` aborted its
whole call on the first queue's script error, silently skipping sibling
queues in a multi-queue `release_many` batch; `harvest_dispatch_dropped_hints`
was missing from the alert-pack's stable-metric catalog, which would have
failed a CI guard test; the dropped-hints sampler never ran in an API-only
process (no `Worker`), even though that topology's own background
publisher can still drop hints.

**Tests.** Full `autumn-harvest` (3696), `autumn-harvest-redis` (45), and
`autumn-harvest-plugin` (1320) unit suites pass. `dispatch_tests.rs`,
`poison_pill_tests.rs`, `sharded_runtime_tests.rs`, `mutex_tests.rs`,
`codec_rotation_db_tests.rs`, and the `timeout`-scoped integration suites
against real local Postgres continue to pass unchanged — this PR did not
modify them. New integration coverage in `autumn-harvest-redis`'s
`dispatch_redis.rs`/`worker_dispatch_e2e.rs` against real local Redis,
including `maintain_recovers_unacked_leases_across_several_queues_in_one_pass`.
New unit coverage: `per_shard_dispatch_requires_full_coverage`,
`a_fully_covered_multi_shard_runtime_is_accepted`,
`per_shard_channels_are_independent_of_each_other_and_of_the_global_slot`,
`ack_many_drops_every_lease_in_the_batch`,
`release_many_gives_every_lease_back_with_its_own_delay` (per-lease delay
independence, not a shared delay), `dispatch_keys_for_one_queue_share_one_hash_tag`,
`dispatch_keys_for_different_queues_carry_different_tags`,
`dispatch_view_reports_installed_state_and_tuning`,
`dispatch_view_never_leaks_a_connection_url`,
`dispatch_dropped_hints_is_an_unlabeled_last_write_wins_gauge`,
`stop_uninstalls_the_per_shard_channels_a_multi_shard_runner_installed`, and
`stop_cancels_and_joins_the_api_only_dispatch_metrics_sampler`.
`docs/audits/comment-hygiene.py --base origin/trunk-dev` reports zero Tier B
regressions. `cargo clippy` (including `--all-features`/pedantic+nursery on
`autumn-harvest`, and the `redis`-feature variants of the other two crates)
is clean.

**Known gaps, not fixed here.** The multi-shard dispatch path (item 3) has
no end-to-end test proving a multi-shard worker claims through the
*correct* shard's channel — only construction-time coverage gating is
covered. `DispatchConfigView.key_prefix` reports the base prefix, not any
of the per-shard prefixes actually in use, for a multi-shard-in-one-process
runner. `harvest_dispatch_dropped_hints` ships as a Gauge rather than a
Counter, despite being a monotonic value; `increase()` still reads it
correctly today, but a "current value" dashboard panel would not.
