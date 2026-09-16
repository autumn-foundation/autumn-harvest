## Fix — the by-id fan-out could mislabel a rebalanced caller's own shard (issue #1324)

`timeout::resolve_delivery_route` already read the caller's real shard off
the held connection (`caller_shard`, the issue #964 fix): a query against
`harvest_workflow_executions.shard_id` on `conn` itself, not a decode of the
caller's `ExecutionId` bits. The function's final same-pool decision did not
use it. It re-derived the caller's pool from `caller_exec_id` through
`ShardedDbPool::exact_pool_for_execution`, which resolves the id's ENCODED
shard — where the run STARTED, not where it currently lives.

For a caller whose execution had been rebalanced (issue #964's shard
migration) onto the same shard as its by-id delivery target, this compared
the target's pool against the caller's STALE origin pool instead of its
current one, judged the two shards different, and took the cross-shard
branch. That branch acquires a fresh connection from the target shard's
pool — the same pool `conn` is already checked out from — which is exactly
the self-deadlock this module's fan-out exists to avoid under the
documented pool-size-1 configuration. The peer-acquisition bound (issue
#1146) turns the indefinite park into a short, bounded skip instead of a
true hang, so the caller-observable effect was a delivery silently left
pending rather than a stall, but the row could keep missing every sweep.

**Fix.** Compare pools with `pool.exact_pool_for(caller_shard)`, not
`pool.exact_pool_for_execution(caller_exec_id)`. Also added a
belt-and-braces guard: if `caller_shard` itself has no configured pool,
`resolve_delivery_route` now returns `DeliveryRoute::Retry` with a named
reason immediately, instead of letting the routing decision fall through to
`pool_for`'s silent default-shard fallback.

A second, independent instance of the same defect sat one step further
down the same sweep, in `enforce_external_cancels_outbox`'s post-commit
deferred unfinished-update-handler check. It already computed a
forwarding-aware `residence` for the checked execution, but its
`same_pool_as_caller` decision still compared `exact_pool_for_execution`,
the same stale, bits-based lookup. Unlike the first instance this one is
not bound by a peer-acquisition timeout: the fallback branch calls a raw
`pool.get()`, so a migrated cancel target sharing its checker's shard
could wedge the checker indefinitely, not just leave a row pending. Fixed
the same way, by comparing pools through `residence` instead. Found
independently by two reviewers: a Claude code-review subagent during this
PR's own multi-angle review, and the repository's Codex PR reviewer.

**Test evidence.**
`shard_rebalance_db_tests::a_rebalanced_caller_self_shard_cancel_reuses_the_held_connection`
reproduces the bug against two real shard databases, with the target
shard's pool built at capacity one so a wrongly-acquired second connection
cannot succeed. It mints a caller execution whose id encodes `SOURCE` but
whose row is written directly onto `TARGET` (what a completed migration
leaves behind), issues a same-shard by-id cancel, and runs
`enforce_external_cancels_outbox` against the pool's only connection.
Confirmed red without the fix (`processed == 0`; the delivery was silently
skipped) and green with it (`processed == 1`, target state `CANCELLED`).
The full `shard_rebalance_db_tests` suite (49 tests) and
`workflow_id_targeted_tests` suite (26 tests) still pass with no
regressions, as does `cargo clippy --features testing --tests -- -D
warnings`.

The second instance has no isolated DB regression test. Reproducing it
needs a real cross-shard migration for the checked execution, so
`residence` genuinely differs from its encoded shard. Resolving that
always calls `shard_rebalance::resolve_execution_shard`, which checks out
its own connection on the destination shard regardless of this bug. Under
the pool-size-1 setup that would expose this defect, that unrelated,
pre-existing checkout hazard fires first and masks the signal. Verified
by inspection instead: the fix mirrors the first instance exactly, over
the same `residence` value the surrounding code already computes.

**No schema change, no new `WorkflowEvent` variant, no migration.**
