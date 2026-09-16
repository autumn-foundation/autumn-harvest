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

A third instance sat under both of the first two, in
`shard_rebalance::resolve_execution_shard` itself — also found by the
Codex PR reviewer. Resolving a target's residence for a REAL migration
needs its hop-walk, which checks out a connection to confirm each hop,
including the terminal one. That checkout has no awareness of any
already-held connection, so a target that migrated onto the caller's own
shard makes the confirmation hop land on `conn`'s own pool — an
unconditional `pool.get()` with no bound at all, worse than either fix
above. **Fix.** Added `resolve_execution_shard_holding`, a twin of
`resolve_execution_shard` that takes the held `(shard, connection)` and
reads any hop landing on that pool through it instead of checking one
out, mirroring how `resolve_target_shard_holding` already does this for a
`WorkflowId` target. `execution_id_residence` and the deferred-check
residence lookup both call it now instead of the bare hop-walk.

The first cut of that fix carried its own defect, also caught by the
Codex PR reviewer: `on_held` checked a hop's pool with `pool_for`, which
falls back to the default shard for a shard this process has no pool for.
A forwarding pointer naming such a shard could then alias onto the held
pool's default by coincidence, get read on `conn` — a database the
execution has no row on — find nothing, and report that unconfigured hop
resolved instead of unreachable. Fixed by using `exact_pool_for` for
every hop after the tolerant origin one, matching `resolve_execution_shard`'s
own `checkout`/`checkout_entry` split exactly, so an unconfigured
forwarded hop now returns `ShardUnavailable` instead of a wrong answer.

**Test evidence.**
`shard_rebalance_db_tests::a_rebalanced_caller_self_shard_cancel_reuses_the_held_connection`
reproduces the first instance against two real shard databases, with the
target shard's pool built at capacity one so a wrongly-acquired second
connection cannot succeed. It mints a caller execution whose id encodes
`SOURCE` but whose row is written directly onto `TARGET` (what a
completed migration leaves behind), issues a same-shard by-id cancel, and
runs `enforce_external_cancels_outbox` against the pool's only
connection. Confirmed red without the first fix (`processed == 0`; the
delivery was silently skipped) and green with it (`processed == 1`,
target state `CANCELLED`).

`shard_rebalance_db_tests::a_migrated_cancel_target_s_unfinished_handler_check_reuses_the_held_connection`
covers the second and third instances together, against a REAL migration
this time (`shard_rebalance::migrate_execution`) so the target's
`residence` genuinely differs from its encoded shard — the case the first
test's direct-insert shortcut cannot reach. Before the third fix this
test hung until its own 10-second guard timeout; with only the second fix
applied it still failed the same way, because the hang sat upstream of
that comparison, in the residence lookup itself. With all three fixes it
passes in under 2 seconds, records `("1324b_target_flow", 1)` on a spy
`MetricsRecorder` (confirming the unfinished-handler check ran, not just
that the sweep returned), and leaves the target `CANCELLED`.

`shard_rebalance_db_tests::a_forward_to_an_unconfigured_shard_fails_closed_not_via_the_held_pools_default`
covers the fail-closed defect in the first cut of the third fix: a
fabricated forward to shard id 2 (never configured) from an origin that
differs from the held shard, with the held shard also set as the pool's
default. Confirmed red on the first cut (`Ok(ShardId(2))`, silently
wrong) and green on the corrected version
(`Err(ShardUnavailable { shard_id: 2, .. })`).

The full `shard_rebalance_db_tests` suite (51 tests) and
`workflow_id_targeted_tests` suite (26 tests) pass with no regressions,
as does the crate's full unit-test suite (3593 tests, 1 pre-existing
ignore) and `cargo clippy --features testing --tests -- -D warnings`.

**No schema change, no new `WorkflowEvent` variant, no migration.**
