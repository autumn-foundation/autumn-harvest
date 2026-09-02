## Phase 5.x — shard-placement-aware resolution for `workflow_id`-addressed signal/cancel (issue #1146)

`ctx.signal_external_workflow_by_id` / `ctx.request_cancel_external_workflow_by_id`
(issue #751) address a target by its stable `(workflow_name, workflow_id)`
business key. To deliver, the engine has to know which shard owns the target,
and it answered that by re-deriving `ShardRouter::pick_for_new_workflow` — the
rendezvous hash a **fresh start** of that key would use.

That is a prediction of where *new* work would be placed, not an observation of
where *existing* work is, and the two diverge in two ways. A workflow started
with an explicit pin (`ShardPlacement::Shard` / `ShardPlacement::ResidencyKey`,
issue #697) lives on a shard the pure hash may never compute. And
`pick_writable` re-hashes over the *current* `writable_shards` when the
readable-set hash lands outside it, so draining a shard moves where a key
resolves **after** a workflow was already placed there. In both cases the
delivery looked at a database the target is not in, found nothing, and — once
the unknown-target grace window elapsed — wrote `target_unknown` durably into
the caller's append-only history for a target that was running the whole time.

**Resolve by observation, not prediction.** The new
`external_target_placement` module fans a read out across every expected shard
and merges the per-shard answers with `execution::select_resolved_run` — the
same active-run-first ranking the management API's by-id endpoints already use
(issue #805), so the engine and the HTTP surface cannot disagree about which run
of a business key is the current one. This is issue #1146's proposal 2
(cross-shard fan-out) rather than proposal 1 (a placement directory):
the executions table already *is* the record of where a run lives, and a
directory would be a second source of truth that has to be written on every
start, migrated, retained, erased, and kept honest across continue-as-new,
resets, retries and cross-shard children. A fan-out closes the drained-shard
drift vector for free, as the issue notes.

Two rules are load-bearing, and are what a naive fan-out gets wrong:

- **No first-hit short circuit.** `(workflow_name, workflow_id)` uniqueness is
  *shard-local*, so a stale terminal run of the key on one shard and the live
  run on another is an ordinary state. Stopping at the first shard that returns
  a row would signal the dead one and report `not_running` while the target is
  alive. Every expected shard is asked before a terminal answer is accepted.
  A **live** winner, by contrast, is authoritative immediately — at most one run
  per key is active — so a delivery is never withheld because an unrelated shard
  is down.
- **"Could not inspect" is never "not there."** A shard with no pool in this
  process (mid a shard-add rollout) or one that cannot be reached yields
  `TargetPlacement::Indeterminate`, and the outbox leaves the row pending.
  `NotFound` — the only outcome that may become a permanent `target_unknown` —
  requires a fan-out that inspected *every* expected shard. Without this, a
  transient outage lasting longer than the grace window would have been written
  into history as a wrong, irreversible answer.

**The inline fast path is narrowed rather than patched.** Both outbox scanners
now share one `resolve_delivery_route`, the single place the engine decides
which database owns an `ExternalTarget`; the worker's inline (same-transaction)
attempt is gated by `inline_delivery_allowed`, which permits an `ExecutionId`
target on the caller's own shard exactly as before and permits a `WorkflowId`
target **only in a single-shard deployment**. The old gate compared the caller's
shard against the key's hash, which does not prevent the hazard it looks like it
prevents: the hash lands on the caller's shard for 1-in-N keys regardless of
where the target actually is, and inline delivery resolves against the caller's
shard alone — so a stale terminal run there produced a permanent `not_running`
against a live target. One shard means the caller's view *is* the deployment, so
single-shard deployments (every pre-sharding deployment, and the default) keep
the fast path and are byte-for-byte unchanged. Multi-shard deployments defer to
the outbox, which is one sweep of latency on an already-asynchronous path and is
where the hash already sent `(N-1)/N` of these deliveries.

**No migration, no new `WorkflowEvent` variant, no write-path change**, and
`harvest_events` gains no new writer — the resolution is read-only and the
`ExternalTarget` type is untouched, so replay determinism is unaffected by
construction. `ExecutionId`-addressed signal/cancel is entirely unchanged: the
shard is decoded from the id and is always authoritative.

**Cost.** One row read per expected shard per by-id delivery attempt, on the
outbox scanners rather than the hot dispatch path — which is what makes an
O(shards) resolution acceptable here. The fan-out is sequential and holds at
most one extra connection at a time. A single-shard deployment expects exactly
one shard, so it makes exactly the one query it already made.

**API changes.** `shard::external_target_owning_shard` is retained and still
correct for the question it answers — *where would this key be placed?* — which
is what `worker::reject_cross_shard_continue_as_new` and the re-run
`workflow_id`-override guard need; its doc comment now says so instead of
carrying a known-limitation note. `ShardedDbPool::exact_pool_for_target` is
`#[deprecated]`: asking which *pool* holds a target is always a "where does it
live" question, and the hash cannot answer it. The plugin's
`shard_fanout::expected_shards` now delegates to the core `fanout_shards`, so
the management API and the engine inspect the same shard set by construction.

**Test evidence, TDD red → green → refactor.** The three end-to-end outbox tests
were written first and observed failing against the pre-fix engine with exactly
the issue's symptom (`ExternalSignalFailed` / `ExternalCancelFailed` with
`target_unknown` against a running target). 12 no-database unit tests in
`external_target_placement` cover the merge rules, the expected-shard union and
the inline gate. `tests/integration/shard_placement_by_id_tests.rs` adds 17 tests
against **two genuinely separate Postgres databases** (a single database mocked
as two shards cannot distinguish "found by fanning out" from "found because both
shards are the same table"): a target pinned off its hash shard is signalled and
cancelled; a run left behind by a `writable_shards` change is still found; the
live run wins over a stale terminal on the hash shard; an un-poolable shard is
`Indeterminate` and produces no `target_unknown` even with the grace window fully
expired; and a complete fan-out that finds nothing still fails `target_unknown`,
so the fix does not make that outcome unreachable. The issue #751 suite
(26 tests) and the cross-workflow / cross-shard / sharding suites pass unchanged.

**Documented in** `docs/sharding.md` (new *Business-key addressing finds a
pinned run wherever it is* section under issue #697), `docs/security-posture.md`
(the #697/#751 interaction bullet, rewritten from a limitation to the resolved
behaviour plus its read-surface and outage posture), and the
`WorkflowContext::*_by_id` doc comments, whose "Known limitation — explicit
shard placement" sections are replaced by the resolution contract.
