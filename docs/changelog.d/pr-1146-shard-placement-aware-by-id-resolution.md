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
`external_target_location` module fans a read out across every expected shard
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
  `TargetLocation::Indeterminate`, and the outbox leaves the row pending.
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

**Cost, and the connection budget.** One to two row reads per expected shard per
by-id delivery attempt (the per-shard resolver probes active-first, then
most-recent-terminal only when a shard holds no active run), on the outbox
scanners rather than the hot dispatch path — which is what makes an O(shards)
resolution acceptable here. A single-shard deployment expects one shard and
**skips the fan-out entirely**, so the default deployment shape is genuinely
unchanged rather than merely cheap.

Connections, not queries, turned out to be the scarce resource, and the first
cut of this change got it wrong: the fan-out asked the caller's own shard pool
for a second connection while the sweep held one from it inside an open
transaction. `pool.get()` is an unbounded wait — Harvest configures no deadpool
`Timeouts` — so on a one-connection pool that parks forever and wedges every
later resident of the scanner tick, exactly the hazard `codec_rotation.rs` and
`audit_export.rs` were each fixed for. Three rules close it: the single-shard
short-circuit above; the caller's own shard probed on the connection already in
hand (equivalent under READ COMMITTED, where each statement takes a fresh
snapshot); and every remaining acquisition bounded by
`audit_export::SHARD_ACQUIRE_BOUND`, with a per-sweep memo so a backlog of
pending rows pays that bound once per shard rather than once per row. A shard
that times out becomes `Indeterminate` — a retry — not a wrong answer.

**Note for the changelog collator.** `docs/shipped-work.md`'s issue #751 entry
(AC8) and its #777 entry both still state that by-id addressing resolves a
`WorkflowId` target's shard "via the identical hash" and that the hash "cannot
disagree with where a real start would place it". Both are superseded by this
entry and should be reconciled when this fragment is folded in.

**API changes.** `shard::external_target_owning_shard` is retained and still
correct for the question it answers — *where would this key be placed?* — which
is what `worker::reject_cross_shard_continue_as_new` and the re-run
`workflow_id`-override guard need; its doc comment now says so instead of
carrying a known-limitation note. `ShardedDbPool::exact_pool_for_target` is
`#[deprecated]`: asking which *pool* holds a target is always a "where does it
live" question, and the hash cannot answer it. The plugin's
`shard_fanout::expected_shards` now delegates to the core `fanout_shards`, and
`version_usage.rs`'s third hand-rolled copy of the same rule delegates to that —
so the management API and the engine inspect the same shard set by construction.
`fanout_shards_from_parts` exists so that delegation costs no `ShardRouter`
clone per management-API request. The new type is `TargetLocation`, not
`TargetPlacement`: `ShardPlacement` is a policy for where new work goes, this is
an observation of where existing work is, and confusing the two is what issue
#1146 *is*.

**Codex round 1 (one P1, one P2, both real).** The P1 is the second act of the
connection story above. Bounding the fan-out's acquisitions stopped an
*indefinite* park but left a circular one: `Worker` runs one timeout checker per
assigned shard, each holding its own pool's connection for the whole pass, so a
process assigned two shards with one connection per pool has checker 0 waiting
on pool 1 while checker 1 waits on pool 0. Both bounds expire, both memoize the
peer, nothing delivers — and with the generous 5s scanner bound both scanners
stall for it every tick. Peer acquisitions now use a tight
`FANOUT_ACQUIRE_BOUND` instead, so neither scanner ever *waits* on the other and
the cycle cannot form; the row is retried on the next tick, whose phase has
drifted. The same circular shape applies to the cross-shard *delivery*
acquisition, which predates this issue entirely (it has always served
`ExecutionId` targets) and was unbounded — now bounded with the same constant.
`docs/sharding.md` states the deterministic answer: a process polling several
shards should size each shard pool at 2 or more.

The P2 is a correctness bug of exactly the class this issue exists to remove.
`resolve_delivery_route` kept only the resolved *shard* and let the delivery
attempt re-resolve shard-locally — which it must, to catch a continue-as-new.
But a shard-local re-read cannot see the key move shards: if the live run
terminates and a new run of the same key starts elsewhere in that window, the
re-read sees only the old terminal run, and its verdict is a permanent
`not_running` for a signal, or a no-op success for a cancel, against a run that
is alive. The route now carries whether the resolution saw a **live** run, and a
shard-local read that disagrees leaves the row pending for a global re-resolve
instead of recording anything. It terminates by construction: once the key is
terminal everywhere the global winner is terminal, the expectation is `false`,
and the verdict is recorded as before. The rule is a pure `classify_by_id_outcome`
with an exhaustive unit matrix — the window it guards is microseconds wide and
has no test seam, so the classifier is split out for the same reason
`worker::classify_cross_shard_continue_as_new` is. (A randomized end-to-end race
test was written first and **discarded**: it passed with the fix disabled, because
the seal never landed inside the window. It would have been evidence of nothing.)

**Codex round 2 (two more P1s).** The first is a distinction between signal and
cancel that the merge rule had collapsed. `merge_locations` returns `Found` for a
live run even when a shard could not be inspected, on the reasoning that a live
run in hand is a real target. That holds for a signal. It does **not** hold for a
cancel, because `ExternalCancelDelivered` does not report a delivery — it asserts
a property of the whole business key, "nothing is running under it" — and
shard-local uniqueness means the shard that could not be read may hold another
live run the cancellation never touched. Recorded, it durably closes the request
and the outbox never looks again.

The asymmetry in the fix follows from idempotence, not taste. Cancelling is
idempotent, so the cancel path still **acts** on the run it found — withholding
that would leave a live run running through an unrelated shard's outage — and
withholds only the terminal event, leaving the row pending until a fan-out that
inspected every shard can make the assertion. It converges: the run just
cancelled is terminal, so the next complete sweep resolves it and reports.
Signal delivery is *not* idempotent without an idempotency key, so "deliver but
stay pending" would deliver the signal twice on the retry; the signal path
therefore delivers and records as before. `TargetLocation::Found` now carries the
`uninspected` list so the two paths can decide differently from the same answer.

The second P1: `FANOUT_ACQUIRE_BOUND` bounded only the *checkout*. A connection
handed over by a peer that then never answers — the database becomes a network
black hole after checkout, or an `ACCESS EXCLUSIVE` DDL lock blocks the read — is
just as fatal, and wedges the caller shard's timeout checker, which also runs
task timeouts, SLA enforcement, session reclaim and every other outbox. The whole
peer probe is now inside `FANOUT_PEER_BOUND`, with the tighter acquisition bound
nested inside it: two bounds because they answer different questions — the inner
one stops scanners waiting on each other's connections, the outer one caps what a
single unhealthy peer can cost — and expiry is classified as an uninspected
shard, never as absence.

**Codex round 3 (one P1, one P2).** Round 2 stopped a cancel reporting over an
*incomplete* fan-out. It did not stop one reporting over a **complete** fan-out
that found two live runs — which shard-local uniqueness makes reachable exactly
here: pin a key to one shard (#697) while an unpinned start of the same key
hashes to another, and no single shard's partial unique index can see both. The
ranking picks the most recently started as the current run, which is right for a
signal; cancelling only that one and reporting success leaves the older run live,
and the instant the newer is cancelled that older one *is* the current run for
the key.

Both cases are now one predicate. `TargetLocation::Found` carries `other_live`
alongside `uninspected`, and `is_authoritative_for_key()` is false when either is
non-empty — a shard that **may** hold a live run and a shard that **does** are
the same problem, and folding them together means the next variant of it cannot
be handled in only one place. The route field is renamed from `fanout_complete`
to `may_assert_key_state` to say what it actually governs. The cancel converges
either way: one live copy per sweep until an unambiguous answer can make the
claim. Terminal siblings deliberately do not block it — they can never become the
current run, and treating them as ambiguity would stall every cancel of a key
that has continued-as-new across shards.

The P2 was a gap the round-2 restructure introduced: only acquisition failures
memoized, so a peer whose *query* failed just under the probe bound was re-probed
once per pending row instead of once per sweep, turning a backlog into
`rows x failure duration`. Every uninspected outcome now funnels through one
`mark_uninspected`, and the peer probe is extracted into `probe_peer_shard`
returning a single `Err(reason)` — so a caller cannot forget to memoize one of
them, which removes the class rather than the instance.

**Codex round 4 (two P1s, neither fixable here — and that is the finding).**
Both are real, both are limits rather than defects, and both were being
*overclaimed* by this change's own comments, which is what actually needed
fixing.

The first: the fan-out reads shards sequentially with no shared snapshot, so a
run of the key starting on an already-read shard while a later shard is being
read is invisible to that pass — and a cancel can then report success while it is
live. `expected_live` does not catch it, correctly: the selected run is still
live, so the shard-local re-read has nothing to disagree with. Closing it means
cross-shard key uniqueness, a coordination primitive the sharding design has so
far declined to add; it is issue #1313, with options. What is fixed here is the
overclaim: `is_authoritative_for_key()` now documents that it is authoritative
over *the observations the fan-out made*, not over an instant, and
`docs/sharding.md` says the same. Two things bound it — it needs two live runs of
one key, which needs a deployment to mix pinned and unpinned starts of the same
`workflow_id` against the documented discipline, and it is strictly better than
the pre-#1146 behaviour, which missed a second live run unconditionally rather
than only under a race.

The second corrected a claim in round 1's own comment. `FANOUT_ACQUIRE_BOUND`
guarantees **bounded return, not progress**: with one connection per pool, a peer
read succeeds only if it lands during that peer scanner's sleep window. That is
likely — passes are short relative to the poll interval, and two independent
tasks doing different work do not stay in phase — but it is a probability, and
the bound does nothing to create it, which round 1's "the next tick, whose phase
has drifted" wrongly implied. The guarantee is capacity, so
`Worker::run_multi_shard` now **warns at startup**, naming the shard, when a
process polling several shards has a pool below two connections: one for that
shard's own scanner, one for a peer's read. That turns an invisible degradation
into a visible misconfiguration, which is the most a library can do about a
deployment's connection budget.

**Codex round 5 (one P1).** Several logical shard ids backed by clones of one
physical pool is a supported topology — a pre-split staging deployment, and what
`ShardedDbPool::from_map` produces whenever the same `DbPool` is inserted under
more than one id. The fan-out's "don't re-enter the held pool" check matched only
the caller's *numeric* shard, so the alias was probed by re-acquiring the very
pool that supplied the held connection. On a one-connection pool that cannot
succeed until the sweep returns, so the alias was uninspected on every sweep, the
answer was never authoritative, and a by-id cancel cancelled the live run and
withheld its terminal **forever** — the round-2/3 withholding rule turned into a
permanent stall by a topology neither round considered.

The fan-out now dedupes by *underlying pool identity* rather than shard id.
`deadpool`'s `Pool` is a handle over an `Arc<PoolInner>` and `Pool::manager()`
borrows out of that shared allocation, so two clones return the same address and
two independently built pools cannot; comparing the handles would not work, since
each shard's pool sits in its own map slot and `std::ptr::eq` on `&DbPool` is
really shard-id equality again. An alias is treated as **inspected** — its
database was read through its sibling, and the resolver filters on
`(workflow_name, workflow_id)` with no shard predicate, so it would return the
same rows — rather than uninspected, which is what made the stall permanent.

**Codex round 6 (one P1, on the round-5 fix).** That fix seeded the
already-probed set *inside* the "this is the held shard" branch, but the fan-out
visits shards in ascending order — so the seeding is too late whenever an alias
sorts before the held shard. A checker holding shard 1 of an aliased {0, 1} pair
still tried to acquire "shard 0", the pool it is itself holding, timed out, and
marked it uninspected on every sweep: exactly the permanent withholding round 5
set out to remove, reachable from the other side. The round-5 test pinned the
caller to shard 0, the one ordering where seeding late happens to work, so it
could not see this.

The seed now happens once, before the loop, so iteration order cannot decide
correctness. `outbox_by_id_resolves_when_the_caller_holds_the_higher_aliased_shard`
is the round-5 test with the caller on the other side of the ordering.

**Deliberate non-fix: an unreachable shard stalls by-id delivery without a
bound.** `target_unknown` goes into an append-only history and cannot be taken
back, so it is recorded only from a *complete* fan-out. The consequence is that a
shard which is permanently uninspectable in a process — a router naming a shard
no pool was configured for — leaves every affected by-id request pending forever,
and a workflow awaiting the outcome waits with it. Previously that situation
resolved, sometimes wrongly. There is no metric for it yet; the signal is the
per-row `by-id target resolution inconclusive` warning naming the shard, and
`docs/sharding.md` and the backup-restore runbook both say so plainly.

**Test evidence, TDD red → green → refactor.** The three end-to-end outbox tests
were written first and observed failing against the pre-fix engine with exactly
the issue's symptom (`ExternalSignalFailed` / `ExternalCancelFailed` with
`target_unknown` against a running target). 12 no-database unit tests in
`external_target_location` cover the merge rules, the expected-shard union and
the inline gate. `tests/integration/shard_placement_by_id_tests.rs` adds **16
DB-backed tests against two genuinely separate Postgres databases** — a single
database mocked as two shards cannot distinguish "found by fanning out" from
"found because both shards are the same table" — and deliberately does not
duplicate the pure rules the unit module already pins:

- a target pinned off its hash shard is signalled, and cancelled;
- a run left behind by a `writable_shards` change is still found;
- the live run wins over a stale terminal both at the resolver level and
  end-to-end through the outbox, with the dead run asserted to receive nothing;
- signal and cancel keep their opposite #751 semantics against a terminal run
  found on *another* shard (`not_running` failure vs. no-op success);
- an un-poolable shard yields no `target_unknown` on either the signal or the
  cancel outbox, even with the grace window fully expired, and the same pending
  row is then delivered on the next sweep once the shard returns — liveness, not
  just the absence of a wrong answer;
- a complete fan-out that finds nothing still fails `target_unknown`, so the fix
  does not make that outcome unreachable;
- the sweep completes against pools holding **exactly one connection**, the
  production shape that would have deadlocked before the connection fixes above;
- and a real `Worker` drives the inline gate end-to-end: a caller whose own shard
  holds a stale `COMPLETED` run of the key, while the live run is on another
  shard, must reach the live one. Verified red on revert — flipping the gate back
  to "always inline" reproduces the permanent `not_running`.

Every DB test restores the process-global router and pool on drop, so the shared
`integration` binary's later files do not inherit a two-shard topology pointing
at dropped databases. The suite is registered in `.github/ci/integration-suites.txt`
(without which the repo's own `ci_run_coverage` guard fails and the file would
compile but never run). The issue #751 suite (26 tests) and the cross-workflow /
cross-shard / sharding suites pass unchanged.

**Documented in** `docs/sharding.md` (new *Business-key addressing finds a
pinned run wherever it is* section under issue #697), `docs/security-posture.md`
(the #697/#751 interaction bullet, rewritten from a limitation to the resolved
behaviour plus its read-surface and outage posture), and the
`WorkflowContext::*_by_id` doc comments, whose "Known limitation — explicit
shard placement" sections are replaced by the resolution contract.
