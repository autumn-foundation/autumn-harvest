## Phase 5.x — shard rebalancing: migrating quiescent workflows across shards (issue #964)

The sharding contract used to end at "cross-shard rebalancing of existing
workflows is out of scope", and the consequences compounded: adding a shard only
helped **new** starts, so a hot shard stayed hot for as long as its residents
lived — forever, for a continue-as-new entity workflow — and a shard could never
be decommissioned. Operators had exactly one lever (add shards, hope the hash
dilutes new load) and it did nothing for the imbalance they already had.

This ships the operator-initiated data-movement primitive: **copy →
replay-verify → one atomic cutover → sealed source**, scoped to what the replay
engine can prove.

**Quiescence is the design, not a limitation.** Restricting to parked executions
converts a live distributed-migration problem into a copy-verify-cutover of an
*inert, append-only event log* — exactly the artifact `WorkflowReplayer` exists
to verify. That collapse is what deletes the catch-up phase every online shard
move in the literature needs, and with it the dual-write and the two-phase
commit. The predicate is a pure function over an explicitly-gathered observation
(`assess_quiescence`), with one named blocker per fact so a dry run explains
itself completely. It distinguishes a *parked* workflow task — `PENDING` in the
future for a timer park, `RUNNING` with no worker for a signal park — from a
claimed or dispatchable one, so the long-lived population the feature exists to
move is **eligible**, and its task row is copied rather than treated as a
blocker.

Two blockers in it are worth naming because they are the ones a quiescence
predicate written from the issue text alone would miss. A run **queued for a
durable mutex** (`ctx.mutex`, issue #691) looks perfectly parked — nothing
claimed, nothing in flight — but its grant is delivered by waking it *on this
shard*, so migrating it would deliver the grant to a sealed row: a lost wake, the
exact failure the bar exists to prevent. And `live_children` counts `PAUSED`
children, not just `RUNNING` ones, because a paused child still completes
eventually and appends its terminal to the shard its parent used to be on.

**The `ExecutionId` never changes.** That is the identity decision (AC4), and it
makes the acceptance bar structural rather than enumerated: any id captured
before a migration is the same 16 bytes after it, so a parent's recorded
`ChildWorkflowStarted.child_id`, a stored handle, an external signal or cancel
target, a webhook reference and a schedule's carryover lineage all keep
resolving with no rewrite pass and no alias table. The encoded shard stops
meaning "where this run lives" and starts meaning "where it originated" — its
routing entry point. Resolution is two-level: the sealed source row carries a
forwarding pointer (`MIGRATED` + `migrated_to_shard`), and
`ShardRouter::with_shard_forwards` maps a *removed* shard's ids straight to its
successor for the decommission case. Chains are followed to a bounded depth and
then fail closed rather than loop; a completed migration best-effort collapses
the chain so hops do not accumulate.

**Sealed, never deleted — and deliberately *not* released from the uniqueness
index.** `MIGRATED` reuses the reset path's `TERMINATED` sealing, and rejects the
half of that precedent the issue suggests copying. A reset forks a successor on
the *same* shard, so its source must release `(workflow_name, workflow_id)`; a
migration writes its copy to a *different database* whose index is its own, so
releasing would let a later start of the same business key — which still hashes
back to the source shard — create a **second live run** alongside the migrated
one. `MIGRATED` stays inside
`harvest_we_workflow_name_workflow_id_active_key`, such a start fails closed,
and `is_active_conflict_state` is widened so a start that does reach the sealed
row treats it as the live prior it is. A schema `CHECK` makes "`MIGRATED`
without a pointer" — an id that resolves nowhere — unrepresentable; it is
one-directional, so an operator's `terminate` force-write is still the
idempotent no-op the contract promises rather than a constraint violation.

**Never lost, never doubled — and never silently rewound.** The cutover
re-evaluates the *whole* quiescence predicate inside its own `UPDATE ... WHERE`,
so a wake arriving at any point since the candidate scan makes it match zero
rows: the migration aborts and the untouched source processes the wake normally.
Quiescence alone is not sufficient, though, and the gap is subtle: a run that
wakes, executes a full decision cycle and re-parks is quiescent *again*, so every
predicate passes while the verified copy is now missing everything that cycle
did. Verification therefore records the source history's high-water mark and the
cutover seals only while the source still matches it — lost progress being a
strictly worse failure than a lost wake, because nothing afterwards reveals it. Past the cutover a wake resolves
through the seal to the target, and activation notices the pending signal and
schedules the restored task at `NOW()` rather than leaving it on a timer days
out. Signals are copied *with* their `idempotency_key`, so a keyed redelivery
collides on the target exactly as it would have on the source.

**Erasure and retention were part of the change, not left to a follow-up.** A
sealed source keeps every payload it had, so a GDPR erasure routed by
`ExecutionId` would follow the forwarding pointer, tombstone the target, report
success, and leave the source's plaintext intact forever. Two things were needed
and both ship here. `MIGRATED` joins `erase::TERMINAL_STATES` so the sealed copy
is erasable at all (terminal for *classification*: the retention janitor's own
candidate lists still exclude it, because hard-deleting the row would destroy the
pointer every pre-migration id resolves through). And the erasure entry point
became **cross-residence**: `erase_workflow_payloads_all_residences` walks the
execution's whole residence chain — every shard that still holds a copy of its
bytes, not just the one its id currently routes to — and scrubs each, reporting
the extra ones in the response's `prior_residences`. That chain is a durable
`migrated_from_shards` array on the live row rather than a backwards walk of the
forwarding pointers, because the pointers are deliberately *collapsed*: after
A → B → C, A points straight at C and B has vanished from the pointer graph while
B's sealed copy still holds the subject's data. Routing wants the shortest path
and erasure wants the complete set; they are different questions and now have
different mechanisms. The live residence is
erased first and its result is the answer, because it is the only copy whose
state can honour the terminal-state and legal-hold gates; a sealed source always
reads as terminal, so gating on one would let a live run be erased through its
own stale shadow. A source copy retention has already collected contributes
nothing and is not an error, but a residence this node cannot reach fails the
call outright — an erasure that cannot be shown to be complete must not be
reported as complete. Workflow logs are copied rather than stranded, and the
migration record clears its cached task payload on settle.

**Every id-routed write follows the pointer, not the id.** The forwarding
resolution is not just for reads. The external-signal, external-cancel and
external-await outboxes, the plugin's exact-shard connection resolver and the
batch executor's per-target dispatch all resolve the *current* residence before
choosing a pool. Batch is the sharpest case: its all-shard scan discovers a
rebalanced run on the live target copy, but the id it hands back still encodes
the origin, so an origin-only pool lookup would send the cancel or terminate into
the sealed source — sealing the wrong copy while the live one kept running. The
update-result poll is the same shape with a different symptom: an update admitted
on the live target while the poll read the sealed source's frozen history would
answer `504` for an update that in fact completed.

**Crash-safe by construction.** `harvest_shard_migrations` lives on the *source*
shard — the database that stays authoritative right up to the cutover — and
`next_migration_action` is a pure function from its phase to the one correct
next step, so every kill point resumes deterministically. Only
`VERIFIED → COMMITTED` changes who is authoritative, and it is a single statement
on one database. The honest gap is documented rather than hidden: between that
commit and the target's activation the run is claimable on *neither* shard —
liveness, not correctness, and closed by the idempotent resume sweep.

**Zero new `WorkflowEvent` variants, and no new `harvest_events` mutator.** The
copy `INSERT`s new rows on a *different* database and never rewrites a stored
row, so this is not a fourth exception to the append-only invariant in
`CLAUDE.md` — it is an instance of it. Verification proves it: the raw stored
event tuples must be byte-identical, which is only satisfiable if nothing was
appended, reordered or rewritten.

**The copy carries columns nobody remembered to list.** It is deliberately
column-list-free — `to_jsonb` on the source, `jsonb_populate_record` on the
target — so a column added to `harvest_workflow_executions` next year is carried
automatically instead of being silently dropped by a stale hand-maintained list.
The converse hazard (a target behind on migrations, where
`jsonb_populate_record` would discard the unknown key *silently*) is refused up
front by a schema-parity check.

**Operator surface.** `harvest shard rebalance --from A --to B --limit N
[--dry-run]` and `harvest shard rebalance-resume`, connecting to the shard
databases directly — a rebalance is inherently two-database — with per-execution
progress, a `shard.rebalance.migrate` audit row per attempt, and a full
decommission runbook (`docs/runbooks/shard-decommission.md`). The dry run walks
the same code path as the real run up to the first write, so an operator running
it during an incident can trust the two agree.

**Scope cuts, stated rather than discovered.** Only *root*, `RUNNING` executions
migrate: a child's terminal appends to its parent's history in a shard-local
transaction, so moving a child would break that edge, and `PAUSED` is a state the
copy and cutover would have to carry through activation for a case an operator
resolves with one resume. In-flight activity work, held or queued durable
mutexes, and executions carrying dead-letter rows are all out of scope by design
— each with a named blocker, so a dry run says which. Migration is
operator-initiated; an auto-balancer can compose on top.

Migration `20260902131705_harvest_shard_rebalancing`. See `docs/sharding.md`
§ *Shard rebalancing* for the contract and
`docs/plans/2026-09-02-shard-rebalancing.md` for the design note, including the
eight mechanisms considered and why seven were rejected.
