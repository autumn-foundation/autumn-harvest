# Shard rebalancing — migrating quiescent workflows across shards (issue #964)

**Status**: design note, written *before* implementation as issue #964's AC4
requires ("Whichever mechanism is chosen must be written up as a design note
before implementation").

**Scope**: an operator-initiated primitive that moves a **quiescent** workflow
execution from shard A to shard B: copy → replay-verify → single atomic cutover
→ sealed source with a forwarding reference. Automatic rebalancing policy,
in-flight migration, and cross-shard transactions stay out of scope.

---

## 1. Brainstorming — the mechanisms that were on the table

Eight candidate shapes were enumerated before any was scored.

| # | Mechanism | One-line summary |
|---|---|---|
| B1 | **Re-mint the id** | Allocate a fresh target-shard `ExecutionId`, rewrite every recorded reference (parents' `ChildWorkflowStarted.child_id`, handles, external signal/cancel targets, webhooks, schedule lineage). |
| B2 | **Old→new alias table** | Keep B1's fresh id but add a durable `old_id → new_id` alias consulted on every id-routed lookup. |
| B3 | **Id-preserving routing indirection** | The `ExecutionId` never changes. The bytes stay the *origin* shard; a sealed source row carries a forwarding pointer, so every captured id keeps resolving. |
| B4 | **Global directory service** | A separate, replicated `exec_id → shard` directory in front of all routing. |
| B5 | **Logical shard layer** | Insert logical shards between the hash and the physical database (Temporal's model); rebalancing moves logical→physical bindings. |
| B6 | **Physical replication cutover** | Stream the whole shard database to a new host (`pg_basebackup` / logical replication) and swap DSNs. |
| B7 | **Drain-by-continue-as-new** | Ask the workflow to `continue_as_new` on the target shard; the successor is a fresh run on B. |
| B8 | **Dual-write / catch-up** | Citus/Vitess-style copy + change-capture catch-up + cutover, with no quiescence requirement. |

### Scoring against the ACs

- **B1** fails AC4's bar outright as stated ("any id captured before migration
  continues to resolve after migration"): an `ExecutionId` a caller wrote into
  *their* database three months ago cannot be rewritten by us, so B1 needs B2
  bolted on regardless — at which point the fresh id buys nothing and costs a
  rewrite pass over every id-holding surface.
- **B2** works but pays B1's rewrite *and* the alias lookup. Strictly dominated
  by B3.
- **B4** is a new distributed component with its own availability and
  consistency story. It is exactly the coordination service `docs/sharding.md`
  already declares out of scope for cross-shard limits ("conflicts with
  Harvest's goal of being a Postgres-native engine").
- **B5** is the right long-run architecture and the wrong change to make here:
  every `ExecutionId` in every deployment already encodes a *physical* shard, so
  adopting logical shards is a fleet-wide id-semantics migration, not a feature.
- **B6** moves a whole shard, never a workflow. It solves "this database's disk
  is too small", not "this shard is hot" — the issue's actual problem.
- **B7** is not a migration: it produces a *different run* with a different
  `ExecutionId`, a truncated history and a fresh `run_id`. It breaks audit,
  breaks `first_exec_id` lineage, requires workflow-author cooperation, and
  cannot move a signal-parked entity workflow at all.
- **B8**'s catch-up phase is precisely what quiescence removes. The issue says
  so ("minus the catch-up phase (quiescence removes it)"), and a catch-up phase
  over an append-only log guarded by a replay engine buys nothing.

**Chosen: B3.**

---

## 2. The identity decision (AC4) — id-preserving routing indirection

> The acceptance bar is: **any id captured before migration continues to resolve
> after migration** (reads, signals, cancels, result waits), verified by tests
> for each holder class.

The `ExecutionId` of a migrated run **does not change**. This is the single
decision from which everything else follows, and it is what makes the bar
*structurally* true rather than true-by-enumeration: nothing that holds an id
needs to learn anything, because no id changed.

The first two bytes therefore stop meaning "the shard this run lives on" and
start meaning "**the shard this run originated on**" — its *routing entry
point*, not its residence. Resolution is two-level:

### Level 1 — origin-shard forwarding (authoritative, always correct)

On cutover the source execution row is sealed into the new terminal-shaped state
`MIGRATED`, carrying `migrated_to_shard` and `migrated_at`. Any id-routed
operation lands on the origin shard exactly as it always did, finds the sealed
row, and follows the pointer to the target.

This is the AC3 seal. It reuses the reset path's precedent for the *sealing*
and deliberately **rejects** it for the uniqueness index, which is worth
spelling out because the issue explicitly suggests copying it ("the reset path's
`TERMINATED` sealing, which already releases the uniqueness index").

A reset forks a successor on the **same shard**, so its source *must* release
`(workflow_name, workflow_id)` or the successor could not be inserted. A
migration writes its copy to a **different database**, whose
`harvest_we_workflow_name_workflow_id_active_key` is its own — so nothing needs
releasing, and releasing would be a correctness bug: a later start of the same
business key still hashes back to the source shard, would find no active row,
and would create a **second live run** alongside the migrated one. `MIGRATED`
therefore stays *inside* the index, and such a start fails closed on it.

`execution::is_active_conflict_state` is widened to match, so a start that does
reach the sealed row treats it as the live prior it is rather than as a terminal
one. The row is never deleted, so the source shard also keeps a complete audit
trail of the run up to the instant it moved.

**Chain collapse.** A run migrated A→B and later B→C leaves A pointing at B and
B pointing at C. Resolution follows the chain (bounded by
`MAX_FORWARD_HOPS = 4`, above which it fails closed with a typed error rather
than looping). The B→C cutover *also* best-effort rewrites A's pointer straight
to C, after its own commit and outside any transaction: correctness comes from
chain-following, performance from the collapse. A failed collapse is a
latency regression, never a correctness one.

### Level 2 — router-declared shard forwards (decommission)

Level 1 needs the origin shard to still be readable. A fully decommissioned
shard is not. `ShardRouter::with_shard_forwards([(retired, successor)])` lets an
operator declare, once the drain is complete, that ids encoding the retired
shard resolve directly to its successor — no hop, no origin database.

The honest limitation, stated here and in the runbook rather than discovered in
production: a *single* declared successor per retired shard means a shard whose
residents were split across several targets cannot be fully removed. Such a
shard stays `readable` as a **forwarding tombstone** — after migration its
content is a few kilobytes of sealed rows, and the operator keeps a tiny
database rather than losing id resolution. `harvest shard rebalance` therefore
defaults to a single `--to` target, which is what makes the common decommission
case cleanly removable.

### Why the shard bytes are not re-derived

`ShardedDbPool::pool_for_execution` keeps decoding the id and keeps hitting the
origin shard first. That is deliberate: it is correct for 100 % of
never-migrated executions (every execution in every deployment that never runs a
rebalance), so the hot path pays literally nothing. The forwarding lookup is
reached only when the origin row says the run has moved.

---

## 3. Reverse brainstorming — how would we make this catastrophically wrong?

Deliberately inverting the goal: *how do we lose or duplicate a workflow?*
Each attack is listed with the specific structural defence, and each defence
names the test that proves it.

| # | How to break it | Structural defence |
|---|---|---|
| R1 | **Run claimable on both shards.** Activate the target before sealing the source. | Ordering is fixed the other way: the source seal **is** the cutover commit, and the target copy is inert (`MIGRATING`) until after it. There is no interleaving in which both are claimable. `cutover_makes_the_source_non_claimable_and_the_target_authoritative`. |
| R2 | **Lost wake.** A signal arrives during the copy and is left on the source. | Cutover re-checks the *full* quiescence predicate inside its own `UPDATE ... WHERE`. A signal that landed mid-copy fails that predicate, the migration aborts, and the source — untouched — processes the signal normally. `a_signal_arriving_mid_migration_aborts_the_cutover_and_is_not_lost`. |
| R2b | **Lost wake, subtler.** The re-check reads a stale snapshot: under `READ COMMITTED` a single statement evaluates its cross-table `EXISTS` subqueries against the statement snapshot, and Postgres re-checks a qual only when the *target tuple* was updated. `signal::send_signal_idempotent` takes `SELECT … FOR UPDATE` on the execution row — a **lock, not an update** — so a signal committing in the gap would be invisible, and the cutover would seal a just-woken run and cancel the task the wake had re-pended. | The cutover runs inside an explicit transaction that takes the **same** `FOR UPDATE` lock on the execution row before the re-check, serialising it against the signal path. |
| R3 | **Lost wake, part two.** A signal arrives *after* cutover and is written to the sealed source. | Every write path resolves through the forwarding pointer first; a `MIGRATED` source is not a signal target. The signal is written to the target, where the copy consumes it after activation. `a_signal_arriving_after_cutover_is_delivered_to_the_target`. |
| R4 | **Doubled wake.** The same signal is delivered on both shards. | Signals are copied *with* their `idempotency_key`, and the target carries the same partial unique index. A redelivery of a keyed signal collides exactly as it would have on the source. `signal_idempotency_keys_survive_the_copy`. |
| R5 | **Doubled timer fire.** The copied timer fires on the target while the source's still exists. | A timer only fires through a *claimed workflow task*. The source's parked task row is not copied while the source is authoritative, and the sealed `MIGRATED` source is never claimable, so its timers are inert forever. |
| R6 | **Silent history corruption.** The copy is subtly lossy (a re-encoded payload, a reordered event). | Rows are copied verbatim, and the target copy is replay-verified against the source **before** cutover: identical decoded event vectors *and* identical `HistoryMatcher` next-command state. A mismatch aborts with the source untouched. `verification_rejects_a_tampered_copy`. |
| R7 | **Half-migration wedge.** A crash between the seal and the target activation strands the run. | The `harvest_shard_migrations` row on the source is durable and drives an idempotent resume sweep. `resume_incomplete_migrations` re-runs whichever phase is outstanding. Kill-point tests cover every phase boundary. |
| R8 | **Broken parent/child edge.** A child is moved away from the parent whose history its terminal must append to. | The quiescence predicate refuses any execution with a non-NULL `parent_id`, and any execution with live children (same-shard or cross-shard). Roots only, by design and by test. |
| R9 | **Uniqueness violation.** The copy collides with a live run of the same `(workflow_name, workflow_id)` on the target. | The target insert takes the active-uniqueness slot in the copy transaction; a collision fails the copy, which aborts before cutover with the source untouched. |
| R9b | **Duplicate live run.** The sealed source releases its business-key slot, a later start of the same `workflow_id` hashes back to the source shard, finds nothing active, and starts a second run alongside the migrated one. | The seal does **not** release the slot (§2). `the_sealed_source_keeps_the_business_key_slot_so_no_duplicate_can_start`. |
| R13 | **Abort deletes the live copy.** Two operators drive the same migration (the runbook tells them to run the resume sweep after an interruption). The loser's `commit_cutover` answers `false` — because the source is *already sealed*, indistinguishable at that call site from "the run woke" — and it then discards the target copy of a run whose source now forwards to it. The run exists nowhere. | `abort_migration` reads the durable phase **first** and refuses outright past the cutover. |
| R14 | **Wrong-shard delivery.** The engine's own external-signal / external-cancel outboxes route by decoding the raw shard bytes, land on the sealed source, read it as terminal, and record `ExternalSignalFailed{target_terminal}` in the *sender's* history for a workflow that is alive. | Both outbox blocks resolve through `resolve_target_shard` before choosing a pool, and `signal.rs` classifies `MIGRATED`/`MIGRATING` as the retryable `ShardUnavailable` rather than "terminal". |
| R15 | **Stranded mutex.** A run parked as a durable-mutex *waiter* looks perfectly quiescent — nothing claimed, nothing in flight — but its grant is delivered by waking it **on this shard**. Migrated, the grant lands on a sealed row. | `HoldsMutexLock` and `QueuedForMutex` are quiescence blockers. |
| R16 | **Broken parent edge, via pause.** A parent is migrated away from a *paused* child, because the live-children count only looked for `RUNNING`. The child resumes, completes, and appends its terminal to the sealed source. | `live_children` counts `RUNNING`, `PAUSED`, `MIGRATING` and `MIGRATED`. |
| R17 | **Unerasable copy.** The sealed source keeps every payload, and `erase.rs` refuses it as non-terminal — so a GDPR erasure routed by id follows the forward, tombstones the target, reports success, and leaves the source's plaintext intact forever. | `MIGRATED` joins `erase::TERMINAL_STATES` (terminal for *classification*; retention's own candidate lists still exclude it, because purging the row would destroy the pointer). Workflow logs are copied for the same reason. |
| R10 | **Deleting the source to "save space".** | `MIGRATED` is a seal, never a delete. Retention treats it as terminal-shaped for indexing but the forwarding pointer is what the id resolution needs, so the runbook forbids purging it while any pre-migration id may still be held. |
| R11 | **A new `WorkflowEvent` variant sneaks in.** | Zero events are appended by any migration path. A test asserts the copied history is byte-identical, which is only possible if nothing was appended. |
| R12 | **`harvest_events` gets mutated**, adding a fourth exception to the CLAUDE.md invariant. | The copy is an `INSERT ... SELECT` on the *target*; no `UPDATE` of `event_data` anywhere. The source's rows are read-only throughout. |

R12 is worth stating loudly because `CLAUDE.md` enumerates exactly two
sanctioned `harvest_events` writers. **Shard rebalancing adds none.** It inserts
new rows on a different database and never rewrites a stored row, so it is not
an exception to the append-only invariant — it is an instance of it.

---

## 4. Six hats

### ⬜ White — the facts the design must respect

- A shard is a **separate Postgres database**. No cross-shard transaction
  exists, and none may be introduced (issue #964, *Out of Scope*).
- `ExecutionId` encodes the shard in bytes 0–1 (`types.rs`), and
  `ShardedDbPool::pool_for_execution` routes off that with no directory.
- A **timer-parked** workflow keeps its workflow task row in `harvest_task_queue`
  in state `RUNNING` with `worker_id IS NULL` and `started_at IS NULL` — the
  *parked* shape (`queue::park_workflow_task`) — or `PENDING` with a future
  `scheduled_at`. "No task rows" is therefore the **wrong** predicate; the right
  one distinguishes parked from claimed.
- `harvest_signals` rows carry `consumed`; `harvest_timers` rows carry `fired`.
- The reset path already seals a source row `TERMINATED` and the active
  uniqueness index already excludes it (migration `20260503000000`).
- `CLAUDE.md` sanctions exactly two `harvest_events` mutators; a third would be
  a design escalation.

### 🟥 Red — what feels dangerous

Losing a customer's workflow is the worst thing this codebase can do, and this
feature moves workflows between databases for a living. The instinct is that
"quiescent" will turn out to be subtler than it looks — and it already did: the
first draft of the predicate said "no task rows", which would have refused to
migrate *every* timer-parked workflow, i.e. exactly the population the issue
exists to move. The gut feeling that the predicate is where the bugs live is
what drove it into a pure, exhaustively-tested function taking an explicit
observation struct rather than a SQL `WHERE` clause nobody can unit-test.

The second uncomfortable feeling: an operator will run this at 3 a.m. during an
incident. The dry-run must therefore be the *same* code path as the real thing
up to the cutover, not a separate estimator that can drift.

### ⬛ Black — what will go wrong

- **The gap between cutover and activation.** Between the source seal and the
  target's `MIGRATING → RUNNING` flip, the run is claimable on *neither* shard.
  This is real and it is not hidden: the contract is "authoritative on exactly
  one shard at every instant", and claimability follows within one resume sweep.
  A two-phase commit would close it and is explicitly out of scope.
- **Shard-local dedupe state is left behind** (AC6). Debounce rows, start-throttle
  rows, rate-limit buckets, concurrency-key accounting and the completion-trigger
  fire ledger are all shard-local and keyed by things other than the execution.
  They are **not** migrated; the accepted at-least-once window is documented in
  `docs/sharding.md` per mechanism.
- **The single-successor forwarding limit** for decommission (§2, Level 2).
- **Scale.** A migration is per-execution and copies a whole history. A 1,000-run
  batch is bounded by `--limit` and reported per run; a run with a huge history
  is a slow copy, not a wedge.
- **Retention interaction.** A retention janitor that hard-deletes the sealed
  source destroys id resolution. The runbook says so; the migrated source is not
  terminal for the purposes of "safe to purge".

### 🟨 Yellow — why this is worth it anyway

Every load-bearing piece already exists and is already trusted in CI. The seal
is the reset path's. The replay verification is `HistoryMatcher`, the same
machinery the whole engine is built on. The forwarding pointer is a column. The
batch driver is the shape `codec_rotation`'s sweep already established. The
result is a genuine data-movement story — the thing DBOS, Inngest and Hatchet
have no answer for — bought almost entirely with primitives that shipped
already, and with **zero** new `WorkflowEvent` variants.

### 🟩 Green — what quiescence unlocks

Restricting to quiescent runs is not a limitation reluctantly accepted; it is
the creative move. It converts a live distributed-migration problem into a
copy-verify-cutover of an **inert, append-only log** — which is exactly the
artifact the replay engine exists to verify. That collapse is what deletes the
catch-up phase, deletes the dual-write, deletes the 2PC, and makes the whole
thing provable with tests instead of hopeful.

### 🟦 Blue — how the work is sequenced

1. This design note (AC4's gate).
2. **Red**: pure-logic tests for `assess_quiescence` and `next_migration_action`.
3. **Green**: the module, the SQL migration, the routing forwards.
4. **Red/Green**: Postgres-backed tests — copy, verify, cutover, kill-points,
   wake redelivery, every id-holder class.
5. Operator surface (CLI, dry-run, progress, audit) and the decommission runbook.
6. **Refactor**: multi-angle review, then the AC evidence table.

---

## 5. The quiescence predicate

Expressed as a pure function over an explicitly-gathered observation, so every
branch is unit-testable without a database:

```rust
pub fn assess_quiescence(obs: &QuiescenceObservation) -> Quiescence
```

An execution is **migratable** when *all* of the following hold. Each is a named
`QuiescenceBlocker` variant when it does not, so a dry-run explains itself.

| Fact | Requirement | Why |
|---|---|---|
| `state` | `RUNNING` | Terminal runs have nothing to move; `MIGRATING`/`MIGRATED` are already in flight. |
| `state` (again) | not `PAUSED` | `PAUSED` is a *state*, not a column — an early draft of this note had that backwards. The copy and the cutover both key on `RUNNING`, and supporting pause would mean carrying the original state through activation. Out of scope; the runbook's remedy is resume → migrate → re-pause. |
| `parent_id` | `NULL` | A child's terminal appends to its parent's history in a shard-local transaction (R8). |
| claimed workflow tasks | 0 | `state='RUNNING' AND worker_id IS NOT NULL` — a worker is mid-cycle. |
| due pending tasks | 0 | `state='PENDING' AND scheduled_at <= now()` — a wake is dispatchable right now. |
| parked/future workflow tasks | ≤ 1, and `wake_requested = false` | This is the migratable shape. It is **copied**, not refused. A set `wake_requested` means a wake raced us. |
| non-terminal activity tasks | 0 | In-flight activity work is out of scope by design. |
| unconsumed signals | 0 | An unprocessed staged signal is undelivered work. |
| completion deliveries in `PENDING` or `INFLIGHT` | 0 | An outbound delivery that has not settled. `PENDING` counts: the row is live, and it is not copied. |
| ACTIVE sessions | 0 | A session's state lives on one worker. |
| non-terminal external tasks | 0 | Same reason as activities. |
| live children (same-shard) | 0 | R8. |
| cross-shard child rows | 0 | R8; the relay row lives on the parent's shard. |
| `nd_blocked_at` | `NULL` | A non-determinism-blocked run has a pending re-dispatch backoff. |
| held durable mutex locks | 0 | The lock row is shard-local and keyed by the holder; moving the holder strands every waiter (R15). |
| queued mutex waiters | 0 | The grant is delivered by waking the waiter on *this* shard (R15). |
| dead-letter rows | 0 | A redrive enqueues a task here, which after a migration targets a sealed row. |

The predicate is evaluated **twice**: once to select candidates, and again
inside the cutover statement's `WHERE` clause, against the same facts. The
second evaluation is what makes R2 safe.

## 6. Phase machine

`harvest_shard_migrations` lives on the **source** shard — the shard that stays
authoritative until cutover — and is the durable record the resume sweep drives.

```
PENDING ──stage_copy──▶ COPIED ──verify──▶ VERIFIED ──cutover──▶ COMMITTED ──activate──▶ DONE
   │                      │                    │                                            
   └──────────────────────┴────────────────────┴──────────▶ ABORTED (source untouched)
```

`next_migration_action(&MigrationObservation) -> MigrationAction` is pure, so
every kill-point resumes deterministically. `resume_incomplete_migrations`
applies it **repeatedly per row, to a settled phase** rather than one step per
call: a one-step sweep would leave a run crashed between the cutover and its
activation claimable on neither shard until an operator happened to run the
sweep again, which is exactly the window least likely to be noticed.

| Phase at crash | Resume action | Source authoritative? |
|---|---|---|
| `PENDING` | re-stage (target copy is deleted and re-made) | yes |
| `COPIED` | verify | yes |
| `VERIFIED` | re-check quiescence, then cutover | yes |
| `COMMITTED` | activate the target (idempotent) | **no** — target is |
| `DONE` / `ABORTED` | retire the row | — |

Only the `VERIFIED → COMMITTED` transition changes who is authoritative, and it
is a single-statement commit on one database.

## 7. Shard-local dedupe scopes (AC6)

| Mechanism | Table | Migrates? | Accepted window |
|---|---|---|---|
| Signal idempotency keys | `harvest_signals.idempotency_key` | **yes**, copied verbatim with the rows | none — the target enforces the same partial unique index |
| Durable timers | `harvest_timers` | **yes**, verbatim including `fired` | none |
| Payload refs | `harvest_payload_refs` | **yes** | none — the blob store is shard-external |
| Parked workflow task | `harvest_task_queue` (the ≤1 parked/future row) | **yes**, re-keyed to a fresh row id | none |
| Terminal task rows (history) | `harvest_task_queue` | **no** — stay on the sealed source | none; the durable record is in `harvest_events` |
| Start idempotency | `harvest_start_idempotency` | **no** | a replayed start key after migration is routed by hash to the *original* shard, so it still dedupes; keys are per-shard by construction and unaffected |
| Debounce | `harvest_debounce` | **no** | at most one extra debounced start per key immediately after migration |
| Start throttle | `harvest_start_throttle` | **no** | tokens are per-shard already; unaffected for an existing run |
| Rate-limit buckets | `harvest_rate_limit_buckets` | **no** | per-shard already |
| Concurrency-key accounting | derived from `harvest_task_queue` | **no** (implicitly moves with the parked row) | the migrated run's slot moves from A's cap to B's cap — the documented `limit × N` scope is unchanged |
| Completion-trigger fires | `harvest_completion_trigger_fires` | **no** | a trigger whose source run migrated may fire once more; triggers are already at-least-once |
| Workflow logs | `harvest_workflow_logs` | **yes** | none — copied for a privacy reason as much as an operational one: free-text logs are a PII sink, and `erase.rs` scrubs them via the execution's own shard, so logs left behind would be out of reach of an erasure issued against the run |
| Audit log | `harvest_audit_log` | **no** | audit is per-shard and both shards record the migration |

## 8. Explicitly not built here

- Automatic/continuous rebalancing policy.
- Migrating non-quiescent executions.
- Migrating non-root executions (any run with a `parent_id`).
- Migrating `PAUSED` executions.
- Migrating executions holding or queued for a durable mutex, or carrying
  dead-letter rows.
- Changing placement of new starts.
- Any form of cross-shard transaction.
