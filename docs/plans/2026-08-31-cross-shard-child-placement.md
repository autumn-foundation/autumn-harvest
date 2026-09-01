# Cross-shard child workflow placement (issue #956)

Planning record for the opt-in `ChildPlacement` policy that lets fanned-out
children land on shards other than the parent's.

---

## 1. Brainstorming — candidate designs

Ideas generated before filtering, so the discarded ones stay on the record.

| # | Idea | Verdict |
|---|---|---|
| B1 | **New `WorkflowEvent` variants** (`ChildWorkflowStartedRemote`, `CrossShardNotifyRequested`) to carry placement. | ❌ AC6 forbids new variants and the child's shard is already recoverable from `child_id`. |
| B2 | **Two-phase commit** across parent and child shards. | ❌ Explicit non-goal; shard-local ACID is the design constraint. |
| B3 | **Inline cross-shard insert** during the parent's decision cycle (open a second connection to shard B, insert the child, then commit the parent). | ❌ A crash between the B-insert and the A-commit orphans the child; the retried cycle mints a *new* `child_id`, so the orphan is unreachable and the fan-out silently double-spawns. |
| B4 | **Outbox row on the parent's shard**, written in the *same* transaction as `ChildWorkflowStarted`; a scanner relays it to shard B. | ✅ Chosen. Atomic with the parent's history, idempotent on replay (`child_id` is the PK on B), and exactly the `harvest_completion_trigger_outbox` precedent. |
| B5 | **Pull the child's terminal from shard B** rather than pushing a notify from B. | ✅ Chosen. The outbox row that started the child is *also* the work-list entry for its terminal; the delivery append and the row delete commit in one transaction on shard A, so "no lost wake under a crash between the child's terminal commit and the parent's notify" is structural rather than argued — there is no handoff to lose. |
| B6 | **Push a notify outbox row from shard B** in the child's terminal transaction. | ❌ Costs a second table, a write on B's hot terminal path, and re-introduces the very crash window B5 removes. |
| B7 | **Directory table** mapping `child_id → shard`. | ❌ AC2 requires O(1) routing with no directory lookup; the `ExecutionId` already encodes it. |
| B8 | Distribute by hashing the child's **random UUID**. | ❌ Uniform, but not restart-stable: a decision cycle retried after a crash re-mints the UUID and re-rolls the shard. AC2 asks for the top-level start's stability contract. |
| B9 | Distribute by hashing a **deterministic placement key** `"{parent_exec_id}#{n}"` through `ShardRouter::pick_for_new_workflow`. | ✅ Chosen. Same rendezvous function, same writable-subset redirect, and a retried cycle re-derives the identical shard. |
| B10 | Make cross-shard placement the default once it works. | ❌ Explicitly out of scope: "parent-pinned remains the default indefinitely." |
| B11 | **Silent fallback** to the parent's shard when the target shard has no pool. | ❌ AC8 names this as the anti-goal. Fail with a typed, retryable error. |
| B12 | Extend `apply_parent_close_cascade` to query sibling shards inline. | ❌ Puts a cross-shard read inside the parent's terminal transaction. Route it through the same outbox row instead. |
| B13 | Reuse the outbox row as a **cancel channel** (`cancel_requested` flag) so race-loser cancels, child-deadline cancels and the parent-close cascade all ride one mechanism. | ✅ Chosen. One table, one scanner, one set of semantics. |

## 2. Reverse brainstorming — how would we *guarantee* this breaks?

Each failure mode is paired with the structural defence that makes it
unreachable, and (where it is testable) the test that pins it.

| # | "How to break it" | Defence |
|---|---|---|
| R1 | Change the default so every existing deployment silently spreads children. | `ChildPlacement::ParentShard` is `#[default]`; the existing `spawn_child_workflow*` methods delegate with it and never consult the router. Test: `parent_shard_placement_never_consults_the_router`. |
| R2 | Route through the router even for `ParentShard`, so a deployment with no installed router starts failing. | Resolution short-circuits on `ParentShard` *before* touching `GLOBAL_SHARD_ROUTER`. |
| R3 | Pick the child's shard from a non-deterministic source, so replay diverges. | The shard is never re-derived on replay: the recorded `child_id` is reused verbatim. Live derivation uses a deterministic key. Test: `distributed_placement_is_stable_across_a_retried_decision_cycle`. |
| R4 | Let a fan-out on a **single-shard** deployment start behaving differently. | With one writable shard the rendezvous pick is that shard, which *is* the parent's shard, so the local path is taken. Test: `distributed_placement_on_a_single_shard_router_stays_local`. |
| R5 | Orphan a child: created on B, parent's history never committed. | The outbox row is written in the parent's transaction. No row ⇒ no child. |
| R6 | Duplicate a child: relay runs twice. | Insert on B is keyed by the `child_id` PK with `ON CONFLICT DO NOTHING`; a second relay is a no-op. |
| R7 | Lose the parent's wake: child commits terminal on B, then every worker dies. | Delivery is *pull*-driven off a durable row on A. Nothing was in flight to lose. Test: `terminal_delivery_survives_a_crash_between_child_terminal_and_parent_notify`. |
| R8 | Deliver the same terminal twice, so the parent sees two `ChildWorkflowCompleted`. | Append + row delete are one transaction on A; the relay additionally skips when a terminal event for that `child_id` already exists. |
| R9 | Have the parent's decision transaction span two shards. | The parent's transaction only ever writes A: its own events, the outbox row. Test: `parent_decision_transaction_touches_only_the_parent_shard`. |
| R10 | Break replay byte-equality by recording placement in history. | Nothing new is recorded. `ChildWorkflowStarted` carries the same three fields it always did. Test: `child_workflow_started_json_is_byte_identical_under_distributed_placement`. |
| R11 | 500 a parent-detail read when one shard is down. | `/workflows/{id}/children` now degrades through `shard_fanout::collect_fanout_rows` and reports `unavailable_shards`, matching `/tree`. |
| R12 | Silently place on the parent's shard when the target has no pool. | Pre-checked at spawn time; `HarvestError::ShardUnavailable` (retryable) is raised instead. Test: `unreachable_target_shard_fails_the_spawn_with_a_typed_retryable_error`. |
| R13 | Leak a detached cross-shard child past its parent's close. | The outbox row survives the child's start and carries the `ParentClosePolicy`; the scanner applies the cascade on B once the parent is terminal. |
| R14 | Leak the outbox table without bound under a 10k fan-out. | Every terminal path deletes the row: awaited on terminal delivery, detached-`Abandon` immediately after start, detached-cancel/terminate after cascade. |
| R15 | Deadlock two shards against each other. | The relay never holds a transaction on A while opening one on B *for the same row in the opposite direction*: phase 1 commits on B then on A; phase 2 reads B outside the A transaction and only then opens it. |

## 3. Six Thinking Hats

**⚪ White (facts).** Three primitives already exist and are load-bearing:
`ExecutionId::new_for_shard` encodes placement in the id's first two bytes;
`ShardRouter::pick_for_new_workflow` is a stable rendezvous pick over
`writable_shards` with a readable-set-first redirect; and two independent
outbox precedents ship today — event-log-as-outbox (`enforce_external_signals_outbox`,
#492) and table-as-outbox (`harvest_completion_trigger_outbox`). Today every
child-spawn site mints `ExecutionId::new_for_shard(self.exec_id.shard())`
— five call sites in `context.rs` — and the worker stamps
`shard_id = parent_execution.shard_id` on the child row.

**🔴 Red (instinct).** The scary part is not the placement — that is three
lines — it is the *four* control-flow edges that today are ordinary
same-transaction function calls and become cross-shard hops: child creation,
terminal notify, explicit cancel, and the parent-close cascade. Missing one
does not fail loudly; it hangs a parent forever. The design must make all four
ride *one* durable row so "did we cover that edge?" is answerable by reading a
single state machine.

**⚫ Black (risks).**
- Wake latency for a cross-shard child becomes a scanner tick, not a
  same-transaction append. Must be documented, not discovered.
- A 10k-child fan-out writes 10k outbox rows on the parent's shard. That is
  cheaper than 10k execution rows + 10k `WorkflowStarted` events + 10k queue
  rows *plus every child's own history*, but it is not free, and the phase-2
  poll must batch per shard or it becomes an N+1.
- `ChildWorkflowCascadeApplied` appended after the parent has sealed adds
  history past closure. The same event is already appended inside the terminal
  transaction on the local path, so the *shape* is unchanged — but it is worth
  stating explicitly.
- Residency (#697) is transitive today. Opting a child out of the parent's
  shard is exactly the thing residency forbids, so the docs table must gain
  the exception rather than quietly becoming wrong.

**🟡 Yellow (upside).** Zero event-schema change means no migration of history,
no replay fixture churn, and no version gate. The feature is invisible until
someone passes a non-default `ChildPlacement`. And because delivery is pull-based
off the parent's shard, the crash-safety argument is a one-liner rather than a
protocol proof.

**🟢 Green (creativity).** The reframing that collapses the design: *the outbox
row is not a message, it is the cross-shard child's lifecycle record on the
parent's side.* Start, cancel, terminal delivery, and close-cascade are four
transitions of one row, not four mechanisms. That also means the row's
existence is the leak detector — an operator can `SELECT count(*)` to see
in-flight cross-shard children.

**🔵 Blue (process).** TDD in three phases. RED: unit tests for the pure
placement resolver and the state machine (runnable with no database), plus
DB-backed multi-shard integration tests following `sharded_runtime_tests.rs`'s
one-database-per-shard harness. GREEN: minimal implementation. REFACTOR: dedupe
the five spawn sites onto one helper, document, changelog fragment. Then
multi-angle agent review, then the AC evidence table.

---

## 4. Chosen design

### 4.1 Public API

```rust
pub enum ChildPlacement {
    ParentShard,              // default — byte-for-byte today
    Distributed,              // rendezvous over writable_shards
    Shard(ShardId),           // honour an explicit pin (#697)
    ResidencyKey(String),     // honour an explicit residency pin (#697)
}
```

Every `spawn_child_workflow*` gains a `_placed` sibling taking a
`&ChildPlacement`. The existing methods delegate with `ChildPlacement::ParentShard`.

### 4.2 Placement resolution

`WorkflowContext::resolve_child_shard`:

- `ParentShard` → `self.exec_id.shard()`, router never consulted.
- otherwise → `GLOBAL_SHARD_ROUTER` is required; absent ⇒ `HarvestError::Config`.
  - `Distributed` → `pick_for_new_workflow(workflow_name, "{parent_exec_id}#{n}")`
    where `n` is a per-parent fresh-dispatch counter.
  - `Shard`/`ResidencyKey` → `ShardRouter::resolve_placement` (reuses #697's
    validation and its typed rejections).

Resolution happens **only on a fresh dispatch**. On replay the recorded
`child_id` is reused verbatim, so placement is not re-derived and replay is
byte-identical.

### 4.3 The cross-shard child outbox

Migration `20260901130054_harvest_cross_shard_children` adds one table on every
shard:

| column | meaning |
|---|---|
| `child_exec_id` (PK) | the child; encodes the target shard |
| `parent_exec_id` | the parent on *this* shard |
| `target_shard` | denormalised for the scanner's index |
| `status` | `PENDING_START` → `STARTED` |
| `cancel_requested` | set by the parent's cancel paths |
| `workflow_name`, `input`, `queue_name`, `parent_close_policy`, `assigned_build_id`, `context_headers`, `priority` | what the relay needs to create the child |
| `attempts`, `last_error`, `last_attempt_at`, `created_at` | operability, and the retry backoff / rotation ordering |

### 4.4 The scanner

`enforce_cross_shard_children` joins the existing `enforce_timeouts_once`
sweep, alongside the #492 outbox scanners.

1. **Start** (`PENDING_START`): open shard B, insert the child row +
   `WorkflowStarted` + enqueue task under `ON CONFLICT DO NOTHING`; then mark
   `STARTED` on A. Detached-`Abandon` rows are deleted here — nothing more is owed.
2. **Cancel** (`cancel_requested`): idempotent `cancel_workflow_execution` on B.
3. **Terminal delivery** (awaited, `STARTED`): batch-read the children's states
   on B; for each terminal one, append `ChildWorkflowCompleted`/`ChildWorkflowFailed`
   on A, wake the parent, and delete the row — all in one transaction on A.
4. **Close cascade** (detached, `STARTED`, parent terminal): apply the policy on
   B, append `ChildWorkflowCascadeApplied` on A, delete the row.

### 4.5 Consistency contract (documented in `docs/sharding.md`)

- The parent's decision transaction is shard-local. Always.
- Child start, terminal notify, and cancel are **at-least-once with dedupe**:
  `child_id` is the dedupe key for the start, the parent's own terminal event is
  the dedupe key for the notify, and cancel/cascade are idempotent by state.
- A cross-shard child's wake latency is one scanner tick, not one transaction.
- Placement never falls back silently. An unreachable target shard is a typed,
  retryable `HarvestError::ShardUnavailable`.

### 4.6 Read-path degradation (AC7)

`GET /workflows/{id}/children` currently propagates a pool error with `?`, so a
single unreachable shard 500s the whole call. It is moved onto
`shard_fanout::collect_fanout_rows` and gains the additive `unavailable_shards`
field `/tree` and the #756 endpoints already carry.
