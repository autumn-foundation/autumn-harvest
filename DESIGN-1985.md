# Design — Issue #1985: four small primitives

Issue #1985 lists four primitives that peer engines ship:

1. A payload-matching event wait, like Cloudflare `step.waitForEvent`.
2. A durable promise, like Restate awakeables.
3. A counting semaphore across workflows.
4. The `AllowAll` schedule overlap mode, like Temporal.

Each needs a decision: build, or decline with a reason in
`docs/comparison.md`. Each built primitive needs a replay test and a docs
entry.

**Decisions.** Build items 1, 2 and 4. Decline item 3.

**No migration. No new `WorkflowEvent` variant. No new route.**

---

## 0. Planning record

### 0.1 Brainstorm — how can each primitive be built?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Match a signal payload with a predicate in the existing signal matcher. Keep non-matching signals buffered. | **Adopted** for item 1. See §1.1. |
| B2 | Match a payload, and discard non-matching signals. | Rejected. A later wait for the same name loses data. |
| B3 | Add a new `WaitForSignalMatching` command. | Rejected. About 20 exhaustive matches change. The worker park and wake path does not need it. |
| B4 | Build the durable promise on external task tokens (`execute_activity_external`). | Rejected. A token wait is a solo suspension. It cannot race a timer. It needs a deadline. The token exists only after the park commits. |
| B5 | Build the durable promise on signals. One signal name per promise. The promise key is the idempotency key. | **Adopted** for item 2. See §1.2. |
| B6 | Add a new `harvest_promises` table, event and API route. | Rejected. Signals already give storage, wake, dedupe, buffering and replay. |
| B7 | Build a semaphore as N mutex slots. | Rejected. `harvest_mutex_locks.lock_key` is the primary key. There is no try-acquire. Each slot has its own FIFO queue. |
| B8 | Build a semaphore with new permit tables, a new event and lease reclaim. | Deferred. It is the size of the durable mutex (#691). The issue says to split an item that grows. |
| B9 | Map `AllowAll` to a new `OverlapAction::Proceed`. Bypass both `max_active_runs` gates. | **Adopted** for item 4. See §1.3. |
| B10 | Implement `AllowAll` as "raise `max_active_runs`". | Rejected. That is a different knob. Temporal `AllowAll` has no cap. |

### 0.2 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | A warm (resident) workflow gets a non-matching payload. `Awaiting::deliver` sends any same-name signal to the parked future. | A predicate wait marks its signal name as probed. `ResidentWorkflow::capture` then declines. A resident test proves it. |
| R2 | Strict or canary replay reports a skipped signal as unconsumed history. | The matcher records each rejected event index. `has_non_lifecycle_unconsumed` excuses it. Replay tests prove a parked and a completed history. |
| R3 | The excuse hides a removed wait. | The excuse is per event index. Only a predicate that examined and rejected the event sets it. A signal that no code examines still flags. Limit: a removed *later* wait that would take a rejected signal does not flag, because consumption records no event. `late_race_signal_events` has the same limit. The docs state it. |
| R4 | A skipped signal is lost. | It stays in `pending_signals`. A later `wait_for_signal` for the same name takes it. A unit test proves it. |
| R5 | A promise resolves twice with different values. | The resolve uses the promise key as the signal idempotency key. The second insert is a no-op. A DB test proves it. |
| R6 | A promise key collides with a user signal name. | The signal name has the reserved prefix `harvest.promise:`. |
| R7 | A promise key breaks the HTTP path or the string form. | `PromiseId::new` accepts only `[A-Za-z0-9._:-]`, 1 to 128 bytes. |
| R8 | `AllowAll` starts runs with colliding workflow IDs. | The scheduled workflow ID includes the slot time. Each slot gets a unique ID. A DB test proves two concurrent runs. |
| R9 | `AllowAll` passes the first gate but the dispatch loop defers it. | The dispatch loop skips its `max_active_runs` check for `AllowAll`. A catch-up test fires several slots in one tick. |
| R10 | An old binary reads `allow_all`. | `OverlapPolicy::from_db` maps an unknown value to `Skip`. Rollback is safe and conservative. |
| R11 | `last_completion_result` carryover reads a stale result. | Carryover assumes runs do not overlap. The docs state this for `AllowAll`. |
| R12 | A predicate that is not pure breaks replay. | The docs require a pure predicate. This is the rule for all workflow code. `harvest-verify` analyses the predicate closure. |
| R13 | A predicate calls `ctx` and deadlocks on the matcher lock. | The context runs the predicate over `signal_candidates` with no lock held, then matches by accepted event index. A test calls `ctx.is_replaying()` inside the predicate. |
| R14 | A caller settles a promise with no key, another key, or a bad payload. | `send_signal_idempotent` forces the promise key, refuses a bad payload, and reserves the key prefix. A DB test covers each case. |
| R15 | `AllowAll` starts every slot of a long outage in one tick. | `ALLOW_ALL_MAX_STARTS_PER_TICK` (100) defers the rest to the next tick. A DB test proves it. |

### 0.3 Six thinking hats

| Hat | Notes |
|-----|-------|
| White | Signals are FIFO by `received_at`, then `id`. `SignalReceived` has no id. The matcher already stashes signals of other names. `send_signal_idempotent` dedupes on `(workflow_exec_id, idempotency_key)`. `overlap_policy` is `TEXT` with no CHECK constraint. The mutex is single-holder by schema. |
| Red | The three built items feel like natural extensions of existing parts. A semaphore built in this PR feels too large to review well. |
| Black | R1 to R12. A promise is bound to one run. Continue-as-new does not carry its key. A named promise can be re-created after continue-as-new. The predicate wait has no timeout form yet. |
| Yellow | No migration and no new event. Replay of all three items uses existing events. The promise races a timer through `ctx.race()` and `wait_timeout`. |
| Green | B1 to B10 in §0.1. |
| Blue | Red phase: write the tests in §2. They fail to compile or fail. Green phase: the code in §1. Refactor phase: docs, comment audit, review. |

---

## 1. Design

### 1.1 Payload-matching signal wait (item 1)

```rust
ctx.wait_for_signal_matching("order", |p| p["id"] == 42).await?;
ctx.receive_signal_matching::<Order, _>("order", |o| o.id == 42).await?;
```

`HistoryMatcher::match_signal_where` generalises `match_signal_inner` with a
predicate. A same-name signal that fails the predicate goes to
`pending_signals`, and its index goes to `predicate_rejected_signal_events`.
The scan continues. The typed form treats a payload that does not decode as
a non-match.

The wait reuses `WorkflowCommand::WaitForSignal`. Any signal wakes the run.
The cold replay runs the predicate again. If nothing matches, the run parks
again.

`unconsumed_signals_by_name` still counts a rejected signal. The
`harvest.signal.unhandled` metric stays true.

Do not register a push handler for the same name. The handler claims every
buffered signal of that name.

### 1.2 Durable promise (item 2)

```rust
let mut promise = ctx.new_promise()?;        // key: a recorded UUIDv7
let token = promise.id().to_string();        // "<exec-id>/<key>"
// ...hand `token` to any caller...
let value: Approval = promise.wait().await??;
```

- `PromiseId { execution_id, key }`. The string form is
  `<execution-id>/<key>`. `ctx.promise` and `ctx.new_promise` record it with
  `side_effect`, so replay does not depend on the execution id.
- The signal name is `harvest.promise:<key>`. The idempotency key is the
  same string.
- The settlement payload is `{"outcome":"resolved","value":…}` or
  `{"outcome":"rejected","error":"…"}`.
- `wait` returns `HarvestResult<Result<T, PromiseRejected>>`. The outer error
  is an engine error. The inner error is a rejection.
- `wait_timeout` uses `wait_for_signal_timeout`.
- Resolvers:
  - `durable_promise::resolve` and `durable_promise::reject` (feature `db`).
    They return `true` on the first settlement and `false` after.
  - `ctx.resolve_promise` and `ctx.reject_promise` from another workflow.
  - `POST /workflows/{id}/signal/{signal_name}`. This route follows the
    workflow retry chain, which helps a named promise only.
- `send_signal_idempotent` enforces the settlement rules on every path.

**Alignment.** #2006 can resolve a promise when a remote MCP task completes.
#1975 (keyed entities) can use named promises for per-key replies. Neither
needs a new storage shape.

### 1.3 `AllowAll` overlap (item 4)

`OverlapPolicy::AllowAll` serialises as `allow_all`. `apply_overlap_policy`
returns `OverlapAction::Proceed`. The dispatch loop and the manual DAG
trigger skip their `max_active_runs` checks for `AllowAll`. So `AllowAll`
ignores `max_active_runs`, as Temporal does. Each dispatch phase of a tick
starts at most `ALLOW_ALL_MAX_STARTS_PER_TICK` runs. Per-workflow concurrency limits,
throttles and admission gates still apply.

### 1.4 Counting semaphore (item 3) — declined

The per-key concurrency limit (#247) gives an N-limit on whole runs across
the fleet. The durable mutex (#691) gives a one-holder region. A region-level
N-permit semaphore needs new permit tables, a new event, lease renewal,
reclaim, a terminal sweep, reset and rebalance hooks. That is the size of
#691. `docs/comparison.md` records the decision and the reason.

---

## 2. Tests

| Test | Item | Phase |
|------|------|-------|
| `replay.rs` `match_signal_where_*` and rejected-signal checks (unit) | 1 | Red, then green |
| `context.rs` `wait_for_signal_matching_*`, `receive_signal_matching_*` (unit) | 1 | Red, then green |
| `resident.rs` `ineligible_suspensions_are_not_resident` predicate case (unit) | 1 | Red, then green |
| `durable_promise.rs` unit tests, settlement rules included | 2 | Red, then green |
| `policy.rs` and `scheduler.rs` `allow_all` unit tests | 4 | Red, then green |
| `tests/integration/small_primitives_tests.rs`: replay, canary and test-env cases | 1, 2 | Red, then green |
| `tests/integration/small_primitives_db_tests.rs`: worker runs, each replayed with `WorkflowReplayer` | 1, 2, 4 | Red, then green |
| `autumn-harvest-verify` `model_coverage` | 1, 2 | Red, then green |
