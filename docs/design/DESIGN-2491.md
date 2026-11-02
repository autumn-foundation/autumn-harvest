# Design — Issue #2491: no final 500 on a chain that still moves

The durable route walks a continue-as-new chain to its last run. The walk
takes at most `max_chain_steps` steps. At the cap, the route answers
pending or a final `500 result-unavailable`. On CI, the route answered a
final 500 on a chain that still moved. A retry would have found the result.

**No migration. No new `WorkflowEvent` variant. No new route.**

---

## 0. Planning record

### 0.1 Facts

- `wait_for_result` reads a snapshot of a run, then asks for its successor.
  These are two reads, with time between them.
- `chain_successor` finds the newest run of the same type. On a moving
  chain, that run can continue as new before the walk reads its snapshot.
- Commit `2f0053b` added `Successor.live`. A lookup that sees a live
  newest run marks the walk as live. That closes the case in the issue for
  a lookup that reads the run state.
- Two lookup paths still mark a successor as ended without a state read:
  a successor from the continue-as-new event of a sealed newest run, and a
  successor of a run with no newer run. Both cross a change of type.
- A walk can also read a stale prefix. Each run that it read had ended,
  but the newest run that a lookup returned can change while the chain
  moves. A chain of type `A -> B -> A` shows this. The first lookup finds
  no second `A` run, so it follows the event to `B`. A retry finds the
  second `A` run in one step.
- The data that a walk reads stops changing when the chain has no live
  run. After that, a retry repeats the walk exactly.

### 0.2 The rule

A final answer at the cap is safe only when the walk read data that no
longer changes. The route therefore answers `Unreadable` only when:

1. a tail read saw that the chain has no live run, and
2. a second walk, which starts after that tail read, reaches the cap at
   the same run as the first walk.

Every other cap answers pending. Pending is always safe: the client asks
again. A chain that has ended gives the same final answer on each request.
So the client stops.

### 0.3 Brainstorm — how can the route know that the answer is final?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Set `Successor.live` on every lookup path. | Rejected alone. It does not catch a stale prefix (§0.1, `A -> B -> A`). |
| B2 | At the cap, always answer pending. | Rejected. A chain that has ended and is too long would answer pending forever. |
| B3 | At the cap, ask for one more successor and read it. | Rejected. The tail can move again. The race moves one step, it does not go away. |
| B4 | At the cap, read the chain tail. Live tail: pending. | **Adopted.** One indexed query, on the cap path only. |
| B5 | After a tail read that saw an ended chain, walk again. Final only when the second walk caps at the same run. | **Adopted.** The second walk reads frozen data, so a retry repeats it. |
| B6 | Compare the tail `completed_at` with the walk start time. | Rejected. It mixes the app clock and the database clock. |
| B7 | Read the tail and the newest run in one statement. | Rejected. Each later lookup is a new statement, so the race stays. |
| B8 | Remove `Successor.live` after B4 and B5. | **Adopted.** The tail read is the one source of truth. Fewer paths to keep in step. |

### 0.4 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | Answer pending forever on a chain that has ended. | The tail read excludes a reconciled migration seal, as `newest_chain_run` does. A test runs an ended long chain and expects a final 500. |
| R2 | Answer final while a run is still live. | A final answer needs an ended tail and an equal second walk. Red tests cover the race and the stale prefix. |
| R3 | Double the work of every request. | The tail read and the second walk run only at the cap. A walk that reaches its end does not change. |
| R4 | Let the second walk wait on a live run past the budget. | Both walks share one deadline and one permit. |
| R5 | Scan the whole executions table. | The query filters by `id` and `first_exec_id`. Both have an index. A Ledger profile records it. |
| R6 | Read the tail on the wrong shard after a rebalance. | The tail read binds to the shard of the live attempt, as `chain_successor` does. |
| R7 | Loosen the integration tests to accept 500. | Not done. The tests keep `200` and gain a live-tail case. |
| R8 | Let a tail read error become a final answer. | `read_error` classifies it. A database error is pending. |

### 0.5 Six thinking hats

- **White (facts).** Two CI failures, both at `max_chain_steps(1)` on a
  ten-hop chain. The walk is generic over `RunReads`, so a fake chain can
  run the race with no database.
- **Red (feeling).** The old comment said "a walk over ended runs only
  repeats itself". That felt true and was false. A rule that depends on
  timing needs a proof by test, not a comment.
- **Black (risk).** A second walk doubles the cost at the cap. A tail query
  with a wrong scope gives a final answer too early, or a pending answer
  for ever. See R1 to R6.
- **Yellow (value).** Clients that treat 500 as final no longer lose a
  result that exists. The rule is simple to state: final means frozen.
- **Green (options).** B1 to B8. A later option: store the chain tail on
  the origin row. That needs a migration and a writer on each hop, so it
  is out of scope.
- **Blue (process).** Red: unit tests on the fake chain. Green: the tail
  read and the second walk. Refactor: remove `Successor.live`. Then a
  Postgres test with a live tail, the gates, and a multi-angle review.

---

## 1. Design

### 1.1 `RunReads::chain_live`

```rust
/// Whether the chain of the run has a run that has not ended.
fn chain_live(&self, exec_id: ExecutionId)
    -> impl Future<Output = HarvestResult<bool>> + Send;
```

The Postgres reader resolves the live attempt of the run and binds to its
shard, as `chain_successor` does. It then asks whether a run with the same
business id, in the chain of that attempt, has a state that is not
terminal. A reconciled migration seal does not count.

### 1.2 `wait_for_result`

The walk moves into `walk_chain`. It returns a terminal result, a failure,
or `Capped { at, saw_live }`. `wait_for_result` then decides:

| First walk | Tail read | Second walk | Answer |
|---|---|---|---|
| ends or fails | — | — | that answer |
| capped, saw a live run | — | — | pending |
| capped | error | — | `read_error` |
| capped | live | — | pending |
| capped at `X` | ended | ends or fails | that answer |
| capped at `X` | ended | capped at `X`, no live run | **final 500** |
| capped at `X` | ended | capped elsewhere, or saw a live run | pending |

Both walks share the deadline, the permit and the waiter semaphore.

### 1.3 Tests

Unit tests on the fake chain (`durable_route.rs`):

- a lookup that returns a run, which continues as new before its snapshot,
  with a live tail, answers pending (the race in the issue);
- a chain that ends during the walk gets a second walk and a result;
- a chain that has ended and is too long stays final, with one tail read;
- a walk that saw a live run reads no tail;
- a tail read error maps like any other read error;
- a second walk that caps at another run answers pending.

Postgres test (`durable_route_integration.rs`): a chain that changes type
twice, with a parked last run, answers pending at `max_chain_steps(1)`. It
answers the final 500 after the last run ends.

## 2. Acceptance criteria

- AC-1: A cap with a live tail answers pending.
- AC-2: A cap after the chain ended answers from a walk that read only
  ended runs.
- AC-3: A final 500 needs an ended tail and two equal capped walks.
- AC-4: `same_type_chain_takes_one_step` and
  `rerun_of_a_chain_end_keeps_the_original_outcome` keep their assertions.
- AC-5: The new query has a Ledger profile.
