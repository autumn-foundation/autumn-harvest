# Design — Issue #1992: `queue::complete_task` requires a claim

Issue #1992 reports that the public `queue::complete_task` passes no claim.
A caller of it skips the claim-epoch fence of issue #1789.

**No migration. No new `WorkflowEvent` variant. No route change.**
The public signature of `queue::complete_task` changes.

---

## 0. Planning record

### 0.1 Facts found before the plan

- `complete_task(conn, task_id, output)` filters on `id` and
  `state = 'RUNNING'` only.
- Only `worker.rs` calls it in production. It completes a *workflow* task in
  `persist_workflow_completion`, `persist_child_workflow_completion` and the
  continue-as-new seal.
- Each of the three calls runs after `claim_still_held_for_update` in the same
  transaction (issue #1184). That guard locks the task row and checks
  `claim_held(worker_id, attempt)`. So no production path completes a stale
  claim today.
- The activity path already uses the fenced `complete_claimed_task`.
- 16 test calls use `complete_task`. One is in the plugin. One models the
  pre-#1789 write on purpose (`Fencing::StateOnly` in
  `dst_differential_tests.rs`).
- A stale-completion path is reachable through the public API. Worker A
  claims. The reclaimer requeues. Worker B claims. A calls
  `complete_task(task_id)`. The row becomes `COMPLETED` with A's output, and
  B loses its claim.

### 0.2 Brainstorm — how can the gap close?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Make `complete_task` `pub(crate)`. | Rejected. 16 test calls need a public completion. A `testing`-feature twin keeps the unfenced write public. |
| B2 | Delete `complete_task`. Callers use `complete_claimed_task`. | Rejected. Each caller then maps `ClaimWrite::LeaseLost` to an error by hand. |
| B3 | Keep the name. Require a `&TaskClaim`. Return `NotFound` when the claim is not current. | **Adopted.** One public completion shape per outcome: `complete_task` errors, `complete_claimed_task` reports. Old call sites fail to compile, so no caller keeps the unfenced write by accident. |
| B4 | Add `#[deprecated]` to the old function. | Rejected. A deprecated unfenced write is still public. |
| B5 | A source-scan guard over `queue.rs`. | Rejected. The type system gives the same guarantee. |

### 0.3 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | A workflow-task claim does not match the fence, so a valid completion fails. | The fence uses the same `claim_held(worker_id, attempt)` as the #1184 guard, under the same row lock. Existing workflow completion suites cover it. |
| R2 | A changed error breaks a caller that matches on it. | The error stays `HarvestError::NotFound`. |
| R3 | A test migration weakens a test. | Each test passes the claim it took. The late-completion test in `force_fail_tests.rs` keeps its assertion. |
| R4 | The DST differential loses its unfenced baseline. | `Fencing::StateOnly` runs the pre-#1789 `UPDATE` as raw SQL in the test. |
| R5 | A downstream crate breaks without guidance. | The changelog fragment names the new argument and `TaskClaim::of`. |
| R6 | An unfenced completion comes back through the shared inner function. | The inner function takes `&TaskClaim`, not `Option`. No unfenced completion can be written in `queue.rs`. |

### 0.4 Six thinking hats

| Hat | Notes |
|-----|-------|
| White | One unfenced public completion. Three production callers, all guarded. 16 test calls. |
| Red | An API that lets any caller complete any running row looks unsafe, guarded or not. |
| Black | A semver break in a 0.x crate. More arguments at each test call. |
| Yellow | The fence becomes a property of the type. The three worker calls get a second check at the write. |
| Green | `complete_task` errors and `complete_claimed_task` reports. Callers pick the shape they need. |
| Blue | Red: a stale owner completes a reclaimed row through `complete_task`. Green: require the claim. Refactor: the inner write takes `&TaskClaim`, then docs. Then a multi-angle review. |

---

## 1. Change

- `queue::complete_task(conn, claim: &TaskClaim, output) -> HarvestResult<()>`.
  It returns `NotFound` when `claim` is not current.
- `complete_task_inner` takes `&TaskClaim`.
- The three worker calls pass `TaskClaim::new(task_id, worker_id, attempt)`.

## 2. Tests

| Test | Where |
|------|-------|
| A stale owner cannot complete a reclaimed row through `complete_task`. B then completes it. | `activity_claim_epoch_tests.rs` |
| Existing callers pass their claim. | integration suites, plugin `api_scheduler_integration.rs` |
