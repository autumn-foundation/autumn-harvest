# Design — Issue #2012: Physical backout, saga or escrow per workflow

Issue #2012 is an R&D spike. It asks one question. For a workflow whose
effects live in the Harvest Postgres, can Harvest pick physical backout or a
saga from observed contention?

The spike delivers four things:

1. A physical backout runner: one transaction, one savepoint per step.
2. A harness that runs one order workflow three ways and measures them.
3. A selection rule, as a pure function with tests.
4. The write-up `docs/rnd/contention-adaptive-atomicity.md`, with a go or
   no-go verdict.

**No migration. No new `WorkflowEvent` variant. No route change. No change
to an existing code path.** The new module `autumn_harvest::atomicity` is
`#[doc(hidden)]`. It has no stability guarantee.

---

## 0. Planning record

### 0.1 Facts found before the plan

- `ActivityContext::run_transactional` (issue #352) commits the closure's
  writes and `ActivityCompleted` in one transaction. Its docs ask for a
  closure under 5 s.
- `Saga` (`saga.rs`) runs forward steps and keeps a LIFO stack of
  compensations. Its steps are plain futures. It needs only a
  `WorkflowContext`, so a harness can drive it without a worker.
- `diesel-async` nests `transaction` calls. An inner call opens a
  `SAVEPOINT`. An inner error rolls back to that savepoint only.
- `tx_retry::run_with_conflict_retry` re-runs a whole top-level transaction
  after `40P01` or `40001`. Its docs forbid a retry inside an open
  transaction: the outer locks stay, so the cycle can form again.
- A regular one-step workflow writes 17 rows per job
  (`docs/performance-standalone-activity-overhead.md`). Each saga step in
  Harvest is one such activity. One transactional activity is one job.

### 0.2 Brainstorm — how can the spike answer the question?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Run real workers. Compare a saga workflow with a one-activity backout workflow. | Rejected for the measurement. Worker poll gaps dominate the time and hide the lock effect. Section 4 records it as next scope. |
| B2 | Run the same three-step order workflow three ways against Postgres, with the real `Saga` helper. Model each Harvest commit as one history-row insert in the same transaction. | **Adopted.** It isolates the lock and commit effects. It omits engine overhead, which only slows the saga. The bias favours the saga, so a backout win is robust. |
| B3 | Implement the backout runner as nested `diesel-async` transactions. | **Adopted.** Diesel issues the savepoints. The runner adds no SQL of its own. |
| B4 | Write explicit `SAVEPOINT` SQL. | Rejected. Diesel then loses track of the depth. |
| B5 | Add a third arm: commit the hot step early as an escrow step, and back out the rest. | **Adopted.** The issue names escrow. The arm tests one branch of the rule. |
| B6 | Measure contention as a lock-wait share from `pg_stat_activity`. | Rejected for the spike. A sampler adds noise and a moving part. The rule takes model inputs instead. |
| B7 | Base the rule on Little's law: the number of runs that want the hot key at once, and the lock hold time that backout adds. | **Adopted.** Each input is a number Harvest can learn from history or from the author. |
| B8 | Fix the go or no-go criteria before the first measurement. | **Adopted.** Section 3. |

### 0.3 Reverse brainstorm — how can this spike mislead?

| # | How to make it misleading | Mitigation |
|---|---------------------------|------------|
| R1 | An arm leaks a reservation or a debit, and still looks fast. | Each cell checks two invariants after the drain: stock taken equals orders placed, and money taken equals order totals. |
| R2 | The saga arm omits the cost of Harvest's per-step commit. | Each committed transaction in every arm inserts one history row. |
| R3 | Noise on a shared host decides the verdict. | Three repetitions per cell. The report gives the median and the range. A tie band of 10 % applies to the rule check. |
| R4 | The rule is fitted to the data after the fact. | The rule and its thresholds are fixed in section 2.3 before the measurement. |
| R5 | A conflict abort inside a step retries in place and deadlocks again. | Backout retries only the whole transaction, through `run_with_conflict_retry`. |
| R6 | The spike module changes production behaviour. | It is a new module. No existing code calls it. |
| R7 | The harness rots after the merge. | A CI test runs one short cell per arm and checks the invariants. |
| R8 | The write-up drifts from the code. | A docs guard pins the cited items to the code. |

### 0.4 Six thinking hats

- **White (facts).** Section 0.1. The paper reports backout up to 1.8x
  faster at low contention. It reports a saga goodput of 15.9 against 2.0,
  and a P90 of 1,100 ms against 24,554 ms, under long steps or hot items.
- **Red (feelings).** Backout feels right for short, same-database flows.
  A lock held across a slow step feels dangerous.
- **Black (risks).** A long transaction holds a pool connection. It blocks
  vacuum and holds row locks. Rollback cannot undo an email or an HTTP
  call. A shared host gives noisy numbers.
- **Yellow (benefits).** Backout needs no compensation code. No other run
  sees a half-done state. It writes fewer rows and commits once.
- **Green (ideas).** Escrow the hot step and back out the rest. Use a
  savepoint to tolerate the failure of an optional step. Learn the rule
  inputs from recorded step durations.
- **Blue (process).** Red, then green, then refactor. The pure rule first.
  The database tests next. Then the measurement, the write-up and review.

---

## 1. Workload

One order run has three steps, in this order:

1. **Reserve.** `UPDATE inventory SET qty = qty - 1 WHERE sku = $1 AND qty > 0`.
   This is the hot step. Its compensation adds the unit back.
2. **Debit.** `UPDATE accounts SET balance = balance - $2 WHERE id = $1 AND balance >= $2`.
   Its compensation credits the amount back.
3. **Place.** Insert the order row. A seeded draw fails the run here with
   probability `fail_rate`. The failure stands for a business rule, such as
   a fraud check.

Each step holds its transaction open for `step_work` before it returns. That
stands for application work inside the step.

Each committed transaction also inserts one row into a history table. That
stands for the `ActivityCompleted` event that Harvest appends.

| Knob | Low contention | High contention |
|------|----------------|-----------------|
| SKUs | 1,000, uniform | 1 |
| Accounts | 1,000, uniform | 1,000, uniform |

## 2. Design

### 2.1 Arms

| Arm | Transactions per run | Undo |
|-----|----------------------|------|
| `Backout` | 1. Each step runs in a savepoint. | `ROLLBACK` |
| `Saga` | 1 per step, through the real `Saga` helper. | Compensations, LIFO |
| `Hybrid` | 2: reserve commits alone, then debit and place in one backout transaction. | Restock compensation, then `ROLLBACK` |

### 2.2 Module layout

- `atomicity::backout`. `run_backout` opens one transaction with conflict
  retry. `Steps::step` runs one step in a savepoint.
- `atomicity::rule`. `WorkloadProfile`, `Atomicity` and `choose`. Pure, no
  database.
- `atomicity::harness`. The workload, the three arms, the measurement and
  the invariant check.

### 2.3 Selection rule (fixed before the measurement)

Inputs:

- `effects_outside_database`: a step writes outside the Harvest Postgres.
- `total_hold`: the sum of the step durations.
- `hot_key_concurrency`: the number of runs that want the hottest key at
  the same time. In an open system it is the arrival rate on that key times
  the run duration. In a closed loop of `C` clients it is `C` times the
  share of runs that touch the key.
- `hold_after_hot_step`: the time that backout holds the hot lock longer
  than a saga does. That is the sum of the step durations after the hot
  step. (Amended 2026-10-11, see section 3.1: the first text said "from the
  hot write to the end of the run", which also counts the hot step itself.)
- `commit_latency`: the time of one small commit.
- `hot_step_commutative`: the hot step is a bounded add or subtract.

Rule, first match wins:

1. `effects_outside_database` gives `Saga`. A rollback cannot undo them.
2. `total_hold > 5 s` gives `Saga`. That is the `run_transactional` limit.
3. `hot_key_concurrency < 1` gives `Backout`. The hot lock is mostly free.
4. `hold_after_hot_step <= commit_latency` gives `Backout`. The extra hold
   costs less than one more commit.
5. `hot_step_commutative` gives `Hybrid`.
6. Otherwise `Saga`.

## 3. Pre-registered verdict criteria

| # | Criterion |
|---|-----------|
| G1 | `Backout` has the highest goodput in both low-contention cells. |
| G2 | `Backout` does not have the highest goodput in the high-contention, long-step cell. |
| G3 | In every cell, the rule picks an arm within 10 % of the best goodput. |
| G4 | Every arm keeps both invariants in every cell. |

**Go** when G1 to G4 hold. **No-go** when G1 or G4 fails. Otherwise the
verdict is **go with changes**, and the write-up names the change.

Matrix: 3 arms, 2 contention levels, `step_work` of 0 ms and 20 ms. Each cell
runs 16 clients for 10 s, with `fail_rate = 0.1`, three times.

### 3.1 Amendment, 2026-10-11, before any measurement

Review found method defects before the first run produced data. The rule,
its thresholds and G1 to G4 do not change. The method changes:

- Goodput counts only the runs that commit before the deadline, over the
  10 s window. The drain no longer counts.
- The pool opens every connection before the first timed cell.
- Each repetition runs every cell and rotates the arm order. A `CHECKPOINT`
  runs before each cell.
- G4 reads only the two invariants, as written above. Run errors and
  conflict retries are reported in their own columns.
- The report flags a cell where the best arm's range overlaps the
  runner-up's range. G1 and G2 are inconclusive in such a cell.

## 4. Test plan (red, green, refactor)

1. Red: unit tests for each rule branch. Green: `rule::choose`.
2. Red: database tests. A failed step rolls back to its savepoint only. A
   failed run leaves no row. The saga arm compensates. Each arm keeps the
   invariants. Green: `backout` and `harness`.
3. Refactor. Then the full matrix, the write-up, the docs guard and review.

## 5. Acceptance criteria

| Criterion | Evidence |
|-----------|----------|
| A spike write-up under `docs/rnd/` with measurements and a go / no-go verdict. | `docs/rnd/contention-adaptive-atomicity.md`. `contention_adaptive_atomicity_docs` pins its claims. |
| Physical backout is implemented for a short same-database workflow. | `atomicity::backout`, tested by `atomicity_spike_tests`. |
| It is measured against the existing `Saga` helper at low and high contention. | `atomicity::harness`. The saga arm calls `saga::Saga`. Results in the write-up. |
| A selection rule is sketched. | `atomicity::rule::choose` and its unit tests. |

## 6. Next scope

- Run the arms as real Harvest workflows to price the engine overhead.
- Learn `hot_key_concurrency` and `hold_after_hot_step` from history.
- Formal saga isolation levels, as the paper proposes.
