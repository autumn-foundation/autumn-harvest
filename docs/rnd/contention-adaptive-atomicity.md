# Contention-adaptive atomicity: physical backout, saga or hybrid (issue #2012)

**Status:** R&D spike. **Measured:** 2026-10-11, revision `e7bb735`.
**Plan and pre-registration:** [`DESIGN-2012.md`](../../DESIGN-2012.md),
committed in `62f99f0` before the code.

## Question

A workflow can keep all its effects in the Harvest Postgres. Can Harvest
then pick physical backout or a saga per workflow, from the contention
that it observes?

## Verdict

**Verdict: Go with changes.**

Physical backout is worth building for cold, short, same-database
workflows. It gives 1.53x the goodput of a saga there. Under a hot key, a
saga or a hybrid wins by up to 3x. The rule picks the winner in three of
four cells. It needs one change before a follow-up builds it: the "extra
hold" input must be the observed hot-lock hold, not the configured step
time. See [The change](#the-change).

| Criterion | Text (pre-registered) | Result |
|---|---|---|
| G1 | `backout` has the highest goodput in both low-contention cells. | **Inconclusive.** It holds at 0 ms. At 20 ms, `backout` has the best median, but its range overlaps the `hybrid` range. `backout` beats `saga` there with no overlap. |
| G2 | `backout` does not have the highest goodput in the hot, long-step cell. | **Holds.** `saga` and `hybrid` give about 2.9x the goodput of `backout`. |
| G3 | In every cell, the rule picks an arm within `TIE_BAND` = 10 % of the best goodput. | **Fails** in the hot, 0 ms cell. The rule picks `backout`, which gives 53 % of the best goodput. |
| G4 | Every arm keeps both invariants in every cell. | **Holds.** All 36 runs: stock taken equals orders, money taken equals order totals. |

The verdict comes from `atomicity::verdict::judge`, applied to the
measured cells. G1 and G4 hold and G3 fails, so the outcome is "go with
changes".

## What the spike built

All code is in `autumn_harvest::atomicity`. It is `#[doc(hidden)]` and has
no production caller.

- **`backout::run_backout`** runs a body in one transaction.
  **`Steps::step`** runs each step in a nested `diesel-async`
  transaction, so Diesel issues one `SAVEPOINT` per step. A failed step
  rolls back to its savepoint. The body then propagates the error, which
  rolls back the run, or goes on. A deadlock or serialization abort makes
  the whole run retry through `tx_retry::run_with_conflict_retry`. This
  holds even when the body ignores the step error. The runner is about
  80 lines of code and adds no SQL of its own.
- **`harness`** runs one order workflow three ways. A run reserves one unit
  of stock (the hot step), debits an account and places the order. The
  place step declines 10 % of orders, which forces an undo.
- **`rule::choose`** is the selection rule. **`verdict::judge`** applies
  G1 to G4.

| Arm | Transactions per run | Undo |
|---|---|---|
| `backout` | 1, with a savepoint per step | `ROLLBACK` |
| `saga` | 1 per step, through the real `saga::Saga` helper | Compensations, last first |
| `hybrid` | 2: the reserve step commits alone as an escrow step, then debit and place back out together | Restock compensation, then `ROLLBACK` |

Each committed transaction inserts one history row. That row stands for
the `ActivityCompleted` event that Harvest appends.

## Setup

| Knob | Value |
|---|---|
| Contention | Low: 1,000 SKUs. High: 1 SKU. Accounts: 1,000. Draws are uniform. |
| Step work | 0 ms and 20 ms, held inside each step after its write |
| Load | 16 clients in a closed loop, 10 s per run |
| Repetitions | 3 repetitions per cell. Each repetition rotates the arm order and runs `CHECKPOINT` before each cell. |
| Declines | 10 % of orders |
| Goodput | Runs committed inside the 10 s window, per second. The drain after the window does not count. |
| Host | 4 vCPU Xeon at 2.1 GHz, 15 GB RAM. The harness and Postgres share the host. |
| Postgres | 16.15, `fsync` on, `synchronous_commit` on, `shared_buffers` 128 MB, read committed |
| Commit latency | 0.44 ms, median of 200 one-row commits |

## Results

Goodput is the median of three repetitions, with the range in brackets.
Latency is from the median repetition.

| Contention | Step work | Arm | Goodput (runs/s) | P50 ms | P90 ms | Declined P90 ms |
|---|---|---|---|---|---|---|
| low | 0 ms | `backout` | **2970.8** [2944.9, 3118.6] | 4.5 | 6.8 | 5.4 |
| low | 0 ms | `saga` | 1947.1 [1907.2, 1986.0] | 6.6 | 9.1 | 13.6 |
| low | 0 ms | `hybrid` | 2334.5 [2233.5, 2370.3] | 5.4 | 8.3 | 9.9 |
| low | 20 ms | `backout` | **211.9** [209.0, 213.7] | 66.8 | 68.8 | 67.7 |
| low | 20 ms | `saga` | 201.2 [200.8, 201.5] | 70.6 | 72.4 | 75.1 |
| low | 20 ms | `hybrid` | 208.4 [207.7, 209.3] | 68.2 | 70.1 | 71.2 |
| high | 0 ms | `backout` | 515.8 [500.4, 517.1] | 19.4 | 60.9 | 66.3 |
| high | 0 ms | `saga` | 899.0 [898.7, 917.7] | 11.1 | 29.7 | 50.0 |
| high | 0 ms | `hybrid` | **972.3** [932.1, 982.5] | 10.1 | 29.0 | 47.0 |
| high | 20 ms | `backout` | 13.4 [13.2, 13.5] | 803.1 | 2271.7 | 2204.3 |
| high | 20 ms | `saga` | 39.0 [38.4, 39.5] | 250.8 | 712.1 | 1027.3 |
| high | 20 ms | `hybrid` | **39.8** [39.1, 39.8] | 226.4 | 694.2 | 1051.2 |

No run had an error. No backout transaction needed a conflict retry,
because every arm locks the SKU row before the account row.

| Contention | Step work | Rule pick | Branch | Best arm | Pick within 10 %? |
|---|---|---|---|---|---|
| low | 0 ms | `backout` | `ColdKey` | `backout` | yes |
| low | 20 ms | `backout` | `ColdKey` | `backout` | yes |
| high | 0 ms | `backout` | `CheapHold` | `hybrid` | **no** |
| high | 20 ms | `hybrid` | `CommutativeHotStep` | `hybrid` | yes |

## Findings

1. **Cold and short: backout wins.** At 0 ms, `backout` gives 1.53x the
   goodput of `saga` and 1.27x that of `hybrid`. It commits once and needs
   no compensation. A declined order costs one rollback, not two
   compensating commits, so its P90 is 5.4 ms against 13.6 ms. The paper
   reports up to 1.8x.
2. **Cold and long: the gap closes.** At 20 ms, step work dominates.
   `backout` gives 1.05x the goodput of `saga`.
3. **Hot and long: backout loses badly.** `backout` holds the hot row
   through all three steps, so the hot SKU serializes about 75 ms of work
   per run. `saga` and `hybrid` give 2.9x the goodput, and their P90 is
   3.2x lower. This matches the direction of the paper (15.9 against 2.0).
4. **Hot and short: backout still loses.** `backout` holds the hot lock
   through the debit, the place insert, the history insert, and a
   `SAVEPOINT` and a `RELEASE` per step. Each costs a round trip. The hot key then admits
   about one run per 1.94 ms. A `hybrid` run holds it for about 1.03 ms.
5. **The hybrid is never worse than the saga.** Its escrow step frees the
   hot row as early as a saga does. Its other steps back out, so it commits
   less and compensates less. In this workload it matches or beats `saga`
   in every cell.

## The selection rule

The rule was fixed in section 2.3 of `DESIGN-2012.md` before the
measurement. The first matching branch wins.

| # | Branch | Condition | Pick |
|---|---|---|---|
| 1 | `ExternalEffects` | A step writes outside the database. | `saga` |
| 2 | `LongHold` | The steps take longer than `MAX_HOLD` = 5 s. | `saga` |
| 3 | `ColdKey` | Fewer than `COLD_KEY_CONCURRENCY` = 1 run wants the hottest key at once. | `backout` |
| 4 | `CheapHold` | Backout holds the hot lock no longer than one more commit takes. | `backout` |
| 5 | `CommutativeHotStep` | The hot step is a bounded add or subtract. | `hybrid` |
| 6 | `Fallback` | None of the above. | `saga` |

Branch 3 uses Little's law. In an open system, the input is the arrival
rate on the key times the run duration. In this closed loop, it is the
clients over the SKUs: 0.016 at low contention and 16 at high.

### The change

Branch 4 failed. Its input, `hold_after_hot_step`, is the configured work
after the hot step: 0 ms in the hot, short cell. The real extra hold is
the round trips and savepoints that backout runs while it holds the hot
row. From the hot-key goodput, that is about 1.94 − 1.03 = 0.91 ms. That
is more than one commit (0.44 ms), so with this input branch 4 does not
match, and branch 5 picks `hybrid`, the best arm.

This check is post hoc and exploratory. A follow-up must measure the
input, not assume it. Harvest can time it: the backout runner can record
the time from the hot step's write to the commit.

## Limits

- **Engine cost.** The harness omits claims, workflow tasks and poll gaps.
  That favours the saga at low contention, so G1 is robust. Under a hot
  key it can go either way. A backout activity also writes its own engine
  rows while it holds the hot row. Poll gaps can make the saga client-bound
  before it is lock-bound.
- **The rule never picks `saga` here.** The workload has no external
  effect, a commutative hot step and a hold far under 5 s. Branches 1, 2
  and 6 are not measured.
- **Contention is configured, not observed.** It is 0.016 or 16, never near
  the threshold of 1. The threshold itself is not tested.
- **One host, one setting.** Postgres and the clients share 4 vCPU. One
  decline rate, 10 %, was tested. A higher rate favours backout. 20 ms is
  a short "long step".
- **Closed loop.** At most 16 runs wait, so the tail latency cannot reach
  the paper's 24.5 s P90. Do not compare the tails.
- **Pool.** The pool has four spare connections, so no client waits for
  one. Backout holds a connection about 3x longer per run. This spike
  does not measure that cost.
- **Compensations do no step work.** That slightly favours the saga.

## Reproduce

With a local Postgres in `HARVEST_TEST_DATABASE_URL`, or with Docker:

```text
cargo test --release -p autumn-harvest --features atomicity-spike \
  --test integration \
  atomicity_spike_tests::measure_the_full_matrix -- --ignored --nocapture
```

The run takes about 6 minutes. It prints both tables above and the
verdict. `atomicity_spike_tests` also holds the correctness tests that CI
runs: savepoint rollback, whole-run rollback, a deadlock retry, the undo
of each arm and the invariants of a short hot cell.

## Next steps

1. Build `backout` as an opt-in mode for a same-database workflow. Keep it
   one `run_transactional` activity with a savepoint per step.
2. Feed the rule with observed inputs: the hot-lock hold time from the
   runner, and the run rate per hot key.
3. Measure the arms as real Harvest workflows, to price the engine cost.
4. Add a cell with a hot step that does not commute, so the rule must pick
   `saga`.
5. Formal saga isolation levels, as the paper proposes, stay out of scope.
