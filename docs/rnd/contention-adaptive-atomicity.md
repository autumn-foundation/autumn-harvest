# Contention-adaptive atomicity: physical backout, saga or hybrid (issue #2012)

**Status:** R&D spike, behind the `atomicity-spike` feature.
**Measured:** 2026-10-11, code at revision `f6893ac`.
**Plan:** [`DESIGN-2012.md`](../../DESIGN-2012.md). Commit `62f99f0` added
its rule and its criteria before the code.

## Question

A workflow can keep all its effects in the Harvest Postgres. Can Harvest
then pick physical backout or a saga per workflow, from the contention
that it observes?

The source is the CIDR 2026 "AC/DC" paper that issue #2012 cites. Below,
"the paper" means that paper.

## Verdict

**Verdict: Go with changes.**

Physical backout is worth building for cold, short, same-database
workflows. There it gives 1.55x the goodput of a saga. Under a hot key, a
saga or a hybrid wins by up to 3x. The rule picks the best arm in three of
four cells. It needs one change before a follow-up builds it. The "extra
hold" input must be the observed hot-lock hold, not the configured step
time. With that input, the rule picks the best arm in all four cells. See
[The change](#the-change).

| Criterion | Text (pre-registered) | Result |
|---|---|---|
| G1 | `backout` has the highest goodput in both low-contention cells. | **Holds**, narrowly. At 20 ms the `backout` range starts 0.1 runs/s above the `hybrid` range. In an earlier run of the same harness, the two ranges overlapped. Treat that cell as a tie with `hybrid`. `backout` beats `saga` there in both runs. |
| G2 | `backout` does not have the highest goodput in the hot, long-step cell. | **Holds.** `saga` and `hybrid` give 2.9x and 3.0x the goodput of `backout`, with no overlap. |
| G3 | In every cell, the rule picks an arm within `TIE_BAND` = 10 % of the best goodput. | **Fails** in the hot, 0 ms cell. The rule picks `backout`, which gives 56 % of the best goodput. |
| G4 | Every arm keeps both invariants in every cell. | **Holds.** In all 36 runs, stock taken equals orders, and money taken equals order totals. |

`atomicity::verdict::judge` computes these values from the measured cells.
G1 does not fail, G4 holds and G3 fails, so the outcome is "go with
changes".

### What a "go" commits the team to

- A follow-up issue builds backout as an opt-in mode. It is one
  `run_transactional` activity with a savepoint per step. The runner here
  is about 80 lines of code, so the cost is in the integration, the docs
  and the tests.
- The follow-up must measure the rule inputs, not assume them. The
  maintainers decide whether Harvest picks the mode or the author does.
- The spike does not answer the question in full. Its contention was
  configured, not observed. A follow-up must observe it.

## What the spike built

All code is in `autumn_harvest::atomicity`. It is `#[doc(hidden)]`, it is
behind the `atomicity-spike` feature, and no production path calls it.

- **`backout::run_backout`** runs a body in one transaction.
  **`Steps::step`** runs each step in a nested `diesel-async`
  transaction, so Diesel issues one `SAVEPOINT` per step. A failed step
  rolls back to its savepoint. The body then propagates the error, which
  rolls back the run, or goes on. A deadlock or a serialization abort
  makes the whole run retry through `tx_retry::run_with_conflict_retry`.
  This holds even when the body ignores the step error. The runner adds
  no SQL of its own.
- **`harness`** runs one order workflow three ways. A run reserves one unit
  of stock (the hot step), debits an account and places the order. The
  place step declines 10 % of orders, which forces an undo.
- **`rule::choose`** is the selection rule. **`verdict::judge`** applies
  G1 to G4.

| Arm | Transactions per run | Undo |
|---|---|---|
| `backout` | 1, with a savepoint per step | `ROLLBACK` |
| `saga` | 1 per step, through the real `saga::Saga` helper | Compensations, last first |
| `hybrid` | 2: the reserve step commits alone as an escrow step, then debit and place back out together | `ROLLBACK`, then the restock compensation |

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
| Hot hold | The time from the hot write to the commit that releases the hot row, for a committed run |
| Host | 4 vCPU Xeon at 2.1 GHz, 15 GB RAM. The harness and Postgres share the host. |
| Postgres | 16.15, `fsync` on, `synchronous_commit` on, `shared_buffers` 128 MB, read committed |
| Commit latency | 0.40 ms, median of 200 one-row commits |

## Results

Goodput is the median of three repetitions, with the range in brackets.
The other columns come from the median repetition.

| Contention | Step work | Arm | Goodput (runs/s) | P50 ms | P90 ms | Declined P90 ms | Hot hold P50 ms |
|---|---|---|---|---|---|---|---|
| low | 0 ms | `backout` | **3004.3** [2910.5, 3026.5] | 4.4 | 6.8 | 5.2 | 3.23 |
| low | 0 ms | `saga` | 1943.6 [1866.6, 1993.8] | 6.6 | 9.2 | 13.7 | 1.21 |
| low | 0 ms | `hybrid` | 2276.9 [2207.9, 2355.0] | 5.7 | 8.2 | 10.2 | 1.21 |
| low | 20 ms | `backout` | **211.1** [208.8, 212.4] | 67.1 | 68.8 | 67.4 | 66.18 |
| low | 20 ms | `saga` | 200.5 [200.0, 202.7] | 71.0 | 72.6 | 75.0 | 22.52 |
| low | 20 ms | `hybrid` | 207.6 [206.6, 208.7] | 68.4 | 70.2 | 71.1 | 22.23 |
| high | 0 ms | `backout` | 543.3 [503.4, 548.9] | 18.7 | 58.9 | 60.0 | 1.60 |
| high | 0 ms | `saga` | 921.5 [894.7, 921.8] | 10.9 | 29.3 | 48.7 | 0.83 |
| high | 0 ms | `hybrid` | **963.6** [953.6, 966.5] | 9.9 | 29.8 | 47.0 | 0.79 |
| high | 20 ms | `backout` | 13.3 [13.3, 14.0] | 746.0 | 1959.2 | 1827.4 | 67.61 |
| high | 20 ms | `saga` | 38.6 [38.1, 38.9] | 252.5 | 654.8 | 999.8 | 22.61 |
| high | 20 ms | `hybrid` | **39.9** [39.5, 40.2] | 267.2 | 652.2 | 969.4 | 22.33 |

No run had an error. No backout transaction needed a conflict retry. Every
arm locks the SKU row before the account row, so no lock cycle can form.
The zero retries say nothing more than that.

| Contention | Step work | Rule pick | Branch | Best arm | Pick within 10 %? |
|---|---|---|---|---|---|
| low | 0 ms | `backout` | `ColdKey` | `backout` | yes |
| low | 20 ms | `backout` | `ColdKey` | `backout` | yes |
| high | 0 ms | `backout` | `CheapHold` | `hybrid` | **no** |
| high | 20 ms | `hybrid` | `CommutativeHotStep` | `hybrid` | yes |

An earlier run of the same matrix, at revision `e7bb735`, gave the same
best arm and the same G3 failure in each cell. Its goodputs were within 6 %
of the ones above. Its low, 20 ms cell had overlapping `backout` and
`hybrid` ranges.

## Findings

1. **Cold and short: backout wins.** At 0 ms, `backout` gives 1.55x the
   goodput of `saga` and 1.32x that of `hybrid`. It commits once and needs
   no compensation. A declined order costs one rollback, not two
   compensating commits. Its P90 is 5.2 ms against 13.7 ms. The paper
   reports up to 1.8x.
2. **Cold and long: the gap closes.** At 20 ms, step work dominates.
   `backout` gives 1.05x the goodput of `saga` and ties with `hybrid`.
3. **Hot and long: backout loses badly.** `backout` holds the hot row
   through all three steps: 60 ms of step work, 67.6 ms with round trips.
   `saga` and `hybrid` hold it for 22.5 ms. They give 2.9x and 3.0x the
   goodput, and their P90 is 3.0x lower. The direction matches the paper,
   which reports a saga goodput of 15.9 against 2.0 for backout.
4. **Hot and short: backout still loses.** `backout` holds the hot row
   through the debit, the place insert and the history insert. It also
   holds it through a `SAVEPOINT` and a `RELEASE` per step. Each costs a
   round trip. The measured hot hold is 1.60 ms, against 0.79 ms for
   `hybrid`.
5. **In this workload, the hybrid is never worse than the saga.** Its
   escrow step frees the hot row as early as a saga does. Its other steps
   back out, so it commits less and compensates less.

## The selection rule

Section 2.3 of `DESIGN-2012.md` fixed the rule before the measurement.
Section 3.1 later amended the text of one input, `hold_after_hot_step`, to
match the code. The amendment changes no pick. The first matching branch
wins.

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
after the hot step. In the hot, short cell that is 0 ms. The measured
extra hold is the backout hot hold minus the saga hot hold:

| Contention | Step work | Measured extra hold | Commit latency | Branch 4 matches? | Pick |
|---|---|---|---|---|---|
| low | 0 ms | 2.02 ms | 0.40 ms | not reached | `backout` (`ColdKey`) |
| low | 20 ms | 43.66 ms | 0.40 ms | not reached | `backout` (`ColdKey`) |
| high | 0 ms | 0.77 ms | 0.40 ms | no | `hybrid` |
| high | 20 ms | 45.00 ms | 0.40 ms | no | `hybrid` |

With the measured input, the rule picks the best arm in every cell. This
check is post hoc: the input comes from the same runs that it corrects. A
follow-up must test it on new runs. Harvest can measure the input from
either mode. The extra hold is the time of the steps after the hot step,
with their round trips.

## Limits

- **Engine cost.** The harness omits claims, workflow tasks and poll gaps.
  That favours the saga at low contention, so G1 is robust there. Under a
  hot key it can go either way. A backout activity also writes its own
  engine rows while it holds the hot row. Poll gaps can make the saga
  client-bound before it is lock-bound.
- **Uncached statements.** The harness uses `diesel::sql_query`, which
  Diesel does not cache. So each statement pays an extra prepare round
  trip. Inside the hot hold, `backout` pays three of them and `saga` pays
  one. Production code with cached statements would shrink the backout
  hold. The measured extra hold is about twice the commit latency, so the
  finding stands. The margin is smaller than it looks.
- **The rule never picks `saga` here.** The workload has no external
  effect, a commutative hot step and a hold far under 5 s. The matrix
  does not reach branches 1, 2 and 6.
- **Contention is configured, not observed.** It is 0.016 or 16, never near
  the threshold of 1. No cell tests the threshold.
- **Small samples.** Each cell has 3 repetitions and no statistical test.
  The ranges and the earlier run are the only checks on noise.
- **One host, one setting.** Postgres and the clients share 4 vCPU. The
  spike tests one decline rate, 10 %. A higher rate favours backout. 20 ms
  is a short "long step".
- **Closed loop.** At most 16 runs wait, so the tail latency cannot reach
  the paper's 24.5 s P90. Do not compare the tails.
- **Pool.** The pool has four spare connections, so no client waits for
  one. Backout holds one connection for its whole run. A saga holds one per
  step. Each backout checkout is about 3x longer. This spike does not
  measure that cost.
- **Compensations do no step work.** That slightly favours the saga.

## Production risks of backout

- A long transaction holds a row lock, a pool connection and the vacuum
  horizon. Branch 2 caps the steps at 5 s, the `run_transactional` limit.
- Rollback cannot undo an effect outside the database. Branch 1 sends such
  a workflow to a saga.
- A step must return the database error as it is. A step that catches a
  conflict hides it, and the run commits without a retry.

## Reproduce

Use a local Postgres in `HARVEST_TEST_DATABASE_URL`, or Docker:

```text
cargo test --release -p autumn-harvest --features atomicity-spike \
  --test integration \
  atomicity_spike_tests::measure_the_full_matrix -- --ignored --nocapture
```

The run takes about 6 minutes. It prints the server settings, both tables
above and the verdict. CI runs the correctness tests in
`atomicity_spike_tests`. They cover savepoint rollback, whole-run rollback
and a deadlock retry. They also cover the undo of each arm and the
invariants of a short hot cell.

## Next steps

1. Build `backout` as an opt-in mode for a same-database workflow. Keep it
   one `run_transactional` activity with a savepoint per step.
2. Feed the rule with observed inputs: the extra hot hold from the runner,
   and the run rate per hot key.
3. Measure the arms as real Harvest workflows, to price the engine cost.
4. Add a cell with a hot step that does not commute, so the rule must pick
   `saga`. Add cells near the cold-key threshold.
5. Formal saga isolation levels, as the paper proposes, stay out of scope.
