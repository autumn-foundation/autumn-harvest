# Design — Issue #2011: Speculative durable execution, a DST spike

Issue #2011 asks one question. Can a worker run the next decision on
resident state while the previous commit flushes? The worker must discard
that decision if the commit or the claim-epoch fence fails.

The spike scope is the DST harness only, not the engine. The deliverable
is `docs/rnd/speculative-execution-spike.md`. It gives the measured
latency, the DST results and a go / no-go verdict.

**No engine change. No migration. No new `WorkflowEvent` variant. No
route change.** The DST harness gets one module, `dst::speculate`. The
e2e bench gets one opt-in probe, `HARVEST_BENCH_COMMIT_PROBE`.

---

## 0. Planning record

### 0.1 Facts found before the plan

- Decisions of one execution are strictly serial today. The worker awaits
  the persist transaction, `COMMIT` included, before any post-commit work
  (`worker.rs`, the `Persisted` arm).
- The next decision of an execution needs new input: an activity result, a
  timer or a signal. An activity starts only after the commit that schedules
  it. Its task row is written in that commit. Dispatch hints and `NOTIFY`
  go out after the commit (issue #1796).
- So, on a chain of sequential activities, no input is ready while a commit
  flushes. A gated speculation then has nothing to run.
- Resident state already exists (issue #1798). A warm resume skips replay.
- Terminal and paused commits check the claim epoch in the transaction
  (`claim_still_held_for_update`, issue #1789).
- The suspended-path commit does not check the claim epoch. It appends
  events from the event id read at the decision start. `UNIQUE
  (workflow_exec_id, event_id)` then rejects a commit whose history moved.
  That is a prefix check, not an owner check.
- The e2e bench measures throughput, dispatch latency, signal round trip
  and replay throughput. `HARVEST_BENCH_INFLIGHT=1` turns the closed loop
  into a measure of unloaded end-to-end latency.
- The bench workflow runs 3 sequential no-op activities. It has 4
  decisions and 3 activity hops.
- No metric gives decision compute time. Replay throughput gives an upper
  bound: a warm resume does less work than a cold replay.

### 0.2 Brainstorm — how can the spike answer the question?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Add speculation to `worker.rs` behind a feature flag. | Rejected. The issue limits the spike to the DST harness. |
| B2 | Extend the Postgres world of issue #2002 with a speculative worker. | Rejected. The world runs real `Worker`s. A speculative worker needs engine change. |
| B3 | A no-database, discrete-event model in `dst::speculate`. A seed fixes every duration and fault. | **Adopted.** It models time, which the oracle and world harnesses do not. A failure replays from its seed. |
| B4 | Three modes: `Serial` (today), `Gated` (speculate, release effects after commit), `Eager` (release effects before commit, as libDSE does). | **Adopted.** `Eager` shows what the commit gate costs and what it buys. |
| B5 | Two fences: `Epoch` (prefix and owner) and `PrefixOnly` (today's suspended path). | **Adopted.** It tests whether speculation needs the owner check. |
| B6 | Two logging modes: `Full` (reads and writes) and `ReadsOnly` (Halfmoon). Replay re-derives the writes. | **Adopted.** It counts the records that asymmetric logging saves. |
| B7 | A planted defect: a worker keeps resident state after a failed commit. | **Adopted.** It proves the invariants catch the repair-semantics risk. |
| B8 | Calibrate the model from the e2e bench. Measure commit time with a probe. | **Adopted.** The probe is opt-in and reports through the scenario notes. |
| B9 | Measure decision time with timestamps around claim and persist. | Rejected. Concurrent empty polls make the pairing unreliable. Replay throughput gives a clean bound. |

### 0.3 Reverse brainstorm — how can this spike mislead?

| # | How to make it misleading | Mitigation |
|---|---------------------------|------------|
| R1 | The model shows a gain that the engine cannot get. | The model takes its durations from the bench. Its `Serial` latency must match the measured unloaded latency within 15 %. |
| R2 | The model hides a safety bug. | Each invariant has a test that breaks it on purpose: the plant, `Eager` and `PrefixOnly`. |
| R3 | The workload has no ready input, so `Gated` looks useless by design. | A second workload has fan-out and signals. Inputs then arrive while a commit flushes. |
| R4 | A run is not a function of its seed. | Every seed runs twice. The traces must be equal. Golden hashes pin the model. |
| R5 | The probe changes the published numbers. | It is off by default. With it off, the bench keeps `NoOpMetrics`. |
| R6 | Measured numbers come from an untuned host. | The write-up names the host, the Postgres settings and both `fsync` modes. |
| R7 | The verdict follows the numbers that I want. | Section 0.5 fixes the go criteria before the measurement. |

### 0.4 Six thinking hats

- **White (facts).** Section 0.1. The bench workflow has 4 commits on its
  critical path. The published 1-shard throughput is 15.46 workflows/s at
  32 in flight.
- **Red (feelings).** A 10x claim is exciting. Harvest already gates
  effects behind commit, so the gain feels much smaller here.
- **Black (risks).** A speculative branch that consumes an input and then
  fails can strand that input. A stale owner can land a commit when only a
  prefix check guards it. Releasing an effect early runs it twice after a
  failed commit.
- **Yellow (benefits).** A clean answer saves engine work. The model is
  reusable for later pipelining or batching ideas.
- **Green (ideas).** Merge ready decisions into one commit, without
  speculation. Cut the post-commit wake hops, which may dominate.
- **Blue (process).** Plan, then red, green and refactor. Model first, with
  no database. Then measure. Then the write-up, its guard and review.

### 0.5 Go criteria, fixed before measurement

The verdict is **go** only if all three hold:

1. `Gated` cuts unloaded end-to-end latency of the bench workflow by 20 %
   or more, in the calibrated model.
2. `Gated` passes every invariant over the full sweep, under crashes,
   stalls and lost claims.
3. The gain does not need `Eager`. `Eager` breaks the commit gate of issue
   #1796, so a gain that needs it is a no-go.

## 1. The model

| Part | Meaning |
|---|---|
| Execution | `rounds` rounds. A round schedules `fanout` activities and waits for all results. Signals arrive at seeded times. |
| Decision | Consumes every durable input that resident state has not seen. Schedules the next round or completes. |
| Commit | Appends one decision record. It needs the decision prefix it read. Under `Epoch` it also needs the current claim. |
| Effect | One activity run. `EffectDone` writes the result as a durable input, if its schedule is durable. |
| Worker | Holds claims, resident state and a commit chain. A crash drops all three. A stall delays its events. |
| Reclaimer | Moves the claim of a dead or stalled worker. The epoch goes up. The new owner loads cold. |

Durations are in virtual microseconds: decide, commit, dispatch hop,
activity and wake hop. A seed draws each commit and activity time from a
range.

### 1.1 Invariants

| Invariant | Meaning |
|---|---|
| `CommitByOwner` | An applied commit comes from the current claim holder. |
| `ReplayEquivalent` | Replay of the durable log from empty state gives each decision record that the log holds. |
| `EffectAfterCommit` | An effect starts only after its schedule is durable. |
| `EffectOnce` | An effect runs at most once. |
| `ResultOnce` | The log holds at most one result for each effect. |
| `ExpectedOutput` | A completed execution returns the value that its workload defines. |
| `Converges` | Every execution completes before the event limit. |

## 2. Test plan (red, green, refactor)

1. Red: `tests/dst/speculate.rs`. Config, run twice, goldens, invariants,
   plant, `Eager` and `PrefixOnly` results, latency order and stats.
2. Green: `src/dst/speculate.rs`.
3. Red, then green: the probe parser test in `e2e_bench_support`.
4. Measure on local Postgres 16, `fsync` on and off.
5. Red: `speculative_execution_docs` guard. Green: the write-up.
6. Refactor, review, CI step on the `lint` job.

## 3. Acceptance criteria

| Criterion | Evidence |
|---|---|
| A spike write-up under `docs/rnd/`. | `docs/rnd/speculative-execution-spike.md`. |
| It gives measured latency. | Its measurement section, with the probe and bench numbers. |
| It gives the DST results. | Its DST section, with the sweep summary and the cited tests. |
| It gives a go / no-go verdict. | Its verdict section, judged on section 0.5. |
