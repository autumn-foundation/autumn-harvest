# Design — Issue #2013: typed state snapshots (R&D spike)

Issue #2013 asks one question. If a run resumes from a typed state snapshot
and not from a full replay, the code before the snapshot point no longer
needs to be deterministic. Is that worth building?

The issue gates the spike on the resident-state hit-rate data of issue
#2007. That data did not exist. This change therefore does two things:

1. **The gate (#2007).** A counter for each decision: did it resume a
   resident workflow, or did it replay? A miss names its reason. Two
   measurements use the counter.
2. **The spike (#2013).** A test-only prototype and a write-up in
   `docs/rnd/` with a design sketch and a go / no-go verdict.

**No migration. No new `WorkflowEvent` variant. No route change.** The gate
adds one counter and one `MetricsRecorder` method with a no-op default. The
spike adds no engine code.

---

## 0. Planning record

### 0.1 Facts found before the plan

- Issue #2007 is open and has no pull request. No hit-rate data exists.
- `resident.rs` already knows each reason. `ResumeDeclined` names each
  decline. `plan_suspension` and `resident_blocker` refuse a suspension, but
  they return `None` or a log string. Nothing counts either.
- `harvest.workflow.cache_hit` counts a cache hit. It does not tell a resume
  from a replay (`docs/sticky-routing.md`).
- The DST world infers a warm decision from a body-start counter. It has no
  direct signal.
- The e2e bench workflow runs three activities in sequence. The bench signal
  workflow waits for one signal. Both use `NoOpMetrics`.
- `agent_loop` (`autumn-harvest-agent`) runs model turns and tool calls in
  sequence. An approval gate is `receive_signal_timeout`: a timer and a
  signal in one suspension. A follow-up waits on a timer.
- The SQLite runtime has no resident path. Only the Postgres worker has one.
- Activity ids are random UUIDs. Replay matches commands by position and
  takes the id from history.
- Reserved names come from context counters, for example
  `__signal_timeout:{seq}:{name}`, `race:{seq}` and `saga_compensated:{seq}`.
  Each new context starts these counters at zero.
- Continue-as-new starts a new run with the state as input. It carries
  unconsumed signals, memo, search attributes, headers and the assigned build.
  It resets the history.
- The keyed entity (#1975) already checkpoints typed state through
  continue-as-new. It adds no event variant.
- The upgrade verdict (#1995) already checks rehydration of stored payloads
  under a candidate build.
- A new event variant must stay readable by build N-1. It ships off by
  default first (`docs/upgrading/README.md`).

### 0.2 Brainstorm — how can the gate measure the hit rate?

| # | Idea | Verdict |
|---|------|---------|
| G1 | Infer a resume from `harvest.replay = false` on spans. | Rejected. Spans are sampled, and a decline has no span attribute. |
| G2 | Log each decline at `info`. | Rejected. Logs are not a rate. |
| G3 | Two counters: one at capture, one at resume. | Rejected. Two counters need a join to give one rate. |
| G4 | One counter per decision, `outcome` = `hit` or `miss`, `reason` bounded. The cache entry keeps why its suspension did not stay resident. | **Adopted.** One series gives the rate. The reason at capture reaches the next decision through the cache. |
| G5 | Count in `ResidentWorkflow::resume_with`. | Rejected. A cold miss never reaches it. |
| G6 | Measure the agent loop on the SQLite runtime. | Rejected. That runtime has no resident path. |
| G7 | Measure the real `agent_loop` on a Postgres worker in a core integration test. The core crate takes the agent crate as a path dev-dependency. | **Adopted.** It measures the real loop, not a copy. |

### 0.3 Brainstorm — what can the spike prototype?

| # | Idea | Verdict |
|---|------|---------|
| S1 | Serialize the parked handler future. | Rejected. Rust cannot serialize an arbitrary future. |
| S2 | A new `StateSnapshot` event and an in-place resume of the same run. | Sketched, not built. It needs a new variant, a new resume path and saved context counters. |
| S3 | A typed snapshot as the input of a continue-as-new successor, with a version and an effect ledger. | **Prototyped** in tests. It needs no engine change, so the spike can test its claims. |
| S4 | Rewrite the history prefix in place. | Rejected. `harvest_events` is append-only. |
| S5 | Snapshot agent memory only, for model recovery. | Noted. The agent memory snapshot activity covers part of it. It is an app concern. |

### 0.4 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | An unbounded label, such as the event type, explodes series. | Each reason is a fixed `&'static str` from a closed list. A test checks the list. |
| R2 | A decision counts twice, for example after a local-activity loop. | The worker counts only the first drive of a decision. A test checks hits + misses = decisions. |
| R3 | A requeued task counts a decision that never ran. | The count happens at the drive, after the requeue paths. |
| R4 | The cache entry grows. | The reason is a one-byte enum. |
| R5 | A custom recorder breaks. | The new method has a no-op default. |
| R6 | The measurement test is flaky in CI. | It asserts exact outcomes on one worker, not rates. The rates go in the write-up. |
| R7 | The dev-dependency drags agent code into the engine build. | It is a dev-dependency only, by path. The published crate does not depend on it. |
| R8 | The spike reads as a commitment. | The write-up states the verdict and what would change it. The prototype lives under `tests/`. |
| R9 | The prototype claims more than it proves. | Each claim names its test. A docs guard checks the names. |
| R10 | A snapshot drops a completed effect. | The ledger check refuses a snapshot taken with an open effect. |
| R11 | An old snapshot loads under new code with the wrong shape. | The load refuses an unknown version and upgrades a known one. |

### 0.5 Six thinking hats

| Hat | Notes |
|-----|-------|
| White | The decline reasons exist. No counter exists. The agent loop is sequential, with race-shaped approval gates. Continue-as-new and the entity already move typed state. |
| Red | A new event variant for an unproven gain feels risky. Teams want upgrades without patch markers. |
| Black | An in-place snapshot needs saved counters, a new variant and a new resume path. A snapshot point must be quiescent, which narrows where it can go. |
| Yellow | The counter is cheap and answers two bets. The prototype shows a typed successor removes the determinism need before the checkpoint. |
| Green | Version the entity checkpoint. Add the ledger check to the upgrade verdict. Count resident outcomes in the DST world later. |
| Blue | Red: reason tests, worker counter tests, docs guards, spike tests. Green: counter, measurement, prototype, write-up. Refactor: docs, review, AC evidence. |

---

## 1. Change — the gate (#2007)

- `resident.rs`:
  - `NotKept`: why a suspension did not stay resident.
  - `ResidentMiss`: why a decision replayed. It wraps `NotKept` and
    `ResumeDeclined`, and adds `Cold`, `Disabled`, `HotSwap` and `Gap`.
  - `ResidentMiss::label` and `NotKept::label` give the bounded reason.
  - `capture` returns `Result<Self, NotKept>`.
- `context.rs`: `resident_blocker` returns `Option<NotKept>`.
- `executor.rs`: `DriveResult` carries `not_kept`.
- `cache.rs`: an entry keeps the `NotKept` reason of its suspension.
- `worker.rs`: one `record_workflow_resident` call for each decision.
- `telemetry.rs`: `METRIC_WORKFLOW_RESIDENT` and the recorder method.
  `metrics_rs_adapter.rs` emits it.
- `docs/telemetry.md` and `docs/sticky-routing.md` list the counter and
  each reason.

## 2. Change — the measurement

- `e2e_bench_support.rs`: a counting recorder replaces `NoOpMetrics`. The
  report prints the resident outcomes of each scenario.
- `tests/integration/resident_hit_rate_db_tests.rs`: runs the real
  `agent_loop` on one worker with a scripted model. It asserts the exact
  outcome of each decision.

## 3. Change — the spike (#2013)

- `tests/integration/typed_snapshot_spike_tests.rs`: the prototype.
  - `EffectLedger::from_history`: refuses an open effect.
  - `Snapshot<S>`: a versioned save and load with an upgrade step.
  - A resume under changed code before the checkpoint.
- `docs/rnd/typed-state-snapshots.md`: the write-up and the verdict.
- `tests/integration/typed_snapshot_docs.rs`: guards the write-up.

## 4. Tests (red first)

| Test | Proves |
|------|--------|
| Each ineligible suspension names its `NotKept` reason. | The reason is exact. |
| Each label is unique and in snake case. | R1. |
| Each decline maps to its label. | Decline reasons reach the counter. |
| A warm run counts `miss/cold`, then `hit` for each later decision. | The counter on a real worker. |
| Two signals in one delta count `miss/extra_events`. | Declines reach the counter. |
| Sticky off counts `miss/cold` for each decision. | Cold misses. |
| Resident off counts `miss/disabled`. | Disabled misses. |
| A join counts `miss/multi_await` on the next decision. | The capture reason reaches the next decision. |
| `agent_loop` counts a hit for each sequential turn and `miss/race` after an approval gate. | The agent-loop measurement. |
| A snapshot with an open activity, timer or child is refused. | R10. |
| A v1 snapshot loads under v2. An unknown version is refused. | R11. |
| Changed code before the checkpoint fails a full replay but resumes from the snapshot. | The spike claim. |
| A resume runs no completed activity again. | Preservation. |
| The write-up holds a verdict, the measured rates and the test names. | R9. |

## 5. Out of scope

- An in-place snapshot event and resume path (S2). The write-up sketches it.
- A change to the entity checkpoint wire form.
- Any generalization of the resident path (#2008).
