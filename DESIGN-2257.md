# Design: issue #2257, the stage 2 gate

Stage 2 of epic #2247 is an owner-local disk cache (#2258). It ships only
if cold decisions are common or slow. The typed-state-snapshot spike set
the thresholds (`docs/rnd/typed-state-snapshots.md`, section 5):

- `harvest.workflow.resident{outcome="cold"}` above 20% of decisions for a
  week, or
- a cold p99 above 50 ms.

This issue builds the instruments for that gate and records the bench half
of it. **No migration. No new `WorkflowEvent` variant. No change to the
decision path.**

---

## 0. Planning record

### 0.1 Facts found before the plan

- The counter `harvest.workflow.resident{workflow, queue, outcome}` (#2114)
  gives the cold share. It records once per decision, after the first
  drive.
- **No metric gives a cold p99.** `harvest.workflow.duration` has no
  `outcome` label. It also starts after the history load, so it misses
  the cost that a disk cache saves.
- The cold path reads the full history from Postgres **twice**:
  `load_history_inflated` for `next_event_id`, then
  `load_history_inflated_with_bytes` after the wake ingest
  (`worker.rs`, `load_workflow_replay_state`). Both decode every event.
- `WorkflowCache` holds 1,000 entries by default. A fleet owner of more
  live runs than that replays cold after each eviction.
- `autumn-harvest-sqlite` stores each event as JSON text in
  `harvest_events(exec_id, seq, event_json)`. Its `load_history` is a
  working local SQLite history read.
- `examples/agent-loop-hit-rate` (#2007) is the precedent for a
  measurement crate: a workspace member, unit tests in CI, the measurement
  by hand.
- The dashboard guard fails when a `METRIC_*` constant has no panel.

### 0.2 Brainstorm: what does the gate need?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Read the cold share from the existing counter. | **Adopted.** It needs no code. |
| B2 | A new histogram `harvest.workflow.resident.duration{workflow, queue, outcome}`. It times the history load and the first drive of each decision. | **Adopted.** It gives the cold p99 on a fleet. |
| B3 | Add `outcome` to `harvest.workflow.duration`. | Rejected. That metric starts after the load. A label change also breaks dashboards. |
| B4 | A histogram of the history load only. | Rejected for now. B2 covers the load. The bench splits load from replay. |
| B5 | A bench in `autumn-harvest/benches`. | Rejected. The core crate cannot depend on `autumn-harvest-sqlite`. |
| B6 | A measurement crate `examples/history-load-gate`. It seeds a history through the real append paths of both stores and times the real load paths. | **Adopted.** |
| B7 | Seed 100k runs through the start API. | Rejected. Too slow. The crate clones one template run in SQL. |
| B8 | Time the owner restart as the cold load of every live run. | **Adopted.** After a restart, each live run replays cold once. |
| B9 | Apply the thresholds in code: `Thresholds` and a verdict function. | **Adopted.** One trip gives `go`. Complete data with no trip gives `no-go`. Anything else is `undecided`. The tests pin the edges. |
| B10 | A dashboard panel with both gate lines. | **Adopted.** The guard needs a panel anyway. |
| B11 | A Prometheus recording-rule pack for the gate. | Rejected. The record doc gives the two 7-day queries. A rule pack needs its own guard suite. |
| B12 | Remove the first full read of the cold path. | Rejected here. It changes the decision path. It is a follow-up finding. |
| B13 | Drive the real `WorkflowCache` over more live runs than its cap. | **Adopted.** Under owner mode, the cap alone sets a floor for the cold share. |

### 0.3 Reverse brainstorm: how can the gate mislead?

| # | How to make it wrong | Counter-measure |
|---|----------------------|-----------------|
| H1 | Time only the drive, so a disk cache looks useless. | B2 starts the clock before the history load. |
| H2 | Time different work for each store. | Both sides load the same events and decode them into `WorkflowEvent`. |
| H3 | Compare a warm SQLite page cache with a cold Postgres. | Both stores run warm. The record says so, and says that both stay warm after an owner restart. |
| H4 | A seed that does not decode. | A unit test round-trips the generated events. Each timed load checks the event count. |
| H5 | One sample per size. | Each size runs many reps. The crate prints p50 and p99. |
| H6 | A verdict from bench data alone. A bench worst case posed as a fleet p99 always says go. | Only fleet values feed the verdict. With no trip and a missing value, it reads `undecided`. The bench prints the history size that passes 50 ms. |
| H7 | Buckets too coarse to resolve 50 ms. | The telemetry docs ask for a bucket bound at 0.05 s. |
| H8 | The histogram adds cost to each decision. | One `Instant` and one record per decision, next to the counter. The no-op default costs nothing. |
| H9 | High label cardinality. | Same labels as the counter, all bounded. |
| H10 | A partial run counts as a cold sample. | The worker records the duration with the counter, once per decision attempt. |
| H11 | The seed leaks into a shared database. | The crate tags its runs and deletes them at the end. |
| H12 | A threshold edge flips the verdict. | The rule is strict `>`, as the spike wrote it. Tests pin 20% and 50 ms exactly. |

### 0.4 Six thinking hats

- **White (facts).** Section 0.1. Fleet data needs a production fleet for
  a week. A container cannot supply it.
- **Red (gut).** A disk cache feels like a solution in search of a
  problem. The bench must be able to show that it is not.
- **Black (risks).** Section 0.3. The main risk is a biased comparison.
  H1 to H5 answer it.
- **Yellow (value).** One histogram makes the gate an operator query.
  The crate makes the load comparison repeatable on any host.
- **Green (ideas).** The double read in the cold path (B12) is a cheaper
  win than a disk cache. Record it as a finding.
- **Blue (process).** Red tests first: the histogram bridge, the worker
  samples and the crate unit tests. Then the code until green. Then the
  measurement, the record doc and the gates. Then a review from several
  angles.

## 1. Change

1. `telemetry.rs`: `METRIC_WORKFLOW_RESIDENT_DURATION` and
   `MetricsRecorder::record_workflow_resident_duration`, a no-op default.
2. `metrics_rs_adapter.rs`: the histogram bridge.
3. `worker.rs`: start the clock before the history load. Record the
   duration with the counter.
4. `examples/history-load-gate`: seed, time and print the Postgres and
   SQLite loads and the restart warm-up. Model the cold share of the
   cache cap. Print the verdict from the fleet values.
5. The dashboard panel, the telemetry rows, CI steps for the new crate, a
   changelog fragment and the record doc
   `docs/rnd/2026-10-22-stage-2-gate.md`.

## 2. Tests

| Test | Proves |
|------|--------|
| `bridges_workflow_resident_duration_with_outcome_labels` | The bridge records a histogram with the counter labels. |
| `a_sequential_run_records_a_duration_per_decision` | The worker records one duration per outcome, in order, each above zero. |
| `a_worker_with_resident_state_off_records_no_outcome` | Resident state off records no duration either. |
| crate unit tests | Percentiles, the verdict edges, the seed round trip, the SQLite load and the cache model. |
| crate Postgres tests | The seed and clone paths load back every event. |
| dashboard guards | The new metric has a panel and valid queries. |

## 3. Out of scope

- The fleet week. An operator runs it with the panel and the queries in
  the record doc.
- The go or no-go on #2258 after the fleet week.
- Removing the first full read of the cold path (B12).
