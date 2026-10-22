# Design: issue #2248, baseline the claim split, the persist phases and the fence cost

Stage 1 of epic #2247. The case for a decision owner extrapolates from the
formula `1 / (claims × claim latency)`. This issue measures the three inputs
that the formula does not have: the claim split by task kind, the phases of
a decision persist and the cost of the second fence row.

**No migration. No new event. No change to the claim or persist SQL.** The
change adds a `kind` label to claim samples, a phase metric for the decision
persist, a criterion bench and a performance page. One fix removes a second
owner fence that DR fencing caused.

---

## 0. Planning record

### 0.1 Facts found before the plan

- `harvest.db.query.duration{op, shard}` times claims and persists (#1815).
  The worker records a claim at three sites in `worker.rs`. Each claim
  returns one task row, with `TaskType::Workflow` or `TaskType::Activity`,
  or none.
- The persist timer `PersistTimer` spans the whole decision transaction,
  `COMMIT` included. A task-local `PersistTiming` scopes the cycle.
- Inside the persist, event rows go through `store::insert_events`. Task
  rows go through `queue::insert_task_rows`. Each wake goes through
  `notify::stage_now`. The fences are `replication::assert_generation` and
  `decision_owner::fence`. No other insert runs in a decision.
- Since #1796 the `NOTIFY` is post-commit. In the transaction, the notify
  phase is the stamp probe or, with no sender, the `pg_notify` fallback.
- The assay 0014 apparatus pins no DR fence and runs no owner mode. Its
  persist therefore takes no fence row. The fence cost needs its own bench.
- The owner row and its fence landed in #2251. The bench can use the real
  row and the real `harvest_decision_owner_fence` function. No prototype
  row is needed.
- The owner fence takes the row `FOR KEY SHARE`, not `FOR SHARE`. The
  generation fence takes its row `FOR SHARE`.
- With DR fencing on, `store::insert_event_rows` calls
  `replication::assert_fence` for each append. That calls
  `decision_owner::fence_pinned`, which does not check the fenced scope.
  So an owner-mode decision with DR on sends the owner fence once more for
  each append. CLAUDE.md says that a nested fence sends no query (#2252).
- The metrics-rs adapter and the plugin scrape endpoint implement
  `record_db_query_duration`. The dashboards and alerts sum by
  `(instance, op, shard)`, so a new label does not split their series.

### 0.2 Brainstorm: how can each number be measured?

| # | Idea | Verdict |
|---|------|---------|
| B1 | A new `DbOp` variant per claim kind. | Rejected. `op` counts work units. A claim is one unit. |
| B2 | A `kind` label on claim samples, through a new trait method that defaults to the old one. | **Adopted.** Old recorders still see every claim. |
| B3 | Classify each SQL statement with diesel instrumentation in the apparatus. | Rejected. An activity result also appends events, so a statement cannot tell a decision persist from another write. |
| B4 | `tracing` spans around each phase. | Rejected. A span needs a subscriber to sum it, and the apparatus reads `MetricsRecorder`. |
| B5 | Phase timers at the chokepoints that add into `PersistTiming`: eight sites in five files. | **Adopted.** One timer per chokepoint covers every call site. |
| B6 | Commit time as the time from the end of the transaction body to the end of the timer. | **Adopted.** Diesel sends `COMMIT` in that gap only. |
| B7 | Record `other` as the total minus the measured phases. | **Adopted.** The phases of one persist then add up to its total. |
| B8 | Phases for every persist timer, the failure writes included. | Rejected. A failure write has no body marker, so its commit is unknown. Phases cover the main decision persist only. |
| B9 | A criterion bench on a model transaction made of public engine calls. | **Adopted.** Criterion repeats an append safely: each run adds rows. |
| B10 | A prototype owner row in the bench. | Rejected. The real row exists since #2251. |
| B11 | Bench writers at 1 and 8, so the row locks become multixacts. | **Adopted.** The issue asks about high decision rates. |
| B12 | Run the assay 0014 apparatus with a patch, as the claim-concurrency page did. | **Adopted.** The shape stays byte-for-byte the assay's. |
| B13 | Skip the owner fence in `fence_pinned` inside a fenced scope. | **Adopted.** It is the rule of #2252, and the bench measures it. |

### 0.3 Reverse brainstorm: how can this change cause harm?

| # | How to make it harmful | Counter-measure |
|---|------------------------|-----------------|
| H1 | Old recorders lose claim samples. | The new method defaults to `record_db_query_duration(DbOp::Claim, ..)`. A test proves it. |
| H2 | A dashboard splits the claim series by kind. | The shipped queries sum by `op` and `shard`. The page says so. |
| H3 | The label grows without bound. | `ClaimKind` has four values. The type bounds them. |
| H4 | The phase timers slow the persist. | A timer is two clock reads and a `Cell` write. A persist takes milliseconds. |
| H5 | A nested phase counts twice. | One phase runs at a time. A nested timer records nothing. |
| H6 | Phases leak into a later persist. | The persist timer clears the sums when it starts. |
| H7 | A timer outside a persist panics. | The task-local read uses `try_with`. Outside a cycle it does nothing. |
| H8 | `other` reads negative from clock jitter. | `other` is clamped at zero. A property test proves the bounds. |
| H9 | The fence fix skips a needed fence. | It skips only the same connection and the same tenure, inside the scope. The start fence holds the row. Existing #2252 tests and a new DR test prove it. |
| H10 | The bench pins DR for the whole process. | The bench is its own process. It runs the variants with no pin first. |
| H11 | The bench times an empty table or a cold cache. | Each writer owns a seeded execution. Criterion warms up first. |
| H12 | The page quotes a live count. | The page states ratios and dated tables. No guard reads a count. |

### 0.4 Six thinking hats

- **White (facts).** Section 0.1. The assay saw 7 claims per workflow, loop
  occupancy 0.91 to 0.99 and a persist of 4 to 6 ms.
- **Red (gut).** A model transaction in a bench feels less real than the
  worker. The real fence and append calls make it close enough.
- **Black (risks).** Section 0.3. The main risks are a lost claim sample and
  a fence skip that is not safe. H1 and H9 answer them.
- **Yellow (value).** Stage 1b gets three numbers instead of one guess. An
  operator gets the claim split and the persist phases in production.
- **Green (ideas).** Skip the per-append generation check too, when the
  start fence took the same row. Deferred: a colocated shard can use
  another generation row in the same transaction.
- **Blue (process).** Red tests first. Then the label, the phase timers and
  the fence fix until green. Then the bench, the apparatus run and the
  page. Then a review from several angles.

## 1. Change

1. `telemetry.rs`: `ClaimKind`, `PersistPhase`,
   `METRIC_DB_PERSIST_PHASE_DURATION`, `record_claim_duration` and
   `record_persist_phase`.
2. `worker.rs`: claim samples with a kind. Phase sums and the body marker
   in `PersistTiming`.
3. `store.rs`, `queue.rs`, `notify.rs`, `replication.rs`,
   `decision_owner.rs`: one phase timer at each chokepoint.
4. `decision_owner.rs`: `fence_pinned` sends no query inside a fenced scope.
5. The metrics-rs adapter and the plugin scrape endpoint: the `kind` label
   and the phase histogram.
6. `benches/decision_fence_bench.rs`: the criterion bench.
7. `docs/perf-artifacts/decision-owner-baseline/`: the apparatus patch, the
   driver, the grader and the raw output.
8. `docs/performance-decision-owner-baseline.md`: the results.

## 2. Review findings

Four review agents checked the engine code, the metrics surface, the
measurement and the tests. Nothing was blocking. The fixes:

- The persist timer read the clock twice. The recorder time then counted as
  commit time. One clock read now gives the total and the commit, so the
  phases add up to the total.
- A decision persist that failed before its body recorded no phase. The
  decision timer now records every phase, with a zero commit.
- A progress wake sent `pg_notify` outside `stage_now`, so its time counted
  as `other`. It now counts as `notify`.
- No test covered the error claim kind, nested phase timers or the `fence`
  phase of a real worker. Unit tests and a DR worker test cover them now.
- The page said that the second row adds 21%. Owner mode with DR on adds
  two statements, not one row. The page now says so, gives the delta of
  each run and adds six runs of the one-writer cases.
- The page read a direction into the 8-writer cells. They change sign
  between runs on this host, so the page now keeps them as raw output only.
- The claims-in-flight query in `performance.md` now sums over `kind`.

Accepted as is:

- A claim by id of a row that another worker took reads as `empty`. The
  doc of `ClaimKind::Empty` says so.
- CI clippy lints no bench target, so it does not lint the fence bench.
  A local clippy run of the bench with `-D warnings` is clean.

## 3. Out of scope

- The owner itself and its claim path: #2251 and #2252.
- A new claim statement. This issue measures only.
