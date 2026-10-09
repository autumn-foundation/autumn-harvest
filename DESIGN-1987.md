# Design — Issue #1987: standalone durable activity

Issue #1987 asks two things:

1. Measure the overhead of a one-step workflow against a bare activity.
2. Record a decision: build a standalone-activity API, or document the
   one-step-workflow pattern.

Temporal made Standalone Activities GA on 15 September 2026. A standalone
activity is a retried, timed-out, durable job with no workflow around it.
Harvest has no such start path. A user wraps the activity in a one-step
workflow.

**No migration. No new `WorkflowEvent` variant. No route change. No engine
change.** This change adds a measurement harness, a docs guard, an ADR and
a guide section.

---

## 0. Planning record

### 0.1 Brainstorm — what can "bare activity" mean, and what can the harness measure?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Wall-clock time from start to completion. | Rejected. `activity_enqueue_batch_perf.rs` does not admit wall-clock on a shared-vCPU host. See §0.5. |
| B2 | `pg_stat_statements` call counts with a live worker. | Kept, with a control. Idle claim polls and samplers add calls. An idle-worker control of the same length measures that noise. |
| B3 | Rows written and WAL bytes per job. | **Adopted as the decider.** An idle claim poll changes no row and writes no WAL. The figures do not depend on poll timing. |
| B4 | Structural counts: events, task rows and claims per job. | **Adopted and asserted.** They are exact. A test pins them. |
| B5 | A bare arm that inserts a task row with a `NULL` `workflow_exec_id`, then claims and completes it with the `queue` API. | **Adopted as arm C, the floor.** It is the least work a durable, retried job can do on the Harvest queue. No real path runs a handler this way (`worker.rs` fails such a row). |
| B6 | Prototype a real standalone-activity start path, then measure it. | Rejected. That builds the API before the decision. The floor (B5) bounds what a build can save. |
| B7 | Measure the existing cheap fast path: a one-step workflow that runs a *local* activity. | **Adopted as arm B.** It has one task row and one claim. The issue asks whether such a fast path is enough. |
| B8 | Compare against Temporal's Standalone Activities on one box. | Rejected for this issue. Assay #11 shows the cost of a fair cross-engine run. It answers a different question. |

### 0.2 Reverse brainstorm — how can this measurement mislead?

| # | How to make it mislead | Mitigation |
|---|------------------------|------------|
| R1 | Count the worker's idle polls as job cost. | The decider uses rows and WAL (B3). Calls are reported next to an idle control. |
| R2 | Measure one job. One job hides fixed per-run noise. | Each arm runs `JOBS_PER_ARM` jobs. The report divides by that count. |
| R3 | Give the arms different payloads. | All arms use the same input and the same handler body. |
| R4 | Let state leak between arms. | Each arm gets a fresh, migrated database. |
| R5 | Publish numbers that the harness never produced. | The harness writes the raw capture to `docs/perf-artifacts/standalone-activity-overhead/`. A docs guard checks that the published structural counts equal the asserted constants. |
| R6 | Pick the decision rule after the numbers. | §0.4 fixes the rule before the first measurement. |
| R7 | Compare against a floor that no real API could reach. | The ADR states that the floor omits the job record that a real API must store. The floor flatters "build", so a "document" verdict against it is conservative. |
| R8 | Turn on `decision_boundaries` in one arm only. | All arms run with the worker defaults. The perf page states the extra `DecisionCommitted` events when it is on. |

### 0.3 Six thinking hats — build or document?

| Hat | Notes |
|-----|-------|
| White | A one-step workflow writes 5 events, 2 task rows and 3 claims per job (code reading; the harness confirms). A task row with a `NULL` `workflow_exec_id` is legal, but no worker path runs it. A local activity has no task row of its own. |
| Red | "I must write a workflow to run one job" feels like ceremony. The cost that users feel is the boilerplate and the extra latency, not WAL bytes. |
| Black | A new start path needs a job record, result fetch, cancel, idempotency, retention, visibility, the management API, the CLI and the UI. Each one is a new surface to keep in step with workflows. A local activity cannot heartbeat, cannot use another queue and has a start-to-close cap. |
| Yellow | A one-step workflow already gets every workflow feature: idempotent start, result fetch, cancel, retention, search attributes, the UI and the API. The pattern needs no new code. |
| Green | Document the pattern with a short example. Name the local-activity variant as the fast path for short work. Keep a follow-up if the numbers say the gap is large. |
| Blue | Red phase: a docs guard and a perf harness fail. Green phase: measure, then write the ADR, the perf page and the guide section. Refactor phase: tidy, run the gates, then review. |

### 0.4 Decision rule (fixed before the first measurement)

The decider is arm B against arm C, on rows written per job and WAL bytes
per job. Arm B is the best existing one-step pattern. Arm C is the floor.

- When arm B is at most **2.0x** arm C on both figures, the cheap fast path
  is enough. **Document the pattern.**
- When arm B is above 2.0x arm C on either figure, the gap is material.
  **Build a standalone-activity API**, as a follow-up issue.

Arm A against arm C is the overhead that issue #1987 asks for. It has no
line. It is reported in full.

### 0.5 Corrections after the first measurement

The rule in §0.4 did not change. Method claims in §0.1 and §0.2 were
wrong. Each fix removed noise from the deciders. None moved a figure across
the line.

1. **"An idle worker writes no row."** False. Scanner election renews a
   `harvest_scanner_leases` row on a timer, and the worker heartbeat
   writes `harvest_workers` and `harvest_worker_task_stats`. The harness
   now turns election off and sets a 10-minute heartbeat. The idle
   control then writes 0 rows.
2. **"An idle worker writes no WAL."** The cluster WAL position also
   holds autovacuum, catalog upkeep and other databases. The idle
   control wrote 110 KB of it. The harness now sums only WAL records on
   the arm database's `public` relations, through `pg_walinspect`,
   without full-page images. The idle control then writes 0 WAL bytes.
3. **Latency.** Each arm enqueues all its jobs and then drains them. A
   start-to-completion time measures the queue, not the job. The harness
   no longer reports it.
4. **Worker stop.** Aborting the worker task left its background loops
   running. The harness now stops the worker, waits for every worker
   connection to close, and then reads the counters. A stopping worker
   updates its `harvest_workers` row twice. The idle control pays the same
   writes, and the capture subtracts them.
5. **Source of rows written.** §1.2 first named `pg_stat_statements`. The
   red-phase harness already read `pg_stat_user_tables`, before any
   measurement.
6. **Guide scope.** §1.3 first wrote the guide section only for a
   "document" verdict. It now ships with either verdict, as the pattern
   to use today.

Before the fixes, the first capture read B/C 3.58x on rows and 4.22x on
WAL. After them, it reads 3.33x and 3.39x. The verdict is the same. The
first capture file was overwritten and is not kept; the figures above
come from its console output.

### 0.6 Re-charter: a realistic floor (fixed before arm D runs)

Review found that the §0.4 line could not fail. Code reading already gave
arm B at least 6 inserts and arm C 3 rows, so B was always above 2.0x C.
§0.2 R7 admitted that arm C flatters "build". §0.4 therefore compared
against the wrong floor. Its verdict stays on record, on its own terms.
This section asks the question again against a floor that a real
standalone-activity API could reach.

**Arm D: realistic floor.** A real API must keep what a workflow activity
already guarantees, but no more:

- a job record that a client reads by id: one `harvest_workflow_executions`
  row, inserted with the task row in one transaction and set to
  `COMPLETED` with the output in the completion transaction;
- the handler-start marker that timeout retries need (ADR 0005): one
  fenced update of the task row after the claim;
- no `harvest_events` rows, because there is nothing to replay.

Arm D runs through the `queue` API, as arm C does. The handler-start and
record-completion writes are the same statements that the worker issues,
in raw SQL.

**Rule.** Compare arm B against arm D, on rows written and WAL bytes per
job.

- At most **2.0x** on both: the existing fast path is enough. **Document
  the pattern.**
- Above 2.0x on either: **build**.

2.0x is the same line as §0.4. A new start path adds a surface to every
operator tool. It is worth that only if it at least halves the write cost
of the cheapest existing pattern.

**Prediction from code reading.** Arm D writes about 6 rows: two inserts
and four updates. Arm B writes 10, so rows give about 1.67x. WAL is the
less certain figure. This re-charter is not a blind test. Its value is the
right comparator, not surprise. The ADR states both verdicts.

### 0.7 Scope found in PR review (after arm D ran)

Review pointed out that §0.6 names arm B only. A job that needs a
heartbeat, another queue or a long start-to-close cannot use arm B. It
uses arm A. Arm A against arm D is 2.83x on rows and 2.65x on WAL, above
the line.

The §0.6 rule is not changed. Its verdict covers jobs that fit a local
activity. The ADR and the perf page publish the arm A gap as an open
question, with no line, because no line was fixed for it in advance.

---

## 1. Design

### 1.1 Arms

| Arm | Shape | Path |
|-----|-------|------|
| A | One-step workflow, regular activity | `start_or_load_workflow_execution`, then a live `Worker` |
| B | One-step workflow, local activity | The same, with `execute_local_activity_raw` |
| C | Bare floor | `queue::enqueue` (no execution), `queue::claim_task`, handler call, `queue::complete_claimed_task` |
| D | Realistic floor (§0.6) | Arm C plus a `harvest_workflow_executions` record and the handler-start marker |

All arms run the same handler on the same input.

### 1.2 Figures

Per job: events, task rows, claims, rows written (from
`pg_stat_user_tables`), WAL bytes (from `pg_walinspect`) and statement
calls (from `pg_stat_statements`). Calls are context, not a decider.

### 1.3 Outputs

- `docs/performance-standalone-activity-overhead.md`: the measurement.
- `docs/adr/0006-standalone-activity.md`: the decision.
- `docs/getting-started/activities.md`: the pattern.
- A changelog fragment.

## 2. Tests

| Test | Phase |
|------|-------|
| `standalone_activity_docs::*` (pure): the ADR, the perf page and the guide exist, agree with each other and with the asserted constants | Red, then green |
| `standalone_activity_overhead_perf::arm_*_structural_counts` (live DB): events, task rows and claims per job equal the constants | Characterization; a mutated constant fails it |
| `standalone_activity_overhead_perf::zz_capture_*` (ignored): writes the evidence | Evidence |
