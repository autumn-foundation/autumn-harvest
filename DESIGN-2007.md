# Design — Issue #2007: measure the resident-state hit rate

Resident workflow state (#1798) skips replay on a warm decision. It accepts
a narrow set of suspensions only. Nobody knows how often real workflows use
that path. Two bets of epic #1970 depend on the answer: a general resident
path, and typed-state snapshots.

This change counts each warm decision as a hit or a miss. A miss carries the
reason. Two measurements are recorded.

**No migration. No new `WorkflowEvent` variant. No change to
`harvest_events`.** Two counters are added.

---

## 0. Planning record

### 0.1 Facts found before the plan

- The resident path can fail at two points:
  - **Capture.** At the end of a cycle, `ResidentWorkflow::capture` keeps the
    future only when `plan_suspension` and `resident_blocker` accept it.
    Today both return `None` with no reason.
  - **Resume.** At the next decision, `resume_with` returns a
    `ResumeDeclined` reason. The worker logs it at `debug` level only.
- A capture failure happens one decision before its effect. The reason must
  travel with the cache entry to the next decision.
- The worker turns the resident path off for a hot-swapped module before it
  drives the cycle (`can_stay_resident`).
- A race and a join both leave two awaited commands. They differ by shape:
  - A `ctx.race()` cycle records a `race:{seq}` marker on its first cycle.
  - A race timer has a reserved id: `__race:`, `__signal_timeout:` or
    `__child_timeout:`.
  - A raw `select!` over two Harvest futures has no marker. It counts as
    `multi_await`.
- `harvest.workflow.cache_hit` counts delta loads. It does not tell whether a
  decision resumed or replayed.
- Every `METRIC_*` constant needs a panel on the starter dashboard. A guard
  test (`dashboard_pack_docs.rs`) checks this.
- The e2e bench `throughput` scenario runs three activities in sequence. It
  builds its registry with `NoOpMetrics`.
- `autumn_harvest_agent::agent_loop` runs one activity at a time: memory
  snapshot, model turn, tool calls, delivery. It runs on SQLite only today.
  The SQLite backend has no warm cache.

### 0.2 Brainstorm — how can Harvest count hits and misses?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Log each decline at `info` level and count the log lines. | Rejected. Logs are not a metric. A capture failure still has no reason. |
| B2 | Count at capture only: "this suspension can resume warm". | Rejected. A captured suspension can still decline at resume. The rate would be too high. |
| B3 | Count once per decision: hit, or miss with a reason. Carry the capture reason in the cache entry. | **Adopted.** One sample per decision, so hits divided by samples is the hit rate. |
| B4 | One counter with an `outcome` label (`hit`, `miss`). | Rejected. The existing pair `cache_hit` and `cache_miss` uses two counters. Two counters match it. |
| B5 | Use `ResumeDeclined` variant names as the reason. | Rejected. A variant can hold an event type name. The label must be a fixed set. |
| B6 | Map every miss to one `ResidentMiss` enum with a static `as_str`. | **Adopted.** The set is closed, so cardinality is bounded by construction. |
| B7 | Tell a race from a join with a new context flag set by each race API. | Rejected. Many race entry points. Command shape gives the same answer for every `ctx` race. |
| B8 | Measure the agent loop with a copy of its shape in the engine tests. | Partly adopted. The engine test checks the counters in CI. The recorded number comes from the real `agent_loop`. |
| B9 | Measure the real `agent_loop` on Postgres through an opt-in feature in the agent crate. | Rejected. A start needs `diesel-async` and `deadpool`. A dev-dependency cannot be optional, so every agent test build would pull Postgres. |
| B10 | Measure the real `agent_loop` in a new example crate, `examples/agent-loop-hit-rate`. | **Adopted.** It depends on the engine with `db` and on the agent crate. Neither crate changes. |

### 0.3 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | A label holds a workflow id or an event type, so series grow without bound. | `reason` comes from `ResidentMiss::as_str` only. A unit test lists every value. `workflow` and `queue` are registered names, as on `cache_hit`. |
| R2 | One decision records two samples, or none. | The worker records in the first iteration of the decision loop only. A local-activity re-drive does not count again. An integration test checks that the samples equal the decisions. |
| R3 | The counter counts with the resident path off, so the rate reads zero. | No sample when the path is off: sticky routing off, `resident_workflows` off, or the cache closed. |
| R4 | Carrying the reason changes when a future stays resident. | The reason is data next to the future. `capture` accepts the same set. The existing resident tests stay green. |
| R5 | A worker arm drops a captured future and the next decision reads `cold`. | Each iteration sets the carried reason. An arm that drops a captured future records `command`. |
| R6 | A race reads as `multi_await`, so the next bet picks the wrong target. | Shape rules cover each `ctx` race API. A raw `select!` is documented as `multi_await`. |
| R7 | The measurement pulls Postgres into the agent crate. | The measurement lives in its own example crate. The agent crate and its adapter-deps guard do not change. |
| R8 | A metric call costs time on the hot path. | One counter increment per decision. The no-op recorder is the default. |

### 0.4 Six thinking hats

| Hat | Notes |
|-----|-------|
| White | Capture fails in `plan_suspension` and `resident_blocker`. Resume fails with `ResumeDeclined`. Neither reaches a metric today. |
| Red | The number feels likely to be high for agents and low for fan-out work. The measurement must be able to show either. |
| Black | Shape-based race detection misses a raw `select!`. A first decision is always `cold`, which lowers the rate on short runs. |
| Yellow | Small change. No migration. The reasons point the next bet at the largest miss class. |
| Green | Later: a gauge of resident memory. A per-reason panel on the starter dashboard. A hit-rate alert. |
| Blue | TDD: unit tests for the reason of each capture and decline, then counter tests on a real worker, then code. Then docs, measurements, review. |

### 0.5 Decisions

1. Two counters:
   - `harvest.workflow.resident_hit{workflow, queue}`.
   - `harvest.workflow.resident_miss{workflow, queue, reason}`.
2. `reason` takes one of these values:

   | `reason` | Meaning |
   |----------|---------|
   | `cold` | No resident state on this worker: the first decision, a cache miss, an eviction or a restart. |
   | `multi_await` | The last suspension awaited two or more commands. |
   | `race` | The last suspension awaited a race, or dropped a wait. |
   | `mutex` | The last suspension held or waited for a durable mutex. |
   | `hot_swap` | The workflow runs in a hot-swapped module. |
   | `command` | The last suspension sent a command that the path does not take, such as a child workflow. |
   | `context` | Another context state blocked the capture, such as a push signal handler. |
   | `key_changed` | A context input changed, such as the deadline or the shard. |
   | `delta` | The new events did not resolve the parked wait exactly. |
   | `failure` | The awaited activity failed or timed out. |

3. The worker records one sample per decision, in the first iteration, when
   the resident path is on.
4. The cache entry keeps `Result<ResidentWorkflow, ResidentMiss>`. A gap in
   the delta sets `delta`.
5. The e2e bench records the counts in its `throughput` notes.
6. A new example crate, `examples/agent-loop-hit-rate`, runs the real
   `agent_loop` on a Postgres worker with an offline model. It prints the
   counts. CI lints it with its own clippy step.

### 0.6 Out of scope

- A wider resident path. This issue only measures.
- A resident-memory gauge.
- An alert rule on the hit rate.

## 1. Measurements

See `docs/rnd/2026-10-11-resident-hit-rate.md`.
