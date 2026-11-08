# Design — Issue #2419: open-loop capacity per shard

The published `throughput` cell runs a closed loop with 32 workflows in
flight per shard. Its rate is `32 / workflow latency`. It is a latency
figure, not a capacity figure. The shard columns also share one 4-core
host, so the 1 → 4 shard curve is a CPU limit of that host.

This record plans an open-loop capacity scenario for `e2e_bench`.

**No migration. No new `WorkflowEvent` variant. No engine change.** The
change is in the bench harness and the docs only.

---

## 0. Planning record

### 0.1 Brainstorm — how can the harness measure capacity?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Raise the closed-loop in-flight target until the rate stops rising. | Rejected. A deep in-flight target measures the claim-depth curve. The rate still depends on latency. |
| B2 | Pre-load a backlog and time the drain. | Rejected. The drain rate changes with backlog depth. No arrival rate is offered. |
| B3 | Offer a fixed start rate, step it up, and keep the highest step that stays bounded. | **Adopted.** It is the method the issue names. |
| B4 | Find the knee by a binary search over the rate. | Rejected. A noisy step can send the search to a wrong half. A rising ladder is monotone and simple to read. |
| B5 | Sample the backlog live with `COUNT(*)` polls. | Rejected. The observer adds load to the server it measures. |
| B6 | Derive the backlog after each step from the scheduled start and the `completed_at` of each run. | **Adopted.** It is exact, and it adds no load during the step. |
| B7 | Run each worker as a tokio task in the bench process. | Rejected for this scenario. The issue asks for worker processes. |
| B8 | Re-run the bench binary as a worker child process. | **Adopted.** Each child has its own runtime and its own pool. Closing its stdin stops it. |

### 0.2 Reverse brainstorm — how can the scenario lie?

| # | How to make the number wrong | Mitigation |
|---|------------------------------|------------|
| R1 | Measure latency from the actual send. A slow generator then hides the queue (coordinated omission). | Latency starts at the scheduled arrival, not the send. |
| R2 | Let the generator fall behind, and report the offered rate anyway. | Each step checks the achieved rate. A step below 95% of its offer, where the engine kept up, is generator-bound. The ladder stops, and the cell is unsound. |
| R2a | Call a saturated box "generator-bound" and void a real knee. | A saturated database also slows the start calls. When the engine fell behind too, the engine reason wins. The smoke run found this case at 186 starts/s. |
| R3 | Count a step as sustainable while its backlog grows. | The least-squares slope of the backlog must stay under 5% of the offer, with a floor of 1 run/s. |
| R4 | Let a slow run escape the p99 because it never completed. | A run that does not complete within the drain grace fails the step. |
| R5 | Report the last step of the ladder as the maximum when no step failed. | The cell is unsound with "the ladder ended before a step failed". |
| R6 | Let one step's tail run into the next step. | Every step drains to zero before the next step starts. |
| R7 | Put a co-located multi-shard cell into a scaling claim. | Each cell reports its host layout. The scaling section refuses a claim when any multi-shard cell shares a host. |
| R8 | Mix host and database clocks in one latency. | The scheduled arrival and `completed_at` both come from the bench host clock. |
| R9 | Leave child processes running after a panic or a timeout. | `kill_on_drop` kills each child. A child also exits on stdin EOF. |
| R10 | Accept a noisy late-window spike as a failure, and stop the ladder early. | The p99 rule allows 2x growth plus 100 ms. A spurious failure only lowers the reported rate. It never raises it. |

### 0.3 Six thinking hats

* **White (facts).** The reference box has 4 vCPUs and Postgres 16.15 on
  the same host. The 0.8.0 closed-loop cell is 52.91 workflows/s at 1
  shard. DBOS reports 20K workflows/s on one 16-core RDS instance with 48
  worker processes per shard.
* **Red (feelings).** A low capacity number on a 4-core box can look bad
  next to 20K/s. Publish it anyway, with the hardware of each beside it.
* **Black (risks).** Four worker processes, Postgres and the generator
  share 4 cores. The number is a host limit as much as an engine limit.
  Each worker process opens up to 33 connections, so a shard needs
  `max_connections` of about 140.
* **Yellow (benefits).** The first Harvest number that has the same shape
  as a published capacity figure. The pure rule is unit-tested, so the
  soundness logic is fixed before a run.
* **Green (alternatives).** A later run on a 16-core host with a separate
  database host makes the comparison fair. The harness takes
  `HARVEST_BENCH_SHARD_URLS` for that layout.
* **Blue (process).** RED tests for the pure rule first. Then the runner.
  Then one measured run on the reference box. Then the docs and their
  guards.

---

## 1. Decisions

### 1.1 Two arms of one scenario

`BenchScenario::Capacity(CapacityArm)` has two arms:

* `capacity_noop`: a workflow with zero activities.
* `capacity_canonical`: the canonical 3-activity workflow.

The arms are opt-in. `cargo xtask bench run` keeps the published 4 x 3
matrix. `HARVEST_BENCH_SCENARIOS=capacity` selects both arms.

### 1.2 The step protocol

1. Start `N` worker processes per shard. Wait for each one to say `ready`.
2. Run one discarded warmup step at the first rate.
3. For each step `k`, offer `start x 1.25^k` starts/s per shard for 20 s.
4. Drain the step. Wait until every run completes, or 20 s pass.
5. Load `workflow_id` and `completed_at` for the step's runs.
6. Judge the step. Stop the ladder at the first step that is not
   sustainable.

### 1.3 The sustainable-rate rule

A step is **sustainable** when each of these holds on every shard:

* The generator held 95% or more of the offered rate.
* The step started 200 runs or more.
* Every run completed within the drain grace, and none failed.
* The least-squares backlog slope over the last three quarters of the step
  is at most `max(5% of the offer, 1 run/s)`.
* The p99 latency of the last quarter is at most 2x the p99 of the second
  quarter, plus 100 ms.

An engine reason wins over a generator reason. A step is **generator-bound**
only when the engine kept up and the generator missed its rate.

The **sustainable rate** is the offer of the last step in the leading run
of sustainable steps. A ladder that never fails a step reports a lower
bound and is unsound. A ladder that the generator or the budget stopped is
unsound too.

### 1.4 Host layout

Each database cell reports its host layout. The layout counts the distinct
hosts in the shard URLs and notes a loopback host. A cell with fewer hosts
than shards says that its shards share a host and that it is not a scaling
result. The run's scaling section prints no ratio when any multi-shard
capacity cell shares a host.

---

## 2. Acceptance criteria

| AC | Evidence |
|----|----------|
| The scenario exists in `e2e_bench.rs` with both arms. Its soundness check fails a step whose backlog grows. | `BenchScenario::Capacity`, `capacity_bench_support.rs`, `a_step_whose_backlog_grows_is_not_sustainable` |
| A unit test pins the sustainable-rate rule. | `the_sustainable_rate_stops_at_the_first_growing_backlog` |
| One measured run on the reference box, 1 shard, both arms. | `docs/benchmarks/capacity-2026-11.md` |
| `benchmarks.md` states that the closed-loop cell is latency-bound, and that co-located shard cells are not a scaling result. | `benchmarks_docs.rs` guards |
| `comparison.md` cites the DBOS figure only next to a Harvest number from this scenario, with the hardware of each. | `capacity_docs` guards |
