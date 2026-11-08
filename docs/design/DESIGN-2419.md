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
| R1 | Measure latency from the actual send. A slow generator then hides the queue (coordinated omission). | Each run records its schedule and its admission. The generator wait, from schedule to admission, has its own p99 limit. A late generator is never silent. |
| R2 | Let the generator fall behind, and report the offered rate anyway. | Each step checks the achieved rate. A step below 95% of its offer, where the engine kept up, is generator-bound. The ladder stops, and the cell is unsound. |
| R2a | Call a saturated box "generator-bound" and void a real knee. | A saturated database also slows the start calls. When the engine fell behind too, the engine reason wins. The smoke run found this case at 186 starts/s. |
| R3 | Count a step as sustainable while its backlog grows. | The least-squares slope of the engine backlog must stay under 5% of the offer. There is no absolute floor, so a low rate gets no wider margin. The median latency must also grow at most 5 ms/s. |
| R4 | Let a slow run escape the p99 because it never completed. | A run that does not complete within the drain grace fails the step. |
| R5 | Report the last step of the ladder as the maximum when no step failed. | The cell is unsound with "the ladder ended before a step failed". |
| R6 | Let one step's tail run into the next step. | Every step drains to zero before the next step starts. |
| R7 | Put a co-located multi-shard cell into a scaling claim. | Each cell reports its host layout. The scaling section judges each multi-shard cell. It prints no ratio for a cell whose shards share a database host, or whose workers share the bench host. |
| R8 | Mix host and database clocks in one latency. | The admission and `completed_at` both come from the bench host clock. |
| R9 | Leave child processes running after a panic or a timeout. | `kill_on_drop` kills each child. A child also exits on stdin EOF. Each child holds a census guard until it exits. |
| R10 | Accept a noisy late-window spike as a failure, and stop the ladder early. | Each judged window holds at least 100 runs. A step that fails only on the p99 rule runs once more. |

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
  152 connections at 4 processes.
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

1. Check that each server can hold the cell's connections.
2. Start `N` worker processes per shard. Each one says `ready` after its
   first pool checkout.
3. Run one discarded warmup step at the first rate. A warmup that leaves
   runs unfinished voids the cell.
4. For each step `k`, offer `start x 1.25^k` starts/s per shard for 20 s.
   Each arrival is its own task. It takes a connection from a
   16-connection generator pool at its scheduled instant. That instant is
   its admission.
5. Drain the step. Wait until every run is terminal, or 20 s pass.
6. Load `workflow_id`, `state` and `completed_at` for the step's runs.
7. Judge the step. A step that fails only on the median or p99 rule runs
   once more. With no budget for that retry, the cell is void. Stop the
   ladder at the first step that is not sustainable.

### 1.3 The sustainable-rate rule

A step splits into four equal windows by admission. The first window is
settle time. A step is **sustainable** when each of these holds on every
shard:

* The generator held 95% or more of the offered rate.
* The p99 generator wait is at most 100 ms.
* The step started 400 runs or more, so each judged window holds 100. The
  start rates, 50 and 25, give 500 runs, which leaves a margin.
* Every run completed within the drain grace. No run and no start call
  failed.
* The least-squares slope of the engine backlog over the judged windows is
  at most 5% of the offer.
* The median latency grows at most 5 ms/s from the second window to the
  last.
* The p99 latency of the last window is at most 2x the p99 of the second
  window, plus 100 ms.

The engine backlog counts runs admitted and not yet complete. A run that
waits in the generator does not count. The median rule catches a steady
overload that stays under the slope limit. A least-squares fit of the mean
latency would also fire on a short tail spike, so the median is used.

An engine reason wins over a generator reason. A step is **generator-bound**
only when the engine kept up and the generator missed its rate.

The **sustainable rate** is the offer of the last step in the leading run
of sustainable steps. A ladder that never fails a step reports a lower
bound and is unsound. A ladder that the generator or the budget stopped is
unsound too.

### 1.4 Host layout

Each database cell reports its host layout. The layout counts the distinct
hosts in the shard URLs. Every loopback spelling counts as one host. A cell
with fewer hosts than shards says that its shards share a host and that it
is not a scaling result. The scaling section judges each multi-shard
capacity cell. It prints no ratio when the cell's shards share a database
host, or when its workers share a worker host. Every worker process runs on
the bench host today, so no multi-shard capacity cell gives a ratio yet.

---

## 2. Acceptance criteria

| AC | Evidence |
|----|----------|
| The scenario exists in `e2e_bench.rs` with both arms. Its soundness check fails a step whose backlog grows. | `BenchScenario::Capacity`, `capacity_bench_support.rs`, `a_step_whose_backlog_grows_is_not_sustainable` |
| A unit test pins the sustainable-rate rule. | `the_sustainable_rate_stops_at_the_first_growing_backlog`, which builds each verdict from `judge_shard` |
| One measured run on the reference box, 1 shard, both arms. | `docs/benchmarks/capacity-2026-11.md` |
| `benchmarks.md` states that the closed-loop cell is latency-bound, and that co-located shard cells are not a scaling result. | `benchmarks_docs.rs` guards |
| `comparison.md` cites the DBOS figure only next to a Harvest number from this scenario, with the hardware of each. | `capacity_docs` guards |

---

## 3. Review round 1

Three read-only reviews ran against the first commit: measurement
soundness, process safety, and docs and guards. The first measured run used
the first rule, so it was stopped and is not published.

| Finding | Fix |
|---------|-----|
| A run the generator had scheduled but not sent counted as engine backlog. A slow generator then read as an engine limit. | Backlog and latency start at admission. The generator wait has its own limit. |
| The p99 rule cannot catch a linear overload: the ratio of two windows stays under 2. | The median-growth rule. |
| The 1 run/s floor let a 10% overload pass at 10 starts/s. | No floor. The start rates are 40 and 20. |
| A window of 50 runs makes the p99 its maximum. | 400 runs per step. One retry for a tail-only failure. |
| Warmup leftovers ran inside step 0. | A warmup that leaves runs unfinished voids the cell. |
| The scaling check saw database hosts only. | It also needs one worker host per shard. |
| `localhost` and `127.0.0.1` counted as two hosts. | Every loopback spelling is one host. |
| A worker that died inside a live child went unnoticed. | The child selects on stdin and on the worker. It exits non-zero when the worker stops. It says `ready` after a pool checkout. |
| A shared server could run out of connections. | The cell checks `max_connections` first. |
| Children were not in the task census. | Each child holds a census guard until it exits. |
| Children inherited the commit probe, and a stdout write could block. | `env_remove` for the probe, and the parent drains stdout. |
| The comparison guard could pass on a stray number or a whole table. | Exact numbers, the arm id, the results link and case-insensitive hardware in the same block. |

## 4. Review round 2

A workflow ran five reviewers over the branch. Two skeptics then tried to
refute each finding. One finding survived both. Several more held with one
vote, and a reading of the code confirmed them.

| Finding | Fix |
|---------|-----|
| The results file blamed the closed-loop change on worker count. Both closed-loop cells ran one worker. | The text now says both ran one in-process worker with 8 slots. |
| A noise-only failure with no budget for its retry became a sound knee. | `after_failure` voids the cell. |
| A start that timed out in an overloaded step panicked the whole cell. | A failed start fails the step instead. |
| The median limit sits near the noise of healthy canonical steps. | A median-only failure also earns the one retry. |
| A window could hold 99 runs, with no margin. | Start rates 50 and 25: 500 runs per step, on the same ladder rates. |
| The connection check ignored connections in use and `reserved_connections`. | It reads both. |
| `ProcessFleet::stop` dropped census guards before it reaped the children. | Each child stays in the fleet until it is reaped. |
| A malformed child variable made a child run the benchmark. | `child_mode` exits the child instead. |
| The DBOS bullet sat under Related, and the guards missed swapped rows, a bare "million", and the headline numbers in `benchmarks.md`. | The bullet moved. The guards check each row, need the engine name, and pin the headline. |

Not changed: the retry's p99 is the published p99, as the rule states. The
admission and completion clocks are both on the bench host. A slow start
path voids the cell as generator-bound, which never inflates a number.
