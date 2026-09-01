# Reproducible end-to-end benchmark suite (issue #941)

Planning record for the four-scenario end-to-end benchmark suite, its
docker-compose topology, and the versioned results doc.

The problem this slice closes is not "Harvest is slow". It is that **Harvest
publishes no end-to-end number at all**, so an evaluating architect sizing a
deployment has nothing to compare against Hatchet's "X tasks/sec on Postgres" or
Temporal's per-shard state-transition guidance, and a contributor changing the
worker poll loop has no way to know whether they moved the end-to-end number.

---

## 1. Brainstorming — candidate designs

Ideas generated before filtering, so the discarded ones stay on the record.

| # | Idea | Verdict |
|---|---|---|
| B1 | Put the suite in a **new workspace member** (`autumn-harvest-bench`). | ❌ Every `cargo check --workspace` / MSRV run would compile it, and it would need its own copy of the DB-provisioning machinery. The AC allows it; the repo's own precedent (#786) does not. |
| B2 | `benches/` + a **shared harness module under `tests/integration/`**, exactly the #786 claim-bench shape. | ✅ Chosen. One harness, two consumers, dev-dependencies only, and `ci_run_coverage`'s "harness, not a suite" rule already covers the shape. |
| B3 | Build the whole thing on **criterion**. | ❌ Every scenario here is destructive and stateful (a drained queue, a consumed signal, a completed execution). Criterion would re-run the closure against an empty queue — the same reason #786 hand-rolled its harness. Replay is the one criterion-shaped scenario, and #135 already benches it that way. |
| B4 | Measure activity dispatch latency **inside** the saturated throughput run. | ❌ Under saturation, schedule → handler-start is a measurement of the backlog, not of the dispatch path. p99 would be the queue depth. |
| B5 | Measure dispatch latency in a **separate, deliberately unsaturated scenario** with paced starts. | ✅ Chosen. Publishes what the number claims to be. The saturated number is still published — as throughput, where saturation is the point. |
| B6 | Take the signal round-trip from `send_signal` (engine entry point), skipping HTTP. | ❌ AC(c) says "HTTP signal → workflow observes it". Skipping the hop publishes a number no operator can act on. |
| B7 | Pull in `axum` + `reqwest` as dev-dependencies to serve the signal route. | ❌ Two large dependency trees in every `cargo test` build of the crate, for one route with a fixed shape. |
| B8 | **Hand-rolled minimal HTTP/1.1** client + server over `tokio::net`, calling the same `signal::send_signal` entry point the plugin's route calls. | ✅ Chosen. Real loopback TCP, real request framing, zero new dependencies, and the framing code is pure enough to unit-test. Honest limit: it is not autumn-web's middleware stack — stated in the doc. |
| B9 | Read the workflow's observation of a signal out of the **event log** after the fact. | ❌ Event timestamps are DB-clock wall time at append; the round-trip needs the instant the workflow's own code resumed. |
| B10 | Have the workflow record `Instant::now()` into a shared sink immediately after `wait_for_signal` returns, keeping the **first** observation per execution. | ✅ Chosen. Same process as the HTTP client, so both ends are one monotonic clock — no skew term at all. Replay re-runs the line; keeping the first observation makes that harmless, and nothing branches on the recorded value, so the workflow stays deterministic. |
| B11 | Measure activity dispatch with the same in-process trick. | ❌ The *schedule* half happens inside the worker's decision cycle, which the harness cannot observe in-process. |
| B12 | Take dispatch latency as `harvest_task_queue.created_at` (DB clock) → handler-start (`Utc::now()`, host clock). | ✅ Chosen, with a measured skew bound. Both clocks are on the reference host; the harness probes `now()` against the local clock and **publishes the observed offset** so a reader can see the error term rather than trust a claim. |
| B13 | Re-implement a 10k-event replay history for the replay scenario. | ❌ Two builders drift; the published replay number would stop being #135's number. |
| B14 | Move #135's history builder into the shared harness and have **both** `replay_bench.rs` and the e2e suite call it. | ✅ Chosen. That is what "reusing/extending the existing #135 bench for continuity" buys: drift becomes impossible rather than merely unlikely. |
| B15 | Run the replay scenario **once**, since replay is shard-invariant. | ❌ Publishes a ragged table, and throws away a free control. |
| B16 | Run replay at every shard count and use it as the **noise control**: it cannot legitimately move with shard count, so drift across the three runs bounds how loaded the box was. | ✅ Chosen. Turns a redundant row into the run's own quality signal. |
| B17 | Model "N shards" as N **databases on one Postgres server**. | ❌ Cheap, but shard scaling would then be measuring one server's contention with itself, and the compose file would not match Harvest's actual shard model. |
| B18 | Model "N shards" as **N independent Postgres servers**, one per shard — compose services on the reproduction path, native clusters on the reference machine. | ✅ Chosen. Matches `ShardedDbPool` (one pool per shard, no `shard_id` column anywhere) and matches the compose file the doc tells readers to run. |
| B19 | Gate the published numbers in CI, like #786's claim budget. | ❌ Explicit non-goal ("CI-gated regression budgets ... is a follow-up once baselines stabilize"). |
| B20 | Ship an **opt-in** `HARVEST_BENCH_CHECK=1` mode that compares a run against the published baselines at the stated ±15% and prints a per-number verdict. | ✅ Chosen. It is how a reader discharges "reproduce within a stated tolerance" without a single CI job being added. |
| B21 | Keep the published baselines only in Markdown. | ❌ The doc and the harness drift the first time someone re-runs the suite. |
| B22 | Keep the baselines as **constants in the harness** and pin the doc to them with a docs-drift test, the `alert_pack_docs` / `chaos_docs` pattern. | ✅ Chosen. |
| B23 | Overwrite `docs/benchmarks.md` each release. | ❌ AC4 requires each release's numbers to be kept. |
| B24 | `docs/benchmarks.md` = framing + methodology + reproduction + index; `docs/benchmarks/results-v<version>.md` = the immutable per-release snapshot. | ✅ Chosen — the `docs/alerts/starter-pack-v0.1.0.json` naming precedent. |
| B25 | Add engine instrumentation (a new event variant, a timing column) to make the measurement easier. | ❌ AC6: zero engine impact. Every number here is taken from existing columns, existing events, and handler-side observation. |

## 2. Reverse brainstorming — how would we *guarantee* this ships a lie?

Each failure mode is paired with the structural defence, and (where it is
testable) the test that pins it.

| # | "How to publish a dishonest number" | Defence |
|---|---|---|
| R1 | Publish a p99 computed from three samples. | Every scenario carries a minimum sample count; below it the report renders `n/a` and the run is marked unsound rather than printing a confident-looking number. Test: `stats_below_the_minimum_sample_count_are_reported_as_unsound`. |
| R2 | Publish a throughput figure measured over a window that includes worker startup and pool warmup. | A discarded warmup batch runs first, and the measured window opens only once the warmup batch has fully drained. Test: `measured_window_excludes_the_warmup_batch`. |
| R3 | Report "workflows/sec" from a run where some workflows never completed. | Throughput is computed from **observed completions**, and a run whose completed count is below the requested count is reported as truncated with the shortfall printed. Test: `an_incomplete_drain_is_reported_as_truncated`. |
| R4 | Silently divide by a zero-length window and print `inf`. | Throughput math returns `None` for a non-positive window or a zero completion count; the renderer prints `n/a`. Test: `throughput_is_none_for_a_degenerate_window`. |
| R5 | Call a saturated-queue measurement "dispatch latency". | Dispatch latency has its own paced, unsaturated scenario, and the harness **asserts the pacing held** by reporting the achieved start rate next to the target; a scenario that could not hold its pace is marked saturated. Test: `pacing_shortfall_marks_the_scenario_saturated`. |
| R6 | Charge cross-host clock skew to the engine. | The single-host topology removes it, and the harness measures and publishes the host↔DB offset instead of asserting there is none. A negative dispatch latency (only possible under skew) is counted and reported, never clamped to zero. Test: `negative_dispatch_samples_are_reported_not_clamped`. |
| R7 | Publish a signal round-trip that starts after the HTTP request was built. | The client's stopwatch starts before the socket write and stops on the workflow-side observation. |
| R8 | Let a replay-scenario history quietly stop being #135's history. | One builder, called by both benches. Test: `replay_history_shape_matches_the_issue_135_contract` pins event count and variant order. |
| R9 | Publish shard scaling measured against a shard set the workers never actually drained. | Every scenario asserts per-shard completion counts are all non-zero and reports the per-shard split, so a run where shard 3 did nothing cannot be published as a 4-shard number. Test: `a_shard_with_no_completions_marks_the_run_unsound`. |
| R10 | Publish numbers whose hardware/config is undocumented. | The report renders the environment block (CPU count, OS, Postgres `version()`, `shared_buffers`, profile, shard URLs redacted) from the live run, and the results file is a copy of that output — not a hand-typed table. |
| R11 | Let the doc's numbers and the harness's baselines drift. | Baselines are constants; `benchmarks_docs.rs` fails when the doc does not contain them, when the documented command does not match the runner, or when the shard matrix or tolerance in prose disagrees with the code. |
| R12 | Quietly turn this into a CI gate that flakes the build. | No manifest row runs any DB scenario. The only wired rows are the pure unit tests and the docs-drift guard. Test: `no_manifest_row_runs_the_e2e_benchmark_scenarios`. |
| R13 | Leak a 4-shard, 100k-row benchmark into a shared database. | Every shard database is freshly created per run with a run-unique name, exactly as #786's harness does; a supplied URL is treated as an **admin** URL. |
| R14 | Fail the build on a laptop with no Docker and no Postgres. | No database ⇒ print a skip notice and exit 0, the #786 contract. |
| R15 | Hide a scenario that failed behind an averaged headline. | Every scenario prints its own soundness verdict; the run's exit summary lists unsound scenarios by name. |
| R16 | Claim shard scaling that is really core count. | The doc states the reference box has 4 logical CPUs and that 1→4 shards on it holds hardware fixed, so the numbers bound *software* scaling only. AC2's "or honestly failing to demonstrate" is taken literally. |
| R17 | Duplicate #786's claim/enqueue scenarios and its CI gate. | The suite contains no claim or enqueue scenario. The doc cross-references #786 as the component-level complement, and `benchmarks_docs.rs` asserts the cross-reference exists. |
| R18 | Change the engine to make the benchmark look good — or at all. | Zero files under `autumn-harvest/src/` change in this slice. |

## 3. Six Thinking Hats

**⚪ White (facts).** Harvest has two performance artifacts today: `replay_bench.rs`
(#135, 10k events < 200 ms) and `claim_bench.rs` + `claim_budget_tests.rs` (#786,
the claim-path microbenchmark with a p50 CI gate). There is no `docker-compose.yml`
anywhere in the repo. `ShardedDbPool` maps one `DbPool` per `ShardId` and there is
no `shard_id` column on `harvest_task_queue`, so "which shard" *is* "which pool" —
`sharded_runtime_tests.rs` already creates one fresh database per shard for exactly
that reason. `WorkerRuntimeConfig::sharded_pool` plus auto-resolved
`shard_assignments` (#961) already drains every writable shard from one worker.
`harvest_task_queue.created_at` exists and is nullable. The plugin's signal route
is `POST /workflows/{id}/signal/{signal_name}`, which lands in
`signal::send_signal`. The reference machine has 4 logical CPUs and Postgres 16.13
— the same class of box `docs/performance.md` was measured on.

**🔴 Red (instinct).** The thing that will embarrass us is not a low number; it is
a number a reader cannot reproduce, or a headline that quietly means something
other than its label. "Dispatch p99" measured under saturation is the trap, and
"4 shards on 4 cores scales 4x" is the lie we would most easily tell by accident.
There is also a pull toward making the numbers look good — resisting it is the
whole point of publishing at all.

**⚫ Black (risks).** (1) Shard scaling on a 4-core box will likely be flat or
negative; publishing that is the AC, but it will read as bad news. (2) The
hand-rolled HTTP endpoint is not the production middleware stack, so the signal
number is an engine-side lower bound. (3) Container Postgres is slower than a
native cluster, so a reader running the compose path may land outside ±15% of a
reference taken on native clusters — this must be stated, not discovered. (4) A
benchmark harness that lives in `tests/integration/` risks being picked up as a
CI suite and flaking the build. (5) 12 scenario runs on one box is a long wall
clock; without per-scenario budgets a stuck run parks for hours. (6) Nullable
`created_at` means a pre-upgrade row yields no dispatch sample — must be counted
and reported, not skipped silently.

**🟡 Yellow (upside).** Every ingredient already exists; this is packaging, not
invention. The replay-as-control idea makes the run self-diagnosing. The
baseline-constants-plus-docs-test pattern is already proven twice in this repo.
Because the suite touches no engine code, its blast radius is a build-time
dev-dependency graph that is not even widened. And the `--check` mode gives the
issue's success metric ("an external user can reproduce any headline number
within ±15%") a literal, runnable answer.

**🟢 Green (creativity).** Replay as the noise control (B16). Publishing the
host↔DB clock offset as a measured error term rather than assuming it away (B12).
Pacing-shortfall detection as the structural guard that "dispatch latency" never
silently becomes "queue depth" (R5). Rendering the results file **from the run's
own output** so the published table cannot be hand-edited into something the
harness never produced (R10).

**🔵 Blue (process).** TDD in three passes. Red: the pure harness contract —
scenario matrix, percentile and throughput math, soundness verdicts, HTTP
framing, tolerance comparison, and the docs-drift guard, all failing. Green: the
harness, the bench binary, the compose topology, the runner script. Refactor:
collapse duplication against `claim_bench_support` (percentiles are shared, not
re-derived), tighten naming, and make the report renderer the single source of
both stdout and the results file. Then run the suite for real on 4 native
Postgres clusters, publish, and review from several angles before opening the PR.

## 4. Chosen shape

```
benchmarks/docker-compose.yml            4 independent Postgres 16 services (shards 0..3)
benchmarks/run.sh                        the one documented command
autumn-harvest/tests/integration/e2e_bench_support.rs
                                         shared harness: pure section (unit-tested
                                         everywhere) + `#[cfg(feature = "db")]` section
autumn-harvest/tests/integration/benchmarks_docs.rs
                                         docs-drift guard (pure, all-OS)
autumn-harvest/benches/e2e_bench.rs      harness = false; runs the 4x3 matrix
autumn-harvest/benches/replay_bench.rs   delegates its history builder to the shared harness
docs/benchmarks.md                       framing, methodology, reproduction, index
docs/benchmarks/results-v0.6.0.md        the immutable 0.6.0 snapshot
```

Scenarios (each at 1, 2 and 4 shards):

| id | headline | shape |
|:--|:--|:--|
| `throughput` | workflows completed/sec | saturated backlog drain of a canonical 3-activity workflow |
| `dispatch_latency` | activity schedule → handler start, p50/p99 | paced, deliberately unsaturated |
| `signal_roundtrip` | HTTP signal → workflow observes, p50/p99 | paced; workflows parked on `wait_for_signal` |
| `replay_throughput` | events/sec over a 10 000-event history | in-memory; the shard-invariant noise control |

---

## 5. Revision after the first measurement

The plan above survived contact with the reference box in every respect but one,
and that one is worth recording because it is exactly what the reverse
brainstorm was for.

**What was planned.** `throughput` would pre-load a backlog of 1 000 workflows
per shard and measure the drain, reporting the middle-half rate (R2/R4).

**What the first run showed.** The middle-half rate came out at **473
workflows/s** while the whole-drain rate over the same run was **55/s** — an
8.6x disagreement between two statistics of one measurement. The cause is not
noise: claim cost grows superlinearly with pending backlog depth (issue #786's
headline finding), so a pre-loaded drain *accelerates* as it empties. The rate
therefore depended entirely on which part of the drain you looked at, and the
"sustained throughput" headline would have been an artifact of a queue depth
nobody chose.

Publishing that would have re-published #786's claim-depth curve under an
end-to-end label — precisely the duplication AC5 forbids, arrived at by
accident rather than by copying a scenario.

**What shipped instead.** A **bounded closed loop**: a fixed population of
workflows is held in flight and topped up as runs complete, so queue depth stays
shallow and constant and the rate is genuinely sustained. Because closed-loop
throughput is `concurrency / latency`, the load level became a documented knob
(`HARVEST_BENCH_INFLIGHT`) rather than a buried constant, and a new soundness
rule (`inflight_soundness`) refuses to publish a run in which the *harness* — not
the engine — was the limiter.

That rule earned its place immediately. Calibrating on the reference box:

| in flight / shard | workflows/s | mean population held | published? |
|--:|--:|--:|:--|
| 32 | 23.4 | 31.0 / 32 | yes |
| 128 | 27.6 | 121.3 / 128 | yes |
| 384 | *(32, computed)* | 319.3 / 384 | **no** — feeder-bound |

128 is the default the published numbers are taken at: the deepest load level at
which the figure is still a statement about Harvest.

**Also checked, and deliberately not changed.** Raising the worker's concurrency
(8 workflow / 16 activity slots → 24 / 48) on the four-core reference box
collapsed throughput rather than raising it — 72 concurrent tasks oversubscribe
four cores. The published configuration stays at 8 / 16, and the worker
concurrency is documented with the numbers rather than being a hidden choice.
