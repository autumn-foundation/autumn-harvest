# ⛏️ Prospect pre-registration: does integrated Redis dispatch clear the founding ">10,000 tasks/sec" line in a deployment-shaped run? (assay ledger #6)

**Committed:** 2026-09-07T01:35:00Z, before any apparatus was built or
measurement taken. This document is the contract; the report that follows it
is graded against these lines, not against whatever the numbers turn out to be.

## 🎯 Question

Ledger #1 and #2 measured `autumn-harvest-redis` standalone: no Postgres, no
worker, no completion write. Both reports named the same open pit: a
deployment-shaped assay "once the worker-integration refactor exists (real
worker pool, real transactional-boundary cost, real complete/ack in the
loop)". Issue #1312 builds that integration
(`docs/plans/2026-09-07-redis-dispatch-worker-integration.md`) and its third
acceptance criterion is this assay: it "gates whether this actually delivers
on the documented >10,000 tasks/sec claim once integration overhead is
included".

**Falsifiable question:** with the integrated path (a real `Worker` pool, real
Postgres claim-by-id and completion transactions, real Redis dispatch,
Postgres remaining the source of truth), does end-to-end task throughput
reach the founding line, and does the integrated path clear a decisive margin
over the same worker pool on the Postgres claim path?

## 👤 Decision this feeds

Whether `docs/autumn-workflow-architecture.md` §9.1 may present Redis
dispatch as a working ">10,000 tasks/sec" escape hatch, or must present it
as a measured multiplier over the Postgres path with a lower absolute
ceiling on the reference machine. **Decider:** the owner of that document and
of `docs/plans/vantage-spec-redis-adapter.md`.

## ⚖️ Success / kill criteria (numeric, set now)

One task is one `harvest_task_queue` row that reaches `COMPLETED`. A
workflow with one activity produces three tasks: the workflow task that
schedules the activity, the activity task, and the workflow task that
completes the run. Throughput is completed tasks per second over the
measured window, read from Postgres, never from the apparatus's own counters.

- **L1 — the founding line.** Redis-dispatch arm, drain shape: **≥ 10,000
  completed tasks/sec** over the window in which the seeded backlog drains
  (window = first claim to last completion). Kill if below.
- **L2 — the integration multiplier.** Redis-dispatch arm divided by the
  Postgres-path control arm, same apparatus, same run, same seeded backlog:
  **≥ 10x**. Kill if below.
- **L3 — steady state does not collapse.** Paced-start shape at the rate the
  drain arm sustained: dispatch latency p99 (row `created_at` to `started_at`)
  **≤ 250 ms** for the Redis arm. Kill if above.

**All three lines must clear for pursue; any one miss is kill.** The report
records each line separately, so a kill on L1 with a pass on L2 is still
informative for the decider: it says the escape hatch delivers a multiplier,
not the founding absolute number, on this machine.

**Correctness precondition, checked in both arms before any line counts:**
every seeded workflow reaches `COMPLETED`, every activity side effect fires
exactly once (a Postgres counter table, one row per activity run), and after
the run the dispatch stream, its pending entries list and its marker keys are
empty. A miss here voids the run.

## 🧪 Conditions

- Reference machine: this session's 4 logical CPUs, 15 GB RAM. Postgres 16 on
  loopback with `fsync=off`, `synchronous_commit=off` (matches
  `docs/benchmarks.md`), Redis 7.0.15 on loopback with `save ""`,
  `appendonly no`.
- Pool shape, taken from the pinned deployment configuration in
  `docs/benchmarks.md`: 4 in-process `Worker` instances, each with 8
  workflow slots and 16 activity slots, one shared pool of 64 connections,
  `poll_interval` 25 ms on the control arm, `dispatch.poll_interval` 20 ms
  on the Redis arm.
- Workload: one workflow type with one no-op activity and a ~40-byte input.
- Drain shape: seed 10,000 workflows (30,000 tasks) with the workers
  stopped, then start the pool and measure until the last completion.
- Paced shape: start workflows at the rate the drain arm sustained, for 30 s,
  and measure dispatch latency from `harvest_task_queue`.
- Repetitions: 3 per arm per shape, alternating arms. The report shows every
  run and grades the mean.
- Control: the identical apparatus with no channel installed
  (`dispatch::uninstall()`), so the workers use `claim_task_on_shard`.

## 🚫 Anticipated stubs

Loopback only, one Postgres, one Redis, no replication, no TLS, no auth, a
single queue, a no-op activity, four workers in one process rather than four
processes, and Postgres durability settings relaxed as in the benchmark
suite. All are recorded again in the report's stubs list.

## 🔍 Prior art

- `docs/assays/0001-redis-adapter-throughput-ceiling.md` and
  `docs/assays/0002-redis-matched-workload-vs-postgres.md`: the standalone
  numbers and the re-charter this assay discharges.
- `docs/benchmarks.md` and `autumn-harvest/benches/e2e_bench.rs`: the pool
  shape and the "idle box" rule (check the replay control drift before
  comparing).
- `docs/performance.md`: the Postgres claim-path numbers that explain any
  large multiplier.

## ⏱️ Time box

One session, single pass, after the integration merges. The apparatus lives
at `docs/assays/apparatus/0006-redis-dispatch-integrated/`, is not a
workspace member, and calls only public APIs of `autumn-harvest` and
`autumn-harvest-redis`.
