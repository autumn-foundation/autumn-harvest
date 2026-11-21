# Design — one elected gauge sampler per shard

Issue #2679 follows PR #2676 and issue #2668 (option B).

Each worker runs every database gauge sampler. The fleet-wide samplers read
every shard. With W workers the fleet pays W reads for one answer. At the
large Ledger fixture one sample costs about 1.2 M buffers per worker.

**One migration (a new table). No new `WorkflowEvent` variant. No route
change. No new metric. No alert or dashboard rule changes.**

## 0. Facts

| sampler | per-shard read | emitted |
|:--|:--|:--|
| queue depth | `bounded_queue_depths`, `oldest_pending_ages` | fleet-wide, per queue |
| concurrency | `concurrency_key_stats` | fleet-wide, per key |
| rate limit | `sample_rate_limit_buckets` | fleet-wide, per key |
| DLQ | `dead_letter_count` | per `shard` |
| queue pause | `paused_queue_names` | fleet-wide, per queue |
| history oversized | `sample_history_oversized_counts` | fleet-wide, per workflow |
| workflow active | `sample_active_workflow_counts` | fleet-wide, per workflow and state |
| stranded work | demand, workers, compat set | per `shard` |

Every alert and dashboard query on these gauges reads `max by`, a bare
selector, or a per-series function. One rule,
`harvest_workflow_population_leak`, reads `sum by (workflow)` over the
replicas. Each replica emits each series today.

## 1. Planning record

### 1.1 Brainstorm — how can one read serve the fleet?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Elect one sampler per shard and sampler kind with `scanner_lease`. Only the leader emits. | Rejected. On a handover the old leader keeps its last value. `metrics-rs` cannot drop a series. `max` then reads a stale value, and `sum` counts twice. |
| B2 | B1, and the old leader zero-fills its series on step-down. | Rejected. A drop to 0 makes `max_over_time - min_over_time` fire the DLQ growth rule. |
| B3 | Emit per shard with a `shard` label, and let PromQL sum. | Rejected. It renames the series of six gauges and changes eight alert and dashboard queries. |
| B4 | Elect a leader per shard and sampler kind. The leader writes its per-shard read to a shared row. Each worker reads the row and emits as before. | **Adopted.** This is the "per-worker cache that the lease holder fills" of the issue. |
| B5 | One lease per shard for all eight kinds. | Rejected. The eight samplers are separate tasks with separate inputs. One lease per kind keeps them separate. |
| B6 | Keep the shared sample in process memory. | Rejected. Workers are separate processes. Only the database is shared. |
| B7 | Cache the final emitted value, not the per-shard read. | Rejected. Workers with different shard sets would emit the wrong sum. The per-shard read composes. |
| B8 | Put the sample in `harvest_scanner_leases`. | Rejected. The lease table is a coordination table. A separate table keeps each row small and each grant clear. |

### 1.2 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | Workers with different inputs share one sample. | The lease key holds a digest of every input of the read: queues, circuit-breaker activities, requirements, threshold and freshness. Different inputs elect different leaders. |
| R2 | A rolling deploy changes the sample format. Old and new workers misread each other. | The key holds a format version. A row that does not decode makes the standby read the shard itself. |
| R3 | A dead leader leaves every standby emitting an old value. | A standby uses a row only while it is younger than three sample intervals. After that it reads the shard itself. The lease expires after its TTL, at least three intervals. |
| R4 | A leader that cannot write the row keeps its lease. Standbys fall back for ever. | A failed write counts as a failed pass. After three, the leader gives up its lease, as the timeout checker does. |
| R5 | A lease or row write goes to a fenced or held shard during a DR cutover. | A held shard skips election. Under a DR pin, each write checks the fence on its connection first. A fenced process reads the shard as before. |
| R6 | The lease table fails, and every gauge stops. | Fail open. A failed lease query reads the shard, as before the change. |
| R7 | Rows for old keys grow without bound. | Each write deletes the sample rows of its shard that no leader wrote for one day. The lease row of an old key stays, as for the timeout checker. |
| R8 | Election adds latency to a gauge. | A standby normally reads a row up to one interval old, and never older than three. Prometheus scrapes each 15 to 60 s, so the normal case is within one scrape. A gauge that grows with time is up to one interval behind. |
| R9 | Shutdown waits on lease releases. | A release runs only for a slot that led, with the 1 s bound of the timeout checker. |
| R10 | A shared row leaks data across tenants. | The row holds what each worker already emits. The table has the same grants as the lease table. |
| R11 | `elect = false` changes behaviour. | Without a holder, each sampler reads the shard and writes no row, as before. |

### 1.3 Six thinking hats

- **White (facts).** See §0. The lease module exists and is proven
  (`scanner_lease_tests`). The large fixture reads 1.2 M buffers per sample
  per worker.
- **Red (feelings).** Operators want their alerts to keep their meaning.
  Changing every query would erode trust in the packs.
- **Black (risks).** A new table needs a migration and a grant. A standby
  gauge is up to one interval older. A bug in the key could merge two
  input sets (R1).
- **Yellow (benefits).** Scans per shard and interval go from W to 1.
  Alerts, dashboards and series names stay the same. A handover changes no
  series, because every worker still emits.
- **Green (ideas).** Later, the leader could publish its role on
  `harvest.scanner.pass`. This change does that, with `scanner` set to
  `gauge_<kind>`. A later change could store the DLQ count in a counter
  table.
- **Blue (process).** Design first. Then red tests: key and decision unit
  tests, then database tests of the shared slot, then a worker-driven
  harness. Then the migration and the module. Then wire each sampler. Then
  docs and audits.

## 2. Decision

1. Add `harvest_gauge_samples`: one row per `(shard_id, sampler)` with the
   leader's per-shard read as JSON and `sampled_at`.
2. Add `gauge_election`. A `GaugeSlot` per shard and sampler kind takes the
   lease `gauge:v1:<kind>[:sha256:<inputs>]`.
   - Leader: read the shard, write the row, use the read.
   - Standby: use a row younger than three intervals. Otherwise read the
     shard (`fail_open`).
   - Lease error, no holder, a held shard or a fenced process: read the shard.
3. Each of the eight samplers reads each shard through its slot. The
   aggregation, zero-fill and failure rules stay in the sampler.
4. Each slot records `harvest.scanner.pass{scanner="gauge_<kind>"}`.
5. A graceful stop releases each lease the slot led.

## 3. Review fixes

A review from four angles changed the first design:

- **DR.** Under a DR pin, every shard is pinned in normal operation. So the
  first design never elected there. Each write now checks the fence first.
- **Outdated leader.** A slow read can outlive its lease. The row write now
  needs a live lease of the writer, so an outdated read writes nothing.
- **Non-finite floats.** `serde_json` writes NaN as `null`, and no standby
  can decode it. The leader now checks the round trip and counts a failure.
- **Shutdown.** Each shard releases its lease within its own bound.

## 4. Failover bound

The lease TTL is at least three sample intervals (`MIN_LEASE_TICKS`). A
standby accepts a row of age at most three intervals. A dead leader leaves
each worker's gauge at most three intervals plus one tick old. A graceful
stop hands over on the next tick.

## 5. Acceptance criteria

1. The fleet pays one read per shard and interval. A worker-driven harness
   with three workers counts the non-standby passes per shard.
2. Each worker emits the same values as an unelected worker. So each alert
   and dashboard rule reads the same values. No rule changes.
3. A handover does not drop or double-count a gauge. A test stops the
   leader and checks that the standby takes over and emits the same value.

## 6. Measurement

`a_fleet_reads_each_shard_once_per_interval_and_emits_unchanged_values` in
`gauge_election_db_tests` runs three real workers on two shards, each its
own database, for 12 intervals of 250 ms. It runs the fleet first without
election and then with it. It counts `harvest.scanner.pass` by shard and
role. A read is a pass in any role but `standby`.

| sampler | shard 0 elected | shard 0 unelected | shard 1 elected | shard 1 unelected |
|:--|--:|--:|--:|--:|
| `gauge_queue_depth` | 11 | 24 | 12 | 24 |
| `gauge_concurrency` | 11 | 28 | 10 | 28 |
| `gauge_rate_limit` | 11 | 28 | 10 | 28 |
| `gauge_dlq` | 12 | 30 | 12 | 29 |
| `gauge_queue_pause` | 10 | 26 | 10 | 26 |
| `gauge_history_oversized` | 10 | 27 | 11 | 27 |
| `gauge_workflow_active` | 10 | 28 | 10 | 28 |
| `gauge_stranded_work` | 11 | 27 | 11 | 26 |

Each worker ticks 10 or 11 times on each shard. The elected fleet reads each
shard about once per tick. A standby can read once more on the first tick,
before the leader writes its first row. The test asserts at most one read
per tick plus one per worker, on each shard. It also asserts that each
elected worker emits the same DLQ, active, pause, depth, stranded and
rate-limit values as an unelected worker.

At the large Ledger fixture one read of each sampler costs about 1.2 M
buffers. With W workers the fleet now pays that once per shard and interval,
not W times.
