# Decision boundaries

A decision boundary records which build and which worker made each decision
of a workflow (issue #1833). Use it to find the build that caused a drift
after an incident, or to audit the builds that ran a workflow.

## What the worker writes

A decision is one run of the workflow code on a worker. It ends when the
worker persists the outcome: a suspension, a completion, a failure, a
continue-as-new, a cancellation or a park on a pause.

When a decision writes one or more events of its own, the worker also
appends one `DecisionCommitted` event. It writes the boundary in the transaction that
persists the outcome, after the outcome events:

```json
{"type": "DecisionCommitted", "data": {"build_id": "2026.10.1", "worker_id": "worker-eu-1"}}
```

- `build_id` is the `build_id` of the worker. It is empty when the worker
  has no build id.
- `worker_id` is the id of the worker.

The outcome events of a decision come just before its boundary. Other
writers also add events between two boundaries: activity workers, timers and
signals. The boundary does not attribute those events. A typical history
looks like this:

| # | Event | Written by |
|---|---|---|
| 0 | `WorkflowStarted` | the start request |
| 1 | `ActivityScheduled` | decision 1 |
| 2 | `DecisionCommitted` | decision 1 |
| 3 | `ActivityStarted` | the activity worker |
| 4 | `ActivityCompleted` | the activity worker |
| 5 | `WorkflowCompleted` | decision 2 |
| 6 | `DecisionCommitted` | decision 2 |

A decision that writes no event of its own writes no boundary. An example is
a wake for a signal that the workflow does not wait for. Such a decision
changes no state. It has nothing to attribute. This is true even when another
writer appends an event while the decision runs.

## How replay treats a boundary

Replay never matches a boundary. The history matcher marks it as consumed
before replay starts, as it does for pause and resume events. A history with
boundaries replays the same as that history without them.

A history written before this release has no boundaries. It replays without
change. The fixture test
`decision_boundary_replay_tests::a_pre_1833_history_replays_unchanged` keeps
that true.

A boundary still has an event id. It still counts toward the history length,
`ctx.history_event_count()`, `ctx.info().history_event_count` and
`ctx.should_continue_as_new()`. It also counts toward these limits:

- `history_event_hard_cap` and `history_continue_as_new_threshold`;
- `max_workflow_history_events`, the ceiling of the timeout scanner;
- the history-bloat warning, which is a fraction of the hard cap;
- the `history_bytes` quota.

The hard-cap check of a run that stays running includes the boundary that
its decision is about to write. See [Storage overhead](#storage-overhead).

## How to read the boundaries

`harvest history export` writes every boundary in its JSON. The redacted
mode keeps `build_id` and `worker_id`, because they are not payloads. This
`jq` filter lists the build and worker of each decision:

```bash
harvest history export <execution-id> \
  | jq -r '.events | to_entries[]
           | select(.value.type == "DecisionCommitted")
           | "\(.key)\t\(.value.data.build_id)\t\(.value.data.worker_id)"'
```

Other tools show the same values:

- `harvest debug replay` prints `decision: build <build>, worker <worker>` in
  the detail column of each boundary row. `--step N` shows them under
  `decision`. See [the replay debugger](replay-debugger.md).
- The Vantage timeline shows `Decision committed: build <build>, worker
  <worker>`.
- The Mermaid diagram shows a note for each boundary.

`harvest debug diff` skips boundaries when it aligns the two sides. A diff
of two builds must show a change in behavior, not a change of build. So a
recording from before the upgrade compares clean with the same run after it.

## Rollout

Boundaries are on by default. A process older than this release cannot
decode a boundary, so the default is a breaking change for a rolling upgrade.
An old worker fails the execution. An old timeout leader cannot enforce its
activity timeouts. An old API node or CLI cannot show, export or cancel it.
See [the upgrade guide](upgrading/0.8.0.md#11-each-decision-records-a-boundary-event--breaking-default).

During a rolling upgrade, turn boundaries off on the new workers:

```rust
let harvest = HarvestBuilder::new()
    .record_decision_boundaries(false)
    // ... the rest of your configuration
    .build();
```

When every process that reads history runs this release, remove the call.

`WorkflowHistoryPolicy::with_decision_boundaries` sets the same switch on a
`HandlerRegistry`.

## Storage overhead

Each boundary adds one row to `harvest_events`. Two measurements give its
cost.

**On a live worker.** The test
`decision_boundary_db_tests::measure_boundary_storage_overhead` runs a
workflow with two activities and a side effect. Its history has 9 other
events and 3 boundaries. The build id has 10 bytes and the worker id has
38 bytes.

| Rows | Count | `event_data` bytes | Row datum bytes |
|---|---|---|---|
| `DecisionCommitted` | 3 | 402 (134 each) | 672 (224 each) |
| Other events | 9 | 1,524 (169 each) | 2,336 (260 each) |

The boundaries add 26% to the `event_data` bytes of this small history.

"Row datum bytes" is `pg_column_size` of the whole row. It excludes the
tuple header and the line pointer.

**On disk, with indexes.** The measurement inserts 100,000 synthetic
boundaries into a copy of `harvest_events` with all its indexes. Each
boundary has a 9-byte build id and a UUID worker id. The execution ids are
random. Random ids are the worst case for B-tree packing. The SQL is in
[Reproduce the measurement](#reproduce-the-measurement).

| Part | Bytes per boundary |
|---|---|
| `pg_column_size(event_data)` | 131 |
| Heap (`pg_relation_size`) | 234 |
| Indexes (`pg_indexes_size`) | 204 |
| **Total** | **438** |

The partial indexes on `event_type` do not hold boundary rows. Six indexes
do: the primary key, the unique `(workflow_exec_id, event_id)` key, and
the indexes on `(workflow_exec_id, event_id)`, `(workflow_exec_id, id)`,
`(workflow_exec_id, timestamp DESC)` and `(timestamp, workflow_exec_id)`.

To estimate the cost for a workflow, multiply the number of decisions that
write events by about 440 bytes. A workflow with 100 such decisions adds
about 44 KB and 100 events. The history byte quota counts the
`event_data` part only, about 131 bytes for each decision. If a workflow ran
close to a history limit before this release, raise that limit.

## Limits

- Only the Postgres worker writes boundaries. The embedded SQLite backend has
  one process and no worker id, so it writes none. Its histories still replay
  on the Postgres engine.
- An inline local activity commits its result before the decision ends. A
  crash between that commit and the outcome leaves those events without a
  boundary. The next boundary then covers them.
- A decision that the engine ends before it persists an outcome writes no
  boundary. Examples are the history cap and a non-determinism block.
- The boundary row stages no `NOTIFY`. The `last_event_type` of the decision
  notification stays the type of its last outcome event. Its `event_count`
  does not count the boundary. A live tail sees the boundary at the next
  notification.

## Reproduce the measurement

Run this in `psql` against a database with the Harvest schema:

```sql
CREATE TABLE m1833 (LIKE harvest_events INCLUDING ALL);
SELECT pg_total_relation_size('m1833') AS empty_total \gset
INSERT INTO m1833 (workflow_exec_id, event_id, event_type, event_data)
SELECT x.exec, d, 'DecisionCommitted',
       jsonb_build_object('type', 'DecisionCommitted', 'data',
         jsonb_build_object('build_id', '2026.10.1',
                            'worker_id', gen_random_uuid()::text))
FROM (SELECT gen_random_uuid() AS exec FROM generate_series(1, 10000)) x,
     generate_series(0, 9) d;
VACUUM ANALYZE m1833;
SELECT round(avg(pg_column_size(event_data)), 1) AS event_data,
       pg_relation_size('m1833') / count(*) AS heap,
       pg_indexes_size('m1833') / count(*) AS indexes,
       (pg_total_relation_size('m1833') - :empty_total) / count(*) AS total
FROM m1833;
DROP TABLE m1833;
```
