# Design: issue #2259, keyed-entity state as history events with a Postgres projection

Stage 3a of epic #2247. [ADR 0006](docs/adr/0006-keyed-entity.md) defines the
keyed entity. Its amendment records this change.

**One migration. One new `SideEffectKind` variant. One new trigger on
`harvest_events`. Old entity histories replay with no change.**

---

## 0. Planning record

### 0.1 Facts found before the plan

- The entity loop keeps the state in memory. Replay rebuilds it from the
  input, the op signals and the activity results. A state query replays the
  body.
- Each op already records one `SideEffectRecorded` event (the checkpoint
  decision). The side-effect path is complete: the codec encodes `value`,
  erasure tombstones `value`, replay matches it by position, and fork, the
  debugger, export, the test env, the simulator and the SQLite runtime pass
  it through.
- A new top-level `WorkflowEvent` variant needs a new command and parallel
  arms in about 30 files. The worker alone has 30 `RecordSideEffect` sites.
- `SideEffectKind` is a bounded enum. New kinds go at the end.
- The deadline probe (issue #772) and the continue-as-new decision (issue
  #2080) use a tolerant matcher. A history without the record replays clean.
- `ctx.patched` reads as "old branch" when signals trail the gate. Entities
  start with signal-with-start, so `patched` is the wrong gate here.
- Five Rust sites insert `harvest_events` rows: the store, reset (two),
  fork and shard rebalance.
- The partition layout changes rebuild `harvest_events` with `LIKE`. Each
  reinstalls the append-only guard and exempts it from the operator-trigger
  refusal.
- `harvest_events.id` comes from one sequence for each database. One entity
  key has at most one writer at a time.
- `CODEC_COLUMNS` needs a `UUID` id column. A unit test pairs it with
  `docs/security-posture.md`.
- Erasure is terminal-only and keys every scrub on the execution id.

### 0.2 Brainstorm: how can state changes become events?

| # | Idea | Verdict |
|---|------|---------|
| B1 | A new top-level `WorkflowEvent::EntityStateRecorded` and a new command. | Rejected. About 30 files of parallel plumbing. A missed non-exhaustive arm is a silent replay bug. |
| B2 | `SideEffectRecorded` with a new kind, `SideEffectKind::EntityState`. | **Adopted.** The whole side-effect path applies. User code cannot forge the kind. A trigger can select it by a plaintext field. |
| B3 | A `Custom` side effect with a reserved name. | Rejected. A user `side_effect` call with that name writes the projection. |
| B4 | A `MarkerRecorded` with a reserved name. | Rejected, as B3. |
| B5 | Gate the new events with `ctx.patched`. | Rejected. A signal-with-start run reads `false` for its whole life (0.1). |
| B6 | A tolerant matcher, as the deadline probe uses. | **Adopted.** A record at the cursor is consumed. Another event means a history from before this change. The end of history is the live frontier. |
| B7 | Replay adopts the recorded state. | **Adopted.** Replay rebuilds the state from the events, as the issue asks. |
| B8 | Record a snapshot when a run starts. | **Adopted.** It is the "equivalent snapshot event" of continue-as-new. The projection moves to the new run at its first decision. |
| B9 | Record a delete as a snapshot with its own name. | **Adopted.** The trigger reads the name, which is never encoded. A list can skip deleted keys without a key. |

### 0.3 Brainstorm: how is the projection kept atomic?

| # | Idea | Verdict |
|---|------|---------|
| P1 | Upsert in Rust after each of the five inserts. | Rejected. A sixth insert path forgets it. Shard rebalance inserts with raw SQL. |
| P2 | An `AFTER INSERT` row trigger with a `WHEN` clause. | **Adopted.** Every insert path writes the projection in its own transaction. The `WHEN` clause filters in C, so other events pay almost nothing. |
| P3 | A statement trigger with a transition table. | Rejected. Each insert then builds a tuple store. |
| P4 | Order writes by `executions.created_at`. | Rejected. Worker clocks set that column. Skew can order a new run before its predecessor. |
| P5 | Order by `harvest_events.id` across runs and by `event_id` in a run. | **Adopted.** One sequence for each database. One writer for each key. |
| P6 | A foreign key to the execution, `ON DELETE CASCADE`. | **Adopted.** Retention of the run removes its row. The row never points at a collected payload. |

### 0.4 Reverse brainstorm: how can this change cause harm?

| # | How to make it harmful | Counter-measure |
|---|------------------------|-----------------|
| R1 | Emit the new event in an old history. Replay diverges after the upgrade. | The tolerant matcher. A unit test replays a pre-change history. |
| R2 | Let the projection run ahead of history, or behind it. | One trigger in the insert transaction. A DB test crashes at every step. |
| R3 | Let a reset or an out-of-order insert move the projection backwards. | The order of P5. A reset run inserts later, so it wins. In one run, an older event loses. A stale run never inserts: the decision fence and its terminal state stop it. DB tests reset a continued run and insert out of order. |
| R4 | Leave plaintext in the projection after an erasure. | `erase.rs` tombstones the row of each erased run. A DB test checks it. |
| R5 | Leave the projection under a retired key. | A `CODEC_COLUMNS` entry. A DB test sweeps it. |
| R6 | Lose the trigger in a partition layout change. | Each layout path reinstalls it with the guard. The operator-trigger refusal exempts it by identity. |
| R7 | Serve a query from a row that is not the queried run. | The fast path needs `execution_id` equal to the run and a `RUNNING` run. Otherwise it replays. |
| R8 | Serve an offloaded or erased snapshot. | The fast path falls back to replay for an offload reference. A tombstone reads as erased. |
| R11 | Store a state past the side-effect cap or the depth cap. A deep value makes the history unloadable. | The loop records an `omitted` marker instead. Replay keeps its own state there, and a query replays. |
| R12 | Lose a snapshot that a cancelled scope captured. | The snapshot reads the scope capture at the frontier, as a side effect does. |
| R13 | Leave a wrong row after a shard move aborts. | The abort rebuilds the row from the runs that stay. The cutover deletes the row of the sealed run. |
| R9 | Grow history by the state size for each op. | The loop counts the snapshot bytes in its byte budget, so it checkpoints sooner. ADR text says to keep state small. |
| R10 | Slow every event insert. | The `WHEN` clause. Only entity snapshots call the function. |

### 0.5 Six thinking hats

- **White (facts).** Section 0.1.
- **Red (feelings).** A new kind feels smaller than a new variant. It is the
  same contract to an old reader: an unknown kind does not decode.
- **Black (risks).** A rolling upgrade. An old worker cannot decode a
  history that holds the new kind. Upgrade every worker before an entity
  records one. The ADR states this. A trigger on the hottest table. R6 and
  R10 cover it.
- **Yellow (benefits).** State is listable and indexable. A state query
  costs one indexed read. Every insert path stays consistent with no code.
- **Green (alternatives).** Sections 0.2 and 0.3.
- **Blue (process).** Red tests first. Then the engine, the migration, the
  coverage, the read path and the docs. Then a review from several angles.

---

## 1. The event

`SideEffectRecorded { kind: EntityState, name, value }`.

- `name` is `harvest.entity.state`, or `harvest.entity.deleted` for the
  delete that ends the run.
- `value` is `{"state": <S>, "stats": <EntityStats>}`. The codec encodes
  it as one payload field. A state past a payload cap records
  `{"omitted": <reason>, "stats": <EntityStats>}`.
- The loop records one at the start of each run, one after each op, and one
  before a delete ends the run.
- Replay consumes the record and adopts its state and stats.

## 2. The projection

`harvest_entity_state`, one row for each `(workflow_name, workflow_id)`.

| Column | Meaning |
|---|---|
| `id` | `UUID` key, for `CODEC_COLUMNS`. |
| `workflow_name`, `workflow_id` | The entity key. Unique. |
| `execution_id` | The run that wrote the row. Foreign key, cascade on delete. |
| `event_id` | The history position of the snapshot in that run. |
| `event_row_id` | `harvest_events.id` of the snapshot. Orders runs. |
| `deleted` | The snapshot ended the entity. |
| `snapshot` | The event `value`, byte for byte. Ciphertext when a codec is set. |
| `updated_at` | The time of the write. |

The trigger `harvest_entity_state_projection_trg` runs `AFTER INSERT` for
each row `WHEN` the row is an entity snapshot. It upserts the row. A row of
the same run moves forward by `event_id`. A row of another run moves when
the new `harvest_events.id` is larger.

## 3. Reads

`autumn_harvest::entity_state` reads and lists rows and decodes them. A
`harvest.entity.state` or `harvest.entity.stats` query on a running run
reads the row of that run. It replays only when no row matches.

## 4. Coverage

- `CODEC_COLUMNS` lists `harvest_entity_state.snapshot`.
- Erasure tombstones `snapshot` for each erased run.
- The SQLite backend keeps no projection. ADR 0006 records the plan.
