# Design — Issue #1974: durable, resumable workflow output streams

`ctx.publish_progress` (#791) is best-effort. A chunk is not stored. A slow
client loses chunks. A client that disconnects cannot resume. This change adds
an opt-in durable mode. The best-effort mode stays the default.

**One migration (new side table). No new `WorkflowEvent` variant. No change to
`harvest_events`. One new route.**

---

## 0. Planning record

### 0.1 Brainstorm — where do durable chunks live?

| # | Idea | Verdict |
|---|------|---------|
| B1 | A new `WorkflowEvent` variant in `harvest_events`. | Rejected. Each chunk uses the 50,000-event and 50 MiB history caps. Replay must skip each chunk. It breaks the no-new-variant discipline. |
| B2 | Keep a ring buffer in the API process memory. | Rejected. A restart or a second API node loses it. It is not durable. |
| B3 | A side table, written in the worker persist transaction. | **Adopted.** Same pattern as durable logs (#790). See §1.1. |
| B4 | Redis or Kafka streams. | Rejected. It adds an external dependency for an opt-in feature. |
| B5 | Use the ephemeral epoch `seq` as the offset. | Rejected. A re-drive at a new history length mints a new `seq` for the same chunk. A durable row would then duplicate. |
| B6 | Use a per-call ordinal as the offset. | **Adopted.** Durable logs use it. See §1.2. |

### 0.2 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | Store a chunk twice after a re-drive. | The offset is the call ordinal. `UNIQUE (workflow_exec_id, stream_offset)` and `ON CONFLICT DO NOTHING` keep the first row. |
| R2 | Lose a chunk when the write fails. | The write is in the persist transaction, so the cycle commits all chunks or none. A failed write follows the engine persist policy: a conflict re-runs the cycle, and another database error fails the execution. |
| R3 | Lose a chunk between backfill and live tail. | The reader opens `LISTEN` before the first read. Each wake reads `stream_offset > cursor` from the table. |
| R4 | Send a chunk twice to one reader. | The reader sends only rows above its cursor, and moves the cursor after each send. |
| R5 | Drop chunks for a slow client. | The durable producer waits on its send. It reads the next page only when the send buffer has room. |
| R6 | Fill the disk with a publish loop. | A per-execution cap of 10,000 chunks, at most about 70 MB. Overflow drops the newest and stores one terminal marker. |
| R7 | Exceed the Postgres limit of 65,535 bind parameters. | The store inserts in batches of 1,000 rows. |
| R8 | Hold a pooled connection while a slow client reads. | The producer releases the connection before it sends a page. |
| R9 | Leave rows after retention, erasure or a shard move. | `ON DELETE CASCADE`, an erasure delete, and the shard-rebalance copy list. The append locks the run row `FOR KEY SHARE` and stores nothing after an erasure, so a stale inline write cannot restore erased output. |
| R10 | Change replay. | The context reads nothing back. During replay the call claims an offset and pushes no command. |
| R11 | Mix offsets of the two modes in one SSE stream. | Each mode has its own route and its own `NOTIFY` channel. |
| R12 | Break today's best-effort callers. | `publish_progress` and `GET /stream` do not change. |
| R13 | Let a stalled client stop the `LISTEN` driver. A stopped driver holds back the NOTIFY queue of the whole cluster. | A forwarder task reads every wake at once and merges it into one pending wake. A client that takes no frame for 60 s loses the stream and resumes. |
| R14 | Hold a `LISTEN` connection for a finished run. | A run that is terminal at connect gets its chunks and `event: end` with no `LISTEN`. |
| R15 | Treat live code as replay. A signal that waits at the cursor makes the raw cursor check report replay, so the chunks before `wait_for_signal` are lost. | The durable gate skips such events, as `prepare_match` does, but it only reads the matcher. It moves no cursor, so later code sees the same `ctx.is_replaying()`. Unit tests and an SSE test cover a signal that arrives before the first cycle. |
| R16 | Lose chunks in a shard move. A decision between verification and cutover can store chunks and park with no new event, so the history guard passes. | Activation copies every source chunk, with `ON CONFLICT DO NOTHING`. A re-run can fill an offset below the target's highest one, so a high-water mark is not enough. The source is sealed then, so no later chunk can appear. The copy runs only when activation moves the target to `RUNNING`: a terminated copy can be erased, and a copy would restore erased output. |

### 0.3 Six thinking hats

| Hat | Notes |
|-----|-------|
| White | `/stream` uses `LISTEN`/`NOTIFY` and `try_send`. It has no backfill. Durable logs (#790) store rows in a side table at persist time, keyed by a call ordinal. |
| Red | "Disconnect and resume at any offset" is what LLM token streaming needs. Peers offer it. |
| Black | Chunks are stored in plain JSON, like durable logs. Payload codecs do not apply. A chunk with secrets is readable at rest. `exec_id` grants read access to all stored chunks. Each reader holds one `LISTEN` connection. SQLite does not support the mode. |
| Yellow | No history cap cost. No replay cost. Reuses the #790 write pattern, the #791 wire format and the shard-fence helpers. |
| Green | B1 to B6 in §0.1. A later change can make the cap configurable. |
| Blue | Red phase: context, store, harness and SSE tests fail. Green phase: command, store, worker write, route. Refactor phase: docs, then review. |

---

## 1. Design

### 1.1 Storage: `harvest_stream_chunks`

| Column | Type | Note |
|---|---|---|
| `id` | `BIGSERIAL` | Primary key. |
| `workflow_exec_id` | `UUID` | `REFERENCES harvest_workflow_executions(id) ON DELETE CASCADE`. |
| `stream_offset` | `BIGINT` | The chunk offset. `UNIQUE (workflow_exec_id, stream_offset)`. |
| `chunk` | `JSONB` | The chunk, or a size-cap marker. |
| `created_at` | `TIMESTAMPTZ` | Wall clock. Not used for order. |

Effect on history caps: none. A chunk is not an event. The event count and the
history byte size do not change.

Effect on replay: none. Replay does not read the table. The context claims an
offset during replay and pushes no command. The replay gate first moves a
signal or an update that waits at the cursor to its stash (R15).

Effect on the append-only invariant: none. The change does not write
`harvest_events`.

### 1.2 Offset

The offset is the 0-based ordinal of the `publish_durable_progress` call in the
workflow body. Each call claims one ordinal, also during replay. A re-drive
re-runs the body from the top, so a chunk gets the same offset again. A
resident cycle keeps the counter.

Consequence: the number and order of durable calls must be deterministic. The
chunk content can change. The first stored content is canonical.

### 1.3 Write path

The worker writes the rows in the persist transaction, next to
`persist_current_details_from_commands`, and it follows the same error policy.
A deadlock or serialization conflict re-runs the cycle. Another database error
fails the execution, as for every write in the persist transaction. The cycle
never commits with a missing chunk. The inline local-activity path writes
outside the outer transaction. A failure there re-runs the same offsets, which
dedup.
After the rows, the worker stages one wake on `harvest_stream_<exec_hex>`.
The post-commit sender of `notify.rs` sends it after the commit, as for task
and history wakes (issue #1796). A full `NOTIFY` queue cannot fail the write,
and the write takes no database-wide `NOTIFY` lock at commit. The payload is
a wake only. A lost wake costs latency, because the reader also reads the
table on each keepalive tick.

Cap: 10,000 chunks for each execution. The store admits rows up to the cap.
It then stores one marker at offset `i64::MAX` and admits no more rows. The
context queues at most `cap + 1` commands in one cycle.

### 1.4 Read path: `GET /workflows/{id}/stream/durable`

1. Read the resume cursor from `?after=<offset>` or `Last-Event-ID`.
2. Load the execution (`404` if it does not exist).
3. A terminal run: send its stored chunks above the cursor, then
   `event: end`. No `LISTEN` is needed, because all its chunks are committed.
4. A live run: open `LISTEN` on the execution's stream channel. Then loop:
   read pages above the cursor and send each row. Wait for a wake or a
   keepalive tick.
5. On a tick, poll the execution state. After a terminal state, read once
   more, send `event: end` and close.

A forwarder task drains the `LISTEN` connection at once and merges the wakes,
so a slow client never stops the connection driver. A client that takes no
frame for 60 s loses the stream, and it resumes with `Last-Event-ID`.

The frames are the `/stream` frames. The SSE `id:` is the offset.

### 1.5 Not done

- A configurable cap, and an operator switch for the mode.
- Payload-codec encryption of stored chunks.
- SQLite support. The SQLite runtime ignores the command, as for `/stream`.
- Chunks from a continue-as-new successor. A successor is a new stream.
- A reset, a fork or a retry is a new execution with a new stream. Its replay
  claims the offsets of the copied history, so its first chunk can have an
  offset above 0.
- Chunks from an update or query handler context. The worker does not drain
  that context, as for `publish_progress`.

---

## 2. Tests

| Acceptance criterion | Test |
|---|---|
| Design note | This file. |
| Resume with no gap and no duplicate | `durable_stream_resume_has_no_gap_and_no_duplicate` in `autumn-harvest-plugin/tests/progress_stream_integration.rs`. |
| No drop under back-pressure | `durable_stream_slow_reader_receives_every_chunk` in the same file. |
| Docs describe both modes | `docs/streaming-progress.md`. |

Unit tests in `context.rs` cover the offset and the queue bound. Store tests in
`autumn-harvest/tests/integration/durable_stream_tests.rs` cover the dedup, the
cap, the batch insert and the `LISTEN` wake. Harness tests are in
`publish_progress_tests.rs` and in `testing.rs`.
