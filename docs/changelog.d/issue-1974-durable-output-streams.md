## Feature — Durable, resumable workflow output streams (issue #1974)

**Problem.** `ctx.publish_progress` is best-effort. A chunk is not stored. A
slow client loses chunks. A client that disconnects cannot resume. LLM token
streaming needs a stream that a client can resume at any offset.

**New, opt-in.**

- `WorkflowContext::publish_durable_progress(chunk)` stores each chunk in the
  new side table `harvest_stream_chunks`, in the persist transaction of its
  decision cycle, so a committed cycle has all of its chunks. A failed write
  follows the engine persist policy: a conflict re-runs the cycle, and another
  database error fails the execution.
- Each chunk has a 0-based offset, the call ordinal. A re-driven cycle gives a
  chunk the same offset, and `ON CONFLICT DO NOTHING` keeps the first copy.
- `GET /workflows/{id}/stream/durable` sends the stored chunks in offset order,
  then tails new ones. A reader resumes with `?after=<offset>` or
  `Last-Event-ID`. The producer waits for a slow client, so it loses nothing.
  A client that takes no frame for 60 s loses the stream and resumes. A
  terminal run streams its chunks, then `event: end`, with no `LISTEN`.
- The `LISTEN` forwarder merges wakes and never blocks, so a slow client
  cannot hold back the shared Postgres NOTIFY queue.
- Cap: 10,000 chunks per execution, then one terminal marker at offset
  `i64::MAX`. The 7,000-byte chunk cap of the best-effort mode applies.
- PII erasure deletes the chunks and reports `stream_chunks_deleted`. A shard
  rebalance copies them. `ON DELETE CASCADE` ties them to the execution.
- `TestRunOutcome::recorded_durable_progress()` returns the chunks of a no-DB
  test run.

**Unchanged.** `ctx.publish_progress` and `GET /workflows/{id}/stream` keep the
best-effort behavior and stay the default.

**Invariants.** One migration,
`20261008040857_harvest_stream_chunks`. No new `WorkflowEvent` variant. No
write to `harvest_events`. Chunks do not count toward the history caps, and
replay never reads them. New `WorkflowCommand::PublishDurableProgress`
bookkeeping variant. The SQLite runtime ignores it.

**Design.** `DESIGN-1974.md`. Docs: `docs/streaming-progress.md`.

**Tests.**

- `context.rs`: offsets from 0, replay claims an offset and pushes nothing, a
  buffered signal at the cursor does not hide a live chunk,
  offsets stay stable when a cycle appends an event and across a pause and
  resume, a resident cycle keeps the counter, the size marker, and the queue
  bound.
- `tests/integration/durable_stream_tests.rs`: offset order, keep-first dedup,
  paging, the cap and its latch, a 30,000-row append, cascade, erasure, the
  `LISTEN` wake (commit only, merged), and a real worker over two cycles.
- `tests/integration/publish_progress_tests.rs`: zero event footprint, replay
  with no divergence, and harness dedup.
- `autumn-harvest-plugin/tests/progress_stream_integration.rs`: resume of a
  live run with no gap and no duplicate, delivery by the `LISTEN` wake before
  the keepalive tick, backfill of a terminal run, a slow reader that gets all
  1,000 chunks, and `400`/`404` cases.
- `testing.rs` and `api.rs` unit tests: harness keep-first dedup and the
  resume cursor parser.
