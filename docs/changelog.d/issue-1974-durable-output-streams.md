## Feature — Durable, resumable workflow output streams (issue #1974)

**Problem.** `ctx.publish_progress` is best-effort. A chunk is not stored. A
slow client loses chunks. A client that disconnects cannot resume. LLM token
streaming needs a stream that a client can resume at any offset.

**New, opt-in.**

- `WorkflowContext::publish_durable_progress(chunk)` stores each chunk in the
  new side table `harvest_stream_chunks`, in the persist transaction of its
  decision cycle. A failed write fails the cycle, so a committed cycle has all
  of its chunks.
- Each chunk has a 0-based offset, the call ordinal. A re-driven cycle gives a
  chunk the same offset, and `ON CONFLICT DO NOTHING` keeps the first copy.
- `GET /workflows/{id}/stream/durable` sends the stored chunks in offset order,
  then tails new ones. A reader resumes with `?after=<offset>` or
  `Last-Event-ID`. The producer sends with back-pressure, so a slow client
  loses nothing. A terminal run still streams its chunks, then `event: end`.
- Cap: 100,000 chunks per execution, then one terminal marker at offset
  `i64::MAX`. The 7,000-byte chunk cap of the best-effort mode applies.
- PII erasure deletes the chunks and reports `stream_chunks_deleted`. A shard
  rebalance copies them. `ON DELETE CASCADE` ties them to the execution.
- `TestRunOutcome::durable_progress()` returns the chunks of a no-DB test run.

**Unchanged.** `ctx.publish_progress` and `GET /workflows/{id}/stream` keep the
best-effort behavior and stay the default.

**Invariants.** One migration,
`20261008040857_harvest_stream_chunks`. No new `WorkflowEvent` variant. No
write to `harvest_events`. Chunks do not count toward the history caps, and
replay never reads them. New `WorkflowCommand::PublishDurableProgress`
bookkeeping variant. The SQLite runtime ignores it.

**Design.** `DESIGN-1974.md`. Docs: `docs/streaming-progress.md`.

**Tests.**

- `context.rs`: offsets from 0, replay claims an offset and pushes nothing,
  offsets stay stable when a cycle appends an event and across a pause and
  resume, a resident cycle keeps the counter, the size marker, and the queue
  bound.
- `tests/integration/durable_stream_tests.rs`: offset order, keep-first dedup,
  paging, the cap and its latch, a 30,000-row append, cascade, erasure, and a
  real worker over two cycles.
- `tests/integration/publish_progress_tests.rs`: zero event footprint, replay
  with no divergence, and harness dedup.
- `autumn-harvest-plugin/tests/progress_stream_integration.rs`: resume with no
  gap and no duplicate, backfill of a terminal run, a slow reader that gets
  all 1,000 chunks, and `400`/`404` cases.
