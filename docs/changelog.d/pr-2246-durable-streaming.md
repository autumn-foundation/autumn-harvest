## Feature — Durable streaming: a durable route that answers with a resumable SSE stream (PR #2246)

`DurableCall::stream()` in `autumn-harvest-plugin` joins durable routes (PR
#2223) and durable streams (issue #1974). A route starts or attaches to its
keyed run, then streams each chunk that the run stores with
`publish_durable_progress`. A `result`, `problem` or `pending` frame ends
the stream.
When the connection drops, the client sends the same request with
`Last-Event-ID`. The stream resumes after that id, and side effects do not
run again.

- The SSE id is `<attempt>.<offset>`. The attempt counts the runs of the
  lineage: the run that the key gives, then each retry attempt and each
  continue-as-new successor. An `attempt` frame marks each new run. The id
  never holds the execution id.
- The app's own auth and the mandatory scope guard the stream. The
  management API can stay off.
- A malformed `Last-Event-ID` answers `400` `stream-cursor-invalid` before
  any I/O.
- `HarvestPlugin::durable_route_max_streams` (default 32) caps the streams
  of one process. A full cap answers `202` pending. `.stream_for(..)`
  (default 5 min, at most 1 h) ends a stream with a `pending` frame. The
  drain checks the budget before each frame, so a slow client cannot hold a
  slot past it.
- An erased run answers a plain `410`.
- Under Autumn's idempotency layer, which buffers bodies to cache them, the
  route answers `500` `misconfigured` and starts nothing.

No migration. No new `WorkflowEvent` variant. No `harvest_events` write. No
management API route. The only core edit removes a duplicated match arm in
`autumn-harvest/src/event.rs`, which fails `clippy -D warnings` with Rust
1.99.
