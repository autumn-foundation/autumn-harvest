# Design — durable streaming

A durable stream is a durable route that answers with a resumable
Server-Sent Events (SSE) stream. The stream sends each chunk that the run
stores with `ctx.publish_durable_progress`, then one final frame with the
result. When the connection drops, the client sends the same request again
with `Last-Event-ID`. The route attaches to the same run and sends the
chunks after that id. Side effects do not run again, and no chunk is lost.

Durable routes (PR #2223) give a keyed run and a stored result. Durable
streams (issue #1974) give stored chunks with offsets. This feature joins
the two behind the app's own route, auth and scope. Vercel `getWritable`,
Inngest Realtime and Temporal Workflow Streams are the references
(`research_notes/Durable workflow engine feature gaps/code_first_engines.md`).

**No migration. No new `WorkflowEvent` variant. No `harvest_events` write. No
management API route. No core engine change.** The feature is one child
module, `durable_route/stream.rs`, in `autumn-harvest-plugin`.

---

## 0. Planning record

### 0.1 Facts found before the plan

| # | Fact | Source |
|---|------|--------|
| F1 | `publish_durable_progress` stores each chunk in `harvest_stream_chunks`, in the transaction of its decision cycle. Each chunk has an offset in its execution. | `docs/streaming-progress.md`, `worker.rs` `persist_durable_stream_from_commands` |
| F2 | `store::load_stream_chunks` reads the chunks above an exclusive offset, in order, one page at a time. | `autumn-harvest/src/store.rs` |
| F3 | The store admits only offsets above the highest stored offset, so a reader cursor never misses a late chunk. | issue #2083, `append_stream_chunks_in_tx` |
| F4 | An append to a terminal or erased run stores nothing. PII erasure deletes the chunks of the run. | `docs/streaming-progress.md` |
| F5 | `DurableStreamListener` holds one `LISTEN` connection on the channel of one execution. A wake is a hint that the worker sends after the commit. | `autumn-harvest/src/notify.rs` |
| F6 | The management route `/workflows/{id}/stream/durable` makes the execution id a bearer capability. It checks no scope. | `docs/streaming-progress.md`, `api.rs` |
| F7 | A workflow-level retry is a new execution with its own offsets. Its row commits in the same transaction that seals the attempt `FAILED`. `retry_of_exec_id` links it, with an index. | `worker.rs` retry start, migration `20260723000000` |
| F8 | A continue-as-new successor is a new execution with its own offsets. `continued_from_exec_id` links it, with an index. | `worker.rs`, migration `20260710000002` |
| F9 | A reset and an operator rerun set neither link. | `reset.rs`, `worker.rs` |
| F10 | Autumn's request timeout bounds the response future only. A streamed body runs after the response leaves the handler. | `autumn-web-0.8.0/src/router.rs`, `RequestTimeoutService` |
| F11 | Autumn's idempotency layer collects the response body up to its cache cap before it answers. It would buffer a live stream and cache a prefix. | `autumn-web-0.8.0/src/idempotency.rs`, `handle_cache_miss` |
| F12 | A browser `EventSource` sends `GET` with no custom header. A durable route needs `Idempotency-Key`, so a browser client needs a `fetch` SSE reader. | WHATWG HTML, server-sent events |
| F13 | One execution stores at most 10,000 chunks of at most 7,000 bytes. A marker at offset `i64::MAX` ends a truncated stream. | `context.rs`, `store.rs` |
| F14 | The plugin keeps the `LISTEN` URL in `HarvestApiState::sse_notification_url`. The handle client keeps its own copy private. | `api.rs`, `handle.rs` |
| F15 | `autumn_web::test::TestClient` collects the whole body. A test reads a stream after it ends. | `autumn-web-0.8.0/src/test.rs` |

### 0.2 Brainstorm — what shape does a durable stream take?

| # | Idea | Verdict |
|---|------|---------|
| B1 | `DurableCall::stream()`, next to `send()`. It returns `DurableStream`, which implements `IntoResponse`. | **Adopted.** The key, scope, fingerprint and start stay the same code. |
| B2 | Redirect the client to `/workflows/{id}/stream/durable`. | Rejected (F6). It exposes the execution id, checks no scope, and needs the management API. |
| B3 | Stream the engine-event tail. | Rejected. It shows engine machinery and is for operators. |
| B4 | Stream the best-effort `publish_progress` chunks. | Rejected. They are not stored, so a resume loses them. |
| B5 | A cursor that is the offset only. | Rejected (F7, F8). A retry or a successor starts its offsets again. |
| B6 | A cursor that holds the execution id. | Rejected. It leaks the management handle, and a forged id needs an ownership check. |
| B7 | A cursor `<attempt>.<offset>`. The attempt is the place of the run in the lineage of the run that the key gives. | **Adopted.** A forged cursor can only skip frames of the caller's own run. |
| B8 | An `attempt` frame when the stream moves to the next run. | **Adopted.** A token view must clear the text of a failed attempt. |
| B9 | A final `result` or `problem` frame, with the body that `send()` answers. | **Adopted.** One client parser covers both paths. |
| B10 | A plain problem answer before the run is known, and a stream after. | **Adopted.** A setup or key error keeps its HTTP status. |
| B11 | Under Autumn's idempotency layer, answer `500` misconfigured before any I/O. | **Adopted** (F11). The stream can never work there. |
| B12 | Under the layer, answer pending with `409`. | Rejected. The client would retry forever and hide a setup error. |
| B13 | A stream cap of its own, `durable_route_max_streams`. | **Adopted.** A stream lives for minutes and a wait for seconds. |
| B14 | Share the waiter cap. | Rejected. Long streams would starve the waits. |
| B15 | A stream budget, `.stream_for(..)`: 5 min by default, 1 h at most. At the end, a `pending` frame. | **Adopted.** A paused run or a slow client cannot hold a connection forever. |
| B16 | Poll the table when the `LISTEN` connect fails. | **Adopted.** It matches the result waiter (issue #1717). Only the latency changes. |
| B17 | A lineage that ended before the connect takes no permit and no `LISTEN` connection. | **Adopted.** It matches the completed replay of `send()`. |
| B18 | Put the cursor in the fingerprint. | Rejected. Each resume would then answer `422`. |
| B19 | A query-string key for a plain `EventSource`. | Deferred (F12). It puts the key in access logs. |
| B20 | A shared `LISTEN` multiplexer. | Deferred. It is a core change. The stream cap bounds the cost. |
| B21 | One recursive query for the whole lineage. | Deferred. One indexed lookup for each run is simpler, and the attempt cap bounds it. |
| B22 | Encrypt chunks with the payload codec. | Out of scope. Durable streams store plain JSON (issue #1974). |

### 0.3 Reverse brainstorm — how can a durable stream do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | Stream the chunks of another principal's run. | The run comes from the scoped claim, as for `send()`. The cursor holds no execution id. |
| R2 | Leak the execution id through the SSE ids. | Each id is an attempt ordinal and an offset. The id header stays opt-in. |
| R3 | Leave a gap on resume. | The cursor is exclusive for each attempt. The store is monotonic (F3). The stream reads the run state before the chunks. |
| R4 | Send a chunk twice on one connection. | The cursor moves after each frame and each page reads above it. A reconnect with an old cursor gets at-least-once delivery. |
| R5 | Skip the chunks of a retry attempt or a successor. | The lineage walk follows `retry_of_exec_id` and `continued_from_exec_id`, one run for each step. |
| R6 | Show the text of a failed attempt as the answer. | An `attempt` frame names the new run and its cause. |
| R7 | Send the end before the last chunks. | The chunks of the last cycle commit with the terminal state. The stream reads the state first, then drains. |
| R8 | Hold a pooled connection for the whole stream. | The stream takes a pooled connection for one page read only. |
| R9 | Exhaust Postgres connections with streams. | A semaphore caps the live streams (B13). A full cap answers pending. An ended lineage needs no permit. |
| R10 | Hold a stream forever for a paused run or a stalled client. | The stream budget ends it (B15). A client that takes no frame for 60 s loses the stream. |
| R11 | Cache a stream prefix under Autumn's layer. | The route answers `500` misconfigured before any I/O (B11). |
| R12 | Cut the stream at Autumn's request deadline. | The deadline bounds the response future only (F10). The start keeps the margins of `send()`. |
| R13 | Start a second run on resume. | A resume is the same request. The claim attaches, and the cursor is not in the fingerprint. |
| R14 | Accept a malformed cursor and stream from a wrong place. | A malformed `Last-Event-ID` answers `400` `stream-cursor-invalid` before any I/O. |
| R15 | Walk a lineage forever. | The walk takes at most `durable_route_max_chain_steps` steps from run to run. A visited set stops a cycle. Both end with `result-unavailable`. |
| R16 | Leak the workflow error text in a frame. | The `problem` frame reuses the fixed problem text of `send()`. |
| R17 | Log the key, the scope, the input or a chunk. | The stream logs hold the kind of a read error only. They never hold chunk content. |
| R18 | Stream the data of an erased run. | Erasure deletes the chunks (F4). The final frame is the `erased` problem. |
| R19 | Let a proxy buffer the stream. | The answer carries `cache-control: no-cache` and `x-accel-buffering: no`. |
| R20 | Inject an SSE field through a chunk. | A chunk is compact JSON, so it holds no raw line break. Axum splits `data` by line. |
| R21 | Grow memory for a slow client. | The frame channel is bounded, and the stream reads one page of 256 rows when the channel has room. |
| R22 | End the stream as failed after one passing read error. | A read error waits one tick and reads again. A codec error in the final read answers `result-unavailable`. |
| R23 | Keep a permit after a panic in the stream task. | The task owns the permit, so an unwind drops it. |
| R24 | Make a client resume in a tight loop. | The `pending` frame sets `retry: 1000`. |
| R25 | Lose the trace of the stream task. | The task runs in the request span and logs to the request's subscriber. |

### 0.4 Six thinking hats

| Hat | Notes |
|-----|-------|
| White | F1 to F15. Unknown: the cost of many live streams. The stream cap bounds it, as the waiter cap does for waits. |
| Red | LLM token streaming must resume after a dropped mobile connection with no lost token. The `attempt` frame keeps a failed attempt from looking like the answer. |
| Black | A cross-user stream, a leaked execution id, a cached prefix, an endless stream, a missed retry. Each has a mitigation in §0.3. |
| Yellow | The app's own auth and scope guard the stream. The management API can stay off. Dedupe, fingerprint and start reuse tested code. |
| Green | A query-string key for `EventSource`, a `LISTEN` multiplexer, a lineage query, codec-encrypted chunks. All are deferred (§2). |
| Blue | Red phase: unit tests on a fake lineage, then Postgres tests and docs guards. Green phase: cursor, producer, stream start, plugin knob. Refactor phase: docs, gates, then a multi-angle review. |

---

## 1. Design

### 1.1 Handler shape

```rust
#[post("/answers")]
async fn answer(
    durable: DurableRoute,
    user: CurrentUser,
    Json(prompt): Json<Prompt>,
) -> DurableStream {
    durable
        .scope(user.id)
        .call("answer", AnswerInput { user_id: user.id, prompt })
        .failure_status("Refused", StatusCode::UNPROCESSABLE_ENTITY)
        .stream()
        .await
}
```

`stream()` runs the checks, the key, the fingerprint and the start of
`send()`. It then reads the run and answers a stream. `.wait(..)` and
`.success_status(..)` do not apply to a stream. `.stream_for(..)` sets the
stream budget.

### 1.2 Lineage and cursor

The lineage of a run is the run that the key gives, then each run that
follows it. A run follows another run when its `retry_of_exec_id` or its
`continued_from_exec_id` names that run. The first run has attempt 0. Each
step adds 1.

| SSE id | Meaning |
|---|---|
| `<attempt>.<offset>` | A chunk at `offset` of the run at `attempt`. |
| `<attempt>` | The stream moved to the run at `attempt`, and sent none of its chunks. |

A resume walks the lineage from attempt 0 to the cursor attempt. It sends the
chunks above the cursor offset, then the later runs. A cursor past the end of
the lineage sends only the final frame. An absent or empty `Last-Event-ID`
starts at attempt 0. An offset above `i64::MAX` clamps to it.

### 1.3 Stream loop

1. Read the state of the current run.
2. Send each stored chunk above the cursor, in pages of 256.
3. When the run has ended, look up the next run. When it exists, send an
   `attempt` frame and go to step 1 for it.
4. When no run follows, read the final answer with the code of `send()`, with
   no wait. Send it as the final frame and end. A pending answer reads again
   at the next tick.
5. When the stream budget is spent, send a `pending` frame and end.
6. Wait for a wake of the current run, or for one tick. The tick is 5 s with
   a `LISTEN` connection, and 1 s without one.

The state read comes before the chunk read. A terminal cycle commits its
chunks with its state, so the drain after a terminal state holds every
chunk.

### 1.4 Start of the stream

A stream needs a live permit only when its lineage has not ended. The route
reads the final answer once with no wait, inside the request deadline. When
the lineage has ended, the stream takes no permit and opens no `LISTEN`
connection. When it is live and the stream cap is full, the route answers
`202` pending with `Retry-After: 1` and the run's headers.

### 1.5 Frames

| Frame | `id:` | `data:` |
|---|---|---|
| `event: progress` | `<attempt>.<offset>` | The chunk JSON, as published. |
| `event: attempt` | `<attempt>` | `{"attempt": n, "cause": "retry"}` or `"continued-as-new"` |
| `event: result` | none | The workflow output JSON. |
| `event: problem` | none | The problem body of `send()`, with `status`. |
| `event: pending` | none | `{"state":"pending"}`, with `retry: 1000`. |

The stream also sends a `: ping` comment every 5 s. The stream answer has
status `200`, `content-type: text/event-stream`, `cache-control: no-cache`
and `x-accel-buffering: no`. It carries the replay header and the opt-in id
header of `send()`.

### 1.6 Answers before the stream

| Case | Status | Problem slug |
|---|---|---|
| Autumn's idempotency layer is on. | 500 | `misconfigured` |
| `Last-Event-ID` is repeated or does not parse. | 400 | `stream-cursor-invalid` |
| Every other check and start error of `send()`. | As for `send()` | As for `send()` |
| The lineage is live and the stream cap is full. | 202, `Retry-After: 1` | — |

---

## 2. Known limits

- All known limits of durable routes apply. Postgres plugin runtime only, and
  one shard.
- A plain `EventSource` cannot send `Idempotency-Key`. Use a `fetch` SSE
  reader that sends headers and `Last-Event-ID`.
- Only `publish_durable_progress` chunks reach the stream. Best-effort
  `publish_progress` chunks do not.
- A reset and an operator rerun start no link from the original run. The
  stream does not follow them.
- The cursor counts from the run that the key gives. After the claim window,
  that can be the run that holds the business id, so an old cursor can skip
  frames.
- Delivery is at-least-once across reconnects. Dedup by the SSE id.
- A chunk is plain JSON at rest, as for durable streams.
- The stream takes at most `durable_route_max_chain_steps` steps from run to
  run. Past that, it ends with `result-unavailable`.
- Each live stream holds one `LISTEN` connection. The stream cap (default 32
  for each process) is the only bound.
- The 60 s send timeout and the 5 s ping are fixed.
- A stream that ends with a final frame does not show the final status in the
  HTTP status line. The status is in the `problem` frame.
- No metric family. The `harvest.durable_route` span records the outcome
  `stream`.

---

## 3. Tests and acceptance criteria

Unit tests live in `autumn-harvest-plugin/src/durable_route/stream.rs`.
Postgres tests live in
`autumn-harvest-plugin/tests/durable_stream_integration.rs`. Docs guards
live in `autumn-harvest-plugin/tests/durable_route_docs.rs`.

| AC | Criterion | Test |
|----|-----------|------|
| SAC-01 | The cursor grammar accepts an attempt, or an attempt and an offset. It refuses a repeated header and malformed text, and clamps a large offset. | `cursor_grammar_table` |
| SAC-02 | Each frame id parses back to the cursor that resumes after it. | `frame_ids_round_trip_through_the_cursor` |
| SAC-03 | Under Autumn's layer, `stream()` answers `500` misconfigured before any I/O. | `stream_refuses_the_idempotent_layer_before_any_io` |
| SAC-04 | A malformed cursor answers `400` `stream-cursor-invalid` before any I/O. A key problem wins over it. | `invalid_cursor_answers_400_before_any_io` |
| SAC-05 | With no runtime, `stream()` answers `503` with `Retry-After: 1`. | `stream_without_runtime_answers_503` |
| SAC-06 | One run streams its chunks in order, then the result frame. | `producer_streams_one_run_then_the_result` |
| SAC-07 | A resume sends only the chunks after the cursor, with no gap and no duplicate. | `producer_resumes_after_the_cursor_with_no_gap_or_duplicate` |
| SAC-08 | The stream follows a retry and a continue-as-new successor, with one `attempt` frame for each. | `producer_follows_retries_and_continue_as_new_with_attempt_frames` |
| SAC-09 | A resume inside a later attempt sends no earlier frame and no repeated `attempt` frame. | `producer_resumes_inside_a_later_attempt` |
| SAC-10 | A cursor past the lineage sends only the final frame. | `producer_cursor_past_the_lineage_sends_only_the_end` |
| SAC-11 | The chunks that commit with the terminal state come before the end. | `the_end_follows_every_chunk_of_the_last_cycle` |
| SAC-12 | A spent stream budget ends with a `pending` frame. | `producer_ends_with_pending_at_the_stream_budget` |
| SAC-13 | Past the step cap, and at a cycle, the stream ends with `result-unavailable`. | `producer_stops_at_the_step_cap_with_result_unavailable`, `producer_stops_at_a_cycle_with_result_unavailable` |
| SAC-14 | A failed read waits one tick and reads again. No chunk drops. | `producer_retries_a_failed_read_and_never_drops_a_chunk` |
| SAC-15 | A failed run ends with the problem frame of `send()`. | `producer_sends_the_problem_frame_for_a_failed_run` |
| SAC-16 | A gone client ends the stream at once. | `producer_stops_when_the_client_is_gone` |
| SAC-17 | More than one page of chunks streams in order. | `producer_pages_large_streams` |
| SAC-18 | An ended lineage never waits. | `producer_waits_only_while_the_run_is_live` |
| SAC-19 | The final frames use the fixed problem text and the output JSON. | `final_frames_reuse_the_fixed_problem_text` |
| SAC-20 | The stream cap defaults to 32. The budget defaults to 5 min and clamps to 1 h. | `stream_limits_have_defaults_and_bounds`, `durable_route_limits_have_defaults_and_a_floor` |
| SAC-21 | The stream answer disables proxy buffering and carries the run headers. | `stream_response_headers_disable_proxy_buffering` |
| SAC-22 | A completed run streams every chunk, then the result. The SSE ids hold no execution id. | `stream_sends_every_chunk_then_the_result` |
| SAC-23 | A resume with `Last-Event-ID` sends the later chunks, with no gap and no duplicate. | `resume_with_last_event_id_has_no_gap_and_no_duplicate` |
| SAC-24 | A chunk published after the connect reaches the live stream. | `live_stream_delivers_chunks_published_after_connect` |
| SAC-25 | A retry of the stream starts no second run and no second side effect, and carries the replay header. | `retry_with_the_same_key_starts_no_second_run` |
| SAC-26 | A changed body under a used key answers `422` and streams nothing. | `changed_body_answers_422_and_streams_nothing` |
| SAC-27 | Two principals with one key get two runs and two streams. | `principals_with_the_same_key_get_separate_streams` |
| SAC-28 | The stream follows a workflow-level retry with an `attempt` frame. | `stream_follows_a_retry_attempt` |
| SAC-29 | The stream follows a continue-as-new chain to the final output. | `stream_follows_continue_as_new` |
| SAC-30 | A mapped failure ends with the problem frame and the mapped status. | `failed_run_ends_with_the_problem_frame` |
| SAC-31 | An erased run ends with the `erased` problem frame and sends no chunk. | `erased_run_ends_with_the_erased_problem` |
| SAC-32 | Under Autumn's layer, the route answers `500` and starts no run. | `stream_under_the_idempotent_layer_answers_500_misconfigured` |
| SAC-33 | A malformed cursor answers `400` and starts no run. | `invalid_cursor_answers_400_and_starts_nothing` |
| SAC-34 | A full stream cap answers `202` for a live run. A completed run still streams. | `full_stream_cap_answers_202_pending` |
| SAC-35 | Live streams hold no pooled connection while they wait. | `live_streams_hold_no_pooled_connection` |
| SAC-36 | The docs page states the stream contract, and the numbers match the code. | `streaming_section_states_the_client_contract` |
| SAC-37 | Every test this table names exists. The note keeps its planning sections, rows, mitigations and limits. | `stream_design_note_ac_table_names_existing_tests`, `stream_design_note_keeps_its_planning_sections` |
| SAC-38 | One changelog fragment describes the feature and states "No migration" and "No new `WorkflowEvent`". | `stream_changelog_fragment_describes_the_feature` |
| SAC-39 | The docs status table lists the new problem slug with its status. | `status_table_matches_problem_slugs`, `status_table_statuses_match_the_code` |
| SAC-40 | The CI manifest runs the new Postgres suite on Linux. | `ci_manifest_lists_every_durable_route_suite` |
| SAC-41 | Comment hygiene adds no Tier A defect and no Tier B regression. `cargo fmt` and `cargo clippy -D warnings` pass. | `docs/audits/comment-hygiene.py --base origin/trunk-dev`, CI lint job |
| SAC-42 | The diff adds no migration, no `harvest_events` writer and no version bump. | Review |
| SAC-43 | Each frame renders its documented event name, `id:`, `data:` and `retry:`. | `frames_render_the_documented_wire_form` |
| SAC-44 | No stream log holds the key, the scope, the input, the cursor, a chunk, the output or codec error text. The span records the outcome. | `stream_logs_hold_no_key_scope_input_or_chunk` |
