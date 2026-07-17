# Live workflow output streaming — `ctx.publish_progress`

*Issue #791.* An **ephemeral, best-effort live-output side channel** for workflow
authors: an AI agent streaming tokens, a long import reporting per-item progress,
any interactive flow that wants to push incremental, author-defined output to a
waiting client while the durable run is still executing — **without** standing up
an external message bus and **without** bloating the event log.

```rust
use autumn_harvest::prelude::*;

#[workflow]
async fn summarize(ctx: &WorkflowContext, doc: Document) -> Result<Summary, String> {
    for (i, section) in doc.sections.iter().enumerate() {
        // Emit an author-defined chunk to any connected subscriber. Fire-and-forget:
        // returns immediately, never blocks on a subscriber, never fails the workflow.
        ctx.publish_progress(serde_json::json!({
            "step": i,
            "status": "summarizing",
            "section": section.title,
        })).map_err(|e| e.to_string())?;

        let _ = ctx.execute_activity(&summarize_section_info(), section.clone()).await;
    }
    Ok(Summary::default())
}
```

## The API

```rust
impl WorkflowContext {
    pub fn publish_progress(&self, chunk: impl serde::Serialize) -> HarvestResult<()>;
}
```

`chunk` is **any** serializable value — the shape is entirely author-defined
(it is your product's stream contract, not the engine's). The call is
synchronous and non-suspending: it does not `.await`, does not wait for a
subscriber, and does not participate in any suspension batch. `Err` is returned
only if the chunk fails to serialize.

Where it fits among the neighboring read surfaces:

| Surface | Direction | Shape | Audience | Durable? |
|---|---|---|---|---|
| **`ctx.publish_progress` + `GET /stream`** (this) | push | ordered author-defined chunks, live | end-user | no (ephemeral) |
| [`set_current_details`](../CLAUDE.md) (#473/#593) | pull | one overwriting status string | operator/end-user | yes (a column) |
| Engine-event SSE tail (#324) | push | engine machinery (`ActivityScheduled`/…) | **operator only** (admin-gated) | reads durable events |
| Await-completion long-poll (#527) | pull | terminal result only | end-user | yes (the result) |
| Durable per-execution logs (#790) | pull | persisted triage record | operator | yes |

Use `publish_progress` when you want **live, incremental, author-defined
content** streamed to a client. Use `set_current_details` for a single
"what am I doing right now" status an operator reads on demand; use the
await-completion long-poll for the final result.

## Determinism contract

`publish_progress` is a **pure side effect**, and that is the whole design:

- **Replay-neutral.** The call is a **no-op when `ctx.is_replaying()` is true** —
  it returns `Ok(())` immediately, pushes **no `WorkflowCommand`**, and consumes
  **no sequence number** (mirroring `set_current_details`).
- **Zero event-log footprint.** It writes **nothing** to `harvest_events`. A
  workflow that publishes N chunks produces a **byte-for-byte identical history**
  to one that publishes none — verified by a replay test
  (`publish_progress_replays_with_zero_divergence`) and a footprint test
  (`publish_progress_leaves_zero_event_footprint`, which compares the recorded
  event-type sequence against a no-progress sibling workflow).
- **No new `WorkflowEvent` variant. No migration. No shard-routing change.**
- **Chunk content MAY be non-deterministic.** Because a chunk is never recorded
  and never replayed, it is *safe* to publish values that would break replay if
  they entered the event log — LLM tokens, `chrono::Utc::now()`, a sampled
  metric, a random id. This is the opposite of an activity input or a workflow
  command, where non-determinism corrupts replay. Publish freely.

Internally the live path pushes a **bookkeeping** `WorkflowCommand::PublishProgress`
that the worker translates into a per-execution `pg_notify` at persist time and
then discards — it is never appended to the durable log. Publishing from a
throwaway `WorkflowContext` (inside a query or update handler) is a harmless
no-op: that context is never drained by the worker persist path.

## Delivery model — ephemeral, best-effort

This is a **product-UX channel, not an audit record.**

- **Dropped when nobody is listening.** Chunks ride Postgres `LISTEN`/`NOTIFY`.
  If no subscriber is connected to the execution's channel at publish time, the
  chunk is **dropped**. The durable workflow **result** remains authoritative —
  the stream is a convenience, never the source of truth.
- **No backfill, no replay of chunks, no at-least-once.** A subscriber that
  connects late does not receive earlier chunks. There is no buffering for late
  subscribers and no gap-filling across a reconnect — durable per-execution logs
  (#790) are the separate primitive for a persisted, replayable record.
- **Dropped under back-pressure from a slow-but-connected client.** The SSE
  producer sends chunks **non-blocking**: if a slow subscriber lets the bounded
  per-stream buffer fill, the *excess* chunks are **dropped** rather than
  buffered unboundedly or back-pressured into Postgres' shared `NOTIFY` queue.
  The monotonic `seq` lets the client detect the resulting gap. Only a
  *disconnected* client ends the stream.
- **Best-effort by construction.** A `NOTIFY` failure at the worker is logged and
  **swallowed** — publishing can never fail or slow a workflow. A published chunk
  is capped at **7000 serialized bytes** (Postgres' `NOTIFY` payload limit is
  8000, with headroom reserved for the delivery envelope); an oversize chunk is
  **replaced** — never silently dropped — with the marker
  `{"_harvest_progress_truncated": true, "bytes": <original_len>}`, so the client
  still sees an ordered slot and knows truncation occurred. Keep chunks small;
  stream many small ones rather than one large one.

## The SSE route

```
GET /api/harvest/workflows/{id}/stream
```

Returns `text/event-stream`. Frames, in publish order:

| Frame | `id:` | `data:` | Meaning |
|---|---|---|---|
| `event: progress` | the chunk's `seq` | the raw chunk JSON (exactly what you published) | one published chunk |
| `event: end` | — | `{"reason": "<terminal-state>"}` | the workflow reached a terminal state; stream closes |
| `event: error` | — | `{"error": "listen_connection_closed"}` | the underlying `LISTEN` connection dropped; stream closes |

The `data:` of a `progress` frame is the chunk **verbatim** — it is *not*
re-wrapped in any envelope; the sequence number rides the SSE `id:` field
instead. The stream also emits periodic `: ping` keepalive comments.

Consume it with any SSE client. With `curl`:

```console
$ curl -N http://localhost:8080/api/harvest/workflows/{exec_id}/stream
event: progress
id: 42
data: {"step":0,"status":"summarizing","section":"Intro"}

event: progress
id: 43
data: {"step":1,"status":"summarizing","section":"Body"}

event: end
data: {"reason":"completed"}
```

**Clean close (AC4).** The stream ends promptly on terminal state: an
already-terminal execution emits a single `end` frame and closes immediately; a
running execution is polled on each keepalive tick so close latency is bounded to
one keepalive interval; an idle client disconnect and a dropped `LISTEN`
connection both end the stream rather than hanging. A malformed execution id
returns `400`; an unknown execution returns `404`.

### Monotonic `seq` — the client contract (AC6)

Each chunk carries a **strictly increasing, execution-lifetime-monotonic**
sequence number, surfaced as the SSE `id:`. It is `epoch`-prefixed — the high
bits encode the workflow's loaded-history length at the start of the decision
cycle that published the chunk, and the low bits are a per-cycle counter — so the
number grows across decision cycles as well as within one. Crucially, `seq` is a
**logical-position identity, not a per-chunk counter**: a re-driven workflow
position (a spurious wake, or a rolled-back cycle that re-runs) deterministically
re-emits the **same** `seq`.

What a subscriber should do:

- **Dedupe by `seq`, keep-first.** For at-most-once-per-position display, ignore
  a chunk whose `seq` you have already seen. A re-emitted chunk MAY carry
  *different content* for a non-deterministic chunk (an LLM token stream, a live
  metric) — **the first-delivered is canonical**.
- **Detect gaps via forward `seq` jumps.** After a reconnect, if the first new
  `id:` jumps past the last seen `seq`, chunks were missed in the gap (delivery
  is best-effort — a slow-consumer drop, a late connect, or a reconnect window).
  **The engine does not fill the gap**; refetch authoritative state from the
  workflow result or a query handler if you need completeness.
- **`seq` is per `exec_id`.** A continue-as-new successor is a **new `exec_id` on
  a new stream** whose `seq` restarts — subscribe to the successor's stream to
  follow the chain.

## Auth — default posture (AC5)

The stream route is **deliberately not** `require_admin`. It is an
**end-user-facing** read path — an app's users stream their own workflows —
distinct from the admin-only engine-event tail (#324).

It inherits the general `api_with_auth` middleware you configure on the Harvest
API, exactly like the other management routes. **If you configure no auth, the
route is open.** Embedders surfacing streams to untrusted end-users **must** front
this route with their own authentication and per-execution authorization
(typically your app already knows which `exec_id` belongs to the requesting
user). Do not expose an unauthenticated `/stream` to the public internet.

**`exec_id` is a bearer capability for live chunk content.** The engine only
checks that the execution *exists* (returning `404` otherwise) — it performs **no
authorization**. Anyone who presents a valid `exec_id` receives that run's live
chunk **content**, which may include sensitive workflow output. If your chunks
carry anything sensitive, you **must** add a per-execution authorization check in
your own middleware. `exec_id` is a random UUIDv4 (not enumerable), which
mitigates blind probing but is **not** an access-control substitute.

**Cap concurrent streams (DoS).** Each subscriber holds **one dedicated,
non-pooled Postgres `LISTEN` connection** for the lifetime of the stream. A flood
of concurrent `/stream` requests can therefore exhaust database connections.
Embedders should rate-limit stream opens and/or cap concurrent streams per user.
A global concurrent-stream cap is a possible future enhancement; today it is the
embedder's responsibility.

## Out of scope

Explicitly **not** provided by this feature (see the issue for rationale):

- **Durable persistence or replay of chunks** — that is the durable per-execution
  logs primitive (#790). Progress chunks are ephemeral.
- **At-least-once / exactly-once delivery, cross-reconnect ordering guarantees,
  or buffering for late subscribers** — best-effort only.
- **Final-result delivery** — use the await-completion long-poll (#527).
- **Snapshot state reads** — use a query handler or `set_current_details`
  (#473/#593).
- **Client → workflow streaming** — that is signals and updates.
- **Cross-shard fan-in of one logical stream** — a stream is scoped to a single
  `exec_id` on its owning shard.

## See also

- `autumn-harvest/examples/streaming_agent.rs` — a worked AI-agent-shaped example
  that publishes per-step chunks and documents the `curl -N` consumer.
- [`set_current_details`](../CLAUDE.md) (#473/#593) — the single-value pull status
  companion.
- Engine-event SSE tail (#324) — the admin-only operator triage stream.
- Await-completion long-poll (#527) — terminal result delivery.
