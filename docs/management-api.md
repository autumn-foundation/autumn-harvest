# Management API

This document covers the HTTP management API mounted by `autumn-harvest-plugin`.

## SSE Execution Event Stream

### Endpoint

```
GET /executions/{exec_id}/events/stream
```

**Auth**: same bearer-token gate as all other management API routes (issue #174). Returns `401` if the token is missing or invalid.

**Content-Type**: `text/event-stream`

**Category**: read-only; classified as `ReadOnly` in the audit system.

---

### Wire format

Each event arrives as a standard SSE block:

```
id: <harvest_events.id BIGSERIAL>
event: <WorkflowEvent type name>
data: <JSON event payload>

```

- `id` is the row-level `BIGSERIAL` primary key of `harvest_events`, **not** the per-execution sequential event ID. It is monotonically increasing across all executions on the shard and is safe to use as a resume cursor.
- `event` is the adjacently-tagged type string (`ActivityScheduled`, `SignalReceived`, etc.). New `WorkflowEvent` variants land in the stream automatically without client changes.
- `data` is the `data` inner object of the adjacently-tagged JSON envelope `{"type":"…","data":{…}}` stored in `harvest_events.event_data`.

#### Keepalive

Between events the server emits an SSE comment line at the configured interval (default 15 s):

```
: ping

```

Keepalive comments prevent reverse proxies and load balancers from killing idle connections. They carry no semantic content and should be discarded by clients.

#### Terminal marker

When the execution reaches a terminal state (`Completed`, `Failed`, `Cancelled`, `TimedOut`, `ResetTerminated`), the server sends a final event block then closes the stream:

```
id: <last_row_id>
event: stream-end
data: {"reason":"completed","execution_id":"<exec_id>","state":"COMPLETED"}

```

The `reason` field mirrors the terminal event type in lowercase. Receiving `event: stream-end` is the client's signal to **stop reconnecting** — the execution will not produce more events. Contrast this with a transport drop, where the client should reconnect with `Last-Event-ID`.

#### Slow consumer

If the client cannot drain the server-side buffer (default depth 1024 events) before it fills, the server sends a final `event: stream-error` block and closes the stream with HTTP 409:

```
event: stream-error
data: {"error":"slow_consumer","drop_after_event_id":<n>}

```

The client may reconnect immediately with `Last-Event-ID: <n>` to resume from where the stream was cut.

---

### Resume protocol (`Last-Event-ID`)

The browser `EventSource` API sends `Last-Event-ID` automatically on reconnect. Curl and custom clients must set it explicitly.

When the server receives `Last-Event-ID: <n>`:

1. It queries `harvest_events` for all rows with `id > n` for this execution (the backfill).
2. It sends the backfill rows over the stream in ascending `id` order.
3. It then enters live-tail mode, forwarding new events via LISTEN/NOTIFY.

A client that drops mid-stream and reconnects with the last `id` it saw will receive every event exactly once with no gaps.

**First connection** (no `Last-Event-ID`): the server starts from the beginning — all existing events are backfilled, then live-tail begins.

---

### Sharding

The endpoint resolves `exec_id.shard()` and subscribes to that shard's Postgres LISTEN/NOTIFY channel. Cross-shard fan-out is not required in v1; one stream always maps to one shard.

---

### Concurrency model

The server holds **no database connection while idle**. Each LISTEN/NOTIFY notification triggers a short-lived pooled connection to load new events, which is released immediately after the batch is sent. A single harvest process can sustain ≥ 1,000 concurrent SSE streams within the configured worker DB pool ceiling (DD-2).

---

### Audit trail

Opening a stream records one `execution.stream.open` audit entry with:
- `actor`: extracted from the auth context
- `exec_id`: the target execution
- `last_event_id_seen`: the cursor value from `Last-Event-ID` (or `-1` for a fresh stream)

Per-event audit entries are **not** written — that would amplify writes by 1:N. Stream close is audited as `execution.stream.close` when the producer task exits.

---

### Working `EventSource` example (browser)

```javascript
const execId = '00000000-0000-0000-0000-000000000001';
const token  = 'your-mgmt-token';

// EventSource does not support custom headers natively in all browsers.
// Use a query-param token or an initial cookie exchange instead.
// For internal tools, a ServiceWorker or fetch-based polyfill handles auth headers.
const url = `/api/harvest/executions/${execId}/events/stream`;

const es = new EventSource(url);

es.addEventListener('message', (e) => {
  const payload = JSON.parse(e.data);
  console.log('event id', e.lastEventId, payload);
});

es.addEventListener('stream-end', (e) => {
  const { reason, state } = JSON.parse(e.data);
  console.log(`execution finished: ${state} (${reason})`);
  es.close();          // stop reconnecting — execution is terminal
});

es.addEventListener('stream-error', (e) => {
  const { drop_after_event_id } = JSON.parse(e.data);
  console.warn(`slow consumer; resume from ${drop_after_event_id}`);
  es.close();
  // reconnect logic: open a new EventSource and the browser will send
  // Last-Event-ID: <drop_after_event_id> automatically.
});

es.onerror = (e) => {
  // transport error or server 4xx/5xx — browser will retry with Last-Event-ID
  console.error('SSE error', e);
};
```

---

### CLI usage

```bash
# Tail a live execution (Ctrl-C to stop)
harvest events tail <exec_id>

# Resume from a known cursor (skips events already seen)
harvest events tail <exec_id> --last-event-id 4200
```

Each event prints as `<event-type>: <json-data>` to stdout, one per line.
Keepalive comments are discarded silently.
The command exits cleanly when the server sends `event: stream-end`.

---

### Reverse-proxy and CDN notes

| Environment | Required config |
|-------------|----------------|
| **nginx** | `proxy_set_header X-Accel-Buffering no;` on the upstream location. Without this, nginx buffers the response and the client sees no events until the buffer fills. |
| **Cloudflare** | No special config required since 2024; Cloudflare proxies SSE transparently. Enterprise plan: disable `rocket_loader` on the management API path if enabled. |
| **AWS ALB** | Set `idle_timeout` ≥ 65 s. The SSE keepalive fires every 15 s (default), which keeps ALB's 60 s default from killing idle connections. |
| **HAProxy** | Set `timeout tunnel` on the backend section (e.g. `timeout tunnel 10m`). Without it, HAProxy applies the shorter `timeout server` to streaming connections. |
| **Traefik** | No special config required. Traefik detects `text/event-stream` and disables response buffering automatically. |

**Important**: SSE requires HTTP/1.1 or HTTP/2. Ensure the proxy does not downgrade the connection to HTTP/1.0, which does not support chunked transfer encoding.

---

## Listing workflows (`GET /workflows`)

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `workflow_name` | string | Filter to a single workflow type by name |
| `workflow_id` | string | Filter by logical workflow ID |
| `state` | string | Filter by execution state (e.g. `RUNNING`, `COMPLETED`) |
| `limit` | integer | Maximum rows to return (default 200, max 200) |
| `search_attr` | repeated | `key:value` pairs against `search_attrs` JSONB |
| `no_progress_minutes` | integer | Return stalled workflows with no task activity for N minutes |
| `sla_breached` | bool | Filter to executions that have breached their SLA |
| `page_size` | integer | Per-page limit for keyset pagination (1–200; overrides `limit`). Presence activates the opt-in paginated envelope. |
| `cursor` | string | Opaque continuation token returned by a previous page's `next_cursor`. Presence activates the opt-in paginated envelope. |
| `order` | `asc` \| `desc` | Walk order on `(created_at, id)`. Default `desc` (newest first). Presence activates the opt-in paginated envelope. |
| `started_after` | RFC 3339 | Return only executions whose `started_at` is after this timestamp |
| `started_before` | RFC 3339 | Return only executions whose `started_at` is before this timestamp |
| `exec_id_prefix` | string | Return only executions whose string-formatted `exec_id` starts with this prefix |

### Response shape — opt-in envelope

When **none** of `page_size`, `cursor`, or `order` are present the endpoint returns a bare JSON array, identical to the pre-pagination behaviour:

```json
[ { "workflow_id": "...", "state": "RUNNING", ... }, ... ]
```

When **any** of `page_size`, `cursor`, or `order` are present the endpoint returns a paginated envelope:

```json
{
  "workflows": [ { "workflow_id": "...", "state": "RUNNING", ... }, ... ],
  "next_cursor": "<opaque string or null>"
}
```

`next_cursor` is `null` on the last page. Pass it verbatim as the `cursor` query parameter to fetch the next page.

### Keyset cursor semantics

Pagination is keyset-based over `(created_at, id)`:

- The cursor encodes the `(created_at, id)` values of the **last row returned** on the previous page.
- Each subsequent page request adds `WHERE (created_at, id) < cursor` (for `order=desc`) or `WHERE (created_at, id) > cursor` (for `order=asc`).
- The cursor is opaque — do not parse or construct it manually.
- Inserting new rows while paginating does not cause duplicates or skips: the keyset predicate anchors each page to a stable position in the index.

### `order` parameter contract

| Value | Walk direction | Use case |
|-------|---------------|----------|
| `desc` (default) | newest first | Browsing the live queue; monitoring dashboards |
| `asc` | oldest first | Draining a backlog in FIFO order; catch-up processing |

`order=asc` combined with `page_size` and `next_cursor` walks from the oldest execution forward.

### Combining with time-range filters

`started_after` / `started_before` filter on the `started_at` column (when the execution began), while the cursor/sort key is `created_at` (when the row was inserted). The two are closely related but not identical (a workflow can be inserted and queued before it starts). You can combine them freely.

### Example — paginate through all running workflows in pages of 50

```bash
# First page
curl "/workflows?state=RUNNING&page_size=50"
# Response: { "workflows": [...50 rows...], "next_cursor": "abc123" }

# Second page
curl "/workflows?state=RUNNING&page_size=50&cursor=abc123"

# Keep following next_cursor until it is null
```

### Notes

- The `no_progress_minutes` (stalled-workflow) filter always returns a bare array regardless of pagination parameters.
- `page_size` and `limit` are aliases; if both are present `page_size` wins.
- On a sharded deployment each shard is queried independently with the same cursor and `page_size+1` limit; the results are k-way merged and truncated to `page_size` before the response is returned. See `docs/sharding.md` for the cross-shard keyset contract.

## Workflow Stack (describe)

### Endpoint

```
GET /workflows/{exec_id}/stack
```

Returns a point-in-time "describe" view of an in-flight workflow: its pending
activities, local activities, external handoffs, timers, signals, buffered
signals, and child workflows. This is the primary view for triaging a running
execution. For a terminal execution the pending arrays are empty.

### Pending activity heartbeat checkpoint (issue #503)

Each entry in `pending_activities[]` surfaces the latest **heartbeat checkpoint
payload** the activity reported via `ctx.heartbeat(...)` — the current value of
`harvest_task_queue.heartbeat_details` for that task row, alongside the existing
`last_heartbeat_at` timestamp. This lets an operator answer "how far has this
in-flight activity progressed?" (e.g. `{"processed": 4500, "total": 10000}`)
from a single API call, with zero direct database access.

| Field | Type | Meaning |
|-------|------|---------|
| `last_heartbeat_at` | timestamp \| null | When the most recent heartbeat was flushed. |
| `heartbeat_details` | JSON \| null | The latest checkpoint payload, verbatim as the activity reported it. `null` when no heartbeat has been flushed, **or** when the payload exceeded the response size cap (see `heartbeat_details_truncated`). |
| `heartbeat_details_truncated` | bool | `true` when the stored payload exceeded the activity-result payload cap (issue #252, default **2 MiB**) and was withheld from the response. |
| `heartbeat_details_bytes` | integer \| null | Observed serialized byte size of the stored payload, when one exists. With `heartbeat_details_truncated: true` this is the size of the withheld blob. |

Notes:

- **Regular activities only.** Local activities cannot heartbeat
  (`ctx.heartbeat(...)` returns a `Config` error), so `pending_local_activities[]`
  has no heartbeat checkpoint field.
- **Read-only.** Surfacing the checkpoint never writes, clears, or alters the
  column, and never affects retry-resume semantics
  (`ActivityContext::heartbeat_details::<T>()` reads the same value unchanged).
- **Shard-correct.** The handler routes by the execution's encoded shard; the
  heartbeat column lives on the same shard as the task row, so no cross-shard
  fan-out is introduced.
- **Size-bounded.** The 2 MiB ceiling reuses the existing activity-result
  payload cap rather than a new limit, so a pathological heartbeat payload
  cannot bloat the describe response.

### Example

```bash
curl "/api/harvest/workflows/$EXEC_ID/stack"
```

```json
{
  "exec_id": "…",
  "workflow_name": "etl_pipeline",
  "state": "RUNNING",
  "pending_activities": [
    {
      "activity_name": "process_batch",
      "task_status": "RUNNING",
      "last_heartbeat_at": "2026-05-30T19:00:04Z",
      "heartbeat_details": { "processed": 4500, "total": 10000 },
      "heartbeat_details_truncated": false,
      "heartbeat_details_bytes": 31
    }
  ]
}
```
