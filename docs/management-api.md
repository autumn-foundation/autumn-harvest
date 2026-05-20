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
