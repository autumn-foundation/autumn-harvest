# Durable per-execution workflow logs

*Issue #790. Opt-in. Additive.*

A workflow author can already emit replay-safe log lines from workflow code:

```rust
#[workflow]
async fn fulfill_order(ctx: &WorkflowContext, order: Order) -> Result<(), String> {
    ctx.log_info("charging card");
    // ...
    ctx.log_warn("carrier API degraded; falling back");
    // ...
    ctx.log_error("no carrier accepted the shipment");
    Ok(())
}
```

Those lines go to the host application's `tracing` subscriber (issue #379), which
means reading them back means leaving Vantage for Loki/Elastic/OTel and
correlating by `execution_id` by hand. That context switch dominates MTTR for the
exact question an operator asks first: *what did this run actually say?*

Enable the **durable sink** and the same lines are also persisted per execution,
readable in one call:

```bash
curl -s .../api/harvest/workflows/{execution_id}/logs | jq -r '.lines[] | "\(.level)\t\(.message)"'
```

## Enabling it

One builder call. Absent it, nothing changes — `ctx.logger()` behaves exactly as
today (tracing-only), and every existing workflow compiles and runs unchanged.

```rust
use autumn_harvest::WorkflowLogPolicy;

HarvestPlugin::new()
    // Defaults: 1,000 lines per execution, 4 KiB per message.
    .workflow_log_persistence(WorkflowLogPolicy::default())
```

Tune the bounds explicitly if the defaults do not suit:

```rust
.workflow_log_persistence(
    WorkflowLogPolicy::default()
        .with_max_lines(200)
        .with_max_message_bytes(1_024),
)
```

`with_max_lines(0)` is clamped up to `1` — a policy that admits nothing would be
a silent-loss trap, and disabling persistence is what *omitting* the builder call
already means.

## Reading them

### HTTP

```
GET /api/harvest/workflows/{id}/logs
```

Admin-gated (a log message is free-form author text that routinely carries
business detail, so it takes the same posture as the sibling per-execution
diagnostics like `/awaitables`, not the plain execution-row one).

| Query param | Meaning |
|---|---|
| `limit` | Page size. Default 200, clamped to `1..=1000`. |
| `cursor` (alias `after`) | Exclusive keyset cursor: the previous page's last `seq`. |
| `level` | Repeatable, or comma-separated. One of `info`, `warn`, `error`. |
| `since` | RFC 3339 exclusive lower bound on `occurred_at`. |

An unknown `level=` value is a **400**, never a silently-empty page: a typo must
never look like "this run logged nothing."

Response:

```json
{
  "execution_id": "…",
  "lines": [
    { "seq": 16777216, "level": "info",  "message": "charging card",
      "occurred_at": "2026-07-19T10:04:11.221Z" }
  ],
  "next_cursor": null,
  "total_lines": 3,
  "truncated": false
}
```

`total_lines` and `truncated` are **filter-independent** — a `?level=error` page
still reports the run's true totals and whether it was capped.

### CLI

```bash
harvest workflow logs <execution-id>
harvest workflow logs <execution-id> --level error --limit 50
harvest workflow logs <execution-id> --cursor 16777218
```

### Vantage

The execution-detail page gains a **Logs** panel with level filtering
(All / info / warn / error), sourced from the same rows the API serves.

Two caveats, both surfaced in the panel itself rather than left implicit:

- **Admin-only.** The panel is rendered only for a principal with Harvest admin
  access, matching the route's own gate. A non-admin viewing the page sees the
  rest of the execution detail with no Logs panel at all.
- **First page only.** The panel loads the **first 200 lines** (oldest first),
  not the most recent. Under the default 1,000-line cap a long run's later lines
  are only reachable through the paginated API or the CLI; the panel says so
  when it is showing a full page.

## Ordering: `seq`, not `occurred_at`

Lines are returned in **emission order**, keyed by `seq` — a deterministic
*logical-position identity* derived from the workflow's position in its own
history, not a wall clock.

`occurred_at` is reported for context but is **not** the sort key and is **not**
monotonic: a workflow's decision cycles can run on different workers whose clocks
disagree. Always paginate with `cursor`, never with `since`.

## At-most-once, deduplicated

The guarantee is **never more than one stored row per logical emission**, with no
duplication and no reordering, regardless of how many times a decision cycle is
re-driven. It is deliberately *not* an exactly-once delivery guarantee — see
"Best-effort delivery" below.

Two mechanisms combine:

A durable write inherits `ctx.logger()`'s existing replay suppression (issue
#379): emission is a no-op while `ctx.is_replaying()` is true, so a replayed
cycle re-emits nothing.

That alone is *not* sufficient. A decision cycle that logs and then parks can be
re-driven at an **unchanged** history position — a spurious wake, or a
rolled-back persist — where `is_replaying()` is still `false`, so the line is
genuinely re-emitted. Because `seq` is a pure **call ordinal** — the Nth
`ctx.log_*` call made by this run of the workflow body always carries `seq == N`,
whichever cycle happens to emit it live — the re-emitted line carries the *same*
`seq`, and a `UNIQUE (workflow_exec_id, seq)` index plus `ON CONFLICT DO NOTHING`
collapses it to one stored row.

### Best-effort delivery

A log write is deliberately **best-effort**: it rides a nested transaction
(SAVEPOINT) inside the workflow's persist transaction, so a failed log INSERT
rolls back only itself and can never wedge the workflow. The consequence is that
a line can be stored **zero** times — if the emitting cycle's persist is rolled
back and the workflow then re-drives down a *different* code path that no longer
makes that call, nothing is retried. Logs are observational (AC7); a missing line
is an observability gap, never a correctness one.

### `seq` keys position, not content

`seq` identifies *which `ctx.log_*` call* produced a line, not *what the line
said*. If a deploy changes the message text of the Nth call and an in-flight
execution is re-driven at that same position, the new text carries the same `seq`
and is **dropped** by `ON CONFLICT DO NOTHING` — the originally stored text wins.
This is the correct trade-off (it is what makes a re-drive idempotent), but it
means the stored line reflects the code that first emitted it, not necessarily
the code currently deployed.

## Bounds and truncation

Volume is bounded two ways:

- **Per message**: `max_message_bytes` (default 4 KiB). An oversized message is
  truncated on a UTF-8 character boundary, never rejected.
- **Per execution**: `max_lines` (default 1,000), **drop-newest**. Once the cap
  is reached, later lines are dropped and a single synthetic **truncation
  marker** row is appended:

  ```
  [harvest] per-execution log cap reached; subsequent lines were dropped
  ```

  The marker is self-describing (`"truncation_marker": true` on the wire) and the
  response-level `truncated` flag reports it independently of the current page's
  filters. Loss is always **visible**, never silent.

Drop-newest is deliberate: the first lines of a run are the ones that explain how
it got where it is. A run that logs 10,000 lines has a logging problem, and the
early lines are what diagnose it.

`max_lines` bounds **memory as well as storage**: a decision cycle stops queuing
`ctx.log_*` calls once it holds `max_lines + 1` of them, so a workflow logging in
a tight loop without suspending cannot retain an unbounded number of messages
before the write. (The one extra is what guarantees the truncation marker still
fires — a batch of exactly `max_lines` would be admitted whole and look like a
run that dropped nothing.) The stored outcome is identical either way; the bound
just moves the drop from the database to the point of the call.

**The marker is terminal.** Once an execution has truncated, later lines stay
rejected even if `max_lines` is subsequently **raised**. `max_lines` is
per-worker-process config, so on a rolling deployment a run can truncate under an
old worker's cap and have its next decision cycle handled by a new worker with a
larger one. Re-deciding admission against the current policy would store a line
*after* the one that was dropped — leaving a hole in the stored prefix and a
marker whose "subsequent lines were dropped" claim is false. Latching keeps the
stored rows a contiguous prefix of the run and keeps the marker honest; the
already-dropped lines are unrecoverable either way, so re-opening the gate buys
nothing. Raise the cap for *future* runs; an already-truncated one stays
truncated.

Both caps clamp **up to 1** rather than accepting zero: `max_lines(0)` would
store nothing at all (silent total loss — exactly what the visible truncation
marker exists to prevent) and `max_message_bytes(0)` would empty every message.

### The `seq` ceiling

`seq` is reserved at its top value for the truncation marker, and the worker
saturates a line's ordinal just below it. A workflow would have to make ~2^63
`ctx.log_*` calls in one run to reach that ceiling, and the per-execution cap
stops storing long before — but note the failure *shape* differs from the
sibling `publish_progress` (#791) primitive, whose saturation is harmless: here
two distinct calls that both saturate would carry the same `seq`, so the second
is deduplicated away rather than stored. Unreachable in practice; documented so
the clamp is not mistaken for a no-op.

## Retention and erasure

Log rows carry a `workflow_exec_id` foreign key with `ON DELETE CASCADE`. When
the retention janitor deletes an execution, its logs go with it in the same
statement — so log retention is **tied to, and can never outlive,**
workflow-history retention. There is no separate log-retention setting to
misconfigure and no separate janitor to fall behind.

Targeted PII erasure (issue #495) **deletes** an execution's log rows rather than
tombstoning them: a log line is a single free-form author string with no field
structure to preserve, so a tombstone would carry no information a plain absence
does not. The `EraseOutcome` reports `logs_deleted`.

**Archival asymmetry.** A registered `HistoryArchiver` (issue #345) receives the
execution's *event history* before retention deletes it, so archived history
outlives the row. Logs have **no** archival hook: they cascade away with the row
and are not included in the archived snapshot. If a run's author-emitted lines
need to outlive its retention window, forward them from the `tracing` sink to
your own log store — the durable sink is an in-product triage surface, not an
archive.

## Contract: observational only

This is the load-bearing boundary. Durable logs are **not** part of the durable
execution contract:

- They are **not** part of the event history. No `WorkflowEvent` variant was
  added and `harvest_events` is untouched. A log line lives only in
  `harvest_workflow_logs`.
- They carry **no determinism guarantee**. A log message may embed
  non-deterministic content; it is never replayed, so nothing depends on it.
- They are **never read back into workflow logic**. There is no
  `ctx.read_logs()` and there never will be. A workflow that needs to *act* on
  something must record it as state — a search attribute, an activity result, or
  `ctx.set_current_details` — not as a log line.

Concretely: the durable sink can be enabled or disabled, and log rows can be
deleted by retention or erasure, with **zero** effect on whether a workflow
replays or what it computes.

## What this is not

| Want | Use instead |
|---|---|
| A live tail of a running workflow's output | `GET /workflows/{id}/stream` (issue #791) |
| A single "what is it doing right now" status string | `ctx.set_current_details` (issues #473/#593) |
| Engine machinery (activity scheduled/completed, timers) | `GET /workflows/{id}/history` |
| Where the time went | `GET /workflows/{id}/timeline` (issue #739) |
| What a run is parked on | `GET /workflows/{id}/awaitables` (issue #615) |
| Full-text search across many executions | Your log aggregator — out of scope here |

Also out of scope: capturing the host app's general `tracing` output,
activity-side logs, structured key/value fields per line, streaming/tailing, and
external sinks.

## See also

- `autumn-harvest/examples/workflow_logs.rs` — a runnable end-to-end example.
- [`docs/workflow-determinism-guide.md`](workflow-determinism-guide.md) — why
  `ctx.log_*` is replay-safe and `println!`/`tracing::info!` are not.
