# Vantage UI

Vantage is the embedded, server-rendered dashboard bundled with `autumn-harvest-plugin`. It mounts at the path configured by `HarvestBuilder::harvest_api` (e.g. `/api/harvest/ui`) and requires no external assets or CDN — all CSS is inlined.

## Pages

### Workflow list — `/workflows`

Displays a paginated table of workflow executions across all configured shards.

**Filter controls**

| Control | Query param | Notes |
|---------|-------------|-------|
| State | `state` | Multi-value; defaults to all states |
| Workflow name | `workflow_name` | Exact match |
| Started after | `started_after` | ISO-8601 datetime string (e.g. `2026-01-01T00:00:00Z`) |
| Started before | `started_before` | ISO-8601 datetime string |
| Execution ID search | `exec_id_search` | Prefix match on the execution UUID |

Combine any number of filters in a single URL. Hitting **Reset** clears all filters and returns to the default view.

### Workflow detail — `/workflows/{exec_id}`

Shows a single workflow execution in full detail.

**Metadata card** — execution ID, workflow/run IDs, shard, queue, start/complete timestamps, duration, parent execution link, sticky worker, and execution timeout.

**Input / Output / Memo / Search attributes** — JSON cards rendered below the metadata.

**Activity attempts panel** — grouped by `activity_id`; shows the activity function name, attempt count, last status, and last error (if any). Hidden when there are no activity events.

**Children panel** — lists child workflow executions that have `parent_id` pointing at this execution. Hidden when there are none.

**Signals & Updates panel** — shows `SignalReceived`, `UpdateAdmitted`, `UpdateCompleted`, and `UpdateFailed` events with type, signal/update name, and timestamp. Capped at 20 entries; the heading shows the total when truncated. Hidden when there are none.

**Event timeline** — paginated at 100 events per page. Shows a human-readable label alongside the raw event type code and timestamp. Pagination controls appear above and below the table for large histories.

**Blocked-on panel** — for RUNNING workflows, shows pending task queue entries (activities), pending timers, and pending signals. Terminal workflows show "No pending work items." instead.

**Operator actions**

- **Cancel** — POST to `/workflows/{exec_id}/cancel` with a confirmation dialog. Redirects to the detail page with a flash message.
- **Terminate** — Disabled button (not yet available).
- **Send signal** — Expandable form (collapsed by default). POST to `/workflows/{exec_id}/signal` with `signal_name` and an optional JSON `payload`. Redirects with flash.
- **Reset to event N** — Expandable form. POST to `/workflows/{exec_id}/reset` with `reset_to_event_id` (1-based event number, as shown in the timeline "#" column) and optional `reason`. Creates a new fork execution. Redirects with flash.
- **Trigger update** — Expandable form. POST to `/workflows/{exec_id}/trigger-update` with `update_name` and optional JSON `payload`. Redirects with flash.
- **Export history** — GET link to `…/workflows/{exec_id}/history/export` for the full JSON event log.

**Event timeline collapsible payload** — each event row in the history table has a `<details>` element. Click "view payload" to expand and see the full JSON event data.

**Jump to event N** — when the history has more than 100 events, a number input labeled "Jump to event:" appears alongside the pagination controls. Enter a 1-based event number and click Go to navigate to the page containing that event.

Status badges include `aria-label="Status: {state}"` and `role="status"` for screen-reader accessibility.

### Schedules — `/schedules`

The operator surface for cron/interval schedules: one row per schedule across every
shard, with the health, policy and bounded-run state needed to answer *"is this
schedule healthy, when does it fire next, and how did its last runs end?"* without
composing API calls by hand.

Every field is read from the existing `GET /admin/schedules` response fields, and every
mutation goes through an already-audited endpoint. The page adds **no new backend
endpoint, no new `WorkflowEvent` variant, and no migration**.

**List columns**

| Column | Source | Notes |
|---|---|---|
| Schedule ID | `harvest_schedules.id` | Truncated; links to the drill-downs |
| Kind / Target | `dag_name` / `workflow_name` | `Dag` or `Workflow` |
| Expression | `schedule_expr` | Cron or `interval:*` |
| Timezone | `timezone` | UTC is subdued; other zones get a badge |
| Next Run | `next_run_at` | Plus the jitter-adjusted **effective** fire time and the jitter window when `jitter_secs > 0` (#240) |
| Last Run | `last_run_at` | |
| Health | derived | See badges below |
| Overlap | `overlap_policy` (#241) | Buffering policies also show `buffered <n>` (and `/<cap>` under `buffer_all`) |
| Catchup | `catchup_policy` (#484) | Effective policy, the `window` duration, and `dropped <n>` from the most recent recovery tick |
| Bounded runs | `max_runs` / `runs_started` / `end_at` / `exhausted_reason` (#478) | `<remaining> of <max> left · ends <ts> · exhausted: <reason>` |
| Created | `created_at` | |
| Shard | fan-out | Multi-shard deployments only |

**Health badges and ordering**

A healthy schedule renders one calm `Active` badge. An unhealthy one renders a badge per
condition:

| Badge | Condition |
|---|---|
| `Paused` | `is_paused` |
| `Auto-paused` | `auto_paused_at` set (#360) — supersedes `Paused` |
| `Exhausted: <reason>` | `exhausted_at` set (#478) |
| `Catchup dropped ×N` | `last_catchup_dropped > 0` (#484) |

Unhealthy schedules **sort above** healthy ones; healthy rows keep their existing
`next_run_at`-ascending order among themselves. A "Needs attention" strip at the top of
the page counts each bucket, and the **Health** filter (`?health=Unhealthy` /
`?health=Healthy`) narrows the list. An unrecognised value is a `400`.

**Filters** — `target`, `kind`, `paused`, `health`, `shard_id`, `limit`, `refresh`.
All of them round-trip into the bulk pause/resume actions, and both bulk handlers apply
the *same* `ScheduleUiFilters::matches` predicate the list does — so "Pause all matching
(N)" acts on exactly the N rows on screen.

**Per-row actions** — Pause, Resume, Run now, and Delete, each behind a confirmation
dialog and each writing an audit record with `source = ui` and the matching operation
(`schedule.pause`, `schedule.resume`, `schedule.trigger`, `schedule.delete`). The UI
adds no unaudited mutation path.

#### Fire-time preview — `/schedules/{id}/preview`

Renders the next N fire times from the same computation as
`GET /admin/schedules/{id}/preview` (#348), so the two can never disagree:

- **Scheduled** (the raw cron instant), **Local** (in the schedule's timezone), and
  **Effective** (after calendar adjustment and jitter) side by side.
- Calendar-excluded entries show `suppressed` with reason `skipped:calendar-excluded`;
  rebased entries show `deferred:calendar`.
- The jitter window (`[scheduled_at, scheduled_at + jitter_secs]`) per entry.
- An advisory **Overlap risk** flag when `skip`/`buffer_one` could drop the firing.
- Bounded-run truncation (#478/#543) is applied, and a preview that comes back empty
  says *why*: paused (with the pause reason), exhausted (with `exhausted_reason`), the
  `end_at` cutoff, or an expression with no future firings.

`?count=N` (1–100, default 10) controls how many entries are projected.

#### Run history — `/schedules/{id}/runs`

Consumes `GET /admin/schedules/{id}/runs` (#534/#762). Newest-slot-first rows with
`nominal_fire_time` (`— (no slot)` for a manual trigger), started/completed timestamps,
a terminal state badge, `origin` (`scheduled` / `backfill` / `manual_trigger`), the
first line of a terminal failure, and a link to the execution detail view.

Above the table, the **scheduled-run summary** counts `scheduled`-origin runs only, so a
backfill storm or an ad-hoc trigger never inflates the failure ratio.

A filter bar exposes `limit` (1–200), `origin`, and `state`. The **Next** link carries the
active filters alongside the cursor, because a keyset cursor is only meaningful under the
filters it was computed with.

Cross-shard degradation is always visible, never silent:

| Response `status` | Rendering |
|---|---|
| `complete` | No banner |
| `partial` | "Some shards unreachable" banner listing each unavailable shard and its error; the summary is flagged as possibly understated |
| `unavailable` | "No shard could be reached" banner; **no** "no runs yet" message, because nothing could be read |

A schedule that exists but has never run renders an explicit "No runs yet." card.
Paging uses the endpoint's own keyset cursor (`?limit=`, `?cursor=`).

This page is **admin-gated**, matching `GET /admin/schedules/{id}/runs` — the one
schedule read route the management API gates.

#### Backfill launcher — `/schedules/{id}/backfill`

A two-stage flow over `POST /admin/schedules/{id}/backfill` (#337):

1. `GET` renders the window form (start, end, max slots, include-paused). This is a
   pure read — it dispatches nothing.
2. Submitting posts `stage=preview`, which runs the endpoint's **dry run** and renders
   the planned slot count, the would-dispatch and would-skip counts, the machine-readable
   skip reasons, and the first 20 planned fire times.
3. Only an explicit `stage=commit` (behind a `confirm()` dialog) dispatches. A POST that
   omits `stage` falls back to the dry run, so a bare form post can never dispatch work.
4. On success the operator is redirected to that schedule's **run history**, which renders
   a flash summarising what was dispatched — including the failure count, which is
   reported nowhere else.

`max_count` is capped at the engine's `DEFAULT_BACKFILL_MAX_COUNT` (1,000). The endpoint
treats a supplied `max_count` as the planning limit that *replaces* its own default, so an
uncapped value from a browser form could make one request enumerate millions of timestamps.

A paused **DAG** schedule cannot be committed at all (the endpoint rejects it, because
backfilled runs would sit `QUEUED` until the schedule resumes), so the confirmation says so
instead of offering a button that can only fail. A rejected submission re-renders the form
with the operator's own window still in it.

The dry run is a POST rather than a GET because it writes a `harvest_backfill_log` row
and an audit record — the read path stays side-effect-free. Both stages are audited
through the API handler with `source = ui`, and every guard the endpoint enforces
(paused, exhausted, `max_active_runs`, `max_runs`, window size) applies unchanged; a
rejection is rendered on the form rather than as a bare error page.

A malformed or inverted window is a rendered form error, never a 500.

#### Not-found vs. indeterminate

All three drill-downs resolve the schedule through the API's own
`resolve_schedule_with_shard`, so an unparseable id is a `400`, "checked every expected
shard and none had it" is a `404`, and "a shard could not be checked" is a `503` — never a
`404` claiming a schedule was deleted when a shard was merely unreachable.

**Out of scope (tracked elsewhere):** creating or editing schedules from the UI (#771),
overdue-schedule detection (#696), recent-runs enrichment (#762).

## Event label mapping

Vantage translates raw `WorkflowEvent` type strings to friendlier labels:

| Event type | Label |
|------------|-------|
| `WorkflowStarted` | Workflow started |
| `WorkflowCompleted` | Workflow completed |
| `WorkflowFailed` | Workflow failed |
| `WorkflowCancelled` | Workflow cancelled |
| `ActivityScheduled` | Activity scheduled: `{name}` |
| `ActivityCompleted` | Activity completed |
| `ActivityFailed` | Activity failed: `{error}` (truncated) |
| `SignalReceived` | Signal received: `{signal_name}` |
| `UpdateAdmitted` | Update admitted |
| `TimerStarted` | Timer started |
| `TimerFired` | Timer fired |
| `LocalActivityScheduled` | Local activity scheduled |
| `VersionMarker` | Version marker |
| `ContinueAsNew` | Continue as new |
| (other) | Raw type string |

Event fields are extracted from the inner `data` object of the adjacently-tagged serde envelope `{"type": "...", "data": {...}}`.

## Navigation

| Route | Description |
|-------|-------------|
| `/` | Redirects to `/workflows` |
| `/workflows` | Workflow list |
| `/workflows/{exec_id}` | Workflow detail |
| `/workers` | Worker fleet |
| `/schedules` | Cron/interval schedules |
| `/schedules/{id}/preview` | Next-fire-time preview for one schedule |
| `/schedules/{id}/runs` | Per-schedule run history (admin) |
| `/schedules/{id}/backfill` | Backfill launcher (form → dry run → commit) |
| `/dags` | DAG list |
| `/dags/{dag_name}` | DAG detail |
| `/dlq` | Dead letter queue |

## Live Event Streaming (feature toggle)

The workflow detail page can show events in real-time as they land in `harvest_events`, using the `GET /executions/{exec_id}/events/stream` SSE endpoint.

### Enabling the toggle

Set the `AUTUMN_HARVEST_UI_LIVE_STREAM=true` environment variable (or the equivalent `harvest.ui.live_stream = true` config key) before starting your application. When the toggle is on and the browser supports `EventSource`, the detail page replaces the manual **Refresh** button with a **Live** / **Paused** indicator:

- **Live** (green dot): the `EventSource` connection is open and events are streaming in real time.
- **Paused** (grey dot): the user clicked the indicator to freeze the view, or `EventSource` is not supported; the existing polled-refresh path is used instead.

When the toggle is **off** (the default), the v1 polled-refresh behaviour is used unchanged — the detail page auto-refreshes at the configured interval and shows the **Refresh** button. No JavaScript is executed in this mode.

### Fallback rules

1. Toggle off → polled refresh (default, no change from v1).
2. Toggle on, browser lacks `EventSource` → polled refresh.
3. Toggle on, browser supports `EventSource`, endpoint returns 4xx/5xx → polled refresh with a "stream unavailable" notice.
4. Execution reaches a terminal state → `event: stream-end` closes the `EventSource`; the page transitions to a static "completed" view without reconnecting.

### Browser / proxy notes

See `docs/management-api.md` for a full list of reverse-proxy and CDN considerations (nginx buffering, Cloudflare, ALB idle timeouts). In short:

- nginx: add `proxy_set_header X-Accel-Buffering no;` on the upstream location block.
- Cloudflare: SSE is proxied without special configuration since 2024; no extra headers needed.
- AWS ALB: set idle timeout ≥ 65 s (the SSE keepalive default is 15 s, well inside this window).

## Accessibility

- Status badges on the detail page include `aria-label` and `role="status"`.
- All filter and action forms use standard `<form>` / `<button>` elements navigable by keyboard.
- The **Live / Paused** SSE indicator is a `<button>` with `aria-label` and `aria-pressed` attributes.
- When SSE is disabled the dashboard has no JavaScript; all interactions are plain HTTP form submissions or link navigations.

## Security

- All user-controlled values are HTML-escaped by Maud before rendering.
- The dashboard never emits `<script>` tags.
- The `require_harvest_admin` middleware (configured via `HarvestBuilder`) can gate Vantage behind token authentication in production.

---

## Operator runbook: Diagnosing a stuck workflow

Use the workflow detail page at `/workflows/{exec_id}` to investigate a workflow that appears stuck or unresponsive. The following five scenarios cover the most common root causes.

### Scenario 1 — Stuck activity retry (activity never completing)

**Symptoms**: The workflow has been RUNNING for an unexpectedly long time. The **Activity attempts** panel shows high attempt counts or repeated `ActivityFailed` events in the event timeline.

**Steps**:
1. Open the detail page and check the **Activity attempts** panel. Look for activities with `last_status = ActivityFailed` and a non-zero attempt count.
2. Check the **Blocked on** panel — if there is a row in the pending activities table with `state = BACKOFF`, the worker is waiting for the retry delay to elapse.
3. Expand the `ActivityFailed` event payload in the **Event history** timeline using the "view payload" details toggle to read the error message.
4. If the error is transient (network timeout, downstream unavailable), wait for the next retry attempt. If the error is permanent, use **Cancel** to halt the workflow and re-investigate.
5. Check `harvest_task_queue` directly with `GET /api/harvest/admin/queue?workflow_exec_id={exec_id}` to confirm the task state.

### Scenario 2 — Workflow waiting on a signal that never arrived

**Symptoms**: The workflow is RUNNING but appears idle. The **Blocked on** panel shows pending signals, or the **Signals & Updates** panel shows an expected signal name that has not been received.

**Steps**:
1. Open the detail page and check the **Blocked on** panel. A row in the pending signals table indicates the workflow registered a signal handler that has not been triggered yet.
2. Check the **Signals & Updates** panel for any signals already received — the workflow may have received the wrong signal name.
3. If the upstream system is confirmed to have sent the signal but it did not arrive, use **Send signal** in the operator actions to manually inject the signal with the expected `signal_name` and a JSON payload.
4. Verify via the event timeline that a `SignalReceived` event was appended after sending.

### Scenario 3 — Workflow blocked waiting on a child workflow

**Symptoms**: The workflow is RUNNING but not making progress. The **Children** panel shows one or more child executions in `RUNNING` or `FAILED` state.

**Steps**:
1. Open the detail page and scroll to the **Children** panel. Click the child execution ID link to open the child detail page.
2. Diagnose the child using the same runbook scenarios (the child may have its own stuck activity, signal, or replay error).
3. If the child is permanently stuck and must be abandoned, cancel it via **Cancel** on its own detail page. The parent workflow will then receive a `ChildWorkflowFailed` event and may retry or propagate the failure depending on its retry policy.
4. If the parent needs to be rolled back past the child launch point, use **Reset to event N** on the parent, specifying the event ID before the `ChildWorkflowStarted` event.

### Scenario 4 — Replay non-determinism (workflow failing on resume)

**Symptoms**: The workflow repeatedly transitions to FAILED with an error message containing "non-determinism" or "replay mismatch". It may also produce `WorkflowFailed` events immediately after being retried.

**Steps**:
1. Open the detail page and expand the most recent `WorkflowFailed` event payload in the **Event history** timeline. The `error` field will describe which expected event did not match.
2. Use the `WorkflowReplayer` testing harness (see `testing.rs`) to reproduce the replay error locally against an exported history (`/workflows/{exec_id}/history/export`).
3. Identify the code change that introduced the non-determinism — common causes are: reordering activity calls, adding unconditional branches, changing timer durations, or removing version gates.
4. Fix the workflow code (add a `ctx.version()` gate to branch on the new vs. old code path) and deploy.
5. Use **Reset to event N** to reset the execution to a safe boundary before the non-deterministic branch, then allow the workflow to re-execute with the corrected code.

### Scenario 5 — Timed-out activity (activity exceeded its start-to-close timeout)

**Symptoms**: The workflow has an `ActivityTimedOut` event in the event timeline. The workflow may be retrying or may be stuck in `FAILED`.

**Steps**:
1. Open the detail page and find the `ActivityTimedOut` event in the **Event history** timeline. Use "view payload" to read the `activity_id` and timeout type.
2. Check the **Activity attempts** panel — `last_status = ActivityTimedOut` confirms the activity was not heartbeating within the configured `start_to_close` or `heartbeat_timeout` window.
3. Check the worker fleet at `/workers` — if workers are shown as `Degraded` or `Unhealthy`, the activity tasks may not be getting picked up.
4. If the timeout was caused by the activity taking too long, consider increasing the `start_to_close` timeout in the `#[activity]` annotation and deploying the update.
5. If the worker is healthy and the activity should complete quickly, the task may have been lost during a worker crash. Cancel the workflow and restart it, or use **Reset to event N** to roll back past the activity scheduling event.
