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
- **Reset to event N** — Expandable form. POST to `/workflows/{exec_id}/reset` with `reset_to_event_id` (0-based event ID) and optional `reason`. Creates a new fork execution. Redirects with flash.
- **Trigger update** — Expandable form. POST to `/workflows/{exec_id}/trigger-update` with `update_name` and optional JSON `payload`. Redirects with flash.
- **Export history** — GET link to `…/workflows/{exec_id}/history/export` for the full JSON event log.

**Event timeline collapsible payload** — each event row in the history table has a `<details>` element. Click "view payload" to expand and see the full JSON event data.

**Jump to event N** — when the history has more than 100 events, a number input labeled "Jump to event:" appears alongside the pagination controls. Enter a 1-based event number and click Go to navigate to the page containing that event.

Status badges include `aria-label="Status: {state}"` and `role="status"` for screen-reader accessibility.

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
| `/dlq` | Dead letter queue |

## Accessibility

- Status badges on the detail page include `aria-label` and `role="status"`.
- All filter and action forms use standard `<form>` / `<button>` elements navigable by keyboard.
- The dashboard has no JavaScript; all interactions are plain HTTP form submissions or link navigations.

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
