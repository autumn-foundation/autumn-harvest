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

**Operator actions**

- **Cancel** — POST to `…/workflows/{exec_id}/cancel` with a confirmation dialog.
- **Export history** — GET link to `…/workflows/{exec_id}/history/export` for the full JSON event log.

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
