# Runbook: Safe Handler Removal

Use this check before a deploy that **deletes or renames a `#[workflow]`
function**. It answers one question exactly: *does any in-flight execution still
need this handler, such that removing it would wedge those runs?*

When an operator removes a `#[workflow]` function and redeploys, any
**non-terminal** execution of that type is silently stranded: on its next replay
the worker hits `HandlerNotFound` and the run can never make progress. It
surfaces only days later as a timeout or DLQ entry. This check turns that slow,
invisible failure into a one-call pre-flight gate.

**When to use:** before any deploy that removes/renames a workflow type, and as
a CI gate on the rollout. For where this sits in the full deploy sequence — and
the optional boot-time `[harvest.startup] orphaned_workflows = warn|fail|off`
gate — see the [Pre-cutover handler-coverage gate](safe-deploy.md#runbook-pre-cutover-handler-coverage-gate) in the safe-deploy runbook.

**When _not_ to use:** retiring a *worker build* (use `build_reachability`,
below) or removing a *`ctx.version()` branch inside a handler* (use the
version-gate retirement check, below). Those are different questions.

---

## Three reachability questions — don't confuse them

Harvest exposes three orthogonal "is it safe to remove X?" checks. Pick the one
that matches what you are deleting:

| You are removing… | Use | Endpoint |
|---|---|---|
| **A whole `#[workflow]` handler** (delete/rename the function) | **This check** (type-level reachability, #520) | `GET /admin/workflow-types/reachability` |
| **A worker build / deployment version** (build-id routing) | `build_reachability` (#171) | `GET /admin/build-routing` |
| **A `ctx.version()` branch inside a handler** | Version-gate retirement (#?) | `GET /admin/version-gates/retirement-check` |

- **This check** counts *non-terminal executions whose `workflow_name` names the
  handler*. It is the answer for the **default, non-build-routed** deployment —
  the common case that build-id routing does not cover, because build routing is
  opt-in.
- **`build_reachability`** answers "can I retire worker build B?" and only
  applies when build-id routing is enabled.
- **Version-gate retirement** answers "can I delete this `if ctx.version(...)`
  branch?" — a question *inside* a handler that still exists.

This check covers **workflow types only**. Activity-type reachability is out of
scope: a running workflow may call an activity it has not yet reached, so
activity-handler safety needs static call-graph analysis.

---

## Step 1 — Ask the question

```bash
harvest workflow-types reachability
```

Example table output:

```
status: complete
observed_at: 2026-05-31T06:00:00Z

WORKFLOW_TYPE   REGISTERED  NON_TERMINAL  OLDEST_AGE_S  VERDICT
onboarding      true        0                           safe_to_remove
subscription    true        4             912           in_use
legacy_export   false       1             88400         orphaned
```

Narrow to a single type you intend to delete:

```bash
harvest workflow-types reachability --type legacy_export
```

Machine-readable output for scripting:

```bash
harvest workflow-types reachability --json
curl -s "$HARVEST_URL/admin/workflow-types/reachability" -H "Authorization: Bearer $TOKEN"
```

---

## Step 2 — Read the verdict

For each workflow type the response returns `workflow_type`, `registered`,
`non_terminal_count`, `oldest_non_terminal_age_secs`, and a `verdict`:

- **`safe_to_remove`** — zero non-terminal (`RUNNING`/`SUSPENDED`/`PAUSED`)
  executions. The handler can be deleted, **subject to the cross-type
  continue-as-new caveat below**.
- **`in_use`** — ≥1 non-terminal execution **and** the handler is still
  registered in this deployment. Do **not** remove it yet; drain or wait first.
- **`orphaned`** — ≥1 non-terminal execution **and** the handler is **not**
  registered. These runs are **already wedged** (the handler was removed in a
  prior deploy). This surfaces the failure *before* it manifests as DLQ/timeout.
  Restore the handler, or terminate/cancel the stranded executions.

A type appears in the report if it is **either** currently registered **or** has
at least one non-terminal execution on any shard.

### Caveat — `safe_to_remove` cannot see cross-type continue-as-new targets

The verdict counts non-terminal executions **of that type**. Since issue #803, a
live run of a *different* type can enter this one via
`ctx.continue_as_new_as(&this_info(), …)` / `continue_as_new_as_type("this", …)`,
and that edge is a static call-graph fact the report cannot observe — the same
limitation already documented for activity-type reachability.

> **Partial static answer (issue #802).** A workflow can opt in to declaring the
> activities it dispatches and the workflow types it references — children, and
> cross-type `continue_as_new_as` targets, since `children` is checked by name
> against the workflow catalog — via `#[workflow(activities = [send_email],
> children = [generate_report])]`, and deploy-time preflight then
> fails when a declared name is not registered. That makes deleting a *declared*
> handler a preflight failure rather than a runtime dead letter. It is
> **opt-in and declaration-based, not a call-graph analysis**: it says nothing about
> an activity no workflow declared, and it can go stale in the other direction (a
> declaration left behind after the dispatch was removed fails the deploy until the
> declaration is deleted too). See
> [Chapter 10 — Operating the service](../getting-started/10-operations.md#catching-a-forgotten-registration-before-rollout).

The canonical shape is a win-back loop (`churned → trial → churned`): a fleet
with many live `churned` runs and momentarily zero `trial` runs reports
`trial_subscription: safe_to_remove`. Deleting that handler arms every future
reactivation to **fail terminally** on its transition.

Before deleting a handler on a `safe_to_remove` verdict, also confirm no live
type targets it:

```bash
# Any workflow that continues INTO the type you are about to delete?
rg -n 'continue_as_new_as(_type)?\b' --glob '*.rs' | rg 'legacy_export'
```

This is the *delete* direction of the same rule the #803 rollout ordering states
for the *deploy* direction ("deploy the new phase's handler fleet-wide first").
See the "Cross-type continue-as-new" section in [`docs/architecture.md`](../architecture.md#cross-type-continue-as-new--multi-phase-entities-issue-803).

---

## Step 3 — Gate the deploy in CI

Wire the filtered check into the rollout pipeline:

```bash
# Fails the pipeline (exit 2) if the type still has live runs or if a shard
# could not be inspected (fail closed).
harvest workflow-types reachability --type legacy_export || exit 1
```

Exit codes differ by mode:

**Filtered (`--type <name>`) — pre-removal safe-removal check:**

| Exit | Meaning |
|------|---------|
| `0` | `safe_to_remove` and every shard inspected — handler can be deleted |
| `2` | `in_use` (live runs exist, drain/wait first), `orphaned`, partial/unavailable report, **or** transport/auth error (fail-closed: uncertain = unsafe) |
| `1` | CLI usage error (invalid flags) |

**Unfiltered (no `--type`) — continuous fleet monitor:**

| Exit | Meaning |
|------|---------|
| `0` | No `orphaned` verdicts and every shard inspected |
| `2` | Any `orphaned` verdict, partial/unavailable report, **or** transport/auth error (fail-closed) |
| `1` | CLI usage error (invalid flags) |

In unfiltered mode `in_use` is normal (registered handlers with running workflows) and does **not** trigger exit 2.

> **Note:** transport and auth errors (bad URL, missing token, server 5xx) deliberately exit `2` rather than `1` so a misconfigured CI environment cannot produce a false "safe" signal. If CI exits `2` unexpectedly, check the Harvest API URL and credentials before concluding that a handler is unsafe to remove.

---

## Multi-shard deployments

Counts aggregate across every shard via `iter_shards()`. The response includes a
per-type `shard_breakdown` and a top-level `shards` array reporting each shard's
inspection status.

If a shard is **unreachable**, it is reported (never silently dropped) and the
report `status` becomes `partial` (some shards inspected) or `unavailable` (none
inspected). **A `safe_to_remove` verdict is authoritative only when
`status == complete`** — an unreachable shard could host non-terminal
executions, so a partial answer must never be mistaken for a safe one. The CLI
fails closed (exit `2`) on `partial`/`unavailable` for exactly this reason.

---

## Properties

- **Read-only and side-effect-free**: no task claims, no state mutation, no
  `WorkflowEvent` appended, no migration.
- **Same auth as all `/admin/*` routes** (admin boundary).
- **Indexed scan** via `idx_harvest_we_non_terminal_wf_name` (a partial index
  over non-terminal rows only, added in migration
  `20260624000001_harvest_non_terminal_reachability_index`): a
  `GROUP BY workflow_name` over non-terminal `harvest_workflow_executions` per
  shard, fanned out in parallel.
