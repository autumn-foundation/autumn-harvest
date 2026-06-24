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
a CI gate on the rollout.

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
| **A worker build / deployment version** (build-id routing) | `build_reachability` (#171) | `GET /admin/build-reachability` |
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
  executions. The handler can be deleted.
- **`in_use`** — ≥1 non-terminal execution **and** the handler is still
  registered in this deployment. Do **not** remove it yet; drain or wait first.
- **`orphaned`** — ≥1 non-terminal execution **and** the handler is **not**
  registered. These runs are **already wedged** (the handler was removed in a
  prior deploy). This surfaces the failure *before* it manifests as DLQ/timeout.
  Restore the handler, or terminate/cancel the stranded executions.

A type appears in the report if it is **either** currently registered **or** has
at least one non-terminal execution on any shard.

---

## Step 3 — Gate the deploy in CI

The CLI exits **`2`** when any type is `orphaned`, or when the report is
incomplete (a shard was unreachable). Wire it into the rollout:

```bash
# Fails the pipeline (exit 2) if removing a handler would strand runs,
# or if a shard could not be inspected (fail closed).
harvest workflow-types reachability --type legacy_export || exit 1
```

Exit codes: `0` = safe (all `safe_to_remove`/`in_use`, every shard inspected),
`2` = an `orphaned` verdict **or** a partial/unavailable cross-shard answer,
`1` = a transport/usage error.

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
