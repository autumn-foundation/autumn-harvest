# Version-Gate Retirement Playbook

This runbook explains how to safely evolve `#[workflow]` code that uses
`ctx.version(...)` gates, and how to prove that an old branch can be deleted
without breaking any in-flight execution.

---

## Background

`ctx.version(change_id, min, max)` records a `MarkerRecorded` event the first
time a live execution crosses it and replays the recorded version on every
subsequent replay cycle.  The engine uses the recorded version to decide which
code branch to follow, so removing a branch while executions with the old
recorded version still exist causes a non-determinism error and moves those
executions to the dead-letter queue.

---

## Lifecycle

### 1 — Add a version gate

Wrap the new behaviour in a version branch:

```rust
#[workflow]
async fn billing_checkout(ctx: &WorkflowContext, ...) -> Result<(), String> {
    let v = ctx.version("billing_v2_tax", 1, 2);
    if v >= 2 {
        // New tax calculation logic
        ctx.execute_activity(compute_new_tax, ...).await?;
    } else {
        // Old logic kept alive for in-flight v1 executions
        ctx.execute_activity(compute_old_tax, ...).await?;
    }
    // ...
}
```

Rules:
- Always increment `max` when adding new behaviour.
- Keep `min` at the lowest version that can still replay correctly.
- Never decrease `min` or `max` of an existing gate — that changes replay
  semantics for stored events.

### 2 — Deploy new code

Deploy the updated binary.  New executions receive `max` (the new version).
In-flight executions from before the deploy continue to replay with their
recorded version.

### 3 — Run replay fixtures

After deploying, verify that existing recorded histories are still
deterministic:

```bash
cargo test -p my-app --test replay_tests
```

or with `WorkflowReplayer` in a staging environment pointed at real histories:

```bash
harvest workflow get <exec_id> | jq .events > fixtures/billing_v1.json
cargo test --features testing -- replayer
```

### 4 — Wait for old executions to drain

Old-version executions must complete (or be cancelled/terminated) before the
old branch can be removed.  Monitor progress with the version-gate usage report:

```bash
# Show all observed versions for this change id
harvest version-usage --change-id billing_v2_tax

# Monitor only active (non-terminal) executions
harvest version-usage --change-id billing_v2_tax --state-group active
```

### 5 — Run the retirement check

When you believe all old executions have drained, run the retirement check to
confirm it is safe to delete the old branch.  The check inspects every shard
and returns `safe_to_retire: true` only when **zero active** (non-terminal)
executions carry a version below the threshold across **all inspected shards**.

```bash
# Table output (default) — useful for manual review
harvest version-gate-retirement \
    --change-id billing_v2_tax \
    --min-safe-version 2

# JSON output — for scripts or CI pipelines
harvest --output json version-gate-retirement \
    --change-id billing_v2_tax \
    --min-safe-version 2

# CI gate — exits non-zero while any blocker or unavailable shard remains
harvest version-gate-retirement \
    --change-id billing_v2_tax \
    --min-safe-version 2 \
    --check
```

The check also accepts `--workflow-name` and `--shard-id` to narrow scope, and
`--state-group active` to ignore terminal history rows if your release policy
only cares about live blockers.

#### Interpreting the report

| `status`      | `safe_to_retire` | Meaning |
|---------------|-----------------|---------|
| `safe`        | `true`          | All shards inspected; no active old-version executions. Safe to remove the old branch. |
| `blocked`     | `false`         | At least one active execution still carries a version below the threshold. Wait and retry. |
| `partial`     | `false`         | Some shards could not be inspected. Investigate the named unavailable shards before proceeding. |
| `unavailable` | `false`         | No shard could be reached. Check shard connectivity before proceeding. |

The `blockers` array in the response includes:
- `workflow_name`, `recorded_version`, `active_executions`, `terminal_executions`
- `oldest_blocker_started_at` / `newest_blocker_started_at` and their age in seconds
- `sample_active_execution_ids` — up to 10 execution UUIDs for investigation

A workflow with **no recorded version gates** returns `status: "safe"` and an
empty `blockers` array — not a 404 or error.

#### Example: blocked report

```json
{
  "status": "blocked",
  "safe_to_retire": false,
  "observed_at": "2026-05-07T14:30:00Z",
  "filters": {
    "change_id": "billing_v2_tax",
    "min_safe_version": 2,
    "state_group": "all"
  },
  "blockers": [{
    "workflow_name": "billing_checkout",
    "change_id": "billing_v2_tax",
    "recorded_version": 1,
    "active_executions": 3,
    "terminal_executions": 142,
    "oldest_blocker_age_secs": 14400,
    "newest_blocker_age_secs": 60,
    "sample_active_execution_ids": [
      "018f1a2b-3c4d-7e8f-9a0b-1c2d3e4f5a6b"
    ],
    "shard_coverage": {
      "inspected_shards": [0, 1],
      "matched_shards": [0],
      "unavailable_shards": []
    }
  }],
  "shards": [
    { "shard_id": 0, "status": "inspected", "matched_groups": 1 },
    { "shard_id": 1, "status": "inspected", "matched_groups": 0 }
  ]
}
```

Investigate the sample execution IDs:

```bash
harvest workflow get 018f1a2b-3c4d-7e8f-9a0b-1c2d3e4f5a6b
```

If executions are genuinely stuck (no worker coverage, heartbeat timed out),
consider cancelling or terminating them:

```bash
harvest workflow cancel 018f1a2b-3c4d-7e8f-9a0b-1c2d3e4f5a6b \
    --reason "version-gate drain before retirement of billing_v2_tax<2"
```

### 6 — Remove the old branch and run replay again

Once `status: "safe"`, remove the old branch:

```rust
#[workflow]
async fn billing_checkout(ctx: &WorkflowContext, ...) -> Result<(), String> {
    // Version gate still present but old branch removed.
    // min is updated to 2 so the gate always returns 2 for new executions.
    ctx.version("billing_v2_tax", 2, 2);
    ctx.execute_activity(compute_new_tax, ...).await?;
    // ...
}
```

Re-run replay fixtures to confirm no determinism regressions, then remove the
gate entirely in a subsequent deploy once all histories have rolled past it.

---

## API reference

`GET /admin/version-gates/retirement-check`

| Parameter         | Type     | Required | Description |
|-------------------|----------|----------|-------------|
| `change_id`       | `string` | Yes      | Version-gate change id to inspect. |
| `min_safe_version`| `u32`    | Yes      | Versions strictly below this are considered old. |
| `workflow_name`   | `string` | No       | Narrow to one workflow name. |
| `state_group`     | `string` | No       | `all` (default), `active`, or `terminal`. |
| `shard_id`        | `i32`    | No       | Restrict to one shard. |

---

## Multi-shard deployments

When a shard is unreachable, the report returns `status: "partial"` and names
the unavailable shard in `shards[].status = "unavailable"`.  The CLI `--check`
flag treats `partial` as a failure (exit code 1) because safety cannot be
proven without inspecting all shards.

Restore shard connectivity before retiring the branch.  You can narrow scope
to a specific healthy shard with `--shard-id` while investigating, but the
final retirement gate should cover all shards.

---

## CI integration example

```yaml
# .github/workflows/deploy.yml
- name: Wait for version-gate to drain
  run: |
    for i in $(seq 1 30); do
      harvest --output json version-gate-retirement \
        --change-id billing_v2_tax \
        --min-safe-version 2 \
        --check && exit 0
      echo "Still blocked — waiting 60s"
      sleep 60
    done
    echo "Timed out waiting for version-gate drain"
    exit 1
  env:
    HARVEST_URL: ${{ vars.HARVEST_URL }}
    HARVEST_TOKEN: ${{ secrets.HARVEST_TOKEN }}
```
