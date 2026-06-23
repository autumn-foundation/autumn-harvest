# Runbook: Safe Worker Drain Before a Deploy

Use the Harvest drain controls to quiesce one or more workers gracefully before
deploying a new version or taking a host out of rotation. This avoids dropped
in-flight tasks and mid-execution interruptions without needing SSH or raw
process signals.

**When to use:** rolling deploys, host maintenance, canary rollbacks, scheduled
scale-in events.

**When _not_ to use:** emergency kills — use SIGKILL directly and let Harvest's
retry/timeout logic recover the in-flight tasks.

---

## Step 1 — Identify the target worker(s)

List all active workers to find the candidates:

```bash
harvest worker list --status Active
```

Filter by queue or shard when operating a large fleet:

```bash
harvest worker list --queue email-workers --shard-id 0
```

Key fields in the response:
- `worker_id` — the string passed to `--drain` below.
- `in_flight_count` — tasks currently executing on this worker.
- `status` — `Active`, `Draining`, or `Stopped`.
- `health` — `healthy` (heartbeat recent) or `stale` (heartbeat expired).

---

## Step 2 — Dry-run with drain-preview

Before draining, confirm which workers would be affected:

```bash
harvest worker drain-preview --queue email-workers
```

`drain-preview` is read-only and never changes any state. The response lists
every matching active worker with its current `in_flight_count`.

---

## Step 3 — Request the drain

Drain a specific worker:

```bash
harvest worker drain <worker-id>
```

The server sets the worker's status to `Draining`. The worker will finish its
current tasks and then transition to `Stopped` within one heartbeat interval
(default: 5 s) after quiescing.

To specify an explicit deadline (RFC 3339):

```bash
harvest worker drain <worker-id> --deadline 2026-05-09T14:30:00Z
```

When `--deadline` is omitted the server uses the configured
`WorkerConfig::shutdown_timeout` (default 30 s from the current time).

### Drain outcome codes

| `outcome`         | Meaning                                                     |
|-------------------|-------------------------------------------------------------|
| `accepted`        | Drain requested; worker will quiesce and stop.              |
| `already_draining`| Worker is already draining; deadline was refreshed.         |
| `already_stopped` | Worker has already stopped; no action taken.                |
| `stale_worker`    | Worker heartbeat is stale; drain was written but the process may already be gone. |
| `not_found`       | Worker ID not found on any shard.                           |

---

## Step 4 — Wait for the worker to stop

### Option A — CLI wait mode (recommended)

The `--wait` flag blocks until the worker reaches `Stopped` or the timeout
expires, polling every 2 s:

```bash
harvest worker drain <worker-id> --wait --wait-timeout-secs 120
```

Exits 0 when the worker stops, exits 1 on timeout.

### Option B — Manual polling

```bash
watch -n 2 'harvest worker get <worker-id> | jq .status'
```

Wait until `"status": "Stopped"` appears.

### Option C — Management API

```http
GET /workers/<worker-id>
```

Poll until `status == "Stopped"`.

---

## Step 5 — Terminate the process

Once the worker is `Stopped` you can safely send SIGTERM (or SIGKILL) to the
process, redeploy the binary, or decommission the host. No in-flight tasks will
be lost.

---

## Degraded mode: unavailable shards

When a shard is temporarily unreachable the drain response includes
`unavailable_shards: [<id>, ...]` and an `outcome` of `not_found`. The worker
_may_ live on an unavailable shard. Retry the drain once the shard recovers, or
use `GET /admin/shards/health` to investigate.

---

## Drain audit trail

Every `POST /workers/{id}/drain` call is recorded in the audit log:

```bash
harvest audit list --operation worker.drain --target-id <worker-id>
```

Audit fields include `actor`, `occurred_at`, `status` (`succeeded` / `failed`),
and `request_id`.

---

## Common mistakes

| Mistake | Fix |
|---------|-----|
| Terminating the process before `Stopped` | Poll with `--wait` or `worker get` until status is `Stopped` |
| Forgetting `--deadline` on a slow worker | The default deadline is `shutdown_timeout` (30 s); set a longer deadline for workers with large in-flight batches |
| Draining the wrong shard | Use `--shard-id` with `drain-preview` to scope the preview first |
| Ignoring `unavailable_shards` in the response | The worker may be on an unreachable shard; retry after shard recovers |

---

# Runbook: Build-Id Routing for Safe Rolling Deploys

Use Harvest's build-id routing to gate which workers may resume specific
workflow executions. This prevents a worker running an incompatible binary from
replaying history in a way that causes non-determinism.

**When to use:** deployments that add or change workflow logic (new
`ctx.schedule_activity` calls, reordered steps, removed branches) where replay
safety cannot be proven by inspection alone.

**When _not_ to use:** purely additive changes that do not alter execution
order (new unrelated workflows, configuration changes, dependency bumps with no
behaviour change). Those deploys can use the plain drain runbook above.

---

## Concepts

| Term | Meaning |
|------|---------|
| `build_id` | Immutable string (Git SHA, semver tag, CI job ID) advertised by a worker at startup via `WorkerConfig::with_build_id("sha-abc123")`. Empty string = legacy worker that can claim any task. |
| Build policy | Per-queue row in `harvest_build_policies`. New workflow starts on the queue receive `assigned_build_id = policy.build_id`. Updated by operators when a new build ships. |
| Compat declaration | Row in `harvest_build_compat`. Means "workers running build B may process executions assigned to build A". Added after replay tests confirm safety. |
| `required_build_id` | Denormalized onto `harvest_task_queue`. Workers skip tasks whose `required_build_id` they are not eligible for. |
| Build reachability | Per-build snapshot: `open_executions`, `pending_tasks`, `active_workers`, `stale_workers`, `safe_to_retire`. Used to decide when old-build workers can be retired. |

---

## Scenario A: Backward-compatible deploy (new code can replay old history)

Use this when your replay test suite (e.g. `WorkflowReplayer`) confirms the new
build handles all in-flight histories safely.

**Step 1 — Deploy new workers with the new build id.**

In your `WorkerConfig`:

```rust
WorkerConfig::default()
    .with_build_id("sha-new123")
    .with_deployment_name("v2.3.0")   // optional human label
```

Start the new workers alongside the existing fleet. They register in
`harvest_workers` with `build_id = "sha-new123"`.

**Step 2 — Declare compat so new workers can resume old executions.**

```rust
// In your deploy tooling or a one-off migration script:
use autumn_harvest::build_routing::declare_compat;

declare_compat(&mut conn, "sha-new123", "sha-old456").await?;
// "workers running sha-new123 may process executions assigned to sha-old456"
```

> **Sharded deployments:** `harvest_build_compat` lives on every shard.
> Fan the write out to all shards:
> ```rust
> for (_, pool) in sharded_pool.iter_shards() {
>     let mut conn = pool.get().await?;
>     declare_compat(&mut conn, "sha-new123", "sha-old456").await?;
> }
> ```

**Step 3 — Advance the build policy so new starts land on the new build.**

```rust
use autumn_harvest::build_routing::set_build_policy;

set_build_policy(&mut conn, "default", "sha-new123", Some("v2.3.0")).await?;
```

> **Sharded deployments:** `harvest_build_policies` lives on every shard.
> Fan the write out to all shards:
> ```rust
> for (_, pool) in sharded_pool.iter_shards() {
>     let mut conn = pool.get().await?;
>     set_build_policy(&mut conn, "default", "sha-new123", Some("v2.3.0")).await?;
> }
> ```

New executions now get `assigned_build_id = "sha-new123"` and old-build
workers are ineligible to claim them.

> **First-adoption prerequisite:** this exclusion only applies to workers
> that advertise a non-empty `build_id`. Workers using the default
> `WorkerConfig` have `build_id = ""` (the legacy sentinel) and the claim
> filter allows them to pick up **any** task regardless of
> `required_build_id`. Before advancing the build policy, ensure the entire
> old fleet is already running with `with_build_id("sha-old456")` — or drain
> all legacy workers first. A mixed fleet with even one legacy worker
> invalidates the routing guarantee.

**Step 4 — Drain and retire old-build workers.**

Check reachability first:

```rust
use autumn_harvest::build_routing::build_reachability;
use std::time::Duration;

let r = build_reachability(&mut conn, "sha-old456", Duration::from_secs(60)).await?;
println!("safe_to_retire: {}", r.safe_to_retire);
```

Wait until `safe_to_retire == true` (no open executions, no pending tasks), then
drain and stop the old workers using the drain runbook above.

---

## Scenario B: Breaking deploy (new code cannot replay old history)

Use this when the new binary changes execution order in a way that would corrupt
in-flight workflows if replayed. Requires using `ctx.version()` gates to guard
the breaking branch.

**Step 1 — Gate the breaking code branch in the workflow with `ctx.version()`.**

```rust
#[workflow]
async fn my_workflow(ctx: &WorkflowContext, ...) -> Result<(), String> {
    if ctx.version("add-step-2", 1, 2).await? >= 2 {
        // new code path — runs for any execution that reaches this code for the first time
        ctx.schedule_activity(new_step, ...).await?;
    }
    // existing steps ...
}
```

`ctx.version()` emits a `VersionMarker` event on first call and replays the
recorded marker on re-entry, so the branch is stable across replays.

**Step 2 — Deploy new workers and set the policy (same as Scenario A steps 1 and 3).**

Do **not** declare compat. Old-build workers will stop receiving new tasks; new
tasks go only to new-build workers. In-flight old-build executions drain on
old-build workers.

**Step 3 — Wait for old-build executions to drain.**

Poll until `safe_to_retire` flips:

```rust
loop {
    let r = build_reachability(&mut conn, "sha-old456", Duration::from_secs(60)).await?;
    if r.safe_to_retire { break; }
    tokio::time::sleep(Duration::from_secs(10)).await;
}
```

Once `true`, retire the old workers.

---

## Scenario C: Emergency rollback

When a bad deploy must be reversed immediately:

**Step 1 — Advance the build policy back to the previous build.**

```rust
set_build_policy(&mut conn, "default", "sha-old456", None).await?;
```

New starts immediately revert to the previous build. In-flight new-build
executions continue on new-build workers.

**Step 2 — If the new build is unsafe to continue, drain new-build workers.**

Use the drain runbook to quiesce all workers advertising `build_id = "sha-new123"`.
New-build executions that were in-flight will be retried and, if the build policy
is back to `sha-old456`, picked up by old-build workers — but only if a compat
declaration exists (or the new executions have no `required_build_id`).

**Step 3 — Declare compat so old-build workers can resume in-flight new-build executions.**

New-build executions that were in-flight when the workers were drained have
`assigned_build_id = "sha-new123"`. For old-build workers to claim those tasks,
declare the reverse compat direction:

```rust
use autumn_harvest::build_routing::declare_compat;

declare_compat(&mut conn, "sha-old456", "sha-new123").await?;
// "workers running sha-old456 may process executions assigned to sha-new123"
```

Optionally revoke the original forward compat to prevent any surviving
new-build workers from claiming old tasks while they finish draining:

```rust
use autumn_harvest::build_routing::revoke_compat;

revoke_compat(&mut conn, "sha-new123", "sha-old456").await?;
// "workers running sha-new123 may NO LONGER process executions assigned to sha-old456"
```

> **Sharded deployments:** fan both calls out over `ShardedDbPool::iter_shards()`
> as shown in Scenario A Steps 2–3 above.

---

## Build reachability reference

```rust
use autumn_harvest::build_routing::{build_reachability, all_build_reachability_sharded};
use std::time::Duration;

// Single build, single shard
let r = build_reachability(&mut conn, "sha-old456", Duration::from_secs(60)).await?;

// All builds across all shards
let all = all_build_reachability_sharded(&pool, Duration::from_secs(60)).await?;
```

Response fields:

| Field | Meaning |
|-------|---------|
| `open_executions` | Non-terminal executions with this `assigned_build_id` |
| `pending_tasks` | Tasks in PENDING state with this `required_build_id` |
| `active_workers` | Workers with this `build_id` and a recent heartbeat |
| `stale_workers` | Workers with this `build_id` whose heartbeat has expired |
| `safe_to_retire` | `true` when `open_executions == 0` and `pending_tasks == 0` |

**Old-build workers are safe to stop once `safe_to_retire` is `true`.**

---

## Common mistakes

| Mistake | Fix |
|---------|-----|
| Retiring old workers before `safe_to_retire` | Check reachability; in-flight executions will be orphaned |
| Forgetting to declare compat in Scenario A | New workers skip old-build tasks; old-build executions stall |
| Breaking deploy without `ctx.version()` gate | New workers corrupt in-flight histories on replay; use version gates |
| Rollback without updating the build policy | New starts continue landing on the bad build; set policy back first |
| Empty `build_id` on new workers | Legacy sentinel — the worker claims any task, bypassing all routing |

---

# Runbook: Pre-Deploy Replay Canary

Use the deploy-time replay canary to verify that a candidate build is compatible with currently running executions before advancing the build policy or declaring compatibility.

**When to use:** before rolling out any new worker deployment where you expect the new code to handle existing in-flight workflow executions (i.e. Scenario A).

**When _not_ to use:** for breaking deploys (Scenario B) where you explicitly do not declare compatibility and instead let old workflows run to completion on old workers.

---

## Concepts

The replay canary performs an in-memory execution audit:
1. **Zero Mutations:** The canary never executes activities, schedules timers, sends signals, writes events, or mutates any database records.
2. **Multi-Shard Sampling:** It queries currently `RUNNING` workflow executions across all active shards (using `iter_shards()`).
3. **In-Memory Replay:** It uses the candidate build's `WorkflowReplayer` and registered workflow definitions to replay those executions' history in-memory from start to current state.
4. **Compatibility Report:** If any execution fails to replay (due to non-determinism, changed steps, or code panics), the canary returns a `fail` verdict and details the failing execution ID, event index, and expected vs actual mismatch.

---

## Step 1 — Run the Canary Check

Run the canary command from your candidate deployment (or continuous delivery pipeline) targeting the active database:

```bash
harvest canary --sample-size 500
```

You can narrow the canary check to a specific workflow name or queue:

```bash
harvest canary --sample-size 100 --workflow-name billing_checkout --queue default
```

### Canary Command Output

A successful canary run outputs a summary table:

```text
Canary Verdict: PASS
Sampled: 142 (succeeded: 142, failed: 0, truncated: false)

Summary by Workflow Type:
WORKFLOW TYPE     SAMPLED  SUCCEEDED  FAILED
billing_checkout  80       80         0
user_onboarding   62       62         0
```

If any execution fails to replay, the canary outputs the failure details and exits with exit code `1`:

```text
Canary Verdict: FAIL
Sampled: 142 (succeeded: 140, failed: 2, truncated: false)

Summary by Workflow Type:
WORKFLOW TYPE     SAMPLED  SUCCEEDED  FAILED
billing_checkout  80       78         2
user_onboarding   62       62         0

Replay Failures:
EXECUTION ID                          WORKFLOW TYPE     KIND             EVENT IDX  ERROR
9bc9759c-6aef-4a1e-8495-2d4e7f91cc09  billing_checkout  missing_command  14         expected ScheduleActivity(charge_card), got ScheduleActivity(charge_card_v2)

Diagnostic details for execution 9bc9759c-6aef-4a1e-8495-2d4e7f91cc09:
  Expected: ScheduleActivity(charge_card)
  Actual:   ScheduleActivity(charge_card_v2)
```

To fetch the raw JSON report (e.g. for custom scripting or diagnostic tools), pass the `--json` flag:

```bash
harvest canary --sample-size 200 --json
```

---

## Step 2 — Combine with Build-Id Routing

1. **Verify candidate build compatibility:** Run `harvest canary` before promoting the new build.
2. **If Canary Passes:** The candidate build is compatible with all sampled running executions. You can safely deploy the new workers, declare compatibility (e.g. `declare_compat("sha-new", "sha-old")`), and advance the routing policy.
3. **If Canary Fails:** Replay safety is compromised. You must either:
   - Fix the non-determinism in the candidate build.
   - Use `ctx.version()` gates to isolate the new behavior.
   - Run the deploy as a breaking deploy (Scenario B).

---

## Caveats and Statistical Nature

> [!WARNING]
> The replay canary is **statistical**, not mathematical proof of 100% compatibility.
> Because it samples up to `sample_size` executions, it may miss rare or edge-case workflow histories that contain compatibility issues.
> For high-risk deployments:
> - Keep `sample_size` sufficiently high (e.g., 500 to 1000).
> - Keep a comprehensive test suite of workflow history replay fixtures (using `harvest history export`).
> - Combine the canary with gradual rollout of workers and active monitoring of non-determinism alarms.

