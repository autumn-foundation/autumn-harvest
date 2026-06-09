# Chapter 9 — Worker routing and capabilities

[← DAGs and schedules](08-dags-and-schedules.md) · [Index](README.md) · [Next: Operating the service →](10-operations.md)

---

As your workflow fleet grows, tasks often need to run on specific hardware, in specific regions, or on nodes with specific permissions. For example, a video transcoding activity might require a GPU, while a data syncing activity must run in `us-west-2` to minimize egress costs.

Harvest provides three primary mechanisms for routing tasks to the right workers:

1. **Queue name partitioning** (separating workers onto different queues).
2. **Build-ID routing** (version-based compatibility gates).
3. **Worker capability labels and activity requirements** (hardware/region-aware routing within a single queue).

---

## Routing Pattern Comparison

| Mechanism | Best for… | Pros | Cons |
|---|---|---|---|
| **Queue Partitioning** | Coarse-grained thread pool separation (e.g. `high-cpu` vs `io-bound`). | Simple setup; isolated database locks per queue. | Queue name explosion if used for fine-grained attributes (e.g., `gpu-useast1-v4`). |
| **Build-ID Routing** | Blue-green deployments, rolling releases, and retirement gates. | Automated compatibility checking; deterministic version gates. | Designed for deployment versions, not static hardware properties. |
| **Capability Labels** | Fine-grained static capabilities (e.g. `gpu = true`, `region in [us-east-1]`). | No queue name explosion; filters at database claim-time; in-memory activity requirements. | Requires restarting the worker to update labels. |

---

## Worker Capability Labels

Instead of creating separate task queues for every combination of hardware and location, you can assign **capability labels** to workers and declare **requirements** on activities. The scheduler automatically gates task delivery at claim time, ensuring workers only receive tasks they are capable of running.

### 1. Declaring Activity Requirements

Activity requirements are defined using the `requires` parameter on the `#[activity]` macro. Harvest parses these requirements in a clean, declarative syntax supporting exact matches (`=`) and set membership (`in`).

```rust
use autumn_harvest::prelude::*;

// Requires a GPU-enabled worker in either us-east-1 or us-west-2
#[activity(
    start_to_close = "10m",
    queue = "transcoding",
    requires = "gpu = true, region in [us-east-1, us-west-2]"
)]
async fn transcode_video(ctx: &ActivityContext, input: TranscodeInput) -> HarvestResult<TranscodeOutput> {
    // Transcoding logic...
    Ok(TranscodeOutput::default())
}
```

### 2. Configuring Worker Capability Labels

Workers publish their labels when registering with the fleet. Configure these labels on the `WorkerConfig` builder:

```rust
use autumn_harvest::WorkerConfig;

let mut labels = std::collections::HashMap::new();
labels.insert("gpu".to_string(), "true".to_string());
labels.insert("region".to_string(), "us-east-1".to_string());

let config = WorkerConfig::default()
    .with_labels(labels);
```

Or programmatically add individual labels:

```rust
let config = WorkerConfig::default()
    .with_label("gpu", "true")
    .with_label("region", "us-east-1");
```

---

## Under the Hood: Claim-Time Filtering

To keep database queries fast and scalable, activity requirements are stored in the worker's handler registry rather than persisted in the database.

1. **Ineligible List Generation**: At boot, the worker evaluates its registered activities against its configured capability labels. Any activity whose requirements are *not* satisfied is added to the worker's `ineligible_activities` list.
2. **Claim Gating**: When polling for tasks, the worker passes its `ineligible_activities` array to the `claim_task` database query. The scheduler uses a `SKIP LOCKED` query with an additive filter:
   ```sql
   AND (
       task_type != 'activity'
       OR activity_name IS NULL
       OR NOT (activity_name = ANY($6))
   )
   ```
   This prevents the worker from ever locking or claiming a task it is not equipped to execute, leaving it available for capable workers.

---

## Monitoring and Triage

When tasks seem stuck, you can inspect the fleet capability state using the dashboard, CLI, or HTTP endpoints.

### Query Capable Workers
To find out which workers can run a specific activity, use the `capable_of` parameter:

```bash
# HTTP API
curl -s "http://localhost:3000/api/harvest/workers?capable_of=transcode_video" | jq .

# CLI
harvest worker list --capable-of transcode_video
```

### Eligibility Triage
If an activity is PENDING and no workers are claiming it, check the queue eligibility endpoint:

```bash
curl -s "http://localhost:3000/api/harvest/admin/queues/transcoding/eligibility" | jq .
```

The response lists ineligible workers and the exact unsatisfied requirement:

```json
{
  "queue_name": "transcoding",
  "pending_tasks_count": 5,
  "eligible_workers": [
    {
      "worker_id": "worker-gpu-node-1",
      "labels": {"gpu": "true", "region": "us-east-1"}
    }
  ],
  "ineligible_workers": [
    {
      "worker_id": "worker-cpu-node-2",
      "labels": {"region": "us-east-1"},
      "reasons": ["unsatisfied_requirement:gpu=true"]
    }
  ]
}
```

---

[← DAGs and schedules](08-dags-and-schedules.md) · [Index](README.md) · [Next: Operating the service →](10-operations.md)
