# Pre-Retention History Archival

Autumn-harvest features an automated, background-scheduled **Retention Janitor** designed to prune completed workflow executions, event logs, signals, and timers to prevent unbound database growth. 

Before permanent deletion from Postgres, operators can register a custom `HistoryArchiver` hook. This allows shipping full, JSON-compatible `HistoryExportDocument` files to cold storage (e.g., AWS S3, Google Cloud Storage, or a local network drive).

```mermaid
sequenceDiagram
    participant D as Postgres Database
    participant J as Retention Janitor (Tick)
    participant A as HistoryArchiver Hook
    participant S as Cold Storage (S3 / Local)
    
    J->>D: Scan terminal workflows older than max_age
    D-->>J: Return eligible workflow executions
    loop For each workflow candidate
        J->>D: Load full event history
        D-->>J: History events
        J->>J: Serialize to HistoryExportDocument
        J->>A: Invoke .archive(document)
        alt Archival Hook Success
            A->>S: Ship document to cold storage
            S-->>A: OK
            A-->>J: Ok(())
            J->>D: Permanently delete Postgres database rows
            D-->>J: Deleted
        else Archival Hook Error
            A-->>J: Err(error)
            J->>J: Skip deletion (Zero-Loss Guarantee)
            Note over J,D: Row remains in Postgres; Retried on next tick
        end
    end
```

---

## Zero-Loss Guarantee

> [!IMPORTANT]
> **Safety First / Zero-Loss Principle**:
> If the registered archival hook fails (due to transient network timeouts, credential errors, filesystem exhaustion, or invalid configurations), the retention janitor **skips database deletion** for that workflow. 
>
> The execution and its history remain safely in Postgres and will automatically be retried on subsequent ticks. Pruning only succeeds when the operator's archival hook returns a definitive `Ok(())`.

---

## Implementing `HistoryArchiver`

The `HistoryArchiver` trait is unconditionally exported in the core crate prelude:

```rust
pub trait HistoryArchiver: Send + Sync + 'static {
    /// Ship the history export document to cold storage.
    ///
    /// If this returns `Err`, the retention janitor skips deleting the
    /// workflow execution and its associated events on this tick, retrying
    /// on the next tick to prevent data loss.
    fn archive(
        &self,
        doc: &crate::history_export::HistoryExportDocument,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<(), Box<dyn std::error::Error + Send + Sync>>,
                > + Send,
        >,
    >;
}
```

### Example: Archiving to local files

Here is a simple implementation that archives history documents to a local `/var/log/archive/` folder:

```rust
use std::fs;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use autumn_harvest::{HistoryArchiver, history_export::HistoryExportDocument};

struct FileSystemArchiver {
    target_dir: PathBuf,
}

impl HistoryArchiver for FileSystemArchiver {
    fn archive(
        &self,
        doc: &HistoryExportDocument,
    ) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send>> {
        let target_dir = self.target_dir.clone();
        let doc = doc.clone();

        Box::pin(async move {
            // Ensure target directory exists
            fs::create_dir_all(&target_dir)?;

            // Render pretty JSON document
            let serialized = serde_json::to_string_pretty(&doc)?;

            // Construct unique file path based on execution ID
            let file_path = target_dir.join(format!("{}.json", doc.execution_id));

            // Write to disk
            fs::write(file_path, serialized)?;

            Ok(())
        })
    }
}
```

### Example: Archiving to AWS S3 (mocked)

For production, you can wire up an SDK client such as `aws-sdk-s3` inside the future block:

```rust
use std::future::Future;
use std::pin::Pin;
use autumn_harvest::{HistoryArchiver, history_export::HistoryExportDocument};

struct S3Archiver {
    s3_client: aws_sdk_s3::Client,
    bucket_name: String,
}

impl HistoryArchiver for S3Archiver {
    fn archive(
        &self,
        doc: &HistoryExportDocument,
    ) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send>> {
        let client = self.s3_client.clone();
        let bucket = self.bucket_name.clone();
        let doc = doc.clone();

        Box::pin(async move {
            let serialized = serde_json::to_string(&doc)?;
            let key = format!("workflow-history/{}/{}.json", doc.workflow_name, doc.execution_id);

            client
                .put_object()
                .bucket(bucket)
                .key(key)
                .body(serialized.into_bytes().into())
                .content_type("application/json")
                .send()
                .await?;

            Ok(())
        })
    }
}
```

---

## Configuration & Wiring

Once you have implemented your archiver, register it fluently during startup using `HarvestBuilder`:

```rust
use std::sync::Arc;
use std::time::Duration;
use autumn_harvest::retention::RetentionConfig;

let retention_config = RetentionConfig::with_max_age(Duration::from_secs(7 * 24 * 60 * 60)) // Global default: prune workflows older than 7 days
    // Per-workflow-type overrides (issue #737): a type without an override
    // falls back to the global default; a type with neither is never deleted.
    // Each override name MUST match a registered `#[workflow]` type (added via
    // `.workflows(workflows![...])` on the same builder); otherwise `build()`
    // panics (or `try_build()` returns
    // `HarvestBuilderError::UnknownRetentionOverrideWorkflow`), and an
    // out-of-range override value yields `HarvestBuilderError::InvalidRetention`.
    .with_workflow_override("compliance_report", Duration::from_secs(365 * 24 * 60 * 60)) // keep 1 year
    .with_workflow_override("ephemeral_ping", Duration::from_secs(60 * 60)) // keep 1 hour
    .with_audit_retention_days(90)
    .with_schedule_decision_retention_days(7);

let archiver = FileSystemArchiver {
    target_dir: "/var/log/archive".into(),
};

let harvest = autumn_harvest::HarvestBuilder::new()
    .retention(retention_config)
    .history_archiver(archiver) // <-- Register custom archival hook
    .build();
```

---

## Operations & Debugging

### Telemetry

Pruning telemetry is emitted dynamically:
* `harvest.retention.deleted` counter tracks the number of workflow histories successfully purged from the database, **labeled by workflow type** (issue #737) so per-type retention overrides are confirmable (a long-retained type reads `0` until its own age is reached). Emitted for real deletions only — `dry_run` reports per-type would-delete counts on `GET /admin/retention` without emitting the counter.
* Shard-level statistics are also observable in your metrics agent (gauges for processing duration and candidate scan sizes).

### Log Diagnosis

Under transient failure, the retention janitor logs warnings:

```
[WARN]  harvest_events: failed to load history events for retention candidate; skipping deletion execution_id=... error=...
[ERROR] harvest_retention: pre-retention archival hook failed; skipping deletion execution_id=... error=...
```

To debug, filter your logs by `autumn_harvest::retention` target. Check network routes and credentials associated with your cold storage bucket. The candidate will continue to re-appear inside subsequent ticks until the hook succeeds.

## See also

* [`docs/partitioned-events.md`](partitioned-events.md) — an opt-in, complementary
  reclamation path: instead of this janitor's row-by-row delete, eligible
  `harvest_events` rows can live in droppable partitions, so retention reclaims
  space by dropping a partition instead of deleting individual rows.
