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

### Per-tenant retention overrides (issue #1977)

A tenant override keeps every run of one tenant for its own age. It matches
the run's stored tenant, `harvest_workflow_executions.tenant`. A start by a
tenant-bound caller sets that tenant. See
[Tenant binding](security-posture.md#tenant-binding-issue-1977).

```rust
use std::time::Duration;
use autumn_harvest::retention::RetentionConfig;

let retention_config = RetentionConfig::with_max_age(Duration::from_secs(7 * 24 * 60 * 60))
    .with_workflow_override("compliance_report", Duration::from_secs(365 * 24 * 60 * 60))
    // Every run of tenant `acme`, of any type, is deleted after 30 days.
    .with_tenant_override("acme", Duration::from_secs(30 * 24 * 60 * 60));
```

- **Precedence.** The tenant override wins. Then the type override. Then the
  global `max_age`. A run of `acme` of type `compliance_report` is kept 30
  days, not one year.
- **No tenant.** A run with no tenant, or a tenant with no override, keeps
  the type and global ages.
- **Alone.** A tenant override alone turns on history retention. Runs of
  other tenants are then never deleted.
- **Legal hold wins.** A held run is never deleted.
- **Bounds.** The age has the type-override bounds, 1 s to 10 years. The key
  is 1 to 128 bytes of visible ASCII, with no spaces. Anything else makes
  `try_build()` return `HarvestBuilderError::InvalidRetention`. The builder
  cannot check that a tenant exists.
- **Report.** `GET /admin/retention` shows the overrides as
  `config.tenant_overrides`, in seconds.

---

## Partition export (issue #2009)

On the [partitioned layout](partitioned-events.md), the sweep drops an aged
`harvest_events` partition when no live run owns a row in it. Register a
`PartitionArchiver` to keep that history in object storage. The sweep then
exports the partition before it drops it.

```rust
use autumn_harvest::partition_archive::DirectoryPartitionArchiver;

let harvest = autumn_harvest::HarvestBuilder::new()
    .retention(retention_config)
    .partition_archiver(DirectoryPartitionArchiver::new("/mnt/cold/harvest"))
    .build();
```

The trait has two calls, `put(key, bytes)` and `get(key)`. Core owns the
keys, the manifest and every check, so any blob store fits. A backend can
compress in `put` and expand in `get`. An object must be durable when `put`
returns `Ok`, because the drop can commit right after it. Give each
deployment its own bucket prefix or root directory.

### What the sweep does

1. **Export.** It reads the rows in key order and uploads segments of up to
   10,000 rows or 8 MiB. Each row is one line of `to_jsonb(row)::text`. The
   manifest goes last.
2. **Verify.** It reads back each segment and the manifest. It compares the
   bytes, the SHA-256 hashes, the row count and a row checksum, and it
   parses every row.
3. **Drop.** It takes the partition's `SHARE` lock and computes the row
   checksum in one statement. It drops the partition only when the row
   count and the checksum match the manifest.

A failure at any step keeps the partition. The last sweep's `blocked` list
in `GET /admin/retention` shows the reason, and the next tick tries again. A
row that changes after the export, for example by a codec key rotation,
gives `changed since export`. `SweepOutcome::exported` lists the manifest
key of each dropped partition. Each backend call has the
`archival_timeout_secs` limit.

### Guards

- **The marker.** Before its first export, the sweep writes one row to
  `harvest_partition_export`. From then on, a sweep with no archiver on that
  shard drops nothing. This covers `harvest partition maintain`,
  `RetentionRuntime::spawn`, and a process started without the archiver.
  They report `export required, but no archiver is set`. To end the
  requirement, for example after you remove the archiver for good, run
  `DELETE FROM harvest_partition_export;` on the shard.
- **The lock.** One process at a time exports a shard. It holds the session
  advisory lock `partition_archive::EXPORT_LOCK_KEY`. Another process
  reports `another process is exporting this shard` and drops nothing.
- **Reuse.** A failed drop leaves its export in place. The next pass reads it
  back and uses it when it still checks clean. A stale export gets one new
  export in the same pass.
- **Budget.** One pass makes at most 4 new exports. A pass that reaches the
  budget reports `truncated`, and the next pass goes on.

### Keys

```text
harvest-partitions/shard-<id>/<partition>/<lower>_<upper>/segment-000001-<sha256:16>.jsonl
harvest-partitions/shard-<id>/<partition>/<lower>_<upper>/manifest.json
```

`<lower>` is `min` for the legacy partition. The bounds keep a later
partition with the same name from replacing an old export. A segment key
holds the first 16 hex digits of its SHA-256, so an upload that lands late
cannot replace a segment that a finished export names.

### Read-back

```rust
use autumn_harvest::partition_archive::read_back;

// `archiver` is an `Arc<dyn PartitionArchiver>`. The caller returns a boxed error.
let part = read_back(archiver.as_ref(), &manifest_key).await?;
let events = part.history(execution_id)?;
```

`read_back` checks every hash before it returns rows. `history` returns one
run's events in this partition, in `event_id` order. A run can span
partitions. `history_with_codecs` decodes payload fields with your codec keys.

### Limits

- **Ciphertext stays ciphertext.** Payload fields keep their stored form. You
  need the codec key that was active at export time to decode them. Keep
  retired keys while their archives exist.
- **Offloaded payloads are not in the export.** Retention collects a run's
  blobs when it deletes the run. Use the per-run `HistoryArchiver` too, if
  cold storage must hold offloaded payloads.
- **Erasure does not reach an export.** Delete the object yourself.
- **Older binaries ignore the marker.** Deploy the archiver to every process
  before you rely on it.
- **No straggler deletes.** `try_build` refuses a partition archiver together
  with `partitions.straggler_grace_secs`. A straggler delete removes rows
  that no export holds. On a marked shard, the sweep skips straggler deletes.
- **Cost.** The sweep reads the partition once to export it, downloads it
  once to verify it, and scans it once under `SHARE`. `SHARE` blocks row
  changes and `VACUUM` on that closed partition. An `UPDATE` with no
  `cohort` filter, such as an erasure or a codec rotation, waits for the
  scan. The scan has the `exact_scan_timeout` limit, the same as the
  ownership scan.

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
