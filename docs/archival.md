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
    fn archive(&self, doc: &HistoryExportDocument) -> ArchiverFuture<'_>;

    /// Read back the archived document (issue #1983). The default returns
    /// `ArchiveFetchError::Unsupported`.
    fn fetch(&self, execution_id: &ExecutionId) -> ArchiveFetchFuture<'_> { /* ... */ }
}
```

Implement `fetch` to make an archive readable through the management API and
Vantage. See [Reading an archive back](#reading-an-archive-back-issue-1983).

Payload fields in the document are in their stored form. With a payload codec
on, they are codec envelopes (ciphertext). Before issue #1983, retention
decoded them with the identity codec. With a real codec, that decode failed,
and retention never archived or deleted the run.

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

### Example: Archiving to AWS S3 (hand-written)

The plugin crate ships S3 and GCS archivers. See
[First-party S3 and GCS adapters](#first-party-s3-and-gcs-adapters-issue-1983).
This example shows the shape of a hand-written archiver only:

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

## First-party S3 and GCS adapters (issue #1983)

The `autumn-harvest-plugin` crate ships adapters for `PayloadStore` and
`HistoryArchiver`. The core crate keeps no cloud dependency.

| Feature | Adds |
|---------|------|
| `object-store` | `ObjectBackend`, `ObjectPayloadStore`, `ObjectHistoryArchiver`, `MemoryBackend`. No client. |
| `s3` | `object_store::s3::S3Backend` on `aws-sdk-s3`. Works with any S3-compatible store, for example MinIO. |
| `gcs` | `object_store::gcs::GcsBackend` on the GCS JSON API. It uses the `reqwest` client that the plugin already has. |

```toml
autumn-harvest-plugin = { version = "0.7", features = ["s3"] }
```

```rust
use std::sync::Arc;
use autumn_harvest_plugin::object_store::s3::{S3Backend, aws_sdk_s3};
use autumn_harvest_plugin::object_store::{ObjectHistoryArchiver, ObjectPayloadStore};

let builder = autumn_harvest::HarvestBuilder::new();
let codecs = builder.payload_codecs().clone();
let client = aws_sdk_s3::Client::new(&aws_config::load_from_env().await);
let backend = Arc::new(S3Backend::new(client, "harvest-archive"));

let builder = builder
    .payload_store(ObjectPayloadStore::new(Arc::clone(&backend)).with_prefix("blobs/"))
    .history_archiver(
        ObjectHistoryArchiver::new(backend)
            .with_prefix("history/")
            .with_codecs(codecs),
    );
```

For GCS, use `GcsBackend::new("harvest-archive", GceMetadataToken::new())`.
`GceMetadataToken` reads the service-account token on GCE, GKE and Cloud
Run. `StaticToken` sends a fixed token. For a service-account JSON key,
implement `GcsTokenSource`.

### Object keys

| Adapter | Key | Note |
|---------|-----|------|
| `ObjectPayloadStore` | `{prefix}{sha256-hex}` | Identical bytes share one object. Retention deletes a blob when no run refers to it. |
| `ObjectHistoryArchiver` | `{prefix}{execution_id}.json` | A second archive of a run overwrites the object. |

A missing object reads as "not found". A delete of a missing object succeeds.

`ObjectPayloadStore` records the store id `"default"` in each reference, like
the trait default. To keep reading references that another store wrote, set
the same id with `with_store_id`.

### Codec

- **Offloaded payloads.** The offloader encodes a payload with the codec
  before upload. With a codec on, each blob is a codec envelope.
- **Archived histories.** Payload fields arrive in their stored form, so they
  are codec envelopes too. `with_codecs` also encodes the whole document.
  This hides metadata such as `workflow_id`.

Pass the builder's `payload_codecs()` to `with_codecs`. The key registry is
shared, so a key rotation applies at once. The codec-rotation sweep does not
re-encrypt archive objects. Keep a retired key registered while its archives
must stay readable.

### Emulator tests

The `object_store_s3_minio` and `object_store_gcs_emulator` suites run each
adapter against an emulator in Docker:

```sh
cargo test -p autumn-harvest-plugin --features s3 --test object_store_s3_minio
cargo test -p autumn-harvest-plugin --features gcs --test object_store_gcs_emulator
```

The official MinIO images are no longer published. The suite uses
`pgsty/minio`, the maintained community build. GCS tests use
`fsouza/fake-gcs-server`.

---

## Reading an archive back (issue #1983)

An archiver that implements `fetch` makes a pruned run readable again:

- `GET /workflows/{id}/archived-history` returns the archived document.
  Admin only. See [`docs/management-api.md`](management-api.md#archived-history-issue-1983).
- Vantage shows it at `/ui/workflows/{id}/archived-history`. When an archiver
  is set, the "not found" page of a pruned run links there.

Payload fields decode under the same gate as live history. The read times out
after `archival_timeout_secs`. Use the document for display and replay
debugging. `WorkflowReplayer` reads the same export format.

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
