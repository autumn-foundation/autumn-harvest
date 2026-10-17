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
on, they are codec envelopes (ciphertext). Retention does not decode payload
fields before it archives them. A decode needs every codec key, and the
archive must keep ciphertext.

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
// `aws-config` is a direct dependency of the application.
let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
let client = aws_sdk_s3::Client::new(&config);
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
| `ObjectPayloadStore` | `{prefix}{uuid}` | Each upload gets a new key. Identical bytes do not share an object, so retention cannot delete a blob that a new run is about to reference. |
| `ObjectHistoryArchiver` | `{prefix}{execution_id}.json` | A second archive of a run overwrites the object. |

`ObjectHistoryArchiver::fetch` reads a missing object as `None`, and the API
returns 404. `ObjectPayloadStore::get` returns an error, because a missing
blob is data loss. A delete of a missing object succeeds.

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
after `archival_timeout_secs`. `ObjectHistoryArchiver` refuses an object over
64 MiB by default; `with_max_fetch_bytes` changes the limit. Vantage shows the
first 1000 events. The API returns all of them. Use the document for display
and replay debugging. `WorkflowReplayer` reads the same export format, but it
does not decode payload fields. With a codec on, replay the API response with
decode-on-read on, not the raw object.

> [!IMPORTANT]
> **PII erasure does not reach the archive.** `erase-payloads` works on rows in
> Postgres. After retention deletes a run, the archive object is the only copy.
> To erase it, delete or overwrite the object in the bucket. A bucket lifecycle
> rule can also expire archives after a fixed age.

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
- **The lock.** Every applying sweep takes the session advisory lock
  `partition_archive::EXPORT_LOCK_KEY`. An exporter takes it exclusive and
  only then writes the marker. A sweep with no archiver takes it shared and
  only then reads the marker. So one process at a time exports a shard, and
  no sweep with no archiver can miss a marker that an exporter is writing. A
  sweep that cannot take the lock reports
  `another process holds the export lock of this shard` and drops nothing.
  In a caller's transaction, each sweep holds a transaction lock on the
  same key instead, until that transaction ends. So a pending marker or a
  pending drop cannot race the other kind of sweep, and an error or a
  cancelled pass cannot leave the lock held.
- **Reuse.** A failed drop leaves its export in place. The next pass reads it
  back and uses it when it still checks clean. A stale export gets one new
  export in the same pass.
- **Budget.** One pass makes at most 4 new exports. A pass that reaches the
  budget reports `truncated`, and the next pass goes on.
- **Dry run.** With `dry_run` set, the retention runtime ignores the
  archiver. It uploads nothing and writes no marker.
- **Isolation.** In a caller's transaction, an exporting sweep needs
  `READ COMMITTED`. At a higher level, one snapshot could hide a rewrite,
  such as an erasure, so the sweep returns an error and drops nothing.

### Keys

```text
harvest-partitions/shard-<id>/<partition>/<lower>_<upper>/segment-000001-<sha256:16>.jsonl
harvest-partitions/shard-<id>/<partition>/<lower>_<upper>/manifest-<sha256:16>.json
harvest-partitions/shard-<id>/<partition>/<lower>_<upper>/latest.json
```

`<lower>` is `min` for the legacy partition. The bounds keep a later
partition with the same name from replacing an old export.

- A segment key and a manifest key hold the first 16 hex digits of a
  SHA-256 of their content. An upload that lands late, after a timeout,
  writes its own key. It cannot replace an object that a finished export
  names.
- The drop log, `harvest_partition_exports` on the shard, names the
  manifest of each export that a drop checked. The sweep inserts the row in
  the drop transaction, so a row exists exactly when its partition was
  dropped. `partition_archive::dropped_manifest_key` reads it.
- `latest.json` names the last verified export. The next pass uses it to
  find an export to reuse. A stale hint costs one new export, because the
  drop checks the partition against the manifest under its lock.

### Read-back

```rust
use autumn_harvest::partition_archive::{dropped_manifest_key, read_back};

// `conn` is a connection to the shard. `archiver` is an `Arc<dyn PartitionArchiver>`.
// The caller returns a boxed error.
let manifest_key = dropped_manifest_key(&mut conn, &partition_name, upper)
    .await?
    .ok_or("not dropped through an export")?;
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
- **Grants.** A least-privilege runtime role needs `SELECT` and `INSERT` on
  `harvest_partition_export` and `harvest_partition_exports`. The startup
  preflight checks them.
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

## Orphaned payload blobs (issue #2123)

A write uploads its offloaded payloads before it opens its transaction. If the
transaction rolls back, the blobs stay in the store with no
`harvest_payload_refs` row. Retention finds blobs only through those rows, so
it never deletes them.

The orphan sweep deletes them. It is off by default. Turn it on with
`RetentionConfig::with_payload_orphan_grace(grace)`.

- The sweep deletes a blob only when the blob is older than the grace period.
  The grace period must be longer than any write transaction. The minimum is
  one hour.
- The sweep deletes a blob only when no shard references it. It checks every
  database.
- A failed reference check stops the sweep. It deletes nothing on a guess.
- Each tick reads at most 100 pages from the store and resumes where it
  stopped.
- `dry_run` counts orphans in the log, deletes nothing and records no metric.

The sweep needs a `PayloadStore` that implements `list`. A store without it
turns the sweep off, and the log says so once. `list` must report the time of
the latest `put` of a key. A `put` of an existing key must refresh that time.
Implement `delete_if_older` too, with an atomic precondition such as `If-Match`.
It keeps a blob that a writer re-uploads after the listing. Without it, the
sweep uses a plain `delete`, and that re-upload can lose its blob.

Use a store that holds only harvest blobs. The sweep deletes every old blob
that no execution references. Do not share the store with the agent response
cache or with another writer.

Give each retention process the full shard list. The sweep checks only the
databases in its own pool set. A reference on an unlisted database is
invisible, and the sweep deletes that blob.

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
