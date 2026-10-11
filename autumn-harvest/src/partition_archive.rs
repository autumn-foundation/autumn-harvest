//! Export of aged `harvest_events` partitions to object storage (issue #2009).
//!
//! The partition sweep drops a closed cohort partition when no live run owns
//! a row in it. With a [`PartitionArchiver`] set, the sweep first exports the
//! partition, reads the export back, and checks it. The drop then hashes the
//! partition again under its `SHARE` lock. It drops only when that hash
//! matches the manifest. See `DESIGN-2009.md`.
//!
//! # Layout
//!
//! Each export is a set of segments and one manifest under one prefix:
//!
//! ```text
//! harvest-partitions/shard-<id>/<partition>/<lower>_<upper>/segment-000001.jsonl
//! harvest-partitions/shard-<id>/<partition>/<lower>_<upper>/manifest.json
//! ```
//!
//! A segment holds one row per line, as `to_jsonb(row)::text`, in `id`
//! order. The manifest goes last, so a manifest always names complete
//! segments.
//!
//! # Scope
//!
//! The export reads rows and the drop is DDL. Neither writes `event_data`,
//! so the two sanctioned writers in `CLAUDE.md` do not change. Payload fields
//! stay in their stored form, so codec ciphertext stays ciphertext.

use std::collections::BTreeMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::event::WorkflowEvent;
use crate::types::ExecutionId;

/// The error type a [`PartitionArchiver`] call returns.
pub type ArchiveError = Box<dyn std::error::Error + Send + Sync>;

/// The future type a [`PartitionArchiver`] call returns.
pub type ArchiveIo<'a, T> = Pin<Box<dyn Future<Output = Result<T, ArchiveError>> + Send + 'a>>;

/// A blob store for partition exports.
///
/// Core owns the key layout, the manifest and every check. An
/// implementation only stores and returns bytes. A backend can compress in
/// [`put`](Self::put) and expand in [`get`](Self::get). The checks cover the
/// bytes that core gives and gets back, so they still match.
pub trait PartitionArchiver: Send + Sync + 'static {
    /// Store `bytes` at `key`. Replace an object that is already there.
    fn put<'a>(&'a self, key: &'a str, bytes: Vec<u8>) -> ArchiveIo<'a, ()>;

    /// Return the bytes at `key`, or `None` when no object is there.
    fn get<'a>(&'a self, key: &'a str) -> ArchiveIo<'a, Option<Vec<u8>>>;
}

/// The manifest format this build writes and reads.
pub const FORMAT_VERSION: u32 = 1;

/// The first path part of every key.
pub const KEY_ROOT: &str = "harvest-partitions";

/// A segment closes when it holds this many rows.
pub const SEGMENT_MAX_ROWS: usize = 10_000;

/// A segment closes before a row that would take it past this many bytes.
/// A single larger row gets a segment of its own.
pub const SEGMENT_MAX_BYTES: usize = 8 * 1024 * 1024;

/// The key prefix of one partition export.
///
/// The prefix holds the shard and the cohort bounds, not only the name. A
/// disable and a new enable can bring back the legacy partition name, but
/// the new conversion instant gives it new bounds. So a new export never
/// replaces an old one.
#[must_use]
pub fn archive_prefix(
    shard_id: i32,
    partition: &str,
    lower: Option<DateTime<Utc>>,
    upper: DateTime<Utc>,
) -> String {
    let _ = (shard_id, partition, lower, upper);
    String::new()
}

/// The manifest key under `prefix`.
#[must_use]
pub fn manifest_key(prefix: &str) -> String {
    let _ = prefix;
    String::new()
}

/// The key of segment `n` under `prefix`. Segments count from 1.
#[must_use]
pub fn segment_key(prefix: &str, n: usize) -> String {
    let _ = (prefix, n);
    String::new()
}

/// One segment in a [`PartitionManifest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentEntry {
    /// The object key.
    pub key: String,
    /// Rows in the segment.
    pub rows: u64,
    /// Bytes in the segment.
    pub bytes: u64,
    /// Lowercase hex SHA-256 of the segment bytes.
    pub sha256: String,
}

/// The index of one partition export.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionManifest {
    /// The manifest format, [`FORMAT_VERSION`].
    pub format: u32,
    /// The shard that held the partition.
    pub shard_id: i32,
    /// The partition table name.
    pub partition: String,
    /// The inclusive lower cohort bound. `None` for `MINVALUE`.
    pub lower: Option<DateTime<Utc>>,
    /// The exclusive upper cohort bound.
    pub upper: DateTime<Utc>,
    /// When the export finished.
    pub exported_at: DateTime<Utc>,
    /// Rows in the partition.
    pub row_count: u64,
    /// Lowercase hex SHA-256 of all segment bytes, in segment order.
    pub sha256: String,
    /// The segments, in order.
    pub segments: Vec<SegmentEntry>,
}

/// A streaming hash over row lines.
///
/// Export and the drop check feed the same lines, so the same rows give the
/// same hash.
#[derive(Debug, Clone, Default)]
pub struct RowDigest {
    hasher: Sha256,
    rows: u64,
}

impl RowDigest {
    /// Add one row line. The digest adds the line feed.
    pub fn push(&mut self, line: &str) {
        let _ = line;
    }

    /// Rows added so far.
    #[must_use]
    pub const fn rows(&self) -> u64 {
        self.rows
    }

    /// The lowercase hex SHA-256 of every line added, each with its line feed.
    #[must_use]
    pub fn finish(self) -> String {
        String::new()
    }
}

/// Splits row lines into segments.
#[derive(Debug)]
pub struct SegmentWriter {
    max_rows: usize,
    max_bytes: usize,
    buf: Vec<u8>,
    rows: usize,
}

impl SegmentWriter {
    /// A writer with the given limits. A zero limit counts as 1.
    #[must_use]
    pub fn new(max_rows: usize, max_bytes: usize) -> Self {
        Self {
            max_rows: max_rows.max(1),
            max_bytes: max_bytes.max(1),
            buf: Vec::new(),
            rows: 0,
        }
    }

    /// Add one row line. Returns the closed segment and its row count when
    /// this line starts a new one.
    pub fn push(&mut self, line: &str) -> Option<(Vec<u8>, u64)> {
        let _ = (line, self.max_rows, self.max_bytes);
        None
    }

    /// Return the open segment, if it holds a row.
    pub fn finish(&mut self) -> Option<(Vec<u8>, u64)> {
        None
    }
}

/// Lowercase hex SHA-256 of `bytes`.
#[must_use]
pub fn sha256_hex(bytes: &[u8]) -> String {
    let _ = bytes;
    String::new()
}

/// One archived `harvest_events` row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArchivedEventRow {
    /// The row id.
    pub id: i64,
    /// The run that wrote the row.
    pub workflow_exec_id: uuid::Uuid,
    /// The event id inside the run.
    pub event_id: i32,
    /// The event type.
    pub event_type: String,
    /// The stored event. Codec envelopes stay ciphertext.
    pub event_data: serde_json::Value,
    /// The event timestamp.
    pub timestamp: DateTime<Utc>,
    /// Every other column, such as `cohort`. A new column lands here.
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_json::Value>,
}

/// A partition export, read back and checked.
#[derive(Debug, Clone)]
pub struct ArchivedPartition {
    /// The manifest.
    pub manifest: PartitionManifest,
    /// The rows, in `id` order.
    pub rows: Vec<ArchivedEventRow>,
}

impl ArchivedPartition {
    /// The events of one run in this partition, in `event_id` order.
    ///
    /// Payload fields stay as stored, so codec envelopes stay ciphertext.
    /// A run can span partitions, so this can be part of its history.
    ///
    /// # Errors
    ///
    /// A row that does not parse as a [`WorkflowEvent`].
    pub fn history(&self, exec_id: ExecutionId) -> Result<Vec<WorkflowEvent>, serde_json::Error> {
        let _ = exec_id;
        Ok(Vec::new())
    }

    /// The same as [`Self::history`], with payload fields decoded by `codecs`.
    ///
    /// # Errors
    ///
    /// A row that does not decode.
    pub fn history_with_codecs(
        &self,
        exec_id: ExecutionId,
        codecs: &crate::payload_codec::PayloadCodecs,
    ) -> crate::error::HarvestResult<Vec<WorkflowEvent>> {
        let _ = (exec_id, codecs);
        Ok(Vec::new())
    }
}

/// Why [`read_back`] failed.
#[derive(Debug, thiserror::Error)]
pub enum ReadBackError {
    /// An object is missing.
    #[error("archive object missing: {0}")]
    Missing(String),
    /// The backend call failed.
    #[error("archive backend error: {0}")]
    Backend(String),
    /// An object does not match its manifest, or does not parse.
    #[error("archive object corrupt: {0}")]
    Corrupt(String),
}

/// Read a partition export back and check every hash.
///
/// # Errors
///
/// [`ReadBackError`] when an object is missing, the backend fails, or an
/// object does not match the manifest.
pub async fn read_back(
    archiver: &dyn PartitionArchiver,
    manifest_key: &str,
) -> Result<ArchivedPartition, ReadBackError> {
    let _ = (archiver, manifest_key);
    Err(ReadBackError::Backend("not implemented".into()))
}

/// A [`PartitionArchiver`] that writes each key as a file under a root
/// directory.
///
/// A write goes to a temporary file first, then a rename puts it in place.
/// A key path part must match `[A-Za-z0-9._-]+` and must not be `.` or `..`.
#[derive(Debug, Clone)]
pub struct DirectoryPartitionArchiver {
    root: PathBuf,
}

impl DirectoryPartitionArchiver {
    /// An archiver rooted at `root`. The root is made on the first write.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// The root directory.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }
}

impl PartitionArchiver for DirectoryPartitionArchiver {
    fn put<'a>(&'a self, key: &'a str, bytes: Vec<u8>) -> ArchiveIo<'a, ()> {
        let _ = (key, bytes);
        Box::pin(async { Err("not implemented".into()) })
    }

    fn get<'a>(&'a self, key: &'a str) -> ArchiveIo<'a, Option<Vec<u8>>> {
        let _ = key;
        Box::pin(async { Err("not implemented".into()) })
    }
}

/// How the sweep exports a partition before it drops it.
#[derive(Clone)]
pub struct PartitionExport {
    /// Where the export goes.
    pub archiver: std::sync::Arc<dyn PartitionArchiver>,
    /// The shard that holds the partitions. It goes into each key.
    pub shard_id: i32,
    /// The limit on each backend call.
    pub io_timeout: std::time::Duration,
}

impl std::fmt::Debug for PartitionExport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionExport")
            .field("shard_id", &self.shard_id)
            .field("io_timeout", &self.io_timeout)
            .finish_non_exhaustive()
    }
}

impl PartitionExport {
    /// An export to `archiver` for `shard_id`, with a 30 s call limit.
    #[must_use]
    pub fn new(archiver: std::sync::Arc<dyn PartitionArchiver>, shard_id: i32) -> Self {
        Self {
            archiver,
            shard_id,
            io_timeout: std::time::Duration::from_secs(30),
        }
    }

    /// Set the limit on each backend call.
    #[must_use]
    pub const fn with_io_timeout(mut self, io_timeout: std::time::Duration) -> Self {
        self.io_timeout = io_timeout;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use std::collections::HashMap;
    use std::sync::Mutex;

    fn ts(y: i32, m: u32, d: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, 0, 0, 0).unwrap()
    }

    #[derive(Default)]
    struct MemoryArchiver {
        objects: Mutex<HashMap<String, Vec<u8>>>,
    }

    impl PartitionArchiver for MemoryArchiver {
        fn put<'a>(&'a self, key: &'a str, bytes: Vec<u8>) -> ArchiveIo<'a, ()> {
            self.objects.lock().unwrap().insert(key.to_string(), bytes);
            Box::pin(async { Ok(()) })
        }

        fn get<'a>(&'a self, key: &'a str) -> ArchiveIo<'a, Option<Vec<u8>>> {
            let got = self.objects.lock().unwrap().get(key).cloned();
            Box::pin(async move { Ok(got) })
        }
    }

    const ROW_A: &str = r#"{"id": 1, "cohort": "2026-01-01T00:00:00+00:00", "event_id": 0, "event_data": {"type": "WorkflowCompleted", "data": {"output": {"ok": true}}}, "timestamp": "2026-01-01T12:00:00+00:00", "event_type": "WorkflowCompleted", "workflow_exec_id": "00000000-0000-0000-0000-000000000001"}"#;
    const ROW_B: &str = r#"{"id": 2, "cohort": "-infinity", "event_id": 1, "event_data": {"type": "MarkerRecorded", "data": {"name": "m", "details": 1}}, "timestamp": "2026-01-01T12:00:01+00:00", "event_type": "MarkerRecorded", "workflow_exec_id": "00000000-0000-0000-0000-000000000001"}"#;

    /// Store a two-segment export of `rows` and return its manifest key.
    async fn store(archiver: &MemoryArchiver, rows: &[&str]) -> String {
        let prefix = archive_prefix(0, "harvest_events_p_20260101", Some(ts(2026, 1, 1)), ts(2026, 1, 2));
        let mut writer = SegmentWriter::new(1, SEGMENT_MAX_BYTES);
        let mut digest = RowDigest::default();
        let mut segments = Vec::new();
        let mut closed = Vec::new();
        for row in rows {
            digest.push(row);
            closed.extend(writer.push(row));
        }
        closed.extend(writer.finish());
        for (n, (bytes, count)) in closed.into_iter().enumerate() {
            let key = segment_key(&prefix, n + 1);
            segments.push(SegmentEntry {
                key: key.clone(),
                rows: count,
                bytes: bytes.len() as u64,
                sha256: sha256_hex(&bytes),
            });
            archiver.put(&key, bytes).await.unwrap();
        }
        let manifest = PartitionManifest {
            format: FORMAT_VERSION,
            shard_id: 0,
            partition: "harvest_events_p_20260101".into(),
            lower: Some(ts(2026, 1, 1)),
            upper: ts(2026, 1, 2),
            exported_at: ts(2026, 1, 3),
            row_count: digest.rows(),
            sha256: digest.finish(),
            segments,
        };
        let key = manifest_key(&prefix);
        archiver
            .put(&key, serde_json::to_vec(&manifest).unwrap())
            .await
            .unwrap();
        key
    }

    #[test]
    fn the_prefix_holds_the_shard_the_name_and_both_bounds() {
        let prefix = archive_prefix(3, "harvest_events_p_20260101", Some(ts(2026, 1, 1)), ts(2026, 1, 2));
        assert_eq!(
            prefix,
            "harvest-partitions/shard-3/harvest_events_p_20260101/20260101T000000Z_20260102T000000Z"
        );
        assert_eq!(manifest_key(&prefix), format!("{prefix}/manifest.json"));
        assert_eq!(segment_key(&prefix, 7), format!("{prefix}/segment-000007.jsonl"));
    }

    #[test]
    fn a_minvalue_bound_is_min_and_a_new_upper_bound_gives_a_new_prefix() {
        let first = archive_prefix(0, "harvest_events_p_legacy", None, ts(2026, 1, 1));
        let again = archive_prefix(0, "harvest_events_p_legacy", None, ts(2026, 3, 1));
        assert!(first.ends_with("/min_20260101T000000Z"), "{first}");
        assert_ne!(first, again, "a new enable must not overwrite an old export");
    }

    #[test]
    fn the_row_digest_is_the_hash_of_every_line_with_its_line_feed() {
        let mut digest = RowDigest::default();
        digest.push("a");
        digest.push("b");
        assert_eq!(digest.rows(), 2);
        assert_eq!(digest.finish(), sha256_hex(b"a\nb\n"));
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn the_segment_writer_closes_on_the_row_limit() {
        let mut writer = SegmentWriter::new(2, SEGMENT_MAX_BYTES);
        assert_eq!(writer.push("a"), None);
        assert_eq!(writer.push("b"), None);
        assert_eq!(writer.push("c"), Some((b"a\nb\n".to_vec(), 2)));
        assert_eq!(writer.finish(), Some((b"c\n".to_vec(), 1)));
        assert_eq!(writer.finish(), None);
    }

    #[test]
    fn the_segment_writer_closes_before_the_byte_limit_and_isolates_a_large_row() {
        let mut writer = SegmentWriter::new(100, 6);
        assert_eq!(writer.push("ab"), None);
        assert_eq!(writer.push("cd"), None);
        assert_eq!(writer.push("e"), Some((b"ab\ncd\n".to_vec(), 2)));
        assert_eq!(writer.push("0123456789"), Some((b"e\n".to_vec(), 1)));
        assert_eq!(writer.push("f"), Some((b"0123456789\n".to_vec(), 1)));
        assert_eq!(writer.finish(), Some((b"f\n".to_vec(), 1)));
    }

    #[test]
    fn concatenated_segments_hash_to_the_row_digest() {
        let mut writer = SegmentWriter::new(1, SEGMENT_MAX_BYTES);
        let mut digest = RowDigest::default();
        let mut all = Vec::new();
        for line in ["x", "y", "z"] {
            digest.push(line);
            if let Some((bytes, _)) = writer.push(line) {
                all.extend(bytes);
            }
        }
        all.extend(writer.finish().unwrap().0);
        assert_eq!(sha256_hex(&all), digest.finish());
    }

    #[tokio::test]
    async fn read_back_returns_typed_rows_and_a_run_history() {
        let archiver = MemoryArchiver::default();
        let key = store(&archiver, &[ROW_A, ROW_B]).await;
        let part = read_back(&archiver, &key).await.expect("read back");
        assert_eq!(part.manifest.row_count, 2);
        assert_eq!(part.manifest.segments.len(), 2);
        assert_eq!(part.rows.len(), 2);
        assert_eq!(part.rows[1].extra.get("cohort"), Some(&serde_json::json!("-infinity")));
        let exec = ExecutionId::from_uuid(uuid::Uuid::from_u128(1));
        let history = part.history(exec).expect("history parses");
        assert_eq!(history.len(), 2);
        assert!(matches!(history[0], WorkflowEvent::WorkflowCompleted { .. }));
        assert!(matches!(history[1], WorkflowEvent::MarkerRecorded { .. }));
        let other = ExecutionId::from_uuid(uuid::Uuid::from_u128(2));
        assert!(part.history(other).unwrap().is_empty());
    }

    #[tokio::test]
    async fn read_back_refuses_a_changed_segment() {
        let archiver = MemoryArchiver::default();
        let key = store(&archiver, &[ROW_A, ROW_B]).await;
        let seg = key.replace("manifest.json", "segment-000002.jsonl");
        archiver
            .objects
            .lock()
            .unwrap()
            .insert(seg, format!("{ROW_A}\n").into_bytes());
        let err = read_back(&archiver, &key).await.unwrap_err();
        assert!(matches!(err, ReadBackError::Corrupt(_)), "{err}");
    }

    #[tokio::test]
    async fn read_back_reports_a_missing_manifest_and_a_missing_segment() {
        let archiver = MemoryArchiver::default();
        let err = read_back(&archiver, "harvest-partitions/none/manifest.json")
            .await
            .unwrap_err();
        assert!(matches!(err, ReadBackError::Missing(_)), "{err}");

        let key = store(&archiver, &[ROW_A, ROW_B]).await;
        let seg = key.replace("manifest.json", "segment-000001.jsonl");
        archiver.objects.lock().unwrap().remove(&seg);
        let err = read_back(&archiver, &key).await.unwrap_err();
        assert!(matches!(err, ReadBackError::Missing(_)), "{err}");
    }

    #[tokio::test]
    async fn read_back_refuses_an_unknown_format() {
        let archiver = MemoryArchiver::default();
        let key = store(&archiver, &[ROW_A]).await;
        let mut manifest: PartitionManifest =
            serde_json::from_slice(&archiver.objects.lock().unwrap()[&key]).unwrap();
        manifest.format = FORMAT_VERSION + 1;
        archiver
            .objects
            .lock()
            .unwrap()
            .insert(key.clone(), serde_json::to_vec(&manifest).unwrap());
        let err = read_back(&archiver, &key).await.unwrap_err();
        assert!(matches!(err, ReadBackError::Corrupt(_)), "{err}");
    }

    #[tokio::test]
    async fn the_directory_archiver_round_trips_and_reports_a_missing_key() {
        let dir = tempfile::tempdir().unwrap();
        let archiver = DirectoryPartitionArchiver::new(dir.path().join("cold"));
        archiver.put("a/b/c.json", b"one".to_vec()).await.unwrap();
        archiver.put("a/b/c.json", b"two".to_vec()).await.unwrap();
        assert_eq!(archiver.get("a/b/c.json").await.unwrap(), Some(b"two".to_vec()));
        assert_eq!(archiver.get("a/b/none.json").await.unwrap(), None);
        assert_eq!(std::fs::read(dir.path().join("cold/a/b/c.json")).unwrap(), b"two");
    }

    #[tokio::test]
    async fn the_directory_archiver_refuses_a_key_outside_its_root() {
        let dir = tempfile::tempdir().unwrap();
        let archiver = DirectoryPartitionArchiver::new(dir.path());
        for key in ["../x", "a/../../x", "/etc/x", "a//b", "a/./b", "", "a\\b", "a/b c"] {
            assert!(archiver.put(key, b"x".to_vec()).await.is_err(), "put {key:?}");
            assert!(archiver.get(key).await.is_err(), "get {key:?}");
        }
    }
}
