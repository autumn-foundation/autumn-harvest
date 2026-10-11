//! Export of aged `harvest_events` partitions to object storage (issue #2009).
//!
//! The partition sweep drops a closed cohort partition when no live run owns
//! a row in it. With a [`PartitionArchiver`] set, the sweep first exports the
//! partition, reads the export back, and checks it. The drop then computes a
//! row checksum under its `SHARE` lock. It drops only when the checksum and
//! the row count match the manifest. See `DESIGN-2009.md`.
//!
//! # Guards
//!
//! - **The marker.** A sweep with an archiver writes one row to
//!   `harvest_partition_export`. A later sweep on that shard with no
//!   archiver drops nothing.
//! - **The lock.** One process at a time exports a shard. The others report
//!   the shard as busy.
//! - **Reuse.** A failed drop leaves its export in place. The next pass uses
//!   it again when it still reads back clean.
//!
//! # Layout
//!
//! Each export is a set of segments and one manifest under one prefix:
//!
//! ```text
//! harvest-partitions/shard-<id>/<partition>/<lower>_<upper>/segment-000001-<sha256:16>.jsonl
//! harvest-partitions/shard-<id>/<partition>/<lower>_<upper>/manifest-<sha256:16>.json
//! harvest-partitions/shard-<id>/<partition>/<lower>_<upper>/dropped.json
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
///
/// Give each deployment its own bucket prefix or root. Two deployments that
/// share one namespace produce the same keys for `shard-0`.
pub trait PartitionArchiver: Send + Sync + 'static {
    /// Store `bytes` at `key`. Replace an object that is already there.
    ///
    /// The object must be durable when this returns `Ok`. The drop can
    /// commit right after it. A call that times out can still land later.
    /// Segment keys name their content, so a late segment is harmless.
    fn put<'a>(&'a self, key: &'a str, bytes: Vec<u8>) -> ArchiveIo<'a, ()>;

    /// Return the bytes at `key`, or `None` when no object is there.
    fn get<'a>(&'a self, key: &'a str) -> ArchiveIo<'a, Option<Vec<u8>>>;
}

/// The manifest format this build writes and reads.
pub const FORMAT_VERSION: u32 = 1;

/// The first path part of every key.
pub const KEY_ROOT: &str = "harvest-partitions";

/// A segment closes when it holds this many rows.
#[cfg_attr(not(feature = "db"), allow(dead_code))]
pub(crate) const SEGMENT_MAX_ROWS: usize = 10_000;

/// A segment closes before a row that would take it past this many bytes.
/// A single larger row gets a segment of its own.
#[cfg_attr(not(feature = "db"), allow(dead_code))]
pub(crate) const SEGMENT_MAX_BYTES: usize = 8 * 1024 * 1024;

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
    format!(
        "{KEY_ROOT}/shard-{shard_id}/{partition}/{}_{}",
        key_bound(lower),
        key_bound(Some(upper))
    )
}

/// One cohort bound as a key part. `MINVALUE` is `min`.
///
/// A cohort bound is whole seconds. The legacy upper bound is the conversion
/// instant, so it keeps its microseconds.
fn key_bound(bound: Option<DateTime<Utc>>) -> String {
    use chrono::Timelike as _;
    bound.map_or_else(
        || "min".to_string(),
        |ts| {
            if ts.nanosecond() == 0 {
                ts.format("%Y%m%dT%H%M%SZ").to_string()
            } else {
                ts.format("%Y%m%dT%H%M%S%.6fZ").to_string()
            }
        },
    )
}

/// The key of a manifest under `prefix`, with the first 16 hex digits of
/// its whole-partition SHA-256.
///
/// The hash in the key makes a manifest key name its content. A late upload
/// from a timed-out attempt then writes its own key. It cannot replace the
/// manifest of the export that the drop checked.
#[must_use]
pub fn manifest_key(prefix: &str, sha256: &str) -> String {
    let short = sha256.get(..16).unwrap_or(sha256);
    format!("{prefix}/manifest-{short}.json")
}

/// The key of the drop record under `prefix`.
///
/// It names the manifest of the export that the drop checks. The sweep writes
/// it before each drop attempt, and a failed write keeps the partition. So a
/// dropped partition always has a record. While the partition still exists,
/// the drop has not happened yet.
#[must_use]
pub fn dropped_key(prefix: &str) -> String {
    format!("{prefix}/dropped.json")
}

/// The key of the reuse hint under `prefix`.
///
/// Each verified export writes it. The next pass reads it to find an export
/// to use again. A stale hint costs one new export, not data, because the
/// drop checks the partition against the manifest under its lock.
#[cfg_attr(not(feature = "db"), allow(dead_code))]
fn latest_key(prefix: &str) -> String {
    format!("{prefix}/latest.json")
}

/// The body of a drop record or a reuse hint.
#[derive(Debug, Serialize, Deserialize)]
struct Pointer {
    /// A manifest key.
    manifest: String,
}

/// The manifest key that the drop record under `prefix` names.
///
/// Returns `None` when no drop record is there, so the partition was not
/// dropped through an export. `SweepOutcome::exported` also names the key.
///
/// # Errors
///
/// [`ReadBackError`] when the backend fails or the record does not parse.
pub async fn find_dropped(
    archiver: &dyn PartitionArchiver,
    prefix: &str,
) -> Result<Option<String>, ReadBackError> {
    let key = dropped_key(prefix);
    match fetch(archiver, &key, None).await {
        Ok(raw) => serde_json::from_slice::<Pointer>(&raw)
            .map(|p| Some(p.manifest))
            .map_err(|e| ReadBackError::Corrupt(format!("{key}: {e}"))),
        Err(ReadBackError::Missing(_)) => Ok(None),
        Err(e) => Err(e),
    }
}

/// The key of segment `n` under `prefix`, with the first 16 hex digits of
/// its SHA-256. Segments count from 1.
///
/// The hash in the key makes a segment key name its content. A late upload
/// from a slow or timed-out attempt then writes its own key. It cannot
/// replace a segment that a finished export names.
#[must_use]
pub fn segment_key(prefix: &str, n: usize, sha256: &str) -> String {
    let short = sha256.get(..16).unwrap_or(sha256);
    format!("{prefix}/segment-{n:06}-{short}.jsonl")
}

/// One segment in a [`PartitionManifest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
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
#[non_exhaustive]
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
    /// The sum of the row hashes, in decimal. See [`row_hash`].
    ///
    /// Postgres computes the same sum in one statement under the drop lock.
    /// The order of the rows does not change it.
    pub row_checksum: String,
    /// The segments, in order.
    pub segments: Vec<SegmentEntry>,
}

impl PartitionManifest {
    /// The key prefix of this export.
    #[must_use]
    pub fn prefix(&self) -> String {
        archive_prefix(self.shard_id, &self.partition, self.lower, self.upper)
    }

    /// The key of this manifest.
    #[must_use]
    pub fn key(&self) -> String {
        manifest_key(&self.prefix(), &self.sha256)
    }
}

/// The hash of one row line: the first 8 bytes of its SHA-256, as a
/// big-endian `i64`.
///
/// Postgres computes the same value with
/// `('x' || encode(substring(sha256(...) FROM 1 FOR 8), 'hex'))::bit(64)::bigint`.
#[must_use]
pub fn row_hash(line: &str) -> i64 {
    let digest = Sha256::digest(line.as_bytes());
    let mut head = [0_u8; 8];
    head.copy_from_slice(&digest[..8]);
    i64::from_be_bytes(head)
}

/// A streaming hash and checksum over row lines.
#[derive(Debug, Clone, Default)]
pub(crate) struct RowDigest {
    hasher: Sha256,
    rows: u64,
    checksum: i128,
}

impl RowDigest {
    /// Add one row line. The hash adds the line feed.
    pub(crate) fn push(&mut self, line: &str) {
        self.hasher.update(line.as_bytes());
        self.hasher.update(b"\n");
        self.rows += 1;
        self.checksum += i128::from(row_hash(line));
    }

    /// Rows added so far.
    pub(crate) const fn rows(&self) -> u64 {
        self.rows
    }

    /// The lowercase hex SHA-256 of every line with its line feed, and the
    /// decimal sum of the row hashes.
    pub(crate) fn finish(self) -> (String, String) {
        (hex(&self.hasher.finalize()), self.checksum.to_string())
    }
}

/// Splits row lines into segments.
#[derive(Debug)]
#[cfg_attr(not(feature = "db"), allow(dead_code))]
pub(crate) struct SegmentWriter {
    max_rows: usize,
    max_bytes: usize,
    buf: Vec<u8>,
    rows: usize,
}

#[cfg_attr(not(feature = "db"), allow(dead_code))]
impl SegmentWriter {
    /// A writer with the given limits. A zero limit counts as 1.
    pub(crate) fn new(max_rows: usize, max_bytes: usize) -> Self {
        Self {
            max_rows: max_rows.max(1),
            max_bytes: max_bytes.max(1),
            buf: Vec::new(),
            rows: 0,
        }
    }

    /// Add one row line. Returns the closed segment and its row count when
    /// this line starts a new one.
    pub(crate) fn push(&mut self, line: &str) -> Option<(Vec<u8>, u64)> {
        let full = self.rows >= self.max_rows || self.buf.len() + line.len() + 1 > self.max_bytes;
        let closed = if full { self.finish() } else { None };
        self.buf.extend_from_slice(line.as_bytes());
        self.buf.push(b'\n');
        self.rows += 1;
        closed
    }

    /// Return the open segment, if it holds a row.
    pub(crate) fn finish(&mut self) -> Option<(Vec<u8>, u64)> {
        if self.rows == 0 {
            return None;
        }
        let rows = self.rows as u64;
        self.rows = 0;
        Some((std::mem::take(&mut self.buf), rows))
    }
}

/// Lowercase hex SHA-256 of `bytes`.
pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::with_capacity(64), |mut out, b| {
        let _ = write!(out, "{b:02x}");
        out
    })
}

/// One archived `harvest_events` row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
#[non_exhaustive]
pub struct ArchivedPartition {
    /// The manifest.
    pub manifest: PartitionManifest,
    /// The rows, in key order.
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
        self.run_rows(exec_id)
            .map(|row| serde_json::from_value(row.event_data.clone()))
            .collect()
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
        self.run_rows(exec_id)
            .map(|row| codecs.decode_event(row.event_data.clone()))
            .collect()
    }

    /// The rows of one run, in `event_id` order.
    fn run_rows(&self, exec_id: ExecutionId) -> impl Iterator<Item = &ArchivedEventRow> {
        let mut rows: Vec<&ArchivedEventRow> = self
            .rows
            .iter()
            .filter(|row| row.workflow_exec_id == exec_id.as_uuid())
            .collect();
        rows.sort_by_key(|row| row.event_id);
        rows.into_iter()
    }
}

/// Why [`read_back`] failed.
#[derive(Debug, thiserror::Error)]
pub enum ReadBackError {
    /// An object is missing.
    #[error("archive object missing: {0}")]
    Missing(String),
    /// The backend call failed.
    #[error("archive backend error at {key}: {source}")]
    Backend {
        /// The key of the call.
        key: String,
        /// The backend error.
        #[source]
        source: ArchiveError,
    },
    /// The backend call ran past its time limit.
    #[error("archive backend call timed out at {0}")]
    TimedOut(String),
    /// An object does not match its manifest, or does not parse.
    #[error("archive object corrupt: {0}")]
    Corrupt(String),
}

/// Read a partition export back and check every hash.
///
/// It checks each segment against the manifest, then the whole-partition
/// hash, the row count and the row checksum. A backend call has no time
/// limit here. Wrap the call in `tokio::time::timeout` to set one.
///
/// # Errors
///
/// [`ReadBackError`] when an object is missing, the backend fails, or an
/// object does not match the manifest.
pub async fn read_back(
    archiver: &dyn PartitionArchiver,
    manifest_key: &str,
) -> Result<ArchivedPartition, ReadBackError> {
    read_back_inner(archiver, manifest_key, None, true, None).await
}

/// The body of [`read_back`].
///
/// `keep_rows` false checks every row but keeps none, so memory stays at one
/// segment. `progress` ticks once per segment.
async fn read_back_inner(
    archiver: &dyn PartitionArchiver,
    manifest_key: &str,
    io_timeout: Option<std::time::Duration>,
    keep_rows: bool,
    mut progress: Option<&mut (dyn FnMut() + Send)>,
) -> Result<ArchivedPartition, ReadBackError> {
    let raw = fetch(archiver, manifest_key, io_timeout).await?;
    let manifest: PartitionManifest = serde_json::from_slice(&raw)
        .map_err(|e| ReadBackError::Corrupt(format!("{manifest_key}: {e}")))?;
    if manifest.format != FORMAT_VERSION {
        return Err(ReadBackError::Corrupt(format!(
            "{manifest_key}: format {} is not {FORMAT_VERSION}",
            manifest.format
        )));
    }
    // A manifest copied to another key, or a segment outside its prefix,
    // would read back another partition's rows with no error.
    let prefix = format!("{}/", manifest.prefix());
    if manifest.key() != manifest_key
        || manifest
            .segments
            .iter()
            .any(|s| !s.key.starts_with(&prefix))
    {
        return Err(ReadBackError::Corrupt(format!(
            "{manifest_key}: the manifest names another partition"
        )));
    }
    let mut total = Sha256::new();
    let mut digest = RowDigest::default();
    let mut rows = Vec::new();
    for seg in &manifest.segments {
        let bytes = fetch(archiver, &seg.key, io_timeout).await?;
        if bytes.len() as u64 != seg.bytes || sha256_hex(&bytes) != seg.sha256 {
            return Err(ReadBackError::Corrupt(format!(
                "{}: the bytes do not match the manifest",
                seg.key
            )));
        }
        total.update(&bytes);
        let text = std::str::from_utf8(&bytes)
            .map_err(|e| ReadBackError::Corrupt(format!("{}: {e}", seg.key)))?;
        let before = digest.rows();
        for line in text.split_terminator('\n') {
            let row: ArchivedEventRow = serde_json::from_str(line)
                .map_err(|e| ReadBackError::Corrupt(format!("{}: {e}", seg.key)))?;
            digest.push(line);
            if keep_rows {
                rows.push(row);
            }
        }
        if digest.rows() - before != seg.rows {
            return Err(ReadBackError::Corrupt(format!(
                "{}: the row count does not match the manifest",
                seg.key
            )));
        }
        if let Some(cb) = progress.as_mut() {
            cb();
        }
    }
    let row_count = digest.rows();
    let (_, row_checksum) = digest.finish();
    if row_count != manifest.row_count
        || hex(&total.finalize()) != manifest.sha256
        || row_checksum != manifest.row_checksum
    {
        return Err(ReadBackError::Corrupt(format!(
            "{manifest_key}: the segments do not match the manifest"
        )));
    }
    Ok(ArchivedPartition { manifest, rows })
}

async fn fetch(
    archiver: &dyn PartitionArchiver,
    key: &str,
    io_timeout: Option<std::time::Duration>,
) -> Result<Vec<u8>, ReadBackError> {
    let got = match io_timeout {
        Some(limit) => tokio::time::timeout(limit, archiver.get(key))
            .await
            .map_err(|_| ReadBackError::TimedOut(key.to_string()))?,
        None => archiver.get(key).await,
    };
    got.map_err(|source| ReadBackError::Backend {
        key: key.to_string(),
        source,
    })?
    .ok_or_else(|| ReadBackError::Missing(key.to_string()))
}

/// A [`PartitionArchiver`] that writes each key as a file under a root
/// directory.
///
/// A write goes to a temporary file first. The file is synced, renamed into
/// place, and then its directory is synced. The parent of each directory
/// that the write creates is synced too. So a crash cannot lose a write that
/// returned `Ok`. A key path part must match `[A-Za-z0-9._-]+` and must
/// not be `.` or `..`.
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

    /// The file path of `key`, or an error for a key outside the root.
    fn path_of(&self, key: &str) -> Result<PathBuf, ArchiveError> {
        let mut path = self.root.clone();
        for part in key.split('/') {
            let valid = !part.is_empty()
                && part != "."
                && part != ".."
                && part
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
            if !valid {
                return Err(format!("invalid archive key {key:?}").into());
            }
            path.push(part);
        }
        Ok(path)
    }
}

/// Write `bytes` to a temporary file next to `path`, sync it, rename it, and
/// sync the directories.
///
/// `std::fs`, not `tokio::fs`: loom and shuttle builds compile `tokio::fs`
/// out. The caller runs this on the blocking pool.
fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write as _;
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    // A new directory's entry lives in its parent. List the directories that
    // `create_dir_all` makes, so their parents can be synced after the write.
    let mut created = Vec::new();
    let mut probe = dir;
    while let Err(e) = std::fs::metadata(probe) {
        if e.kind() != std::io::ErrorKind::NotFound {
            return Err(e);
        }
        created.push(probe.to_path_buf());
        match probe.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => probe = parent,
            _ => break,
        }
    }
    std::fs::create_dir_all(dir)?;
    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("object");
    let tmp = dir.join(format!(".{name}.{}.tmp", uuid::Uuid::new_v4().simple()));
    let written = (|| {
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&tmp, path)?;
        sync_dir(dir)?;
        for new_dir in &created {
            if let Some(parent) = new_dir.parent() {
                sync_dir(parent)?;
            }
        }
        Ok(())
    })();
    if written.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    written
}

/// Sync a directory, so a rename in it survives a crash. A no-op off Unix,
/// where a directory cannot be opened for a sync.
fn sync_dir(dir: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    std::fs::File::open(dir)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
}

/// Read the file at `path`, or `None` when it does not exist.
fn read_file(path: &Path) -> std::io::Result<Option<Vec<u8>>> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// Run blocking file I/O on the blocking pool, off the async workers.
async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> std::io::Result<T> + Send + 'static,
) -> Result<T, ArchiveError> {
    Ok(tokio::task::spawn_blocking(work).await??)
}

impl PartitionArchiver for DirectoryPartitionArchiver {
    fn put<'a>(&'a self, key: &'a str, bytes: Vec<u8>) -> ArchiveIo<'a, ()> {
        let path = self.path_of(key);
        Box::pin(async move {
            let path = path?;
            blocking(move || write_atomic(&path, &bytes)).await
        })
    }

    fn get<'a>(&'a self, key: &'a str) -> ArchiveIo<'a, Option<Vec<u8>>> {
        let path = self.path_of(key);
        Box::pin(async move {
            let path = path?;
            blocking(move || read_file(&path)).await
        })
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
    /// The most new exports one pass makes.
    ///
    /// An export reads, uploads and reads back a whole partition, inside the
    /// fenced pass. This bound keeps one pass short. A pass that reaches it
    /// reports `truncated`, and the next pass goes on. A reuse of an earlier
    /// export does not count.
    pub max_exports_per_pass: usize,
}

impl std::fmt::Debug for PartitionExport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionExport")
            .field("shard_id", &self.shard_id)
            .field("io_timeout", &self.io_timeout)
            .field("max_exports_per_pass", &self.max_exports_per_pass)
            .finish_non_exhaustive()
    }
}

impl PartitionExport {
    /// The default for [`Self::max_exports_per_pass`].
    pub const DEFAULT_MAX_EXPORTS_PER_PASS: usize = 4;

    /// An export to `archiver` for `shard_id`, with a 30 s call limit and
    /// [`Self::DEFAULT_MAX_EXPORTS_PER_PASS`].
    #[must_use]
    pub fn new(archiver: std::sync::Arc<dyn PartitionArchiver>, shard_id: i32) -> Self {
        Self {
            archiver,
            shard_id,
            io_timeout: std::time::Duration::from_secs(30),
            max_exports_per_pass: Self::DEFAULT_MAX_EXPORTS_PER_PASS,
        }
    }

    /// Set the limit on each backend call.
    #[must_use]
    pub const fn with_io_timeout(mut self, io_timeout: std::time::Duration) -> Self {
        self.io_timeout = io_timeout;
        self
    }

    /// Set the most new exports one pass makes. Zero counts as 1.
    #[must_use]
    pub const fn with_max_exports_per_pass(mut self, max: usize) -> Self {
        self.max_exports_per_pass = if max == 0 { 1 } else { max };
        self
    }
}

// ── Export and the drop check (database) ──────────────────────────────────

/// Rows one export query reads. Small, because one row can be large.
#[cfg(feature = "db")]
const PAGE_ROWS: usize = 256;

#[cfg(feature = "db")]
#[derive(diesel::QueryableByName)]
struct ExportRow {
    /// The export line, `to_jsonb(row)::text`.
    #[diesel(sql_type = diesel::sql_types::Text)]
    v: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    id: i64,
    /// The cohort as text. A legacy row holds `-infinity`, which `chrono`
    /// cannot hold.
    #[diesel(sql_type = diesel::sql_types::Text)]
    c: String,
}

/// The position after the last row read, on the key `(id, cohort)`.
///
/// The keyset uses the whole primary key. A keyset on `id` alone would skip a
/// second row with the same `id`.
#[cfg(feature = "db")]
type Keyset = Option<(i64, String)>;

/// Read the next page of export lines from `table`, an already quoted name.
///
/// The caller must have set `TimeZone` to `UTC` in the open transaction.
/// `to_jsonb` writes a `timestamptz` in the session zone, so the zone is part
/// of each line.
#[cfg(feature = "db")]
async fn read_page(
    conn: &mut diesel_async::AsyncPgConnection,
    table: &str,
    after: &Keyset,
) -> crate::error::HarvestResult<Vec<ExportRow>> {
    use diesel::sql_types::{BigInt, Text};
    use diesel_async::RunQueryDsl as _;
    let select = format!("SELECT to_jsonb(e)::text AS v, e.id, e.cohort::text AS c FROM {table} e");
    let order = format!("ORDER BY e.id, e.cohort LIMIT {PAGE_ROWS}");
    let rows = match after {
        None => {
            diesel::sql_query(format!("{select} {order}"))
                .load::<ExportRow>(conn)
                .await
        }
        Some((id, cohort)) => {
            diesel::sql_query(format!(
                "{select} WHERE (e.id, e.cohort) > ($1, $2::timestamptz) {order}"
            ))
            .bind::<BigInt, _>(*id)
            .bind::<Text, _>(cohort.clone())
            .load::<ExportRow>(conn)
            .await
        }
    };
    rows.map_err(crate::error::database_error)
}

#[cfg(feature = "db")]
#[derive(diesel::QueryableByName)]
struct ChecksumRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n: i64,
    #[diesel(sql_type = diesel::sql_types::Text)]
    s: String,
}

/// The row count and row checksum of partition `name`, in one statement.
///
/// The drop calls this under the `SHARE` lock, so no row can change during
/// the scan. It is one statement, so the caller's `statement_timeout` bounds
/// the whole scan. No row leaves the server.
///
/// # Errors
///
/// [`crate::error::HarvestError::Database`] on a query failure, a
/// `statement_timeout` included.
#[cfg(feature = "db")]
pub(crate) async fn locked_checksum(
    conn: &mut diesel_async::AsyncPgConnection,
    name: &str,
) -> crate::error::HarvestResult<(u64, String)> {
    use diesel_async::RunQueryDsl as _;
    crate::partition::exec(conn, "SET LOCAL TimeZone = 'UTC'").await?;
    let sql = format!(
        "SELECT count(*)::bigint AS n,
                COALESCE(sum(('x' || encode(substring(
                    sha256(convert_to(to_jsonb(e)::text, 'UTF8')) FROM 1 FOR 8), 'hex'))
                    ::bit(64)::bigint), 0)::text AS s
           FROM {} e",
        crate::partition::quote_ident(name)
    );
    let row = diesel::sql_query(sql)
        .get_result::<ChecksumRow>(conn)
        .await
        .map_err(crate::error::database_error)?;
    Ok((u64::try_from(row.n).unwrap_or(0), row.s))
}

/// The session advisory lock that lets one process at a time export a shard.
///
/// Every runner sweeps every shard. Without this lock, two processes export
/// the same partition at once, and each uploads the whole partition. An
/// exporter holds it exclusive. A sweep with no archiver holds it shared. The
/// key shows in `pg_locks` as `objid` and `classid` of an `advisory` lock.
pub const EXPORT_LOCK_KEY: i64 = 0x4856_5354_2009_0001;

/// What an applying sweep may do with a droppable partition (issue #2009).
///
/// Every applying sweep takes the export lock. An exporting sweep takes it
/// exclusive, and only then writes the marker. A sweep with no archiver takes
/// it shared, and only then reads the marker. So no sweep with no archiver can
/// decide that a shard has no marker while an exporter turns the marker on.
#[cfg(feature = "db")]
#[derive(Debug)]
pub(crate) enum ExportGate<'a> {
    /// A read-only pass. It holds no lock and drops nothing.
    Off,
    /// A read-only pass on a shard that holds the marker. It holds no lock.
    /// It reports a droppable partition as `export required`, the way a
    /// sweep with no archiver acts on it.
    OffMarked,
    /// No archiver and no marker: drop as before. Holds the lock shared.
    Plain,
    /// No archiver, but the shard holds the marker. Holds the lock shared.
    Required,
    /// Another process holds the lock in a conflicting mode. Holds nothing.
    Busy,
    /// This sweep has an archiver. Holds the lock exclusive.
    Active(&'a PartitionExport),
}

#[cfg(feature = "db")]
#[derive(diesel::QueryableByName)]
struct FlagRow {
    #[diesel(sql_type = diesel::sql_types::Bool)]
    v: bool,
}

#[cfg(feature = "db")]
async fn flag(
    conn: &mut diesel_async::AsyncPgConnection,
    sql: &str,
) -> crate::error::HarvestResult<bool> {
    use diesel_async::RunQueryDsl as _;
    diesel::sql_query(sql)
        .get_result::<FlagRow>(conn)
        .await
        .map(|r| r.v)
        .map_err(crate::error::database_error)
}

/// Whether the shard holds the export marker. A database without the marker
/// table holds none.
#[cfg(feature = "db")]
async fn marked(conn: &mut diesel_async::AsyncPgConnection) -> crate::error::HarvestResult<bool> {
    Ok(flag(
        conn,
        "SELECT to_regclass('harvest_partition_export') IS NOT NULL AS v",
    )
    .await?
        && flag(
            conn,
            "SELECT EXISTS (SELECT 1 FROM harvest_partition_export) AS v",
        )
        .await?)
}

#[cfg(feature = "db")]
impl<'a> ExportGate<'a> {
    /// Resolve the gate for one read-only pass. It takes no lock.
    ///
    /// # Errors
    ///
    /// [`crate::error::HarvestError::Database`] on a query failure.
    pub(crate) async fn read_only(
        conn: &mut diesel_async::AsyncPgConnection,
    ) -> crate::error::HarvestResult<Self> {
        Ok(if marked(conn).await? {
            Self::OffMarked
        } else {
            Self::Off
        })
    }

    /// Whether a read-only pass must report droppable partitions as
    /// `export required`.
    pub(crate) const fn marked(&self) -> bool {
        matches!(self, Self::OffMarked)
    }

    /// Resolve the gate for one applying pass. It never waits for the lock.
    ///
    /// # Errors
    ///
    /// [`crate::error::HarvestError::Database`] on a query failure. With an
    /// archiver, a database without the marker table also fails, so no
    /// export runs where the marker cannot protect it. The lock is released
    /// before an error returns.
    pub(crate) async fn resolve(
        conn: &mut diesel_async::AsyncPgConnection,
        export: Option<&'a PartitionExport>,
    ) -> crate::error::HarvestResult<Self> {
        let Some(export) = export else {
            let shared = flag(
                conn,
                &format!("SELECT pg_try_advisory_lock_shared({EXPORT_LOCK_KEY}) AS v"),
            )
            .await?;
            if !shared {
                return Ok(Self::Busy);
            }
            let gate = match marked(conn).await {
                Ok(true) => Self::Required,
                Ok(false) => Self::Plain,
                Err(e) => {
                    Self::Plain.release(conn).await;
                    return Err(e);
                }
            };
            return Ok(gate);
        };
        let exclusive = flag(
            conn,
            &format!("SELECT pg_try_advisory_lock({EXPORT_LOCK_KEY}) AS v"),
        )
        .await?;
        if !exclusive {
            return Ok(Self::Busy);
        }
        let gate = Self::Active(export);
        if let Err(e) = crate::partition::exec(
            conn,
            "INSERT INTO harvest_partition_export (singleton) VALUES (TRUE) ON CONFLICT DO NOTHING",
        )
        .await
        {
            gate.release(conn).await;
            return Err(e);
        }
        Ok(gate)
    }

    /// The blocked reason when this gate allows no drop.
    pub(crate) const fn blocks_drops(&self) -> Option<&'static str> {
        match self {
            Self::Required => Some(crate::partition::EXPORT_REQUIRED_REASON),
            Self::Busy => Some(crate::partition::EXPORT_BUSY_REASON),
            Self::Off | Self::OffMarked | Self::Plain | Self::Active(_) => None,
        }
    }

    /// Whether the straggler `DELETE` may run. It removes rows that no
    /// export holds, so it runs only on a shard with no export in use.
    pub(crate) const fn allows_straggler_delete(&self) -> bool {
        matches!(self, Self::Off | Self::Plain)
    }

    /// Release the export lock in the mode this gate holds it.
    ///
    /// A failure is only logged. A broken connection ends its session, and
    /// the session end releases the lock.
    pub(crate) async fn release(&self, conn: &mut diesel_async::AsyncPgConnection) {
        let sql = match self {
            Self::Plain | Self::Required => {
                format!("SELECT pg_advisory_unlock_shared({EXPORT_LOCK_KEY}) AS v")
            }
            Self::Active(_) => format!("SELECT pg_advisory_unlock({EXPORT_LOCK_KEY}) AS v"),
            Self::Off | Self::OffMarked | Self::Busy => return,
        };
        if let Err(e) = flag(conn, &sql).await {
            tracing::warn!(error = %e, "could not release the partition export lock");
        }
    }
}

/// Run one backend call under the export's time limit.
#[cfg(feature = "db")]
async fn bounded<T>(export: &PartitionExport, call: ArchiveIo<'_, T>) -> Result<T, String> {
    match tokio::time::timeout(export.io_timeout, call).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(e)) => Err(e.to_string()),
        Err(_) => Err(format!(
            "the backend call timed out after {:?}",
            export.io_timeout
        )),
    }
}

/// Upload one closed segment and record it.
#[cfg(feature = "db")]
async fn upload_segment(
    export: &PartitionExport,
    prefix: &str,
    segments: &mut Vec<SegmentEntry>,
    (bytes, rows): (Vec<u8>, u64),
) -> Result<(), String> {
    let sha256 = sha256_hex(&bytes);
    let key = segment_key(prefix, segments.len() + 1, &sha256);
    let entry = SegmentEntry {
        key: key.clone(),
        rows,
        bytes: bytes.len() as u64,
        sha256,
    };
    bounded(export, export.archiver.put(&key, bytes)).await?;
    segments.push(entry);
    Ok(())
}

/// An earlier export of partition `part` that still reads back clean.
///
/// A failed drop leaves its export in place. The next pass can use it, and
/// the drop check under the lock still proves that it matches the partition.
/// Returns `None` when there is no such export, or when any check fails.
#[cfg(feature = "db")]
pub(crate) async fn reusable_export(
    export: &PartitionExport,
    part: &crate::partition::PartitionInfo,
    upper: DateTime<Utc>,
    progress: Option<&mut (dyn FnMut() + Send)>,
) -> Option<PartitionManifest> {
    let prefix = archive_prefix(export.shard_id, &part.name, part.lower, upper);
    let raw = bounded(export, export.archiver.get(&latest_key(&prefix)))
        .await
        .ok()??;
    let hint: Pointer = serde_json::from_slice(&raw).ok()?;
    // `read_back_inner` checks that the manifest names its own key. The
    // prefix check then ties it to this shard, partition and bounds.
    let back = read_back_inner(
        export.archiver.as_ref(),
        &hint.manifest,
        Some(export.io_timeout),
        false,
        progress,
    )
    .await
    .ok()?;
    (back.manifest.prefix() == prefix).then_some(back.manifest)
}

/// Write the drop record of `manifest`, before the drop.
///
/// The record goes first, so a dropped partition always has one. A failed
/// write keeps the partition. If the drop then does not happen, the record
/// names a verified export of a partition that still exists. The next
/// attempt writes it again.
///
/// # Errors
///
/// The reason the drop must wait.
#[cfg(feature = "db")]
pub(crate) async fn record_drop(
    export: &PartitionExport,
    manifest: &PartitionManifest,
) -> Result<(), String> {
    let key = dropped_key(&manifest.prefix());
    let body = Pointer {
        manifest: manifest.key(),
    };
    let bytes = serde_json::to_vec(&body).map_err(|e| e.to_string())?;
    bounded(export, export.archiver.put(&key, bytes))
        .await
        .map_err(|e| format!("{key}: {e}"))
}

/// Export partition `part`, then read the export back and check it.
///
/// Reads run in short transactions, one per page. No transaction stays open
/// across a backend call. The drop checks the partition again under its
/// lock, so a row that changes during the export blocks the drop.
///
/// Returns the manifest, or the reason the drop must wait. A failure is a
/// reason, not an error, so one bad partition cannot stop the pass.
#[cfg(feature = "db")]
pub(crate) async fn export_partition(
    conn: &mut diesel_async::AsyncPgConnection,
    export: &PartitionExport,
    part: &crate::partition::PartitionInfo,
    upper: DateTime<Utc>,
    mut progress: Option<&mut (dyn FnMut() + Send)>,
) -> Result<PartitionManifest, String> {
    use diesel_async::AsyncConnection as _;
    let prefix = archive_prefix(export.shard_id, &part.name, part.lower, upper);
    let table = crate::partition::quote_ident(&part.name);
    let mut writer = SegmentWriter::new(SEGMENT_MAX_ROWS, SEGMENT_MAX_BYTES);
    let mut digest = RowDigest::default();
    let mut segments = Vec::new();
    let mut after: Keyset = None;
    loop {
        let page = Box::pin(
            conn.transaction::<_, crate::error::HarvestError, _>(async |c| {
                crate::partition::exec(c, "SET LOCAL TimeZone = 'UTC'").await?;
                read_page(c, &table, &after).await
            }),
        )
        .await
        .map_err(|e| e.to_string())?;
        let last_page = page.len() < PAGE_ROWS;
        for row in page {
            digest.push(&row.v);
            if let Some(closed) = writer.push(&row.v) {
                upload_segment(export, &prefix, &mut segments, closed).await?;
                if let Some(cb) = progress.as_mut() {
                    cb();
                }
            }
            after = Some((row.id, row.c));
        }
        if last_page {
            break;
        }
    }
    if let Some(closed) = writer.finish() {
        upload_segment(export, &prefix, &mut segments, closed).await?;
    }
    let row_count = digest.rows();
    let (sha256, row_checksum) = digest.finish();
    let manifest = PartitionManifest {
        format: FORMAT_VERSION,
        shard_id: export.shard_id,
        partition: part.name.clone(),
        lower: part.lower,
        upper,
        exported_at: Utc::now(),
        row_count,
        sha256,
        row_checksum,
        segments,
    };
    let key = manifest.key();
    let bytes = serde_json::to_vec(&manifest).map_err(|e| e.to_string())?;
    bounded(export, export.archiver.put(&key, bytes)).await?;
    if let Some(cb) = progress.as_mut() {
        cb();
    }
    // The verify step: read every object back and check every hash.
    let back = read_back_inner(
        export.archiver.as_ref(),
        &key,
        Some(export.io_timeout),
        false,
        progress,
    )
    .await
    .map_err(|e| e.to_string())?;
    if back.manifest != manifest {
        return Err(format!("{key} differs from the upload"));
    }
    // The hint only helps the next pass find this export. A failed write
    // costs a new export then, so it does not fail this one.
    if let Ok(hint) = serde_json::to_vec(&Pointer {
        manifest: key.clone(),
    }) {
        let _ = bounded(export, export.archiver.put(&latest_key(&prefix), hint)).await;
    }
    Ok(manifest)
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
        let prefix = archive_prefix(
            0,
            "harvest_events_p_20260101",
            Some(ts(2026, 1, 1)),
            ts(2026, 1, 2),
        );
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
            let sha256 = sha256_hex(&bytes);
            let key = segment_key(&prefix, n + 1, &sha256);
            segments.push(SegmentEntry {
                key: key.clone(),
                rows: count,
                bytes: bytes.len() as u64,
                sha256,
            });
            archiver.put(&key, bytes).await.unwrap();
        }
        let row_count = digest.rows();
        let (sha256, row_checksum) = digest.finish();
        let manifest = PartitionManifest {
            format: FORMAT_VERSION,
            shard_id: 0,
            partition: "harvest_events_p_20260101".into(),
            lower: Some(ts(2026, 1, 1)),
            upper: ts(2026, 1, 2),
            exported_at: ts(2026, 1, 3),
            row_count,
            sha256,
            row_checksum,
            segments,
        };
        let key = manifest.key();
        assert!(key.starts_with(&prefix));
        archiver
            .put(&key, serde_json::to_vec(&manifest).unwrap())
            .await
            .unwrap();
        key
    }

    fn segment_keys(archiver: &MemoryArchiver, manifest_key: &str) -> Vec<String> {
        let raw = archiver.objects.lock().unwrap()[manifest_key].clone();
        let manifest: PartitionManifest = serde_json::from_slice(&raw).unwrap();
        manifest.segments.into_iter().map(|s| s.key).collect()
    }

    #[tokio::test]
    async fn exports_of_different_rows_have_different_manifest_keys() {
        // A late manifest from an older attempt then writes its own key and
        // cannot replace the export that a drop checked.
        let archiver = MemoryArchiver::default();
        let old = store(&archiver, &[ROW_A]).await;
        let new = store(&archiver, &[ROW_A, ROW_B]).await;
        assert_ne!(old, new);
        assert_eq!(read_back(&archiver, &old).await.unwrap().rows.len(), 1);
        assert_eq!(read_back(&archiver, &new).await.unwrap().rows.len(), 2);
    }

    #[tokio::test]
    async fn find_dropped_reads_the_drop_record() {
        let archiver = MemoryArchiver::default();
        let key = store(&archiver, &[ROW_A]).await;
        let prefix = key.rsplit_once('/').unwrap().0.to_string();
        assert_eq!(find_dropped(&archiver, &prefix).await.unwrap(), None);
        let record = serde_json::to_vec(&Pointer {
            manifest: key.clone(),
        })
        .unwrap();
        archiver.put(&dropped_key(&prefix), record).await.unwrap();
        assert_eq!(find_dropped(&archiver, &prefix).await.unwrap(), Some(key));
    }

    #[tokio::test]
    async fn read_back_refuses_a_manifest_under_another_key() {
        let archiver = MemoryArchiver::default();
        let key = store(&archiver, &[ROW_A]).await;
        let raw = archiver.objects.lock().unwrap()[&key].clone();
        let other = "harvest-partitions/shard-9/elsewhere/min_20260102T000000Z/manifest.json";
        archiver
            .objects
            .lock()
            .unwrap()
            .insert(other.to_string(), raw);
        let err = read_back(&archiver, other).await.unwrap_err();
        assert!(matches!(err, ReadBackError::Corrupt(_)), "{err}");
    }

    #[test]
    fn the_prefix_holds_the_shard_the_name_and_both_bounds() {
        let prefix = archive_prefix(
            3,
            "harvest_events_p_20260101",
            Some(ts(2026, 1, 1)),
            ts(2026, 1, 2),
        );
        assert_eq!(
            prefix,
            "harvest-partitions/shard-3/harvest_events_p_20260101/20260101T000000Z_20260102T000000Z"
        );
        let sha = sha256_hex(b"x");
        assert_eq!(
            manifest_key(&prefix, &sha),
            format!("{prefix}/manifest-{}.json", &sha[..16])
        );
        assert_eq!(dropped_key(&prefix), format!("{prefix}/dropped.json"));
        assert_eq!(
            segment_key(&prefix, 7, &sha),
            format!("{prefix}/segment-000007-{}.jsonl", &sha[..16])
        );
    }

    #[test]
    fn a_sub_second_bound_keeps_its_microseconds() {
        let cutover = ts(2026, 1, 1) + chrono::Duration::microseconds(1_500_250);
        let prefix = archive_prefix(0, "harvest_events_p_legacy", None, cutover);
        assert!(prefix.ends_with("/min_20260101T000001.500250Z"), "{prefix}");
    }

    #[test]
    fn the_row_hash_is_the_first_eight_digest_bytes_as_a_signed_integer() {
        // SHA-256("a") starts with ca978112ca1bbdca.
        let head = [0xca, 0x97, 0x81, 0x12, 0xca, 0x1b, 0xbd, 0xca];
        assert_eq!(row_hash("a"), i64::from_be_bytes(head));
        let mut digest = RowDigest::default();
        digest.push("a");
        digest.push("a");
        let (_, checksum) = digest.finish();
        assert_eq!(checksum, (2 * i128::from(row_hash("a"))).to_string());
    }

    #[test]
    fn a_minvalue_bound_is_min_and_a_new_upper_bound_gives_a_new_prefix() {
        let first = archive_prefix(0, "harvest_events_p_legacy", None, ts(2026, 1, 1));
        let again = archive_prefix(0, "harvest_events_p_legacy", None, ts(2026, 3, 1));
        assert!(first.ends_with("/min_20260101T000000Z"), "{first}");
        assert_ne!(
            first, again,
            "a new enable must not overwrite an old export"
        );
    }

    #[test]
    fn the_row_digest_is_the_hash_of_every_line_with_its_line_feed() {
        let mut digest = RowDigest::default();
        digest.push("a");
        digest.push("b");
        assert_eq!(digest.rows(), 2);
        assert_eq!(
            digest.finish().0,
            sha256_hex(
                b"a
b
"
            )
        );
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
        assert_eq!(sha256_hex(&all), digest.finish().0);
    }

    #[tokio::test]
    async fn read_back_returns_typed_rows_and_a_run_history() {
        let archiver = MemoryArchiver::default();
        let key = store(&archiver, &[ROW_A, ROW_B]).await;
        let part = read_back(&archiver, &key).await.expect("read back");
        assert_eq!(part.manifest.row_count, 2);
        assert_eq!(part.manifest.segments.len(), 2);
        assert_eq!(part.rows.len(), 2);
        assert_eq!(
            part.rows[1].extra.get("cohort"),
            Some(&serde_json::json!("-infinity"))
        );
        let exec = ExecutionId::from_uuid(uuid::Uuid::from_u128(1));
        let history = part.history(exec).expect("history parses");
        assert_eq!(history.len(), 2);
        assert!(matches!(
            history[0],
            WorkflowEvent::WorkflowCompleted { .. }
        ));
        assert!(matches!(history[1], WorkflowEvent::MarkerRecorded { .. }));
        let other = ExecutionId::from_uuid(uuid::Uuid::from_u128(2));
        assert!(part.history(other).unwrap().is_empty());
    }

    #[tokio::test]
    async fn read_back_refuses_a_changed_segment() {
        let archiver = MemoryArchiver::default();
        let key = store(&archiver, &[ROW_A, ROW_B]).await;
        let seg = segment_keys(&archiver, &key)[1].clone();
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
        let seg = segment_keys(&archiver, &key)[0].clone();
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
        archiver
            .put("x/y/z/new.json", b"deep".to_vec())
            .await
            .unwrap();
        assert_eq!(
            archiver.get("x/y/z/new.json").await.unwrap(),
            Some(b"deep".to_vec())
        );
        archiver.put("a/b/c.json", b"two".to_vec()).await.unwrap();
        assert_eq!(
            archiver.get("a/b/c.json").await.unwrap(),
            Some(b"two".to_vec())
        );
        assert_eq!(archiver.get("a/b/none.json").await.unwrap(), None);
        assert_eq!(
            std::fs::read(dir.path().join("cold/a/b/c.json")).unwrap(),
            b"two"
        );
    }

    #[tokio::test]
    async fn the_directory_archiver_refuses_a_key_outside_its_root() {
        let dir = tempfile::tempdir().unwrap();
        let archiver = DirectoryPartitionArchiver::new(dir.path());
        for key in [
            "../x",
            "a/../../x",
            "/etc/x",
            "a//b",
            "a/./b",
            "",
            "a\\b",
            "a/b c",
        ] {
            assert!(
                archiver.put(key, b"x".to_vec()).await.is_err(),
                "put {key:?}"
            );
            assert!(archiver.get(key).await.is_err(), "get {key:?}");
        }
    }
}
