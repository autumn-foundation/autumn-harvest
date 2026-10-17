//! File-backed stores for the agent: memory, delivery, the heartbeat
//! precheck, and the blobs of the response cache.
//!
//! Each store writes under the state directory. A write goes to a synced
//! scratch file and then a rename. So a crash or a power cut leaves the old
//! file or the new one, never half of one.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

use autumn_harvest::payload_store::{PayloadStore, PayloadStoreError, PayloadStoreFuture};
use autumn_harvest_agent::memory::{MemoryOp, apply_op};
use autumn_harvest_agent::{
    AgentError, BoxFuture, CacheIndex, CacheKey, Delivery, EditKey, ErrorKind, HeartbeatTask,
    MemoryBlock, MemoryScope, MemoryStore, Precheck, Report, ReportSource, SessionId,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::lab::Lab;

/// A file name for any key: its SHA-256 in hex.
pub(crate) fn hashed(key: &str) -> String {
    hex(&Sha256::digest(key.as_bytes()))
}

fn hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut text, b| {
            let _ = write!(text, "{b:02x}");
            text
        })
}

/// The stored state of one memory scope.
#[derive(Debug, Default, Serialize, Deserialize)]
struct ScopeFile {
    blocks: Vec<MemoryBlock>,
    /// Each applied key, with its error if the edit failed.
    applied: BTreeMap<String, Option<String>>,
}

/// A [`MemoryStore`] with one JSON file per scope.
///
/// The file holds the blocks and every applied [`EditKey`] with its first
/// result. A key that comes back after a crash changes nothing.
#[derive(Debug)]
pub struct FileMemoryStore {
    dir: PathBuf,
    lock: Mutex<()>,
}

impl FileMemoryStore {
    /// A store under `dir`.
    ///
    /// # Errors
    ///
    /// Returns an error when `dir` cannot be created.
    pub fn new(dir: impl Into<PathBuf>) -> Result<Self, String> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)
            .map_err(|e| format!("cannot create {}: {e}", dir.display()))?;
        Ok(Self {
            dir,
            lock: Mutex::new(()),
        })
    }

    fn path(&self, scope: &MemoryScope) -> PathBuf {
        self.dir.join(format!("{}.json", hashed(scope.as_str())))
    }

    fn read(&self, scope: &MemoryScope) -> Result<ScopeFile, AgentError> {
        match std::fs::read(self.path(scope)) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map_err(|e| AgentError::new(ErrorKind::Decode, format!("memory file: {e}"))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(ScopeFile {
                blocks: MemoryBlock::defaults(),
                applied: BTreeMap::new(),
            }),
            Err(e) => Err(AgentError::new(
                ErrorKind::Unavailable,
                format!("memory read: {e}"),
            )),
        }
    }

    fn apply_now(
        &self,
        scope: &MemoryScope,
        op: &MemoryOp,
        key: &EditKey,
    ) -> Result<MemoryBlock, AgentError> {
        let _guard = self.lock.lock().unwrap_or_else(PoisonError::into_inner);
        let mut file = self.read(scope)?;
        let tool_error = |message: &str| AgentError::new(ErrorKind::Tool, message);
        let current = |file: &ScopeFile| {
            file.blocks
                .iter()
                .find(|block| block.label == op.block())
                .cloned()
                .ok_or_else(|| tool_error(&format!("no memory block named {:?}", op.block())))
        };
        match file.applied.get(key.as_str()) {
            Some(Some(error)) => return Err(tool_error(error)),
            Some(None) => return current(&file),
            None => {}
        }
        let result = file
            .blocks
            .iter_mut()
            .find(|block| block.label == op.block())
            .map_or_else(
                || {
                    Err(tool_error(&format!(
                        "no memory block named {:?}",
                        op.block()
                    )))
                },
                |block| apply_op(block, op).map(|()| block.clone()),
            );
        file.applied.insert(
            key.as_str().to_string(),
            result.as_ref().err().map(|e| e.message().to_string()),
        );
        let bytes = serde_json::to_vec_pretty(&file)
            .map_err(|e| AgentError::new(ErrorKind::Decode, format!("memory file: {e}")))?;
        write_atomic(&self.path(scope), &bytes)
            .map_err(|e| AgentError::new(ErrorKind::Unavailable, e))?;
        result
    }
}

impl MemoryStore for FileMemoryStore {
    fn load<'a>(
        &'a self,
        scope: &'a MemoryScope,
    ) -> BoxFuture<'a, Result<Vec<MemoryBlock>, AgentError>> {
        let blocks = {
            let _guard = self.lock.lock().unwrap_or_else(PoisonError::into_inner);
            self.read(scope).map(|file| file.blocks)
        };
        Box::pin(std::future::ready(blocks))
    }

    fn apply<'a>(
        &'a self,
        scope: &'a MemoryScope,
        op: MemoryOp,
        key: &'a EditKey,
    ) -> BoxFuture<'a, Result<MemoryBlock, AgentError>> {
        Box::pin(std::future::ready(self.apply_now(scope, &op, key)))
    }
}

/// A [`Delivery`] that writes each report to `reports/<key>.md` in the
/// state directory of its lab. A second delivery of one key changes nothing.
#[derive(Debug, Clone)]
pub struct FileDelivery {
    labs: Arc<Vec<Lab>>,
}

impl FileDelivery {
    /// A delivery for `labs`.
    #[must_use]
    pub fn new(labs: Vec<Lab>) -> Self {
        Self {
            labs: Arc::new(labs),
        }
    }

    /// The report directory of `lab`.
    ///
    /// # Errors
    ///
    /// Returns an error when the directory cannot be created.
    pub fn dir(lab: &Lab) -> Result<PathBuf, String> {
        let dir = lab.state_dir()?.join("reports");
        std::fs::create_dir_all(&dir)
            .map_err(|e| format!("cannot create {}: {e}", dir.display()))?;
        Ok(dir)
    }

    fn write(&self, report: &Report) -> Result<(), AgentError> {
        let session = report.session_id.as_ref().map(SessionId::as_str);
        let lab = Lab::serving(&self.labs, session).ok_or_else(|| {
            AgentError::new(
                ErrorKind::Config,
                "the report names no lab that this process serves",
            )
        })?;
        let dir = Self::dir(lab).map_err(|e| AgentError::new(ErrorKind::Unavailable, e))?;
        // The key dedupes a retried delivery: the same report, the same file.
        let path = dir.join(format!("{}.md", hashed(&report.key())));
        if !path.exists() {
            let text = format!(
                "# autoresearch report\n\nrun: {}\nstop: {:?}\n\n{}\n",
                report.run_id.as_str(),
                report.stop,
                report.text.trim()
            );
            write_atomic(&path, text.as_bytes())
                .map_err(|e| AgentError::new(ErrorKind::Unavailable, e))?;
        }
        // A heartbeat speaks only to alert, so the alert holds these rows.
        // A retry marks them too, when the process died before the mark.
        if report.source == ReportSource::Heartbeat {
            mark_alerted(lab).map_err(|e| AgentError::new(ErrorKind::Unavailable, e))?;
        }
        Ok(())
    }
}

impl Delivery for FileDelivery {
    fn deliver<'a>(&'a self, report: &'a Report) -> BoxFuture<'a, Result<(), AgentError>> {
        Box::pin(std::future::ready(self.write(report)))
    }
}

/// A [`Precheck`] that runs a heartbeat tick only when the newest
/// [`CRASH_STREAK`] rows of `results.tsv` are crashes, and no alert holds the
/// newest row yet.
///
/// The check is a pure read, as the [`Precheck`] contract asks. The mark
/// moves only when [`FileDelivery`] delivers a heartbeat alert. So a tick
/// whose model call fails leaves the rows for the next tick.
#[derive(Debug, Clone)]
pub struct ResultsPrecheck {
    labs: Arc<Vec<Lab>>,
}

/// The crashes in a row that a heartbeat speaks up about.
pub const CRASH_STREAK: usize = 3;

/// The file in the state directory that holds the row count at the last
/// delivered heartbeat alert.
const ALERTED: &str = "heartbeat.alerted";

impl ResultsPrecheck {
    /// A precheck for `labs`.
    #[must_use]
    pub fn new(labs: Vec<Lab>) -> Self {
        Self {
            labs: Arc::new(labs),
        }
    }

    /// `true` when a new crash streak waits for an alert. An unreadable lab
    /// skips the tick.
    fn check(&self, task: &HeartbeatTask) -> bool {
        let session = task.session_id.as_ref().map(SessionId::as_str);
        let Some(lab) = Lab::serving(&self.labs, session) else {
            return false;
        };
        let (Ok(rows), Ok(state)) = (lab.rows(), lab.state_dir()) else {
            return false;
        };
        let alerted: usize = std::fs::read_to_string(state.join(ALERTED))
            .ok()
            .and_then(|text| text.trim().parse().ok())
            .unwrap_or(0);
        let streak = rows.len() >= CRASH_STREAK
            && rows
                .iter()
                .rev()
                .take(CRASH_STREAK)
                .all(|row| row.status == crate::rules::Status::Crash);
        streak && rows.len() > alerted
    }
}

/// Record that a heartbeat alert holds the rows of `lab` up to now.
fn mark_alerted(lab: &Lab) -> Result<(), String> {
    let rows = lab.rows()?.len();
    write_atomic(&lab.state_dir()?.join(ALERTED), rows.to_string().as_bytes())
}

impl Precheck for ResultsPrecheck {
    fn should_run<'a>(&'a self, task: &'a HeartbeatTask) -> BoxFuture<'a, bool> {
        Box::pin(std::future::ready(self.check(task)))
    }
}

/// A [`CacheIndex`] in one directory, so it outlives the process.
///
/// A model call whose result a crash lost runs again with the same request.
/// The index on disk then finds the stored answer. So the call is not paid
/// twice, and its answer does not change. Each entry is a file named by its
/// key. It holds an order number and the blob key. When the index is full, a
/// new entry evicts the oldest one.
#[derive(Debug)]
pub struct FileCacheIndex {
    dir: PathBuf,
    capacity: usize,
    /// The order number of the next entry.
    next: Mutex<u64>,
}

impl FileCacheIndex {
    /// An index in `dir` that holds at most `capacity` entries.
    ///
    /// # Errors
    ///
    /// Returns an error when `dir` cannot be created or read.
    pub fn new(dir: impl Into<PathBuf>, capacity: usize) -> Result<Self, String> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)
            .map_err(|e| format!("cannot create {}: {e}", dir.display()))?;
        let index = Self {
            dir,
            capacity: capacity.max(1),
            next: Mutex::new(0),
        };
        let next = index.entries()?.last().map_or(0, |(order, ..)| order + 1);
        *index.next.lock().unwrap_or_else(PoisonError::into_inner) = next;
        Ok(index)
    }

    /// Each entry, oldest first: its order number, its file and its blob.
    /// A scratch file of a write is not an entry.
    fn entries(&self) -> Result<Vec<(u64, PathBuf, String)>, String> {
        let list = std::fs::read_dir(&self.dir)
            .map_err(|e| format!("cannot read {}: {e}", self.dir.display()))?;
        let mut entries: Vec<_> = list
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| {
                        name.len() == 64 && name.bytes().all(|b| b.is_ascii_hexdigit())
                    })
            })
            .filter_map(|path| read_entry(&path).map(|(order, blob)| (order, path, blob)))
            .collect();
        entries.sort();
        Ok(entries)
    }

    fn get_now(&self, key: &CacheKey) -> Option<String> {
        read_entry(&self.dir.join(key.as_str())).map(|(_, blob)| blob)
    }

    fn put_now(&self, key: &CacheKey, blob: &str) -> Result<Vec<String>, String> {
        let mut next = self.next.lock().unwrap_or_else(PoisonError::into_inner);
        let path = self.dir.join(key.as_str());
        let mut dropped: Vec<String> = read_entry(&path)
            .map(|(_, old)| old)
            .filter(|old| old != blob)
            .into_iter()
            .collect();
        write_atomic(&path, format!("{}\n{blob}\n", *next).as_bytes())?;
        *next += 1;
        drop(next);
        let entries = self.entries()?;
        let extra = entries.len().saturating_sub(self.capacity);
        for (_, oldest, old) in entries.into_iter().take(extra) {
            // Another write can evict the same entry first.
            match std::fs::remove_file(&oldest) {
                Ok(()) => dropped.push(old),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(format!("cannot remove {}: {e}", oldest.display())),
            }
        }
        Ok(dropped)
    }
}

/// The order number and the blob key in the entry file at `path`.
fn read_entry(path: &Path) -> Option<(u64, String)> {
    let text = std::fs::read_to_string(path).ok()?;
    let (order, blob) = text.split_once('\n')?;
    Some((order.parse().ok()?, blob.trim().to_string()))
}

impl CacheIndex for FileCacheIndex {
    fn get<'a>(&'a self, key: &'a CacheKey) -> BoxFuture<'a, Result<Option<String>, AgentError>> {
        Box::pin(std::future::ready(Ok(self.get_now(key))))
    }

    fn put<'a>(
        &'a self,
        key: &'a CacheKey,
        blob: String,
    ) -> BoxFuture<'a, Result<Vec<String>, AgentError>> {
        let result = self
            .put_now(key, &blob)
            .map_err(|e| AgentError::new(ErrorKind::Unavailable, e));
        Box::pin(std::future::ready(result))
    }
}

/// A content-addressed [`PayloadStore`] in one directory, for the response
/// cache. The key is the SHA-256 of the bytes.
#[derive(Debug, Clone)]
pub struct DirPayloadStore {
    dir: PathBuf,
}

impl DirPayloadStore {
    /// A store under `dir`.
    ///
    /// # Errors
    ///
    /// Returns an error when `dir` cannot be created.
    pub fn new(dir: impl Into<PathBuf>) -> Result<Self, String> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)
            .map_err(|e| format!("cannot create {}: {e}", dir.display()))?;
        Ok(Self { dir })
    }

    /// The path of `key`. A key is 64 hex digits, so it cannot name a path
    /// outside the directory.
    fn path(&self, key: &str) -> Result<PathBuf, PayloadStoreError> {
        if key.len() == 64 && key.bytes().all(|b| b.is_ascii_hexdigit()) {
            Ok(self.dir.join(key))
        } else {
            Err(PayloadStoreError(format!("not a blob key: {key:?}")))
        }
    }
}

impl PayloadStore for DirPayloadStore {
    fn store_id(&self) -> &'static str {
        "autoresearch-dir"
    }

    fn put(&self, bytes: &[u8]) -> PayloadStoreFuture<'_, String> {
        let key = hex(&Sha256::digest(bytes));
        let result = self.path(&key).and_then(|path| {
            if path.exists() {
                return Ok(key.clone());
            }
            write_atomic(&path, bytes)
                .map(|()| key.clone())
                .map_err(PayloadStoreError)
        });
        Box::pin(std::future::ready(result))
    }

    fn get(&self, key: &str) -> PayloadStoreFuture<'_, Vec<u8>> {
        let result = self.path(key).and_then(|path| {
            std::fs::read(&path).map_err(|e| PayloadStoreError(format!("blob {key}: {e}")))
        });
        Box::pin(std::future::ready(result))
    }

    fn delete(&self, key: &str) -> PayloadStoreFuture<'_, ()> {
        let result = self
            .path(key)
            .and_then(|path| match std::fs::remove_file(&path) {
                Ok(()) => Ok(()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(e) => Err(PayloadStoreError(format!("blob {key}: {e}"))),
            });
        Box::pin(std::future::ready(result))
    }
}

/// Open `path` to read, and refuse anything but a regular file.
///
/// A run is code that the model wrote, and it can put a symlink in the lab
/// or the state directory. The open does not follow a symlink at `path`, so
/// the runner cannot read a file that the link names, such as
/// `/proc/self/environ`. The check uses the open handle, not the path, so
/// no swap can come between the two. Off Unix, a symlink is refused before
/// the open. Returns `None` when `path` does not exist.
///
/// # Errors
///
/// Returns an error when `path` is a symlink or not a regular file, or the
/// open fails.
pub fn open_regular(path: &Path) -> std::io::Result<Option<std::fs::File>> {
    #[cfg(unix)]
    let opened = {
        use rustix::fs::{Mode, OFlags};
        rustix::fs::open(
            path,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
            Mode::empty(),
        )
        .map(std::fs::File::from)
        .map_err(std::io::Error::from)
    };
    #[cfg(not(unix))]
    let opened = match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_symlink() => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "a symlink",
        )),
        Ok(_) => std::fs::File::open(path),
        Err(e) => Err(e),
    };
    let file = match opened {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(not_regular(path, &e)),
    };
    if file.metadata()?.is_file() {
        Ok(Some(file))
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{} is not a regular file", path.display()),
        ))
    }
}

/// The error of an open that [`open_regular`] refused. A symlink at the
/// path fails with `ELOOP`, and the message says so.
fn not_regular(path: &Path, error: &std::io::Error) -> std::io::Error {
    #[cfg(unix)]
    if error.raw_os_error() == Some(rustix::io::Errno::LOOP.raw_os_error()) {
        return std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "{} is a symlink, which the runner does not follow",
                path.display()
            ),
        );
    }
    std::io::Error::new(error.kind(), format!("{}: {error}", path.display()))
}

/// The bytes of the regular file at `path`, or `None` when it does not
/// exist. See [`open_regular`].
///
/// # Errors
///
/// Returns an error when `path` is a symlink or not a regular file, or a
/// read fails.
pub fn read_regular(path: &Path) -> std::io::Result<Option<Vec<u8>>> {
    use std::io::Read as _;
    let Some(mut file) = open_regular(path)? else {
        return Ok(None);
    };
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Ok(Some(bytes))
}

/// Write `bytes` to `path` through a scratch file and a rename.
///
/// The scratch file is new: an old file or a symlink at its name is removed
/// first, so a planted link cannot redirect the write. The data is synced
/// before the rename, and the directory after it.
///
/// # Errors
///
/// Returns an error when a write or the rename fails.
pub fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), String> {
    use std::io::Write as _;
    use std::sync::atomic::{AtomicU64, Ordering};
    // Two threads of one process get two scratch names.
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let name = path
        .file_name()
        .ok_or_else(|| format!("{} has no file name", path.display()))?;
    let scratch = path.with_file_name(format!(
        ".{}.{}.{}.tmp",
        name.to_string_lossy(),
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let failed = |e: std::io::Error| {
        let _ = std::fs::remove_file(&scratch);
        format!("cannot write {}: {e}", path.display())
    };
    let _ = std::fs::remove_file(&scratch);
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&scratch)
        .map_err(failed)?;
    file.write_all(bytes).map_err(failed)?;
    file.sync_all().map_err(failed)?;
    drop(file);
    std::fs::rename(&scratch, path).map_err(failed)?;
    if let Some(dir) = path.parent()
        && let Ok(dir) = std::fs::File::open(dir)
    {
        let _ = dir.sync_all();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use autumn_harvest_agent::{ReportSource, RunId, SessionId};

    use super::*;
    use crate::results::ResultRow;
    use crate::rules::Status;

    fn add(text: &str) -> MemoryOp {
        MemoryOp::Add {
            block: "memory".into(),
            text: text.into(),
        }
    }

    /// A cache over the blobs and the index in `dir`, as a new process
    /// builds it.
    fn disk_cache(dir: &Path, capacity: usize) -> autumn_harvest_agent::ResponseCache {
        autumn_harvest_agent::ResponseCache::new(
            Arc::new(DirPayloadStore::new(dir.join("blobs")).unwrap()),
            Arc::new(FileCacheIndex::new(dir.join("index"), capacity).unwrap()),
            autumn_harvest::payload_codec::PayloadCodecs::default(),
        )
    }

    fn chat(
        text: &str,
    ) -> (
        autumn_harvest_agent::ChatRequest,
        autumn_harvest_agent::ChatResponse,
    ) {
        use autumn_harvest_agent::{ChatMessage, ChatRole, ContentPart, StopReason, TokenUsage};
        (
            autumn_harvest_agent::ChatRequest {
                messages: vec![ChatMessage::text(ChatRole::User, text)],
                tools: Vec::new(),
                max_tokens: Some(64),
                temperature: None,
            },
            autumn_harvest_agent::ChatResponse {
                content: vec![ContentPart::Text(format!("answer to {text}"))],
                stop_reason: StopReason::EndTurn,
                usage: TokenUsage::new(10, 5),
            },
        )
    }

    /// A model call whose result a crash lost runs again in a new process.
    /// The index on disk finds the stored answer.
    #[tokio::test]
    async fn the_cache_index_survives_a_new_process_and_evicts_the_oldest() {
        use autumn_harvest_agent::KeyTenant;
        let dir = tempfile::tempdir().unwrap();
        let first = disk_cache(dir.path(), 2);
        let (ask, answer) = chat("one");
        let one = first.key(KeyTenant::None, "m", &ask).unwrap();
        first.store(&one, &answer).await.unwrap();
        let again = disk_cache(dir.path(), 2);
        assert_eq!(again.lookup(&one).await.unwrap(), Some(answer));
        // Two more entries: the oldest goes, and so does its blob.
        for text in ["two", "three"] {
            let (ask, answer) = chat(text);
            let key = again.key(KeyTenant::None, "m", &ask).unwrap();
            again.store(&key, &answer).await.unwrap();
        }
        assert_eq!(again.lookup(&one).await.unwrap(), None);
        let blobs = std::fs::read_dir(dir.path().join("blobs")).unwrap().count();
        assert_eq!(blobs, 2);
    }

    #[tokio::test]
    async fn memory_survives_a_new_store_and_dedupes_a_key() {
        let dir = tempfile::tempdir().unwrap();
        let scope = MemoryScope::new("autoresearch/oct17");
        let store = FileMemoryStore::new(dir.path()).unwrap();
        store
            .apply(&scope, add("tried MATRIX_LR"), &EditKey::new("k1"))
            .await
            .unwrap();
        // The same key again, after a crash: nothing changes.
        store
            .apply(&scope, add("tried MATRIX_LR"), &EditKey::new("k1"))
            .await
            .unwrap();
        let reopened = FileMemoryStore::new(dir.path()).unwrap();
        let blocks = reopened.load(&scope).await.unwrap();
        let memory = blocks.iter().find(|b| b.label == "memory").unwrap();
        assert_eq!(memory.entries, vec!["tried MATRIX_LR".to_string()]);
        // A new scope gets the default blocks.
        let other = reopened.load(&MemoryScope::new("other")).await.unwrap();
        assert!(other.iter().all(|b| b.entries.is_empty()));
    }

    #[tokio::test]
    async fn a_failed_edit_keeps_its_key_and_its_error() {
        let dir = tempfile::tempdir().unwrap();
        let scope = MemoryScope::new("s");
        let store = FileMemoryStore::new(dir.path()).unwrap();
        let remove = MemoryOp::Remove {
            block: "memory".into(),
            old: "absent".into(),
        };
        let first = store
            .apply(&scope, remove.clone(), &EditKey::new("k"))
            .await;
        assert!(first.is_err());
        store
            .apply(&scope, add("absent entry"), &EditKey::new("k2"))
            .await
            .unwrap();
        // The retry of `k` returns the stored error, not a new success.
        let retry = store.apply(&scope, remove, &EditKey::new("k")).await;
        assert_eq!(retry.unwrap_err(), first.unwrap_err());
    }

    fn report(lab: &Lab, segment: u32) -> Report {
        Report {
            source: ReportSource::Run,
            run_id: RunId::new("exec-1"),
            segment,
            session_id: Some(SessionId::new(lab.root().display().to_string())),
            text: "Morning report: 3 experiments.".into(),
            stop: autumn_harvest_agent::AgentStop::Completed,
        }
    }

    #[tokio::test]
    async fn a_report_lands_once_in_its_lab() {
        let dir = tempfile::tempdir().unwrap();
        let lab = Lab::init(dir.path().join("lab")).unwrap();
        let delivery = FileDelivery::new(vec![lab.clone()]);
        delivery.deliver(&report(&lab, 0)).await.unwrap();
        delivery.deliver(&report(&lab, 0)).await.unwrap();
        let files: Vec<_> = std::fs::read_dir(FileDelivery::dir(&lab).unwrap())
            .unwrap()
            .collect();
        assert_eq!(files.len(), 1);
        let mut stranger = report(&lab, 1);
        stranger.session_id = Some(SessionId::new("/not/a/lab"));
        assert!(delivery.deliver(&stranger).await.is_err());
    }

    #[tokio::test]
    async fn the_precheck_runs_a_tick_only_for_a_new_crash_streak() {
        let dir = tempfile::tempdir().unwrap();
        let lab = Lab::init(dir.path().join("lab")).unwrap();
        let precheck = ResultsPrecheck::new(vec![lab.clone()]);
        let tick = crate::agent::heartbeat_task(lab.root());
        let row = |status: Status, key: &str| {
            let row = ResultRow {
                commit: "aaaaaaa".into(),
                val_bpb: 1.0,
                memory_gb: 1.0,
                status,
                description: key.into(),
            };
            lab.record(&row, key).unwrap();
        };
        assert!(!precheck.should_run(&tick).await, "no rows yet");
        row(Status::Keep, "k1");
        for key in ["c1", "c2"] {
            row(Status::Crash, key);
        }
        assert!(!precheck.should_run(&tick).await, "two crashes");
        row(Status::Crash, "c3");
        assert!(precheck.should_run(&tick).await, "three crashes");
        // The check has no side effect: a tick that failed runs again.
        assert!(
            precheck.should_run(&tick).await,
            "the same streak, no alert yet"
        );
        let alert = Report {
            source: ReportSource::Heartbeat,
            ..report(&lab, 0)
        };
        FileDelivery::new(vec![lab.clone()])
            .deliver(&alert)
            .await
            .unwrap();
        assert!(
            !precheck.should_run(&tick).await,
            "the alert holds the rows"
        );
        row(Status::Crash, "c4");
        assert!(
            precheck.should_run(&tick).await,
            "a new crash in the streak"
        );
    }

    #[tokio::test]
    async fn a_redelivered_alert_marks_its_rows() {
        let dir = tempfile::tempdir().unwrap();
        let lab = Lab::init(dir.path().join("lab")).unwrap();
        let precheck = ResultsPrecheck::new(vec![lab.clone()]);
        let tick = crate::agent::heartbeat_task(lab.root());
        for key in ["c1", "c2", "c3"] {
            let row = ResultRow {
                commit: "aaaaaaa".into(),
                val_bpb: 0.0,
                memory_gb: 0.0,
                status: Status::Crash,
                description: key.into(),
            };
            lab.record(&row, key).unwrap();
        }
        let alert = Report {
            source: ReportSource::Heartbeat,
            ..report(&lab, 0)
        };
        let delivery = FileDelivery::new(vec![lab.clone()]);
        delivery.deliver(&alert).await.unwrap();
        // The process died after the report and before the mark.
        std::fs::remove_file(lab.state_dir().unwrap().join(ALERTED)).unwrap();
        delivery.deliver(&alert).await.unwrap();
        assert!(
            !precheck.should_run(&tick).await,
            "the redelivery marks the rows"
        );
    }

    #[tokio::test]
    async fn the_blob_store_round_trips_and_deletes() {
        let dir = tempfile::tempdir().unwrap();
        let store = DirPayloadStore::new(dir.path()).unwrap();
        let key = store.put(b"hello").await.unwrap();
        assert_eq!(store.put(b"hello").await.unwrap(), key, "content addressed");
        assert_eq!(store.get(&key).await.unwrap(), b"hello");
        store.delete(&key).await.unwrap();
        assert!(store.get(&key).await.is_err());
        assert!(store.get("../escape").await.is_err());
    }

    #[cfg(unix)]
    #[test]
    fn an_atomic_write_does_not_follow_a_planted_link() {
        let dir = tempfile::tempdir().unwrap();
        let victim = dir.path().join("victim");
        std::fs::write(&victim, "keep me").unwrap();
        let path = dir.path().join("f.json");
        // A link at a scratch name that an older build used.
        let scratch = dir
            .path()
            .join(format!(".f.json.{}.0.tmp", std::process::id()));
        std::os::unix::fs::symlink(&victim, scratch).unwrap();
        write_atomic(&path, b"new").unwrap();
        assert_eq!(std::fs::read_to_string(&victim).unwrap(), "keep me");
        assert!(!path.is_symlink());
        assert_eq!(std::fs::read(&path).unwrap(), b"new");
    }

    #[test]
    fn an_atomic_write_replaces_the_whole_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f.json");
        write_atomic(&path, b"one").unwrap();
        write_atomic(&path, b"two").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"two");
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }
}
