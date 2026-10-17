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
    AgentError, BoxFuture, Delivery, EditKey, ErrorKind, HeartbeatTask, MemoryBlock, MemoryScope,
    MemoryStore, Precheck, Report, SessionId,
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
        if path.exists() {
            return Ok(());
        }
        let text = format!(
            "# autoresearch report\n\nrun: {}\nstop: {:?}\n\n{}\n",
            report.run_id.as_str(),
            report.stop,
            report.text.trim()
        );
        write_atomic(&path, text.as_bytes()).map_err(|e| AgentError::new(ErrorKind::Unavailable, e))
    }
}

impl Delivery for FileDelivery {
    fn deliver<'a>(&'a self, report: &'a Report) -> BoxFuture<'a, Result<(), AgentError>> {
        Box::pin(std::future::ready(self.write(report)))
    }
}

/// A [`Precheck`] that runs a heartbeat tick only when `results.tsv` has
/// rows that the last tick did not see.
#[derive(Debug, Clone)]
pub struct ResultsPrecheck {
    labs: Arc<Vec<Lab>>,
}

impl ResultsPrecheck {
    /// A precheck for `labs`.
    #[must_use]
    pub fn new(labs: Vec<Lab>) -> Self {
        Self {
            labs: Arc::new(labs),
        }
    }

    /// Compare the rows with the count that the last tick saw, and store
    /// the new count. An unreadable lab skips the tick.
    fn check(&self, task: &HeartbeatTask) -> bool {
        let session = task.session_id.as_ref().map(SessionId::as_str);
        let Some(lab) = Lab::serving(&self.labs, session) else {
            return false;
        };
        let (Ok(rows), Ok(state)) = (lab.rows(), lab.state_dir()) else {
            return false;
        };
        let seen_path = state.join("heartbeat.seen");
        let seen: usize = std::fs::read_to_string(&seen_path)
            .ok()
            .and_then(|text| text.trim().parse().ok())
            .unwrap_or(0);
        if rows.len() <= seen {
            return false;
        }
        write_atomic(&seen_path, rows.len().to_string().as_bytes()).is_ok()
    }
}

impl Precheck for ResultsPrecheck {
    fn should_run<'a>(&'a self, task: &'a HeartbeatTask) -> BoxFuture<'a, bool> {
        Box::pin(std::future::ready(self.check(task)))
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
    async fn the_precheck_runs_a_tick_only_for_new_rows() {
        let dir = tempfile::tempdir().unwrap();
        let lab = Lab::init(dir.path().join("lab")).unwrap();
        let precheck = ResultsPrecheck::new(vec![lab.clone()]);
        let tick = crate::agent::heartbeat_task(lab.root());
        assert!(!precheck.should_run(&tick).await, "no rows yet");
        lab.record(
            &ResultRow {
                commit: "aaaaaaa".into(),
                val_bpb: 1.0,
                memory_gb: 1.0,
                status: Status::Keep,
                description: "baseline".into(),
            },
            "k1",
        )
        .unwrap();
        assert!(precheck.should_run(&tick).await, "a new row");
        assert!(!precheck.should_run(&tick).await, "the same rows");
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
