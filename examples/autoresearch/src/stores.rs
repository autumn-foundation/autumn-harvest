//! File-backed stores for the agent: memory, delivery, the heartbeat
//! precheck, and the blobs of the response cache.
//!
//! Each store writes under the state directory with a scratch file and a
//! rename, so a crash leaves the old file or the new one, never half of one.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use autumn_harvest::payload_store::{PayloadStore, PayloadStoreFuture};
use autumn_harvest_agent::memory::MemoryOp;
use autumn_harvest_agent::{
    AgentError, BoxFuture, Delivery, EditKey, HeartbeatTask, MemoryBlock, MemoryScope,
    MemoryStore, Precheck, Report,
};

use crate::lab::Lab;

/// A [`MemoryStore`] with one JSON file per scope.
///
/// The file holds the blocks and every applied [`EditKey`] with its first
/// result. A key that comes back after a crash changes nothing.
#[derive(Debug)]
pub struct FileMemoryStore {
    dir: PathBuf,
    lock: std::sync::Mutex<()>,
}

impl FileMemoryStore {
    /// A store under `dir`.
    ///
    /// # Errors
    ///
    /// Returns an error when `dir` cannot be created.
    pub fn new(dir: impl Into<PathBuf>) -> Result<Self, String> {
        let _ = dir.into();
        unimplemented!("green phase")
    }
}

impl MemoryStore for FileMemoryStore {
    fn load<'a>(&'a self, scope: &'a MemoryScope) -> BoxFuture<'a, Result<Vec<MemoryBlock>, AgentError>> {
        let _ = (scope, &self.dir, &self.lock);
        unimplemented!("green phase")
    }

    fn apply<'a>(
        &'a self,
        scope: &'a MemoryScope,
        op: MemoryOp,
        key: &'a EditKey,
    ) -> BoxFuture<'a, Result<MemoryBlock, AgentError>> {
        let _ = (scope, op, key);
        unimplemented!("green phase")
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
        let _ = lab;
        unimplemented!("green phase")
    }
}

impl Delivery for FileDelivery {
    fn deliver<'a>(&'a self, report: &'a Report) -> BoxFuture<'a, Result<(), AgentError>> {
        let _ = (report, &self.labs);
        unimplemented!("green phase")
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
}

impl Precheck for ResultsPrecheck {
    fn should_run<'a>(&'a self, task: &'a HeartbeatTask) -> BoxFuture<'a, bool> {
        let _ = (task, &self.labs);
        unimplemented!("green phase")
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
        let _ = dir.into();
        unimplemented!("green phase")
    }
}

impl PayloadStore for DirPayloadStore {
    fn put(&self, bytes: &[u8]) -> PayloadStoreFuture<'_, String> {
        let _ = (bytes, &self.dir);
        unimplemented!("green phase")
    }

    fn get(&self, key: &str) -> PayloadStoreFuture<'_, Vec<u8>> {
        let _ = key;
        unimplemented!("green phase")
    }

    fn delete(&self, key: &str) -> PayloadStoreFuture<'_, ()> {
        let _ = key;
        unimplemented!("green phase")
    }
}

/// Write `bytes` to `path` through a scratch file and a rename.
///
/// # Errors
///
/// Returns an error when a write or the rename fails.
pub fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let _ = (path, bytes);
    unimplemented!("green phase")
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
        store.apply(&scope, add("tried MATRIX_LR"), &EditKey::new("k1")).await.unwrap();
        // The same key again, after a crash: nothing changes.
        store.apply(&scope, add("tried MATRIX_LR"), &EditKey::new("k1")).await.unwrap();
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
        let first = store.apply(&scope, remove.clone(), &EditKey::new("k")).await;
        assert!(first.is_err());
        store.apply(&scope, add("absent entry"), &EditKey::new("k2")).await.unwrap();
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
        lab.record(&ResultRow {
            commit: "aaaaaaa".into(),
            val_bpb: 1.0,
            memory_gb: 1.0,
            status: Status::Keep,
            description: "baseline".into(),
        })
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
