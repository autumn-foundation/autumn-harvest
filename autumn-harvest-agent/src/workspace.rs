//! Persistent workspaces for durable agents (issue #2274).
//!
//! A workspace is a set of files that lasts across the ops of an agent and
//! across restarts, for example a repository checkout or notes. It lives
//! outside history, as snapshots in a [`WorkspaceBlobs`] store. This is the
//! claim-check pattern: history records only the [`WorkspaceRef`] of each
//! snapshot that a tool call made.
//!
//! - **Content-addressed.** The key of a snapshot is the SHA-256 digest of
//!   its plain bytes. A load checks the digest.
//! - **Encrypted.** The [`PayloadCodecs`] of the engine encode each
//!   snapshot. An encrypting codec therefore encrypts it at rest.
//! - **Per call.** A tool reads and writes the files through
//!   [`ToolContext::workspace`](crate::ToolContext::workspace). After the
//!   call, the tool-call activity saves a changed workspace and returns the
//!   new reference with the result. Replay reads the recorded reference and
//!   loads nothing.
//! - **Fenced.** A reference becomes the head only when history records the
//!   result that names it. A stale attempt can write a snapshot, but no
//!   recorded result names that snapshot.
//! - **Retention.** The end of each op that changed the head keeps the head
//!   and deletes every other snapshot of the agent, stale ones included.
//!   `Control::ResetWorkspace` deletes them all.

use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

use autumn_harvest::payload_codec::{PayloadCodecs, is_codec_envelope};
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{AgentError, ErrorKind};
use crate::model::BoxFuture;

/// The default cap on the plain size of one snapshot: 64 MiB.
pub const DEFAULT_MAX_BYTES: u64 = 64 * 1024 * 1024;

/// The default cap on the files of one workspace.
pub const DEFAULT_MAX_FILES: usize = 10_000;

/// The longest file path, in bytes.
pub const MAX_PATH_BYTES: usize = 512;

/// The version of the snapshot format. A new format gives new digests.
const FORMAT_VERSION: u32 = 1;

/// The reference of one snapshot. History records it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkspaceRef {
    /// The SHA-256 digest of the plain snapshot, as lowercase hex.
    pub digest: String,
    /// The plain size of the snapshot, in bytes.
    pub bytes: u64,
    /// The number of files.
    pub files: u32,
}

/// The workspace that a tool call mounts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceMount {
    /// The scope of the snapshots, `agent:<id>` for a durable agent.
    pub scope: String,
    /// The head. `None` is an empty workspace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head: Option<WorkspaceRef>,
}

impl WorkspaceMount {
    /// The scope of the durable agent `id`.
    #[must_use]
    pub fn agent_scope(id: &str) -> String {
        format!("agent:{id}")
    }
}

/// One file of a workspace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceFile {
    /// The content.
    pub data: Vec<u8>,
    /// The file is executable.
    pub executable: bool,
}

/// The files of a workspace, by relative path.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Workspace {
    files: BTreeMap<String, WorkspaceFile>,
    dirty: bool,
}

impl Workspace {
    /// The file at `path`.
    #[must_use]
    pub fn get(&self, path: &str) -> Option<&WorkspaceFile> {
        self.files.get(path)
    }

    /// The file at `path` as UTF-8 text.
    #[must_use]
    pub fn read_text(&self, path: &str) -> Option<&str> {
        self.get(path)
            .and_then(|file| std::str::from_utf8(&file.data).ok())
    }

    /// Write `data` at `path`, as a file that is not executable.
    ///
    /// # Errors
    ///
    /// Returns a [`ErrorKind::Tool`] error for a path that is not a plain
    /// relative path.
    pub fn write(&mut self, path: &str, data: impl Into<Vec<u8>>) -> Result<(), AgentError> {
        self.write_file(
            path,
            WorkspaceFile {
                data: data.into(),
                executable: false,
            },
        )
    }

    /// Write `file` at `path`.
    ///
    /// # Errors
    ///
    /// As [`Self::write`].
    pub fn write_file(&mut self, path: &str, file: WorkspaceFile) -> Result<(), AgentError> {
        check_path(path)?;
        if self.files.get(path) != Some(&file) {
            self.files.insert(path.to_owned(), file);
            self.dirty = true;
        }
        Ok(())
    }

    /// Remove the file at `path`. Returns whether it was there.
    pub fn remove(&mut self, path: &str) -> bool {
        let removed = self.files.remove(path).is_some();
        self.dirty |= removed;
        removed
    }

    /// Every file, by path.
    #[must_use]
    pub const fn files(&self) -> &BTreeMap<String, WorkspaceFile> {
        &self.files
    }

    /// Whether a write or a remove changed the workspace since it was
    /// loaded.
    #[must_use]
    pub const fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// The plain size of the content of every file.
    #[must_use]
    pub fn bytes(&self) -> u64 {
        self.files
            .values()
            .map(|file| u64::try_from(file.data.len()).unwrap_or(u64::MAX))
            .fold(0, u64::saturating_add)
    }
}

/// A plain relative path: not empty, no root, no `..`, no NUL.
fn check_path(path: &str) -> Result<(), AgentError> {
    let bad = |why: &str| {
        AgentError::new(
            ErrorKind::Tool,
            format!("the workspace path {path:?} {why}"),
        )
    };
    if path.is_empty() || path.len() > MAX_PATH_BYTES {
        return Err(bad("is empty or too long"));
    }
    if path.contains('\0') || path.contains('\\') {
        return Err(bad("holds a NUL or a backslash"));
    }
    let plain = Path::new(path)
        .components()
        .all(|part| matches!(part, Component::Normal(_)));
    if plain {
        Ok(())
    } else {
        Err(bad("must be relative, with no `.` or `..` part"))
    }
}

/// A handle on the workspace of one tool call. Clones share the files.
#[derive(Debug, Clone, Default)]
pub struct WorkspaceHandle(Arc<Mutex<Workspace>>);

impl WorkspaceHandle {
    /// A handle on `workspace`.
    #[must_use]
    pub fn new(workspace: Workspace) -> Self {
        Self(Arc::new(Mutex::new(workspace)))
    }

    /// Run `f` on the files.
    pub fn with<T>(&self, f: impl FnOnce(&mut Workspace) -> T) -> T {
        let mut workspace = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        f(&mut workspace)
    }

    /// A copy of the files.
    #[must_use]
    pub fn snapshot(&self) -> Workspace {
        self.with(|workspace| workspace.clone())
    }
}

impl PartialEq for WorkspaceHandle {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for WorkspaceHandle {}

/// Where snapshots are kept: a blob store with one key space per scope.
///
/// Use a store for workspaces only. Retention deletes every blob of a scope
/// that is not its head.
pub trait WorkspaceBlobs: Send + Sync + std::fmt::Debug {
    /// Store `bytes` under `digest` in `scope`. A put of an existing digest
    /// replaces it.
    ///
    /// # Errors
    ///
    /// Returns an error when the store cannot write.
    fn put<'a>(
        &'a self,
        scope: &'a str,
        digest: &'a str,
        bytes: Vec<u8>,
    ) -> BoxFuture<'a, Result<(), AgentError>>;

    /// The bytes under `digest` in `scope`, if any.
    ///
    /// # Errors
    ///
    /// Returns an error when the store cannot read.
    fn get<'a>(
        &'a self,
        scope: &'a str,
        digest: &'a str,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, AgentError>>;

    /// Every digest in `scope`.
    ///
    /// # Errors
    ///
    /// Returns an error when the store cannot read.
    fn list<'a>(&'a self, scope: &'a str) -> BoxFuture<'a, Result<Vec<String>, AgentError>>;

    /// Delete the blob under `digest` in `scope`. Returns whether it was
    /// there.
    ///
    /// # Errors
    ///
    /// Returns an error when the store cannot write.
    fn delete<'a>(
        &'a self,
        scope: &'a str,
        digest: &'a str,
    ) -> BoxFuture<'a, Result<bool, AgentError>>;
}

/// Blobs in process memory, for tests and one process.
#[derive(Debug, Default)]
pub struct InMemoryWorkspaceBlobs {
    blobs: Mutex<BTreeMap<(String, String), Vec<u8>>>,
}

impl InMemoryWorkspaceBlobs {
    fn locked(&self) -> std::sync::MutexGuard<'_, BTreeMap<(String, String), Vec<u8>>> {
        self.blobs.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Every stored blob, for a test that looks for plain text at rest.
    #[must_use]
    pub fn raw(&self) -> Vec<Vec<u8>> {
        self.locked().values().cloned().collect()
    }
}

impl WorkspaceBlobs for InMemoryWorkspaceBlobs {
    fn put<'a>(
        &'a self,
        scope: &'a str,
        digest: &'a str,
        bytes: Vec<u8>,
    ) -> BoxFuture<'a, Result<(), AgentError>> {
        self.locked()
            .insert((scope.to_owned(), digest.to_owned()), bytes);
        Box::pin(async { Ok(()) })
    }

    fn get<'a>(
        &'a self,
        scope: &'a str,
        digest: &'a str,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, AgentError>> {
        let found = self
            .locked()
            .get(&(scope.to_owned(), digest.to_owned()))
            .cloned();
        Box::pin(async move { Ok(found) })
    }

    fn list<'a>(&'a self, scope: &'a str) -> BoxFuture<'a, Result<Vec<String>, AgentError>> {
        let digests = self
            .locked()
            .keys()
            .filter(|(owner, _)| owner == scope)
            .map(|(_, digest)| digest.clone())
            .collect();
        Box::pin(async move { Ok(digests) })
    }

    fn delete<'a>(
        &'a self,
        scope: &'a str,
        digest: &'a str,
    ) -> BoxFuture<'a, Result<bool, AgentError>> {
        let removed = self
            .locked()
            .remove(&(scope.to_owned(), digest.to_owned()))
            .is_some();
        Box::pin(async move { Ok(removed) })
    }
}

/// Blobs in a directory: one folder per scope, one file per digest.
///
/// The folder name is the SHA-256 digest of the scope, so any agent id is a
/// safe name. A write goes to a temporary file first, then a rename makes
/// it visible, so a crash never leaves half a blob.
#[derive(Debug, Clone)]
pub struct DirWorkspaceBlobs {
    root: PathBuf,
}

impl DirWorkspaceBlobs {
    /// Blobs under `root`. The folder is made on the first write.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn folder(&self, scope: &str) -> PathBuf {
        self.root.join(hex(&Sha256::digest(scope.as_bytes())))
    }

    /// The file of `digest`. A digest that is not 64 hex digits is refused,
    /// so it can never name a path outside the folder.
    fn file(&self, scope: &str, digest: &str) -> Result<PathBuf, AgentError> {
        if digest.len() == 64 && digest.bytes().all(|b| b.is_ascii_hexdigit()) {
            Ok(self.folder(scope).join(digest))
        } else {
            Err(AgentError::new(
                ErrorKind::Decode,
                "a workspace digest must be 64 hex digits",
            ))
        }
    }
}

fn io_error(err: &std::io::Error) -> AgentError {
    AgentError::new(
        ErrorKind::Unavailable,
        format!("the workspace store cannot reach its folder: {err}"),
    )
}

/// Write `bytes` to a new file at `path` that only its owner can read.
///
/// The file must not exist, so two writers never share one handle.
async fn write_private(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt as _;
    let mut options = tokio::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options.open(path).await?;
    file.write_all(bytes).await?;
    file.flush().await
}

/// A scratch path next to `path` that no other put of this process uses.
///
/// Two attempts of one turn can save the same digest at the same time. A
/// shared scratch file would mix their bytes.
fn scratch_path(path: &std::path::Path) -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    path.with_extension(format!("tmp-{}-{n}", std::process::id()))
}

impl WorkspaceBlobs for DirWorkspaceBlobs {
    fn put<'a>(
        &'a self,
        scope: &'a str,
        digest: &'a str,
        bytes: Vec<u8>,
    ) -> BoxFuture<'a, Result<(), AgentError>> {
        Box::pin(async move {
            let path = self.file(scope, digest)?;
            tokio::fs::create_dir_all(self.folder(scope))
                .await
                .map_err(|e| io_error(&e))?;
            let tmp = scratch_path(&path);
            // A crashed process with the same id can leave this name behind.
            let _ = tokio::fs::remove_file(&tmp).await;
            let written = match write_private(&tmp, &bytes).await {
                Ok(()) => tokio::fs::rename(&tmp, &path).await,
                Err(err) => Err(err),
            };
            if let Err(err) = written {
                let _ = tokio::fs::remove_file(&tmp).await;
                return Err(io_error(&err));
            }
            Ok(())
        })
    }

    fn get<'a>(
        &'a self,
        scope: &'a str,
        digest: &'a str,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, AgentError>> {
        Box::pin(async move {
            match tokio::fs::read(self.file(scope, digest)?).await {
                Ok(bytes) => Ok(Some(bytes)),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(err) => Err(io_error(&err)),
            }
        })
    }

    fn list<'a>(&'a self, scope: &'a str) -> BoxFuture<'a, Result<Vec<String>, AgentError>> {
        Box::pin(async move {
            let mut entries = match tokio::fs::read_dir(self.folder(scope)).await {
                Ok(entries) => entries,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
                Err(err) => return Err(io_error(&err)),
            };
            let mut digests = Vec::new();
            while let Some(entry) = entries.next_entry().await.map_err(|e| io_error(&e))? {
                if let Some(name) = entry.file_name().to_str()
                    && self.file(scope, name).is_ok()
                {
                    digests.push(name.to_owned());
                }
            }
            Ok(digests)
        })
    }

    fn delete<'a>(
        &'a self,
        scope: &'a str,
        digest: &'a str,
    ) -> BoxFuture<'a, Result<bool, AgentError>> {
        Box::pin(async move {
            match tokio::fs::remove_file(self.file(scope, digest)?).await {
                Ok(()) => Ok(true),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
                Err(err) => Err(io_error(&err)),
            }
        })
    }
}

/// The snapshots of workspaces: a blob store, the codecs and the caps.
///
/// Install it with [`AgentHarness::workspace`](crate::AgentHarness::workspace).
#[derive(Clone)]
pub struct WorkspaceStore {
    blobs: Arc<dyn WorkspaceBlobs>,
    codecs: PayloadCodecs,
    max_bytes: u64,
    max_files: usize,
}

impl std::fmt::Debug for WorkspaceStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkspaceStore")
            .field("blobs", &self.blobs)
            .field("max_bytes", &self.max_bytes)
            .field("max_files", &self.max_files)
            .finish_non_exhaustive()
    }
}

/// One file of the snapshot format.
#[derive(Serialize, Deserialize)]
struct StoredFile {
    path: String,
    base64: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    executable: bool,
}

/// The snapshot format. The files are in path order, so equal workspaces
/// give equal bytes.
#[derive(Serialize, Deserialize)]
struct Stored {
    v: u32,
    files: Vec<StoredFile>,
}

impl WorkspaceStore {
    /// A store on `blobs`, encoded by `codecs`.
    ///
    /// Pass a clone of the codec registry that the engine uses. Its active
    /// codec then encrypts every snapshot. A registry with no keyed codec
    /// keeps the snapshots as plain text, so the store logs a warning.
    #[must_use]
    pub fn new(blobs: Arc<dyn WorkspaceBlobs>, codecs: PayloadCodecs) -> Self {
        if codecs.active_codec_id().is_none() {
            tracing::warn!(
                "the workspace store has no keyed payload codec, so it keeps snapshots as plain text"
            );
        }
        Self {
            blobs,
            codecs,
            max_bytes: DEFAULT_MAX_BYTES,
            max_files: DEFAULT_MAX_FILES,
        }
    }

    /// Cap the plain size of one snapshot.
    #[must_use]
    pub const fn max_bytes(mut self, max: u64) -> Self {
        self.max_bytes = max;
        self
    }

    /// Cap the files of one workspace.
    #[must_use]
    pub const fn max_files(mut self, max: usize) -> Self {
        self.max_files = max;
        self
    }

    /// The blob store.
    #[must_use]
    pub fn blobs(&self) -> &Arc<dyn WorkspaceBlobs> {
        &self.blobs
    }

    /// Load the head of `mount`. No head is an empty workspace.
    ///
    /// # Errors
    ///
    /// Returns an error when the store cannot read, the head is gone, or
    /// the snapshot does not match its digest.
    pub async fn load(&self, mount: &WorkspaceMount) -> Result<Workspace, AgentError> {
        let Some(head) = &mount.head else {
            return Ok(Workspace::default());
        };
        let Some(bytes) = self.blobs.get(&mount.scope, &head.digest).await? else {
            return Err(AgentError::new(
                ErrorKind::Decode,
                "the workspace head is not in the store",
            ));
        };
        let plain = self.decode(&bytes)?;
        if hex(&Sha256::digest(&plain)) != head.digest {
            return Err(AgentError::new(
                ErrorKind::Decode,
                "the workspace snapshot does not match its digest",
            ));
        }
        let stored: Stored = serde_json::from_slice(&plain).map_err(|_| shape_error())?;
        let mut files = BTreeMap::new();
        for file in stored.files {
            let data = base64::engine::general_purpose::STANDARD
                .decode(file.base64)
                .map_err(|_| shape_error())?;
            files.insert(
                file.path,
                WorkspaceFile {
                    data,
                    executable: file.executable,
                },
            );
        }
        Ok(Workspace {
            files,
            dirty: false,
        })
    }

    /// Save `workspace` in `scope` and return its reference.
    ///
    /// # Errors
    ///
    /// Returns a [`ErrorKind::Tool`] error when the workspace is over a
    /// cap, and an error when the codec or the store fails.
    pub async fn save(
        &self,
        scope: &str,
        workspace: &Workspace,
    ) -> Result<WorkspaceRef, AgentError> {
        let bytes = workspace.bytes();
        if bytes > self.max_bytes || workspace.files.len() > self.max_files {
            return Err(AgentError::new(
                ErrorKind::Tool,
                format!(
                    "the workspace holds {bytes} bytes in {} files; the caps are {} bytes and {} \
                     files. Remove files and try again",
                    workspace.files.len(),
                    self.max_bytes,
                    self.max_files
                ),
            ));
        }
        let stored = Stored {
            v: FORMAT_VERSION,
            files: workspace
                .files
                .iter()
                .map(|(path, file)| StoredFile {
                    path: path.clone(),
                    base64: base64::engine::general_purpose::STANDARD.encode(&file.data),
                    executable: file.executable,
                })
                .collect(),
        };
        let plain = serde_json::to_vec(&stored).map_err(|_| shape_error())?;
        let digest = hex(&Sha256::digest(&plain));
        let encoded = self.encode(&plain)?;
        self.blobs.put(scope, &digest, encoded).await?;
        Ok(WorkspaceRef {
            digest,
            bytes,
            files: u32::try_from(workspace.files.len()).unwrap_or(u32::MAX),
        })
    }

    /// Delete every snapshot of `scope` but `keep`. Returns the number
    /// deleted.
    ///
    /// # Errors
    ///
    /// Returns an error when the store fails.
    pub async fn retain(
        &self,
        scope: &str,
        keep: Option<&WorkspaceRef>,
    ) -> Result<u64, AgentError> {
        let mut deleted = 0;
        for digest in self.blobs.list(scope).await? {
            if keep.is_some_and(|head| head.digest == digest) {
                continue;
            }
            if self.blobs.delete(scope, &digest).await? {
                deleted += 1;
            }
        }
        Ok(deleted)
    }

    /// Delete every snapshot of `scope`. Returns the number deleted.
    ///
    /// # Errors
    ///
    /// Returns an error when the store fails.
    pub async fn erase(&self, scope: &str) -> Result<u64, AgentError> {
        self.retain(scope, None).await
    }

    /// The codec envelope of `plain`, as bytes.
    fn encode(&self, plain: &[u8]) -> Result<Vec<u8>, AgentError> {
        let text = base64::engine::general_purpose::STANDARD.encode(plain);
        let encoded = self
            .codecs
            .encode_payload(&serde_json::Value::String(text))
            .map_err(|_| {
                AgentError::new(
                    ErrorKind::Config,
                    "the payload codec could not encode a workspace snapshot",
                )
            })?;
        serde_json::to_vec(&encoded).map_err(|_| shape_error())
    }

    /// The plain bytes inside the codec envelope `bytes`.
    fn decode(&self, bytes: &[u8]) -> Result<Vec<u8>, AgentError> {
        let value: serde_json::Value = serde_json::from_slice(bytes).map_err(|_| shape_error())?;
        // With an encoding codec, a plain blob is not one this store wrote.
        let encoding = self
            .codecs
            .active_codec_id()
            .unwrap_or_else(|| self.codecs.default_codec_id());
        if encoding != "identity" && !is_codec_envelope(&value) {
            return Err(AgentError::new(
                ErrorKind::Decode,
                "the workspace snapshot is not a codec envelope",
            ));
        }
        let decoded = self.codecs.decode_payload(&value).map_err(|_| {
            AgentError::new(
                ErrorKind::Decode,
                "the payload codec could not decode a workspace snapshot",
            )
        })?;
        let text = decoded.as_str().ok_or_else(shape_error)?;
        base64::engine::general_purpose::STANDARD
            .decode(text)
            .map_err(|_| shape_error())
    }
}

fn shape_error() -> AgentError {
    AgentError::new(
        ErrorKind::Decode,
        "a workspace snapshot has the wrong shape",
    )
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::with_capacity(64), |mut out, b| {
        let _ = write!(out, "{b:02x}");
        out
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store(blobs: &Arc<InMemoryWorkspaceBlobs>) -> WorkspaceStore {
        WorkspaceStore::new(blobs.clone(), PayloadCodecs::default())
    }

    #[tokio::test]
    async fn a_saved_workspace_loads_back_with_the_same_digest() {
        let blobs = Arc::new(InMemoryWorkspaceBlobs::default());
        let store = store(&blobs);
        let mut workspace = Workspace::default();
        workspace.write("notes/a.md", "hello").unwrap();
        workspace
            .write_file(
                "run.sh",
                WorkspaceFile {
                    data: b"#!/bin/sh\n".to_vec(),
                    executable: true,
                },
            )
            .unwrap();
        let head = store.save("agent:a", &workspace).await.unwrap();
        assert_eq!((head.files, head.bytes), (2, 15));
        let again = store.save("agent:a", &workspace).await.unwrap();
        assert_eq!(head, again, "equal content gives an equal digest");
        let mount = WorkspaceMount {
            scope: "agent:a".to_owned(),
            head: Some(head),
        };
        let loaded = store.load(&mount).await.unwrap();
        assert_eq!(loaded.read_text("notes/a.md"), Some("hello"));
        assert!(loaded.get("run.sh").unwrap().executable);
        assert!(!loaded.is_dirty());
    }

    #[tokio::test]
    async fn a_changed_blob_fails_its_digest_check() {
        let blobs = Arc::new(InMemoryWorkspaceBlobs::default());
        let store = store(&blobs);
        let mut workspace = Workspace::default();
        workspace.write("a", "one").unwrap();
        let head = store.save("s", &workspace).await.unwrap();
        workspace.write("a", "two").unwrap();
        let other = store.save("s", &workspace).await.unwrap();
        let swapped = blobs.get("s", &other.digest).await.unwrap().unwrap();
        blobs.put("s", &head.digest, swapped).await.unwrap();
        let mount = WorkspaceMount {
            scope: "s".to_owned(),
            head: Some(head),
        };
        let err = store.load(&mount).await.unwrap_err();
        assert!(err.message().contains("digest"), "{err}");
    }

    #[tokio::test]
    async fn retain_keeps_only_the_head_and_erase_keeps_nothing() {
        let blobs = Arc::new(InMemoryWorkspaceBlobs::default());
        let store = store(&blobs);
        let mut heads = Vec::new();
        for n in 0..3 {
            let mut workspace = Workspace::default();
            workspace.write("n", n.to_string()).unwrap();
            heads.push(store.save("s", &workspace).await.unwrap());
        }
        let mut other = Workspace::default();
        other.write("x", "y").unwrap();
        store.save("t", &other).await.unwrap();
        assert_eq!(store.retain("s", heads.last()).await.unwrap(), 2);
        assert_eq!(blobs.list("s").await.unwrap(), [heads[2].digest.clone()]);
        assert_eq!(store.erase("s").await.unwrap(), 1);
        assert_eq!(blobs.list("s").await.unwrap(), Vec::<String>::new());
        assert_eq!(
            blobs.list("t").await.unwrap().len(),
            1,
            "another scope stays"
        );
    }

    #[tokio::test]
    async fn a_workspace_over_a_cap_is_a_tool_error() {
        let blobs = Arc::new(InMemoryWorkspaceBlobs::default());
        let store = store(&blobs).max_bytes(4).max_files(1);
        let mut workspace = Workspace::default();
        workspace.write("a", "12345").unwrap();
        let err = store.save("s", &workspace).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Tool);
        assert_eq!(blobs.raw(), Vec::<Vec<u8>>::new());
    }

    #[test]
    fn a_path_must_be_plain_and_relative() {
        let mut workspace = Workspace::default();
        for bad in ["", "/etc/passwd", "../up", "a/../b", "./a", "a\\b", "a\0b"] {
            assert!(workspace.write(bad, "x").is_err(), "{bad:?}");
        }
        assert!(workspace.write("src/main.rs", "x").is_ok());
        assert!(workspace.is_dirty());
    }

    #[test]
    fn an_unchanged_write_leaves_the_workspace_clean() {
        let mut workspace = Workspace::default();
        workspace.write("a", "x").unwrap();
        let mut loaded = Workspace {
            dirty: false,
            ..workspace
        };
        loaded.write("a", "x").unwrap();
        assert!(!loaded.is_dirty());
        assert!(!loaded.remove("missing"));
        assert!(loaded.remove("a"));
        assert!(loaded.is_dirty());
    }

    #[tokio::test]
    async fn the_dir_store_keeps_scopes_apart_and_refuses_odd_digests() {
        let dir = tempfile::tempdir().unwrap();
        let blobs = DirWorkspaceBlobs::new(dir.path());
        let digest = "a".repeat(64);
        blobs
            .put("agent:x/y", &digest, b"one".to_vec())
            .await
            .unwrap();
        assert_eq!(
            blobs.get("agent:x/y", &digest).await.unwrap().as_deref(),
            Some(&b"one"[..])
        );
        assert_eq!(
            blobs.list("agent:x/y").await.unwrap(),
            std::slice::from_ref(&digest)
        );
        assert_eq!(
            blobs.list("agent:other").await.unwrap(),
            Vec::<String>::new()
        );
        assert!(blobs.get("agent:x/y", "../../etc/passwd").await.is_err());
        assert!(blobs.delete("agent:x/y", &digest).await.unwrap());
        assert!(!blobs.delete("agent:x/y", &digest).await.unwrap());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn the_dir_store_writes_files_that_only_the_owner_reads() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::tempdir().unwrap();
        let blobs = DirWorkspaceBlobs::new(dir.path());
        let digest = "b".repeat(64);
        blobs
            .put("agent:x/y", &digest, b"two".to_vec())
            .await
            .unwrap();
        let path = blobs.file("agent:x/y", &digest).unwrap();
        let mode = std::fs::metadata(path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_puts_of_one_digest_each_write_a_whole_blob() {
        let dir = tempfile::tempdir().unwrap();
        let blobs = Arc::new(DirWorkspaceBlobs::new(dir.path()));
        let digest = "c".repeat(64);
        // An AEAD codec gives each put of one snapshot different bytes.
        let bodies: Vec<Vec<u8>> = (0..16_u8).map(|n| vec![n; 256 * 1024]).collect();
        // Spawn every put before the first await, so the puts overlap.
        let mut puts = Vec::new();
        for bytes in bodies.iter().cloned() {
            let blobs = blobs.clone();
            let digest = digest.clone();
            puts.push(tokio::spawn(async move {
                blobs.put("agent:race", &digest, bytes).await
            }));
        }
        for put in puts {
            put.await.unwrap().unwrap();
        }
        let stored = blobs.get("agent:race", &digest).await.unwrap().unwrap();
        assert!(bodies.contains(&stored), "the blob is one whole put");
        let names: Vec<_> = std::fs::read_dir(blobs.folder("agent:race"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert_eq!(names.len(), 1, "no scratch file is left: {names:?}");
    }
}
