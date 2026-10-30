//! Persistent workspaces for durable agents (issue #2274).
//!
//! A workspace is a set of files that lasts across the ops of an agent and
//! across restarts, for example a repository checkout or notes. It lives
//! outside history, as snapshots in a [`WorkspaceBlobs`] store. This is the
//! claim-check pattern: history records only the [`WorkspaceRef`] of each
//! snapshot that a tool call made.
//!
//! - **Content-addressed.** The digest of a snapshot is the SHA-256 digest
//!   of its plain bytes. A load checks the digest.
//! - **Generations.** The key of a snapshot is its generation and its
//!   digest. A durable agent gives each segment and each reset a higher
//!   generation than the last.
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
//! - **Retention.** The end of each op that changed the head deletes every
//!   other snapshot of its generation or older, stale ones included.
//!   `Control::ResetWorkspace` deletes every snapshot below its own
//!   generation and leaves an empty marker at it. A stale attempt of either
//!   activity can run after a newer op. It then deletes only older keys, so
//!   it never deletes a newer head, also when that head has old content.
//! - **Lives.** A new life of an agent starts above the highest key or
//!   marker in its scope. So no attempt of an old life deletes a key of the
//!   new one. `formal/tla/AgentWorkspaceRetention.tla` checks both.

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
    /// The generation of the op that saved the snapshot. Retention deletes
    /// no key of a newer generation.
    #[serde(default)]
    pub generation: u64,
}

impl WorkspaceRef {
    /// The key of the snapshot in its scope: the generation as 16 hex
    /// digits, a dash and the digest.
    #[must_use]
    pub fn key(&self) -> String {
        format!("{:016x}-{}", self.generation, self.digest)
    }
}

/// The generation in `key`, when `key` has the form of
/// [`WorkspaceRef::key`].
fn key_generation(key: &str) -> Option<u64> {
    let (generation, digest) = key.split_once('-')?;
    if !hex_digits(generation, 16) || !hex_digits(digest, 64) {
        return None;
    }
    u64::from_str_radix(generation, 16).ok()
}

/// The suffix of the key of a reset marker.
const RESET_MARKER: &str = "reset";

/// The key of the marker that a reset below `below` leaves.
fn reset_marker(below: u64) -> String {
    format!("{below:016x}-{RESET_MARKER}")
}

/// The bound in `key`, when `key` is a reset marker.
fn marker_generation(key: &str) -> Option<u64> {
    let (generation, tag) = key.split_once('-')?;
    if tag != RESET_MARKER || !hex_digits(generation, 16) {
        return None;
    }
    u64::from_str_radix(generation, 16).ok()
}

/// The infix between a key and the tag of its scratch name.
const SCRATCH_INFIX: &str = ".tmp-";

/// The key that the scratch name `name` belongs to (issue #2315).
///
/// [`DirWorkspaceBlobs`] writes a scratch name as a key or a reset marker,
/// then `.tmp-` and 32 lowercase hex digits. Each other form is `None`.
fn scratch_owner(name: &str) -> Option<&str> {
    let (owner, tag) = name.split_once(SCRATCH_INFIX)?;
    let known = key_generation(owner).is_some() || marker_generation(owner).is_some();
    (known && hex_digits(tag, 32)).then_some(owner)
}

/// The generation of the key that the scratch name `name` belongs to.
fn scratch_generation(name: &str) -> Option<u64> {
    scratch_owner(name).and_then(|owner| key_generation(owner).or_else(|| marker_generation(owner)))
}

/// The generation of `name`: a snapshot key, a reset marker or a scratch
/// name.
fn name_generation(name: &str) -> Option<u64> {
    key_generation(name)
        .or_else(|| marker_generation(name))
        .or_else(|| scratch_generation(name))
}

/// `text` is `len` lowercase hex digits.
fn hex_digits(text: &str, len: usize) -> bool {
    text.len() == len && text.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// The workspace that a tool call mounts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceMount {
    /// The scope of the snapshots, `agent:<id>` for a durable agent.
    pub scope: String,
    /// The head. `None` is an empty workspace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head: Option<WorkspaceRef>,
    /// The generation of a snapshot that this mount saves. For a reset, the
    /// bound below which it deletes.
    #[serde(default)]
    pub generation: u64,
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
/// Use a store for workspaces only. Retention deletes the keys of a scope
/// that are not newer than its head. It never deletes a key that does not
/// have the form of [`WorkspaceRef::key`].
///
/// A store that writes through scratch names can list them too. Each
/// scratch name starts with the key it belongs to, so retention and reset
/// read its generation (issue #2315). `delete` must accept each listed
/// name.
pub trait WorkspaceBlobs: Send + Sync + std::fmt::Debug {
    /// Store `bytes` under the key `digest` in `scope`. A put of an existing
    /// key replaces it.
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

    /// The bytes under the key `digest` in `scope`, if any.
    ///
    /// # Errors
    ///
    /// Returns an error when the store cannot read.
    fn get<'a>(
        &'a self,
        scope: &'a str,
        digest: &'a str,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, AgentError>>;

    /// Every key in `scope`, and every scratch name that a put left there.
    ///
    /// # Errors
    ///
    /// Returns an error when the store cannot read.
    fn list<'a>(&'a self, scope: &'a str) -> BoxFuture<'a, Result<Vec<String>, AgentError>>;

    /// Delete the blob under the key `digest` in `scope`. Returns whether
    /// it was there.
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
/// safe name. A write goes to a scratch file first, then a rename makes it
/// visible, so a key never holds half a blob.
///
/// A put returns only after the data is on disk. It syncs the scratch file,
/// renames it, and syncs the folder, and the parent of each folder that it
/// creates. Retention can delete the previous snapshot, so a lost head would
/// lose the workspace.
///
/// A put that stops before its rename leaves its scratch file. A crash, a
/// timeout or a cancel can cause that. The store lists the scratch name, so
/// retention, reset and erase delete it by the generation of its key (issue
/// #2315).
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

    /// The file of `name` for a read, a list or a delete: a key, a reset
    /// marker or a scratch name. Each other name is refused, so it can never
    /// name a path outside the folder.
    fn entry(&self, scope: &str, name: &str) -> Result<PathBuf, AgentError> {
        if scratch_owner(name).is_some() {
            Ok(self.folder(scope).join(name))
        } else {
            self.file(scope, name)
        }
    }

    /// The file of `key` for a put. The method refuses a key that is not a
    /// snapshot key or a reset marker. So a key never names a path outside
    /// the folder, and no put writes a scratch name.
    fn file(&self, scope: &str, key: &str) -> Result<PathBuf, AgentError> {
        if key_generation(key).is_some() || marker_generation(key).is_some() {
            Ok(self.folder(scope).join(key))
        } else {
            Err(AgentError::new(
                ErrorKind::Decode,
                "a workspace key must be 16 hex digits, a dash and 64 hex digits",
            ))
        }
    }
}

/// Durability calls, in order, and faults for them. Tests use both.
#[cfg(test)]
mod sync_log {
    use std::path::{Path, PathBuf};
    use std::sync::{Mutex, PoisonError};

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum Kind {
        File,
        Rename,
        Dir,
    }

    type Call = (Kind, PathBuf);

    static LOG: Mutex<Vec<Call>> = Mutex::new(Vec::new());
    static FAULTS: Mutex<Vec<Call>> = Mutex::new(Vec::new());

    /// Record a call. Fail it when a fault covers the kind and the path.
    pub(super) fn hit(kind: Kind, path: &Path) -> std::io::Result<()> {
        {
            let mut log = LOG.lock().unwrap_or_else(PoisonError::into_inner);
            log.push((kind, path.to_path_buf()));
        }
        let faults = FAULTS.lock().unwrap_or_else(PoisonError::into_inner);
        if faults
            .iter()
            .any(|(k, root)| *k == kind && path.starts_with(root))
        {
            return Err(std::io::Error::other("injected sync fault"));
        }
        Ok(())
    }

    /// Fail each call of `kind` on a path below `root`.
    pub(super) fn fail(kind: Kind, root: &Path) {
        FAULTS
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((kind, root.to_path_buf()));
    }

    /// Remove and return the calls on a path below `root`.
    pub(super) fn take_below(root: &Path) -> Vec<Call> {
        let mut log = LOG.lock().unwrap_or_else(PoisonError::into_inner);
        let (taken, kept) = log.drain(..).partition(|(_, p)| p.starts_with(root));
        *log = kept;
        taken
    }
}

fn io_error(err: &std::io::Error) -> AgentError {
    AgentError::new(
        ErrorKind::Unavailable,
        format!("the workspace store cannot reach its folder: {err}"),
    )
}

/// Write `bytes` to a new file at `path` that only its owner can read, and
/// sync it to disk.
///
/// The file must not exist, so two writers never share one handle.
fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write as _;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    #[cfg(test)]
    sync_log::hit(sync_log::Kind::File, path)?;
    file.sync_all()
}

/// Sync the directory `dir`, so its entries survive a power loss.
///
/// A directory cannot be opened for a sync on Windows, so the call does
/// nothing there.
fn sync_dir(dir: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        #[cfg(test)]
        sync_log::hit(sync_log::Kind::Dir, dir)?;
        std::fs::File::open(dir)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
        Ok(())
    }
}

/// Write `bytes` to `path` so that the blob survives a power loss.
///
/// The scratch file is synced before the rename. The folder is synced after
/// it, because the rename lives in the folder entry. The parent of the folder
/// is synced on every put. An earlier put can create the folder and then fail
/// before its parent sync, so a folder that exists is not proof that its entry
/// is durable. The parent of each folder that this call creates is synced too.
/// Any sync error fails the put, so a put never acknowledges a blob that can
/// vanish.
///
/// On Windows the rename is not durable, because a directory cannot be opened
/// for a sync there.
fn write_durable(folder: &Path, path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut created = Vec::new();
    let mut probe = folder;
    while !probe.exists() {
        created.push(probe);
        match probe.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => probe = parent,
            _ => break,
        }
    }
    std::fs::create_dir_all(folder)?;
    let tmp = scratch_path(path);
    let written = (|| {
        write_private(&tmp, bytes)?;
        std::fs::rename(&tmp, path)?;
        #[cfg(test)]
        sync_log::hit(sync_log::Kind::Rename, path)?;
        let mut dirs = vec![folder];
        dirs.extend(created.iter().copied());
        dirs.extend(folder.parent());
        for new_dir in &created {
            dirs.extend(new_dir.parent());
        }
        let mut done: Vec<&Path> = Vec::new();
        for dir in dirs {
            if done.contains(&dir) {
                continue;
            }
            done.push(dir);
            sync_dir(if dir.as_os_str().is_empty() {
                Path::new(".")
            } else {
                dir
            })?;
        }
        Ok(())
    })();
    if written.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    written
}

/// A scratch path next to `path` that no other put uses.
///
/// Two attempts of one turn can save the same key at the same time, also on
/// two hosts that share the folder. A shared scratch file would mix their
/// bytes. A process id is not enough, because each container can run as
/// process 1, so the name holds a random UUID.
fn scratch_path(path: &Path) -> PathBuf {
    let tag = autumn_harvest::uuid::Uuid::new_v4().simple();
    path.with_extension(format!("tmp-{tag}"))
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
            let folder = self.folder(scope);
            tokio::task::spawn_blocking(move || write_durable(&folder, &path, &bytes))
                .await
                .map_err(|e| {
                    AgentError::new(
                        ErrorKind::Unavailable,
                        format!("the workspace write stopped: {e}"),
                    )
                })?
                .map_err(|e| io_error(&e))
        })
    }

    fn get<'a>(
        &'a self,
        scope: &'a str,
        digest: &'a str,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, AgentError>> {
        Box::pin(async move {
            match tokio::fs::read(self.entry(scope, digest)?).await {
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
                    && self.entry(scope, name).is_ok()
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
            match tokio::fs::remove_file(self.entry(scope, digest)?).await {
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
        let Some(bytes) = self.blobs.get(&mount.scope, &head.key()).await? else {
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

    /// Save `workspace` in `scope` at `generation`, and return its
    /// reference.
    ///
    /// A save at a generation above 0 also checks for a reset marker above
    /// that generation after its put. It then deletes its own key, because
    /// a later reset cleared the scope (issue #2315). Generation 0 has no
    /// order, so a save at 0 skips the check.
    ///
    /// # Errors
    ///
    /// Returns a [`ErrorKind::Tool`] error when the workspace is over a cap
    /// or a later reset cleared the scope. Returns an error when the codec
    /// or the store fails.
    pub async fn save(
        &self,
        scope: &str,
        generation: u64,
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
        let head = WorkspaceRef {
            digest,
            bytes,
            files: u32::try_from(workspace.files.len()).unwrap_or(u32::MAX),
            generation,
        };
        self.blobs.put(scope, &head.key(), encoded).await?;
        // A stale attempt can save after a reset of a later op (issue
        // #2315). The reset puts its marker before it lists. So either the
        // reset lists this key and deletes it, or this save sees the marker.
        if generation > 0
            && self
                .blobs
                .list(scope)
                .await?
                .iter()
                .filter_map(|key| marker_generation(key))
                .any(|bound| bound > generation)
        {
            self.blobs.delete(scope, &head.key()).await?;
            return Err(AgentError::new(
                ErrorKind::Tool,
                "a reset of the workspace ran after this call started, so the call saved nothing",
            ));
        }
        Ok(head)
    }

    /// Delete every snapshot of `scope` but `keep`, of the generation of
    /// `keep` or older. Delete each scratch name of that generation or
    /// older too, also one of `keep`. Returns the number deleted.
    ///
    /// A late attempt that runs after a newer op deletes no newer head,
    /// because a newer head has a newer generation. For the same reason it
    /// deletes no scratch file of a live put of a newer op.
    ///
    /// # Errors
    ///
    /// Returns an error when the store fails.
    pub async fn retain(&self, scope: &str, keep: &WorkspaceRef) -> Result<u64, AgentError> {
        let head = keep.key();
        let old = |generation: u64| generation <= keep.generation;
        self.delete_where(scope, |key| {
            (key != head && key_generation(key).is_some_and(old))
                || scratch_generation(key).is_some_and(old)
        })
        .await
    }

    /// Leave a reset marker at the generation `below`. Then delete every
    /// snapshot, reset marker and scratch name of `scope` below `below`, and
    /// every listed name in another form. Returns the number deleted.
    ///
    /// A name in another form is never a head, so an erasure removes it too.
    /// The marker is empty. It keeps the bound for
    /// [`Self::highest_generation`], so a new life starts above it. The
    /// marker comes first, so a stale save that lands later sees it and
    /// deletes its own key (issue #2315).
    ///
    /// # Errors
    ///
    /// Returns an error when the store fails.
    pub async fn reset(&self, scope: &str, below: u64) -> Result<u64, AgentError> {
        self.blobs
            .put(scope, &reset_marker(below), Vec::new())
            .await?;
        self.delete_where(scope, |key| {
            name_generation(key).is_none_or(|generation| generation < below)
        })
        .await
    }

    /// The highest generation of a snapshot, a reset marker or a scratch name
    /// in `scope`, or 0 when it has none.
    ///
    /// A new life of an agent starts above it, so no attempt of an old life
    /// deletes a key of the new one. The marker counts too: after a reset
    /// empties the scope, a late attempt of that reset still deletes only
    /// keys below its bound.
    ///
    /// # Errors
    ///
    /// Returns an error when the store fails.
    pub async fn highest_generation(&self, scope: &str) -> Result<u64, AgentError> {
        let keys = self.blobs.list(scope).await?;
        Ok(keys
            .iter()
            .filter_map(|key| name_generation(key))
            .max()
            .unwrap_or(0))
    }

    /// Delete every key and scratch name of `scope`, of any generation.
    /// Returns the number deleted.
    ///
    /// Call it only when no run uses the scope. It can delete the scratch
    /// file of a live put.
    ///
    /// # Errors
    ///
    /// Returns an error when the store fails.
    pub async fn erase(&self, scope: &str) -> Result<u64, AgentError> {
        self.delete_where(scope, |_| true).await
    }

    /// Delete each key of `scope` that `doomed` selects.
    async fn delete_where(
        &self,
        scope: &str,
        doomed: impl Fn(&str) -> bool,
    ) -> Result<u64, AgentError> {
        let mut deleted = 0;
        for key in self.blobs.list(scope).await? {
            if doomed(&key) && self.blobs.delete(scope, &key).await? {
                deleted += 1;
            }
        }
        Ok(deleted)
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

    /// A snapshot key of `generation` whose digest repeats `fill`.
    fn key_at(generation: u64, fill: &str) -> String {
        format!("{generation:016x}-{}", fill.repeat(64))
    }

    /// The file that a put of `key` leaves when its future drops.
    fn plant_scratch(blobs: &DirWorkspaceBlobs, scope: &str, key: &str, bytes: &[u8]) -> PathBuf {
        std::fs::create_dir_all(blobs.folder(scope)).unwrap();
        let path = scratch_path(&blobs.file(scope, key).unwrap());
        std::fs::write(&path, bytes).unwrap();
        path
    }

    fn dir_store(dir: &tempfile::TempDir) -> (DirWorkspaceBlobs, WorkspaceStore) {
        let blobs = DirWorkspaceBlobs::new(dir.path());
        let store = WorkspaceStore::new(Arc::new(blobs.clone()), PayloadCodecs::default());
        (blobs, store)
    }

    fn one_file(text: &str) -> Workspace {
        let mut workspace = Workspace::default();
        workspace.write("a", text).unwrap();
        workspace
    }

    /// Blobs in memory that record each call, in order.
    #[derive(Debug, Default)]
    struct Logged {
        inner: InMemoryWorkspaceBlobs,
        calls: Mutex<Vec<String>>,
    }

    impl Logged {
        fn log(&self, call: String) {
            self.calls.lock().unwrap().push(call);
        }
    }

    impl WorkspaceBlobs for Logged {
        fn put<'a>(
            &'a self,
            scope: &'a str,
            digest: &'a str,
            bytes: Vec<u8>,
        ) -> BoxFuture<'a, Result<(), AgentError>> {
            self.log(format!("put {digest}"));
            self.inner.put(scope, digest, bytes)
        }

        fn get<'a>(
            &'a self,
            scope: &'a str,
            digest: &'a str,
        ) -> BoxFuture<'a, Result<Option<Vec<u8>>, AgentError>> {
            self.inner.get(scope, digest)
        }

        fn list<'a>(&'a self, scope: &'a str) -> BoxFuture<'a, Result<Vec<String>, AgentError>> {
            self.log("list".to_owned());
            self.inner.list(scope)
        }

        fn delete<'a>(
            &'a self,
            scope: &'a str,
            digest: &'a str,
        ) -> BoxFuture<'a, Result<bool, AgentError>> {
            self.log(format!("delete {digest}"));
            self.inner.delete(scope, digest)
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_put_syncs_the_file_then_renames_then_syncs_the_folder() {
        use sync_log::Kind::{Dir, File, Rename};
        let dir = tempfile::tempdir().unwrap();
        let blobs = DirWorkspaceBlobs::new(dir.path().join("blobs"));
        let key = key_at(1, "a");
        blobs.put("s", &key, b"x".to_vec()).await.unwrap();
        let folder = blobs.folder("s");
        let kinds: Vec<_> = sync_log::take_below(dir.path())
            .into_iter()
            .map(|(kind, path)| (kind, path.starts_with(&folder), path == folder))
            .collect();
        assert_eq!(kinds[0], (File, true, false), "the scratch file first");
        assert_eq!(kinds[1], (Rename, true, false), "the rename second");
        assert_eq!(kinds[2], (Dir, true, true), "the folder third");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_put_syncs_the_parent_of_each_folder_that_it_creates() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("a").join("b").join("blobs");
        let blobs = DirWorkspaceBlobs::new(&root);
        blobs
            .put("s", &key_at(1, "a"), b"x".to_vec())
            .await
            .unwrap();
        let synced: Vec<_> = sync_log::take_below(dir.path())
            .into_iter()
            .filter(|(kind, _)| *kind == sync_log::Kind::Dir)
            .map(|(_, path)| path)
            .collect();
        for expected in [
            blobs.folder("s"),
            root.clone(),
            root.parent().unwrap().to_path_buf(),
            dir.path().join("a"),
            dir.path().to_path_buf(),
        ] {
            assert!(synced.contains(&expected), "{expected:?} is synced");
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_put_into_an_existing_folder_still_syncs_the_folder_and_its_parent() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("blobs");
        let blobs = DirWorkspaceBlobs::new(&root);
        blobs
            .put("s", &key_at(1, "a"), b"x".to_vec())
            .await
            .unwrap();
        sync_log::take_below(dir.path());
        blobs
            .put("s", &key_at(2, "a"), b"y".to_vec())
            .await
            .unwrap();
        let dirs: Vec<_> = sync_log::take_below(dir.path())
            .into_iter()
            .filter(|(kind, _)| *kind == sync_log::Kind::Dir)
            .map(|(_, path)| path)
            .collect();
        assert_eq!(dirs, [blobs.folder("s"), root], "no other directory");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_failed_sync_fails_the_put_and_leaves_no_scratch_file() {
        for kind in [sync_log::Kind::File, sync_log::Kind::Dir] {
            let dir = tempfile::tempdir().unwrap();
            let blobs = DirWorkspaceBlobs::new(dir.path().join("blobs"));
            sync_log::fail(kind, dir.path());
            let err = blobs.put("s", &key_at(1, "a"), b"x".to_vec()).await;
            assert_eq!(err.unwrap_err().kind(), ErrorKind::Unavailable, "{kind:?}");
            let names = blobs.list("s").await.unwrap();
            assert!(
                names.iter().all(|n| scratch_owner(n).is_none()),
                "{kind:?}: {names:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_reset_deletes_scratch_files_below_its_bound_and_keeps_a_newer_one() {
        let dir = tempfile::tempdir().unwrap();
        let (blobs, store) = dir_store(&dir);
        let head = store.save("s", 1, &one_file("secret")).await.unwrap();
        let dropped = plant_scratch(&blobs, "s", &head.key(), b"secret");
        let marker = plant_scratch(&blobs, "s", &reset_marker(2), b"");
        let live = plant_scratch(&blobs, "s", &key_at(5, "e"), b"newer");
        assert_eq!(store.reset("s", 3).await.unwrap(), 3);
        assert!(!dropped.exists() && !marker.exists());
        assert!(live.exists(), "a put of a newer op keeps its scratch file");
    }

    #[tokio::test]
    async fn a_retain_deletes_scratch_files_up_to_the_head_generation() {
        let dir = tempfile::tempdir().unwrap();
        let (blobs, store) = dir_store(&dir);
        let head = store.save("s", 2, &one_file("x")).await.unwrap();
        let older = plant_scratch(&blobs, "s", &key_at(1, "d"), b"x");
        let stale = plant_scratch(&blobs, "s", &key_at(2, "d"), b"x");
        let newer = plant_scratch(&blobs, "s", &key_at(3, "d"), b"x");
        assert_eq!(store.retain("s", &head).await.unwrap(), 2);
        assert!(!older.exists() && !stale.exists());
        assert!(newer.exists());
        assert!(blobs.get("s", &head.key()).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn the_floor_counts_a_scratch_file_and_erase_deletes_it() {
        let dir = tempfile::tempdir().unwrap();
        let (blobs, store) = dir_store(&dir);
        let path = plant_scratch(&blobs, "s", &key_at(9, "d"), b"x");
        assert_eq!(store.highest_generation("s").await.unwrap(), 9);
        assert_eq!(store.erase("s").await.unwrap(), 1);
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn the_dir_store_deletes_a_scratch_name_but_never_puts_one() {
        let dir = tempfile::tempdir().unwrap();
        let blobs = DirWorkspaceBlobs::new(dir.path());
        let path = plant_scratch(&blobs, "s", &key_at(1, "d"), b"x");
        let name = path.file_name().unwrap().to_str().unwrap().to_owned();
        assert_eq!(blobs.list("s").await.unwrap(), std::slice::from_ref(&name));
        assert!(blobs.get("s", &name).await.unwrap().is_some());
        assert!(blobs.put("s", &name, Vec::new()).await.is_err());
        let key = key_at(1, "d");
        for bad in [
            format!("{key}.tmp-../../x"),
            format!("{key}.tmp-{}", "A".repeat(32)),
            format!("{key}.tmp-{}", "a".repeat(31)),
            format!("{}.tmp-{}", "a".repeat(64), "a".repeat(32)),
            format!("{name}.tmp-{}", "a".repeat(32)),
        ] {
            assert!(blobs.delete("s", &bad).await.is_err(), "{bad}");
        }
        assert!(blobs.delete("s", &name).await.unwrap());
    }

    #[tokio::test]
    async fn a_save_below_a_reset_marker_deletes_its_own_key() {
        let blobs = Arc::new(InMemoryWorkspaceBlobs::default());
        let store = store(&blobs);
        store.reset("s", 5).await.unwrap();
        let late = store.save("s", 3, &one_file("late")).await;
        assert_eq!(late.map_err(|err| err.kind()), Err(ErrorKind::Tool));
        assert_eq!(blobs.list("s").await.unwrap(), [reset_marker(5)]);
        assert!(
            store.save("s", 6, &one_file("late")).await.is_ok(),
            "a later segment saves"
        );
        // A plain task saves at generation 0, which has no order.
        assert!(store.save("s", 0, &one_file("plain")).await.is_ok());
    }

    #[tokio::test]
    async fn a_reset_puts_its_marker_before_it_lists() {
        let blobs = Arc::new(Logged::default());
        let store = WorkspaceStore::new(blobs.clone(), PayloadCodecs::default());
        store.reset("s", 4).await.unwrap();
        let calls = blobs.calls.lock().unwrap().clone();
        assert_eq!(
            calls.first(),
            Some(&format!("put {}", reset_marker(4))),
            "{calls:?}"
        );
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
        let head = store.save("agent:a", 1, &workspace).await.unwrap();
        assert_eq!((head.files, head.bytes), (2, 15));
        let again = store.save("agent:a", 1, &workspace).await.unwrap();
        assert_eq!(head, again, "equal content gives an equal digest");
        let later = store.save("agent:a", 2, &workspace).await.unwrap();
        assert_eq!(later.digest, head.digest);
        assert_ne!(later.key(), head.key(), "a generation gives its own key");
        let mount = WorkspaceMount {
            scope: "agent:a".to_owned(),
            head: Some(head),
            generation: 0,
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
        let head = store.save("s", 1, &workspace).await.unwrap();
        workspace.write("a", "two").unwrap();
        let other = store.save("s", 1, &workspace).await.unwrap();
        let swapped = blobs.get("s", &other.key()).await.unwrap().unwrap();
        blobs.put("s", &head.key(), swapped).await.unwrap();
        let mount = WorkspaceMount {
            scope: "s".to_owned(),
            head: Some(head),
            generation: 0,
        };
        let err = store.load(&mount).await.unwrap_err();
        assert!(err.message().contains("digest"), "{err}");
    }

    #[tokio::test]
    async fn retain_keeps_the_head_generation_and_erase_keeps_nothing() {
        let blobs = Arc::new(InMemoryWorkspaceBlobs::default());
        let store = store(&blobs);
        let mut heads = Vec::new();
        for n in 0..3 {
            let mut workspace = Workspace::default();
            workspace.write("n", n.to_string()).unwrap();
            heads.push(store.save("s", n, &workspace).await.unwrap());
        }
        let mut other = Workspace::default();
        other.write("x", "y").unwrap();
        store.save("t", 0, &other).await.unwrap();
        assert_eq!(store.retain("s", &heads[2]).await.unwrap(), 2);
        assert_eq!(blobs.list("s").await.unwrap(), [heads[2].key()]);
        assert_eq!(store.erase("s").await.unwrap(), 1);
        assert_eq!(blobs.list("s").await.unwrap(), Vec::<String>::new());
        assert_eq!(
            blobs.list("t").await.unwrap().len(),
            1,
            "another scope stays"
        );
    }

    #[tokio::test]
    async fn a_stale_retain_never_deletes_a_newer_head() {
        let blobs = Arc::new(InMemoryWorkspaceBlobs::default());
        let store = store(&blobs);
        let mut workspace = Workspace::default();
        workspace.write("a", "one").unwrap();
        let first = store.save("s", 1, &workspace).await.unwrap();
        workspace.write("a", "two").unwrap();
        let second = store.save("s", 2, &workspace).await.unwrap();
        // The retention that the first op scheduled runs late.
        store.retain("s", &first).await.unwrap();
        let mount = WorkspaceMount {
            scope: "s".to_owned(),
            head: Some(second),
            generation: 0,
        };
        let loaded = store.load(&mount).await.unwrap();
        assert_eq!(loaded.read_text("a"), Some("two"));
    }

    #[tokio::test]
    async fn a_newer_head_with_old_content_survives_a_stale_retain() {
        let blobs = Arc::new(InMemoryWorkspaceBlobs::default());
        let store = store(&blobs);
        let mut workspace = Workspace::default();
        workspace.write("a", "one").unwrap();
        let old = store.save("s", 1, &workspace).await.unwrap();
        workspace.write("a", "two").unwrap();
        let middle = store.save("s", 2, &workspace).await.unwrap();
        // The agent writes the old content back.
        workspace.write("a", "one").unwrap();
        let restored = store.save("s", 3, &workspace).await.unwrap();
        assert_eq!(restored.digest, old.digest);
        store.retain("s", &middle).await.unwrap();
        let mount = WorkspaceMount {
            scope: "s".to_owned(),
            head: Some(restored),
            generation: 0,
        };
        assert_eq!(
            store.load(&mount).await.unwrap().read_text("a"),
            Some("one")
        );
        assert!(blobs.get("s", &old.key()).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn a_stale_reset_never_deletes_a_later_head() {
        let blobs = Arc::new(InMemoryWorkspaceBlobs::default());
        let store = store(&blobs);
        let mut workspace = Workspace::default();
        workspace.write("a", "one").unwrap();
        let before = store.save("s", 1, &workspace).await.unwrap();
        assert_eq!(store.reset("s", 2).await.unwrap(), 1);
        let after = store.save("s", 3, &workspace).await.unwrap();
        // A late attempt of the reset runs again.
        assert_eq!(store.reset("s", 2).await.unwrap(), 0);
        assert!(blobs.get("s", &before.key()).await.unwrap().is_none());
        assert!(blobs.get("s", &after.key()).await.unwrap().is_some());
        assert_eq!(store.highest_generation("s").await.unwrap(), 3);
    }

    #[tokio::test]
    async fn retain_keeps_a_foreign_key_and_reset_deletes_it() {
        let blobs = Arc::new(InMemoryWorkspaceBlobs::default());
        let store = store(&blobs);
        let mut workspace = Workspace::default();
        workspace.write("a", "one").unwrap();
        let head = store.save("s", 1, &workspace).await.unwrap();
        blobs.put("s", "foreign", b"x".to_vec()).await.unwrap();
        store.retain("s", &head).await.unwrap();
        assert!(blobs.get("s", "foreign").await.unwrap().is_some());
        assert_eq!(store.reset("s", 2).await.unwrap(), 2);
        assert_eq!(blobs.list("s").await.unwrap(), [reset_marker(2)]);
    }

    #[tokio::test]
    async fn a_reset_marker_keeps_the_floor_of_an_empty_scope() {
        let blobs = Arc::new(InMemoryWorkspaceBlobs::default());
        let store = store(&blobs);
        assert_eq!(store.highest_generation("s").await.unwrap(), 0);
        let mut workspace = Workspace::default();
        workspace.write("a", "one").unwrap();
        store.save("s", 4, &workspace).await.unwrap();
        store.reset("s", 9).await.unwrap();
        // The scope holds no snapshot, but a new life still starts above 9.
        assert_eq!(store.highest_generation("s").await.unwrap(), 9);
        // A later reset replaces the marker. A late older one keeps both.
        store.reset("s", 12).await.unwrap();
        store.reset("s", 9).await.unwrap();
        assert_eq!(store.highest_generation("s").await.unwrap(), 12);
        assert_eq!(store.erase("s").await.unwrap(), 2, "erase deletes markers");
    }

    #[test]
    fn a_key_is_the_generation_and_the_digest() {
        let head = WorkspaceRef {
            digest: "ab".repeat(32),
            bytes: 1,
            files: 1,
            generation: 0x1f,
        };
        assert_eq!(head.key(), format!("000000000000001f-{}", "ab".repeat(32)));
        assert_eq!(key_generation(&head.key()), Some(0x1f));
        assert_eq!(key_generation(&"ab".repeat(32)), None, "a bare digest");
        assert_eq!(key_generation(&format!("{}-x", "0".repeat(16))), None);
        assert_eq!(key_generation(&head.key().to_uppercase()), None);
        assert_eq!(marker_generation(&reset_marker(0x1f)), Some(0x1f));
        assert_eq!(marker_generation(&head.key()), None);
        assert_eq!(key_generation(&reset_marker(0x1f)), None);
    }

    #[tokio::test]
    async fn a_workspace_over_a_cap_is_a_tool_error() {
        let blobs = Arc::new(InMemoryWorkspaceBlobs::default());
        let store = store(&blobs).max_bytes(4).max_files(1);
        let mut workspace = Workspace::default();
        workspace.write("a", "12345").unwrap();
        let err = store.save("s", 1, &workspace).await.unwrap_err();
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
    async fn the_dir_store_keeps_scopes_apart_and_refuses_odd_keys() {
        let dir = tempfile::tempdir().unwrap();
        let blobs = DirWorkspaceBlobs::new(dir.path());
        let digest = format!("{}-{}", "0".repeat(16), "a".repeat(64));
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
        assert!(blobs.get("agent:x/y", &"a".repeat(64)).await.is_err());
        let marker = reset_marker(7);
        blobs.put("agent:x/y", &marker, Vec::new()).await.unwrap();
        assert!(blobs.delete("agent:x/y", &marker).await.unwrap());
        assert!(blobs.delete("agent:x/y", &digest).await.unwrap());
        assert!(!blobs.delete("agent:x/y", &digest).await.unwrap());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn the_dir_store_writes_files_that_only_the_owner_reads() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::tempdir().unwrap();
        let blobs = DirWorkspaceBlobs::new(dir.path());
        let digest = format!("{}-{}", "0".repeat(16), "b".repeat(64));
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
        let digest = format!("{}-{}", "0".repeat(16), "c".repeat(64));
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
