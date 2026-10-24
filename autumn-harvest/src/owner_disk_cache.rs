//! The owner-local disk cache of decided histories (issue #2258).
//!
//! A decision owner keeps the history of each live run in a local file. A
//! cold decision then reads the file and the Postgres delta, not the full
//! Postgres history. [ADR 0012](../../docs/adr/0012-owner-disk-cache.md)
//! records the design.
//!
//! ## Rules
//!
//! - **Local disk is a cache only.** Postgres stays the source of truth. Any
//!   doubt about an entry purges it, and the decision loads from Postgres.
//! - **One file per partition.** The file `s{shard}-p{partition}.hcache`
//!   uses the event-table shape of `autumn-harvest-sqlite`:
//!   `harvest_events(exec_id, seq, event_json)`.
//! - **Every row is sealed.** AES-256-GCM seals each row under a node key.
//!   The key lives in memory only. A crash therefore leaves ciphertext whose
//!   key no longer exists. The associated data binds the partition, the run
//!   and the sequence, so a changed, moved or swapped row fails to open.
//! - **Decoded events only.** The cache stores events after the payload
//!   codec decodes them. No codec ciphertext reaches the disk, so a retired
//!   codec key leaves nothing here.
//! - **Disposable.** [`OwnerDiskCache::open`] deletes every cache file in
//!   the directory. A drop does the same. The cache needs no durable
//!   inventory.
//! - **One process per directory.** An exclusive OS lock on a sidecar file
//!   keeps a second process out, as `autumn-harvest-sqlite/src/lock.rs`
//!   does. The kernel releases the lock when the process exits.
//! - **Validity stamps.** The file stamp holds the owner epoch and the codec
//!   key generation. The run stamp holds the run state, the next event id
//!   and the hash of the last event. Any mismatch purges.
//! - **Freed pages hold zeros.** `secure_delete` zeroes deleted rows. The
//!   journal and temporary tables stay in memory.
//!
//! All methods block on file I/O. The worker calls them in
//! `spawn_blocking`.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use aes_gcm::aead::{AeadInPlace, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce, Tag};
use rand::RngCore as _;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::cache::{CachedWorkflowState, HistoryBytesMark};
use crate::decision_owner::{OwnerEpoch, OwnerKey};
use crate::error::{HarvestError, HarvestResult};
use crate::event::WorkflowEvent;
use crate::types::ShardId;

/// The extension of each cache file.
pub const CACHE_FILE_EXTENSION: &str = "hcache";

/// The name of the sidecar lock file in the cache directory.
pub const LOCK_FILE_NAME: &str = "owner-disk-cache.lock";

/// The format version in each file stamp. Version 2 adds the entity
/// projection table (issue #2260).
const FORMAT_VERSION: u32 = 2;

/// The run state that the cache stores. Only a live run has an entry.
pub const CACHED_RUN_STATE: &str = "RUNNING";

/// Length of a GCM nonce.
const NONCE_BYTES: usize = 12;

/// Length of a GCM tag.
const TAG_BYTES: usize = 16;

/// The schema of each cache file.
const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS harvest_cache_stamp (
    id     INTEGER PRIMARY KEY CHECK (id = 1),
    sealed BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS harvest_cache_runs (
    exec_id TEXT PRIMARY KEY,
    sealed  BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS harvest_events (
    exec_id    TEXT NOT NULL,
    seq        INTEGER NOT NULL,
    event_json BLOB NOT NULL,
    PRIMARY KEY (exec_id, seq)
);
CREATE TABLE IF NOT EXISTS harvest_entity_projection (
    exec_id TEXT PRIMARY KEY,
    sealed  BLOB NOT NULL
);
";

/// The pragmas of each cache file.
///
/// - `journal_mode = MEMORY` and `temp_store = MEMORY` keep old pages off
///   the disk.
/// - `secure_delete = ON` zeroes the bytes of each deleted row.
/// - `synchronous = OFF` skips `fsync`. A crash wipes the cache anyway.
const PRAGMAS: &str = "
PRAGMA journal_mode = MEMORY;
PRAGMA temp_store = MEMORY;
PRAGMA secure_delete = ON;
PRAGMA synchronous = OFF;
";

/// The validity scope of one disk entry: the partition, the tenure epoch
/// and the codec key generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiskSlot {
    /// The shard and partition of the run.
    pub key: OwnerKey,
    /// The epoch of the tenure that decides the run.
    pub epoch: OwnerEpoch,
    /// [`crate::payload_codec::KeyEpoch::generation`] of the codec registry.
    pub codec_generation: u64,
}

/// One disk entry that passed every check of [`OwnerDiskCache::load`].
#[derive(Debug)]
pub struct DiskEntry {
    /// The decoded events and the next event id.
    pub state: CachedWorkflowState,
    /// The hash of the last event. The caller compares it with Postgres.
    pub last_event_hash: [u8; 32],
    /// The stored-history byte mark of the run, if the decision had one.
    pub(crate) history_bytes: Option<HistoryBytesMark>,
}

/// The result of [`OwnerDiskCache::load`].
#[derive(Debug)]
pub enum DiskLoad {
    /// The entry passed every check.
    Hit(DiskEntry),
    /// The cache holds no entry for the run.
    Miss,
    /// A stamp did not match. The cache purged the entry or the file.
    Stale,
    /// A row did not open or decode. The cache purged the entry or the file.
    Corrupt,
}

impl DiskLoad {
    /// The outcome label of `harvest.workflow.disk_cache`.
    #[must_use]
    pub const fn outcome(&self) -> &'static str {
        match self {
            Self::Hit(_) => "hit",
            Self::Miss => "miss",
            Self::Stale => "stale",
            Self::Corrupt => "corrupt",
        }
    }
}

/// The result of [`OwnerDiskCache::store`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskStore {
    /// The cache appended `appended` events. A full rewrite counts each one.
    Written {
        /// The number of event rows written.
        appended: usize,
    },
    /// The tenure ended, or the state has a gap. The cache wrote nothing and
    /// purged any old entry.
    Refused,
    /// A write failed. The cache purged the entry.
    Failed,
}

/// One entity snapshot in the projection table (issue #2260).
///
/// The row is sealed. The entity key is not in it, so the disk holds no
/// key in plaintext.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectedEntity {
    /// The history position of the snapshot in its run.
    pub event_id: i32,
    /// The snapshot ended the entity.
    pub deleted: bool,
    /// The decoded state and counters.
    pub snapshot: crate::entity::EntitySnapshot<serde_json::Value>,
}

/// The result of [`OwnerDiskCache::load_entity`].
#[derive(Debug)]
pub enum EntityLoad {
    /// The row passed every check.
    Hit(ProjectedEntity),
    /// The cache holds no row for the run.
    Miss,
    /// A stamp did not match, or the file is gone. The cache purged the
    /// row or the file.
    Stale,
    /// The row did not open or decode. The cache purged it.
    Corrupt,
}

/// The file stamp. It is sealed in `harvest_cache_stamp`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct FileStamp {
    format: u32,
    shard: i32,
    partition: u16,
    epoch: i64,
    codec_generation: u64,
}

impl FileStamp {
    const fn of(slot: &DiskSlot) -> Self {
        Self {
            format: FORMAT_VERSION,
            shard: slot.key.shard.as_i32(),
            partition: slot.key.partition.get(),
            epoch: slot.epoch.get(),
            codec_generation: slot.codec_generation,
        }
    }
}

/// The run stamp. It is sealed in `harvest_cache_runs`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RunStamp {
    state: String,
    next_event_id: i32,
    last_event_hash: [u8; 32],
    history_bytes: Option<(u64, i32, u32)>,
}

/// The node key and the nonce source.
///
/// `aes-gcm` zeroes the round keys on drop. The raw key is a
/// [`zeroize::Zeroizing`] buffer that drops at the end of [`Sealer::new`].
struct Sealer {
    cipher: Aes256Gcm,
    prefix: [u8; 4],
    counter: AtomicU64,
}

impl Sealer {
    fn new() -> Self {
        let mut key = zeroize::Zeroizing::new([0_u8; 32]);
        rand::rngs::OsRng.fill_bytes(key.as_mut_slice());
        let mut prefix = [0_u8; 4];
        rand::rngs::OsRng.fill_bytes(&mut prefix);
        Self {
            cipher: Aes256Gcm::new(key.as_slice().into()),
            prefix,
            counter: AtomicU64::new(0),
        }
    }

    /// The next nonce: the random prefix, then a counter. A nonce never
    /// repeats under one key. The counter refuses to wrap.
    fn next_nonce(&self) -> Option<[u8; NONCE_BYTES]> {
        let mut count = self.counter.load(Ordering::Relaxed);
        loop {
            let next = count.checked_add(1)?;
            match self.counter.compare_exchange_weak(
                count,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(seen) => count = seen,
            }
        }
        let mut nonce = [0_u8; NONCE_BYTES];
        nonce[..4].copy_from_slice(&self.prefix);
        nonce[4..].copy_from_slice(&count.to_be_bytes());
        Some(nonce)
    }

    /// Seal `plaintext` as `nonce || ciphertext || tag`.
    fn seal(&self, aad: &[u8], plaintext: &[u8]) -> Option<Vec<u8>> {
        let nonce = self.next_nonce()?;
        let mut body = plaintext.to_vec();
        let tag = self
            .cipher
            .encrypt_in_place_detached(Nonce::from_slice(&nonce), aad, &mut body)
            .ok()?;
        let mut sealed = Vec::with_capacity(NONCE_BYTES + body.len() + TAG_BYTES);
        sealed.extend_from_slice(&nonce);
        sealed.extend_from_slice(&body);
        sealed.extend_from_slice(&tag);
        Some(sealed)
    }

    /// Open a sealed row. `None` means a changed row or a wrong key.
    fn open(&self, aad: &[u8], sealed: &[u8]) -> Option<Vec<u8>> {
        if sealed.len() < NONCE_BYTES + TAG_BYTES {
            return None;
        }
        let (nonce, rest) = sealed.split_at(NONCE_BYTES);
        let (body, tag) = rest.split_at(rest.len() - TAG_BYTES);
        let mut body = body.to_vec();
        self.cipher
            .decrypt_in_place_detached(
                Nonce::from_slice(nonce),
                aad,
                &mut body,
                Tag::from_slice(tag),
            )
            .ok()?;
        Some(body)
    }
}

/// The associated data of one sealed row.
fn aad(kind: &str, key: OwnerKey, exec: Option<Uuid>, seq: Option<i64>) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    out.extend_from_slice(b"harvest-owner-disk-cache/v1/");
    out.extend_from_slice(kind.as_bytes());
    out.extend_from_slice(&key.shard.as_i32().to_be_bytes());
    out.extend_from_slice(&key.partition.get().to_be_bytes());
    if let Some(exec) = exec {
        out.extend_from_slice(exec.as_bytes());
    }
    if let Some(seq) = seq {
        out.extend_from_slice(&seq.to_be_bytes());
    }
    out
}

/// The hash of one decoded event: SHA-256 of its JSON form.
///
/// # Errors
///
/// Returns [`HarvestError::Serialization`] when the event does not encode.
pub fn event_hash(event: &WorkflowEvent) -> HarvestResult<[u8; 32]> {
    let json = serde_json::to_vec(event)?;
    Ok(Sha256::digest(&json).into())
}

/// The file name of the partition `key`.
#[must_use]
pub fn file_name(key: OwnerKey) -> String {
    format!(
        "s{}-p{}.{CACHE_FILE_EXTENSION}",
        key.shard.as_i32(),
        key.partition.get()
    )
}

/// Whether `name` is a cache file or one of its SQLite side files.
fn is_cache_file(name: &str) -> bool {
    let base = name
        .strip_suffix("-journal")
        .or_else(|| name.strip_suffix("-wal"))
        .or_else(|| name.strip_suffix("-shm"))
        .unwrap_or(name);
    base.starts_with('s')
        && Path::new(base)
            .extension()
            .is_some_and(|ext| ext == CACHE_FILE_EXTENSION)
}

/// Delete a cache file and its side files. A missing file is not an error.
fn remove_cache_file(path: &Path) {
    for suffix in ["", "-journal", "-wal", "-shm"] {
        let mut name = path.as_os_str().to_os_string();
        name.push(suffix);
        if let Err(err) = std::fs::remove_file(&name)
            && err.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(path = ?name, error = %err, "owner disk cache: a file delete failed");
        }
    }
}

/// The identity of a file on disk: the device and the inode on Unix.
type FileIdentity = (u64, u64);

/// The identity of the file at `path`, or `None` when it is gone.
fn file_identity(path: &Path) -> Option<FileIdentity> {
    let meta = std::fs::metadata(path).ok()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Some((meta.dev(), meta.ino()))
    }
    #[cfg(not(unix))]
    {
        let _ = meta;
        Some((0, 0))
    }
}

/// One partition file and its connection.
struct Partition {
    path: PathBuf,
    conn: Option<Connection>,
    /// The identity of the file that `conn` opened. A deleted or replaced
    /// file has another identity, or none (issue #2260).
    identity: Option<FileIdentity>,
    /// A drop sets this. An operation that still holds the partition then
    /// writes nothing.
    closed: bool,
}

/// The owner-local disk cache of one process. See the module docs.
pub struct OwnerDiskCache {
    dir: PathBuf,
    // Held only for its drop, which releases the OS lock.
    #[allow(dead_code)]
    lock: File,
    sealer: Sealer,
    /// Lock order: take this map lock, clone the partition, release the map
    /// lock, then lock the partition. A partition lock may take `index`.
    partitions: Mutex<HashMap<OwnerKey, Arc<Mutex<Partition>>>>,
    /// The partition of each cached run. It routes a purge by run id.
    index: Mutex<HashMap<Uuid, OwnerKey>>,
    /// The partition and the snapshot event id of each projected entity
    /// run (issue #2260).
    entity_index: Mutex<HashMap<Uuid, (OwnerKey, i32)>>,
    /// The runs that an evict wake purged, and when. A store refuses such a
    /// run for [`EVICT_HOLD`], so an in-flight store cannot write it back.
    evicted: Mutex<HashMap<Uuid, std::time::Instant>>,
}

/// How long a store refuses a run after an evict wake purged it.
pub const EVICT_HOLD: std::time::Duration = std::time::Duration::from_secs(30);

impl std::fmt::Debug for OwnerDiskCache {
    /// Prints the directory only. The node key never prints.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnerDiskCache")
            .field("dir", &self.dir)
            .finish_non_exhaustive()
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl OwnerDiskCache {
    /// Open the cache in `dir`, and delete every cache file in it.
    ///
    /// The open creates `dir` when it is missing, with the mode `0700` on
    /// Unix. On Unix it refuses an existing directory that its group or other
    /// users can reach. It never changes the mode of an existing directory.
    /// It takes an exclusive lock on [`LOCK_FILE_NAME`] first, so a second
    /// process cannot delete the files of a live one. Files with other names
    /// stay.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Config`] when another process holds the lock,
    /// or when the directory is open to other users. It also returns it when
    /// the directory or a file cannot be created or deleted.
    pub fn open(dir: impl AsRef<Path>) -> HarvestResult<Self> {
        let dir = dir.as_ref().to_path_buf();
        prepare_dir(&dir)?;
        let lock = lock_dir(&dir)?;
        wipe_dir(&dir)?;
        Ok(Self {
            dir,
            lock,
            sealer: Sealer::new(),
            partitions: Mutex::new(HashMap::new()),
            index: Mutex::new(HashMap::new()),
            entity_index: Mutex::new(HashMap::new()),
            evicted: Mutex::new(HashMap::new()),
        })
    }

    /// The cache directory.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The path of the file of partition `key`.
    #[must_use]
    pub fn file_path(&self, key: OwnerKey) -> PathBuf {
        self.dir.join(file_name(key))
    }

    /// Whether the cache holds an entry for `exec`.
    #[must_use]
    pub fn contains(&self, exec: Uuid) -> bool {
        lock(&self.index).contains_key(&exec)
    }

    /// The partitions that hold an open file.
    #[must_use]
    pub fn cached_partitions(&self) -> Vec<OwnerKey> {
        lock(&self.partitions).keys().copied().collect()
    }

    /// The cached runs of `shard`.
    #[must_use]
    pub fn cached_runs(&self, shard: ShardId) -> Vec<Uuid> {
        lock(&self.index)
            .iter()
            .filter(|(_, key)| key.shard == shard)
            .map(|(exec, _)| *exec)
            .collect()
    }

    /// Load the entry of `exec` for `slot`.
    ///
    /// A stamp mismatch returns [`DiskLoad::Stale`]. A row that does not open
    /// or decode returns [`DiskLoad::Corrupt`]. Both purge first. A file stamp
    /// failure deletes the whole file.
    #[must_use]
    pub fn load(&self, slot: &DiskSlot, exec: Uuid) -> DiskLoad {
        let Some(partition) = self.existing_partition(slot.key) else {
            return DiskLoad::Miss;
        };
        let mut guard = lock(&partition);
        if guard.closed || guard.conn.is_none() {
            return DiskLoad::Miss;
        }
        match self.check_file_stamp(&guard, slot) {
            StampCheck::Valid => {}
            StampCheck::Mismatch | StampCheck::Gone => {
                self.close_partition(slot.key, &partition, &mut guard);
                return DiskLoad::Stale;
            }
            StampCheck::Broken => {
                self.close_partition(slot.key, &partition, &mut guard);
                return DiskLoad::Corrupt;
            }
        }
        let Some(conn) = guard.conn.as_ref() else {
            return DiskLoad::Miss;
        };
        let outcome = self.read_run(conn, slot.key, exec);
        if !matches!(outcome, DiskLoad::Hit(_) | DiskLoad::Miss)
            && !self.delete_run(conn, slot.key, exec)
        {
            self.close_partition(slot.key, &partition, &mut guard);
        }
        drop(guard);
        outcome
    }

    /// Store the state of `exec` for `slot`.
    ///
    /// The store appends the events after the stored `next_event_id`. It
    /// rewrites the run when the stored last event differs. It refuses a
    /// state whose event count differs from its `next_event_id`.
    ///
    /// `still_owned` runs under the partition lock. A tenure end drops the
    /// partition under the same lock, after the tenure set changes. So a
    /// store that passes the check finishes before the drop deletes the
    /// file, and a later store is refused.
    pub fn store(
        &self,
        slot: &DiskSlot,
        exec: Uuid,
        state: &CachedWorkflowState,
        still_owned: impl FnOnce() -> bool,
    ) -> DiskStore {
        self.store_marked(slot, exec, state, None, still_owned)
    }

    /// [`Self::store`] with the stored-history byte mark of the decision.
    pub(crate) fn store_marked(
        &self,
        slot: &DiskSlot,
        exec: Uuid,
        state: &CachedWorkflowState,
        history_bytes: Option<HistoryBytesMark>,
        still_owned: impl FnOnce() -> bool,
    ) -> DiskStore {
        let aligned = usize::try_from(state.next_event_id)
            .is_ok_and(|next| next > 0 && next == state.events.len());
        if !aligned {
            self.purge(exec);
            return DiskStore::Refused;
        }
        // A run moved to another partition key keeps no old entry.
        let previous = lock(&self.index).get(&exec).copied();
        if previous.is_some_and(|key| key != slot.key) {
            self.purge(exec);
        }
        let partition = self.partition(slot.key);
        let mut guard = lock(&partition);
        if guard.closed || self.held_back(exec) || !still_owned() {
            let deleted = guard
                .conn
                .as_ref()
                .is_none_or(|conn| self.delete_run(conn, slot.key, exec));
            if !deleted {
                self.close_partition(slot.key, &partition, &mut guard);
            }
            return DiskStore::Refused;
        }
        if !self.ensure_file(&mut guard, slot) {
            return DiskStore::Failed;
        }
        let Some(conn) = guard.conn.as_mut() else {
            return DiskStore::Failed;
        };
        let written = self.write_run(conn, slot.key, exec, state, history_bytes);
        if written.is_some() {
            lock(&self.index).insert(exec, slot.key);
        } else if !self.delete_run(conn, slot.key, exec) {
            self.close_partition(slot.key, &partition, &mut guard);
        }
        drop(guard);
        written.map_or(DiskStore::Failed, |appended| DiskStore::Written {
            appended,
        })
    }

    /// Store the entity snapshot of `exec` for `slot` (issue #2260).
    ///
    /// The row replaces an older row of the run. It never replaces a newer
    /// one. `still_owned` runs under the partition lock, as in
    /// [`Self::store`].
    pub fn store_entity(
        &self,
        slot: &DiskSlot,
        exec: Uuid,
        entity: &ProjectedEntity,
        still_owned: impl FnOnce() -> bool,
    ) -> DiskStore {
        let previous = lock(&self.entity_index).get(&exec).copied();
        if previous.is_some_and(|(key, _)| key != slot.key) {
            self.purge(exec);
        }
        let partition = self.partition(slot.key);
        let mut guard = lock(&partition);
        if guard.closed || self.held_back(exec) || !still_owned() {
            let deleted = guard
                .conn
                .as_ref()
                .is_none_or(|conn| self.delete_run(conn, slot.key, exec));
            if !deleted {
                self.close_partition(slot.key, &partition, &mut guard);
            }
            return DiskStore::Refused;
        }
        if lock(&self.entity_index)
            .get(&exec)
            .is_some_and(|(key, event_id)| *key == slot.key && *event_id > entity.event_id)
        {
            return DiskStore::Refused;
        }
        if !self.ensure_file(&mut guard, slot) {
            return DiskStore::Failed;
        }
        let Some(conn) = guard.conn.as_ref() else {
            return DiskStore::Failed;
        };
        if self.write_entity(conn, slot.key, exec, entity).is_some() {
            lock(&self.entity_index).insert(exec, (slot.key, entity.event_id));
            drop(guard);
            return DiskStore::Written { appended: 1 };
        }
        if !self.delete_run(conn, slot.key, exec) {
            self.close_partition(slot.key, &partition, &mut guard);
        }
        DiskStore::Failed
    }

    /// Load the entity snapshot of `exec` for `slot` (issue #2260).
    ///
    /// A stamp mismatch or a file that left the directory returns
    /// [`EntityLoad::Stale`]. A row that does not open returns
    /// [`EntityLoad::Corrupt`]. Both purge first.
    #[must_use]
    pub fn load_entity(&self, slot: &DiskSlot, exec: Uuid) -> EntityLoad {
        let Some(partition) = self.existing_partition(slot.key) else {
            return EntityLoad::Miss;
        };
        let mut guard = lock(&partition);
        if guard.closed || guard.conn.is_none() {
            return EntityLoad::Miss;
        }
        match self.check_file_stamp(&guard, slot) {
            StampCheck::Valid => {}
            StampCheck::Mismatch | StampCheck::Gone => {
                self.close_partition(slot.key, &partition, &mut guard);
                return EntityLoad::Stale;
            }
            StampCheck::Broken => {
                self.close_partition(slot.key, &partition, &mut guard);
                return EntityLoad::Corrupt;
            }
        }
        let Some(conn) = guard.conn.as_ref() else {
            return EntityLoad::Miss;
        };
        let outcome = self.read_entity(conn, slot.key, exec);
        if matches!(outcome, EntityLoad::Corrupt) && !self.delete_run(conn, slot.key, exec) {
            self.close_partition(slot.key, &partition, &mut guard);
        }
        drop(guard);
        outcome
    }

    /// Delete only the entity row of `exec`, and keep its run rows (issue
    /// #2260). Returns `true` when the projection held a row.
    pub fn purge_entity(&self, exec: Uuid) -> bool {
        let Some((key, _)) = lock(&self.entity_index).get(&exec).copied() else {
            return false;
        };
        if let Some(partition) = self.existing_partition(key) {
            let mut guard = lock(&partition);
            let deleted = guard.conn.as_ref().is_none_or(|conn| {
                conn.execute(
                    "DELETE FROM harvest_entity_projection WHERE exec_id = ?1",
                    params![exec.to_string()],
                )
                .is_ok()
            });
            if !deleted {
                self.close_partition(key, &partition, &mut guard);
            }
            drop(guard);
        }
        let mut entities = lock(&self.entity_index);
        if entities.get(&exec).is_some_and(|(k, _)| *k == key) {
            entities.remove(&exec);
        }
        true
    }

    /// Whether the projection holds a row for `exec` (issue #2260).
    #[must_use]
    pub fn contains_entity(&self, exec: Uuid) -> bool {
        lock(&self.entity_index).contains_key(&exec)
    }

    /// The event id of the projected snapshot of `exec`, if any.
    #[must_use]
    pub fn entity_event_id(&self, exec: Uuid) -> Option<i32> {
        lock(&self.entity_index)
            .get(&exec)
            .map(|(_, event_id)| *event_id)
    }

    /// Whether the cache holds a run entry or an entity row for `exec`.
    #[must_use]
    pub fn holds(&self, exec: Uuid) -> bool {
        self.contains(exec) || self.contains_entity(exec)
    }

    /// Delete the entry of `exec`, and refuse to store it for
    /// [`EVICT_HOLD`]. An evict wake calls this. Returns `true` when the
    /// cache held an entry.
    ///
    /// The hold starts before the purge takes the partition lock. So a store
    /// in flight either writes before the purge deletes, or is refused.
    pub fn purge_evicted(&self, exec: Uuid) -> bool {
        let now = std::time::Instant::now();
        let mut evicted = lock(&self.evicted);
        evicted.retain(|_, at| now.duration_since(*at) < EVICT_HOLD);
        evicted.insert(exec, now);
        drop(evicted);
        self.purge(exec)
    }

    /// Whether an evict wake purged `exec` within [`EVICT_HOLD`].
    fn held_back(&self, exec: Uuid) -> bool {
        lock(&self.evicted)
            .get(&exec)
            .is_some_and(|at| at.elapsed() < EVICT_HOLD)
    }

    /// Delete the entry of `exec`. Returns `true` when the cache held one.
    pub fn purge(&self, exec: Uuid) -> bool {
        let run_key = lock(&self.index).get(&exec).copied();
        let Some(key) =
            run_key.or_else(|| lock(&self.entity_index).get(&exec).map(|(key, _)| *key))
        else {
            return false;
        };
        if let Some(partition) = self.existing_partition(key) {
            let mut guard = lock(&partition);
            let deleted = guard
                .conn
                .as_ref()
                .is_none_or(|conn| self.delete_run(conn, key, exec));
            if !deleted {
                self.close_partition(key, &partition, &mut guard);
            }
            drop(guard);
        }
        let mut index = lock(&self.index);
        if index.get(&exec) == Some(&key) {
            index.remove(&exec);
        }
        drop(index);
        let mut entities = lock(&self.entity_index);
        if entities.get(&exec).is_some_and(|(k, _)| *k == key) {
            entities.remove(&exec);
        }
        true
    }

    /// Delete the file of each partition in `keys`.
    ///
    /// Call it after the tenure set drops those tenures. See
    /// [`Self::store`].
    pub fn drop_partitions(&self, keys: &[OwnerKey]) {
        for &key in keys {
            // Lock the current partition before it leaves the map. A store
            // then cannot open a second partition for the key in between.
            if let Some(partition) = self.existing_partition(key) {
                let mut guard = lock(&partition);
                self.close_partition(key, &partition, &mut guard);
                drop(guard);
            } else {
                remove_cache_file(&self.file_path(key));
            }
        }
    }

    /// Delete each partition file whose stamp has another codec generation.
    /// Returns the number of files deleted.
    pub fn retain_codec_generation(&self, codec_generation: u64) -> usize {
        let open: Vec<(OwnerKey, Arc<Mutex<Partition>>)> = lock(&self.partitions)
            .iter()
            .map(|(key, partition)| (*key, Arc::clone(partition)))
            .collect();
        let mut dropped = 0;
        for (key, partition) in open {
            let mut guard = lock(&partition);
            let stale = guard.conn.as_ref().is_some_and(|conn| {
                self.read_file_stamp(conn, key)
                    .is_none_or(|stamp| stamp.codec_generation != codec_generation)
            });
            if stale {
                self.close_partition(key, &partition, &mut guard);
                dropped += 1;
            }
            drop(guard);
        }
        dropped
    }

    /// Delete every cache file.
    pub fn wipe(&self) {
        let all: Vec<OwnerKey> = lock(&self.partitions).keys().copied().collect();
        self.drop_partitions(&all);
        lock(&self.index).clear();
        lock(&self.entity_index).clear();
        if let Err(err) = wipe_dir(&self.dir) {
            tracing::warn!(error = %err, "owner disk cache: the wipe failed");
        }
    }

    fn existing_partition(&self, key: OwnerKey) -> Option<Arc<Mutex<Partition>>> {
        lock(&self.partitions).get(&key).cloned()
    }

    fn partition(&self, key: OwnerKey) -> Arc<Mutex<Partition>> {
        let path = self.file_path(key);
        Arc::clone(lock(&self.partitions).entry(key).or_insert_with(|| {
            Arc::new(Mutex::new(Partition {
                path,
                conn: None,
                identity: None,
                closed: false,
            }))
        }))
    }

    /// Close the connection, delete the file and forget its runs.
    ///
    /// The map entry goes too when it is still `partition`. A later
    /// operation then gets a fresh partition.
    fn close_partition(
        &self,
        key: OwnerKey,
        partition: &Arc<Mutex<Partition>>,
        guard: &mut MutexGuard<'_, Partition>,
    ) {
        // A closed partition already left the map. Its path can now belong to
        // a newer partition, so it deletes nothing.
        if guard.closed {
            return;
        }
        guard.closed = true;
        drop(guard.conn.take());
        guard.identity = None;
        remove_cache_file(&guard.path);
        self.forget_partition(key);
        let mut partitions = lock(&self.partitions);
        if partitions
            .get(&key)
            .is_some_and(|current| Arc::ptr_eq(current, partition))
        {
            partitions.remove(&key);
        }
    }

    /// Make the file of `slot` exist with a valid stamp. A stale or broken
    /// file is replaced. Returns `false` when the file cannot be made.
    fn ensure_file(&self, guard: &mut MutexGuard<'_, Partition>, slot: &DiskSlot) -> bool {
        if guard.conn.is_some() {
            if matches!(self.check_file_stamp(guard, slot), StampCheck::Valid) {
                return true;
            }
            drop(guard.conn.take());
            guard.identity = None;
            remove_cache_file(&guard.path);
            self.forget_partition(slot.key);
        }
        remove_cache_file(&guard.path);
        match self.create_file(&guard.path, slot) {
            Ok(conn) => {
                guard.conn = Some(conn);
                guard.identity = file_identity(&guard.path);
                true
            }
            Err(err) => {
                tracing::warn!(error = %err, "owner disk cache: a file create failed");
                remove_cache_file(&guard.path);
                false
            }
        }
    }

    fn create_file(&self, path: &Path, slot: &DiskSlot) -> Result<Connection, CacheFault> {
        let conn = Connection::open(path)?;
        conn.execute_batch(PRAGMAS)?;
        conn.execute_batch(SCHEMA)?;
        let stamp = serde_json::to_vec(&FileStamp::of(slot))?;
        let sealed = self
            .sealer
            .seal(&aad("stamp", slot.key, None, None), &stamp)
            .ok_or(CacheFault::Seal)?;
        conn.execute(
            "INSERT INTO harvest_cache_stamp (id, sealed) VALUES (1, ?1)",
            params![sealed],
        )?;
        Ok(conn)
    }

    fn check_file_stamp(&self, guard: &MutexGuard<'_, Partition>, slot: &DiskSlot) -> StampCheck {
        let Some(conn) = guard.conn.as_ref() else {
            return StampCheck::Broken;
        };
        // On Unix an open connection still reads a deleted file. A file
        // that left the directory is gone for the cache (issue #2260).
        if guard.identity.is_none() || file_identity(&guard.path) != guard.identity {
            return StampCheck::Gone;
        }
        match self.read_file_stamp(conn, slot.key) {
            None => StampCheck::Broken,
            Some(stamp) if stamp == FileStamp::of(slot) => StampCheck::Valid,
            Some(_) => StampCheck::Mismatch,
        }
    }

    fn read_file_stamp(&self, conn: &Connection, key: OwnerKey) -> Option<FileStamp> {
        let sealed: Vec<u8> = conn
            .query_row(
                "SELECT sealed FROM harvest_cache_stamp WHERE id = 1",
                [],
                |row| row.get(0),
            )
            .ok()?;
        let plain = self.sealer.open(&aad("stamp", key, None, None), &sealed)?;
        serde_json::from_slice(&plain).ok()
    }

    fn read_run_stamp(&self, conn: &Connection, key: OwnerKey, exec: Uuid) -> RunRead {
        let sealed: Option<Vec<u8>> = match conn
            .query_row(
                "SELECT sealed FROM harvest_cache_runs WHERE exec_id = ?1",
                params![exec.to_string()],
                |row| row.get(0),
            )
            .optional()
        {
            Ok(sealed) => sealed,
            Err(_) => return RunRead::Broken,
        };
        let Some(sealed) = sealed else {
            return RunRead::Absent;
        };
        self.sealer
            .open(&aad("run", key, Some(exec), None), &sealed)
            .and_then(|plain| serde_json::from_slice::<RunStamp>(&plain).ok())
            .map_or(RunRead::Broken, RunRead::Found)
    }

    fn read_run(&self, conn: &Connection, key: OwnerKey, exec: Uuid) -> DiskLoad {
        let stamp = match self.read_run_stamp(conn, key, exec) {
            RunRead::Absent => return DiskLoad::Miss,
            RunRead::Broken => return DiskLoad::Corrupt,
            RunRead::Found(stamp) => stamp,
        };
        if stamp.state != CACHED_RUN_STATE {
            return DiskLoad::Stale;
        }
        match self.read_events(conn, key, exec, stamp.next_event_id) {
            Some(events) => {
                let last = events.last().map(event_hash);
                if !matches!(last, Some(Ok(hash)) if hash == stamp.last_event_hash) {
                    return DiskLoad::Corrupt;
                }
                DiskLoad::Hit(DiskEntry {
                    state: CachedWorkflowState {
                        events,
                        next_event_id: stamp.next_event_id,
                    },
                    last_event_hash: stamp.last_event_hash,
                    history_bytes: stamp.history_bytes.map(|(bytes, through, warm_steps)| {
                        HistoryBytesMark {
                            bytes,
                            through,
                            warm_steps,
                        }
                    }),
                })
            }
            None => DiskLoad::Corrupt,
        }
    }

    /// Read and open every event row of `exec`. `None` on any gap, extra
    /// row, sealing failure or decode failure.
    fn read_events(
        &self,
        conn: &Connection,
        key: OwnerKey,
        exec: Uuid,
        next_event_id: i32,
    ) -> Option<Vec<WorkflowEvent>> {
        let expected = usize::try_from(next_event_id).ok()?;
        let mut stmt = conn
            .prepare("SELECT seq, event_json FROM harvest_events WHERE exec_id = ?1 ORDER BY seq")
            .ok()?;
        let rows = stmt
            .query_map(params![exec.to_string()], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?))
            })
            .ok()?;
        let mut events = Vec::with_capacity(expected);
        for (index, row) in rows.enumerate() {
            let (seq, sealed) = row.ok()?;
            if i64::try_from(index).ok()? != seq {
                return None;
            }
            let plain = self
                .sealer
                .open(&aad("event", key, Some(exec), Some(seq)), &sealed)?;
            events.push(serde_json::from_slice(&plain).ok()?);
        }
        (events.len() == expected).then_some(events)
    }

    /// Write the rows of `exec` in one transaction. Returns the number of
    /// event rows written.
    fn write_run(
        &self,
        conn: &mut Connection,
        key: OwnerKey,
        exec: Uuid,
        state: &CachedWorkflowState,
        history_bytes: Option<HistoryBytesMark>,
    ) -> Option<usize> {
        let last_event_hash = event_hash(state.events.last()?).ok()?;
        // Append when the stored prefix ends with the same event.
        let start = match self.read_run_stamp(conn, key, exec) {
            RunRead::Found(stamp)
                if stamp.state == CACHED_RUN_STATE
                    && stamp.next_event_id <= state.next_event_id
                    && usize::try_from(stamp.next_event_id)
                        .ok()
                        .and_then(|n| n.checked_sub(1))
                        .and_then(|i| state.events.get(i))
                        .and_then(|event| event_hash(event).ok())
                        == Some(stamp.last_event_hash) =>
            {
                usize::try_from(stamp.next_event_id).ok()?
            }
            _ => 0,
        };
        let exec_text = exec.to_string();
        let tx = conn.transaction().ok()?;
        if start == 0 {
            tx.execute(
                "DELETE FROM harvest_events WHERE exec_id = ?1",
                params![exec_text],
            )
            .ok()?;
        }
        {
            let mut insert = tx
                .prepare_cached(
                    "INSERT INTO harvest_events (exec_id, seq, event_json) VALUES (?1, ?2, ?3)",
                )
                .ok()?;
            for (index, event) in state.events.iter().enumerate().skip(start) {
                let seq = i64::try_from(index).ok()?;
                let json = serde_json::to_vec(event).ok()?;
                let sealed = self
                    .sealer
                    .seal(&aad("event", key, Some(exec), Some(seq)), &json)?;
                insert.execute(params![exec_text, seq, sealed]).ok()?;
            }
        }
        let stamp = RunStamp {
            state: CACHED_RUN_STATE.to_string(),
            next_event_id: state.next_event_id,
            last_event_hash,
            history_bytes: history_bytes.map(|m| (m.bytes, m.through, m.warm_steps)),
        };
        let sealed = self.sealer.seal(
            &aad("run", key, Some(exec), None),
            &serde_json::to_vec(&stamp).ok()?,
        )?;
        tx.execute(
            "INSERT INTO harvest_cache_runs (exec_id, sealed) VALUES (?1, ?2) \
             ON CONFLICT (exec_id) DO UPDATE SET sealed = excluded.sealed",
            params![exec_text, sealed],
        )
        .ok()?;
        tx.commit().ok()?;
        Some(state.events.len() - start)
    }

    /// Seal and upsert the entity row of `exec`. `None` on any failure.
    fn write_entity(
        &self,
        conn: &Connection,
        key: OwnerKey,
        exec: Uuid,
        entity: &ProjectedEntity,
    ) -> Option<()> {
        let plain = serde_json::to_vec(entity).ok()?;
        let sealed = self
            .sealer
            .seal(&aad("entity", key, Some(exec), None), &plain)?;
        conn.execute(
            "INSERT INTO harvest_entity_projection (exec_id, sealed) VALUES (?1, ?2) \
             ON CONFLICT (exec_id) DO UPDATE SET sealed = excluded.sealed",
            params![exec.to_string(), sealed],
        )
        .ok()?;
        Some(())
    }

    /// Read and open the entity row of `exec`.
    fn read_entity(&self, conn: &Connection, key: OwnerKey, exec: Uuid) -> EntityLoad {
        let sealed: Option<Vec<u8>> = match conn
            .query_row(
                "SELECT sealed FROM harvest_entity_projection WHERE exec_id = ?1",
                params![exec.to_string()],
                |row| row.get(0),
            )
            .optional()
        {
            Ok(sealed) => sealed,
            Err(_) => return EntityLoad::Corrupt,
        };
        let Some(sealed) = sealed else {
            return EntityLoad::Miss;
        };
        self.sealer
            .open(&aad("entity", key, Some(exec), None), &sealed)
            .and_then(|plain| serde_json::from_slice::<ProjectedEntity>(&plain).ok())
            .map_or(EntityLoad::Corrupt, EntityLoad::Hit)
    }

    /// Forget every run and entity row of partition `key`.
    fn forget_partition(&self, key: OwnerKey) {
        lock(&self.index).retain(|_, k| *k != key);
        lock(&self.entity_index).retain(|_, (k, _)| *k != key);
    }

    /// Delete the rows of `exec`. Returns `false` when the delete failed.
    /// The caller then closes the partition, which deletes the whole file.
    fn delete_run(&self, conn: &Connection, key: OwnerKey, exec: Uuid) -> bool {
        let exec_text = exec.to_string();
        let deleted = conn
            .execute(
                "DELETE FROM harvest_events WHERE exec_id = ?1",
                params![exec_text],
            )
            .and_then(|_| {
                conn.execute(
                    "DELETE FROM harvest_cache_runs WHERE exec_id = ?1",
                    params![exec_text],
                )
            })
            .and_then(|_| {
                conn.execute(
                    "DELETE FROM harvest_entity_projection WHERE exec_id = ?1",
                    params![exec_text],
                )
            });
        let mut index = lock(&self.index);
        if index.get(&exec) == Some(&key) {
            index.remove(&exec);
        }
        drop(index);
        let mut entities = lock(&self.entity_index);
        if entities.get(&exec).is_some_and(|(k, _)| *k == key) {
            entities.remove(&exec);
        }
        drop(entities);
        if let Err(err) = &deleted {
            tracing::warn!(error = %err, "owner disk cache: a purge failed; the file is deleted");
        }
        deleted.is_ok()
    }
}

impl Drop for OwnerDiskCache {
    /// Delete every cache file. The lock file stays, as in
    /// `autumn-harvest-sqlite/src/lock.rs`.
    fn drop(&mut self) {
        self.wipe();
    }
}

/// The result of a file stamp check.
enum StampCheck {
    Valid,
    Mismatch,
    Broken,
    /// The file left the directory, or another file took its path.
    Gone,
}

/// The result of a run stamp read.
enum RunRead {
    Absent,
    Found(RunStamp),
    Broken,
}

/// An internal fault of a file operation.
#[derive(Debug, thiserror::Error)]
enum CacheFault {
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("the node key refused to seal a row")]
    Seal,
}

fn io_config(path: &Path, err: &std::io::Error) -> HarvestError {
    HarvestError::Config(format!(
        "owner disk cache: {} failed: {err}",
        path.display()
    ))
}

/// Create `dir` with the mode `0700`, or check an existing one.
///
/// On Unix an existing directory must give its group and other users no
/// access. The open never changes the mode of a directory that it did not
/// create.
fn prepare_dir(dir: &Path) -> HarvestResult<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
        if !dir.exists() {
            std::fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(dir)
                .map_err(|e| io_config(dir, &e))?;
        }
        let mode = std::fs::metadata(dir)
            .map_err(|e| io_config(dir, &e))?
            .permissions()
            .mode();
        if mode & 0o077 != 0 {
            return Err(HarvestError::Config(format!(
                "owner disk cache: {} is open to other users (mode {:o}); use a private \
                 directory with mode 0700",
                dir.display(),
                mode & 0o777
            )));
        }
    }
    #[cfg(not(unix))]
    std::fs::create_dir_all(dir).map_err(|e| io_config(dir, &e))?;
    Ok(())
}

/// Take the exclusive lock on the sidecar file of `dir`.
fn lock_dir(dir: &Path) -> HarvestResult<File> {
    let path = dir.join(LOCK_FILE_NAME);
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options.open(&path).map_err(|e| io_config(&path, &e))?;
    // Fully qualified: std has an inherent `File::try_lock` from Rust 1.89,
    // above the MSRV.
    match fs4::FileExt::try_lock(&file) {
        Ok(()) => Ok(file),
        Err(fs4::TryLockError::WouldBlock) => Err(HarvestError::Config(format!(
            "owner disk cache: another process holds {}",
            path.display()
        ))),
        Err(fs4::TryLockError::Error(err)) => Err(io_config(&path, &err)),
    }
}

/// Delete every cache file in `dir`. Other files stay.
fn wipe_dir(dir: &Path) -> HarvestResult<()> {
    let entries = std::fs::read_dir(dir).map_err(|e| io_config(dir, &e))?;
    for entry in entries {
        let entry = entry.map_err(|e| io_config(dir, &e))?;
        let name = entry.file_name();
        if name.to_str().is_some_and(is_cache_file) {
            let path = entry.path();
            std::fs::remove_file(&path).map_err(|e| io_config(&path, &e))?;
        }
    }
    Ok(())
}

/// The partitions of `shard` in `cached` that `owns` does not hold.
pub(crate) fn unowned_partitions(
    cached: &[OwnerKey],
    shard: ShardId,
    owns: impl Fn(OwnerKey) -> bool,
) -> Vec<OwnerKey> {
    cached
        .iter()
        .copied()
        .filter(|key| key.shard == shard && !owns(*key))
        .collect()
}

/// Whether `tail`, read from event `anchor` on, starts with the cached last
/// event and has no gap.
fn anchors(tail: &crate::store::EventHistory, anchor: i32, last_event_hash: &[u8; 32]) -> bool {
    let span = i64::from(tail.next_event_id) - i64::from(anchor);
    let contiguous = usize::try_from(span).is_ok_and(|span| span == tail.events.len());
    contiguous
        && tail
            .events
            .first()
            .is_some_and(|event| event_hash(event).is_ok_and(|hash| &hash == last_event_hash))
}

/// Find the snapshot of the entity `(workflow_name, workflow_id)` for a
/// query (issue #2260).
///
/// A hit reads the projection file of a held tenure. A miss reads the
/// snapshot event from history, and an owner seals it into the file.
///
/// # Errors
///
/// [`EntitySqlError::NotFound`], [`EntitySqlError::Erased`],
/// [`EntitySqlError::NotRecorded`], or a database or codec error.
///
/// [`EntitySqlError::NotFound`]: crate::entity_sql::EntitySqlError::NotFound
/// [`EntitySqlError::Erased`]: crate::entity_sql::EntitySqlError::Erased
/// [`EntitySqlError::NotRecorded`]: crate::entity_sql::EntitySqlError::NotRecorded
pub(crate) async fn entity_record(
    disk: Option<&DiskTier>,
    conn: &mut diesel_async::AsyncPgConnection,
    owned: Arc<crate::decision_owner::OwnedPartitions>,
    codecs: &crate::payload_codec::PayloadCodecs,
    offloader: Option<&crate::payload_store::PayloadOffloader>,
    workflow_name: &str,
    workflow_id: &str,
) -> Result<
    (
        crate::entity_sql::EntityRecord,
        crate::entity_sql::EntitySqlSource,
    ),
    crate::entity_sql::EntitySqlError,
> {
    use crate::entity_sql::{EntityRecord, EntitySqlError, EntitySqlSource};
    use crate::entity_state::ProjectedSnapshot;

    let not_found = || EntitySqlError::NotFound {
        workflow_name: workflow_name.to_string(),
        workflow_id: workflow_id.to_string(),
    };
    let pointer = crate::entity_state::pointer(conn, workflow_name, workflow_id)
        .await?
        .ok_or_else(not_found)?;
    let exec = pointer.execution_id;
    let record = |entity: ProjectedEntity| {
        EntityRecord::new(
            workflow_name,
            workflow_id,
            exec,
            entity.event_id,
            entity.deleted,
            entity.snapshot,
        )
    };
    // Only a live run of a held tenure reads or writes the file.
    let slot = disk
        .filter(|_| pointer.run_state == CACHED_RUN_STATE)
        .and_then(|tier| {
            tier.entity_slot(&owned, workflow_id)
                .map(|(key, epoch)| (tier, key, epoch))
        });
    if let Some((tier, key, epoch)) = slot
        && tier.cache.contains_entity(exec.as_uuid())
    {
        let at = tier.slot(key, epoch);
        let cache = Arc::clone(&tier.cache);
        let exec_uuid = exec.as_uuid();
        let loaded = tokio::task::spawn_blocking(move || cache.load_entity(&at, exec_uuid))
            .await
            .unwrap_or(EntityLoad::Miss);
        match loaded {
            EntityLoad::Hit(entity) if entity.event_id == pointer.event_id => {
                return Ok((record(entity), EntitySqlSource::File));
            }
            EntityLoad::Hit(_) => {
                let cache = Arc::clone(&tier.cache);
                let _ = tokio::task::spawn_blocking(move || cache.purge_entity(exec_uuid)).await;
            }
            EntityLoad::Miss | EntityLoad::Stale | EntityLoad::Corrupt => {}
        }
    }
    let snapshot =
        crate::entity_state::snapshot_at(conn, codecs, offloader, exec, pointer.event_id).await?;
    let entity = match snapshot.ok_or_else(not_found)? {
        ProjectedSnapshot::Current(snapshot) => ProjectedEntity {
            event_id: pointer.event_id,
            deleted: pointer.deleted,
            snapshot,
        },
        ProjectedSnapshot::Erased => return Err(EntitySqlError::Erased),
        ProjectedSnapshot::Omitted => {
            return Err(EntitySqlError::NotRecorded(
                "the state was past a payload cap".to_string(),
            ));
        }
        ProjectedSnapshot::Offloaded => {
            return Err(EntitySqlError::NotRecorded(
                "the payload store holds the state, and no store is set".to_string(),
            ));
        }
    };
    if let Some((tier, key, epoch)) = slot {
        let at = tier.slot(key, epoch);
        let cache = Arc::clone(&tier.cache);
        let exec_uuid = exec.as_uuid();
        let stored = entity.clone();
        let written = tokio::task::spawn_blocking(move || {
            cache.store_entity(&at, exec_uuid, &stored, || {
                owned.may_cache(key, Some(epoch))
            })
        })
        .await;
        if matches!(written, Ok(DiskStore::Written { .. })) {
            lock(&tier.written).insert(key);
        }
    }
    Ok((record(entity), EntitySqlSource::History))
}

/// The last entity snapshot in `events` after the event id `stored`.
///
/// Returns its event id, and the row to store, or `None` when the snapshot
/// holds no state. Event `i` must be at index `i`.
fn latest_entity(
    events: &[WorkflowEvent],
    stored: Option<i32>,
) -> Option<(i32, Option<ProjectedEntity>)> {
    use crate::entity_state::ProjectedSnapshot;
    use crate::event::SideEffectKind;

    events
        .iter()
        .enumerate()
        .rev()
        .map_while(|(index, event)| {
            let event_id = i32::try_from(index).ok()?;
            stored
                .is_none_or(|id| event_id > id)
                .then_some((event_id, event))
        })
        .find_map(|(event_id, event)| match event {
            WorkflowEvent::SideEffectRecorded {
                kind: SideEffectKind::EntityState,
                name,
                value,
            } => {
                let deleted = name.as_deref() == Some(crate::entity::ENTITY_DELETED_SIDE_EFFECT);
                let entity = match crate::entity_state::classify_decoded(value.clone()) {
                    Ok(ProjectedSnapshot::Current(snapshot)) => Some(ProjectedEntity {
                        event_id,
                        deleted,
                        snapshot,
                    }),
                    _ => None,
                };
                Some((event_id, entity))
            }
            _ => None,
        })
}

/// How often an owner checks its cached runs against Postgres.
///
/// The check is the backstop for a lost evict wake. It purges each cached
/// run that is no longer `RUNNING`.
pub(crate) const RECONCILE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

/// The most run ids in one reconcile query.
const RECONCILE_CHUNK: usize = 1_000;

/// The disk tier of a worker: the cache and the codec registry whose key
/// generation stamps each file.
///
/// Each method runs the file work in `spawn_blocking`. Each one waits for
/// that work, so the order of the calls is the order of the file changes.
#[derive(Clone)]
pub(crate) struct DiskTier {
    cache: Arc<OwnerDiskCache>,
    codecs: crate::payload_codec::PayloadCodecs,
    last_reconcile: Arc<Mutex<HashMap<Option<ShardId>, std::time::Instant>>>,
    /// The partitions that this worker wrote. Workers of one registry share
    /// the cache, so a shutdown drops only these.
    written: Arc<Mutex<std::collections::HashSet<OwnerKey>>>,
    /// The fewest events that a run needs before the tier keeps it. A
    /// shorter history loads faster from Postgres.
    min_events: usize,
    /// Whether the tier keeps an entity projection (issue #2260).
    entity_sql: bool,
}

/// What a disk read gives a decision.
pub(crate) struct DiskRead {
    /// The cached state, its byte mark and the Postgres delta after it,
    /// once every check passed.
    pub(crate) entry: Option<(
        CachedWorkflowState,
        Option<HistoryBytesMark>,
        crate::store::EventHistory,
    )>,
    /// The outcome label of `harvest.workflow.disk_cache`.
    pub(crate) outcome: &'static str,
}

impl DiskTier {
    pub(crate) fn new(
        cache: Arc<OwnerDiskCache>,
        codecs: crate::payload_codec::PayloadCodecs,
        min_events: usize,
    ) -> Self {
        Self {
            cache,
            codecs,
            last_reconcile: Arc::default(),
            written: Arc::default(),
            min_events,
            entity_sql: false,
        }
    }

    /// Keep an entity projection (issue #2260).
    #[must_use]
    pub(crate) const fn with_entity_sql(mut self, on: bool) -> Self {
        self.entity_sql = on;
        self
    }

    /// Seal the last entity snapshot in `events` into the projection
    /// (issue #2260).
    ///
    /// Call it after a committed suspension of a held tenure. A run with no
    /// snapshot, or with the same snapshot as the row, writes nothing.
    pub(crate) async fn apply_entity(
        &self,
        key: OwnerKey,
        epoch: OwnerEpoch,
        exec: Uuid,
        state: &CachedWorkflowState,
        owned: Arc<crate::decision_owner::OwnedPartitions>,
    ) {
        if !self.entity_sql {
            return;
        }
        // Event `i` is at index `i` only in a full history.
        let aligned =
            usize::try_from(state.next_event_id).is_ok_and(|next| next == state.events.len());
        if !aligned {
            return;
        }
        let stored = self.cache.entity_event_id(exec);
        let Some((event_id, entity)) = latest_entity(&state.events, stored) else {
            return;
        };
        let cache = Arc::clone(&self.cache);
        let Some(entity) = entity else {
            // A snapshot that holds no state, such as an omitted one. The
            // row would be stale, so it goes.
            if let Err(err) = tokio::task::spawn_blocking(move || cache.purge_entity(exec)).await {
                tracing::warn!(error = %err, "owner disk cache: an entity purge task died");
            }
            return;
        };
        let slot = self.slot(key, epoch);
        let stored = tokio::task::spawn_blocking(move || {
            cache.store_entity(&slot, exec, &entity, || owned.may_cache(key, Some(epoch)))
        })
        .await;
        match stored {
            Ok(DiskStore::Written { .. }) => {
                lock(&self.written).insert(key);
            }
            Ok(DiskStore::Failed) => {
                tracing::warn!(%key, event_id, "owner disk cache: an entity write failed");
            }
            Ok(DiskStore::Refused) => {}
            Err(err) => {
                tracing::warn!(error = %err, "owner disk cache: an entity write task died");
                self.purge(exec).await;
            }
        }
    }

    /// The held tenure of the partition of `workflow_id`, if exactly one
    /// shard has a live one (issue #2260).
    fn entity_slot(
        &self,
        owned: &crate::decision_owner::OwnedPartitions,
        workflow_id: &str,
    ) -> Option<(OwnerKey, OwnerEpoch)> {
        use crate::decision_owner::{DecisionPartition, DecisionPin};
        if !self.entity_sql {
            return None;
        }
        let partition = DecisionPartition::for_workflow_id(workflow_id);
        let mut held = owned
            .tenures()
            .into_iter()
            .filter(|tenure| tenure.partition() == partition);
        let tenure = held.next()?;
        if held.next().is_some() {
            return None;
        }
        match owned.pin_for(tenure.key()) {
            DecisionPin::Owned(live) if live.epoch() == tenure.epoch() => {
                Some((live.key(), live.epoch()))
            }
            _ => None,
        }
    }

    /// The disk cache itself.
    pub(crate) const fn cache(&self) -> &Arc<OwnerDiskCache> {
        &self.cache
    }

    fn slot(&self, key: OwnerKey, epoch: OwnerEpoch) -> DiskSlot {
        DiskSlot {
            key,
            epoch,
            codec_generation: self.codecs.key_state().epoch.generation,
        }
    }

    /// Read the entry of `exec` for a decision of a memory miss.
    ///
    /// `run_state` is the state of the execution row. A run that is not
    /// `RUNNING` purges its entry. A hit must pass the anchor check: the
    /// last cached event equals the stored event of the same id. One query
    /// reads that event and the delta after it, so a hit costs one round
    /// trip. Any failure reads as no entry, so the decision loads from
    /// Postgres.
    pub(crate) async fn read(
        &self,
        conn: &mut diesel_async::AsyncPgConnection,
        key: OwnerKey,
        epoch: OwnerEpoch,
        exec: crate::types::ExecutionId,
        run_state: &str,
        offloader: Option<&crate::payload_store::PayloadOffloader>,
    ) -> DiskRead {
        let exec_uuid = exec.as_uuid();
        if run_state != CACHED_RUN_STATE {
            return DiskRead {
                entry: None,
                outcome: if self.purge(exec_uuid).await {
                    "stale"
                } else {
                    "miss"
                },
            };
        }
        // The index lives in memory, so a miss reads no file.
        if !self.cache.contains(exec_uuid) {
            return DiskRead {
                entry: None,
                outcome: "miss",
            };
        }
        let slot = self.slot(key, epoch);
        let cache = Arc::clone(&self.cache);
        let loaded = tokio::task::spawn_blocking(move || cache.load(&slot, exec_uuid))
            .await
            .unwrap_or(DiskLoad::Miss);
        let DiskLoad::Hit(entry) = loaded else {
            return DiskRead {
                entry: None,
                outcome: loaded.outcome(),
            };
        };
        let anchor = entry.state.next_event_id - 1;
        let tail =
            crate::store::load_history_since_inflated(conn, exec, anchor, &self.codecs, offloader)
                .await;
        match tail {
            Ok(mut tail) if anchors(&tail, anchor, &entry.last_event_hash) => {
                // The anchor is the first event. The rest is the delta.
                tail.events.remove(0);
                DiskRead {
                    entry: Some((entry.state, entry.history_bytes, tail)),
                    outcome: "hit",
                }
            }
            Ok(_) => {
                self.purge(exec_uuid).await;
                DiskRead {
                    entry: None,
                    outcome: "stale",
                }
            }
            Err(err) => {
                tracing::debug!(error = %err, "owner disk cache: the anchor read failed");
                DiskRead {
                    entry: None,
                    outcome: "miss",
                }
            }
        }
    }

    /// Write the state of a committed suspension, and give the state back.
    ///
    /// `owned` checks the tenure under the partition lock. `None` means
    /// that the blocking task died, so the state is lost.
    pub(crate) async fn write(
        &self,
        key: OwnerKey,
        epoch: OwnerEpoch,
        exec: Uuid,
        state: CachedWorkflowState,
        history_bytes: Option<HistoryBytesMark>,
        owned: Arc<crate::decision_owner::OwnedPartitions>,
    ) -> Option<CachedWorkflowState> {
        // A short history loads faster from Postgres. Histories only grow,
        // so a short one has no older entry to purge.
        if state.events.len() < self.min_events {
            return Some(state);
        }
        let slot = self.slot(key, epoch);
        let cache = Arc::clone(&self.cache);
        let written = tokio::task::spawn_blocking(move || {
            let outcome = cache.store_marked(&slot, exec, &state, history_bytes, || {
                owned.may_cache(key, Some(epoch))
            });
            (state, outcome)
        })
        .await;
        match written {
            Ok((state, outcome)) => {
                match outcome {
                    DiskStore::Written { .. } => {
                        lock(&self.written).insert(key);
                    }
                    DiskStore::Failed => {
                        tracing::warn!(%key, "owner disk cache: a write failed; the entry is purged");
                    }
                    DiskStore::Refused => {}
                }
                Some(state)
            }
            Err(err) => {
                tracing::warn!(error = %err, "owner disk cache: a write task died");
                self.purge(exec).await;
                None
            }
        }
    }

    /// Delete the entry of `exec`. Returns `true` when the cache held one.
    pub(crate) async fn purge(&self, exec: Uuid) -> bool {
        if !self.cache.holds(exec) {
            return false;
        }
        let cache = Arc::clone(&self.cache);
        tokio::task::spawn_blocking(move || cache.purge(exec))
            .await
            .unwrap_or(false)
    }

    /// Delete the entry of `exec` for an evict wake, and hold back its
    /// stores for [`EVICT_HOLD`].
    pub(crate) async fn purge_evicted(&self, exec: Uuid) {
        let cache = Arc::clone(&self.cache);
        if let Err(err) = tokio::task::spawn_blocking(move || cache.purge_evicted(exec)).await {
            tracing::warn!(error = %err, "owner disk cache: an evict purge task died");
        }
    }

    /// Delete the file of each partition that this worker wrote. A stopped
    /// worker then keeps no disk file. Other workers of the registry keep
    /// theirs.
    pub(crate) async fn close(&self) {
        let keys: Vec<OwnerKey> = lock(&self.written).drain().collect();
        self.drop_partitions(&keys).await;
    }

    /// Delete the file of each partition in `keys`.
    pub(crate) async fn drop_partitions(&self, keys: &[OwnerKey]) {
        if keys.is_empty() {
            return;
        }
        let cache = Arc::clone(&self.cache);
        let keys = keys.to_vec();
        if let Err(err) = tokio::task::spawn_blocking(move || cache.drop_partitions(&keys)).await {
            tracing::warn!(error = %err, "owner disk cache: a partition drop task died");
        }
    }

    /// Whether a reconcile of `shard` is due. A due check also starts the
    /// next interval.
    pub(crate) fn reconcile_due(&self, shard: ShardId) -> bool {
        let now = std::time::Instant::now();
        let mut last = lock(&self.last_reconcile);
        let due = last
            .get(&Some(shard))
            .is_none_or(|at| now.duration_since(*at) >= RECONCILE_INTERVAL);
        if due {
            last.insert(Some(shard), now);
        }
        due
    }

    /// Purge each cached run of `shard` that is not `RUNNING`, and each file
    /// of an older codec generation. `None` checks every shard against
    /// `conn`. Returns the number of runs purged.
    ///
    /// # Errors
    ///
    /// Returns a database error of a lookup.
    pub(crate) async fn reconcile(
        &self,
        conn: &mut diesel_async::AsyncPgConnection,
        shard: Option<ShardId>,
    ) -> HarvestResult<usize> {
        use diesel::{ExpressionMethods as _, QueryDsl as _};
        use diesel_async::RunQueryDsl as _;

        use crate::schema::harvest_workflow_executions as runs;

        let generation = self.codecs.key_state().epoch.generation;
        let cache = Arc::clone(&self.cache);
        let dropped =
            tokio::task::spawn_blocking(move || cache.retain_codec_generation(generation))
                .await
                .unwrap_or(0);
        if dropped > 0 {
            tracing::info!(
                dropped,
                "owner disk cache: the codec key set changed; files deleted"
            );
        }
        let mut cached: Vec<Uuid> = lock(&self.cache.index)
            .iter()
            .filter(|(_, key)| shard.is_none_or(|s| key.shard == s))
            .map(|(exec, _)| *exec)
            .collect();
        // Issue #2260: the entity rows follow the same rule.
        cached.extend(
            lock(&self.cache.entity_index)
                .iter()
                .filter(|(_, (key, _))| shard.is_none_or(|s| key.shard == s))
                .map(|(exec, _)| *exec),
        );
        cached.sort_unstable();
        cached.dedup();
        let mut doomed = Vec::new();
        for chunk in cached.chunks(RECONCILE_CHUNK) {
            let live: Vec<Uuid> = runs::table
                .filter(runs::id.eq_any(chunk))
                .filter(runs::state.eq(CACHED_RUN_STATE))
                .select(runs::id)
                .load(conn)
                .await
                .map_err(crate::error::database_error)?;
            doomed.extend(chunk.iter().filter(|exec| !live.contains(exec)).copied());
        }
        if doomed.is_empty() {
            return Ok(0);
        }
        let cache = Arc::clone(&self.cache);
        Ok(tokio::task::spawn_blocking(move || {
            doomed.into_iter().filter(|exec| cache.purge(*exec)).count()
        })
        .await
        .unwrap_or(0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decision_owner::DecisionPartition;
    use serde_json::{Value, json};

    /// A payload marker. A test searches the raw files for it.
    const MARKER: &str = "pii-marker-7f3c9a";

    fn key(partition: u16) -> OwnerKey {
        OwnerKey {
            shard: ShardId::UNENCODED,
            partition: DecisionPartition::new(partition).expect("in range"),
        }
    }

    fn slot(partition: u16) -> DiskSlot {
        DiskSlot {
            key: key(partition),
            epoch: OwnerEpoch::new(1),
            codec_generation: 0,
        }
    }

    fn event(step: usize) -> WorkflowEvent {
        if step == 0 {
            WorkflowEvent::WorkflowStarted {
                input: json!({"secret": MARKER}),
                timestamp: chrono::DateTime::from_timestamp(1_790_000_000, 0).unwrap_or_default(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            }
        } else {
            WorkflowEvent::ActivityCompleted {
                activity_id: crate::types::ActivityExecId::from_uuid(Uuid::from_u128(
                    u128::try_from(step).unwrap_or_default(),
                )),
                output: json!({"step": step}),
            }
        }
    }

    fn state(len: usize) -> CachedWorkflowState {
        CachedWorkflowState {
            events: (0..len).map(event).collect(),
            next_event_id: i32::try_from(len).expect("small"),
        }
    }

    fn as_json(events: &[WorkflowEvent]) -> Value {
        serde_json::to_value(events).expect("encode")
    }

    fn cache_bytes(cache: &OwnerDiskCache) -> Vec<u8> {
        let mut all = Vec::new();
        for entry in std::fs::read_dir(cache.dir()).expect("read dir") {
            let path = entry.expect("entry").path();
            if path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(is_cache_file)
            {
                all.extend(std::fs::read(&path).expect("read file"));
            }
        }
        all
    }

    fn contains(haystack: &[u8], needle: &[u8]) -> bool {
        haystack.windows(needle.len()).any(|w| w == needle)
    }

    fn sealed_rows(cache: &OwnerDiskCache, partition: u16) -> Vec<Vec<u8>> {
        let conn = Connection::open(cache.file_path(key(partition))).expect("open");
        let mut stmt = conn
            .prepare(
                "SELECT event_json FROM harvest_events \
                 UNION ALL SELECT sealed FROM harvest_cache_runs",
            )
            .expect("prepare");
        stmt.query_map([], |row| row.get::<_, Vec<u8>>(0))
            .expect("query")
            .map(|row| row.expect("row"))
            .collect()
    }

    fn owned() -> bool {
        true
    }

    /// A temporary directory with the mode `0700`, as the open requires.
    fn private_dir() -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("dir");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700))
                .expect("chmod");
        }
        dir
    }

    /// Bump the file change counter in the SQLite header, bytes 24 to 27.
    /// An open connection then drops its page cache, as after any outside
    /// write.
    fn bump_change_counter(raw: &mut [u8]) {
        let mut counter = [0_u8; 4];
        counter.copy_from_slice(&raw[24..28]);
        let next = u32::from_be_bytes(counter).wrapping_add(1);
        raw[24..28].copy_from_slice(&next.to_be_bytes());
    }

    #[test]
    fn a_stored_run_loads_back() {
        let dir = private_dir();
        let cache = OwnerDiskCache::open(dir.path()).expect("open");
        let exec = Uuid::new_v4();
        let stored = cache.store(&slot(1), exec, &state(3), owned);
        assert_eq!(stored, DiskStore::Written { appended: 3 });
        let DiskLoad::Hit(entry) = cache.load(&slot(1), exec) else {
            panic!("a stored run is a hit");
        };
        assert_eq!(entry.state.next_event_id, 3);
        assert_eq!(as_json(&entry.state.events), as_json(&state(3).events));
        let last = event_hash(&event(2)).expect("hash");
        assert_eq!(entry.last_event_hash, last);
        assert!(cache.contains(exec));
    }

    #[test]
    fn a_later_store_appends_only_new_events() {
        let dir = private_dir();
        let cache = OwnerDiskCache::open(dir.path()).expect("open");
        let exec = Uuid::new_v4();
        let _ = cache.store(&slot(1), exec, &state(3), owned);
        let stored = cache.store(&slot(1), exec, &state(5), owned);
        assert_eq!(stored, DiskStore::Written { appended: 2 });
        let DiskLoad::Hit(entry) = cache.load(&slot(1), exec) else {
            panic!("hit");
        };
        assert_eq!(as_json(&entry.state.events), as_json(&state(5).events));
    }

    #[test]
    fn a_changed_prefix_rewrites_the_run() {
        let dir = private_dir();
        let cache = OwnerDiskCache::open(dir.path()).expect("open");
        let exec = Uuid::new_v4();
        let _ = cache.store(&slot(1), exec, &state(3), owned);
        let mut other = state(4);
        other.events[2] = event(9);
        let stored = cache.store(&slot(1), exec, &other, owned);
        assert_eq!(stored, DiskStore::Written { appended: 4 });
        let DiskLoad::Hit(entry) = cache.load(&slot(1), exec) else {
            panic!("hit");
        };
        assert_eq!(as_json(&entry.state.events), as_json(&other.events));
    }

    #[test]
    fn a_state_with_a_gap_is_refused() {
        let dir = private_dir();
        let cache = OwnerDiskCache::open(dir.path()).expect("open");
        let exec = Uuid::new_v4();
        let mut gap = state(3);
        gap.next_event_id = 5;
        assert_eq!(cache.store(&slot(1), exec, &gap, owned), DiskStore::Refused);
        assert!(matches!(cache.load(&slot(1), exec), DiskLoad::Miss));
    }

    #[test]
    fn an_ended_tenure_is_refused_and_purged() {
        let dir = private_dir();
        let cache = OwnerDiskCache::open(dir.path()).expect("open");
        let exec = Uuid::new_v4();
        let _ = cache.store(&slot(1), exec, &state(2), owned);
        assert_eq!(
            cache.store(&slot(1), exec, &state(3), || false),
            DiskStore::Refused
        );
        assert!(matches!(cache.load(&slot(1), exec), DiskLoad::Miss));
        assert!(!cache.contains(exec));
    }

    #[test]
    fn a_new_epoch_finds_the_file_stale_and_deletes_it() {
        let dir = private_dir();
        let cache = OwnerDiskCache::open(dir.path()).expect("open");
        let exec = Uuid::new_v4();
        let _ = cache.store(&slot(1), exec, &state(2), owned);
        let newer = DiskSlot {
            epoch: OwnerEpoch::new(2),
            ..slot(1)
        };
        assert!(matches!(cache.load(&newer, exec), DiskLoad::Stale));
        assert!(!cache.file_path(key(1)).exists());
        assert!(matches!(cache.load(&slot(1), exec), DiskLoad::Miss));
    }

    #[test]
    fn a_new_codec_generation_finds_the_file_stale() {
        let dir = private_dir();
        let cache = OwnerDiskCache::open(dir.path()).expect("open");
        let exec = Uuid::new_v4();
        let _ = cache.store(&slot(1), exec, &state(2), owned);
        let rotated = DiskSlot {
            codec_generation: 1,
            ..slot(1)
        };
        assert!(matches!(cache.load(&rotated, exec), DiskLoad::Stale));
        assert!(!cache.file_path(key(1)).exists());
    }

    #[test]
    fn retain_codec_generation_drops_each_older_file() {
        let dir = private_dir();
        let cache = OwnerDiskCache::open(dir.path()).expect("open");
        let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
        let _ = cache.store(&slot(1), a, &state(2), owned);
        let _ = cache.store(&slot(2), b, &state(2), owned);
        assert_eq!(cache.retain_codec_generation(0), 0);
        assert_eq!(cache.retain_codec_generation(1), 2);
        assert!(!cache.contains(a) && !cache.contains(b));
        assert_eq!(cache_bytes(&cache).len(), 0, "no cache bytes remain");
    }

    #[test]
    fn a_flipped_byte_is_corrupt_and_purges_the_run() {
        let dir = private_dir();
        let cache = OwnerDiskCache::open(dir.path()).expect("open");
        let exec = Uuid::new_v4();
        let _ = cache.store(&slot(1), exec, &state(3), owned);
        let path = cache.file_path(key(1));
        let mut raw = std::fs::read(&path).expect("read");
        let row = sealed_rows(&cache, 1).remove(1);
        let at = raw
            .windows(row.len())
            .position(|w| w == row.as_slice())
            .expect("the sealed row is in the file");
        raw[at + NONCE_BYTES + 2] ^= 0x40;
        bump_change_counter(&mut raw);
        std::fs::write(&path, raw).expect("write");
        assert!(matches!(cache.load(&slot(1), exec), DiskLoad::Corrupt));
        assert!(matches!(cache.load(&slot(1), exec), DiskLoad::Miss));
    }

    #[test]
    fn swapped_rows_are_corrupt() {
        let dir = private_dir();
        let cache = OwnerDiskCache::open(dir.path()).expect("open");
        let exec = Uuid::new_v4();
        let _ = cache.store(&slot(1), exec, &state(3), owned);
        let conn = Connection::open(cache.file_path(key(1))).expect("open");
        conn.execute_batch(
            "UPDATE harvest_events SET seq = -1 WHERE seq = 1;
             UPDATE harvest_events SET seq = 1 WHERE seq = 2;
             UPDATE harvest_events SET seq = 2 WHERE seq = -1;",
        )
        .expect("swap");
        drop(conn);
        assert!(matches!(cache.load(&slot(1), exec), DiskLoad::Corrupt));
    }

    #[test]
    fn a_row_of_another_run_does_not_open() {
        let dir = private_dir();
        let cache = OwnerDiskCache::open(dir.path()).expect("open");
        let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
        let _ = cache.store(&slot(1), a, &state(2), owned);
        let _ = cache.store(&slot(1), b, &state(2), owned);
        let conn = Connection::open(cache.file_path(key(1))).expect("open");
        conn.execute(
            "UPDATE harvest_events SET event_json = \
             (SELECT event_json FROM harvest_events WHERE exec_id = ?1 AND seq = 1) \
             WHERE exec_id = ?2 AND seq = 1",
            params![a.to_string(), b.to_string()],
        )
        .expect("copy a row");
        drop(conn);
        assert!(matches!(cache.load(&slot(1), b), DiskLoad::Corrupt));
        assert!(matches!(cache.load(&slot(1), a), DiskLoad::Hit(_)));
    }

    #[test]
    fn a_broken_file_stamp_deletes_the_file() {
        let dir = private_dir();
        let cache = OwnerDiskCache::open(dir.path()).expect("open");
        let exec = Uuid::new_v4();
        let _ = cache.store(&slot(1), exec, &state(2), owned);
        let conn = Connection::open(cache.file_path(key(1))).expect("open");
        conn.execute("UPDATE harvest_cache_stamp SET sealed = x'00'", [])
            .expect("break the stamp");
        drop(conn);
        assert!(matches!(cache.load(&slot(1), exec), DiskLoad::Corrupt));
        assert!(!cache.file_path(key(1)).exists());
    }

    #[test]
    fn no_plaintext_reaches_the_disk() {
        let dir = private_dir();
        let cache = OwnerDiskCache::open(dir.path()).expect("open");
        let _ = cache.store(&slot(1), Uuid::new_v4(), &state(3), owned);
        let raw = cache_bytes(&cache);
        assert_ne!(raw.len(), 0, "the file holds rows");
        assert!(!contains(&raw, MARKER.as_bytes()));
        assert!(!contains(&raw, b"WorkflowStarted"));
    }

    /// AC 1 at the module level: a purge leaves no byte of the run.
    #[test]
    fn a_purged_run_leaves_no_local_bytes() {
        let dir = private_dir();
        let cache = OwnerDiskCache::open(dir.path()).expect("open");
        let (gone, kept) = (Uuid::new_v4(), Uuid::new_v4());
        let _ = cache.store(&slot(1), kept, &state(2), owned);
        let before: Vec<Vec<u8>> = sealed_rows(&cache, 1);
        let _ = cache.store(&slot(1), gone, &state(4), owned);
        let rows: Vec<Vec<u8>> = sealed_rows(&cache, 1)
            .into_iter()
            .filter(|row| !before.contains(row))
            .collect();
        assert_eq!(rows.len(), 5, "four events and one run stamp");
        assert!(cache.purge(gone));
        let raw = cache_bytes(&cache);
        for row in &rows {
            assert!(!contains(&raw, row), "a sealed row of the purged run stays");
        }
        assert!(!contains(&raw, gone.to_string().as_bytes()));
        assert!(matches!(cache.load(&slot(1), kept), DiskLoad::Hit(_)));
    }

    #[test]
    fn a_dropped_partition_deletes_its_file() {
        let dir = private_dir();
        let cache = OwnerDiskCache::open(dir.path()).expect("open");
        let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
        let _ = cache.store(&slot(1), a, &state(2), owned);
        let _ = cache.store(&slot(2), b, &state(2), owned);
        cache.drop_partitions(&[key(1)]);
        assert!(!cache.file_path(key(1)).exists());
        assert!(cache.file_path(key(2)).exists());
        assert!(!cache.contains(a));
        assert!(matches!(cache.load(&slot(1), a), DiskLoad::Miss));
        assert!(matches!(cache.load(&slot(2), b), DiskLoad::Hit(_)));
    }

    #[test]
    fn open_wipes_cache_files_and_keeps_other_files() {
        let dir = private_dir();
        for name in ["s0-p1.hcache", "s7-p3.hcache-journal", "notes.txt"] {
            std::fs::write(dir.path().join(name), b"old").expect("seed");
        }
        let cache = OwnerDiskCache::open(dir.path()).expect("open");
        assert!(!dir.path().join("s0-p1.hcache").exists());
        assert!(!dir.path().join("s7-p3.hcache-journal").exists());
        assert!(dir.path().join("notes.txt").exists());
        drop(cache);
    }

    #[test]
    fn a_second_open_of_one_directory_is_refused() {
        let dir = private_dir();
        let first = OwnerDiskCache::open(dir.path()).expect("open");
        let exec = Uuid::new_v4();
        let _ = first.store(&slot(1), exec, &state(2), owned);
        let err = OwnerDiskCache::open(dir.path()).expect_err("locked");
        assert!(err.to_string().contains(LOCK_FILE_NAME));
        assert!(matches!(first.load(&slot(1), exec), DiskLoad::Hit(_)));
    }

    #[test]
    fn a_drop_wipes_the_directory() {
        let dir = private_dir();
        let cache = OwnerDiskCache::open(dir.path()).expect("open");
        let _ = cache.store(&slot(1), Uuid::new_v4(), &state(2), owned);
        let path = cache.file_path(key(1));
        assert!(path.exists());
        drop(cache);
        assert!(!path.exists());
        let again = OwnerDiskCache::open(dir.path()).expect("the lock is free");
        drop(again);
    }

    #[test]
    fn another_process_key_cannot_open_a_row() {
        let a = Sealer::new();
        let b = Sealer::new();
        let aad = aad("event", key(1), None, Some(0));
        let sealed = a.seal(&aad, b"plain").expect("seal");
        assert_eq!(a.open(&aad, &sealed).as_deref(), Some(&b"plain"[..]));
        assert!(b.open(&aad, &sealed).is_none());
    }

    #[test]
    fn nonces_never_repeat() {
        let sealer = Sealer::new();
        let first = sealer.next_nonce().expect("nonce");
        let second = sealer.next_nonce().expect("nonce");
        assert_ne!(first, second);
        sealer.counter.store(u64::MAX, Ordering::Relaxed);
        assert!(sealer.next_nonce().is_none(), "the counter refuses to wrap");
    }

    #[test]
    fn the_cache_file_filter_matches_only_cache_files() {
        assert!(is_cache_file("s0-p1.hcache"));
        assert!(is_cache_file("s-1-p511.hcache-wal"));
        assert!(!is_cache_file(LOCK_FILE_NAME));
        assert!(!is_cache_file("notes.hcache.bak"));
        assert!(!is_cache_file("data.sqlite"));
    }

    #[test]
    fn an_evicted_run_is_held_back_from_a_late_store() {
        let dir = private_dir();
        let cache = OwnerDiskCache::open(dir.path()).expect("open");
        let exec = Uuid::new_v4();
        let _ = cache.store(&slot(1), exec, &state(2), owned);
        assert!(cache.purge_evicted(exec));
        // A decision in flight commits after the evict wake.
        assert_eq!(
            cache.store(&slot(1), exec, &state(3), owned),
            DiskStore::Refused
        );
        assert!(!cache.contains(exec));
        let other = Uuid::new_v4();
        assert!(matches!(
            cache.store(&slot(1), other, &state(2), owned),
            DiskStore::Written { .. }
        ));
    }

    #[test]
    fn a_closed_partition_never_deletes_a_newer_file() {
        let dir = private_dir();
        let cache = OwnerDiskCache::open(dir.path()).expect("open");
        let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
        let _ = cache.store(&slot(1), a, &state(2), owned);
        let old = cache.existing_partition(key(1)).expect("partition");
        cache.drop_partitions(&[key(1)]);
        // A new tenure opens a new file for the same key.
        let _ = cache.store(&slot(1), b, &state(2), owned);
        let mut guard = lock(&old);
        cache.close_partition(key(1), &old, &mut guard);
        drop(guard);
        assert!(cache.file_path(key(1)).exists());
        assert!(cache.contains(b));
        assert!(matches!(cache.load(&slot(1), b), DiskLoad::Hit(_)));
    }

    #[cfg(unix)]
    #[test]
    fn open_refuses_a_directory_that_others_can_reach() {
        use std::os::unix::fs::PermissionsExt;
        let dir = private_dir();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755))
            .expect("chmod");
        let err = OwnerDiskCache::open(dir.path()).expect_err("an open directory");
        assert!(err.to_string().contains("open to other users"), "{err}");
        let mode = std::fs::metadata(dir.path())
            .expect("stat")
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o755, "the open never changes the mode");
    }

    #[cfg(unix)]
    #[test]
    fn open_creates_a_private_directory() {
        use std::os::unix::fs::PermissionsExt;
        let parent = private_dir();
        let dir = parent.path().join("cache");
        let cache = OwnerDiskCache::open(&dir).expect("open");
        let mode = std::fs::metadata(&dir).expect("stat").permissions().mode();
        assert_eq!(mode & 0o777, 0o700);
        drop(cache);
    }

    #[test]
    fn a_partition_without_a_live_tenure_is_unowned() {
        let dir = private_dir();
        let cache = OwnerDiskCache::open(dir.path()).expect("open");
        for partition in [1, 2, 3] {
            let _ = cache.store(&slot(partition), Uuid::new_v4(), &state(2), owned);
        }
        let mut cached = cache.cached_partitions();
        cached.sort();
        assert_eq!(cached, vec![key(1), key(2), key(3)]);
        let other_shard = OwnerKey {
            shard: ShardId::new(7),
            partition: key(4).partition,
        };
        cached.push(other_shard);
        let unowned = unowned_partitions(&cached, ShardId::UNENCODED, |k| k == key(2));
        assert_eq!(
            unowned,
            vec![key(1), key(3)],
            "only this shard, only lost tenures"
        );
        cache.drop_partitions(&unowned);
        assert_eq!(cache.cached_partitions(), vec![key(2)]);
    }

    #[tokio::test]
    async fn the_tier_keeps_no_short_history() {
        let dir = private_dir();
        let cache = Arc::new(OwnerDiskCache::open(dir.path()).expect("open"));
        let tier = DiskTier::new(
            Arc::clone(&cache),
            crate::payload_codec::PayloadCodecs::default(),
            4,
        );
        let owned = Arc::new(crate::decision_owner::OwnedPartitions::default());
        let _ = owned.adopt(crate::decision_owner::OwnerTenure::pinned(
            ShardId::UNENCODED,
            key(1).partition,
            crate::decision_owner::OwnerHolder::for_worker("disk-test"),
            OwnerEpoch::new(1),
            crate::decision_owner::OwnerLeaseTtl::DEFAULT,
        ));
        let (short, long) = (Uuid::new_v4(), Uuid::new_v4());
        let back = tier
            .write(
                key(1),
                OwnerEpoch::new(1),
                short,
                state(3),
                None,
                Arc::clone(&owned),
            )
            .await;
        assert_eq!(
            back.map(|s| s.next_event_id),
            Some(3),
            "the state comes back"
        );
        assert!(!cache.contains(short), "a short history stays in Postgres");
        let _ = tier
            .write(key(1), OwnerEpoch::new(1), long, state(4), None, owned)
            .await;
        assert!(cache.contains(long), "a history at the bound goes to disk");
    }

    fn entity(event_id: i32) -> ProjectedEntity {
        ProjectedEntity {
            event_id,
            deleted: false,
            snapshot: crate::entity::EntitySnapshot {
                state: json!({"secret": MARKER, "n": event_id}),
                stats: crate::entity::EntityStats::default(),
            },
        }
    }

    fn entity_rows(cache: &OwnerDiskCache, partition: u16) -> Vec<Vec<u8>> {
        let conn = Connection::open(cache.file_path(key(partition))).expect("open");
        let mut stmt = conn
            .prepare("SELECT sealed FROM harvest_entity_projection")
            .expect("prepare");
        stmt.query_map([], |row| row.get::<_, Vec<u8>>(0))
            .expect("query")
            .map(|row| row.expect("row"))
            .collect()
    }

    /// Issue #2260: a stored snapshot loads back, and a newer one replaces
    /// it.
    #[test]
    fn a_stored_entity_loads_back() {
        let dir = private_dir();
        let cache = OwnerDiskCache::open(dir.path()).expect("open");
        let exec = Uuid::new_v4();
        assert!(matches!(
            cache.load_entity(&slot(1), exec),
            EntityLoad::Miss
        ));
        assert_eq!(
            cache.store_entity(&slot(1), exec, &entity(3), owned),
            DiskStore::Written { appended: 1 }
        );
        assert!(cache.contains_entity(exec));
        assert!(!cache.contains(exec), "the run index stays apart");
        assert_eq!(cache.entity_event_id(exec), Some(3));
        let EntityLoad::Hit(loaded) = cache.load_entity(&slot(1), exec) else {
            panic!("expected a hit");
        };
        assert_eq!(loaded, entity(3));

        let _ = cache.store_entity(&slot(1), exec, &entity(5), owned);
        assert_eq!(entity_rows(&cache, 1).len(), 1, "one row for each run");
        let EntityLoad::Hit(loaded) = cache.load_entity(&slot(1), exec) else {
            panic!("expected a hit");
        };
        assert_eq!(loaded.event_id, 5);
        assert_eq!(cache.entity_event_id(exec), Some(5));
    }

    /// Issue #2260: the state is sealed. No plaintext reaches the disk.
    #[test]
    fn an_entity_row_holds_no_plaintext() {
        let dir = private_dir();
        let cache = OwnerDiskCache::open(dir.path()).expect("open");
        let _ = cache.store_entity(&slot(1), Uuid::new_v4(), &entity(2), owned);
        let raw = cache_bytes(&cache);
        assert!(!raw.is_empty());
        assert!(!contains(&raw, MARKER.as_bytes()));
        assert!(!contains(&raw, b"secret"));
    }

    /// Issue #2260: a purge deletes the entity row and the run rows, and
    /// leaves none of their bytes.
    #[test]
    fn a_purge_deletes_the_entity_row() {
        let dir = private_dir();
        let cache = OwnerDiskCache::open(dir.path()).expect("open");
        let exec = Uuid::new_v4();
        let _ = cache.store(&slot(1), exec, &state(3), owned);
        let _ = cache.store_entity(&slot(1), exec, &entity(2), owned);
        let rows = entity_rows(&cache, 1);
        assert_eq!(rows.len(), 1);
        assert!(cache.purge(exec));
        assert!(!cache.contains_entity(exec));
        assert!(!cache.contains(exec));
        assert!(entity_rows(&cache, 1).is_empty());
        let raw = cache_bytes(&cache);
        assert!(!contains(&raw, &rows[0]), "the sealed row is gone");
        assert!(!contains(&raw, exec.to_string().as_bytes()));

        // A run with only an entity row purges too.
        let other = Uuid::new_v4();
        let _ = cache.store_entity(&slot(1), other, &entity(4), owned);
        assert!(cache.purge(other));
        assert!(!cache.contains_entity(other));
        assert!(!cache.purge(other), "a second purge finds nothing");
    }

    /// Issue #2260: an evict wake holds back a late entity store.
    #[test]
    fn an_evicted_entity_is_held_back() {
        let dir = private_dir();
        let cache = OwnerDiskCache::open(dir.path()).expect("open");
        let exec = Uuid::new_v4();
        let _ = cache.store_entity(&slot(1), exec, &entity(2), owned);
        assert!(cache.purge_evicted(exec));
        assert_eq!(
            cache.store_entity(&slot(1), exec, &entity(3), owned),
            DiskStore::Refused
        );
        assert!(!cache.contains_entity(exec));
    }

    /// Issue #2260: an ended tenure refuses the store and purges the row.
    #[test]
    fn an_ended_tenure_refuses_an_entity_store() {
        let dir = private_dir();
        let cache = OwnerDiskCache::open(dir.path()).expect("open");
        let exec = Uuid::new_v4();
        let _ = cache.store_entity(&slot(1), exec, &entity(2), owned);
        assert_eq!(
            cache.store_entity(&slot(1), exec, &entity(3), || false),
            DiskStore::Refused
        );
        assert!(!cache.contains_entity(exec));
        assert!(entity_rows(&cache, 1).is_empty());
    }

    /// Issue #2260: a new epoch or codec generation finds the row stale and
    /// deletes the file.
    #[test]
    fn a_new_slot_finds_the_entity_stale() {
        let dir = private_dir();
        let cache = OwnerDiskCache::open(dir.path()).expect("open");
        let exec = Uuid::new_v4();
        let _ = cache.store_entity(&slot(1), exec, &entity(2), owned);
        let next = DiskSlot {
            epoch: OwnerEpoch::new(2),
            ..slot(1)
        };
        assert!(matches!(cache.load_entity(&next, exec), EntityLoad::Stale));
        assert!(!cache.contains_entity(exec));
        assert!(!cache.file_path(key(1)).exists());

        let _ = cache.store_entity(&slot(1), exec, &entity(2), owned);
        assert_eq!(cache.retain_codec_generation(7), 1);
        assert!(!cache.contains_entity(exec));
        assert_eq!(cache_bytes(&cache).len(), 0);
    }

    /// Issue #2260: a changed row fails its seal. The load purges it.
    #[test]
    fn a_flipped_entity_byte_is_corrupt() {
        let dir = private_dir();
        let cache = OwnerDiskCache::open(dir.path()).expect("open");
        let exec = Uuid::new_v4();
        let _ = cache.store_entity(&slot(1), exec, &entity(2), owned);
        let mut row = entity_rows(&cache, 1).remove(0);
        let at = row.len() / 2;
        row[at] ^= 0x5a;
        let outside = Connection::open(cache.file_path(key(1))).expect("open");
        outside
            .execute(
                "UPDATE harvest_entity_projection SET sealed = ?1",
                params![row],
            )
            .expect("tamper");
        drop(outside);
        assert!(matches!(
            cache.load_entity(&slot(1), exec),
            EntityLoad::Corrupt
        ));
        assert!(!cache.contains_entity(exec));
        assert!(entity_rows(&cache, 1).is_empty());
    }

    /// Issue #2260: a file that left the directory reads as stale, for a run
    /// and for an entity. The next store makes a new file.
    #[test]
    fn a_deleted_file_reads_as_stale_and_a_store_rebuilds_it() {
        let dir = private_dir();
        let cache = OwnerDiskCache::open(dir.path()).expect("open");
        let exec = Uuid::new_v4();
        let _ = cache.store(&slot(1), exec, &state(3), owned);
        let _ = cache.store_entity(&slot(1), exec, &entity(2), owned);
        let path = cache.file_path(key(1));
        std::fs::remove_file(&path).expect("delete the file");

        assert!(matches!(
            cache.load_entity(&slot(1), exec),
            EntityLoad::Stale
        ));
        assert!(!cache.contains_entity(exec));
        assert!(!cache.contains(exec), "the file held the run rows too");
        assert!(matches!(cache.load(&slot(1), exec), DiskLoad::Miss));

        let _ = cache.store_entity(&slot(1), exec, &entity(2), owned);
        assert!(path.exists(), "the store makes a new file");
        assert!(matches!(
            cache.load_entity(&slot(1), exec),
            EntityLoad::Hit(_)
        ));

        // A run load sees a deleted file too.
        let _ = cache.store(&slot(1), exec, &state(3), owned);
        std::fs::remove_file(&path).expect("delete the file again");
        assert!(matches!(cache.load(&slot(1), exec), DiskLoad::Stale));
        assert!(!cache.contains_entity(exec));
    }

    /// Issue #2260: a store into a deleted file makes a new file, so the
    /// row is not lost in the deleted inode.
    #[test]
    fn a_store_after_a_deleted_file_makes_a_new_file() {
        let dir = private_dir();
        let cache = OwnerDiskCache::open(dir.path()).expect("open");
        let exec = Uuid::new_v4();
        let _ = cache.store_entity(&slot(1), exec, &entity(2), owned);
        let path = cache.file_path(key(1));
        std::fs::remove_file(&path).expect("delete the file");
        let _ = cache.store_entity(&slot(1), exec, &entity(3), owned);
        assert!(path.exists());
        assert_eq!(entity_rows(&cache, 1).len(), 1);
    }

    /// AC 3 at the module level. The cache stores decoded events, so no
    /// codec ciphertext reaches the disk. A key change makes each file
    /// stale.
    #[test]
    fn a_retired_codec_key_leaves_no_ciphertext() {
        use crate::aead_codec::{AeadCodec, DataKey};
        use crate::payload_codec::PayloadCodecs;

        let codecs = PayloadCodecs::default();
        AeadCodec::new("k1", &DataKey::generate())
            .expect("codec")
            .register_with(&codecs)
            .expect("register k1");
        codecs.set_active_key("k1").expect("activate k1");
        let stored: Vec<Value> = state(3)
            .events
            .iter()
            .map(|e| codecs.encode_event(e).expect("encode"))
            .collect();
        let envelope = stored[0]["data"]["input"].to_string();
        assert!(
            crate::payload_codec::codec_envelope_key_id(&stored[0]["data"]["input"]) == Some("k1"),
            "the stored input is a k1 envelope: {envelope}"
        );
        let decoded: Vec<WorkflowEvent> = stored
            .into_iter()
            .map(|v| codecs.decode_event(v).expect("decode"))
            .collect();

        let dir = private_dir();
        let cache = OwnerDiskCache::open(dir.path()).expect("open");
        let exec = Uuid::new_v4();
        let generation = codecs.key_state().epoch.generation;
        let at = DiskSlot {
            codec_generation: generation,
            ..slot(1)
        };
        let cached = CachedWorkflowState {
            events: decoded,
            next_event_id: 3,
        };
        let _ = cache.store(&at, exec, &cached, owned);
        let raw = cache_bytes(&cache);
        let mut checked = 0;
        for part in envelope.split(|c: char| !c.is_ascii_alphanumeric() && c != '+' && c != '/') {
            if part.len() >= 16 {
                checked += 1;
                assert!(!contains(&raw, part.as_bytes()), "codec ciphertext on disk");
            }
        }
        assert!(checked > 0, "the envelope holds a ciphertext segment");

        AeadCodec::new("k2", &DataKey::generate())
            .expect("codec")
            .register_with(&codecs)
            .expect("register k2");
        codecs.set_active_key("k2").expect("activate k2");
        let activated = codecs.key_state().epoch.generation;
        codecs.retire_key_local("k1").expect("retire k1");
        let rotated = codecs.key_state().epoch.generation;
        assert_ne!(
            rotated, activated,
            "the retirement alone changes the generation"
        );
        assert_eq!(cache.retain_codec_generation(rotated), 1);
        assert_eq!(cache_bytes(&cache).len(), 0, "no cache bytes remain");
        assert!(!cache.contains(exec));
    }

    /// The role of the killed owner in
    /// [`a_killed_owner_leaves_no_readable_bytes`]. It runs only in the
    /// child process.
    #[test]
    #[ignore = "the child role of a_killed_owner_leaves_no_readable_bytes"]
    fn killed_owner_child() {
        use std::io::Write as _;
        let Ok(dir) = std::env::var("HARVEST_DISK_CACHE_CHILD_DIR") else {
            return;
        };
        let cache = OwnerDiskCache::open(&dir).expect("open");
        let exec = Uuid::new_v4();
        let _ = cache.store(&slot(1), exec, &state(4), owned);
        // Issue #2260: an entity row with the marker in its state.
        let _ = cache.store_entity(&slot(1), exec, &entity(3), owned);
        assert!(cache.contains_entity(exec), "the entity row is on disk");
        // The terminal commit happened. The purge never runs.
        let mut out = std::io::stdout();
        writeln!(out, "CHILD-READY").expect("write");
        out.flush().expect("flush");
        std::thread::sleep(std::time::Duration::from_secs(120));
    }

    /// AC 2 at the module level. The owner dies by `SIGKILL` between the
    /// terminal commit and the purge. Its key dies with it. The file holds
    /// no plaintext, and the next open wipes it.
    #[test]
    fn a_killed_owner_leaves_no_readable_bytes() {
        use std::io::BufRead as _;
        let dir = private_dir();
        let exe = std::env::current_exe().expect("test binary");
        let mut child = std::process::Command::new(exe)
            .args([
                "owner_disk_cache::tests::killed_owner_child",
                "--exact",
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("HARVEST_DISK_CACHE_CHILD_DIR", dir.path())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .expect("spawn the owner");
        let stdout = child.stdout.take().expect("stdout");
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let ready = std::io::BufReader::new(stdout)
                .lines()
                .map_while(Result::ok)
                .any(|line| line.contains("CHILD-READY"));
            let _ = ready_tx.send(ready);
        });
        let ready = ready_rx.recv_timeout(std::time::Duration::from_secs(60));
        if ready != Ok(true) {
            let _ = child.kill();
            panic!("the child did not store a run in time: {ready:?}");
        }
        child.kill().expect("kill the owner");
        let _ = child.wait();

        let file = dir.path().join(file_name(key(1)));
        let raw = std::fs::read(&file).expect("the killed owner left its file");
        assert_ne!(raw.len(), 0, "the file holds rows");
        assert!(!contains(&raw, MARKER.as_bytes()));
        assert!(!contains(&raw, b"WorkflowStarted"));

        let restarted = OwnerDiskCache::open(dir.path()).expect("the lock died with the owner");
        assert!(
            !file.exists(),
            "the open wipes the file of the killed owner"
        );
        drop(restarted);
    }
}
