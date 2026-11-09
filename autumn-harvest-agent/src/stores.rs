//! Durable episode, memory and transcript stores on `SQLite` and Postgres.
//!
//! Each store keeps its rows in two tables. `harvest_agent_store_docs`
//! holds one document per store and scope: an [`EpisodeLog`] or a list of
//! memory blocks, with the applied edit keys. `harvest_agent_transcripts`
//! holds the windows of model turns, keyed by digest and session.
//!
//! The rules come from the in-memory stores:
//!
//! - A store applies an edit key once. The key goes into the document, so
//!   one statement writes the edit and its key. A retry after a crash finds
//!   the key and changes nothing.
//! - A write names the version that it read. A write that loses the race
//!   reads again and applies the edit to the newer document. So two
//!   processes on one scope lose no edit.
//! - An episode clear keeps a tombstone with the generation and the counts
//!   of the clear. It holds no episode text.
//! - A memory clear and a transcript erase delete their rows.
//!
//! The `SQLite` stores open their own connection, and can share the file of
//! a `SqliteRuntime`. A call runs its statements on the polling thread, and
//! a busy file can hold that thread for up to 5 seconds. The Postgres
//! stores take a pool. In a sharded deployment, give them the pool of
//! shard 0.
//!
//! The stores keep text in clear, outside the payload codecs. An erase
//! removes it.

use std::collections::{BTreeSet, VecDeque};
use std::time::Duration;

use autumn_harvest::agent_store::{DocRow, STORE_EPISODES, STORE_MEMORY, WindowPut};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::episode::{
    Appended, Cleared, Episode, EpisodeLog, EpisodeStore, MAX_KEPT_KEYS, PassMark,
};
use crate::error::{AgentError, ErrorKind};
use crate::memory::{EditKey, MemoryBlock, MemoryOp, MemoryScope, MemoryStore, apply_to, current};
use crate::message::{ChatMessage, SessionId};
use crate::model::BoxFuture;
use crate::transcript::{DEFAULT_KEEP_PER_RUN, DEFAULT_MAX_AGE, TranscriptStore, WindowOwner};

/// The most reads and writes of one edit before a store gives up. Each
/// lost race means that another edit of the scope landed.
pub const MAX_SWAP_ATTEMPTS: u32 = 32;

/// The rows of the agent stores in one database.
///
/// The crate implements it for `SQLite` and Postgres. The stores call it,
/// so an app does not.
#[doc(hidden)]
pub trait AgentStoreDb: Send + Sync + 'static {
    /// The document of `store` and `scope`.
    fn read_doc<'a>(
        &'a self,
        store: &'static str,
        scope: &'a str,
    ) -> BoxFuture<'a, Result<Option<DocRow>, AgentError>>;

    /// Write `body` when the stored version is still `expected`. Returns
    /// `false` when another writer changed the document first.
    fn swap_doc<'a>(
        &'a self,
        store: &'static str,
        scope: &'a str,
        expected: Option<u64>,
        body: Vec<u8>,
    ) -> BoxFuture<'a, Result<bool, AgentError>>;

    /// Delete the document of `store` and `scope`.
    fn delete_doc<'a>(
        &'a self,
        store: &'static str,
        scope: &'a str,
    ) -> BoxFuture<'a, Result<(), AgentError>>;

    /// The newest window with `digest` that is younger than `max_age`.
    fn get_window<'a>(
        &'a self,
        digest: &'a str,
        max_age: Duration,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, AgentError>>;

    /// Keep one window, and prune the old windows of its run.
    fn put_window<'a>(&'a self, put: WindowPut<'a>) -> BoxFuture<'a, Result<(), AgentError>>;

    /// Delete every window of `session`, and count them.
    fn erase_windows<'a>(&'a self, session: &'a str) -> BoxFuture<'a, Result<u64, AgentError>>;
}

/// A database error. The caller can retry it.
fn backend(err: impl std::fmt::Display) -> AgentError {
    AgentError::new(
        ErrorKind::Unavailable,
        format!("the agent store failed: {err}"),
    )
}

fn encode<T: Serialize>(doc: &T) -> Result<Vec<u8>, AgentError> {
    serde_json::to_vec(doc).map_err(|err| {
        AgentError::new(
            ErrorKind::Decode,
            format!("an agent store row does not encode: {err}"),
        )
    })
}

fn decode<T: DeserializeOwned>(body: &[u8]) -> Result<T, AgentError> {
    serde_json::from_slice(body).map_err(|err| {
        AgentError::new(
            ErrorKind::Decode,
            format!("an agent store row does not decode: {err}"),
        )
    })
}

/// Read the document of `store` and `scope`, or `init` for a new scope.
async fn read<D: AgentStoreDb, T: DeserializeOwned>(
    db: &D,
    store: &'static str,
    scope: &str,
    init: impl FnOnce() -> T,
) -> Result<T, AgentError> {
    match db.read_doc(store, scope).await? {
        Some(row) => decode(&row.body),
        None => Ok(init()),
    }
}

/// Apply `edit` to the document of `store` and `scope`, and write it with a
/// compare-and-swap. A lost race reads the newer document and applies the
/// edit again. An edit that changes no byte writes nothing.
///
/// # Errors
///
/// Returns a store error, a decode error, or an `Unavailable` error after
/// [`MAX_SWAP_ATTEMPTS`] lost races.
async fn update<D, T, R>(
    db: &D,
    store: &'static str,
    scope: &str,
    init: impl Fn() -> T,
    mut edit: impl FnMut(&mut T) -> R,
) -> Result<R, AgentError>
where
    D: AgentStoreDb,
    T: Serialize + DeserializeOwned,
{
    for _ in 0..MAX_SWAP_ATTEMPTS {
        let row = db.read_doc(store, scope).await?;
        let (expected, old, mut doc) = match row {
            Some(row) => {
                let doc = decode(&row.body)?;
                (Some(row.version), Some(row.body), doc)
            }
            None => (None, None, init()),
        };
        let out = edit(&mut doc);
        let body = encode(&doc)?;
        if old.as_ref() == Some(&body) {
            return Ok(out);
        }
        if db.swap_doc(store, scope, expected, body).await? {
            return Ok(out);
        }
    }
    Err(AgentError::new(
        ErrorKind::Unavailable,
        format!(
            "the agent store lost {MAX_SWAP_ATTEMPTS} races on one scope: retry the edit later"
        ),
    ))
}

// ---------------------------------------------------------------------------
// The episode store.
// ---------------------------------------------------------------------------

/// The document of one episode scope.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
struct EpisodeDoc {
    log: EpisodeLog,
    /// The last clear, with its key. A retry with the key reads it.
    #[serde(skip_serializing_if = "Option::is_none")]
    cleared: Option<(EditKey, Cleared)>,
}

/// An [`EpisodeStore`] on a database. Use [`SqliteEpisodeStore`] or
/// [`PostgresEpisodeStore`].
///
/// Its `Debug` output shows no episode.
pub struct DbEpisodeStore<D> {
    db: D,
}

impl<D> std::fmt::Debug for DbEpisodeStore<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbEpisodeStore").finish_non_exhaustive()
    }
}

impl<D: AgentStoreDb> DbEpisodeStore<D> {
    /// The store on `db`.
    #[must_use]
    pub const fn on(db: D) -> Self {
        Self { db }
    }
}

impl<D: AgentStoreDb> EpisodeStore for DbEpisodeStore<D> {
    fn load<'a>(&'a self, scope: &'a MemoryScope) -> BoxFuture<'a, Result<EpisodeLog, AgentError>> {
        Box::pin(async move {
            let doc: EpisodeDoc = read(
                &self.db,
                STORE_EPISODES,
                scope.as_str(),
                EpisodeDoc::default,
            )
            .await?;
            Ok(doc.log)
        })
    }

    fn append<'a>(
        &'a self,
        scope: &'a MemoryScope,
        key: &'a EditKey,
        episode: Episode,
        max_episodes: u32,
    ) -> BoxFuture<'a, Result<Appended, AgentError>> {
        Box::pin(async move {
            update(
                &self.db,
                STORE_EPISODES,
                scope.as_str(),
                EpisodeDoc::default,
                |doc: &mut EpisodeDoc| doc.log.append(key, episode.clone(), max_episodes),
            )
            .await
        })
    }

    fn mark<'a>(
        &'a self,
        scope: &'a MemoryScope,
        mark: PassMark,
    ) -> BoxFuture<'a, Result<bool, AgentError>> {
        Box::pin(async move {
            update(
                &self.db,
                STORE_EPISODES,
                scope.as_str(),
                EpisodeDoc::default,
                |doc: &mut EpisodeDoc| doc.log.mark(&mark),
            )
            .await
        })
    }

    fn clear<'a>(
        &'a self,
        scope: &'a MemoryScope,
        key: &'a EditKey,
    ) -> BoxFuture<'a, Result<Cleared, AgentError>> {
        Box::pin(async move {
            update(
                &self.db,
                STORE_EPISODES,
                scope.as_str(),
                EpisodeDoc::default,
                |doc: &mut EpisodeDoc| {
                    if let Some((last, cleared)) = &doc.cleared
                        && last == key
                    {
                        return cleared.clone();
                    }
                    let cleared = doc.log.clear();
                    doc.cleared = Some((key.clone(), cleared.clone()));
                    cleared
                },
            )
            .await
        })
    }
}

// ---------------------------------------------------------------------------
// The memory store.
// ---------------------------------------------------------------------------

/// The first result of one applied key.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Applied {
    key: String,
    /// The error of a failed edit. A retry returns it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    error: Option<StoredError>,
}

/// An [`AgentError`] as a document keeps it.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredError {
    kind: ErrorKind,
    message: String,
}

/// The document of one memory scope.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
struct MemoryDoc {
    blocks: Vec<MemoryBlock>,
    /// The applied keys, oldest first.
    applied: VecDeque<Applied>,
}

/// A [`MemoryStore`] on a database. Use [`SqliteMemoryStore`] or
/// [`PostgresMemoryStore`].
///
/// A new scope starts with [`MemoryBlock::defaults`]. The store keeps the
/// newest [`MAX_KEPT_KEYS`] keys of each scope. A retry comes soon after
/// its first attempt, so an old key is safe to forget.
///
/// Its `Debug` output shows no entry.
pub struct DbMemoryStore<D> {
    db: D,
    template: Vec<MemoryBlock>,
}

impl<D> std::fmt::Debug for DbMemoryStore<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbMemoryStore").finish_non_exhaustive()
    }
}

impl<D: AgentStoreDb> DbMemoryStore<D> {
    /// The store on `db`.
    #[must_use]
    pub fn on(db: D) -> Self {
        Self {
            db,
            template: MemoryBlock::defaults(),
        }
    }

    /// The store with new scopes that start with copies of `template`.
    #[must_use]
    pub fn with_template(mut self, template: Vec<MemoryBlock>) -> Self {
        self.template = template;
        self
    }

    fn template_doc(&self) -> MemoryDoc {
        MemoryDoc {
            blocks: self.template.clone(),
            applied: VecDeque::new(),
        }
    }

    async fn apply_now(
        &self,
        scope: &MemoryScope,
        op: &MemoryOp,
        key: &EditKey,
        labels: &BTreeSet<String>,
    ) -> Result<MemoryBlock, AgentError> {
        update(
            &self.db,
            STORE_MEMORY,
            scope.as_str(),
            || self.template_doc(),
            |doc: &mut MemoryDoc| {
                if let Some(applied) = doc.applied.iter().find(|held| held.key == key.as_str()) {
                    return match &applied.error {
                        Some(error) => Err(AgentError::new(error.kind, error.message.clone())),
                        None => current(&doc.blocks, op),
                    };
                }
                let result = apply_to(&mut doc.blocks, op, labels);
                doc.applied.push_back(Applied {
                    key: key.as_str().to_owned(),
                    error: result.as_ref().err().map(|err| StoredError {
                        kind: err.kind(),
                        message: err.message().to_owned(),
                    }),
                });
                while doc.applied.len() > MAX_KEPT_KEYS {
                    doc.applied.pop_front();
                }
                result
            },
        )
        .await?
    }
}

impl<D: AgentStoreDb> MemoryStore for DbMemoryStore<D> {
    fn load<'a>(
        &'a self,
        scope: &'a MemoryScope,
    ) -> BoxFuture<'a, Result<Vec<MemoryBlock>, AgentError>> {
        Box::pin(async move {
            let doc: MemoryDoc = read(&self.db, STORE_MEMORY, scope.as_str(), || {
                self.template_doc()
            })
            .await?;
            Ok(doc.blocks)
        })
    }

    fn apply<'a>(
        &'a self,
        scope: &'a MemoryScope,
        op: MemoryOp,
        key: &'a EditKey,
    ) -> BoxFuture<'a, Result<MemoryBlock, AgentError>> {
        Box::pin(async move { self.apply_now(scope, &op, key, &BTreeSet::new()).await })
    }

    fn apply_labelled<'a>(
        &'a self,
        scope: &'a MemoryScope,
        op: MemoryOp,
        key: &'a EditKey,
        labels: &'a BTreeSet<String>,
    ) -> BoxFuture<'a, Result<MemoryBlock, AgentError>> {
        Box::pin(async move { self.apply_now(scope, &op, key, labels).await })
    }

    fn ensure_block<'a>(
        &'a self,
        scope: &'a MemoryScope,
        block: MemoryBlock,
    ) -> BoxFuture<'a, Result<(), AgentError>> {
        Box::pin(async move {
            update(
                &self.db,
                STORE_MEMORY,
                scope.as_str(),
                MemoryDoc::default,
                |doc: &mut MemoryDoc| {
                    if !doc.blocks.iter().any(|held| held.label == block.label) {
                        doc.blocks.push(block.clone());
                    }
                },
            )
            .await
        })
    }

    fn clear<'a>(&'a self, scope: &'a MemoryScope) -> BoxFuture<'a, Result<(), AgentError>> {
        Box::pin(async move { self.db.delete_doc(STORE_MEMORY, scope.as_str()).await })
    }
}

// ---------------------------------------------------------------------------
// The transcript store.
// ---------------------------------------------------------------------------

/// A [`TranscriptStore`] on a database. Use [`SqliteTranscriptStore`] or
/// [`PostgresTranscriptStore`].
///
/// It keeps the newest [`DEFAULT_KEEP_PER_RUN`] windows of each run, and
/// no window past [`DEFAULT_MAX_AGE`]. Each put deletes the older windows
/// of its run and a batch of expired windows. A read or write error is a
/// miss, so the turn goes in the full form.
///
/// Its `Debug` output shows no window.
pub struct DbTranscriptStore<D> {
    db: D,
    keep_per_run: u32,
    max_age: Duration,
}

impl<D> std::fmt::Debug for DbTranscriptStore<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbTranscriptStore")
            .field("keep_per_run", &self.keep_per_run)
            .field("max_age", &self.max_age)
            .finish_non_exhaustive()
    }
}

impl<D: AgentStoreDb> DbTranscriptStore<D> {
    /// The store on `db`.
    #[must_use]
    pub const fn on(db: D) -> Self {
        Self {
            db,
            keep_per_run: DEFAULT_KEEP_PER_RUN,
            max_age: DEFAULT_MAX_AGE,
        }
    }

    /// Keep the newest `n` windows of each run. 0 counts as 1.
    #[must_use]
    pub const fn keep_per_run(mut self, n: u32) -> Self {
        self.keep_per_run = n;
        self
    }

    /// Read no window older than `age`, and delete it on a later put.
    #[must_use]
    pub const fn max_age(mut self, age: Duration) -> Self {
        self.max_age = age;
        self
    }
}

impl<D: AgentStoreDb> TranscriptStore for DbTranscriptStore<D> {
    fn get<'a>(&'a self, digest: &'a str) -> BoxFuture<'a, Option<Vec<ChatMessage>>> {
        Box::pin(async move {
            let body = match self.db.get_window(digest, self.max_age).await {
                Ok(body) => body?,
                Err(err) => {
                    tracing::warn!(error = %err, "the transcript store did not read a window");
                    return None;
                }
            };
            decode(&body)
                .inspect_err(|err| {
                    tracing::warn!(error = %err, "the transcript store holds a bad window");
                })
                .ok()
        })
    }

    fn put<'a>(
        &'a self,
        owner: WindowOwner<'a>,
        digest: &'a str,
        window: &'a [ChatMessage],
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let body = match encode(&window) {
                Ok(body) => body,
                Err(err) => {
                    tracing::warn!(error = %err, "the transcript store did not keep a window");
                    return;
                }
            };
            let put = WindowPut {
                digest,
                session: owner.session.map_or("", SessionId::as_str),
                run_id: owner.run,
                body: &body,
                keep: self.keep_per_run,
                max_age_secs: self.max_age.as_secs(),
            };
            if let Err(err) = self.db.put_window(put).await {
                tracing::warn!(error = %err, "the transcript store did not keep a window");
            }
        })
    }

    fn erase<'a>(
        &'a self,
        session: &'a SessionId,
    ) -> BoxFuture<'a, Result<Option<u64>, AgentError>> {
        Box::pin(async move {
            if session.as_str().is_empty() {
                return Err(AgentError::new(
                    ErrorKind::Config,
                    "an erase needs a session: the empty session holds the windows of runs \
                     with no session",
                ));
            }
            self.db.erase_windows(session.as_str()).await.map(Some)
        })
    }
}

// ---------------------------------------------------------------------------
// SQLite.
// ---------------------------------------------------------------------------

#[cfg(feature = "sqlite")]
pub use self::sqlite::{
    SqliteAgentDb, SqliteEpisodeStore, SqliteMemoryStore, SqliteTranscriptStore,
};

#[cfg(feature = "sqlite")]
mod sqlite {
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Duration;

    use autumn_harvest::agent_store::{DocRow, WindowPut};
    use autumn_harvest_sqlite::SqliteAgentStoreDb;

    use super::{AgentStoreDb, DbEpisodeStore, DbMemoryStore, DbTranscriptStore, backend};
    use crate::error::AgentError;
    use crate::model::BoxFuture;

    /// The rows of the agent stores in one `SQLite` file. Clone it to share
    /// one connection between stores.
    #[derive(Clone)]
    pub struct SqliteAgentDb(Arc<SqliteAgentStoreDb>);

    impl std::fmt::Debug for SqliteAgentDb {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("SqliteAgentDb").finish_non_exhaustive()
        }
    }

    impl SqliteAgentDb {
        /// Open the rows in the database at `path`, and create the tables.
        ///
        /// # Errors
        ///
        /// Returns an `Unavailable` error when the file does not open.
        pub fn open(path: impl AsRef<Path>) -> Result<Self, AgentError> {
            Ok(Self(Arc::new(
                SqliteAgentStoreDb::open(path).map_err(backend)?,
            )))
        }

        /// Open the rows in a new, private in-memory database.
        ///
        /// # Errors
        ///
        /// Returns an `Unavailable` error when the database does not open.
        pub fn open_in_memory() -> Result<Self, AgentError> {
            Ok(Self(Arc::new(
                SqliteAgentStoreDb::open_in_memory().map_err(backend)?,
            )))
        }
    }

    fn now_ms() -> i64 {
        autumn_harvest::chrono::Utc::now().timestamp_millis()
    }

    impl AgentStoreDb for SqliteAgentDb {
        fn read_doc<'a>(
            &'a self,
            store: &'static str,
            scope: &'a str,
        ) -> BoxFuture<'a, Result<Option<DocRow>, AgentError>> {
            Box::pin(std::future::ready(
                self.0.read_doc(store, scope).map_err(backend),
            ))
        }

        fn swap_doc<'a>(
            &'a self,
            store: &'static str,
            scope: &'a str,
            expected: Option<u64>,
            body: Vec<u8>,
        ) -> BoxFuture<'a, Result<bool, AgentError>> {
            Box::pin(std::future::ready(
                self.0
                    .swap_doc(store, scope, expected, &body, now_ms())
                    .map_err(backend),
            ))
        }

        fn delete_doc<'a>(
            &'a self,
            store: &'static str,
            scope: &'a str,
        ) -> BoxFuture<'a, Result<(), AgentError>> {
            Box::pin(std::future::ready(
                self.0.delete_doc(store, scope).map(|_| ()).map_err(backend),
            ))
        }

        fn get_window<'a>(
            &'a self,
            digest: &'a str,
            max_age: Duration,
        ) -> BoxFuture<'a, Result<Option<Vec<u8>>, AgentError>> {
            Box::pin(std::future::ready(
                self.0
                    .get_window(digest, max_age.as_secs(), now_ms())
                    .map_err(backend),
            ))
        }

        fn put_window<'a>(&'a self, put: WindowPut<'a>) -> BoxFuture<'a, Result<(), AgentError>> {
            Box::pin(std::future::ready(
                self.0.put_window(&put, now_ms()).map_err(backend),
            ))
        }

        fn erase_windows<'a>(&'a self, session: &'a str) -> BoxFuture<'a, Result<u64, AgentError>> {
            Box::pin(std::future::ready(
                self.0.erase_windows(session).map_err(backend),
            ))
        }
    }

    /// An [`EpisodeStore`](crate::episode::EpisodeStore) in a `SQLite` file.
    pub type SqliteEpisodeStore = DbEpisodeStore<SqliteAgentDb>;

    /// A [`MemoryStore`](crate::memory::MemoryStore) in a `SQLite` file.
    pub type SqliteMemoryStore = DbMemoryStore<SqliteAgentDb>;

    /// A [`TranscriptStore`](crate::transcript::TranscriptStore) in a
    /// `SQLite` file.
    pub type SqliteTranscriptStore = DbTranscriptStore<SqliteAgentDb>;

    impl DbEpisodeStore<SqliteAgentDb> {
        /// Open the store in the database at `path`.
        ///
        /// # Errors
        ///
        /// Returns an `Unavailable` error when the file does not open.
        pub fn open(path: impl AsRef<Path>) -> Result<Self, AgentError> {
            SqliteAgentDb::open(path).map(Self::on)
        }

        /// Open a store in a new, private in-memory database.
        ///
        /// # Errors
        ///
        /// Returns an `Unavailable` error when the database does not open.
        pub fn open_in_memory() -> Result<Self, AgentError> {
            SqliteAgentDb::open_in_memory().map(Self::on)
        }
    }

    impl DbMemoryStore<SqliteAgentDb> {
        /// Open the store in the database at `path`.
        ///
        /// # Errors
        ///
        /// Returns an `Unavailable` error when the file does not open.
        pub fn open(path: impl AsRef<Path>) -> Result<Self, AgentError> {
            SqliteAgentDb::open(path).map(Self::on)
        }

        /// Open a store in a new, private in-memory database.
        ///
        /// # Errors
        ///
        /// Returns an `Unavailable` error when the database does not open.
        pub fn open_in_memory() -> Result<Self, AgentError> {
            SqliteAgentDb::open_in_memory().map(Self::on)
        }
    }

    impl DbTranscriptStore<SqliteAgentDb> {
        /// Open the store in the database at `path`.
        ///
        /// # Errors
        ///
        /// Returns an `Unavailable` error when the file does not open.
        pub fn open(path: impl AsRef<Path>) -> Result<Self, AgentError> {
            SqliteAgentDb::open(path).map(Self::on)
        }

        /// Open a store in a new, private in-memory database.
        ///
        /// # Errors
        ///
        /// Returns an `Unavailable` error when the database does not open.
        pub fn open_in_memory() -> Result<Self, AgentError> {
            SqliteAgentDb::open_in_memory().map(Self::on)
        }
    }
}

// ---------------------------------------------------------------------------
// Postgres.
// ---------------------------------------------------------------------------

#[cfg(feature = "postgres")]
pub use self::postgres::{
    PostgresAgentDb, PostgresEpisodeStore, PostgresMemoryStore, PostgresTranscriptStore,
};

#[cfg(feature = "postgres")]
mod postgres {
    use std::time::Duration;

    use autumn_harvest::agent_store::{
        DocRow, WindowPut, delete_agent_doc, erase_agent_windows, get_agent_window,
        put_agent_window, read_agent_doc, swap_agent_doc,
    };
    use autumn_harvest::worker::DbPool;

    use super::{AgentStoreDb, DbEpisodeStore, DbMemoryStore, DbTranscriptStore, backend};
    use crate::error::AgentError;
    use crate::model::BoxFuture;

    /// The rows of the agent stores on a Postgres pool.
    #[derive(Clone)]
    pub struct PostgresAgentDb(DbPool);

    impl std::fmt::Debug for PostgresAgentDb {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("PostgresAgentDb").finish_non_exhaustive()
        }
    }

    impl PostgresAgentDb {
        /// The rows on `pool`.
        #[must_use]
        pub const fn new(pool: DbPool) -> Self {
            Self(pool)
        }
    }

    impl AgentStoreDb for PostgresAgentDb {
        fn read_doc<'a>(
            &'a self,
            store: &'static str,
            scope: &'a str,
        ) -> BoxFuture<'a, Result<Option<DocRow>, AgentError>> {
            Box::pin(async move {
                let mut conn = self.0.get().await.map_err(backend)?;
                read_agent_doc(&mut conn, store, scope)
                    .await
                    .map_err(backend)
            })
        }

        fn swap_doc<'a>(
            &'a self,
            store: &'static str,
            scope: &'a str,
            expected: Option<u64>,
            body: Vec<u8>,
        ) -> BoxFuture<'a, Result<bool, AgentError>> {
            Box::pin(async move {
                let mut conn = self.0.get().await.map_err(backend)?;
                swap_agent_doc(&mut conn, store, scope, expected, &body)
                    .await
                    .map_err(backend)
            })
        }

        fn delete_doc<'a>(
            &'a self,
            store: &'static str,
            scope: &'a str,
        ) -> BoxFuture<'a, Result<(), AgentError>> {
            Box::pin(async move {
                let mut conn = self.0.get().await.map_err(backend)?;
                delete_agent_doc(&mut conn, store, scope)
                    .await
                    .map(|_| ())
                    .map_err(backend)
            })
        }

        fn get_window<'a>(
            &'a self,
            digest: &'a str,
            max_age: Duration,
        ) -> BoxFuture<'a, Result<Option<Vec<u8>>, AgentError>> {
            Box::pin(async move {
                let mut conn = self.0.get().await.map_err(backend)?;
                get_agent_window(&mut conn, digest, max_age.as_secs())
                    .await
                    .map_err(backend)
            })
        }

        fn put_window<'a>(&'a self, put: WindowPut<'a>) -> BoxFuture<'a, Result<(), AgentError>> {
            Box::pin(async move {
                let mut conn = self.0.get().await.map_err(backend)?;
                put_agent_window(&mut conn, &put).await.map_err(backend)
            })
        }

        fn erase_windows<'a>(&'a self, session: &'a str) -> BoxFuture<'a, Result<u64, AgentError>> {
            Box::pin(async move {
                let mut conn = self.0.get().await.map_err(backend)?;
                erase_agent_windows(&mut conn, session)
                    .await
                    .map_err(backend)
            })
        }
    }

    /// An [`EpisodeStore`](crate::episode::EpisodeStore) on Postgres.
    pub type PostgresEpisodeStore = DbEpisodeStore<PostgresAgentDb>;

    /// A [`MemoryStore`](crate::memory::MemoryStore) on Postgres.
    pub type PostgresMemoryStore = DbMemoryStore<PostgresAgentDb>;

    /// A [`TranscriptStore`](crate::transcript::TranscriptStore) on
    /// Postgres.
    pub type PostgresTranscriptStore = DbTranscriptStore<PostgresAgentDb>;

    impl DbEpisodeStore<PostgresAgentDb> {
        /// A store on `pool`.
        #[must_use]
        pub const fn new(pool: DbPool) -> Self {
            Self::on(PostgresAgentDb::new(pool))
        }
    }

    impl DbMemoryStore<PostgresAgentDb> {
        /// A store on `pool`.
        #[must_use]
        pub fn new(pool: DbPool) -> Self {
            Self::on(PostgresAgentDb::new(pool))
        }
    }

    impl DbTranscriptStore<PostgresAgentDb> {
        /// A store on `pool`.
        #[must_use]
        pub const fn new(pool: DbPool) -> Self {
            Self::on(PostgresAgentDb::new(pool))
        }
    }
}
