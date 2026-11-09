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
//! - An episode clear keeps a tombstone with the generation, the key of
//!   the clear and its result: a count and the fact hashes. It holds no
//!   episode text.
//! - A memory clear keeps the document with no blocks and no keys, so its
//!   version still grows. A transcript erase deletes the rows of a session.
//!
//! The `SQLite` stores open their own connection, and can share the file of
//! a `SqliteRuntime`. A call runs its statements on the polling thread, and
//! a busy file can hold that thread for up to 5 seconds. The Postgres
//! stores take a pool. In a sharded deployment, give them the pool of
//! shard 0.
//!
//! The stores keep text in clear, outside the payload codecs. An erase
//! removes it from the rows. The `SQLite` stores set `secure_delete`, so
//! a delete also zeroes the freed pages. A Postgres row stays until
//! `VACUUM`.

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

/// The most bytes of a stored edit error. A failed edit can quote model
/// text, so the bound keeps a document small.
pub const MAX_STORED_ERROR_BYTES: usize = 1_024;

/// The most reads and writes of one edit before a store gives up. Each
/// lost race means that another edit of the scope landed.
pub const MAX_SWAP_ATTEMPTS: u32 = 32;

/// The rows of the agent stores in one database.
///
/// The crate implements it for `SQLite` and Postgres. It is sealed, so an
/// app cannot implement it, and a change to it breaks no app.
#[doc(hidden)]
pub trait AgentStoreDb: sealed::Sealed + Send + Sync + 'static {
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

    /// The newest window with `digest` that is younger than `max_age`.
    fn get_window<'a>(
        &'a self,
        digest: &'a str,
        max_age: Duration,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, AgentError>>;

    /// Keep one window, and prune the old windows of its run.
    fn put_window<'a>(&'a self, put: WindowPut<'a>) -> BoxFuture<'a, Result<(), AgentError>>;

    /// The number of windows that `session` holds.
    fn count_windows<'a>(&'a self, session: &'a str) -> BoxFuture<'a, Result<u64, AgentError>>;

    /// Delete every window of `session`, and count them.
    fn erase_windows<'a>(&'a self, session: &'a str) -> BoxFuture<'a, Result<u64, AgentError>>;
}

mod sealed {
    /// Only the backends of this crate implement [`super::AgentStoreDb`].
    pub trait Sealed {}
}

/// A database error. The caller can retry it.
fn backend(err: impl std::fmt::Display) -> AgentError {
    AgentError::new(
        ErrorKind::Unavailable,
        format!("the agent store failed: {err}"),
    )
}

/// A JSON error with no row text. The `Display` of a serde error can
/// quote a stored string, so the message names only its class and place.
fn json_error(what: &str, err: &serde_json::Error) -> AgentError {
    AgentError::new(
        ErrorKind::Decode,
        format!(
            "an agent store row does not {what}: a {:?} error at line {}, column {}",
            err.classify(),
            err.line(),
            err.column()
        ),
    )
}

fn encode<T: Serialize>(doc: &T) -> Result<Vec<u8>, AgentError> {
    serde_json::to_vec(doc).map_err(|err| json_error("encode", &err))
}

fn decode<T: DeserializeOwned>(body: &[u8]) -> Result<T, AgentError> {
    serde_json::from_slice(body).map_err(|err| json_error("decode", &err))
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
    Err(lost_races())
}

/// Write the body that `edit` makes from the stored bytes, with a
/// compare-and-swap. Unlike [`update`], it does not need a document that
/// decodes, so a clear works on a row that an older or newer build wrote.
async fn replace<D, R>(
    db: &D,
    store: &'static str,
    scope: &str,
    mut edit: impl FnMut(Option<&[u8]>) -> Result<(Vec<u8>, R), AgentError>,
) -> Result<R, AgentError>
where
    D: AgentStoreDb,
{
    for _ in 0..MAX_SWAP_ATTEMPTS {
        let row = db.read_doc(store, scope).await?;
        let (body, out) = edit(row.as_ref().map(|row| row.body.as_slice()))?;
        if row.as_ref().is_some_and(|row| row.body == body) {
            return Ok(out);
        }
        if db
            .swap_doc(store, scope, row.map(|row| row.version), body)
            .await?
        {
            return Ok(out);
        }
    }
    Err(lost_races())
}

fn lost_races() -> AgentError {
    AgentError::new(
        ErrorKind::Unavailable,
        format!(
            "the agent store lost {MAX_SWAP_ATTEMPTS} races on one scope: retry the edit later"
        ),
    )
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
            replace(&self.db, STORE_EPISODES, scope.as_str(), |old| {
                let (mut doc, held) = match old.map(decode::<EpisodeDoc>) {
                    None => (EpisodeDoc::default(), None),
                    Some(Ok(doc)) => (doc, None),
                    Some(Err(_)) => lenient_episode_doc(old.unwrap_or_default()),
                };
                if let Some((last, cleared)) = &doc.cleared
                    && last == key
                {
                    return Ok((encode(&doc)?, cleared.clone()));
                }
                let mut cleared = doc.log.clear();
                if let Some(held) = held {
                    cleared.episodes = held;
                }
                doc.cleared = Some((key.clone(), cleared.clone()));
                Ok((encode(&doc)?, cleared))
            })
            .await
        })
    }
}

/// What a clear keeps of an episode document that does not decode in
/// full. That is an empty log with the old generation, and the number of
/// episodes that the log held. So a stale pass still marks nothing.
fn lenient_episode_doc(body: &[u8]) -> (EpisodeDoc, Option<u64>) {
    tracing::warn!("an episode document does not decode, so the clear keeps only its generation");
    let value: serde_json::Value = serde_json::from_slice(body).unwrap_or_default();
    let log = &value["log"];
    let generation = log["generation"].as_u64().unwrap_or(0);
    let held = log["episodes"]
        .as_array()
        .map_or(0, |episodes| episodes.len() as u64);
    let log =
        serde_json::from_value(serde_json::json!({ "generation": generation })).unwrap_or_default();
    (EpisodeDoc { log, cleared: None }, Some(held))
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
///
/// The kind is text. A kind that this build does not know reads as
/// `Tool`, so a row from a newer build still decodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredError {
    kind: String,
    message: String,
}

impl StoredError {
    fn of(err: &AgentError) -> Self {
        let kind = serde_json::to_value(err.kind())
            .ok()
            .and_then(|kind| kind.as_str().map(str::to_owned))
            .unwrap_or_default();
        Self {
            kind,
            message: err.message().to_owned(),
        }
    }

    fn to_error(&self) -> AgentError {
        let kind = serde_json::from_value(serde_json::Value::String(self.kind.clone()))
            .unwrap_or(ErrorKind::Tool);
        AgentError::new(kind, self.message.clone())
    }
}

/// The document of one memory scope.
///
/// A clear keeps the document with no blocks and no keys. It holds no
/// entry text. Its version still grows, so a writer that read the scope
/// before the clear cannot write over it.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
struct MemoryDoc {
    /// The blocks. `None` is a new or cleared scope, which reads as the
    /// template.
    #[serde(skip_serializing_if = "Option::is_none")]
    blocks: Option<Vec<MemoryBlock>>,
    /// The applied keys, oldest first.
    #[serde(skip_serializing_if = "VecDeque::is_empty")]
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
            MemoryDoc::default,
            |doc: &mut MemoryDoc| {
                let blocks = doc.blocks.get_or_insert_with(|| self.template.clone());
                if let Some(applied) = doc.applied.iter().find(|held| held.key == key.as_str()) {
                    return applied
                        .error
                        .as_ref()
                        .map_or_else(|| current(blocks, op), |error| Err(error.to_error()));
                }
                // The first call returns the error that a retry reads back.
                let result = apply_to(blocks, op, labels).map_err(|err| {
                    AgentError::new(
                        err.kind(),
                        crate::agent::cut(err.message(), MAX_STORED_ERROR_BYTES),
                    )
                });
                doc.applied.push_back(Applied {
                    key: key.as_str().to_owned(),
                    error: result.as_ref().err().map(StoredError::of),
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
            let doc: MemoryDoc =
                read(&self.db, STORE_MEMORY, scope.as_str(), MemoryDoc::default).await?;
            Ok(doc.blocks.unwrap_or_else(|| self.template.clone()))
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
                    // A new scope holds only this block, with no template.
                    let blocks = doc.blocks.get_or_insert_with(Vec::new);
                    if !blocks.iter().any(|held| held.label == block.label) {
                        blocks.push(block.clone());
                    }
                },
            )
            .await
        })
    }

    fn clear<'a>(&'a self, scope: &'a MemoryScope) -> BoxFuture<'a, Result<(), AgentError>> {
        Box::pin(async move {
            let empty = encode(&MemoryDoc::default())?;
            replace(&self.db, STORE_MEMORY, scope.as_str(), |_| {
                Ok((empty.clone(), ()))
            })
            .await
        })
    }
}

// ---------------------------------------------------------------------------
// The transcript store.
// ---------------------------------------------------------------------------

/// An age limit in whole seconds, rounded up. A limit under one second
/// then keeps a window for one second, not for none.
fn age_secs(age: Duration) -> u64 {
    age.as_secs()
        .saturating_add(u64::from(age.subsec_nanos() > 0))
}

/// A [`TranscriptStore`] on a database. Use [`SqliteTranscriptStore`] or
/// [`PostgresTranscriptStore`].
///
/// It keeps the newest [`DEFAULT_KEEP_PER_RUN`] windows of each run. A
/// read misses a window past [`DEFAULT_MAX_AGE`]. Each put deletes the
/// older windows of its run and a batch of expired windows. A read or write error is a
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
                max_age_secs: age_secs(self.max_age),
            };
            if let Err(err) = self.db.put_window(put).await {
                tracing::warn!(error = %err, "the transcript store did not keep a window");
            }
        })
    }

    fn count<'a>(
        &'a self,
        session: &'a SessionId,
    ) -> BoxFuture<'a, Result<Option<u64>, AgentError>> {
        Box::pin(async move { self.db.count_windows(session.as_str()).await.map(Some) })
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

    impl super::sealed::Sealed for SqliteAgentDb {}

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

        fn get_window<'a>(
            &'a self,
            digest: &'a str,
            max_age: Duration,
        ) -> BoxFuture<'a, Result<Option<Vec<u8>>, AgentError>> {
            Box::pin(std::future::ready(
                self.0
                    .get_window(digest, super::age_secs(max_age), now_ms())
                    .map_err(backend),
            ))
        }

        fn put_window<'a>(&'a self, put: WindowPut<'a>) -> BoxFuture<'a, Result<(), AgentError>> {
            Box::pin(std::future::ready(
                self.0.put_window(&put, now_ms()).map_err(backend),
            ))
        }

        fn count_windows<'a>(&'a self, session: &'a str) -> BoxFuture<'a, Result<u64, AgentError>> {
            Box::pin(std::future::ready(
                self.0.count_windows(session).map_err(backend),
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
        DocRow, WindowPut, count_agent_windows, erase_agent_windows, get_agent_window,
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

    impl super::sealed::Sealed for PostgresAgentDb {}

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

        fn get_window<'a>(
            &'a self,
            digest: &'a str,
            max_age: Duration,
        ) -> BoxFuture<'a, Result<Option<Vec<u8>>, AgentError>> {
            Box::pin(async move {
                let mut conn = self.0.get().await.map_err(backend)?;
                get_agent_window(&mut conn, digest, super::age_secs(max_age))
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

        fn count_windows<'a>(&'a self, session: &'a str) -> BoxFuture<'a, Result<u64, AgentError>> {
            Box::pin(async move {
                let mut conn = self.0.get().await.map_err(backend)?;
                count_agent_windows(&mut conn, session)
                    .await
                    .map_err(backend)
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

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

    use super::*;

    #[test]
    fn an_age_under_one_second_rounds_up() {
        assert_eq!(age_secs(Duration::ZERO), 0);
        assert_eq!(age_secs(Duration::from_millis(500)), 1);
        assert_eq!(age_secs(Duration::from_secs(2)), 2);
        assert_eq!(age_secs(Duration::from_millis(2_001)), 3);
        assert_eq!(age_secs(Duration::MAX), u64::MAX);
    }

    #[test]
    fn a_stored_error_of_an_unknown_kind_reads_as_a_tool_error() {
        let err = AgentError::new(ErrorKind::RateLimited, "slow down");
        let stored = StoredError::of(&err);
        assert_eq!(stored.kind, "rate_limited");
        assert_eq!(stored.to_error(), err);
        let newer = StoredError {
            kind: "kind_from_a_newer_build".into(),
            message: "m".into(),
        };
        assert_eq!(newer.to_error().kind(), ErrorKind::Tool);
    }

    fn add(text: &str) -> MemoryOp {
        MemoryOp::Add {
            block: "memory".into(),
            text: text.into(),
        }
    }

    fn entries(blocks: &[MemoryBlock]) -> Vec<String> {
        blocks
            .iter()
            .find(|block| block.label == "memory")
            .map(|block| block.entries.clone())
            .unwrap_or_default()
    }

    /// A backend that loses swaps on purpose. `rival` makes one competing
    /// edit land between a read and its swap. `lose` refuses that many
    /// swaps with no write.
    #[cfg(feature = "sqlite")]
    struct Racing {
        inner: SqliteAgentDb,
        rival: AtomicBool,
        lose: AtomicU32,
        swaps: AtomicU32,
    }

    #[cfg(feature = "sqlite")]
    impl Racing {
        fn new(rival: bool, lose: u32) -> Self {
            Self {
                inner: SqliteAgentDb::open_in_memory().unwrap(),
                rival: AtomicBool::new(rival),
                lose: AtomicU32::new(lose),
                swaps: AtomicU32::new(0),
            }
        }
    }

    #[cfg(feature = "sqlite")]
    impl sealed::Sealed for Racing {}

    #[cfg(feature = "sqlite")]
    impl AgentStoreDb for Racing {
        fn read_doc<'a>(
            &'a self,
            store: &'static str,
            scope: &'a str,
        ) -> BoxFuture<'a, Result<Option<DocRow>, AgentError>> {
            self.inner.read_doc(store, scope)
        }

        fn swap_doc<'a>(
            &'a self,
            store: &'static str,
            scope: &'a str,
            expected: Option<u64>,
            body: Vec<u8>,
        ) -> BoxFuture<'a, Result<bool, AgentError>> {
            Box::pin(async move {
                self.swaps.fetch_add(1, Ordering::SeqCst);
                if self.rival.swap(false, Ordering::SeqCst) {
                    DbMemoryStore::on(self.inner.clone())
                        .apply(&MemoryScope::new(scope), add("rival"), &EditKey::new("r"))
                        .await?;
                }
                if self
                    .lose
                    .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                    .is_ok()
                {
                    return Ok(false);
                }
                self.inner.swap_doc(store, scope, expected, body).await
            })
        }

        fn get_window<'a>(
            &'a self,
            digest: &'a str,
            max_age: Duration,
        ) -> BoxFuture<'a, Result<Option<Vec<u8>>, AgentError>> {
            self.inner.get_window(digest, max_age)
        }

        fn put_window<'a>(&'a self, put: WindowPut<'a>) -> BoxFuture<'a, Result<(), AgentError>> {
            self.inner.put_window(put)
        }

        fn count_windows<'a>(&'a self, session: &'a str) -> BoxFuture<'a, Result<u64, AgentError>> {
            self.inner.count_windows(session)
        }

        fn erase_windows<'a>(&'a self, session: &'a str) -> BoxFuture<'a, Result<u64, AgentError>> {
            self.inner.erase_windows(session)
        }
    }

    /// A rival edit that lands between the read and the swap is kept, and
    /// the edit applies again on top of it.
    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn a_lost_race_reads_again_and_keeps_both_edits() {
        let store = DbMemoryStore::on(Racing::new(true, 0));
        let scope = MemoryScope::new("agent:a/race");
        store
            .apply(&scope, add("mine"), &EditKey::new("m"))
            .await
            .unwrap();
        let blocks = store.load(&scope).await.unwrap();
        assert_eq!(entries(&blocks), ["rival", "mine"]);
        assert_eq!(
            store.db.swaps.load(Ordering::SeqCst),
            2,
            "one lost swap, one won"
        );
    }

    /// A scope that loses every race gives up with a retryable error, and
    /// writes nothing.
    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn a_store_gives_up_after_the_last_lost_race() {
        let store = DbMemoryStore::on(Racing::new(false, MAX_SWAP_ATTEMPTS));
        let scope = MemoryScope::new("agent:a/busy");
        let err = store
            .apply(&scope, add("mine"), &EditKey::new("m"))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unavailable);
        assert!(err.kind().is_retryable());
        assert_eq!(store.db.swaps.load(Ordering::SeqCst), MAX_SWAP_ATTEMPTS);
        assert_eq!(
            entries(&store.load(&scope).await.unwrap()),
            Vec::<String>::new()
        );
        // One race fewer, and the edit lands.
        let store = DbMemoryStore::on(Racing::new(false, MAX_SWAP_ATTEMPTS - 1));
        store
            .apply(&scope, add("mine"), &EditKey::new("m"))
            .await
            .unwrap();
        assert_eq!(entries(&store.load(&scope).await.unwrap()), ["mine"]);
    }

    /// A writer that read the scope before a clear cannot write over the
    /// edits after it, because a clear never resets the version.
    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn a_stale_write_after_a_clear_loses() {
        let db = SqliteAgentDb::open_in_memory().unwrap();
        let store = DbMemoryStore::on(db.clone());
        let scope = MemoryScope::new("agent:a/b");
        store
            .apply(&scope, add("secret"), &EditKey::new("k1"))
            .await
            .unwrap();
        let stale = db
            .read_doc(STORE_MEMORY, "agent:a/b")
            .await
            .unwrap()
            .unwrap();
        store.clear(&scope).await.unwrap();
        store
            .apply(&scope, add("after"), &EditKey::new("k2"))
            .await
            .unwrap();
        let won = db
            .swap_doc(STORE_MEMORY, "agent:a/b", Some(stale.version), stale.body)
            .await
            .unwrap();
        assert!(!won, "the stale write must lose");
        let blocks = store.load(&scope).await.unwrap();
        assert_eq!(
            entries(&blocks),
            ["after"],
            "the clear stays, and the new edit too"
        );
    }

    /// A cleared scope reads as a new one: the template, and no keys.
    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn a_cleared_scope_keeps_no_entry_text() {
        let db = SqliteAgentDb::open_in_memory().unwrap();
        let store = DbMemoryStore::on(db.clone());
        let scope = MemoryScope::new("agent:a/c");
        store
            .apply(&scope, add("secret"), &EditKey::new("k1"))
            .await
            .unwrap();
        store.clear(&scope).await.unwrap();
        let row = db
            .read_doc(STORE_MEMORY, "agent:a/c")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.body, b"{}", "the tombstone holds no text");
        assert_eq!(store.load(&scope).await.unwrap(), MemoryBlock::defaults());
    }

    /// A row that this build cannot decode still clears, so a bad row
    /// never blocks an erase. The episode clear keeps the generation.
    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn a_row_that_does_not_decode_still_clears() {
        let db = SqliteAgentDb::open_in_memory().unwrap();
        let bad = br#"{"log":{"generation":4,"episodes":[{"id":"e1","stop":{"type":"from_a_newer_build"}}]}}"#;
        assert!(
            db.swap_doc(STORE_EPISODES, "ep", None, bad.to_vec())
                .await
                .unwrap()
        );
        let episodes = DbEpisodeStore::on(db.clone());
        let scope = MemoryScope::new("ep");
        assert!(
            episodes.load(&scope).await.is_err(),
            "the row does not decode"
        );
        let cleared = episodes.clear(&scope, &EditKey::new("r")).await.unwrap();
        assert_eq!(cleared.episodes, 1);
        let log = episodes.load(&scope).await.unwrap();
        assert!(log.is_empty());
        assert_eq!(log.generation(), 5, "the clear moves the generation on");
        assert_eq!(
            episodes.clear(&scope, &EditKey::new("r")).await.unwrap(),
            cleared
        );

        assert!(
            db.swap_doc(STORE_MEMORY, "mem", None, b"not json".to_vec())
                .await
                .unwrap()
        );
        let memory = DbMemoryStore::on(db.clone());
        let scope = MemoryScope::new("mem");
        memory.clear(&scope).await.unwrap();
        assert_eq!(memory.load(&scope).await.unwrap(), MemoryBlock::defaults());
    }

    /// A store with a template starts each new and each cleared scope with
    /// it.
    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn a_template_seeds_new_and_cleared_scopes() {
        let template = vec![MemoryBlock::new("notes", "Notes.", 100)];
        let store = DbMemoryStore::on(SqliteAgentDb::open_in_memory().unwrap())
            .with_template(template.clone());
        let scope = MemoryScope::new("agent:a/t");
        assert_eq!(store.load(&scope).await.unwrap(), template);
        let op = MemoryOp::Add {
            block: "notes".into(),
            text: "n1".into(),
        };
        store.apply(&scope, op, &EditKey::new("k")).await.unwrap();
        store.clear(&scope).await.unwrap();
        assert_eq!(store.load(&scope).await.unwrap(), template);
    }

    fn one(text: &str) -> Vec<ChatMessage> {
        vec![ChatMessage::text(crate::message::ChatRole::User, text)]
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn keep_per_run_bounds_the_windows_of_a_run() {
        let store = DbTranscriptStore::on(SqliteAgentDb::open_in_memory().unwrap()).keep_per_run(0);
        let session = SessionId::new("a/k");
        for n in 0..3 {
            let window = one(&format!("w{n}"));
            store
                .put(
                    WindowOwner::new("r", Some(&session)),
                    &format!("d{n}"),
                    &window,
                )
                .await;
        }
        assert_eq!(
            store.count(&session).await.unwrap(),
            Some(1),
            "0 counts as 1"
        );
        assert_eq!(store.get("d2").await, Some(one("w2")), "the newest stays");
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn an_expired_window_is_a_miss_and_a_put_deletes_it() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("agent.db");
        let store = DbTranscriptStore::on(SqliteAgentDb::open(&path).unwrap())
            .max_age(Duration::from_secs(60));
        let session = SessionId::new("a/x");
        store
            .put(WindowOwner::new("r1", Some(&session)), "old", &one("old"))
            .await;
        rusqlite::Connection::open(&path)
            .unwrap()
            .execute(
                "UPDATE harvest_agent_transcripts SET created_at_ms = created_at_ms - 120000",
                [],
            )
            .unwrap();
        assert_eq!(store.get("old").await, None, "past the age limit");
        store
            .put(WindowOwner::new("r2", None), "new", &one("new"))
            .await;
        assert_eq!(
            store.count(&session).await.unwrap(),
            Some(0),
            "the put deleted it"
        );
        assert_eq!(store.get("new").await, Some(one("new")));
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn an_erase_of_the_empty_session_is_refused() {
        let store = DbTranscriptStore::on(SqliteAgentDb::open_in_memory().unwrap());
        let err = store.erase(&SessionId::new("")).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Config);
    }
}
