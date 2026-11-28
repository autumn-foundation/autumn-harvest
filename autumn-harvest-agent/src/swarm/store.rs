//! The stores of a swarm board (issue #2814).
//!
//! A store keeps rows and compares versions. It decides nothing: the
//! [`Swarm`](super::Swarm) reads a row, asks the [`kernel`](super::kernel),
//! and writes with a compare-and-swap. So the three stores decide alike, and
//! `tests/swarm_store_suite` runs one suite on each.

use std::collections::BTreeMap;
use std::sync::{Mutex, PoisonError};

use autumn_harvest::swarm_board::{
    CommitOutcome, ItemQuery, ItemRow, MainRow, MergeCommit, MergeRow, NewPost, PostRow, bounded,
};

use super::SwarmError;
use crate::model::BoxFuture;

/// The rows of the boards of many swarms.
///
/// Each method takes the swarm name first. The crate implements the trait
/// on memory, `SQLite` and Postgres. An app can implement it on another
/// store. It must keep the rules of `autumn_harvest::swarm_board`.
pub trait SwarmStore: Send + Sync + std::fmt::Debug {
    /// Insert the first main head. Returns `false` when the swarm already
    /// has one, and then changes nothing.
    fn seed_main<'a>(
        &'a self,
        swarm: &'a str,
        main: &'a MainRow,
    ) -> BoxFuture<'a, Result<bool, SwarmError>>;

    /// The main head, if the swarm has one.
    fn main<'a>(&'a self, swarm: &'a str) -> BoxFuture<'a, Result<Option<MainRow>, SwarmError>>;

    /// Insert an item. Returns `false` when the id is taken, and then
    /// changes nothing.
    fn insert_item<'a>(
        &'a self,
        swarm: &'a str,
        row: &'a ItemRow,
    ) -> BoxFuture<'a, Result<bool, SwarmError>>;

    /// The item with the id `item`, if any.
    fn item<'a>(
        &'a self,
        swarm: &'a str,
        item: &'a str,
    ) -> BoxFuture<'a, Result<Option<ItemRow>, SwarmError>>;

    /// The items that match `query`, in id order.
    fn items<'a>(
        &'a self,
        swarm: &'a str,
        query: ItemQuery<'a>,
    ) -> BoxFuture<'a, Result<Vec<ItemRow>, SwarmError>>;

    /// The number of items that `holder` holds.
    fn count_held<'a>(
        &'a self,
        swarm: &'a str,
        holder: &'a str,
    ) -> BoxFuture<'a, Result<u64, SwarmError>>;

    /// Write `next` over its item when the stored `rev` is `expected_rev`.
    /// The store sets the new `rev` to `expected_rev + 1`. Returns `false`
    /// when another write landed first.
    fn swap_item<'a>(
        &'a self,
        swarm: &'a str,
        expected_rev: u64,
        next: &'a ItemRow,
    ) -> BoxFuture<'a, Result<bool, SwarmError>>;

    /// Add a post, or return the post that its key already added.
    fn append_post<'a>(
        &'a self,
        swarm: &'a str,
        post: NewPost<'a>,
    ) -> BoxFuture<'a, Result<PostRow, SwarmError>>;

    /// The posts after `after`, in `seq` order, of one channel or of all.
    fn posts<'a>(
        &'a self,
        swarm: &'a str,
        channel: Option<&'a str>,
        after: u64,
        limit: u32,
    ) -> BoxFuture<'a, Result<Vec<PostRow>, SwarmError>>;

    /// Log a verdict that merges nothing, or return the verdict that its key
    /// already logged.
    fn record_verdict<'a>(
        &'a self,
        swarm: &'a str,
        record: &'a MergeRow,
    ) -> BoxFuture<'a, Result<MergeRow, SwarmError>>;

    /// The verdict that the submit `key` logged, if any.
    fn merge_by_key<'a>(
        &'a self,
        swarm: &'a str,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<MergeRow>, SwarmError>>;

    /// The verdicts after `after`, in `seq` order.
    fn merges<'a>(
        &'a self,
        swarm: &'a str,
        after: u64,
        limit: u32,
    ) -> BoxFuture<'a, Result<Vec<MergeRow>, SwarmError>>;

    /// Move main, close the item and log the verdict, all or nothing.
    fn commit_merge<'a>(
        &'a self,
        swarm: &'a str,
        commit: &'a MergeCommit,
    ) -> BoxFuture<'a, Result<CommitOutcome, SwarmError>>;
}

/// The board of one swarm in memory.
#[derive(Debug, Default)]
struct Board {
    main: Option<MainRow>,
    items: BTreeMap<String, ItemRow>,
    posts: Vec<PostRow>,
    merges: Vec<MergeRow>,
}

impl Board {
    fn merge_by_key(&self, key: &str) -> Option<MergeRow> {
        self.merges
            .iter()
            .find(|row| row.submit_key == key)
            .cloned()
    }

    fn log(&mut self, record: &MergeRow) -> MergeRow {
        let mut row = record.clone();
        row.seq = self.merges.last().map_or(1, |last| last.seq + 1);
        self.merges.push(row.clone());
        row
    }

    fn swap(&mut self, expected_rev: u64, next: &ItemRow) -> bool {
        match self.items.get_mut(&next.item) {
            Some(stored) if stored.rev == expected_rev => {
                let mut row = next.clone();
                row.rev = expected_rev.saturating_add(1);
                row.created_by.clone_from(&stored.created_by);
                row.created_at_ms = stored.created_at_ms;
                *stored = row;
                true
            }
            _ => false,
        }
    }
}

/// A swarm store in memory, for tests and one-process apps.
#[derive(Debug, Default)]
pub struct InMemorySwarmStore {
    boards: Mutex<BTreeMap<String, Board>>,
}

impl InMemorySwarmStore {
    /// An empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn with<T>(&self, swarm: &str, f: impl FnOnce(&mut Board) -> T) -> T {
        let mut boards = self.boards.lock().unwrap_or_else(PoisonError::into_inner);
        f(boards.entry(swarm.to_owned()).or_default())
    }
}

/// A ready future.
fn ready<'a, T: Send + 'a>(value: T) -> BoxFuture<'a, T> {
    Box::pin(std::future::ready(value))
}

impl SwarmStore for InMemorySwarmStore {
    fn seed_main<'a>(
        &'a self,
        swarm: &'a str,
        main: &'a MainRow,
    ) -> BoxFuture<'a, Result<bool, SwarmError>> {
        ready(Ok(self.with(swarm, |board| {
            if board.main.is_some() {
                false
            } else {
                board.main = Some(main.clone());
                true
            }
        })))
    }

    fn main<'a>(&'a self, swarm: &'a str) -> BoxFuture<'a, Result<Option<MainRow>, SwarmError>> {
        ready(Ok(self.with(swarm, |board| board.main.clone())))
    }

    fn insert_item<'a>(
        &'a self,
        swarm: &'a str,
        row: &'a ItemRow,
    ) -> BoxFuture<'a, Result<bool, SwarmError>> {
        ready(Ok(self.with(swarm, |board| {
            if board.items.contains_key(&row.item) {
                false
            } else {
                board.items.insert(row.item.clone(), row.clone());
                true
            }
        })))
    }

    fn item<'a>(
        &'a self,
        swarm: &'a str,
        item: &'a str,
    ) -> BoxFuture<'a, Result<Option<ItemRow>, SwarmError>> {
        ready(Ok(self.with(swarm, |board| board.items.get(item).cloned())))
    }

    fn items<'a>(
        &'a self,
        swarm: &'a str,
        query: ItemQuery<'a>,
    ) -> BoxFuture<'a, Result<Vec<ItemRow>, SwarmError>> {
        let limit = usize::try_from(query.bounded_limit()).unwrap_or(usize::MAX);
        ready(Ok(self.with(swarm, |board| {
            board
                .items
                .values()
                .filter(|row| query.lane.is_none_or(|lane| row.lane == lane))
                .filter(|row| query.status.is_none_or(|status| row.status == status))
                .filter(|row| {
                    query
                        .holder
                        .is_none_or(|h| row.holder.as_deref() == Some(h))
                })
                .filter(|row| query.after.is_none_or(|after| row.item.as_str() > after))
                .take(limit)
                .cloned()
                .collect()
        })))
    }

    fn count_held<'a>(
        &'a self,
        swarm: &'a str,
        holder: &'a str,
    ) -> BoxFuture<'a, Result<u64, SwarmError>> {
        ready(Ok(self.with(swarm, |board| {
            let held = board
                .items
                .values()
                .filter(|row| row.status == "claimed" && row.holder.as_deref() == Some(holder))
                .count();
            u64::try_from(held).unwrap_or(u64::MAX)
        })))
    }

    fn swap_item<'a>(
        &'a self,
        swarm: &'a str,
        expected_rev: u64,
        next: &'a ItemRow,
    ) -> BoxFuture<'a, Result<bool, SwarmError>> {
        ready(Ok(self.with(swarm, |board| board.swap(expected_rev, next))))
    }

    fn append_post<'a>(
        &'a self,
        swarm: &'a str,
        post: NewPost<'a>,
    ) -> BoxFuture<'a, Result<PostRow, SwarmError>> {
        ready(Ok(self.with(swarm, |board| {
            if let Some(found) = board.posts.iter().find(|row| row.post_key == post.post_key) {
                return found.clone();
            }
            let row = PostRow {
                seq: board.posts.last().map_or(1, |last| last.seq + 1),
                post_key: post.post_key.to_owned(),
                channel: post.channel.to_owned(),
                author: post.author.to_owned(),
                body: post.body.to_owned(),
                created_at_ms: post.at_ms,
            };
            board.posts.push(row.clone());
            row
        })))
    }

    fn posts<'a>(
        &'a self,
        swarm: &'a str,
        channel: Option<&'a str>,
        after: u64,
        limit: u32,
    ) -> BoxFuture<'a, Result<Vec<PostRow>, SwarmError>> {
        let limit = usize::try_from(bounded(limit)).unwrap_or(usize::MAX);
        ready(Ok(self.with(swarm, |board| {
            board
                .posts
                .iter()
                .filter(|row| row.seq > after)
                .filter(|row| channel.is_none_or(|channel| row.channel == channel))
                .take(limit)
                .cloned()
                .collect()
        })))
    }

    fn record_verdict<'a>(
        &'a self,
        swarm: &'a str,
        record: &'a MergeRow,
    ) -> BoxFuture<'a, Result<MergeRow, SwarmError>> {
        ready(Ok(self.with(swarm, |board| {
            board
                .merge_by_key(&record.submit_key)
                .unwrap_or_else(|| board.log(record))
        })))
    }

    fn merge_by_key<'a>(
        &'a self,
        swarm: &'a str,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<MergeRow>, SwarmError>> {
        ready(Ok(self.with(swarm, |board| board.merge_by_key(key))))
    }

    fn merges<'a>(
        &'a self,
        swarm: &'a str,
        after: u64,
        limit: u32,
    ) -> BoxFuture<'a, Result<Vec<MergeRow>, SwarmError>> {
        let limit = usize::try_from(bounded(limit)).unwrap_or(usize::MAX);
        ready(Ok(self.with(swarm, |board| {
            board
                .merges
                .iter()
                .filter(|row| row.seq > after)
                .take(limit)
                .cloned()
                .collect()
        })))
    }

    fn commit_merge<'a>(
        &'a self,
        swarm: &'a str,
        commit: &'a MergeCommit,
    ) -> BoxFuture<'a, Result<CommitOutcome, SwarmError>> {
        ready(Ok(self.with(swarm, |board| {
            if let Some(found) = board.merge_by_key(&commit.record.submit_key) {
                return CommitOutcome::Recorded(found);
            }
            let actual = board.main.as_ref().map(|main| main.version);
            if actual != Some(commit.expected_main) {
                return CommitOutcome::MainMoved { actual };
            }
            if !board.swap(commit.item_rev, &commit.next_item) {
                return CommitOutcome::ItemMoved;
            }
            board.main = Some(commit.next_main.clone());
            CommitOutcome::Committed(board.log(&commit.record))
        })))
    }
}

/// A store failure as a [`SwarmError`].
#[cfg(any(feature = "sqlite", feature = "postgres"))]
fn backend(err: impl std::fmt::Display) -> SwarmError {
    SwarmError::Backend(err.to_string())
}

/// A swarm store in a `SQLite` file.
///
/// A call runs its statements on the calling thread. It can share the file
/// of a `SqliteRuntime`.
#[cfg(feature = "sqlite")]
#[derive(Debug)]
pub struct SqliteSwarmStore {
    board: autumn_harvest_sqlite::SqliteSwarmBoard,
}

#[cfg(feature = "sqlite")]
impl SqliteSwarmStore {
    /// Open the store in the database at `path`.
    ///
    /// # Errors
    ///
    /// Returns [`SwarmError::Backend`] when the file does not open.
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, SwarmError> {
        let board = autumn_harvest_sqlite::SqliteSwarmBoard::open(path).map_err(backend)?;
        Ok(Self { board })
    }

    /// Open the store on the connections that `connect` opens, as
    /// `SqliteOverlayStore::with_connector` does.
    ///
    /// # Errors
    ///
    /// Returns [`SwarmError::Backend`] when `connect` fails or the schema
    /// cannot be applied.
    pub fn with_connector(
        connect: autumn_harvest_sqlite::SqliteConnector,
    ) -> Result<Self, SwarmError> {
        let board =
            autumn_harvest_sqlite::SqliteSwarmBoard::with_connector(connect).map_err(backend)?;
        Ok(Self { board })
    }

    /// Open a store in a new, private in-memory database.
    ///
    /// # Errors
    ///
    /// Returns [`SwarmError::Backend`] when the database does not open.
    pub fn open_in_memory() -> Result<Self, SwarmError> {
        let board = autumn_harvest_sqlite::SqliteSwarmBoard::open_in_memory().map_err(backend)?;
        Ok(Self { board })
    }
}

#[cfg(feature = "sqlite")]
impl SwarmStore for SqliteSwarmStore {
    fn seed_main<'a>(
        &'a self,
        swarm: &'a str,
        main: &'a MainRow,
    ) -> BoxFuture<'a, Result<bool, SwarmError>> {
        ready(self.board.seed_main(swarm, main).map_err(backend))
    }

    fn main<'a>(&'a self, swarm: &'a str) -> BoxFuture<'a, Result<Option<MainRow>, SwarmError>> {
        ready(self.board.main(swarm).map_err(backend))
    }

    fn insert_item<'a>(
        &'a self,
        swarm: &'a str,
        row: &'a ItemRow,
    ) -> BoxFuture<'a, Result<bool, SwarmError>> {
        ready(self.board.insert_item(swarm, row).map_err(backend))
    }

    fn item<'a>(
        &'a self,
        swarm: &'a str,
        item: &'a str,
    ) -> BoxFuture<'a, Result<Option<ItemRow>, SwarmError>> {
        ready(self.board.item(swarm, item).map_err(backend))
    }

    fn items<'a>(
        &'a self,
        swarm: &'a str,
        query: ItemQuery<'a>,
    ) -> BoxFuture<'a, Result<Vec<ItemRow>, SwarmError>> {
        ready(self.board.items(swarm, &query).map_err(backend))
    }

    fn count_held<'a>(
        &'a self,
        swarm: &'a str,
        holder: &'a str,
    ) -> BoxFuture<'a, Result<u64, SwarmError>> {
        ready(self.board.count_held(swarm, holder).map_err(backend))
    }

    fn swap_item<'a>(
        &'a self,
        swarm: &'a str,
        expected_rev: u64,
        next: &'a ItemRow,
    ) -> BoxFuture<'a, Result<bool, SwarmError>> {
        ready(
            self.board
                .swap_item(swarm, expected_rev, next)
                .map_err(backend),
        )
    }

    fn append_post<'a>(
        &'a self,
        swarm: &'a str,
        post: NewPost<'a>,
    ) -> BoxFuture<'a, Result<PostRow, SwarmError>> {
        ready(self.board.append_post(swarm, &post).map_err(backend))
    }

    fn posts<'a>(
        &'a self,
        swarm: &'a str,
        channel: Option<&'a str>,
        after: u64,
        limit: u32,
    ) -> BoxFuture<'a, Result<Vec<PostRow>, SwarmError>> {
        ready(
            self.board
                .posts(swarm, channel, after, limit)
                .map_err(backend),
        )
    }

    fn record_verdict<'a>(
        &'a self,
        swarm: &'a str,
        record: &'a MergeRow,
    ) -> BoxFuture<'a, Result<MergeRow, SwarmError>> {
        ready(self.board.record_verdict(swarm, record).map_err(backend))
    }

    fn merge_by_key<'a>(
        &'a self,
        swarm: &'a str,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<MergeRow>, SwarmError>> {
        ready(self.board.merge_by_key(swarm, key).map_err(backend))
    }

    fn merges<'a>(
        &'a self,
        swarm: &'a str,
        after: u64,
        limit: u32,
    ) -> BoxFuture<'a, Result<Vec<MergeRow>, SwarmError>> {
        ready(self.board.merges(swarm, after, limit).map_err(backend))
    }

    fn commit_merge<'a>(
        &'a self,
        swarm: &'a str,
        commit: &'a MergeCommit,
    ) -> BoxFuture<'a, Result<CommitOutcome, SwarmError>> {
        ready(self.board.commit_merge(swarm, commit).map_err(backend))
    }
}

/// A swarm store on Postgres. In a sharded deployment, give it the pool of
/// shard 0.
#[cfg(feature = "postgres")]
pub struct PostgresSwarmStore {
    pool: autumn_harvest::worker::DbPool,
}

#[cfg(feature = "postgres")]
impl std::fmt::Debug for PostgresSwarmStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresSwarmStore").finish_non_exhaustive()
    }
}

#[cfg(feature = "postgres")]
impl PostgresSwarmStore {
    /// A store on `pool`. The database needs the migration
    /// `20261128001506_harvest_swarm_board`.
    #[must_use]
    pub const fn new(pool: autumn_harvest::worker::DbPool) -> Self {
        Self { pool }
    }

    async fn conn(
        &self,
    ) -> Result<
        impl std::ops::DerefMut<Target = autumn_harvest::diesel_async::AsyncPgConnection>,
        SwarmError,
    > {
        self.pool.get().await.map_err(backend)
    }
}

#[cfg(feature = "postgres")]
impl SwarmStore for PostgresSwarmStore {
    fn seed_main<'a>(
        &'a self,
        swarm: &'a str,
        main: &'a MainRow,
    ) -> BoxFuture<'a, Result<bool, SwarmError>> {
        Box::pin(async move {
            let mut conn = self.conn().await?;
            autumn_harvest::swarm_board::seed_swarm_main(&mut conn, swarm, main)
                .await
                .map_err(backend)
        })
    }

    fn main<'a>(&'a self, swarm: &'a str) -> BoxFuture<'a, Result<Option<MainRow>, SwarmError>> {
        Box::pin(async move {
            let mut conn = self.conn().await?;
            autumn_harvest::swarm_board::swarm_main(&mut conn, swarm)
                .await
                .map_err(backend)
        })
    }

    fn insert_item<'a>(
        &'a self,
        swarm: &'a str,
        row: &'a ItemRow,
    ) -> BoxFuture<'a, Result<bool, SwarmError>> {
        Box::pin(async move {
            let mut conn = self.conn().await?;
            autumn_harvest::swarm_board::insert_swarm_item(&mut conn, swarm, row)
                .await
                .map_err(backend)
        })
    }

    fn item<'a>(
        &'a self,
        swarm: &'a str,
        item: &'a str,
    ) -> BoxFuture<'a, Result<Option<ItemRow>, SwarmError>> {
        Box::pin(async move {
            let mut conn = self.conn().await?;
            autumn_harvest::swarm_board::swarm_item(&mut conn, swarm, item)
                .await
                .map_err(backend)
        })
    }

    fn items<'a>(
        &'a self,
        swarm: &'a str,
        query: ItemQuery<'a>,
    ) -> BoxFuture<'a, Result<Vec<ItemRow>, SwarmError>> {
        Box::pin(async move {
            let mut conn = self.conn().await?;
            autumn_harvest::swarm_board::swarm_items(&mut conn, swarm, &query)
                .await
                .map_err(backend)
        })
    }

    fn count_held<'a>(
        &'a self,
        swarm: &'a str,
        holder: &'a str,
    ) -> BoxFuture<'a, Result<u64, SwarmError>> {
        Box::pin(async move {
            let mut conn = self.conn().await?;
            autumn_harvest::swarm_board::count_swarm_held(&mut conn, swarm, holder)
                .await
                .map_err(backend)
        })
    }

    fn swap_item<'a>(
        &'a self,
        swarm: &'a str,
        expected_rev: u64,
        next: &'a ItemRow,
    ) -> BoxFuture<'a, Result<bool, SwarmError>> {
        Box::pin(async move {
            let mut conn = self.conn().await?;
            autumn_harvest::swarm_board::swap_swarm_item(&mut conn, swarm, expected_rev, next)
                .await
                .map_err(backend)
        })
    }

    fn append_post<'a>(
        &'a self,
        swarm: &'a str,
        post: NewPost<'a>,
    ) -> BoxFuture<'a, Result<PostRow, SwarmError>> {
        Box::pin(async move {
            let mut conn = self.conn().await?;
            autumn_harvest::swarm_board::append_swarm_post(&mut conn, swarm, &post)
                .await
                .map_err(backend)
        })
    }

    fn posts<'a>(
        &'a self,
        swarm: &'a str,
        channel: Option<&'a str>,
        after: u64,
        limit: u32,
    ) -> BoxFuture<'a, Result<Vec<PostRow>, SwarmError>> {
        Box::pin(async move {
            let mut conn = self.conn().await?;
            autumn_harvest::swarm_board::swarm_posts(&mut conn, swarm, channel, after, limit)
                .await
                .map_err(backend)
        })
    }

    fn record_verdict<'a>(
        &'a self,
        swarm: &'a str,
        record: &'a MergeRow,
    ) -> BoxFuture<'a, Result<MergeRow, SwarmError>> {
        Box::pin(async move {
            let mut conn = self.conn().await?;
            autumn_harvest::swarm_board::record_swarm_verdict(&mut conn, swarm, record)
                .await
                .map_err(backend)
        })
    }

    fn merge_by_key<'a>(
        &'a self,
        swarm: &'a str,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<MergeRow>, SwarmError>> {
        Box::pin(async move {
            let mut conn = self.conn().await?;
            autumn_harvest::swarm_board::swarm_merge_by_key(&mut conn, swarm, key)
                .await
                .map_err(backend)
        })
    }

    fn merges<'a>(
        &'a self,
        swarm: &'a str,
        after: u64,
        limit: u32,
    ) -> BoxFuture<'a, Result<Vec<MergeRow>, SwarmError>> {
        Box::pin(async move {
            let mut conn = self.conn().await?;
            autumn_harvest::swarm_board::swarm_merges(&mut conn, swarm, after, limit)
                .await
                .map_err(backend)
        })
    }

    fn commit_merge<'a>(
        &'a self,
        swarm: &'a str,
        commit: &'a MergeCommit,
    ) -> BoxFuture<'a, Result<CommitOutcome, SwarmError>> {
        Box::pin(async move {
            let mut conn = self.conn().await?;
            autumn_harvest::swarm_board::commit_swarm_merge(&mut conn, swarm, commit)
                .await
                .map_err(backend)
        })
    }
}
