//! The rows of the board of an agent swarm (issue #2814).
//!
//! The typed board is `autumn_harvest_agent::swarm`. This module holds the
//! row types and the Postgres queries. `autumn-harvest-sqlite` holds the
//! `SQLite` queries.
//!
//! The rules:
//!
//! - A write of an item names the `rev` that it read. See
//!   [`swap_swarm_item`]. A write that lost the race changes nothing, and
//!   the board reads again.
//! - The posts of one swarm take their `seq` from a counter row. The insert
//!   holds the lock of that row until it commits. So the posts commit in
//!   `seq` order, and a reader with a cursor never skips one.
//! - A post and a verdict never change. A trigger refuses each `UPDATE`.
//! - A merge moves the main head, closes its item and logs its verdict in
//!   one transaction. It locks the main row, then the item row.
//! - A post and a verdict carry a key. A second write with the same key
//!   returns the first row, so a retried tool call writes once.
//!
//! The design is in `docs/design/DESIGN-2814.md`.

/// The most rows that one read of the board returns.
pub const MAX_READ: u32 = 500;

/// One stored work item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ItemRow {
    /// The id of the item in its swarm.
    pub item: String,
    /// The lane of the item.
    pub lane: String,
    /// The short title.
    pub title: String,
    /// The text of the task.
    pub body: String,
    /// The stored status name: `open`, `claimed`, `merged` or `dropped`.
    pub status: String,
    /// The member that holds the item, or that merged it.
    pub holder: Option<String>,
    /// The epoch of the last claim. A new item has epoch 0.
    pub epoch: u64,
    /// The end of the lease of the claim, in epoch milliseconds.
    pub lease_until_ms: i64,
    /// The main version that the claim checked out.
    pub base_version: u64,
    /// The workspace reference of that main version, as JSON. An item that
    /// no claim took has an empty text.
    pub base_snapshot: String,
    /// The row version, from 1. Each write increments it.
    pub rev: u64,
    /// Who added the item.
    pub created_by: String,
    /// When the item was added, in epoch milliseconds.
    pub created_at_ms: i64,
    /// When the item last changed, in epoch milliseconds.
    pub updated_at_ms: i64,
}

/// One stored post.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostRow {
    /// The number of the post in its swarm, from 1.
    pub seq: u64,
    /// The key of the write that added the post.
    pub post_key: String,
    /// The channel of the post.
    pub channel: String,
    /// The member that wrote the post.
    pub author: String,
    /// The text.
    pub body: String,
    /// When the post was added, in epoch milliseconds.
    pub created_at_ms: i64,
}

/// A post to add.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NewPost<'a> {
    /// The key of the write. A second write with the same key adds nothing.
    pub post_key: &'a str,
    /// The channel of the post.
    pub channel: &'a str,
    /// The member that writes the post.
    pub author: &'a str,
    /// The text.
    pub body: &'a str,
    /// The time of the write, in epoch milliseconds.
    pub at_ms: i64,
}

/// The stored head of the shared files of a swarm.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MainRow {
    /// The version of main, from 1.
    pub version: u64,
    /// The workspace reference of the files, as JSON.
    pub snapshot: String,
    /// The score of the files, in score units.
    pub score: i64,
    /// The item whose merge made this version, if any.
    pub item: Option<String>,
    /// The member whose merge made this version, if any.
    pub author: Option<String>,
    /// When the head last moved, in epoch milliseconds.
    pub updated_at_ms: i64,
}

/// One stored verdict of the merge gate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeRow {
    /// The number of the verdict in its swarm, from 1. A new row passes 0,
    /// and the store sets it.
    pub seq: u64,
    /// The key of the submit. A second write with the same key returns the
    /// first row.
    pub submit_key: String,
    /// The item of the submit.
    pub item: String,
    /// The member that submitted.
    pub holder: String,
    /// The epoch of the claim of the submit.
    pub epoch: u64,
    /// The main version that the claim checked out.
    pub base_version: u64,
    /// The main version that the gate merged onto.
    pub main_version: u64,
    /// The stored verdict name.
    pub verdict: String,
    /// The score of the merged files, if the judge ran.
    pub candidate_score: Option<i64>,
    /// The score of main, if the judge ran.
    pub main_score: Option<i64>,
    /// The new main version of a merge.
    pub merged_version: Option<u64>,
    /// The workspace reference of the files of a merge, as JSON.
    pub snapshot: Option<String>,
    /// A short text for a person, such as the conflicting paths.
    pub detail: String,
    /// When the gate decided, in epoch milliseconds.
    pub created_at_ms: i64,
}

/// A filter on the items of one swarm.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ItemQuery<'a> {
    /// Only the items of this lane.
    pub lane: Option<&'a str>,
    /// Only the items with this stored status name.
    pub status: Option<&'a str>,
    /// Only the items of this holder.
    pub holder: Option<&'a str>,
    /// Only the items after this id, for paging.
    pub after: Option<&'a str>,
    /// The most rows. It is at most [`MAX_READ`], and 0 counts as 1.
    pub limit: u32,
}

impl ItemQuery<'_> {
    /// The limit as a bind value, from 1 to [`MAX_READ`].
    #[must_use]
    pub const fn bounded_limit(&self) -> u32 {
        bounded(self.limit)
    }
}

/// A read limit from 1 to [`MAX_READ`].
#[must_use]
pub const fn bounded(limit: u32) -> u32 {
    if limit == 0 {
        1
    } else if limit > MAX_READ {
        MAX_READ
    } else {
        limit
    }
}

/// One merge to commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeCommit {
    /// The main version that the gate merged onto.
    pub expected_main: u64,
    /// The new head.
    pub next_main: MainRow,
    /// The `rev` of the item that the gate read.
    pub item_rev: u64,
    /// The item as the merge leaves it.
    pub next_item: ItemRow,
    /// The verdict to log. Its `seq` is set by the store.
    pub record: MergeRow,
}

/// What a merge commit did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitOutcome {
    /// The merge landed. The row is the logged verdict.
    Committed(MergeRow),
    /// A verdict with the same key was already logged. Nothing changed.
    Recorded(MergeRow),
    /// Main moved past the version that the gate read. Nothing changed.
    MainMoved {
        /// The current main version.
        actual: Option<u64>,
    },
    /// The item changed after the gate read it. Nothing changed.
    ItemMoved,
}

/// A stored count as `u64`. The table checks keep each count at 0 or more.
///
/// # Errors
///
/// Returns a message when the value is negative.
pub fn stored_u64(value: i64, what: &str) -> Result<u64, String> {
    u64::try_from(value).map_err(|_| format!("the stored {what} {value} is negative"))
}

/// A count as a bind value. A count above `i64::MAX` cannot be stored.
///
/// # Errors
///
/// Returns a message when the value is above `i64::MAX`.
pub fn bind_u64(value: u64, what: &str) -> Result<i64, String> {
    i64::try_from(value).map_err(|_| format!("the {what} {value} is above i64::MAX"))
}

#[cfg(feature = "db")]
#[doc(hidden)]
pub use pg::{
    append_swarm_post, commit_swarm_merge, count_swarm_held, insert_swarm_item,
    record_swarm_verdict, seed_swarm_main, swap_swarm_item, swarm_item, swarm_items, swarm_main,
    swarm_merge_by_key, swarm_merges, swarm_posts,
};

/// The Postgres queries.
#[cfg(feature = "db")]
mod pg {
    use diesel::QueryableByName;
    use diesel::sql_types::{BigInt, Nullable, Text};
    use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};

    use super::{
        CommitOutcome, ItemQuery, ItemRow, MainRow, MergeCommit, MergeRow, NewPost, PostRow,
        bind_u64, stored_u64,
    };
    use crate::error::{HarvestError, HarvestResult, database_error};

    const fn decode_error(message: String) -> HarvestError {
        HarvestError::Database(message)
    }

    fn bind(value: u64, what: &str) -> HarvestResult<i64> {
        bind_u64(value, what).map_err(decode_error)
    }

    fn opt_bind(value: Option<u64>, what: &str) -> HarvestResult<Option<i64>> {
        value.map(|value| bind(value, what)).transpose()
    }

    const ITEM_COLUMNS: &str = "item, lane, title, body, status, holder, epoch, lease_until_ms, \
         base_version, base_snapshot, rev, created_by, created_at_ms, updated_at_ms";

    const POST_COLUMNS: &str = "seq, post_key, channel, author, body, created_at_ms";

    const MAIN_COLUMNS: &str = "version, snapshot, score, item, author, updated_at_ms";

    const MERGE_COLUMNS: &str = "seq, submit_key, item, holder, epoch, base_version, \
         main_version, verdict, candidate_score, main_score, merged_version, snapshot, detail, \
         created_at_ms";

    #[derive(QueryableByName)]
    struct ItemSql {
        #[diesel(sql_type = Text)]
        item: String,
        #[diesel(sql_type = Text)]
        lane: String,
        #[diesel(sql_type = Text)]
        title: String,
        #[diesel(sql_type = Text)]
        body: String,
        #[diesel(sql_type = Text)]
        status: String,
        #[diesel(sql_type = Nullable<Text>)]
        holder: Option<String>,
        #[diesel(sql_type = BigInt)]
        epoch: i64,
        #[diesel(sql_type = BigInt)]
        lease_until_ms: i64,
        #[diesel(sql_type = BigInt)]
        base_version: i64,
        #[diesel(sql_type = Text)]
        base_snapshot: String,
        #[diesel(sql_type = BigInt)]
        rev: i64,
        #[diesel(sql_type = Text)]
        created_by: String,
        #[diesel(sql_type = BigInt)]
        created_at_ms: i64,
        #[diesel(sql_type = BigInt)]
        updated_at_ms: i64,
    }

    impl ItemSql {
        fn decode(self) -> HarvestResult<ItemRow> {
            Ok(ItemRow {
                item: self.item,
                lane: self.lane,
                title: self.title,
                body: self.body,
                status: self.status,
                holder: self.holder,
                epoch: stored_u64(self.epoch, "epoch").map_err(decode_error)?,
                lease_until_ms: self.lease_until_ms,
                base_version: stored_u64(self.base_version, "base version")
                    .map_err(decode_error)?,
                base_snapshot: self.base_snapshot,
                rev: stored_u64(self.rev, "rev").map_err(decode_error)?,
                created_by: self.created_by,
                created_at_ms: self.created_at_ms,
                updated_at_ms: self.updated_at_ms,
            })
        }
    }

    #[derive(QueryableByName)]
    struct PostSql {
        #[diesel(sql_type = BigInt)]
        seq: i64,
        #[diesel(sql_type = Text)]
        post_key: String,
        #[diesel(sql_type = Text)]
        channel: String,
        #[diesel(sql_type = Text)]
        author: String,
        #[diesel(sql_type = Text)]
        body: String,
        #[diesel(sql_type = BigInt)]
        created_at_ms: i64,
    }

    impl PostSql {
        fn decode(self) -> HarvestResult<PostRow> {
            Ok(PostRow {
                seq: stored_u64(self.seq, "post seq").map_err(decode_error)?,
                post_key: self.post_key,
                channel: self.channel,
                author: self.author,
                body: self.body,
                created_at_ms: self.created_at_ms,
            })
        }
    }

    #[derive(QueryableByName)]
    struct MainSql {
        #[diesel(sql_type = BigInt)]
        version: i64,
        #[diesel(sql_type = Text)]
        snapshot: String,
        #[diesel(sql_type = BigInt)]
        score: i64,
        #[diesel(sql_type = Nullable<Text>)]
        item: Option<String>,
        #[diesel(sql_type = Nullable<Text>)]
        author: Option<String>,
        #[diesel(sql_type = BigInt)]
        updated_at_ms: i64,
    }

    impl MainSql {
        fn decode(self) -> HarvestResult<MainRow> {
            Ok(MainRow {
                version: stored_u64(self.version, "main version").map_err(decode_error)?,
                snapshot: self.snapshot,
                score: self.score,
                item: self.item,
                author: self.author,
                updated_at_ms: self.updated_at_ms,
            })
        }
    }

    #[derive(QueryableByName)]
    struct MergeSql {
        #[diesel(sql_type = BigInt)]
        seq: i64,
        #[diesel(sql_type = Text)]
        submit_key: String,
        #[diesel(sql_type = Text)]
        item: String,
        #[diesel(sql_type = Text)]
        holder: String,
        #[diesel(sql_type = BigInt)]
        epoch: i64,
        #[diesel(sql_type = BigInt)]
        base_version: i64,
        #[diesel(sql_type = BigInt)]
        main_version: i64,
        #[diesel(sql_type = Text)]
        verdict: String,
        #[diesel(sql_type = Nullable<BigInt>)]
        candidate_score: Option<i64>,
        #[diesel(sql_type = Nullable<BigInt>)]
        main_score: Option<i64>,
        #[diesel(sql_type = Nullable<BigInt>)]
        merged_version: Option<i64>,
        #[diesel(sql_type = Nullable<Text>)]
        snapshot: Option<String>,
        #[diesel(sql_type = Text)]
        detail: String,
        #[diesel(sql_type = BigInt)]
        created_at_ms: i64,
    }

    impl MergeSql {
        fn decode(self) -> HarvestResult<MergeRow> {
            let merged_version = self
                .merged_version
                .map(|value| stored_u64(value, "merged version"))
                .transpose()
                .map_err(decode_error)?;
            Ok(MergeRow {
                seq: stored_u64(self.seq, "merge seq").map_err(decode_error)?,
                submit_key: self.submit_key,
                item: self.item,
                holder: self.holder,
                epoch: stored_u64(self.epoch, "epoch").map_err(decode_error)?,
                base_version: stored_u64(self.base_version, "base version")
                    .map_err(decode_error)?,
                main_version: stored_u64(self.main_version, "main version")
                    .map_err(decode_error)?,
                verdict: self.verdict,
                candidate_score: self.candidate_score,
                main_score: self.main_score,
                merged_version,
                snapshot: self.snapshot,
                detail: self.detail,
                created_at_ms: self.created_at_ms,
            })
        }
    }

    /// Insert the first main head of `swarm`. Returns `false` when the swarm
    /// already has one, and then changes nothing.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] when the statement fails.
    pub async fn seed_swarm_main(
        conn: &mut AsyncPgConnection,
        swarm: &str,
        main: &MainRow,
    ) -> HarvestResult<bool> {
        let inserted = diesel::sql_query(
            "INSERT INTO harvest_swarm_mains
                 (swarm, version, snapshot, score, item, author, updated_at_ms)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (swarm) DO NOTHING",
        )
        .bind::<Text, _>(swarm)
        .bind::<BigInt, _>(bind(main.version, "main version")?)
        .bind::<Text, _>(&main.snapshot)
        .bind::<BigInt, _>(main.score)
        .bind::<Nullable<Text>, _>(main.item.as_deref())
        .bind::<Nullable<Text>, _>(main.author.as_deref())
        .bind::<BigInt, _>(main.updated_at_ms)
        .execute(conn)
        .await
        .map_err(database_error)?;
        Ok(inserted == 1)
    }

    /// The main head of `swarm`, if it has one.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] when the statement fails.
    pub async fn swarm_main(
        conn: &mut AsyncPgConnection,
        swarm: &str,
    ) -> HarvestResult<Option<MainRow>> {
        let rows: Vec<MainSql> = diesel::sql_query(format!(
            "SELECT {MAIN_COLUMNS} FROM harvest_swarm_mains WHERE swarm = $1"
        ))
        .bind::<Text, _>(swarm)
        .load(conn)
        .await
        .map_err(database_error)?;
        rows.into_iter().next().map(MainSql::decode).transpose()
    }

    /// Insert `row` into `swarm`. Returns `false` when the swarm already
    /// has an item with its id, and then changes nothing.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] when the statement fails.
    pub async fn insert_swarm_item(
        conn: &mut AsyncPgConnection,
        swarm: &str,
        row: &ItemRow,
    ) -> HarvestResult<bool> {
        let inserted = diesel::sql_query(format!(
            "INSERT INTO harvest_swarm_items (swarm, {ITEM_COLUMNS})
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
             ON CONFLICT (swarm, item) DO NOTHING"
        ))
        .bind::<Text, _>(swarm)
        .bind::<Text, _>(&row.item)
        .bind::<Text, _>(&row.lane)
        .bind::<Text, _>(&row.title)
        .bind::<Text, _>(&row.body)
        .bind::<Text, _>(&row.status)
        .bind::<Nullable<Text>, _>(row.holder.as_deref())
        .bind::<BigInt, _>(bind(row.epoch, "epoch")?)
        .bind::<BigInt, _>(row.lease_until_ms)
        .bind::<BigInt, _>(bind(row.base_version, "base version")?)
        .bind::<Text, _>(&row.base_snapshot)
        .bind::<BigInt, _>(bind(row.rev, "rev")?)
        .bind::<Text, _>(&row.created_by)
        .bind::<BigInt, _>(row.created_at_ms)
        .bind::<BigInt, _>(row.updated_at_ms)
        .execute(conn)
        .await
        .map_err(database_error)?;
        Ok(inserted == 1)
    }

    /// The item `item` of `swarm`, if it exists.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] when the statement fails.
    pub async fn swarm_item(
        conn: &mut AsyncPgConnection,
        swarm: &str,
        item: &str,
    ) -> HarvestResult<Option<ItemRow>> {
        let rows: Vec<ItemSql> = diesel::sql_query(format!(
            "SELECT {ITEM_COLUMNS} FROM harvest_swarm_items WHERE swarm = $1 AND item = $2"
        ))
        .bind::<Text, _>(swarm)
        .bind::<Text, _>(item)
        .load(conn)
        .await
        .map_err(database_error)?;
        rows.into_iter().next().map(ItemSql::decode).transpose()
    }

    /// The items of `swarm` that match `query`, in id order.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] when the statement fails.
    pub async fn swarm_items(
        conn: &mut AsyncPgConnection,
        swarm: &str,
        query: &ItemQuery<'_>,
    ) -> HarvestResult<Vec<ItemRow>> {
        let rows: Vec<ItemSql> = diesel::sql_query(format!(
            "SELECT {ITEM_COLUMNS} FROM harvest_swarm_items
             WHERE swarm = $1
               AND ($2::TEXT IS NULL OR lane = $2)
               AND ($3::TEXT IS NULL OR status = $3)
               AND ($4::TEXT IS NULL OR holder = $4)
               AND ($5::TEXT IS NULL OR item > $5)
             ORDER BY item
             LIMIT $6"
        ))
        .bind::<Text, _>(swarm)
        .bind::<Nullable<Text>, _>(query.lane)
        .bind::<Nullable<Text>, _>(query.status)
        .bind::<Nullable<Text>, _>(query.holder)
        .bind::<Nullable<Text>, _>(query.after)
        .bind::<BigInt, _>(i64::from(query.bounded_limit()))
        .load(conn)
        .await
        .map_err(database_error)?;
        rows.into_iter().map(ItemSql::decode).collect()
    }

    #[derive(QueryableByName)]
    struct Count {
        #[diesel(sql_type = BigInt)]
        n: i64,
    }

    /// The number of items of `swarm` that `holder` holds.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] when the statement fails.
    pub async fn count_swarm_held(
        conn: &mut AsyncPgConnection,
        swarm: &str,
        holder: &str,
    ) -> HarvestResult<u64> {
        let row: Count = diesel::sql_query(
            "SELECT COUNT(*) AS n FROM harvest_swarm_items
             WHERE swarm = $1 AND holder = $2 AND status = 'claimed'",
        )
        .bind::<Text, _>(swarm)
        .bind::<Text, _>(holder)
        .get_result(conn)
        .await
        .map_err(database_error)?;
        stored_u64(row.n, "count").map_err(decode_error)
    }

    fn swap_item_sql() -> String {
        "UPDATE harvest_swarm_items
         SET lane = $3, title = $4, body = $5, status = $6, holder = $7, epoch = $8,
             lease_until_ms = $9, base_version = $10, base_snapshot = $11, rev = rev + 1,
             updated_at_ms = $12
         WHERE swarm = $1 AND item = $2 AND rev = $13"
            .to_owned()
    }

    /// Write `next` over the item with its id when its stored `rev` is
    /// still `expected_rev`. The store sets the new `rev` to
    /// `expected_rev + 1`. Returns `false` when another write landed first.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] when the statement fails.
    pub async fn swap_swarm_item(
        conn: &mut AsyncPgConnection,
        swarm: &str,
        expected_rev: u64,
        next: &ItemRow,
    ) -> HarvestResult<bool> {
        let Ok(expected) = i64::try_from(expected_rev) else {
            return Ok(false);
        };
        let updated = diesel::sql_query(swap_item_sql())
            .bind::<Text, _>(swarm)
            .bind::<Text, _>(&next.item)
            .bind::<Text, _>(&next.lane)
            .bind::<Text, _>(&next.title)
            .bind::<Text, _>(&next.body)
            .bind::<Text, _>(&next.status)
            .bind::<Nullable<Text>, _>(next.holder.as_deref())
            .bind::<BigInt, _>(bind(next.epoch, "epoch")?)
            .bind::<BigInt, _>(next.lease_until_ms)
            .bind::<BigInt, _>(bind(next.base_version, "base version")?)
            .bind::<Text, _>(&next.base_snapshot)
            .bind::<BigInt, _>(next.updated_at_ms)
            .bind::<BigInt, _>(expected)
            .execute(conn)
            .await
            .map_err(database_error)?;
        Ok(updated == 1)
    }

    /// Add `post` to `swarm`, or return the post that its key already added.
    ///
    /// The counter row of the swarm hands out the `seq`. Its lock lasts
    /// until the commit, so the posts of one swarm commit in `seq` order. A
    /// post whose key exists adds nothing, and its counter step is a gap.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] when a statement fails.
    pub async fn append_swarm_post(
        conn: &mut AsyncPgConnection,
        swarm: &str,
        post: &NewPost<'_>,
    ) -> HarvestResult<PostRow> {
        if let Some(found) = post_by_key(conn, swarm, post.post_key).await? {
            return Ok(found);
        }
        let rows: Vec<PostSql> = diesel::sql_query(format!(
            "WITH counter AS (
                 INSERT INTO harvest_swarm_counters (swarm, posts) VALUES ($1, 1)
                 ON CONFLICT (swarm) DO UPDATE
                     SET posts = harvest_swarm_counters.posts + 1
                 RETURNING posts
             )
             INSERT INTO harvest_swarm_posts
                 (swarm, seq, post_key, channel, author, body, created_at_ms)
             SELECT $1, counter.posts, $2, $3, $4, $5, $6 FROM counter
             ON CONFLICT (swarm, post_key) DO NOTHING
             RETURNING {POST_COLUMNS}"
        ))
        .bind::<Text, _>(swarm)
        .bind::<Text, _>(post.post_key)
        .bind::<Text, _>(post.channel)
        .bind::<Text, _>(post.author)
        .bind::<Text, _>(post.body)
        .bind::<BigInt, _>(post.at_ms)
        .load(conn)
        .await
        .map_err(database_error)?;
        match rows.into_iter().next() {
            Some(row) => row.decode(),
            None => post_by_key(conn, swarm, post.post_key)
                .await?
                .ok_or_else(|| decode_error("a swarm post with a known key is gone".to_owned())),
        }
    }

    /// The post of `swarm` that `key` added, if any.
    async fn post_by_key(
        conn: &mut AsyncPgConnection,
        swarm: &str,
        key: &str,
    ) -> HarvestResult<Option<PostRow>> {
        let rows: Vec<PostSql> = diesel::sql_query(format!(
            "SELECT {POST_COLUMNS} FROM harvest_swarm_posts WHERE swarm = $1 AND post_key = $2"
        ))
        .bind::<Text, _>(swarm)
        .bind::<Text, _>(key)
        .load(conn)
        .await
        .map_err(database_error)?;
        rows.into_iter().next().map(PostSql::decode).transpose()
    }

    /// The posts of `swarm` after `after`, in `seq` order. With a channel,
    /// only the posts of that channel.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] when the statement fails.
    pub async fn swarm_posts(
        conn: &mut AsyncPgConnection,
        swarm: &str,
        channel: Option<&str>,
        after: u64,
        limit: u32,
    ) -> HarvestResult<Vec<PostRow>> {
        let Ok(after) = i64::try_from(after) else {
            return Ok(Vec::new());
        };
        let rows: Vec<PostSql> = diesel::sql_query(format!(
            "SELECT {POST_COLUMNS} FROM harvest_swarm_posts
             WHERE swarm = $1 AND ($2::TEXT IS NULL OR channel = $2) AND seq > $3
             ORDER BY seq
             LIMIT $4"
        ))
        .bind::<Text, _>(swarm)
        .bind::<Nullable<Text>, _>(channel)
        .bind::<BigInt, _>(after)
        .bind::<BigInt, _>(i64::from(super::bounded(limit)))
        .load(conn)
        .await
        .map_err(database_error)?;
        rows.into_iter().map(PostSql::decode).collect()
    }

    /// The verdict of `swarm` that the submit `key` logged, if any.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] when the statement fails.
    pub async fn swarm_merge_by_key(
        conn: &mut AsyncPgConnection,
        swarm: &str,
        key: &str,
    ) -> HarvestResult<Option<MergeRow>> {
        let rows: Vec<MergeSql> = diesel::sql_query(format!(
            "SELECT {MERGE_COLUMNS} FROM harvest_swarm_merges
             WHERE swarm = $1 AND submit_key = $2"
        ))
        .bind::<Text, _>(swarm)
        .bind::<Text, _>(key)
        .load(conn)
        .await
        .map_err(database_error)?;
        rows.into_iter().next().map(MergeSql::decode).transpose()
    }

    /// The verdicts of `swarm` after `after`, in `seq` order.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] when the statement fails.
    pub async fn swarm_merges(
        conn: &mut AsyncPgConnection,
        swarm: &str,
        after: u64,
        limit: u32,
    ) -> HarvestResult<Vec<MergeRow>> {
        let Ok(after) = i64::try_from(after) else {
            return Ok(Vec::new());
        };
        let rows: Vec<MergeSql> = diesel::sql_query(format!(
            "SELECT {MERGE_COLUMNS} FROM harvest_swarm_merges
             WHERE swarm = $1 AND seq > $2
             ORDER BY seq
             LIMIT $3"
        ))
        .bind::<Text, _>(swarm)
        .bind::<BigInt, _>(after)
        .bind::<BigInt, _>(i64::from(super::bounded(limit)))
        .load(conn)
        .await
        .map_err(database_error)?;
        rows.into_iter().map(MergeSql::decode).collect()
    }

    /// Log `record` in `swarm`, with the next verdict `seq`.
    async fn insert_merge(
        conn: &mut AsyncPgConnection,
        swarm: &str,
        record: &MergeRow,
    ) -> HarvestResult<Option<MergeRow>> {
        let rows: Vec<MergeSql> = diesel::sql_query(format!(
            "WITH counter AS (
                 INSERT INTO harvest_swarm_counters (swarm, merges) VALUES ($1, 1)
                 ON CONFLICT (swarm) DO UPDATE
                     SET merges = harvest_swarm_counters.merges + 1
                 RETURNING merges
             )
             INSERT INTO harvest_swarm_merges
                 (swarm, seq, submit_key, item, holder, epoch, base_version, main_version,
                  verdict, candidate_score, main_score, merged_version, snapshot, detail,
                  created_at_ms)
             SELECT $1, counter.merges, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
             FROM counter
             ON CONFLICT (swarm, submit_key) DO NOTHING
             RETURNING {MERGE_COLUMNS}"
        ))
        .bind::<Text, _>(swarm)
        .bind::<Text, _>(&record.submit_key)
        .bind::<Text, _>(&record.item)
        .bind::<Text, _>(&record.holder)
        .bind::<BigInt, _>(bind(record.epoch, "epoch")?)
        .bind::<BigInt, _>(bind(record.base_version, "base version")?)
        .bind::<BigInt, _>(bind(record.main_version, "main version")?)
        .bind::<Text, _>(&record.verdict)
        .bind::<Nullable<BigInt>, _>(record.candidate_score)
        .bind::<Nullable<BigInt>, _>(record.main_score)
        .bind::<Nullable<BigInt>, _>(opt_bind(record.merged_version, "merged version")?)
        .bind::<Nullable<Text>, _>(record.snapshot.as_deref())
        .bind::<Text, _>(&record.detail)
        .bind::<BigInt, _>(record.created_at_ms)
        .load(conn)
        .await
        .map_err(database_error)?;
        rows.into_iter().next().map(MergeSql::decode).transpose()
    }

    /// Log a verdict that merges nothing, or return the verdict that its key
    /// already logged.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] when a statement fails.
    pub async fn record_swarm_verdict(
        conn: &mut AsyncPgConnection,
        swarm: &str,
        record: &MergeRow,
    ) -> HarvestResult<MergeRow> {
        if let Some(found) = swarm_merge_by_key(conn, swarm, &record.submit_key).await? {
            return Ok(found);
        }
        match insert_merge(conn, swarm, record).await? {
            Some(row) => Ok(row),
            None => swarm_merge_by_key(conn, swarm, &record.submit_key)
                .await?
                .ok_or_else(|| decode_error("a swarm verdict with a known key is gone".to_owned())),
        }
    }

    /// Lock the main row of `swarm`, and read its version.
    async fn lock_main(conn: &mut AsyncPgConnection, swarm: &str) -> HarvestResult<Option<u64>> {
        let rows: Vec<Count> = diesel::sql_query(
            "SELECT version AS n FROM harvest_swarm_mains WHERE swarm = $1 FOR UPDATE",
        )
        .bind::<Text, _>(swarm)
        .load(conn)
        .await
        .map_err(database_error)?;
        rows.into_iter()
            .next()
            .map(|row| stored_u64(row.n, "main version").map_err(decode_error))
            .transpose()
    }

    /// Move the main head of `swarm` under the lock of [`lock_main`].
    async fn move_main(
        conn: &mut AsyncPgConnection,
        swarm: &str,
        expected: u64,
        next: &MainRow,
    ) -> HarvestResult<()> {
        let updated = diesel::sql_query(
            "UPDATE harvest_swarm_mains
             SET version = $2, snapshot = $3, score = $4, item = $5, author = $6,
                 updated_at_ms = $7
             WHERE swarm = $1 AND version = $8",
        )
        .bind::<Text, _>(swarm)
        .bind::<BigInt, _>(bind(next.version, "main version")?)
        .bind::<Text, _>(&next.snapshot)
        .bind::<BigInt, _>(next.score)
        .bind::<Nullable<Text>, _>(next.item.as_deref())
        .bind::<Nullable<Text>, _>(next.author.as_deref())
        .bind::<BigInt, _>(next.updated_at_ms)
        .bind::<BigInt, _>(bind(expected, "main version")?)
        .execute(conn)
        .await
        .map_err(database_error)?;
        if updated == 1 {
            Ok(())
        } else {
            Err(decode_error(format!(
                "the main head of swarm {swarm:?} moved under its lock"
            )))
        }
    }

    /// Commit one merge: move main, close the item and log the verdict.
    ///
    /// One transaction locks the main row, then the item row. It compares
    /// the main version and the item `rev` under the locks. A mismatch
    /// changes nothing. A verdict with the same key returns
    /// [`CommitOutcome::Recorded`], so a retried submit merges once.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] when a statement fails.
    pub async fn commit_swarm_merge(
        conn: &mut AsyncPgConnection,
        swarm: &str,
        commit: &MergeCommit,
    ) -> HarvestResult<CommitOutcome> {
        Box::pin(conn.transaction(async |conn| {
            let actual = lock_main(conn, swarm).await?;
            if let Some(found) = swarm_merge_by_key(conn, swarm, &commit.record.submit_key).await? {
                return Ok(CommitOutcome::Recorded(found));
            }
            if actual != Some(commit.expected_main) {
                return Ok(CommitOutcome::MainMoved { actual });
            }
            let locked: Vec<Count> = diesel::sql_query(
                "SELECT rev AS n FROM harvest_swarm_items
                 WHERE swarm = $1 AND item = $2 FOR UPDATE",
            )
            .bind::<Text, _>(swarm)
            .bind::<Text, _>(&commit.next_item.item)
            .load(conn)
            .await
            .map_err(database_error)?;
            let rev = locked
                .into_iter()
                .next()
                .map(|row| stored_u64(row.n, "rev").map_err(decode_error))
                .transpose()?;
            if rev != Some(commit.item_rev) {
                return Ok(CommitOutcome::ItemMoved);
            }
            move_main(conn, swarm, commit.expected_main, &commit.next_main).await?;
            if !swap_swarm_item(conn, swarm, commit.item_rev, &commit.next_item).await? {
                return Err(decode_error(format!(
                    "the item {:?} of swarm {swarm:?} moved under its lock",
                    commit.next_item.item
                )));
            }
            let row = insert_merge(conn, swarm, &commit.record)
                .await?
                .ok_or_else(|| {
                    decode_error("a merge verdict key appeared under its lock".into())
                })?;
            Ok(CommitOutcome::Committed(row))
        }))
        .await
    }
}
