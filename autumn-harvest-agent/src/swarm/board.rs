//! The board and the merge gate of one swarm (issue #2814).
//!
//! [`Swarm`] joins a [`SwarmStore`], a [`WorkspaceStore`] for the main
//! snapshots, a [`SwarmJudge`] and a [`SwarmPolicy`]. Each call reads its
//! row, asks the [`kernel`](super::kernel), and writes with a compare and
//! swap. A lost race reads again, at most [`MAX_SWAPS`] times.
//!
//! A submit runs in a tool call, so it can run again after a crash. Its
//! verdict carries the idempotency key of the call. A second run with the
//! same key returns the first verdict, and merges nothing twice.

use std::sync::Arc;

use autumn_harvest::jiff::Timestamp;
use autumn_harvest::swarm_board::{
    CommitOutcome, ItemQuery, ItemRow, MainRow, MergeCommit, MergeRow, NewPost, PostRow,
};

use super::kernel::{
    ClaimDecision, ClaimRefusal, FenceRefusal, ItemStatus, decide_claim, fence, lease_end,
    lease_live, passes,
};
use super::merge::{Files, MergeResult, three_way, under};
use super::{
    MAX_ITEM_BODY_LEN, MAX_POST_LEN, MAX_TITLE_LEN, Score, SwarmError, SwarmId, SwarmPolicy,
    SwarmStore, Verdict, check_len, check_member, check_name,
};
use crate::StoreClock;
use crate::error::AgentError;
use crate::model::BoxFuture;
use crate::workspace::{Workspace, WorkspaceMount, WorkspaceRef, WorkspaceStore};

/// The most reads and writes of one claim, renew or release before it
/// gives up. Each lost race means that another write of the item landed.
pub const MAX_SWAPS: u32 = 16;

/// The eval of a swarm. The operator implements it.
///
/// The judge scores the files of main and of each merge candidate. A
/// higher score is better. It never runs as a tool, so a member never
/// reads it. Give it the same answer for the same files: the gate compares
/// the score of a candidate with the stored score of main.
pub trait SwarmJudge: Send + Sync + std::fmt::Debug {
    /// The score of `files`, the files under the root of `swarm`.
    fn score<'a>(
        &'a self,
        swarm: &'a SwarmId,
        files: &'a Files,
    ) -> BoxFuture<'a, Result<Score, AgentError>>;
}

/// A work item to add.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewItem {
    /// The id of the item in its swarm.
    pub id: String,
    /// The lane of the item.
    pub lane: String,
    /// The short title.
    pub title: String,
    /// The text of the task.
    pub body: String,
}

impl NewItem {
    /// An item with `id` in `lane`.
    #[must_use]
    pub fn new(
        id: impl Into<String>,
        lane: impl Into<String>,
        title: impl Into<String>,
        body: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            lane: lane.into(),
            title: title.into(),
            body: body.into(),
        }
    }
}

/// What a claim did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimOutcome {
    /// The member took the item at a new epoch. Main is in its workspace.
    Claimed(ItemRow),
    /// The member already held the item. The lease is renewed, and the base
    /// of the claim is back in its workspace.
    Kept(ItemRow),
    /// The claim took nothing.
    Refused {
        /// Why.
        refusal: ClaimRefusal,
        /// The item as the claim read it.
        item: ItemRow,
    },
}

/// The status of a stored item. A name that no status has means a corrupt
/// row.
///
/// # Errors
///
/// Returns [`SwarmError::Backend`] for a name that no status has.
pub fn item_status(row: &ItemRow) -> Result<ItemStatus, SwarmError> {
    ItemStatus::parse(&row.status).ok_or_else(|| {
        SwarmError::Backend(format!(
            "the item {:?} has the unknown status {:?}",
            row.item, row.status
        ))
    })
}

/// The facts of one submit call that each verdict row repeats.
struct SubmitCall<'a> {
    key: &'a str,
    member: &'a str,
    item: &'a str,
    epoch: u64,
}

/// The end of one attempt of a submit.
enum Attempt {
    /// The submit ends with this logged verdict.
    Done(Box<MergeRow>),
    /// Another merge moved main first. Try again on the newer main.
    Again {
        /// The main version that this attempt read.
        main_version: u64,
    },
}

/// One swarm: its board, its main and its gate.
pub struct Swarm {
    id: SwarmId,
    store: Arc<dyn SwarmStore>,
    workspaces: Arc<WorkspaceStore>,
    judge: Arc<dyn SwarmJudge>,
    policy: SwarmPolicy,
    clock: StoreClock,
}

impl std::fmt::Debug for Swarm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Swarm")
            .field("id", &self.id)
            .field("store", &self.store)
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}

/// A workspace failure as a [`SwarmError`].
fn workspace_error(err: &AgentError) -> SwarmError {
    SwarmError::Workspace(err.message().to_owned())
}

/// The workspace reference in the stored JSON `text`.
fn parse_ref(text: &str) -> Result<WorkspaceRef, SwarmError> {
    serde_json::from_str(text)
        .map_err(|err| SwarmError::Backend(format!("a stored snapshot reference is bad: {err}")))
}

/// A workspace reference as stored JSON.
fn ref_text(snapshot: &WorkspaceRef) -> Result<String, SwarmError> {
    serde_json::to_string(snapshot).map_err(|err| SwarmError::Backend(err.to_string()))
}

impl Swarm {
    /// A swarm `id` with its rows in `store`, its main snapshots in
    /// `workspaces`, and `judge` as its eval. The policy is the default.
    #[must_use]
    pub fn new(
        id: SwarmId,
        store: Arc<dyn SwarmStore>,
        workspaces: Arc<WorkspaceStore>,
        judge: Arc<dyn SwarmJudge>,
    ) -> Self {
        Self {
            id,
            store,
            workspaces,
            judge,
            policy: SwarmPolicy::default(),
            clock: Arc::new(Timestamp::now),
        }
    }

    /// The swarm with `policy`.
    #[must_use]
    pub fn with_policy(mut self, policy: SwarmPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// The swarm with `clock` in place of the system clock. The leases use
    /// it.
    #[must_use]
    pub fn with_clock(mut self, clock: StoreClock) -> Self {
        self.clock = clock;
        self
    }

    /// The name of the swarm.
    #[must_use]
    pub const fn id(&self) -> &SwarmId {
        &self.id
    }

    /// The policy of the swarm.
    #[must_use]
    pub const fn policy(&self) -> &SwarmPolicy {
        &self.policy
    }

    /// The store of the swarm.
    #[must_use]
    pub fn store(&self) -> &Arc<dyn SwarmStore> {
        &self.store
    }

    fn now_ms(&self) -> i64 {
        (self.clock)().as_millisecond()
    }

    fn name(&self) -> &str {
        self.id.as_str()
    }

    /// Load the files of the snapshot `snapshot`.
    async fn load(&self, snapshot: &WorkspaceRef) -> Result<Files, SwarmError> {
        let mount = WorkspaceMount {
            scope: self.id.main_scope(),
            head: Some(snapshot.clone()),
            generation: snapshot.generation,
        };
        let workspace = self
            .workspaces
            .load(&mount)
            .await
            .map_err(|err| workspace_error(&err))?;
        Ok(workspace.files().clone())
    }

    /// Save `files` as the snapshot of main version `version`.
    async fn save(&self, version: u64, files: &Files) -> Result<WorkspaceRef, SwarmError> {
        let mut workspace = Workspace::default();
        for (path, file) in files {
            workspace
                .write_file(path, file.clone())
                .map_err(|err| workspace_error(&err))?;
        }
        self.workspaces
            .save(&self.id.main_scope(), version, &workspace)
            .await
            .map_err(|err| workspace_error(&err))
    }

    /// The files of the member under the root.
    fn ours(&self, workspace: &Workspace) -> Files {
        under(workspace.files(), &self.policy.prefix())
    }

    /// Replace the files under the root of `workspace` with `files`.
    fn check_out(&self, workspace: &mut Workspace, files: &Files) -> Result<(), SwarmError> {
        let prefix = self.policy.prefix();
        let stale: Vec<String> = workspace
            .files()
            .keys()
            .filter(|path| path.starts_with(&prefix))
            .cloned()
            .collect();
        for path in stale {
            workspace.remove(&path);
        }
        for (path, file) in files {
            workspace
                .write_file(&format!("{prefix}{path}"), file.clone())
                .map_err(|err| workspace_error(&err))?;
        }
        Ok(())
    }

    /// Score `files` with the judge. A failure is a message for the log.
    async fn score(&self, files: &Files) -> Result<Score, String> {
        self.judge
            .score(&self.id, files)
            .await
            .map_err(|err| err.message().to_owned())
    }

    /// Seed main with `files`, scored by the judge. A swarm that already
    /// has a main keeps it. Returns the main head.
    ///
    /// # Errors
    ///
    /// Returns an error when the judge, the workspace store or the store
    /// fails.
    pub async fn seed(&self, files: &Files) -> Result<MainRow, SwarmError> {
        if let Some(main) = self.store.main(self.name()).await? {
            return Ok(main);
        }
        let score = self
            .score(files)
            .await
            .map_err(|err| SwarmError::Invalid(format!("the judge failed on the seed: {err}")))?;
        let snapshot = self.save(1, files).await?;
        let row = MainRow {
            version: 1,
            snapshot: ref_text(&snapshot)?,
            score: score.units(),
            item: None,
            author: None,
            updated_at_ms: self.now_ms(),
        };
        self.store.seed_main(self.name(), &row).await?;
        self.require_main().await
    }

    /// The main head, if the swarm has one.
    ///
    /// # Errors
    ///
    /// Returns an error when the store fails.
    pub async fn main(&self) -> Result<Option<MainRow>, SwarmError> {
        self.store.main(self.name()).await
    }

    async fn require_main(&self) -> Result<MainRow, SwarmError> {
        self.store
            .main(self.name())
            .await?
            .ok_or_else(|| SwarmError::NoMain(self.name().to_owned()))
    }

    /// The main head and its files.
    ///
    /// # Errors
    ///
    /// Returns [`SwarmError::NoMain`] before a seed, or an error when a
    /// store fails.
    pub async fn main_files(&self) -> Result<(MainRow, Files), SwarmError> {
        let main = self.require_main().await?;
        let files = self.load(&parse_ref(&main.snapshot)?).await?;
        Ok((main, files))
    }

    /// Add `item`, written by `by`. An item with the same id and the same
    /// lane, title and text is the same item, so a retried add returns it.
    ///
    /// # Errors
    ///
    /// Returns [`SwarmError::Invalid`] for a bad input or an id that
    /// another item has.
    pub async fn add_item(&self, item: &NewItem, by: &str) -> Result<ItemRow, SwarmError> {
        check_name("item", &item.id)?;
        check_name("lane", &item.lane)?;
        check_member(by)?;
        check_len("title", &item.title, MAX_TITLE_LEN)?;
        check_len("item text", &item.body, MAX_ITEM_BODY_LEN)?;
        let now = self.now_ms();
        let row = ItemRow {
            item: item.id.clone(),
            lane: item.lane.clone(),
            title: item.title.clone(),
            body: item.body.clone(),
            status: ItemStatus::Open.as_str().to_owned(),
            holder: None,
            epoch: 0,
            lease_until_ms: 0,
            base_version: 0,
            base_snapshot: String::new(),
            rev: 1,
            created_by: by.to_owned(),
            created_at_ms: now,
            updated_at_ms: now,
        };
        if self.store.insert_item(self.name(), &row).await? {
            return Ok(row);
        }
        let stored = self.require_item(&item.id).await?;
        if stored.lane == row.lane && stored.title == row.title && stored.body == row.body {
            Ok(stored)
        } else {
            Err(SwarmError::Invalid(format!(
                "the swarm {} already has another item {}",
                self.name(),
                item.id
            )))
        }
    }

    /// The item `item`, if any.
    ///
    /// # Errors
    ///
    /// Returns an error when the store fails.
    pub async fn item(&self, item: &str) -> Result<Option<ItemRow>, SwarmError> {
        self.store.item(self.name(), item).await
    }

    async fn require_item(&self, item: &str) -> Result<ItemRow, SwarmError> {
        self.store
            .item(self.name(), item)
            .await?
            .ok_or_else(|| SwarmError::NoItem {
                swarm: self.name().to_owned(),
                item: item.to_owned(),
            })
    }

    /// The items that match `query`, in id order.
    ///
    /// # Errors
    ///
    /// Returns an error when the store fails.
    pub async fn items(&self, query: ItemQuery<'_>) -> Result<Vec<ItemRow>, SwarmError> {
        self.store.items(self.name(), query).await
    }

    /// The posts after `after`, of `channel` or of all channels.
    ///
    /// # Errors
    ///
    /// Returns an error when the store fails.
    pub async fn posts(
        &self,
        channel: Option<&str>,
        after: u64,
        limit: u32,
    ) -> Result<Vec<PostRow>, SwarmError> {
        self.store.posts(self.name(), channel, after, limit).await
    }

    /// The verdicts after `after`.
    ///
    /// # Errors
    ///
    /// Returns an error when the store fails.
    pub async fn merges(&self, after: u64, limit: u32) -> Result<Vec<MergeRow>, SwarmError> {
        self.store.merges(self.name(), after, limit).await
    }

    /// Add a post by `member` to `channel`. `key` names the write, so a
    /// retried post lands once.
    ///
    /// # Errors
    ///
    /// Returns [`SwarmError::Invalid`] for a bad input, or an error when the
    /// store fails.
    pub async fn post(
        &self,
        member: &str,
        channel: &str,
        body: &str,
        key: &str,
    ) -> Result<PostRow, SwarmError> {
        check_member(member)?;
        check_name("channel", channel)?;
        check_len("post", body, MAX_POST_LEN)?;
        if key.is_empty() {
            return Err(SwarmError::Invalid("a post needs a key".to_owned()));
        }
        let post = NewPost {
            post_key: key,
            channel,
            author: member,
            body,
            at_ms: self.now_ms(),
        };
        self.store.append_post(self.name(), post).await
    }

    /// Claim `item` for `member`, and check out its base into `workspace`.
    ///
    /// A take checks out the current main and records it as the base. A
    /// member that already holds the item keeps its epoch. Its lease is
    /// renewed, and its base is checked out again. So a claim that runs
    /// again after a crash leaves the same files. A take counts the items of
    /// the member again after its write. A take that sees the cap broken
    /// gives its item back and returns [`ClaimRefusal::AtCap`].
    ///
    /// # Errors
    ///
    /// Returns [`SwarmError::NoItem`] for an unknown item,
    /// [`SwarmError::NoMain`] before a seed, or an error when a store
    /// fails.
    pub async fn claim(
        &self,
        member: &str,
        item: &str,
        workspace: &mut Workspace,
    ) -> Result<ClaimOutcome, SwarmError> {
        check_member(member)?;
        for _ in 0..MAX_SWAPS {
            let row = self.require_item(item).await?;
            let status = item_status(&row)?;
            let now = self.now_ms();
            let mine = row.holder.as_deref() == Some(member);
            let live = lease_live(row.lease_until_ms, now);
            let held = self.store.count_held(self.name(), member).await?;
            let others = if status == ItemStatus::Claimed && mine {
                held.saturating_sub(1)
            } else {
                held
            };
            let others = u32::try_from(others).unwrap_or(u32::MAX);
            let lease = lease_end(now, self.policy.lease_ms());
            let (next, base) = match decide_claim(
                status,
                mine,
                live,
                row.epoch,
                others,
                self.policy.claim_cap(),
            ) {
                ClaimDecision::Refuse(refusal) => {
                    return Ok(ClaimOutcome::Refused { refusal, item: row });
                }
                ClaimDecision::Keep => {
                    let base = self.load(&parse_ref(&row.base_snapshot)?).await?;
                    let next = ItemRow {
                        lease_until_ms: lease,
                        updated_at_ms: now,
                        ..row.clone()
                    };
                    (next, base)
                }
                ClaimDecision::Take { epoch } => {
                    let main = self.require_main().await?;
                    let base = self.load(&parse_ref(&main.snapshot)?).await?;
                    let next = ItemRow {
                        status: ItemStatus::Claimed.as_str().to_owned(),
                        holder: Some(member.to_owned()),
                        epoch,
                        lease_until_ms: lease,
                        base_version: main.version,
                        base_snapshot: main.snapshot.clone(),
                        updated_at_ms: now,
                        ..row.clone()
                    };
                    (next, base)
                }
            };
            if self.store.swap_item(self.name(), row.rev, &next).await? {
                let took = !(mine && status == ItemStatus::Claimed);
                if took && self.over_cap(member).await? {
                    // Another take by the same member landed after the
                    // count. The take that sees the cap broken gives its
                    // item back. Each racing take does so, so the member
                    // never keeps more than the cap.
                    // A fence refusal means that the item already left the
                    // member, which keeps the cap too.
                    let _released = self.release(member, item, next.epoch).await?;
                    let item = self.require_item(item).await?;
                    return Ok(ClaimOutcome::Refused {
                        refusal: ClaimRefusal::AtCap,
                        item,
                    });
                }
                self.check_out(workspace, &base)?;
                let stored = ItemRow {
                    rev: row.rev.saturating_add(1),
                    ..next
                };
                return Ok(if mine && status == ItemStatus::Claimed {
                    ClaimOutcome::Kept(stored)
                } else {
                    ClaimOutcome::Claimed(stored)
                });
            }
        }
        Err(SwarmError::Contended(format!("item {item}")))
    }

    /// Whether `member` holds more items than the cap, after a take.
    async fn over_cap(&self, member: &str) -> Result<bool, SwarmError> {
        let held = self.store.count_held(self.name(), member).await?;
        Ok(held > u64::from(self.policy.claim_cap()))
    }

    /// Run `change` on `item` when the fence passes for `member` at
    /// `epoch`. Returns the refusal of the fence, or the stored item.
    async fn fenced(
        &self,
        member: &str,
        item: &str,
        epoch: u64,
        change: impl Fn(ItemRow, i64) -> ItemRow + Send,
    ) -> Result<Result<ItemRow, FenceRefusal>, SwarmError> {
        check_member(member)?;
        for _ in 0..MAX_SWAPS {
            let row = self.require_item(item).await?;
            let status = item_status(&row)?;
            let holder = row.holder.as_deref() == Some(member);
            if let Some(refusal) = fence(status, holder, row.epoch == epoch) {
                return Ok(Err(refusal));
            }
            let rev = row.rev;
            let next = change(row, self.now_ms());
            if self.store.swap_item(self.name(), rev, &next).await? {
                return Ok(Ok(ItemRow {
                    rev: rev.saturating_add(1),
                    ..next
                }));
            }
        }
        Err(SwarmError::Contended(format!("item {item}")))
    }

    /// Renew the lease of `member` on `item` at `epoch`.
    ///
    /// # Errors
    ///
    /// Returns [`SwarmError::NoItem`] for an unknown item, or an error when
    /// the store fails.
    pub async fn renew(
        &self,
        member: &str,
        item: &str,
        epoch: u64,
    ) -> Result<Result<ItemRow, FenceRefusal>, SwarmError> {
        let lease_ms = self.policy.lease_ms();
        self.fenced(member, item, epoch, move |row, now| ItemRow {
            lease_until_ms: lease_end(now, lease_ms),
            updated_at_ms: now,
            ..row
        })
        .await
    }

    /// Release the claim of `member` on `item` at `epoch`. The item is open
    /// again and keeps its epoch, so the next take moves past it.
    ///
    /// # Errors
    ///
    /// Returns [`SwarmError::NoItem`] for an unknown item, or an error when
    /// the store fails.
    pub async fn release(
        &self,
        member: &str,
        item: &str,
        epoch: u64,
    ) -> Result<Result<ItemRow, FenceRefusal>, SwarmError> {
        self.fenced(member, item, epoch, |row, now| ItemRow {
            status: ItemStatus::Open.as_str().to_owned(),
            holder: None,
            lease_until_ms: 0,
            updated_at_ms: now,
            ..row
        })
        .await
    }

    /// Close `item` for good. An operator call: it needs no fence.
    ///
    /// # Errors
    ///
    /// Returns [`SwarmError::NoItem`] for an unknown item, or an error when
    /// the store fails.
    pub async fn drop_item(&self, item: &str) -> Result<ItemRow, SwarmError> {
        for _ in 0..MAX_SWAPS {
            let row = self.require_item(item).await?;
            if item_status(&row)? == ItemStatus::Dropped {
                return Ok(row);
            }
            let rev = row.rev;
            let next = ItemRow {
                status: ItemStatus::Dropped.as_str().to_owned(),
                updated_at_ms: self.now_ms(),
                ..row
            };
            if self.store.swap_item(self.name(), rev, &next).await? {
                return Ok(ItemRow {
                    rev: rev.saturating_add(1),
                    ..next
                });
            }
        }
        Err(SwarmError::Contended(format!("item {item}")))
    }

    /// A verdict row of `call` with no scores.
    fn verdict_row(
        &self,
        call: &SubmitCall<'_>,
        row: &ItemRow,
        main_version: u64,
        verdict: Verdict,
        detail: String,
    ) -> MergeRow {
        MergeRow {
            seq: 0,
            submit_key: call.key.to_owned(),
            item: row.item.clone(),
            holder: call.member.to_owned(),
            epoch: call.epoch,
            base_version: row.base_version,
            main_version,
            verdict: verdict.as_str().to_owned(),
            candidate_score: None,
            main_score: None,
            merged_version: None,
            snapshot: None,
            detail,
            created_at_ms: self.now_ms(),
        }
    }

    /// Submit the files of `member` for `item` at `epoch`.
    ///
    /// The gate merges the files under the root of `workspace` onto the
    /// current main, against the base of the claim. The judge scores the
    /// merged files. A score that passes the bar moves main, closes the
    /// item, and checks the new main out into `workspace`. A merge of
    /// another member first means a new attempt on the newer main, up to
    /// [`SwarmPolicy::max_attempts`]. Each outcome is logged with `key`. A
    /// second submit with the same key returns the first verdict.
    ///
    /// A rejected or conflicting submit keeps the claim, so the member can
    /// try again.
    ///
    /// # Errors
    ///
    /// Returns [`SwarmError::NoItem`] for an unknown item,
    /// [`SwarmError::NoMain`] before a seed, or an error when a store
    /// fails.
    pub async fn submit(
        &self,
        member: &str,
        item: &str,
        epoch: u64,
        key: &str,
        workspace: &mut Workspace,
    ) -> Result<MergeRow, SwarmError> {
        check_member(member)?;
        if key.is_empty() {
            return Err(SwarmError::Invalid("a submit needs a key".to_owned()));
        }
        if let Some(found) = self.store.merge_by_key(self.name(), key).await? {
            self.check_out_verdict(&found, workspace).await?;
            return Ok(found);
        }
        let call = SubmitCall {
            key,
            member,
            item,
            epoch,
        };
        let ours = self.ours(workspace);
        let mut last_main = 0;
        for _ in 0..self.policy.attempts() {
            match self.attempt(&call, &ours).await? {
                Attempt::Done(found) => {
                    let found = *found;
                    self.check_out_verdict(&found, workspace).await?;
                    return Ok(found);
                }
                Attempt::Again { main_version } => last_main = main_version,
            }
        }
        let row = self.require_item(item).await?;
        let record = self.verdict_row(
            &call,
            &row,
            last_main,
            Verdict::Contended,
            "main moved on every attempt".to_owned(),
        );
        self.store.record_verdict(self.name(), &record).await
    }

    /// One attempt of a submit on the current main.
    ///
    /// A fence refusal, a conflict, a judge failure and a score under the
    /// bar each log a verdict and end the submit. A commit that loses to
    /// another merge asks for one more attempt.
    async fn attempt(&self, call: &SubmitCall<'_>, ours: &Files) -> Result<Attempt, SwarmError> {
        let row = self.require_item(call.item).await?;
        let status = item_status(&row)?;
        let main = self.require_main().await?;
        let holder = row.holder.as_deref() == Some(call.member);
        let end = |verdict: Verdict, detail: String| {
            self.verdict_row(call, &row, main.version, verdict, detail)
        };
        if let Some(refusal) = fence(status, holder, row.epoch == call.epoch) {
            let record = end(Verdict::Fenced, refusal.as_str().to_owned());
            return self.logged(&record).await;
        }
        let base = self.load(&parse_ref(&row.base_snapshot)?).await?;
        let theirs = self.load(&parse_ref(&main.snapshot)?).await?;
        let merged = match three_way(&base, ours, &theirs) {
            MergeResult::Clean(files) => files,
            MergeResult::Conflict(paths) => {
                return self.logged(&end(Verdict::Conflict, paths.join(", "))).await;
            }
        };
        let score = match self.score(&merged).await {
            Ok(score) => score,
            Err(message) => return self.logged(&end(Verdict::JudgeFailed, message)).await,
        };
        let scored = |verdict: Verdict, detail: String| MergeRow {
            candidate_score: Some(score.units()),
            main_score: Some(main.score),
            ..end(verdict, detail)
        };
        if !passes(self.policy.bar, score.units(), main.score) {
            let detail = "the merged files do not pass the bar over main".to_owned();
            return self.logged(&scored(Verdict::Rejected, detail)).await;
        }
        let Some(next_version) = main.version.checked_add(1) else {
            return Err(SwarmError::Contended("main version counter".to_owned()));
        };
        let snapshot = ref_text(&self.save(next_version, &merged).await?)?;
        let now = self.now_ms();
        let commit = MergeCommit {
            expected_main: main.version,
            next_main: MainRow {
                version: next_version,
                snapshot: snapshot.clone(),
                score: score.units(),
                item: Some(row.item.clone()),
                author: Some(call.member.to_owned()),
                updated_at_ms: now,
            },
            item_rev: row.rev,
            next_item: ItemRow {
                status: ItemStatus::Merged.as_str().to_owned(),
                lease_until_ms: 0,
                updated_at_ms: now,
                ..row.clone()
            },
            record: MergeRow {
                merged_version: Some(next_version),
                snapshot: Some(snapshot),
                ..scored(Verdict::Merged, String::new())
            },
        };
        Ok(match self.store.commit_merge(self.name(), &commit).await? {
            CommitOutcome::Committed(found) | CommitOutcome::Recorded(found) => {
                Attempt::Done(Box::new(found))
            }
            CommitOutcome::MainMoved { .. } | CommitOutcome::ItemMoved => Attempt::Again {
                main_version: main.version,
            },
        })
    }

    /// Log `record`, and end the submit with the logged verdict.
    async fn logged(&self, record: &MergeRow) -> Result<Attempt, SwarmError> {
        Ok(Attempt::Done(Box::new(
            self.store.record_verdict(self.name(), record).await?,
        )))
    }

    /// After a merge, check the current main out into `workspace`.
    ///
    /// The current main holds the merge, because each later merge keeps
    /// each change of main. A prune can delete the snapshot of an older
    /// verdict, so a replayed verdict never loads it. A prune can also move
    /// past the main that a read saw, so the read tries again.
    async fn check_out_verdict(
        &self,
        verdict: &MergeRow,
        workspace: &mut Workspace,
    ) -> Result<(), SwarmError> {
        if verdict.merged_version.is_none() {
            return Ok(());
        }
        let mut last = None;
        for _ in 0..MAX_SWAPS {
            match self.main_files().await {
                Ok((_, files)) => return self.check_out(workspace, &files),
                Err(err @ SwarmError::Workspace(_)) => last = Some(err),
                Err(err) => return Err(err),
            }
        }
        Err(last.unwrap_or_else(|| SwarmError::Contended("main".to_owned())))
    }

    /// Delete the main snapshots that nothing needs. Returns the count.
    ///
    /// A snapshot stays when it is the current main or the base of a
    /// claimed item. A snapshot at or above the current main version stays
    /// too. A gate in flight saves above the main that it read. So the
    /// bound never deletes a snapshot that a commit can still name.
    ///
    /// # Errors
    ///
    /// Returns an error when a store fails.
    pub async fn prune(&self) -> Result<u64, SwarmError> {
        let Some(main) = self.store.main(self.name()).await? else {
            return Ok(0);
        };
        let mut keep = std::collections::BTreeSet::new();
        keep.insert(parse_ref(&main.snapshot)?.key());
        let mut after: Option<String> = None;
        loop {
            let page = self
                .store
                .items(
                    self.name(),
                    ItemQuery {
                        status: Some(ItemStatus::Claimed.as_str()),
                        after: after.as_deref(),
                        limit: super::MAX_READ,
                        ..ItemQuery::default()
                    },
                )
                .await?;
            for row in &page {
                keep.insert(parse_ref(&row.base_snapshot)?.key());
            }
            match page.last() {
                Some(last) => after = Some(last.item.clone()),
                None => break,
            }
        }
        let scope = self.id.main_scope();
        let blobs = self.workspaces.blobs();
        let floor = format!("{:016x}", main.version);
        let mut deleted = 0_u64;
        for key in blobs
            .list(&scope)
            .await
            .map_err(|err| workspace_error(&err))?
        {
            // A key starts with its generation as 16 hex digits, so a text
            // compare of the prefix is a number compare.
            let below = key.get(..16).is_some_and(|generation| *generation < *floor);
            if below
                && !keep.contains(&key)
                && blobs
                    .delete(&scope, &key)
                    .await
                    .map_err(|err| workspace_error(&err))?
            {
                deleted = deleted.saturating_add(1);
            }
        }
        Ok(deleted)
    }
}
