//! One suite for every `SwarmStore` (issue #2814).
//!
//! `swarm_store.rs` runs it on the in-memory and `SQLite` stores.
//! `swarm_store_postgres.rs` runs it on Postgres. Each case uses a new swarm
//! name, so the cases can share one database.
//!
//! The first part checks the rows of the store. The second part drives a
//! `Swarm` on the store, so the claim fence and the merge gate decide alike
//! on every backend.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use autumn_harvest::payload_codec::PayloadCodecs;
use autumn_harvest_agent::swarm::{
    Bar, ClaimOutcome, ClaimRefusal, CommitOutcome, FenceRefusal, ItemQuery, ItemRow, MainRow,
    MergeCommit, MergeRow, NewItem, NewPost, Score, Swarm, SwarmId, SwarmJudge, SwarmPolicy,
    SwarmStore, Verdict,
};
use autumn_harvest_agent::{
    AgentError, BoxFuture, ErrorKind, InMemoryWorkspaceBlobs, StoreClock, Workspace, WorkspaceFile,
    WorkspaceStore,
};
use jiff::{SignedDuration, Timestamp};

/// The store under test. Each call of the suite gets the same store.
pub type Store = Arc<dyn SwarmStore>;

static NEXT: AtomicU64 = AtomicU64::new(0);

/// A swarm name that no other case uses, in this process or an earlier one.
pub fn fresh(prefix: &str) -> String {
    let n = NEXT.fetch_add(1, Ordering::SeqCst);
    let stamp = Timestamp::now().as_microsecond();
    format!("{prefix}-{stamp}-{n}")
}

/// Run every case on `store`.
pub async fn run_all(store: &Store) {
    rows_seed_once(store).await;
    rows_items_filter_and_page(store).await;
    rows_swap_needs_the_read_rev(store).await;
    rows_posts_take_seq_and_dedupe(store).await;
    rows_verdicts_dedupe_by_key(store).await;
    rows_commit_is_all_or_nothing(store).await;
    gate_claim_checks_out_main(store).await;
    gate_lapsed_lease_fences_the_old_holder(store).await;
    gate_cap_and_release(store).await;
    gate_merges_only_a_better_change(store).await;
    gate_rebases_onto_a_newer_main(store).await;
    gate_names_a_conflict(store).await;
    gate_logs_a_judge_failure(store).await;
    gate_closed_items_refuse(store).await;
    gate_prune_keeps_live_bases(store).await;
    gate_posts_read_after_a_cursor(store).await;
}

fn item_row(id: &str, lane: &str) -> ItemRow {
    ItemRow {
        item: id.to_owned(),
        lane: lane.to_owned(),
        title: format!("title {id}"),
        body: format!("body {id}"),
        status: "open".to_owned(),
        holder: None,
        epoch: 0,
        lease_until_ms: 0,
        base_version: 0,
        base_snapshot: String::new(),
        rev: 1,
        created_by: "ops".to_owned(),
        created_at_ms: 10,
        updated_at_ms: 10,
    }
}

fn main_row(version: u64, score: i64) -> MainRow {
    MainRow {
        version,
        snapshot: format!("{{\"v\":{version}}}"),
        score,
        item: None,
        author: None,
        updated_at_ms: 1,
    }
}

fn verdict(key: &str, verdict: Verdict) -> MergeRow {
    MergeRow {
        seq: 0,
        submit_key: key.to_owned(),
        item: "a".to_owned(),
        holder: "coder/c1".to_owned(),
        epoch: 1,
        base_version: 1,
        main_version: 1,
        verdict: verdict.as_str().to_owned(),
        candidate_score: Some(3),
        main_score: Some(2),
        merged_version: None,
        snapshot: None,
        detail: String::new(),
        created_at_ms: 5,
    }
}

async fn rows_seed_once(store: &Store) {
    let swarm = fresh("seed");
    assert!(store.main(&swarm).await.unwrap().is_none());
    assert!(store.seed_main(&swarm, &main_row(1, 5)).await.unwrap());
    assert!(!store.seed_main(&swarm, &main_row(1, 9)).await.unwrap());
    assert_eq!(store.main(&swarm).await.unwrap(), Some(main_row(1, 5)));
}

async fn rows_items_filter_and_page(store: &Store) {
    let swarm = fresh("items");
    for (id, lane) in [("a", "x"), ("b", "y"), ("c", "x"), ("d", "x")] {
        assert!(
            store
                .insert_item(&swarm, &item_row(id, lane))
                .await
                .unwrap()
        );
    }
    assert!(
        !store
            .insert_item(&swarm, &item_row("a", "y"))
            .await
            .unwrap()
    );
    assert_eq!(
        store.item(&swarm, "a").await.unwrap(),
        Some(item_row("a", "x"))
    );
    assert!(store.item(&swarm, "zz").await.unwrap().is_none());
    let ids = |rows: Vec<ItemRow>| rows.into_iter().map(|r| r.item).collect::<Vec<_>>();
    let lane_x = ItemQuery {
        lane: Some("x"),
        limit: 10,
        ..ItemQuery::default()
    };
    assert_eq!(
        ids(store.items(&swarm, lane_x).await.unwrap()),
        ["a", "c", "d"]
    );
    let page = ItemQuery {
        after: Some("a"),
        limit: 2,
        ..ItemQuery::default()
    };
    assert_eq!(ids(store.items(&swarm, page).await.unwrap()), ["b", "c"]);
    let claimed = ItemRow {
        status: "claimed".to_owned(),
        holder: Some("coder/c1".to_owned()),
        ..item_row("c", "x")
    };
    assert!(store.swap_item(&swarm, 1, &claimed).await.unwrap());
    let mine = ItemQuery {
        holder: Some("coder/c1"),
        status: Some("claimed"),
        limit: 10,
        ..ItemQuery::default()
    };
    assert_eq!(ids(store.items(&swarm, mine).await.unwrap()), ["c"]);
    assert_eq!(store.count_held(&swarm, "coder/c1").await.unwrap(), 1);
    assert_eq!(store.count_held(&swarm, "coder/c2").await.unwrap(), 0);
    let other = fresh("items-other");
    assert_eq!(store.items(&other, lane_x).await.unwrap(), Vec::new());
}

async fn rows_swap_needs_the_read_rev(store: &Store) {
    let swarm = fresh("swap");
    store
        .insert_item(&swarm, &item_row("a", "x"))
        .await
        .unwrap();
    let next = ItemRow {
        title: "renamed".to_owned(),
        created_by: "someone-else".to_owned(),
        ..item_row("a", "x")
    };
    assert!(store.swap_item(&swarm, 1, &next).await.unwrap());
    assert!(!store.swap_item(&swarm, 1, &next).await.unwrap());
    let stored = store.item(&swarm, "a").await.unwrap().unwrap();
    assert_eq!(stored.rev, 2);
    assert_eq!(stored.title, "renamed");
    assert_eq!(stored.created_by, "ops", "a swap keeps who added the item");
    assert!(!store.swap_item(&swarm, u64::MAX, &next).await.unwrap());
}

async fn rows_posts_take_seq_and_dedupe(store: &Store) {
    let swarm = fresh("posts");
    let post = |key: &'static str, channel: &'static str| NewPost {
        post_key: key,
        channel,
        author: "coder/c1",
        body: "hello",
        at_ms: 7,
    };
    let first = store
        .append_post(&swarm, post("k1", "general"))
        .await
        .unwrap();
    let second = store
        .append_post(&swarm, post("k2", "quality"))
        .await
        .unwrap();
    let again = store
        .append_post(&swarm, post("k1", "general"))
        .await
        .unwrap();
    assert_eq!((first.seq, second.seq), (1, 2));
    assert_eq!(again, first, "a retried post lands once");
    let third = store
        .append_post(&swarm, post("k3", "general"))
        .await
        .unwrap();
    assert!(third.seq > second.seq);
    let all = store.posts(&swarm, None, 0, 10).await.unwrap();
    assert_eq!(all.len(), 3);
    let general = store.posts(&swarm, Some("general"), 0, 10).await.unwrap();
    assert_eq!(
        general.iter().map(|p| p.seq).collect::<Vec<_>>(),
        [first.seq, third.seq]
    );
    let after = store.posts(&swarm, None, first.seq, 1).await.unwrap();
    assert_eq!(after, vec![second]);
    assert_eq!(
        store.posts(&swarm, None, u64::MAX, 10).await.unwrap(),
        Vec::new()
    );
}

async fn rows_verdicts_dedupe_by_key(store: &Store) {
    let swarm = fresh("verdicts");
    let first = store
        .record_verdict(&swarm, &verdict("s1", Verdict::Rejected))
        .await
        .unwrap();
    let again = store
        .record_verdict(&swarm, &verdict("s1", Verdict::Conflict))
        .await
        .unwrap();
    assert_eq!(first.seq, 1);
    assert_eq!(again, first, "a retried submit logs once");
    let second = store
        .record_verdict(&swarm, &verdict("s2", Verdict::Fenced))
        .await
        .unwrap();
    assert_eq!(second.seq, 2);
    assert_eq!(
        store.merge_by_key(&swarm, "s2").await.unwrap(),
        Some(second.clone())
    );
    assert_eq!(store.merges(&swarm, 1, 10).await.unwrap(), vec![second]);
}

async fn rows_commit_is_all_or_nothing(store: &Store) {
    let swarm = fresh("commit");
    store.seed_main(&swarm, &main_row(1, 5)).await.unwrap();
    store
        .insert_item(&swarm, &item_row("a", "x"))
        .await
        .unwrap();
    let merged_item = ItemRow {
        status: "merged".to_owned(),
        holder: Some("coder/c1".to_owned()),
        ..item_row("a", "x")
    };
    let record = MergeRow {
        merged_version: Some(2),
        snapshot: Some("{\"v\":2}".to_owned()),
        ..verdict("m1", Verdict::Merged)
    };
    let commit = |expected_main: u64, item_rev: u64, key: &str| MergeCommit {
        expected_main,
        next_main: main_row(2, 9),
        item_rev,
        next_item: merged_item.clone(),
        record: MergeRow {
            submit_key: key.to_owned(),
            ..record.clone()
        },
    };
    assert_eq!(
        store
            .commit_merge(&swarm, &commit(7, 1, "m0"))
            .await
            .unwrap(),
        CommitOutcome::MainMoved { actual: Some(1) }
    );
    assert_eq!(
        store
            .commit_merge(&swarm, &commit(1, 9, "m0"))
            .await
            .unwrap(),
        CommitOutcome::ItemMoved
    );
    assert_eq!(store.main(&swarm).await.unwrap(), Some(main_row(1, 5)));
    assert_eq!(store.item(&swarm, "a").await.unwrap().unwrap().rev, 1);
    assert_eq!(store.merges(&swarm, 0, 10).await.unwrap(), Vec::new());
    let CommitOutcome::Committed(row) = store
        .commit_merge(&swarm, &commit(1, 1, "m1"))
        .await
        .unwrap()
    else {
        panic!("the commit lands");
    };
    assert_eq!((row.seq, row.merged_version), (1, Some(2)));
    assert_eq!(store.main(&swarm).await.unwrap(), Some(main_row(2, 9)));
    let stored = store.item(&swarm, "a").await.unwrap().unwrap();
    assert_eq!((stored.status.as_str(), stored.rev), ("merged", 2));
    assert_eq!(
        store
            .commit_merge(&swarm, &commit(1, 1, "m1"))
            .await
            .unwrap(),
        CommitOutcome::Recorded(row),
        "a retried commit merges once"
    );
    let missing = fresh("commit-none");
    assert_eq!(
        store
            .commit_merge(&missing, &commit(1, 1, "m1"))
            .await
            .unwrap(),
        CommitOutcome::MainMoved { actual: None }
    );
}

/// A judge that counts the files whose text holds `good`. A file named
/// `boom` fails it.
#[derive(Debug, Default)]
pub struct CountJudge {
    pub calls: AtomicU64,
}

impl SwarmJudge for CountJudge {
    fn score<'a>(
        &'a self,
        _swarm: &'a SwarmId,
        files: &'a BTreeMap<String, WorkspaceFile>,
    ) -> BoxFuture<'a, Result<Score, AgentError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let result = if files.contains_key("boom") {
            Err(AgentError::new(ErrorKind::Tool, "the eval broke"))
        } else {
            let good = files
                .values()
                .filter(|file| String::from_utf8_lossy(&file.data).contains("good"))
                .count();
            Ok(Score::new(i64::try_from(good).unwrap() * 10))
        };
        Box::pin(std::future::ready(result))
    }
}

/// A clock that a test moves by hand.
#[derive(Clone)]
pub struct HandClock(Arc<Mutex<Timestamp>>);

impl HandClock {
    pub fn at(at: Timestamp) -> Self {
        Self(Arc::new(Mutex::new(at)))
    }

    pub fn advance(&self, by: SignedDuration) {
        let mut at = self.0.lock().unwrap();
        *at += by;
    }

    pub fn clock(&self) -> StoreClock {
        let cell = Arc::clone(&self.0);
        Arc::new(move || *cell.lock().unwrap())
    }
}

/// One swarm on `store`, with its own snapshots, judge and clock.
pub struct Fixture {
    pub swarm: Swarm,
    pub judge: Arc<CountJudge>,
    pub clock: HandClock,
    pub blobs: Arc<InMemoryWorkspaceBlobs>,
}

pub fn files(pairs: &[(&str, &str)]) -> BTreeMap<String, WorkspaceFile> {
    pairs
        .iter()
        .map(|(path, text)| {
            (
                (*path).to_owned(),
                WorkspaceFile {
                    data: text.as_bytes().to_vec(),
                    executable: false,
                },
            )
        })
        .collect()
}

/// A swarm with `policy` and a main seeded with `seed`.
pub async fn fixture(store: &Store, policy: SwarmPolicy, seed: &[(&str, &str)]) -> Fixture {
    let blobs = Arc::new(InMemoryWorkspaceBlobs::default());
    let workspaces = Arc::new(WorkspaceStore::new(blobs.clone(), PayloadCodecs::default()));
    let judge = Arc::new(CountJudge::default());
    let clock = HandClock::at(Timestamp::from_second(1_800_000_000).unwrap());
    let id = SwarmId::new(fresh("gate")).unwrap();
    let swarm = Swarm::new(id, store.clone(), workspaces, judge.clone())
        .with_policy(policy)
        .with_clock(clock.clock());
    swarm.seed(&files(seed)).await.unwrap();
    Fixture {
        swarm,
        judge,
        clock,
        blobs,
    }
}

fn text(workspace: &Workspace, path: &str) -> Option<String> {
    workspace.read_text(path).map(str::to_owned)
}

fn claimed(outcome: ClaimOutcome) -> ItemRow {
    match outcome {
        ClaimOutcome::Claimed(row) => row,
        other => panic!("expected a take, got {other:?}"),
    }
}

async fn add(fx: &Fixture, id: &str) {
    fx.swarm
        .add_item(
            &NewItem::new(id, "core", format!("do {id}"), "the task"),
            "ops",
        )
        .await
        .unwrap();
}

async fn gate_claim_checks_out_main(store: &Store) {
    let policy = SwarmPolicy {
        root: "repo".to_owned(),
        ..SwarmPolicy::default()
    };
    let fx = fixture(store, policy, &[("a.txt", "base")]).await;
    add(&fx, "one").await;
    let mut ws = Workspace::default();
    ws.write("notes.md", "mine").unwrap();
    ws.write("repo/stale.txt", "old").unwrap();
    let row = claimed(fx.swarm.claim("coder/c1", "one", &mut ws).await.unwrap());
    assert_eq!((row.epoch, row.base_version), (1, 1));
    assert_eq!(text(&ws, "repo/a.txt").as_deref(), Some("base"));
    assert_eq!(
        text(&ws, "repo/stale.txt"),
        None,
        "a checkout replaces the root"
    );
    assert_eq!(
        text(&ws, "notes.md").as_deref(),
        Some("mine"),
        "files outside the root stay"
    );
    ws.write("repo/a.txt", "edited").unwrap();
    let ClaimOutcome::Kept(kept) = fx.swarm.claim("coder/c1", "one", &mut ws).await.unwrap() else {
        panic!("the holder keeps its claim");
    };
    assert_eq!(kept.epoch, 1, "a second claim keeps the epoch");
    assert!(kept.lease_until_ms >= row.lease_until_ms);
    assert_eq!(
        text(&ws, "repo/a.txt").as_deref(),
        Some("base"),
        "a second claim checks out the base again"
    );
    let added = fx
        .swarm
        .add_item(&NewItem::new("one", "core", "do one", "the task"), "ops")
        .await
        .unwrap();
    assert_eq!(added.item, "one", "a retried add returns the item");
    assert!(
        fx.swarm
            .add_item(&NewItem::new("one", "core", "other", "x"), "ops")
            .await
            .is_err()
    );
}

async fn gate_lapsed_lease_fences_the_old_holder(store: &Store) {
    let fx = fixture(store, SwarmPolicy::default(), &[("a", "x")]).await;
    add(&fx, "one").await;
    let (mut ws1, mut ws2) = (Workspace::default(), Workspace::default());
    claimed(fx.swarm.claim("coder/c1", "one", &mut ws1).await.unwrap());
    let ClaimOutcome::Refused { refusal, .. } =
        fx.swarm.claim("coder/c2", "one", &mut ws2).await.unwrap()
    else {
        panic!("a live lease refuses another member");
    };
    assert_eq!(refusal, ClaimRefusal::Taken);
    fx.clock.advance(SignedDuration::from_mins(31));
    let taken = claimed(fx.swarm.claim("coder/c2", "one", &mut ws2).await.unwrap());
    assert_eq!(taken.epoch, 2, "a take after a lapse moves the epoch");
    assert_eq!(
        fx.swarm.renew("coder/c1", "one", 1).await.unwrap(),
        Err(FenceRefusal::NotHolder)
    );
    assert_eq!(
        fx.swarm.renew("coder/c2", "one", 1).await.unwrap(),
        Err(FenceRefusal::StaleEpoch)
    );
    assert!(fx.swarm.renew("coder/c2", "one", 2).await.unwrap().is_ok());
    ws1.write("a", "good").unwrap();
    let row = fx
        .swarm
        .submit("coder/c1", "one", 1, "old-holder", &mut ws1)
        .await
        .unwrap();
    assert_eq!(row.verdict, Verdict::Fenced.as_str());
    assert_eq!(row.detail, "not_holder");
    assert_eq!(fx.swarm.main().await.unwrap().unwrap().version, 1);
    assert_eq!(
        fx.judge.calls.load(Ordering::SeqCst),
        1,
        "a fenced submit runs no eval"
    );
}

async fn gate_cap_and_release(store: &Store) {
    let fx = fixture(store, SwarmPolicy::default(), &[("a", "x")]).await;
    add(&fx, "one").await;
    add(&fx, "two").await;
    let mut ws = Workspace::default();
    claimed(fx.swarm.claim("coder/c1", "one", &mut ws).await.unwrap());
    let ClaimOutcome::Refused { refusal, .. } =
        fx.swarm.claim("coder/c1", "two", &mut ws).await.unwrap()
    else {
        panic!("the cap refuses a second item");
    };
    assert_eq!(refusal, ClaimRefusal::AtCap);
    let released = fx
        .swarm
        .release("coder/c1", "one", 1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((released.status.as_str(), released.holder), ("open", None));
    assert_eq!(
        fx.swarm.release("coder/c1", "one", 1).await.unwrap(),
        Err(FenceRefusal::NotClaimed)
    );
    let again = claimed(fx.swarm.claim("coder/c2", "one", &mut ws).await.unwrap());
    assert_eq!(
        again.epoch, 2,
        "a release keeps the epoch, so the next take moves past it"
    );
    claimed(fx.swarm.claim("coder/c1", "two", &mut ws).await.unwrap());
}

async fn gate_merges_only_a_better_change(store: &Store) {
    let fx = fixture(
        store,
        SwarmPolicy::default(),
        &[("a", "plain"), ("b", "plain")],
    )
    .await;
    add(&fx, "one").await;
    let mut ws = Workspace::default();
    claimed(fx.swarm.claim("coder/c1", "one", &mut ws).await.unwrap());
    ws.write("a", "still plain").unwrap();
    let rejected = fx
        .swarm
        .submit("coder/c1", "one", 1, "s1", &mut ws)
        .await
        .unwrap();
    assert_eq!(rejected.verdict, Verdict::Rejected.as_str());
    assert_eq!(
        (rejected.candidate_score, rejected.main_score),
        (Some(0), Some(0))
    );
    assert_eq!(fx.swarm.main().await.unwrap().unwrap().version, 1);
    assert_eq!(
        text(&ws, "a").as_deref(),
        Some("still plain"),
        "a rejection keeps the files"
    );
    ws.write("a", "good").unwrap();
    let merged = fx
        .swarm
        .submit("coder/c1", "one", 1, "s2", &mut ws)
        .await
        .unwrap();
    assert_eq!(merged.verdict, Verdict::Merged.as_str());
    assert_eq!(merged.merged_version, Some(2));
    let main = fx.swarm.main().await.unwrap().unwrap();
    assert_eq!((main.version, main.score), (2, 10));
    assert_eq!(main.author.as_deref(), Some("coder/c1"));
    let item = fx.swarm.item("one").await.unwrap().unwrap();
    assert_eq!(item.status, "merged");
    let (_, main_files) = fx.swarm.main_files().await.unwrap();
    assert_eq!(main_files, files(&[("a", "good"), ("b", "plain")]));
    let calls = fx.judge.calls.load(Ordering::SeqCst);
    let mut fresh_ws = Workspace::default();
    let retried = fx
        .swarm
        .submit("coder/c1", "one", 1, "s2", &mut fresh_ws)
        .await
        .unwrap();
    assert_eq!(retried, merged, "a retried submit returns its verdict");
    assert_eq!(
        text(&fresh_ws, "a").as_deref(),
        Some("good"),
        "and checks out the merge"
    );
    assert_eq!(
        fx.judge.calls.load(Ordering::SeqCst),
        calls,
        "a retry runs no eval"
    );
    assert_eq!(fx.swarm.main().await.unwrap().unwrap().version, 2);
    let neutral = SwarmPolicy {
        bar: Bar::NoRegression,
        ..SwarmPolicy::default()
    };
    let tie = fixture(store, neutral, &[("a", "plain")]).await;
    add(&tie, "one").await;
    let mut ws = Workspace::default();
    claimed(tie.swarm.claim("coder/c1", "one", &mut ws).await.unwrap());
    ws.write("c", "a neutral change").unwrap();
    let row = tie
        .swarm
        .submit("coder/c1", "one", 1, "t1", &mut ws)
        .await
        .unwrap();
    assert_eq!(
        row.verdict,
        Verdict::Merged.as_str(),
        "no regression takes a tie"
    );
}

async fn gate_rebases_onto_a_newer_main(store: &Store) {
    let fx = fixture(
        store,
        SwarmPolicy::default(),
        &[("a", "plain"), ("b", "plain")],
    )
    .await;
    add(&fx, "one").await;
    add(&fx, "two").await;
    let (mut ws1, mut ws2) = (Workspace::default(), Workspace::default());
    claimed(fx.swarm.claim("coder/c1", "one", &mut ws1).await.unwrap());
    claimed(fx.swarm.claim("coder/c2", "two", &mut ws2).await.unwrap());
    ws1.write("a", "good").unwrap();
    ws2.write("b", "good").unwrap();
    let first = fx
        .swarm
        .submit("coder/c1", "one", 1, "r1", &mut ws1)
        .await
        .unwrap();
    assert_eq!(first.merged_version, Some(2));
    let second = fx
        .swarm
        .submit("coder/c2", "two", 1, "r2", &mut ws2)
        .await
        .unwrap();
    assert_eq!(second.verdict, Verdict::Merged.as_str());
    assert_eq!((second.base_version, second.main_version), (1, 2));
    assert_eq!(
        (second.candidate_score, second.main_score),
        (Some(20), Some(10)),
        "the second change must beat the newer main"
    );
    let (main, main_files) = fx.swarm.main_files().await.unwrap();
    assert_eq!(main.version, 3);
    assert_eq!(
        main_files,
        files(&[("a", "good"), ("b", "good")]),
        "both changes stay"
    );
    assert_eq!(
        text(&ws2, "a").as_deref(),
        Some("good"),
        "the workspace is the new main"
    );
}

async fn gate_names_a_conflict(store: &Store) {
    let fx = fixture(store, SwarmPolicy::default(), &[("a", "plain")]).await;
    add(&fx, "one").await;
    add(&fx, "two").await;
    let (mut ws1, mut ws2) = (Workspace::default(), Workspace::default());
    claimed(fx.swarm.claim("coder/c1", "one", &mut ws1).await.unwrap());
    claimed(fx.swarm.claim("coder/c2", "two", &mut ws2).await.unwrap());
    ws1.write("a", "good one").unwrap();
    ws2.write("a", "good two").unwrap();
    fx.swarm
        .submit("coder/c1", "one", 1, "c1", &mut ws1)
        .await
        .unwrap();
    let row = fx
        .swarm
        .submit("coder/c2", "two", 1, "c2", &mut ws2)
        .await
        .unwrap();
    assert_eq!(row.verdict, Verdict::Conflict.as_str());
    assert_eq!(row.detail, "a");
    let item = fx.swarm.item("two").await.unwrap().unwrap();
    assert_eq!(item.status, "claimed", "a conflict keeps the claim");
}

async fn gate_logs_a_judge_failure(store: &Store) {
    let fx = fixture(store, SwarmPolicy::default(), &[("a", "plain")]).await;
    add(&fx, "one").await;
    let mut ws = Workspace::default();
    claimed(fx.swarm.claim("coder/c1", "one", &mut ws).await.unwrap());
    ws.write("boom", "good").unwrap();
    let row = fx
        .swarm
        .submit("coder/c1", "one", 1, "j1", &mut ws)
        .await
        .unwrap();
    assert_eq!(row.verdict, Verdict::JudgeFailed.as_str());
    assert_eq!(row.detail, "the eval broke");
    assert_eq!(fx.swarm.main().await.unwrap().unwrap().version, 1);
}

async fn gate_closed_items_refuse(store: &Store) {
    let fx = fixture(store, SwarmPolicy::default(), &[("a", "plain")]).await;
    add(&fx, "one").await;
    let dropped = fx.swarm.drop_item("one").await.unwrap();
    assert_eq!(dropped.status, "dropped");
    let mut ws = Workspace::default();
    let ClaimOutcome::Refused { refusal, .. } =
        fx.swarm.claim("coder/c1", "one", &mut ws).await.unwrap()
    else {
        panic!("a dropped item refuses a claim");
    };
    assert_eq!(refusal, ClaimRefusal::Closed);
    assert!(
        fx.swarm
            .claim("coder/c1", "missing", &mut ws)
            .await
            .is_err()
    );
    assert!(fx.swarm.claim("bad\nmember", "one", &mut ws).await.is_err());
}

async fn gate_prune_keeps_live_bases(store: &Store) {
    use autumn_harvest_agent::WorkspaceBlobs as _;

    let fx = fixture(store, SwarmPolicy::default(), &[("a", "plain")]).await;
    let scope = fx.swarm.id().main_scope();
    for id in ["one", "two", "three"] {
        add(&fx, id).await;
    }
    let (mut ws1, mut ws2, mut ws3) = (
        Workspace::default(),
        Workspace::default(),
        Workspace::default(),
    );
    claimed(fx.swarm.claim("coder/c3", "three", &mut ws3).await.unwrap());
    claimed(fx.swarm.claim("coder/c1", "one", &mut ws1).await.unwrap());
    ws1.write("a", "good").unwrap();
    fx.swarm
        .submit("coder/c1", "one", 1, "p1", &mut ws1)
        .await
        .unwrap();
    claimed(fx.swarm.claim("coder/c2", "two", &mut ws2).await.unwrap());
    ws2.write("b", "good").unwrap();
    fx.swarm
        .submit("coder/c2", "two", 1, "p2", &mut ws2)
        .await
        .unwrap();
    assert_eq!(fx.swarm.main().await.unwrap().unwrap().version, 3);
    assert_eq!(fx.blobs.list(&scope).await.unwrap().len(), 3);
    assert_eq!(
        fx.swarm.prune().await.unwrap(),
        1,
        "only version 2 is unused"
    );
    let keys = fx.blobs.list(&scope).await.unwrap();
    assert!(
        keys.iter()
            .any(|key| key.starts_with(&format!("{:016x}", 1)))
    );
    assert!(
        keys.iter()
            .any(|key| key.starts_with(&format!("{:016x}", 3)))
    );
    ws3.write("c", "good").unwrap();
    let row = fx
        .swarm
        .submit("coder/c3", "three", 1, "p3", &mut ws3)
        .await
        .unwrap();
    assert_eq!(
        row.verdict,
        Verdict::Merged.as_str(),
        "the base of a live claim stays"
    );
}

async fn gate_posts_read_after_a_cursor(store: &Store) {
    let fx = fixture(store, SwarmPolicy::default(), &[("a", "plain")]).await;
    let first = fx
        .swarm
        .post("coder/c1", "general", "taking one", "k1")
        .await
        .unwrap();
    fx.swarm
        .post("coder/c2", "quality", "found a bug", "k2")
        .await
        .unwrap();
    let again = fx
        .swarm
        .post("coder/c1", "general", "taking one", "k1")
        .await
        .unwrap();
    assert_eq!(again, first);
    let after = fx.swarm.posts(None, first.seq, 10).await.unwrap();
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].author, "coder/c2");
    assert!(
        fx.swarm
            .post("coder/c1", "Bad Channel", "x", "k3")
            .await
            .is_err()
    );
    assert!(
        fx.swarm
            .post("coder/c1", "general", &"x".repeat(9_000), "k4")
            .await
            .is_err()
    );
}
