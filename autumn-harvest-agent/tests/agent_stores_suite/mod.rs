//! One suite for the durable episode, memory and transcript stores.
//!
//! `agent_stores.rs` runs it on `SQLite`. `agent_stores_postgres.rs` runs
//! it on Postgres. Each call of the factory opens new handles on the same
//! database, as a new process does. Each case uses its own scope names, so
//! the cases can share one database.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use autumn_harvest_agent::episode::{Appended, Episode, EpisodeStore, PassMark};
use autumn_harvest_agent::memory::MemoryOp;
use autumn_harvest_agent::transcript::WindowOwner;
use autumn_harvest_agent::{
    AgentStop, ChatMessage, ChatRole, EditKey, ErrorKind, MemoryBlock, MemoryScope, MemoryStore,
    SessionId, TranscriptStore, transcript,
};

/// The three stores on one database.
pub struct Stores {
    /// The episode store.
    pub episodes: Arc<dyn EpisodeStore>,
    /// The memory store.
    pub memory: Arc<dyn MemoryStore>,
    /// The transcript store.
    pub transcripts: Arc<dyn TranscriptStore>,
}

/// Opens new handles on the database of the suite.
pub type MakeStores<'a> = &'a (dyn Fn() -> Stores + Sync);

static NEXT: AtomicU64 = AtomicU64::new(0);

/// A name that no other case of this process uses. The time keeps two
/// runs on a reused database apart.
pub fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |at| at.as_nanos());
    format!("{prefix}-{nanos}-{}", NEXT.fetch_add(1, Ordering::Relaxed))
}

/// An episode with the id `id`.
pub fn episode(id: &str) -> Episode {
    Episode {
        id: id.to_owned(),
        text: format!("text of {id}"),
        stop: AgentStop::Completed,
        at_ms: 1,
        labels: BTreeSet::new(),
    }
}

/// A window of `n` user messages that start with `tag`.
pub fn window(tag: &str, n: usize) -> Vec<ChatMessage> {
    (0..n)
        .map(|i| ChatMessage::text(ChatRole::User, format!("{tag} {i}")))
        .collect()
}

/// Run every case.
pub async fn run_all(make: MakeStores<'_>) {
    an_append_lands_once_across_a_reopen(make).await;
    concurrent_appends_all_land(make).await;
    a_clear_keeps_its_generation_and_its_first_counts(make).await;
    a_mark_of_an_old_generation_changes_nothing(make).await;
    a_memory_edit_applies_once_across_a_reopen(make).await;
    a_failed_memory_edit_keeps_its_error(make).await;
    a_labelled_memory_edit_keeps_its_labels(make).await;
    ensure_block_adds_a_block_once(make).await;
    a_memory_clear_removes_the_scope(make).await;
    a_window_round_trips(make).await;
    an_erase_removes_only_the_windows_of_its_session(make).await;
    a_put_keeps_only_the_newest_windows_of_a_run(make).await;
    a_window_with_text_that_holds_a_nul_round_trips(make).await;
}

/// A retried append after a crash finds its key in the database.
pub async fn an_append_lands_once_across_a_reopen(make: MakeStores<'_>) {
    let scope = MemoryScope::new(unique("agent:suite/a:episodes"));
    let key = EditKey::new("episode:s1");
    let first = make()
        .episodes
        .append(&scope, &key, episode("s1"), 10)
        .await
        .unwrap();
    let retry = make()
        .episodes
        .append(&scope, &key, episode("s1"), 10)
        .await
        .unwrap();
    assert_eq!(
        first,
        Appended {
            pending: 1,
            evicted: 0
        }
    );
    assert_eq!(retry, first, "the retry reports the same pending count");
    let log = make().episodes.load(&scope).await.unwrap();
    assert_eq!(log.len(), 1, "one key, one episode");
}

/// Two handles that write one scope at once lose no write.
pub async fn concurrent_appends_all_land(make: MakeStores<'_>) {
    let scope = MemoryScope::new(unique("agent:suite/b:episodes"));
    let mut tasks = Vec::new();
    for n in 0..8 {
        let store = make().episodes;
        let scope = scope.clone();
        tasks.push(tokio::spawn(async move {
            let id = format!("e{n}");
            store
                .append(&scope, &EditKey::new(&id), episode(&id), 100)
                .await
                .unwrap()
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    let log = make().episodes.load(&scope).await.unwrap();
    assert_eq!(log.len(), 8, "each concurrent append landed");
}

/// A retried clear reports its first counts, and a reopen keeps the
/// generation.
pub async fn a_clear_keeps_its_generation_and_its_first_counts(make: MakeStores<'_>) {
    let scope = MemoryScope::new(unique("agent:suite/c:episodes"));
    let stores = make();
    for id in ["e1", "e2"] {
        stores
            .episodes
            .append(&scope, &EditKey::new(id), episode(id), 10)
            .await
            .unwrap();
    }
    let first = stores
        .episodes
        .clear(&scope, &EditKey::new("r1"))
        .await
        .unwrap();
    let retry = make()
        .episodes
        .clear(&scope, &EditKey::new("r1"))
        .await
        .unwrap();
    assert_eq!(first.episodes, 2);
    assert_eq!(retry, first, "a retried clear reports the same counts");
    let log = make().episodes.load(&scope).await.unwrap();
    assert!(log.is_empty());
    assert_eq!(log.generation(), 1, "the generation survives a reopen");
}

/// A pass that read the log before a clear marks nothing after it.
pub async fn a_mark_of_an_old_generation_changes_nothing(make: MakeStores<'_>) {
    let scope = MemoryScope::new(unique("agent:suite/d:episodes"));
    let stores = make();
    stores
        .episodes
        .append(&scope, &EditKey::new("e1"), episode("e1"), 10)
        .await
        .unwrap();
    stores
        .episodes
        .clear(&scope, &EditKey::new("r1"))
        .await
        .unwrap();
    stores
        .episodes
        .append(&scope, &EditKey::new("e2"), episode("e2"), 10)
        .await
        .unwrap();
    let stale = PassMark {
        key: EditKey::new("p1"),
        generation: 0,
        episodes: vec!["e2".into()],
        ..PassMark::default()
    };
    assert!(!make().episodes.mark(&scope, stale).await.unwrap());
    let fresh = PassMark {
        key: EditKey::new("p2"),
        generation: 1,
        episodes: vec!["e2".into()],
        facts: vec!["a b c [episodes: e2]".into()],
        ..PassMark::default()
    };
    assert!(make().episodes.mark(&scope, fresh).await.unwrap());
    let log = make().episodes.load(&scope).await.unwrap();
    assert_eq!(log.pending(10).len(), 0, "the fresh pass marked e2");
    assert_eq!(log.derived().count(), 1);
}

fn add(text: &str) -> MemoryOp {
    MemoryOp::Add {
        block: "memory".into(),
        text: text.into(),
    }
}

/// A retried memory edit changes nothing, even after another edit.
pub async fn a_memory_edit_applies_once_across_a_reopen(make: MakeStores<'_>) {
    let scope = MemoryScope::new(unique("agent:suite/e"));
    let key = EditKey::new("run:1:c1");
    let remove = MemoryOp::Remove {
        block: "memory".into(),
        old: "tea".into(),
    };
    make()
        .memory
        .apply(&scope, add("likes tea"), &key)
        .await
        .unwrap();
    make()
        .memory
        .apply(&scope, remove.clone(), &EditKey::new("run:1:c2"))
        .await
        .unwrap();
    // The first call comes back after a crash. It must not add the entry
    // that the second call removed.
    make()
        .memory
        .apply(&scope, add("likes tea"), &key)
        .await
        .unwrap();
    let blocks = make().memory.load(&scope).await.unwrap();
    let memory = blocks.iter().find(|block| block.label == "memory").unwrap();
    assert!(memory.entries.is_empty(), "{:?}", memory.entries);
    assert_eq!(
        blocks.len(),
        MemoryBlock::defaults().len(),
        "a new scope starts with the default blocks"
    );
}

/// A failed edit keeps its error, so a retry fails the same way.
pub async fn a_failed_memory_edit_keeps_its_error(make: MakeStores<'_>) {
    let scope = MemoryScope::new(unique("agent:suite/f"));
    let key = EditKey::new("run:1:c1");
    let remove = MemoryOp::Remove {
        block: "memory".into(),
        old: "tea".into(),
    };
    let first = make()
        .memory
        .apply(&scope, remove.clone(), &key)
        .await
        .unwrap_err();
    make()
        .memory
        .apply(&scope, add("likes tea"), &EditKey::new("run:1:c2"))
        .await
        .unwrap();
    let retry = make().memory.apply(&scope, remove, &key).await.unwrap_err();
    assert_eq!(retry, first, "the retry returns the stored error");
    assert_eq!(retry.kind(), ErrorKind::Tool);
    let blocks = make().memory.load(&scope).await.unwrap();
    let memory = blocks.iter().find(|block| block.label == "memory").unwrap();
    assert_eq!(memory.entries, ["likes tea"], "the retry removed nothing");
}

/// A write from a labelled run keeps its labels across a reopen.
pub async fn a_labelled_memory_edit_keeps_its_labels(make: MakeStores<'_>) {
    let scope = MemoryScope::new(unique("agent:suite/g"));
    let labels: BTreeSet<String> = ["untrusted_input".to_owned()].into();
    make()
        .memory
        .apply_labelled(&scope, add("from a web page"), &EditKey::new("k"), &labels)
        .await
        .unwrap();
    let blocks = make().memory.load(&scope).await.unwrap();
    let memory = blocks.iter().find(|block| block.label == "memory").unwrap();
    assert_eq!(memory.labels.get("from a web page"), Some(&labels));
}

/// `ensure_block` on a new scope gives only that block, once.
pub async fn ensure_block_adds_a_block_once(make: MakeStores<'_>) {
    let scope = MemoryScope::new(unique("agent:suite/h"));
    let block = MemoryBlock::new("lessons", "Lessons of the kind.", 500);
    make()
        .memory
        .ensure_block(&scope, block.clone())
        .await
        .unwrap();
    make()
        .memory
        .ensure_block(&scope, MemoryBlock::new("lessons", "Other.", 1))
        .await
        .unwrap();
    let blocks = make().memory.load(&scope).await.unwrap();
    assert_eq!(blocks, [block], "the held block keeps its description");
}

/// A clear removes the entries and the keys of a scope.
pub async fn a_memory_clear_removes_the_scope(make: MakeStores<'_>) {
    let scope = MemoryScope::new(unique("agent:suite/i"));
    let key = EditKey::new("run:1:c1");
    make()
        .memory
        .apply(&scope, add("likes tea"), &key)
        .await
        .unwrap();
    make().memory.clear(&scope).await.unwrap();
    let blocks = make().memory.load(&scope).await.unwrap();
    assert_eq!(blocks, MemoryBlock::defaults());
    // The clear removed the key, so the same key applies again.
    make()
        .memory
        .apply(&scope, add("likes tea"), &key)
        .await
        .unwrap();
    let blocks = make().memory.load(&scope).await.unwrap();
    let memory = blocks.iter().find(|block| block.label == "memory").unwrap();
    assert_eq!(memory.entries, ["likes tea"]);
}

/// A kept window comes back from a new handle.
pub async fn a_window_round_trips(make: MakeStores<'_>) {
    let messages = window(&unique("round trip"), 3);
    let digest = transcript::chain(None, &messages);
    let session = SessionId::new(unique("suite/j"));
    make()
        .transcripts
        .put(
            WindowOwner::new("run-j", Some(&session)),
            &digest,
            &messages,
        )
        .await;
    assert_eq!(make().transcripts.get(&digest).await, Some(messages));
    assert_eq!(make().transcripts.get("no such digest").await, None);
}

/// An erase deletes the windows of one session, counts them, and leaves
/// the windows of another session.
pub async fn an_erase_removes_only_the_windows_of_its_session(make: MakeStores<'_>) {
    let gone = SessionId::new(unique("suite/k"));
    let kept = SessionId::new(unique("suite/l"));
    let shared = window(&unique("shared"), 2);
    let shared_digest = transcript::chain(None, &shared);
    let own = window(&unique("own"), 2);
    let own_digest = transcript::chain(None, &own);
    let stores = make();
    for (session, digest, messages) in [
        (&gone, &own_digest, &own),
        (&gone, &shared_digest, &shared),
        (&kept, &shared_digest, &shared),
    ] {
        stores
            .transcripts
            .put(WindowOwner::new("run-k", Some(session)), digest, messages)
            .await;
    }
    let erased = make().transcripts.erase(&gone).await.unwrap();
    assert_eq!(erased, Some(2), "two windows of the session");
    assert_eq!(make().transcripts.get(&own_digest).await, None);
    assert_eq!(
        make().transcripts.get(&shared_digest).await,
        Some(shared),
        "the other session still holds its window"
    );
    assert_eq!(make().transcripts.erase(&gone).await.unwrap(), Some(0));
}

/// A run keeps only its newest windows. A later turn names the last one.
pub async fn a_put_keeps_only_the_newest_windows_of_a_run(make: MakeStores<'_>) {
    let session = SessionId::new(unique("suite/m"));
    let tag = unique("growing");
    let all = window(&tag, 12);
    let stores = make();
    let mut digests = Vec::new();
    for len in 1..=all.len() {
        let messages = &all[..len];
        let digest = transcript::chain(None, messages);
        stores
            .transcripts
            .put(WindowOwner::new("run-m", Some(&session)), &digest, messages)
            .await;
        digests.push(digest);
    }
    let newest = digests.last().unwrap();
    assert_eq!(
        stores.transcripts.get(newest).await.as_deref(),
        Some(&all[..])
    );
    assert_eq!(
        stores.transcripts.get(&digests[0]).await,
        None,
        "the oldest window of the run is gone"
    );
    let held = make().transcripts.erase(&session).await.unwrap().unwrap();
    assert!(
        (1..=u64::from(transcript::DEFAULT_KEEP_PER_RUN)).contains(&held),
        "the run kept {held} windows"
    );
}

/// A NUL in model text is safe in a body column.
pub async fn a_window_with_text_that_holds_a_nul_round_trips(make: MakeStores<'_>) {
    let messages = vec![ChatMessage::text(
        ChatRole::Assistant,
        format!("{}\u{0}after the nul", unique("nul")),
    )];
    let digest = transcript::chain(None, &messages);
    make()
        .transcripts
        .put(WindowOwner::new("run-n", None), &digest, &messages)
        .await;
    assert_eq!(make().transcripts.get(&digest).await, Some(messages));
    let scope = MemoryScope::new(unique("agent:suite/n:episodes"));
    let mut nul = episode("n1");
    nul.text = "before\u{0}after".into();
    make()
        .episodes
        .append(&scope, &EditKey::new("n1"), nul.clone(), 10)
        .await
        .unwrap();
    let log = make().episodes.load(&scope).await.unwrap();
    assert_eq!(log.episodes().next(), Some(&nul));
}
