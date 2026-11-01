//! Live text of model turns (issue #2268).
//!
//! A model that streams sends its text while it writes. The harness passes
//! it to a live sink at once, before the turn completes. A retry starts
//! again with a `start` event. With `stream`, the workflow also publishes the
//! whole text of each finished turn to the durable stream, once.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use autumn_harvest::activity_progress::{ACTIVITY_PROGRESS_KEY, CaptureProgressSink};
use autumn_harvest::context::{ActivityContext, SharedStateMap};
use autumn_harvest::testing::WorkflowTestEnv;
use autumn_harvest_agent::workflow::{agent_loop_info, agent_model_turn};
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentModel, AgentTask, BoxFuture, ChatMessage, ChatRequest,
    ChatResponse, ChatRole, ContentPart, ErrorKind, LiveEvent, LiveSink, ModelTurn,
    ModelTurnRequest, StopReason, TokenUsage, TurnText, sqlite,
};
use autumn_harvest_sqlite::SqliteReader;
use common::{report, runtime};
use serde_json::json;

fn fresh_db() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("agent.db");
    (dir, path)
}

/// One scripted answer: its words, and whether it fails after them.
#[derive(Debug, Clone)]
struct Answer {
    words: Vec<&'static str>,
    fail_after: bool,
}

/// A model that streams each answer word by word.
#[derive(Debug)]
struct StreamingModel {
    answers: Mutex<VecDeque<Answer>>,
    calls: AtomicUsize,
}

impl StreamingModel {
    fn new(answers: Vec<Answer>) -> Arc<Self> {
        Arc::new(Self {
            answers: Mutex::new(answers.into()),
            calls: AtomicUsize::new(0),
        })
    }
}

impl AgentModel for StreamingModel {
    fn chat<'a>(
        &'a self,
        request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        // A turn with no reader asks for the whole answer at once.
        self.chat_stream(request, &|_| {})
    }

    fn chat_stream<'a>(
        &'a self,
        _request: &'a ChatRequest,
        on_delta: &'a (dyn Fn(&str) + Send + Sync),
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let answer = self.answers.lock().unwrap().pop_front().expect("an answer");
        Box::pin(async move {
            for word in &answer.words {
                on_delta(word);
                // Each word arrives later, as from a provider.
                tokio::time::sleep(std::time::Duration::from_millis(60)).await;
            }
            if answer.fail_after {
                return Err(AgentError::new(ErrorKind::Transport, "the stream broke"));
            }
            Ok(ChatResponse {
                content: vec![ContentPart::Text(answer.words.concat())],
                stop_reason: StopReason::EndTurn,
                usage: TokenUsage::new(1, 1),
            })
        })
    }
}

fn words(words: &[&'static str]) -> Answer {
    Answer {
        words: words.to_vec(),
        fail_after: false,
    }
}

/// A sink that keeps each event, and checks the history at the first delta.
#[derive(Debug)]
struct Watcher {
    db: PathBuf,
    events: Mutex<Vec<LiveEvent>>,
    turns_done_at_first_delta: Mutex<Option<usize>>,
}

impl Watcher {
    fn new(db: &std::path::Path) -> Arc<Self> {
        Arc::new(Self {
            db: db.to_owned(),
            events: Mutex::new(Vec::new()),
            turns_done_at_first_delta: Mutex::new(None),
        })
    }

    /// The text that a reader shows: each `start` drops the text before it.
    fn shown(&self) -> String {
        let mut shown = String::new();
        for event in self.events.lock().unwrap().iter() {
            match event {
                LiveEvent::Start { .. } => shown.clear(),
                LiveEvent::Delta { text, .. } => shown.push_str(text),
            }
        }
        shown
    }
}

impl LiveSink for Watcher {
    fn send(&self, event: &LiveEvent) {
        {
            let mut first = self.turns_done_at_first_delta.lock().unwrap();
            if matches!(event, LiveEvent::Delta { .. }) && first.is_none() {
                *first = Some(ledger_rows(&self.db));
            }
        }
        self.events.lock().unwrap().push(event.clone());
    }
}

/// The ledger rows of the file, read through a read-only handle. A turn
/// writes its row with its completion event, so the count is the number of
/// completed turns.
fn ledger_rows(db: &std::path::Path) -> usize {
    let now = autumn_harvest::chrono::Utc::now();
    let day = autumn_harvest::chrono::Duration::days(1);
    SqliteReader::open(db)
        .unwrap()
        .llm_ledger_between(now - day, now + day)
        .unwrap()
        .len()
}

#[tokio::test(flavor = "multi_thread")]
async fn tokens_arrive_before_the_turn_completes() {
    let (_dir, db) = fresh_db();
    let model = StreamingModel::new(vec![words(&["The ", "answer ", "is ", "42."])]);
    let watcher = Watcher::new(&db);
    let harness = AgentHarness::new(model).live_sink(watcher.clone());
    let mut rt = runtime(&db, harness);
    let exec = sqlite::start(&mut rt, &AgentTask::new("what is it?")).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());

    assert_eq!(report.text, "The answer is 42.");
    assert_eq!(
        *watcher.turns_done_at_first_delta.lock().unwrap(),
        Some(0),
        "the first delta came before the turn completed"
    );
    assert_eq!(ledger_rows(&db), 1, "the turn completed after it");
    assert_eq!(watcher.shown(), "The answer is 42.");
    let events = watcher.events.lock().unwrap().clone();
    assert!(matches!(events[0], LiveEvent::Start { .. }));
    assert!(events.len() > 2, "more than one delta: {events:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_retry_starts_the_live_text_again() {
    let (_dir, db) = fresh_db();
    let model = StreamingModel::new(vec![
        Answer {
            words: vec!["half an ", "answer"],
            fail_after: true,
        },
        words(&["the ", "whole ", "answer"]),
    ]);
    let watcher = Watcher::new(&db);
    let harness = AgentHarness::new(model.clone()).live_sink(watcher.clone());
    let mut rt = runtime(&db, harness);
    let exec = sqlite::start(&mut rt, &AgentTask::new("go")).unwrap();
    // The retry waits on a durable backoff timer, so the drive moves past it.
    let mut state = rt.run_until_blocked(exec).await.unwrap();
    while matches!(state, autumn_harvest_sqlite::RunState::WaitingTimer) {
        let later =
            autumn_harvest::chrono::Utc::now() + autumn_harvest::chrono::Duration::minutes(5);
        state = rt.run_until_blocked_as_of(exec, later).await.unwrap();
    }
    let report = report(state);

    assert_eq!(report.text, "the whole answer");
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
    let starts = watcher
        .events
        .lock()
        .unwrap()
        .iter()
        .filter(|event| matches!(event, LiveEvent::Start { .. }))
        .count();
    assert_eq!(starts, 2, "each attempt starts again");
    assert_eq!(
        watcher.shown(),
        "the whole answer",
        "the broken half is dropped"
    );
}

fn request(stream: bool) -> ModelTurnRequest {
    ModelTurnRequest {
        skills: false,
        tool_guidance: std::collections::BTreeMap::new(),
        run_id: "run-1".into(),
        session_id: None,
        steps_used: 0,
        max_steps: 8,
        usage: TokenUsage::default(),
        messages: vec![ChatMessage::text(ChatRole::User, "go")],
        max_output_tokens: None,
        read_only: false,
        memory_scope: None,
        extra_tools: Vec::new(),
        tenant: None,
        agent: None,
        allowed_tools: None,
        flow_labels: std::collections::BTreeSet::new(),
        flow_released: autumn_harvest_agent::Released::new(),
        interruptible: false,
        mesh: None,
        stream,
        prefix: None,
        remember: false,
        model: None,
    }
}

fn engine_ctx(
    model: Arc<StreamingModel>,
    attempt: u32,
    sink: Arc<CaptureProgressSink>,
) -> ActivityContext {
    let mut state: SharedStateMap = std::collections::HashMap::new();
    state.insert(
        std::any::TypeId::of::<AgentHarness>(),
        Box::new(AgentHarness::new(model)),
    );
    ActivityContext::new_test_with_state(Arc::new(state))
        .with_attempt(attempt)
        .with_progress_sink(sink)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_retried_attempt_tags_its_live_text_with_the_new_attempt() {
    // The engine path: the model-turn activity publishes through the
    // activity context, which tags each chunk with its attempt.
    let sink = Arc::new(CaptureProgressSink::default());
    let model = StreamingModel::new(vec![words(&["one ", "two"])]);
    let ctx = engine_ctx(model, 3, sink.clone());
    agent_model_turn(&ctx, request(true)).await.unwrap();

    let chunks = sink.chunks();
    assert!(chunks.len() >= 2);
    assert!(chunks.iter().all(|chunk| chunk.attempt == 3));
    let indexes: Vec<u64> = chunks.iter().map(|chunk| chunk.index).collect();
    assert_eq!(indexes, (0..chunks.len() as u64).collect::<Vec<_>>());
    let first: LiveEvent = serde_json::from_value(chunks[0].chunk.clone()).unwrap();
    assert!(matches!(first, LiveEvent::Start { .. }));
    assert!(chunks[0].wire_chunk().get(ACTIVITY_PROGRESS_KEY).is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_turn_that_does_not_stream_publishes_nothing() {
    let sink = Arc::new(CaptureProgressSink::default());
    let model = StreamingModel::new(vec![words(&["quiet"])]);
    let ctx = engine_ctx(model, 1, sink.clone());
    agent_model_turn(&ctx, request(false)).await.unwrap();
    assert_eq!(
        sink.chunks().len(),
        0,
        "a turn that does not stream sends nothing"
    );
}

/// Live text that only a broken attempt streams. No finished turn holds it.
const LOST_TEXT: &str = "lost-draft-of-a-broken-stream";

#[tokio::test(flavor = "multi_thread")]
async fn live_text_never_reaches_history() {
    // The same run with and without a live sink records the same events.
    // The first attempt streams a marker and breaks, so only the live sink
    // can see the marker.
    let mut kinds = Vec::new();
    for live in [false, true] {
        let (_dir, db) = fresh_db();
        let model = StreamingModel::new(vec![
            Answer {
                words: vec![LOST_TEXT, " and more"],
                fail_after: true,
            },
            words(&["a ", "b"]),
        ]);
        let watcher = Watcher::new(&db);
        let mut harness = AgentHarness::new(model);
        if live {
            harness = harness.live_sink(watcher.clone());
        }
        let mut rt = runtime(&db, harness);
        let exec = sqlite::start(&mut rt, &AgentTask::new("go").stream()).unwrap();
        // The retry waits on a durable backoff timer, so the drive moves past it.
        let mut state = rt.run_until_blocked(exec).await.unwrap();
        while matches!(state, autumn_harvest_sqlite::RunState::WaitingTimer) {
            let later =
                autumn_harvest::chrono::Utc::now() + autumn_harvest::chrono::Duration::minutes(5);
            state = rt.run_until_blocked_as_of(exec, later).await.unwrap();
        }
        assert_eq!(report(state).text, "a b");

        let history = rt.load_history(exec).unwrap();
        let text = serde_json::to_string(&history).unwrap();
        assert!(
            !text.contains(LOST_TEXT),
            "live text reached history (live sink: {live})"
        );
        let seen: String = watcher
            .events
            .lock()
            .unwrap()
            .iter()
            .filter_map(|event| match event {
                LiveEvent::Delta { text, .. } => Some(text.as_str()),
                LiveEvent::Start { .. } => None,
            })
            .collect();
        assert_eq!(seen.contains(LOST_TEXT), live, "the sink saw {seen:?}");
        // `WorkflowEvent` is tagged by `type`, with its fields under `data`.
        let types: Vec<String> = history
            .iter()
            .map(|event| {
                serde_json::to_value(event).unwrap()["type"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned()
            })
            .collect();
        assert!(types.iter().all(|kind| !kind.is_empty()), "{types:?}");
        kinds.push(types);
    }
    assert_eq!(kinds[0], kinds[1]);
}

// ---------------------------------------------------------------------------
// The durable text of each finished turn
// ---------------------------------------------------------------------------

fn turn(text: &str, call: bool) -> ModelTurn {
    let mut content = vec![ContentPart::Text(text.to_owned())];
    let mut decisions = Vec::new();
    if call {
        content.push(ContentPart::ToolCall {
            id: "c1".into(),
            name: "lookup".into(),
            arguments: json!({}),
        });
        decisions.push(autumn_harvest_agent::ToolDecision::Allow);
    }
    ModelTurn {
        content,
        stop: if call {
            StopReason::ToolUse
        } else {
            StopReason::EndTurn
        },
        usage: TokenUsage::new(1, 1),
        decisions,
        cache_hit: false,
        labels: Vec::new(),
        releases: Vec::new(),
        oversized: false,
        interruptible: Vec::new(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reconnect_after_the_turn_gets_the_durable_text_once() {
    let env = WorkflowTestEnv::new()
        .mock_activity("agent_model_turn", |input| {
            let request: ModelTurnRequest = serde_json::from_value(input).unwrap();
            let answered = request.messages.iter().any(|m| m.role == ChatRole::Tool);
            let turn = if answered {
                turn("all done", false)
            } else {
                turn("let me look", true)
            };
            Ok(serde_json::to_value(turn).unwrap())
        })
        .mock_activity("agent_tool_call", |_| {
            Ok(serde_json::to_value(autumn_harvest_agent::ToolOutcome::ok("found")).unwrap())
        });
    let outcome = env
        .run(
            agent_loop_info().handler,
            json!(AgentTask::new("look").stream()),
        )
        .await;
    assert!(outcome.result.is_ok(), "{:?}", outcome.result);

    let texts: Vec<TurnText> = outcome
        .recorded_durable_progress()
        .iter()
        .map(|chunk| serde_json::from_value(chunk.chunk.clone()).unwrap())
        .collect();
    let shown: Vec<&str> = texts.iter().map(|text| text.text.as_str()).collect();
    assert_eq!(
        shown,
        ["let me look", "all done"],
        "each turn once, in order"
    );
    let offsets: Vec<u64> = outcome
        .recorded_durable_progress()
        .iter()
        .map(|chunk| chunk.offset)
        .collect();
    assert_eq!(offsets, [0, 1]);
}
