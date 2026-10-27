//! Deterministic instruction-count profile of `AgentHarness::model_turn`,
//! the activity side of one model turn (issues #1973, #2264).
//!
//! Wall-clock timing is not admissible on this machine. Drive the binary under
//! `valgrind --tool=callgrind` or `valgrind --tool=dhat`.
//!
//! # Workload
//!
//! One run has `AGENT_MODEL_TURN_PROFILE_N` model turns over one growing
//! conversation. The window starts with a system prompt and a task of 1,024
//! bytes each. A canned model answers each turn with one `lookup` call. The
//! profile then adds the assistant message and a tool result of 1,024 bytes,
//! as `agent_loop` does. So each turn adds two messages to the window.
//!
//! `AGENT_MODEL_TURN_PROFILE_SHAPE` picks the request form:
//!
//! - `delta` (default): the form that `send_turn` sends for a task with
//!   `transcript_delta`. The first turn sends the whole window. Each later
//!   turn sends a `PrefixRef` of the previous window and the added messages.
//!   Every turn sets `remember`.
//! - `full`: the form of a task without `transcript_delta`. Each turn sends
//!   the whole window, with no prefix and no `remember`.
//! - `cache`: as `delta`, with a `ResponseCache` and a model that reports a
//!   model id. Each turn misses the cache, then stores its answer.
//!
//! The profile builds each request before it calls `model_turn`. It also
//! computes the `Sent` digests of the workflow outside `model_turn`. So the
//! inclusive cost of `model_turn` leaves out the setup.
//! `AGENT_MODEL_TURN_PROFILE_REPS` (default 1) repeats the run with a fresh
//! harness.
//!
//! # Nondeterminism
//!
//! `model_turn_in` reads `Utc::now`, and the turn body reads `Instant::now`.
//! `decide_calls` and each `tokio::time::timeout` read the tokio clock. The
//! response cache stamps each entry with the system clock. The transcript
//! store and the cache index are `HashMap`s with a random seed. No clock
//! value or hash seed changes the work, so two runs give totals within 0.1%.
//! All other inputs are fixed.
//!
//! # Running
//!
//! ```text
//! BIN=$(cargo bench -p autumn-harvest-agent --bench agent_model_turn_profile \
//!   --no-run --message-format=json 2>/dev/null \
//!   | jq -r 'select(.reason=="compiler-artifact" and .target.name=="agent_model_turn_profile") | .executable')
//! AGENT_MODEL_TURN_PROFILE_N=200 valgrind --tool=callgrind --branch-sim=no \
//!   --cache-sim=no --callgrind-out-file=cg.out "$BIN"
//! AGENT_MODEL_TURN_PROFILE_N=200 valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError};

use autumn_harvest::payload_codec::PayloadCodecs;
use autumn_harvest::payload_store::{PayloadStore, PayloadStoreError, PayloadStoreFuture};
use autumn_harvest_agent::bounds::json_len;
use autumn_harvest_agent::transcript::Sent;
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentModel, BoxFuture, ChatMessage, ChatRequest, ChatResponse,
    ChatRole, ContentPart, FnTool, InMemoryCacheIndex, ModelTurn, ModelTurnRequest, PrefixRef,
    Released, ResponseCache, StopReason, TokenUsage, ToolDecision, ToolEffect,
};
use serde_json::{Value, json};

/// The messages before the first turn: the system prompt and the task.
const OPENING: usize = 2;

/// The size of the system prompt, the task and each tool result, in bytes.
const TEXT_BYTES: usize = 1_024;

/// The one tool of the harness. The canned model calls it on every turn.
const TOOL: &str = "lookup";

/// The model id of the `cache` shape. The other shapes have no model id.
const MODEL_ID: &str = "profile-model-2026-10";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shape {
    Delta,
    Full,
    Cache,
}

impl Shape {
    fn from_env() -> Self {
        let Ok(value) = std::env::var("AGENT_MODEL_TURN_PROFILE_SHAPE") else {
            return Self::Delta;
        };
        match value.as_str() {
            "delta" => Self::Delta,
            "full" => Self::Full,
            "cache" => Self::Cache,
            other => {
                panic!("AGENT_MODEL_TURN_PROFILE_SHAPE must be delta, full or cache, got {other:?}")
            }
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Delta => "delta",
            Self::Full => "full",
            Self::Cache => "cache",
        }
    }
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key).map_or(default, |value| {
        value
            .parse()
            .unwrap_or_else(|_| panic!("{key} must be an unsigned integer, got {value:?}"))
    })
}

/// Deterministic filler text of exactly `bytes` bytes, after `head`.
///
/// It uses the word list and the generator of `words` in
/// `tests/history_delta_measure.rs`.
fn filler(seed: u64, head: &str, bytes: usize) -> String {
    const WORDS: [&str; 16] = [
        "harvest", "cohort", "replay", "ledger", "tenant", "signal", "worker", "fence", "payload",
        "codec", "shard", "commit", "event", "history", "queue", "timer",
    ];
    let mut state = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
    let mut out = String::with_capacity(bytes + 16);
    out.push_str(head);
    while out.len() < bytes {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let index = usize::try_from((state >> 33) % 16).expect("an index below 16 fits in usize");
        out.push(' ');
        out.push_str(WORDS[index]);
    }
    // Every word is ASCII, so any byte offset is a character boundary.
    out.truncate(bytes);
    out
}

/// The provider id of the call that the model makes on turn `page`.
fn call_id(page: usize) -> String {
    format!("call-{page:05}")
}

/// A model that answers each turn with one `lookup` call.
///
/// The page number follows from the window length, so the model keeps no
/// state. It reads no message and clones no part of the request.
#[derive(Debug)]
struct CannedModel {
    id: Option<&'static str>,
}

impl AgentModel for CannedModel {
    fn chat<'a>(
        &'a self,
        request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        let page = request.messages.len().saturating_sub(OPENING) / 2;
        Box::pin(std::future::ready(Ok(ChatResponse {
            content: vec![ContentPart::ToolCall {
                id: call_id(page),
                name: TOOL.to_owned(),
                arguments: json!({ "page": page }),
            }],
            stop_reason: StopReason::ToolUse,
            usage: TokenUsage::new(1_000, 20),
        })))
    }

    fn model_id(&self) -> Option<&str> {
        self.id
    }
}

/// The blobs of [`MapStore`], with the next key number.
#[derive(Debug, Default)]
struct Blobs {
    next: usize,
    by_key: BTreeMap<String, Vec<u8>>,
}

/// A payload store in process memory. It numbers its keys in order, so each
/// run stores the same blobs under the same keys.
#[derive(Debug, Default)]
struct MapStore {
    blobs: Mutex<Blobs>,
}

impl MapStore {
    fn blobs(&self) -> std::sync::MutexGuard<'_, Blobs> {
        self.blobs.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn len(&self) -> usize {
        self.blobs().by_key.len()
    }
}

impl PayloadStore for MapStore {
    fn put(&self, bytes: &[u8]) -> PayloadStoreFuture<'_, String> {
        let key = {
            let mut blobs = self.blobs();
            let key = format!("blob-{:08}", blobs.next);
            blobs.next += 1;
            blobs.by_key.insert(key.clone(), bytes.to_vec());
            key
        };
        Box::pin(std::future::ready(Ok(key)))
    }

    fn get(&self, key: &str) -> PayloadStoreFuture<'_, Vec<u8>> {
        let found = self.blobs().by_key.get(key).cloned();
        let result = found.ok_or_else(|| PayloadStoreError(format!("no blob {key}")));
        Box::pin(std::future::ready(result))
    }

    fn delete(&self, key: &str) -> PayloadStoreFuture<'_, ()> {
        self.blobs().by_key.remove(key);
        Box::pin(std::future::ready(Ok(())))
    }
}

/// The harness of `shape`, and the payload store of its response cache.
fn harness(shape: Shape) -> (AgentHarness, Option<Arc<MapStore>>) {
    // `model_turn` runs no tool, so the body of the tool never runs.
    let lookup = FnTool::new(
        TOOL,
        "Read one page.",
        json!({"type": "object", "properties": {"page": {"type": "integer"}}}),
        |_args: Value| async { Ok(Value::Null) },
    )
    .effect(ToolEffect::ReadOnly)
    .shared();
    let model = Arc::new(CannedModel {
        id: (shape == Shape::Cache).then_some(MODEL_ID),
    });
    let harness = AgentHarness::new(model).tool(lookup);
    if shape != Shape::Cache {
        return (harness, None);
    }
    let store = Arc::new(MapStore::default());
    let cache = ResponseCache::new(
        Arc::clone(&store) as Arc<dyn PayloadStore>,
        Arc::new(InMemoryCacheIndex::default()),
        PayloadCodecs::default(),
    );
    (harness.response_cache(cache), Some(store))
}

/// The state of one run that `agent_loop` keeps in its `Progress`.
struct Conversation {
    window: Vec<ChatMessage>,
    sent: Option<Sent>,
    usage: TokenUsage,
}

impl Conversation {
    fn opening() -> Self {
        Self {
            window: vec![
                ChatMessage::text(ChatRole::System, filler(1, "You read pages.", TEXT_BYTES)),
                ChatMessage::text(ChatRole::User, filler(2, "Read every page.", TEXT_BYTES)),
            ],
            sent: None,
            usage: TokenUsage::default(),
        }
    }

    /// The request of turn `page`, as `next_request` and `send_turn` in
    /// `workflow.rs` build it.
    fn request(&self, shape: Shape, page: usize, max_steps: u32) -> ModelTurnRequest {
        let (messages, prefix) = match (shape, &self.sent) {
            (Shape::Full, _) | (Shape::Delta | Shape::Cache, None) => (self.window.clone(), None),
            (Shape::Delta | Shape::Cache, Some(sent)) => (
                self.window[sent.len..].to_vec(),
                Some(PrefixRef {
                    digest: sent.digest.clone(),
                    len: u32::try_from(sent.len).expect("a window length fits in u32"),
                }),
            ),
        };
        ModelTurnRequest {
            run_id: "run-agent-model-turn-profile".into(),
            session_id: None,
            steps_used: u32::try_from(page).expect("a turn number fits in u32"),
            max_steps,
            usage: self.usage,
            messages,
            max_output_tokens: Some(4_096),
            read_only: false,
            memory_scope: None,
            extra_tools: Vec::new(),
            tenant: None,
            agent: None,
            allowed_tools: None,
            flow_labels: BTreeSet::new(),
            flow_released: Released::new(),
            interruptible: false,
            mesh: None,
            stream: false,
            prefix,
            // `send_turn` sets `remember` on every turn of the delta form.
            remember: shape != Shape::Full,
            model: None,
        }
    }

    /// Keep the answer of a turn, as `send_turn`, `push_assistant` and
    /// `run_round` do. The tool message holds `result` as the call output.
    fn record(&mut self, shape: Shape, turn: ModelTurn, result: String) {
        if shape != Shape::Full {
            let sent = self
                .sent
                .as_ref()
                .map_or_else(|| Sent::of(&self.window), |sent| sent.after(&self.window));
            self.sent = Some(sent);
        }
        self.usage = self.usage.saturating_add(turn.usage);
        let tool_call_id = match turn.content.as_slice() {
            [ContentPart::ToolCall { id, .. }] => id.clone(),
            other => panic!("the canned model makes one tool call, got {other:?}"),
        };
        self.window.push(ChatMessage {
            role: ChatRole::Assistant,
            content: turn.content,
        });
        self.window.push(ChatMessage {
            role: ChatRole::Tool,
            content: vec![ContentPart::ToolResult {
                tool_call_id,
                content: result,
            }],
        });
    }
}

/// Assert the turn that the canned model and `AllowAll` give on turn `page`.
fn check(turn: &ModelTurn, page: usize) {
    assert!(
        !turn.cache_hit && !turn.oversized,
        "turn {page} must be a fresh answer that fits the result cap"
    );
    let calls = turn.calls();
    assert!(
        calls.len() == 1 && calls[0].name == TOOL && calls[0].id == call_id(page),
        "turn {page} must ask for one lookup call, got {calls:?}"
    );
    assert_eq!(
        turn.decisions,
        [ToolDecision::Allow],
        "AllowAll must allow the lookup call of turn {page}"
    );
}

/// What one run reports.
#[derive(Debug, Default)]
struct Summary {
    prefixed: usize,
    stored: usize,
    window_messages: usize,
    window_bytes: u64,
}

/// Run `turns` model turns of one conversation through `model_turn`.
async fn run(shape: Shape, turns: usize) -> Summary {
    let (harness, store) = harness(shape);
    let results: Vec<String> = (0..turns)
        .map(|page| {
            let seed = 100 + u64::try_from(page).expect("a turn number fits in u64");
            filler(seed, &format!("page {page}:"), TEXT_BYTES)
        })
        .collect();
    let max_steps = u32::try_from(turns + 1).expect("a turn count fits in u32");
    let mut conversation = Conversation::opening();
    let mut prefixed = 0_usize;
    for (page, result) in results.into_iter().enumerate() {
        let request = conversation.request(shape, page, max_steps);
        prefixed += usize::from(request.prefix.is_some());
        let turn = harness
            .model_turn(request)
            .await
            .unwrap_or_else(|err| panic!("model turn {page} failed: {err}"));
        check(&turn, page);
        conversation.record(shape, turn, result);
    }
    let expected_prefixed = if shape == Shape::Full { 0 } else { turns - 1 };
    assert_eq!(
        prefixed,
        expected_prefixed,
        "every turn after the first must send a prefix in the {} shape",
        shape.name()
    );
    let stored = store.map_or(0, |store| store.len());
    if shape == Shape::Cache {
        assert_eq!(stored, turns, "every turn must miss the cache, then store");
    }
    Summary {
        prefixed,
        stored,
        window_messages: conversation.window.len(),
        window_bytes: json_len(&conversation.window),
    }
}

fn main() {
    let shape = Shape::from_env();
    let turns = env_usize("AGENT_MODEL_TURN_PROFILE_N", 200);
    let reps = env_usize("AGENT_MODEL_TURN_PROFILE_REPS", 1);
    assert!(turns > 0, "AGENT_MODEL_TURN_PROFILE_N must be at least 1");
    assert!(reps > 0, "AGENT_MODEL_TURN_PROFILE_REPS must be at least 1");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("a current-thread runtime with a timer");
    let mut summary = Summary::default();
    for _ in 0..reps {
        summary = runtime.block_on(run(shape, turns));
    }
    println!(
        "agent_model_turn_profile: shape={} turns={turns} reps={reps} prefixed={} stored={} \
         window_messages={} window_bytes={}",
        shape.name(),
        summary.prefixed,
        summary.stored,
        summary.window_messages,
        summary.window_bytes,
    );
}
