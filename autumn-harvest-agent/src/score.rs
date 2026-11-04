//! Scorers, recorded verdicts and the selection kernel (issue #2441).
//!
//! A [`Scorer`] checks one run and returns a [`Verdict`]: a finite score or
//! a reject. The `agent_score` activity calls it, so history records each
//! verdict. Replay reads the record and never calls the scorer again.
//!
//! A score comes from a verifier, not from log-probabilities: a pure check,
//! a test run or a judge model. A judge scorer calls [`ScoreContext::chat`].
//! That call checks the LLM budget first, and it records its tokens in the
//! cost ledger of the run that scores.
//!
//! [`select`] picks the best draw. A tie goes to the lowest draw id, and a
//! reject never beats a score. See `docs/design/DESIGN-2441.md`.

use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use autumn_harvest::failure::{ActivityFailure, IntoActivityErrorString};
use autumn_harvest::llm_ledger::{LlmCall, MAX_LLM_CALLS_PER_ATTEMPT};
use autumn_harvest::policy::RetryPolicy;
use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};

use crate::draw::DrawId;
use crate::error::{AgentError, ErrorKind};
use crate::harness::{AgentHarness, cut_bytes, model_failure};
use crate::message::{ContentPart, TokenUsage};
use crate::model::{BoxFuture, ChatRequest, ChatResponse};
use crate::refine::model_proposer::unfenced;
use crate::types::{AgentReport, AgentStop, AgentTask};

/// The most bytes of a reject reason that history keeps.
pub const MAX_REJECT_REASON_BYTES: usize = 4_096;

/// The most schema violations that a [`JsonSchema`] reject names.
const MAX_NAMED_VIOLATIONS: usize = 5;

/// A finite `f64`: never NaN and never an infinity.
///
/// It stores `-0.0` as `0.0`, so two equal scores always tie. It encodes as
/// a JSON number. A value that is not finite does not decode.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(try_from = "f64", into = "f64")]
pub struct FiniteF64(f64);

/// A score that is NaN or an infinity.
#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("a score must be finite, not {0}")]
pub struct NonFinite(pub f64);

impl From<NonFinite> for AgentError {
    fn from(err: NonFinite) -> Self {
        Self::new(ErrorKind::Decode, err.to_string())
    }
}

impl FiniteF64 {
    /// The score `1.0`.
    pub const ONE: Self = Self(1.0);

    /// The value, when it is finite.
    ///
    /// # Errors
    ///
    /// Returns [`NonFinite`] for NaN and for both infinities.
    pub const fn new(value: f64) -> Result<Self, NonFinite> {
        if !value.is_finite() {
            return Err(NonFinite(value));
        }
        // `-0.0 == 0.0`, so this also stores `-0.0` as `0.0`.
        if value == 0.0 {
            return Ok(Self(0.0));
        }
        Ok(Self(value))
    }

    /// The value.
    #[must_use]
    pub const fn get(self) -> f64 {
        self.0
    }

    /// A key whose integer order is the order of the scores.
    ///
    /// It is the key of `f64::total_cmp`. For a finite value with no `-0.0`,
    /// `a < b` exactly when `key(a) < key(b)`.
    const fn order_key(self) -> i64 {
        key_of_bits(self.0.to_bits().cast_signed())
    }
}

/// The order key of the bits of an `f64`.
///
/// A negative value has its bits flipped, all but the sign.
/// `formal/verus/src/agent_select.rs` proves that the key keeps the
/// sign-magnitude order of the bits. IEEE 754 orders finite values in that
/// order, except `-0.0`, which `FiniteF64` never holds.
#[allow(clippy::cast_sign_loss, clippy::cast_possible_wrap)]
const fn key_of_bits(bits: i64) -> i64 {
    bits ^ ((((bits >> 63i64) as u64) >> 1u64) as i64)
}

impl TryFrom<f64> for FiniteF64 {
    type Error = NonFinite;

    fn try_from(value: f64) -> Result<Self, NonFinite> {
        Self::new(value)
    }
}

impl From<FiniteF64> for f64 {
    fn from(value: FiniteF64) -> Self {
        value.0
    }
}

impl PartialEq for FiniteF64 {
    fn eq(&self, other: &Self) -> bool {
        self.order_key() == other.order_key()
    }
}

impl Eq for FiniteF64 {}

impl std::hash::Hash for FiniteF64 {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.order_key().hash(state);
    }
}

impl PartialOrd for FiniteF64 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FiniteF64 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.order_key().cmp(&other.order_key())
    }
}

impl fmt::Display for FiniteF64 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// The verdict of one scorer on one run.
///
/// It encodes as `{"score": 0.5}` or `{"reject": "reason"}`. It stays
/// exhaustive, because history pins these two shapes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    /// The run passes with this score. A higher score is better.
    Score(FiniteF64),
    /// The run fails the check. It never beats a score.
    Reject(String),
}

impl Verdict {
    /// A score verdict.
    ///
    /// # Errors
    ///
    /// Returns an [`ErrorKind::Decode`] error for NaN and for both
    /// infinities.
    pub fn score(value: f64) -> Result<Self, AgentError> {
        Ok(Self::Score(FiniteF64::new(value)?))
    }

    /// A reject verdict. It cuts a reason over [`MAX_REJECT_REASON_BYTES`].
    #[must_use]
    pub fn reject(reason: impl Into<String>) -> Self {
        Self::Reject(cut_bytes(reason.into(), MAX_REJECT_REASON_BYTES))
    }

    /// The score, or `None` for a reject.
    #[must_use]
    pub const fn as_score(&self) -> Option<FiniteF64> {
        match self {
            Self::Score(score) => Some(*score),
            Self::Reject(_) => None,
        }
    }

    /// Whether the verdict is a reject.
    #[must_use]
    pub const fn is_reject(&self) -> bool {
        matches!(self, Self::Reject(_))
    }
}

/// Pick the draw with the highest score.
///
/// A tie goes to the lowest draw id. A reject never beats a score. The
/// result is `None` only when every verdict is a reject, or when there is
/// no verdict. The order of the input does not change the result.
#[must_use]
pub fn select(verdicts: &[(DrawId, Verdict)]) -> Option<DrawId> {
    let entries: Vec<(u32, Option<i64>)> = verdicts
        .iter()
        .map(|(draw, verdict)| (draw.get(), verdict.as_score().map(FiniteF64::order_key)))
        .collect();
    best_draw(&entries).map(|(draw, _key)| DrawId::new(draw))
}

/// The better of `best` and the entry `next`.
///
/// An entry is a draw id and an order key. A reject has no key.
/// `formal/verus/src/agent_select.rs` proves the kernel.
const fn pick(best: Option<(u32, i64)>, next: (u32, Option<i64>)) -> Option<(u32, i64)> {
    match next.1 {
        None => best,
        Some(key) => match best {
            None => Some((next.0, key)),
            Some(b) => {
                if key > b.1 || (key == b.1 && next.0 < b.0) {
                    Some((next.0, key))
                } else {
                    best
                }
            }
        },
    }
}

/// The best entry of `draws`: the highest key, then the lowest draw id.
///
/// `formal/verus/src/agent_select.rs` proves three properties. The result
/// has the top key. A reject never wins while a key exists. The order of
/// `draws` does not change the result.
const fn best_draw(draws: &[(u32, Option<i64>)]) -> Option<(u32, i64)> {
    let mut best = None;
    let mut i = 0;
    while i < draws.len() {
        best = pick(best, draws[i]);
        i += 1;
    }
    best
}

/// The name and the result text of one tool call of a run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ToolRecord {
    /// The tool name. It is empty when the run has no call for the result.
    pub name: String,
    /// The result text that the model read.
    pub content: String,
}

/// What a scorer reads: the task, the final answer, the stop reason and
/// the tool results of one run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ScoreInput {
    /// The task input of the run.
    pub task: String,
    /// The final answer of the run.
    pub answer: String,
    /// Why the run ended.
    pub stop: AgentStop,
    /// The tool results of the run, in order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<ToolRecord>,
}

impl ScoreInput {
    /// The input of a run with no tool call.
    #[must_use]
    pub fn new(task: impl Into<String>, answer: impl Into<String>, stop: AgentStop) -> Self {
        Self {
            task: task.into(),
            answer: answer.into(),
            stop,
            tools: Vec::new(),
        }
    }

    /// Add one tool result.
    #[must_use]
    pub fn tool(mut self, name: impl Into<String>, content: impl Into<String>) -> Self {
        self.tools.push(ToolRecord {
            name: name.into(),
            content: content.into(),
        });
        self
    }

    /// The input of the run that `task` started and `report` ended.
    ///
    /// The tool results come from the transcript of the report, so a run
    /// with many large results gives a large input. History records the
    /// input, so trim it when it can be larger than the size cap of an
    /// activity input.
    #[must_use]
    pub fn from_run(task: &AgentTask, report: &AgentReport) -> Self {
        let parts = || report.messages.iter().flat_map(|m| m.content.iter());
        let names: std::collections::HashMap<&str, &str> = parts()
            .filter_map(|part| match part {
                ContentPart::ToolCall { id, name, .. } => Some((id.as_str(), name.as_str())),
                _ => None,
            })
            .collect();
        let tools = parts()
            .filter_map(|part| match part {
                ContentPart::ToolResult {
                    tool_call_id,
                    content,
                } => Some(ToolRecord {
                    name: names
                        .get(tool_call_id.as_str())
                        .map_or_else(String::new, |name| (*name).to_owned()),
                    content: content.clone(),
                }),
                _ => None,
            })
            .collect();
        Self {
            task: task.input.clone(),
            answer: report.text.clone(),
            stop: report.stop,
            tools,
        }
    }
}

/// The input of the `agent_score` activity: a scorer id and what it reads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ScoreRequest {
    /// The id of a scorer that [`AgentHarness::scorer`] installed.
    pub scorer: String,
    /// What the scorer reads.
    pub input: ScoreInput,
}

impl ScoreRequest {
    /// A request for the scorer `scorer`.
    #[must_use]
    pub fn new(scorer: impl Into<String>, input: ScoreInput) -> Self {
        Self {
            scorer: scorer.into(),
            input,
        }
    }
}

/// The LLM budget check of one judge call.
pub(crate) type BudgetCheck<'a> =
    Box<dyn Fn() -> BoxFuture<'a, Result<(), String>> + Send + Sync + 'a>;

/// The context of one score: the harness models, the LLM budget and the
/// cost ledger.
pub struct ScoreContext<'a> {
    harness: &'a AgentHarness,
    check: BudgetCheck<'a>,
    calls: Mutex<Vec<LlmCall>>,
    refusal: Mutex<Option<String>>,
    /// The judge calls that hold a ledger slot: done, or still in flight.
    slots: AtomicUsize,
    /// The sum of the usage of each judge reply.
    usage: Mutex<TokenUsage>,
    /// The model name of each judge call. `None` is the default model.
    routes: Mutex<std::collections::BTreeSet<Option<String>>>,
}

/// One reserved ledger slot. A drop before [`Slot::keep`] frees it, so a
/// failed or cancelled call gives its slot back.
struct Slot<'s> {
    slots: &'s AtomicUsize,
    kept: bool,
}

impl Slot<'_> {
    fn keep(mut self) {
        self.kept = true;
    }
}

impl Drop for Slot<'_> {
    fn drop(&mut self) {
        if !self.kept {
            self.slots.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

impl fmt::Debug for ScoreContext<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScoreContext").finish_non_exhaustive()
    }
}

impl<'a> ScoreContext<'a> {
    /// A context over `harness` with no budget check and no ledger record.
    ///
    /// Use it to call a scorer in a test. The `agent_score` activity builds
    /// its own context, which checks the budget and records each call.
    #[must_use]
    pub fn unmetered(harness: &'a AgentHarness) -> Self {
        Self::checked(harness, Box::new(|| Box::pin(async { Ok(()) })))
    }

    /// A context whose judge calls run `check` first.
    pub(crate) fn checked(harness: &'a AgentHarness, check: BudgetCheck<'a>) -> Self {
        Self {
            harness,
            check,
            calls: Mutex::new(Vec::new()),
            refusal: Mutex::new(None),
            slots: AtomicUsize::new(0),
            usage: Mutex::new(TokenUsage::default()),
            routes: Mutex::new(std::collections::BTreeSet::new()),
        }
    }

    /// Send one judge request to a harness model.
    ///
    /// `model` names a model of [`AgentHarness::model_named`]. `None` is the
    /// default model. The call checks the LLM budget of the run first. It
    /// then calls the model under the model timeout of the harness, and it
    /// keeps one cost ledger call.
    ///
    /// # Errors
    ///
    /// - [`ErrorKind::Config`] for an unknown model.
    /// - [`ErrorKind::Budget`] when the budget refuses the call. The
    ///   activity then fails with the refusal, even when the scorer handles
    ///   the error.
    /// - [`ErrorKind::Budget`] past [`MAX_LLM_CALLS_PER_ATTEMPT`] calls.
    /// - [`ErrorKind::Unavailable`] when the model does not answer in time.
    /// - The model error.
    pub async fn chat(
        &self,
        model: Option<&str>,
        request: &ChatRequest,
    ) -> Result<ChatResponse, AgentError> {
        if self.refused().is_some() {
            return Err(AgentError::new(
                ErrorKind::Budget,
                "the LLM budget refused an earlier judge call",
            ));
        }
        let client = self.harness.model(model).map_err(|_| {
            AgentError::new(
                ErrorKind::Config,
                "the harness has no such judge model; add it with AgentHarness::model_named",
            )
        })?;
        self.routes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(model.map(str::to_owned));
        // Reserve the slot before the call, so concurrent calls never pass
        // the ledger cap together.
        let slot = self.reserve().ok_or_else(|| {
            AgentError::new(
                ErrorKind::Budget,
                format!("one score makes at most {MAX_LLM_CALLS_PER_ATTEMPT} judge calls"),
            )
        })?;
        if let Err(refusal) = self.harness.check_budget(|| (self.check)()).await {
            *self.refusal.lock().unwrap_or_else(PoisonError::into_inner) = Some(refusal);
            return Err(AgentError::new(
                ErrorKind::Budget,
                "the LLM budget refused the judge call",
            ));
        }
        let budget = self.harness.model_budget();
        let started = Instant::now();
        let response = tokio::time::timeout(budget, client.chat(request))
            .await
            .map_err(|_| {
                AgentError::new(
                    ErrorKind::Unavailable,
                    format!("the judge did not answer within {budget:?}"),
                )
            })??;
        let call = self
            .harness
            .ledger_call(model, &response.usage, started.elapsed());
        self.lock_calls().push(call);
        let mut usage = self.usage.lock().unwrap_or_else(PoisonError::into_inner);
        *usage = usage.saturating_add(response.usage);
        drop(usage);
        slot.keep();
        Ok(response)
    }

    /// Reserve one ledger slot, or `None` when every slot is taken.
    fn reserve(&self) -> Option<Slot<'_>> {
        let mut taken = self.slots.load(Ordering::SeqCst);
        loop {
            if taken >= MAX_LLM_CALLS_PER_ATTEMPT {
                return None;
            }
            match self.slots.compare_exchange_weak(
                taken,
                taken + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    return Some(Slot {
                        slots: &self.slots,
                        kept: false,
                    });
                }
                Err(now) => taken = now,
            }
        }
    }

    /// The refusal payload of a judge call, if the budget refused one.
    fn refused(&self) -> Option<String> {
        self.refusal
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn lock_calls(&self) -> std::sync::MutexGuard<'_, Vec<LlmCall>> {
        self.calls.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The sum of the token usage of the judge replies so far (issue #2447).
    #[must_use]
    pub fn usage(&self) -> TokenUsage {
        *self.usage.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The model name of each judge call so far. `None` is the default
    /// model. An anonymised pool redacts these names (issue #2447).
    #[must_use]
    pub fn routes(&self) -> std::collections::BTreeSet<Option<String>> {
        self.routes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// The cost ledger calls of the judge calls so far.
    #[must_use]
    pub fn ledger_calls(&self) -> Vec<LlmCall> {
        self.lock_calls().clone()
    }
}

/// A check of one run.
///
/// The `agent_score` activity calls it, so history records each verdict.
/// A scorer can be a pure check, a test run or a judge model. A judge
/// scorer calls [`ScoreContext::chat`], so the budget and the cost ledger
/// see each call.
///
/// Return a retryable [`ErrorKind`] for a fault that a retry can fix. A
/// score that is not finite fails the activity with [`ErrorKind::Decode`].
/// An error after a paid judge call fails the activity for good. Return a
/// reject for a judge reply that does not parse.
pub trait Scorer: Send + Sync + fmt::Debug {
    /// The id under which the harness keeps the scorer.
    fn id(&self) -> &str;

    /// Check one run.
    fn score<'a>(
        &'a self,
        input: &'a ScoreInput,
        cx: &'a ScoreContext<'_>,
    ) -> BoxFuture<'a, Result<Verdict, AgentError>>;
}

/// Run one score: find the scorer, call it, and keep its judge calls.
///
/// Returns the verdict or an activity error payload, and the ledger calls.
pub(crate) async fn run_score<'a>(
    harness: &'a AgentHarness,
    request: ScoreRequest,
    check: BudgetCheck<'a>,
) -> (Result<Verdict, String>, Vec<LlmCall>) {
    let Some(scorer) = harness.scorer_of(&request.scorer) else {
        let failure = ActivityFailure::non_retryable(
            "ScorerMissing",
            format!(
                "the harness has no scorer {:?}; add it with AgentHarness::scorer",
                cut_bytes(request.scorer, crate::mesh::MAX_NAME_LEN)
            ),
        );
        return (Err(failure.into_error_payload()), Vec::new());
    };
    let cx = ScoreContext::checked(harness, check);
    let budget = harness.score_budget();
    let outcome = tokio::time::timeout(budget, scorer.score(&request.input, &cx))
        .await
        .unwrap_or_else(|_| {
            Err(AgentError::new(
                ErrorKind::Unavailable,
                format!("the scorer did not end within {budget:?}"),
            ))
        });
    let calls = cx.ledger_calls();
    // A refusal wins, also when the scorer handled the error.
    if let Some(refusal) = cx.refused() {
        return (
            Err(never_retried_after_pay(refusal, !calls.is_empty())),
            calls,
        );
    }
    let verdict = match outcome {
        Ok(Verdict::Reject(reason)) => Ok(Verdict::reject(reason)),
        Ok(verdict) => Ok(verdict),
        Err(err) => Err(score_failure(&err, !calls.is_empty())),
    };
    (verdict, calls)
}

/// `payload`, made non-retryable when a judge call was paid.
///
/// A failed budget read is retryable. After a paid call, a retry would pay
/// again unseen, so the payload keeps its type and stops the retries.
fn never_retried_after_pay(payload: String, paid: bool) -> String {
    match autumn_harvest::failure::parse_typed_payload(&payload) {
        Some(mut failure) if paid && !failure.non_retryable => {
            failure.non_retryable = true;
            failure.into_error_payload()
        }
        _ => payload,
    }
}

/// The activity error payload of a scorer error.
///
/// The message is cut to [`MAX_REJECT_REASON_BYTES`], because history keeps
/// it. A failed attempt writes no ledger row, so the spend of its judge
/// calls is lost. An error after a paid judge call is therefore never
/// retried, and a retry cannot pay again unseen.
fn score_failure(err: &AgentError, paid: bool) -> String {
    let message = cut_bytes(err.message().to_owned(), MAX_REJECT_REASON_BYTES);
    let err = AgentError::new(err.kind(), message);
    if paid && err.kind().is_retryable() {
        return ActivityFailure::non_retryable(format!("{:?}", err.kind()), err.to_string())
            .into_error_payload();
    }
    model_failure(&err)
}

/// One score, as an activity.
///
/// It calls the scorer that `request.scorer` names. History records the
/// verdict, so replay never calls the scorer again. Each judge call goes to
/// the cost ledger of the run that scores. A spent LLM budget refuses a
/// judge call before it runs, and the activity fails with
/// `LlmBudgetExceeded`. A retryable scorer error retries with backoff,
/// unless a judge call was paid. `AgentHarness::score_timeout` bounds the
/// scorer.
///
/// # Errors
///
/// Returns an error when no harness or no such scorer is installed, when
/// the scorer fails, or when the budget refuses a judge call.
#[activity(
    start_to_close = "15m",
    retry = RetryPolicy::exponential(4, Duration::from_secs(2))
)]
pub async fn agent_score(ctx: &ActivityContext, request: ScoreRequest) -> Result<Verdict, String> {
    let harness = crate::workflow::harness(ctx)?;
    let (verdict, calls) = run_score(
        harness,
        request,
        Box::new(|| Box::pin(ctx.check_llm_budget())),
    )
    .await;
    for call in calls {
        if let Err(err) = ctx.record_llm_call(call) {
            tracing::warn!(error = %err, "the agent cost ledger refused a judge call");
        }
    }
    verdict
}

/// A scorer that accepts an answer equal to the expected text.
///
/// It trims both texts. A match gives `Score(1.0)`. A miss, or a run that
/// did not complete, gives a reject.
#[derive(Debug, Clone)]
pub struct ExactMatch {
    id: String,
    expected: String,
}

impl ExactMatch {
    /// The default id.
    pub const ID: &'static str = "exact_match";

    /// A scorer that expects `expected`, under the id [`Self::ID`].
    #[must_use]
    pub fn new(expected: impl Into<String>) -> Self {
        Self {
            id: Self::ID.to_owned(),
            expected: expected.into(),
        }
    }

    /// Set the id, so a harness can keep more than one.
    #[must_use]
    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = id.into();
        self
    }
}

impl Scorer for ExactMatch {
    fn id(&self) -> &str {
        &self.id
    }

    fn score<'a>(
        &'a self,
        input: &'a ScoreInput,
        _cx: &'a ScoreContext<'_>,
    ) -> BoxFuture<'a, Result<Verdict, AgentError>> {
        let verdict = incomplete(input.stop).unwrap_or_else(|| {
            if input.answer.trim() == self.expected.trim() {
                Verdict::Score(FiniteF64::ONE)
            } else {
                Verdict::reject("the answer does not match the expected text")
            }
        });
        Box::pin(std::future::ready(Ok(verdict)))
    }
}

/// A scorer that accepts an answer that is JSON and matches a schema.
///
/// It reads the answer as JSON. It drops a Markdown code fence around it.
/// It checks the JSON with the engine schema check,
/// `autumn_harvest::info::validate_against_schema`, which covers a subset
/// of JSON Schema. A valid answer gives `Score(1.0)`. An answer that is not
/// JSON, an invalid answer, or a run that did not complete gives a reject.
#[derive(Debug, Clone)]
pub struct JsonSchema {
    id: String,
    schema: serde_json::Value,
}

impl JsonSchema {
    /// The default id.
    pub const ID: &'static str = "json_schema";

    /// A scorer for `schema`, under the id [`Self::ID`].
    #[must_use]
    pub fn new(schema: serde_json::Value) -> Self {
        Self {
            id: Self::ID.to_owned(),
            schema,
        }
    }

    /// Set the id, so a harness can keep more than one.
    #[must_use]
    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = id.into();
        self
    }

    /// The verdict on one answer.
    fn check(&self, answer: &str) -> Verdict {
        let value: serde_json::Value = match serde_json::from_str(unfenced(answer.trim())) {
            Ok(value) => value,
            Err(err) => return Verdict::reject(format!("the answer is not JSON: {err}")),
        };
        match autumn_harvest::info::validate_against_schema(&self.schema, &value) {
            Ok(()) => Verdict::Score(FiniteF64::ONE),
            Err(violations) => {
                let named: Vec<String> = violations
                    .iter()
                    .take(MAX_NAMED_VIOLATIONS)
                    .map(|v| format!("{}: {}", v.field_path.as_deref().unwrap_or("/"), v.message))
                    .collect();
                Verdict::reject(format!(
                    "the answer does not match the schema ({} violations): {}",
                    violations.len(),
                    named.join("; ")
                ))
            }
        }
    }
}

/// A reject for a run that did not complete, or `None` for one that did.
///
/// A run that stopped another way can hold a cut or missing answer.
fn incomplete(stop: AgentStop) -> Option<Verdict> {
    (stop != AgentStop::Completed).then(|| {
        let name = serde_json::to_value(stop)
            .ok()
            .and_then(|value| value.as_str().map(str::to_owned))
            .unwrap_or_else(|| format!("{stop:?}"));
        Verdict::reject(format!("the run stopped as {name}, not completed"))
    })
}

impl Scorer for JsonSchema {
    fn id(&self) -> &str {
        &self.id
    }

    fn score<'a>(
        &'a self,
        input: &'a ScoreInput,
        _cx: &'a ScoreContext<'_>,
    ) -> BoxFuture<'a, Result<Verdict, AgentError>> {
        let verdict = incomplete(input.stop).unwrap_or_else(|| self.check(&input.answer));
        Box::pin(std::future::ready(Ok(verdict)))
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::float_cmp,
        clippy::unnecessary_literal_bound
    )]

    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use autumn_harvest::failure::ActivityFailure;
    use autumn_harvest::llm_budget::{
        ERROR_TYPE_LLM_BUDGET_EXCEEDED, LlmBudgetExceeded, LlmSpend, check_llm_budget,
    };
    use autumn_harvest::quota::QuotaPolicy;
    use proptest::prelude::*;
    use serde_json::json;

    use super::*;
    use crate::message::{ChatMessage, ChatRole, StopReason, TokenUsage};
    use crate::model::AgentModel;

    fn score(value: f64) -> Verdict {
        Verdict::score(value).unwrap()
    }

    fn reject() -> Verdict {
        Verdict::reject("no")
    }

    fn draws(verdicts: Vec<(u32, Verdict)>) -> Vec<(DrawId, Verdict)> {
        verdicts
            .into_iter()
            .map(|(draw, verdict)| (DrawId::new(draw), verdict))
            .collect()
    }

    // ── FiniteF64 and Verdict ───────────────────────────────────────────────

    #[test]
    fn a_finite_value_refuses_nan_and_both_infinities() {
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(FiniteF64::new(value).is_err(), "{value}");
        }
        for value in [0.0, -1e300, 1e300, f64::MIN, f64::MAX, f64::MIN_POSITIVE] {
            assert_eq!(FiniteF64::new(value).unwrap().get(), value);
        }
    }

    #[test]
    fn negative_zero_is_stored_as_zero() {
        let zero = FiniteF64::new(-0.0).unwrap();
        assert!(zero.get().is_sign_positive());
        assert_eq!(zero, FiniteF64::new(0.0).unwrap());
    }

    #[test]
    fn a_non_finite_score_is_a_decode_error() {
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let err = Verdict::score(value).unwrap_err();
            assert_eq!(err.kind(), ErrorKind::Decode, "{value}");
        }
    }

    #[test]
    fn a_verdict_encodes_in_a_stable_shape() {
        assert_eq!(
            serde_json::to_value(score(0.5)).unwrap(),
            json!({"score": 0.5})
        );
        assert_eq!(
            serde_json::to_value(Verdict::reject("bad")).unwrap(),
            json!({"reject": "bad"})
        );
        let back: Verdict = serde_json::from_value(json!({"score": -2.0})).unwrap();
        assert_eq!(back, score(-2.0));
        assert!(serde_json::from_value::<Verdict>(json!({"score": null})).is_err());
    }

    #[test]
    fn a_non_finite_value_does_not_decode() {
        use serde::de::value::{Error, F64Deserializer};
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let decoded = FiniteF64::deserialize(F64Deserializer::<Error>::new(value));
            assert!(decoded.is_err(), "{value}");
        }
        let decoded = FiniteF64::deserialize(F64Deserializer::<Error>::new(-0.0)).unwrap();
        assert!(decoded.get().is_sign_positive());
    }

    #[test]
    fn a_long_reject_reason_is_cut() {
        let Verdict::Reject(reason) = Verdict::reject("x".repeat(MAX_REJECT_REASON_BYTES * 2))
        else {
            panic!("not a reject");
        };
        assert!(reason.len() <= MAX_REJECT_REASON_BYTES, "{}", reason.len());
        assert!(reason.starts_with("xxx"));
    }

    #[test]
    fn the_order_key_orders_edge_values() {
        let values = [
            f64::MIN,
            -1e300,
            -1.0,
            -f64::MIN_POSITIVE,
            -5e-324,
            0.0,
            5e-324,
            f64::MIN_POSITIVE,
            1.0,
            1e300,
            f64::MAX,
        ];
        for pair in values.windows(2) {
            let (a, b) = (
                FiniteF64::new(pair[0]).unwrap(),
                FiniteF64::new(pair[1]).unwrap(),
            );
            assert!(a.order_key() < b.order_key(), "{a} < {b}");
            assert!(a < b);
        }
    }

    /// Any finite float of either sign, `-0.0` included.
    fn finite() -> proptest::num::f64::Any {
        use proptest::num::f64::{NEGATIVE, NORMAL, POSITIVE, SUBNORMAL, ZERO};
        POSITIVE | NEGATIVE | NORMAL | SUBNORMAL | ZERO
    }

    proptest! {
        #[test]
        fn the_order_key_matches_the_float_order(a in finite(), b in finite()) {
            let (x, y) = (FiniteF64::new(a).unwrap(), FiniteF64::new(b).unwrap());
            prop_assert_eq!(a < b, x.order_key() < y.order_key());
            prop_assert_eq!(a == b, x.order_key() == y.order_key());
        }
    }

    // ── select ──────────────────────────────────────────────────────────────

    #[test]
    fn a_very_low_score_beats_a_reject() {
        let input = draws(vec![(0, reject()), (1, score(-1e300))]);
        assert_eq!(select(&input), Some(DrawId::new(1)));
    }

    #[test]
    fn only_rejects_give_none() {
        let input = draws(vec![(0, reject()), (1, reject()), (2, reject())]);
        assert_eq!(select(&input), None);
    }

    #[test]
    fn no_entry_gives_none() {
        assert_eq!(select(&[]), None);
    }

    #[test]
    fn the_highest_score_wins() {
        let input = draws(vec![(0, score(0.2)), (1, score(0.9)), (2, score(0.5))]);
        assert_eq!(select(&input), Some(DrawId::new(1)));
    }

    #[test]
    fn a_tie_goes_to_the_lowest_draw_id() {
        let input = draws(vec![(7, score(1.0)), (3, score(1.0)), (5, score(0.1))]);
        assert_eq!(select(&input), Some(DrawId::new(3)));
        let zeros = draws(vec![(4, score(0.0)), (2, score(-0.0))]);
        assert_eq!(select(&zeros), Some(DrawId::new(2)));
    }

    #[test]
    fn the_kernel_steps_match_the_spec() {
        assert_eq!(pick(None, (1, None)), None);
        assert_eq!(pick(None, (1, Some(-3))), Some((1, -3)));
        assert_eq!(pick(Some((2, 5)), (1, None)), Some((2, 5)));
        assert_eq!(pick(Some((2, 5)), (1, Some(4))), Some((2, 5)));
        assert_eq!(pick(Some((2, 5)), (1, Some(5))), Some((1, 5)));
        assert_eq!(pick(Some((2, 5)), (3, Some(5))), Some((2, 5)));
        assert_eq!(pick(Some((2, 5)), (3, Some(6))), Some((3, 6)));
        assert_eq!(
            best_draw(&[(4, None), (9, Some(1)), (8, Some(1))]),
            Some((8, 1))
        );
    }

    fn verdict_strategy() -> impl Strategy<Value = Verdict> {
        prop_oneof![
            Just(Verdict::reject("no")),
            // A small range makes ties common.
            (-3_i32..3).prop_map(|n| score(f64::from(n))),
            Just(score(-0.0)),
            Just(score(0.0)),
            Just(score(f64::MAX)),
            Just(score(f64::MIN)),
            Just(score(-1e300)),
            any::<f64>()
                .prop_filter("finite", |x| x.is_finite())
                .prop_map(score),
        ]
    }

    fn entries() -> impl Strategy<Value = Vec<(DrawId, Verdict)>> {
        proptest::collection::vec((0_u32..12, verdict_strategy()), 0..16).prop_map(|v| {
            v.into_iter()
                .map(|(draw, verdict)| (DrawId::new(draw), verdict))
                .collect()
        })
    }

    /// The reference: the highest score, then the lowest draw id. It
    /// orders the floats with `total_cmp`, not with the order key.
    fn reference(input: &[(DrawId, Verdict)]) -> Option<DrawId> {
        input
            .iter()
            .filter_map(|(draw, verdict)| verdict.as_score().map(|s| (s.get(), *draw)))
            .max_by(|(sa, da), (sb, db)| sa.total_cmp(sb).then(db.cmp(da)))
            .map(|(_, draw)| draw)
    }

    proptest! {
        #[test]
        fn select_does_not_depend_on_the_order(
            (input, shuffled) in entries().prop_flat_map(|v| (Just(v.clone()), Just(v).prop_shuffle()))
        ) {
            prop_assert_eq!(select(&input), select(&shuffled));
        }

        #[test]
        fn select_matches_the_reference(input in entries()) {
            let got = select(&input);
            prop_assert_eq!(got, reference(&input));
            let any_score = input.iter().any(|(_, v)| !v.is_reject());
            prop_assert_eq!(got.is_some(), any_score);
            if let Some(draw) = got {
                prop_assert!(input.iter().any(|(d, v)| *d == draw && !v.is_reject()));
            }
        }
    }

    // ── ScoreInput ──────────────────────────────────────────────────────────

    #[test]
    fn an_input_from_a_run_holds_the_task_answer_stop_and_tools() {
        let task = AgentTask::new("What is 2 + 2?");
        let report = AgentReport {
            stop: AgentStop::Completed,
            text: "4".into(),
            steps_used: 1,
            tool_calls: 1,
            usage: TokenUsage::default(),
            messages: vec![
                ChatMessage::text(ChatRole::User, "What is 2 + 2?"),
                ChatMessage {
                    role: ChatRole::Assistant,
                    content: vec![ContentPart::ToolCall {
                        id: "c1".into(),
                        name: "calc".into(),
                        arguments: json!({"expr": "2+2"}),
                    }],
                },
                ChatMessage {
                    role: ChatRole::Tool,
                    content: vec![
                        ContentPart::ToolResult {
                            tool_call_id: "c1".into(),
                            content: "4".into(),
                        },
                        ContentPart::ToolResult {
                            tool_call_id: "lost".into(),
                            content: "?".into(),
                        },
                    ],
                },
                ChatMessage::text(ChatRole::Assistant, "4"),
            ],
            followups: 0,
            followup_dropped: false,
            delegations: Vec::new(),
            flow_labels: std::collections::BTreeSet::new(),
            run_id: None,
            workspace: None,
            error: None,
            resets: 0,
        };
        let input = ScoreInput::from_run(&task, &report);
        assert_eq!(
            input,
            ScoreInput::new("What is 2 + 2?", "4", AgentStop::Completed)
                .tool("calc", "4")
                .tool("", "?")
        );
    }

    // ── scorers and run_score ───────────────────────────────────────────────

    /// A model that answers each call with the same text and usage.
    #[derive(Debug)]
    struct Judge {
        calls: AtomicUsize,
    }

    impl AgentModel for Judge {
        fn chat<'a>(
            &'a self,
            _request: &'a ChatRequest,
        ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {
                Ok(ChatResponse {
                    content: vec![ContentPart::Text("0.75".into())],
                    stop_reason: StopReason::EndTurn,
                    usage: TokenUsage::new(30, 4),
                })
            })
        }

        fn model_id(&self) -> Option<&str> {
            Some("judge-1")
        }
    }

    /// A scorer that asks the judge, then returns its reply as a score.
    /// With `swallow` set, it turns any judge error into a reject.
    #[derive(Debug)]
    struct JudgeScorer {
        swallow: bool,
    }

    impl Scorer for JudgeScorer {
        fn id(&self) -> &str {
            "judge"
        }

        fn score<'a>(
            &'a self,
            input: &'a ScoreInput,
            cx: &'a ScoreContext<'_>,
        ) -> BoxFuture<'a, Result<Verdict, AgentError>> {
            Box::pin(async move {
                let request = ChatRequest {
                    messages: vec![ChatMessage::text(ChatRole::User, input.answer.clone())],
                    tools: Vec::new(),
                    max_tokens: Some(8),
                    temperature: None,
                    draw: None,
                };
                let reply = match cx.chat(None, &request).await {
                    Ok(reply) => reply,
                    Err(err) if self.swallow => return Ok(Verdict::reject(err.to_string())),
                    Err(err) => return Err(err),
                };
                let text = match reply.content.first() {
                    Some(ContentPart::Text(text)) => text.clone(),
                    _ => String::new(),
                };
                Verdict::score(text.parse().unwrap_or(f64::NAN))
            })
        }
    }

    /// A scorer that returns a fixed raw value or error.
    #[derive(Debug)]
    struct Fixed(Result<f64, ErrorKind>);

    impl Scorer for Fixed {
        fn id(&self) -> &str {
            "fixed"
        }

        fn score<'a>(
            &'a self,
            _input: &'a ScoreInput,
            _cx: &'a ScoreContext<'_>,
        ) -> BoxFuture<'a, Result<Verdict, AgentError>> {
            Box::pin(async move {
                match self.0 {
                    Ok(value) => Verdict::score(value),
                    Err(kind) => Err(AgentError::new(kind, "scorer fault")),
                }
            })
        }
    }

    fn judge() -> Arc<Judge> {
        Arc::new(Judge {
            calls: AtomicUsize::new(0),
        })
    }

    fn input() -> ScoreInput {
        ScoreInput::new("task", "answer", AgentStop::Completed)
    }

    fn pass<'a>() -> BudgetCheck<'a> {
        Box::new(|| Box::pin(async { Ok(()) }))
    }

    /// The check of the engine with a run token cap of zero.
    fn zero_budget<'a>() -> BudgetCheck<'a> {
        Box::new(|| {
            Box::pin(async {
                let policy = QuotaPolicy::new("tenant").with_max_run_llm_tokens(0);
                check_llm_budget(&LlmSpend::default(), &policy).map_or(Ok(()), |violation| {
                    Err(LlmBudgetExceeded::from_violation(violation)
                        .into_failure()
                        .into_error_payload())
                })
            })
        })
    }

    fn failure(payload: &str) -> ActivityFailure {
        autumn_harvest::failure::parse_typed_payload(payload).expect("a typed failure payload")
    }

    #[tokio::test]
    async fn a_nan_score_fails_the_activity_with_decode() {
        let harness = AgentHarness::new(judge()).scorer(Arc::new(Fixed(Ok(f64::NAN))));
        let (verdict, _) = run_score(&harness, ScoreRequest::new("fixed", input()), pass()).await;
        let failure = failure(&verdict.unwrap_err());
        assert_eq!(failure.error_type, "Decode");
        assert!(failure.non_retryable);
    }

    #[tokio::test]
    async fn a_retryable_scorer_error_is_retryable() {
        let harness =
            AgentHarness::new(judge()).scorer(Arc::new(Fixed(Err(ErrorKind::Unavailable))));
        let (verdict, _) = run_score(&harness, ScoreRequest::new("fixed", input()), pass()).await;
        assert!(!failure(&verdict.unwrap_err()).non_retryable);
    }

    #[tokio::test]
    async fn an_unknown_scorer_fails_for_good() {
        let harness = AgentHarness::new(judge());
        let (verdict, calls) =
            run_score(&harness, ScoreRequest::new("missing", input()), pass()).await;
        let failure = failure(&verdict.unwrap_err());
        assert_eq!(failure.error_type, "ScorerMissing");
        assert!(failure.non_retryable);
        assert_eq!(calls.len(), 0);
    }

    #[tokio::test]
    async fn a_scorer_reject_that_skips_the_constructor_is_still_cut() {
        #[derive(Debug)]
        struct Long;
        impl Scorer for Long {
            fn id(&self) -> &str {
                "long"
            }
            fn score<'a>(
                &'a self,
                _input: &'a ScoreInput,
                _cx: &'a ScoreContext<'_>,
            ) -> BoxFuture<'a, Result<Verdict, AgentError>> {
                Box::pin(async { Ok(Verdict::Reject("é".repeat(MAX_REJECT_REASON_BYTES))) })
            }
        }
        let harness = AgentHarness::new(judge()).scorer(Arc::new(Long));
        let (verdict, _) = run_score(&harness, ScoreRequest::new("long", input()), pass()).await;
        let Verdict::Reject(reason) = verdict.unwrap() else {
            panic!("not a reject");
        };
        assert!(reason.len() <= MAX_REJECT_REASON_BYTES);
    }

    #[tokio::test]
    async fn judge_tokens_go_to_the_ledger_calls() {
        let model = judge();
        let harness =
            AgentHarness::new(model.clone()).scorer(Arc::new(JudgeScorer { swallow: false }));
        let (verdict, calls) =
            run_score(&harness, ScoreRequest::new("judge", input()), pass()).await;
        assert_eq!(verdict.unwrap(), score(0.75));
        assert_eq!(model.calls.load(Ordering::SeqCst), 1);
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].model(), "judge-1");
        assert_eq!(calls[0].input_tokens(), 30);
        assert_eq!(calls[0].output_tokens(), 4);
    }

    #[tokio::test]
    async fn a_zero_run_budget_stops_the_judge_before_its_call() {
        for swallow in [false, true] {
            let model = judge();
            let harness =
                AgentHarness::new(model.clone()).scorer(Arc::new(JudgeScorer { swallow }));
            let (verdict, calls) =
                run_score(&harness, ScoreRequest::new("judge", input()), zero_budget()).await;
            let failure = failure(&verdict.unwrap_err());
            assert_eq!(
                failure.error_type, ERROR_TYPE_LLM_BUDGET_EXCEEDED,
                "{swallow}"
            );
            assert!(failure.non_retryable);
            assert_eq!(model.calls.load(Ordering::SeqCst), 0, "no judge call ran");
            assert_eq!(calls.len(), 0);
        }
    }

    #[tokio::test]
    async fn a_judge_call_on_an_unknown_model_is_a_config_error() {
        let harness = AgentHarness::new(judge());
        let cx = ScoreContext::unmetered(&harness);
        let request = ChatRequest {
            messages: Vec::new(),
            tools: Vec::new(),
            max_tokens: None,
            temperature: None,
            draw: None,
        };
        let err = cx.chat(Some("nope"), &request).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Config);
        assert_eq!(cx.ledger_calls().len(), 0);
    }

    #[tokio::test]
    async fn a_slow_judge_is_unavailable() {
        #[derive(Debug)]
        struct Slow;
        impl AgentModel for Slow {
            fn chat<'a>(
                &'a self,
                _request: &'a ChatRequest,
            ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
                Box::pin(std::future::pending())
            }
        }
        let harness = AgentHarness::new(Arc::new(Slow)).model_timeout(Duration::from_millis(20));
        let cx = ScoreContext::unmetered(&harness);
        let request = ChatRequest {
            messages: Vec::new(),
            tools: Vec::new(),
            max_tokens: None,
            temperature: None,
            draw: None,
        };
        let err = cx.chat(None, &request).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unavailable);
    }

    async fn verdict_of(scorer: &dyn Scorer, input: &ScoreInput) -> Verdict {
        let harness = AgentHarness::new(judge());
        let cx = ScoreContext::unmetered(&harness);
        scorer.score(input, &cx).await.unwrap()
    }

    #[tokio::test]
    async fn exact_match_accepts_the_expected_answer() {
        let scorer = ExactMatch::new("Paris");
        let input = ScoreInput::new("Capital of France?", "  Paris\n", AgentStop::Completed);
        assert_eq!(verdict_of(&scorer, &input).await, score(1.0));
        assert_eq!(scorer.id(), ExactMatch::ID);
        assert_eq!(ExactMatch::new("x").with_id("city").id(), "city");
    }

    #[tokio::test]
    async fn exact_match_rejects_another_answer() {
        let scorer = ExactMatch::new("Paris");
        let wrong = ScoreInput::new("Capital of France?", "Lyon", AgentStop::Completed);
        assert!(verdict_of(&scorer, &wrong).await.is_reject());
        let cut = ScoreInput::new("Capital of France?", "Paris", AgentStop::OutputCapped);
        let Verdict::Reject(reason) = verdict_of(&scorer, &cut).await else {
            panic!("a cut run must be rejected");
        };
        assert!(reason.contains("output_capped"), "{reason}");
    }

    fn person_schema() -> serde_json::Value {
        json!({
            "type": "object",
            "required": ["name", "age"],
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer", "minimum": 0}
            }
        })
    }

    #[tokio::test]
    async fn json_schema_accepts_a_valid_answer() {
        let scorer = JsonSchema::new(person_schema());
        for answer in [
            r#"{"name": "Ada", "age": 36}"#,
            "```json\n{\"name\": \"Ada\", \"age\": 36}\n```",
        ] {
            let input = ScoreInput::new("Who?", answer, AgentStop::Completed);
            assert_eq!(verdict_of(&scorer, &input).await, score(1.0), "{answer}");
        }
        assert_eq!(scorer.id(), JsonSchema::ID);
    }

    #[tokio::test]
    async fn json_schema_rejects_an_invalid_answer() {
        let scorer = JsonSchema::new(person_schema());
        let missing = ScoreInput::new("Who?", r#"{"name": "Ada"}"#, AgentStop::Completed);
        let Verdict::Reject(reason) = verdict_of(&scorer, &missing).await else {
            panic!("a missing field must be rejected");
        };
        assert!(reason.contains("age"), "{reason}");
        let prose = ScoreInput::new("Who?", "Ada, 36", AgentStop::Completed);
        let Verdict::Reject(reason) = verdict_of(&scorer, &prose).await else {
            panic!("prose must be rejected");
        };
        assert!(reason.contains("not JSON"), "{reason}");
        let cut = ScoreInput::new(
            "Who?",
            r#"{"name": "Ada", "age": 36}"#,
            AgentStop::StepsExhausted,
        );
        assert!(verdict_of(&scorer, &cut).await.is_reject());
    }

    #[test]
    fn a_later_scorer_with_the_same_id_replaces_the_earlier_one() {
        let harness = AgentHarness::new(judge())
            .scorer(Arc::new(ExactMatch::new("a")))
            .scorer(Arc::new(ExactMatch::new("b")));
        assert!(harness.has_scorer(ExactMatch::ID));
        assert!(!harness.has_scorer(JsonSchema::ID));
        assert_eq!(
            format!("{:?}", harness.scorer_of(ExactMatch::ID).unwrap()),
            format!("{:?}", ExactMatch::new("b"))
        );
    }

    #[test]
    fn the_score_budget_fits_the_activity_start_to_close() {
        let start_to_close = agent_score_info().default_start_to_close.unwrap();
        assert!(crate::harness::DEFAULT_SCORE_TIMEOUT < start_to_close);
        let harness = AgentHarness::new(judge());
        assert_eq!(
            harness.score_budget(),
            crate::harness::DEFAULT_SCORE_TIMEOUT
        );
        let raised = AgentHarness::new(judge()).score_timeout(Duration::from_secs(3600));
        assert_eq!(raised.score_budget(), crate::harness::DEFAULT_SCORE_TIMEOUT);
        let zero = AgentHarness::new(judge()).score_timeout(Duration::ZERO);
        assert_eq!(zero.score_budget(), crate::harness::DEFAULT_SCORE_TIMEOUT);
        let lowered = AgentHarness::new(judge()).score_timeout(Duration::from_secs(5));
        assert_eq!(lowered.score_budget(), Duration::from_secs(5));
    }

    /// A scorer that never ends.
    #[derive(Debug)]
    struct Stuck;

    impl Scorer for Stuck {
        fn id(&self) -> &str {
            "stuck"
        }

        fn score<'a>(
            &'a self,
            _input: &'a ScoreInput,
            _cx: &'a ScoreContext<'_>,
        ) -> BoxFuture<'a, Result<Verdict, AgentError>> {
            Box::pin(std::future::pending())
        }
    }

    #[tokio::test]
    async fn a_slow_scorer_fails_retryable_at_the_score_timeout() {
        let harness = AgentHarness::new(judge())
            .scorer(Arc::new(Stuck))
            .score_timeout(Duration::from_millis(20));
        let (verdict, calls) =
            run_score(&harness, ScoreRequest::new("stuck", input()), pass()).await;
        let failure = failure(&verdict.unwrap_err());
        assert_eq!(failure.error_type, "Unavailable");
        assert!(!failure.non_retryable);
        assert_eq!(calls.len(), 0);
    }

    /// A scorer that pays for one judge call, then fails with `kind`.
    #[derive(Debug)]
    struct PaidThenFails(ErrorKind);

    impl Scorer for PaidThenFails {
        fn id(&self) -> &str {
            "paid"
        }

        fn score<'a>(
            &'a self,
            _input: &'a ScoreInput,
            cx: &'a ScoreContext<'_>,
        ) -> BoxFuture<'a, Result<Verdict, AgentError>> {
            Box::pin(async move {
                cx.chat(None, &empty_request()).await?;
                Err(AgentError::new(
                    self.0,
                    "x".repeat(MAX_REJECT_REASON_BYTES * 2),
                ))
            })
        }
    }

    fn empty_request() -> ChatRequest {
        ChatRequest {
            messages: Vec::new(),
            tools: Vec::new(),
            max_tokens: None,
            temperature: None,
            draw: None,
        }
    }

    #[tokio::test]
    async fn an_error_after_a_paid_judge_call_is_never_retried() {
        let model = judge();
        let harness = AgentHarness::new(model.clone())
            .scorer(Arc::new(PaidThenFails(ErrorKind::Unavailable)));
        let (verdict, calls) =
            run_score(&harness, ScoreRequest::new("paid", input()), pass()).await;
        let payload = verdict.unwrap_err();
        let failure = failure(&payload);
        assert_eq!(failure.error_type, "Unavailable");
        assert!(failure.non_retryable, "a retry would pay again unseen");
        assert_eq!(model.calls.load(Ordering::SeqCst), 1);
        assert_eq!(calls.len(), 1);
        assert!(
            failure.message.len() <= MAX_REJECT_REASON_BYTES + 32,
            "{}",
            failure.message.len()
        );
    }

    #[tokio::test]
    async fn a_long_scorer_error_is_cut() {
        let harness = AgentHarness::new(judge()).scorer(Arc::new(Fixed(Err(ErrorKind::Tool))));
        let (verdict, _) = run_score(&harness, ScoreRequest::new("fixed", input()), pass()).await;
        assert!(failure(&verdict.unwrap_err()).non_retryable);

        let long = AgentError::new(ErrorKind::Tool, "y".repeat(MAX_REJECT_REASON_BYTES * 4));
        let failure = failure(&score_failure(&long, false));
        assert!(
            failure.message.len() <= MAX_REJECT_REASON_BYTES + 32,
            "{}",
            failure.message.len()
        );
    }

    #[tokio::test]
    async fn a_score_makes_at_most_the_ledger_cap_of_judge_calls() {
        let model = judge();
        let harness = AgentHarness::new(model.clone());
        let cx = ScoreContext::unmetered(&harness);
        for _ in 0..MAX_LLM_CALLS_PER_ATTEMPT {
            cx.chat(None, &empty_request()).await.unwrap();
        }
        let err = cx.chat(None, &empty_request()).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Budget);
        assert_eq!(cx.ledger_calls().len(), MAX_LLM_CALLS_PER_ATTEMPT);
        assert_eq!(
            model.calls.load(Ordering::SeqCst),
            MAX_LLM_CALLS_PER_ATTEMPT
        );
    }

    /// A model that waits at a barrier, so two calls are in flight at once.
    #[derive(Debug)]
    struct Gate {
        barrier: tokio::sync::Barrier,
        calls: AtomicUsize,
    }

    impl AgentModel for Gate {
        fn chat<'a>(
            &'a self,
            _request: &'a ChatRequest,
        ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                if self.calls.load(Ordering::SeqCst) >= MAX_LLM_CALLS_PER_ATTEMPT {
                    // Wait at most a moment for a second call to join.
                    let _ =
                        tokio::time::timeout(Duration::from_millis(200), self.barrier.wait()).await;
                }
                Ok(ChatResponse {
                    content: vec![ContentPart::Text("1".into())],
                    stop_reason: StopReason::EndTurn,
                    usage: TokenUsage::new(1, 1),
                })
            })
        }
    }

    #[tokio::test]
    async fn concurrent_judge_calls_never_pass_the_ledger_cap() {
        let model = Arc::new(Gate {
            barrier: tokio::sync::Barrier::new(2),
            calls: AtomicUsize::new(0),
        });
        let harness = AgentHarness::new(model.clone());
        let cx = ScoreContext::unmetered(&harness);
        for _ in 0..MAX_LLM_CALLS_PER_ATTEMPT - 1 {
            cx.chat(None, &empty_request()).await.unwrap();
        }
        let request = empty_request();
        let (a, b) = tokio::join!(cx.chat(None, &request), cx.chat(None, &request));
        assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
        assert_eq!(cx.ledger_calls().len(), MAX_LLM_CALLS_PER_ATTEMPT);
        assert_eq!(
            model.calls.load(Ordering::SeqCst),
            MAX_LLM_CALLS_PER_ATTEMPT
        );
    }

    #[tokio::test]
    async fn a_failed_judge_call_frees_its_slot() {
        let harness = AgentHarness::new(Arc::new(PricedOnce {
            calls: AtomicUsize::new(0),
        }));
        let cx = ScoreContext::unmetered(&harness);
        assert!(cx.chat(None, &empty_request()).await.is_err());
        assert_eq!(cx.slots.load(Ordering::SeqCst), 0);
        cx.chat(None, &empty_request()).await.unwrap();
        assert_eq!(cx.slots.load(Ordering::SeqCst), 1);
    }

    /// A model that fails its first call, then answers.
    #[derive(Debug)]
    struct PricedOnce {
        calls: AtomicUsize,
    }

    impl AgentModel for PricedOnce {
        fn chat<'a>(
            &'a self,
            _request: &'a ChatRequest,
        ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
            let first = self.calls.fetch_add(1, Ordering::SeqCst) == 0;
            Box::pin(async move {
                if first {
                    return Err(AgentError::new(ErrorKind::Provider, "down"));
                }
                Ok(ChatResponse {
                    content: Vec::new(),
                    stop_reason: StopReason::EndTurn,
                    usage: TokenUsage::new(1, 1),
                })
            })
        }
    }

    /// A scorer that makes two judge calls and hides any error.
    #[derive(Debug)]
    struct TwoCalls;

    impl Scorer for TwoCalls {
        fn id(&self) -> &str {
            "two"
        }

        fn score<'a>(
            &'a self,
            _input: &'a ScoreInput,
            cx: &'a ScoreContext<'_>,
        ) -> BoxFuture<'a, Result<Verdict, AgentError>> {
            Box::pin(async move {
                for _ in 0..2 {
                    if let Err(err) = cx.chat(None, &empty_request()).await {
                        return Ok(Verdict::reject(err.to_string()));
                    }
                }
                Verdict::score(1.0)
            })
        }
    }

    #[tokio::test]
    async fn a_failed_budget_read_after_a_paid_call_is_never_retried() {
        let model = judge();
        let harness = AgentHarness::new(model.clone()).scorer(Arc::new(TwoCalls));
        let reads = AtomicUsize::new(0);
        // The first read passes. The second read fails as the engine does.
        let check: BudgetCheck<'_> = Box::new(|| {
            let first = reads.fetch_add(1, Ordering::SeqCst) == 0;
            Box::pin(async move {
                if first {
                    Ok(())
                } else {
                    Err(
                        autumn_harvest::llm_budget::check_failed(&"the pool is down")
                            .into_error_payload(),
                    )
                }
            })
        });
        let (verdict, calls) = run_score(&harness, ScoreRequest::new("two", input()), check).await;
        let failure = failure(&verdict.unwrap_err());
        assert_eq!(
            failure.error_type,
            autumn_harvest::llm_budget::ERROR_TYPE_LLM_BUDGET_CHECK_FAILED
        );
        assert!(failure.non_retryable, "a retry would pay again unseen");
        assert_eq!(calls.len(), 1);
        assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_failed_budget_read_before_any_call_stays_retryable() {
        let model = judge();
        let harness = AgentHarness::new(model.clone()).scorer(Arc::new(TwoCalls));
        let check: BudgetCheck<'_> = Box::new(|| {
            Box::pin(async {
                Err(
                    autumn_harvest::llm_budget::check_failed(&"the pool is down")
                        .into_error_payload(),
                )
            })
        });
        let (verdict, calls) = run_score(&harness, ScoreRequest::new("two", input()), check).await;
        assert!(!failure(&verdict.unwrap_err()).non_retryable);
        assert_eq!(calls.len(), 0);
        assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    }
}
