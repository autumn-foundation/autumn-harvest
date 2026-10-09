//! Agent cost ledger: model, tokens, cost and latency per LLM step (issue
//! #1996).
//!
//! An activity records each model call with
//! [`ActivityContext::record_llm_call`](crate::context::ActivityContext::record_llm_call).
//! When the attempt completes, the engine writes one `harvest_llm_ledger` row
//! per call. The write is in the transaction that appends the completion
//! event, so the row and the event commit together.
//!
//! The ledger is outside `harvest_events`. History does not change, so replay
//! does not change. The payload codec does not cover the table, so SQL usage
//! reports can sum it without a key. The model id and the counts are in
//! clear. See `docs/security-posture.md`.

use std::time::{Duration, Instant};

/// The longest model id the ledger accepts, in bytes.
pub const MAX_MODEL_ID_BYTES: usize = 200;

/// The characters a model id can hold, besides ASCII letters and digits.
///
/// A model id is a token, such as `claude-sonnet-5-5` or
/// `models/gemini-2.0:latest`. No space or control character can appear, so
/// a prompt or a sentence does not fit.
pub const MODEL_ID_PUNCTUATION: &str = "._:/@+-";

/// The most calls one activity attempt can record.
pub const MAX_LLM_CALLS_PER_ATTEMPT: usize = 256;

/// The most input or output tokens one call can record: 10^12.
pub const MAX_TOKENS_PER_CALL: u64 = 1_000_000_000_000;

/// The highest cost one call can record: 10^15 millionths of a US dollar.
pub const MAX_COST_USD_MICROS: u64 = 1_000_000_000_000_000;

/// The longest latency the ledger stores: 10^10 ms, about 115 days.
///
/// A longer latency saturates to this value.
pub const MAX_LATENCY_MS: i64 = 10_000_000_000;

/// One model call, as an activity records it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LlmCall {
    model: String,
    input_tokens: u64,
    output_tokens: u64,
    cost_usd_micros: Option<u64>,
    latency: Option<Duration>,
}

impl LlmCall {
    /// A call to `model` that read `input_tokens` and wrote `output_tokens`.
    #[must_use]
    pub fn new(model: impl Into<String>, input_tokens: u64, output_tokens: u64) -> Self {
        Self {
            model: model.into(),
            input_tokens,
            output_tokens,
            cost_usd_micros: None,
            latency: None,
        }
    }

    /// Set the cost in millionths of a US dollar.
    #[must_use]
    pub const fn with_cost_usd_micros(mut self, cost: u64) -> Self {
        self.cost_usd_micros = Some(cost);
        self
    }

    /// Set the latency of the call.
    ///
    /// When it is not set, the context records the time since the previous
    /// call of the attempt, or since the attempt started.
    #[must_use]
    pub const fn with_latency(mut self, latency: Duration) -> Self {
        self.latency = Some(latency);
        self
    }

    /// The model id.
    #[must_use]
    pub fn model(&self) -> &str {
        &self.model
    }

    /// The input tokens.
    #[must_use]
    pub const fn input_tokens(&self) -> u64 {
        self.input_tokens
    }

    /// The output tokens.
    #[must_use]
    pub const fn output_tokens(&self) -> u64 {
        self.output_tokens
    }

    /// The cost in millionths of a US dollar, or `None` when unpriced.
    #[must_use]
    pub const fn cost_usd_micros(&self) -> Option<u64> {
        self.cost_usd_micros
    }

    /// The latency, or `None` before the context records the call.
    #[must_use]
    pub const fn latency(&self) -> Option<Duration> {
        self.latency
    }

    /// Check that the ledger can store this call.
    ///
    /// # Errors
    ///
    /// Returns [`LlmCallError`] for an empty or long model id, a model id with
    /// a character outside the token charset, or a count above its limit.
    pub fn validate(&self) -> Result<(), LlmCallError> {
        if self.model.trim().is_empty() {
            return Err(LlmCallError::EmptyModel);
        }
        if self.model.len() > MAX_MODEL_ID_BYTES {
            return Err(LlmCallError::ModelTooLong {
                len: self.model.len(),
                max: MAX_MODEL_ID_BYTES,
            });
        }
        // A NUL byte would fail the insert at commit and roll back the
        // completion. The token charset also keeps prompt text out.
        if !self
            .model
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || MODEL_ID_PUNCTUATION.contains(c))
        {
            return Err(LlmCallError::InvalidModelCharacter);
        }
        for (field, value, max) in [
            ("input_tokens", Some(self.input_tokens), MAX_TOKENS_PER_CALL),
            (
                "output_tokens",
                Some(self.output_tokens),
                MAX_TOKENS_PER_CALL,
            ),
            ("cost_usd_micros", self.cost_usd_micros, MAX_COST_USD_MICROS),
        ] {
            if value.is_some_and(|v| v > max) {
                return Err(LlmCallError::OutOfRange { field, max });
            }
        }
        Ok(())
    }
}

/// Why the ledger refuses a call.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum LlmCallError {
    /// The model id is empty or blank.
    #[error("the LLM call has an empty model id")]
    EmptyModel,
    /// The model id is longer than [`MAX_MODEL_ID_BYTES`].
    #[error("the LLM model id is {len} bytes; the limit is {max} bytes")]
    ModelTooLong {
        /// The length of the model id, in bytes.
        len: usize,
        /// The limit.
        max: usize,
    },
    /// The model id holds a character other than an ASCII letter, a digit
    /// or one of [`MODEL_ID_PUNCTUATION`].
    #[error(
        "the LLM model id holds a character outside A-Z, a-z, 0-9 and {}",
        MODEL_ID_PUNCTUATION
    )]
    InvalidModelCharacter,
    /// A count is above its limit.
    #[error("the LLM call field {field} is above its limit of {max}")]
    OutOfRange {
        /// The field name.
        field: &'static str,
        /// The limit.
        max: u64,
    },
    /// The attempt already recorded [`MAX_LLM_CALLS_PER_ATTEMPT`] calls.
    #[error("the activity attempt already recorded {max} LLM calls")]
    TooManyCalls {
        /// The limit.
        max: usize,
    },
}

/// A non-retryable activity failure, so `?` in an activity does not retry.
///
/// A refusal is deterministic. A retry would call the model and pay again.
impl From<LlmCallError> for String {
    fn from(err: LlmCallError) -> Self {
        use crate::failure::IntoActivityErrorString as _;
        crate::failure::ActivityFailure::non_retryable("LlmLedgerRefused", err.to_string())
            .into_error_payload()
    }
}

/// The latency in whole milliseconds, saturated to [`MAX_LATENCY_MS`].
#[cfg_attr(not(feature = "db"), allow(dead_code))]
pub(crate) fn latency_ms(latency: Duration) -> i64 {
    i64::try_from(latency.as_millis()).map_or(MAX_LATENCY_MS, |ms| ms.min(MAX_LATENCY_MS))
}

/// The calls of one attempt, and the time of the last record.
#[derive(Debug)]
struct SlotState {
    calls: Vec<LlmCall>,
    last_mark: Instant,
}

/// The per-attempt store behind `ActivityContext::record_llm_call`.
#[derive(Debug)]
pub(crate) struct LlmCallSlot {
    state: std::sync::Mutex<SlotState>,
}

impl LlmCallSlot {
    pub(crate) fn new() -> Self {
        Self {
            state: std::sync::Mutex::new(SlotState {
                calls: Vec::new(),
                last_mark: Instant::now(),
            }),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, SlotState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Check the call, then keep it.
    ///
    /// An unset latency becomes the time since the last record, or since the
    /// slot was made. So the filled latencies of an attempt add up to its run
    /// time.
    pub(crate) fn record(&self, mut call: LlmCall) -> Result<(), LlmCallError> {
        call.validate()?;
        let mut state = self.lock();
        if state.calls.len() >= MAX_LLM_CALLS_PER_ATTEMPT {
            return Err(LlmCallError::TooManyCalls {
                max: MAX_LLM_CALLS_PER_ATTEMPT,
            });
        }
        let now = Instant::now();
        let since_last = now.saturating_duration_since(state.last_mark);
        call.latency.get_or_insert(since_last);
        state.last_mark = now;
        state.calls.push(call);
        drop(state);
        Ok(())
    }

    /// A copy of the calls kept so far.
    pub(crate) fn snapshot(&self) -> Vec<LlmCall> {
        self.lock().calls.clone()
    }

    /// Take the calls and empty the slot.
    pub(crate) fn take(&self) -> Vec<LlmCall> {
        std::mem::take(&mut self.lock().calls)
    }
}

/// One `harvest_llm_ledger` row to insert.
#[cfg(feature = "db")]
#[derive(diesel::Insertable)]
#[diesel(table_name = crate::schema::harvest_llm_ledger)]
struct NewLedgerRow<'a> {
    workflow_exec_id: uuid::Uuid,
    event_id: i32,
    call_index: i32,
    activity_name: &'a str,
    model: &'a str,
    input_tokens: i64,
    output_tokens: i64,
    cost_usd_micros: Option<i64>,
    latency_ms: i64,
}

/// Write one ledger row per call, keyed by the completion event.
///
/// Call it in the transaction that appends the completion event, after the
/// append. The rows then commit with the event or not at all. An empty slice
/// writes nothing.
///
/// `record_llm_call` checks every call, so a call that fails its check here
/// is a bug. The write skips it and logs a warning, so the completion still
/// commits.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] if the insert fails.
#[cfg(feature = "db")]
pub(crate) async fn insert_ledger_rows(
    conn: &mut diesel_async::AsyncPgConnection,
    exec_id: crate::types::ExecutionId,
    event_id: i32,
    activity_name: &str,
    calls: &[LlmCall],
) -> crate::error::HarvestResult<()> {
    use diesel_async::RunQueryDsl as _;

    let as_bigint = |value: u64| i64::try_from(value).unwrap_or(i64::MAX);
    let mut rows = Vec::with_capacity(calls.len());
    for (index, call) in calls.iter().enumerate() {
        if let Err(err) = call.validate() {
            tracing::warn!(error = %err, activity = activity_name, "skipping an invalid LLM ledger call");
            continue;
        }
        rows.push(NewLedgerRow {
            workflow_exec_id: exec_id.as_uuid(),
            event_id,
            call_index: i32::try_from(index).unwrap_or(i32::MAX),
            activity_name,
            model: &call.model,
            input_tokens: as_bigint(call.input_tokens),
            output_tokens: as_bigint(call.output_tokens),
            cost_usd_micros: call.cost_usd_micros.map(as_bigint),
            latency_ms: call.latency.map_or(0, latency_ms),
        });
    }
    if rows.is_empty() {
        return Ok(());
    }
    diesel::insert_into(crate::schema::harvest_llm_ledger::table)
        .values(&rows)
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_valid_call_passes_the_check() {
        let call = LlmCall::new("claude-sonnet-5-5", 1_200, 340)
            .with_cost_usd_micros(8_700)
            .with_latency(Duration::from_millis(950));
        assert_eq!(call.validate(), Ok(()));
        assert_eq!(call.model(), "claude-sonnet-5-5");
        assert_eq!(call.input_tokens(), 1_200);
        assert_eq!(call.output_tokens(), 340);
        assert_eq!(call.cost_usd_micros(), Some(8_700));
        assert_eq!(call.latency(), Some(Duration::from_millis(950)));
    }

    #[test]
    fn an_empty_model_id_is_refused() {
        assert_eq!(
            LlmCall::new("", 1, 1).validate(),
            Err(LlmCallError::EmptyModel)
        );
        assert_eq!(
            LlmCall::new("   ", 1, 1).validate(),
            Err(LlmCallError::EmptyModel)
        );
    }

    #[test]
    fn a_long_model_id_is_refused() {
        let long = "m".repeat(MAX_MODEL_ID_BYTES + 1);
        assert_eq!(
            LlmCall::new(long, 1, 1).validate(),
            Err(LlmCallError::ModelTooLong {
                len: MAX_MODEL_ID_BYTES + 1,
                max: MAX_MODEL_ID_BYTES,
            })
        );
        let at_limit = "m".repeat(MAX_MODEL_ID_BYTES);
        assert_eq!(LlmCall::new(at_limit, 1, 1).validate(), Ok(()));
    }

    #[test]
    fn a_count_above_its_limit_is_refused() {
        let tokens = MAX_TOKENS_PER_CALL + 1;
        assert_eq!(
            LlmCall::new("m", tokens, 0).validate(),
            Err(LlmCallError::OutOfRange {
                field: "input_tokens",
                max: MAX_TOKENS_PER_CALL
            })
        );
        assert_eq!(
            LlmCall::new("m", 0, tokens).validate(),
            Err(LlmCallError::OutOfRange {
                field: "output_tokens",
                max: MAX_TOKENS_PER_CALL
            })
        );
        assert_eq!(
            LlmCall::new("m", 0, 0)
                .with_cost_usd_micros(MAX_COST_USD_MICROS + 1)
                .validate(),
            Err(LlmCallError::OutOfRange {
                field: "cost_usd_micros",
                max: MAX_COST_USD_MICROS
            })
        );
        let at_limit = LlmCall::new("m", MAX_TOKENS_PER_CALL, MAX_TOKENS_PER_CALL)
            .with_cost_usd_micros(MAX_COST_USD_MICROS);
        assert_eq!(at_limit.validate(), Ok(()));
    }

    #[test]
    fn a_model_id_outside_the_token_charset_is_refused() {
        for model in [
            "model\0x",
            "model\nx",
            "\u{7f}model",
            "summarise the contract",
            "modèle",
        ] {
            assert_eq!(
                LlmCall::new(model, 1, 1).validate(),
                Err(LlmCallError::InvalidModelCharacter),
                "{model:?}"
            );
        }
        for model in [
            "claude-sonnet-5-5",
            "models/gemini-2.0:latest",
            "anthropic.claude-v2:1",
            "org/llama+lora@v3",
        ] {
            assert_eq!(LlmCall::new(model, 1, 1).validate(), Ok(()), "{model}");
        }
    }

    #[test]
    fn a_huge_latency_saturates_to_the_ledger_limit() {
        let call = LlmCall::new("m", 0, 0).with_latency(Duration::MAX);
        assert_eq!(call.validate(), Ok(()));
        assert_eq!(latency_ms(Duration::MAX), MAX_LATENCY_MS);
        assert_eq!(latency_ms(Duration::from_micros(1_500)), 1);
    }

    #[test]
    fn the_rust_limits_match_the_table_checks() {
        let up = include_str!("../migrations/20261009050156_harvest_llm_ledger/up.sql");
        for check in [
            format!("octet_length(model) BETWEEN 1 AND {MAX_MODEL_ID_BYTES}"),
            format!("input_tokens BETWEEN 0 AND {MAX_TOKENS_PER_CALL}"),
            format!("output_tokens BETWEEN 0 AND {MAX_TOKENS_PER_CALL}"),
            format!("cost_usd_micros BETWEEN 0 AND {MAX_COST_USD_MICROS}"),
            format!("latency_ms BETWEEN 0 AND {MAX_LATENCY_MS}"),
            format!("call_index BETWEEN 0 AND {}", MAX_LLM_CALLS_PER_ATTEMPT - 1),
            format!("model ~ '^[A-Za-z0-9{MODEL_ID_PUNCTUATION}]+$'"),
        ] {
            assert!(up.contains(&check), "up.sql must hold `{check}`");
        }
    }

    #[test]
    fn the_context_keeps_each_recorded_call_in_order() {
        let ctx = crate::context::ActivityContext::new_test();
        ctx.record_llm_call(LlmCall::new("model-a", 10, 2)).unwrap();
        ctx.record_llm_call(LlmCall::new("model-b", 20, 4).with_cost_usd_micros(7))
            .unwrap();
        let calls = ctx.llm_calls();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].model(), "model-a");
        assert_eq!(calls[1].model(), "model-b");
        assert_eq!(calls[1].cost_usd_micros(), Some(7));
    }

    #[test]
    fn the_context_refuses_a_bad_call_and_keeps_none_of_it() {
        let ctx = crate::context::ActivityContext::new_test();
        assert_eq!(
            ctx.record_llm_call(LlmCall::new("", 1, 1)),
            Err(LlmCallError::EmptyModel)
        );
        assert_eq!(ctx.llm_calls(), Vec::new());
    }

    #[test]
    fn the_context_caps_the_calls_per_attempt() {
        let ctx = crate::context::ActivityContext::new_test();
        for _ in 0..MAX_LLM_CALLS_PER_ATTEMPT {
            ctx.record_llm_call(LlmCall::new("m", 1, 1)).unwrap();
        }
        assert_eq!(
            ctx.record_llm_call(LlmCall::new("m", 1, 1)),
            Err(LlmCallError::TooManyCalls {
                max: MAX_LLM_CALLS_PER_ATTEMPT
            })
        );
        assert_eq!(ctx.llm_calls().len(), MAX_LLM_CALLS_PER_ATTEMPT);
    }

    #[test]
    fn record_fills_an_unset_latency_with_the_time_since_the_last_call() {
        let ctx = crate::context::ActivityContext::new_test();
        std::thread::sleep(Duration::from_millis(5));
        ctx.record_llm_call(LlmCall::new("timed", 1, 1).with_latency(Duration::from_secs(3)))
            .unwrap();
        std::thread::sleep(Duration::from_millis(5));
        ctx.record_llm_call(LlmCall::new("untimed", 1, 1)).unwrap();
        let calls = ctx.llm_calls();
        assert_eq!(calls[0].latency(), Some(Duration::from_secs(3)));
        let filled = calls[1].latency().expect("the context fills the latency");
        assert!(filled >= Duration::from_millis(5), "{filled:?}");
        assert!(
            filled < Duration::from_secs(3),
            "the fill counts from the last record, not the attempt start: {filled:?}"
        );
    }

    #[test]
    fn take_empties_the_slot() {
        let ctx = crate::context::ActivityContext::new_test();
        ctx.record_llm_call(LlmCall::new("m", 1, 1)).unwrap();
        assert_eq!(ctx.take_llm_calls().len(), 1);
        assert_eq!(ctx.llm_calls(), Vec::new());
        assert_eq!(ctx.take_llm_calls(), Vec::new());
    }

    #[test]
    fn the_error_converts_to_a_non_retryable_activity_failure() {
        let text: String = LlmCallError::EmptyModel.into();
        let failure =
            crate::failure::parse_typed_payload(&text).expect("a typed activity failure payload");
        assert!(
            failure.non_retryable,
            "a refusal must not retry and pay again"
        );
        assert_eq!(failure.error_type, "LlmLedgerRefused");
        assert_eq!(failure.message, "the LLM call has an empty model id");
    }

    #[test]
    fn the_security_posture_names_every_clear_ledger_field() {
        let doc = include_str!("../../docs/security-posture.md");
        let section = doc
            .split("### LLM cost ledger (issue #1996)")
            .nth(1)
            .expect("docs/security-posture.md has an LLM cost ledger section");
        let section = section
            .lines()
            .take_while(|line| !line.starts_with("## ") && !line.starts_with("### "))
            .collect::<Vec<_>>()
            .join("\n");
        for field in [
            "harvest_llm_ledger",
            "model",
            "activity_name",
            "input_tokens",
            "output_tokens",
            "cost_usd_micros",
            "latency_ms",
            "recorded_at",
        ] {
            assert!(
                section.contains(&format!("`{field}`")),
                "the ledger section must name `{field}`"
            );
        }
        assert!(
            section.contains("in clear"),
            "the ledger section must say that the fields are in clear"
        );
    }
}
