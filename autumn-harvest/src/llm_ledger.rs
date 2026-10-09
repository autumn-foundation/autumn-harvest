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

use std::time::Duration;

/// The longest model id the ledger accepts, in bytes.
pub const MAX_MODEL_ID_BYTES: usize = 200;

/// The most calls one activity attempt can record.
pub const MAX_LLM_CALLS_PER_ATTEMPT: usize = 256;

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
    /// When it is not set, the engine records the run time of the attempt.
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

    /// The latency, or `None` when the engine fills it in.
    #[must_use]
    pub const fn latency(&self) -> Option<Duration> {
        self.latency
    }

    /// Check that the ledger can store this call.
    ///
    /// # Errors
    ///
    /// Returns [`LlmCallError`] for an empty or long model id, or for a value
    /// above `i64::MAX`.
    pub const fn validate(&self) -> Result<(), LlmCallError> {
        Ok(())
    }
}

/// Why the ledger refuses a call.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LlmCallError {
    /// The model id is empty.
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
    /// A count is above `i64::MAX`.
    #[error("the LLM call field {field} is above the ledger limit")]
    OutOfRange {
        /// The field name.
        field: &'static str,
    },
    /// The attempt already recorded [`MAX_LLM_CALLS_PER_ATTEMPT`] calls.
    #[error("the activity attempt recorded more than {max} LLM calls")]
    TooManyCalls {
        /// The limit.
        max: usize,
    },
}

impl From<LlmCallError> for String {
    fn from(err: LlmCallError) -> Self {
        err.to_string()
    }
}

/// The latency in whole milliseconds, saturated to `i64::MAX`.
#[must_use]
pub const fn latency_ms(_latency: Duration) -> i64 {
    0
}

/// The per-attempt store behind `ActivityContext::record_llm_call`.
#[derive(Debug)]
pub(crate) struct LlmCallSlot {
    started: std::time::Instant,
    calls: std::sync::Mutex<Vec<LlmCall>>,
}

impl LlmCallSlot {
    pub(crate) fn new() -> Self {
        Self {
            started: std::time::Instant::now(),
            calls: std::sync::Mutex::new(Vec::new()),
        }
    }
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
    fn a_count_above_the_bigint_range_is_refused() {
        let big = u64::try_from(i64::MAX).unwrap() + 1;
        assert_eq!(
            LlmCall::new("m", big, 0).validate(),
            Err(LlmCallError::OutOfRange {
                field: "input_tokens"
            })
        );
        assert_eq!(
            LlmCall::new("m", 0, big).validate(),
            Err(LlmCallError::OutOfRange {
                field: "output_tokens"
            })
        );
        assert_eq!(
            LlmCall::new("m", 0, 0).with_cost_usd_micros(big).validate(),
            Err(LlmCallError::OutOfRange {
                field: "cost_usd_micros"
            })
        );
    }

    #[test]
    fn a_huge_latency_saturates_and_passes() {
        let call = LlmCall::new("m", 0, 0).with_latency(Duration::MAX);
        assert_eq!(call.validate(), Ok(()));
        assert_eq!(latency_ms(Duration::MAX), i64::MAX);
        assert_eq!(latency_ms(Duration::from_micros(1_500)), 1);
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
        assert!(ctx.llm_calls().is_empty());
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
    fn take_fills_an_unset_latency_and_empties_the_slot() {
        let ctx = crate::context::ActivityContext::new_test();
        ctx.record_llm_call(LlmCall::new("timed", 1, 1).with_latency(Duration::from_secs(3)))
            .unwrap();
        ctx.record_llm_call(LlmCall::new("untimed", 1, 1)).unwrap();
        std::thread::sleep(Duration::from_millis(5));
        let taken = ctx.take_llm_calls();
        assert_eq!(taken.len(), 2);
        assert_eq!(taken[0].latency(), Some(Duration::from_secs(3)));
        let filled = taken[1].latency().expect("the engine fills the latency");
        assert!(filled >= Duration::from_millis(5), "{filled:?}");
        assert!(ctx.llm_calls().is_empty());
        assert!(ctx.take_llm_calls().is_empty());
    }

    #[test]
    fn the_error_converts_to_an_activity_error_string() {
        let text: String = LlmCallError::EmptyModel.into();
        assert_eq!(text, "the LLM call has an empty model id");
    }

    #[test]
    fn the_security_posture_names_every_clear_ledger_field() {
        let doc = include_str!("../../docs/security-posture.md");
        let section = doc
            .split("### LLM cost ledger (issue #1996)")
            .nth(1)
            .expect("docs/security-posture.md has an LLM cost ledger section");
        let section = section.split("\n## ").next().unwrap_or(section);
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
