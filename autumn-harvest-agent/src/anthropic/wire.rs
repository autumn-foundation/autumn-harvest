//! The rules of the Messages API that every caller shares.
//!
//! This module has no I/O. It holds the constants, the request settings of
//! each model, the error table, the usage counters and the prices.

use serde_json::{Value, json};

use crate::{ErrorKind, TokenUsage};

/// The Messages API endpoint.
pub const ENDPOINT: &str = "https://api.anthropic.com/v1/messages";

/// The API version header value.
///
/// This version refuses sampling settings. No request of this module sends
/// one.
pub const API_VERSION: &str = "2026-10-21";

/// The default model. Only this model gets the settings of [`apply_settings`].
pub const DEFAULT_MODEL: &str = "claude-opus-5-5";

/// The beta that turns on the server-side refusal fallback.
pub const FALLBACK_BETA: &str = "server-side-fallback-2026-07-01";

/// The output cap of one turn when the caller sets none.
pub const DEFAULT_MAX_TOKENS: u32 = 16_000;

/// The effort of the default model.
///
/// The model default is `medium`. Agent work needs more, so the request
/// sets it.
pub const EFFORT: &str = "high";

/// The stop reason of a turn that the model finished.
pub const STOP_END_TURN: &str = "end_turn";

/// The stop reason of a turn that stopped to call a tool.
pub const STOP_TOOL_USE: &str = "tool_use";

/// The stop reason of a turn that hit the output cap.
pub const STOP_MAX_TOKENS: &str = "max_tokens";

/// The stop reason of a turn that the model declined.
pub const STOP_REFUSAL: &str = "refusal";

/// Add the settings that `model` takes to a request `body`.
///
/// A model that does not take a setting refuses the whole request. This
/// module cannot read the capabilities of a model, only its name. So only
/// [`DEFAULT_MODEL`] gets adaptive thinking and [`EFFORT`]. Another model
/// gets the API defaults. That gives a weaker answer, not a failed turn.
///
/// The result depends on the model name only. So a replay of one session
/// sends the same settings on each turn.
pub fn apply_settings(body: &mut Value, model: &str) {
    if model == DEFAULT_MODEL {
        body["thinking"] = json!({"type": "adaptive"});
        body["output_config"] = json!({"effort": EFFORT});
    }
}

/// Ask for the server-side refusal fallback, when `model` takes it.
///
/// A fallback answers one turn on another model. The [`Client`] sends the
/// beta header for each body that holds `fallbacks`.
///
/// [`Client`]: super::Client
pub fn apply_fallbacks(body: &mut Value, model: &str) {
    if model == DEFAULT_MODEL {
        body["fallbacks"] = json!("default");
    }
}

/// The id of `model` with its settings, for a cache key and the cost ledger.
///
/// The id changes with each setting that changes an answer. It uses only the
/// characters that the ledger accepts: ASCII letters, digits and `._:/@+-`.
#[must_use]
pub fn model_id(model: &str, fallbacks: bool) -> String {
    if model != DEFAULT_MODEL {
        return model.to_owned();
    }
    let mut id = format!("{model}+effort:{EFFORT}");
    if fallbacks {
        id.push_str("+fallbacks:default");
    }
    id
}

/// The error kind of an HTTP status.
///
/// The table is the retry table of the Anthropic SDKs. A timeout, a
/// conflict, a rate limit and a server fault retry. `529` is an overload,
/// so it retries too. A request that is too large is a `Budget` error.
#[must_use]
pub const fn error_kind(status: u16) -> ErrorKind {
    match status {
        429 => ErrorKind::RateLimited,
        401 | 403 => ErrorKind::Authentication,
        413 => ErrorKind::Budget,
        408 | 409 | 500..=599 => ErrorKind::Unavailable,
        _ => ErrorKind::Provider,
    }
}

/// The error kind of an API error `type`, as an `error` stream event names
/// it.
#[must_use]
pub fn error_type_kind(kind: &str) -> ErrorKind {
    match kind {
        "rate_limit_error" => ErrorKind::RateLimited,
        "overloaded_error" | "api_error" | "timeout_error" => ErrorKind::Unavailable,
        "authentication_error" | "permission_error" => ErrorKind::Authentication,
        "request_too_large" => ErrorKind::Budget,
        _ => ErrorKind::Provider,
    }
}

/// Does `payload` have the shape of a Messages API response?
///
/// It needs a `content` array and a stop reason. The stop reason must not
/// be blank, and it must have no space around it. A trim would turn
/// ` tool_use ` into the reason that lets a tool run.
#[must_use]
pub fn is_message(payload: &Value) -> bool {
    payload.get("content").is_some_and(Value::is_array)
        && payload
            .get("stop_reason")
            .and_then(Value::as_str)
            .is_some_and(|reason| !reason.is_empty() && reason.trim() == reason)
}

/// The block types that [`is_replayable_block`] checks field by field.
pub const KNOWN_BLOCKS: [&str; 4] = ["text", "thinking", "redacted_thinking", "tool_use"];

/// Is one content block whole enough to send back to the API?
///
/// A block of a known type must carry the fields of that type. The API
/// refuses a block without them on the next request. A type that a later
/// API added is checked for its name only.
#[must_use]
pub fn is_replayable_block(block: &Value) -> bool {
    let Some(kind) = block.get("type").and_then(Value::as_str) else {
        return false;
    };
    let names = |field: &str| {
        block
            .get(field)
            .and_then(Value::as_str)
            .is_some_and(|text| !text.trim().is_empty())
    };
    match kind {
        // A text block needs one character at least. One space is valid.
        "text" => block
            .get("text")
            .and_then(Value::as_str)
            .is_some_and(|text| !text.is_empty()),
        // The default display returns an empty thinking text. The signature
        // carries the reasoning, so the signature must be there.
        "thinking" => block.get("thinking").is_some_and(Value::is_string) && names("signature"),
        "redacted_thinking" => names("data"),
        // A tool input has an object schema, so `null` is refused.
        "tool_use" => {
            names("id") && names("name") && block.get("input").is_some_and(Value::is_object)
        }
        // A padded known name is a damaged known block, not a new type.
        other => {
            let bare = other.trim();
            !bare.is_empty() && !KNOWN_BLOCKS.contains(&bare)
        }
    }
}

/// The token usage of one response body.
///
/// `input_tokens` holds the whole prompt: the uncached part, the cache
/// reads and the cache writes. The cache counters hold their parts.
#[must_use]
pub fn usage(body: &Value) -> TokenUsage {
    let count =
        |key: &str| u32::try_from(body["usage"][key].as_u64().unwrap_or(0)).unwrap_or(u32::MAX);
    let read = count("cache_read_input_tokens");
    let write = count("cache_creation_input_tokens");
    let mut usage = TokenUsage::new(
        count("input_tokens")
            .saturating_add(read)
            .saturating_add(write),
        count("output_tokens"),
    );
    usage.cache_read_tokens = read;
    usage.cache_write_tokens = write;
    usage
}

/// The price of one model, in billionths of a US dollar per token.
///
/// A price per million tokens in US dollars is the same number in millionths
/// of a dollar per token. This unit is a thousand times smaller again. So a
/// cache read of `$0.01` per million tokens is a whole number.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Price {
    /// One uncached prompt token.
    pub input: u64,
    /// One output token.
    pub output: u64,
    /// One prompt token read from the cache.
    pub cache_read: u64,
    /// One prompt token written to the cache, at the five-minute rate.
    pub cache_write: u64,
}

impl Price {
    /// A price from the input and output rates, the cache read rate, and a
    /// cache write at 1.25 times the input rate.
    const fn of(input: u64, output: u64, cache_read: u64) -> Self {
        Self {
            input,
            output,
            cache_read,
            cache_write: input * 5 / 4,
        }
    }
}

/// The prompt size above which Claude Haiku 5.5 bills at its long rate.
pub const HAIKU_LONG_PROMPT_TOKENS: u32 = 100_000;

/// The first-party price of `model` for a prompt of `prompt_tokens`.
///
/// The name must match exactly. An alias, a dated name or an unknown model
/// gets `None`, so the ledger counts the call as unpriced. The table holds
/// the standard rates. It does not price fast mode, the batch API or the
/// one-hour cache.
#[must_use]
pub const fn price(model: &str, prompt_tokens: u32) -> Option<Price> {
    let price = match model.as_bytes() {
        b"claude-fable-5-1" => Price::of(10_000, 50_000, 250),
        b"claude-fable-5" => Price::of(10_000, 50_000, 1_000),
        b"claude-opus-5-5" => Price::of(4_000, 20_000, 200),
        b"claude-opus-5" => Price::of(5_000, 25_000, 500),
        b"claude-sonnet-5-5" | b"claude-sonnet-5" => Price::of(2_000, 10_000, 200),
        b"claude-haiku-5-5" if prompt_tokens > HAIKU_LONG_PROMPT_TOKENS => {
            Price::of(500, 2_500, 50)
        }
        b"claude-haiku-5-5" => Price::of(100, 500, 10),
        _ => return None,
    };
    Some(price)
}

/// The cost of one call of `model`, in millionths of a US dollar.
///
/// The cost rounds to the nearest millionth. `None` means that the price of
/// `model` is not known.
#[must_use]
pub fn cost_usd_micros(model: &str, usage: &TokenUsage) -> Option<u64> {
    let price = price(model, usage.input_tokens)?;
    let uncached = usage
        .input_tokens
        .saturating_sub(usage.cache_read_tokens)
        .saturating_sub(usage.cache_write_tokens);
    let nanos = u128::from(uncached) * u128::from(price.input)
        + u128::from(usage.cache_read_tokens) * u128::from(price.cache_read)
        + u128::from(usage.cache_write_tokens) * u128::from(price.cache_write)
        + u128::from(usage.output_tokens) * u128::from(price.output);
    Some(u64::try_from(nanos.saturating_add(500) / 1_000).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use autumn_harvest::llm_ledger::LlmCall;

    use super::*;

    #[test]
    fn a_rate_limit_or_an_overload_retries_and_a_bad_request_does_not() {
        for status in [429, 529] {
            assert!(error_kind(status).is_retryable(), "{status}");
        }
        assert_eq!(error_kind(429), ErrorKind::RateLimited);
        assert_eq!(error_kind(529), ErrorKind::Unavailable);
        assert_eq!(error_kind(400), ErrorKind::Provider);
        assert!(!error_kind(400).is_retryable());
    }

    #[test]
    fn the_status_table_is_the_sdk_retry_table() {
        for status in [408, 409, 429, 500, 502, 503, 504, 529] {
            assert!(error_kind(status).is_retryable(), "{status}");
        }
        for status in [400, 401, 403, 404, 413, 422, 425] {
            assert!(!error_kind(status).is_retryable(), "{status}");
        }
        assert_eq!(error_kind(401), ErrorKind::Authentication);
        assert_eq!(error_kind(403), ErrorKind::Authentication);
        assert_eq!(error_kind(413), ErrorKind::Budget);
    }

    #[test]
    fn an_error_type_maps_as_its_status_does() {
        assert_eq!(error_type_kind("rate_limit_error"), ErrorKind::RateLimited);
        assert_eq!(error_type_kind("overloaded_error"), ErrorKind::Unavailable);
        assert_eq!(error_type_kind("api_error"), ErrorKind::Unavailable);
        assert_eq!(
            error_type_kind("authentication_error"),
            ErrorKind::Authentication
        );
        assert_eq!(error_type_kind("request_too_large"), ErrorKind::Budget);
        assert_eq!(
            error_type_kind("invalid_request_error"),
            ErrorKind::Provider
        );
    }

    #[test]
    fn only_the_default_model_gets_thinking_effort_and_fallbacks() {
        let mut known = json!({});
        apply_settings(&mut known, DEFAULT_MODEL);
        apply_fallbacks(&mut known, DEFAULT_MODEL);
        assert_eq!(known["thinking"], json!({"type": "adaptive"}));
        assert_eq!(known["output_config"]["effort"], EFFORT);
        assert_eq!(known["fallbacks"], "default");
        let mut other = json!({});
        apply_settings(&mut other, "claude-haiku-5-5");
        apply_fallbacks(&mut other, "claude-haiku-5-5");
        assert_eq!(other, json!({}));
    }

    #[test]
    fn every_model_id_passes_the_ledger_check() {
        for (model, fallbacks) in [
            (DEFAULT_MODEL, true),
            (DEFAULT_MODEL, false),
            ("claude-haiku-5-5", true),
        ] {
            let id = model_id(model, fallbacks);
            LlmCall::new(&id, 1, 1)
                .validate()
                .unwrap_or_else(|e| panic!("{id}: {e}"));
        }
        assert_ne!(
            model_id(DEFAULT_MODEL, true),
            model_id(DEFAULT_MODEL, false)
        );
        assert_eq!(model_id("claude-haiku-5-5", true), "claude-haiku-5-5");
    }

    #[test]
    fn usage_counts_the_whole_prompt_and_each_cache_part() {
        let body = json!({"usage": {
            "input_tokens": 100, "output_tokens": 20,
            "cache_read_input_tokens": 30, "cache_creation_input_tokens": 5
        }});
        let usage = usage(&body);
        assert_eq!(usage.input_tokens, 135);
        assert_eq!(usage.output_tokens, 20);
        assert_eq!(usage.cache_read_tokens, 30);
        assert_eq!(usage.cache_write_tokens, 5);
        assert_eq!(super::usage(&json!({})), TokenUsage::new(0, 0));
    }

    #[test]
    fn a_call_is_priced_from_its_four_counters() {
        // One million tokens of each kind on Opus 5.5: $4 + $0.20 + $5 + $20.
        let mut usage = TokenUsage::new(3_000_000, 1_000_000);
        usage.cache_read_tokens = 1_000_000;
        usage.cache_write_tokens = 1_000_000;
        assert_eq!(cost_usd_micros(DEFAULT_MODEL, &usage), Some(29_200_000));
        // One output token of Haiku 5.5 is half a millionth. It rounds up.
        assert_eq!(
            cost_usd_micros("claude-haiku-5-5", &TokenUsage::new(0, 1)),
            Some(1)
        );
        // Two cache reads of Haiku 5.5 are two hundredths. They round down.
        let mut cached = TokenUsage::new(2, 0);
        cached.cache_read_tokens = 2;
        assert_eq!(cost_usd_micros("claude-haiku-5-5", &cached), Some(0));
    }

    #[test]
    fn a_long_haiku_prompt_bills_at_the_long_rate() {
        let short = TokenUsage::new(HAIKU_LONG_PROMPT_TOKENS, 0);
        let long = TokenUsage::new(HAIKU_LONG_PROMPT_TOKENS + 1, 0);
        assert_eq!(cost_usd_micros("claude-haiku-5-5", &short), Some(10_000));
        assert_eq!(cost_usd_micros("claude-haiku-5-5", &long), Some(50_001));
    }

    #[test]
    fn an_unknown_or_aliased_model_is_unpriced() {
        let usage = TokenUsage::new(10, 10);
        for model in [
            "claude-opus-5-5-20260401",
            "acme-chat",
            "",
            "CLAUDE-OPUS-5-5",
        ] {
            assert_eq!(cost_usd_micros(model, &usage), None, "{model}");
        }
    }

    #[test]
    fn every_priced_cost_fits_the_ledger() {
        let usage = TokenUsage::new(u32::MAX, u32::MAX);
        let cost = cost_usd_micros("claude-fable-5-1", &usage).unwrap_or(0);
        LlmCall::new("claude-fable-5-1", 1, 1)
            .with_cost_usd_micros(cost)
            .validate()
            .unwrap_or_else(|e| panic!("{cost}: {e}"));
    }

    #[test]
    fn a_message_needs_content_and_an_exact_stop_reason() {
        assert!(is_message(
            &json!({"content": [], "stop_reason": "end_turn"})
        ));
        for bad in [
            json!({"content": []}),
            json!({"stop_reason": "end_turn"}),
            json!({"content": {}, "stop_reason": "end_turn"}),
            json!({"content": [], "stop_reason": ""}),
            json!({"content": [], "stop_reason": " tool_use "}),
        ] {
            assert!(!is_message(&bad), "{bad}");
        }
    }

    #[test]
    fn a_known_block_needs_its_fields_and_a_new_type_needs_a_name() {
        let good = [
            json!({"type": "text", "text": " "}),
            json!({"type": "thinking", "thinking": "", "signature": "s"}),
            json!({"type": "redacted_thinking", "data": "d"}),
            json!({"type": "tool_use", "id": "t", "name": "n", "input": {}}),
            json!({"type": "fallback"}),
        ];
        for block in good {
            assert!(is_replayable_block(&block), "{block}");
        }
        let bad = [
            json!({"type": "text", "text": ""}),
            json!({"type": "thinking", "thinking": ""}),
            json!({"type": "tool_use", "id": "t", "name": "n", "input": null}),
            json!({"type": " text "}),
            json!({"type": ""}),
            json!({}),
        ];
        for block in bad {
            assert!(!is_replayable_block(&block), "{block}");
        }
    }
}
