//! Follow-ups: the agent books its own next wake-up.
//!
//! A run with [`Followups`] set offers the model the `schedule_followup`
//! tool. The workflow handles the call itself: no activity runs. When the
//! run ends, the workflow waits on a durable timer, then starts a new segment
//! in the same conversation with the follow-up prompt.
//!
//! The timer is durable, so a restart keeps the wake-up. A chain cap and a
//! maximum delay stop an agent from waking itself forever.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::message::ToolDefinition;

/// The name of the follow-up tool.
pub const FOLLOWUP_TOOL: &str = "schedule_followup";

/// The longest follow-up prompt, in characters.
pub const MAX_PROMPT_CHARS: usize = 2_000;

/// The default cap on follow-ups in one chain.
pub const DEFAULT_MAX_CHAIN: u32 = 10;

/// The follow-up settings of a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Followups {
    /// The longest delay a follow-up may ask for, in seconds.
    pub max_delay_secs: u64,
    /// The most follow-ups one run may chain.
    pub max_chain: u32,
}

impl Followups {
    /// Follow-ups up to `max_delay`, with the default chain cap.
    #[must_use]
    pub const fn new(max_delay: Duration) -> Self {
        Self {
            max_delay_secs: max_delay.as_secs(),
            max_chain: DEFAULT_MAX_CHAIN,
        }
    }

    /// Set the chain cap.
    #[must_use]
    pub const fn max_chain(mut self, max_chain: u32) -> Self {
        self.max_chain = max_chain;
        self
    }
}

/// The definition the model sees.
#[must_use]
pub fn definition() -> ToolDefinition {
    ToolDefinition {
        name: FOLLOWUP_TOOL.to_owned(),
        description: "Wake yourself up later. After delay_minutes, a new turn starts \
                      with `prompt` in this conversation, and its answer goes to the \
                      user. Use it to check back on something that is not ready yet."
            .to_owned(),
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "delay_minutes": {"type": "integer", "minimum": 1},
                "prompt": {"type": "string", "description": "What to do when you wake up."}
            },
            "required": ["delay_minutes", "prompt"]
        }),
    }
}

/// One planned follow-up.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Planned {
    pub(crate) prompt: String,
    pub(crate) delay_secs: u64,
}

/// Check one follow-up call.
///
/// `chain` counts the follow-ups that already ran. `pending` is `true` when
/// this segment already booked one.
pub(crate) fn plan(
    arguments: &serde_json::Value,
    settings: &Followups,
    chain: u32,
    pending: bool,
) -> Result<Planned, String> {
    if pending {
        return Err("a follow-up is already scheduled in this turn".to_owned());
    }
    if chain >= settings.max_chain {
        return Err(format!(
            "no more follow-ups: this chain reached its cap of {}",
            settings.max_chain
        ));
    }
    let prompt = arguments
        .get("prompt")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|prompt| !prompt.is_empty())
        .ok_or("give a `prompt` for the follow-up")?;
    if prompt.chars().count() > MAX_PROMPT_CHARS {
        return Err(format!(
            "the follow-up prompt must be at most {MAX_PROMPT_CHARS} characters"
        ));
    }
    let minutes = arguments
        .get("delay_minutes")
        .and_then(serde_json::Value::as_u64)
        .filter(|minutes| *minutes >= 1)
        .ok_or("give `delay_minutes` as a whole number of at least 1")?;
    let delay_secs = minutes.saturating_mul(60);
    if delay_secs > settings.max_delay_secs {
        return Err(format!(
            "the delay must be at most {} minutes",
            settings.max_delay_secs / 60
        ));
    }
    Ok(Planned {
        prompt: prompt.to_owned(),
        delay_secs,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn settings() -> Followups {
        Followups::new(Duration::from_secs(3_600)).max_chain(2)
    }

    #[test]
    fn a_valid_call_is_planned() {
        let planned = plan(
            &json!({"prompt": " check ", "delay_minutes": 5}),
            &settings(),
            0,
            false,
        )
        .unwrap();
        assert_eq!(planned.prompt, "check");
        assert_eq!(planned.delay_secs, 300);
    }

    #[test]
    fn each_bound_is_refused_with_a_reason() {
        let s = settings();
        let ok = json!({"prompt": "p", "delay_minutes": 5});
        assert!(
            plan(&ok, &s, 0, true)
                .unwrap_err()
                .contains("already scheduled")
        );
        assert!(plan(&ok, &s, 2, false).unwrap_err().contains("cap of 2"));
        assert!(
            plan(&json!({"delay_minutes": 5}), &s, 0, false)
                .unwrap_err()
                .contains("prompt")
        );
        assert!(
            plan(&json!({"prompt": "p"}), &s, 0, false)
                .unwrap_err()
                .contains("delay_minutes")
        );
        assert!(plan(&json!({"prompt": "p", "delay_minutes": 0}), &s, 0, false).is_err());
        assert!(
            plan(&json!({"prompt": "p", "delay_minutes": 61}), &s, 0, false)
                .unwrap_err()
                .contains("at most 60 minutes")
        );
        let long = "x".repeat(MAX_PROMPT_CHARS + 1);
        assert!(plan(&json!({"prompt": long, "delay_minutes": 1}), &s, 0, false).is_err());
    }

    #[test]
    fn the_definition_names_the_tool() {
        assert_eq!(definition().name, FOLLOWUP_TOOL);
    }
}
