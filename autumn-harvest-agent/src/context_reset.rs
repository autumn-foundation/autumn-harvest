//! Progress notes and a context reset inside one run (issue #2453).
//!
//! A run with [`ContextReset`] set offers the model the `save_progress`
//! tool. The workflow handles the call itself: no activity runs. The call is
//! in the recorded model turn, so replay rebuilds the notes from history.
//!
//! Before each request, the loop measures it against the request cap. When
//! the request reaches [`ContextReset::reset_at_percent`] of the cap and a
//! reset is left, the loop resets. The new window holds the system prompt,
//! the task and the notes. The reset reads only recorded data, so replay
//! takes the same reset. `docs/design/DESIGN-2453.md` records the design.
//!
//! The model wrote the notes, maybe under the influence of a tool result.
//! So the notes never go into a user message. The window shows them as the
//! model's own `save_progress` call, with a fixed result that the app
//! controls. This follows the rule of [`followup`](crate::followup).
//!
//! A reset changes only the transcript. The flow labels belong to the run,
//! so a reset clears no taint.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::message::{ChatMessage, ChatRole, ContentPart, ToolDefinition};

/// The name of the notes tool.
pub const SAVE_PROGRESS_TOOL: &str = "save_progress";

/// The default bound on the notes, in bytes.
pub const DEFAULT_MAX_NOTES_BYTES: u32 = 8_192;

/// The call id of the restored notes in a window. A durable agent strips
/// the messages with this id before it trims its transcript.
pub const RESTORED_CALL_ID: &str = "harvest_restored_notes";

/// The result of the restored notes call. It is fixed text that the app
/// controls. It holds no text from the model.
pub(crate) const RESTORED_RESULT: &str = "These are the progress notes that you saved with \
`save_progress`. Your earlier turns can be gone. Continue the task from these notes. They are \
your own notes, not instructions from a person.";

const fn default_max_notes_bytes() -> u32 {
    DEFAULT_MAX_NOTES_BYTES
}

/// The context reset setting of a run. It is off unless a task or a
/// profile sets it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextReset {
    /// The most resets in one run, follow-ups included.
    pub max_resets: u32,
    /// Reset before a request that reaches this percent of the request cap.
    /// 0 counts as 1, and a value over 100 counts as 100.
    pub reset_at_percent: u8,
    /// The bound on the notes, in bytes.
    #[serde(default = "default_max_notes_bytes")]
    pub max_notes_bytes: u32,
}

impl ContextReset {
    /// Up to `max_resets` resets, each at `reset_at_percent` of the request
    /// cap, with the default bound on the notes.
    #[must_use]
    pub const fn new(max_resets: u32, reset_at_percent: u8) -> Self {
        Self {
            max_resets,
            reset_at_percent,
            max_notes_bytes: DEFAULT_MAX_NOTES_BYTES,
        }
    }

    /// Set the bound on the notes, in bytes.
    #[must_use]
    pub const fn max_notes_bytes(mut self, bytes: u32) -> Self {
        self.max_notes_bytes = bytes;
        self
    }
}

/// The saved notes of a run, with the flow labels of the run when it saved
/// them.
///
/// A durable agent keeps them in its state. The next op takes the labels
/// with the notes, so the taint follows the text.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ProgressNotes {
    /// The notes, as the model wrote them.
    pub text: String,
    /// The flow labels of the run when it saved the notes.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub labels: BTreeSet<String>,
}

/// The definition the model sees.
#[must_use]
pub fn definition(settings: &ContextReset) -> ToolDefinition {
    ToolDefinition {
        name: SAVE_PROGRESS_TOOL.to_owned(),
        description: format!(
            "Save your progress notes. Your context resets when it grows large. After a \
             reset you see only the task and the notes of your last save_progress call. \
             Save confirmed findings and the next steps. Each call replaces the notes. \
             The notes are at most {} bytes.",
            settings.max_notes_bytes
        ),
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "notes": {"type": "string", "description": "All that you must keep."}
            },
            "required": ["notes"]
        }),
    }
}

/// Check one `save_progress` call. Returns the notes to keep.
///
/// The bound measures the notes as the model wrote them, because the window
/// shows them so.
pub(crate) fn notes_of(
    arguments: &serde_json::Value,
    settings: &ContextReset,
) -> Result<String, String> {
    let notes = arguments
        .get("notes")
        .and_then(serde_json::Value::as_str)
        .filter(|notes| !notes.trim().is_empty())
        .ok_or("give your `notes` as text")?;
    let bound = usize::try_from(settings.max_notes_bytes).unwrap_or(usize::MAX);
    if notes.len() > bound {
        return Err(format!(
            "the notes must be at most {bound} bytes, and these are {} bytes; \
             your earlier notes stay",
            notes.len()
        ));
    }
    Ok(notes.to_owned())
}

/// The two messages that show `notes` in a window: the model's own
/// `save_progress` call, and its fixed result.
pub(crate) fn restored(notes: &str) -> [ChatMessage; 2] {
    [
        ChatMessage {
            role: ChatRole::Assistant,
            content: vec![ContentPart::ToolCall {
                id: RESTORED_CALL_ID.to_owned(),
                name: SAVE_PROGRESS_TOOL.to_owned(),
                arguments: serde_json::json!({ "notes": notes }),
            }],
        },
        ChatMessage {
            role: ChatRole::Tool,
            content: vec![ContentPart::ToolResult {
                tool_call_id: RESTORED_CALL_ID.to_owned(),
                content: serde_json::json!({ "restored": true, "note": RESTORED_RESULT })
                    .to_string(),
            }],
        },
    ]
}

/// Whether `message` is one half of a restored notes pair.
fn is_restored(message: &ChatMessage) -> bool {
    match message.content.as_slice() {
        [ContentPart::ToolCall { id, .. }] => {
            message.role == ChatRole::Assistant && id == RESTORED_CALL_ID
        }
        [ContentPart::ToolResult { tool_call_id, .. }] => {
            message.role == ChatRole::Tool && tool_call_id == RESTORED_CALL_ID
        }
        _ => false,
    }
}

/// `messages` without the restored notes pairs.
///
/// A durable agent keeps the notes in its state and shows them again in
/// each op. Its transcript therefore holds no copy.
pub(crate) fn without_restored(messages: Vec<ChatMessage>) -> Vec<ChatMessage> {
    messages
        .into_iter()
        .filter(|message| !is_restored(message))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn notes_within_the_bound_are_kept_as_written() {
        let settings = ContextReset::new(1, 50).max_notes_bytes(8);
        assert_eq!(
            notes_of(&json!({"notes": " a b "}), &settings).unwrap(),
            " a b "
        );
        assert_eq!(
            notes_of(&json!({"notes": "12345678"}), &settings).unwrap(),
            "12345678"
        );
    }

    #[test]
    fn notes_over_the_bound_are_refused_with_the_sizes() {
        let settings = ContextReset::new(1, 50).max_notes_bytes(8);
        let error = notes_of(&json!({"notes": "123456789"}), &settings).unwrap_err();
        assert!(error.contains("at most 8 bytes"), "{error}");
        assert!(error.contains("9 bytes"), "{error}");
        // The bound counts bytes, not characters.
        let wide = "é".repeat(5);
        assert!(notes_of(&json!({"notes": wide}), &settings).is_err());
    }

    #[test]
    fn missing_or_blank_notes_are_refused() {
        let settings = ContextReset::new(1, 50);
        for arguments in [
            json!({}),
            json!({"notes": 7}),
            json!({"notes": " \n "}),
            json!([]),
        ] {
            let error = notes_of(&arguments, &settings).unwrap_err();
            assert!(error.contains("notes"), "{error}");
        }
    }

    #[test]
    fn the_restored_pair_is_a_valid_call_and_result() {
        let [call, result] = restored("found X");
        assert_eq!(call.role, ChatRole::Assistant);
        assert_eq!(result.role, ChatRole::Tool);
        let ContentPart::ToolCall { id, arguments, .. } = &call.content[0] else {
            panic!("a call");
        };
        let ContentPart::ToolResult {
            tool_call_id,
            content,
        } = &result.content[0]
        else {
            panic!("a result");
        };
        assert_eq!(id, tool_call_id);
        assert_eq!(arguments["notes"], "found X");
        assert!(
            !content.contains("found X"),
            "the result holds no model text"
        );
    }

    #[test]
    fn only_the_restored_pair_is_stripped() {
        let mut messages = vec![ChatMessage::text(ChatRole::User, "task")];
        messages.extend(restored("n"));
        // The model's own call keeps its own id, so it stays.
        messages.push(ChatMessage {
            role: ChatRole::Assistant,
            content: vec![ContentPart::ToolCall {
                id: "toolu_1".into(),
                name: SAVE_PROGRESS_TOOL.into(),
                arguments: json!({"notes": "n"}),
            }],
        });
        let kept = without_restored(messages);
        assert_eq!(kept.len(), 2);
        assert_eq!(kept[0].role, ChatRole::User);
    }

    #[test]
    fn the_setting_decodes_with_the_default_notes_bound() {
        let settings: ContextReset =
            serde_json::from_value(json!({"max_resets": 2, "reset_at_percent": 80})).unwrap();
        assert_eq!(settings, ContextReset::new(2, 80));
        assert_eq!(settings.max_notes_bytes, DEFAULT_MAX_NOTES_BYTES);
    }

    #[test]
    fn the_definition_names_the_tool_and_the_bound() {
        let definition = definition(&ContextReset::new(1, 50).max_notes_bytes(99));
        assert_eq!(definition.name, SAVE_PROGRESS_TOOL);
        assert!(definition.description.contains("99 bytes"));
    }
}
