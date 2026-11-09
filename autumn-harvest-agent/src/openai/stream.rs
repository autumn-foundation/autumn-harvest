//! The chunks of a streamed Chat Completions response.

use serde_json::{Value, json};

use super::wire::error_of;
use crate::{AgentError, ErrorKind};

/// One tool call of a stream, as its chunks build it.
#[derive(Debug, Default)]
struct Call {
    id: String,
    name: String,
    arguments: String,
}

/// The most tool calls that one streamed answer may hold.
const MAX_CALLS: u64 = 1024;

/// Builds the body of a plain response from the chunks of a stream.
///
/// The result has the shape of a plain response, so
/// [`parse_response`](super::parse_response) reads it.
#[derive(Debug, Default)]
pub struct ChunkAssembler {
    text: String,
    refusal: String,
    /// Each call with its stream index, in the order the calls began.
    calls: Vec<(u64, Call)>,
    finish: Option<String>,
    usage: Option<Value>,
    done: bool,
}

impl ChunkAssembler {
    /// Apply the data of one event. Pass each text delta to `on_delta`, and
    /// `""` at each other chunk.
    ///
    /// The harness joins small deltas, and it sends the text that waits only
    /// when `on_delta` runs. So the empty piece lets the text go out while
    /// the model writes a tool call.
    ///
    /// # Errors
    ///
    /// Data that is not JSON, or a tool call chunk of the wrong shape, is a
    /// `Decode` error. An `error` object is the error of its code.
    pub fn event(&mut self, data: &str, on_delta: &dyn Fn(&str)) -> Result<(), AgentError> {
        if data.trim() == "[DONE]" {
            self.done = true;
            on_delta("");
            return Ok(());
        }
        let chunk: Value = serde_json::from_str(data)
            .map_err(|e| AgentError::new(ErrorKind::Decode, format!("a chunk is not JSON: {e}")))?;
        if chunk.get("error").is_some_and(|error| !error.is_null()) {
            return Err(error_of(&chunk["error"]));
        }
        if chunk.get("usage").is_some_and(Value::is_object) {
            self.usage = Some(chunk["usage"].clone());
        }
        let mut piece = "";
        if let Some(choice) = chunk["choices"].as_array().and_then(|c| c.first()) {
            // `OpenRouter` puts an upstream error in the choice.
            if choice.get("error").is_some_and(|error| !error.is_null()) {
                return Err(error_of(&choice["error"]));
            }
            let delta = &choice["delta"];
            // A refusal comes in pieces. The parse refuses the whole of it.
            if let Some(piece) = delta["refusal"].as_str() {
                self.refusal.push_str(piece);
            }
            if let Some(text) = delta["content"].as_str() {
                self.text.push_str(text);
                piece = text;
            }
            for call in delta["tool_calls"]
                .as_array()
                .map(Vec::as_slice)
                .unwrap_or_default()
            {
                self.call(call)?;
            }
            if let Some(reason) = choice["finish_reason"].as_str() {
                self.finish = Some(reason.to_owned());
            }
        }
        on_delta(piece);
        Ok(())
    }

    /// Apply one tool call chunk. The first chunk of a call names it. Each
    /// chunk adds a piece of its arguments.
    fn call(&mut self, chunk: &Value) -> Result<(), AgentError> {
        let index = chunk["index"]
            .as_u64()
            .ok_or_else(|| AgentError::new(ErrorKind::Decode, "a tool call chunk has no index"))?;
        let id = chunk["id"].as_str().filter(|id| !id.is_empty());
        // Ollama sends each call whole, each at index 0. A new id at an index
        // that holds another call starts a new call.
        let fresh = self.calls.last().is_none_or(|(at, call)| {
            *at != index || id.is_some_and(|id| !call.id.is_empty() && call.id != id)
        });
        if fresh {
            let held = u64::try_from(self.calls.len()).unwrap_or(u64::MAX);
            if held >= MAX_CALLS {
                return Err(AgentError::new(
                    ErrorKind::Decode,
                    format!("a stream holds more than {MAX_CALLS} tool calls"),
                ));
            }
            if let Some(pos) = self.calls.iter().position(|(at, _)| *at == index)
                && id.is_none()
            {
                // A later piece of an earlier call at this index.
                let entry = self.calls.remove(pos);
                self.calls.push(entry);
            } else {
                self.calls.push((index, Call::default()));
            }
        }
        let Some((_, call)) = self.calls.last_mut() else {
            return Ok(());
        };
        if let Some(id) = id {
            id.clone_into(&mut call.id);
        }
        let function = &chunk["function"];
        // Most servers name a call in its first chunk only. Some repeat the
        // whole name in each chunk, so the name is set once.
        if let Some(name) = function["name"].as_str()
            && call.name.is_empty()
        {
            name.clone_into(&mut call.name);
        }
        match &function["arguments"] {
            Value::String(piece) => call.arguments.push_str(piece),
            Value::Null => {}
            // A server that sends the arguments whole sends an object.
            other => call.arguments.push_str(&other.to_string()),
        }
        Ok(())
    }

    /// The whole response body.
    ///
    /// # Errors
    ///
    /// A stream with no finish reason, or with no end marker and no usage,
    /// ended early. That is a `Transport` error, so a retry asks again.
    pub fn finish(self) -> Result<Value, AgentError> {
        let whole = self.finish.is_some() && (self.done || self.usage.is_some());
        if !whole {
            return Err(AgentError::new(
                ErrorKind::Transport,
                "the stream ended before the answer",
            ));
        }
        let calls: Vec<Value> = self
            .calls
            .into_iter()
            .map(|(_, call)| {
                let id = if call.id.is_empty() {
                    Value::Null
                } else {
                    json!(call.id)
                };
                json!({"id": id, "type": "function",
                       "function": {"name": call.name, "arguments": call.arguments}})
            })
            .collect();
        let mut message = json!({"role": "assistant", "content": self.text});
        if !calls.is_empty() {
            message["tool_calls"] = json!(calls);
        }
        if !self.refusal.is_empty() {
            message["refusal"] = json!(self.refusal);
        }
        Ok(json!({
            "choices": [{"index": 0, "message": message, "finish_reason": self.finish}],
            "usage": self.usage.unwrap_or_else(|| json!({})),
        }))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::sync::Mutex;

    use serde_json::json;

    use super::*;
    use crate::openai::parse_response;
    use crate::{ContentPart, StopReason};

    /// Apply each event, and return the body and the deltas.
    fn streamed(events: &[&str]) -> Result<(Value, Vec<String>), AgentError> {
        let mut stream = ChunkAssembler::default();
        let seen = Mutex::new(Vec::new());
        let on_delta = |piece: &str| seen.lock().unwrap().push(piece.to_owned());
        for data in events {
            stream.event(data, &on_delta)?;
        }
        Ok((stream.finish()?, seen.into_inner().unwrap()))
    }

    fn chunk(delta: &Value, finish: Option<&str>) -> String {
        json!({"choices": [{"index": 0, "delta": delta, "finish_reason": finish}]}).to_string()
    }

    #[test]
    fn a_stream_builds_the_body_of_a_plain_response() {
        let usage = json!({"choices": [], "usage": {"prompt_tokens": 7, "completion_tokens": 3,
            "prompt_tokens_details": {"cached_tokens": 4}}})
        .to_string();
        let events = [
            chunk(&json!({"role": "assistant", "content": ""}), None),
            chunk(&json!({"content": "Hel"}), None),
            chunk(&json!({"content": "lo"}), None),
            chunk(
                &json!({"tool_calls": [{"index": 0, "id": "call_a", "type": "function",
                    "function": {"name": "search", "arguments": ""}}]}),
                None,
            ),
            chunk(
                &json!({"tool_calls": [{"index": 0, "function": {"arguments": "{\"q\":"}}]}),
                None,
            ),
            chunk(
                &json!({"tool_calls": [{"index": 0, "function": {"arguments": "\"ab\"}"}}]}),
                None,
            ),
            chunk(&json!({}), Some("tool_calls")),
            usage,
            "[DONE]".to_owned(),
        ];
        let refs: Vec<&str> = events.iter().map(String::as_str).collect();
        let (body, deltas) = streamed(&refs).unwrap();
        let text: Vec<&String> = deltas.iter().filter(|d| !d.is_empty()).collect();
        assert_eq!(text, ["Hel", "lo"]);
        let response = parse_response(&body).unwrap();
        assert_eq!(response.stop_reason, StopReason::ToolUse);
        assert_eq!(response.content[0], ContentPart::Text("Hello".into()));
        assert!(matches!(&response.content[1],
            ContentPart::ToolCall { id, arguments, .. } if id == "call_a" && arguments["q"] == "ab"));
        assert_eq!(response.usage.input_tokens, 7);
        assert_eq!(response.usage.cache_read_tokens, 4);
        assert_eq!(response.usage.output_tokens, 3);
    }

    #[test]
    fn a_finish_with_the_end_marker_and_no_usage_is_whole() {
        let events = [
            chunk(&json!({"content": "a"}), Some("stop")),
            "[DONE]".to_owned(),
        ];
        let refs: Vec<&str> = events.iter().map(String::as_str).collect();
        let (body, _) = streamed(&refs).unwrap();
        assert_eq!(
            parse_response(&body).unwrap().stop_reason,
            StopReason::EndTurn
        );
    }

    #[test]
    fn a_stream_that_breaks_off_is_a_transport_error() {
        let no_finish = [chunk(&json!({"content": "a"}), None)];
        let finish_only = [chunk(&json!({"content": "a"}), Some("stop"))];
        for events in [&no_finish[..], &finish_only[..]] {
            let refs: Vec<&str> = events.iter().map(String::as_str).collect();
            assert_eq!(streamed(&refs).unwrap_err().kind(), ErrorKind::Transport);
        }
    }

    #[test]
    fn an_error_chunk_or_data_that_is_not_json_is_an_error() {
        let error = json!({"error": {"code": 502, "message": "upstream"}}).to_string();
        assert_eq!(
            streamed(&[&error]).unwrap_err().kind(),
            ErrorKind::Unavailable
        );
        assert_eq!(streamed(&["{"]).unwrap_err().kind(), ErrorKind::Decode);
        let odd = json!({"choices": [{"delta": {"tool_calls": [{"index": "x"}]}}]}).to_string();
        assert_eq!(streamed(&[&odd]).unwrap_err().kind(), ErrorKind::Decode);
    }

    #[test]
    fn two_calls_at_one_index_with_two_ids_stay_two_calls() {
        // Ollama sends each call whole, each at index 0.
        let call = |id: &str, name: &str, args: &str| {
            chunk(
                &json!({"tool_calls": [{"index": 0, "id": id, "type": "function",
                    "function": {"name": name, "arguments": args}}]}),
                None,
            )
        };
        let events = [
            call("call_a", "search", "{\"q\":1}"),
            call("call_b", "close", "{\"id\":2}"),
            chunk(&json!({}), Some("tool_calls")),
            "[DONE]".to_owned(),
        ];
        let refs: Vec<&str> = events.iter().map(String::as_str).collect();
        let (body, _) = streamed(&refs).unwrap();
        let response = parse_response(&body).unwrap();
        assert_eq!(response.content.len(), 2, "{response:?}");
        assert!(matches!(&response.content[1],
            ContentPart::ToolCall { id, name, .. } if id == "call_b" && name == "close"));
    }

    #[test]
    fn a_name_repeated_in_each_chunk_is_kept_once() {
        let piece = |args: &str| {
            chunk(
                &json!({"tool_calls": [{"index": 0,
                    "function": {"name": "search", "arguments": args}}]}),
                None,
            )
        };
        let events = [
            piece("{\"q\":"),
            piece("1}"),
            chunk(&json!({}), Some("tool_calls")),
            "[DONE]".to_owned(),
        ];
        let refs: Vec<&str> = events.iter().map(String::as_str).collect();
        let (body, _) = streamed(&refs).unwrap();
        assert!(matches!(&parse_response(&body).unwrap().content[0],
            ContentPart::ToolCall { name, .. } if name == "search"));
    }

    #[test]
    fn a_streamed_refusal_is_a_provider_error() {
        let events = [
            chunk(&json!({"refusal": "I cannot "}), None),
            chunk(&json!({"refusal": "help."}), Some("stop")),
            "[DONE]".to_owned(),
        ];
        let refs: Vec<&str> = events.iter().map(String::as_str).collect();
        let (body, _) = streamed(&refs).unwrap();
        let error = parse_response(&body).unwrap_err();
        assert_eq!(error.kind(), crate::ErrorKind::Provider);
        assert!(error.message().contains("I cannot help."), "{error}");
    }

    #[test]
    fn an_error_in_a_streamed_choice_is_an_error() {
        let event = json!({"choices": [{"index": 0, "delta": {}, "finish_reason": "error",
            "error": {"code": 502, "message": "upstream"}}]})
        .to_string();
        let error = streamed(&[&event, "[DONE]"]).unwrap_err();
        assert_eq!(error.kind(), crate::ErrorKind::Unavailable);
    }

    #[test]
    fn every_chunk_that_is_not_text_passes_an_empty_piece() {
        let events = [
            chunk(&json!({"role": "assistant"}), None),
            chunk(&json!({"content": "a"}), None),
            chunk(&json!({}), Some("stop")),
            "[DONE]".to_owned(),
        ];
        let refs: Vec<&str> = events.iter().map(String::as_str).collect();
        let (_, deltas) = streamed(&refs).unwrap();
        assert_eq!(deltas, ["", "a", "", ""]);
    }
}
