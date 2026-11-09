//! Server-Sent Events of a streamed Messages API response.
//!
//! [`SseFrames`] splits the bytes into events. [`StreamAssembler`] builds
//! the message that a plain request returns from those events.

use std::collections::HashMap;

use serde_json::{Value, json};

use super::wire::error_type_kind;
use crate::{AgentError, ErrorKind};

/// Splits a Server-Sent Events byte stream into the `data` of each event.
///
/// An event ends at a blank line. A line ends at `\r\n`, `\n` or `\r`. A
/// network chunk can end inside a line, inside a `\r\n` pair or inside a
/// multi-byte character. So the parser keeps bytes, and it decodes a line
/// only when the line is whole. The rest waits for the next chunk.
#[derive(Debug, Default)]
pub struct SseFrames {
    /// The bytes of a line that is not whole yet.
    pending: Vec<u8>,
    /// The `data` lines of the event that is not whole yet.
    data: Vec<String>,
    /// The last chunk ended with `\r`. A `\n` at the start of the next
    /// chunk is the rest of that line end.
    after_cr: bool,
    /// The bytes of `pending` that hold no line end. A new chunk scans from
    /// here, so a long line costs linear time, not quadratic time.
    scanned: usize,
}

impl SseFrames {
    /// Add `bytes`. Returns the data of each event that they complete.
    ///
    /// # Errors
    ///
    /// A `Decode` error when a whole line is not UTF-8. A lossy decode puts
    /// U+FFFD in place of a bad byte, and that can change a streamed tool
    /// input.
    pub fn push(&mut self, mut bytes: &[u8]) -> Result<Vec<String>, AgentError> {
        if self.after_cr && !bytes.is_empty() {
            self.after_cr = false;
            bytes = bytes.strip_prefix(b"\n").unwrap_or(bytes);
        }
        self.pending.extend_from_slice(bytes);
        let mut out = Vec::new();
        let mut start = 0;
        let mut from = self.scanned;
        while let Some(at) = self.pending[from..]
            .iter()
            .position(|byte| matches!(byte, b'\n' | b'\r'))
        {
            let end = from + at;
            let mut next = end + 1;
            if self.pending[end] == b'\r' {
                match self.pending.get(next) {
                    Some(b'\n') => next += 1,
                    Some(_) => {}
                    // The `\n` of a `\r\n` pair can come in the next chunk.
                    None => self.after_cr = true,
                }
            }
            let line = text(&self.pending[start..end])?.to_owned();
            self.line(&line, &mut out);
            start = next;
            from = next;
        }
        self.pending.drain(..start);
        self.scanned = self.pending.len();
        Ok(out)
    }

    /// The data of an event that the stream ended without a blank line.
    ///
    /// # Errors
    ///
    /// A `Decode` error when the last line is not UTF-8.
    pub fn end(&mut self) -> Result<Vec<String>, AgentError> {
        let rest = std::mem::take(&mut self.pending);
        self.after_cr = false;
        self.scanned = 0;
        let mut out = Vec::new();
        if !rest.is_empty() {
            self.line(text(&rest)?, &mut out);
        }
        self.line("", &mut out);
        Ok(out)
    }

    /// Apply one whole line. A blank line ends the event.
    fn line(&mut self, line: &str, out: &mut Vec<String>) {
        if line.is_empty() {
            if !self.data.is_empty() {
                out.push(self.data.join("\n"));
                self.data.clear();
            }
        } else if let Some(data) = line.strip_prefix("data:") {
            self.data
                .push(data.strip_prefix(' ').unwrap_or(data).to_owned());
        }
    }
}

/// One whole line as text, or a `Decode` error.
fn text(line: &[u8]) -> Result<&str, AgentError> {
    std::str::from_utf8(line).map_err(|e| {
        AgentError::new(
            ErrorKind::Decode,
            format!(
                "a stream line is not UTF-8 text: the byte at {} begins no character",
                e.valid_up_to()
            ),
        )
    })
}

/// A `Decode` error for an event of the wrong shape.
fn malformed(what: &str) -> AgentError {
    AgentError::new(
        ErrorKind::Decode,
        format!("a stream event is malformed: {what}"),
    )
}

/// Builds the message of a streamed response from its events.
///
/// The result has the shape of a plain response body, so
/// [`parse_response`](super::parse_response) and
/// [`replayable`](super::replayable) read it.
#[derive(Debug, Default)]
pub struct StreamAssembler {
    message: Option<Value>,
    blocks: Vec<Value>,
    /// The input text of each tool-use block, by index.
    inputs: HashMap<usize, String>,
    stopped: bool,
}

impl StreamAssembler {
    /// Apply the data of one event. Pass each text delta to `on_delta`.
    ///
    /// Every other event passes `""` to `on_delta`. The harness joins small
    /// deltas, and it sends the text that waits only when `on_delta` runs.
    /// So the empty piece lets the text go out while the model thinks or
    /// writes a tool input.
    ///
    /// # Errors
    ///
    /// An `error` event is the error of its type. Data that is not JSON, or
    /// a delta for no block, is a `Decode` error.
    pub fn event(&mut self, data: &str, on_delta: &dyn Fn(&str)) -> Result<(), AgentError> {
        let event: Value = serde_json::from_str(data).map_err(|e| {
            AgentError::new(ErrorKind::Decode, format!("an event is not JSON: {e}"))
        })?;
        let is_text =
            event["type"] == "content_block_delta" && event["delta"]["type"] == "text_delta";
        self.apply(&event, on_delta)?;
        if !is_text {
            on_delta("");
        }
        Ok(())
    }

    /// Apply one parsed event.
    ///
    /// Each value that the assembler writes into must be an object. A
    /// `serde_json` index on another value panics, so the shape is checked.
    fn apply(&mut self, event: &Value, on_delta: &dyn Fn(&str)) -> Result<(), AgentError> {
        match event["type"].as_str() {
            Some("message_start") => {
                if !event["message"].is_object() {
                    return Err(malformed("`message_start` holds no message object"));
                }
                self.message = Some(event["message"].clone());
            }
            Some("content_block_start") => {
                let index = block_index(event)?;
                if !event["content_block"].is_object() {
                    return Err(malformed("`content_block_start` holds no block object"));
                }
                if self.blocks.len() <= index {
                    self.blocks.resize(index + 1, Value::Null);
                }
                self.blocks[index] = event["content_block"].clone();
            }
            Some("content_block_delta") => self.delta(event, on_delta)?,
            Some("content_block_stop") => self.close_block(block_index(event)?)?,
            Some("message_delta") => {
                let message = self
                    .message
                    .get_or_insert_with(|| json!({}))
                    .as_object_mut()
                    .ok_or_else(|| malformed("the message is not an object"))?;
                if let Some(delta) = event["delta"].as_object() {
                    for (key, value) in delta {
                        message.insert(key.clone(), value.clone());
                    }
                }
                if let Some(usage) = event["usage"].as_object() {
                    let counters = message
                        .entry("usage")
                        .or_insert_with(|| json!({}))
                        .as_object_mut()
                        .ok_or_else(|| malformed("the usage is not an object"))?;
                    for (key, value) in usage {
                        counters.insert(key.clone(), value.clone());
                    }
                }
            }
            Some("message_stop") => self.stopped = true,
            Some("error") => {
                let kind = error_type_kind(event["error"]["type"].as_str().unwrap_or_default());
                let detail = super::client::printable(
                    event["error"]["message"].as_str().unwrap_or("no message"),
                );
                return Err(AgentError::new(kind, format!("stream error: {detail}")));
            }
            // A ping, and any later event type, changes nothing.
            _ => {}
        }
        Ok(())
    }

    fn delta(&mut self, event: &Value, on_delta: &dyn Fn(&str)) -> Result<(), AgentError> {
        let index = block_index(event)?;
        let block = self
            .blocks
            .get_mut(index)
            .and_then(Value::as_object_mut)
            .ok_or_else(|| AgentError::new(ErrorKind::Decode, "a delta for no block"))?;
        let delta = &event["delta"];
        // Append in place. A copy of the whole text on each delta costs
        // quadratic time over a long answer.
        let append =
            |block: &mut serde_json::Map<String, Value>, key: &str, piece: &str| match block
                .entry(key)
                .or_insert_with(|| json!(""))
            {
                Value::String(text) => text.push_str(piece),
                other => *other = json!(piece),
            };
        match delta["type"].as_str() {
            Some("text_delta") => {
                let piece = delta["text"].as_str().unwrap_or_default();
                append(block, "text", piece);
                on_delta(piece);
            }
            Some("thinking_delta") => {
                append(
                    block,
                    "thinking",
                    delta["thinking"].as_str().unwrap_or_default(),
                );
            }
            Some("signature_delta") => {
                append(
                    block,
                    "signature",
                    delta["signature"].as_str().unwrap_or_default(),
                );
            }
            Some("input_json_delta") => {
                self.inputs
                    .entry(index)
                    .or_default()
                    .push_str(delta["partial_json"].as_str().unwrap_or_default());
            }
            _ => {}
        }
        Ok(())
    }

    /// Parse the whole input of a tool-use block. Eager input streaming
    /// leaves the check to the client, so input that is not JSON fails.
    fn close_block(&mut self, index: usize) -> Result<(), AgentError> {
        let Some(text) = self.inputs.remove(&index) else {
            return Ok(());
        };
        let input: Value = if text.trim().is_empty() {
            json!({})
        } else {
            serde_json::from_str(&text).map_err(|e| {
                AgentError::new(
                    ErrorKind::Decode,
                    format!("a streamed tool input is not JSON: {e}"),
                )
            })?
        };
        if let Some(block) = self.blocks.get_mut(index).and_then(Value::as_object_mut) {
            block.insert("input".to_owned(), input);
        }
        Ok(())
    }

    /// The whole message body.
    ///
    /// # Errors
    ///
    /// A stream with no `message_start` or no `message_stop` is a
    /// `Transport` error: it ended early, so a retry asks again. A tool input
    /// with no `content_block_stop` is a `Decode` error. The call would
    /// otherwise run with the empty input of its start event.
    pub fn finish(self) -> Result<Value, AgentError> {
        let (Some(mut message), true) = (self.message, self.stopped) else {
            return Err(AgentError::new(
                ErrorKind::Transport,
                "the stream ended before the message",
            ));
        };
        if !self.inputs.is_empty() {
            return Err(malformed("a tool input has no `content_block_stop`"));
        }
        message
            .as_object_mut()
            .ok_or_else(|| malformed("the message is not an object"))?
            .insert(
                "content".to_owned(),
                Value::Array(self.blocks.into_iter().filter(|b| !b.is_null()).collect()),
            );
        Ok(message)
    }
}

/// The `index` of a content-block event.
fn block_index(event: &Value) -> Result<usize, AgentError> {
    event["index"]
        .as_u64()
        .and_then(|index| usize::try_from(index).ok())
        .filter(|index| *index < 4096)
        .ok_or_else(|| AgentError::new(ErrorKind::Decode, "a content-block event has no index"))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::sync::Mutex;

    use super::*;
    use crate::anthropic::{parse_response, replayable};
    use crate::{ContentPart, StopReason};

    /// One SSE event with `data`.
    fn sse(data: &Value) -> String {
        format!("event: x\ndata: {data}\n\n")
    }

    fn streamed(events: &[Value]) -> Result<(Value, Vec<String>), AgentError> {
        streamed_in(events, 7)
    }

    /// Stream `events` in network chunks of `size` bytes.
    fn streamed_in(events: &[Value], size: usize) -> Result<(Value, Vec<String>), AgentError> {
        let wire: String = events.iter().map(sse).collect();
        let mut frames = SseFrames::default();
        let mut stream = StreamAssembler::default();
        let seen = Mutex::new(Vec::new());
        let on_delta = |piece: &str| seen.lock().unwrap().push(piece.to_owned());
        // Split the wire in odd places, as a network does.
        for chunk in wire.as_bytes().chunks(size) {
            for data in frames.push(chunk)? {
                stream.event(&data, &on_delta)?;
            }
        }
        for data in frames.end()? {
            stream.event(&data, &on_delta)?;
        }
        Ok((stream.finish()?, seen.into_inner().unwrap()))
    }

    fn start() -> Value {
        json!({"type": "message_start", "message": {"type": "message", "role": "assistant",
            "content": [], "stop_reason": null, "usage": {"input_tokens": 40, "output_tokens": 1}}})
    }

    #[test]
    fn a_stream_builds_the_message_of_a_plain_response() {
        let (body, deltas) = streamed(&[
            start(),
            json!({"type": "content_block_start", "index": 0,
                   "content_block": {"type": "thinking", "thinking": "", "signature": ""}}),
            json!({"type": "content_block_delta", "index": 0,
                   "delta": {"type": "signature_delta", "signature": "sig"}}),
            json!({"type": "content_block_stop", "index": 0}),
            json!({"type": "content_block_start", "index": 1,
                   "content_block": {"type": "text", "text": ""}}),
            json!({"type": "content_block_delta", "index": 1,
                   "delta": {"type": "text_delta", "text": "Hel"}}),
            json!({"type": "ping"}),
            json!({"type": "content_block_delta", "index": 1,
                   "delta": {"type": "text_delta", "text": "lo"}}),
            json!({"type": "content_block_stop", "index": 1}),
            json!({"type": "content_block_start", "index": 2,
                   "content_block": {"type": "tool_use", "id": "toolu_1", "name": "search",
                                     "input": {}}}),
            json!({"type": "content_block_delta", "index": 2,
                   "delta": {"type": "input_json_delta", "partial_json": "{\"q\": \"a"}}),
            json!({"type": "content_block_delta", "index": 2,
                   "delta": {"type": "input_json_delta", "partial_json": "b\"}"}}),
            json!({"type": "content_block_stop", "index": 2}),
            json!({"type": "message_delta", "delta": {"stop_reason": "tool_use"},
                   "usage": {"output_tokens": 12}}),
            json!({"type": "message_stop"}),
        ])
        .unwrap();
        let text: Vec<&String> = deltas.iter().filter(|d| !d.is_empty()).collect();
        assert_eq!(text, ["Hel", "lo"]);
        let response = parse_response(&body).unwrap();
        assert_eq!(response.stop_reason, StopReason::ToolUse);
        assert_eq!(response.content[0], ContentPart::Text("Hello".into()));
        assert!(matches!(&response.content[1],
            ContentPart::ToolCall { arguments, .. } if arguments["q"] == "ab"));
        assert_eq!(
            (response.usage.input_tokens, response.usage.output_tokens),
            (40, 12)
        );
        // The thinking block keeps its signature for the replay.
        let (_, raw) = replayable(&[], &body).unwrap();
        assert_eq!(raw[0]["signature"], "sig");
    }

    #[test]
    fn a_stream_that_breaks_off_or_fails_is_an_error() {
        let early = streamed(&[start()]).unwrap_err();
        assert_eq!(early.kind(), ErrorKind::Transport);
        let overloaded = streamed(&[
            start(),
            json!({"type": "error", "error": {"type": "overloaded_error", "message": "busy"}}),
        ])
        .unwrap_err();
        assert_eq!(overloaded.kind(), ErrorKind::Unavailable);
        let bad_input = streamed(&[
            start(),
            json!({"type": "content_block_start", "index": 0,
                   "content_block": {"type": "tool_use", "id": "t", "name": "n", "input": {}}}),
            json!({"type": "content_block_delta", "index": 0,
                   "delta": {"type": "input_json_delta", "partial_json": "{\"q\": "}}),
            json!({"type": "content_block_stop", "index": 0}),
        ])
        .unwrap_err();
        assert_eq!(bad_input.kind(), ErrorKind::Decode);
        let orphan = streamed(&[
            start(),
            json!({"type": "content_block_delta", "index": 3,
                   "delta": {"type": "text_delta", "text": "x"}}),
        ])
        .unwrap_err();
        assert_eq!(orphan.kind(), ErrorKind::Decode);
    }

    #[test]
    fn sse_frames_join_data_lines_and_skip_comments() {
        let mut frames = SseFrames::default();
        assert_eq!(
            frames.push(b": ping\n\nevent: a\r\ndata: 1").unwrap(),
            Vec::<String>::new()
        );
        assert_eq!(frames.push(b"\r\ndata: 2\r\n\r\n").unwrap(), ["1\n2"]);
        assert_eq!(frames.push(b"data: 3").unwrap(), Vec::<String>::new());
        assert_eq!(frames.end().unwrap(), ["3"]);
    }

    #[test]
    fn a_line_end_split_across_chunks_or_a_bare_cr_ends_a_line_once() {
        let mut frames = SseFrames::default();
        // A `\r\n` pair split over two chunks is one line end.
        assert_eq!(frames.push(b"data: 1\r").unwrap(), Vec::<String>::new());
        assert_eq!(frames.push(b"\ndata: 2\r").unwrap(), Vec::<String>::new());
        assert_eq!(frames.push(b"\n\r").unwrap(), ["1\n2"]);
        assert_eq!(frames.push(b"\n").unwrap(), Vec::<String>::new());
        // A bare `\r` ends a line, and a blank one ends the event.
        assert_eq!(frames.push(b"data: 3\rdata: 4\r\r").unwrap(), ["3\n4"]);
        assert_eq!(frames.end().unwrap(), Vec::<String>::new());
    }

    #[test]
    fn a_long_line_in_small_chunks_is_scanned_once() {
        let mut frames = SseFrames::default();
        for _ in 0..1000 {
            assert_eq!(frames.push(b"xxxxxxxx").unwrap(), Vec::<String>::new());
        }
        assert_eq!(frames.scanned, 8000, "each chunk scans its own bytes only");
        assert_eq!(frames.push(b"\n\n").unwrap(), Vec::<String>::new());
        assert_eq!(frames.scanned, 0);
    }

    #[test]
    fn a_line_that_is_not_utf8_is_refused() {
        // A streamed tool path with a raw `FF` byte must not become U+FFFD.
        let mut frames = SseFrames::default();
        let refused = frames
            .push(b"data: {\"p\": \"note\xff.md\"}\n")
            .unwrap_err();
        assert_eq!(refused.kind(), ErrorKind::Decode);
        let mut frames = SseFrames::default();
        frames.push(b"data: \xff").unwrap();
        assert_eq!(frames.end().unwrap_err().kind(), ErrorKind::Decode);
    }

    #[test]
    fn an_event_of_the_wrong_shape_is_an_error_and_not_a_panic() {
        let block = |content: Value| json!({"type": "content_block_start", "index": 0, "content_block": content});
        let cases = [
            vec![json!({"type": "message_start", "message": 1})],
            vec![start(), block(json!("x"))],
            vec![
                json!({"type": "message_delta", "delta": {}, "usage": {"output_tokens": 1}}),
                json!({"type": "message_delta", "delta": {"usage": 7},
                       "usage": {"output_tokens": 1}}),
            ],
        ];
        for events in cases {
            let error = streamed(&events).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::Decode, "{events:?}");
        }
    }

    #[test]
    fn a_tool_input_with_no_stop_is_refused() {
        let error = streamed(&[
            start(),
            json!({"type": "content_block_start", "index": 0,
                   "content_block": {"type": "tool_use", "id": "t", "name": "n", "input": {}}}),
            json!({"type": "content_block_delta", "index": 0,
                   "delta": {"type": "input_json_delta", "partial_json": "{\"path\": \"a\"}"}}),
            json!({"type": "message_delta", "delta": {"stop_reason": "tool_use"}}),
            json!({"type": "message_stop"}),
        ])
        .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Decode);
    }

    #[test]
    fn a_stream_error_message_loses_its_control_characters() {
        let error = streamed(&[
            start(),
            json!({"type": "error",
                   "error": {"type": "overloaded_error", "message": "busy\u{1b}[31m"}}),
        ])
        .unwrap_err();
        assert!(!error.message().contains('\u{1b}'), "{}", error.message());
    }

    #[test]
    fn a_character_split_across_chunks_stays_whole() {
        let text = "h\u{e9}llo \u{1f642}";
        let events = [
            start(),
            json!({"type": "content_block_start", "index": 0,
                   "content_block": {"type": "text", "text": ""}}),
            json!({"type": "content_block_delta", "index": 0,
                   "delta": {"type": "text_delta", "text": text}}),
            json!({"type": "content_block_stop", "index": 0}),
            json!({"type": "message_delta", "delta": {"stop_reason": "end_turn"}}),
            json!({"type": "message_stop"}),
        ];
        // One byte per chunk splits each character of more than one byte.
        let (body, deltas) = streamed_in(&events, 1).unwrap();
        assert_eq!(deltas.concat(), text);
        assert_eq!(body["content"][0]["text"], text);
    }

    #[test]
    fn every_event_that_is_not_text_passes_an_empty_piece() {
        let (_, deltas) = streamed(&[
            start(),
            json!({"type": "content_block_start", "index": 0,
                   "content_block": {"type": "text", "text": ""}}),
            json!({"type": "content_block_delta", "index": 0,
                   "delta": {"type": "text_delta", "text": "a"}}),
            json!({"type": "content_block_stop", "index": 0}),
            json!({"type": "content_block_start", "index": 1,
                   "content_block": {"type": "thinking", "thinking": "", "signature": ""}}),
            json!({"type": "content_block_delta", "index": 1,
                   "delta": {"type": "thinking_delta", "thinking": "hm"}}),
            json!({"type": "ping"}),
            json!({"type": "content_block_stop", "index": 1}),
            json!({"type": "message_delta", "delta": {"stop_reason": "end_turn"}}),
            json!({"type": "message_stop"}),
        ])
        .unwrap();
        // The harness sends the text that waits at each empty piece.
        assert_eq!(deltas, ["", "", "a", "", "", "", "", "", "", ""]);
    }
}
