//! Server-Sent Events framing, shared by the provider models.

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

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

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
}
