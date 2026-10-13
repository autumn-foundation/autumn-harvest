//! Bounded capture of stdout and stderr.
//!
//! Code can write without end. The capture keeps a fixed number of bytes and
//! counts the rest, so host memory stays bounded. It keeps the head and the
//! tail of a stream, because a traceback is at the end of stderr.

use std::collections::VecDeque;

use tokio::io::{AsyncRead, AsyncReadExt};

/// The text that marks the dropped middle of a stream.
pub const TRUNCATION_MARKER: &str = "\n[... output truncated ...]\n";

/// A stream capture that keeps the head and the tail.
#[derive(Debug)]
pub struct StreamCapture {
    head: Vec<u8>,
    tail: VecDeque<u8>,
    head_cap: usize,
    tail_cap: usize,
    total: u64,
}

/// The text of a finished capture.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapturedText {
    /// The kept text. Invalid UTF-8 becomes U+FFFD.
    pub text: String,
    /// All bytes that the stream carried.
    pub bytes: u64,
    /// Whether the capture dropped bytes from the middle.
    pub truncated: bool,
}

impl StreamCapture {
    /// A capture that keeps at most `cap` bytes.
    #[must_use]
    pub const fn new(cap: usize) -> Self {
        let head_cap = cap.div_ceil(2);
        Self {
            head: Vec::new(),
            tail: VecDeque::new(),
            head_cap,
            tail_cap: cap - head_cap,
            total: 0,
        }
    }

    /// Add bytes from the stream.
    pub fn push(&mut self, bytes: &[u8]) {
        self.total = self
            .total
            .saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        let room = self.head_cap - self.head.len();
        let (to_head, rest) = bytes.split_at(room.min(bytes.len()));
        self.head.extend_from_slice(to_head);
        if self.tail_cap == 0 {
            return;
        }
        // Keep only the last `tail_cap` bytes of the rest.
        let rest = &rest[rest.len().saturating_sub(self.tail_cap)..];
        let overflow = (self.tail.len() + rest.len()).saturating_sub(self.tail_cap);
        self.tail.drain(..overflow);
        self.tail.extend(rest);
    }

    /// All bytes that the stream carried so far.
    #[must_use]
    pub const fn total(&self) -> u64 {
        self.total
    }

    /// End the capture and decode the kept bytes.
    #[must_use]
    pub fn finish(self) -> CapturedText {
        let kept = self.head.len() + self.tail.len();
        let truncated = u64::try_from(kept).unwrap_or(u64::MAX) < self.total;
        let mut head = self.head;
        let text = if truncated {
            let tail: Vec<u8> = self.tail.into_iter().collect();
            format!("{}{TRUNCATION_MARKER}{}", decode(&head), decode(&tail))
        } else {
            // Nothing was dropped, so decode one buffer. Two decodes could
            // split a multi-byte character at the head and tail boundary.
            head.extend(self.tail);
            decode(&head)
        };
        CapturedText {
            text,
            bytes: self.total,
            truncated,
        }
    }
}

/// Decode bytes as lossy UTF-8, and replace each NUL with U+FFFD.
///
/// Postgres `jsonb` rejects `\u0000`, so a NUL in a recorded outcome would
/// fail the result insert.
fn decode(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).replace('\0', "\u{fffd}")
}

/// Read `reader` to its end into a capture of `cap` bytes.
///
/// The read keeps going past the cap, so the writer never blocks on a full
/// pipe. The capture drops the extra bytes.
///
/// # Errors
///
/// Returns the I/O error of the read.
pub async fn capture<R: AsyncRead + Unpin>(
    reader: &mut R,
    cap: usize,
) -> std::io::Result<StreamCapture> {
    let mut capture = StreamCapture::new(cap);
    let mut chunk = [0u8; 8192];
    loop {
        let n = reader.read(&mut chunk).await?;
        if n == 0 {
            return Ok(capture);
        }
        capture.push(&chunk[..n]);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn a_short_stream_is_kept_whole() {
        let mut cap = StreamCapture::new(16);
        cap.push(b"hello ");
        cap.push(b"world");
        let text = cap.finish();
        assert_eq!(text.text, "hello world");
        assert_eq!(text.bytes, 11);
        assert!(!text.truncated);
    }

    #[test]
    fn a_long_stream_keeps_the_head_and_the_tail() {
        let mut cap = StreamCapture::new(8);
        for chunk in [b"HEAD".as_slice(), b"middle-middle-middle", b"TAIL"] {
            cap.push(chunk);
        }
        let text = cap.finish();
        assert!(text.truncated);
        assert_eq!(text.bytes, 28);
        assert_eq!(text.text, format!("HEAD{TRUNCATION_MARKER}TAIL"));
    }

    #[test]
    fn a_stream_of_exactly_the_cap_is_not_truncated() {
        let mut cap = StreamCapture::new(4);
        cap.push(b"abcd");
        let text = cap.finish();
        assert_eq!(text.text, "abcd");
        assert!(!text.truncated);
    }

    #[test]
    fn a_zero_cap_keeps_nothing_and_counts_everything() {
        let mut cap = StreamCapture::new(0);
        cap.push(b"abc");
        let text = cap.finish();
        assert_eq!(text.bytes, 3);
        assert!(text.truncated);
        assert_eq!(text.text, TRUNCATION_MARKER);
    }

    #[test]
    fn kept_bytes_never_exceed_the_cap() {
        let mut cap = StreamCapture::new(10);
        for _ in 0..1000 {
            cap.push(&[b'x'; 97]);
        }
        assert_eq!(cap.total(), 97_000);
        let text = cap.finish();
        assert!(text.text.len() <= 10 + TRUNCATION_MARKER.len());
    }

    #[test]
    fn a_character_across_the_head_and_tail_boundary_survives() {
        let mut cap = StreamCapture::new(4);
        cap.push("a\u{e9}!".as_bytes());
        let text = cap.finish();
        assert_eq!(text.text, "a\u{e9}!");
        assert!(!text.truncated);
    }

    #[test]
    fn invalid_utf8_becomes_a_replacement_character() {
        let mut cap = StreamCapture::new(16);
        cap.push(&[b'a', 0xff, b'b']);
        assert_eq!(cap.finish().text, "a\u{fffd}b");
    }

    #[tokio::test]
    async fn the_reader_is_drained_past_the_cap() {
        let data = vec![b'z'; 100_000];
        let mut reader = data.as_slice();
        let cap = capture(&mut reader, 100).await.unwrap();
        assert_eq!(cap.total(), 100_000);
        assert!(reader.is_empty(), "the capture must read to the end");
    }
}
