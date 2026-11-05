//! A port of the `PyYAML` reader and scanner: `yaml/reader.py` and
//! `yaml/scanner.py` of `PyYAML` 6.
//!
//! The audits of this unit report a YAML error in the words of `PyYAML`. They
//! also accept and reject the same documents as `PyYAML`. libyaml differs from
//! the pure Python scanner in both ways. For example, `PyYAML` rejects a tab
//! after a plain scalar, and libyaml accepts it. So this module ports the
//! Python scanner line by line, and each function keeps its Python name.
//!
//! The reader works on a `str`, as the scripts did. So it checks every
//! character first, and it appends a NUL to the buffer. Each mark counts
//! characters, not bytes.

use std::collections::VecDeque;

use super::pytext::repr_str;
use super::pyyaml::{Mark, YamlError};

/// The characters that end a plain token: NUL, space, tab and the breaks.
const BLANKZ: &str = "\0 \t\r\n\u{85}\u{2028}\u{2029}";
/// NUL and the line breaks.
const BREAKZ: &str = "\0\r\n\u{85}\u{2028}\u{2029}";
/// NUL, space and the line breaks, without the tab.
const SPACEZ: &str = "\0 \r\n\u{85}\u{2028}\u{2029}";
/// The line breaks.
const BREAKS: &str = "\r\n\u{85}\u{2028}\u{2029}";
const HEX: &str = "0123456789ABCDEFabcdef";

/// Returns true when `ch` is one of the characters of `set`.
fn is_in(ch: char, set: &str) -> bool {
    set.contains(ch)
}

/// Ports the alphanumeric test of the scanner: ASCII letters, digits, `-`
/// and `_`.
const fn is_word(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '-' || ch == '_'
}

/// Returns true for a character that `Reader.NON_PRINTABLE` accepts.
fn is_printable(ch: char) -> bool {
    matches!(ch, '\t' | '\n' | '\r' | '\x20'..='\x7e' | '\u{85}')
        || ('\u{a0}'..='\u{d7ff}').contains(&ch)
        || ('\u{e000}'..='\u{fffd}').contains(&ch)
        || ('\u{10000}'..='\u{10ffff}').contains(&ch)
}

/// The value of a directive token.
#[derive(Clone, Debug)]
pub enum DirectiveValue {
    /// A reserved directive. `PyYAML` skips it.
    None,
    /// `%YAML major.minor`. A number too large for `u64` is `None`.
    Yaml(Option<u64>, Option<u64>),
    /// `%TAG handle prefix`.
    Tag(String, String),
}

/// The kind of a token, with its value.
#[derive(Clone, Debug)]
pub enum TokenKind {
    Directive {
        name: String,
        value: DirectiveValue,
    },
    DocumentStart,
    DocumentEnd,
    StreamStart,
    StreamEnd,
    BlockSequenceStart,
    BlockMappingStart,
    BlockEnd,
    FlowSequenceStart,
    FlowMappingStart,
    FlowSequenceEnd,
    FlowMappingEnd,
    Key,
    Value,
    BlockEntry,
    FlowEntry,
    Alias(String),
    Anchor(String),
    Tag {
        handle: Option<String>,
        suffix: String,
    },
    Scalar {
        value: String,
        plain: bool,
    },
}

impl TokenKind {
    /// The `id` attribute of the `PyYAML` token class.
    pub const fn id(&self) -> &'static str {
        match self {
            Self::Directive { .. } => "<directive>",
            Self::DocumentStart => "<document start>",
            Self::DocumentEnd => "<document end>",
            Self::StreamStart => "<stream start>",
            Self::StreamEnd => "<stream end>",
            Self::BlockSequenceStart => "<block sequence start>",
            Self::BlockMappingStart => "<block mapping start>",
            Self::BlockEnd => "<block end>",
            Self::FlowSequenceStart => "[",
            Self::FlowMappingStart => "{",
            Self::FlowSequenceEnd => "]",
            Self::FlowMappingEnd => "}",
            Self::Key => "?",
            Self::Value => ":",
            Self::BlockEntry => "-",
            Self::FlowEntry => ",",
            Self::Alias(_) => "<alias>",
            Self::Anchor(_) => "<anchor>",
            Self::Tag { .. } => "<tag>",
            Self::Scalar { .. } => "<scalar>",
        }
    }
}

/// One token with its marks.
#[derive(Clone, Debug)]
pub struct Token {
    pub kind: TokenKind,
    pub start: Mark,
    pub end: Mark,
}

/// A place where a simple key may start.
#[derive(Clone, Debug)]
struct SimpleKey {
    token_number: usize,
    required: bool,
    index: usize,
    line: usize,
    column: usize,
    mark: Mark,
}

/// Renders a character as Python `repr` of a one-character `str`.
fn repr_char(ch: char) -> String {
    repr_str(&ch.to_string())
}

/// Builds a `ScannerError`.
fn scanner_error(
    context: Option<&str>,
    context_mark: Option<Mark>,
    problem: impl Into<String>,
    problem_mark: Mark,
) -> YamlError {
    YamlError::new(
        context.map(str::to_owned),
        context_mark,
        problem,
        Some(problem_mark),
    )
}

/// Ports `Reader` and `Scanner` for a `str` stream.
pub struct Scanner {
    /// The characters of the stream, with a NUL at the end.
    buffer: Vec<char>,
    /// The position of the next character. It is also the mark index.
    pointer: usize,
    line: usize,
    column: usize,
    done: bool,
    flow_level: i64,
    tokens: VecDeque<Token>,
    tokens_taken: usize,
    indent: i64,
    indents: Vec<i64>,
    allow_simple_key: bool,
    /// The possible simple keys by flow level, in the insertion order of the
    /// Python dict.
    possible_simple_keys: Vec<(i64, SimpleKey)>,
}

/// The result of one scanner step.
type Scan<T> = Result<T, YamlError>;

impl Scanner {
    /// Ports `Reader.__init__` and `Scanner.__init__`.
    ///
    /// The reader checks the whole stream first, so a special character fails
    /// the load before any token.
    pub fn new(text: &str) -> Scan<Self> {
        if let Some((position, ch)) = text.chars().enumerate().find(|(_, ch)| !is_printable(*ch)) {
            return Err(YamlError::reader(u32::from(ch), position));
        }
        let mut buffer: Vec<char> = text.chars().collect();
        buffer.push('\0');
        let mut scanner = Self {
            buffer,
            pointer: 0,
            line: 0,
            column: 0,
            done: false,
            flow_level: 0,
            tokens: VecDeque::new(),
            tokens_taken: 0,
            indent: -1,
            indents: Vec::new(),
            allow_simple_key: true,
            possible_simple_keys: Vec::new(),
        };
        scanner.fetch_stream_start();
        Ok(scanner)
    }

    // ── Reader ──────────────────────────────────────────────────────────────

    /// Ports `Reader.peek`. A position past the buffer reads as NUL.
    fn peek(&self, index: usize) -> char {
        self.buffer
            .get(self.pointer + index)
            .copied()
            .unwrap_or('\0')
    }

    /// Ports `Reader.prefix`.
    fn prefix(&self, length: usize) -> String {
        let end = (self.pointer + length).min(self.buffer.len());
        self.buffer[self.pointer.min(end)..end].iter().collect()
    }

    /// Ports `Reader.forward`. A BOM does not move the column.
    fn forward(&mut self, length: usize) {
        for _ in 0..length {
            let Some(&ch) = self.buffer.get(self.pointer) else {
                return;
            };
            self.pointer += 1;
            let next = self.peek(0);
            if is_in(ch, "\n\u{85}\u{2028}\u{2029}") || (ch == '\r' && next != '\n') {
                self.line += 1;
                self.column = 0;
            } else if ch != '\u{feff}' {
                self.column += 1;
            }
        }
    }

    /// Ports `Reader.get_mark`.
    const fn get_mark(&self) -> Mark {
        Mark {
            index: self.pointer,
            line: self.line,
            column: self.column,
        }
    }

    // ── Public methods ──────────────────────────────────────────────────────

    /// Fetches tokens until the first one is final, as `check_token` does.
    fn fill(&mut self) -> Scan<()> {
        while self.need_more_tokens()? {
            self.fetch_more_tokens()?;
        }
        Ok(())
    }

    /// Ports `peek_token`. The stream always ends with a stream end token,
    /// and the parser reads no further. So an empty queue is an error.
    pub fn peek_token(&mut self) -> Scan<&Token> {
        self.fill()?;
        self.tokens
            .front()
            .ok_or_else(|| YamlError::new(None, None, "no more tokens", None))
    }

    /// Ports `get_token`.
    pub fn get_token(&mut self) -> Scan<Token> {
        self.fill()?;
        let token = self
            .tokens
            .pop_front()
            .ok_or_else(|| YamlError::new(None, None, "no more tokens", None))?;
        self.tokens_taken += 1;
        Ok(token)
    }

    // ── Private methods ─────────────────────────────────────────────────────

    fn need_more_tokens(&mut self) -> Scan<bool> {
        if self.done {
            return Ok(false);
        }
        if self.tokens.is_empty() {
            return Ok(true);
        }
        // The current token may be a potential simple key, so the scanner
        // looks further.
        self.stale_possible_simple_keys()?;
        Ok(self.next_possible_simple_key() == Some(self.tokens_taken))
    }

    fn fetch_more_tokens(&mut self) -> Scan<()> {
        self.scan_to_next_token();
        self.stale_possible_simple_keys()?;
        self.unwind_indent(i64::try_from(self.column).unwrap_or(i64::MAX));
        let ch = self.peek(0);
        match ch {
            '\0' => {
                self.fetch_stream_end()?;
                return Ok(());
            }
            '%' if self.check_directive() => return self.fetch_directive(),
            '-' if self.check_document_start() => {
                return self.fetch_document_indicator(TokenKind::DocumentStart);
            }
            '.' if self.check_document_end() => {
                return self.fetch_document_indicator(TokenKind::DocumentEnd);
            }
            '[' => return self.fetch_flow_collection_start(TokenKind::FlowSequenceStart),
            '{' => return self.fetch_flow_collection_start(TokenKind::FlowMappingStart),
            ']' => return self.fetch_flow_collection_end(TokenKind::FlowSequenceEnd),
            '}' => return self.fetch_flow_collection_end(TokenKind::FlowMappingEnd),
            ',' => return self.fetch_flow_entry(),
            '-' if self.check_block_entry() => return self.fetch_block_entry(),
            '?' if self.check_key() => return self.fetch_key(),
            ':' if self.check_value() => return self.fetch_value(),
            '*' => return self.fetch_anchor(true),
            '&' => return self.fetch_anchor(false),
            '!' => return self.fetch_tag(),
            '|' if self.flow_level == 0 => return self.fetch_block_scalar(false),
            '>' if self.flow_level == 0 => return self.fetch_block_scalar(true),
            '\'' => return self.fetch_flow_scalar(false),
            '"' => return self.fetch_flow_scalar(true),
            _ => {}
        }
        if self.check_plain() {
            return self.fetch_plain();
        }
        Err(scanner_error(
            Some("while scanning for the next token"),
            None,
            format!(
                "found character {} that cannot start any token",
                repr_char(ch)
            ),
            self.get_mark(),
        ))
    }

    // ── Simple keys ─────────────────────────────────────────────────────────

    fn next_possible_simple_key(&self) -> Option<usize> {
        self.possible_simple_keys
            .iter()
            .map(|(_, key)| key.token_number)
            .min()
    }

    /// Removes the keys that can no longer start a simple key. A simple key
    /// stays on one line and is at most 1024 characters long.
    fn stale_possible_simple_keys(&mut self) -> Scan<()> {
        let mut kept = Vec::with_capacity(self.possible_simple_keys.len());
        for (level, key) in std::mem::take(&mut self.possible_simple_keys) {
            if key.line != self.line || self.pointer - key.index > 1024 {
                if key.required {
                    return Err(scanner_error(
                        Some("while scanning a simple key"),
                        Some(key.mark),
                        "could not find expected ':'",
                        self.get_mark(),
                    ));
                }
                continue;
            }
            kept.push((level, key));
        }
        self.possible_simple_keys = kept;
        Ok(())
    }

    /// Records the position of a possible simple key. A simple key is
    /// required at the first token of a block line.
    fn save_possible_simple_key(&mut self) -> Scan<()> {
        let required =
            self.flow_level == 0 && self.indent == i64::try_from(self.column).unwrap_or(i64::MAX);
        if self.allow_simple_key {
            self.remove_possible_simple_key()?;
            let key = SimpleKey {
                token_number: self.tokens_taken + self.tokens.len(),
                required,
                index: self.pointer,
                line: self.line,
                column: self.column,
                mark: self.get_mark(),
            };
            self.possible_simple_keys.push((self.flow_level, key));
        }
        Ok(())
    }

    fn remove_possible_simple_key(&mut self) -> Scan<()> {
        let level = self.flow_level;
        if let Some(position) = self
            .possible_simple_keys
            .iter()
            .position(|(key_level, _)| *key_level == level)
        {
            let (_, key) = self.possible_simple_keys.remove(position);
            if key.required {
                return Err(scanner_error(
                    Some("while scanning a simple key"),
                    Some(key.mark),
                    "could not find expected ':'",
                    self.get_mark(),
                ));
            }
        }
        Ok(())
    }

    // ── Indentation ─────────────────────────────────────────────────────────

    fn unwind_indent(&mut self, column: i64) {
        // In the flow context, indentation is ignored.
        if self.flow_level != 0 {
            return;
        }
        while self.indent > column {
            let mark = self.get_mark();
            self.indent = self.indents.pop().unwrap_or(-1);
            self.push(TokenKind::BlockEnd, mark, mark);
        }
    }

    fn add_indent(&mut self, column: usize) -> bool {
        let column = i64::try_from(column).unwrap_or(i64::MAX);
        if self.indent < column {
            self.indents.push(self.indent);
            self.indent = column;
            return true;
        }
        false
    }

    fn push(&mut self, kind: TokenKind, start: Mark, end: Mark) {
        self.tokens.push_back(Token { kind, start, end });
    }

    /// Ports `tokens.insert(key.token_number - tokens_taken, token)`.
    fn insert(&mut self, token_number: usize, kind: TokenKind, mark: Mark) {
        let at = token_number
            .saturating_sub(self.tokens_taken)
            .min(self.tokens.len());
        self.tokens.insert(
            at,
            Token {
                kind,
                start: mark,
                end: mark,
            },
        );
    }

    // ── Fetchers ────────────────────────────────────────────────────────────

    fn fetch_stream_start(&mut self) {
        let mark = self.get_mark();
        self.push(TokenKind::StreamStart, mark, mark);
    }

    fn fetch_stream_end(&mut self) -> Scan<()> {
        self.unwind_indent(-1);
        self.remove_possible_simple_key()?;
        self.allow_simple_key = false;
        self.possible_simple_keys.clear();
        let mark = self.get_mark();
        self.push(TokenKind::StreamEnd, mark, mark);
        self.done = true;
        Ok(())
    }

    fn fetch_directive(&mut self) -> Scan<()> {
        self.unwind_indent(-1);
        self.remove_possible_simple_key()?;
        self.allow_simple_key = false;
        let token = self.scan_directive()?;
        self.tokens.push_back(token);
        Ok(())
    }

    fn fetch_document_indicator(&mut self, kind: TokenKind) -> Scan<()> {
        self.unwind_indent(-1);
        self.remove_possible_simple_key()?;
        self.allow_simple_key = false;
        let start = self.get_mark();
        self.forward(3);
        let end = self.get_mark();
        self.push(kind, start, end);
        Ok(())
    }

    fn fetch_flow_collection_start(&mut self, kind: TokenKind) -> Scan<()> {
        self.save_possible_simple_key()?;
        self.flow_level += 1;
        self.allow_simple_key = true;
        self.fetch_indicator(kind);
        Ok(())
    }

    fn fetch_flow_collection_end(&mut self, kind: TokenKind) -> Scan<()> {
        self.remove_possible_simple_key()?;
        // A stray `]` or `}` makes the level negative, as in Python. A
        // negative level counts as the flow context.
        self.flow_level -= 1;
        self.allow_simple_key = false;
        self.fetch_indicator(kind);
        Ok(())
    }

    /// Adds a one-character indicator token.
    fn fetch_indicator(&mut self, kind: TokenKind) {
        let start = self.get_mark();
        self.forward(1);
        let end = self.get_mark();
        self.push(kind, start, end);
    }

    fn fetch_flow_entry(&mut self) -> Scan<()> {
        self.allow_simple_key = true;
        self.remove_possible_simple_key()?;
        self.fetch_indicator(TokenKind::FlowEntry);
        Ok(())
    }

    fn fetch_block_entry(&mut self) -> Scan<()> {
        if self.flow_level == 0 {
            if !self.allow_simple_key {
                return Err(scanner_error(
                    None,
                    None,
                    "sequence entries are not allowed here",
                    self.get_mark(),
                ));
            }
            if self.add_indent(self.column) {
                let mark = self.get_mark();
                self.push(TokenKind::BlockSequenceStart, mark, mark);
            }
        }
        self.allow_simple_key = true;
        self.remove_possible_simple_key()?;
        self.fetch_indicator(TokenKind::BlockEntry);
        Ok(())
    }

    fn fetch_key(&mut self) -> Scan<()> {
        if self.flow_level == 0 {
            if !self.allow_simple_key {
                return Err(scanner_error(
                    None,
                    None,
                    "mapping keys are not allowed here",
                    self.get_mark(),
                ));
            }
            if self.add_indent(self.column) {
                let mark = self.get_mark();
                self.push(TokenKind::BlockMappingStart, mark, mark);
            }
        }
        self.allow_simple_key = self.flow_level == 0;
        self.remove_possible_simple_key()?;
        self.fetch_indicator(TokenKind::Key);
        Ok(())
    }

    fn fetch_value(&mut self) -> Scan<()> {
        let level = self.flow_level;
        if let Some(position) = self
            .possible_simple_keys
            .iter()
            .position(|(key_level, _)| *key_level == level)
        {
            // A simple key ends here, so the scanner adds its KEY token.
            let (_, key) = self.possible_simple_keys.remove(position);
            self.insert(key.token_number, TokenKind::Key, key.mark);
            // In the block context, a simple key may also open a mapping.
            if self.flow_level == 0 && self.add_indent(key.column) {
                self.insert(key.token_number, TokenKind::BlockMappingStart, key.mark);
            }
            self.allow_simple_key = false;
        } else {
            // A complex key ends here. In the block context, the value must
            // be allowed.
            if self.flow_level == 0 && !self.allow_simple_key {
                return Err(scanner_error(
                    None,
                    None,
                    "mapping values are not allowed here",
                    self.get_mark(),
                ));
            }
            if self.flow_level == 0 && self.add_indent(self.column) {
                let mark = self.get_mark();
                self.push(TokenKind::BlockMappingStart, mark, mark);
            }
            self.allow_simple_key = self.flow_level == 0;
            self.remove_possible_simple_key()?;
        }
        self.fetch_indicator(TokenKind::Value);
        Ok(())
    }

    /// Ports `fetch_alias` when `alias` is set, else `fetch_anchor`.
    fn fetch_anchor(&mut self, alias: bool) -> Scan<()> {
        self.save_possible_simple_key()?;
        self.allow_simple_key = false;
        let token = self.scan_anchor(alias)?;
        self.tokens.push_back(token);
        Ok(())
    }

    fn fetch_tag(&mut self) -> Scan<()> {
        self.save_possible_simple_key()?;
        self.allow_simple_key = false;
        let token = self.scan_tag()?;
        self.tokens.push_back(token);
        Ok(())
    }

    fn fetch_block_scalar(&mut self, folded: bool) -> Scan<()> {
        // A simple key may follow a block scalar.
        self.allow_simple_key = true;
        self.remove_possible_simple_key()?;
        let token = self.scan_block_scalar(folded)?;
        self.tokens.push_back(token);
        Ok(())
    }

    fn fetch_flow_scalar(&mut self, double: bool) -> Scan<()> {
        self.save_possible_simple_key()?;
        self.allow_simple_key = false;
        let token = self.scan_flow_scalar(double)?;
        self.tokens.push_back(token);
        Ok(())
    }

    fn fetch_plain(&mut self) -> Scan<()> {
        self.save_possible_simple_key()?;
        // `scan_plain` sets `allow_simple_key` again after a line break.
        self.allow_simple_key = false;
        let token = self.scan_plain();
        self.tokens.push_back(token);
        Ok(())
    }

    // ── Checkers ────────────────────────────────────────────────────────────

    const fn check_directive(&self) -> bool {
        self.column == 0
    }

    fn check_document_start(&self) -> bool {
        self.column == 0 && self.prefix(3) == "---" && is_in(self.peek(3), BLANKZ)
    }

    fn check_document_end(&self) -> bool {
        self.column == 0 && self.prefix(3) == "..." && is_in(self.peek(3), BLANKZ)
    }

    fn check_block_entry(&self) -> bool {
        is_in(self.peek(1), BLANKZ)
    }

    fn check_key(&self) -> bool {
        self.flow_level != 0 || is_in(self.peek(1), BLANKZ)
    }

    fn check_value(&self) -> bool {
        self.flow_level != 0 || is_in(self.peek(1), BLANKZ)
    }

    /// A plain scalar may start with most characters. It may also start with
    /// `-`, `?` or `:` when a non-space follows.
    fn check_plain(&self) -> bool {
        let ch = self.peek(0);
        !is_in(ch, "\0 \t\r\n\u{85}\u{2028}\u{2029}-?:,[]{}#&*!|>'\"%@`")
            || (!is_in(self.peek(1), BLANKZ)
                && (ch == '-' || (self.flow_level == 0 && is_in(ch, "?:"))))
    }

    // ── Scanners ────────────────────────────────────────────────────────────

    /// Skips spaces, comments and line breaks up to the next token.
    ///
    /// `PyYAML` skips spaces only. A tab between tokens is therefore an error,
    /// where libyaml accepts it in some places.
    fn scan_to_next_token(&mut self) {
        if self.pointer == 0 && self.peek(0) == '\u{feff}' {
            self.forward(1);
        }
        loop {
            while self.peek(0) == ' ' {
                self.forward(1);
            }
            if self.peek(0) == '#' {
                while !is_in(self.peek(0), BREAKZ) {
                    self.forward(1);
                }
            }
            if self.scan_line_break().is_empty() {
                return;
            }
            if self.flow_level == 0 {
                self.allow_simple_key = true;
            }
        }
    }

    fn scan_directive(&mut self) -> Scan<Token> {
        let start = self.get_mark();
        self.forward(1);
        let name = self.scan_directive_name(start)?;
        let value = match name.as_str() {
            "YAML" => self.scan_yaml_directive_value(start)?,
            "TAG" => self.scan_tag_directive_value(start)?,
            _ => DirectiveValue::None,
        };
        let end = self.get_mark();
        if matches!(value, DirectiveValue::None) {
            while !is_in(self.peek(0), BREAKZ) {
                self.forward(1);
            }
        }
        self.scan_directive_ignored_line(start)?;
        Ok(Token {
            kind: TokenKind::Directive { name, value },
            start,
            end,
        })
    }

    fn directive_error(&self, start: Mark, problem: String) -> YamlError {
        scanner_error(
            Some("while scanning a directive"),
            Some(start),
            problem,
            self.get_mark(),
        )
    }

    fn scan_directive_name(&mut self, start: Mark) -> Scan<String> {
        let mut length = 0;
        while is_word(self.peek(length)) {
            length += 1;
        }
        let ch = self.peek(length);
        if length == 0 {
            return Err(self.directive_error(
                start,
                format!(
                    "expected alphabetic or numeric character, but found {}",
                    repr_char(ch)
                ),
            ));
        }
        let value = self.prefix(length);
        self.forward(length);
        let ch = self.peek(0);
        if !is_in(ch, SPACEZ) {
            return Err(self.directive_error(
                start,
                format!(
                    "expected alphabetic or numeric character, but found {}",
                    repr_char(ch)
                ),
            ));
        }
        Ok(value)
    }

    fn scan_yaml_directive_value(&mut self, start: Mark) -> Scan<DirectiveValue> {
        while self.peek(0) == ' ' {
            self.forward(1);
        }
        let major = self.scan_yaml_directive_number(start)?;
        if self.peek(0) != '.' {
            return Err(self.directive_error(
                start,
                format!(
                    "expected a digit or '.', but found {}",
                    repr_char(self.peek(0))
                ),
            ));
        }
        self.forward(1);
        let minor = self.scan_yaml_directive_number(start)?;
        if !is_in(self.peek(0), SPACEZ) {
            return Err(self.directive_error(
                start,
                format!(
                    "expected a digit or ' ', but found {}",
                    repr_char(self.peek(0))
                ),
            ));
        }
        Ok(DirectiveValue::Yaml(major, minor))
    }

    fn scan_yaml_directive_number(&mut self, start: Mark) -> Scan<Option<u64>> {
        let ch = self.peek(0);
        if !ch.is_ascii_digit() {
            return Err(self.directive_error(
                start,
                format!("expected a digit, but found {}", repr_char(ch)),
            ));
        }
        let mut length = 0;
        while self.peek(length).is_ascii_digit() {
            length += 1;
        }
        let value = self.prefix(length).parse().ok();
        self.forward(length);
        Ok(value)
    }

    fn scan_tag_directive_value(&mut self, start: Mark) -> Scan<DirectiveValue> {
        while self.peek(0) == ' ' {
            self.forward(1);
        }
        let handle = self.scan_tag_handle("directive", start)?;
        if self.peek(0) != ' ' {
            return Err(self.directive_error(
                start,
                format!("expected ' ', but found {}", repr_char(self.peek(0))),
            ));
        }
        while self.peek(0) == ' ' {
            self.forward(1);
        }
        let prefix = self.scan_tag_uri("directive", start)?;
        if !is_in(self.peek(0), SPACEZ) {
            return Err(self.directive_error(
                start,
                format!("expected ' ', but found {}", repr_char(self.peek(0))),
            ));
        }
        Ok(DirectiveValue::Tag(handle, prefix))
    }

    fn scan_directive_ignored_line(&mut self, start: Mark) -> Scan<()> {
        while self.peek(0) == ' ' {
            self.forward(1);
        }
        if self.peek(0) == '#' {
            while !is_in(self.peek(0), BREAKZ) {
                self.forward(1);
            }
        }
        let ch = self.peek(0);
        if !is_in(ch, BREAKZ) {
            return Err(self.directive_error(
                start,
                format!(
                    "expected a comment or a line break, but found {}",
                    repr_char(ch)
                ),
            ));
        }
        self.scan_line_break();
        Ok(())
    }

    /// Ports `scan_anchor` for an alias (`*`) or an anchor (`&`).
    fn scan_anchor(&mut self, alias: bool) -> Scan<Token> {
        let start = self.get_mark();
        let name = if alias { "alias" } else { "anchor" };
        self.forward(1);
        let mut length = 0;
        while is_word(self.peek(length)) {
            length += 1;
        }
        let error = |scanner: &Self, ch: char| {
            scanner_error(
                Some(&format!("while scanning an {name}")),
                Some(start),
                format!(
                    "expected alphabetic or numeric character, but found {}",
                    repr_char(ch)
                ),
                scanner.get_mark(),
            )
        };
        if length == 0 {
            return Err(error(self, self.peek(length)));
        }
        let value = self.prefix(length);
        self.forward(length);
        let ch = self.peek(0);
        if !is_in(ch, "\0 \t\r\n\u{85}\u{2028}\u{2029}?:,]}%@`") {
            return Err(error(self, ch));
        }
        let end = self.get_mark();
        let kind = if alias {
            TokenKind::Alias(value)
        } else {
            TokenKind::Anchor(value)
        };
        Ok(Token { kind, start, end })
    }

    fn scan_tag(&mut self) -> Scan<Token> {
        let start = self.get_mark();
        let ch = self.peek(1);
        let (handle, suffix) = if ch == '<' {
            self.forward(2);
            let suffix = self.scan_tag_uri("tag", start)?;
            if self.peek(0) != '>' {
                return Err(scanner_error(
                    Some("while parsing a tag"),
                    Some(start),
                    format!("expected '>', but found {}", repr_char(self.peek(0))),
                    self.get_mark(),
                ));
            }
            self.forward(1);
            (None, suffix)
        } else if is_in(ch, BLANKZ) {
            self.forward(1);
            (None, "!".to_owned())
        } else {
            let mut length = 1;
            let mut ch = ch;
            let mut use_handle = false;
            while !is_in(ch, SPACEZ) {
                if ch == '!' {
                    use_handle = true;
                    break;
                }
                length += 1;
                ch = self.peek(length);
            }
            let handle = if use_handle {
                self.scan_tag_handle("tag", start)?
            } else {
                self.forward(1);
                "!".to_owned()
            };
            let suffix = self.scan_tag_uri("tag", start)?;
            (Some(handle), suffix)
        };
        let ch = self.peek(0);
        if !is_in(ch, SPACEZ) {
            return Err(scanner_error(
                Some("while scanning a tag"),
                Some(start),
                format!("expected ' ', but found {}", repr_char(ch)),
                self.get_mark(),
            ));
        }
        let end = self.get_mark();
        Ok(Token {
            kind: TokenKind::Tag { handle, suffix },
            start,
            end,
        })
    }

    #[allow(clippy::too_many_lines)]
    fn scan_block_scalar(&mut self, folded: bool) -> Scan<Token> {
        let mut chunks = String::new();
        let start = self.get_mark();
        self.forward(1);
        let (chomping, increment) = self.scan_block_scalar_indicators(start)?;
        self.scan_block_scalar_ignored_line(start)?;

        // Determine the indentation level and go to the first non-empty line.
        let min_indent = (self.indent + 1).max(1);
        let (mut breaks, mut end, indent) = match increment {
            None => {
                let (breaks, max_indent, end) = self.scan_block_scalar_indentation();
                (breaks, end, min_indent.max(max_indent))
            }
            Some(increment) => {
                let indent = min_indent + i64::from(increment) - 1;
                let (breaks, end) = self.scan_block_scalar_breaks(indent);
                (breaks, end, indent)
            }
        };
        let mut line_break = String::new();

        // Scan the inner part of the block scalar.
        while self.column_is(indent) && self.peek(0) != '\0' {
            chunks.push_str(&breaks.concat());
            let leading_non_space = !is_in(self.peek(0), " \t");
            let mut length = 0;
            while !is_in(self.peek(length), BREAKZ) {
                length += 1;
            }
            chunks.push_str(&self.prefix(length));
            self.forward(length);
            line_break = self.scan_line_break();
            (breaks, end) = self.scan_block_scalar_breaks(indent);
            if !(self.column_is(indent) && self.peek(0) != '\0') {
                break;
            }
            // A folded scalar joins two text lines with a space.
            if folded && line_break == "\n" && leading_non_space && !is_in(self.peek(0), " \t") {
                if breaks.is_empty() {
                    chunks.push(' ');
                }
            } else {
                chunks.push_str(&line_break);
            }
        }

        // Chomp the tail.
        if chomping != Some(false) {
            chunks.push_str(&line_break);
        }
        if chomping == Some(true) {
            chunks.push_str(&breaks.concat());
        }
        Ok(Token {
            kind: TokenKind::Scalar {
                value: chunks,
                plain: false,
            },
            start,
            end,
        })
    }

    /// Returns true when the column equals `indent`.
    fn column_is(&self, indent: i64) -> bool {
        i64::try_from(self.column).is_ok_and(|column| column == indent)
    }

    /// Returns true when the column is below `indent`.
    fn column_below(&self, indent: i64) -> bool {
        i64::try_from(self.column).is_ok_and(|column| column < indent)
    }

    fn block_scalar_error(&self, start: Mark, problem: String) -> YamlError {
        scanner_error(
            Some("while scanning a block scalar"),
            Some(start),
            problem,
            self.get_mark(),
        )
    }

    /// Reads the chomping and indentation indicators, in either order.
    fn scan_block_scalar_indicators(&mut self, start: Mark) -> Scan<(Option<bool>, Option<u32>)> {
        let zero = "expected indentation indicator in the range 1-9, but found 0";
        let mut chomping = None;
        let mut increment = None;
        let ch = self.peek(0);
        if is_in(ch, "+-") {
            chomping = Some(ch == '+');
            self.forward(1);
            let ch = self.peek(0);
            if let Some(digit) = ch.to_digit(10) {
                if digit == 0 {
                    return Err(self.block_scalar_error(start, zero.to_owned()));
                }
                increment = Some(digit);
                self.forward(1);
            }
        } else if let Some(digit) = ch.to_digit(10) {
            if digit == 0 {
                return Err(self.block_scalar_error(start, zero.to_owned()));
            }
            increment = Some(digit);
            self.forward(1);
            let ch = self.peek(0);
            if is_in(ch, "+-") {
                chomping = Some(ch == '+');
                self.forward(1);
            }
        }
        let ch = self.peek(0);
        if !is_in(ch, SPACEZ) {
            return Err(self.block_scalar_error(
                start,
                format!(
                    "expected chomping or indentation indicators, but found {}",
                    repr_char(ch)
                ),
            ));
        }
        Ok((chomping, increment))
    }

    fn scan_block_scalar_ignored_line(&mut self, start: Mark) -> Scan<()> {
        while self.peek(0) == ' ' {
            self.forward(1);
        }
        if self.peek(0) == '#' {
            while !is_in(self.peek(0), BREAKZ) {
                self.forward(1);
            }
        }
        let ch = self.peek(0);
        if !is_in(ch, BREAKZ) {
            return Err(self.block_scalar_error(
                start,
                format!(
                    "expected a comment or a line break, but found {}",
                    repr_char(ch)
                ),
            ));
        }
        self.scan_line_break();
        Ok(())
    }

    fn scan_block_scalar_indentation(&mut self) -> (Vec<String>, i64, Mark) {
        let mut chunks = Vec::new();
        let mut max_indent = 0;
        let mut end = self.get_mark();
        while is_in(self.peek(0), " \r\n\u{85}\u{2028}\u{2029}") {
            if self.peek(0) == ' ' {
                self.forward(1);
                let column = i64::try_from(self.column).unwrap_or(i64::MAX);
                if column > max_indent {
                    max_indent = column;
                }
            } else {
                chunks.push(self.scan_line_break());
                end = self.get_mark();
            }
        }
        (chunks, max_indent, end)
    }

    fn scan_block_scalar_breaks(&mut self, indent: i64) -> (Vec<String>, Mark) {
        let mut chunks = Vec::new();
        let mut end = self.get_mark();
        while self.column_below(indent) && self.peek(0) == ' ' {
            self.forward(1);
        }
        while is_in(self.peek(0), BREAKS) {
            chunks.push(self.scan_line_break());
            end = self.get_mark();
            while self.column_below(indent) && self.peek(0) == ' ' {
                self.forward(1);
            }
        }
        (chunks, end)
    }

    fn scan_flow_scalar(&mut self, double: bool) -> Scan<Token> {
        let mut chunks = String::new();
        let start = self.get_mark();
        let quote = self.peek(0);
        self.forward(1);
        self.scan_flow_scalar_non_spaces(double, start, &mut chunks)?;
        while self.peek(0) != quote {
            self.scan_flow_scalar_spaces(double, start, &mut chunks)?;
            self.scan_flow_scalar_non_spaces(double, start, &mut chunks)?;
        }
        self.forward(1);
        let end = self.get_mark();
        Ok(Token {
            kind: TokenKind::Scalar {
                value: chunks,
                plain: false,
            },
            start,
            end,
        })
    }

    fn double_quoted_error(&self, start: Mark, problem: String) -> YamlError {
        scanner_error(
            Some("while scanning a double-quoted scalar"),
            Some(start),
            problem,
            self.get_mark(),
        )
    }

    fn scan_flow_scalar_non_spaces(
        &mut self,
        double: bool,
        start: Mark,
        chunks: &mut String,
    ) -> Scan<()> {
        loop {
            let mut length = 0;
            while !is_in(self.peek(length), "'\"\\\0 \t\r\n\u{85}\u{2028}\u{2029}") {
                length += 1;
            }
            if length > 0 {
                chunks.push_str(&self.prefix(length));
                self.forward(length);
            }
            let ch = self.peek(0);
            if !double && ch == '\'' && self.peek(1) == '\'' {
                chunks.push('\'');
                self.forward(2);
            } else if (double && ch == '\'') || (!double && is_in(ch, "\"\\")) {
                chunks.push(ch);
                self.forward(1);
            } else if double && ch == '\\' {
                self.forward(1);
                let ch = self.peek(0);
                if let Some(replacement) = escape_replacement(ch) {
                    chunks.push(replacement);
                    self.forward(1);
                } else if let Some(length) = escape_code_length(ch) {
                    self.forward(1);
                    for k in 0..length {
                        if !is_in(self.peek(k), HEX) {
                            return Err(self.double_quoted_error(
                                start,
                                format!(
                                    "expected escape sequence of {length} hexadecimal numbers, but found {}",
                                    repr_char(self.peek(k))
                                ),
                            ));
                        }
                    }
                    let code = u32::from_str_radix(&self.prefix(length), 16).unwrap_or(u32::MAX);
                    chunks.push(escaped_char(code).ok_or_else(|| {
                        YamlError::new(None, None, "chr() arg not in range(0x110000)", None)
                    })?);
                    self.forward(length);
                } else if is_in(ch, BREAKS) {
                    self.scan_line_break();
                    self.scan_flow_scalar_breaks(start, chunks)?;
                } else {
                    return Err(self.double_quoted_error(
                        start,
                        format!("found unknown escape character {}", repr_char(ch)),
                    ));
                }
            } else {
                return Ok(());
            }
        }
    }

    fn scan_flow_scalar_spaces(
        &mut self,
        _double: bool,
        start: Mark,
        chunks: &mut String,
    ) -> Scan<()> {
        let mut length = 0;
        while is_in(self.peek(length), " \t") {
            length += 1;
        }
        let whitespaces = self.prefix(length);
        self.forward(length);
        let ch = self.peek(0);
        if ch == '\0' {
            return Err(scanner_error(
                Some("while scanning a quoted scalar"),
                Some(start),
                "found unexpected end of stream",
                self.get_mark(),
            ));
        }
        if is_in(ch, BREAKS) {
            let line_break = self.scan_line_break();
            let mut breaks = String::new();
            let count = self.scan_flow_scalar_breaks(start, &mut breaks)?;
            if line_break != "\n" {
                chunks.push_str(&line_break);
            } else if count == 0 {
                chunks.push(' ');
            }
            chunks.push_str(&breaks);
        } else {
            chunks.push_str(&whitespaces);
        }
        Ok(())
    }

    /// Ports `scan_flow_scalar_breaks`. It appends the breaks to `chunks` and
    /// returns how many it found.
    fn scan_flow_scalar_breaks(&mut self, start: Mark, chunks: &mut String) -> Scan<usize> {
        let mut count = 0;
        loop {
            // Instead of checking indentation, the scanner checks for document
            // separators.
            let prefix = self.prefix(3);
            if (prefix == "---" || prefix == "...") && is_in(self.peek(3), BLANKZ) {
                return Err(scanner_error(
                    Some("while scanning a quoted scalar"),
                    Some(start),
                    "found unexpected document separator",
                    self.get_mark(),
                ));
            }
            while is_in(self.peek(0), " \t") {
                self.forward(1);
            }
            if !is_in(self.peek(0), BREAKS) {
                return Ok(count);
            }
            chunks.push_str(&self.scan_line_break());
            count += 1;
        }
    }

    /// Ports `scan_plain`.
    ///
    /// A plain scalar ends at `: ` and at ` #`. In the flow context it also
    /// ends at `,`, `?`, `[`, `]`, `{` and `}`. A tab ends it as well.
    fn scan_plain(&mut self) -> Token {
        let mut chunks = String::new();
        let start = self.get_mark();
        let mut end = start;
        let indent = self.indent + 1;
        let mut spaces = String::new();
        loop {
            if self.peek(0) == '#' {
                break;
            }
            let mut length = 0;
            loop {
                let ch = self.peek(length);
                let after = self.peek(length + 1);
                let value_end = ch == ':'
                    && (is_in(after, BLANKZ) || (self.flow_level != 0 && is_in(after, ",[]{}")));
                if is_in(ch, BLANKZ) || value_end || (self.flow_level != 0 && is_in(ch, ",?[]{}")) {
                    break;
                }
                length += 1;
            }
            if length == 0 {
                break;
            }
            self.allow_simple_key = false;
            chunks.push_str(&spaces);
            chunks.push_str(&self.prefix(length));
            self.forward(length);
            end = self.get_mark();
            let Some(next) = self.scan_plain_spaces() else {
                break;
            };
            spaces = next;
            if spaces.is_empty()
                || self.peek(0) == '#'
                || (self.flow_level == 0 && self.column_below(indent))
            {
                break;
            }
        }
        Token {
            kind: TokenKind::Scalar {
                value: chunks,
                plain: true,
            },
            start,
            end,
        }
    }

    /// Ports `scan_plain_spaces`. `None` stands for the `None` that Python
    /// returns at a document separator.
    fn scan_plain_spaces(&mut self) -> Option<String> {
        let mut chunks = String::new();
        let mut length = 0;
        while self.peek(length) == ' ' {
            length += 1;
        }
        let whitespaces = self.prefix(length);
        self.forward(length);
        let ch = self.peek(0);
        if is_in(ch, BREAKS) {
            let line_break = self.scan_line_break();
            self.allow_simple_key = true;
            if self.at_document_separator() {
                return None;
            }
            let mut breaks = String::new();
            let mut count = 0;
            while is_in(self.peek(0), " \r\n\u{85}\u{2028}\u{2029}") {
                if self.peek(0) == ' ' {
                    self.forward(1);
                } else {
                    breaks.push_str(&self.scan_line_break());
                    count += 1;
                    if self.at_document_separator() {
                        return None;
                    }
                }
            }
            if line_break != "\n" {
                chunks.push_str(&line_break);
            } else if count == 0 {
                chunks.push(' ');
            }
            chunks.push_str(&breaks);
        } else if !whitespaces.is_empty() {
            chunks.push_str(&whitespaces);
        }
        Some(chunks)
    }

    fn at_document_separator(&self) -> bool {
        let prefix = self.prefix(3);
        (prefix == "---" || prefix == "...") && is_in(self.peek(3), BLANKZ)
    }

    /// Ports `scan_tag_handle`. A handle is `!`, `!!` or `!word!`.
    fn scan_tag_handle(&mut self, name: &str, start: Mark) -> Scan<String> {
        let error = |scanner: &Self, ch: char| {
            scanner_error(
                Some(&format!("while scanning a {name}")),
                Some(start),
                format!("expected '!', but found {}", repr_char(ch)),
                scanner.get_mark(),
            )
        };
        let ch = self.peek(0);
        if ch != '!' {
            return Err(error(self, ch));
        }
        let mut length = 1;
        let mut ch = self.peek(length);
        if ch != ' ' {
            while is_word(ch) {
                length += 1;
                ch = self.peek(length);
            }
            if ch != '!' {
                self.forward(length);
                return Err(error(self, ch));
            }
            length += 1;
        }
        let value = self.prefix(length);
        self.forward(length);
        Ok(value)
    }

    /// Ports `scan_tag_uri`. A `%` starts an escaped octet.
    fn scan_tag_uri(&mut self, name: &str, start: Mark) -> Scan<String> {
        let mut chunks = String::new();
        let mut any = false;
        let mut length = 0;
        let mut ch = self.peek(length);
        while ch.is_ascii_alphanumeric() || is_in(ch, "-;/?:@&=+$,_.!~*'()[]%") {
            if ch == '%' {
                chunks.push_str(&self.prefix(length));
                self.forward(length);
                length = 0;
                chunks.push_str(&self.scan_uri_escapes(name, start)?);
                any = true;
            } else {
                length += 1;
            }
            ch = self.peek(length);
        }
        if length > 0 {
            chunks.push_str(&self.prefix(length));
            self.forward(length);
            any = true;
        }
        if !any {
            return Err(scanner_error(
                Some(&format!("while parsing a {name}")),
                Some(start),
                format!("expected URI, but found {}", repr_char(ch)),
                self.get_mark(),
            ));
        }
        Ok(chunks)
    }

    fn scan_uri_escapes(&mut self, name: &str, start: Mark) -> Scan<String> {
        let mut codes = Vec::new();
        let mark = self.get_mark();
        while self.peek(0) == '%' {
            self.forward(1);
            for k in 0..2 {
                if !is_in(self.peek(k), HEX) {
                    return Err(scanner_error(
                        Some(&format!("while scanning a {name}")),
                        Some(start),
                        format!(
                            "expected URI escape sequence of 2 hexadecimal numbers, but found {}",
                            repr_char(self.peek(k))
                        ),
                        self.get_mark(),
                    ));
                }
            }
            codes.push(u8::from_str_radix(&self.prefix(2), 16).unwrap_or(0));
            self.forward(2);
        }
        String::from_utf8(codes).map_err(|error| {
            scanner_error(
                Some(&format!("while scanning a {name}")),
                Some(start),
                utf8_error_text(error.as_bytes(), &error.utf8_error()),
                mark,
            )
        })
    }

    /// Ports `scan_line_break`. It folds `\r\n`, `\r`, `\n` and NEL to `\n`,
    /// and keeps LS and PS. It returns an empty string when no break follows.
    fn scan_line_break(&mut self) -> String {
        let ch = self.peek(0);
        if is_in(ch, "\r\n\u{85}") {
            if self.prefix(2) == "\r\n" {
                self.forward(2);
            } else {
                self.forward(1);
            }
            return "\n".to_owned();
        }
        if is_in(ch, "\u{2028}\u{2029}") {
            self.forward(1);
            return ch.to_string();
        }
        String::new()
    }
}

/// Ports `Scanner.ESCAPE_REPLACEMENTS`.
const fn escape_replacement(ch: char) -> Option<char> {
    Some(match ch {
        '0' => '\0',
        'a' => '\x07',
        'b' => '\x08',
        't' | '\t' => '\t',
        'n' => '\n',
        'v' => '\x0b',
        'f' => '\x0c',
        'r' => '\r',
        'e' => '\x1b',
        ' ' => ' ',
        '"' => '"',
        '\\' => '\\',
        '/' => '/',
        'N' => '\u{85}',
        '_' => '\u{a0}',
        'L' => '\u{2028}',
        'P' => '\u{2029}',
        _ => return None,
    })
}

/// Ports `Scanner.ESCAPE_CODES`: the digit count of `\x`, `\u` and `\U`.
const fn escape_code_length(ch: char) -> Option<usize> {
    match ch {
        'x' => Some(2),
        'u' => Some(4),
        'U' => Some(8),
        _ => None,
    }
}

/// Ports `chr(code)` for an escape.
///
/// Python keeps a lone surrogate in a `str`, and a Rust `String` cannot hold
/// one. So a surrogate becomes U+FFFD. A code past U+10FFFF fails, as `chr`
/// does.
fn escaped_char(code: u32) -> Option<char> {
    if (0xd800..=0xdfff).contains(&code) {
        return Some('\u{fffd}');
    }
    char::from_u32(code)
}

/// Renders a UTF-8 decode error as Python `str(UnicodeDecodeError)` does.
fn utf8_error_text(bytes: &[u8], error: &std::str::Utf8Error) -> String {
    let start = error.valid_up_to();
    let (end, reason) = match error.error_len() {
        None => (bytes.len(), "unexpected end of data"),
        Some(_)
            if bytes
                .get(start)
                .is_some_and(|byte| *byte >= 0xc2 && *byte <= 0xf4) =>
        {
            (start + 1, "invalid continuation byte")
        }
        Some(_) => (start + 1, "invalid start byte"),
    };
    if end - start <= 1 {
        let byte = bytes.get(start).copied().unwrap_or(0);
        format!("'utf-8' codec can't decode byte 0x{byte:02x} in position {start}: {reason}")
    } else {
        format!(
            "'utf-8' codec can't decode bytes in position {start}-{}: {reason}",
            end - 1
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kinds(text: &str) -> Vec<&'static str> {
        let mut scanner = Scanner::new(text).unwrap_or_else(|error| panic!("{error:?}"));
        let mut out = Vec::new();
        loop {
            let token = scanner
                .get_token()
                .unwrap_or_else(|error| panic!("{error:?}"));
            out.push(token.kind.id());
            if matches!(token.kind, TokenKind::StreamEnd) {
                return out;
            }
        }
    }

    fn error(text: &str) -> String {
        let mut scanner = match Scanner::new(text) {
            Ok(scanner) => scanner,
            Err(error) => return error.render(text),
        };
        loop {
            match scanner.get_token() {
                Ok(token) if matches!(token.kind, TokenKind::StreamEnd) => return String::new(),
                Ok(_) => {}
                Err(error) => return error.render(text),
            }
        }
    }

    #[test]
    fn a_block_mapping_scans_to_pyyaml_tokens() {
        assert_eq!(
            kinds("a: b\n"),
            [
                "<stream start>",
                "<block mapping start>",
                "?",
                "<scalar>",
                ":",
                "<scalar>",
                "<block end>",
                "<stream end>"
            ]
        );
    }

    #[test]
    fn a_tab_after_a_plain_scalar_cannot_start_a_token() {
        assert_eq!(
            error("a: b\tc\n"),
            "while scanning for the next token\nfound character '\\t' that cannot start any token\n  in \"<unicode string>\", line 1, column 5:\n    a: b\tc\n        ^"
        );
        assert_eq!(error("a: 'b\tc'\n"), "");
    }

    #[test]
    fn a_reserved_directive_is_skipped() {
        assert_eq!(error("%FOO bar\n--- a\n"), "");
    }

    #[test]
    fn the_reader_rejects_a_control_character_by_its_character_position() {
        assert_eq!(
            error("é: \u{7}\n"),
            "unacceptable character #x0007: special characters are not allowed\n  in \"<unicode string>\", position 3"
        );
    }

    #[test]
    fn escapes_decode_as_in_python() {
        let mut scanner =
            Scanner::new("\"\\x41\\u00e9\\N\\ud800\"").unwrap_or_else(|e| panic!("{e:?}"));
        let _ = scanner.get_token();
        let token = scanner.get_token().unwrap_or_else(|e| panic!("{e:?}"));
        let TokenKind::Scalar { value, .. } = token.kind else {
            panic!("not a scalar");
        };
        assert_eq!(value, "A\u{e9}\u{85}\u{fffd}");
        assert!(error("\"\\q\"").contains("found unknown escape character 'q'"));
    }

    #[test]
    fn a_bad_uri_escape_reports_the_python_decode_error() {
        assert!(
            error("!<%ff> a\n")
                .contains("'utf-8' codec can't decode byte 0xff in position 0: invalid start byte"),
            "{}",
            error("!<%ff> a\n")
        );
    }
}
