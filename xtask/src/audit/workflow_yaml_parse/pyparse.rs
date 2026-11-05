//! A port of the `PyYAML` parser: `yaml/parser.py` of `PyYAML` 6.
//!
//! The parser turns the tokens of [`super::pyscan`] into events. Each state
//! of the Python state machine is a variant of [`State`], with the name of
//! its Python method. The error messages and marks are those of `PyYAML`.

use super::pyscan::{DirectiveValue, Scanner, Token, TokenKind};
use super::pytext::repr_str;
use super::pyyaml::{Mark, YamlError};

/// One parser event. The composer reads these.
#[derive(Debug)]
pub enum Event {
    StreamStart,
    StreamEnd,
    DocumentStart,
    DocumentEnd,
    Alias(String),
    Scalar {
        anchor: Option<String>,
        tag: Option<String>,
        value: String,
        plain_implicit: bool,
    },
    SequenceStart {
        anchor: Option<String>,
        tag: Option<String>,
    },
    SequenceEnd,
    MappingStart {
        anchor: Option<String>,
        tag: Option<String>,
    },
    MappingEnd,
}

/// The states of the parser. Each one names a Python `parse_*` method.
#[derive(Clone, Copy, Debug)]
enum State {
    StreamStart,
    ImplicitDocumentStart,
    DocumentStart,
    DocumentEnd,
    DocumentContent,
    BlockNode,
    BlockSequenceFirstEntry,
    BlockSequenceEntry,
    IndentlessSequenceEntry,
    BlockMappingFirstKey,
    BlockMappingKey,
    BlockMappingValue,
    FlowSequenceFirstEntry,
    FlowSequenceEntry,
    FlowSequenceEntryMappingKey,
    FlowSequenceEntryMappingValue,
    FlowSequenceEntryMappingEnd,
    FlowMappingFirstKey,
    FlowMappingKey,
    FlowMappingValue,
    FlowMappingEmptyValue,
}

/// The tag handles that every document knows.
fn default_tags() -> Vec<(String, String)> {
    vec![
        ("!".to_owned(), "!".to_owned()),
        ("!!".to_owned(), "tag:yaml.org,2002:".to_owned()),
    ]
}

/// One parsed event with its start mark.
type Parsed = Result<(Event, Mark), YamlError>;

/// Ports `yaml.parser.Parser` over a [`Scanner`].
pub struct Parser {
    scanner: Scanner,
    /// The next state, or `None` after the stream end.
    state: Option<State>,
    states: Vec<State>,
    marks: Vec<Mark>,
    /// The tag handles of the current document, in insertion order.
    tag_handles: Vec<(String, String)>,
}

impl Parser {
    /// Reads the stream. The reader checks every character here.
    pub fn new(text: &str) -> Result<Self, YamlError> {
        Ok(Self {
            scanner: Scanner::new(text)?,
            state: Some(State::StreamStart),
            states: Vec::new(),
            marks: Vec::new(),
            tag_handles: Vec::new(),
        })
    }

    /// Ports `get_event`: it runs the next state once.
    pub fn next(&mut self) -> Parsed {
        let state = self
            .state
            .ok_or_else(|| YamlError::new(None, None, "no more events", None))?;
        self.run(state)
    }

    fn run(&mut self, state: State) -> Parsed {
        match state {
            State::StreamStart => self.parse_stream_start(),
            State::ImplicitDocumentStart => self.parse_implicit_document_start(),
            State::DocumentStart => self.parse_document_start(),
            State::DocumentEnd => self.parse_document_end(),
            State::DocumentContent => self.parse_document_content(),
            State::BlockNode => self.parse_node(true, false),
            State::BlockSequenceFirstEntry => self.parse_block_sequence_first_entry(),
            State::BlockSequenceEntry => self.parse_block_sequence_entry(),
            State::IndentlessSequenceEntry => self.parse_indentless_sequence_entry(),
            State::BlockMappingFirstKey => self.parse_block_mapping_first_key(),
            State::BlockMappingKey => self.parse_block_mapping_key(),
            State::BlockMappingValue => self.parse_block_mapping_value(),
            State::FlowSequenceFirstEntry => self.parse_flow_sequence_first_entry(),
            State::FlowSequenceEntry => self.parse_flow_sequence_entry(false),
            State::FlowSequenceEntryMappingKey => self.parse_flow_sequence_entry_mapping_key(),
            State::FlowSequenceEntryMappingValue => self.parse_flow_sequence_entry_mapping_value(),
            State::FlowSequenceEntryMappingEnd => self.parse_flow_sequence_entry_mapping_end(),
            State::FlowMappingFirstKey => self.parse_flow_mapping_first_key(),
            State::FlowMappingKey => self.parse_flow_mapping_key(false),
            State::FlowMappingValue => self.parse_flow_mapping_value(),
            State::FlowMappingEmptyValue => self.parse_flow_mapping_empty_value(),
        }
    }

    // ── Token helpers ───────────────────────────────────────────────────────

    fn peek(&mut self) -> Result<&Token, YamlError> {
        self.scanner.peek_token()
    }

    fn get(&mut self) -> Result<Token, YamlError> {
        self.scanner.get_token()
    }

    /// Ports `check_token` with one predicate over the token kind.
    fn check(&mut self, test: impl Fn(&TokenKind) -> bool) -> Result<bool, YamlError> {
        Ok(test(&self.peek()?.kind))
    }

    /// Ports `self.state = self.states.pop()`.
    fn pop_state(&mut self) {
        self.state = self.states.pop();
    }

    fn pop_mark(&mut self) -> Mark {
        self.marks.pop().unwrap_or(Mark {
            index: 0,
            line: 0,
            column: 0,
        })
    }

    fn last_mark(&self) -> Option<Mark> {
        self.marks.last().copied()
    }

    /// Ports `process_empty_scalar`.
    const fn empty_scalar(mark: Mark) -> (Event, Mark) {
        (
            Event::Scalar {
                anchor: None,
                tag: None,
                value: String::new(),
                plain_implicit: true,
            },
            mark,
        )
    }

    // ── Stream and documents ────────────────────────────────────────────────

    fn parse_stream_start(&mut self) -> Parsed {
        let token = self.get()?;
        self.state = Some(State::ImplicitDocumentStart);
        Ok((Event::StreamStart, token.start))
    }

    fn parse_implicit_document_start(&mut self) -> Parsed {
        let explicit = self.check(|kind| {
            matches!(
                kind,
                TokenKind::Directive { .. } | TokenKind::DocumentStart | TokenKind::StreamEnd
            )
        })?;
        if explicit {
            return self.parse_document_start();
        }
        self.tag_handles = default_tags();
        let start = self.peek()?.start;
        self.states.push(State::DocumentEnd);
        self.state = Some(State::BlockNode);
        Ok((Event::DocumentStart, start))
    }

    fn parse_document_start(&mut self) -> Parsed {
        // Parse any extra document end indicators.
        while self.check(|kind| matches!(kind, TokenKind::DocumentEnd))? {
            self.get()?;
        }
        if self.check(|kind| matches!(kind, TokenKind::StreamEnd))? {
            let token = self.get()?;
            self.state = None;
            return Ok((Event::StreamEnd, token.start));
        }
        let start = self.peek()?.start;
        self.process_directives()?;
        if !self.check(|kind| matches!(kind, TokenKind::DocumentStart))? {
            let token = self.peek()?;
            return Err(YamlError::new(
                None,
                None,
                format!(
                    "expected '<document start>', but found {}",
                    repr_str(token.kind.id())
                ),
                Some(token.start),
            ));
        }
        self.get()?;
        self.states.push(State::DocumentEnd);
        self.state = Some(State::DocumentContent);
        Ok((Event::DocumentStart, start))
    }

    fn parse_document_end(&mut self) -> Parsed {
        let start = self.peek()?.start;
        if self.check(|kind| matches!(kind, TokenKind::DocumentEnd))? {
            self.get()?;
        }
        self.state = Some(State::DocumentStart);
        Ok((Event::DocumentEnd, start))
    }

    fn parse_document_content(&mut self) -> Parsed {
        let empty = self.check(|kind| {
            matches!(
                kind,
                TokenKind::Directive { .. }
                    | TokenKind::DocumentStart
                    | TokenKind::DocumentEnd
                    | TokenKind::StreamEnd
            )
        })?;
        if empty {
            let mark = self.peek()?.start;
            self.pop_state();
            return Ok(Self::empty_scalar(mark));
        }
        self.parse_node(true, false)
    }

    /// Ports `process_directives`. It reads the `%YAML` and `%TAG`
    /// directives of a document and sets its tag handles.
    fn process_directives(&mut self) -> Result<(), YamlError> {
        let mut yaml_version = false;
        self.tag_handles = Vec::new();
        while self.check(|kind| matches!(kind, TokenKind::Directive { .. }))? {
            let token = self.get()?;
            let TokenKind::Directive { name, value } = token.kind else {
                continue;
            };
            match (name.as_str(), value) {
                ("YAML", DirectiveValue::Yaml(major)) => {
                    if yaml_version {
                        return Err(YamlError::new(
                            None,
                            None,
                            "found duplicate YAML directive",
                            Some(token.start),
                        ));
                    }
                    if major != Some(1) {
                        return Err(YamlError::new(
                            None,
                            None,
                            "found incompatible YAML document (version 1.* is required)",
                            Some(token.start),
                        ));
                    }
                    yaml_version = true;
                }
                ("TAG", DirectiveValue::Tag(handle, prefix)) => {
                    if self.tag_handles.iter().any(|(known, _)| *known == handle) {
                        return Err(YamlError::new(
                            None,
                            None,
                            format!("duplicate tag handle {}", repr_str(&handle)),
                            Some(token.start),
                        ));
                    }
                    self.tag_handles.push((handle, prefix));
                }
                _ => {}
            }
        }
        for (handle, prefix) in default_tags() {
            if !self.tag_handles.iter().any(|(known, _)| *known == handle) {
                self.tag_handles.push((handle, prefix));
            }
        }
        Ok(())
    }

    // ── Nodes ───────────────────────────────────────────────────────────────

    /// Ports `parse_node`. `block` allows a block collection, and
    /// `indentless` allows a sequence at the indent of its mapping key.
    #[allow(clippy::too_many_lines)]
    fn parse_node(&mut self, block: bool, indentless: bool) -> Parsed {
        if self.check(|kind| matches!(kind, TokenKind::Alias(_)))? {
            let token = self.get()?;
            self.pop_state();
            let TokenKind::Alias(anchor) = token.kind else {
                return Err(YamlError::new(None, None, "expected an alias", None));
            };
            return Ok((Event::Alias(anchor), token.start));
        }
        let mut anchor = None;
        let mut tag = None;
        let mut start = None;
        let mut tag_mark = None;
        if self.check(|kind| matches!(kind, TokenKind::Anchor(_)))? {
            let token = self.get()?;
            start = Some(token.start);
            if let TokenKind::Anchor(name) = token.kind {
                anchor = Some(name);
            }
            if self.check(|kind| matches!(kind, TokenKind::Tag { .. }))? {
                let token = self.get()?;
                tag_mark = Some(token.start);
                if let TokenKind::Tag { handle, suffix } = token.kind {
                    tag = Some((handle, suffix));
                }
            }
        } else if self.check(|kind| matches!(kind, TokenKind::Tag { .. }))? {
            let token = self.get()?;
            start = Some(token.start);
            tag_mark = Some(token.start);
            if let TokenKind::Tag { handle, suffix } = token.kind {
                tag = Some((handle, suffix));
            }
            if self.check(|kind| matches!(kind, TokenKind::Anchor(_)))? {
                let token = self.get()?;
                if let TokenKind::Anchor(name) = token.kind {
                    anchor = Some(name);
                }
            }
        }
        let tag = match tag {
            None => None,
            Some((None, suffix)) => Some(suffix),
            Some((Some(handle), suffix)) => {
                let Some((_, prefix)) = self.tag_handles.iter().find(|(known, _)| *known == handle)
                else {
                    return Err(YamlError::new(
                        Some("while parsing a node".to_owned()),
                        start,
                        format!("found undefined tag handle {}", repr_str(&handle)),
                        tag_mark,
                    ));
                };
                Some(format!("{prefix}{suffix}"))
            }
        };
        let start = match start {
            Some(start) => start,
            None => self.peek()?.start,
        };
        if indentless && self.check(|kind| matches!(kind, TokenKind::BlockEntry))? {
            self.state = Some(State::IndentlessSequenceEntry);
            return Ok((Event::SequenceStart { anchor, tag }, start));
        }
        let kind = self.peek()?.kind.clone();
        match kind {
            TokenKind::Scalar { value, plain } => {
                self.get()?;
                let plain_implicit = (plain && tag.is_none()) || tag.as_deref() == Some("!");
                self.pop_state();
                Ok((
                    Event::Scalar {
                        anchor,
                        tag,
                        value,
                        plain_implicit,
                    },
                    start,
                ))
            }
            TokenKind::FlowSequenceStart => {
                self.state = Some(State::FlowSequenceFirstEntry);
                Ok((Event::SequenceStart { anchor, tag }, start))
            }
            TokenKind::FlowMappingStart => {
                self.state = Some(State::FlowMappingFirstKey);
                Ok((Event::MappingStart { anchor, tag }, start))
            }
            TokenKind::BlockSequenceStart if block => {
                self.state = Some(State::BlockSequenceFirstEntry);
                Ok((Event::SequenceStart { anchor, tag }, start))
            }
            TokenKind::BlockMappingStart if block => {
                self.state = Some(State::BlockMappingFirstKey);
                Ok((Event::MappingStart { anchor, tag }, start))
            }
            _ if anchor.is_some() || tag.is_some() => {
                // An empty scalar with an anchor or a tag. Python passes
                // `(implicit, False)`, where `implicit` is true for no tag
                // or `!`.
                let plain_implicit = tag.is_none() || tag.as_deref() == Some("!");
                self.pop_state();
                Ok((
                    Event::Scalar {
                        anchor,
                        tag,
                        value: String::new(),
                        plain_implicit,
                    },
                    start,
                ))
            }
            other => {
                let node = if block { "block" } else { "flow" };
                let mark = self.peek()?.start;
                Err(YamlError::new(
                    Some(format!("while parsing a {node} node")),
                    Some(start),
                    format!(
                        "expected the node content, but found {}",
                        repr_str(other.id())
                    ),
                    Some(mark),
                ))
            }
        }
    }

    /// Parses a block node after pushing `next` as the state to return to.
    fn block_node_then(&mut self, next: State, indentless: bool) -> Parsed {
        self.states.push(next);
        self.parse_node(true, indentless)
    }

    /// Parses a flow node after pushing `next` as the state to return to.
    fn flow_node_then(&mut self, next: State) -> Parsed {
        self.states.push(next);
        self.parse_node(false, false)
    }

    // ── Block collections ───────────────────────────────────────────────────

    fn parse_block_sequence_first_entry(&mut self) -> Parsed {
        let token = self.get()?;
        self.marks.push(token.start);
        self.parse_block_sequence_entry()
    }

    fn parse_block_sequence_entry(&mut self) -> Parsed {
        if self.check(|kind| matches!(kind, TokenKind::BlockEntry))? {
            let token = self.get()?;
            if !self.check(|kind| matches!(kind, TokenKind::BlockEntry | TokenKind::BlockEnd))? {
                return self.block_node_then(State::BlockSequenceEntry, false);
            }
            self.state = Some(State::BlockSequenceEntry);
            return Ok(Self::empty_scalar(token.end));
        }
        if !self.check(|kind| matches!(kind, TokenKind::BlockEnd))? {
            let context_mark = self.last_mark();
            let token = self.peek()?;
            return Err(YamlError::new(
                Some("while parsing a block collection".to_owned()),
                context_mark,
                format!(
                    "expected <block end>, but found {}",
                    repr_str(token.kind.id())
                ),
                Some(token.start),
            ));
        }
        let token = self.get()?;
        self.pop_state();
        self.pop_mark();
        Ok((Event::SequenceEnd, token.start))
    }

    fn parse_indentless_sequence_entry(&mut self) -> Parsed {
        if self.check(|kind| matches!(kind, TokenKind::BlockEntry))? {
            let token = self.get()?;
            let ends = self.check(|kind| {
                matches!(
                    kind,
                    TokenKind::BlockEntry | TokenKind::Key | TokenKind::Value | TokenKind::BlockEnd
                )
            })?;
            if !ends {
                return self.block_node_then(State::IndentlessSequenceEntry, false);
            }
            self.state = Some(State::IndentlessSequenceEntry);
            return Ok(Self::empty_scalar(token.end));
        }
        let start = self.peek()?.start;
        self.pop_state();
        Ok((Event::SequenceEnd, start))
    }

    fn parse_block_mapping_first_key(&mut self) -> Parsed {
        let token = self.get()?;
        self.marks.push(token.start);
        self.parse_block_mapping_key()
    }

    fn parse_block_mapping_key(&mut self) -> Parsed {
        if self.check(|kind| matches!(kind, TokenKind::Key))? {
            let token = self.get()?;
            let ends = self.check(|kind| {
                matches!(
                    kind,
                    TokenKind::Key | TokenKind::Value | TokenKind::BlockEnd
                )
            })?;
            if !ends {
                return self.block_node_then(State::BlockMappingValue, true);
            }
            self.state = Some(State::BlockMappingValue);
            return Ok(Self::empty_scalar(token.end));
        }
        if !self.check(|kind| matches!(kind, TokenKind::BlockEnd))? {
            let context_mark = self.last_mark();
            let token = self.peek()?;
            return Err(YamlError::new(
                Some("while parsing a block mapping".to_owned()),
                context_mark,
                format!(
                    "expected <block end>, but found {}",
                    repr_str(token.kind.id())
                ),
                Some(token.start),
            ));
        }
        let token = self.get()?;
        self.pop_state();
        self.pop_mark();
        Ok((Event::MappingEnd, token.start))
    }

    fn parse_block_mapping_value(&mut self) -> Parsed {
        if self.check(|kind| matches!(kind, TokenKind::Value))? {
            let token = self.get()?;
            let ends = self.check(|kind| {
                matches!(
                    kind,
                    TokenKind::Key | TokenKind::Value | TokenKind::BlockEnd
                )
            })?;
            if !ends {
                return self.block_node_then(State::BlockMappingKey, true);
            }
            self.state = Some(State::BlockMappingKey);
            return Ok(Self::empty_scalar(token.end));
        }
        self.state = Some(State::BlockMappingKey);
        let mark = self.peek()?.start;
        Ok(Self::empty_scalar(mark))
    }

    // ── Flow collections ────────────────────────────────────────────────────

    fn parse_flow_sequence_first_entry(&mut self) -> Parsed {
        let token = self.get()?;
        self.marks.push(token.start);
        self.parse_flow_sequence_entry(true)
    }

    fn parse_flow_sequence_entry(&mut self, first: bool) -> Parsed {
        if !self.check(|kind| matches!(kind, TokenKind::FlowSequenceEnd))? {
            if !first {
                if self.check(|kind| matches!(kind, TokenKind::FlowEntry))? {
                    self.get()?;
                } else {
                    let context_mark = self.last_mark();
                    let token = self.peek()?;
                    return Err(YamlError::new(
                        Some("while parsing a flow sequence".to_owned()),
                        context_mark,
                        format!("expected ',' or ']', but got {}", repr_str(token.kind.id())),
                        Some(token.start),
                    ));
                }
            }
            if self.check(|kind| matches!(kind, TokenKind::Key))? {
                let start = self.peek()?.start;
                self.state = Some(State::FlowSequenceEntryMappingKey);
                return Ok((
                    Event::MappingStart {
                        anchor: None,
                        tag: None,
                    },
                    start,
                ));
            }
            if !self.check(|kind| matches!(kind, TokenKind::FlowSequenceEnd))? {
                return self.flow_node_then(State::FlowSequenceEntry);
            }
        }
        let token = self.get()?;
        self.pop_state();
        self.pop_mark();
        Ok((Event::SequenceEnd, token.start))
    }

    fn parse_flow_sequence_entry_mapping_key(&mut self) -> Parsed {
        let token = self.get()?;
        let ends = self.check(|kind| {
            matches!(
                kind,
                TokenKind::Value | TokenKind::FlowEntry | TokenKind::FlowSequenceEnd
            )
        })?;
        if !ends {
            return self.flow_node_then(State::FlowSequenceEntryMappingValue);
        }
        self.state = Some(State::FlowSequenceEntryMappingValue);
        Ok(Self::empty_scalar(token.end))
    }

    fn parse_flow_sequence_entry_mapping_value(&mut self) -> Parsed {
        if self.check(|kind| matches!(kind, TokenKind::Value))? {
            let token = self.get()?;
            let ends = self
                .check(|kind| matches!(kind, TokenKind::FlowEntry | TokenKind::FlowSequenceEnd))?;
            if !ends {
                return self.flow_node_then(State::FlowSequenceEntryMappingEnd);
            }
            self.state = Some(State::FlowSequenceEntryMappingEnd);
            return Ok(Self::empty_scalar(token.end));
        }
        self.state = Some(State::FlowSequenceEntryMappingEnd);
        let mark = self.peek()?.start;
        Ok(Self::empty_scalar(mark))
    }

    fn parse_flow_sequence_entry_mapping_end(&mut self) -> Parsed {
        self.state = Some(State::FlowSequenceEntry);
        let start = self.peek()?.start;
        Ok((Event::MappingEnd, start))
    }

    fn parse_flow_mapping_first_key(&mut self) -> Parsed {
        let token = self.get()?;
        self.marks.push(token.start);
        self.parse_flow_mapping_key(true)
    }

    fn parse_flow_mapping_key(&mut self, first: bool) -> Parsed {
        if !self.check(|kind| matches!(kind, TokenKind::FlowMappingEnd))? {
            if !first {
                if self.check(|kind| matches!(kind, TokenKind::FlowEntry))? {
                    self.get()?;
                } else {
                    let context_mark = self.last_mark();
                    let token = self.peek()?;
                    return Err(YamlError::new(
                        Some("while parsing a flow mapping".to_owned()),
                        context_mark,
                        format!(
                            "expected ',' or '}}', but got {}",
                            repr_str(token.kind.id())
                        ),
                        Some(token.start),
                    ));
                }
            }
            if self.check(|kind| matches!(kind, TokenKind::Key))? {
                let token = self.get()?;
                let ends = self.check(|kind| {
                    matches!(
                        kind,
                        TokenKind::Value | TokenKind::FlowEntry | TokenKind::FlowMappingEnd
                    )
                })?;
                if !ends {
                    return self.flow_node_then(State::FlowMappingValue);
                }
                self.state = Some(State::FlowMappingValue);
                return Ok(Self::empty_scalar(token.end));
            }
            if !self.check(|kind| matches!(kind, TokenKind::FlowMappingEnd))? {
                return self.flow_node_then(State::FlowMappingEmptyValue);
            }
        }
        let token = self.get()?;
        self.pop_state();
        self.pop_mark();
        Ok((Event::MappingEnd, token.start))
    }

    fn parse_flow_mapping_value(&mut self) -> Parsed {
        if self.check(|kind| matches!(kind, TokenKind::Value))? {
            let token = self.get()?;
            let ends = self
                .check(|kind| matches!(kind, TokenKind::FlowEntry | TokenKind::FlowMappingEnd))?;
            if !ends {
                return self.flow_node_then(State::FlowMappingKey);
            }
            self.state = Some(State::FlowMappingKey);
            return Ok(Self::empty_scalar(token.end));
        }
        self.state = Some(State::FlowMappingKey);
        let mark = self.peek()?.start;
        Ok(Self::empty_scalar(mark))
    }

    fn parse_flow_mapping_empty_value(&mut self) -> Parsed {
        self.state = Some(State::FlowMappingKey);
        let mark = self.peek()?.start;
        Ok(Self::empty_scalar(mark))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn events(text: &str) -> Result<Vec<String>, String> {
        let mut parser = Parser::new(text).map_err(|error| error.render(text))?;
        let mut out = Vec::new();
        loop {
            let (event, _) = parser.next().map_err(|error| error.render(text))?;
            let done = matches!(event, Event::StreamEnd);
            out.push(format!("{event:?}"));
            if done {
                return Ok(out);
            }
        }
    }

    #[test]
    fn a_tagged_quoted_scalar_resolves_its_handle() {
        let got = events("a: !!str 'x'\n").unwrap_or_default();
        assert!(
            got.iter()
                .any(|event| event.contains("tag: Some(\"tag:yaml.org,2002:str\")")),
            "{got:?}"
        );
    }

    #[test]
    fn errors_use_the_pyyaml_words() {
        let got = events("a:\n  b: 1\n c: 2\n").unwrap_err();
        assert_eq!(
            got,
            "while parsing a block mapping\n  in \"<unicode string>\", line 1, column 1:\n    a:\n    ^\nexpected <block end>, but found '<block mapping start>'\n  in \"<unicode string>\", line 3, column 2:\n     c: 2\n     ^"
        );
        let got = events("[a b: c d]\n").unwrap_or_default();
        assert_eq!(got.len(), 10, "{got:?}");
        let got = events("{a: 1, b c: 2 d: 3}").unwrap_err();
        assert!(got.contains("expected ',' or '}', but got ':'"), "{got}");
    }

    #[test]
    fn an_undefined_handle_and_a_duplicate_directive_fail() {
        assert!(
            events("a: !e!x 1\n")
                .unwrap_err()
                .starts_with("while parsing a node\nfound undefined tag handle '!e!'")
        );
        assert!(
            events("%YAML 1.1\n%YAML 1.1\n--- a\n")
                .unwrap_err()
                .starts_with("found duplicate YAML directive")
        );
        assert!(
            events("%YAML 2.0\n--- a\n")
                .unwrap_err()
                .starts_with("found incompatible YAML document")
        );
    }
}
