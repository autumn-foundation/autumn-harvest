//! A YAML reader that keeps the observable results of `PyYAML` `SafeLoader`.
//!
//! Three audits of this unit read YAML: `workflow-yaml-parse`, `action-sha-pin`
//! and `worker-image-contract`. Each Python original used `PyYAML`. Their
//! findings depend on `PyYAML` details that `serde_yaml` does not keep.
//!
//! - `serde_yaml` follows YAML 1.2. `PyYAML` follows YAML 1.1, so a plain `on`
//!   is `True` and `0755` is an octal integer.
//! - `serde_yaml` drops the position of each node. `action-sha-pin` reads the
//!   source line of each `uses` key.
//! - `serde_yaml` rejects a repeated mapping key in every file. `PyYAML` keeps
//!   the last value, and only `workflow-yaml-parse` asks for the rejection.
//! - libyaml, under `serde_yaml`, accepts and rejects other documents than the
//!   pure Python scanner. It also words each error in its own way.
//!
//! So [`super::pyscan`] and [`super::pyparse`] port the pure Python reader,
//! scanner and parser. This module composes nodes as `yaml.composer` does. It
//! resolves tags and constructs values as `yaml.constructor.SafeConstructor`
//! does. An error renders in the layout of a `PyYAML` `MarkedYAMLError`.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Write as _;
use std::sync::LazyLock;

use regex::Regex;

use super::pyparse::{Event, Parser};
use super::pytext::repr_str;

pub const NULL: &str = "tag:yaml.org,2002:null";
pub const BOOL: &str = "tag:yaml.org,2002:bool";
pub const INT: &str = "tag:yaml.org,2002:int";
pub const FLOAT: &str = "tag:yaml.org,2002:float";
pub const BINARY: &str = "tag:yaml.org,2002:binary";
pub const TIMESTAMP: &str = "tag:yaml.org,2002:timestamp";
pub const OMAP: &str = "tag:yaml.org,2002:omap";
pub const PAIRS: &str = "tag:yaml.org,2002:pairs";
pub const SET: &str = "tag:yaml.org,2002:set";
pub const STR: &str = "tag:yaml.org,2002:str";
pub const SEQ: &str = "tag:yaml.org,2002:seq";
pub const MAP: &str = "tag:yaml.org,2002:map";
pub const MERGE: &str = "tag:yaml.org,2002:merge";
pub const VALUE: &str = "tag:yaml.org,2002:value";
/// The tag of the resolver that `PyYAML` registers for `!`, `&` and `*`.
pub const YAML: &str = "tag:yaml.org,2002:yaml";

/// The name that `PyYAML` gives a stream read from a Python `str`.
const STREAM_NAME: &str = "<unicode string>";

// ── Errors ──────────────────────────────────────────────────────────────────

/// A position in the input. `index` counts characters, as the pointer of a
/// `PyYAML` mark does. `line` and `column` count from zero.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Mark {
    pub index: usize,
    pub line: usize,
    pub column: usize,
}

impl Mark {
    /// Renders the mark as `PyYAML` `Mark.__str__`, with its source snippet.
    fn render(&self, text: &str) -> String {
        let mut out = format!(
            "  in \"{STREAM_NAME}\", line {}, column {}",
            self.line + 1,
            self.column + 1
        );
        out.push_str(":\n");
        out.push_str(&self.snippet(text));
        out
    }

    /// Ports `Mark.get_snippet` with its defaults: indent 4, max length 75.
    ///
    /// `PyYAML` appends a NUL to the buffer, so the snippet stops there too.
    fn snippet(&self, text: &str) -> String {
        const INDENT: usize = 4;
        const HALF: usize = 36;
        let is_break =
            |c: char| matches!(c, '\0' | '\r' | '\n' | '\u{85}' | '\u{2028}' | '\u{2029}');
        let buffer: Vec<char> = text.chars().chain(std::iter::once('\0')).collect();
        let pointer = self.index.min(buffer.len());
        let mut head = "";
        let mut start = pointer;
        while start > 0 && !is_break(buffer[start - 1]) {
            start -= 1;
            // Python tests `pointer - start > max_length / 2 - 1`, which is 36.5.
            if pointer - start > HALF {
                head = " ... ";
                start += 5;
                break;
            }
        }
        let mut tail = "";
        let mut end = pointer;
        while end < buffer.len() && !is_break(buffer[end]) {
            end += 1;
            if end - pointer > HALF {
                tail = " ... ";
                end -= 5;
                break;
            }
        }
        let snippet: String = buffer[start..end].iter().collect();
        let caret = INDENT + pointer - start + head.len();
        format!(
            "{}{head}{snippet}{tail}\n{}^",
            " ".repeat(INDENT),
            " ".repeat(caret)
        )
    }
}

/// A YAML error with the parts of a `PyYAML` `MarkedYAMLError`. The parts sit
/// in a box, so a `Result` that holds the error stays small.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct YamlError(Box<ErrorParts>);

#[derive(Clone, Debug, PartialEq, Eq)]
struct ErrorParts {
    context: Option<String>,
    context_mark: Option<Mark>,
    problem: String,
    problem_mark: Option<Mark>,
    /// A `ReaderError` names a character and its position, and has no mark.
    reader: Option<(u32, usize)>,
}

impl YamlError {
    pub(super) fn new(
        context: Option<String>,
        context_mark: Option<Mark>,
        problem: impl Into<String>,
        problem_mark: Option<Mark>,
    ) -> Self {
        Self(Box::new(ErrorParts {
            context,
            context_mark,
            problem: problem.into(),
            problem_mark,
            reader: None,
        }))
    }

    /// Builds the `ReaderError` of a special character at a character
    /// position.
    pub(super) fn reader(character: u32, position: usize) -> Self {
        let mut error = Self::new(None, None, "special characters are not allowed", None);
        error.0.reader = Some((character, position));
        error
    }

    fn at(problem: impl Into<String>, mark: Mark) -> Self {
        Self::new(None, None, problem, Some(mark))
    }

    fn within(context: &str, context_mark: Mark, problem: impl Into<String>, mark: Mark) -> Self {
        Self::new(
            Some(context.to_owned()),
            Some(context_mark),
            problem,
            Some(mark),
        )
    }

    /// Renders the error as `str()` of the `PyYAML` error renders it.
    ///
    /// `text` is the parsed input. Each mark quotes its line from it.
    pub fn render(&self, text: &str) -> String {
        let parts = &*self.0;
        if let Some((value, position)) = parts.reader {
            return format!(
                "unacceptable character #x{value:04x}: {}\n  in \"{STREAM_NAME}\", position {position}",
                parts.problem
            );
        }
        let mut lines = Vec::new();
        if let Some(context) = &parts.context {
            lines.push(context.clone());
        }
        if let Some(context_mark) = &parts.context_mark {
            let same = parts.problem_mark.is_some_and(|problem| {
                problem.line == context_mark.line && problem.column == context_mark.column
            });
            if !same {
                lines.push(context_mark.render(text));
            }
        }
        lines.push(parts.problem.clone());
        if let Some(problem_mark) = &parts.problem_mark {
            lines.push(problem_mark.render(text));
        }
        lines.join("\n")
    }
}

// ── Composer ────────────────────────────────────────────────────────────────

/// The shape of a node, as `ScalarNode`, `SequenceNode` and `MappingNode`.
#[derive(Clone, Debug)]
pub enum Kind {
    Scalar(String),
    Sequence(Vec<usize>),
    Mapping(Vec<(usize, usize)>),
}

impl Kind {
    /// The `id` attribute of the `PyYAML` node class.
    const fn id(&self) -> &'static str {
        match self {
            Self::Scalar(_) => "scalar",
            Self::Sequence(_) => "sequence",
            Self::Mapping(_) => "mapping",
        }
    }
}

/// One composed node. Its children are indexes into [`Graph::nodes`].
#[derive(Clone, Debug)]
pub struct Node {
    pub tag: String,
    pub start: Mark,
    pub kind: Kind,
}

/// The nodes of a composed stream. An alias shares the node of its anchor.
#[derive(Clone, Debug, Default)]
pub struct Graph {
    pub nodes: Vec<Node>,
}

/// The YAML 1.1 rules of `yaml.resolver.Resolver`, in registration order.
static IMPLICIT: LazyLock<Vec<(&'static str, Regex)>> = LazyLock::new(|| {
    let rules = [
        (
            BOOL,
            r"^(?:yes|Yes|YES|no|No|NO|true|True|TRUE|false|False|FALSE|on|On|ON|off|Off|OFF)$",
        ),
        (
            FLOAT,
            r"^(?:[-+]?(?:[0-9][0-9_]*)\.[0-9_]*(?:[eE][-+][0-9]+)?|\.[0-9][0-9_]*(?:[eE][-+][0-9]+)?|[-+]?[0-9][0-9_]*(?::[0-5]?[0-9])+\.[0-9_]*|[-+]?\.(?:inf|Inf|INF)|\.(?:nan|NaN|NAN))$",
        ),
        (
            INT,
            r"^(?:[-+]?0b[0-1_]+|[-+]?0[0-7_]+|[-+]?(?:0|[1-9][0-9_]*)|[-+]?0x[0-9a-fA-F_]+|[-+]?[1-9][0-9_]*(?::[0-5]?[0-9])+)$",
        ),
        (MERGE, r"^(?:<<)$"),
        (NULL, r"^(?:~|null|Null|NULL|)$"),
        (
            TIMESTAMP,
            r"^(?:[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]|[0-9][0-9][0-9][0-9]-[0-9][0-9]?-[0-9][0-9]?(?:[Tt]|[ \t]+)[0-9][0-9]?:[0-9][0-9]:[0-9][0-9](?:\.[0-9]*)?(?:[ \t]*(?:Z|[-+][0-9][0-9]?(?::[0-9][0-9])?))?)$",
        ),
        (VALUE, r"^(?:=)$"),
        // `PyYAML` registers this rule for documentation only. A plain scalar
        // cannot start with these characters, but a scalar tagged `!` can.
        (YAML, r"^(?:!|&|\*)$"),
    ];
    // A Python `$` also matches before a final line break. Only a quoted
    // scalar tagged `!` can end in one.
    rules
        .into_iter()
        .filter_map(|(tag, pattern)| {
            let pattern = format!("{}\\n?$", pattern.strip_suffix('$').unwrap_or(pattern));
            Regex::new(&pattern).ok().map(|re| (tag, re))
        })
        .collect()
});

/// Resolves the tag of an untagged scalar, as `Resolver.resolve` does.
fn resolve_scalar(value: &str, plain_implicit: bool) -> &'static str {
    if plain_implicit && let Some((tag, _)) = IMPLICIT.iter().find(|(_, re)| re.is_match(value)) {
        return tag;
    }
    STR
}

/// Composes nodes from parser events, as `yaml.composer.Composer` does.
struct Composer {
    parser: Parser,
    peeked: Option<(Event, Mark)>,
    graph: Graph,
    anchors: HashMap<String, usize>,
}

impl Composer {
    fn new(text: &str) -> Result<Self, YamlError> {
        Ok(Self {
            parser: Parser::new(text)?,
            peeked: None,
            graph: Graph::default(),
            anchors: HashMap::new(),
        })
    }

    fn peek(&mut self) -> Result<&Event, YamlError> {
        if self.peeked.is_none() {
            self.peeked = Some(self.parser.next()?);
        }
        self.peeked
            .as_ref()
            .map(|(event, _)| event)
            .ok_or_else(|| YamlError::new(None, None, "no event", None))
    }

    fn next(&mut self) -> Result<(Event, Mark), YamlError> {
        match self.peeked.take() {
            Some(event) => Ok(event),
            None => self.parser.next(),
        }
    }

    fn at_stream_end(&mut self) -> Result<bool, YamlError> {
        Ok(matches!(self.peek()?, Event::StreamEnd))
    }

    /// Ports `Composer.check_node`: drops the stream start, then reports
    /// whether a document follows.
    fn check_node(&mut self) -> Result<bool, YamlError> {
        if matches!(self.peek()?, Event::StreamStart) {
            self.next()?;
        }
        Ok(!self.at_stream_end()?)
    }

    fn compose_document(&mut self) -> Result<usize, YamlError> {
        self.next()?;
        let node = self.compose_node()?;
        self.next()?;
        self.anchors.clear();
        Ok(node)
    }

    fn check_anchor(&self, anchor: Option<&String>, mark: Mark) -> Result<(), YamlError> {
        if let Some(anchor) = anchor
            && let Some(&first) = self.anchors.get(anchor)
        {
            return Err(YamlError::new(
                Some(format!(
                    "found duplicate anchor {}; first occurrence",
                    repr_str(anchor)
                )),
                Some(self.graph.nodes[first].start),
                "second occurrence",
                Some(mark),
            ));
        }
        Ok(())
    }

    fn push(&mut self, anchor: Option<String>, node: Node) -> usize {
        let id = self.graph.nodes.len();
        self.graph.nodes.push(node);
        // PyYAML registers the anchor before it composes the children.
        if let Some(anchor) = anchor {
            self.anchors.insert(anchor, id);
        }
        id
    }

    fn compose_node(&mut self) -> Result<usize, YamlError> {
        let (event, mark) = self.next()?;
        match event {
            Event::Alias(anchor) => self.anchors.get(&anchor).copied().ok_or_else(|| {
                YamlError::at(format!("found undefined alias {}", repr_str(&anchor)), mark)
            }),
            Event::Scalar {
                anchor,
                tag,
                value,
                plain_implicit,
            } => {
                self.check_anchor(anchor.as_ref(), mark)?;
                let tag = match tag {
                    Some(tag) if tag != "!" => tag,
                    _ => resolve_scalar(&value, plain_implicit).to_owned(),
                };
                let node = Node {
                    tag,
                    start: mark,
                    kind: Kind::Scalar(value),
                };
                Ok(self.push(anchor, node))
            }
            Event::SequenceStart { anchor, tag } => {
                self.check_anchor(anchor.as_ref(), mark)?;
                let node = Node {
                    tag: collection_tag(tag, SEQ),
                    start: mark,
                    kind: Kind::Sequence(Vec::new()),
                };
                let id = self.push(anchor, node);
                let mut items = Vec::new();
                while !matches!(self.peek()?, Event::SequenceEnd) {
                    items.push(self.compose_node()?);
                }
                self.next()?;
                self.graph.nodes[id].kind = Kind::Sequence(items);
                Ok(id)
            }
            Event::MappingStart { anchor, tag } => {
                self.check_anchor(anchor.as_ref(), mark)?;
                let node = Node {
                    tag: collection_tag(tag, MAP),
                    start: mark,
                    kind: Kind::Mapping(Vec::new()),
                };
                let id = self.push(anchor, node);
                let mut pairs = Vec::new();
                while !matches!(self.peek()?, Event::MappingEnd) {
                    let key = self.compose_node()?;
                    let value = self.compose_node()?;
                    pairs.push((key, value));
                }
                self.next()?;
                self.graph.nodes[id].kind = Kind::Mapping(pairs);
                Ok(id)
            }
            other => Err(YamlError::at(format!("unexpected event {other:?}"), mark)),
        }
    }
}

/// Resolves the tag of a collection. A missing tag or `!` takes the default.
fn collection_tag(tag: Option<String>, default: &str) -> String {
    match tag {
        Some(tag) if tag != "!" => tag,
        _ => default.to_owned(),
    }
}

/// Composes every document, as `yaml.compose_all` does. Returns the graph and
/// the root node of each document.
pub fn compose_all(text: &str) -> Result<(Graph, Vec<usize>), YamlError> {
    let mut composer = Composer::new(text)?;
    let mut roots = Vec::new();
    while composer.check_node()? {
        roots.push(composer.compose_document()?);
    }
    Ok((composer.graph, roots))
}

/// Composes the only document, as `Composer.get_single_node` does.
fn compose_single(text: &str) -> Result<(Graph, Option<usize>), YamlError> {
    let mut composer = Composer::new(text)?;
    composer.next()?;
    let root = if composer.at_stream_end()? {
        None
    } else {
        Some(composer.compose_document()?)
    };
    if !composer.at_stream_end()? {
        let (_, mark) = composer.next()?;
        let first = root.map(|id| composer.graph.nodes[id].start);
        return Err(YamlError::new(
            Some("expected a single document in the stream".to_owned()),
            first,
            "but found another document",
            Some(mark),
        ));
    }
    Ok((composer.graph, root))
}

// ── Constructor ─────────────────────────────────────────────────────────────

/// A constructed Python value.
///
/// `Other` holds a `!!binary` or `!!timestamp` scalar as its source text. An
/// `!!omap` or `!!pairs` entry is a two-item `List`, where Python has a tuple.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    None,
    Bool(bool),
    Int(i128),
    /// An integer outside the range of `i128`, as its decimal digits.
    BigInt(String),
    Float(f64),
    Str(String),
    Other(String),
    List(Vec<Self>),
    Dict(Vec<(Self, Self)>),
    Set(Vec<Self>),
}

/// One constructed value inside the constructor. A collection holds slot
/// indexes, so a recursive document constructs as in `PyYAML`.
#[derive(Clone, Debug)]
enum Slot {
    Scalar(Value),
    List(Vec<usize>),
    Dict(Vec<(usize, usize)>),
    Set(Vec<usize>),
}

/// The constructed values of one document.
#[derive(Debug)]
pub struct Document {
    slots: Vec<Slot>,
    root: Option<usize>,
}

impl Document {
    /// Returns the value of `key` when `slot` is a dict, as `dict.get` does.
    fn dict_get(&self, slot: usize, key: &str) -> Option<usize> {
        let Slot::Dict(pairs) = &self.slots[slot] else {
            return None;
        };
        pairs.iter().find_map(|&(k, v)| match &self.slots[k] {
            Slot::Scalar(Value::Str(name)) if name == key => Some(v),
            _ => None,
        })
    }

    /// Ports `doc.get(key) if isinstance(doc, dict) else None`, then reports
    /// whether that value is a non-empty dict.
    pub fn root_has_nonempty_dict(&self, key: &str) -> bool {
        self.root
            .and_then(|root| self.dict_get(root, key))
            .is_some_and(|slot| matches!(&self.slots[slot], Slot::Dict(pairs) if !pairs.is_empty()))
    }

    /// Builds the value tree of the document root.
    pub fn into_value(self) -> Result<Value, YamlError> {
        self.root.map_or(Ok(Value::None), |root| {
            self.materialize(root, &mut HashSet::new())
        })
    }

    fn materialize(&self, slot: usize, path: &mut HashSet<usize>) -> Result<Value, YamlError> {
        if !path.insert(slot) {
            return Err(YamlError::new(
                None,
                None,
                "found a recursive structure, which this port cannot build as a tree",
                None,
            ));
        }
        let value = match &self.slots[slot] {
            Slot::Scalar(value) => value.clone(),
            Slot::List(items) => Value::List(
                items
                    .iter()
                    .map(|&item| self.materialize(item, path))
                    .collect::<Result<_, _>>()?,
            ),
            Slot::Set(items) => Value::Set(
                items
                    .iter()
                    .map(|&item| self.materialize(item, path))
                    .collect::<Result<_, _>>()?,
            ),
            Slot::Dict(pairs) => Value::Dict(
                pairs
                    .iter()
                    .map(|&(k, v)| Ok((self.materialize(k, path)?, self.materialize(v, path)?)))
                    .collect::<Result<_, YamlError>>()?,
            ),
        };
        path.remove(&slot);
        Ok(value)
    }
}

/// Ports `SafeConstructor`, plus the repeated-key check of
/// `workflow-yaml-parse` when `unique_keys` is set.
struct Constructor<'g> {
    graph: &'g mut Graph,
    unique_keys: bool,
    slots: Vec<Slot>,
    cache: HashMap<usize, usize>,
    /// The collections that wait for their content. `PyYAML` defers each one
    /// through a generator, so it fills them in this first-in, first-out order.
    queue: VecDeque<(usize, usize)>,
    /// The mappings that `flatten_mapping` is inside, to stop a merge cycle.
    flattening: HashSet<usize>,
}

impl<'g> Constructor<'g> {
    fn new(graph: &'g mut Graph, unique_keys: bool) -> Self {
        Self {
            graph,
            unique_keys,
            slots: Vec::new(),
            cache: HashMap::new(),
            queue: VecDeque::new(),
            flattening: HashSet::new(),
        }
    }

    /// Ports `construct_document`: the root first, then each deferred fill.
    fn construct_document(mut self, root: Option<usize>) -> Result<Document, YamlError> {
        let root = root.map(|node| self.construct_object(node)).transpose()?;
        while let Some((node, slot)) = self.queue.pop_front() {
            self.fill(node, slot)?;
        }
        Ok(Document {
            slots: self.slots,
            root,
        })
    }

    fn alloc(&mut self, slot: Slot) -> usize {
        self.slots.push(slot);
        self.slots.len() - 1
    }

    fn scalar_of(&self, node: usize) -> Result<&str, YamlError> {
        let node = &self.graph.nodes[node];
        match &node.kind {
            Kind::Scalar(value) => Ok(value),
            other => Err(YamlError::at(
                format!("expected a scalar node, but found {}", other.id()),
                node.start,
            )),
        }
    }

    fn construct_object(&mut self, node: usize) -> Result<usize, YamlError> {
        if let Some(&slot) = self.cache.get(&node) {
            return Ok(slot);
        }
        let tag = self.graph.nodes[node].tag.clone();
        let start = self.graph.nodes[node].start;
        let slot = match tag.as_str() {
            SEQ | OMAP | PAIRS => {
                let slot = self.alloc(Slot::List(Vec::new()));
                self.queue.push_back((node, slot));
                slot
            }
            MAP => {
                let slot = self.alloc(Slot::Dict(Vec::new()));
                self.queue.push_back((node, slot));
                slot
            }
            SET => {
                let slot = self.alloc(Slot::Set(Vec::new()));
                self.queue.push_back((node, slot));
                slot
            }
            NULL | BOOL | INT | FLOAT | BINARY | TIMESTAMP | STR => {
                let text = self.scalar_of(node)?.to_owned();
                let value = construct_scalar(&tag, &text, start)?;
                self.alloc(Slot::Scalar(value))
            }
            _ => {
                return Err(YamlError::at(
                    format!(
                        "could not determine a constructor for the tag {}",
                        repr_str(&tag)
                    ),
                    start,
                ));
            }
        };
        self.cache.insert(node, slot);
        Ok(slot)
    }

    fn fill(&mut self, node: usize, slot: usize) -> Result<(), YamlError> {
        let tag = self.graph.nodes[node].tag.clone();
        let start = self.graph.nodes[node].start;
        match tag.as_str() {
            SEQ => {
                let items = match &self.graph.nodes[node].kind {
                    Kind::Sequence(items) => items.clone(),
                    other => {
                        return Err(YamlError::at(
                            format!("expected a sequence node, but found {}", other.id()),
                            start,
                        ));
                    }
                };
                let items = items
                    .into_iter()
                    .map(|item| self.construct_object(item))
                    .collect::<Result<_, _>>()?;
                self.slots[slot] = Slot::List(items);
            }
            OMAP | PAIRS => {
                let context = if tag == OMAP {
                    "while constructing an ordered map"
                } else {
                    "while constructing pairs"
                };
                let items = match &self.graph.nodes[node].kind {
                    Kind::Sequence(items) => items.clone(),
                    other => {
                        let problem = format!("expected a sequence, but found {}", other.id());
                        return Err(YamlError::within(context, start, problem, start));
                    }
                };
                let mut entries = Vec::new();
                for item in items {
                    let sub = &self.graph.nodes[item];
                    let (key, value) = match &sub.kind {
                        Kind::Mapping(pairs) if pairs.len() == 1 => pairs[0],
                        Kind::Mapping(pairs) => {
                            let problem = format!(
                                "expected a single mapping item, but found {} items",
                                pairs.len()
                            );
                            return Err(YamlError::within(context, start, problem, sub.start));
                        }
                        other => {
                            let problem =
                                format!("expected a mapping of length 1, but found {}", other.id());
                            return Err(YamlError::within(context, start, problem, sub.start));
                        }
                    };
                    let key = self.construct_object(key)?;
                    let value = self.construct_object(value)?;
                    entries.push(self.alloc(Slot::List(vec![key, value])));
                }
                self.slots[slot] = Slot::List(entries);
            }
            MAP => {
                let pairs = self.construct_mapping(node)?;
                self.slots[slot] = Slot::Dict(pairs);
            }
            SET => {
                let pairs = self.construct_mapping(node)?;
                self.slots[slot] = Slot::Set(pairs.into_iter().map(|(key, _)| key).collect());
            }
            _ => {}
        }
        Ok(())
    }

    /// Ports the `construct_mapping` override of `UniqueKeyLoader`.
    ///
    /// It compares the spelling of each key, not the resolved value. YAML 1.1
    /// turns both `on` and `true` into `True`, but GitHub keeps them distinct.
    fn check_unique_keys(&mut self, node: usize) -> Result<(), YamlError> {
        let Kind::Mapping(pairs) = self.graph.nodes[node].kind.clone() else {
            return Ok(());
        };
        let mut seen = HashSet::new();
        for (key, _) in pairs {
            // Python compares `repr` of a collection key that it constructs in
            // full. A spelling and a `repr` share one set, as in Python.
            let spelling = if let Kind::Scalar(value) = &self.graph.nodes[key].kind {
                value.clone()
            } else {
                let slot = self.construct_deep(key)?;
                self.slot_repr(slot, &mut Vec::new())
            };
            if !seen.insert(spelling.clone()) {
                return Err(YamlError::at(
                    format!("duplicate mapping key {}", repr_str(&spelling)),
                    self.graph.nodes[key].start,
                ));
            }
        }
        Ok(())
    }

    /// Ports `construct_object(node, deep=True)`.
    ///
    /// A deep construction runs each generator at once, depth first. So its
    /// fills leave the queue and run before this call returns.
    fn construct_deep(&mut self, node: usize) -> Result<usize, YamlError> {
        let before = self.queue.len();
        let slot = self.construct_object(node)?;
        let mut pending = self.queue.split_off(before);
        while let Some((node, slot)) = pending.pop_front() {
            let before = self.queue.len();
            self.fill(node, slot)?;
            for item in self.queue.split_off(before).into_iter().rev() {
                pending.push_front(item);
            }
        }
        Ok(slot)
    }

    /// Ports `repr` of a constructed value. A collection that contains itself
    /// renders as `[...]` or `{...}`, as in Python.
    fn slot_repr(&self, slot: usize, stack: &mut Vec<usize>) -> String {
        let recursive = stack.contains(&slot);
        let join = |items: Vec<String>| items.join(", ");
        stack.push(slot);
        let out = match &self.slots[slot] {
            Slot::Scalar(value) => value.py_repr(),
            Slot::List(_) if recursive => "[...]".to_owned(),
            Slot::Dict(_) | Slot::Set(_) if recursive => "{...}".to_owned(),
            Slot::List(items) => {
                let items = items.iter().map(|&item| self.slot_repr(item, stack));
                format!("[{}]", join(items.collect()))
            }
            Slot::Set(items) if items.is_empty() => "set()".to_owned(),
            Slot::Set(items) => {
                let items = items.iter().map(|&item| self.slot_repr(item, stack));
                format!("{{{}}}", join(items.collect()))
            }
            Slot::Dict(pairs) => {
                let pairs = pairs.iter().map(|&(key, value)| {
                    format!(
                        "{}: {}",
                        self.slot_repr(key, stack),
                        self.slot_repr(value, stack)
                    )
                });
                format!("{{{}}}", join(pairs.collect()))
            }
        };
        stack.pop();
        out
    }

    /// Ports `SafeConstructor.construct_mapping` and the base version.
    fn construct_mapping(&mut self, node: usize) -> Result<Vec<(usize, usize)>, YamlError> {
        if self.unique_keys {
            self.check_unique_keys(node)?;
        }
        let start = self.graph.nodes[node].start;
        if matches!(self.graph.nodes[node].kind, Kind::Mapping(_)) {
            self.flatten_mapping(node)?;
        }
        let pairs = match &self.graph.nodes[node].kind {
            Kind::Mapping(pairs) => pairs.clone(),
            other => {
                return Err(YamlError::at(
                    format!("expected a mapping node, but found {}", other.id()),
                    start,
                ));
            }
        };
        let mut mapping: Vec<(usize, usize)> = Vec::new();
        for (key_node, value_node) in pairs {
            let key = self.construct_object(key_node)?;
            if !matches!(self.slots[key], Slot::Scalar(_)) {
                return Err(YamlError::within(
                    "while constructing a mapping",
                    start,
                    "found unhashable key",
                    self.graph.nodes[key_node].start,
                ));
            }
            let value = self.construct_object(value_node)?;
            // A Python dict keeps the first key and takes the last value.
            let existing = mapping.iter().position(|&(k, _)| self.keys_equal(k, key));
            match existing {
                Some(index) => mapping[index].1 = value,
                None => mapping.push((key, value)),
            }
        }
        Ok(mapping)
    }

    fn keys_equal(&self, a: usize, b: usize) -> bool {
        match (&self.slots[a], &self.slots[b]) {
            (Slot::Scalar(a), Slot::Scalar(b)) => py_key_eq(a, b),
            _ => false,
        }
    }

    /// Ports `SafeConstructor.flatten_mapping`. It edits the node in place, as
    /// `PyYAML` does, so a later read of the same node sees the merged pairs.
    fn flatten_mapping(&mut self, node: usize) -> Result<(), YamlError> {
        let start = self.graph.nodes[node].start;
        if !self.flattening.insert(node) {
            return Err(YamlError::at("found unconstructable recursive node", start));
        }
        let Kind::Mapping(pairs) = self.graph.nodes[node].kind.clone() else {
            self.flattening.remove(&node);
            return Ok(());
        };
        let mut merge = Vec::new();
        let mut kept = Vec::new();
        for (key, value) in pairs {
            let key_tag = self.graph.nodes[key].tag.clone();
            if key_tag == MERGE {
                let value_node = self.graph.nodes[value].clone();
                match &value_node.kind {
                    Kind::Mapping(_) => {
                        self.flatten_mapping(value)?;
                        if let Kind::Mapping(sub) = &self.graph.nodes[value].kind {
                            merge.extend(sub.iter().copied());
                        }
                    }
                    Kind::Sequence(items) => {
                        let mut submerge = Vec::new();
                        for &item in items {
                            let sub = &self.graph.nodes[item];
                            if !matches!(sub.kind, Kind::Mapping(_)) {
                                return Err(YamlError::within(
                                    "while constructing a mapping",
                                    start,
                                    format!(
                                        "expected a mapping for merging, but found {}",
                                        sub.kind.id()
                                    ),
                                    sub.start,
                                ));
                            }
                            self.flatten_mapping(item)?;
                            if let Kind::Mapping(sub) = &self.graph.nodes[item].kind {
                                submerge.push(sub.clone());
                            }
                        }
                        for sub in submerge.into_iter().rev() {
                            merge.extend(sub);
                        }
                    }
                    Kind::Scalar(_) => {
                        return Err(YamlError::within(
                            "while constructing a mapping",
                            start,
                            "expected a mapping or list of mappings for merging, but found scalar",
                            value_node.start,
                        ));
                    }
                }
            } else {
                if key_tag == VALUE {
                    STR.clone_into(&mut self.graph.nodes[key].tag);
                }
                kept.push((key, value));
            }
        }
        merge.extend(kept);
        self.graph.nodes[node].kind = Kind::Mapping(merge);
        self.flattening.remove(&node);
        Ok(())
    }
}

/// Compares two dict keys as Python does: `True == 1 == 1.0`.
fn py_key_eq(a: &Value, b: &Value) -> bool {
    let number = |value: &Value| match value {
        Value::Bool(flag) => Some(f64::from(u8::from(*flag))),
        #[allow(clippy::cast_precision_loss)]
        Value::Int(int) => Some(*int as f64),
        Value::Float(float) => Some(*float),
        _ => None,
    };
    match (a, b) {
        (Value::Int(a), Value::Int(b)) => a == b,
        (Value::Str(a), Value::Str(b))
        | (Value::Other(a), Value::Other(b))
        | (Value::BigInt(a), Value::BigInt(b)) => a == b,
        (Value::None, Value::None) => true,
        _ => match (number(a), number(b)) {
            (Some(a), Some(b)) => a == b,
            _ => false,
        },
    }
}

/// Ports the scalar constructors of `SafeConstructor`.
///
/// `PyYAML` raises a bare `ValueError` or `KeyError` for a bad `!!int`,
/// `!!float` or `!!bool` value. The port reports a YAML error at the node.
fn construct_scalar(tag: &str, text: &str, mark: Mark) -> Result<Value, YamlError> {
    let invalid = || {
        YamlError::at(
            format!(
                "invalid value {} for the tag {}",
                repr_str(text),
                repr_str(tag)
            ),
            mark,
        )
    };
    Ok(match tag {
        NULL => Value::None,
        BOOL => match text.to_lowercase().as_str() {
            "yes" | "true" | "on" => Value::Bool(true),
            "no" | "false" | "off" => Value::Bool(false),
            _ => return Err(invalid()),
        },
        INT => construct_int(text).ok_or_else(invalid)?,
        FLOAT => Value::Float(construct_float(text).ok_or_else(invalid)?),
        BINARY | TIMESTAMP => Value::Other(text.to_owned()),
        _ => Value::Str(text.to_owned()),
    })
}

/// Splits a leading sign off as `construct_yaml_int` does.
fn split_sign(value: &str) -> (i8, &str) {
    value.strip_prefix('-').map_or_else(
        || (1, value.strip_prefix('+').unwrap_or(value)),
        |rest| (-1, rest),
    )
}

/// A non-negative integer of any size, in limbs of base 10^9, least
/// significant first. A Python `int` has no size limit, so a long digit
/// string in a workflow is still an integer.
#[derive(Clone, Debug, Default)]
struct BigUint(Vec<u32>);

impl BigUint {
    const BASE: u64 = 1_000_000_000;

    /// Sets the value to `self * factor + addend`.
    fn mul_add(&mut self, factor: u32, addend: u32) {
        let mut carry = u64::from(addend);
        for limb in &mut self.0 {
            let total = u64::from(*limb) * u64::from(factor) + carry;
            *limb = u32::try_from(total % Self::BASE).unwrap_or(0);
            carry = total / Self::BASE;
        }
        while carry > 0 {
            self.0.push(u32::try_from(carry % Self::BASE).unwrap_or(0));
            carry /= Self::BASE;
        }
    }

    /// Sets the value to `self + other`.
    fn add(&mut self, other: &Self) {
        let mut carry = 0;
        for index in 0..self.0.len().max(other.0.len()) {
            if index == self.0.len() {
                self.0.push(0);
            }
            let total = u64::from(self.0[index])
                + u64::from(other.0.get(index).copied().unwrap_or(0))
                + carry;
            self.0[index] = u32::try_from(total % Self::BASE).unwrap_or(0);
            carry = total / Self::BASE;
        }
        if carry > 0 {
            self.0.push(u32::try_from(carry).unwrap_or(0));
        }
    }

    /// Ports `int(digits, radix)` for ASCII digits. Python strips the
    /// surrounding whitespace first.
    fn parse(digits: &str, radix: u32) -> Option<Self> {
        let digits = super::pytext::strip(digits);
        if digits.is_empty() {
            return None;
        }
        let mut value = Self::default();
        for ch in digits.chars() {
            value.mul_add(radix, ch.to_digit(radix)?);
        }
        Some(value)
    }

    fn to_decimal(&self) -> String {
        let mut limbs = self.0.iter().rev().skip_while(|limb| **limb == 0);
        let Some(first) = limbs.next() else {
            return "0".to_owned();
        };
        let mut out = first.to_string();
        for limb in limbs {
            let _ = write!(out, "{limb:09}");
        }
        out
    }
}

/// Builds an `Int`, or a `BigInt` when the value does not fit `i128`.
fn int_value(negative: bool, magnitude: &BigUint) -> Value {
    let decimal = magnitude.to_decimal();
    let signed = if negative && decimal != "0" {
        format!("-{decimal}")
    } else {
        decimal
    };
    signed
        .parse::<i128>()
        .map_or(Value::BigInt(signed), Value::Int)
}

/// Ports `construct_yaml_int`: binary, hex, octal, base 60 and decimal.
fn construct_int(text: &str) -> Option<Value> {
    let value = text.replace('_', "");
    let (sign, value) = split_sign(&value);
    let negative = sign < 0;
    if value == "0" {
        return Some(Value::Int(0));
    }
    let magnitude = if let Some(digits) = value.strip_prefix("0b") {
        BigUint::parse(digits, 2)?
    } else if let Some(digits) = value.strip_prefix("0x") {
        BigUint::parse(digits, 16)?
    } else if value.starts_with('0') {
        BigUint::parse(value, 8)?
    } else if value.contains(':') {
        let mut total = BigUint::default();
        for part in value.split(':') {
            total.mul_add(60, 0);
            total.add(&BigUint::parse(part, 10)?);
        }
        total
    } else {
        BigUint::parse(value, 10)?
    };
    Some(int_value(negative, &magnitude))
}

/// Ports `construct_yaml_float`: `.inf`, `.nan`, base 60 and decimal.
fn construct_float(text: &str) -> Option<f64> {
    let value = text.replace('_', "").to_lowercase();
    let (sign, value) = split_sign(&value);
    let sign = f64::from(sign);
    if value == ".inf" {
        return Some(sign * f64::INFINITY);
    }
    if value == ".nan" {
        return Some(f64::NAN);
    }
    if value.contains(':') {
        // Python sums `digit * base` from the last part, with `base *= 60`.
        // A fused multiply-add rounds differently, so the port keeps the
        // same plain operations.
        let mut total = 0.0;
        let mut base = 1.0;
        for part in value.rsplit(':') {
            let digit = part.parse::<f64>().ok()?;
            #[allow(clippy::suboptimal_flops)]
            {
                total += digit * base;
            }
            base *= 60.0;
        }
        return Some(sign * total);
    }
    value.parse::<f64>().ok().map(|float| sign * float)
}

/// Ports `yaml.load(text, Loader=...)` for one document.
///
/// `unique_keys` adds the repeated-key check of `workflow-yaml-parse`.
pub fn load(text: &str, unique_keys: bool) -> Result<Document, YamlError> {
    let (mut graph, root) = compose_single(text)?;
    Constructor::new(&mut graph, unique_keys).construct_document(root)
}

/// Ports `yaml.safe_load`.
pub fn safe_load(text: &str) -> Result<Value, YamlError> {
    load(text, false)?.into_value()
}

/// Ports `yaml.safe_load_all`. Each document constructs on its own.
pub fn safe_load_all(text: &str) -> Result<Vec<Value>, YamlError> {
    let (graph, roots) = compose_all(text)?;
    roots
        .into_iter()
        .map(|root| {
            let mut graph = graph.clone();
            Constructor::new(&mut graph, false)
                .construct_document(Some(root))?
                .into_value()
        })
        .collect()
}

// ── Python views of a value ─────────────────────────────────────────────────

impl Value {
    /// Ports `dict.get(key)` for a string key. A non-dict gives `None`.
    pub fn get(&self, key: &str) -> Option<&Self> {
        let Self::Dict(pairs) = self else {
            return None;
        };
        pairs.iter().find_map(|(k, v)| match k {
            Self::Str(name) if name == key => Some(v),
            _ => None,
        })
    }

    /// Ports `(self or {}).get(key)`. A falsy or non-dict value gives `None`.
    pub fn or_get<'a>(value: Option<&'a Self>, key: &str) -> Option<&'a Self> {
        value
            .filter(|value| value.truthy())
            .and_then(|value| value.get(key))
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::Str(text) => Some(text),
            _ => None,
        }
    }

    /// Ports `isinstance(value, int)`, which is true for a `bool` as well.
    ///
    /// A `BigInt` saturates at the bound of `i128`. It still compares as
    /// Python compares it with a small number.
    pub fn as_int(&self) -> Option<i128> {
        match self {
            Self::Int(int) => Some(*int),
            Self::BigInt(digits) if digits.starts_with('-') => Some(i128::MIN),
            Self::BigInt(_) => Some(i128::MAX),
            Self::Bool(flag) => Some(i128::from(*flag)),
            _ => None,
        }
    }

    /// Ports Python truthiness.
    pub fn truthy(&self) -> bool {
        match self {
            Self::None => false,
            Self::Bool(flag) => *flag,
            Self::Int(int) => *int != 0,
            Self::BigInt(_) => true,
            Self::Float(float) => *float != 0.0,
            Self::Str(text) | Self::Other(text) => !text.is_empty(),
            Self::List(items) | Self::Set(items) => !items.is_empty(),
            Self::Dict(pairs) => !pairs.is_empty(),
        }
    }

    /// Ports `str(value)`.
    pub fn py_str(&self) -> String {
        match self {
            Self::Str(text) | Self::Other(text) => text.clone(),
            other => other.py_repr(),
        }
    }

    /// Ports `repr(value)`.
    pub fn py_repr(&self) -> String {
        let join = |items: &[Self]| {
            items
                .iter()
                .map(Self::py_repr)
                .collect::<Vec<_>>()
                .join(", ")
        };
        match self {
            Self::None => "None".to_owned(),
            Self::Bool(true) => "True".to_owned(),
            Self::Bool(false) => "False".to_owned(),
            Self::Int(int) => int.to_string(),
            Self::BigInt(digits) => digits.clone(),
            Self::Float(float) => float_repr(*float),
            Self::Str(text) => repr_str(text),
            Self::Other(text) => text.clone(),
            Self::List(items) => format!("[{}]", join(items)),
            Self::Set(items) if items.is_empty() => "set()".to_owned(),
            Self::Set(items) => format!("{{{}}}", join(items)),
            Self::Dict(pairs) => {
                let mut out = String::from("{");
                for (index, (key, value)) in pairs.iter().enumerate() {
                    if index > 0 {
                        out.push_str(", ");
                    }
                    let _ = write!(out, "{}: {}", key.py_repr(), value.py_repr());
                }
                out.push('}');
                out
            }
        }
    }
}

/// Ports `repr(float)`: the shortest digits, in fixed form for an exponent
/// from -4 to 15, else in scientific form.
fn float_repr(value: f64) -> String {
    if value.is_nan() {
        return "nan".to_owned();
    }
    if value.is_infinite() {
        return if value > 0.0 { "inf" } else { "-inf" }.to_owned();
    }
    let scientific = format!("{value:e}");
    let Some((mantissa, exponent)) = scientific.split_once('e') else {
        return scientific;
    };
    let Ok(exponent) = exponent.parse::<i32>() else {
        return scientific;
    };
    let (sign, mantissa) = mantissa
        .strip_prefix('-')
        .map_or(("", mantissa), |rest| ("-", rest));
    let digits: String = mantissa.chars().filter(char::is_ascii_digit).collect();
    if !(-4..16).contains(&exponent) {
        let (first, rest) = digits.split_at(1);
        let fraction = if rest.is_empty() {
            String::new()
        } else {
            format!(".{rest}")
        };
        let exponent_sign = if exponent < 0 { '-' } else { '+' };
        return format!(
            "{sign}{first}{fraction}e{exponent_sign}{:02}",
            exponent.abs()
        );
    }
    let point = exponent + 1;
    let body = if point <= 0 {
        let zeros = "0".repeat(usize::try_from(-point).unwrap_or(0));
        format!("0.{zeros}{digits}")
    } else {
        let point = usize::try_from(point).unwrap_or(0);
        if digits.len() <= point {
            format!("{digits}{}.0", "0".repeat(point - digits.len()))
        } else {
            format!("{}.{}", &digits[..point], &digits[point..])
        }
    };
    format!("{sign}{body}")
}

impl From<serde_json::Value> for Value {
    /// Builds a value from JSON, for test fixtures.
    fn from(value: serde_json::Value) -> Self {
        match value {
            serde_json::Value::Null => Self::None,
            serde_json::Value::Bool(flag) => Self::Bool(flag),
            serde_json::Value::Number(number) => number
                .as_i64()
                .map(|int| Self::Int(i128::from(int)))
                .or_else(|| number.as_f64().map(Self::Float))
                .unwrap_or(Self::None),
            serde_json::Value::String(text) => Self::Str(text),
            serde_json::Value::Array(items) => {
                Self::List(items.into_iter().map(Self::from).collect())
            }
            serde_json::Value::Object(map) => Self::Dict(
                map.into_iter()
                    .map(|(key, value)| (Self::Str(key), Self::from(value)))
                    .collect(),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn load_err(text: &str, unique: bool) -> String {
        match load(text, unique) {
            Ok(_) => String::new(),
            Err(error) => error.render(text),
        }
    }

    #[test]
    fn yaml_1_1_resolution_matches_pyyaml() {
        let value = safe_load(
            "a: on\nb: 0755\nc: 1_000\nd: 1:30\ne: 1.5\nf: ~\ng: 'on'\nh: 0x1f\ni: .inf\n",
        )
        .unwrap_or(Value::None);
        assert_eq!(value.get("a"), Some(&Value::Bool(true)));
        assert_eq!(value.get("b"), Some(&Value::Int(493)));
        assert_eq!(value.get("c"), Some(&Value::Int(1000)));
        assert_eq!(value.get("d"), Some(&Value::Int(90)));
        assert_eq!(value.get("e"), Some(&Value::Float(1.5)));
        assert_eq!(value.get("f"), Some(&Value::None));
        assert_eq!(value.get("g"), Some(&Value::Str("on".into())));
        assert_eq!(value.get("h"), Some(&Value::Int(31)));
        assert_eq!(value.get("i"), Some(&Value::Float(f64::INFINITY)));
    }

    #[test]
    fn the_on_key_is_true_and_a_repeated_key_keeps_the_last_value() {
        let value = safe_load("on: push\nx: 1\nx: 2\n").unwrap_or(Value::None);
        let Value::Dict(pairs) = &value else {
            panic!("not a dict: {value:?}");
        };
        assert_eq!(pairs[0].0, Value::Bool(true));
        assert_eq!(value.get("x"), Some(&Value::Int(2)));
        assert_eq!(pairs.len(), 2);
    }

    #[test]
    fn merge_keys_flatten_with_explicit_keys_winning() {
        let text = "base: &b {x: 1, y: 2}\nm:\n  <<: *b\n  y: 3\n";
        let value = safe_load(text).unwrap_or(Value::None);
        let merged = value.get("m").cloned().unwrap_or(Value::None);
        assert_eq!(merged.get("x"), Some(&Value::Int(1)));
        assert_eq!(merged.get("y"), Some(&Value::Int(3)));
        let bad = load_err("m:\n  <<: 1\n", false);
        assert!(
            bad.contains("expected a mapping or list of mappings for merging"),
            "{bad}"
        );
    }

    #[test]
    fn a_repeated_key_renders_as_pyyaml_does() {
        let text = "on: push\non: pull_request\njobs: {}\n";
        assert_eq!(
            load_err(text, true),
            "duplicate mapping key 'on'\n  in \"<unicode string>\", line 2, column 1:\n    on: pull_request\n    ^"
        );
        // The spelling decides, so `on` and `true` are distinct keys.
        assert_eq!(load_err("on: 1\ntrue: 2\n", true), "");
        // A quoted key spells the same as a plain one.
        assert!(load_err("a: 1\n'a': 2\n", true).starts_with("duplicate mapping key 'a'"));
    }

    #[test]
    fn composer_errors_render_as_pyyaml_does() {
        assert_eq!(
            load_err("a: *x\n", false),
            "found undefined alias 'x'\n  in \"<unicode string>\", line 1, column 4:\n    a: *x\n       ^"
        );
        let duplicate = load_err("a: &x 1\nb: &x 2\n", false);
        assert!(duplicate.starts_with("found duplicate anchor 'x'; first occurrence\n  in \"<unicode string>\", line 1, column 4:"), "{duplicate}");
        let many = load_err("a: 1\n---\nb: 2\n", false);
        assert!(
            many.starts_with("expected a single document in the stream\n"),
            "{many}"
        );
        assert!(many.contains("but found another document\n  in \"<unicode string>\", line 2, column 1:\n    ---\n    ^"), "{many}");
        let tag = load_err("a: !foo 1\n", false);
        assert!(
            tag.starts_with("could not determine a constructor for the tag '!foo'"),
            "{tag}"
        );
        let unhashable = load_err("? [a]\n: 1\n", false);
        assert!(
            unhashable.starts_with("while constructing a mapping\n"),
            "{unhashable}"
        );
        assert!(unhashable.contains("found unhashable key"), "{unhashable}");
    }

    #[test]
    fn a_parse_error_carries_its_marks() {
        let text = "jobs: [unclosed\n";
        let rendered = load_err(text, false);
        assert!(rendered.contains("while parsing a flow sequence\n  in \"<unicode string>\", line 1, column 7:\n    jobs: [unclosed\n          ^"), "{rendered}");
        let reader = load_err("a: \u{7}\n", false);
        assert_eq!(
            reader,
            "unacceptable character #x0007: special characters are not allowed\n  in \"<unicode string>\", position 3"
        );
    }

    #[test]
    fn compose_all_keeps_marks_and_shares_alias_nodes() {
        let (graph, roots) = compose_all("a: &x {b: 1}\nc: *x\n---\nd: 2\n").unwrap_or_default();
        assert_eq!(roots.len(), 2);
        let Kind::Mapping(pairs) = &graph.nodes[roots[0]].kind else {
            panic!("not a mapping");
        };
        assert_eq!(pairs[0].1, pairs[1].1);
        assert_eq!(graph.nodes[pairs[1].0].start.line, 1);
    }

    #[test]
    fn float_repr_matches_python() {
        assert_eq!(float_repr(1.0), "1.0");
        assert_eq!(float_repr(0.7), "0.7");
        assert_eq!(float_repr(-2.5), "-2.5");
        assert_eq!(float_repr(1e16), "1e+16");
        assert_eq!(float_repr(1.5e-5), "1.5e-05");
        assert_eq!(float_repr(0.0001), "0.0001");
        assert_eq!(float_repr(123_456.0), "123456.0");
    }

    /// Each expected text is the output of `PyYAML` 6 with the
    /// `UniqueKeyLoader` of `workflow-yaml-parse`. libyaml decides each of
    /// these inputs in another way.
    #[test]
    fn the_pure_python_rules_decide_as_pyyaml_does() {
        let value = |text: &str| match load(text, true).and_then(Document::into_value) {
            Ok(value) => value.py_repr(),
            Err(error) => error.render(text),
        };
        assert_eq!(
            value("a: b\tc\n"),
            "while scanning for the next token\nfound character '\\t' that cannot start any token\n  in \"<unicode string>\", line 1, column 5:\n    a: b\tc\n        ^"
        );
        assert_eq!(value("a:\n  b: |\n     \tx\n"), "{'a': {'b': '\\tx\\n'}}");
        assert_eq!(
            value("%FOO bar\n--- {jobs: {a: 1}}\n"),
            "{'jobs': {'a': 1}}"
        );
        assert_eq!(value("a: 1\n\u{feff}b: 2\n"), "{'a': 1, '\\ufeffb': 2}");
        assert_eq!(
            value("a: 123456789012345678901234567890123456789012345\n"),
            "{'a': 123456789012345678901234567890123456789012345}"
        );
        assert_eq!(
            value("a: -0x123456789abcdef0123456789abcdef0123456789\n"),
            "{'a': -1662864085140938409743844499106522448980869474185}"
        );
        assert_eq!(
            value("? [a]\n: 1\n? [a]\n: 2\n"),
            "duplicate mapping key \"['a']\"\n  in \"<unicode string>\", line 3, column 3:\n    ? [a]\n      ^"
        );
        assert_eq!(value("a: [x:y]\n"), "{'a': ['x:y']}");
        assert_eq!(value("a:\n- b\n  - c\n"), "{'a': ['b - c']}");
        assert_eq!(
            value("{a: 1, a: 2}\n"),
            "duplicate mapping key 'a'\n  in \"<unicode string>\", line 1, column 8:\n    {a: 1, a: 2}\n           ^"
        );
        assert_eq!(value("x: 'a\n\n  b'\n"), "{'x': 'a\\nb'}");
    }

    #[test]
    fn recursion_is_reported_not_overflowed() {
        assert_ne!(load_err("a: &x {<<: *x}\n", false), "");
        assert!(safe_load("a: &x [*x]\n").is_err());
        assert!(load("a: &x [*x]\njobs: {b: 1}\n", true).is_ok());
    }
}
