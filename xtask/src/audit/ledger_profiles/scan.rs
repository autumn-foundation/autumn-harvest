//! The source scan of the Ledger profile gate.
//!
//! The scan reads Rust source text. It does not compile the code. It blanks
//! comments and string bodies, walks items and braces into frames, and finds
//! each call that sends SQL. `cargo xtask audit ledger-census` reuses the lexer
//! and the frames to map a census stack frame to its profile.
//!
//! Each view of a file keeps every byte offset and newline. So a position in
//! one view is the same position in the other views and in the source.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};

use regex::Regex;
use smop::prelude::*;

/// The catalog root, relative to the repository root.
pub const CATALOG: &str = "docs/ledger";

/// The detected facts of one function that holds a query site.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Site {
    /// The 1-based line of the first query site in the function.
    pub line: usize,
    /// The backends that the sites use.
    pub backend: BTreeSet<String>,
    /// The kinds of statement: `read`, `write` and `ddl`.
    pub kind: BTreeSet<String>,
    /// The known tables that the body or its SQL holders name.
    pub tables: BTreeSet<String>,
}

impl Site {
    /// Returns one detected field by its profile name.
    pub fn field(&self, name: &str) -> &BTreeSet<String> {
        match name {
            "backend" => &self.backend,
            "kind" => &self.kind,
            _ => &self.tables,
        }
    }
}

/// The kind of a frame: an item or a plain block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// The whole file.
    File,
    /// A brace block that no item opened.
    Block,
    /// A function.
    Fn,
    /// An `impl` block.
    Impl,
    /// A trait.
    Trait,
    /// An inline module.
    Mod,
    /// A `macro_rules!` macro.
    Macro,
}

/// One brace scope of a file.
#[derive(Debug, Clone)]
pub struct Frame {
    /// What opened the scope.
    pub kind: Kind,
    /// The item name, or an empty string.
    pub name: String,
    /// The offset of the opening brace.
    pub start: usize,
    /// The offset after the closing brace, or the file length.
    pub end: usize,
    /// Whether a test attribute gates the scope or an enclosing scope.
    pub test: bool,
    /// The index of the enclosing frame.
    pub parent: Option<usize>,
}

/// The frames of one file. Index 0 is the whole file.
#[derive(Debug, Clone)]
pub struct Frames {
    /// Every frame, in the order that its brace opens. Index 0 is the root.
    pub all: Vec<Frame>,
    /// Whether an inner test attribute gates the whole file.
    pub test_file: bool,
    /// The modules that a `#[cfg(test)] mod name;` declaration includes.
    pub test_mods: Vec<String>,
}

impl Frames {
    /// Returns the item path of a frame, such as `Store::depth` or `name!`.
    pub fn chain(&self, index: usize) -> String {
        let mut names = Vec::new();
        let mut cursor = Some(index);
        while let Some(i) = cursor {
            let frame = &self.all[i];
            match frame.kind {
                Kind::Fn | Kind::Impl | Kind::Trait | Kind::Mod => names.push(frame.name.clone()),
                Kind::Macro => names.push(format!("{}!", frame.name)),
                Kind::File | Kind::Block => {}
            }
            cursor = frame.parent;
        }
        names.reverse();
        names.join("::")
    }

    /// Returns the innermost enclosing function or macro, which owns a query site.
    pub fn owner(&self, index: usize) -> Option<usize> {
        let mut cursor = Some(index);
        while let Some(i) = cursor {
            if matches!(self.all[i].kind, Kind::Fn | Kind::Macro) {
                return Some(i);
            }
            cursor = self.all[i].parent;
        }
        None
    }

    /// Returns the innermost frame that holds `pos`. The root never counts.
    pub fn innermost(&self, pos: usize) -> Option<usize> {
        let mut best: Option<usize> = None;
        for (i, frame) in self.all.iter().enumerate().skip(1) {
            let inside = frame.start <= pos && pos < frame.end;
            if inside && best.is_none_or(|b| frame.start >= self.all[b].start) {
                best = Some(i);
            }
        }
        best
    }
}

/// The compiled patterns of the scan.
pub struct Scanner {
    item: Regex,
    test_attr: Regex,
    whitespace: Regex,
    postgres_call: Regex,
    sqlite_call: Regex,
    either_call: Regex,
    atomic_arg: Regex,
    db_file: Regex,
    rusqlite_word: Regex,
    postgres_word: Regex,
    sqlite_hint: Regex,
    write_sql: Regex,
    write_dsl: Regex,
    ddl_sql: Regex,
    read_sql: Regex,
    read_dsl_select: Regex,
    read_dsl_reject: Regex,
    leading_space: Regex,
    create_table: Regex,
    ident: Regex,
    sql_holder: Regex,
    const_item: Regex,
    escape_space: Regex,
    impl_where: Regex,
    impl_for: Regex,
    impl_names: Regex,
}

/// An optional turbofish, such as `::<i64>` or `::<(Uuid, String)>`.
///
/// A tuple type has parentheses, so the pattern stops at the first `>(` instead.
const TURBOFISH: &str = r"(?:::\s*<[^;{}]*?>\s*)?";

/// The Diesel and `rusqlite` methods that read rows.
const READ_METHODS: [&str; 7] = [
    "load",
    "load_stream",
    "first",
    "get_result",
    "get_results",
    "query_row",
    "query_map",
];

fn compile(pattern: &str) -> Result<Regex> {
    Regex::new(pattern).with_context(|| format!("compiling the pattern {pattern}"))
}

impl Scanner {
    /// Compiles every pattern of the scan.
    pub fn new() -> Result<Self> {
        Ok(Self {
            item: compile(
                r"#!?\s*\[|\bfn\s+(?:r#)?([A-Za-z_]\w*)|\bimpl\b|\btrait\s+([A-Za-z_]\w*)|\bmod\s+([A-Za-z_]\w*)|\bmacro_rules!\s*([A-Za-z_]\w*)|[{};]",
            )?,
            test_attr: compile(r"^#\[\s*(?:test|(?:\w+::)+test|cfg\(\s*(?:test|all\(\s*test\b))")?,
            whitespace: compile(r"\s+")?,
            // Calls that only Diesel makes, as methods or in the fully qualified form.
            postgres_call: compile(&format!(
                r"\bsql_query\s*\(|\binsert_into\s*\(|\bdiesel::(?:update|delete)\s*\(|\b(?:SimpleAsyncConnection|RunQueryDsl)::(?:batch_execute|execute|load|get_result|get_results|first)\s*\(|\.(?:get_result|get_results|load|load_stream|first|batch_execute)\s*{TURBOFISH}\("
            ))?,
            // Calls that only rusqlite makes.
            sqlite_call: compile(
                r"\.(?:prepare|prepare_cached|query_row|query_row_and_then|query_map|query_and_then|execute_batch|pragma_update|pragma_update_and_check|pragma_query_value)\s*\(",
            )?,
            // Both libraries name a call `execute`.
            either_call: compile(&format!(r"\.execute\s*{TURBOFISH}\("))?,
            // An atomic `load(Ordering::...)` is not a query.
            atomic_arg: compile(r"\b(?:Ordering|SeqCst|Acquire|Relaxed|AcqRel|Release)\b")?,
            db_file: compile(
                r"\b(?:diesel|diesel_async|rusqlite|AsyncPgConnection|RunQueryDsl)\b",
            )?,
            rusqlite_word: compile(r"\brusqlite\b")?,
            postgres_word: compile(r"\b(?:diesel|diesel_async|AsyncPgConnection)\b")?,
            sqlite_hint: compile(r"\brusqlite\b|\bConnection\b|\bTransaction\b")?,
            write_sql: compile(
                r"\bINSERT\s+INTO\b|\bUPDATE\s+(?:ONLY\s+)?[\w.]+(?:\s+(?:AS\s+)?\w+)?\s+SET\b|\bDELETE\s+FROM\b|\bON\s+CONFLICT\b|\bREPLACE\s+INTO\b",
            )?,
            write_dsl: compile(r"\binsert_into\s*\(|\bdiesel::(?:update|delete)\s*\(")?,
            ddl_sql: compile(
                r"\b(?:CREATE|ALTER|DROP)\s+(?:UNIQUE\s+|OR\s+REPLACE\s+|TEMP\s+|TEMPORARY\s+|UNLOGGED\s+)?(?:TABLE|INDEX|TRIGGER|FUNCTION|VIEW|SCHEMA|EXTENSION|TYPE|SEQUENCE|POLICY|ROLE|PUBLICATION|SUBSCRIPTION)\b|\bTRUNCATE\b",
            )?,
            read_sql: compile(r"\bSELECT\b|\bRETURNING\b|\bPRAGMA\b")?,
            read_dsl_select: compile(r"\bdiesel::select\s*\(")?,
            // The Python pattern refused these with a negative lookahead.
            // The regex crate has no lookahead, so `read_dsl` tests each end.
            read_dsl_reject: compile(
                r"^(?:\s*\)|\s*(?:\w+::)*(?:Ordering|SeqCst|Acquire|Relaxed|AcqRel|Release)\b)",
            )?,
            leading_space: compile(r"^\s*")?,
            create_table: compile(
                r#"(?i)\bCREATE\s+(?:UNLOGGED\s+|TEMP\s+|TEMPORARY\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:\w+\.)?"?([A-Za-z_]\w*)"#,
            )?,
            ident: compile(r"\b[A-Za-z_]\w*\b")?,
            // A same-file function or macro whose name says it holds SQL: `claim_query_for()`.
            sql_holder: compile(r"query|sql|stmt|ddl|statement")?,
            const_item: compile(r"\b(?:const|static)\s+([A-Z][A-Z0-9_]*)\s*:[^=;]*=")?,
            escape_space: compile(r"\\(?:\s+|[nt])")?,
            impl_where: compile(r"\bwhere\b")?,
            impl_for: compile(r"\bfor\b")?,
            impl_names: compile(r"[A-Za-z_]\w*")?,
        })
    }

    /// Walks items and braces, and returns the frames of a lexed file.
    pub fn frames_of(&self, code: &str) -> Frames {
        let bytes = code.as_bytes();
        let root = Frame {
            kind: Kind::File,
            name: String::new(),
            start: 0,
            end: 0,
            test: false,
            parent: None,
        };
        let mut all = vec![root];
        let mut stack = vec![0_usize];
        let mut pending: Option<(Kind, String)> = None;
        let mut pending_test = false;
        let mut test_file = false;
        let mut test_mods = Vec::new();
        let mut i = 0;
        while let Some(caps) = self.item.captures_at(code, i) {
            let Some(m) = caps.get(0) else { break };
            let tok = m.as_str();
            i = m.end();
            if tok.starts_with('#') {
                let mut depth = 1_i64;
                let mut j = i;
                while j < bytes.len() && depth != 0 {
                    match bytes[j] {
                        b'[' => depth += 1,
                        b']' => depth -= 1,
                        _ => {}
                    }
                    j += 1;
                }
                let attr = format!("#[{}", code[i..j].trim());
                let squashed = self.whitespace.replace_all(&attr, "");
                if self.test_attr.is_match(&squashed) {
                    if tok.starts_with("#!") && stack.len() == 1 {
                        test_file = true;
                    }
                    pending_test = true;
                }
                i = j;
            } else if tok == "{" {
                let (kind, name) = pending.take().unwrap_or((Kind::Block, String::new()));
                let parent = stack.last().copied().unwrap_or(0);
                all.push(Frame {
                    kind,
                    name,
                    start: m.start(),
                    end: code.len(),
                    test: all[parent].test || pending_test,
                    parent: Some(parent),
                });
                stack.push(all.len() - 1);
                pending_test = false;
            } else if tok == "}" {
                if stack.len() > 1
                    && let Some(top) = stack.pop()
                {
                    all[top].end = m.end();
                }
            } else if tok == ";" {
                if let Some((Kind::Mod, name)) = &pending
                    && pending_test
                {
                    test_mods.push(name.clone());
                }
                pending = None;
                pending_test = false;
            } else if tok == "impl" {
                let before = code[..m.start()].trim_end();
                let opens = before
                    .chars()
                    .next_back()
                    .is_none_or(|c| matches!(c, '{' | '}' | ';' | ']'));
                if opens || ends_with_word(before, "unsafe") || ends_with_word(before, "default") {
                    let end = code[i..].find(['{', ';']).map_or(code.len(), |k| i + k);
                    pending = Some((Kind::Impl, self.impl_name(&code[i..end])));
                }
            } else if let Some(name) = caps.get(1) {
                pending = Some((Kind::Fn, name.as_str().to_string()));
            } else if let Some(name) = caps.get(2) {
                pending = Some((Kind::Trait, name.as_str().to_string()));
            } else if let Some(name) = caps.get(3) {
                pending = Some((Kind::Mod, name.as_str().to_string()));
            } else if let Some(name) = caps.get(4) {
                pending = Some((Kind::Macro, name.as_str().to_string()));
            }
        }
        Frames {
            all,
            test_file,
            test_mods,
        }
    }

    /// Returns the self type of an `impl` header, as its last path segment.
    fn impl_name(&self, header: &str) -> String {
        let header = strip_angles(&header.replace("->", "  "));
        let header = self.impl_where.split(&header).next().unwrap_or("");
        let target = self.impl_for.split(header).last().unwrap_or("");
        let target = target.replace("dyn", " ").replace("mut", " ");
        self.impl_names
            .find_iter(&target)
            .last()
            .map_or_else(|| "impl".to_string(), |m| m.as_str().to_string())
    }

    /// Returns `(offset, backend)` for each query call.
    ///
    /// The backend is `None` when either library fits. A Diesel method takes
    /// the connection as its one argument. So a method of the same name with
    /// more arguments is some other `load` or `first`.
    fn query_sites(&self, code: &str) -> Vec<(usize, Option<&'static str>)> {
        let mut out = Vec::new();
        let patterns = [
            (&self.postgres_call, Some("postgres")),
            (&self.sqlite_call, Some("sqlite")),
            (&self.either_call, None),
        ];
        for (pattern, initial) in patterns {
            // The Python scan rebinds this name once an `execute` call takes
            // more than one argument. Each later `execute` match in the file
            // keeps `sqlite`, and the port keeps that behavior.
            let mut backend = initial;
            for m in pattern.find_iter(code) {
                let arg = call_arg(code, m.end() - 1);
                if arg.trim().is_empty() || self.atomic_arg.is_match(arg) {
                    continue;
                }
                let args = top_level_args(arg);
                if backend == Some("postgres") && m.as_str().starts_with('.') && args != 1 {
                    continue;
                }
                if backend.is_none() && args > 1 {
                    backend = Some("sqlite");
                }
                out.push((m.start(), backend));
            }
        }
        out
    }

    /// Maps each same-file SQL-holding function, macro or constant name to its spans.
    fn sql_holders(&self, code: &str, frames: &Frames) -> HashMap<String, Vec<(usize, usize)>> {
        let mut holders: HashMap<String, Vec<(usize, usize)>> = HashMap::new();
        for frame in frames.all.iter().skip(1) {
            let named = matches!(frame.kind, Kind::Fn | Kind::Macro);
            if named && !frame.test && self.sql_holder.is_match(&frame.name.to_lowercase()) {
                holders
                    .entry(frame.name.clone())
                    .or_default()
                    .push((frame.start, frame.end));
            }
        }
        for caps in self.const_item.captures_iter(code) {
            let (Some(whole), Some(name)) = (caps.get(0), caps.get(1)) else {
                continue;
            };
            holders
                .entry(name.as_str().to_string())
                .or_default()
                .push((whole.end(), item_end(code, whole.end())));
        }
        holders
    }

    /// Returns the identifiers inside `full[start..end]`.
    ///
    /// The text before `start` still counts for a word boundary. The text
    /// after `end` does not. That matches `pattern.findall(text, pos, endpos)`.
    fn idents_in<'a>(&self, full: &'a str, start: usize, end: usize) -> Vec<&'a str> {
        let hay = &full[..end];
        let mut out = Vec::new();
        let mut pos = start;
        while let Some(m) = self.ident.find_at(hay, pos) {
            out.push(m.as_str());
            pos = m.end();
        }
        out
    }

    /// Returns `span` plus the span of each SQL holder that it names, six levels deep.
    fn with_holders(
        &self,
        full: &str,
        span: (usize, usize),
        holders: &HashMap<String, Vec<(usize, usize)>>,
    ) -> Vec<(usize, usize)> {
        let mut seen: HashSet<String> = HashSet::new();
        let mut spans = vec![span];
        let mut frontier = vec![span];
        for _ in 0..6 {
            let mut next = Vec::new();
            for &(start, end) in &frontier {
                let mut unique: HashSet<&str> = HashSet::new();
                let mut names: Vec<&str> = Vec::new();
                for name in self.idents_in(full, start, end) {
                    if !seen.contains(name) && unique.insert(name) {
                        names.push(name);
                    }
                }
                for name in names {
                    if let Some(found) = holders.get(name) {
                        seen.insert(name.to_string());
                        next.extend(found.iter().copied());
                    }
                }
            }
            spans.extend(next.iter().copied());
            frontier = next;
        }
        spans
    }

    /// Keeps only string contents, so `"UPDATE t ", "SET"` reads as SQL text.
    ///
    /// A line continuation or a `\n` escape inside a string becomes spaces.
    /// The view keeps every offset, like the other two.
    fn sql_view(&self, code: &str, full: &str) -> String {
        let bytes: Vec<u8> = code
            .bytes()
            .zip(full.bytes())
            .map(|(c, f)| if c == f { b' ' } else { f })
            .collect();
        let text = String::from_utf8_lossy(&bytes).into_owned();
        self.escape_space
            .replace_all(&text, |caps: &regex::Captures<'_>| {
                " ".repeat(caps[0].len())
            })
            .into_owned()
    }

    /// Returns the detected query sites of one Rust file, keyed by profile.
    ///
    /// The SQL of a call often lives in a builder function or a constant of
    /// the same file, such as `sql_query(claim_query())`. The scan reads those
    /// too. It reads a function or macro whose name contains `query`, `sql`,
    /// `stmt`, `ddl` or `statement`, and an upper-case constant or static.
    pub fn scan_source(&self, source: &str, tables: &BTreeSet<String>) -> BTreeMap<String, Site> {
        let (code, full) = lex(source);
        let mut found: BTreeMap<String, Site> = BTreeMap::new();
        if !self.db_file.is_match(&code) {
            return found;
        }
        let frames = self.frames_of(&code);
        if frames.test_file {
            return found;
        }
        let holders = self.sql_holders(&code, &frames);
        let sql = self.sql_view(&code, &full);
        let sqlite_file = self.rusqlite_word.is_match(&code);
        let postgres_file = self.postgres_word.is_match(&code);
        for (pos, backend) in self.query_sites(&code) {
            let Some(frame) = frames.innermost(pos) else {
                continue;
            };
            let Some(owner) = frames.owner(frame) else {
                continue;
            };
            if frames.all[frame].test {
                continue;
            }
            let (start, end) = (frames.all[owner].start, frames.all[owner].end);
            let body_code = &code[start..end];
            let spans = self.with_holders(&full, (start, end), &holders);
            let body: Vec<&str> = spans.iter().map(|&(a, b)| &full[a..b]).collect();
            let body = body.join("\n");
            let body_sql: Vec<String> = spans
                .iter()
                .map(|&(a, b)| self.whitespace.replace_all(&sql[a..b], " ").into_owned())
                .collect();
            let body_sql = body_sql.join(" ");
            let backend = backend.unwrap_or_else(|| {
                if sqlite_file && !postgres_file {
                    "sqlite"
                } else if postgres_file && !sqlite_file {
                    "postgres"
                } else if self.sqlite_call.is_match(body_code)
                    || self.sqlite_hint.is_match(body_code)
                {
                    "sqlite"
                } else {
                    "postgres"
                }
            });
            let key = frames.chain(owner);
            let entry = found.entry(key).or_insert_with(|| Site {
                line: source.as_bytes()[..pos].split(|&b| b == b'\n').count(),
                backend: BTreeSet::new(),
                kind: BTreeSet::new(),
                tables: BTreeSet::new(),
            });
            entry.backend.insert(backend.to_string());
            entry.kind.extend(self.kinds_of(&body_sql, body_code));
            for name in self.ident.find_iter(&body) {
                if tables.contains(name.as_str()) {
                    entry.tables.insert(name.as_str().to_string());
                }
            }
        }
        found
    }

    /// Detects `ddl`, `write` and `read` from the SQL text and the Diesel calls.
    fn kinds_of(&self, body_sql: &str, body_code: &str) -> Vec<String> {
        let mut kinds = Vec::new();
        if self.ddl_sql.is_match(body_sql) {
            kinds.push("ddl".to_string());
        }
        if self.write_sql.is_match(body_sql) || self.write_dsl.is_match(body_code) {
            kinds.push("write".to_string());
        }
        if self.read_sql.is_match(body_sql) || self.read_dsl(body_code) {
            kinds.push("read".to_string());
        }
        kinds
    }

    /// Whether the code holds a Diesel or `rusqlite` read call.
    ///
    /// A read method counts unless its argument list is empty or starts with
    /// an atomic ordering. Each possible end of the turbofish is tried, as
    /// the backtracking Python pattern did.
    fn read_dsl(&self, code: &str) -> bool {
        if self.read_dsl_select.is_match(code) {
            return true;
        }
        for (dot, _) in code.match_indices('.') {
            for name in READ_METHODS {
                if !code[dot + 1..].starts_with(name) {
                    continue;
                }
                let after = dot + 1 + name.len();
                let open = after + self.space_len(&code[after..]);
                let accepted = self
                    .call_ends(code, open)
                    .into_iter()
                    .any(|end| !self.read_dsl_reject.is_match(&code[end..]));
                if accepted {
                    return true;
                }
            }
        }
        false
    }

    /// Returns each offset just past a `(` that can follow a method name.
    ///
    /// `open` is the first offset after the name and its whitespace. A
    /// turbofish can end at any `>` before a `;`, `{` or `}`.
    fn call_ends(&self, code: &str, open: usize) -> Vec<usize> {
        let bytes = code.as_bytes();
        let mut ends = Vec::new();
        if bytes.get(open) == Some(&b'(') {
            ends.push(open + 1);
        }
        if code[open..].starts_with("::") {
            let angle = open + 2 + self.space_len(&code[open + 2..]);
            if bytes.get(angle) == Some(&b'<') {
                let mut k = angle + 1;
                while k < bytes.len() && !matches!(bytes[k], b';' | b'{' | b'}') {
                    if bytes[k] == b'>' {
                        let paren = k + 1 + self.space_len(&code[k + 1..]);
                        if bytes.get(paren) == Some(&b'(') {
                            ends.push(paren + 1);
                        }
                    }
                    k += 1;
                }
            }
        }
        ends
    }

    /// Returns the length of the leading whitespace of `text`.
    fn space_len(&self, text: &str) -> usize {
        self.leading_space.find(text).map_or(0, |m| m.end())
    }

    /// Returns every table that a migration or a shipped `CREATE TABLE` names.
    pub fn known_tables<'a>(
        &self,
        paths_and_sources: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> BTreeSet<String> {
        let mut tables = BTreeSet::new();
        for (path, source) in paths_and_sources {
            let text = if Path::new(path).extension().is_some_and(|e| e == "rs") {
                let (code, full) = lex(source);
                let frames = self.frames_of(&code);
                if frames.test_file {
                    continue;
                }
                let mut text = full.into_bytes();
                for frame in frames.all.iter().skip(1) {
                    let parent_test = frame.parent.is_some_and(|p| frames.all[p].test);
                    if frame.test && !parent_test {
                        blank(&mut text, frame.start, frame.end);
                    }
                }
                String::from_utf8_lossy(&text).into_owned()
            } else {
                source.to_string()
            };
            for caps in self.create_table.captures_iter(&text) {
                if let Some(name) = caps.get(1)
                    && name.as_str().chars().count() > 3
                {
                    tables.insert(name.as_str().to_string());
                }
            }
        }
        tables
    }
}

/// Whether `text` ends with `word` after a word boundary.
fn ends_with_word(text: &str, word: &str) -> bool {
    text.strip_suffix(word).is_some_and(|head| {
        head.chars()
            .next_back()
            .is_none_or(|c| !(c.is_alphanumeric() || c == '_'))
    })
}

/// Drops generic arguments, so `Foo<Bar<T>>` becomes `Foo`.
fn strip_angles(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut depth = 0_usize;
    for ch in text.chars() {
        if ch == '<' {
            depth += 1;
        } else if ch == '>' && depth > 0 {
            depth -= 1;
        } else if depth == 0 {
            out.push(ch);
        }
    }
    out
}

/// Returns the argument text of the call whose `(` is at `open_paren`.
fn call_arg(code: &str, open_paren: usize) -> &str {
    let bytes = code.as_bytes();
    let mut depth = 0_i64;
    for (j, &b) in bytes.iter().enumerate().skip(open_paren) {
        match b {
            b'(' => depth += 1,
            b')' => depth -= 1,
            _ => {}
        }
        if depth == 0 {
            return &code[open_paren + 1..j];
        }
    }
    &code[open_paren + 1..]
}

/// Counts the arguments of a call, ignoring commas inside nested brackets.
fn top_level_args(arg: &str) -> i64 {
    let mut depth = 0_i64;
    let mut count = 1_i64;
    for b in arg.bytes() {
        match b {
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => depth -= 1,
            b',' if depth == 0 => count += 1,
            _ => {}
        }
    }
    count - i64::from(arg.trim_end().ends_with(','))
}

/// Returns the offset of the `;` that ends the item, skipping nested brackets.
fn item_end(code: &str, start: usize) -> usize {
    let mut depth = 0_i64;
    for (j, &b) in code.as_bytes().iter().enumerate().skip(start) {
        match b {
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => depth -= 1,
            b';' if depth <= 0 => return j,
            _ => {}
        }
    }
    code.len()
}

/// Replaces every byte but a newline in `view[start..end]` with a space.
///
/// Each range starts and ends on a character boundary. So a multi-byte
/// character becomes several spaces, and the view stays valid UTF-8.
fn blank(view: &mut [u8], start: usize, end: usize) {
    let end = end.min(view.len());
    if start >= end {
        return;
    }
    for byte in &mut view[start..end] {
        if *byte != b'\n' {
            *byte = b' ';
        }
    }
}

/// Returns `(code, full)`: comments blanked, and string bodies blanked in `code` only.
///
/// Both views keep every offset and newline. So a position in one is the
/// same position in the other and in the source.
pub fn lex(source: &str) -> (String, String) {
    let src = source.as_bytes();
    let n = src.len();
    let mut code = src.to_vec();
    let mut full = src.to_vec();
    let mut i = 0;
    while i < n {
        let c = src[i];
        if src[i..].starts_with(b"//") {
            let end = source[i..].find('\n').map_or(n, |k| i + k);
            blank(&mut code, i, end);
            blank(&mut full, i, end);
            i = end;
        } else if src[i..].starts_with(b"/*") {
            let mut depth = 1;
            let mut j = i + 2;
            while j < n && depth > 0 {
                if src[j..].starts_with(b"/*") {
                    depth += 1;
                    j += 2;
                } else if src[j..].starts_with(b"*/") {
                    depth -= 1;
                    j += 2;
                } else {
                    j += 1;
                }
            }
            let j = j.min(n);
            blank(&mut code, i, j);
            blank(&mut full, i, j);
            i = j;
        } else if matches!(c, b'r' | b'b' | b'c') && !follows_word(source, i) {
            if let Some((body, hashes)) = raw_string_open(src, i) {
                let close: Vec<u8> = std::iter::once(b'"')
                    .chain(std::iter::repeat_n(b'#', hashes))
                    .collect();
                if let Some(k) = find_bytes(src, &close, body) {
                    blank(&mut code, body, k);
                    i = k + close.len();
                } else {
                    blank(&mut code, body, back_chars(source, n, close.len()));
                    i = n;
                }
                continue;
            }
            if matches!(c, b'b' | b'c') && src.get(i + 1) == Some(&b'"') {
                i = skip_string(src, i + 1, &mut code);
                continue;
            }
            i += 1;
        } else if c == b'"' {
            i = skip_string(src, i, &mut code);
        } else if c == b'\'' {
            i = skip_char_literal(source, i);
        } else {
            i += 1;
        }
    }
    (
        String::from_utf8_lossy(&code).into_owned(),
        String::from_utf8_lossy(&full).into_owned(),
    )
}

/// Whether the character before `i` is alphanumeric or `_`.
fn follows_word(source: &str, i: usize) -> bool {
    source[..i]
        .chars()
        .next_back()
        .is_some_and(|c| c.is_alphanumeric() || c == '_')
}

/// Matches `(?:b|c)?r(#*)"` at `i`, and returns the body offset and the hash count.
fn raw_string_open(src: &[u8], i: usize) -> Option<(usize, usize)> {
    let try_at = |r: usize| -> Option<(usize, usize)> {
        if src.get(r) != Some(&b'r') {
            return None;
        }
        let mut j = r + 1;
        while src.get(j) == Some(&b'#') {
            j += 1;
        }
        (src.get(j) == Some(&b'"')).then(|| (j + 1, j - r - 1))
    };
    if matches!(src[i], b'b' | b'c')
        && let Some(found) = try_at(i + 1)
    {
        return Some(found);
    }
    try_at(i)
}

/// Returns the first offset at or after `from` where `needle` starts.
fn find_bytes(hay: &[u8], needle: &[u8], from: usize) -> Option<usize> {
    if from > hay.len() {
        return None;
    }
    hay[from..]
        .windows(needle.len())
        .position(|w| w == needle)
        .map(|k| from + k)
}

/// Returns the offset `count` characters before `end`.
fn back_chars(source: &str, end: usize, count: usize) -> usize {
    let mut pos = end;
    for _ in 0..count {
        match source[..pos].chars().next_back() {
            Some(ch) => pos -= ch.len_utf8(),
            None => return 0,
        }
    }
    pos
}

/// Blanks the body of the string that opens at `start` in `code`, and returns its end.
fn skip_string(src: &[u8], start: usize, code: &mut [u8]) -> usize {
    let n = src.len();
    let mut j = start + 1;
    while j < n && src[j] != b'"' {
        j += if src[j] == b'\\' { 2 } else { 1 };
    }
    blank(code, start + 1, j.min(n));
    j + 1
}

/// Skips a character literal or a lifetime that starts at `i`.
fn skip_char_literal(source: &str, i: usize) -> usize {
    let src = source.as_bytes();
    let n = src.len();
    if src.get(i + 1) == Some(&b'\\') {
        let mut end = find_bytes(src, b"'", i + 2);
        while let Some(e) = end {
            if src[e - 1] == b'\\' && src[e - 2] != b'\\' {
                end = find_bytes(src, b"'", e + 1);
            } else {
                break;
            }
        }
        return end.map_or(n, |e| e + 1);
    }
    if let Some(ch) = source[i + 1..].chars().next() {
        let close = i + 1 + ch.len_utf8();
        if close < n && src[close] == b'\'' {
            return close + 1;
        }
    }
    i + 1
}

/// Maps a source path to its catalog file.
///
/// `autumn-harvest/src/dst/mod.rs` maps to `docs/ledger/autumn-harvest/dst/mod.toml`.
pub fn catalog_path(source_path: &str) -> String {
    let (krate, rest) = source_path.split_once("/src/").unwrap_or((source_path, ""));
    let mut path = PathBuf::from(CATALOG);
    path.push(krate);
    if !rest.is_empty() {
        path.push(rest);
    }
    path.set_extension("toml");
    path.to_string_lossy().into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lex_blanks_comments_and_strings_but_keeps_offsets() {
        let source = "let a = \"x // y\"; // gone\nlet b = r#\"q\"#; /* c /* d */ e */ 'z';\n";
        let (code, full) = lex(source);
        assert_eq!(code.len(), source.len());
        assert_eq!(full.len(), source.len());
        assert!(!code.contains("gone") && !full.contains("gone"));
        assert!(full.contains("\"x // y\""));
        assert!(!code.contains("x // y"));
        assert!(!full.contains("/*"));
    }

    #[test]
    fn lex_keeps_multibyte_text_valid() {
        let source = "let s = \"caf\u{e9}\"; // \u{e9}t\u{e9}\nlet c = '\u{e9}';\n";
        let (code, full) = lex(source);
        assert_eq!(code.len(), source.len());
        assert!(full.contains("caf\u{e9}"));
        assert!(code.contains("'\u{e9}'"));
    }

    #[test]
    fn frames_name_impl_methods_and_macros() -> Result<()> {
        let scanner = Scanner::new()?;
        let code =
            "impl<'a> Reader for Store<'a> { fn depth(&self) {} }\nmacro_rules! m { () => {} }\n";
        let frames = scanner.frames_of(code);
        let names: Vec<String> = (1..frames.all.len()).map(|i| frames.chain(i)).collect();
        assert!(names.contains(&"Store::depth".to_string()), "{names:?}");
        assert!(names.contains(&"m!".to_string()), "{names:?}");
        Ok(())
    }

    #[test]
    fn a_test_module_declaration_is_recorded() -> Result<()> {
        let scanner = Scanner::new()?;
        let frames = scanner.frames_of("#[cfg(test)]\nmod fixtures;\nmod real;\n");
        assert_eq!(frames.test_mods, vec!["fixtures".to_string()]);
        Ok(())
    }

    #[test]
    fn catalog_path_maps_under_the_crate() {
        assert_eq!(
            catalog_path("autumn-harvest/src/dst/mod.rs"),
            "docs/ledger/autumn-harvest/dst/mod.toml"
        );
        assert_eq!(catalog_path("c/src/queue.rs"), "docs/ledger/c/queue.toml");
    }

    #[test]
    fn read_dsl_refuses_an_atomic_load_and_an_empty_call() -> Result<()> {
        let scanner = Scanner::new()?;
        assert!(!scanner.read_dsl("x.load(Ordering::SeqCst)"));
        assert!(!scanner.read_dsl("items.first()"));
        assert!(scanner.read_dsl("t.first::<(i64, String)>(conn)"));
        assert!(scanner.read_dsl("t.load(conn)"));
        Ok(())
    }

    #[test]
    fn top_level_args_ignores_nested_commas() {
        assert_eq!(top_level_args("a, (b, c), [d, e]"), 3);
        assert_eq!(top_level_args("conn,"), 1);
        assert_eq!(top_level_args("conn"), 1);
    }
}
