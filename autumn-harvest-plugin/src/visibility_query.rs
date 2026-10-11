//! The `filter` grammar of `GET /workflows` and the Vantage list (issue #1982).
//!
//! A filter joins predicates with `AND`, `OR` and parentheses. `AND` binds
//! tighter than `OR`. `DESIGN-1982.md` holds the full grammar.
//!
//! The compiler binds every key and every value. Operator text comes from a
//! closed set, so no caller text reaches the SQL. Each branch of each `OR` must
//! hold an indexed predicate. A filter that breaks this rule gets a `400`.

use diesel::pg::Pg;
use diesel::query_builder::{AstPass, QueryFragment, QueryId};
use diesel::result::QueryResult;
use serde_json::Value;

use crate::api::{CmpOp, SearchAttrPredicate};

/// The longest filter text, in bytes.
pub const MAX_FILTER_LEN: usize = 2048;
/// The deepest parenthesis nesting.
pub const MAX_DEPTH: usize = 8;
/// The most predicates in one filter.
pub const MAX_PREDICATES: usize = 32;
/// The most values in one `IN` list.
pub const MAX_IN_VALUES: usize = 100;

/// A parsed filter.
#[derive(Debug, Clone)]
pub enum Expr {
    And(Vec<Self>),
    Or(Vec<Self>),
    Leaf(Leaf),
}

/// One predicate.
#[derive(Debug, Clone)]
pub enum Leaf {
    /// An `attrs.<key>` predicate. It uses the SQL of issue #506.
    Attr(SearchAttrPredicate),
    /// A predicate on a column of `harvest_workflow_executions`.
    System(SystemField, SystemTest),
}

/// A column that the grammar can filter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SystemField {
    State,
    WorkflowName,
    Owner,
    Severity,
    StartedAt,
}

/// The test on a system column.
#[derive(Debug, Clone)]
pub enum SystemTest {
    Eq(String),
    Ne(String),
    In(Vec<String>),
    Cmp(CmpOp, chrono::DateTime<chrono::Utc>),
}

/// Parses `raw` into a filter and checks the index rule.
///
/// # Errors
///
/// Returns a message for the caller when the text is not a valid filter.
pub fn parse(raw: &str) -> Result<Expr, String> {
    if raw.len() > MAX_FILTER_LEN {
        return Err(format!(
            "filter is {} bytes; the limit is {MAX_FILTER_LEN} bytes",
            raw.len()
        ));
    }
    let tokens = lex(raw)?;
    if tokens.is_empty() {
        return Err("filter is empty".to_string());
    }
    let mut parser = Parser {
        tokens,
        pos: 0,
        end: raw.len(),
        depth: 0,
        predicates: 0,
    };
    let expr = parser.or_expr()?;
    if let Some(token) = parser.tokens.get(parser.pos) {
        return Err(format!(
            "unexpected {} at byte {}; join predicates with AND or OR",
            token.kind.describe(),
            token.at
        ));
    }
    // An `OR` that no index can serve forces a full scan. So a filter with
    // an `OR` must find each row that it matches through an index.
    if has_or(&expr) && !anchored(&expr) {
        return Err(
            "a filter with OR needs an indexed predicate on each OR branch, \
             or on the whole filter: an attrs.* predicate, or workflow_name with = or IN"
                .to_string(),
        );
    }
    Ok(expr)
}

/// The number of predicates in `expr`.
pub fn predicate_count(expr: &Expr) -> usize {
    match expr {
        Expr::Leaf(_) => 1,
        Expr::And(children) | Expr::Or(children) => children.iter().map(predicate_count).sum(),
    }
}

/// The workflow names that `expr` names with `workflow_name =` or `IN`.
pub fn workflow_names(expr: &Expr) -> Vec<String> {
    match expr {
        Expr::Leaf(Leaf::System(SystemField::WorkflowName, SystemTest::Eq(name))) => {
            vec![name.clone()]
        }
        Expr::Leaf(Leaf::System(SystemField::WorkflowName, SystemTest::In(names))) => names.clone(),
        Expr::Leaf(_) => Vec::new(),
        Expr::And(children) | Expr::Or(children) => {
            children.iter().flat_map(workflow_names).collect()
        }
    }
}

/// Whether `expr` holds an `OR`.
fn has_or(expr: &Expr) -> bool {
    match expr {
        Expr::Leaf(_) => false,
        Expr::Or(_) => true,
        Expr::And(children) => children.iter().any(has_or),
    }
}

/// Whether each row that `expr` matches is found through an index.
fn anchored(expr: &Expr) -> bool {
    match expr {
        Expr::Leaf(Leaf::Attr(_)) => true,
        Expr::Leaf(Leaf::System(field, test)) => {
            *field == SystemField::WorkflowName
                && matches!(test, SystemTest::Eq(_) | SystemTest::In(_))
        }
        Expr::And(children) => children.iter().any(anchored),
        Expr::Or(children) => children.iter().all(anchored),
    }
}

#[derive(Debug, Clone, PartialEq)]
enum TokenKind {
    LParen,
    RParen,
    Comma,
    Op(&'static str),
    Word(String),
    Str(String),
    Num(String),
}

impl TokenKind {
    fn describe(&self) -> String {
        match self {
            Self::LParen => "'('".to_string(),
            Self::RParen => "')'".to_string(),
            Self::Comma => "','".to_string(),
            Self::Op(op) => format!("operator '{op}'"),
            Self::Word(word) => format!("word '{word}'"),
            Self::Str(_) => "string".to_string(),
            Self::Num(num) => format!("number {num}"),
        }
    }

    fn is_keyword(&self, keyword: &str) -> bool {
        matches!(self, Self::Word(word) if word.eq_ignore_ascii_case(keyword))
    }
}

#[derive(Debug, Clone)]
struct Token {
    kind: TokenKind,
    at: usize,
}

fn lex(raw: &str) -> Result<Vec<Token>, String> {
    let bytes = raw.as_bytes();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        let at = i;
        let kind = match c {
            b' ' | b'\t' | b'\n' | b'\r' => {
                i += 1;
                continue;
            }
            b'(' => {
                i += 1;
                TokenKind::LParen
            }
            b')' => {
                i += 1;
                TokenKind::RParen
            }
            b',' => {
                i += 1;
                TokenKind::Comma
            }
            b'=' => {
                i += 1;
                TokenKind::Op("=")
            }
            b'!' if bytes.get(i + 1) == Some(&b'=') => {
                i += 2;
                TokenKind::Op("!=")
            }
            b'<' if bytes.get(i + 1) == Some(&b'>') => {
                i += 2;
                TokenKind::Op("!=")
            }
            b'>' | b'<' => {
                let wide = bytes.get(i + 1) == Some(&b'=');
                i += if wide { 2 } else { 1 };
                TokenKind::Op(match (c, wide) {
                    (b'>', false) => ">",
                    (b'>', true) => ">=",
                    (_, false) => "<",
                    (_, true) => "<=",
                })
            }
            b'"' | b'\'' => {
                let (text, next) = lex_string(raw, i)?;
                i = next;
                TokenKind::Str(text)
            }
            b'-' | b'0'..=b'9' => {
                let next = lex_number(bytes, i);
                if next == i || (c == b'-' && next == i + 1) {
                    return Err(format!("expected a number at byte {at}"));
                }
                i = next;
                let text = &raw[at..i];
                if number_json(text).is_none() {
                    return Err(format!("the number at byte {at} is out of range"));
                }
                TokenKind::Num(text.to_string())
            }
            c if c.is_ascii_alphabetic() || c == b'_' => {
                while i < bytes.len()
                    && (bytes[i].is_ascii_alphanumeric() || matches!(bytes[i], b'_' | b'-' | b'.'))
                {
                    i += 1;
                }
                TokenKind::Word(raw[at..i].to_string())
            }
            _ => {
                let shown = raw[at..].chars().next().unwrap_or('?');
                return Err(format!("unexpected character '{shown}' at byte {at}"));
            }
        };
        tokens.push(Token { kind, at });
    }
    Ok(tokens)
}

/// Reads a quoted string that starts at `start`. Returns the text and the
/// byte after the closing quote.
fn lex_string(raw: &str, start: usize) -> Result<(String, usize), String> {
    let quote = raw.as_bytes()[start];
    let mut text = String::new();
    let mut chars = raw[start + 1..].char_indices();
    while let Some((offset, c)) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some((_, escaped)) => text.push(escaped),
                None => break,
            }
        } else if c as u32 == u32::from(quote) {
            return Ok((text, start + 1 + offset + 1));
        } else if c.is_control() {
            // Postgres rejects a NUL byte in text and in `jsonb`.
            return Err(format!(
                "the string at byte {start} holds a control character"
            ));
        } else {
            text.push(c);
        }
    }
    Err(format!("the string at byte {start} has no closing quote"))
}

/// Returns the byte after a number that starts at `start`.
fn lex_number(bytes: &[u8], start: usize) -> usize {
    let digits = |mut i: usize| {
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        i
    };
    let mut i = start;
    if bytes.get(i) == Some(&b'-') {
        i += 1;
    }
    let int_end = digits(i);
    if int_end == i {
        return start;
    }
    i = int_end;
    if bytes.get(i) == Some(&b'.') {
        let frac_end = digits(i + 1);
        if frac_end > i + 1 {
            i = frac_end;
        }
    }
    if matches!(bytes.get(i), Some(b'e' | b'E')) {
        let mut j = i + 1;
        if matches!(bytes.get(j), Some(b'+' | b'-')) {
            j += 1;
        }
        let exp_end = digits(j);
        if exp_end > j {
            i = exp_end;
        }
    }
    i
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
    end: usize,
    depth: usize,
    predicates: usize,
}

/// A parsed value literal.
enum Literal {
    Str(String),
    Num(String),
    Bool(bool),
}

impl Parser {
    fn peek(&self) -> Option<&TokenKind> {
        self.tokens.get(self.pos).map(|t| &t.kind)
    }

    fn at(&self) -> usize {
        self.tokens.get(self.pos).map_or(self.end, |t| t.at)
    }

    fn next(&mut self) -> Option<Token> {
        let token = self.tokens.get(self.pos).cloned();
        self.pos += 1;
        token
    }

    fn or_expr(&mut self) -> Result<Expr, String> {
        let mut children = vec![self.and_expr()?];
        while self.peek().is_some_and(|k| k.is_keyword("OR")) {
            self.pos += 1;
            children.push(self.and_expr()?);
        }
        Ok(flatten(children, Expr::Or))
    }

    fn and_expr(&mut self) -> Result<Expr, String> {
        let mut children = vec![self.term()?];
        while self.peek().is_some_and(|k| k.is_keyword("AND")) {
            self.pos += 1;
            children.push(self.term()?);
        }
        Ok(flatten(children, Expr::And))
    }

    fn term(&mut self) -> Result<Expr, String> {
        if self.peek() == Some(&TokenKind::LParen) {
            let open = self.at();
            self.pos += 1;
            self.depth += 1;
            if self.depth > MAX_DEPTH {
                return Err(format!(
                    "filter is nested too deep at byte {open}; the limit is {MAX_DEPTH} levels"
                ));
            }
            let inner = self.or_expr()?;
            if self.peek() != Some(&TokenKind::RParen) {
                return Err(format!(
                    "expected ')' at byte {} to close the '(' at byte {open}",
                    self.at()
                ));
            }
            self.pos += 1;
            self.depth -= 1;
            return Ok(inner);
        }
        self.predicate().map(Expr::Leaf)
    }

    fn predicate(&mut self) -> Result<Leaf, String> {
        self.predicates += 1;
        if self.predicates > MAX_PREDICATES {
            return Err(format!("filter has more than {MAX_PREDICATES} predicates"));
        }
        let at = self.at();
        let Some(Token {
            kind: TokenKind::Word(field),
            ..
        }) = self.next()
        else {
            return Err(format!("expected a field name at byte {at}"));
        };
        if let Some(key) = field.strip_prefix("attrs.") {
            let key = if key.is_empty() {
                match self.peek() {
                    Some(TokenKind::Str(quoted)) => {
                        let quoted = quoted.clone();
                        self.pos += 1;
                        quoted
                    }
                    _ => String::new(),
                }
            } else {
                key.to_string()
            };
            if key.is_empty() {
                return Err(format!("attrs. at byte {at} needs a key"));
            }
            if field.len() > "attrs.".len() && key.contains('.') {
                return Err(format!(
                    "attrs.{key} at byte {at} is a nested path; only top-level keys are \
                     filterable"
                ));
            }
            return self.attr_test(key).map(Leaf::Attr);
        }
        let system = match field.to_ascii_lowercase().as_str() {
            "state" => SystemField::State,
            "workflow_name" => SystemField::WorkflowName,
            "owner" => SystemField::Owner,
            "severity" => SystemField::Severity,
            "started_at" => SystemField::StartedAt,
            _ => {
                return Err(format!(
                    "unknown field '{field}' at byte {at}; use attrs.<key> for a search \
                     attribute, or one of state, workflow_name, owner, severity, started_at"
                ));
            }
        };
        self.system_test(system, &field)
            .map(|test| Leaf::System(system, test))
    }

    fn attr_test(&mut self, key: String) -> Result<SearchAttrPredicate, String> {
        let at = self.at();
        match self.next().map(|t| t.kind) {
            Some(k) if k.is_keyword("EXISTS") => Ok(SearchAttrPredicate::Exists { key }),
            Some(k) if k.is_keyword("IN") => {
                let values = self
                    .in_list()?
                    .into_iter()
                    .map(Literal::into_json)
                    .collect();
                Ok(SearchAttrPredicate::In { key, values })
            }
            Some(TokenKind::Op(op)) => {
                let value_at = self.at();
                let value = self.literal()?;
                let cmp = match op {
                    "=" => {
                        return Ok(SearchAttrPredicate::Eq {
                            key,
                            value: value.into_json(),
                        });
                    }
                    "!=" => {
                        return Ok(SearchAttrPredicate::Ne {
                            key,
                            value: value.into_json(),
                        });
                    }
                    ">" => CmpOp::Gt,
                    ">=" => CmpOp::Gte,
                    "<" => CmpOp::Lt,
                    _ => CmpOp::Lte,
                };
                match value {
                    Literal::Num(text) if text.parse::<f64>().is_ok_and(f64::is_finite) => {
                        Ok(SearchAttrPredicate::Cmp {
                            key,
                            op: cmp,
                            value: text,
                        })
                    }
                    _ => Err(format!(
                        "operator '{op}' at byte {value_at} needs a finite number"
                    )),
                }
            }
            _ => Err(format!("expected an operator, IN or EXISTS at byte {at}")),
        }
    }

    fn system_test(&mut self, field: SystemField, name: &str) -> Result<SystemTest, String> {
        let at = self.at();
        let op = match self.next().map(|t| t.kind) {
            Some(TokenKind::Op(op)) => op,
            Some(k) if k.is_keyword("IN") => "IN",
            Some(k) if k.is_keyword("EXISTS") => {
                return Err(format!("EXISTS at byte {at} applies to attrs.* only"));
            }
            _ => return Err(format!("expected an operator or IN at byte {at}")),
        };
        if field == SystemField::StartedAt {
            let cmp = match op {
                ">" => CmpOp::Gt,
                ">=" => CmpOp::Gte,
                "<" => CmpOp::Lt,
                "<=" => CmpOp::Lte,
                _ => {
                    return Err(format!(
                        "started_at at byte {at} takes the operator >, >=, < or <="
                    ));
                }
            };
            let value_at = self.at();
            let text = self.string_literal(name)?;
            let ts = chrono::DateTime::parse_from_rfc3339(&text)
                .map_err(|_| format!("started_at at byte {value_at} needs an RFC 3339 time"))?;
            return Ok(SystemTest::Cmp(cmp, ts.with_timezone(&chrono::Utc)));
        }
        let check = |value: String, value_at: usize| -> Result<String, String> {
            if field != SystemField::State {
                return Ok(value);
            }
            let upper = value.to_ascii_uppercase();
            // The default list hides `MIGRATED` rows, so this filter could
            // never match. The `state` parameter shows them.
            if upper == "MIGRATED" {
                return Err(format!(
                    "state MIGRATED at byte {value_at} is not filterable here; use state=MIGRATED"
                ));
            }
            if crate::api::KNOWN_WORKFLOW_STATES.contains(&upper.as_str()) {
                Ok(upper)
            } else {
                Err(format!("unknown state '{value}' at byte {value_at}"))
            }
        };
        match op {
            "=" | "!=" => {
                let value_at = self.at();
                let value = check(self.string_literal(name)?, value_at)?;
                Ok(if op == "=" {
                    SystemTest::Eq(value)
                } else {
                    SystemTest::Ne(value)
                })
            }
            "IN" => {
                let value_at = self.at();
                let mut values = Vec::new();
                for literal in self.in_list()? {
                    match literal {
                        Literal::Str(text) => values.push(check(text, value_at)?),
                        _ => return Err(format!("{name} at byte {value_at} needs string values")),
                    }
                }
                Ok(SystemTest::In(values))
            }
            _ => Err(format!(
                "{name} at byte {at} takes the operator =, != or IN"
            )),
        }
    }

    fn string_literal(&mut self, name: &str) -> Result<String, String> {
        let at = self.at();
        match self.literal()? {
            Literal::Str(text) => Ok(text),
            _ => Err(format!("{name} at byte {at} needs a quoted string")),
        }
    }

    fn in_list(&mut self) -> Result<Vec<Literal>, String> {
        if self.peek() != Some(&TokenKind::LParen) {
            return Err(format!("expected '(' after IN at byte {}", self.at()));
        }
        self.pos += 1;
        let mut values = vec![self.literal()?];
        while self.peek() == Some(&TokenKind::Comma) {
            self.pos += 1;
            values.push(self.literal()?);
            if values.len() > MAX_IN_VALUES {
                return Err(format!("IN list has more than {MAX_IN_VALUES} values"));
            }
        }
        if self.peek() != Some(&TokenKind::RParen) {
            return Err(format!("expected ')' at byte {} to close IN", self.at()));
        }
        self.pos += 1;
        Ok(values)
    }

    fn literal(&mut self) -> Result<Literal, String> {
        let at = self.at();
        match self.next().map(|t| t.kind) {
            Some(TokenKind::Str(text)) => Ok(Literal::Str(text)),
            Some(TokenKind::Num(text)) => Ok(Literal::Num(text)),
            Some(k) if k.is_keyword("true") => Ok(Literal::Bool(true)),
            Some(k) if k.is_keyword("false") => Ok(Literal::Bool(false)),
            _ => Err(format!(
                "expected a value at byte {at}: a quoted string, a number, true or false"
            )),
        }
    }
}

impl Literal {
    fn into_json(self) -> Value {
        match self {
            Self::Str(text) => Value::String(text),
            // The lexer checks each number, so `Null` does not occur.
            Self::Num(text) => number_json(&text).unwrap_or(Value::Null),
            Self::Bool(flag) => Value::Bool(flag),
        }
    }
}

/// Converts number text to a JSON number. Returns `None` when JSON or
/// Postgres `numeric` cannot hold the value, for example `1e400`, or
/// `1e-400`, which `f64` reads as zero.
fn number_json(text: &str) -> Option<Value> {
    if let Ok(n) = text.parse::<i64>() {
        return Some(Value::Number(n.into()));
    }
    if let Ok(n) = text.parse::<u64>() {
        return Some(Value::Number(n.into()));
    }
    let n = text.parse::<f64>().ok().filter(|n| n.is_finite())?;
    let mantissa = text.split(['e', 'E']).next().unwrap_or(text);
    if n == 0.0 && mantissa.bytes().any(|b| matches!(b, b'1'..=b'9')) {
        return None;
    }
    serde_json::Number::from_f64(n).map(Value::Number)
}

/// Returns the one child, or a node of the children.
fn flatten(mut children: Vec<Expr>, node: fn(Vec<Expr>) -> Expr) -> Expr {
    if children.len() == 1 {
        children.remove(0)
    } else {
        node(children)
    }
}

/// One part of a compiled filter.
#[derive(Debug, Clone)]
enum Part {
    Sql(&'static str),
    Text(String),
    TextArray(Vec<String>),
    Jsonb(Value),
    JsonbArray(Vec<Value>),
    Timestamptz(chrono::DateTime<chrono::Utc>),
}

/// A compiled filter: SQL text with bound values.
#[derive(Debug, Clone, Default)]
pub struct SqlFilter {
    parts: Vec<Part>,
}

impl SqlFilter {
    /// Compiles a parsed filter.
    pub fn from_expr(expr: &Expr) -> Self {
        let mut filter = Self::default();
        filter.push_expr(expr);
        filter
    }

    /// Compiles one #506 predicate.
    pub fn from_attr(predicate: &SearchAttrPredicate) -> Self {
        let mut filter = Self::default();
        filter.push_attr(predicate);
        filter
    }

    /// Compiles one `search_attr=key:value` containment object.
    pub fn contains(object: Value) -> Self {
        Self {
            parts: vec![Part::Sql(SEARCH_ATTRS_CONTAINS), Part::Jsonb(object)],
        }
    }

    fn sql(&mut self, sql: &'static str) {
        self.parts.push(Part::Sql(sql));
    }

    fn push_expr(&mut self, expr: &Expr) {
        let (children, joiner) = match expr {
            Expr::Leaf(Leaf::Attr(predicate)) => return self.push_attr(predicate),
            Expr::Leaf(Leaf::System(field, test)) => return self.push_system(*field, test),
            Expr::And(children) => (children, " AND "),
            Expr::Or(children) => (children, " OR "),
        };
        self.sql("(");
        for (i, child) in children.iter().enumerate() {
            if i > 0 {
                self.sql(joiner);
            }
            self.push_expr(child);
        }
        self.sql(")");
    }

    fn push_attr(&mut self, predicate: &SearchAttrPredicate) {
        match predicate {
            SearchAttrPredicate::Eq { key, value } => {
                self.sql(SEARCH_ATTRS_CONTAINS);
                self.parts.push(Part::Jsonb(
                    serde_json::json!({ key.clone(): value.clone() }),
                ));
            }
            SearchAttrPredicate::Ne { key, value } => {
                self.sql("(harvest_workflow_executions.search_attrs ? ");
                self.parts.push(Part::Text(key.clone()));
                self.sql(" AND NOT (harvest_workflow_executions.search_attrs @> ");
                self.parts.push(Part::Jsonb(
                    serde_json::json!({ key.clone(): value.clone() }),
                ));
                self.sql("))");
            }
            SearchAttrPredicate::Cmp { key, op, value } => {
                // The `?` test lets the GIN index find the rows. The numeric
                // cast then checks each row. The bound text keeps integers past
                // 2^53 exact.
                self.sql("(harvest_workflow_executions.search_attrs ? ");
                self.parts.push(Part::Text(key.clone()));
                self.sql(" AND jsonb_typeof(harvest_workflow_executions.search_attrs -> ");
                self.parts.push(Part::Text(key.clone()));
                self.sql(") = 'number' AND (harvest_workflow_executions.search_attrs ->> ");
                self.parts.push(Part::Text(key.clone()));
                self.sql(match op {
                    CmpOp::Gt => ")::numeric > ",
                    CmpOp::Gte => ")::numeric >= ",
                    CmpOp::Lt => ")::numeric < ",
                    CmpOp::Lte => ")::numeric <= ",
                });
                self.parts.push(Part::Text(value.clone()));
                self.sql("::numeric)");
            }
            SearchAttrPredicate::In { key, values } => {
                self.sql("(harvest_workflow_executions.search_attrs ? ");
                self.parts.push(Part::Text(key.clone()));
                self.sql(" AND harvest_workflow_executions.search_attrs -> ");
                self.parts.push(Part::Text(key.clone()));
                self.sql(" = ANY(");
                self.parts.push(Part::JsonbArray(values.clone()));
                self.sql("))");
            }
            SearchAttrPredicate::Exists { key } => {
                self.sql("harvest_workflow_executions.search_attrs ? ");
                self.parts.push(Part::Text(key.clone()));
            }
        }
    }

    fn push_system(&mut self, field: SystemField, test: &SystemTest) {
        self.sql(match field {
            SystemField::State => "harvest_workflow_executions.state",
            SystemField::WorkflowName => "harvest_workflow_executions.workflow_name",
            SystemField::Owner => "harvest_workflow_executions.owner",
            SystemField::Severity => "harvest_workflow_executions.severity",
            SystemField::StartedAt => "harvest_workflow_executions.started_at",
        });
        match test {
            SystemTest::Eq(value) => {
                self.sql(" = ");
                self.parts.push(Part::Text(value.clone()));
            }
            SystemTest::Ne(value) => {
                self.sql(" <> ");
                self.parts.push(Part::Text(value.clone()));
            }
            SystemTest::In(values) => {
                self.sql(" = ANY(");
                self.parts.push(Part::TextArray(values.clone()));
                self.sql(")");
            }
            SystemTest::Cmp(op, at) => {
                self.sql(match op {
                    CmpOp::Gt => " > ",
                    CmpOp::Gte => " >= ",
                    CmpOp::Lt => " < ",
                    CmpOp::Lte => " <= ",
                });
                self.parts.push(Part::Timestamptz(*at));
            }
        }
    }
}

const SEARCH_ATTRS_CONTAINS: &str = "harvest_workflow_executions.search_attrs @> ";

impl diesel::expression::Expression for SqlFilter {
    type SqlType = diesel::sql_types::Bool;
}

impl<QS> diesel::expression::AppearsOnTable<QS> for SqlFilter {}

impl<QS> diesel::expression::SelectableExpression<QS> for SqlFilter {}

impl diesel::expression::ValidGrouping<()> for SqlFilter {
    type IsAggregate = diesel::expression::is_aggregate::Never;
}

impl QueryId for SqlFilter {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl QueryFragment<Pg> for SqlFilter {
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        use diesel::sql_types::{Array, Jsonb, Text, Timestamptz};

        // The SQL text changes with the filter shape. A cached statement for
        // each shape only fills the cache.
        out.unsafe_to_cache_prepared();
        for part in &self.parts {
            match part {
                Part::Sql(sql) => out.push_sql(sql),
                Part::Text(v) => out.push_bind_param::<Text, _>(v)?,
                Part::TextArray(v) => out.push_bind_param::<Array<Text>, _>(v)?,
                Part::Jsonb(v) => out.push_bind_param::<Jsonb, _>(v)?,
                Part::JsonbArray(v) => out.push_bind_param::<Array<Jsonb>, _>(v)?,
                Part::Timestamptz(v) => out.push_bind_param::<Timestamptz, _>(v)?,
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    /// A short, stable form of a tree for assertions.
    fn shape(expr: &Expr) -> String {
        match expr {
            Expr::And(children) => format!(
                "and({})",
                children.iter().map(shape).collect::<Vec<_>>().join(",")
            ),
            Expr::Or(children) => format!(
                "or({})",
                children.iter().map(shape).collect::<Vec<_>>().join(",")
            ),
            Expr::Leaf(Leaf::Attr(p)) => match p {
                SearchAttrPredicate::Eq { key, value } => format!("{key}={value}"),
                SearchAttrPredicate::Ne { key, value } => format!("{key}!={value}"),
                SearchAttrPredicate::Cmp { key, op, value } => format!("{key}{op:?}{value}"),
                SearchAttrPredicate::In { key, values } => format!(
                    "{key} in [{}]",
                    values
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(",")
                ),
                SearchAttrPredicate::Exists { key } => format!("{key} exists"),
            },
            Expr::Leaf(Leaf::System(field, test)) => format!("{field:?}:{test:?}"),
        }
    }

    fn parsed(raw: &str) -> String {
        shape(&parse(raw).unwrap_or_else(|e| panic!("{raw:?} must parse: {e}")))
    }

    fn rejected(raw: &str) -> String {
        match parse(raw) {
            Ok(expr) => panic!("{raw:?} must not parse, got {}", shape(&expr)),
            Err(message) => message,
        }
    }

    fn sql(raw: &str) -> String {
        let filter = SqlFilter::from_expr(&parse(raw).unwrap());
        diesel::debug_query::<Pg, _>(&filter).to_string()
    }

    #[test]
    fn and_binds_tighter_than_or() {
        assert_eq!(
            parsed("attrs.a = 1 OR attrs.b = 2 AND attrs.c = 3"),
            "or(a=1,and(b=2,c=3))"
        );
        assert_eq!(
            parsed("attrs.a = 1 AND attrs.b = 2 OR attrs.c = 3"),
            "or(and(a=1,b=2),c=3)"
        );
    }

    #[test]
    fn parentheses_override_precedence() {
        assert_eq!(
            parsed("(attrs.a = 1 OR attrs.b = 2) AND attrs.c = 3"),
            "and(or(a=1,b=2),c=3)"
        );
        assert_eq!(
            parsed("attrs.a = 1 AND (attrs.b = 2 OR (attrs.c = 3 AND attrs.d = 4))"),
            "and(a=1,or(b=2,and(c=3,d=4)))"
        );
    }

    #[test]
    fn a_chain_of_one_operator_is_flat() {
        assert_eq!(
            parsed("attrs.a = 1 OR attrs.b = 2 OR attrs.c = 3"),
            "or(a=1,b=2,c=3)"
        );
        assert_eq!(parsed("((attrs.a = 1))"), "a=1");
    }

    #[test]
    fn keywords_are_not_case_sensitive() {
        assert_eq!(
            parsed("attrs.a = 1 or attrs.b in (1, 2) And attrs.c exists"),
            "or(a=1,and(b in [1,2],c exists))"
        );
    }

    #[test]
    fn quotes_decide_the_value_type() {
        assert_eq!(parsed("attrs.n = 100"), "n=100");
        assert_eq!(parsed("attrs.n = \"100\""), "n=\"100\"");
        assert_eq!(parsed("attrs.n = '100'"), "n=\"100\"");
        assert_eq!(parsed("attrs.flag = true"), "flag=true");
        assert_eq!(parsed("attrs.flag = \"true\""), "flag=\"true\"");
        assert_eq!(parsed("attrs.x = -2.5"), "x=-2.5");
    }

    #[test]
    fn strings_take_escapes_and_spaces() {
        assert_eq!(parsed(r#"attrs.note = "a \"b\" c""#), r#"note="a \"b\" c""#);
        assert_eq!(parsed(r"attrs.note = 'it\'s'"), r#"note="it's""#);
        assert_eq!(parsed(r#"attrs."my key" = 1"#), "my key=1");
    }

    #[test]
    fn comparison_ops_keep_the_numeric_text() {
        assert_eq!(parsed("attrs.amount > 10000"), "amountGt10000");
        assert_eq!(parsed("attrs.amount>=1e3"), "amountGte1e3");
        assert_eq!(parsed("attrs.amount < -1"), "amountLt-1");
        assert_eq!(parsed("attrs.amount <= 2.5"), "amountLte2.5");
        assert!(rejected("attrs.amount > \"10\"").contains("number"));
    }

    #[test]
    fn system_fields_parse() {
        assert_eq!(parsed("state = 'RUNNING'"), "State:Eq(\"RUNNING\")");
        assert_eq!(parsed("state = 'running'"), "State:Eq(\"RUNNING\")");
        assert_eq!(
            parsed("state IN ('RUNNING', 'FAILED')"),
            "State:In([\"RUNNING\", \"FAILED\"])"
        );
        assert_eq!(parsed("workflow_name != 'x'"), "WorkflowName:Ne(\"x\")");
        assert_eq!(parsed("owner = 'ops'"), "Owner:Eq(\"ops\")");
        assert_eq!(parsed("severity = 'high'"), "Severity:Eq(\"high\")");
        assert!(parsed("started_at >= '2026-01-01T00:00:00Z'").starts_with("StartedAt:Cmp(Gte"));
    }

    #[test]
    fn system_fields_reject_bad_input() {
        assert!(rejected("state = 'BOGUS'").contains("state"));
        assert!(rejected("state > 'RUNNING'").contains("operator"));
        assert!(rejected("state = 1").contains("string"));
        assert!(rejected("started_at = '2026-01-01T00:00:00Z'").contains("operator"));
        assert!(rejected("started_at > 'yesterday'").contains("RFC 3339"));
        assert!(rejected("owner EXISTS").contains("EXISTS"));
        assert!(rejected("queue_name = 'q'").contains("unknown field"));
        assert!(rejected("phase = 'x'").contains("attrs."));
    }

    #[test]
    fn syntax_errors_name_the_offset() {
        assert!(rejected("").contains("empty"));
        assert!(rejected("   ").contains("empty"));
        assert!(rejected("(attrs.a = 1").contains("')'"));
        assert!(rejected("attrs.a = 1)").contains("byte 11"));
        assert!(rejected("attrs.a = 1 attrs.b = 2").contains("byte 12"));
        assert!(rejected("attrs.a =").contains("value"));
        assert!(rejected("attrs.a = 'open").contains("quote"));
        assert!(rejected("attrs.a = 1 OR").contains("field"));
        assert!(rejected("attrs.a == 1").contains("value"));
        assert!(rejected("attrs.a = 1 & attrs.b = 2").contains("character"));
        assert!(rejected("attrs.a IN ()").contains("value"));
        assert!(rejected("attrs.a = bare").contains("value"));
    }

    #[test]
    fn attribute_keys_are_top_level() {
        assert!(rejected("attrs.a.b = 1").contains("nested"));
        assert!(rejected("attrs. = 1").contains("key"));
        assert!(rejected("attrs.\"\" = 1").contains("key"));
    }

    #[test]
    fn limits_hold() {
        let long = format!("attrs.a = '{}'", "x".repeat(MAX_FILTER_LEN));
        assert!(rejected(&long).contains("bytes"));

        let deep = format!(
            "{}attrs.a = 1{}",
            "(".repeat(MAX_DEPTH + 1),
            ")".repeat(MAX_DEPTH + 1)
        );
        assert!(rejected(&deep).contains("deep"));
        let ok_deep = format!(
            "{}attrs.a = 1{}",
            "(".repeat(MAX_DEPTH),
            ")".repeat(MAX_DEPTH)
        );
        parse(&ok_deep).expect("the depth limit is inclusive");

        let many = (0..=MAX_PREDICATES)
            .map(|i| format!("attrs.k{i} = 1"))
            .collect::<Vec<_>>()
            .join(" AND ");
        assert!(rejected(&many).contains("predicates"));

        let values = (0..=MAX_IN_VALUES)
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(",");
        assert!(rejected(&format!("attrs.a IN ({values})")).contains("values"));
    }

    #[test]
    fn a_filter_with_or_needs_an_index_anchor() {
        // A state-only branch has no general index.
        let message = rejected("attrs.a = 1 OR state = 'RUNNING'");
        assert!(message.contains("index"), "{message}");
        assert!(rejected("owner = 'x' OR severity = 'y'").contains("index"));
        assert!(rejected("started_at > '2026-01-01T00:00:00Z' OR attrs.a = 1").contains("index"));
        assert!(rejected("workflow_name != 'w' OR attrs.a = 1").contains("index"));
        // Each OR branch holds an anchor.
        parse("(attrs.a = 1 AND state = 'RUNNING') OR workflow_name = 'w'").unwrap();
        parse("workflow_name IN ('a','b') OR attrs.x EXISTS").unwrap();
        parse("attrs.a != 1 OR attrs.b > 2").unwrap();
        // The whole filter holds an anchor, so an OR below it needs none.
        parse("attrs.c = 1 AND (state = 'FAILED' OR severity = 'high')").unwrap();
        parse("(attrs.a = 1 OR owner = 'x') AND attrs.c = 1").unwrap();
        parse("(attrs.a = 1 OR attrs.b = 1) AND state = 'RUNNING'").unwrap();
        // With no OR, no anchor is needed.
        parse("state = 'RUNNING' AND owner = 'x'").unwrap();
    }

    #[test]
    fn values_that_postgres_rejects_fail_early() {
        assert!(rejected("attrs.a > 1e-2000000000").contains("out of range"));
        assert!(rejected("attrs.a = 1e400").contains("out of range"));
        assert!(rejected("owner = 'a\u{0}b'").contains("control character"));
        assert!(rejected("attrs.a = 'x\u{1}'").contains("control character"));
        assert_eq!(parsed("attrs.a = 0.0e5"), "a=0.0");
        assert!(rejected("state = 'MIGRATED'").contains("state=MIGRATED"));
        assert!(rejected("state IN ('RUNNING', 'migrated')").contains("state=MIGRATED"));
    }

    #[test]
    fn workflow_names_lists_eq_and_in_names() {
        let expr =
            parse("workflow_name = 'a' OR (workflow_name IN ('b', 'c') AND attrs.x = 1)").unwrap();
        assert_eq!(workflow_names(&expr), ["a", "b", "c"]);
        let expr = parse("workflow_name != 'a' AND attrs.x = 1").unwrap();
        assert_eq!(workflow_names(&expr), Vec::<String>::new());
    }

    #[test]
    fn angle_brackets_mean_not_equal() {
        assert_eq!(parsed("attrs.a <> 1"), "a!=1");
        assert_eq!(parsed("owner<>'x'"), "Owner:Ne(\"x\")");
    }

    #[test]
    fn sql_groups_every_node_and_binds_every_value() {
        let text = sql("attrs.a = 1 OR attrs.b = 'x' AND state = 'RUNNING'");
        assert!(
            text.starts_with(
                "(harvest_workflow_executions.search_attrs @> $1 OR \
                 (harvest_workflow_executions.search_attrs @> $2 AND \
                 harvest_workflow_executions.state = $3))"
            ),
            "{text}"
        );
        assert!(text.contains("Object {\"a\": Number(1)}"), "{text}");
        assert!(text.contains("\"RUNNING\""), "{text}");
    }

    #[test]
    fn sql_for_each_leaf_kind() {
        assert!(sql("attrs.a != 1").starts_with(
            "(harvest_workflow_executions.search_attrs ? $1 AND NOT \
             (harvest_workflow_executions.search_attrs @> $2))"
        ));
        assert!(sql("attrs.a > 5").starts_with(
            "(harvest_workflow_executions.search_attrs ? $1 AND \
             jsonb_typeof(harvest_workflow_executions.search_attrs -> $2) = 'number' AND \
             (harvest_workflow_executions.search_attrs ->> $3)::numeric > $4::numeric)"
        ));
        assert!(sql("attrs.a IN (1, 'b')").starts_with(
            "(harvest_workflow_executions.search_attrs ? $1 AND \
             harvest_workflow_executions.search_attrs -> $2 = ANY($3))"
        ));
        assert!(sql("attrs.a EXISTS").starts_with("harvest_workflow_executions.search_attrs ? $1"));
        assert!(
            sql("workflow_name IN ('a')")
                .starts_with("harvest_workflow_executions.workflow_name = ANY($1)")
        );
        assert!(sql("owner != 'a'").starts_with("harvest_workflow_executions.owner <> $1"));
        assert!(
            sql("started_at < '2026-01-01T00:00:00Z'")
                .starts_with("harvest_workflow_executions.started_at < $1")
        );
    }

    #[test]
    fn injection_text_stays_in_a_bind() {
        let text = sql("attrs.\"a') OR 1=1 --\" = 'x\\' OR 1=1 --'");
        let (statement, binds) = text.split_once("-- binds").expect("debug form");
        assert!(!statement.contains("1=1"), "{statement}");
        assert!(binds.contains("1=1"), "{binds}");
    }
}
