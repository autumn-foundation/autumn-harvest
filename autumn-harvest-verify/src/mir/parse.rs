//! Parser entry point for the textual MIR that stable `rustc --emit=mir` emits.
//!
//! The format is not a stable API, so the parser is *tolerant by construction*
//! (D1): it works item by item, every construct it does not understand is kept
//! as opaque text, an item whose body will not parse is recorded in
//! [`MirDoc::parse_failures`] and skipped, and nothing in here can panic on a
//! truncated or garbled dump.
//!
//! Shape of a dump (rustc 1.94.x):
//!
//! ```text
//! fn PATH(_1: T, ...) -> R {          // `static [mut] P: T = {`, `const P: T = {`
//!     debug name => PLACE;            // and the keyword-less `P: T = {` too
//!     let mut _2: TYPE;
//!     scope 1 { debug x => _3; let _4: TYPE; }
//!     bb0: { STATEMENT; ... TERMINATOR; }
//!     bb1 (cleanup): { ... }
//! }
//! alloc1 (static: COUNTER, size: 8, align: 8) { .. }
//! ```
//!
//! Items always start in column 0 and always end at a `}` in column 0, which
//! is what the item scanner keys on; everything else is parsed per line.

use std::collections::BTreeMap;

use super::ast::{
    BasicBlock, Body, Local, MirDoc, Operand, ParseFailure, Place, Projection, Rvalue, Statement,
    StaticItem, Terminator,
};
use super::lexer;

/// Guard against pathological nesting in a garbled dump.
const MAX_PLACE_DEPTH: u32 = 64;

/// Parse one `.mir` dump. Never panics; unparseable items land in
/// [`MirDoc::parse_failures`].
#[must_use]
pub fn parse(crate_name: &str, path: &str, text: &str) -> MirDoc {
    let mut doc = MirDoc {
        crate_name: crate_name.to_owned(),
        path: path.to_owned(),
        ..MirDoc::default()
    };
    let lines: Vec<&str> = text.lines().collect();
    let mut idx = 0_usize;
    while let Some(header) = lines.get(idx).copied() {
        if !is_item_start(header) {
            idx += 1;
            continue;
        }
        if !header.trim_end().ends_with('{') {
            // A body-less item such as `const _: () = const ();`.
            idx += 1;
            continue;
        }
        let end =
            (idx + 1..lines.len()).find(|j| lines.get(*j).is_some_and(|l| l.starts_with('}')));
        let Some(end) = end else {
            doc.parse_failures.push(ParseFailure {
                item: label_of(header),
                reason: "unterminated item body (truncated dump)".to_owned(),
                line: idx + 1,
            });
            break;
        };
        let content = lines.get(idx + 1..end).unwrap_or(&[]);
        parse_item(&mut doc, header, content, idx + 1);
        idx = end + 1;
    }
    doc
}

/// Column-0, non-comment, non-closing lines start an item.
fn is_item_start(line: &str) -> bool {
    !line.is_empty() && !line.starts_with([' ', '\t', '}']) && !line.starts_with("//")
}

/// A short, stable name for a header that failed to parse.
fn label_of(header: &str) -> String {
    let trimmed = header.trim();
    trimmed
        .get(..trimmed.len().min(120))
        .unwrap_or(trimmed)
        .to_owned()
}

// ── items ───────────────────────────────────────────────────────────────────

fn parse_item(doc: &mut MirDoc, header: &str, content: &[&str], line: usize) {
    let head = header.trim_end();
    let head = head.strip_suffix('{').unwrap_or(head).trim_end();
    if let Some(sig) = head.strip_prefix("fn ") {
        push_body(doc, sig, content, line, false);
    } else if let Some(sig) = head.strip_prefix("static ") {
        let (sig, is_mut) = sig
            .strip_prefix("mut ")
            .map_or((sig, false), |rest| (rest, true));
        match split_typed(sig) {
            Some((path, ty)) => doc.statics.push(StaticItem { path, ty, is_mut }),
            None => fail(doc, header, "malformed static header", line),
        }
    } else if let Some(sig) = head.strip_prefix("const ") {
        push_const_body(doc, sig, header, content, line);
    } else if is_alloc_header(head) {
        parse_alloc_footer(doc, head, header, line);
    } else if lexer::find_type_colon(head).is_some() {
        // The keyword-less anonymous-constant form: `PATH: TY = { .. }`.
        push_const_body(doc, head, header, content, line);
    } else {
        fail(doc, header, "unrecognized item header", line);
    }
}

fn fail(doc: &mut MirDoc, header: &str, reason: &str, line: usize) {
    doc.parse_failures.push(ParseFailure {
        item: label_of(header),
        reason: reason.to_owned(),
        line,
    });
}

fn push_body(doc: &mut MirDoc, sig: &str, content: &[&str], line: usize, is_const: bool) {
    match parse_fn_signature(sig) {
        Some((path, params, return_ty)) => {
            doc.bodies
                .push(build_body(path, is_const, params, return_ty, content, line));
        }
        None => fail(doc, sig, "malformed fn header", line),
    }
}

fn push_const_body(doc: &mut MirDoc, sig: &str, header: &str, content: &[&str], line: usize) {
    match split_typed(sig) {
        Some((path, ty)) => {
            doc.bodies
                .push(build_body(path, true, Vec::new(), ty, content, line));
        }
        None => fail(doc, header, "malformed constant header", line),
    }
}

fn build_body(
    path: String,
    is_const: bool,
    params: Vec<(Local, String)>,
    return_ty: String,
    content: &[&str],
    line: usize,
) -> Body {
    let mut locals: BTreeMap<Local, String> = params.iter().cloned().collect();
    let mut debug_names = Vec::new();
    let blocks = parse_body_content(content, &mut locals, &mut debug_names);
    Body {
        path,
        is_const,
        params,
        return_ty,
        locals,
        debug_names,
        blocks,
        line,
    }
}

/// `(path, params, return type)` of a `fn` item header.
type FnSignature = (String, Vec<(Local, String)>, String);

/// `PATH(_1: T, _2: U) -> RET` (the trailing `= {` / `{` is already stripped).
fn parse_fn_signature(sig: &str) -> Option<FnSignature> {
    let open = lexer::find_top(sig, "(")?;
    let close = lexer::match_at(sig, open)?;
    let path = sig.get(..open)?.trim();
    if path.is_empty() {
        return None;
    }
    let mut params = Vec::new();
    for param in lexer::split_top(sig.get(open + 1..close)?, b',') {
        let param = param.trim();
        let Some(colon) = lexer::find_type_colon(param) else {
            continue;
        };
        let (Some(name), Some(ty)) = (param.get(..colon), param.get(colon + 1..)) else {
            continue;
        };
        if let Some(local) = parse_local(name) {
            params.push((local, ty.trim().to_owned()));
        }
    }
    let tail = sig.get(close + 1..)?.trim();
    let return_ty = tail.strip_prefix("->").map_or(tail, str::trim);
    if return_ty.is_empty() {
        return None;
    }
    Some((path.to_owned(), params, return_ty.trim().to_owned()))
}

/// `PATH: TYPE =` → `(PATH, TYPE)`.
fn split_typed(sig: &str) -> Option<(String, String)> {
    let sig = sig.trim_end();
    let sig = sig.strip_suffix('=').map_or(sig, str::trim_end);
    let colon = lexer::find_type_colon(sig)?;
    let path = sig.get(..colon)?.trim();
    let ty = sig.get(colon + 1..)?.trim();
    if path.is_empty() || ty.is_empty() {
        return None;
    }
    Some((path.to_owned(), ty.to_owned()))
}

fn is_alloc_header(head: &str) -> bool {
    head.strip_prefix("alloc").is_some_and(|rest| {
        let digits = rest
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(rest.len());
        digits > 0
            && rest
                .get(digits..)
                .is_some_and(|t| t.trim_start().starts_with('('))
    })
}

/// `allocN (static: NAME, size: .., align: ..)` — the map from an `allocN`
/// operand back to the `static` item it points at. Duplicated footers (rustc
/// prints one per referring body) are idempotent.
fn parse_alloc_footer(doc: &mut MirDoc, head: &str, header: &str, line: usize) {
    let Some(space) = head.find(|c: char| c.is_whitespace()) else {
        fail(doc, header, "malformed alloc footer", line);
        return;
    };
    let Some(name) = head.get(..space) else {
        return;
    };
    let Some((_, inner)) = lexer::trailing_group(head.trim_end(), b'(', b')') else {
        fail(doc, header, "malformed alloc footer", line);
        return;
    };
    for field in lexer::split_top(inner, b',') {
        let Some(rest) = field.trim().strip_prefix("static:") else {
            continue;
        };
        let target = rest.trim();
        if target.is_empty() {
            fail(doc, header, "alloc footer names an empty static", line);
        } else {
            doc.alloc_statics.insert(name.to_owned(), target.to_owned());
        }
        return;
    }
}

// ── body content ────────────────────────────────────────────────────────────

fn parse_body_content(
    content: &[&str],
    locals: &mut BTreeMap<Local, String>,
    debug_names: &mut Vec<(String, Place)>,
) -> Vec<BasicBlock> {
    let mut blocks = Vec::new();
    let mut current: Option<(String, bool, Vec<&str>)> = None;
    for raw in content {
        let line = raw.trim();
        if line.is_empty() {
            continue;
        }
        if let Some((label, cleanup)) = block_header(line) {
            if let Some(open) = current.take() {
                blocks.push(finish_block(open));
            }
            current = Some((label, cleanup, Vec::new()));
        } else if line == "}" {
            if let Some(open) = current.take() {
                blocks.push(finish_block(open));
            }
        } else if let Some(open) = current.as_mut() {
            open.2.push(line);
        } else if let Some(binding) = line.strip_prefix("debug ") {
            parse_debug(binding, debug_names);
        } else if let Some(decl) = line.strip_prefix("let ") {
            parse_decl(decl, locals);
        }
        // `scope N {` and anything else in the declaration section is skipped;
        // its `let`/`debug` children are picked up by the flat scan above.
    }
    if let Some(open) = current.take() {
        blocks.push(finish_block(open));
    }
    blocks
}

/// `bbN: {` / `bbN (cleanup): {`.
fn block_header(line: &str) -> Option<(String, bool)> {
    let after = line.strip_prefix("bb")?;
    let end = after.find(|c: char| !c.is_ascii_digit())?;
    if end == 0 {
        return None;
    }
    let digits = after.get(..end)?;
    let tail = after.get(end..)?.trim();
    let (cleanup, tail) = tail
        .strip_prefix("(cleanup)")
        .map_or((false, tail), |rest| (true, rest.trim()));
    if tail != ": {" && tail != ":{" {
        return None;
    }
    Some((format!("bb{digits}"), cleanup))
}

fn finish_block((label, cleanup, mut lines): (String, bool, Vec<&str>)) -> BasicBlock {
    let terminator = lines.pop().map_or_else(
        || Terminator::Other {
            text: String::new(),
            targets: Vec::new(),
        },
        parse_terminator,
    );
    BasicBlock {
        label,
        cleanup,
        statements: lines.into_iter().map(parse_statement).collect(),
        terminator,
    }
}

/// `debug NAME => PLACE;`
fn parse_debug(binding: &str, out: &mut Vec<(String, Place)>) {
    let Some(arrow) = binding.find(" => ") else {
        return;
    };
    let (Some(name), Some(place)) = (binding.get(..arrow), binding.get(arrow + 4..)) else {
        return;
    };
    let place = place.trim().trim_end_matches(';').trim();
    if let Some(parsed) = parse_place(place) {
        out.push((name.trim().to_owned(), parsed));
    }
}

/// `let [mut] _N: TYPE;`
fn parse_decl(decl: &str, out: &mut BTreeMap<Local, String>) {
    let decl = decl.strip_prefix("mut ").unwrap_or(decl);
    let Some(colon) = lexer::find_type_colon(decl) else {
        return;
    };
    let (Some(name), Some(ty)) = (decl.get(..colon), decl.get(colon + 1..)) else {
        return;
    };
    if let Some(local) = parse_local(name) {
        out.insert(local, ty.trim().trim_end_matches(';').trim().to_owned());
    }
}

fn parse_local(name: &str) -> Option<Local> {
    let digits = name.trim().strip_prefix('_')?;
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse::<u32>().ok().map(Local)
}

// ── places ──────────────────────────────────────────────────────────────────

/// `_N`, `(*_N)`, `(_N.0: Ty)`, `((_N as variant#3).1: Ty)`, `_N[_M]`,
/// `_N[3 of 4]`, and any nesting of those. Projections come out in source
/// order, outermost last: `(*(_1.0: &mut X))` is `[Field(0), Deref]`.
fn parse_place(src: &str) -> Option<Place> {
    parse_place_at(src, 0)
}

fn parse_place_at(src: &str, depth: u32) -> Option<Place> {
    if depth > MAX_PLACE_DEPTH {
        return None;
    }
    let src = src.trim();
    if src.is_empty() {
        return None;
    }
    if src.ends_with(']')
        && let Some((head, inner)) = lexer::trailing_group(src, b'[', b']')
    {
        let mut place = parse_place_at(head, depth + 1)?;
        place.projections.push(index_projection(inner));
        return Some(place);
    }
    if let Some(local) = parse_local(src) {
        return Some(Place {
            local,
            projections: Vec::new(),
        });
    }
    if lexer::is_wrapped(src) {
        let inner = src.get(1..src.len().checked_sub(1)?)?;
        return parse_projection(inner, depth + 1);
    }
    None
}

/// The inside of a parenthesised place: `*P`, `P.N: Ty`, or `P as variant#K`.
fn parse_projection(inner: &str, depth: u32) -> Option<Place> {
    let inner = inner.trim();
    if let Some(pointee) = inner.strip_prefix('*') {
        let mut place = parse_place_at(pointee, depth + 1)?;
        place.projections.push(Projection::Deref);
        return Some(place);
    }
    if let Some(dot) = lexer::find_top(inner, ".") {
        let base = inner.get(..dot)?;
        let after = inner.get(dot + 1..)?;
        let end = after
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(after.len());
        let field: u32 = after.get(..end)?.parse().ok()?;
        let mut place = parse_place_at(base, depth + 1)?;
        place.projections.push(Projection::Field(field));
        return Some(place);
    }
    if let Some(at) = lexer::find_top(inner, " as ") {
        let base = inner.get(..at)?;
        let variant = inner.get(at + 4..)?.trim();
        let mut place = parse_place_at(base, depth + 1)?;
        place
            .projections
            .push(Projection::Downcast(variant.to_owned()));
        return Some(place);
    }
    parse_place_at(inner, depth + 1)
}

fn index_projection(inner: &str) -> Projection {
    let inner = inner.trim();
    if parse_local(inner).is_some() {
        Projection::Index
    } else {
        Projection::Other(inner.to_owned())
    }
}

// ── operands ────────────────────────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq, Eq)]
enum OperandKind {
    Copy,
    Move,
    Const,
}

fn parse_operand(src: &str) -> Operand {
    let src = src.trim().trim_start_matches('!').trim();
    if let Some(place) = src.strip_prefix("copy ").and_then(parse_place) {
        return Operand::Copy(place);
    }
    if let Some(place) = src.strip_prefix("move ").and_then(parse_place) {
        return Operand::Move(place);
    }
    const_operand(src)
}

fn const_operand(src: &str) -> Operand {
    let body = src.trim().to_owned();
    Operand::Const {
        alloc: alloc_id(&body),
        closure: closure_span(&body),
        text: body,
    }
}

/// `const {alloc1: &AtomicU64}` → `alloc1` (the `allocN` footer key).
fn alloc_id(src: &str) -> Option<String> {
    let at = src.find("{alloc")?;
    let after = src.get(at + 6..)?;
    let end = after
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(after.len());
    if end == 0 {
        return None;
    }
    Some(format!("alloc{}", after.get(..end)?))
}

/// `const ZeroSized: {closure@f.rs:104:32: 104:35}` → `f.rs:104:32: 104:35`.
fn closure_span(src: &str) -> Option<String> {
    let at = src.find("{closure@")?;
    let close = lexer::match_at(src, at)?;
    Some(src.get(at + 9..close)?.to_owned())
}

/// Every `copy`/`move`/`const` operand anywhere in an rvalue, at any nesting
/// depth: tuple and array aggregates, adt/closure/coroutine aggregates,
/// binops, casts and `ShallowInitBox` all read through this.
fn collect_operands(src: &str) -> Vec<Operand> {
    let bytes = src.as_bytes();
    let mut hits: Vec<(usize, u32, OperandKind)> = Vec::new();
    lexer::walk(src, |i, c, depth| {
        if c != b'c' && c != b'm' {
            return true;
        }
        let Some(tail) = src.get(i..) else {
            return true;
        };
        let kind = if tail.starts_with("copy ") {
            OperandKind::Copy
        } else if tail.starts_with("move ") {
            OperandKind::Move
        } else if tail.starts_with("const ") {
            OperandKind::Const
        } else {
            return true;
        };
        // A keyword, not the tail of `remove`/`*const`/`copy_from_slice`.
        let boundary = i == 0
            || matches!(
                bytes.get(i.wrapping_sub(1)),
                Some(b' ' | b'\t' | b'(' | b'[' | b'{' | b',')
            );
        if boundary {
            hits.push((i, depth, kind));
        }
        true
    });

    let mut out = Vec::new();
    let mut consumed = 0_usize;
    for (at, depth, kind) in hits {
        if at < consumed {
            continue;
        }
        if kind == OperandKind::Const {
            let end = const_end(src, at + 6, depth);
            if let Some(chunk) = src.get(at..end) {
                out.push(const_operand(chunk));
                consumed = end;
            }
            continue;
        }
        let Some((end, token)) = place_token(src, at + 5) else {
            continue;
        };
        let Some(place) = parse_place(token) else {
            continue;
        };
        out.push(if kind == OperandKind::Copy {
            Operand::Copy(place)
        } else {
            Operand::Move(place)
        });
        consumed = end;
    }
    out
}

/// The place token starting at `from`: `_N`, `(..)`, plus any `[..]` suffixes.
fn place_token(src: &str, from: usize) -> Option<(usize, &str)> {
    let bytes = src.as_bytes();
    let mut i = from;
    while bytes.get(i) == Some(&b' ') {
        i += 1;
    }
    let start = i;
    match bytes.get(i)? {
        b'(' => i = lexer::match_at(src, i)?.checked_add(1)?,
        b'_' => {
            i += 1;
            while bytes.get(i).is_some_and(u8::is_ascii_digit) {
                i += 1;
            }
            if i == start + 1 {
                return None;
            }
        }
        _ => return None,
    }
    while bytes.get(i) == Some(&b'[') {
        i = lexer::match_at(src, i)?.checked_add(1)?;
    }
    Some((i, src.get(start..i)?))
}

/// A `const` operand runs to the next separator at its own nesting depth.
fn const_end(src: &str, from: usize, depth: u32) -> usize {
    let mut end = src.len();
    lexer::walk(src, |i, c, d| {
        if i < from {
            return true;
        }
        if d < depth || (d == depth && c == b',') {
            end = i;
            return false;
        }
        true
    });
    end
}

// ── statements and rvalues ──────────────────────────────────────────────────

fn parse_statement(src: &str) -> Statement {
    let body = src.trim().trim_end_matches(';').trim();
    if let Some(at) = lexer::find_top(body, " = ")
        && let (Some(lhs), Some(rhs)) = (body.get(..at), body.get(at + 3..))
        && let Some(dest) = parse_place(lhs)
    {
        return Statement::Assign {
            dest,
            rvalue: parse_rvalue(rhs),
        };
    }
    // `StorageLive`, `StorageDead`, `FakeRead`, `PlaceMention`, `nop`,
    // `ConstEvalCounter`, `Retag`, `Deinit`, `discriminant(P) = K`, ...
    Statement::Other(src.trim().to_owned())
}

fn parse_rvalue(src: &str) -> Rvalue {
    let body = src.trim();
    let mut rvalue = Rvalue {
        text: body.to_owned(),
        reads: Vec::new(),
        discriminant_of: None,
        ref_of: None,
        unsize_to: None,
        static_alloc: None,
    };
    if let Some(referent) = body.strip_prefix('&') {
        let (is_mut, place_src) = borrow_parts(referent);
        if let Some(place) = parse_place(place_src) {
            rvalue.reads.push(Operand::Copy(place.clone()));
            rvalue.ref_of = Some((place, is_mut));
        }
        return rvalue;
    }
    if let Some(inner) = call_like(body, "discriminant") {
        if let Some(place) = parse_place(inner) {
            rvalue.reads.push(Operand::Copy(place.clone()));
            rvalue.discriminant_of = Some(place);
        }
        return rvalue;
    }
    for form in ["Len", "CopyForDeref"] {
        if let Some(inner) = call_like(body, form) {
            if let Some(place) = parse_place(inner) {
                rvalue.reads.push(Operand::Copy(place));
            }
            return rvalue;
        }
    }
    rvalue.unsize_to = unsize_target(body);
    rvalue.reads = collect_operands(body);
    if body.starts_with("const {") {
        rvalue.static_alloc = alloc_id(body);
    }
    rvalue
}

/// `&P`, `&mut P`, `&raw const P`, `&raw mut P`, `&/*tls*/ PATH`.
fn borrow_parts(referent: &str) -> (bool, &str) {
    let referent = strip_block_comment(referent);
    if let Some(place) = referent.strip_prefix("raw const ") {
        return (false, place);
    }
    if let Some(place) = referent.strip_prefix("raw mut ") {
        return (true, place);
    }
    referent
        .strip_prefix("mut ")
        .map_or((false, referent), |place| (true, place))
}

fn strip_block_comment(src: &str) -> &str {
    let src = src.trim_start();
    let Some(after) = src.strip_prefix("/*") else {
        return src;
    };
    after
        .find("*/")
        .and_then(|at| after.get(at + 2..))
        .map_or(src, str::trim_start)
}

/// `NAME(INNER)` covering the whole of `src` → `INNER`.
fn call_like<'a>(src: &'a str, name: &str) -> Option<&'a str> {
    let (head, inner) = lexer::trailing_group(src, b'(', b')')?;
    (head.trim() == name).then_some(inner)
}

/// `OPERAND as TY (PointerCoercion(Unsize, ..))` → `TY` (the RTA-lite input).
fn unsize_target(src: &str) -> Option<String> {
    let at = lexer::find_top(src, " as ")?;
    let cast = src.get(at + 4..)?;
    let (ty, kind) = lexer::trailing_group(cast, b'(', b')')?;
    kind.starts_with("PointerCoercion(Unsize")
        .then(|| ty.trim().to_owned())
}

// ── terminators ─────────────────────────────────────────────────────────────

/// `(key, target)` pairs of a `-> [return: bb1, unwind: bb2]` spec; the key is
/// `None` for the short `-> bbN` form.
type Entries<'a> = Vec<(Option<&'a str>, &'a str)>;

/// Terminator heads that are never calls even though they look like one.
const NON_CALL_HEADS: &[&str] = &[
    "resume",
    "abort",
    "terminate",
    "coroutine_drop",
    "falseEdge",
    "falseUnwind",
    "unwind",
    "yield",
    "return",
    "unreachable",
];

fn parse_terminator(src: &str) -> Terminator {
    let full = src.trim().trim_end_matches(';').trim();
    let (head, entries) = split_targets(full);
    let head = head.trim();
    if head == "return" {
        return Terminator::Return;
    }
    if head == "unreachable" {
        return Terminator::Unreachable;
    }
    if head == "goto" {
        return first_bb(&entries).map_or_else(
            || other_terminator(full, &entries),
            |target| Terminator::Goto { target },
        );
    }
    if let Some(inner) = call_like(head, "switchInt") {
        return Terminator::SwitchInt {
            operand: parse_operand(inner),
            targets: cfg_targets(&entries),
        };
    }
    if let Some(inner) = call_like(head, "drop") {
        if let (Some(place), Some(target)) = (parse_place(inner), entry_target(&entries, "return"))
        {
            return Terminator::Drop {
                place,
                target,
                unwind: unwind_target(&entries),
            };
        }
        return other_terminator(full, &entries);
    }
    if let Some(inner) = call_like(head, "assert") {
        if let Some(target) = entry_target(&entries, "success") {
            let condition = lexer::split_top(inner, b',')
                .first()
                .copied()
                .unwrap_or(inner);
            return Terminator::Assert {
                operand: parse_operand(condition),
                target,
                unwind: unwind_target(&entries),
            };
        }
        return other_terminator(full, &entries);
    }
    if head.starts_with("asm!") || head.starts_with("asm(") {
        return Terminator::InlineAsm {
            targets: cfg_targets(&entries),
        };
    }
    parse_call(full, head, &entries)
}

fn parse_call(full: &str, head: &str, entries: &Entries<'_>) -> Terminator {
    let (dest_src, call_src) = lexer::find_top(head, " = ").map_or(("", head), |at| {
        (
            head.get(..at).unwrap_or(""),
            head.get(at + 3..).unwrap_or(head),
        )
    });
    let call_src = call_src.trim();
    let name_end = call_src.find(['(', ' ']).unwrap_or(call_src.len());
    if call_src
        .get(..name_end)
        .is_some_and(|name| NON_CALL_HEADS.contains(&name))
    {
        return other_terminator(full, entries);
    }
    let Some((callee_src, args_src)) = lexer::trailing_group(call_src, b'(', b')') else {
        return other_terminator(full, entries);
    };
    let callee_src = callee_src.trim();
    if callee_src.is_empty() {
        return other_terminator(full, entries);
    }
    let indirect = (callee_src.starts_with("copy ") || callee_src.starts_with("move "))
        .then(|| parse_operand(callee_src));
    let callee = if indirect.is_some() {
        None
    } else {
        Some(callee_src.to_owned())
    };
    Terminator::Call {
        dest: parse_place(dest_src).unwrap_or(Place {
            local: Local(0),
            projections: Vec::new(),
        }),
        dest_ty: place_annotation(dest_src),
        callee,
        indirect,
        args: lexer::split_top(args_src, b',')
            .into_iter()
            .filter(|arg| !arg.trim().is_empty())
            .map(parse_operand)
            .collect(),
        target: entry_target(entries, "return"),
        unwind: unwind_target(entries),
    }
}

/// `(((*_55) as variant#3).1: std::cell::RefCell<u64>)` → the type after the
/// last top-level `: `.
///
/// MIR annotates a *projected* place with the type of the field it names. A
/// plain local (`_7`) carries no annotation, because its `let` declaration
/// already has one.
fn place_annotation(dest_src: &str) -> Option<String> {
    let trimmed = dest_src.trim();
    let inner = trimmed.strip_prefix('(')?.strip_suffix(')')?;
    let at = lexer::rfind_top(inner, ": ")?;
    let ty = inner.get(at.saturating_add(2)..)?.trim();
    (!ty.is_empty()).then(|| ty.to_owned())
}

fn other_terminator(full: &str, entries: &Entries<'_>) -> Terminator {
    Terminator::Other {
        text: full.to_owned(),
        targets: cfg_targets(entries),
    }
}

/// Splits `HEAD -> [k: v, ..]`, `HEAD -> bbN` and `HEAD -> unwind ..`.
fn split_targets(src: &str) -> (&str, Entries<'_>) {
    if src.ends_with(']')
        && let Some((head, inner)) = lexer::trailing_group(src, b'[', b']')
        && let Some(before) = head.trim_end().strip_suffix("->")
    {
        let entries = lexer::split_top(inner, b',')
            .into_iter()
            .map(target_entry)
            .collect();
        return (before.trim_end(), entries);
    }
    if let Some(at) = lexer::rfind_top(src, "->")
        && let (Some(head), Some(tail)) = (src.get(..at), src.get(at + 2..))
    {
        let tail = tail.trim();
        if is_bb(tail) {
            return (head.trim_end(), vec![(None, tail)]);
        }
        if tail.starts_with("unwind") {
            return (head.trim_end(), vec![target_entry(tail)]);
        }
    }
    (src, Vec::new())
}

fn target_entry(field: &str) -> (Option<&str>, &str) {
    let field = field.trim();
    if let Some(colon) = lexer::find_type_colon(field)
        && let (Some(key), Some(value)) = (field.get(..colon), field.get(colon + 1..))
    {
        return (Some(key.trim()), value.trim());
    }
    field
        .split_once(' ')
        .map_or((None, field), |(key, value)| (Some(key), value.trim()))
}

fn is_bb(label: &str) -> bool {
    label
        .strip_prefix("bb")
        .is_some_and(|digits| !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()))
}

fn entry_target(entries: &Entries<'_>, key: &str) -> Option<String> {
    entries.iter().find_map(|(k, value)| {
        (k.is_none_or(|k| k == key) && is_bb(value)).then(|| (*value).to_owned())
    })
}

fn unwind_target(entries: &Entries<'_>) -> Option<String> {
    entries
        .iter()
        .find_map(|(k, value)| (*k == Some("unwind") && is_bb(value)).then(|| (*value).to_owned()))
}

fn first_bb(entries: &Entries<'_>) -> Option<String> {
    entries
        .iter()
        .find_map(|(_, value)| is_bb(value).then(|| (*value).to_owned()))
}

/// Every successor that is a real CFG edge — `unwind:` edges are excluded (D5).
fn cfg_targets(entries: &Entries<'_>) -> Vec<String> {
    entries
        .iter()
        .filter(|(k, value)| *k != Some("unwind") && is_bb(value))
        .map(|(_, value)| (*value).to_owned())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parses a one-body dump and asserts it was clean.
    fn one_body(src: &str) -> Body {
        let doc = parse("c", "t.mir", src);
        assert!(doc.parse_failures.is_empty(), "{:?}", doc.parse_failures);
        doc.bodies.into_iter().next().expect("one body")
    }

    /// Wraps `blocks` in a minimal `fn f() -> u8` body.
    fn body_with(blocks: &str) -> Body {
        let mut src = String::from("fn f() -> u8 {\n    let mut _0: u8;\n\n");
        src.push_str(blocks);
        src.push_str("}\n");
        one_body(&src)
    }

    fn only_statement(blocks: &str) -> Statement {
        let body = body_with(blocks);
        body.blocks
            .into_iter()
            .next()
            .expect("bb0")
            .statements
            .into_iter()
            .next()
            .expect("a statement")
    }

    fn assigned(blocks: &str) -> (Place, Rvalue) {
        match only_statement(blocks) {
            Statement::Assign { dest, rvalue } => (dest, rvalue),
            Statement::Other(other) => panic!("expected an assignment, got {other}"),
        }
    }

    fn terminator(blocks: &str) -> Terminator {
        body_with(blocks)
            .blocks
            .into_iter()
            .next()
            .expect("bb0")
            .terminator
    }

    fn place(local: u32, projections: &[Projection]) -> Place {
        Place {
            local: Local(local),
            projections: projections.to_vec(),
        }
    }

    // ── places ──────────────────────────────────────────────────────────────

    #[test]
    fn projections_are_outermost_last() {
        assert_eq!(parse_place("_7"), Some(place(7, &[])));
        assert_eq!(parse_place("(*_7)"), Some(place(7, &[Projection::Deref])));
        assert_eq!(
            parse_place("(_7.0: &mut X)"),
            Some(place(7, &[Projection::Field(0)]))
        );
        // `(*(_1.0: &mut X))` is a field read *then* a deref.
        assert_eq!(
            parse_place("(*(_1.0: &mut X))"),
            Some(place(1, &[Projection::Field(0), Projection::Deref]))
        );
        assert_eq!(
            parse_place("(((_21 as Some).0: (std::string::String, u32)).1: u32)"),
            Some(place(
                21,
                &[
                    Projection::Downcast("Some".to_owned()),
                    Projection::Field(0),
                    Projection::Field(1)
                ]
            ))
        );
        assert_eq!(
            parse_place("(((*_61) as variant#3).2: u32)"),
            Some(place(
                61,
                &[
                    Projection::Deref,
                    Projection::Downcast("variant#3".to_owned()),
                    Projection::Field(2)
                ]
            ))
        );
    }

    #[test]
    fn field_type_annotations_with_spans_do_not_confuse_the_split() {
        // The `.` inside `{closure@spike.rs:18:13: 18:21}` is nested, not a field.
        assert_eq!(
            parse_place("(((*_61) as variant#3).1: {closure@spike.rs:18:13: 18:21})"),
            Some(place(
                61,
                &[
                    Projection::Deref,
                    Projection::Downcast("variant#3".to_owned()),
                    Projection::Field(1)
                ]
            ))
        );
    }

    #[test]
    fn index_constant_index_and_subslice_projections() {
        assert_eq!(parse_place("_1[_2]"), Some(place(1, &[Projection::Index])));
        assert_eq!(
            parse_place("_1[3 of 4]"),
            Some(place(1, &[Projection::Other("3 of 4".to_owned())]))
        );
        assert_eq!(
            parse_place("_1[-1 of 2]"),
            Some(place(1, &[Projection::Other("-1 of 2".to_owned())]))
        );
        assert_eq!(
            parse_place("(*_1)[2:-1]"),
            Some(place(
                1,
                &[Projection::Deref, Projection::Other("2:-1".to_owned())]
            ))
        );
    }

    #[test]
    fn non_places_and_hostile_nesting_are_rejected_not_panicked_on() {
        assert_eq!(parse_place(""), None);
        assert_eq!(parse_place("COUNTER"), None);
        assert_eq!(parse_place("(*"), None);
        assert_eq!(parse_place("_"), None);
        assert_eq!(parse_place("_x"), None);
        let deep = "(".repeat(400) + "*_1" + &")".repeat(400);
        assert_eq!(parse_place(&deep), None, "the depth cap must hold");
    }

    // ── rvalues ─────────────────────────────────────────────────────────────

    #[test]
    fn aggregates_report_every_operand_in_order() {
        let (_, tuple) = assigned(
            "    bb0: {\n        _1 = (move _2, copy _3, const 7_u8);\n        return;\n    }\n",
        );
        assert_eq!(
            tuple.reads,
            vec![
                Operand::Move(place(2, &[])),
                Operand::Copy(place(3, &[])),
                Operand::Const {
                    text: "const 7_u8".to_owned(),
                    alloc: None,
                    closure: None
                }
            ]
        );
        let (_, adt) = assigned(
            "    bb0: {\n        _1 = Foo { a: copy _2, b: move (_3.1: u8) };\n        return;\n    }\n",
        );
        assert_eq!(
            adt.reads,
            vec![
                Operand::Copy(place(2, &[])),
                Operand::Move(place(3, &[Projection::Field(1)]))
            ]
        );
    }

    #[test]
    fn keyword_lookalikes_are_not_operands() {
        // `remove`, `copy_from_slice` and `*const` must not read anything.
        let (_, rv) = assigned(
            "    bb0: {\n        _1 = ShallowInitBox(move _2, *const u8);\n        return;\n    }\n",
        );
        assert_eq!(rv.reads, vec![Operand::Move(place(2, &[]))]);
        let (_, cast) = assigned(
            "    bb0: {\n        _1 = copy _2 as *const dyn Src (Transmute);\n        return;\n    }\n",
        );
        assert_eq!(cast.reads, vec![Operand::Copy(place(2, &[]))]);
        assert_eq!(cast.unsize_to, None);
    }

    #[test]
    fn borrows_reborrows_and_raw_pointers() {
        for (src, expect_mut) in [
            ("&_2", false),
            ("&mut _2", true),
            ("&raw const _2", false),
            ("&raw mut _2", true),
        ] {
            let mut line = String::from("    bb0: {\n        _1 = ");
            line.push_str(src);
            line.push_str(";\n        return;\n    }\n");
            let (_, rv) = assigned(&line);
            assert_eq!(rv.ref_of, Some((place(2, &[]), expect_mut)), "{src}");
            assert_eq!(rv.reads, vec![Operand::Copy(place(2, &[]))], "{src}");
        }
        // A thread-local static reference has no place to alias; it must not
        // invent one (the `/*tls*/` comment is skipped).
        let (_, tls) = assigned(
            "    bb0: {\n        _1 = &/*tls*/ TL::{constant#0}::{closure#0}::__RUST_STD_INTERNAL_VAL;\n        return;\n    }\n",
        );
        assert_eq!(tls.ref_of, None);
        assert!(tls.reads.is_empty());
    }

    #[test]
    fn unsize_coercions_and_static_allocs() {
        let (_, unsize) = assigned(
            "    bb0: {\n        _1 = move _2 as std::boxed::Box<dyn Clock> (PointerCoercion(Unsize, Implicit));\n        return;\n    }\n",
        );
        assert_eq!(
            unsize.unsize_to.as_deref(),
            Some("std::boxed::Box<dyn Clock>")
        );
        let (_, alloc) = assigned(
            "    bb0: {\n        _1 = const {alloc12+0x8: &AtomicU64};\n        return;\n    }\n",
        );
        assert_eq!(alloc.static_alloc.as_deref(), Some("alloc12"));
        assert!(matches!(
            alloc.reads.first(),
            Some(Operand::Const { alloc: Some(a), .. }) if a == "alloc12"
        ));
    }

    #[test]
    fn set_discriminant_is_opaque_but_reading_one_is_not() {
        assert!(matches!(
            only_statement(
                "    bb0: {\n        discriminant((*_6)) = 1;\n        return;\n    }\n"
            ),
            Statement::Other(_)
        ));
        let (_, read) =
            assigned("    bb0: {\n        _1 = discriminant((*_6));\n        return;\n    }\n");
        assert_eq!(read.discriminant_of, Some(place(6, &[Projection::Deref])));
    }

    // ── terminators ─────────────────────────────────────────────────────────

    #[test]
    fn call_target_forms() {
        // No return target at all: a diverging call.
        let diverging = terminator(
            "    bb0: {\n        _1 = core::panicking::panic(const \"x\") -> unwind continue;\n    }\n",
        );
        let Terminator::Call {
            target,
            unwind,
            callee,
            ..
        } = &diverging
        else {
            panic!("expected a Call, got {diverging:?}");
        };
        assert_eq!(*target, None);
        assert_eq!(*unwind, None);
        assert_eq!(callee.as_deref(), Some("core::panicking::panic"));

        // Short form.
        let short = terminator("    bb0: {\n        _1 = f() -> bb2;\n    }\n");
        assert!(matches!(&short, Terminator::Call { target: Some(t), .. } if t == "bb2"));

        // Indirect through a moved fn pointer.
        let indirect = terminator(
            "    bb0: {\n        _1 = move _5(copy _2) -> [return: bb1, unwind unreachable];\n    }\n",
        );
        let Terminator::Call {
            callee,
            indirect,
            args,
            ..
        } = &indirect
        else {
            panic!("expected a Call");
        };
        assert_eq!(*callee, None);
        assert_eq!(*indirect, Some(Operand::Move(place(5, &[]))));
        assert_eq!(*args, vec![Operand::Copy(place(2, &[]))]);
    }

    #[test]
    fn a_turbofished_fn_item_argument_survives_the_comma_split() {
        let call = terminator(
            "    bb0: {\n        _1 = LazyStorage::<RefCell<u64>, ()>::get_or_init::<fn() -> RefCell<u64> {init}>(copy _3, copy _2, init) -> [return: bb1, unwind continue];\n    }\n",
        );
        let Terminator::Call { callee, args, .. } = &call else {
            panic!("expected a Call, got {call:?}");
        };
        assert_eq!(
            callee.as_deref(),
            Some("LazyStorage::<RefCell<u64>, ()>::get_or_init::<fn() -> RefCell<u64> {init}>")
        );
        assert_eq!(args.len(), 3);
        assert!(matches!(args.get(2), Some(Operand::Const { text, .. }) if text == "init"));
    }

    #[test]
    fn switch_drop_assert_and_asm() {
        let switch = terminator(
            "    bb0: {\n        switchInt(copy (((*_61) as variant#3).4: bool)) -> [-1: bb1, 0: bb2, otherwise: bb3];\n    }\n",
        );
        assert_eq!(switch.successors(), vec!["bb1", "bb2", "bb3"]);
        assert!(matches!(
            switch,
            Terminator::SwitchInt {
                operand: Operand::Copy(_),
                ..
            }
        ));

        let drop_short = terminator("    bb0: {\n        drop(_5) -> bb1;\n    }\n");
        assert!(
            matches!(&drop_short, Terminator::Drop { target, unwind: None, .. } if target == "bb1")
        );

        let assert_msg = terminator(
            "    bb0: {\n        assert(!move (_3.1: bool), \"compute `{} + {}`, overflow\", copy _1, move _2) -> [success: bb1, unwind: bb9];\n    }\n",
        );
        let Terminator::Assert {
            operand,
            target,
            unwind,
        } = &assert_msg
        else {
            panic!("expected an Assert, got {assert_msg:?}");
        };
        assert_eq!(*operand, Operand::Move(place(3, &[Projection::Field(1)])));
        assert_eq!(target, "bb1");
        assert_eq!(unwind.as_deref(), Some("bb9"));

        let asm = terminator(
            "    bb0: {\n        asm!(\"nop\", options(())) -> [return: bb1, unwind continue];\n    }\n",
        );
        assert_eq!(asm.successors(), vec!["bb1"]);
        assert!(matches!(asm, Terminator::InlineAsm { .. }));
    }

    #[test]
    fn coroutine_and_unwind_terminators_keep_their_cfg_edges() {
        let false_unwind =
            terminator("    bb0: {\n        falseUnwind -> [real: bb1, unwind: bb9];\n    }\n");
        assert_eq!(false_unwind.successors(), vec!["bb1"]);
        let resume = terminator("    bb0: {\n        resume;\n    }\n");
        assert!(matches!(resume, Terminator::Other { .. }));
        assert!(resume.successors().is_empty());
        let terminate = terminator("    bb0: {\n        terminate(cleanup);\n    }\n");
        assert!(matches!(terminate, Terminator::Other { .. }));
        let yielded = terminator(
            "    bb0: {\n        _1 = yield(move _2) -> [resume: bb1, drop: bb2];\n    }\n",
        );
        assert_eq!(yielded.successors(), vec!["bb1", "bb2"]);
    }

    // ── item headers ────────────────────────────────────────────────────────

    #[test]
    fn balanced_paths_in_item_headers() {
        let body = one_body(
            "fn <Foo as Bar<(u8, u8)>>::m::<T>(_1: &Foo, _2: (u8, u8)) -> {async fn body of m()} {\n    let mut _0: u8;\n\n    bb0: {\n        return;\n    }\n}\n",
        );
        assert_eq!(body.path, "<Foo as Bar<(u8, u8)>>::m::<T>");
        assert_eq!(
            body.params,
            vec![
                (Local(1), "&Foo".to_owned()),
                (Local(2), "(u8, u8)".to_owned())
            ]
        );
        assert_eq!(body.return_ty, "{async fn body of m()}");
        assert!(!body.is_const);
    }

    #[test]
    fn statics_constants_and_anonymous_constants() {
        let doc = parse(
            "c",
            "t.mir",
            "static mut RAW: u64 = {\n    let mut _0: u64;\n\n    bb0: {\n        return;\n    }\n}\n\nconst K: LocalKey<u8> = {\n    let mut _0: u8;\n\n    bb0: {\n        return;\n    }\n}\n\nK::{constant#0}: for<'a> fn(Option<&'a mut u8>) -> *const u8 = {\n    let mut _0: u8;\n\n    bb0: {\n        return;\n    }\n}\n\nconst _: () = const ();\n\nalloc4 (static: RAW, size: 8, align: 8) {\n    00 00\n}\n",
        );
        assert!(doc.parse_failures.is_empty(), "{:?}", doc.parse_failures);
        assert_eq!(doc.statics.len(), 1);
        assert!(
            doc.statics
                .first()
                .is_some_and(|s| s.is_mut && s.ty == "u64")
        );
        let paths: Vec<&str> = doc.bodies.iter().map(|b| b.path.as_str()).collect();
        assert_eq!(paths, vec!["K", "K::{constant#0}"]);
        assert!(doc.bodies.iter().all(|b| b.is_const));
        assert_eq!(
            doc.alloc_statics.get("alloc4").map(String::as_str),
            Some("RAW")
        );
    }

    #[test]
    fn scope_nested_declarations_and_debug_bindings_are_flat() {
        let body = one_body(
            "fn f(_1: u8) -> u8 {\n    debug a => _1;\n    let mut _0: u8;\n    scope 1 {\n        debug b => (*_2);\n        let _2: &u8;\n        scope 2 {\n            let mut _3: u8;\n        }\n    }\n\n    bb0: {\n        return;\n    }\n}\n",
        );
        assert_eq!(
            body.locals.len(),
            4,
            "params and scoped lets: {:?}",
            body.locals
        );
        assert_eq!(
            body.debug_names,
            vec![
                ("a".to_owned(), place(1, &[])),
                ("b".to_owned(), place(2, &[Projection::Deref]))
            ]
        );
    }

    #[test]
    fn a_malformed_header_is_recorded_and_the_next_item_still_parses() {
        let doc = parse(
            "c",
            "t.mir",
            "const : = {\n    bb0: {\n        return;\n    }\n}\n\nfn good() -> u8 {\n    let mut _0: u8;\n\n    bb0: {\n        return;\n    }\n}\n",
        );
        assert_eq!(doc.parse_failures.len(), 1);
        assert!(doc.parse_failures.first().is_some_and(|f| f.line == 1));
        assert_eq!(doc.bodies.len(), 1);
        assert!(doc.bodies.first().is_some_and(|b| b.path == "good"));
    }

    #[test]
    fn an_unterminated_body_is_a_parse_failure_not_a_silent_drop() {
        let doc = parse(
            "c",
            "t.mir",
            "fn f() -> u8 {\n    let mut _0: u8;\n\n    bb0: {\n        _0 = const 1_u8;\n",
        );
        assert_eq!(doc.bodies.len(), 0);
        assert_eq!(doc.parse_failures.len(), 1);
        assert!(
            doc.parse_failures
                .first()
                .is_some_and(|f| f.line == 1 && !f.reason.is_empty())
        );
    }
}
