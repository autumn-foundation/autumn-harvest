//! Guards and rejections: whether an empty or a failed body still reaches
//! a success, and the binding scopes that every reader shares.

use std::collections::BTreeSet;
use std::rc::Rc;

use super::carriers::{Carriers, closure_calls, handoffs, wrapped_carriers};
use super::py::{self, brace, paren};
use super::state::{self, B};
use super::symbols::{function_parts, site_in};
use super::syntax::{
    closure_value, condition_keeps, enclosing_block, match_arm, same_scope, scope_rest,
    split_top_level, unconditional,
};
use super::tables::{
    BINDING, ERROR_TOKENS, FREE_CALL, FREE_CALL_HEAD, HELPER_DEPTH, INSPECTIONS, JSON, NOT_CALLS,
    OPTION, PATTERN_LEAD, RESPONSE_TYPE, RESULT, SUCCESS_EXIT, error_or_panic, failure_variants,
};

orc_cache!(PATTERN_SCOPES: (String, String) => Rc<Vec<(usize, usize, usize, usize)>>);
orc_cache!(SHADOWING_LETS: (String, String) => Rc<Vec<(usize, usize)>>);

/// Whether an `if` on `<variable>.is_empty()` lets an empty body skip the
/// parse.
///
/// The test counts when the parse is in the arm that runs for a non-empty
/// body. That is the `else` of `if body.is_empty()`, or the condition or
/// block of `if !body.is_empty()` when its `else` does not reject. A plain
/// `if body.is_empty() { .. }` before the parse also counts when its block
/// returns a success, as `success_value` reads it, and no error. Any other
/// use, such as a log field, is no guard.
pub fn guards(block: &str, position: usize, variable: &str) -> bool {
    let pattern = format!(r"\bif\s+(!\s*)?{}\.is_empty\(\)", py::esc(variable));
    for test in py::finditer(&pattern, py::upto(block, position)) {
        let opener = py::find(block, "{", test.end());
        if opener < 0 {
            continue;
        }
        let opener = py::to_usize(opener);
        let negated = test.get(1).is_some_and(|text| !text.is_empty());
        // The whole condition must hold for every empty body, or for none.
        // `body.is_empty() && x` can skip the arm, and `!body.is_empty() || x`
        // can take it with an empty body. `condition_keeps` decides.
        if !condition_keeps(&block[test.end()..opener], !negated) {
            continue;
        }
        let taken = brace(&block[opener..]);
        let taken_end = opener + taken.len();
        let mut end = taken_end;
        while py::matches(r"\s*else\b", py::from(block, end)) {
            let branch = py::find(block, "{", end);
            if branch < 0 {
                break;
            }
            let branch = py::to_usize(branch);
            end = branch + brace(&block[branch..]).len();
        }
        // A parse in the arm of `if !body.is_empty()` is guarded only when
        // the `else`, which runs for an empty body, lets the request through.
        if negated
            && test.end() < position
            && position < taken_end
            && !rejecting_exit(py::sl(block, taken_end, end))
        {
            return true;
        }
        // The `else` of `if body.is_empty()` is a guard only when the
        // empty-body arm itself lets the request through.
        if !negated && taken_end < position && position < end && !rejecting_exit(taken) {
            return true;
        }
        // An early return counts only for a parse after the whole `if`. A
        // parse inside the empty-body arm runs before that return.
        let early_return = returns_success(&unconditional(taken));
        let after_if = position >= end && same_scope(block, test.start(), position);
        if !negated && after_if && early_return && !py::found(ERROR_TOKENS, taken) {
            return true;
        }
    }
    // A `match` on the same test reads like the `if`. Its empty-body arm must
    // let the request through, and the parse must be in the other arm, or
    // after a `match` whose empty-body arm returns a success.
    for (start, empty_arm, other_arm, end) in empty_matches(block, variable) {
        if start >= position || rejecting_exit(&empty_arm.2) {
            continue;
        }
        if other_arm.0 < position && position < other_arm.1 {
            return true;
        }
        let after = position >= end && same_scope(block, start, position);
        let early = returns_success(&unconditional(&empty_arm.2));
        if after && early && !py::found(ERROR_TOKENS, &empty_arm.2) {
            return true;
        }
    }
    false
}

/// One `match` on `.is_empty()`: its start, empty arm, other arm and end.
pub type EmptyMatch = (usize, (usize, usize, String), (usize, usize), usize);

/// `(start, empty arm, other arm, end)` for each `match` on
/// `<variable>.is_empty()`.
///
/// The empty arm is `(start, end, text)` of the arm that runs for an empty
/// body: `true` for `match body.is_empty()`, `false` for `match
/// !body.is_empty()`. The other arm is its span. Only a match with exactly
/// one `true` arm and one `false` arm, and no guard or catch-all, is read.
/// Any other form is no guard, so the body stays mandatory, which is the
/// fail-closed direction.
pub fn empty_matches(block: &str, variable: &str) -> Vec<EmptyMatch> {
    let mut found = Vec::new();
    let pattern = format!(
        r"\bmatch\s+(!\s*)?{}\.is_empty\(\)\s*\{{",
        py::esc(variable)
    );
    for test in py::finditer(&pattern, block) {
        let opener = test.end() - 1;
        let arms_text = brace(&block[opener..]);
        let mut arms: Vec<(String, (usize, usize, String))> = Vec::new();
        let mut readable = true;
        for arm in py::finditer(r"(?:^|[,{}])\s*([^,{}=]*?)\s*=>", arms_text) {
            let label = arm.g(1).trim();
            if !(label == "true" || label == "false")
                || arms.iter().any(|(known, _)| known == label)
            {
                readable = false;
                break;
            }
            let body_start = opener + arm.end();
            let rest = py::from(block, body_start);
            let lead = py::lead_len(rest);
            let body_end = if rest[lead..].starts_with('{') {
                body_start + lead + brace(&rest[lead..]).len()
            } else {
                let replaced = rest.replace(',', ";");
                let scoped = scope_rest(&replaced);
                body_start + scoped.split(';').next().unwrap_or("").len()
            };
            let text = py::sl(block, body_start, body_end).to_string();
            arms.push((label.to_string(), (body_start, body_end, text)));
        }
        let labels: BTreeSet<&str> = arms.iter().map(|(label, _)| label.as_str()).collect();
        if !readable || labels != BTreeSet::from(["false", "true"]) {
            continue;
        }
        let negated = test.get(1).is_some_and(|text| !text.is_empty());
        let empty = if negated { "false" } else { "true" };
        let other = if empty == "false" { "true" } else { "false" };
        let pick = |label: &str| {
            arms.iter()
                .find(|(known, _)| known == label)
                .map(|(_, arm)| arm.clone())
                .unwrap_or_default()
        };
        let empty_arm = pick(empty);
        let other_arm = pick(other);
        found.push((
            test.start(),
            empty_arm,
            (other_arm.0, other_arm.1),
            opener + arms_text.len(),
        ));
    }
    found
}

/// Whether an empty body can reach a success past the parse at `position`.
///
/// This verdict is kept apart from `tolerant`, which says only that a parse
/// error turns into a value. An `.is_empty()` guard, as `guards` reads it,
/// lets an empty body skip the parse. A tolerant parse lets it through too,
/// unless an earlier empty-body test rejects it, as `rejects_empty` reads
/// it.
///
/// A position in a closure that runs only where it is called, as
/// `closure_calls` finds, is also read at each call when no guard in the
/// closure applies. The strictest call decides.
pub fn empty_allowed(block: B<'_>, position: usize, variable: &str, tolerant: bool) -> bool {
    if guards(&block, position, variable) {
        return true;
    }
    let calls = closure_calls(block, position);
    if !calls.is_empty() {
        return calls
            .into_iter()
            .all(|call| empty_allowed(block, call, variable, tolerant));
    }
    tolerant && !rejects_empty(&block, position, variable)
}

/// Whether an `if` on `<variable>.is_empty()` rejects every empty body
/// before `position`.
///
/// The arm that runs for an empty body is the block of `if body.is_empty()`,
/// or the `else` of `if !body.is_empty()`, when `condition_keeps` holds for
/// the whole condition. It counts when that arm rejects, as
/// `rejecting_exit` reads it, and the parse is in the other arm or after the
/// whole `if` in the same scope.
pub fn rejects_empty(block: &str, position: usize, variable: &str) -> bool {
    let pattern = format!(r"\bif\s+(!\s*)?{}\.is_empty\(\)", py::esc(variable));
    for test in py::finditer(&pattern, py::upto(block, position)) {
        let opener = py::find(block, "{", test.end());
        let negated = test.get(1).is_some_and(|text| !text.is_empty());
        if opener < 0 {
            continue;
        }
        let opener = py::to_usize(opener);
        if !condition_keeps(&block[test.end()..opener], !negated) {
            continue;
        }
        let taken = brace(&block[opener..]);
        let taken_end = opener + taken.len();
        let mut end = taken_end;
        let mut otherwise = None;
        if py::matches(r"\s*else\s*\{", py::from(block, end)) {
            let branch = py::to_usize(py::find(block, "{", end));
            let group = brace(&block[branch..]);
            end = branch + group.len();
            otherwise = Some(group);
        }
        let (empty_arm, other_arm) = if negated {
            (otherwise, (opener, taken_end))
        } else {
            (Some(taken), (taken_end, end))
        };
        let Some(empty_arm) = empty_arm else {
            continue;
        };
        if !rejecting_exit(empty_arm) {
            continue;
        }
        let in_other = other_arm.0 < position && position < other_arm.1;
        let after_if = position >= end && same_scope(block, test.start(), position);
        if in_other || after_if {
            return true;
        }
    }
    for (start, empty_arm, other_arm, end) in empty_matches(block, variable) {
        if start >= position || !rejecting_exit(&empty_arm.2) {
            continue;
        }
        let in_other = other_arm.0 < position && position < other_arm.1;
        if in_other || (position >= end && same_scope(block, start, position)) {
            return true;
        }
    }
    false
}

/// Whether a `Result<Json<T>, _>` body is mandatory, since its error
/// rejects.
///
/// The body is mandatory when a method chain on it ends in `?`, or reaches
/// `.unwrap()` or `.expect(..)`, before any call that turns the error into
/// a value. It is also mandatory when an `if` on `.is_err()` or `.is_ok()`
/// sends the error to a rejection, or when any `Err` or catch-all arm of a
/// `match` on it rejects. So does an `if let Err(..) = body` block that
/// rejects, and the `else` of a `let Ok(..) = body else` or an `if let
/// Ok(..) = body` that rejects, as `rejecting_exit` reads it. It is
/// mandatory too when the handler returns it, or a chain on it that keeps
/// the error, as its value. An `Err` arm that hands the request on, for
/// example to replay a committed key, leaves it optional.
pub fn rejects_result_body(params: &str, block: B<'_>) -> bool {
    let pattern = format!(r"\b([a-z_][a-z_0-9]*)\s*:\s*{RESULT}<\s*{JSON}<");
    py::search(&pattern, params).is_some_and(|found| error_rejects(found.g(1), block, "result"))
}

/// Whether a wrapped body is mandatory, since its failure rejects.
///
/// It reads `Option<Json<T>>`, `Option<Bytes>` and `Result<Bytes, _>`.
/// `error_rejects` reads each with its failure, `None` or `Err`. So the same
/// forms decide: `ok_or(..)?`, `?`, `.unwrap()`, `.expect(..)`, a failure or
/// catch-all arm that rejects, `if let` on the failure, a let-else on the
/// success, and an `if` on an inspection such as `.is_none()`.
/// `rejects_result_body` reads `Result<Json<T>, _>`.
pub fn rejects_wrapped_body(params: &str, block: B<'_>) -> bool {
    let pattern = format!(r"\b([a-z_][a-z_0-9]*)\s*:\s*{OPTION}<\s*{JSON}<");
    let mut wrapped: Vec<(String, &str)> = Vec::new();
    for option in py::finditer(&pattern, params) {
        let name = option.g(1).to_string();
        if !wrapped.iter().any(|(known, _)| *known == name) {
            wrapped.push((name, "option"));
        }
    }
    for (name, kind) in wrapped_carriers(params) {
        if let Some(entry) = wrapped.iter_mut().find(|(known, _)| *known == name) {
            entry.1 = kind;
        } else {
            wrapped.push((name, kind));
        }
    }
    wrapped
        .iter()
        .any(|(name, kind)| error_rejects(name, block, kind))
}

/// Each name that holds the byte carrier inside the wrapper `name`, with
/// where.
///
/// The success variant, `Ok` or `Some`, binds it in a `let`, `if let`,
/// `while let`, let-else or `match` arm. The wrapper itself is a carrier
/// too. So a success projection such as `body.as_ref().unwrap()` or `body?`
/// reads the inner carrier wherever it is written, as `argument_root` reads
/// it. Each name is bound where its pattern is, so `live_binding` reads it
/// in that scope. A use of `name` that a later binding shadows is left out.
pub fn wrapped_successes(block: &str, name: &str, kind: &str) -> Vec<(String, usize)> {
    let (success, _, _) = failure_variants(kind);
    let variable = py::esc(name);
    let inner = format!(r"{PATTERN_LEAD}{success}\s*\(\s*{BINDING}([a-z_][a-z_0-9]*)\s*\)");
    let mut found: Vec<(String, usize)> = Vec::new();
    let add = |found: &mut Vec<(String, usize)>, bound: &str, at: usize| {
        if !found.iter().any(|(known, _)| known == bound) {
            found.push((bound.to_string(), at));
        }
    };
    let held =
        format!(r"\b(?:if\s+|while\s+)?let\s+{inner}\s*=\s*(?:&\s*(?:mut\s+)?)?{variable}\b");
    for hit in py::finditer(&held, block) {
        if live_binding(block, name, hit.end(), -1) {
            add(&mut found, hit.g(1), py::to_usize(hit.start_of(1)));
        }
    }
    let scrutinee = format!(r"\bmatch\s+(?:&\s*(?:mut\s+)?)?{variable}\s*\{{");
    for found_match in py::finditer(&scrutinee, block) {
        if !live_binding(block, name, found_match.start(), -1) {
            continue;
        }
        let arms = brace(&block[found_match.end() - 1..]);
        let offset = found_match.end() - 1;
        for arm in py::finditer(&format!(r"{inner}\s*(?:if\b[^{{}}]*?)?=>"), arms) {
            add(&mut found, arm.g(1), offset + py::to_usize(arm.start_of(1)));
        }
    }
    found
}

/// Whether the handler rejects the failure of the extractor `name`.
///
/// `kind` is `"result"` for a `Result` extractor, whose failure is `Err`, or
/// `"option"` for an `Option` one, whose failure is `None`. Every form below
/// reads the failure variant of that kind, as `FAILURE_VARIANTS` names it.
pub fn error_rejects(name: &str, block: B<'_>, kind: &str) -> bool {
    error_rejects_from(name, block, &BTreeSet::new(), -1, &BTreeSet::new(), kind)
}

/// `error_rejects` with the names already followed, where `name` is bound,
/// and the helpers already on the chain.
///
/// `rejects_result_body` gives the forms it reads. A move into another name,
/// such as `let captured = body;`, is followed. A pattern can read the
/// extractor by value, borrowed, or through `as_ref()` or `as_mut()`.
/// `block` has its comments and literals masked, so a call in them is no
/// use. A later `let` that gives `name` a new value ends the extractor, as
/// `live_binding` reads it. `bound_at` is where a move created `name`.
///
/// A helper that gets the extractor, as `handoffs` reads it, is read the
/// same way, with `path` and `HELPER_DEPTH` as in `carrier_parses`. A helper
/// the audit cannot find or read makes the body mandatory, so the audit
/// fails closed.
pub fn error_rejects_from(
    name: &str,
    block: B<'_>,
    seen: &BTreeSet<String>,
    bound_at: isize,
    path: &BTreeSet<(String, String)>,
    kind: &str,
) -> bool {
    let variable = py::esc(name);
    let (success, _, failure) = failure_variants(kind);
    let live = |position: usize| live_binding(&block, name, position, bound_at);
    let moved = format!(r"\blet\s+(?:mut\s+)?([a-z_][a-z_0-9]*)\s*(?::[^=;]*)?=\s*{variable}\s*;");
    let mut seen_more = seen.clone();
    seen_more.insert(name.to_string());
    for alias in py::finditer(&moved, &block) {
        if !seen_more.contains(alias.g(1))
            && live(alias.start())
            && error_rejects_from(
                alias.g(1),
                block,
                &seen_more,
                py::to_isize(alias.start()),
                path,
                kind,
            )
        {
            return true;
        }
    }
    let borrow = r"(?:&\s*(?:mut\s+)?)?";
    let method = r"(?:\s*\.\s*as_(?:ref|mut)\s*\(\s*\))?";
    // The extractor as a pattern reads it: by value, borrowed or through
    // `as_ref`.
    let read = format!("{borrow}{variable}{method}");
    for found in py::finditer(&format!(r"(?<![.\w]){variable}\b"), &block) {
        if !live(found.start()) || binds_name(&block, found.start(), found.end()) {
            continue;
        }
        let (mut verdict, inspection, rest) = walk_chain(py::from(&block, found.end()), kind);
        let before = py::upto(&block, found.start());
        // A chain that still holds the error and is the value of the block
        // hands that error to the caller, so it rejects like `?` does.
        if verdict == "open" && yields_block_value(before, rest) {
            verdict = "reject";
        }
        // Any other use that still holds the error is strict, unless it is a
        // form that another part of this function reads.
        if verdict == "open" && !read_elsewhere(block, found.start(), rest) {
            verdict = "reject";
        }
        if verdict == "reject" && !error_exits_before(&block, &variable, &read, found.start()) {
            return true;
        }
        if let Some(inspection) = inspection
            && inspection_rejects(before, inspection, rest)
        {
            return true;
        }
    }
    let scrutinee = format!(r"\bmatch\s+{read}\s*\{{");
    for found in py::finditer(&scrutinee, &block) {
        if !live(found.start()) {
            continue;
        }
        let arms = brace(&block[found.end() - 1..]);
        for (start, bound) in error_arms(arms, kind) {
            if arm_rejects(match_arm(arms, start), bound.as_deref()) {
                return true;
            }
        }
    }
    let held = format!(r"\blet\s+{PATTERN_LEAD}{success}\s*\([^;=]*?\)\s*=\s*{read}\s*else\s*\{{");
    for binding in py::finditer(&held, &block) {
        let otherwise = brace(&block[binding.end() - 1..]);
        if live(binding.start()) && rejecting_exit(otherwise) {
            return true;
        }
    }
    // After `let Err(e) = body else { .. };`, the rest of the scope runs only
    // on failure, so it is the failure arm.
    let bound_err = format!(r"\blet\s+{PATTERN_LEAD}{failure}[^=]*=\s*{read}\s*else\s*\{{");
    for binding in py::finditer(&bound_err, &block) {
        let otherwise = brace(&block[binding.end() - 1..]);
        let rest = py::from(&block, binding.end() - 1 + otherwise.len())
            .trim_start()
            .trim_start_matches(';');
        if live(binding.start()) && arm_rejects(scope_rest(rest), binding.get(1)) {
            return true;
        }
    }
    let failed = format!(r"\bif\s+let\s+{PATTERN_LEAD}{failure}[^=]*=\s*{read}\s*\{{");
    for tested in py::finditer(&failed, &block) {
        let taken = brace(&block[tested.end() - 1..]);
        if !live(tested.start()) {
            continue;
        }
        if rejecting_exit(taken) || rejecting_arm(taken, tested.get(1)) {
            return true;
        }
    }
    let kept = format!(r"\bif\s+let\s+{PATTERN_LEAD}{success}\s*\([^;=]*?\)\s*=\s*{read}\s*\{{");
    for tested in py::finditer(&kept, &block) {
        let taken = brace(&block[tested.end() - 1..]);
        let rest = py::from(&block, tested.end() - 1 + taken.len());
        if live(tested.start()) && py::matches(r"\s*else\s*\{", rest) {
            let opener = rest.find('{').unwrap_or(0);
            let otherwise = brace(&rest[opener..]);
            if rejecting_exit(otherwise) {
                return true;
            }
        }
    }
    let carriers = Carriers::plain(vec![(name.to_string(), bound_at)]);
    for handoff in handoffs(&state::source(), block, &carriers, false, "") {
        let (Some(parts), Some(parameter)) = (&handoff.parts, &handoff.parameter) else {
            return true;
        };
        if path.len() >= HELPER_DEPTH {
            return true;
        }
        let step = (handoff.helper.clone(), parameter.clone());
        if !path.contains(&step) {
            let mut deeper = path.clone();
            deeper.insert(step);
            if error_rejects_from(
                parameter,
                parts.block(),
                &BTreeSet::new(),
                -1,
                &deeper,
                kind,
            ) {
                return true;
            }
        }
    }
    false
}

/// Whether `name` at `position` still holds the value bound at `bound_at`.
///
/// `bound_at` is where a `let` created `name`, or -1 for a parameter. A
/// later `let` that gives `name` a new value ends it for the rest of that
/// scope, as `shadowing_lets` reads it. A pattern that binds `name` ends it
/// in the code that pattern guards, as `pattern_scopes` reads it. The
/// Result-extractor scan and the raw-body carriers both ask this.
pub fn live_binding(block: &str, name: &str, position: usize, bound_at: isize) -> bool {
    if shadowing_lets(block, name).iter().any(|(start, end)| {
        bound_at < py::to_isize(*start) && *end <= position && same_scope(block, *start, position)
    }) {
        return false;
    }
    !pattern_scopes(block, name)
        .iter()
        .any(|(bind, _, scope_start, scope_end)| {
            bound_at < py::to_isize(*bind) && *scope_start <= position && position < *scope_end
        })
}

/// `(start, end, scope start, scope end)` for each pattern that binds
/// `name`.
///
/// A pattern is a `match` arm before `=>`, or the pattern of an `if let`, a
/// `while let` or a destructuring `let`, before its `=`. A plain `let name =
/// ..` is not read here, since `shadowing_lets` reads it. The scope is the
/// arm, the block of the `if let` or `while let`, or the rest of the block
/// after the `let` statement.
pub fn pattern_scopes(block: &str, name: &str) -> Rc<Vec<(usize, usize, usize, usize)>> {
    state::cached(
        &PATTERN_SCOPES,
        (block.to_string(), name.to_string()),
        || Rc::new(pattern_scopes_uncached(block, name)),
    )
}

fn pattern_scopes_uncached(block: &str, name: &str) -> Vec<(usize, usize, usize, usize)> {
    let bytes = block.as_bytes();
    let mut found = Vec::new();
    for hit in py::finditer(&format!(r"(?<![.\w]){}\b", py::esc(name)), block) {
        let tail = py::from(block, hit.end());
        let Some(ahead) = py::rmatch(r"[^;{}]*?(=>|(?<![=!<>])=(?![=>]))", tail) else {
            continue;
        };
        if py::matches(r"\s*:(?!:)", tail) {
            continue;
        }
        let marker = hit.end() + py::to_usize(ahead.start_of(1));
        let after = marker + ahead.g(1).len();
        // A `,` after more closers than openers leaves the pattern, as in an
        // arm value `handle(body),` before the next arm's `=>`.
        let mut depth = 0i64;
        let mut left = false;
        for &char in &bytes[hit.end()..marker] {
            depth += i64::from(char == b'(' || char == b'[');
            depth -= i64::from(char == b')' || char == b']');
            if char == b',' && depth < 0 {
                left = true;
                break;
            }
        }
        if left {
            continue;
        }
        let statement = py::rfind_before(block, ";", hit.start());
        let cut = statement
            .max(py::rfind_before(block, "{", hit.start()))
            .max(py::rfind_before(block, "}", hit.start()))
            + 1;
        let head = py::sl(block, py::to_usize(cut), hit.start());
        if ahead.g(1) == "=>" {
            let rest = py::from(block, after);
            let opener = py::lead_len(rest);
            let end = if rest[opener..].starts_with('{') {
                after + opener + brace(&rest[opener..]).len()
            } else {
                after + scope_rest(&rest.replace(',', ";")).len()
            };
            found.push((hit.start(), hit.end(), after, end));
        } else if py::found(r"\b(?:if|while)\s+let\b", head) {
            let opener = py::find(block, "{", after);
            if opener >= 0 {
                let opener = py::to_usize(opener);
                found.push((
                    hit.start(),
                    hit.end(),
                    opener,
                    opener + brace(&block[opener..]).len(),
                ));
            }
        } else if py::found(r"\blet\b", head) && !py::full(r"\s*let\s+(?:mut\s+)?", head) {
            let rest = scope_rest(py::from(block, after));
            let stop = after + rest.split(';').next().unwrap_or("").len();
            found.push((
                hit.start(),
                hit.end(),
                stop,
                enclosing_block(block, hit.start()).1,
            ));
        }
    }
    found
}

/// `(start, end)` of each `let <name> = ..;` that makes `name` a new value.
///
/// After that statement, `name` in the same scope is no longer the
/// extractor. A value that is `name` itself, or a method chain on it that
/// still holds the error, keeps the extractor. So `let body = body;` and
/// `let body = body.map_err(f);` are no shadow. Any other value, such as
/// `match body { .. }`, is a new value.
pub fn shadowing_lets(block: &str, name: &str) -> Rc<Vec<(usize, usize)>> {
    state::cached(
        &SHADOWING_LETS,
        (block.to_string(), name.to_string()),
        || {
            let variable = py::esc(name);
            let bytes = block.as_bytes();
            let mut found = Vec::new();
            let pattern = format!(r"\blet\s+(?:mut\s+)?{variable}\s*(?::[^=;]*)?=(?!=)");
            for binding in py::finditer(&pattern, block) {
                let mut depth = 0i64;
                let mut end = block.len();
                for (index, &char) in bytes.iter().enumerate().skip(binding.end()) {
                    depth += i64::from(b"([{".contains(&char));
                    depth -= i64::from(b")]}".contains(&char));
                    if depth < 0 || (depth == 0 && char == b';') {
                        end = index;
                        break;
                    }
                }
                let value = py::sl(block, binding.end(), end).trim();
                if let Some(lead) = py::rmatch(&format!(r"&?\s*(?:mut\s+)?{variable}\b"), value) {
                    let (verdict, _, rest) = walk_chain(&value[lead.end()..], "result");
                    if verdict != "tolerate" && rest.trim().is_empty() {
                        continue;
                    }
                }
                found.push((binding.start(), end));
            }
            Rc::new(found)
        },
    )
}

/// Whether the name at `start..end` is bound there, not read.
///
/// That is the pattern of a `let`, as in `let body = ..`, or a field name or
/// a type ascription, as in `body: T`. A field shorthand such as `Payload {
/// body }` still reads the name.
pub fn binds_name(block: &str, start: usize, end: usize) -> bool {
    let bound = py::found(r"\blet\s+(?:mut\s+)?$", py::upto(block, start));
    if bound || py::matches(r"\s*:(?!:)", py::from(block, end)) {
        return true;
    }
    let name = py::sl(block, start, end);
    pattern_scopes(block, name)
        .iter()
        .any(|(bind, _, _, _)| *bind == start)
}

/// Whether an extractor use at `position` is a form `error_rejects` reads.
///
/// `rest` is the text after the use and its method chain. The forms are the
/// whole value of a `match`, an `if let` or a `while let`, a let-else, a
/// move into another name, and a direct argument of a helper that the audit
/// can find. Any other use, such as an argument of a macro or a method, or a
/// field read, is not read. The audit treats it as strict and fails closed.
pub fn read_elsewhere(block: B<'_>, position: usize, rest: &str) -> bool {
    let before = py::upto(&block, position);
    let borrow = r"\s*(?:&\s*(?:mut\s+)?)?$";
    let scrutinee = format!(r"(?:\bmatch|\b(?:if|while)\s+let\s[^=;{{}}]*=){borrow}");
    if py::found(&scrutinee, before) && rest.trim_start().starts_with('{') {
        return true;
    }
    if py::found(&format!(r"\blet\s[^=;{{}}]*={borrow}"), before)
        && py::matches(r"\s*(?:;|else\b)", rest)
    {
        return true;
    }
    let bytes = before.as_bytes();
    let mut depth = 0i64;
    for index in (0..bytes.len()).rev() {
        let char = bytes[index];
        depth += i64::from(b")]}".contains(&char));
        depth -= i64::from(b"([{".contains(&char));
        if depth < 0 {
            if char != b'(' {
                return false;
            }
            let call = py::search(&format!(r"{FREE_CALL_HEAD}\s*$"), &before[..index]);
            let Some(call) = call.filter(|call| !NOT_CALLS.contains(&call.g(1))) else {
                return false;
            };
            let site = site_in(block, index);
            return function_parts(&state::source(), call.g(1), Some(&site)).is_some();
        }
        if depth == 0 && char == b';' {
            return false;
        }
    }
    false
}

/// Whether an expression is the value of its function block.
///
/// The expression is the tail of the block, or the operand of `return`.
/// `before` is the text before the expression, and `rest` the text after
/// it. A tail inside a nested block, such as an `if` arm, is not read.
pub fn yields_block_value(before: &str, rest: &str) -> bool {
    let tail = rest.trim() == "}";
    let returned = py::found(r"\breturn\s*$", before) && py::matches(r"\s*[;}]", rest);
    tail || returned
}

/// Whether an earlier `if` sends the extractor error to a success exit.
///
/// The `if` tests `is_err()`, `!is_ok()` or `let Err(..)`, and its block
/// returns a success on every path. A later `?` or `unwrap` then sees only
/// `Ok`, so it rejects nothing. The `if` must be in the scope of the use.
pub fn error_exits_before(block: &str, variable: &str, read: &str, position: usize) -> bool {
    // The rest of the condition must keep the arm entered on every error, as
    // `condition_keeps` reads it.
    let tested = format!(
        r"\bif\s+(?:!\s*{variable}\s*\.\s*is_ok|{variable}\s*\.\s*is_err)\s*\(\s*\)([^{{]*)\{{"
    );
    let failed = format!(r"\bif\s+let\s+{PATTERN_LEAD}Err\s*\([^=]*=\s*{read}\s*\{{");
    for test in py::finditer(&format!("{tested}|{failed}"), py::upto(block, position)) {
        if let Some(joined) = test.get(1)
            && !condition_keeps(joined, true)
        {
            continue;
        }
        let taken = brace(&block[test.end() - 1..]);
        if test.end() - 1 + taken.len() > position || !same_scope(block, test.start(), position) {
            continue;
        }
        if returns_success(&unconditional(taken)) && !rejecting_exit(taken) {
            return true;
        }
    }
    false
}

/// Whether a fallback block rejects the request.
///
/// It rejects when it builds an error, panics, or returns anything other
/// than a success, as `success_value` reads it. A 4xx or 5xx `StatusCode` is
/// an error token, so it rejects. It also rejects when its value is a call
/// to a helper that can build a rejection, as `builds_rejection` reads it.
pub fn rejecting_exit(block: &str) -> bool {
    if py::found(&error_or_panic(), block) {
        return true;
    }
    for returned in py::findall1(r"\breturn\b\s*([^;}]*)", block) {
        if !success_value(returned) {
            return true;
        }
    }
    builds_rejection(closure_value(block))
}

/// Whether an arm that receives an extractor error rejects the request.
///
/// It rejects as `rejecting_arm` reads it. It also rejects when it exits as
/// `rejecting_exit` reads it, or when its value is a call to a free
/// function. A call that hands the bound error to a helper is the one
/// exception, since such a helper can recover the request, as the start
/// route does (#808). The exception covers that call only. Any other exit of
/// the arm that rejects still makes the arm reject. A type path such as
/// `Gadget::default()` builds a value.
pub fn arm_rejects(arm: &str, bound: Option<&str>) -> bool {
    exits_reject(arm, bound, false)
}

/// Whether a block that receives an error rejects through one of its exits.
///
/// `body` is an `Err` arm, or a fallback closure when `closure` is true. It
/// rejects as `rejecting_arm` reads it, or when it panics. Its exits are
/// each `return` and its tail value. A `return` in an arm leaves the
/// handler, so it must be a success, as `success_value` reads it. A `return`
/// in a closure gives the closure value, so it is read like a tail. A tail
/// rejects when `builds_rejection` reads it as one. A call that only
/// observes the error, such as `observe(error);`, is no exit, so it decides
/// nothing. In an arm, an exit that hands the error to a helper can recover
/// the request, as the start route does (#808). In a closure, the helper's
/// return type decides.
pub fn exits_reject(body: &str, bound: Option<&str>, closure: bool) -> bool {
    if rejecting_arm(body, bound) || py::found(r"\b(?:panic|unreachable|todo)!", body) {
        return true;
    }
    let handed = bound
        .filter(|bound| *bound != "_" && !closure)
        .map(|bound| {
            format!(
                r"(?:return\s+)?[A-Za-z_][\w:]*\s*\([^;]*\b{}\b",
                py::esc(bound)
            )
        });
    for returned in py::findall1(r"\breturn\b\s*([^;}]*)", body) {
        if let Some(handed) = &handed
            && py::matches(handed, returned.trim())
        {
            continue;
        }
        let rejects = if closure {
            builds_rejection(returned.trim())
        } else {
            !success_value(returned)
        };
        if rejects {
            return true;
        }
    }
    let value = closure_value(body);
    let passed = handed
        .as_ref()
        .is_some_and(|handed| py::matches(handed, value));
    !passed && builds_rejection(value)
}

/// Whether an `Err` arm rejects the request.
///
/// It rejects when it builds an error, or when it returns or converts the
/// rejection it binds, such as `rejection.into_response()`. Passing the
/// rejection to a helper, as the start route does to replay a key, is not
/// rejection.
pub fn rejecting_arm(arm: &str, bound: Option<&str>) -> bool {
    if py::found(ERROR_TOKENS, arm) {
        return true;
    }
    let Some(bound) = bound.filter(|bound| *bound != "_") else {
        return false;
    };
    let name = py::esc(bound);
    let returned = format!(r"\breturn\s+(?:Err\(\s*)?{name}\b(?!\s*[,)])");
    let converted = format!(r"\b{name}\s*\.\s*(?:into_response|into)\s*\(\s*\)");
    py::found(&format!("{returned}|{converted}"), arm)
}

/// Where each alternative of the pattern `text[start:end]` begins.
///
/// A leading `&`, `&mut`, `ref` or `ref mut` is skipped. Grouping
/// parentheses are opened, and a top-level `|` splits alternatives, at any
/// depth of grouping. A guard such as `if cond` is not part of the pattern.
pub fn pattern_alternatives(text: &str, start: usize, end: usize) -> Vec<usize> {
    let bytes = text.as_bytes();
    let mut start = start;
    let mut end = end;
    if let Some(lead) = py::rmatch(r"(?:\s|&|\bmut\b|\bref\b)*", py::sl(text, start, end)) {
        start += lead.end();
    }
    let mut depth = 0i64;
    for index in start..end {
        depth += i64::from(b"([{".contains(&bytes[index]));
        depth -= i64::from(b")]}".contains(&bytes[index]));
        if depth == 0
            && py::matches(r"\bif\b", py::sl(text, index, end))
            && !py::is_word(py::byte_at(text, py::to_isize(index) - 1))
        {
            end = index;
            break;
        }
    }
    while end > start && py::is_space(bytes[end - 1]) {
        end -= 1;
    }
    let mut pieces = vec![0];
    depth = 0;
    for index in start..end {
        depth += i64::from(b"([{".contains(&bytes[index]));
        depth -= i64::from(b")]}".contains(&bytes[index]));
        if depth == 0 && bytes[index] == b'|' {
            pieces.push(index + 1 - start);
        }
    }
    if pieces.len() > 1 {
        let mut bounds = pieces.clone();
        bounds.push(end - start + 1);
        return bounds
            .windows(2)
            .flat_map(|pair| pattern_alternatives(text, start + pair[0], start + pair[1] - 1))
            .collect();
    }
    if start < end && bytes[start] == b'(' && paren(&text[start..end]).len() == end - start {
        return pattern_alternatives(text, start + 1, end - 1);
    }
    vec![start]
}

/// Each top-level arm pattern of this `match` that can receive the failure.
///
/// The failure is `Err` for a `Result` and `None` for an `Option`, as
/// `FAILURE_VARIANTS` gives it for `kind`.
///
/// That is an `Err(..)` pattern, a catch-all `_` or binding, or a binding
/// such as `error @ Err(_)`. `pattern_alternatives` reads each arm. So a
/// borrowed `&Err(ref e)`, a grouped `(Err(e))` and an `Ok(_) | Err(e)`
/// alternative all count. A guard is not evaluated, so a rejecting arm
/// counts even when a guard limits it to some errors. Each result is where
/// the pattern starts and the name the arm binds, if any. `arms` includes
/// the outer braces. A guarded arm can fall through to a later arm, so
/// every one is returned.
pub fn error_arms(arms: &str, kind: &str) -> Vec<(usize, Option<String>)> {
    let (_, failure_name, failure_pattern) = failure_variants(kind);
    let catch_all = format!(
        r"{BINDING}([a-z_][a-z_0-9]*)(?=\s*(?:if\b[^{{}}]*?)?=>|\s*@\s*(?:{failure_name}\b|_))"
    );
    let bytes = arms.as_bytes();
    let mut found = Vec::new();
    let mut depth = 0i64;
    let mut arm_start: Option<usize> = None;
    for (index, &char) in bytes.iter().enumerate() {
        if b"([{".contains(&char) {
            depth += 1;
            if depth == 1 {
                arm_start = Some(index + 1);
            }
        } else if b")]}".contains(&char) {
            depth -= 1;
            if depth == 1 && char == b'}' {
                arm_start = Some(index + 1);
            }
        } else if depth == 1 && char == b',' {
            arm_start = Some(index + 1);
        } else if depth == 1 && bytes[index..].starts_with(b"=>") {
            if let Some(start) = arm_start {
                for at in pattern_alternatives(arms, start, index) {
                    let pattern = py::rmatch_at(failure_pattern, arms, at)
                        .or_else(|| py::rmatch_at(&catch_all, arms, at));
                    if let Some(pattern) = pattern {
                        found.push((pattern.start(), pattern.get(1).map(str::to_string)));
                    }
                }
                arm_start = None;
            }
        }
    }
    found
}

/// What a method chain on a `Result` does with its error.
///
/// The chain moves from a `Result` to an `Option` through `.ok()`, back
/// through `.ok_or(..)` or `.ok_or_else(..)`, and to a plain value through
/// an `.unwrap_or*` call. A `?`, `.unwrap()` or `.expect(..)` on a `Result`
/// or an `Option` stops the handler, so the chain is `"reject"`. An
/// inspection such as `.is_ok()` also yields a plain value. An `.or(..)` or
/// `.or_else(..)` whose fallback always returns `Ok(..)` recovers the
/// `Result`, so a later `?` cannot reject. A chain that ends on an `Option`,
/// a value or a recovered `Result` is `"tolerate"`. One that ends on a
/// `Result` is `"open"`, since the code after it decides. Other methods
/// carry the error on.
///
/// It returns the verdict, the last inspection and the text after the
/// chain.
///
/// `start` is `"result"` for a `Result` extractor or `"option"` for an
/// `Option` one. For an `Option`, the chain holds the failure while it is
/// the `Option`, or a `Result` that `ok_or(..)` made from it.
pub fn walk_chain<'a>(
    after: &'a str,
    start: &str,
) -> (&'static str, Option<&'static str>, &'a str) {
    let mut state: &str = if start == "option" {
        "option"
    } else {
        "result"
    };
    let mut inspection: Option<&'static str> = None;
    let holding: &[&str] = if start == "result" {
        &["result"]
    } else {
        &["option", "result"]
    };
    let mut rest = after;
    loop {
        rest = rest.trim_start();
        // `.await` passes the `Result` of an async call through unchanged.
        if let Some(awaited) = py::rmatch(r"\.\s*await\b", rest) {
            rest = &rest[awaited.end()..];
            continue;
        }
        // A `Result` that `or_else` has recovered holds no error, so `?` on
        // it cannot reject.
        if rest.starts_with('?') && state == "recovered" {
            state = "value";
            rest = &rest[1..];
            continue;
        }
        if rest.starts_with('?') {
            return ("reject", inspection, rest);
        }
        let Some(call) = py::rmatch(r"\.\s*([a-z_][a-z_0-9]*)\s*\(", rest) else {
            let verdict = if holding.contains(&state) {
                "open"
            } else {
                "tolerate"
            };
            return (verdict, inspection, rest);
        };
        let method = call.g(1);
        if (method == "unwrap" || method == "expect") && state != "value" && state != "recovered" {
            return ("reject", inspection, rest);
        }
        let opener = call.end() - 1;
        if (method == "or" || method == "or_else")
            && state == "result"
            && always_ok(paren(&rest[opener..]))
        {
            state = "recovered";
        }
        if method == "ok" && state == "result" {
            state = "option";
        } else if (method == "ok_or" || method == "ok_or_else") && state == "option" {
            state = "result";
        } else if let Some(found) = INSPECTIONS.iter().find(|known| **known == method) {
            state = "value";
            inspection = Some(found);
        } else if matches!(
            method,
            "unwrap_or" | "unwrap_or_default" | "unwrap_or_else" | "map_or" | "map_or_else"
        ) {
            let arguments = paren(&rest[opener..]);
            if (state == "result" || state == "option")
                && fallback_rejects(method, arguments, state)
            {
                return ("reject", inspection, rest);
            }
            state = "value";
        }
        rest = &rest[opener + paren(&rest[opener..]).len()..];
    }
}

/// Whether the fallback of `unwrap_or*` or `map_or*` rejects.
///
/// `state` is `"result"` or `"option"`. The fallback is the first argument.
/// It rejects when it builds an error or panics. A closure is read by its
/// exits, as `exits_reject` reads it, so a call that only observes the error
/// decides nothing. On a `Result`, a function path given to
/// `unwrap_or_else` or `map_or_else` receives the error, so it rejects. On
/// an `Option`, the path's return type decides.
pub fn fallback_rejects(method: &str, arguments: &str, state: &str) -> bool {
    let items = split_top_level(py::sl(arguments, 1, arguments.len().saturating_sub(1)));
    let fallback = items.first().map_or("", String::as_str);
    if py::found(&error_or_panic(), fallback) {
        return true;
    }
    let Some(closure) = py::rmatch(
        r"(?s)(?:move\s+)?\|\s*([a-z_][a-z_0-9]*)?[^|]*\|(.*)",
        fallback,
    ) else {
        if builds_rejection(fallback) {
            return true;
        }
        if !method.ends_with("_else") || !py::full(r"[A-Za-z_][\w:]*", fallback) {
            return false;
        }
        // On an `Option` the path receives no error, so its return type
        // decides.
        return state == "result" || builds_rejection(&format!("{fallback}("));
    };
    // The closure is read by its exits, as `exits_reject` reads an `Err` arm.
    exits_reject(closure.g(2), closure.get(1), true)
}

/// Whether an exit value is a success, not a rejection.
///
/// A value that `SUCCESS_EXIT` matches is a success. Any other value is a
/// success only when it is a helper call that `builds_rejection` rejects as
/// a rejection, such as a helper that returns a bare `Json<T>`. Every exit
/// is read through this one rule.
pub fn success_value(value: &str) -> bool {
    let value = value.trim();
    if py::matches(SUCCESS_EXIT, value) {
        return true;
    }
    py::matches(FREE_CALL, value) && !builds_rejection(value)
}

/// Whether a `return` in `block` gives a success, as `success_value` reads
/// it.
pub fn returns_success(block: &str) -> bool {
    py::findall1(r"\breturn\b\s*([^;}]*)", block)
        .into_iter()
        .any(success_value)
}

/// Whether an expression is a call to a helper that can build a rejection.
///
/// The helper's return type decides. A plain value type such as `Gadget` is
/// no rejection. A response, error or `Result` type is one. A helper the
/// audit cannot find counts as one, so the audit fails closed.
pub fn builds_rejection(value: &str) -> bool {
    let Some(call) = py::rmatch(FREE_CALL, value) else {
        return false;
    };
    let site = site_in(B::plain(value), 0);
    function_parts(&state::source(), call.g(1), Some(&site))
        .is_none_or(|parts| py::found(RESPONSE_TYPE, &parts.returns))
}

/// Whether the fallback of `or` or `or_else` yields `Ok(..)` on every path.
///
/// The fallback is `Ok(..)` itself, or a closure whose value is `Ok(..)`. A
/// branch, a `return`, a `?`, a panic or an error token in it may fail, so
/// it does not count.
pub fn always_ok(arguments: &str) -> bool {
    let fallback = py::sl(arguments, 1, arguments.len().saturating_sub(1)).trim();
    let closure = py::rmatch(r"(?s)(?:move\s+)?\|[^|]*\|(.*)", fallback);
    let value = closure
        .as_ref()
        .map_or(fallback, |closure| closure_value(closure.g(1)));
    let Some(ok) = py::rmatch(r"Ok\s*(?:::<[^()]*>\s*)?\(", value) else {
        return false;
    };
    if paren(&value[ok.end() - 1..]).len() != value.len() - ok.end() + 1 {
        return false;
    }
    let risky = format!(r"{ERROR_TOKENS}|\b(?:if|match|return|panic!|unreachable!|todo!)|\?");
    !py::found(&risky, fallback)
}

/// Whether an `if` on an inspected parse sends a failed parse to a
/// rejection.
///
/// `if parse.is_err() { .. }` runs its block on failure. `if parse.is_ok()`
/// runs its `else` on failure. A leading `!` swaps the two. The parse is
/// mandatory when that failure arm rejects, as `rejecting_exit` reads it.
///
/// A term true on every failure keeps a `||` condition true. A term false on
/// every failure keeps an `&&` condition false, unless a `||` follows. Any
/// other join, or `is_err_and`, whose predicate may be false, leaves no arm
/// certain.
pub fn inspection_rejects(before: &str, inspection: &str, rest: &str) -> bool {
    let condition = py::search(r"\bif\s+(!\s*)?(?:serde_json::)?$", before);
    let opener = rest.find('{');
    let (Some(condition), Some(opener)) = (condition, opener) else {
        return false;
    };
    if inspection == "is_err_and" {
        return false;
    }
    let mut on_failure = inspection == "is_err" || inspection == "is_none";
    if condition.get(1).is_some_and(|text| !text.is_empty()) {
        on_failure = !on_failure;
    }
    if !condition_keeps(&rest[..opener], on_failure) {
        return false;
    }
    let taken = brace(&rest[opener..]);
    let after_taken = &rest[opener + taken.len()..];
    let mut otherwise = None;
    if py::matches(r"\s*else\s*\{", after_taken) {
        let at = after_taken.find('{').unwrap_or(0);
        otherwise = Some(brace(&after_taken[at..]));
    }
    let mut failure_arm = if on_failure { Some(taken) } else { otherwise };
    // With no `else`, a failure falls through past the `if`. When the arm for
    // success always returns, the rest of the scope runs only on failure.
    if failure_arm.is_none() && py::found(r"\breturn\b", &unconditional(taken)) {
        failure_arm = Some(scope_rest(after_taken));
    }
    failure_arm.is_some_and(rejecting_exit)
}

/// Whether a parse turns its error into a value, so an empty body still
/// runs.
///
/// A method chain after the call does so when `walk_chain` ends it on an
/// `Option` or a value, as `.ok()` or `.unwrap_or_default()` does. So does
/// `if let Ok(..) =` before it, unless its `else` returns or builds an
/// error. A `match` on the parse tolerates it when it has an `Err` or
/// catch-all arm and no such arm rejects.
pub fn discards_error(before: &str, after: &str) -> bool {
    let (verdict, inspection, rest) = walk_chain(after, "result");
    if verdict == "tolerate" {
        return !inspection.is_some_and(|inspection| inspection_rejects(before, inspection, rest));
    }
    if let Some(arms) = py::rmatch(r"\s*\{", after)
        && py::found(r"\bmatch\s+(?:serde_json::)?$", before)
    {
        let arms = brace(&after[arms.end() - 1..]);
        let failures = error_arms(arms, "result");
        // Unlike `arm_rejects`, a helper handed the parse error rejects here.
        // The #808 replay exception covers only a `Result` extractor.
        return !failures.is_empty()
            && !failures.iter().any(|(start, bound)| {
                let arm = match_arm(arms, *start);
                rejecting_exit(arm)
                    || rejecting_arm(arm, bound.as_deref())
                    || builds_rejection(closure_value(arm))
            });
    }
    // A standalone `let Ok(..) = parse else { .. }` tolerates the error when
    // its fallback lets the request through. An `if let` has a block, not
    // `else`, right after the call, so it never matches here.
    let bound = r"\blet\s+Ok\s*\([^()]*\)\s*=\s*(?:serde_json::)?$";
    if let Some(fallback) = py::rmatch(r"\s*else\s*\{", after)
        && py::found(bound, before)
    {
        let block = brace(&after[fallback.end() - 1..]);
        return !rejecting_exit(block);
    }
    let tested = r"\b(?:if|while|&&)\s+let\s+Ok\s*\([^()]*\)\s*=\s*(?:serde_json::)?$";
    if !py::found(tested, before) {
        return false;
    }
    let Some(opener) = after.find('{') else {
        return true;
    };
    let rest = &after[opener + brace(&after[opener..]).len()..];
    if !py::matches(r"\s*else\b", rest) {
        return true;
    }
    let at = rest.find('{').unwrap_or(0);
    !rejecting_exit(brace(&rest[at..]))
}

/// Whether the text after a `from_slice(..)` call leaves its type unchanged.
///
/// `?`, `.map_err(..)`, `.unwrap()`, `.unwrap_or_default()` and
/// `.expect(..)` keep the parsed type. The expression must then end, or open
/// a `match` block.
pub fn ends_expression(after: &str) -> bool {
    let mut rest = after;
    loop {
        rest = rest.trim_start();
        if let Some(stripped) = rest.strip_prefix('?') {
            rest = stripped;
            continue;
        }
        let Some(suffix) = py::rmatch(r"\.(?:map_err|expect|unwrap|unwrap_or_default)\s*\(", rest)
        else {
            break;
        };
        let opener = suffix.end() - 1;
        rest = &rest[opener + paren(&rest[opener..]).len()..];
    }
    matches!(rest.as_bytes().first(), Some(b';' | b'}' | b'{'))
}

/// The struct a `from_slice` call yields, or `None` when it is unnamed.
///
/// A typed `let` or a `Result<T, _>` return type names the call only when
/// the call ends its expression, since a `.map(..)` after it yields another
/// type.
pub fn parse_type(
    turbofish: Option<&str>,
    before: &str,
    after: &str,
    returns: &str,
) -> Option<String> {
    if let Some(turbofish) = turbofish.filter(|turbofish| !turbofish.is_empty()) {
        return Some(turbofish.to_string());
    }
    if !ends_expression(after) {
        return None;
    }
    let statement = py::from(before, py::to_usize(py::rfind(before, ";") + 1));
    if let Some(binding) = py::search(
        r"\blet\s+(?:mut\s+)?[a-z_0-9]+\s*:\s*([A-Za-z0-9_:]+)\s*=",
        statement,
    ) {
        return Some(binding.g(1).to_string());
    }
    if py::found(r"\blet\b", statement) {
        return None;
    }
    py::search(r"->\s*Result<\s*([A-Za-z0-9_:]+)\s*,", returns)
        .map(|result| result.g(1).to_string())
}
