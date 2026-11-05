//! The handler scan: the router, the helpers a handler calls, method
//! receivers, and the key arms of a hand-rolled query parser.

use std::collections::BTreeSet;
use std::rc::Rc;

use smop::prelude::*;

use super::carriers::{argument_root, receiver_type};
use super::py::{self, M, balanced, paren};
use super::state::{self, B};
use super::symbols::{function_index, owner_block, path_segments, resolve_fn, site_in};
use super::syntax::matching_opener;
use super::tables::{
    BYTE_RECEIVER_TYPES, FREE_CALL_NAME, GENERIC_HELPERS, KEY_ARM, MACRO_CALL, NOT_CALLS,
    QUALIFIED_CALL, VERBS,
};

/// One macro invocation: its path, start, token tree and end.
pub type Macro = (String, usize, String, usize);

orc_cache!(MACROS: String => Rc<Vec<Macro>>);
orc_cache!(QUALIFIED: String => Rc<Vec<Rc<[Option<(usize, usize)>]>>>);

/// Every `(METHOD, path, handler)` registered in `harvest_api_router`.
pub fn router_routes(source: &str) -> Result<Vec<(String, String, String)>> {
    let start = source
        .find("pub fn harvest_api_router(")
        .context("cannot find `pub fn harvest_api_router(`")?;
    let end = start
        + source[start..]
            .find("\n}\n")
            .context("cannot find the end of `harvest_api_router`")?;
    let router = &source[start..end];
    let verbs = format!(r"\b({})\(\s*([A-Za-z0-9_]+)", VERBS.join("|"));
    let mut routes = Vec::new();
    let mut at = py::find(router, ".route(", 0);
    while at >= 0 {
        let at_index = py::to_usize(at);
        let args = paren(&router[at_index + ".route(".len() - 1..]);
        if let Some(path) = py::search(r#""([^"]+)""#, args) {
            for verb in py::finditer(&verbs, args) {
                routes.push((
                    verb.g(1).to_uppercase(),
                    path.g(1).to_string(),
                    verb.g(2).to_string(),
                ));
            }
        }
        at = py::find(router, ".route(", at_index + ".route(".len());
    }
    Ok(routes)
}

/// Where each function in this file that `body` calls is defined.
///
/// Every call is read: bare, path-qualified and method calls, each from its
/// own site. A method call reaches what `method_targets` finds. A call that
/// can reach more than one definition reads all of them. So no status a
/// helper can return is dropped.
pub fn called_helpers(body: B<'_>, params: &str) -> Vec<usize> {
    let mut starts: BTreeSet<usize> = BTreeSet::new();
    for call in py::finditer(FREE_CALL_NAME, &body) {
        let name = call.g(1);
        if !GENERIC_HELPERS.contains(&name) && !NOT_CALLS.contains(&name) {
            let site = site_in(body, call.start());
            starts.extend(resolve_fn(name, Some(&site)).unwrap_or_default());
        }
    }
    for call in qualified_calls(&body) {
        if GENERIC_HELPERS.contains(&call.g(2)) {
            continue;
        }
        if call.g(1).trim() == "." {
            let receiver = call_receiver(&body, call.start());
            let targets =
                method_targets(body, params, receiver.as_deref(), call.g(2), call.start());
            // A receiver of unknown type reads every method of that name. This
            // is deliberate: a status is never dropped for want of a type.
            let targets = targets.unwrap_or_else(|| {
                resolve_fn(&format!(".{}", call.g(2)), None).unwrap_or_default()
            });
            starts.extend(targets);
            continue;
        }
        let reference = format!("{}{}", call.g(1), call.g(2));
        let site = site_in(body, call.start());
        starts.extend(resolve_fn(&reference, Some(&site)).unwrap_or_default());
    }
    starts.into_iter().collect()
}

/// The methods that a call `receiver.method(..)` at `at` in `block` reaches,
/// or `None`.
///
/// `receiver_type` reads the receiver's written type from `block` or
/// `params`, and the call reaches that type's methods only. A raw body as
/// the receiver has its parameter's type, such as `Bytes`, `Vec` or `[u8]`.
/// So it reaches only a method of an `impl` for that type. A `self`
/// receiver reaches the methods of the enclosing `impl`. `None` means the
/// receiver has no type the audit can know. The status scan, the handoff
/// scan and the raw-body receiver scan all read method calls through this
/// one step.
pub fn method_targets(
    block: B<'_>,
    params: &str,
    receiver: Option<&str>,
    method: &str,
    at: usize,
) -> Option<Vec<usize>> {
    let receiver = receiver.filter(|receiver| !receiver.is_empty())?;
    let typed = receiver_type(block, params, receiver, at)?;
    let site = site_in(block, at);
    Some(resolve_fn(&format!("{typed}::{method}"), Some(&site)).unwrap_or_default())
}

/// The local methods named `method` that a carrier `receiver` at `at` may
/// run.
///
/// A method of an `impl` for the carrier's type counts, as `receiver_type`
/// reads that type. A carrier of unknown type may have any byte type in
/// `BYTE_RECEIVER_TYPES`. A default method of a local trait counts, and so
/// does a method of a blanket `impl` for a type parameter. Each may run on
/// the carrier. A method of an `impl` for any other type does not.
pub fn local_methods(
    block: B<'_>,
    params: &str,
    receiver: &str,
    method: &str,
    at: usize,
) -> Vec<usize> {
    let typed = receiver_type(block, params, receiver, at);
    let types: Vec<String> = match typed.as_deref().map(path_segments) {
        Some(segments) if !segments.is_empty() => vec![segments[segments.len() - 1].clone()],
        _ => BYTE_RECEIVER_TYPES
            .iter()
            .map(|name| (*name).to_string())
            .collect(),
    };
    let source = state::source();
    let mut found = Vec::new();
    for def in function_index(&source).get(method).into_iter().flatten() {
        let Some(owner) = &def.owner else {
            continue;
        };
        let (keyword, generics) = owner_block(&source, def.start);
        if keyword.as_deref() == Some("trait") || generics.contains(owner) || types.contains(owner)
        {
            found.push(def.start);
        }
    }
    found
}

/// The variable a method call at `dot` reads whole as its receiver, or
/// `None`.
///
/// The receiver expression, as `receiver_expression` finds it, goes through
/// `argument_root`, the normalizer that argument handoffs use. So `body`,
/// `(body)`, `(&body)` and `(*body).as_ref()` all name `body`.
pub fn call_receiver(block: &str, dot: usize) -> Option<String> {
    let (root, whole) = argument_root(receiver_expression(block, dot), false, true);
    if whole { root } else { None }
}

/// The receiver expression of a method call at `dot`, as `receiver_start`
/// bounds it.
pub fn receiver_expression(block: &str, dot: usize) -> &str {
    py::sl(block, receiver_start(block, dot), dot).trim()
}

/// Where the postfix expression that ends at the `.` of a method call at
/// `dot` starts.
///
/// It reads back over names, and over grouped `( .. )` and indexed `[ .. ]`
/// parts. It also reads back over a call's own name with its turbofish, `?`,
/// and the `.` or `::` that join them. So the expression is `(&body)` in `(&body).decode()`, and
/// `body.as_ref()` in `body.as_ref().decode()`. A prefix operator outside
/// the group, as in `&body.decode()`, applies to the call, so it is not
/// read.
pub fn receiver_start(block: &str, dot: usize) -> usize {
    let bytes = block.as_bytes();
    let skip = |mut index: usize| {
        while index > 0 && py::is_space(bytes[index - 1]) {
            index -= 1;
        }
        index
    };
    let mut index = skip(dot.min(bytes.len()));
    while index > 0 {
        let char = bytes[index - 1];
        if char == b')' || char == b']' {
            let Some(opener) = matching_opener(block, index - 1) else {
                break;
            };
            index = opener;
            // A call's own name, with its turbofish, belongs to the call.
            if char == b')' && index > 0 && bytes[index - 1] == b'>' {
                let mut depth = 0i64;
                for back in (0..index).rev() {
                    depth += i64::from(bytes[back] == b'>');
                    depth -= i64::from(bytes[back] == b'<');
                    if depth == 0 {
                        index = if back >= 2 && &bytes[back - 2..back] == b"::" {
                            back - 2
                        } else {
                            back
                        };
                        break;
                    }
                }
            }
            let window = py::sl(block, index.saturating_sub(64), index);
            if let Some(name) = py::search(r"[A-Za-z_]\w*!?$", window) {
                index -= name.whole().len();
            }
        } else if py::is_word(char) {
            while index > 0 && py::is_word(bytes[index - 1]) {
                index -= 1;
            }
        } else if char == b'?' {
            index -= 1;
            continue;
        } else {
            break;
        }
        let joint = skip(index);
        if joint > 1 && &bytes[joint - 2..joint] == b"::" {
            index = skip(joint - 2);
            continue;
        }
        if joint > 0
            && bytes[joint - 1] == b'.'
            && py::ps(block, py::to_isize(joint) - 2, py::to_isize(joint)) != ".."
        {
            index = skip(joint - 1);
            continue;
        }
        break;
    }
    index
}

/// `(path, start, token tree, end)` for each macro invocation in `block`.
///
/// It reads `name!(..)`, `name![..]` and `name!{..}`, with or without a
/// path. The token tree is the text inside the brackets. No macro is
/// modeled, so a carrier in any macro is no parse the audit can type. A
/// scalar read such as `body.len()` is no handoff, as `argument_passes`
/// reads it outside a macro too.
pub fn macro_invocations(block: &str) -> Rc<Vec<Macro>> {
    state::cached(&MACROS, block.to_string(), || {
        let mut found = Vec::new();
        for invocation in py::finditer(MACRO_CALL, block) {
            let opener = invocation.end() - 1;
            let open = invocation.g(3).as_bytes()[0];
            let close = match open {
                b'(' => b')',
                b'[' => b']',
                _ => b'}',
            };
            let tree = balanced(&block[opener..], open, close);
            let path = py::no_space(invocation.g(1));
            let inner = py::sl(tree, 1, tree.len().saturating_sub(1)).to_string();
            found.push((path, invocation.start(), inner, opener + tree.len()));
        }
        Rc::new(found)
    })
}

/// Every `QUALIFIED_CALL` match in `block`, found once.
pub fn qualified_calls(block: &str) -> Vec<M<'_>> {
    let spans = state::cached(&QUALIFIED, block.to_string(), || {
        Rc::new(
            py::finditer(QUALIFIED_CALL, block)
                .iter()
                .map(M::spans)
                .collect(),
        )
    });
    spans
        .iter()
        .map(|spans| M::from_spans(block, Rc::clone(spans)))
        .collect()
}

/// Query-key literals matched at the top of a `pairs` loop.
pub fn key_arms(body: &str) -> Vec<Vec<String>> {
    let mut arms = Vec::new();
    let bindings: BTreeSet<&str> = py::findall1(
        r"for\s*\(\s*([a-z_0-9]+)\s*,\s*[a-z_0-9]+\s*\)\s*in[^\n{]*pairs",
        body,
    )
    .into_iter()
    .collect();
    for binding in bindings {
        let pattern = format!(r"match\s+{}\.as_str\(\)\s*\{{", py::esc(binding));
        for found in py::finditer(&pattern, body) {
            let block = py::brace(&body[found.end() - 1..]);
            // Depth 1 is the arm list itself. A nested match sits deeper. So a
            // value arm such as `"asc" => Order::Asc` is not read as a key.
            let mut depth = 0i64;
            for line in block.split('\n') {
                if depth == 1
                    && let Some(hit) = py::rmatch(KEY_ARM, line)
                {
                    arms.push(
                        py::findall1(r#""([a-z_0-9-]+)""#, hit.g(1))
                            .into_iter()
                            .map(str::to_string)
                            .collect(),
                    );
                }
                depth += py::count_byte(line, b'{') - py::count_byte(line, b'}');
            }
        }
    }
    arms
}

/// Whether a `.with_status(..)` call immediately follows a call whose
/// argument list opens at `call_open_paren`.
///
/// That `(` comes right after the constructor name. When this is true, the
/// constructor's own implied status is not what the route returns. The
/// `StatusCode::` literal inside `.with_status(..)` is, and the plain scan
/// already requires that literal to be declared.
pub fn overridden_by_with_status(body: &str, call_open_paren: usize) -> bool {
    let call_text = paren(&body[call_open_paren..]);
    py::from(body, call_open_paren + call_text.len())
        .trim_start()
        .starts_with(".with_status(")
}
