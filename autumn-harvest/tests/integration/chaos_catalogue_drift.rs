//! Chaos injection-point catalogue drift guard (issue #940).
//!
//! A no-DB, no-`chaos`-feature source scan asserting that every point in
//! `autumn_harvest::chaos::points::ALL` is wired at **exactly one** production
//! call site in `src/` via the matching macro. `points` is unconditional
//! (compiled into every build), so this test runs in the normal test build and
//! can never silently pass while a catalogue entry has lost its wiring — the
//! failure mode that would otherwise make an injection point a dead no-op.
//!
//! The scan lexes each source into a `proc_macro2::TokenStream` and matches the
//! `<macro> ! ( IDENT )` token pattern, rather than scanning raw text. Lexing
//! drops comments and delimits string / raw-string / char literals, so a
//! commented-out or stringified `chaos_point!(NAME)` is **not** miscounted as
//! live wiring — the precise drift this guard exists to catch.

use std::path::{Path, PathBuf};

use proc_macro2::{Delimiter, TokenStream, TokenTree};

/// Expected `(point ident, wiring macro)` for every catalogue entry. Kept in
/// lockstep with `points::ALL` by the assertions below.
const EXPECTED: &[(&str, &str)] = &[
    ("QUEUE_PARK_BEFORE_UPDATE", "chaos_point"),
    ("WORKER_PERSIST_BEFORE_COMMIT", "chaos_point"),
    ("WORKER_AFTER_OUTER_COMMIT", "chaos_point"),
    ("OUTBOX_INLINE_AFTER_REQUESTED", "chaos_point"),
    ("SCHED_AFTER_CLAIM", "chaos_point"),
    ("SCHED_AFTER_START_BEFORE_ADVANCE", "chaos_point"),
    ("POISON_RECLAIM_BEFORE_LOAD", "chaos_fallible"),
    ("NOTIFY_TASK_ENQUEUED", "chaos_drop_notify"),
];

/// The wiring macros a production call site may use.
const CHAOS_MACROS: &[&str] = &["chaos_point", "chaos_fallible", "chaos_drop_notify"];

fn is_chaos_macro(name: &str) -> bool {
    CHAOS_MACROS.contains(&name)
}

fn src_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("src")
}

/// Recursively lex every `.rs` file under `src/` into a token stream, EXCEPT the
/// two files where the chaos vocabulary is *defined* rather than *used*:
/// `chaos.rs` (the point consts + catalogue) and `lib.rs` (the
/// `chaos_point!` / `chaos_fallible!` / `chaos_drop_notify!` `macro_rules`
/// definitions, whose bodies and doc examples reference the macro names and a
/// `NAME` placeholder). Neither is production wiring.
fn production_sources() -> Vec<(String, TokenStream)> {
    let mut out = Vec::new();
    let mut stack = vec![src_dir()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).expect("read_dir src") {
            let path = entry.expect("dir entry").path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if name == "chaos.rs" || name == "lib.rs" {
                    continue;
                }
                let display = path.display().to_string();
                let body = std::fs::read_to_string(&path).expect("read source");
                let tokens: TokenStream = body
                    .parse()
                    .unwrap_or_else(|e| panic!("failed to lex {display}: {e}"));
                out.push((display, tokens));
            }
        }
    }
    out
}

/// Collect every chaos-macro call site as `(source, macro, ident)`.
///
/// Walks the token tree, matching the `Ident(<chaos macro>) Punct('!')
/// Group('(' … ')')` sequence and extracting the single ident argument. A call
/// whose parenthesised argument is not exactly one ident is recorded with a
/// `<non-ident:N>` sentinel so [`no_orphan_chaos_macro_call_sites`] flags it
/// (production wiring always passes a bare catalogue ident). Recurses into every
/// group (blocks, `if` bodies, …) so a wrapped call — e.g.
/// `if cond { crate::chaos_point!(NAME); }` — is still found.
fn all_chaos_calls(sources: &[(String, TokenStream)]) -> Vec<(String, String, String)> {
    let mut out = Vec::new();
    for (path, tokens) in sources {
        let mut calls = Vec::new();
        scan_stream(tokens.clone(), &mut calls);
        for (macro_name, ident) in calls {
            out.push((path.clone(), macro_name, ident));
        }
    }
    out
}

/// Push every `<chaos macro> ! ( arg )` call in `ts` as `(macro, ident)` into
/// `out`, recursing into nested groups.
fn scan_stream(ts: TokenStream, out: &mut Vec<(String, String)>) {
    let trees: Vec<TokenTree> = ts.into_iter().collect();
    let mut i = 0;
    while i < trees.len() {
        if let TokenTree::Ident(id) = &trees[i] {
            let name = id.to_string();
            let bang = matches!(trees.get(i + 1), Some(TokenTree::Punct(p)) if p.as_char() == '!');
            if is_chaos_macro(&name)
                && bang
                && let Some(TokenTree::Group(g)) = trees.get(i + 2)
                && g.delimiter() == Delimiter::Parenthesis
            {
                let inner: Vec<TokenTree> = g.stream().into_iter().collect();
                let arg = match inner.as_slice() {
                    [TokenTree::Ident(arg)] => arg.to_string(),
                    _ => format!("<non-ident:{}>", inner.len()),
                };
                out.push((name, arg));
                // Skip ident + '!' + arg group; the arg holds no nested chaos
                // macro, so there is nothing to recurse into there.
                i += 3;
                continue;
            }
        }
        if let TokenTree::Group(g) = &trees[i] {
            scan_stream(g.stream(), out);
        }
        i += 1;
    }
}

fn wiring_count(calls: &[(String, String, String)], macro_name: &str, ident: &str) -> usize {
    calls
        .iter()
        .filter(|(_, m, id)| m == macro_name && id == ident)
        .count()
}

#[test]
fn every_catalogue_point_is_wired_exactly_once() {
    let sources = production_sources();
    let calls = all_chaos_calls(&sources);
    for (ident, macro_name) in EXPECTED {
        let count = wiring_count(&calls, macro_name, ident);
        assert_eq!(
            count, 1,
            "chaos point {ident} must be wired at exactly one production call site \
             via {macro_name}!(...); found {count}. Wire it (or remove it from the \
             catalogue) — a catalogue entry with no wiring is a dead no-op."
        );
    }
}

#[test]
fn expected_list_matches_catalogue_all() {
    use autumn_harvest::chaos::points::ALL;
    assert_eq!(
        ALL.len(),
        EXPECTED.len(),
        "the drift-test EXPECTED list ({}) must match points::ALL ({}); \
         update both when adding or removing a point",
        EXPECTED.len(),
        ALL.len()
    );
    // Every catalogue point's `name()` uses dotted namespacing and is unique;
    // the count assertion above binds `ALL` and `EXPECTED` to the same length,
    // and `every_catalogue_point_is_wired_exactly_once` binds each `EXPECTED`
    // ident to a real wired call site — so neither list can drift alone.
    let mut names: Vec<&str> = ALL.iter().map(|p| p.name()).collect();
    let before = names.len();
    names.sort_unstable();
    names.dedup();
    assert_eq!(before, names.len(), "two catalogue points share a name()");
}

/// No production wiring may reference an ident that is not in the catalogue —
/// otherwise a stray `chaos_point!(TYPO)` would compile-fail (good) or a
/// renamed-but-not-removed point would leave an orphan call site.
#[test]
fn no_orphan_chaos_macro_call_sites() {
    let sources = production_sources();
    let calls = all_chaos_calls(&sources);
    let known: Vec<&str> = EXPECTED.iter().map(|(ident, _)| *ident).collect();
    for (path, macro_name, ident) in &calls {
        assert!(
            known.contains(&ident.as_str()),
            "orphan chaos macro call {macro_name}!({ident}) in {path}: \
             ident is not in the catalogue drift list"
        );
    }
}

/// The token scan must ignore a `chaos_point!(NAME)` that appears only in a
/// comment or a string literal — the exact drift (a commented-out call still
/// leaving one textual match) a raw-text scan cannot catch.
#[test]
fn token_scan_ignores_comments_and_strings() {
    let src = r##"
        fn wired() {
            // A live call — the only one that must count.
            crate::chaos_point!(QUEUE_PARK_BEFORE_UPDATE);
        }
        // A commented-out call must NOT count:
        // crate::chaos_point!(QUEUE_PARK_BEFORE_UPDATE);
        fn strings() {
            let _sql = "chaos_point!(QUEUE_PARK_BEFORE_UPDATE) in a string";
            let _url = "http://example.com/not-a-comment";
            let _raw = r#"chaos_point!(QUEUE_PARK_BEFORE_UPDATE) in a raw string"#;
            let _c = '"'; // a char literal holding a quote must not open a string
            /* block // comment with chaos_point!(QUEUE_PARK_BEFORE_UPDATE) */
        }
    "##;
    let tokens: TokenStream = src.parse().expect("lex synthetic source");
    let mut calls = Vec::new();
    scan_stream(tokens, &mut calls);
    let count = calls
        .iter()
        .filter(|(m, id)| m == "chaos_point" && id == "QUEUE_PARK_BEFORE_UPDATE")
        .count();
    assert_eq!(
        count, 1,
        "only the live call site must count; comments and string/raw/char \
         literals must be ignored (found {count})"
    );
}
