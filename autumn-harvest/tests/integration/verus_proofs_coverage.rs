//! Verus proof coverage guard. No DB, no feature gate.
//!
//! Verus cannot compile the engine crate. Each proof in `formal/verus`
//! therefore checks a copy of an engine function. This suite stops a copy
//! from drifting away from its source.
//!
//! - A `// mirror: <path> <fn>` line marks each copy. After the guard drops
//!   comments, whitespace and `proof { }` blocks, the copy's body must equal
//!   the body of `<fn>` in `<path>`.
//! - Each source file names the Verus file that proves it.
//! - No proof assumes what it must prove.
//! - `ci.yml` runs the Verus runner on every PR.
//! - `docs/testing/formal-methods.md` names every Verus file and mirror.

use std::path::Path;

use super::ci_run_coverage::{parse_workflow, repo_root};
use super::formal_models_coverage::formal_job_defect;

/// Name parts that mark a way to skip a step. No identifier may hold one.
///
/// The guard lexes each file, so comments and literals do not count. A
/// space or a comment between two tokens cannot hide a name. The guard
/// bans the name itself, so a `use` alias cannot hide it either. A part
/// matches anywhere in a name, so `external` also bans
/// `external_body` and `external_trait_specification`.
const TRUST_ESCAPES: &[&str] = &[
    "assume",
    "admit",
    // `axiom fn f() ensures false;` verifies, and so does every proof
    // that calls it.
    "axiom",
    "external",
    "truncate",
    // A local macro can delete a proof before Verus sees it.
    "macro_rules",
];

/// The attributes a proof file may use. Any other attribute fails the
/// guard. A `#[verifier::...]` attribute can trust a step, and a
/// `#[cfg(...)]` can delete a proof, so a new one must be allowed here
/// on purpose.
const ALLOWED_ATTRIBUTES: &[&str] = &["trigger"];

/// One `// mirror:` marker.
#[derive(Debug)]
struct Mirror {
    /// The Verus file, relative to the repo root.
    verus_file: String,
    /// The engine file, relative to the repo root.
    source_file: String,
    /// The function name in the engine file.
    source_fn: String,
    /// The normalized body of the Verus copy.
    verus_body: Vec<String>,
}

/// The index of the `}` that closes the `{` at `open`.
fn matching_brace(tokens: &[String], open: usize) -> usize {
    let mut depth = 0_usize;
    for (i, t) in tokens.iter().enumerate().skip(open) {
        match t.as_str() {
            "{" => depth += 1,
            "}" => {
                depth -= 1;
                if depth == 0 {
                    return i;
                }
            }
            _ => {}
        }
    }
    panic!("unbalanced braces after token {open}");
}

/// The body tokens of the first `fn <name>(` in `tokens`, with its braces.
///
/// The body opens at the first `{` outside parentheses. A Verus `ensures`
/// clause can hold a block, but only inside parentheses.
fn fn_body(tokens: &[String], name: &str) -> Option<Vec<String>> {
    let start = tokens
        .windows(3)
        .position(|w| w[0] == "fn" && w[1] == name && w[2] == "(")?;
    let mut parens = 0_i32;
    for (i, t) in tokens.iter().enumerate().skip(start) {
        match t.as_str() {
            "(" => parens += 1,
            ")" => parens -= 1,
            "{" if parens == 0 => return Some(tokens[i..=matching_brace(tokens, i)].to_vec()),
            _ => {}
        }
    }
    None
}

/// Drops each `proof { ... }` block. Ghost code has no runtime effect.
fn strip_proof_blocks(mut body: Vec<String>) -> Vec<String> {
    while let Some(at) = body.windows(2).position(|w| w[0] == "proof" && w[1] == "{") {
        let end = matching_brace(&body, at + 1);
        body.drain(at..=end);
    }
    body
}

/// The keywords that open a Verus loop clause (issue #2330).
///
/// A clause states a spec of the loop. Verus erases it, so it cannot
/// change what the copy computes. An engine body must hold none of them.
const LOOP_CLAUSES: &[&str] = &[
    "invariant",
    "invariant_except_break",
    "ensures",
    "decreases",
];

/// Drops each loop clause. A clause runs from its keyword to the `{` that
/// opens the loop body. A `{` inside parentheses or brackets does not end
/// it.
fn strip_loop_clauses(body: Vec<String>) -> Vec<String> {
    let mut out = Vec::with_capacity(body.len());
    let mut tokens = body.into_iter().peekable();
    while let Some(t) = tokens.next() {
        if !LOOP_CLAUSES.contains(&t.as_str()) {
            out.push(t);
            continue;
        }
        let mut depth = 0_i32;
        while let Some(next) = tokens.peek() {
            match next.as_str() {
                "(" | "[" => depth += 1,
                ")" | "]" => depth -= 1,
                "{" if depth == 0 => break,
                _ => {}
            }
            tokens.next();
        }
    }
    out
}

/// The tokens of a body, without comments, ghost blocks, loop clauses or
/// whitespace.
fn normalize(body: &[String]) -> Vec<String> {
    strip_loop_clauses(strip_proof_blocks(body.to_vec()))
}

fn read(rel: &str) -> String {
    let path = repo_root().join(rel);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// The `.rs` files at any depth under `formal/verus/src`, relative to the
/// repo root.
///
/// The walk is recursive because `mod m;` also resolves to `m/mod.rs`. A
/// nested file then fails the crate root check, so no file escapes a guard.
fn verus_files() -> Vec<String> {
    rs_files_under("formal/verus/src")
}

/// Every mirror marker in the Verus sources.
fn mirrors() -> Vec<Mirror> {
    let mut out = Vec::new();
    for verus_file in verus_files() {
        let text = read(&verus_file);
        let mut rest = text.as_str();
        while let Some(at) = rest.find("// mirror: ") {
            let line = rest[at..].lines().next().unwrap_or_default();
            let fields: Vec<&str> = line["// mirror: ".len()..].split_whitespace().collect();
            assert_eq!(fields.len(), 2, "{verus_file}: bad marker `{line}`");
            let after = &rest[at + line.len()..];
            let tokens = code_tokens(after);
            let copy = tokens
                .iter()
                .position(|t| t == "fn")
                .and_then(|i| tokens.get(i + 1))
                .unwrap_or_else(|| panic!("{verus_file}: `{line}` precedes no fn"));
            let body = fn_body(&tokens, copy)
                .unwrap_or_else(|| panic!("{verus_file}: no body for `{copy}`"));
            out.push(Mirror {
                verus_file: verus_file.clone(),
                source_file: fields[0].to_string(),
                source_fn: fields[1].to_string(),
                verus_body: normalize(&body),
            });
            rest = after;
        }
    }
    out
}

/// The normalized body of `fn f` in `src`.
fn norm(src: &str) -> Vec<String> {
    normalize(&fn_body(&code_tokens(src), "f").expect(src))
}

#[test]
fn normalize_drops_comments_ghost_blocks_and_whitespace() {
    let a = "fn f() {\n    let x = 1; // one\n    proof { lemma(x); }\n    x + 1\n}";
    let b = "fn f() { let x = 1;\n /* two */ x + 1 }";
    assert_eq!(norm(a), norm(b));
    assert_ne!(norm("fn f() { x + 1 }"), norm("fn f() { x + 2 }"));
    // A name that only starts with `proof` is code, not a ghost block.
    assert_eq!(norm("fn f() { proofs { 1 } }").concat(), "{proofs{1}}");
    // A literal keeps its text, and a `//` inside it starts no comment.
    assert_ne!(
        norm("fn f() { let s = \"//\"; good() }"),
        norm("fn f() { let s = \"//\"; bad() }")
    );
    assert_ne!(norm("fn f() { \"a\" }"), norm("fn f() { \"b\" }"));
    // A brace inside a literal or a comment does not end the body.
    assert_eq!(
        norm("fn f() { let s = \"}\"; /* } */ g() }").concat(),
        "{lets=\"}\";g()}"
    );
}

#[test]
fn normalize_drops_loop_clauses() {
    let verus = "fn f(v: &mut [u8]) {\n    let mut i = 0;\n    while i < v.len()\n        \
                 invariant\n            i <= v.len(),\n            ({ let n = v.len(); i <= n }),\n        \
                 decreases v.len() - i,\n    {\n        i += 1;\n    }\n}";
    let engine = "fn f(v: &mut [u8]) { let mut i = 0; while i < v.len() { i += 1; } }";
    assert_eq!(norm(verus), norm(engine));
    let looped = "fn f() { loop invariant x < 3, decreases 3 - x, { x += 1; } }";
    assert_eq!(norm(looped).concat(), "{loop{x+=1;}}");
    // A clause ends at the loop body, so the body still counts.
    assert_ne!(
        norm("fn f() { loop invariant x < 3, { x += 1; } }"),
        norm("fn f() { loop invariant x < 3, { x += 2; } }")
    );
}

#[test]
fn fn_body_skips_blocks_inside_an_ensures_clause() {
    let body = |src: &str, name: &str| fn_body(&code_tokens(src), name).map(|b| b.concat());
    let text = "fn f(x: u8) -> (r: u8)\n    ensures ({ let y = x; r == y }),\n{\n    x\n}\n";
    assert_eq!(body(text, "f").as_deref(), Some("{x}"));
    assert_eq!(
        body("const fn g() -> u8 { 1 }", "g").as_deref(),
        Some("{1}")
    );
    assert_eq!(body("fn g() {}", "h"), None);
    assert_eq!(body("// fn h() { 1 }\nfn g() {}", "h"), None);
}

#[test]
fn verus_crate_holds_mirrors() {
    let mirrors = mirrors();
    assert!(
        mirrors.len() >= 3,
        "want >= 3 mirrored functions, found {mirrors:?}"
    );
}

#[test]
fn every_verus_mirror_matches_its_engine_source() {
    for m in mirrors() {
        let source = read(&m.source_file);
        let body = fn_body(&code_tokens(&source), &m.source_fn).unwrap_or_else(|| {
            panic!(
                "{}: `{}` mirrors `fn {}`, which is gone",
                m.verus_file, m.source_file, m.source_fn
            )
        });
        // A loop clause in the engine body would vanish from both sides.
        let clause = body.iter().find(|t| LOOP_CLAUSES.contains(&t.as_str()));
        assert!(
            clause.is_none(),
            "{}: `fn {}` holds the loop clause keyword {clause:?}. The guard drops \
             loop clauses, so it cannot compare this body",
            m.source_file,
            m.source_fn
        );
        assert_eq!(
            normalize(&body),
            m.verus_body,
            "{} copies `{}` in {}, but the bodies differ. Port the change to \
             the Verus copy, then run scripts/check-verus-proofs.sh",
            m.verus_file,
            m.source_fn,
            m.source_file
        );
    }
}

/// The source crates that a Verus proof may mirror.
///
/// The agent crate holds pure kernels of the durable agent (issue #2262).
/// A proof of one names its Verus file as an engine function does.
const MIRRORED_CRATES: &[&str] = &["autumn-harvest/src", "autumn-harvest-agent/src"];

/// The `.rs` files under each of [`MIRRORED_CRATES`], relative to the repo
/// root.
fn engine_files() -> Vec<String> {
    MIRRORED_CRATES
        .iter()
        .copied()
        .flat_map(rs_files_under)
        .collect()
}

/// The `.rs` files at any depth under `dir`, relative to the repo root.
/// Each path joins its parts with `/` on every OS, as a mirror marker does.
fn rs_files_under(dir: &str) -> Vec<String> {
    fn walk(dir: &Path, out: &mut Vec<String>) {
        for entry in std::fs::read_dir(dir).expect("read source dir").flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, out);
            } else if path.extension().is_some_and(|e| e == "rs") {
                let rel = path.strip_prefix(repo_root()).expect("under the repo root");
                let parts: Vec<_> = rel
                    .components()
                    .map(|c| c.as_os_str().to_string_lossy().into_owned())
                    .collect();
                out.push(parts.join("/"));
            }
        }
    }
    let mut out = Vec::new();
    walk(&repo_root().join(dir), &mut out);
    out.sort();
    out
}

/// Each `(fn, Verus file)` pair where the `///` doc block of `fn` names
/// a file under `formal/verus/src/`.
fn proof_named_fns(source: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut doc = String::new();
    for line in source.lines() {
        let line = line.trim();
        if let Some(text) = line.strip_prefix("///") {
            doc.push_str(text);
            doc.push(' ');
            continue;
        }
        if line.starts_with("#[") {
            continue;
        }
        if let Some(at) = line.find("fn ") {
            let name: String = line[at + 3..]
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            let mut rest = doc.as_str();
            while let Some(i) = rest.find("formal/verus/src/") {
                let tail = &rest[i..];
                let end = tail.find(".rs").map_or(tail.len(), |e| e + 3);
                out.push((name.clone(), tail[..end].to_string()));
                rest = &tail[end..];
            }
        }
        doc.clear();
    }
    out
}

/// The mirror set must equal the set of engine functions whose doc comment
/// names a Verus proof. A deleted `// mirror:` marker fails the guard, and
/// an editor of a proved function finds its proof in the doc comment.
#[test]
fn mirror_inventory_matches_the_engine_doc_comments() {
    use std::collections::BTreeSet;
    let mut named = BTreeSet::new();
    for file in engine_files() {
        for (func, verus_file) in proof_named_fns(&read(&file)) {
            named.insert((file.clone(), func, verus_file));
        }
    }
    let mirrored: BTreeSet<_> = mirrors()
        .into_iter()
        .map(|m| (m.source_file, m.source_fn, m.verus_file))
        .collect();
    let unmirrored: Vec<_> = named.difference(&mirrored).collect();
    let undocumented: Vec<_> = mirrored.difference(&named).collect();
    assert!(
        unmirrored.is_empty(),
        "these functions name a Verus proof, but no `// mirror:` marker copies them: {unmirrored:?}"
    );
    assert!(
        undocumented.is_empty(),
        "these mirrored functions must name their Verus file in their doc comment: {undocumented:?}"
    );
}

/// The stored list of every Verus contract.
const CONTRACTS: &str = "formal/verus/contracts.txt";

/// The first lines of [`CONTRACTS`].
const CONTRACTS_HEADER: &str = "\
# The contract of every item in formal/verus/src, one item per line.
# A spec fn keeps its body, because its definition gives a contract its
# meaning. Any other fn keeps its signature and its requires, ensures and
# decreases clauses. Verus checks its body.
#
# Do not edit this file by hand. A change here shows that a proof now
# states a different property. Rewrite it with:
#   UPDATE_VERUS_CONTRACTS=1 cargo test -p autumn-harvest --test integration \\
#     verus_proofs_coverage::every_verus_contract_matches_the_stored_list
";

/// The contract of each item in the `verus!` block of `src`, as one line
/// of tokens.
///
/// An item ends at a `;` or at the `}` of its body, outside parentheses
/// and brackets. A `spec fn` keeps its body. Any other `fn` drops its body.
/// An `impl`, `trait` or `mod` item gives one line for its header, one for
/// each item inside it, and one for its `}`. Any other item keeps its body,
/// because a `struct` or `enum` body gives a contract its meaning.
fn contracts(src: &str) -> Vec<String> {
    let tokens = code_tokens(src);
    let Some(at) = tokens
        .windows(3)
        .position(|w| w[0] == "verus" && w[1] == "!" && w[2] == "{")
    else {
        return Vec::new();
    };
    let close = matching_brace(&tokens, at + 2);
    let mut out = Vec::new();
    item_contracts(&tokens, at + 3, close, &mut out);
    out
}

/// Pushes the contract of each item in `tokens[from..to]` to `out`.
fn item_contracts(tokens: &[String], from: usize, to: usize, out: &mut Vec<String>) {
    let mut i = from;
    while i < to {
        let start = i;
        let mut depth = 0_i32;
        let mut body = None;
        while i < to {
            let t = tokens[i].as_str();
            i += 1;
            match t {
                "(" | "[" => depth += 1,
                ")" | "]" => depth -= 1,
                ";" if depth == 0 => break,
                "{" if depth == 0 => {
                    body = Some(i - 1);
                    i = matching_brace(tokens, i - 1) + 1;
                    break;
                }
                _ => {}
            }
        }
        let item = &tokens[start..i];
        let Some(b) = body else {
            out.push(render(item));
            continue;
        };
        let head = &tokens[start..b];
        if head.iter().any(|t| t == "fn") {
            let spec = head.iter().take_while(|t| *t != "fn").any(|t| t == "spec");
            out.push(render(if spec { item } else { head }));
        } else if head
            .iter()
            .any(|t| matches!(t.as_str(), "impl" | "trait" | "mod"))
        {
            out.push(render(&tokens[start..=b]));
            item_contracts(tokens, b + 1, i - 1, out);
            out.push("}".to_string());
        } else {
            out.push(render(item));
        }
    }
}

/// Joins tokens into one line of code-like text.
///
/// The lexer splits each punctuation character, so `->` arrives as `-` and
/// `>`. Two adjacent operator characters join again. A call, a path and a
/// bracket take no space, but a keyword such as `ensures` keeps its space.
/// Any other pair of tokens takes one space.
fn render(tokens: &[String]) -> String {
    let word = |t: &str| {
        t.chars()
            .next()
            .is_some_and(|c| c.is_alphanumeric() || c == '_')
    };
    let grouping = |t: &str| matches!(t, "(" | ")" | "[" | "]" | "{" | "}" | "," | ";" | "#");
    let keyword = |t: &str| {
        matches!(
            t,
            "as" | "by" | "decreases" | "ensures" | "if" | "in" | "match" | "requires" | "return"
        )
    };
    let mut out = String::new();
    for (i, b) in tokens.iter().enumerate() {
        if let Some(a) = i.checked_sub(1).map(|j| tokens[j].as_str()) {
            let path = a == ":" && i >= 2 && tokens[i - 2] == ":";
            let operator = !word(a) && !word(b) && !grouping(a) && !grouping(b);
            let tight = matches!(b.as_str(), "," | ";" | ")" | "]" | ".")
                || matches!(a, "(" | "[" | "." | "#")
                || (b == "(" && word(a) && !keyword(a))
                || (b == ":" && word(a))
                || path
                || operator;
            if !tight {
                out.push(' ');
            }
        }
        out.push_str(b);
    }
    out
}

/// The text that [`CONTRACTS`] must hold.
fn contract_inventory() -> String {
    let mut out = String::from(CONTRACTS_HEADER);
    for file in verus_files() {
        if file.ends_with("/lib.rs") {
            continue;
        }
        out.push_str("\n## ");
        out.push_str(&file);
        out.push('\n');
        for contract in contracts(&read(&file)) {
            out.push_str(&contract);
            out.push('\n');
        }
    }
    out
}

/// Each contract must equal its line in [`CONTRACTS`].
///
/// A weaker `ensures` clause still verifies, and the mirrored body still
/// matches. This guard makes the weaker property a visible change to a
/// second, generated file. A deleted lemma or theorem fails the same way.
#[test]
fn every_verus_contract_matches_the_stored_list() {
    let want = contract_inventory();
    let have = std::fs::read_to_string(repo_root().join(CONTRACTS))
        .unwrap_or_default()
        .replace("\r\n", "\n");
    if have == want {
        return;
    }
    if std::env::var_os("UPDATE_VERUS_CONTRACTS").is_some() {
        let path = repo_root().join(CONTRACTS);
        std::fs::write(&path, &want)
            .unwrap_or_else(|e| panic!("cannot write {}: {e}", path.display()));
        return;
    }
    let gone: Vec<&str> = have
        .lines()
        .filter(|l| !want.lines().any(|w| w == *l))
        .collect();
    let new: Vec<&str> = want
        .lines()
        .filter(|l| !have.lines().any(|h| h == *l))
        .collect();
    panic!(
        "a Verus contract differs from {CONTRACTS}.\n\
         stored, now gone: {gone:#?}\n\
         now, not stored: {new:#?}\n\
         If the proof should state the new property, rewrite the list with:\n  \
         UPDATE_VERUS_CONTRACTS=1 cargo test -p autumn-harvest --test integration \
         verus_proofs_coverage::every_verus_contract_matches_the_stored_list"
    );
}

#[test]
fn contracts_keep_clauses_and_spec_bodies_only() {
    let src = "use vstd::prelude::*;\nverus! {\n\
        pub open spec fn two() -> int { 2 }\n\
        // mirror: a.rs f\n\
        pub fn f(x: u8) -> (r: u8)\n    requires x < 9,\n    ensures ({ let y = x; r == y }),\n{\n    x\n}\n\
        proof fn lemma(x: u64) by (bit_vector)\n    ensures x == x,\n{\n}\n\
        }\n";
    assert_eq!(
        contracts(src),
        [
            "pub open spec fn two() -> int { 2 }",
            "pub fn f(x: u8) -> (r: u8) requires x < 9, ensures ({ let y = x; r == y }),",
            "proof fn lemma(x: u64) by (bit_vector) ensures x == x,",
        ]
    );
    let weaker = src.replace("r == y", "r >= 0");
    assert_ne!(contracts(src), contracts(&weaker));
    let body_only = src.replace("    x\n}", "    x + 0\n}");
    assert_eq!(contracts(src), contracts(&body_only));
}

#[test]
fn contracts_read_the_items_inside_a_container() {
    let src = "verus! {\n\
        struct P { x: u8 }\n\
        impl P {\n    proof fn lemma(self)\n        ensures self.x < 9,\n    {\n    }\n}\n\
        mod m {\n    trait T {\n        spec fn f(&self) -> int;\n    }\n}\n\
        }\n";
    assert_eq!(
        contracts(src),
        [
            "struct P { x: u8 }",
            "impl P {",
            "proof fn lemma(self) ensures self.x < 9,",
            "}",
            "mod m {",
            "trait T {",
            "spec fn f(& self) -> int;",
            "}",
            "}",
        ]
    );
    let weaker = src.replace("self.x < 9", "self.x < 99");
    assert_ne!(contracts(src), contracts(&weaker));
    let deleted = src.replace(
        "proof fn lemma(self)\n        ensures self.x < 9,\n    {\n    }\n",
        "",
    );
    assert_ne!(contracts(src), contracts(&deleted));
}

#[test]
fn proof_named_fns_reads_the_doc_block_of_each_fn() {
    let src = "/// Plain.\nfn a() {}\n/// `formal/verus/src/x.rs` proves it.\n#[inline]\n    pub const fn b(x: u8) {}\n// formal/verus/src/y.rs\nfn c() {}\n";
    assert_eq!(
        proof_named_fns(src),
        [("b".to_string(), "formal/verus/src/x.rs".to_string())]
    );
}

/// The tokens of `src` in order. A token is an identifier, one punctuation
/// char, or a whole string, raw string or char literal with its quotes.
/// Comments, nested block comments and whitespace yield none.
fn code_tokens(src: &str) -> Vec<String> {
    let c: Vec<char> = src.chars().collect();
    let at = |i: usize| c.get(i).copied().unwrap_or('\0');
    let is_ident = |ch: char| ch.is_alphanumeric() || ch == '_';
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < c.len() {
        let from = i;
        match (c[i], at(i + 1)) {
            ('/', '/') => {
                while i < c.len() && c[i] != '\n' {
                    i += 1;
                }
            }
            ('/', '*') => {
                let mut depth = 0_usize;
                while i < c.len() {
                    match (c[i], at(i + 1)) {
                        ('/', '*') => {
                            depth += 1;
                            i += 2;
                        }
                        ('*', '/') => {
                            depth -= 1;
                            i += 2;
                            if depth == 0 {
                                break;
                            }
                        }
                        _ => i += 1,
                    }
                }
            }
            ('"', _) => {
                i += 1;
                while i < c.len() && c[i] != '"' {
                    i += if c[i] == '\\' { 2 } else { 1 };
                }
                i += 1;
                tokens.push(c[from..i.min(c.len())].iter().collect());
            }
            // A char literal. A lifetime has no closing quote, so it
            // falls through and its name lexes as an identifier.
            ('\'', '\\') => {
                i += 2;
                while i < c.len() && c[i] != '\'' {
                    i += if c[i] == '\\' { 2 } else { 1 };
                }
                i += 1;
                tokens.push(c[from..i.min(c.len())].iter().collect());
            }
            ('\'', _) if at(i + 2) == '\'' => {
                i += 3;
                tokens.push(c[from..i.min(c.len())].iter().collect());
            }
            (ch, _) if is_ident(ch) => {
                let start = i;
                while i < c.len() && is_ident(c[i]) {
                    i += 1;
                }
                let word: String = c[start..i].iter().collect();
                // A raw string: `r"..."`, `r#"..."#` or `br#"..."#`.
                if (word == "r" || word == "br") && matches!(at(i), '"' | '#') {
                    let mut hashes = 0;
                    while at(i) == '#' {
                        hashes += 1;
                        i += 1;
                    }
                    if at(i) == '"' {
                        i += 1;
                        while i < c.len()
                            && !(c[i] == '"' && (1..=hashes).all(|k| at(i + k) == '#'))
                        {
                            i += 1;
                        }
                        i += 1 + hashes;
                        tokens.push(c[from..i.min(c.len())].iter().collect());
                        continue;
                    }
                }
                tokens.push(word);
            }
            (ch, _) => {
                if !ch.is_whitespace() {
                    tokens.push(ch.to_string());
                }
                i += 1;
            }
        }
    }
    tokens
}

/// The first trust escape in `code`, if any.
///
/// An attribute outside [`ALLOWED_ATTRIBUTES`] reports as `#[<name>]`. A
/// name that holds a part of [`TRUST_ESCAPES`] reports as itself.
fn trust_escape(code: &str) -> Option<String> {
    let tokens = code_tokens(code);
    for (i, t) in tokens.iter().enumerate() {
        if t != "#" {
            continue;
        }
        // `#[...]` or `#![...]`.
        let open = if tokens.get(i + 1).is_some_and(|n| n == "!") {
            i + 2
        } else {
            i + 1
        };
        if tokens.get(open).is_some_and(|n| n == "[") {
            let name = tokens.get(open + 1).map_or("", String::as_str);
            if !ALLOWED_ATTRIBUTES.contains(&name) {
                return Some(format!("#[{name}]"));
            }
        }
    }
    tokens
        .into_iter()
        .filter(|t| t.chars().all(|ch| ch.is_alphanumeric() || ch == '_'))
        .find(|word| TRUST_ESCAPES.iter().any(|part| word.contains(part)))
}

/// Checks that a proof file holds `use` items and then one `verus! { }`
/// block, with nothing after the block.
///
/// Verus checks only the code inside `verus!`. A function outside the
/// block compiles as plain Rust, so a mirror moved there proves nothing.
/// With this shape, every function in the file is inside the block.
fn proof_file_shape(src: &str) -> Result<(), String> {
    let tokens = code_tokens(src);
    let mut i = 0;
    while tokens.get(i).is_some_and(|t| t == "use") {
        let semi = tokens[i..]
            .iter()
            .position(|t| t == ";")
            .ok_or("a `use` item has no `;`")?;
        i += semi + 1;
    }
    let head: Vec<&str> = tokens[i..].iter().take(3).map(String::as_str).collect();
    if head != ["verus", "!", "{"] {
        return Err(format!(
            "want `verus! {{` after the `use` items, found `{}`",
            head.join(" ")
        ));
    }
    let close = matching_brace(&tokens, i + 2);
    if close + 1 != tokens.len() {
        return Err(format!(
            "code follows the `verus!` block: `{}`",
            tokens[close + 1]
        ));
    }
    // `mod m;` and `include!` pull in another file. No guard reads that
    // file, so it could hold an unproved step.
    for (j, t) in tokens.iter().enumerate() {
        if t == "mod" && tokens.get(j + 2).is_some_and(|n| n == ";") {
            return Err(format!(
                "`mod {};` loads a file that no guard reads",
                tokens[j + 1]
            ));
        }
        if t.starts_with("include") && tokens.get(j + 1).is_some_and(|n| n == "!") {
            return Err(format!("`{t}!` loads a file that no guard reads"));
        }
    }
    Ok(())
}

/// The modules that a crate root declares, in order.
///
/// The root may hold only `mod <name>;` and `pub mod <name>;` items, and
/// comments. Any other token is an error. An attribute such as
/// `#[cfg(any())]` or `#[path = "..."]` could hide a module from Verus, so
/// none may appear.
fn root_modules(src: &str) -> Result<Vec<String>, String> {
    let tokens = code_tokens(src);
    let mut modules = Vec::new();
    let mut i = 0;
    while i < tokens.len() {
        let start = if tokens[i] == "pub" { i + 1 } else { i };
        match tokens.get(start..start + 3) {
            Some([kw, name, semi])
                if kw == "mod"
                    && semi == ";"
                    && name.chars().all(|ch| ch.is_alphanumeric() || ch == '_') =>
            {
                modules.push(name.clone());
                i = start + 3;
            }
            _ => {
                let rest: Vec<&str> = tokens[i..].iter().take(8).map(String::as_str).collect();
                return Err(format!("unexpected `{}`", rest.join(" ")));
            }
        }
    }
    Ok(modules)
}

#[test]
fn no_verus_proof_trusts_an_unproved_step() {
    for file in verus_files() {
        if let Some(escape) = trust_escape(&read(&file)) {
            panic!("{file} uses `{escape}`, so a proof can skip a step");
        }
    }
}

#[test]
fn the_trust_escape_check_lexes_the_source() {
    let hidden = [
        ("proof { assume (false); }", "assume"),
        ("proof { admit\n(); }", "admit"),
        ("proof { assume/* hidden */(false); }", "assume"),
        ("let s = \"//\"; assume(false);", "assume"),
        ("let s = \"/*\"; admit(); // */", "admit"),
        ("use vstd::prelude::assume as a;", "assume"),
        ("pub axiom fn bogus()\n    ensures false;", "axiom"),
        (
            "pub broadcast axiom fn bogus()\n    ensures false;",
            "axiom",
        ),
        (
            "assume_specification[ f ](x: u8) -> u8;",
            "assume_specification",
        ),
        (
            "use vstd::external_trait_specification as e;",
            "external_trait_specification",
        ),
        ("#[verifier :: external]\nfn f() {}", "#[verifier]"),
        ("#[verifier(external_body)]\nfn f() {}", "#[verifier]"),
        (
            "#[verifier::external_trait_specification]\npub trait T {}",
            "#[verifier]",
        ),
        ("#![verifier::truncate]", "#[verifier]"),
        ("#[cfg(any())]\nverus! { }", "#[cfg]"),
        ("macro_rules! hide { ($($t:tt)*) => {}; }", "macro_rules"),
        (
            "fn f<'a>(x: &'a u8) { let q = '\"'; assume(false); }",
            "assume",
        ),
    ];
    for (code, escape) in hidden {
        assert_eq!(trust_escape(code).as_deref(), Some(escape), "{code}");
    }
    let clean = [
        "// assume (false)\nfn f() {}",
        "/* outer /* assume(x) */ admit() */ fn f() {}",
        "let s = \"assume(false)\";",
        "let s = r#\"admit() \" assume\"#;",
        "let c = '\\''; let d = 'x';",
        "proof { assert(x == x); }",
        "forall|x: u64| #[trigger] f(x) == x",
        "// #[verifier::external_body]\nfn f() {}",
    ];
    for code in clean {
        assert_eq!(trust_escape(code), None, "{code}");
    }
}

#[test]
fn every_verus_proof_file_is_one_verus_block() {
    for file in verus_files() {
        if file.ends_with("/lib.rs") {
            continue;
        }
        if let Err(e) = proof_file_shape(&read(&file)) {
            panic!("{file} must hold `use` items and one `verus! {{ }}` block: {e}");
        }
    }
}

#[test]
fn the_proof_file_shape_check_finds_code_outside_the_block() {
    let ok = "use vstd::prelude::*;\n// note\nverus! {\nfn f() {}\n} // verus!\n";
    assert_eq!(proof_file_shape(ok), Ok(()));
    for bad in [
        "use vstd::prelude::*;\nfn f() {}\nverus! { }",
        "use vstd::prelude::*;\nverus! { }\nfn f() {}",
        "verus! { }\nverus! { }",
        "fn f() {}",
        "use a::b",
        "use vstd::prelude::*;\nverus! {\nmod hidden;\n}",
        "verus! {\npub mod hidden ;\n}",
        "verus! {\ninclude!(\"hidden.rs\");\n}",
    ] {
        assert!(proof_file_shape(bad).is_err(), "{bad}");
    }
}

#[test]
fn every_verus_module_is_in_the_crate_root() {
    let root = root_modules(&read("formal/verus/src/lib.rs")).unwrap_or_else(|e| {
        panic!("formal/verus/src/lib.rs may hold only `mod <name>;` items: {e}")
    });
    for file in verus_files() {
        let rel = file.strip_prefix("formal/verus/src/").unwrap_or(&file);
        assert!(
            !rel.contains('/'),
            "{file} is nested: keep every Verus file directly in formal/verus/src, where the guards expect it"
        );
        let stem = Path::new(&file)
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or_default();
        if stem != "lib" {
            assert!(
                root.iter().any(|m| m == stem),
                "formal/verus/src/lib.rs must declare `mod {stem};`, or Verus never checks {file}"
            );
        }
    }
}

#[test]
fn ci_runs_the_verus_proofs() {
    let ci = parse_workflow(".github/workflows/ci.yml");
    if let Some(defect) = formal_job_defect(&ci, "verus", "scripts/check-verus-proofs.sh") {
        panic!("{defect}");
    }
    let script = read("scripts/check-verus-proofs.sh");
    assert!(script.contains("formal/verus/src/lib.rs"));
    assert!(
        script.contains("sha256sum"),
        "the Verus download must be pinned"
    );
    assert!(script.contains("verified, 0 errors"));
}

#[test]
fn formal_methods_doc_names_every_verus_file_and_mirror() {
    let doc = read("docs/testing/formal-methods.md");
    for file in verus_files() {
        let name = Path::new(&file)
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or_default()
            .to_string();
        if name != "lib.rs" {
            assert!(
                doc.contains(&format!("`{name}`")),
                "docs/testing/formal-methods.md does not name `{name}`"
            );
        }
    }
    for m in mirrors() {
        assert!(
            doc.contains(&format!("`{}`", m.source_fn)),
            "docs/testing/formal-methods.md does not name `{}`",
            m.source_fn
        );
    }
}

#[test]
fn the_crate_root_check_counts_only_active_modules() {
    let ok = |src: &str| root_modules(src).expect(src);
    assert_eq!(ok("mod a;\npub mod b;"), ["a", "b"]);
    assert_eq!(ok("mod a;\n// mod b;\n/* mod c; */"), ["a"]);
    for bad in [
        "#[cfg(any())] mod a;",
        "#[path = \"other.rs\"] mod a;",
        "mod a { }",
        "fn f() {}",
    ] {
        assert!(root_modules(bad).is_err(), "{bad}");
    }
}
