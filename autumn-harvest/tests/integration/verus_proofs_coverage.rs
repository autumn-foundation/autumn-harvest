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

/// The tokens of a body, without comments, ghost blocks or whitespace.
fn normalize(body: &[String]) -> Vec<String> {
    strip_proof_blocks(body.to_vec())
}

fn read(rel: &str) -> String {
    let path = repo_root().join(rel);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// The `.rs` files under `formal/verus/src`, relative to the repo root.
fn verus_files() -> Vec<String> {
    let dir = repo_root().join("formal/verus/src");
    let mut files: Vec<String> = std::fs::read_dir(&dir)
        .expect("read formal/verus/src")
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "rs"))
        .filter_map(|p| Some(format!("formal/verus/src/{}", p.file_name()?.to_str()?)))
        .collect();
    files.sort();
    files
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

#[test]
fn every_mirrored_source_names_its_verus_proof() {
    for m in mirrors() {
        let source = read(&m.source_file);
        assert!(
            source.contains(&m.verus_file),
            "{} must name {}, so an editor of `{}` finds its proof",
            m.source_file,
            m.verus_file,
            m.source_fn
        );
    }
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
