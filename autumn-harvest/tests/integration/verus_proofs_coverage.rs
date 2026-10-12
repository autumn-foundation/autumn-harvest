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

/// Identifiers that let a proof skip a step. None may appear in code.
///
/// The guard lexes each file, so comments and literals do not count. A
/// space or a comment between two tokens cannot hide a name. The guard
/// bans the name itself, so a `use` alias cannot hide it either.
const TRUST_ESCAPES: &[&str] = &[
    "assume",
    "admit",
    // `axiom fn f() ensures false;` verifies, and so does every proof
    // that calls it.
    "axiom",
    "external_body",
    "external",
    "external_fn_specification",
    "external_type_specification",
    "assume_specification",
    "truncate",
];

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
    verus_body: String,
}

/// Removes `//` comments. No mirrored body holds `//` inside a string.
fn strip_comments(text: &str) -> String {
    text.lines()
        .map(|l| l.split("//").next().unwrap_or_default())
        .collect::<Vec<_>>()
        .join("\n")
}

/// The index just past the brace that closes the one at `open`.
fn matching_brace(text: &str, open: usize) -> usize {
    let mut depth = 0_usize;
    for (i, c) in text[open..].char_indices() {
        match c {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return open + i + 1;
                }
            }
            _ => {}
        }
    }
    panic!("unbalanced braces after byte {open}");
}

/// The body of the first `fn <name>(` in `text`, with its braces.
///
/// The body opens at the first `{` outside parentheses. A Verus `ensures`
/// clause can hold a block, but only inside parentheses.
fn fn_body(text: &str, name: &str) -> Option<String> {
    let start = text.find(&format!("fn {name}("))?;
    let mut parens = 0_i32;
    for (i, c) in text[start..].char_indices() {
        match c {
            '(' => parens += 1,
            ')' => parens -= 1,
            '{' if parens == 0 => {
                let open = start + i;
                return Some(text[open..matching_brace(text, open)].to_string());
            }
            _ => {}
        }
    }
    None
}

/// Drops each `proof { ... }` block. Ghost code has no runtime effect.
fn strip_proof_blocks(body: &str) -> String {
    let mut out = body.to_string();
    while let Some(at) = find_proof_block(&out) {
        let open = at + out[at..].find('{').expect("a proof block has a brace");
        let end = matching_brace(&out, open);
        out.replace_range(at..end, "");
    }
    out
}

/// The byte index of the next `proof {` keyword in `text`.
fn find_proof_block(text: &str) -> Option<usize> {
    let mut from = 0;
    while let Some(i) = text[from..].find("proof") {
        let at = from + i;
        let before_ok = text[..at]
            .chars()
            .next_back()
            .is_none_or(|c| !(c.is_alphanumeric() || c == '_'));
        let after = text[at + "proof".len()..].trim_start();
        if before_ok && after.starts_with('{') {
            return Some(at);
        }
        from = at + 1;
    }
    None
}

/// A body with comments, ghost blocks and whitespace removed.
fn normalize(body: &str) -> String {
    strip_proof_blocks(&strip_comments(body))
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect()
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
            let copy = after
                .find("fn ")
                .map(|i| &after[i + 3..])
                .and_then(|s| s.split('(').next())
                .unwrap_or_else(|| panic!("{verus_file}: `{line}` precedes no fn"));
            let body = fn_body(after, copy.trim())
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

#[test]
fn normalize_drops_comments_ghost_blocks_and_whitespace() {
    let a = "{\n    let x = 1; // one\n    proof { lemma(x); }\n    x + 1\n}";
    let b = "{ let x = 1;\n x + 1 }";
    assert_eq!(normalize(a), normalize(b));
    assert_ne!(normalize("{ x + 1 }"), normalize("{ x + 2 }"));
    // A name that only starts with `proof` is code, not a ghost block.
    assert_eq!(normalize("{ proofs { 1 } }"), "{proofs{1}}");
}

#[test]
fn fn_body_skips_blocks_inside_an_ensures_clause() {
    let text = "fn f(x: u8) -> (r: u8)\n    ensures ({ let y = x; r == y }),\n{\n    x\n}\n";
    assert_eq!(fn_body(text, "f").as_deref(), Some("{\n    x\n}"));
    assert_eq!(
        fn_body("const fn g() -> u8 { 1 }", "g").as_deref(),
        Some("{ 1 }")
    );
    assert_eq!(fn_body("fn g() {}", "h"), None);
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
        let body = fn_body(&source, &m.source_fn).unwrap_or_else(|| {
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

/// The tokens of `src` in order. A token is an identifier or one
/// punctuation char. Comments, nested block comments and whitespace yield
/// none. A string, raw string or char literal yields the token `<lit>`.
fn code_tokens(src: &str) -> Vec<String> {
    let c: Vec<char> = src.chars().collect();
    let at = |i: usize| c.get(i).copied().unwrap_or('\0');
    let is_ident = |ch: char| ch.is_alphanumeric() || ch == '_';
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < c.len() {
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
                tokens.push(LITERAL.to_owned());
            }
            // A char literal. A lifetime has no closing quote, so it
            // falls through and its name lexes as an identifier.
            ('\'', '\\') => {
                i += 2;
                while i < c.len() && c[i] != '\'' {
                    i += if c[i] == '\\' { 2 } else { 1 };
                }
                i += 1;
                tokens.push(LITERAL.to_owned());
            }
            ('\'', _) if at(i + 2) == '\'' => {
                i += 3;
                tokens.push(LITERAL.to_owned());
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
                        tokens.push(LITERAL.to_owned());
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

/// The token that [`code_tokens`] yields for a literal.
const LITERAL: &str = "<lit>";

/// The first trust escape in `code`, if any.
fn trust_escape(code: &str) -> Option<&'static str> {
    code_tokens(code)
        .iter()
        .find_map(|word| TRUST_ESCAPES.iter().copied().find(|e| e == word))
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
        ("#[verifier :: external]\nfn f() {}", "external"),
        ("#[verifier(external_body)]\nfn f() {}", "external_body"),
        (
            "fn f<'a>(x: &'a u8) { let q = '\"'; assume(false); }",
            "assume",
        ),
    ];
    for (code, escape) in hidden {
        assert_eq!(trust_escape(code), Some(escape), "{code}");
    }
    let clean = [
        "// assume (false)\nfn f() {}",
        "/* outer /* assume(x) */ admit() */ fn f() {}",
        "let s = \"assume(false)\";",
        "let s = r#\"admit() \" assume\"#;",
        "let c = '\\''; let d = 'x';",
        "proof { assert(x == x); }",
    ];
    for code in clean {
        assert_eq!(trust_escape(code), None, "{code}");
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
