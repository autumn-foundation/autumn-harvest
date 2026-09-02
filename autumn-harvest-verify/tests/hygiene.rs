//! Source hygiene (D2): the verifier must not be able to abort the build it is
//! auditing. Zero `unwrap`/`expect`/`panic!`/`todo!`/`unimplemented!`/
//! `unreachable!` in non-test code — a parser boundary is reported as
//! `BoundaryKind::MirParse`, never as a crash.
//!
//! RED phase: the scaffold is a wall of `todo!("RED phase: implemented in GREEN")`.

use std::path::{Path, PathBuf};

const FORBIDDEN: &[&str] = &[
    ".unwrap(",
    ".expect(",
    "panic!(",
    "todo!(",
    "unimplemented!(",
    "unreachable!(",
];

fn src_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("src")
}

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries =
        std::fs::read_dir(dir).unwrap_or_else(|e| panic!("read_dir {}: {e}", dir.display()));
    let mut paths: Vec<PathBuf> = entries
        .filter_map(std::result::Result::ok)
        .map(|e| e.path())
        .collect();
    paths.sort();
    for path in paths {
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// Line numbers (1-based) that belong to a `#[cfg(test)]` module, found by
/// brace-matching from the module's opening brace.
fn cfg_test_lines(text: &str) -> Vec<bool> {
    let lines: Vec<&str> = text.lines().collect();
    let mut masked = vec![false; lines.len()];
    let mut i = 0;
    while i < lines.len() {
        if lines[i].trim_start().starts_with("#[cfg(test)]") {
            // Find the `{` that opens the module/block and brace-match to its close.
            let mut depth = 0_i32;
            let mut started = false;
            let mut j = i;
            while j < lines.len() {
                for c in lines[j].chars() {
                    if c == '{' {
                        depth += 1;
                        started = true;
                    } else if c == '}' {
                        depth -= 1;
                    }
                }
                masked[j] = true;
                if started && depth <= 0 {
                    break;
                }
                j += 1;
            }
            i = j + 1;
        } else {
            i += 1;
        }
    }
    masked
}

fn is_comment(line: &str) -> bool {
    let t = line.trim_start();
    t.starts_with("//") || t.starts_with("/*") || t.starts_with('*')
}

#[test]
fn no_panicking_constructs_outside_test_modules() {
    let mut files = Vec::new();
    rust_files(&src_dir(), &mut files);
    assert!(
        !files.is_empty(),
        "no .rs files under {}",
        src_dir().display()
    );

    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut offenders: Vec<String> = Vec::new();

    for file in &files {
        let text = std::fs::read_to_string(file)
            .unwrap_or_else(|e| panic!("read {}: {e}", file.display()));
        let masked = cfg_test_lines(&text);
        for (idx, line) in text.lines().enumerate() {
            if masked.get(idx).copied().unwrap_or(false) || is_comment(line) {
                continue;
            }
            for needle in FORBIDDEN {
                if line.contains(needle) {
                    let rel = file.strip_prefix(root).unwrap_or(file);
                    offenders.push(format!(
                        "{}:{}: {needle}  |  {}",
                        rel.display(),
                        idx + 1,
                        line.trim()
                    ));
                }
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "the verifier must never panic on its input (D2); {} offending line(s):\n{}",
        offenders.len(),
        offenders.join("\n")
    );
}

#[test]
fn no_indexing_that_can_panic_on_untrusted_input() {
    // A cheap proxy for "no index-panics": direct slicing of the MIR text.
    let mut files = Vec::new();
    rust_files(&src_dir().join("mir"), &mut files);
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut offenders: Vec<String> = Vec::new();
    for file in &files {
        let text = std::fs::read_to_string(file)
            .unwrap_or_else(|e| panic!("read {}: {e}", file.display()));
        let masked = cfg_test_lines(&text);
        for (idx, line) in text.lines().enumerate() {
            if masked.get(idx).copied().unwrap_or(false) || is_comment(line) {
                continue;
            }
            // `text[a..b]` on a &str panics on a non-char-boundary or out-of-range.
            if line.contains("text[") || line.contains("line[") || line.contains("rest[") {
                let rel = file.strip_prefix(root).unwrap_or(file);
                offenders.push(format!("{}:{}: {}", rel.display(), idx + 1, line.trim()));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "slice the MIR text with `get(..)`, not `[..]` — truncated dumps are a supported input:\n{}",
        offenders.join("\n")
    );
}

#[test]
fn the_cfg_test_masker_itself_works() {
    let sample = "\
fn a() { let _ = 1; }
#[cfg(test)]
mod tests {
    #[test]
    fn t() { let x: Option<u8> = None; let _ = x.unwrap(); }
}
fn b() { let _ = 2; }
";
    let masked = cfg_test_lines(sample);
    assert_eq!(masked, vec![false, true, true, true, true, true, false]);
}
