//! `cargo xtask audit standalone-chapter-sync`: the standalone chapter shows
//! the bytes that CI runs (issue #1614).
//!
//! This is the port of `docs/audits/standalone-chapter-sync.py`. It is
//! deterministic. It needs no network and no build.
//!
//! The standalone getting-started chapter shows a real crate,
//! `examples/standalone-quickstart`. CI compiles that crate and runs the shell
//! blocks of the chapter with `cargo xtask dev run-standalone-chapter`. This
//! audit makes sure that the chapter shows the same bytes that CI runs, and
//! that no shell block escapes the run.
//!
//! It checks these items:
//!
//! 1. The chapter has no CR byte. The runner reads the chapter line by line,
//!    so a CR would hide a marker from it. The shell runner read lines with
//!    awk.
//! 2. Each `<!-- sync: PATH -->` marker has a fenced block after it. The block
//!    body is identical to the file at PATH. Each file in `REQUIRED_SYNC` has a
//!    marker, so a deleted marker cannot hide a stale block.
//! 3. Each shell fence has a `<!-- chapter-run: KIND -->` marker before it.
//!    Each marker parses, and the chapter has each kind that the runner needs.
//! 4. The first Rust block of Chapter 2 appears verbatim in the crate. The fork
//!    then really runs "the same first workflow".
//! 5. `docs/embedding.md` has no Rust fence that rustdoc skips. The
//!    `EmbeddingDocSnippets` doctest then compiles every Rust block in it.
//! 6. The doctest harness and the CI steps that run these guards exist, and
//!    are not commented out.
//! 7. The CI Postgres service matches `compose.yaml`, so CI tests the database
//!    that the chapter starts.
//!
//! Usage: `cargo xtask audit standalone-chapter-sync [--self-test]`.

use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::process::ExitCode;
use std::sync::LazyLock;

use regex::Regex;
use smop::prelude::*;

use super::workflow_yaml_parse::pytext::{repr_str, strip};
use crate::util;

/// Arguments of `cargo xtask audit standalone-chapter-sync`.
#[derive(Debug, Args)]
pub struct Args {
    /// Run the checks against fixtures instead of the repository.
    #[arg(long)]
    pub self_test: bool,
}

const CHAPTER: &str = "docs/getting-started/standalone-axum.md";
const EMBEDDING: &str = "docs/embedding.md";
const CRATE: &str = "examples/standalone-quickstart";
const CHAPTER_2: &str = "docs/getting-started/02-first-workflow.md";
const CHAPTER_2_FILE: &str = "examples/standalone-quickstart/src/workflows.rs";
const COMPOSE: &str = "examples/standalone-quickstart/compose.yaml";
const CI: &str = ".github/workflows/ci.yml";
const CI_JOB: &str = "standalone-chapter";

const REQUIRED_SYNC: [&str; 4] = [
    "examples/standalone-quickstart/Cargo.toml",
    COMPOSE,
    "examples/standalone-quickstart/src/workflows.rs",
    "examples/standalone-quickstart/src/main.rs",
];
const REQUIRED_RUN_KINDS: [&str; 3] = ["serve", "expect", "preflight"];

/// Each pair is (file, line). The line must appear in the file, uncommented.
const REQUIRED_WIRING: [(&str, &str); 5] = [
    (
        "autumn-harvest-plugin/src/lib.rs",
        r#"#[cfg(all(doctest, feature = "metrics", feature = "webhooks"))]"#,
    ),
    (
        "autumn-harvest-plugin/src/lib.rs",
        r#"#[doc = include_str!("../../docs/embedding.md")]"#,
    ),
    (CI, "cargo xtask audit standalone-chapter-sync"),
    (
        CI,
        "cargo test -p autumn-harvest-plugin --features metrics,webhooks --doc EmbeddingDocSnippets 2>&1 | tee embedding-doctests.log",
    ),
    (CI, "run: cargo xtask dev run-standalone-chapter"),
];

const SHELL_LANGS: [&str; 5] = ["bash", "sh", "shell", "console", "zsh"];
const RUST_SKIP_ATTRS: [&str; 2] = ["text", "compile_fail"];

/// Mirrors of the Docker Official Images. CI pulls an official image from one
/// of them, because Docker Hub rate-limits anonymous pulls. A mirror serves the
/// same image, so it matches `compose.yaml`.
const OFFICIAL_IMAGE_MIRRORS: [&str; 2] =
    ["mirror.gcr.io/library/", "public.ecr.aws/docker/library/"];

/// Compiles one of the fixed patterns of this module.
fn pattern(source: &str) -> Regex {
    Regex::new(source).unwrap_or_else(|error| unreachable!("pattern {source}: {error}"))
}

static SYNC_RE: LazyLock<Regex> = LazyLock::new(|| pattern(r"^<!-- sync: (\S+) -->$"));
static RUN_RE: LazyLock<Regex> =
    LazyLock::new(|| pattern(r"^<!-- chapter-run: (serve|preflight|expect \S.*|skip \S.*) -->$"));
static FENCE_OPEN_RE: LazyLock<Regex> = LazyLock::new(|| pattern(r"^ {0,3}```(.*)$"));
static WORD_SPLIT: LazyLock<Regex> = LazyLock::new(|| pattern(r"[,\s]+"));
static PORT_RE: LazyLock<Regex> = LazyLock::new(|| pattern(r"(\d+):5432"));
static JOB_HEADER_RE: LazyLock<Regex> = LazyLock::new(|| pattern(r"^  [\w-]+:\n"));

/// Returns the info string of an opening fence, or `None`.
fn fence_info(line: &str) -> Option<String> {
    FENCE_OPEN_RE
        .captures(line)
        .map(|opener| strip(&opener[1]).to_owned())
}

/// Returns (info string, body) of the first fence at or after `start`.
///
/// Only blank lines can stand between the marker and the fence.
fn next_block(lines: &[&str], start: usize) -> Option<(String, String)> {
    let mut index = start;
    while index < lines.len() && strip(lines[index]).is_empty() {
        index += 1;
    }
    let info = fence_info(lines.get(index)?)?;
    let mut body = String::new();
    for line in &lines[index + 1..] {
        if strip(line) == "```" {
            return Some((info, body));
        }
        body.push_str(line);
        body.push('\n');
    }
    None
}

/// The chapter has no CR byte.
fn check_line_endings(text: &str) -> Vec<String> {
    if text.contains('\r') {
        return vec![format!(
            "{CHAPTER}: contains a CR byte. Save it with LF line endings."
        )];
    }
    Vec::new()
}

/// Compares each synced block with its file. Returns errors and paths.
fn check_sync(
    chapter_text: &str,
    read_file: &dyn Fn(&str) -> Option<String>,
) -> (Vec<String>, BTreeSet<String>) {
    let mut errors = Vec::new();
    let mut seen = BTreeSet::new();
    let lines: Vec<&str> = chapter_text.split('\n').collect();
    for (index, line) in lines.iter().enumerate() {
        let number = index + 1;
        let Some(marker) = SYNC_RE.captures(line) else {
            continue;
        };
        let path = &marker[1];
        seen.insert(path.to_owned());
        let Some((_, body)) = next_block(&lines, number) else {
            errors.push(format!(
                "{CHAPTER}:{number}: no fenced block after sync marker for {path}"
            ));
            continue;
        };
        let Some(expected) = read_file(path) else {
            errors.push(format!("{CHAPTER}:{number}: {path} does not exist"));
            continue;
        };
        if body != expected {
            errors.push(format!(
                "{CHAPTER}:{number}: the block differs from {path}. Copy the file into the chapter, or the chapter into the file."
            ));
        }
    }
    (errors, seen)
}

/// Splits an info string into words as `re.split(r"[,\s]+", info)` does.
fn split_words(info: &str) -> Vec<&str> {
    WORD_SPLIT.split(info).collect()
}

fn is_shell_fence(info: &str) -> bool {
    split_words(info)
        .first()
        .is_some_and(|word| SHELL_LANGS.contains(word))
}

/// Each marker parses and has a bash block. Each shell fence has a marker.
fn check_run_markers(chapter_text: &str) -> Vec<String> {
    let mut errors = Vec::new();
    let mut kinds = BTreeSet::new();
    let mut marked_fences = BTreeSet::new();
    let lines: Vec<&str> = chapter_text.split('\n').collect();
    for (index, line) in lines.iter().enumerate() {
        let number = index + 1;
        if !line.contains("chapter-run") {
            continue;
        }
        let Some(marker) = RUN_RE.captures(line) else {
            errors.push(format!(
                "{CHAPTER}:{number}: a chapter-run marker does not parse: {}",
                repr_str(line)
            ));
            continue;
        };
        let kind = marker[1].split(' ').next().unwrap_or_default().to_owned();
        kinds.insert(kind);
        let mut fence = number;
        while fence < lines.len() && strip(lines[fence]).is_empty() {
            fence += 1;
        }
        marked_fences.insert(fence);
        if next_block(&lines, number).is_none_or(|(info, _)| info != "bash") {
            errors.push(format!(
                "{CHAPTER}:{number}: a chapter-run marker needs a bash block after it"
            ));
        }
    }

    let mut in_fence = false;
    for (index, line) in lines.iter().enumerate() {
        if in_fence {
            if strip(line) == "```" {
                in_fence = false;
            }
            continue;
        }
        let Some(info) = fence_info(line) else {
            continue;
        };
        in_fence = true;
        if is_shell_fence(&info) && !marked_fences.contains(&index) {
            errors.push(format!(
                "{CHAPTER}:{}: a shell block has no chapter-run marker. Mark it, or mark it `skip REASON`.",
                index + 1
            ));
        }
    }

    for kind in REQUIRED_RUN_KINDS {
        if !kinds.contains(kind) {
            errors.push(format!("{CHAPTER}: no <!-- chapter-run: {kind} --> marker"));
        }
    }
    errors
}

/// Renders a list of strings as Python `repr` of a `list`.
fn repr_list(items: &[&str]) -> String {
    let items: Vec<String> = items.iter().map(|item| repr_str(item)).collect();
    format!("[{}]", items.join(", "))
}

/// Every Rust fence in `embedding.md` is one that rustdoc compiles.
fn check_embedding_fences(text: &str) -> Vec<String> {
    let mut errors = Vec::new();
    for (index, line) in text.split('\n').enumerate() {
        let Some(info) = fence_info(line).filter(|info| !info.is_empty()) else {
            continue;
        };
        let words: Vec<&str> = split_words(&info)
            .into_iter()
            .filter(|word| !word.is_empty())
            .collect();
        if words.first() != Some(&"rust") {
            continue;
        }
        let skipped: Vec<&str> = words[1..]
            .iter()
            .copied()
            .filter(|word| word.starts_with("ignore") || RUST_SKIP_ATTRS.contains(word))
            .collect();
        if !skipped.is_empty() {
            errors.push(format!(
                "{EMBEDDING}:{}: a rust fence marked {} is not compiled. Make the block compile.",
                index + 1,
                repr_list(&skipped)
            ));
        }
    }
    errors
}

/// Returns the body of the first `rust` fence in `text`.
fn first_rust_block(text: &str) -> Option<String> {
    let lines: Vec<&str> = text.split('\n').collect();
    let index = lines.iter().position(|line| *line == "```rust")?;
    next_block(&lines, index).map(|(_, body)| body)
}

/// The Chapter 2 workflow is a verbatim part of the crate file.
fn check_same_workflow(chapter_2: Option<&str>, crate_file: Option<&str>) -> Vec<String> {
    let Some(block) = first_rust_block(chapter_2.unwrap_or_default()) else {
        return vec![format!("{CHAPTER_2}: no rust block found")];
    };
    if !crate_file.is_some_and(|file| file.contains(&block)) {
        return vec![format!(
            "{CHAPTER_2_FILE}: does not contain the first rust block of {CHAPTER_2} verbatim"
        )];
    }
    Vec::new()
}

/// Ports `re.search(rf"^\s*{key}:\s*(\S+)\s*$", compose, re.M)`.
fn compose_value(compose: &str, key: &str) -> Option<String> {
    let re = Regex::new(&format!(r"(?m)^\s*{key}:\s*(\S+)\s*$")).ok()?;
    re.captures(compose).map(|found| found[1].to_owned())
}

/// The CI service uses the image, the credentials and the port of `compose.yaml`.
fn check_service_matches_compose(compose: &str, ci_job: &str) -> Vec<String> {
    let mut errors = Vec::new();
    // Each entry holds the forms that satisfy one value. The first is shown.
    let mut needles: Vec<Vec<String>> = Vec::new();
    for key in ["image", "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB"] {
        let Some(value) = compose_value(compose, key) else {
            errors.push(format!("{COMPOSE}: no {key}"));
            continue;
        };
        let mut forms = vec![format!("{key}: {value}")];
        if key == "image" && !value.contains('/') {
            forms.extend(
                OFFICIAL_IMAGE_MIRRORS
                    .iter()
                    .map(|mirror| format!("{key}: {mirror}{value}")),
            );
        }
        needles.push(forms);
    }
    match PORT_RE.captures(compose) {
        None => errors.push(format!("{COMPOSE}: no published Postgres port")),
        Some(port) => needles.push(vec![format!("{}:5432", &port[1])]),
    }
    for forms in needles {
        if !forms.iter().any(|form| ci_job.contains(form.as_str())) {
            errors.push(format!(
                "{CI}: job {CI_JOB} does not have {} from {COMPOSE}",
                repr_str(&forms[0])
            ));
        }
    }
    errors
}

/// Returns the lines of one top-level job in `ci.yml`.
///
/// Python matched `^  JOB:\n(.*?)(?=^  [\w-]+:\n|\Z)`. The Rust engine has no
/// lookahead. So the port finds the header, then ends the body at the first
/// later line that opens another job.
fn ci_job_text(ci: &str, job: &str) -> String {
    let Ok(header) = Regex::new(&format!(r"(?m)^  {}:\n", regex::escape(job))) else {
        return String::new();
    };
    let Some(found) = header.find(ci) else {
        return String::new();
    };
    let body = &ci[found.end()..];
    let mut start = 0;
    loop {
        if JOB_HEADER_RE.is_match(&body[start..]) {
            return body[..start].to_owned();
        }
        match body[start..].find('\n') {
            Some(newline) => start += newline + 1,
            None => return body.to_owned(),
        }
    }
}

/// Returns true when `needle` is on a line that is not a YAML or Rust comment.
fn wiring_present(text: &str, needle: &str) -> bool {
    text.split('\n').any(|line| {
        let stripped = strip(line);
        let comment = (stripped.starts_with('#') || stripped.starts_with("//"))
            && !stripped.starts_with("#[");
        !comment && line.contains(needle)
    })
}

/// Reads a repository file with its line endings unchanged.
fn read_repo_file(root: &Path, path: &str) -> Option<String> {
    let target = root.join(path);
    if !target.is_file() {
        return None;
    }
    std::fs::read_to_string(target).ok()
}

/// Runs every check under `root`. Returns the errors.
fn audit(root: &Path) -> Vec<String> {
    let read = |path: &str| read_repo_file(root, path);
    let mut errors = Vec::new();
    match read(CHAPTER) {
        None => errors.push(format!("{CHAPTER}: not found")),
        Some(chapter) => {
            errors.extend(check_line_endings(&chapter));
            let (sync_errors, seen) = check_sync(&chapter, &read);
            errors.extend(sync_errors);
            for path in REQUIRED_SYNC {
                if !seen.contains(path) {
                    errors.push(format!("{CHAPTER}: no <!-- sync: {path} --> marker"));
                }
            }
            errors.extend(check_run_markers(&chapter));
        }
    }

    errors.extend(check_same_workflow(
        read(CHAPTER_2).as_deref(),
        read(CHAPTER_2_FILE).as_deref(),
    ));

    match read(EMBEDDING) {
        None => errors.push(format!("{EMBEDDING}: not found")),
        Some(embedding) => errors.extend(check_embedding_fences(&embedding)),
    }

    for (path, needle) in REQUIRED_WIRING {
        if !wiring_present(&read(path).unwrap_or_default(), needle) {
            errors.push(format!("{path}: missing {}", repr_str(needle)));
        }
    }

    errors.extend(check_service_matches_compose(
        &read(COMPOSE).unwrap_or_default(),
        &ci_job_text(&read(CI).unwrap_or_default(), CI_JOB),
    ));
    errors
}

/// Runs the fixtures. Returns the label of each check that failed.
///
/// Python ran these as `assert` statements. The port collects every failed
/// check, so one run names them all.
#[allow(clippy::too_many_lines)]
fn self_test_failures() -> Vec<&'static str> {
    let mut failed = Vec::new();
    let mut check = |ok: bool, label: &'static str| {
        if !ok {
            failed.push(label);
        }
    };
    let files: HashMap<&str, &str> = HashMap::from([("a.rs", "fn a() {}\n")]);
    let read = |path: &str| files.get(path).map(|text| (*text).to_owned());

    let good = "<!-- sync: a.rs -->\n\n```rust\nfn a() {}\n```\n";
    let (errors, seen) = check_sync(good, &read);
    check(
        errors.is_empty() && seen == BTreeSet::from(["a.rs".to_owned()]),
        "a synced block matches its file",
    );

    let stale = "<!-- sync: a.rs -->\n```rust\nfn b() {}\n```\n";
    let (errors, _) = check_sync(stale, &read);
    check(
        errors.len() == 1 && errors[0].contains("differs"),
        "a stale block differs",
    );

    let no_block = "<!-- sync: a.rs -->\nSome prose.\n```rust\nfn a() {}\n```\n";
    let (errors, _) = check_sync(no_block, &read);
    check(
        errors.len() == 1 && errors[0].contains("no fenced block"),
        "prose after a marker leaves it with no block",
    );

    let unclosed = "<!-- sync: a.rs -->\n```rust\nfn a() {}\n";
    check(
        check_sync(unclosed, &read).0.len() == 1,
        "an unclosed fence is no block",
    );

    let missing = "<!-- sync: b.rs -->\n```rust\n```\n";
    let (errors, _) = check_sync(missing, &read);
    check(
        errors.len() == 1 && errors[0].contains("does not exist"),
        "a marker for a missing file",
    );

    let runs = concat!(
        "<!-- chapter-run: serve -->\n```bash\ncargo run\n```\n",
        "<!-- chapter-run: expect sent -->\n```bash\ncurl x\n```\n",
        "<!-- chapter-run: preflight -->\n```bash\nharvest preflight\n```\n",
    );
    check(
        check_run_markers(runs).is_empty(),
        "every run kind is present",
    );
    let wrong_lang = "<!-- chapter-run: serve -->\n```sh\ncargo run\n```\n";
    check(
        check_run_markers(wrong_lang).len() == 3,
        "a marker needs a bash block",
    );

    let chapter_2 = "Text.\n\n```rust\nfn a() {}\n```\n\n```rust\nfn b() {}\n```\n";
    check(
        check_same_workflow(Some(chapter_2), Some("use x;\nfn a() {}\nfn c() {}\n")).is_empty(),
        "the first rust block is in the crate",
    );
    check(
        check_same_workflow(Some(chapter_2), Some("fn b() {}\n")).len() == 1,
        "only the first rust block counts",
    );
    check(
        check_same_workflow(Some("no code"), Some("fn a() {}\n")).len() == 1,
        "a chapter with no rust block",
    );

    check(
        check_embedding_fences("```rust\nfn a() {}\n```\n").is_empty(),
        "a plain rust fence compiles",
    );
    check(
        check_embedding_fences("```rust,no_run\nfn a() {}\n```\n").is_empty(),
        "a no_run fence compiles",
    );
    check(
        check_embedding_fences("```rust,ignore\nfn a() {}\n```\n").len() == 1,
        "an ignore fence is skipped",
    );
    // A CR anywhere in the chapter is a finding, because the runner sees it.
    check(
        check_line_endings("a\r\nb\n").len() == 1,
        "a CR byte is a finding",
    );
    // A line that mentions chapter-run but does not parse is a finding.
    check(
        !check_run_markers("<!--  chapter-run: serve -->\n```bash\nx\n```\n").is_empty(),
        "a marker with two spaces does not parse",
    );
    check(
        !check_run_markers("<!-- chapter-run: expect  -->\n```bash\nx\n```\n").is_empty(),
        "an expect marker needs its text",
    );
    check(
        !check_run_markers("<!-- chapter-run: serving -->\n```bash\nx\n```\n").is_empty(),
        "an unknown kind does not parse",
    );
    // Every shell fence needs a marker. `skip` with a reason is a marker.
    let unmarked = format!("{runs}```bash\ndocker compose up\n```\n");
    check(
        check_run_markers(&unmarked).len() == 1,
        "an unmarked bash fence",
    );
    let indented = format!("{runs}   ```sh\nls\n   ```\n");
    check(
        check_run_markers(&indented).len() == 1,
        "an unmarked indented sh fence",
    );
    let skipped =
        format!("{runs}<!-- chapter-run: skip CI uses a service. -->\n```bash\nup\n```\n");
    check(
        check_run_markers(&skipped).is_empty(),
        "a skip marker with a reason",
    );
    // rustdoc also skips these forms.
    check(
        check_embedding_fences("```rust, ignore\nfn a() {}\n```\n").len() == 1,
        "a comma and a space before ignore",
    );
    check(
        check_embedding_fences("```rust ignore\nfn a() {}\n```\n").len() == 1,
        "a space before ignore",
    );
    check(
        check_embedding_fences("  ```rust,ignore-wasm32\nfn a() {}\n  ```\n").len() == 1,
        "an ignore-target attribute",
    );
    // The CI service matches compose.yaml.
    let compose = concat!(
        "    image: postgres:16\n      POSTGRES_USER: u\n",
        "      POSTGRES_PASSWORD: p\n      POSTGRES_DB: d\n      - \"127.0.0.1:5435:5432\"\n",
    );
    let job =
        "image: postgres:16\nPOSTGRES_USER: u\nPOSTGRES_PASSWORD: p\nPOSTGRES_DB: d\n- 5435:5432\n";
    check(
        check_service_matches_compose(compose, job).is_empty(),
        "the CI service matches compose.yaml",
    );
    check(
        check_service_matches_compose(compose, &job.replace("16", "17")).len() == 1,
        "another image tag is a finding",
    );
    for mirror in OFFICIAL_IMAGE_MIRRORS {
        let mirrored = job.replace("image: ", &format!("image: {mirror}"));
        check(
            check_service_matches_compose(compose, &mirrored).is_empty(),
            "a mirror of the official image matches",
        );
    }
    let other = job.replace("image: ", "image: example.com/");
    check(
        check_service_matches_compose(compose, &other).len() == 1,
        "another registry is a finding",
    );
    // A commented-out CI line does not count as wiring.
    check(
        !wiring_present("# run: cargo test x\n", "run: cargo test x"),
        "a commented-out line is not wiring",
    );
    check(
        wiring_present("  run: cargo test x\n", "run: cargo test x"),
        "an active line is wiring",
    );
    failed
}

/// Runs `cargo xtask audit standalone-chapter-sync`.
///
/// No step can fail with an error, but each command keeps the signature that
/// the dispatcher of `cargo xtask audit` calls.
#[allow(clippy::unnecessary_wraps)]
pub fn run(args: Args) -> Result<ExitCode> {
    if args.self_test {
        let failed = self_test_failures();
        for label in &failed {
            eprintln!("standalone-chapter-sync self-test: failed: {label}");
        }
        if !failed.is_empty() {
            return Ok(ExitCode::FAILURE);
        }
        println!("standalone-chapter-sync self-test: OK");
        return Ok(ExitCode::SUCCESS);
    }
    let errors = audit(&util::repo_root());
    for error in &errors {
        eprintln!("{error}");
    }
    if !errors.is_empty() {
        eprintln!("standalone-chapter-sync: {} finding(s)", errors.len());
        return Ok(ExitCode::FAILURE);
    }
    println!("standalone-chapter-sync: OK");
    Ok(ExitCode::SUCCESS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_self_test_check_passes() {
        assert_eq!(self_test_failures(), Vec::<&str>::new());
    }

    #[test]
    fn the_job_text_ends_at_the_next_job() {
        let ci = "jobs:\n  a:\n    x: 1\n  standalone-chapter:\n    image: p\n\n    y: 2\n  next-job:\n    z: 3\n";
        assert_eq!(ci_job_text(ci, CI_JOB), "    image: p\n\n    y: 2\n");
        let last = "jobs:\n  standalone-chapter:\n    image: p\n";
        assert_eq!(ci_job_text(last, CI_JOB), "    image: p\n");
        assert_eq!(ci_job_text("jobs:\n  other:\n", CI_JOB), "");
        let empty = "jobs:\n  standalone-chapter:\n  b:\n";
        assert_eq!(ci_job_text(empty, CI_JOB), "");
    }

    #[test]
    fn a_compose_value_may_sit_on_the_next_line() {
        // Python `\s*` crosses a line end in multiline mode, and so does Rust.
        assert_eq!(
            compose_value("image:\n  postgres:16\n", "image").as_deref(),
            Some("postgres:16")
        );
        assert_eq!(compose_value("x: 1\n", "image"), None);
    }

    #[test]
    fn a_skipped_rust_fence_names_its_attributes() {
        assert_eq!(
            check_embedding_fences("x\n```rust,ignore,text\nfn a() {}\n```\n"),
            [
                "docs/embedding.md:2: a rust fence marked ['ignore', 'text'] is not compiled. Make the block compile."
            ]
        );
    }

    /// The wiring check reads any uncommented line. A step name that held the
    /// needle would satisfy it with every run line commented out.
    #[test]
    fn only_a_ci_run_line_holds_a_wiring_needle() {
        let ci = util::read_repo_file(CI).unwrap_or_else(|error| panic!("{error:#}"));
        for (_, needle) in REQUIRED_WIRING.iter().filter(|(path, _)| *path == CI) {
            let holders: Vec<&str> = ci
                .lines()
                .map(str::trim)
                .filter(|line| line.contains(needle) && !line.starts_with('#'))
                .collect();
            assert!(
                holders.iter().all(|line| !line.starts_with("- name:")),
                "a step name holds {needle:?}: {holders:?}"
            );
        }
        let commented = ci.replace(
            "          cargo xtask audit standalone-chapter-sync",
            "          # cargo xtask audit standalone-chapter-sync",
        );
        assert!(!wiring_present(
            &commented,
            "cargo xtask audit standalone-chapter-sync"
        ));
    }

    #[test]
    fn a_bad_marker_is_quoted_as_python_repr() {
        let errors = check_run_markers("<!-- chapter-run: it's -->\n");
        assert!(
            errors[0]
                .ends_with(": a chapter-run marker does not parse: \"<!-- chapter-run: it's -->\""),
            "{errors:?}"
        );
    }
}
