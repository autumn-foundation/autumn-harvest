//! `cargo xtask audit config-cli-drift`: config-key, env-var and CLI-flag drift scan for `docs/`.
//!
//! This is the Folio corpus harness, ported from `docs/audits/config-cli-drift.py`.
//! It is deterministic and reproducible on any checkout. It needs no network access.
//! It is the drift-scan Tier-1 audit of `docs/audits/README.md`. It checks the
//! docs corpus against the current public surface: the `[harvest]` TOML config
//! schema, its env-var overrides and the flags of the `harvest` CLI. It reports
//! each key, env var or flag that a doc cites and the source does not define.
//! An answer that was true once and is false now is worse than no answer. The
//! reader cannot see the difference. The reader follows it into a config file
//! or a command line that silently does nothing.
//!
//! The ground truth comes mechanically from source, never from a hand-kept list.
//!
//! - `autumn-harvest-plugin/src/config.rs`: the `PartialHarvestRuntimeConfig`
//!   struct tree gives the TOML config-key paths. That tree is what the
//!   `[harvest]` section of `autumn.toml` deserializes into. The `env.var("...")`
//!   calls give the `AUTUMN_*` env-var overrides.
//! - `autumn-harvest-cli/src/**/*.rs`: each `#[arg(...)]` field gives a CLI
//!   flag. An explicit `long = "..."` wins. Otherwise the flag is the field name
//!   in kebab case. Each `env = "..."` on such a field gives a CLI env var
//!   (`HARVEST_*`).
//!
//! Two doc-side checks run against that ground truth.
//!
//! 1. The config-key check reads dotted `harvest.foo.bar` keys only inside a
//!    fenced TOML block with a `[harvest...]` section header. The `[section]`
//!    headers and the `key = value` lines give a dotted path. The path is
//!    checked against the real schema. An earlier version also matched a bare
//!    inline `` `harvest.x.y = value` `` code span outside a TOML block. That
//!    went, because `OTel` span attributes and Prometheus expressions use the
//!    same shape in another namespace. Examples are `harvest.replay = true` in
//!    a trace excerpt and `harvest.replication.standbys == 0` in an alert. Each
//!    such hit was a false positive, not drift.
//! 2. The env-var check reads each `AUTUMN_HARVEST[A-Z0-9_]*` token anywhere in
//!    the page. `ENV_TOKEN` says why a bare `HARVEST_*` is out of scope. The
//!    check also reads each `--flag-name` token on a line that runs the
//!    `harvest` binary inside a fenced code block.
//!
//! # Known limitations
//!
//! - The CLI-flag check does not know commands. It checks that a flag exists
//!   somewhere on the `harvest` CLI, not on the subcommand of the example. A
//!   flag of one subcommand that a doc puts on another is not caught. That
//!   needs a real clap parse or a per-subcommand field map, which is a bigger
//!   scope. Each drift found so far was a flag that the CLI lacks entirely,
//!   and this check catches that.
//! - The config-key extraction walks only `PartialHarvestRuntimeConfig` and the
//!   structs that it reaches. A config surface outside that tree, such as the
//!   section of another plugin, is invisible here. That is out of scope: this
//!   audit covers the Harvest config and CLI surface.
//! - The env-var check covers `AUTUMN_HARVEST*` only. A real drift in a bare
//!   `HARVEST_*` CLI var, such as `HARVEST_TOKEN`, is not caught. That is
//!   accepted. The namespace is not closed, because many unrelated tools mint
//!   `HARVEST_*` vars. A check of it gave only false positives.
//!
//! # Usage
//!
//! ```sh
//! cargo xtask audit config-cli-drift [--json]
//! ```
//!
//! The exit code is 1 if a drift hit is in the reader corpus. The corpus and
//! process-artifact split is the one of `cargo xtask audit corpus-link-check`
//! (see `PROCESS_ARTIFACT_PREFIXES` there). A hit inside a process artifact is
//! reported but does not fail the run.

use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use regex::Regex;
use smop::prelude::*;

use super::corpus_link_check::pycompat::{self, Json};
use super::corpus_link_check::{self, is_fence_close};
use crate::util;

/// Arguments of `cargo xtask audit config-cli-drift`.
#[derive(Debug, Args)]
pub struct Args {
    /// Print the result as JSON.
    #[arg(long)]
    pub json: bool,
}

/// The config source, relative to the repository root.
const CONFIG_RS: &str = "autumn-harvest-plugin/src/config.rs";

/// Each CLI source file sits under this directory.
///
/// A subcommand module such as `agent_profile.rs` (issue #2394) declares its
/// own `#[arg(...)]` fields.
const CLI_SRC: &str = "autumn-harvest-cli/src";

/// The fenced-block languages that hold runnable shell content.
pub const INVOCATION_LANGS: [&str; 6] = ["bash", "sh", "shell", "console", "", "text"];

/// The compiled patterns of the drift scan.
///
/// CLI env vars on `Cli` or subcommand `#[arg(env = ...)]` fields use a bare
/// `HARVEST_*` prefix, with no `AUTUMN_`. The `harvest` binary reads them
/// itself, not through the config layer of Autumn. The same scan of the CLI
/// sources collects them with the flags, so no separate list is necessary.
pub struct Patterns {
    field: Regex,
    strukt: Regex,
    option: Regex,
    env_var_call: Regex,
    arg_attr: Regex,
    long_name: Regex,
    bare_long: Regex,
    env_name: Regex,
    next_field: Regex,
    blockquote: Regex,
    fence_open: Regex,
    section_header: Regex,
    toml_kv: Regex,
    flag_token: Regex,
}

impl Patterns {
    /// Compiles each pattern.
    pub fn new() -> Result<Self> {
        Ok(Self {
            field: pycompat::regex(r"^\s*(?:pub\s+)?(\w+)\s*:\s*([\w:<>]+)\s*,?\s*$")?,
            strukt: pycompat::regex(r"(?s)struct\s+(\w+)\s*\{(.*?)\n\}")?,
            option: pycompat::regex(r"^Option<(.+)>$")?,
            env_var_call: pycompat::regex(r#"env\.var\("([A-Z0-9_]+)"\)"#)?,
            arg_attr: pycompat::regex(r"(?s)#\[arg\((.*?)\)\]")?,
            long_name: pycompat::regex(r#"long\s*=\s*"([^"]+)""#)?,
            bare_long: pycompat::regex(r"(?:^|,)\s*long\s*(?:,|$)")?,
            env_name: pycompat::regex(r#"env\s*=\s*"([A-Z0-9_]+)""#)?,
            next_field: pycompat::regex(r"^\s*(?:pub\s+)?(\w+)\s*:")?,
            blockquote: pycompat::regex(r"^(>[ \t]*)")?,
            fence_open: pycompat::regex(r"^[ \t]{0,3}(`{3,}|~{3,})\s*([\w+-]*)")?,
            section_header: pycompat::regex(r"^\s*\[([\w.]+)\]\s*$")?,
            toml_kv: pycompat::regex(r"^\s*([\w.]+)\s*=")?,
            flag_token: pycompat::regex(r"--([a-z][a-z0-9-]*)")?,
        })
    }

    /// Returns each `--flag` name in `text`, in order.
    pub fn flag_tokens<'h>(&self, text: &'h str) -> Vec<&'h str> {
        self.flag_token
            .captures_iter(text)
            .filter_map(|c| c.get(1).map(|m| m.as_str()))
            .collect()
    }
}

/// Returns the bare type name of a field type.
///
/// `Option<Foo>` gives `Foo`. Config fields are only bare or wrapped once in
/// `Option` in `config.rs`, with no `Vec` or `HashMap`.
fn bare_type_name(pats: &Patterns, ty: &str) -> String {
    let inner = pats
        .option
        .captures(ty)
        .and_then(|c| c.get(1))
        .map_or(ty, |m| m.as_str());
    inner.rsplit("::").next().unwrap_or(inner).to_owned()
}

/// Maps each struct name to its `(field, bare type)` pairs.
///
/// This reads each plain `struct Name { ... }` of the file. No struct body in
/// `config.rs` holds a nested `{}`.
fn parse_rust_structs(pats: &Patterns, text: &str) -> HashMap<String, Vec<(String, String)>> {
    let mut structs = HashMap::new();
    for caps in pats.strukt.captures_iter(text) {
        let name = caps.get(1).map_or("", |m| m.as_str());
        let body = caps.get(2).map_or("", |m| m.as_str());
        let mut fields = Vec::new();
        for line in pycompat::splitlines(body) {
            let line = pycompat::strip(line);
            if line.is_empty() || line.starts_with("//") || line.starts_with("#[") {
                continue;
            }
            if let Some(fm) = pats.field.captures(line) {
                let field = fm.get(1).map_or("", |m| m.as_str());
                let ty = fm.get(2).map_or("", |m| m.as_str());
                fields.push((field.to_owned(), bare_type_name(pats, ty)));
            }
        }
        structs.insert(name.to_owned(), fields);
    }
    structs
}

/// Walks the struct tree into dotted `harvest.x.y` paths.
///
/// A field whose bare type is a known struct is a nested config section. The
/// walk recurses into it and emits no leaf for the field itself. Each other
/// field is a leaf key.
fn config_key_paths(
    structs: &HashMap<String, Vec<(String, String)>>,
    root: &str,
    prefix: &str,
    depth: usize,
) -> HashSet<String> {
    let mut paths = HashSet::new();
    let Some(fields) = structs.get(root) else {
        return paths;
    };
    for (field, ty) in fields {
        let dotted = format!("{prefix}.{field}");
        if structs.contains_key(ty) && depth < 64 {
            paths.extend(config_key_paths(structs, ty, &dotted, depth + 1));
        } else {
            paths.insert(dotted);
        }
    }
    paths
}

/// Returns the config-key paths and the env vars of `config.rs`.
pub fn extract_config_ground_truth(
    pats: &Patterns,
    root: &Path,
) -> Result<(HashSet<String>, HashSet<String>)> {
    let text = pycompat::read_text(&root.join(CONFIG_RS))?;
    let structs = parse_rust_structs(pats, &text);
    let key_paths = config_key_paths(&structs, "PartialHarvestRuntimeConfig", "harvest", 0);
    let env_vars = pats
        .env_var_call
        .captures_iter(&text)
        .filter_map(|c| c.get(1).map(|m| m.as_str().to_owned()))
        .collect();
    Ok((key_paths, env_vars))
}

/// Returns the CLI flags and the CLI env vars of each CLI source file.
pub fn extract_cli_ground_truth(
    pats: &Patterns,
    root: &Path,
) -> Result<(HashSet<String>, HashSet<String>)> {
    let mut flags = HashSet::new();
    let mut env_vars = HashSet::new();
    let mut sources: Vec<PathBuf> = walkdir::WalkDir::new(root.join(CLI_SRC))
        .into_iter()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().ends_with(".rs"))
        .map(walkdir::DirEntry::into_path)
        .collect();
    sources.sort();
    for path in sources {
        scan_cli_file(
            pats,
            &pycompat::read_text(&path)?,
            &mut flags,
            &mut env_vars,
        );
    }
    add_builtin_flags(&mut flags);
    Ok((flags, env_vars))
}

/// Adds the flags and env vars of one CLI source file.
fn scan_cli_file(
    pats: &Patterns,
    text: &str,
    flags: &mut HashSet<String>,
    env: &mut HashSet<String>,
) {
    let lines = pycompat::splitlines(text);
    // The match runs over the whole file, so a multi-line `#[arg(...)]` is one
    // match. Each match maps back to a line number. The field declaration is
    // on the lines right after it.
    for caps in pats.arg_attr.captures_iter(text) {
        let (Some(whole), Some(body)) = (caps.get(0), caps.get(1)) else {
            continue;
        };
        let body = body.as_str();
        if !body.contains("long") {
            // A positional or short-only arg is not a `--flag`.
            continue;
        }
        let end_line = text[..whole.end()].matches('\n').count();
        if let Some(name) = pats.long_name.captures(body).and_then(|c| c.get(1)) {
            flags.insert(name.as_str().to_owned());
        } else if pats.bare_long.is_match(body) {
            // A bare `long` takes the field name in kebab case. The scan skips
            // doc comments and further attributes to reach the field.
            for line in lines.iter().skip(end_line).take(6) {
                let stripped = pycompat::strip(line);
                if stripped.is_empty() || stripped.starts_with("///") || stripped.starts_with("#[")
                {
                    continue;
                }
                if let Some(field) = pats.next_field.captures(line).and_then(|c| c.get(1)) {
                    flags.insert(field.as_str().replace('_', "-"));
                }
                break;
            }
        }
        if let Some(var) = pats.env_name.captures(body).and_then(|c| c.get(1)) {
            env.insert(var.as_str().to_owned());
        }
    }
}

/// Adds the flags that clap adds to each command.
///
/// The clap derives add `--help` and `--version` at each level of the command
/// tree. They are never `#[arg(...)]` fields, so the scan cannot see them.
/// They are real unless a `#[command(...)]` suppresses them, and no such
/// attribute is in the CLI (checked). A fenced `$ harvest --help` example is
/// exactly the kind of snippet that this scan must not flag (PR #1373).
/// `docs/shipped-work.md` cites `harvest --help`, and the CLI integration
/// tests run it for real.
fn add_builtin_flags(flags: &mut HashSet<String>) {
    flags.insert("help".to_owned());
    flags.insert("version".to_owned());
}

/// One fenced block: its language tag and its raw lines.
pub type Block = (String, Vec<String>);

/// Blanks fenced blocks and collects each one with its language.
///
/// The blanked lines serve the inline scan. The block list serves the TOML and
/// CLI-invocation scans, which need the fence content.
///
/// One level of `>` blockquote prefix is recognized on each fence line: open,
/// body and close. `docs/runbooks/triage-pending-tasks-idle-workers.md` uses
/// that shape for its `harvest workflow diagnose ... --json` example (PR
/// #1373). Nested blockquotes are not handled. Nor is a body line that omits
/// the prefix, the lazy continuation of `CommonMark`. That is real scope beyond
/// the one instance in this corpus. `corpus-link-check` makes the same call
/// for the same shape.
pub fn mask_fenced(pats: &Patterns, lines: &[&str]) -> (Vec<String>, Vec<Block>) {
    let mut out = Vec::with_capacity(lines.len());
    let mut blocks = Vec::new();
    let mut fence: Option<(char, usize)> = None;
    let mut lang = String::new();
    let mut body_lines: Vec<String> = Vec::new();
    let mut bq_prefix = String::new();
    for line in lines {
        let Some((fence_char, fence_len)) = fence else {
            let bq = pats.blockquote.captures(line).and_then(|c| c.get(1));
            let candidate = bq.map_or(*line, |m| &line[m.end()..]);
            if let Some(caps) = pats.fence_open.captures(candidate) {
                let delim = caps.get(1).map_or("", |m| m.as_str());
                fence = delim.chars().next().map(|c| (c, delim.len()));
                lang = caps.get(2).map_or("", |m| m.as_str()).to_lowercase();
                body_lines = Vec::new();
                bq_prefix = bq.map_or_else(String::new, |m| m.as_str().to_owned());
                out.push(String::new());
            } else {
                out.push((*line).to_owned());
            }
            continue;
        };
        let body = if !bq_prefix.is_empty() && line.starts_with(bq_prefix.as_str()) {
            &line[bq_prefix.len()..]
        } else {
            line
        };
        if is_fence_close(body, fence_char, fence_len) {
            fence = None;
            blocks.push((std::mem::take(&mut lang), std::mem::take(&mut body_lines)));
            bq_prefix.clear();
            out.push(String::new());
            continue;
        }
        body_lines.push(body.to_owned());
        out.push(String::new());
    }
    (out, blocks)
}

/// Returns each `AUTUMN_HARVEST*` env-var token of `text`, in order.
///
/// The scan covers `AUTUMN_HARVEST*` only, not a bare `HARVEST_*` prefix. This
/// scan has no ground truth for `HARVEST_*`. Bench harnesses, integration
/// tests, curl-auth runbooks and dev-setup docs mint their own `HARVEST_*`
/// vars, such as `HARVEST_TEST_DATABASE_URL` and `HARVEST_ADMIN_TOKEN`. They
/// are real and defined elsewhere in the workspace. An earlier version checked
/// bare `HARVEST_*` too, and each hit was such a var, not drift.
/// `AUTUMN_HARVEST*` is different. It is the reserved override namespace of
/// `config.rs`, and the config extraction lists it fully. So a cited var that
/// is missing from that set is unambiguous.
///
/// At least one character must follow `AUTUMN_HARVEST`. Each real var has a
/// `__FIELD` suffix at least. Without that rule, the bare notation
/// `AUTUMN_HARVEST*` reads as a drift hit (PR #1373). Docs use that notation
/// for the family, not for one var, and the scan failed on its own repository.
///
/// Both ends need a Python `\b` word boundary, so a token is a whole word.
pub fn env_tokens(text: &str) -> Vec<&str> {
    const STEM: &str = "AUTUMN_HARVEST";
    let mut found = Vec::new();
    let mut from = 0;
    while let Some(off) = text[from..].find(STEM) {
        let start = from + off;
        from = start + STEM.len();
        if text[..start]
            .chars()
            .next_back()
            .is_some_and(pycompat::is_word)
        {
            continue;
        }
        let tail = &text[from..];
        let run = tail.len()
            - tail
                .trim_start_matches(|c: char| {
                    c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_'
                })
                .len();
        let end = from + run;
        if run == 0 || text[end..].chars().next().is_some_and(pycompat::is_word) {
            continue;
        }
        found.push(&text[start..end]);
        from = end;
    }
    found
}

/// Finds the `harvest` invocation on one logical shell line.
///
/// It returns the start of the match and the text after `harvest`. The word
/// must start the line or follow whitespace or a `$`. A `-` must not follow
/// it. That excludes `harvest-verify`, a distinct binary and cargo subcommand
/// (`cargo harvest-verify ...`). Its flags, such as `--all-examples` and
/// `--source-root`, are unrelated to the `harvest` management CLI.
pub fn harvest_invocation(line: &str) -> Option<(usize, &str)> {
    const WORD: &str = "harvest";
    let mut from = 0;
    while let Some(off) = line[from..].find(WORD) {
        let at = from + off;
        from = at + WORD.len();
        let prev = line[..at].chars().next_back();
        let lead_ok = prev.is_none_or(|c| pycompat::is_space(c) || c == '$');
        let next = line[from..].chars().next();
        let tail_ok = next.is_none_or(|c| c != '-' && !pycompat::is_word(c));
        if lead_ok && tail_ok {
            let start = prev.map_or(at, |c| at - c.len_utf8());
            return Some((start, &line[from..]));
        }
    }
    None
}

/// Joins shell lines that end in a backslash into one logical line each.
///
/// So a multi-line invocation keeps its flags visible, even though they are
/// not on the line with `harvest` (PR #1373). The `harvest shard rebalance \`
/// examples of `docs/sharding.md` put each flag on a continuation line.
pub fn join_shell_continuations(lines: &[String]) -> Vec<String> {
    let mut logical = Vec::new();
    let mut buf: Option<String> = None;
    for line in lines {
        let piece = buf.as_ref().map_or_else(
            || line.clone(),
            |b| format!("{b} {}", pycompat::strip(line)),
        );
        let stripped = pycompat::rstrip(&piece);
        if let Some(head) = stripped.strip_suffix('\\') {
            buf = Some(pycompat::rstrip(head).to_owned());
            continue;
        }
        logical.push(piece);
        buf = None;
    }
    if let Some(b) = buf {
        logical.push(b);
    }
    logical
}

/// Returns the flags of the `harvest` invocation on one logical line.
///
/// A comment line gives none. Nor does a `harvest` mention inside a curl URL
/// or string. Only a line that runs the binary as a command counts.
pub fn invocation_flags<'l>(pats: &Patterns, line: &'l str) -> Vec<&'l str> {
    let Some((start, rest)) = harvest_invocation(line) else {
        return Vec::new();
    };
    if pycompat::strip(line).starts_with('#') {
        return Vec::new();
    }
    if line[..start].contains("curl") || line.contains("http://") || line.contains("https://") {
        return Vec::new();
    }
    pats.flag_tokens(rest)
}

/// The ground truth that each doc is checked against.
struct Truth {
    config_paths: HashSet<String>,
    env_vars: HashSet<String>,
    flags: HashSet<String>,
}

/// Returns the drift hits of one doc, deduplicated in first-seen order.
fn scan_doc(pats: &Patterns, page: &Path, truth: &Truth) -> Result<Vec<(&'static str, String)>> {
    let text = pycompat::read_text_lossy(page)?;
    let (_masked, blocks) = mask_fenced(pats, &pycompat::splitlines(&text));
    let mut hits: Vec<(&'static str, String)> = Vec::new();

    // Config keys count only inside a fenced TOML block with a `[harvest...]`
    // header. An inline `harvest.x.y = value` code span collides with OTel
    // span attributes and Prometheus metric names of the same shape. Those
    // live in another namespace than the `[harvest]` TOML config. Each inline
    // hit of an early version was one of them, not drift.
    for (lang, block_lines) in &blocks {
        let has_harvest_section = block_lines.iter().any(|l| {
            pats.section_header.is_match(l)
                && pycompat::strip(l)
                    .trim_start_matches('[')
                    .starts_with("harvest")
        });
        if lang == "toml" || has_harvest_section {
            let mut section = String::new();
            for line in block_lines {
                if let Some(name) = pats.section_header.captures(line).and_then(|c| c.get(1)) {
                    name.as_str().clone_into(&mut section);
                    continue;
                }
                let code = line.split('#').next().unwrap_or("");
                let Some(key) = pats.toml_kv.captures(code).and_then(|c| c.get(1)) else {
                    continue;
                };
                let full = if section.is_empty() {
                    key.as_str().to_owned()
                } else {
                    format!("{section}.{}", key.as_str())
                };
                if full.starts_with("harvest") && !truth.config_paths.contains(&full) {
                    hits.push(("config-key", full));
                }
            }
        }
        if INVOCATION_LANGS.contains(&lang.as_str()) {
            for line in join_shell_continuations(block_lines) {
                for flag in invocation_flags(pats, &line) {
                    if !truth.flags.contains(flag) {
                        hits.push(("cli-flag", flag.to_owned()));
                    }
                }
            }
        }
    }

    // Env-var tokens count anywhere in the page, in code or prose. The token
    // shape itself is the signal.
    for var in env_tokens(&text) {
        if !truth.env_vars.contains(var) {
            hits.push(("env-var", var.to_owned()));
        }
    }

    let mut seen = HashSet::new();
    hits.retain(|hit| seen.insert(hit.clone()));
    Ok(hits)
}

/// Returns `path` relative to `root` after a Python `resolve`, for a report line.
///
/// Each report branch needs it. The Python script once lacked it, so a single
/// drift hit ended the run with a `NameError` instead of naming the page.
fn rel(path: &Path, root: &Path) -> String {
    pycompat::rel(&pycompat::resolve(path), root)
}

/// One drift hit: the page, the kind and the value.
type Hit = (PathBuf, &'static str, String);

/// Runs `cargo xtask audit config-cli-drift`.
pub fn run(args: Args) -> Result<ExitCode> {
    let root = pycompat::resolve(&util::repo_root());
    let (code, out) = drift(&root, args.json)?;
    print!("{out}");
    Ok(code)
}

/// Runs the drift scan over the repository at `root` and renders its report.
fn drift(root: &Path, json: bool) -> Result<(ExitCode, String)> {
    let pats = Patterns::new()?;
    let (config_paths, config_env) = extract_config_ground_truth(&pats, root)?;
    let (flags, cli_env) = extract_cli_ground_truth(&pats, root)?;
    let env_vars: HashSet<String> = config_env.union(&cli_env).cloned().collect();
    let truth = Truth {
        config_paths,
        env_vars,
        flags,
    };

    let files = corpus_link_check::docs_markdown(root);
    // This is the corpus and process-artifact split of `corpus-link-check`. A
    // process-artifact page is graded corpus if a real corpus page links to
    // it. The closure runs over the full file set of that audit. Top-level
    // sources such as `README.md` are link sources there, though this scan
    // never checks them for drift.
    let reachable =
        corpus_link_check::compute_corpus_reachable(root, &corpus_link_check::corpus_files(root))?;

    let mut corpus_hits: Vec<Hit> = Vec::new();
    let mut process_hits: Vec<Hit> = Vec::new();
    for f in &files {
        for (kind, value) in scan_doc(&pats, f, &truth)? {
            let target = if reachable.contains(f) {
                &mut corpus_hits
            } else {
                &mut process_hits
            };
            target.push((f.clone(), kind, value));
        }
    }

    let sorted = |set: &HashSet<String>| {
        let mut v: Vec<String> = set.iter().cloned().collect();
        v.sort();
        Json::strings(v)
    };
    let entries = |list: &[Hit]| {
        Json::Arr(
            list.iter()
                .map(|(s, k, v)| {
                    Json::Obj(vec![
                        ("source".into(), Json::Str(rel(s, root))),
                        ("kind".into(), Json::Str((*k).into())),
                        ("value".into(), Json::Str(v.clone())),
                    ])
                })
                .collect(),
        )
    };
    let out = if json {
        Json::Obj(vec![
            ("files_scanned".into(), Json::Int(files.len())),
            ("real_config_keys".into(), sorted(&truth.config_paths)),
            ("real_env_vars".into(), sorted(&truth.env_vars)),
            ("real_cli_flags".into(), sorted(&truth.flags)),
            ("drift_corpus".into(), entries(&corpus_hits)),
            ("drift_process_artifacts".into(), entries(&process_hits)),
        ])
        .render()
            + "\n"
    } else {
        let mut out = format!(
            "Folio config/CLI drift scan — {} files scanned ({} config keys, {} env vars, {} CLI flags known)\n\n",
            files.len(),
            truth.config_paths.len(),
            truth.env_vars.len(),
            truth.flags.len()
        );
        let _ = writeln!(out, "Drift (corpus, fails CI): {}", corpus_hits.len());
        for (s, k, v) in &corpus_hits {
            let _ = writeln!(
                out,
                "  {}: {k} `{v}` — not found in current source",
                rel(s, root)
            );
        }
        let _ = writeln!(
            out,
            "\nDrift (process artifacts, reported only): {}",
            process_hits.len()
        );
        for (s, k, v) in &process_hits {
            let _ = writeln!(
                out,
                "  {}: {k} `{v}` — not found in current source",
                rel(s, root)
            );
        }
        out
    };
    Ok((util::gate_exit(corpus_hits.is_empty()), out))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pats() -> Patterns {
        Patterns::new().unwrap()
    }

    #[test]
    fn env_tokens_need_a_suffix_and_word_boundaries() {
        let text = "AUTUMN_HARVEST* AUTUMN_HARVEST__DB_URL xAUTUMN_HARVEST_X AUTUMN_HARVEST_Yz `AUTUMN_HARVEST_Z`";
        assert_eq!(
            env_tokens(text),
            ["AUTUMN_HARVEST__DB_URL", "AUTUMN_HARVEST_Z"]
        );
    }

    #[test]
    fn harvest_invocation_skips_harvest_verify() {
        assert_eq!(harvest_invocation("harvest --a"), Some((0, " --a")));
        assert_eq!(harvest_invocation("$ harvest x"), Some((1, " x")));
        assert_eq!(harvest_invocation("cargo harvest-verify --all"), None);
        assert_eq!(harvest_invocation("autumn-harvest --x"), None);
        assert_eq!(
            harvest_invocation("harvest-verify; harvest --y"),
            Some((15, " --y"))
        );
        assert_eq!(harvest_invocation("harvesting"), None);
    }

    #[test]
    fn continuations_join() {
        let lines: Vec<String> = ["harvest shard \\", "  --shard 1 \\", "  --dry-run", "x \\"]
            .iter()
            .map(|s| (*s).to_owned())
            .collect();
        assert_eq!(
            join_shell_continuations(&lines),
            ["harvest shard --shard 1 --dry-run", "x"]
        );
    }

    #[test]
    fn invocation_flags_skip_comments_and_urls() {
        let p = pats();
        assert_eq!(
            invocation_flags(&p, "harvest run --wait --json"),
            ["wait", "json"]
        );
        assert_eq!(
            invocation_flags(&p, "# harvest run --wait"),
            [] as [&str; 0]
        );
        assert_eq!(
            invocation_flags(&p, "curl http://x/harvest --data"),
            [] as [&str; 0]
        );
    }

    #[test]
    fn mask_fenced_handles_blockquotes_and_unclosed_fences() {
        let p = pats();
        let lines = [
            "> ```bash",
            "> harvest x --y",
            "> ```",
            "```TOML",
            "[harvest]",
        ];
        let (masked, blocks) = mask_fenced(&p, &lines);
        assert!(masked.iter().all(String::is_empty));
        assert_eq!(
            blocks,
            [("bash".to_owned(), vec!["harvest x --y".to_owned()])]
        );
    }

    #[test]
    fn struct_walk_builds_dotted_paths() {
        let p = pats();
        let src = "pub struct PartialHarvestRuntimeConfig {\n    pub db: Option<DbConfig>,\n    // note\n    pub name: Option<String>,\n}\nstruct DbConfig {\n    url: String,\n    pool: Option<crate::x::Pool>,\n}\nstruct Pool {\n    size: u32,\n}\n";
        let structs = parse_rust_structs(&p, src);
        let mut paths: Vec<_> =
            config_key_paths(&structs, "PartialHarvestRuntimeConfig", "harvest", 0)
                .into_iter()
                .collect();
        paths.sort();
        assert_eq!(
            paths,
            ["harvest.db.pool.size", "harvest.db.url", "harvest.name"]
        );
    }

    #[test]
    fn cli_scan_reads_long_names_and_bare_fields() {
        let p = pats();
        // The Python scan starts at the line that closes the attribute. A
        // lone `)]` line ends the search, so `multi_line` gives no flag.
        let src = "#[arg(long = \"dry-run\", env = \"HARVEST_DRY\")]\nx: bool,\n#[arg(long)]\n/// doc\npub max_items: u32,\n#[arg(short)]\ny: u8,\n#[arg(\n    long,\n)]\npub multi_line: u8,\n";
        let mut flags = HashSet::new();
        let mut env = HashSet::new();
        scan_cli_file(&p, src, &mut flags, &mut env);
        let mut f: Vec<_> = flags.into_iter().collect();
        f.sort();
        assert_eq!(f, ["dry-run", "max-items"]);
        assert!(env.contains("HARVEST_DRY"));
    }

    #[test]
    fn a_fixture_repository_reports_drift() {
        let dir = smop::fs::temp_dir().unwrap();
        let root = pycompat::resolve(dir.path());
        let write = |path: &str, body: &str| {
            let full = root.join(path);
            std::fs::create_dir_all(full.parent().unwrap()).unwrap();
            std::fs::write(full, body).unwrap();
        };
        write(
            CONFIG_RS,
            "struct PartialHarvestRuntimeConfig {\n    pub port: Option<u16>,\n}\nfn f() { env.var(\"AUTUMN_HARVEST__PORT\"); }\n",
        );
        write(
            &format!("{CLI_SRC}/lib.rs"),
            "#[arg(long)]\npub wait: bool,\n",
        );
        write(
            "docs/a.md",
            "```toml\n[harvest]\nport = 1\nhost = \"x\"\n```\n```sh\nharvest run --wait --gone\n```\nAUTUMN_HARVEST__PORT AUTUMN_HARVEST__NOPE\n",
        );
        write("docs/plans/p.md", "AUTUMN_HARVEST__OLD\n");
        let (code, out) = drift(&root, false).unwrap();
        assert_eq!(code, ExitCode::FAILURE);
        assert_eq!(
            out,
            "Folio config/CLI drift scan — 2 files scanned (1 config keys, 1 env vars, 3 CLI flags known)\n\n\
             Drift (corpus, fails CI): 3\n\
             \x20 docs/a.md: config-key `harvest.host` — not found in current source\n\
             \x20 docs/a.md: cli-flag `gone` — not found in current source\n\
             \x20 docs/a.md: env-var `AUTUMN_HARVEST__NOPE` — not found in current source\n\
             \nDrift (process artifacts, reported only): 1\n\
             \x20 docs/plans/p.md: env-var `AUTUMN_HARVEST__OLD` — not found in current source\n"
        );
    }
}
