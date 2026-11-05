//! `cargo xtask ci run-suites`: the manifest-driven CI suite runner.
//!
//! It reads `.github/ci/integration-suites.txt`. It then runs, or compiles
//! with `--no-run`, the per-suite `cargo test` calls that the `test` job once
//! spelled out step by step. A new suite is one manifest line. This module
//! and `ci.yml` do not change.
//!
//! This is the port of `.github/ci/run-suites.sh`. That script was written for
//! bash 3.2 and Git Bash, so that it ran on every CI runner. The port runs as
//! a Rust binary on each OS, so it has no shell dependency.
//!
//! # Usage
//!
//! - `run linux` runs the `osclass=linux` rows serially (`--test-threads=1`).
//! - `run linuxpart` runs the `osclass=linuxpart` rows. They have the
//!   live-DB shape of `linux`, but with `HARVEST_TEST_PARTITIONED=1`. So each
//!   suite runs again against the opt-in partitioned `harvest_events` layout
//!   (issue #958).
//! - `run allos` runs the `osclass=allos` rows in parallel, with no live DB.
//! - `compile` compiles the plugin rows (`osclass` `linux` or `compileonly`)
//!   with `--no-run`.
//!
//! The live-DB CI job builds once and runs many times through cargo-nextest:
//!
//! - `archive <file>` builds the test binary of each `linux` and `linuxpart`
//!   row into one nextest archive.
//! - `extract <file>` unpacks that archive into `./target`.
//! - `nextest linux` runs the `linux` rows from `./target`, serially.
//! - `nextest linuxpart` runs the `linuxpart` rows the same way, with
//!   `HARVEST_TEST_PARTITIONED=1`.
//! - `filterset <class>` prints the nextest filterset of a class.
//!
//! Set `DRY_RUN=1` to print the cargo commands instead of running them. Set
//! `SUITES_MANIFEST` to read another manifest. Only the runner tests do that.
//!
//! # Sharding
//!
//! Set `SEMAPHORE_SHARD_COUNT` to an integer N and `SEMAPHORE_SHARD_INDEX` to
//! a value in 0..N-1. Then `run <linux|linuxpart|allos>` and `compile`
//! process only every Nth matching row (`row_ordinal % N == SHARD_INDEX`).
//!
//! For `run`, `row_ordinal` counts the rows of that osclass, in manifest
//! order. For `compile`, it counts the (crate, features, target) rows that
//! `compile` draws from: the `linux` and `compileonly` plugin rows. It counts
//! them in the sorted order that [`compile_plan`] batches them by.
//!
//! A filter by row ordinal over a sorted list gives a sorted subsequence. So
//! rows that share a (crate, features) key still land next to each other.
//! They batch into one `cargo` call exactly as they do unsharded, with fewer
//! targets in it.
//!
//! The `--test-threads=1` inside the `cargo test` call of one row is not
//! affected. Sharding changes which ROWS one call of this command runs. It
//! does not change how the tests of a row are ordered against each other.
//! With no shard count (the default), nothing is filtered. The behavior is
//! then identical to the behavior before sharding. A CI job runs this command
//! once per shard, in parallel, each with a different `SEMAPHORE_SHARD_INDEX`.
//!
//! `nextest` shards by test, not by row. It passes `--partition
//! hash:<index+1>/<count>`, so the hash of its name puts each test on one
//! shard. A large suite then spreads over every shard. The row-ordinal scheme
//! above keeps a whole suite on one shard, and the coverage nightly still
//! uses it.
//!
//! `cargo xtask audit shard-weight-drift` mirrors [`records`] and the row
//! ordinal of [`run_plan`]. Keep the manifest format and that arithmetic in
//! step with it.

use std::fmt::Write as _;
use std::path::PathBuf;
use std::process::ExitCode;

use smop::prelude::*;

use crate::util::repo_path;

/// The manifest that the runner reads unless `SUITES_MANIFEST` names another.
pub const MANIFEST: &str = ".github/ci/integration-suites.txt";

/// The usage line for a missing mode.
const USAGE: &str = "usage: cargo xtask ci run-suites <run <linux|linuxpart|allos> | compile \
                     | archive <file> | extract <file> | nextest <linux|linuxpart> \
                     | filterset <osclass>>";

/// The extracted archive lives where cargo builds.
///
/// So a path that a test bakes in with `env!` resolves. `CARGO_BIN_EXE_*` and
/// `CARGO_TARGET_TMPDIR` are two such paths. GitHub checks the repository out
/// at the same absolute path in every job. So the build runner and each shard
/// agree on that path.
const NEXTEST_DIR: &str = "target/nextest";

/// Arguments of `cargo xtask ci run-suites`.
#[derive(Debug, Args)]
pub struct Args {
    /// The mode and its argument, for example `run linux` or `archive <file>`.
    ///
    /// Extra words are ignored, as the shell script ignored them.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub words: Vec<String>,
}

/// One command that the runner runs, or prints under `DRY_RUN=1`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Invocation {
    /// The program and its arguments.
    pub argv: Vec<String>,
    /// Variables set for this command only.
    pub env: Vec<(String, String)>,
    /// The words that the `::group::` and `+ ` lines print.
    ///
    /// They equal `argv`, except that a command run through `env` names the
    /// `env` program and each assignment first.
    pub shown: Vec<String>,
    /// The line that `FAILED SUITES:` prints when the command fails.
    pub label: String,
}

impl Invocation {
    /// A plain command that prints as it runs.
    fn plain(argv: Vec<String>, label: String) -> Self {
        Self {
            shown: argv.clone(),
            argv,
            env: Vec::new(),
            label,
        }
    }

    /// A command that runs through `env`, with each assignment shown first.
    fn through_env(env: Vec<(String, String)>, argv: Vec<String>, label: String) -> Self {
        let mut shown = vec!["env".to_string()];
        shown.extend(env.iter().map(|(k, v)| format!("{k}={v}")));
        shown.extend(argv.iter().cloned());
        Self {
            argv,
            env,
            shown,
            label,
        }
    }

    /// The command line, as `$*` printed it.
    pub fn line(&self) -> String {
        self.shown.join(" ")
    }
}

/// The shard settings of one call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Shard {
    /// `SEMAPHORE_SHARD_COUNT`.
    pub count: i64,
    /// `SEMAPHORE_SHARD_INDEX`.
    pub index: i64,
}

impl Shard {
    /// True when this shard keeps the row with `ordinal`.
    ///
    /// The remainder truncates toward zero, as bash and awk compute it.
    const fn keeps(self, ordinal: i64) -> bool {
        ordinal % self.count == self.index
    }
}

/// Reads the shard settings from `count` and `index`.
///
/// An empty or unset count means no sharding. An empty or unset index means
/// shard 0.
pub fn shard_from(count: Option<&str>, index: Option<&str>) -> Result<Option<Shard>> {
    let count = match count.map(str::trim) {
        None | Some("") => return Ok(None),
        Some(text) => text
            .parse::<i64>()
            .with_context(|| format!("SEMAPHORE_SHARD_COUNT is not an integer: {text:?}"))?,
    };
    ensure!(count != 0, "SEMAPHORE_SHARD_COUNT must not be 0");
    Ok(Some(Shard {
        count,
        index: shard_index(index)?,
    }))
}

/// Reads `SEMAPHORE_SHARD_INDEX`, with 0 for an empty or unset value.
fn shard_index(index: Option<&str>) -> Result<i64> {
    match index.map(str::trim) {
        None | Some("") => Ok(0),
        Some(text) => text
            .parse::<i64>()
            .with_context(|| format!("SEMAPHORE_SHARD_INDEX is not an integer: {text:?}")),
    }
}

/// Reads the shard settings of this process.
pub fn shard_from_env() -> Result<Option<Shard>> {
    shard_from(
        std::env::var("SEMAPHORE_SHARD_COUNT").ok().as_deref(),
        std::env::var("SEMAPHORE_SHARD_INDEX").ok().as_deref(),
    )
}

/// Returns the manifest path: `SUITES_MANIFEST`, or the committed manifest.
pub fn manifest_path() -> PathBuf {
    match std::env::var_os("SUITES_MANIFEST") {
        Some(path) if !path.is_empty() => PathBuf::from(path),
        _ => repo_path(MANIFEST),
    }
}

/// Reads the manifest text.
pub fn read_manifest() -> Result<String> {
    let path = manifest_path();
    std::fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))
}

/// The characters that the POSIX `[[:space:]]` class holds.
const fn is_posix_space(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n' | '\x0b' | '\x0c' | '\r')
}

/// The blanks that split a field, as the default `IFS` and awk split it.
const fn is_blank(c: char) -> bool {
    matches!(c, ' ' | '\t')
}

/// Returns the data lines of the manifest.
///
/// A line that holds only whitespace, or whose first non-space character is
/// `#`, is not a record. This is `grep -vE '^[[:space:]]*(#|$)'`.
pub fn records(text: &str) -> Vec<&str> {
    let mut lines: Vec<&str> = text.split('\n').collect();
    if text.ends_with('\n') {
        lines.pop();
    }
    lines
        .into_iter()
        .filter(|line| {
            let rest = line.trim_start_matches(is_posix_space);
            !rest.is_empty() && !rest.starts_with('#')
        })
        .collect()
}

/// Splits a record into awk fields: runs of blanks separate them.
fn awk_fields(line: &str) -> Vec<&str> {
    line.split(is_blank).filter(|f| !f.is_empty()).collect()
}

/// Returns awk field `n` (1-based) of a record, or the empty string.
fn awk_field<'a>(fields: &[&'a str], n: usize) -> &'a str {
    fields.get(n - 1).copied().unwrap_or("")
}

/// Splits a record as `read -r os crate target feats filter` does.
///
/// The first four fields end at a blank. The last field takes the rest of
/// the line, with its outer blanks removed.
fn read_fields(line: &str) -> [&str; 5] {
    let mut out = [""; 5];
    let mut rest = line.trim_start_matches(is_blank);
    for slot in out.iter_mut().take(4) {
        let end = rest.find(is_blank).unwrap_or(rest.len());
        *slot = &rest[..end];
        rest = rest[end..].trim_start_matches(is_blank);
    }
    out[4] = rest.trim_end_matches(is_blank);
    out
}

/// Turns string literals into owned words.
fn words(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| (*s).to_string()).collect()
}

/// `run <class>`: one `cargo test` call for each matching record.
pub fn run_plan(records: &[&str], want: &str, shard: Option<Shard>) -> Vec<Invocation> {
    let mut plan = Vec::new();
    // The row ordinal among the rows of `want`, for sharding. It counts every
    // matching row whether or not sharding is on. So the ordinal of a row
    // does not depend on SEMAPHORE_SHARD_COUNT.
    let mut row_ordinal: i64 = 0;
    for line in records {
        let [os, krate, target, feats, filter] = read_fields(line);
        if os.is_empty() || os != want {
            continue;
        }
        if let Some(shard) = shard {
            let this_ordinal = row_ordinal;
            row_ordinal += 1;
            if !shard.keeps(this_ordinal) {
                continue;
            }
        }

        let mut argv = words(&["cargo", "test", "-p", krate]);
        // autumn-harvest has `default = ["db", "unified-dag-execution"]`. The
        // `allos` class means no live DB. Its `integration` aggregate target
        // connects to Postgres, so those rows drop the defaults with
        // `--no-default-features`. Each such row lists the features that it
        // needs. The `linux` rows keep the defaults, because they run against
        // Docker Postgres. Other `allos` targets keep them too, for example
        // `property`, which tests pure functions.
        if os == "allos" && krate == "autumn-harvest" && target == "integration" {
            argv.push("--no-default-features".into());
        }
        if feats != "-" {
            argv.push("--features".into());
            argv.push(feats.into());
        }
        argv.push("--test".into());
        argv.push(target.into());

        // The test-binary arguments follow `--`. The positional filter comes
        // first, then `--test-threads=1`. Only `linux` rows pass that flag,
        // because they share one Postgres. The flag always comes after any
        // filter, so the order is correct by construction.
        let mut post: Vec<String> = Vec::new();
        if filter != "-" {
            post.push(filter.into());
        }
        if os == "linux" || os == "linuxpart" {
            post.push("--test-threads=1".into());
        }
        if !post.is_empty() {
            argv.push("--".into());
            argv.extend(post);
        }

        // The failure label carries the FILTER. Without it, each failing row
        // of one (crate, target, osclass) prints the same line. The core rows
        // are all `autumn-harvest/integration (linux)`, so "3 suites failed"
        // named none of them. The `::group::` output of each suite names it.
        // But that output is often outside the log window that the API
        // returns for a long job. So the summary is the only place where the
        // name reliably survives.
        let mut label = format!("{krate}/{target} ({want})");
        if filter != "-" {
            label = format!("{label} -- {filter}");
        }

        let mut invocation = Invocation::plain(argv, label);
        // `linuxpart` is `linux` with the layout switch on. The cargo call is
        // identical, and it runs against the opt-in partitioned
        // `harvest_events` layout. The flag is set for this call only. So a
        // mixed run cannot leak it into the plain `linux` pass.
        if want == "linuxpart" {
            invocation
                .env
                .push(("HARVEST_TEST_PARTITIONED".into(), "1".into()));
        }
        plan.push(invocation);
    }
    plan
}

/// `compile`: compile each plugin row with osclass `linux` or `compileonly`.
///
/// The rows compile with `--no-run`, grouped by (crate, features). The
/// blanket core `--no-run` step in `ci.yml` compiles the core rows, so this
/// mode leaves them out on purpose.
pub fn compile_plan(records: &[&str], shard: Option<Shard>) -> Vec<Invocation> {
    // Collect (crate, features, target) for the plugin rows. Sort them, so
    // that rows with the same (crate, features) sit next to each other. The
    // sort compares the tab-joined lines by bytes, as `LC_ALL=C sort` does.
    let mut grouped: Vec<(&str, &str, &str)> = records
        .iter()
        .map(|line| awk_fields(line))
        .filter(|f| {
            let class = awk_field(f, 1);
            (class == "linux" || class == "compileonly")
                && awk_field(f, 2) == "autumn-harvest-plugin"
        })
        .map(|f| (awk_field(&f, 2), awk_field(&f, 4), awk_field(&f, 3)))
        .collect();
    grouped.sort_by_cached_key(|(c, f, t)| format!("{c}\t{f}\t{t}"));

    // Sharding keeps every Nth row, by ordinal in the sorted list. It is the
    // `row_ordinal % shard_count == shard_index` scheme of `run_plan`.
    if let Some(shard) = shard {
        grouped = grouped
            .into_iter()
            .zip(0_i64..)
            .filter(|(_, ordinal)| shard.keeps(*ordinal))
            .map(|(row, _)| row)
            .collect();
    }

    let mut plan = Vec::new();
    let mut current: Option<(&str, &str, Vec<&str>)> = None;
    for (krate, feats, target) in grouped.into_iter().map(read_tab_fields) {
        if krate.is_empty() {
            continue;
        }
        match current.as_mut() {
            Some((c, f, targets)) if *c == krate && *f == feats => targets.push(target),
            _ => {
                if let Some((c, f, targets)) = current.take() {
                    plan.push(compile_invocation(c, f, &targets));
                }
                current = Some((krate, feats, vec![target]));
            }
        }
    }
    // Flush the final group.
    if let Some((c, f, targets)) = current {
        plan.push(compile_invocation(c, f, &targets));
    }
    plan
}

/// Splits a sorted `crate<TAB>features<TAB>target` line as the script read it.
///
/// The script read each line with `IFS=<TAB> read -r c f t`. A tab is IFS
/// whitespace, so a run of tabs is one separator. So an empty field moves
/// the next field into its place. Only a short row has an empty field.
fn read_tab_fields<'a>(row: (&'a str, &'a str, &'a str)) -> (&'a str, &'a str, &'a str) {
    let mut fields = <[&str; 3]>::from(row)
        .into_iter()
        .filter(|field| !field.is_empty());
    let mut next = || fields.next().unwrap_or("");
    (next(), next(), next())
}

/// True for a character of the default `IFS`: a space, a tab or a newline.
pub const fn is_ifs(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n')
}

/// One `--no-run` call for a (crate, features) group.
fn compile_invocation(krate: &str, feats: &str, targets: &[&str]) -> Invocation {
    let mut argv = words(&["cargo", "test", "-p", krate]);
    if feats != "-" {
        argv.push("--features".into());
        argv.push(feats.into());
    }
    // The script split the joined targets on the default IFS.
    for target in targets
        .iter()
        .flat_map(|t| t.split(is_ifs))
        .filter(|t| !t.is_empty())
    {
        argv.push("--test".into());
        argv.push(target.into());
    }
    argv.push("--no-run".into());
    Invocation::plain(argv, format!("compile {krate} ({feats})"))
}

/// The rows whose test binaries the archive must hold.
fn live_db_rows<'a>(records: &[&'a str]) -> Vec<Vec<&'a str>> {
    records
        .iter()
        .map(|line| awk_fields(line))
        .filter(|f| {
            let class = awk_field(f, 1);
            class == "linux" || class == "linuxpart"
        })
        .collect()
}

/// Sorts `items` by bytes and drops duplicates, as `LC_ALL=C sort -u` does.
fn sort_unique(mut items: Vec<String>) -> Vec<String> {
    items.sort_unstable();
    items.dedup();
    items
}

/// `archive <file>`: build each live-DB test binary into one nextest archive.
///
/// One cargo call resolves features once. So the archive takes the union of
/// the feature columns, each one qualified by its crate. Each feature in that
/// union is additive, so a suite still compiles and runs under it.
///
/// Debug info is off by default. A plugin test binary is about 1.4 GB with
/// full debug info and about 130 MB without it. The archive holds over a
/// hundred such binaries. A panic still reports its file and line without
/// debug info. `dev_debug` and `test_debug` override the two profile
/// settings, as `CARGO_PROFILE_DEV_DEBUG` and `CARGO_PROFILE_TEST_DEBUG` do.
pub fn archive_invocation(
    records: &[&str],
    out: &str,
    dev_debug: &str,
    test_debug: &str,
) -> Invocation {
    let rows = live_db_rows(records);
    let mut argv = words(&["cargo", "nextest", "archive", "--archive-file", out]);
    let column = |n: usize| -> Vec<String> {
        sort_unique(rows.iter().map(|f| awk_field(f, n).to_string()).collect())
    };
    for krate in column(2).iter().filter(|c| !c.is_empty()) {
        argv.push("-p".into());
        argv.push(krate.clone());
    }
    for target in column(3).iter().filter(|t| !t.is_empty()) {
        argv.push("--test".into());
        argv.push(target.clone());
    }
    let feats = sort_unique(
        rows.iter()
            .filter(|f| awk_field(f, 4) != "-" && !awk_field(f, 4).is_empty())
            .flat_map(|f| {
                let krate = awk_field(f, 2);
                awk_field(f, 4)
                    .split(',')
                    .map(move |feat| format!("{krate}/{feat}"))
            })
            .collect(),
    )
    .join(",");
    if !feats.is_empty() {
        argv.push("--features".into());
        argv.push(feats);
    }
    let env = vec![
        ("CARGO_PROFILE_DEV_DEBUG".to_string(), dev_debug.to_string()),
        (
            "CARGO_PROFILE_TEST_DEBUG".to_string(),
            test_debug.to_string(),
        ),
    ];
    Invocation::through_env(env, argv, format!("archive {out}"))
}

/// `extract <file>`: unpack the archive into `./target`, metadata included.
///
/// `binaries-only` lists the binaries without running them, so this step only
/// unpacks. `--extract-overwrite` keeps the files already in the checkout.
pub fn extract_invocation(archive: &str) -> Invocation {
    let argv = words(&[
        "cargo",
        "nextest",
        "list",
        "--archive-file",
        archive,
        "--workspace-remap",
        ".",
        "--extract-to",
        ".",
        "--extract-overwrite",
        "--list-type",
        "binaries-only",
    ]);
    Invocation::plain(argv, format!("extract {archive}"))
}

/// True when a filter is `-` or holds only `[a-z0-9_:]`.
fn filter_is_safe(filter: &str) -> bool {
    filter == "-"
        || (!filter.is_empty()
            && filter
                .bytes()
                .all(|b| matches!(b, b'a'..=b'z' | b'0'..=b'9' | b'_' | b':')))
}

/// `filterset <class>`: one nextest filterset that selects the rows of a class.
///
/// A row with no filter selects its whole binary. A row with a filter selects
/// the tests whose name contains it. That is the substring match of the
/// positional filter of `cargo test`. So a row selects the same tests either
/// way. A filter with a character outside `[a-z0-9_:]` is refused, because
/// the filterset grammar would read it.
///
/// Returns the expression, or the refused filters.
pub fn filterset(records: &[&str], want: &str) -> std::result::Result<String, Vec<String>> {
    let mut terms: Vec<String> = Vec::new();
    let mut bad: Vec<String> = Vec::new();
    for line in records {
        let f = awk_fields(line);
        if awk_field(&f, 1) != want {
            continue;
        }
        let filter = awk_field(&f, 5);
        if !filter_is_safe(filter) {
            bad.push(filter.to_string());
            continue;
        }
        let binary = format!("binary_id(={}::{})", awk_field(&f, 2), awk_field(&f, 3));
        terms.push(if filter == "-" {
            binary
        } else {
            format!("({binary} & test({filter}))")
        });
    }
    if bad.is_empty() {
        Ok(terms.join(" | "))
    } else {
        Err(bad)
    }
}

/// The message for refused filters.
fn refused_message(bad: &[String]) -> String {
    let mut message = "run-suites: unsupported manifest filter:".to_string();
    for filter in bad {
        message.push(' ');
        message.push_str(filter);
    }
    message
}

/// `nextest <linux|linuxpart>`: run the rows of a class from the archive.
///
/// The run is serial, as `run linux` is. The suites start Postgres
/// containers, and the runner has room for one at a time. `--no-tests warn`
/// lets a shard pass when the hash gives it no test of a small class.
///
/// `count` is the raw `SEMAPHORE_SHARD_COUNT`, and `index` is the shard index.
pub fn nextest_invocation(want: &str, expr: &str, count: Option<&str>, index: i64) -> Invocation {
    let mut env = Vec::new();
    if want == "linuxpart" {
        env.push(("HARVEST_TEST_PARTITIONED".to_string(), "1".to_string()));
    }
    let binaries = format!("{NEXTEST_DIR}/binaries-metadata.json");
    let metadata = format!("{NEXTEST_DIR}/cargo-metadata.json");
    let mut argv = words(&[
        "cargo",
        "nextest",
        "run",
        "--binaries-metadata",
        &binaries,
        "--cargo-metadata",
        &metadata,
        "--workspace-remap",
        ".",
        "--target-dir-remap",
        "target",
        "--test-threads",
        "1",
        "--no-fail-fast",
        "--no-tests",
        "warn",
        "-E",
        expr,
    ]);
    if let Some(count) = count.filter(|c| !c.is_empty()) {
        argv.push("--partition".into());
        argv.push(format!("hash:{}/{count}", index + 1));
    }
    Invocation::through_env(env, argv, format!("nextest {want}"))
}

/// Renders the dry-run output of `plan`, as `DRY_RUN=1` prints it.
pub fn dry_run_text(plan: &[Invocation]) -> String {
    let mut out = String::new();
    for inv in plan {
        let line = inv.line();
        let _ = write!(out, "::group::{line}\n+ {line}\n::endgroup::\n");
    }
    out
}

/// The dry-run output of `run <class>` for this process's manifest and shard.
///
/// `cargo xtask ci pull-test-images` reads it to find the suites of a shard.
pub fn run_dry_run_text(want: &str) -> Result<String> {
    let text = read_manifest()?;
    let plan = run_plan(&records(&text), want, shard_from_env()?);
    Ok(dry_run_text(&plan))
}

/// Runs the commands and collects the failures.
struct Runner {
    /// True under `DRY_RUN=1`.
    dry_run: bool,
    /// The label of each failed command.
    failed: Vec<String>,
}

impl Runner {
    /// Runs one command, or prints it under `DRY_RUN=1`.
    ///
    /// The `::group::` lines fold the output of each command in the CI log.
    /// Returns true when the command succeeds.
    fn run(&self, inv: &Invocation) -> bool {
        let line = inv.line();
        println!("::group::{line}");
        let ok = if self.dry_run {
            println!("+ {line}");
            true
        } else {
            Self::spawn(inv)
        };
        println!("::endgroup::");
        ok
    }

    /// Runs `inv` with inherited stdio and reports whether it passed.
    fn spawn(inv: &Invocation) -> bool {
        let Some((program, args)) = inv.argv.split_first() else {
            return false;
        };
        let mut command = sh::cmd(program).args(args);
        for (key, value) in &inv.env {
            command = command.env(key, value);
        }
        match command.status() {
            Ok(status) => status.success(),
            Err(error) => {
                eprintln!("run-suites: {error:#}");
                false
            }
        }
    }

    /// Runs `inv` and records its label when it fails.
    fn run_or_note(&mut self, inv: &Invocation) -> bool {
        let ok = self.run(inv);
        if !ok {
            self.failed.push(inv.label.clone());
        }
        ok
    }

    /// Prints the failed suites and returns the exit code of the run.
    fn finish(self) -> ExitCode {
        if self.failed.is_empty() {
            return ExitCode::SUCCESS;
        }
        let mut text = "FAILED SUITES:\n".to_string();
        for label in &self.failed {
            text.push_str("  ");
            text.push_str(label);
            text.push('\n');
        }
        eprint!("{text}");
        ExitCode::FAILURE
    }
}

/// Prints a usage line and returns exit code 2.
fn usage(line: &str) -> ExitCode {
    eprintln!("{line}");
    ExitCode::from(2)
}

/// Reports a manifest with no rows and returns exit code 1.
///
/// The script failed this way in every mode but `run` and `extract`. Its
/// `grep` found no line, and `pipefail` passed that status on.
fn no_rows() -> ExitCode {
    eprintln!(
        "run-suites: the manifest {} has no rows",
        manifest_path().display()
    );
    ExitCode::FAILURE
}

/// Reads a variable, with `default` for an empty or unset value.
fn var_or(key: &str, default: &str) -> String {
    match std::env::var(key) {
        Ok(value) if !value.is_empty() => value,
        _ => default.to_string(),
    }
}

/// The outcome of a mode: run on to the summary, or exit with a code now.
type Step = Result<Option<ExitCode>>;

/// `archive <file>`: build the live-DB archive.
fn archive(runner: &mut Runner, out: &str) -> Step {
    let text = read_manifest()?;
    let rows = records(&text);
    if rows.is_empty() {
        return Ok(Some(no_rows()));
    }
    let inv = archive_invocation(
        &rows,
        out,
        &var_or("CARGO_PROFILE_DEV_DEBUG", "0"),
        &var_or("CARGO_PROFILE_TEST_DEBUG", "0"),
    );
    runner.run_or_note(&inv);
    Ok(None)
}

/// `extract <file>`: unpack the archive. This mode reads no manifest.
fn extract(runner: &mut Runner, archive: &str) -> Step {
    if runner.run_or_note(&extract_invocation(archive)) && !runner.dry_run {
        // `CARGO_TARGET_TMPDIR` names this directory, and cargo makes it only
        // when it builds. A test that writes a log there needs it to exist.
        fs::create_dir_all("target/tmp")?;
    }
    Ok(None)
}

/// `nextest <class>`: run the rows of a class from the archive.
fn nextest(runner: &mut Runner, want: &str) -> Step {
    let text = read_manifest()?;
    let rows = records(&text);
    if rows.is_empty() {
        return Ok(Some(no_rows()));
    }
    let expr = match filterset(&rows, want) {
        Ok(expr) => expr,
        Err(bad) => {
            eprintln!("{}", refused_message(&bad));
            return Ok(Some(ExitCode::FAILURE));
        }
    };
    if !expr.is_empty() {
        let count = std::env::var("SEMAPHORE_SHARD_COUNT").ok();
        let index = shard_index(std::env::var("SEMAPHORE_SHARD_INDEX").ok().as_deref())?;
        runner.run_or_note(&nextest_invocation(want, &expr, count.as_deref(), index));
    }
    Ok(None)
}

/// `filterset <class>`: print the filterset of a class.
fn print_filterset(want: &str) -> Step {
    let text = read_manifest()?;
    let rows = records(&text);
    match filterset(&rows, want) {
        Ok(expr) => {
            println!("{expr}");
            Ok(rows.is_empty().then(no_rows))
        }
        Err(bad) => {
            eprintln!("{}", refused_message(&bad));
            Ok(Some(ExitCode::FAILURE))
        }
    }
}

/// `run <class>` or `compile`: one cargo call per planned command.
fn run_rows(runner: &mut Runner, want: Option<&str>) -> Step {
    let text = read_manifest()?;
    let rows = records(&text);
    let plan = match want {
        Some(want) => run_plan(&rows, want, shard_from_env()?),
        None if rows.is_empty() => return Ok(Some(no_rows())),
        None => compile_plan(&rows, shard_from_env()?),
    };
    for inv in plan {
        runner.run_or_note(&inv);
    }
    Ok(None)
}

/// Runs `cargo xtask ci run-suites`.
#[expect(
    clippy::needless_pass_by_value,
    reason = "every command takes its `Args` by value, so each dispatch arm has one shape"
)]
pub fn run(args: Args) -> Result<ExitCode> {
    let mode = args.words.first().map_or("", String::as_str);
    let operand = args.words.get(1).map_or("", String::as_str);
    let mut runner = Runner {
        dry_run: std::env::var("DRY_RUN").is_ok_and(|v| v == "1"),
        failed: Vec::new(),
    };
    let step = match mode {
        "" => return Ok(usage(USAGE)),
        "archive" | "extract" if operand.is_empty() => {
            return Ok(usage(&format!(
                "usage: cargo xtask ci run-suites {mode} <archive-file>"
            )));
        }
        "archive" => archive(&mut runner, operand)?,
        "extract" => extract(&mut runner, operand)?,
        "nextest" if operand == "linux" || operand == "linuxpart" => nextest(&mut runner, operand)?,
        "nextest" => {
            return Ok(usage(
                "usage: cargo xtask ci run-suites nextest <linux|linuxpart>",
            ));
        }
        "filterset" if operand.is_empty() => {
            return Ok(usage(
                "usage: cargo xtask ci run-suites filterset <osclass>",
            ));
        }
        "filterset" => print_filterset(operand)?,
        "run" if matches!(operand, "linux" | "linuxpart" | "allos") => {
            run_rows(&mut runner, Some(operand))?
        }
        "run" => {
            return Ok(usage(
                "usage: cargo xtask ci run-suites run <linux|linuxpart|allos>",
            ));
        }
        "compile" => run_rows(&mut runner, None)?,
        _ => {
            return Ok(usage(&format!(
                "bad mode: {mode} (expected: run <linux|linuxpart|allos> | compile \
                 | archive <file> | extract <file> | nextest <linux|linuxpart> \
                 | filterset <osclass>)"
            )));
        }
    };
    Ok(step.unwrap_or_else(|| runner.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;

    const MANIFEST_TEXT: &str = "\
# a comment
   # an indented comment

allos        autumn-harvest         integration   -                 alpha_docs
allos        autumn-harvest         property      -                 -
allos        autumn-harvest-plugin  openapi_spec  sqlite            -
compileonly  autumn-harvest-plugin  zeta          kafka             -
linux        autumn-harvest         integration   -                 queue_tests
linux        autumn-harvest         integration   testing           -
linux        autumn-harvest-plugin  beta          -                 -
linux        autumn-harvest-plugin  alpha         -                 -
linux        autumn-harvest-plugin  gamma         kafka,sqs         a::b
linuxpart    autumn-harvest         integration   -                 store_tests
";

    fn plan_lines(plan: &[Invocation]) -> Vec<String> {
        plan.iter().map(Invocation::line).collect()
    }

    #[test]
    fn records_drop_comments_and_blank_lines() {
        let rows = records("# x\n \t\n  # y\nlinux a b - -\n\nallos c d - -");
        assert_eq!(rows, vec!["linux a b - -", "allos c d - -"]);
        assert_eq!(records(""), Vec::<&str>::new());
    }

    #[test]
    fn read_fields_gives_the_rest_of_the_line_to_the_filter() {
        assert_eq!(
            read_fields("  linux a  b\t- one two  "),
            ["linux", "a", "b", "-", "one two"]
        );
        assert_eq!(read_fields("linux a"), ["linux", "a", "", "", ""]);
    }

    #[test]
    fn run_linux_is_serial_and_keeps_the_filter_first() {
        let plan = run_plan(&records(MANIFEST_TEXT), "linux", None);
        assert_eq!(
            plan_lines(&plan),
            vec![
                "cargo test -p autumn-harvest --test integration -- queue_tests --test-threads=1",
                "cargo test -p autumn-harvest --features testing --test integration -- --test-threads=1",
                "cargo test -p autumn-harvest-plugin --test beta -- --test-threads=1",
                "cargo test -p autumn-harvest-plugin --test alpha -- --test-threads=1",
                "cargo test -p autumn-harvest-plugin --features kafka,sqs --test gamma -- a::b --test-threads=1",
            ]
        );
        assert_eq!(
            plan[0].label,
            "autumn-harvest/integration (linux) -- queue_tests"
        );
        assert_eq!(plan[1].label, "autumn-harvest/integration (linux)");
        assert!(plan.iter().all(|inv| inv.env.is_empty()));
    }

    #[test]
    fn run_allos_drops_defaults_only_on_the_core_integration_target() {
        let plan = run_plan(&records(MANIFEST_TEXT), "allos", None);
        assert_eq!(
            plan_lines(&plan),
            vec![
                "cargo test -p autumn-harvest --no-default-features --test integration -- alpha_docs",
                "cargo test -p autumn-harvest --test property",
                "cargo test -p autumn-harvest-plugin --features sqlite --test openapi_spec",
            ]
        );
    }

    #[test]
    fn run_linuxpart_sets_the_layout_flag_without_showing_it() {
        let plan = run_plan(&records(MANIFEST_TEXT), "linuxpart", None);
        assert_eq!(plan.len(), 1);
        assert_eq!(
            plan[0].env,
            vec![("HARVEST_TEST_PARTITIONED".to_string(), "1".to_string())]
        );
        assert!(plan[0].line().starts_with("cargo test -p autumn-harvest "));
    }

    #[test]
    fn run_shards_by_ordinal_among_matching_rows() {
        let rows = records(MANIFEST_TEXT);
        let all = plan_lines(&run_plan(&rows, "linux", None));
        let mut union = Vec::new();
        for index in 0..2 {
            let shard = Shard { count: 2, index };
            let part = plan_lines(&run_plan(&rows, "linux", Some(shard)));
            let want: Vec<String> = all
                .iter()
                .enumerate()
                .filter(|(i, _)| i % 2 == usize::try_from(index).unwrap_or(0))
                .map(|(_, l)| l.clone())
                .collect();
            assert_eq!(part, want);
            union.extend(part);
        }
        union.sort();
        let mut sorted = all;
        sorted.sort();
        assert_eq!(union, sorted, "the shards must cover every row once");
        let past_the_end = Shard { count: 2, index: 5 };
        assert_eq!(run_plan(&rows, "linux", Some(past_the_end)), Vec::new());
    }

    #[test]
    fn compile_batches_sorted_plugin_rows_by_features() {
        let plan = compile_plan(&records(MANIFEST_TEXT), None);
        assert_eq!(
            plan_lines(&plan),
            vec![
                "cargo test -p autumn-harvest-plugin --test alpha --test beta --no-run",
                "cargo test -p autumn-harvest-plugin --features kafka --test zeta --no-run",
                "cargo test -p autumn-harvest-plugin --features kafka,sqs --test gamma --no-run",
            ]
        );
        assert_eq!(plan[0].label, "compile autumn-harvest-plugin (-)");
    }

    #[test]
    fn compile_reads_a_short_row_as_the_tab_read_did() {
        // A row with no features column gives `crate<TAB><TAB>target`. The
        // tab read collapses the two tabs, so the target lands in `features`.
        let plan = compile_plan(&["linux autumn-harvest-plugin solo"], None);
        assert_eq!(
            plan_lines(&plan),
            vec!["cargo test -p autumn-harvest-plugin --features solo --no-run"]
        );
        assert_eq!(plan[0].label, "compile autumn-harvest-plugin (solo)");
        assert_eq!(read_tab_fields(("a", "b", "c")), ("a", "b", "c"));
        assert_eq!(read_tab_fields(("a", "", "")), ("a", "", ""));
    }

    #[test]
    fn compile_splits_targets_on_the_default_ifs_only() {
        // A carriage return is not in the default IFS, so it stays in the
        // target, as the unquoted shell expansion kept it.
        let plan = compile_plan(&["linux autumn-harvest-plugin t\r -  -"], None);
        assert_eq!(plan[0].argv[4..6], ["--test", "t\r"]);
    }

    #[test]
    fn compile_shards_the_sorted_list() {
        let rows = records(MANIFEST_TEXT);
        let shard0 = compile_plan(&rows, Some(Shard { count: 2, index: 0 }));
        assert_eq!(
            plan_lines(&shard0),
            vec![
                "cargo test -p autumn-harvest-plugin --test alpha --no-run",
                "cargo test -p autumn-harvest-plugin --features kafka --test zeta --no-run",
            ]
        );
        let shard1 = compile_plan(&rows, Some(Shard { count: 2, index: 1 }));
        assert_eq!(
            plan_lines(&shard1),
            vec![
                "cargo test -p autumn-harvest-plugin --test beta --no-run",
                "cargo test -p autumn-harvest-plugin --features kafka,sqs --test gamma --no-run",
            ]
        );
        assert_eq!(
            compile_plan(&rows, Some(Shard { count: 9, index: 8 })),
            Vec::new()
        );
    }

    #[test]
    fn archive_takes_the_union_of_live_db_rows() {
        let inv = archive_invocation(&records(MANIFEST_TEXT), "/tmp/a.tar.zst", "0", "0");
        assert_eq!(
            inv.line(),
            "env CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 cargo nextest archive \
             --archive-file /tmp/a.tar.zst -p autumn-harvest -p autumn-harvest-plugin \
             --test alpha --test beta --test gamma --test integration \
             --features autumn-harvest-plugin/kafka,autumn-harvest-plugin/sqs,autumn-harvest/testing"
        );
        assert_eq!(inv.argv[0], "cargo");
        assert_eq!(inv.label, "archive /tmp/a.tar.zst");
        let none = archive_invocation(&records("allos a b - -\n"), "x", "2", "1");
        assert_eq!(
            none.line(),
            "env CARGO_PROFILE_DEV_DEBUG=2 CARGO_PROFILE_TEST_DEBUG=1 cargo nextest archive --archive-file x"
        );
    }

    #[test]
    fn extract_unpacks_into_the_checkout() {
        assert_eq!(
            extract_invocation("/tmp/a.tar.zst").line(),
            "cargo nextest list --archive-file /tmp/a.tar.zst --workspace-remap . \
             --extract-to . --extract-overwrite --list-type binaries-only"
        );
    }

    #[test]
    fn filterset_has_one_term_per_row_in_order() {
        let rows = records(MANIFEST_TEXT);
        assert_eq!(
            filterset(&rows, "linux"),
            Ok(
                "(binary_id(=autumn-harvest::integration) & test(queue_tests)) \
                | binary_id(=autumn-harvest::integration) \
                | binary_id(=autumn-harvest-plugin::beta) \
                | binary_id(=autumn-harvest-plugin::alpha) \
                | (binary_id(=autumn-harvest-plugin::gamma) & test(a::b))"
                    .to_string()
            )
        );
        assert_eq!(filterset(&rows, "nothing"), Ok(String::new()));
    }

    #[test]
    fn filterset_refuses_a_filter_outside_the_safe_charset() {
        let rows = records("linux a b - good\nlinux a b - bad|tests\nlinux a b - Up\n");
        let bad = filterset(&rows, "linux");
        assert_eq!(bad, Err(vec!["bad|tests".to_string(), "Up".to_string()]));
        assert_eq!(
            refused_message(&["bad|tests".to_string()]),
            "run-suites: unsupported manifest filter: bad|tests"
        );
    }

    #[test]
    fn nextest_partitions_by_hash_and_flags_linuxpart() {
        let inv = nextest_invocation("linux", "E", Some("21"), 20);
        assert_eq!(
            inv.line(),
            "env cargo nextest run --binaries-metadata target/nextest/binaries-metadata.json \
             --cargo-metadata target/nextest/cargo-metadata.json --workspace-remap . \
             --target-dir-remap target --test-threads 1 --no-fail-fast --no-tests warn -E E \
             --partition hash:21/21"
        );
        let part = nextest_invocation("linuxpart", "E", None, 0);
        assert!(
            part.line()
                .starts_with("env HARVEST_TEST_PARTITIONED=1 cargo nextest run")
        );
        assert!(!part.line().contains("--partition"));
        assert_eq!(part.label, "nextest linuxpart");
    }

    #[test]
    fn shard_settings_parse_like_the_script() {
        assert_eq!(shard_from(None, Some("3")).ok(), Some(None));
        assert_eq!(shard_from(Some(""), None).ok(), Some(None));
        assert_eq!(
            shard_from(Some("4"), None).ok(),
            Some(Some(Shard { count: 4, index: 0 }))
        );
        assert_eq!(
            shard_from(Some("4"), Some("3")).ok(),
            Some(Some(Shard { count: 4, index: 3 }))
        );
        assert!(shard_from(Some("0"), None).is_err());
        assert!(shard_from(Some("four"), None).is_err());
        assert!(shard_from(Some("4"), Some("x")).is_err());
    }

    #[test]
    fn dry_run_text_wraps_each_command_in_a_group() {
        let plan = run_plan(&records("allos c t - -\n"), "allos", None);
        assert_eq!(
            dry_run_text(&plan),
            "::group::cargo test -p c --test t\n+ cargo test -p c --test t\n::endgroup::\n"
        );
    }

    #[test]
    fn the_committed_manifest_has_rows_of_each_class() {
        let text = crate::util::read_repo_file(MANIFEST).unwrap_or_default();
        let rows = records(&text);
        for class in ["linux", "linuxpart", "allos"] {
            assert_ne!(run_plan(&rows, class, None), Vec::new(), "{class}");
        }
        assert_ne!(compile_plan(&rows, None), Vec::new());
        assert!(filterset(&rows, "linux").is_ok());
    }
}
