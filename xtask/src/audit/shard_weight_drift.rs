//! `cargo xtask audit shard-weight-drift`: the `test-db-linux` per-shard weight drift report.
//!
//! This is the port of `docs/audits/shard-weight-drift.py`, a Folio corpus
//! harness. The report is deterministic and reproducible on any checkout. It
//! needs no network access and no build of the workspace. It is report-only and
//! always exits 0. "Why report-only" below gives the reason.
//!
//! # The modulo assignment
//!
//! `.github/ci/integration-suites.txt` assigns each `linux` row to a shard by
//! `row_ordinal % SEMAPHORE_SHARD_COUNT`. The `row_ordinal` of a row is its
//! position among the other `linux` rows only, in manifest order. The `do_run`
//! function of `cargo xtask ci run-suites` (`xtask/src/ci/run_suites.rs`) is
//! the implementation that this report mirrors. Its logic stays in this
//! module, so the report does not depend on that module.
//!
//! Two rows land on the same shard when their ordinals are exactly
//! `SEMAPHORE_SHARD_COUNT` apart. The manifest grows alphabetically as suites
//! are added, so that gap drifts with no warning. A shard-10 collision between
//! `integration_e2e` and `quota_enforcement_tests` was found and fixed once.
//! Issue #1267 moved `SEMAPHORE_SHARD_COUNT` from 10 to 11. By 2026-09-21 the
//! gap of the same two suites had drifted from 10 rows back onto 11. That
//! recreated the exact collision that the fix targeted
//! (`docs/rnd/2026-09-21-ci-health-semaphore-quota-outbox-recurrence.md`).
//!
//! That report, and the four before it in the same series, computed
//! `row_ordinal` from a plain line count over the whole manifest file. That
//! count includes comments, blank lines and rows of other classes. `do_run`
//! counts `linux` rows only. The two schemes agree on whether two rows are the
//! same distance apart, because a fixed offset survives either way. So the
//! headline finding of those reports still held. They do not agree on which
//! shard a given row lands in. This report counts the way `do_run` counts. So
//! its shard numbers match the job names of a real CI run.
//!
//! The 2026-09-21 report asked for "a full per-shard test-weight accounting...
//! so the invariant cannot silently drift again". This report is that
//! accounting, run automatically instead of by hand:
//!
//! 1. Read the manifest exactly as the `records()` step of `run-suites` does.
//!    Strip comment and blank lines, and keep the `linux` rows in order.
//!    Number them the way the `row_ordinal` of `do_run` does.
//! 2. Resolve each row to what it actually runs. Each crate and target other
//!    than `autumn-harvest`/`integration` has its own binary. A row there names
//!    `<crate>/tests/<target>.rs` directly. All `autumn-harvest`/`integration`
//!    rows share one binary, because `tests/integration/mod.rs` declares every
//!    module. `run-suites` passes the filter of each row to libtest with no
//!    `--exact`. So the filter is a substring match against every test in that
//!    whole binary, not only against the file that the filter seems to name.
//!    [`integration_test_index`] builds the qualified-name index once.
//!    [`integration_row_weight`] matches each row against it.
//!    **Correction:** an earlier version resolved a row to one guessed file and
//!    counted only the tests of that file. That undercounted any row whose filter
//!    also matches a test elsewhere. For example, `nd_block` was reported as 10,
//!    the tests of `nd_block_tests.rs` only. Nine more tests elsewhere also match
//!    the substring, so the real weight is 19. They are in
//!    `retry_clock_skew_tests`, `dag_compensation_tests`,
//!    `event_partitioning_tests`, `mutex_tests`, `claim_budget_tests` and
//!    `replayer_tests`. [`enumerate_qualified_tests`] explains the mechanism.
//!    The fix was checked against each of the 91 `autumn-harvest`/`integration`
//!    rows in the manifest of that day. Only `nd_block` changed, and none of the
//!    seven heavy collisions below moved.
//! 3. Weigh each row by its runnable tests only. A runnable test is a `#[test]`
//!    or `#[tokio::test]` function that compiles for the feature set of that
//!    row, and that has no `#[ignore]`. The feature set is the default set of the
//!    crate plus the `feats` column of the manifest. `run-suites` never passes
//!    `--no-default-features` for `linux` rows. It passes no `--ignored` or
//!    `--include-ignored`, so an ignored test never runs. A feature gate on an
//!    `autumn-harvest`/`integration` test can live in three places, and each is
//!    honored. The first is the `#[cfg(...)] mod X;` line in
//!    `tests/integration/mod.rs` ([`parse_mod_rs_gates`]). Most gates are
//!    there: 199 declarations, most gated on `db`. The second is an inner
//!    `#![cfg(...)]` attribute that gates a whole file. The third is a
//!    `#[cfg(...)]` on one test or on the `mod { ... }` block that holds several.
//!    [`test_weight`] lists three further corrections to this count. They fix
//!    an undercount of indented tests and plain `#[test]` tests, an undercount
//!    of multi-line attributes, and an overcount of `#[ignore]` tests. Every number
//!    in this comment reflects every fix.
//! 4. Read `SEMAPHORE_SHARD_COUNT` out of the `test-db-linux` job block in
//!    `ci.yml`, not from a copy. Report the total weight of each shard. Report
//!    each shard that carries more than one row at or above the heavy threshold.
//! 5. Add the total weight of the `linuxpart` class onto shard 0, whatever
//!    `SEMAPHORE_SHARD_COUNT` is. **Correction:** an earlier version read only
//!    `linux` rows. The `test-db-linux` job of `ci.yml` also runs the whole
//!    `linuxpart` set as an extra step gated on `matrix.shard == 0`. That set is
//!    the re-run on the partitioned `harvest_events` layout (issue #958 AC2). It
//!    is not sharded by ordinal like a `linux` row. It is always the full set,
//!    and it always runs on shard 0. On that day it held 9 rows and 196 more test
//!    executions than the `linux` rows of shard 0. That includes a second pass
//!    of the 120-test `integration_e2e` filter. The omission under-reported the
//!    total of shard 0. It also hid a real collision of two heavy suites there:
//!    `cross_region_dr_tests` from the `linux` rows and `integration_e2e` from
//!    `linuxpart`. And it named the wrong shard as the slowest of the job, as
//!    the update below shows. [`linuxpart_rows_in_order`] and the
//!    `linuxpart_weights` parameter of [`shard_report`] are the fix. [`run`]
//!    always adds them, with no flag, because shard 0 always pays this cost.
//!
//! # Update of 2026-09-27
//!
//! The manifest drifted three times on the same day, during the second PR of
//! this report (#1746). `SEMAPHORE_SHARD_COUNT` is now 21, not 11. Each
//! collision named earlier in this comment was found under 11. Issue #1707
//! rebalanced the count after the first PR of this report. The manifest grew
//! from 151 to 156 `linux` rows after that. A `trunk-dev` merge in the middle
//! of review grew `dispatch_redis` past the heavy threshold. The `linuxpart`
//! omission above was found in the same review. No collision that this
//! comment named under the old count or the old manifest still holds. That is
//! not a defect in the logic of this report. It shows that a copied finding is
//! a snapshot, correct only for the commit that produced it. The table below
//! is the tree that PR merged. Run the report again rather than copy it into
//! a future report:
//!
//! | Shard | Colliding rows (ordinal, weight) |
//! |---|---|
//! | 0  | `cross_region_dr_tests` (84, 31), `integration_e2e` (linuxpart, 120) |
//! | 4  | `shard_rebalance_db_tests` (88, 112), `workflow_rerun_integration` (151, 68) |
//! | 6  | `admission_gate_authoritative` (6, 34), `dispatch_redis` (153, 33) |
//! | 8  | `event_partitioning_tests` (29, 155), `rate_limit_bucket_gc_tests` (50, 30) |
//! | 10 | `interface_schema_integration` (115, 31), `stall_diagnosis_integration` (136, 79) |
//! | 13 | `capability_miss_tests` (13, 46), `pacing_override_integration` (118, 46) |
//! | 19 | `backup_verify_tests` (82, 57), `ui_integration` (145, 149) |
//!
//! The collision on shard 0 is new to this report, from the `linuxpart` fix
//! above. It is not a move of an earlier collision. PR #1703 found seven pairs
//! on 09-22 under the old count of 11. Its own shard-0 pair of
//! `integration_e2e` and `quota_enforcement_tests` is one of them. None of the
//! seven still collides at N=21. They land on six different shards that do not
//! collide, plus the new shard-0 pair. With `linuxpart` counted, the real total
//! of shard 0 is 316, not 120. So shard 0 is the heaviest shard of the job, not
//! shard 19 with its earlier total of 269. The older tables of this comment
//! stay for their history, not as a claim about the manifest of today. The
//! corrections to the counting logic are why this report counts as it does.
//!
//! A sweep of `SEMAPHORE_SHARD_COUNT` from 9 through 24 ran again on that tree.
//! It included the `linuxpart` set in each total of shard 0. It still found no
//! value with zero heavy collisions (the `--sweep` output of 2026-09-27). The
//! floor across the sixteen counts is 4 colliding shards, at N=18 and N=20
//! only. So a new bump of `SEMAPHORE_SHARD_COUNT` would likely move the next
//! collision rather than remove the failure mode. The job of this report is to
//! keep that visible, not to pick the next number.
//!
//! # Why report-only
//!
//! A gate now would make every open PR red on a structural property that
//! nobody caused and that this report cannot fix. The real fix is one of two
//! changes. One is a weight-aware assignment in `run-suites` that replaces the
//! modulo scheme. The other is a new simulation before each change of
//! `SEMAPHORE_SHARD_COUNT`. Either is a bigger decision than one CI-health pass
//! should gate on alone. [`run`] always returns exit code 0. When the
//! assignment is fixed, make it fail on a heavy collision, so the invariant
//! cannot drift again.
//!
//! Usage:
//!
//! ```text
//! cargo xtask audit shard-weight-drift [--threshold N] [--sweep]
//! ```
//!
//! The exit code is always 0. `--sweep` also prints the N=9..24 collision
//! sweep that the findings of this comment came from.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use regex::Regex;
use smop::prelude::*;

use crate::util::repo_root;

/// The default heavy threshold, in tests per row.
pub const HEAVY_THRESHOLD_DEFAULT: i64 = 30;

/// Arguments of `cargo xtask audit shard-weight-drift`.
#[derive(Debug, Clone, Copy, Args)]
pub struct Args {
    /// A row with at least this many tests is heavy.
    #[arg(long, default_value_t = HEAVY_THRESHOLD_DEFAULT, allow_negative_numbers = true)]
    pub threshold: i64,
    /// Also print the N=9..24 shard-count sweep.
    #[arg(long)]
    pub sweep: bool,
    /// Run the parser fixtures, then exit.
    #[arg(long)]
    pub self_test: bool,
}

/// One manifest record: `os crate target feats filter`.
pub type Record = Vec<String>;

/// A test and the `cfg` conditions that must all hold for it to exist.
pub type QualifiedTest = (String, Vec<String>);

/// One weighed row: its ordinal, label and runnable test count.
pub type Weight = (usize, String, i64);

/// A row whose test file is missing: its ordinal, label and path.
pub type Unresolved = (usize, String, PathBuf);

/// One member of a shard: its ordinal text, label and weight.
///
/// The ordinal of a `linuxpart` row is the text `linuxpart`.
pub type Member = (String, String, i64);

/// The compiled patterns of the scan.
pub struct Patterns {
    // Optional leading whitespace, because nested-module tests are indented.
    // Then either `#[test]` or `#[tokio::test...]`, with any argument variant.
    // An earlier version anchored to column 0 and matched only `#[tokio::test`.
    // That missed every indented test and every plain `#[test]`. The
    // correction notes in the module comment name the files that this changed.
    test_attr: Regex,
    ignore_attr: Regex,
    attr_line: Regex,
    fn_line: Regex,
    comment_or_blank: Regex,
    // `#[cfg(...)]` gates one item, typically the next `mod` or `fn`.
    // `#![cfg(...)]` is an inner attribute that gates the item it appears
    // inside. Here that is always the whole file, because each one in this
    // corpus is the first line of a `tests/integration/*.rs` module file.
    // Both are single-line in every instance of the corpus. That was checked
    // against the five `linux`-reachable files that use `cfg(feature`.
    cfg_attr: Regex,
    cfg_inner_attr: Regex,
    mod_open: Regex,
    mod_decl: Regex,
    feature: Regex,
    feature_full: Regex,
    default_features: Regex,
    job_start: Regex,
    next_job: Regex,
    shard_count: Regex,
}

impl Patterns {
    /// Compiles every pattern.
    pub fn new() -> Result<Self> {
        let compile = |p: &str| Regex::new(p).with_context(|| format!("compiling the pattern {p}"));
        Ok(Self {
            test_attr: compile(r"^\s*#\[(?:tokio::test|test)\b")?,
            ignore_attr: compile(r"^\s*#\[ignore\b")?,
            attr_line: compile(r"^\s*#\[")?,
            fn_line: compile(r"^\s*(?:pub\s+)?(?:async\s+)?fn\s+(\w+)")?,
            comment_or_blank: compile(r"^\s*(?:#|\z)")?,
            cfg_attr: compile(r"^\s*#\[cfg\((.*)\)\]\s*\z")?,
            cfg_inner_attr: compile(r"^\s*#!\[cfg\((.*)\)\]\s*\z")?,
            mod_open: compile(r"^\s*(?:pub\s+)?mod\s+(\w+)\s*\{")?,
            mod_decl: compile(r"^\s*(?:pub\s+)?mod\s+(\w+)\s*;")?,
            feature: compile(r#"feature\s*=\s*"([^"]+)""#)?,
            feature_full: compile(r#"^feature\s*=\s*"([^"]+)"\z"#)?,
            default_features: compile(r"(?m)^default\s*=\s*\[([^\]]*)\]")?,
            job_start: compile(r"(?m)^  test-db-linux:")?,
            next_job: compile(r"(?m)^  \S")?,
            shard_count: compile(r#"SEMAPHORE_SHARD_COUNT:\s*"(\d+)""#)?,
        })
    }

    /// Mirrors the `records()` step of `run-suites`: strip comment and blank lines.
    pub fn parse_manifest_records(&self, text: &str) -> Vec<Record> {
        splitlines(text)
            .into_iter()
            .filter(|line| !self.comment_or_blank.is_match(line))
            .map(|line| {
                line.split_whitespace()
                    .map(ToString::to_string)
                    .collect::<Record>()
            })
            .filter(|parts| parts.len() == 5)
            .collect()
    }

    /// Evaluates the inner condition text of a `cfg(...)` against a feature set.
    ///
    /// It handles only the forms that the corpus holds today, checked by hand.
    /// A bare `feature = "X"` and an `all(feature = "A", ...)` read the set.
    /// No `any(feature = ...)` combination appears in the corpus. `test`,
    /// `unix` and `target_os = "linux"` are always true, because the Docker
    /// shard of CI runs on `ubuntu-latest`. `not(unix)` is always false there.
    ///
    /// Any other condition counts as enabled, so the scan fails open. An
    /// unknown condition must not make the report undercount a test that runs.
    /// Each correction in the history of this report fixed an undercount. An
    /// overcount is the new risk that this leaves open. So the coverage of this
    /// function is documented here and not left silent.
    pub fn cfg_is_enabled(&self, condition: &str, enabled: &BTreeSet<String>) -> bool {
        let condition = condition.trim();
        if condition == "test" || condition == "unix" || condition == "target_os = \"linux\"" {
            return true;
        }
        if condition == "not(unix)" {
            return false;
        }
        if let Some(inner) = condition
            .strip_prefix("all(")
            .and_then(|c| c.strip_suffix(')'))
        {
            return self
                .feature
                .captures_iter(inner)
                .all(|caps| enabled.contains(&caps[1]));
        }
        if let Some(caps) = self.feature_full.captures(condition) {
            return enabled.contains(&caps[1]);
        }
        true
    }

    /// Reads `default = [...]` out of the `[features]` table of `<crate>/Cargo.toml`.
    ///
    /// The set is not copied by hand, because the defaults of a crate can
    /// change on their own. No `default` key, or no `[features]` table, means
    /// an empty default set. That matches the behavior of Cargo.
    pub fn crate_default_features(&self, root: &Path, krate: &str) -> Result<BTreeSet<String>> {
        let path = crate_dir(root, krate).join("Cargo.toml");
        let text = match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(BTreeSet::new());
            }
            Err(error) => return Err(error).with_context(|| format!("reading {}", path.display())),
        };
        let Some(caps) = self.default_features.captures(&text) else {
            return Ok(BTreeSet::new());
        };
        Ok(caps[1]
            .split(',')
            .filter(|f| !f.trim().is_empty())
            .map(|f| f.trim().trim_matches('"').to_string())
            .collect())
    }

    /// Returns the feature set of the `cargo test` call of one row.
    ///
    /// `do_run` never passes `--no-default-features` for `linux` rows. Only one
    /// `allos` case does. So the set is the defaults of the crate plus the
    /// features that the `feats` column of the manifest adds.
    ///
    /// Known limitation: this is a plain set union, not a walk of the
    /// `[features]` graph. A requested feature X can imply a feature Y through
    /// `X = ["Y", ...]` in `Cargo.toml`. When neither the row nor the defaults
    /// name Y, the scan does not see Y. None of the features that gate a test
    /// in a `linux`-reachable file hit that gap today. `db` is always in the
    /// defaults of `autumn-harvest`. Each row that gates on `testing` either
    /// lists it in `feats` or does not enable it. That was checked by hand
    /// against the five affected files.
    pub fn enabled_features_for_row(
        &self,
        root: &Path,
        krate: &str,
        feats: &str,
    ) -> Result<BTreeSet<String>> {
        let mut features = self.crate_default_features(root, krate)?;
        if feats != "-" {
            features.extend(
                feats
                    .split(',')
                    .map(str::trim)
                    .filter(|f| !f.is_empty())
                    .map(ToString::to_string),
            );
        }
        Ok(features)
    }

    /// Counts the runnable test functions of a file, or `None` when it is missing.
    ///
    /// See [`Self::test_weight_text`] for the scan.
    pub fn test_weight(&self, path: &Path, enabled: &BTreeSet<String>) -> Result<Option<i64>> {
        Ok(read_optional(path)?.map(|text| self.test_weight_text(&text, enabled)))
    }

    /// Counts the `#[test]` and `#[tokio::test]` functions that run under `enabled`.
    ///
    /// `run-suites` calls plain `cargo test` with no `--ignored` or
    /// `--include-ignored`. So an `#[ignore]` test never runs. A test behind a
    /// `cfg(feature = "X")` that the row does not enable never compiles. The
    /// count excludes both.
    ///
    /// **Three corrections.** Each one is the opposite failure of the
    /// undercounts that the module comment covers. Without them, this function
    /// counts too many:
    ///
    /// 1. `#[ignore]`: `claim_budget_tests.rs` carries 7 ignored one-shot
    ///    evidence generators. The file itself says they are "not a repeatable
    ///    CI assertion". They raised its weight from 29, what runs, to 36. That
    ///    made up a heavy collision that direct log evidence does not support.
    /// 2. `cfg(feature = ...)`: the `replay_tests` module of
    ///    `retry_after_tests.rs` is `#[cfg(feature = "testing")]`. Its manifest
    ///    row requests no extra features, and the defaults of `autumn-harvest`
    ///    do not include `testing`. So its one test never compiles for that row.
    ///    The real weight of the file is 6, not the 7 of a plain count.
    /// 3. `#![cfg(...)]`: an inner attribute gates the enclosing item. Here that
    ///    is always the whole file, because each one in the corpus is the first
    ///    line of a file. `claim_budget_tests.rs` and
    ///    `quota_history_bytes_perf_tests.rs` both open with
    ///    `#![cfg(feature = "db")]`. `db` is a default feature of
    ///    `autumn-harvest`, so this is true for every `linux` row today. This
    ///    case is inert today, but it stays correct rather than assumed.
    ///
    /// Attributes stack directly above their `fn` or `async fn` line, with no
    /// blank line between. A `///` doc block can sit above the stack, but never
    /// inside it. So a small forward scan is accurate without a Rust parser. It
    /// collects contiguous `#[...]` lines, decides on the next `fn` line, and
    /// resets. A `#[cfg(...)]` directly above a `mod name {` gates every test
    /// inside that module until its closing brace. A running count of `{` and
    /// `}` tracks that. No `linux`-reachable file that uses `cfg(feature` nests
    /// a gated `mod` inside another. That was checked, not assumed. So one
    /// stack level suffices, but the code stacks correctly anyway.
    ///
    /// An attribute can span several lines in two ways in this corpus. One is
    /// an argument list broken across lines (`#[allow(\n clippy::x,\n)]`). The
    /// other is a string literal continued with a trailing backslash, as in
    /// `#[ignore = "...` and a `...script.sh"]` continuation line. A running
    /// count of unmatched `[` and `(` against `]` and `)` tracks both. The first
    /// form stays unbalanced until its closing `)]`. The second stays unbalanced
    /// from the `[` of `#[` until the `]` on the continuation line. The
    /// backslash needs no special handling, because the count sees only
    /// brackets. **Correction:** an earlier version tracked only the
    /// backslash form. It closed a multi-line `#[allow(...)]` at its first line
    /// and lost the pending test attribute before the `fn`. That undercounted
    /// `hot_code_swap_tests.rs` by 2 real tests: 79 against the correct 77, once
    /// the `#[ignore]` fix also applied.
    pub fn test_weight_text(&self, text: &str, enabled: &BTreeSet<String>) -> i64 {
        let lines = splitlines(text);
        for line in lines.iter().take(5) {
            if let Some(caps) = self.cfg_inner_attr.captures(line)
                && !self.cfg_is_enabled(&caps[1], enabled)
            {
                return 0;
            }
        }
        let mut count = 0;
        let mut scan = Scan::default();
        // Each scope entry holds the brace depth to pop at and whether it is enabled.
        let mut scopes: Vec<(i64, bool)> = Vec::new();
        for line in lines {
            let delta = count_of(line, &['{']) - count_of(line, &['}']);
            if scan.bracket_depth > 0 {
                scan.bracket_depth += count_of(line, &['[', '(']) - count_of(line, &[']', ')']);
                scan.brace_depth += delta;
                pop_scopes(&mut scopes, scan.brace_depth);
                continue;
            }
            if self.attr_line.is_match(line) {
                self.read_attr(line, &mut scan);
                scan.brace_depth += delta;
                pop_scopes(&mut scopes, scan.brace_depth);
                continue;
            }
            if self.mod_open.is_match(line) {
                let on = scan.cfgs.iter().all(|c| self.cfg_is_enabled(c, enabled));
                scopes.push((scan.brace_depth, on));
                scan.reset();
                scan.brace_depth += delta;
                continue;
            }
            if self.fn_line.is_match(line) {
                let scope_on = scopes.iter().all(|(_, on)| *on);
                let item_on = scan.cfgs.iter().all(|c| self.cfg_is_enabled(c, enabled));
                if scan.test && !scan.ignore && scope_on && item_on {
                    count += 1;
                }
                scan.reset();
                scan.brace_depth += delta;
                pop_scopes(&mut scopes, scan.brace_depth);
                continue;
            }
            scan.brace_depth += delta;
            pop_scopes(&mut scopes, scan.brace_depth);
            let stripped = line.trim();
            if stripped.is_empty() || stripped.starts_with("//") {
                continue;
            }
            // Any other code line between attributes and a fn would be unusual.
            // It ends the pending attribute stack, so a later fn does not take it.
            scan.reset();
        }
        count
    }

    /// Records one attribute line in the pending stack.
    fn read_attr(&self, line: &str, scan: &mut Scan) {
        if self.test_attr.is_match(line) {
            scan.test = true;
        }
        if self.ignore_attr.is_match(line) {
            scan.ignore = true;
        }
        if let Some(caps) = self.cfg_attr.captures(line) {
            scan.cfgs.push(caps[1].to_string());
        }
        scan.bracket_depth = count_of(line, &['[', '(']) - count_of(line, &[']', ')']);
    }

    /// Returns the qualified name and the conditions of each runnable test of a file.
    ///
    /// The name is libtest-style, `<mod>::<nested_mod>::<fn_name>`, without the
    /// file stem. The caller prepends the stem, because the `mod X;` name of
    /// `mod.rs` is what libtest uses as the top path segment. That name can
    /// differ from the file stem in principle, though it never does today. The
    /// conditions must all hold for the test to exist. They are the inner
    /// `#![cfg(...)]` of the file, each enclosing `#[cfg(...)] mod { ... }`, and
    /// the item-level `#[cfg(...)]` of the test.
    ///
    /// **Why this exists apart from [`Self::test_weight`]:** `run-suites` passes
    /// the `filter` column of a row to libtest as a plain positional argument,
    /// with no `--exact`. The default of libtest is a substring match against
    /// the qualified name of every test in the whole `integration` binary. It
    /// is not limited to the one file that [`resolve_test_file`] guesses from
    /// the filter. So the real weight of a row is the count of qualified names
    /// in the entire binary that contain its filter. That can be more than the
    /// file that it seems to name. For example, the `nd_block` row was reported
    /// as 10, the count of `nd_block_tests.rs`. But `retry_clock_skew_tests.rs`,
    /// `dag_compensation_tests.rs` and `event_partitioning_tests.rs` each carry
    /// a test whose name also contains `nd_block`. Those names are
    /// `requeue_workflow_task_nd_blocked_...`, `..._never_nd_blocks` and
    /// `..._dropped_and_blocked`. So the real weight of the row is higher.
    /// [`integration_test_index`] builds the index that the substring match
    /// needs. The single-file guess of [`resolve_test_file`] no longer weighs
    /// an `autumn-harvest`/`integration` row. It only labels the row.
    ///
    /// The scan is the same forward scan as [`Self::test_weight_text`]. Its
    /// comment explains the attribute, bracket and brace mechanics. This scan
    /// also tracks the stack of enclosing `mod name { ... }` names for the
    /// qualified path, and it collects each test instead of a count.
    pub fn enumerate_qualified_tests(&self, path: &Path) -> Result<Option<Vec<QualifiedTest>>> {
        Ok(read_optional(path)?.map(|text| self.enumerate_qualified_tests_text(&text)))
    }

    /// Returns the qualified tests of one file's text.
    pub fn enumerate_qualified_tests_text(&self, text: &str) -> Vec<QualifiedTest> {
        let lines = splitlines(text);
        let file_gate: Vec<String> = lines
            .iter()
            .take(5)
            .filter_map(|line| self.cfg_inner_attr.captures(line).map(|c| c[1].to_string()))
            .collect();
        let mut results = Vec::new();
        let mut scan = Scan::default();
        // Each scope entry holds the brace depth to pop at, the module name and its conditions.
        let mut scopes: Vec<(i64, String, Vec<String>)> = Vec::new();
        let pop = |scopes: &mut Vec<(i64, String, Vec<String>)>, depth: i64| {
            while scopes.last().is_some_and(|s| depth <= s.0) {
                scopes.pop();
            }
        };
        for line in lines {
            let delta = count_of(line, &['{']) - count_of(line, &['}']);
            if scan.bracket_depth > 0 {
                scan.bracket_depth += count_of(line, &['[', '(']) - count_of(line, &[']', ')']);
                scan.brace_depth += delta;
                pop(&mut scopes, scan.brace_depth);
                continue;
            }
            if self.attr_line.is_match(line) {
                self.read_attr(line, &mut scan);
                scan.brace_depth += delta;
                pop(&mut scopes, scan.brace_depth);
                continue;
            }
            if let Some(caps) = self.mod_open.captures(line) {
                scopes.push((scan.brace_depth, caps[1].to_string(), scan.cfgs.clone()));
                scan.reset();
                scan.brace_depth += delta;
                continue;
            }
            if let Some(caps) = self.fn_line.captures(line) {
                if scan.test && !scan.ignore {
                    let mut conditions = file_gate.clone();
                    for (_, _, mod_conditions) in &scopes {
                        conditions.extend(mod_conditions.iter().cloned());
                    }
                    conditions.extend(scan.cfgs.iter().cloned());
                    let mut path: Vec<&str> =
                        scopes.iter().map(|(_, name, _)| name.as_str()).collect();
                    path.push(&caps[1]);
                    results.push((path.join("::"), conditions));
                }
                scan.reset();
                scan.brace_depth += delta;
                pop(&mut scopes, scan.brace_depth);
                continue;
            }
            scan.brace_depth += delta;
            pop(&mut scopes, scan.brace_depth);
            let stripped = line.trim();
            if stripped.is_empty() || stripped.starts_with("//") {
                continue;
            }
            scan.reset();
        }
        results
    }

    /// Returns each `mod X;` declaration of `mod.rs` with the `cfg` conditions above it.
    ///
    /// Most feature gates of the integration tests live here, not inside the
    /// files. There are 199 `mod` declarations, and most carry
    /// `#[cfg(feature = "db")]`. A handful gate on `testing`, `chaos`,
    /// `debugger` or `wasm-activities` instead. A file can also carry its own
    /// `#![cfg(...)]` and `#[cfg(...)]` gates ([`Self::enumerate_qualified_tests`]).
    /// [`integration_test_index`] joins both with AND.
    pub fn parse_mod_rs_gates(&self, text: &str) -> Vec<(String, Vec<String>)> {
        let mut gates: Vec<(String, Vec<String>)> = Vec::new();
        let mut pending = Vec::new();
        for line in splitlines(text) {
            if let Some(caps) = self.cfg_attr.captures(line) {
                pending.push(caps[1].to_string());
                continue;
            }
            if let Some(caps) = self.mod_decl.captures(line) {
                let name = caps[1].to_string();
                let gate = std::mem::take(&mut pending);
                // A repeated name keeps its first position and takes the last gate, as a dict does.
                if let Some(slot) = gates.iter_mut().find(|(n, _)| *n == name) {
                    slot.1 = gate;
                } else {
                    gates.push((name, gate));
                }
                continue;
            }
            let stripped = line.trim();
            if stripped.is_empty() || stripped.starts_with("//") {
                continue;
            }
            pending.clear();
        }
        gates
    }

    /// Extracts `SEMAPHORE_SHARD_COUNT` from the `test-db-linux` job block only.
    ///
    /// `test-nodb` declares the same variable with a different value (4). So a
    /// search of the whole file would be ambiguous.
    pub fn read_shard_count(&self, ci_yaml: &str) -> Result<Option<usize>> {
        for start in self.job_start.find_iter(ci_yaml) {
            let Some(next) = self.next_job.find_at(ci_yaml, start.end()) else {
                continue;
            };
            let block = &ci_yaml[start.start()..next.start()];
            let Some(caps) = self.shard_count.captures(block) else {
                return Ok(None);
            };
            let count = caps[1]
                .parse::<usize>()
                .with_context(|| format!("reading SEMAPHORE_SHARD_COUNT {}", &caps[1]))?;
            return Ok(Some(count));
        }
        Ok(None)
    }
}

/// The pending attribute stack of the forward scan.
#[derive(Default)]
struct Scan {
    test: bool,
    ignore: bool,
    cfgs: Vec<String>,
    bracket_depth: i64,
    brace_depth: i64,
}

impl Scan {
    fn reset(&mut self) {
        self.test = false;
        self.ignore = false;
        self.cfgs.clear();
    }
}

fn pop_scopes(scopes: &mut Vec<(i64, bool)>, depth: i64) {
    while scopes.last().is_some_and(|s| depth <= s.0) {
        scopes.pop();
    }
}

fn count_of(line: &str, chars: &[char]) -> i64 {
    i64::try_from(line.chars().filter(|c| chars.contains(c)).count()).unwrap_or(i64::MAX)
}

/// Reads a file, or returns `None` when it does not exist.
fn read_optional(path: &Path) -> Result<Option<String>> {
    match std::fs::read_to_string(path) {
        Ok(text) => Ok(Some(text)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).with_context(|| format!("reading {}", path.display())),
    }
}

/// Splits text at line boundaries, as Python's `str.splitlines` does.
pub fn splitlines(text: &str) -> Vec<&str> {
    let mut lines = Vec::new();
    let mut start = 0;
    let mut chars = text.char_indices().peekable();
    while let Some((i, ch)) = chars.next() {
        let boundary = matches!(
            ch,
            '\n' | '\r'
                | '\u{b}'
                | '\u{c}'
                | '\u{1c}'
                | '\u{1d}'
                | '\u{1e}'
                | '\u{85}'
                | '\u{2028}'
                | '\u{2029}'
        );
        if !boundary {
            continue;
        }
        lines.push(&text[start..i]);
        let mut next = i + ch.len_utf8();
        if ch == '\r' && chars.peek().is_some_and(|&(_, c)| c == '\n') {
            chars.next();
            next += 1;
        }
        start = next;
    }
    if start < text.len() {
        lines.push(&text[start..]);
    }
    lines
}

/// Mirrors the `row_ordinal` of `do_run`: the position among `linux` rows only.
pub fn linux_rows_in_order(records: &[Record]) -> Vec<Record> {
    records
        .iter()
        .filter(|r| r[0] == "linux")
        .cloned()
        .collect()
}

/// Returns the `linuxpart` rows in manifest order.
///
/// These rows are the re-run on the partitioned `harvest_events` layout.
/// Unlike `linux` rows, they are never sharded by ordinal. The `test-db-linux`
/// job of `ci.yml` runs the whole `linuxpart` set as one extra step gated on
/// `matrix.shard == 0`. Each total of shard 0 must add the weight of this set.
/// Each heavy-collision check of shard 0 must consider its members beside the
/// `linux` rows of shard 0. Otherwise the report undercounts the real workload
/// of shard 0, whatever `SEMAPHORE_SHARD_COUNT` is.
pub fn linuxpart_rows_in_order(records: &[Record]) -> Vec<Record> {
    records
        .iter()
        .filter(|r| r[0] == "linuxpart")
        .cloned()
        .collect()
}

/// Returns the directory of a manifest `crate`.
///
/// Library crates sit at the repository root. An example crate, such as
/// `standalone-runner` (issue #1615), sits under `examples/`.
pub fn crate_dir(root: &Path, krate: &str) -> PathBuf {
    let root_dir = root.join(krate);
    let example_dir = root.join("examples").join(krate);
    if !root_dir.exists() && example_dir.exists() {
        example_dir
    } else {
        root_dir
    }
}

/// Returns the test binary source of a `linux` row's crate and target.
///
/// This covers every crate and target except `autumn-harvest`/`integration`.
/// [`weigh_rows`] never routes that target here. Its tests compile into one
/// shared binary (`tests/integration/mod.rs`). So a row there is weighed by a
/// substring match on [`integration_test_index`] instead
/// ([`integration_row_weight`]), not by one resolved file.
pub fn resolve_test_file(root: &Path, krate: &str, target: &str) -> PathBuf {
    crate_dir(root, krate)
        .join("tests")
        .join(format!("{target}.rs"))
}

/// Returns the display label of a row.
pub fn row_label(krate: &str, target: &str, filter: &str) -> String {
    if krate == "autumn-harvest" && target == "integration" && filter != "-" {
        format!("{krate}/{target} -- {filter}")
    } else {
        format!("{krate}/{target}")
    }
}

/// Returns `(qualified_name, conditions)` for every test of the `autumn-harvest`/`integration` binary.
///
/// It covers every file that `mod.rs` declares. The qualified name starts with
/// the module name that `mod.rs` declares, which matches the stem-less output
/// of [`Patterns::enumerate_qualified_tests`]. The conditions start with the
/// gate of that `mod X;` line, followed by the gates inside the file. The
/// structural scan does not depend on the enabled features, so the caller
/// builds the index once. Only the later filter of each row depends on them.
pub fn integration_test_index(patterns: &Patterns, mod_rs: &Path) -> Result<Vec<QualifiedTest>> {
    let text = fs::read_string(mod_rs)?;
    let dir = mod_rs.parent().unwrap_or_else(|| Path::new(""));
    let mut index = Vec::new();
    for (name, gate) in patterns.parse_mod_rs_gates(&text) {
        let Some(tests) = patterns.enumerate_qualified_tests(&dir.join(format!("{name}.rs")))?
        else {
            continue;
        };
        for (qualified, conditions) in tests {
            let mut all = gate.clone();
            all.extend(conditions);
            index.push((format!("{name}::{qualified}"), all));
        }
    }
    Ok(index)
}

/// Returns the real weight of an `autumn-harvest`/`integration` row.
///
/// It counts the qualified names of [`integration_test_index`] that are
/// enabled under `enabled` and that contain `filter` as a substring. That
/// matches the libtest call of `run-suites`, which has no `--exact`.
/// [`Patterns::enumerate_qualified_tests`] explains why a per-file guess undercounts.
pub fn integration_row_weight(
    patterns: &Patterns,
    index: &[QualifiedTest],
    filter: &str,
    enabled: &BTreeSet<String>,
) -> i64 {
    let hits = index
        .iter()
        .filter(|(name, conditions)| {
            name.contains(filter)
                && conditions
                    .iter()
                    .all(|c| patterns.cfg_is_enabled(c, enabled))
        })
        .count();
    i64::try_from(hits).unwrap_or(i64::MAX)
}

/// Weighs each row, and returns the weights and the unresolved rows.
pub fn weigh_rows(
    patterns: &Patterns,
    root: &Path,
    index: &mut Option<Vec<QualifiedTest>>,
    rows: &[Record],
) -> Result<(Vec<Weight>, Vec<Unresolved>)> {
    let mut weights = Vec::new();
    let mut unresolved = Vec::new();
    for (ordinal, row) in rows.iter().enumerate() {
        let (krate, target, feats, filter) = (&row[1], &row[2], &row[3], &row[4]);
        let label = row_label(krate, target, filter);
        let enabled = patterns.enabled_features_for_row(root, krate, feats)?;
        // All autumn-harvest/integration rows share one binary. The filter of
        // run-suites is a libtest substring match against every test in it,
        // with no --exact. A weight from the one file that the filter seems to
        // name undercounts when the substring also matches tests elsewhere.
        // Each other crate and target has its own binary per row. In the
        // manifest of today its filter is always "-", so the simpler per-file
        // count stays correct and cheaper there.
        let weight = if krate == "autumn-harvest" && target == "integration" {
            if index.is_none() {
                let mod_rs = root.join("autumn-harvest/tests/integration/mod.rs");
                *index = Some(integration_test_index(patterns, &mod_rs)?);
            }
            integration_row_weight(patterns, index.as_deref().unwrap_or(&[]), filter, &enabled)
        } else {
            let path = resolve_test_file(root, krate, target);
            let found = patterns.test_weight(&path, &enabled)?;
            if found.is_none() {
                unresolved.push((ordinal, label.clone(), path));
            }
            found.unwrap_or(0)
        };
        weights.push((ordinal, label, weight));
    }
    Ok((weights, unresolved))
}

/// The totals, members and heavy collisions of one shard count.
pub struct Report {
    /// The total weight of each shard.
    pub totals: Vec<i64>,
    /// The rows of each shard.
    pub members: Vec<Vec<Member>>,
    /// Each shard with more than one heavy row, and those rows.
    pub heavy_collisions: Vec<(usize, Vec<Member>)>,
}

/// Assigns rows to shards by ordinal, and finds the heavy collisions.
///
/// The `linuxpart_weights` always land on shard 0, whatever `shard_count` is.
/// They are not assigned by `ordinal % shard_count` like a `linux` row. The
/// `test-db-linux` job runs them as its own extra step gated on
/// `matrix.shard == 0`, outside the sharding scheme that this function models.
pub fn shard_report(
    weights: &[Weight],
    shard_count: usize,
    heavy_threshold: i64,
    linuxpart_weights: &[Weight],
) -> Report {
    let mut totals = vec![0_i64; shard_count];
    let mut members: Vec<Vec<Member>> = vec![Vec::new(); shard_count];
    for (ordinal, label, weight) in weights {
        let shard = ordinal % shard_count;
        totals[shard] += weight;
        members[shard].push((ordinal.to_string(), label.clone(), *weight));
    }
    for (_, label, weight) in linuxpart_weights {
        totals[0] += weight;
        members[0].push(("linuxpart".into(), label.clone(), *weight));
    }
    let heavy_collisions = members
        .iter()
        .enumerate()
        .filter_map(|(shard, rows)| {
            let heavy: Vec<Member> = rows
                .iter()
                .filter(|m| m.2 >= heavy_threshold)
                .cloned()
                .collect();
            (heavy.len() > 1).then_some((shard, heavy))
        })
        .collect();
    Report {
        totals,
        members,
        heavy_collisions,
    }
}

fn spread(totals: &[i64]) -> (i64, i64) {
    let max = totals.iter().copied().max().unwrap_or(0);
    let min = totals.iter().copied().min().unwrap_or(0);
    (max, min)
}

/// Prints the collision sweep over shard counts `lo..=hi`.
fn sweep(
    weights: &[Weight],
    heavy_threshold: i64,
    linuxpart_weights: &[Weight],
    lo: usize,
    hi: usize,
) {
    println!("\n--- Shard-count sweep N={lo}..{hi} (heavy >= {heavy_threshold}) ---");
    for n in lo..=hi {
        let report = shard_report(weights, n, heavy_threshold, linuxpart_weights);
        let (max, min) = spread(&report.totals);
        println!(
            "N={n:2} spread={:4} max={max:4} min={min:4} heavy_collisions={}",
            max - min,
            report.heavy_collisions.len()
        );
    }
}

fn ensure_eq<T: PartialEq + std::fmt::Debug>(got: &T, want: &T, what: &str) -> Result<()> {
    ensure!(got == want, "{what}: got {got:?}, want {want:?}");
    Ok(())
}

fn features(items: &[&str]) -> BTreeSet<String> {
    items.iter().map(ToString::to_string).collect()
}

/// Checks the parser against small fixtures in the five-column manifest format.
///
/// The report is only as good as its parser.
pub fn self_test(patterns: &Patterns) -> Result<()> {
    let fixture = "\
# comment line, skipped
   # indented comment, skipped

linux        crate-a  integration  -  mod_a
linux        crate-a  integration  -  mod_b
allos        crate-a  integration  -  mod_c
linux        crate-b  suite_x      -  -
linuxpart    crate-a  integration  -  mod_a
";
    let records = patterns.parse_manifest_records(fixture);
    ensure_eq(&records.len(), &5, "non-comment records")?;
    let linux_rows = linux_rows_in_order(&records);
    ensure_eq(&linux_rows.len(), &3, "linux rows")?;
    ensure_eq(&linux_rows[0][4].as_str(), &"mod_a", "first linux filter")?;
    ensure_eq(&linux_rows[1][4].as_str(), &"mod_b", "second linux filter")?;
    ensure_eq(&linux_rows[2][2].as_str(), &"suite_x", "third linux target")?;
    let linuxpart_rows = linuxpart_rows_in_order(&records);
    ensure_eq(&linuxpart_rows.len(), &1, "linuxpart rows")?;
    ensure_eq(&linuxpart_rows[0][4].as_str(), &"mod_a", "linuxpart filter")?;

    // A linuxpart weight is pinned to shard 0, whatever SEMAPHORE_SHARD_COUNT is.
    // It is not assigned by ordinal % shard_count like a linux row. With
    // shard_count=2 a linux ordinal of 1 would otherwise land on shard 1.
    let fake_weights = vec![(0, "row-a".to_string(), 5), (1, "row-b".to_string(), 7)];
    let fake_linuxpart = vec![(0, "linuxpart-row".to_string(), 100)];
    let report = shard_report(&fake_weights, 2, HEAVY_THRESHOLD_DEFAULT, &fake_linuxpart);
    ensure_eq(&report.totals, &vec![105, 7], "totals")?;
    let pinned: Member = ("linuxpart".into(), "linuxpart-row".into(), 100);
    ensure!(
        report.members[0].contains(&pinned),
        "{:?}",
        report.members[0]
    );
    ensure_eq(
        &row_label("autumn-harvest", "integration", "quota_enforcement_tests"),
        &"autumn-harvest/integration -- quota_enforcement_tests".to_string(),
        "integration label",
    )?;
    ensure_eq(
        &row_label("autumn-harvest-plugin", "ui_integration", "-"),
        &"autumn-harvest-plugin/ui_integration".to_string(),
        "plugin label",
    )?;

    self_test_weights(patterns)?;
    self_test_features(patterns)
}

/// Checks the forward scan of test files.
fn self_test_weights(patterns: &Patterns) -> Result<()> {
    // A plain #[test], an indented #[tokio::test], an ignored test and a doc
    // comment above an attribute stack. The ignored test has a reason string
    // continued with a backslash, as in the real corpus. None of them may
    // confuse the scan.
    let fixture_rs = "\
/// Some doc comment.
#[test]
fn plain_test() {}

mod nested {
    #[tokio::test(flavor = \"multi_thread\")]
    async fn nested_test() {}
}

/// `#[ignore]`d on purpose: a one-shot evidence generator.
#[tokio::test]
#[ignore = \"evidence generator, not a CI assertion -- run via \\
            some/script.sh\"]
#[allow(clippy::too_many_lines)]
async fn ignored_test() {}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    clippy::needless_return
)]
async fn multiline_bracketed_attr_test() {}

fn not_a_test() {}
";
    let none = BTreeSet::new();
    ensure_eq(
        &patterns.test_weight_text(fixture_rs, &none),
        &3,
        "runnable tests",
    )?;
    let missing = patterns.test_weight(Path::new("/nonexistent/path/does/not/exist.rs"), &none)?;
    ensure_eq(&missing, &None, "a missing file")?;

    // A plain test, a gated mod block and an item-level gate on one test. A
    // gated mod gates every test inside it until its closing brace. Each is
    // checked with and without its feature, as in retry_after_tests.rs.
    let cfg_fixture_rs = "\
#[tokio::test]
async fn always_runs() {}

#[cfg(feature = \"testing\")]
mod gated_mod {
    #[tokio::test]
    async fn inside_gated_mod() {}
}

#[cfg(feature = \"chaos\")]
#[tokio::test]
async fn item_level_gated() {}

#[cfg(all(feature = \"testing\", feature = \"db\"))]
#[tokio::test]
async fn needs_both_features() {}
";
    ensure_eq(
        &patterns.test_weight_text(cfg_fixture_rs, &none),
        &1,
        "nothing gated on",
    )?;
    ensure_eq(
        &patterns.test_weight_text(cfg_fixture_rs, &features(&["testing"])),
        &2,
        "testing only",
    )?;
    ensure_eq(
        &patterns.test_weight_text(cfg_fixture_rs, &features(&["testing", "db"])),
        &3,
        "testing and db",
    )?;
    ensure_eq(
        &patterns.test_weight_text(cfg_fixture_rs, &features(&["chaos"])),
        &2,
        "chaos only",
    )?;

    // An inner #![cfg(...)] attribute gates the whole file. Each one in the
    // real corpus is the first line of a file. The outer forms above gate only
    // the next item.
    let inner_cfg_fixture_rs = "\
#![cfg(feature = \"db\")]

#[tokio::test]
async fn only_if_db_enabled() {}
";
    ensure_eq(
        &patterns.test_weight_text(inner_cfg_fixture_rs, &none),
        &0,
        "db off",
    )?;
    ensure_eq(
        &patterns.test_weight_text(inner_cfg_fixture_rs, &features(&["db"])),
        &1,
        "db on",
    )?;

    let check = |condition: &str, on: &[&str], want: bool| -> Result<()> {
        ensure_eq(
            &patterns.cfg_is_enabled(condition, &features(on)),
            &want,
            condition,
        )
    };
    check("feature = \"testing\"", &["testing"], true)?;
    check("feature = \"testing\"", &[], false)?;
    check("all(feature = \"a\", feature = \"b\")", &["a", "b"], true)?;
    check("all(feature = \"a\", feature = \"b\")", &["a"], false)?;
    check("test", &[], true)?;
    check("unix", &[], true)?;
    check("not(unix)", &[], false)?;

    Ok(())
}

/// Checks the feature reads, the qualified names and the gates of `mod.rs`.
fn self_test_features(patterns: &Patterns) -> Result<()> {
    let none = BTreeSet::new();
    // These checks read a disposable fixture tree, not the live default set of
    // autumn-harvest. An earlier version asserted the live set and broke when
    // `tls` joined it (issue #1717). That was a legitimate change, not a defect.
    let fixture_root = fs::temp_dir()?;
    let root = fixture_root.path();
    fs::create_dir_all(root.join("fixture-crate"))?;
    fs::write_string(
        root.join("fixture-crate/Cargo.toml"),
        "[features]\ndefault = [\"alpha\", \"beta\"]\n",
    )?;
    fs::create_dir_all(root.join("no-features-crate"))?;
    fs::write_string(
        root.join("no-features-crate/Cargo.toml"),
        "[package]\nname = \"no-features-crate\"\n",
    )?;
    ensure_eq(
        &patterns.crate_default_features(root, "fixture-crate")?,
        &features(&["alpha", "beta"]),
        "fixture defaults",
    )?;
    ensure_eq(
        &patterns.crate_default_features(root, "no-features-crate")?,
        &none,
        "no features table",
    )?;
    // No Cargo.toml at all matches the no-default-features behavior of Cargo.
    ensure_eq(
        &patterns.crate_default_features(root, "missing-crate")?,
        &none,
        "no manifest",
    )?;
    ensure_eq(
        &patterns.enabled_features_for_row(root, "fixture-crate", "-")?,
        &features(&["alpha", "beta"]),
        "defaults only",
    )?;
    ensure_eq(
        &patterns.enabled_features_for_row(root, "fixture-crate", "gamma,delta")?,
        &features(&["alpha", "beta", "gamma", "delta"]),
        "defaults and feats",
    )?;
    ensure_eq(
        &patterns.enabled_features_for_row(root, "no-features-crate", "-")?,
        &none,
        "no defaults",
    )?;

    // Qualified paths are prefixed with the module name, and they have no file
    // stem, because the caller adds that. The fixture holds a gated nested mod
    // and a plain top-level test.
    let enum_fixture_rs = "\
#[tokio::test]
async fn top_level() {}

#[cfg(feature = \"testing\")]
mod nested {
    #[tokio::test]
    async fn inner() {}
}

#[cfg(feature = \"chaos\")]
#[tokio::test]
async fn item_gated() {}

#[ignore = \"not a CI assertion\"]
#[tokio::test]
async fn skipped() {}
";
    let results: BTreeMap<String, Vec<String>> = patterns
        .enumerate_qualified_tests_text(enum_fixture_rs)
        .into_iter()
        .collect();
    let names: Vec<&str> = results.keys().map(String::as_str).collect();
    ensure_eq(
        &names,
        &vec!["item_gated", "nested::inner", "top_level"],
        "qualified names",
    )?;
    ensure_eq(
        &results["top_level"],
        &Vec::<String>::new(),
        "top_level gate",
    )?;
    ensure_eq(
        &results["nested::inner"],
        &vec!["feature = \"testing\"".to_string()],
        "nested gate",
    )?;
    ensure_eq(
        &results["item_gated"],
        &vec!["feature = \"chaos\"".to_string()],
        "item gate",
    )?;
    let missing = patterns.enumerate_qualified_tests(Path::new("/nonexistent/x.rs"))?;
    ensure_eq(&missing, &None, "a missing file")?;

    // A fixture in the real shape of mod.rs: a bare mod, a gated one and one gated with all(...).
    let gates = patterns.parse_mod_rs_gates(
        "mod plain_mod;\n#[cfg(feature = \"db\")]\nmod db_mod;\n\
         #[cfg(all(feature = \"db\", feature = \"testing\"))]\nmod both_mod;\n",
    );
    let want = vec![
        ("plain_mod".to_string(), Vec::new()),
        ("db_mod".to_string(), vec!["feature = \"db\"".to_string()]),
        (
            "both_mod".to_string(),
            vec!["all(feature = \"db\", feature = \"testing\")".to_string()],
        ),
    ];
    ensure_eq(&gates, &want, "mod.rs gates")?;
    Ok(())
}

/// Runs the self-test, and prints its outcome as the Python script did.
fn run_self_test(patterns: &Patterns) -> bool {
    match self_test(patterns) {
        Ok(()) => {
            println!("self-test: ok");
            true
        }
        Err(error) => {
            eprintln!("self-test: FAILED: {error:#}");
            false
        }
    }
}

/// Runs `cargo xtask audit shard-weight-drift`.
pub fn run(args: Args) -> Result<ExitCode> {
    let Args {
        threshold,
        sweep: with_sweep,
        self_test: only_self_test,
    } = args;
    let patterns = Patterns::new()?;
    if only_self_test {
        return Ok(crate::util::gate_exit(run_self_test(&patterns)));
    }
    if !run_self_test(&patterns) {
        return Ok(ExitCode::FAILURE);
    }
    let root = repo_root();
    let ci_yaml = root.join(".github/workflows/ci.yml");
    let Some(shard_count) = patterns.read_shard_count(&fs::read_string(&ci_yaml)?)? else {
        eprintln!(
            "::warning::shard-weight-drift could not find SEMAPHORE_SHARD_COUNT in the \
             test-db-linux job block of {} \u{2014} skipping the report",
            ci_yaml.display()
        );
        return Ok(ExitCode::SUCCESS);
    };
    ensure!(shard_count > 0, "SEMAPHORE_SHARD_COUNT is 0");
    let manifest = fs::read_string(root.join(".github/ci/integration-suites.txt"))?;
    let records = patterns.parse_manifest_records(&manifest);
    let mut index = None;
    let linux_rows = linux_rows_in_order(&records);
    let (weights, unresolved) = weigh_rows(&patterns, &root, &mut index, &linux_rows)?;
    let linuxpart_rows = linuxpart_rows_in_order(&records);
    let (linuxpart_weights, linuxpart_unresolved) =
        weigh_rows(&patterns, &root, &mut index, &linuxpart_rows)?;
    for (ordinal, label, path) in unresolved.iter().chain(&linuxpart_unresolved) {
        eprintln!(
            "::warning::shard-weight-drift could not resolve a test file for row {ordinal} \
             ({label}) at {} \u{2014} weighed as 0",
            path.display()
        );
    }
    let report = shard_report(&weights, shard_count, threshold, &linuxpart_weights);
    let linuxpart_total: i64 = linuxpart_weights.iter().map(|w| w.2).sum();
    println!(
        "test-db-linux: {} linux rows + {} linuxpart rows (weight {linuxpart_total}, always on \
         shard 0), SEMAPHORE_SHARD_COUNT={shard_count}, heavy threshold={}",
        linux_rows.len(),
        linuxpart_rows.len(),
        threshold
    );
    let (max, min) = spread(&report.totals);
    println!("shard totals: min={min} max={max} spread={}", max - min);
    for shard in 0..shard_count {
        let mut top = report.members[shard].clone();
        top.sort_by_key(|m| std::cmp::Reverse(m.2));
        let top: Vec<String> = top
            .iter()
            .take(3)
            .map(|(_, label, weight)| format!("{label}({weight})"))
            .collect();
        println!(
            "  shard {shard}: total={:4}  n_rows={:3}  top={}",
            report.totals[shard],
            report.members[shard].len(),
            top.join(", ")
        );
    }
    if report.heavy_collisions.is_empty() {
        println!("\nno shard carries 2+ suites at or above the heavy threshold ({threshold})");
    } else {
        println!(
            "\n{} shard(s) carry 2+ suites at or above the heavy threshold ({}) \u{2014} each \
             pair/triple below runs sequentially on one shard, so its members' durations add:",
            report.heavy_collisions.len(),
            threshold
        );
        for (shard, heavy) in &report.heavy_collisions {
            let names: Vec<String> = heavy
                .iter()
                .map(|(o, label, w)| format!("{label} (ordinal {o}, {w} tests)"))
                .collect();
            println!(
                "::warning::shard {shard} carries multiple heavy suites: {}",
                names.join(", ")
            );
        }
    }
    if with_sweep {
        sweep(&weights, threshold, &linuxpart_weights, 9, 24);
    }
    // Report-only: the module comment explains why, under "Why report-only".
    Ok(ExitCode::SUCCESS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_self_test_fixtures_pass() -> Result<()> {
        self_test(&Patterns::new()?)
    }

    #[test]
    fn splitlines_matches_python() {
        assert_eq!(
            splitlines("a\nb\r\nc\rd\u{2028}e\n"),
            vec!["a", "b", "c", "d", "e"]
        );
        assert_eq!(splitlines(""), Vec::<&str>::new());
        assert_eq!(splitlines("a\n\nb"), vec!["a", "", "b"]);
    }

    #[test]
    fn the_shard_count_comes_from_the_test_db_linux_block_only() -> Result<()> {
        let patterns = Patterns::new()?;
        let yaml = "jobs:\n  test-nodb:\n    env:\n      SEMAPHORE_SHARD_COUNT: \"4\"\n  \
                    test-db-linux:\n    env:\n      SEMAPHORE_SHARD_COUNT: \"21\"\n  next:\n    x: 1\n";
        assert_eq!(patterns.read_shard_count(yaml)?, Some(21));
        let open = "jobs:\n  test-db-linux:\n    env:\n      SEMAPHORE_SHARD_COUNT: \"21\"\n";
        assert_eq!(patterns.read_shard_count(open)?, None);
        let bare = "jobs:\n  test-db-linux:\n    steps: []\n  next:\n    x: 1\n";
        assert_eq!(patterns.read_shard_count(bare)?, None);
        Ok(())
    }

    #[test]
    fn a_substring_filter_counts_tests_in_other_files() -> Result<()> {
        let patterns = Patterns::new()?;
        let index = vec![
            (
                "nd_block_tests::a".to_string(),
                vec!["feature = \"db\"".to_string()],
            ),
            ("retry_tests::requeue_nd_blocked".to_string(), Vec::new()),
            ("other::x".to_string(), Vec::new()),
        ];
        let db = BTreeSet::from(["db".to_string()]);
        assert_eq!(
            integration_row_weight(&patterns, &index, "nd_block", &db),
            2
        );
        assert_eq!(
            integration_row_weight(&patterns, &index, "nd_block", &BTreeSet::new()),
            1
        );
        Ok(())
    }
}
