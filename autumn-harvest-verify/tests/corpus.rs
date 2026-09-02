//! The corpus contract for issue #962.
//!
//! Five claims, one per test, in the order they have to hold:
//!
//! 1. [`seeded_corpus_is_clean_under_the_syntactic_layer`] — the *premise*. The
//!    whole corpus compiles under `RUSTFLAGS="-D warnings"` (which is the only
//!    available proof that HVG001–HVG011 produce **zero findings of any
//!    severity**: hard blockers are `compile_error!`, warnings are a
//!    `#[deprecated]` const, and `autumn-harvest-macros` is a `proc-macro`
//!    crate that cannot export its visitor to a test), and
//!    `det_check::check_paths` returns zero findings *and* zero suppressions,
//!    with no `allow_nondeterministic_apis` and no `harvest-suppress` comment
//!    anywhere. Without this, AC3's "guardrails pass, analyzer catches" claim
//!    is unfounded and every number below is meaningless.
//! 2. [`expectations_cover_every_corpus_workflow_and_vice_versa`] — the oracle
//!    in `corpus/expectations.toml` is a *bijection* with the workflows on
//!    disk, so neither a corpus module nor an expectation row can drift out of
//!    the other's sight.
//! 3. [`analyzer_matches_the_expectations_oracle`] — every verdict matches, and
//!    every found case's trace names the helper it flowed through.
//! 4. [`detection_rate_meets_the_success_metric`] — ≥ 90 % of the seeded cases
//!    come back found *with a named trace*, computed live from the run.
//! 5. [`every_unknown_names_its_boundary`] — AC2's three-valued honesty.
//! 6. [`every_seeded_case_is_detected`] — the *ratchet*. Claim 4 is a floor;
//!    this one pins the live count to the number of seeded rows, so a model
//!    that quietly stops matching (the rustc 1.94 → 1.98 `Atomic<T>` incident)
//!    turns red at the first lost case instead of at the sixth.
//!
//! Tests 3–6 share one analyzer run through a [`OnceLock`], so the (slow) MIR
//! build happens once per test binary. It emits into
//! `target/harvest-verify/corpus`, its own directory: `examples_metrics.rs`
//! drives cargo with a different feature set, and one shared emit directory
//! doubles the cargo work for no benefit. (Since the driver accepts only
//! artifacts whose `package_id` is one of the packages *this* run asked for, a
//! shared directory would no longer poison a verdict — the split is now cache
//! hygiene, not correctness. `tests/cli.rs` needs no emit directory at all: it
//! analyzes the checked-in `.mir` fixtures.)

// The matrix printers are long by nature: they exist to make a failure
// diagnosable at a glance, and splitting them would hide the shape of the
// report they print.
#![allow(clippy::too_many_lines)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

use autumn_harvest::det_check;
use autumn_harvest_verify::driver::BuildRequest;
use autumn_harvest_verify::{Finding, Options, Report, Verdict, verify};
use serde::Deserialize;

// ── Layout ───────────────────────────────────────────────────────────────────

/// Cargo package names of the three crates that contain `#[workflow]` fns.
const WORKFLOW_PACKAGES: [&str; 3] = [
    "harvest-verify-corpus-seeded",
    "harvest-verify-corpus-clean",
    "harvest-verify-corpus-boundary",
];

/// Every corpus package, including the two helper crates the analyzer must
/// have MIR for in order to follow a call out of a workflow.
const ALL_CORPUS_PACKAGES: [&str; 5] = [
    "harvest-verify-corpus-seeded",
    "harvest-verify-corpus-clean",
    "harvest-verify-corpus-boundary",
    "harvest-verify-corpus-helpers",
    "harvest-verify-corpus-helpers-deep",
];

/// Directory name under `corpus/` for a package (`…-helpers-deep` → `helpers-deep`).
fn corpus_subdir(package: &str) -> String {
    package
        .strip_prefix("harvest-verify-corpus-")
        .unwrap_or(package)
        .to_string()
}

/// The Rust crate identifier for a Cargo package name.
fn crate_ident(package: &str) -> String {
    package.replace('-', "_")
}

/// Walks up from `CARGO_MANIFEST_DIR` to the directory whose `Cargo.toml`
/// declares `[workspace]`.
fn workspace_root() -> &'static Path {
    static ROOT: OnceLock<PathBuf> = OnceLock::new();
    ROOT.get_or_init(|| {
        let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        loop {
            let manifest = dir.join("Cargo.toml");
            if manifest.is_file() {
                let text = std::fs::read_to_string(&manifest).unwrap_or_default();
                if text.contains("[workspace]") {
                    return dir;
                }
            }
            assert!(
                dir.pop(),
                "no [workspace] Cargo.toml above CARGO_MANIFEST_DIR"
            );
        }
    })
    .as_path()
}

fn corpus_root() -> PathBuf {
    workspace_root().join("autumn-harvest-verify/corpus")
}

fn package_src_dir(package: &str) -> PathBuf {
    corpus_root().join(corpus_subdir(package)).join("src")
}

/// Every `.rs` file under `dir`, sorted, so failures are stable.
fn rust_sources(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(next) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&next) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "rs") {
                out.push(path);
            }
        }
    }
    out.sort();
    out
}

// ── The oracle ───────────────────────────────────────────────────────────────

/// `corpus/expectations.toml`. See that file's header for the schema.
#[derive(Debug, Deserialize)]
struct Oracle {
    schema_version: u32,
    #[serde(default)]
    workflow: Vec<Expectation>,
}

#[derive(Debug, Deserialize)]
struct Expectation {
    #[serde(rename = "crate")]
    package: String,
    workflow: String,
    verdict: String,
    #[serde(default)]
    trace_contains: Vec<String>,
    #[serde(default)]
    boundary: Option<String>,
    mechanism: String,
    launder: String,
    #[serde(default)]
    mandatory: Option<String>,
}

fn oracle() -> &'static Oracle {
    static ORACLE: OnceLock<Oracle> = OnceLock::new();
    ORACLE.get_or_init(|| {
        let path = corpus_root().join("expectations.toml");
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
        toml::from_str(&text).unwrap_or_else(|e| panic!("cannot parse {}: {e}", path.display()))
    })
}

// ── Workflow discovery from the corpus sources ───────────────────────────────

/// A `#[workflow]` fn found by scanning the corpus sources.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Discovered {
    /// Fully-qualified `crate_ident::module::fn`.
    path: String,
    /// Cargo package name.
    package: String,
    /// File it was found in, relative to the workspace root.
    file: String,
}

/// Scans the three workflow crates for `#[workflow]` attributes and the
/// `async fn` that follows each one.
fn discover_workflows() -> Vec<Discovered> {
    let mut found = Vec::new();
    for package in WORKFLOW_PACKAGES {
        let src = package_src_dir(package);
        let ident = crate_ident(package);
        for file in rust_sources(&src) {
            let text = std::fs::read_to_string(&file)
                .unwrap_or_else(|e| panic!("cannot read {}: {e}", file.display()));
            let module = file
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or_default()
                .to_string();
            let relative = file
                .strip_prefix(workspace_root())
                .unwrap_or(&file)
                .display()
                .to_string();
            let lines: Vec<&str> = text.lines().collect();
            for (index, line) in lines.iter().enumerate() {
                if line.trim_start() != "#[workflow]"
                    && !line.trim_start().starts_with("#[workflow(")
                {
                    continue;
                }
                let name = lines[index + 1..]
                    .iter()
                    .find_map(|l| fn_name_of(l))
                    .unwrap_or_else(|| {
                        panic!(
                            "`#[workflow]` at {relative}:{} has no `fn` after it",
                            index + 1
                        )
                    });
                let path = if module == "lib" {
                    format!("{ident}::{name}")
                } else {
                    format!("{ident}::{module}::{name}")
                };
                found.push(Discovered {
                    path,
                    package: package.to_string(),
                    file: relative.clone(),
                });
            }
        }
    }
    found.sort();
    found
}

/// Extracts the identifier from a line declaring a function, if it declares one.
fn fn_name_of(line: &str) -> Option<String> {
    let rest = line.split_once("fn ")?.1;
    let name: String = rest
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    (!name.is_empty()).then_some(name)
}

// ── Test 1: the premise ──────────────────────────────────────────────────────

/// Everything before the first `//` on each line. The corpus has no block
/// comments, and no string literal in it contains an escape-hatch token, so
/// this is enough to separate code from prose.
fn strip_line_comments(text: &str) -> String {
    text.lines()
        .map(|line| line.split_once("//").map_or(line, |(code, _)| code))
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn seeded_corpus_is_clean_under_the_syntactic_layer() {
    // (a) det_check: zero findings at ANY severity, and zero suppressions.
    let dirs: Vec<PathBuf> = ALL_CORPUS_PACKAGES
        .iter()
        .map(|p| package_src_dir(p))
        .collect();
    let refs: Vec<&Path> = dirs.iter().map(PathBuf::as_path).collect();
    let report = det_check::check_paths(&refs).expect("det_check must be able to read the corpus");

    let rendered: Vec<String> = report
        .findings
        .iter()
        .map(|finding| {
            let location = finding.location.as_ref().map_or_else(
                || "<no location>".to_string(),
                |l| format!("{}:{}:{}", l.file, l.line, l.col),
            );
            format!(
                "  [{}] {location} in {:?}: {}",
                finding.rule_id, finding.workflow_name, finding.message
            )
        })
        .collect();
    assert!(
        rendered.is_empty(),
        "det_check must report nothing on the corpus — AC3's premise is that the \
         syntactic layer passes every seeded case cleanly:\n{}",
        rendered.join("\n")
    );
    assert!(
        report.suppressions.is_empty(),
        "the corpus must not suppress anything — the whole point is that the \
         syntactic layer passes it cleanly on its own merits; found: {:?}",
        report
            .suppressions
            .iter()
            .map(|s| (&s.rule_id, &s.reason))
            .collect::<Vec<_>>()
    );

    // (b) No escape hatches anywhere in the corpus sources. Comments are
    //     stripped first: the ban is on *code*, and the corpus documentation
    //     necessarily quotes the names of the hatches it refuses to use.
    for package in ALL_CORPUS_PACKAGES {
        for file in rust_sources(&package_src_dir(package)) {
            let text = std::fs::read_to_string(&file)
                .unwrap_or_else(|e| panic!("cannot read {}: {e}", file.display()));
            let code = strip_line_comments(&text);
            for needle in ["allow_nondeterministic_apis", "harvest-suppress"] {
                assert!(
                    !code.contains(needle),
                    "{} contains `{needle}` in code; the corpus must pass the \
                     syntactic layer without any escape hatch",
                    file.display()
                );
            }
        }
    }

    // (c) The HVG proof: the corpus compiles with warnings denied. HVG hard
    //     blockers are `compile_error!` and HVG warnings are a `#[deprecated]`
    //     const, so a clean `-D warnings` build IS "zero HVG findings".
    let target_dir = workspace_root().join("target/harvest-verify/guardrail-build");
    let mut cargo = Command::new(std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string()));
    cargo
        .current_dir(workspace_root())
        // BOTH spellings. Cargo's precedence is `CARGO_ENCODED_RUSTFLAGS` >
        // `RUSTFLAGS` > `build.rustflags`, so in any environment that already
        // exports the encoded form — a wrapper, a shim, some CI images — setting
        // only `RUSTFLAGS` means the corpus builds with the *ambient* flags and
        // `-D warnings` is silently dropped. The assertion below would then pass
        // while proving nothing, which is the one thing this test may not do.
        // The encoded form is `\x1f`-separated, one flag per field.
        .env("RUSTFLAGS", "-D warnings")
        .env("CARGO_ENCODED_RUSTFLAGS", "-D\u{1f}warnings")
        .env_remove("CARGO_BUILD_RUSTFLAGS")
        .arg("build")
        .arg("--message-format=json")
        .arg("--manifest-path")
        .arg(workspace_root().join("Cargo.toml"))
        .arg("--target-dir")
        .arg(&target_dir);
    for package in WORKFLOW_PACKAGES {
        cargo.arg("-p").arg(package);
    }
    let output = cargo.output().expect("failed to run cargo");
    assert!(
        output.status.success(),
        "the corpus must build with `-D warnings` — that build is the \
         proof that HVG001–HVG011 report nothing at any severity.\n\
         --- stderr ---\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    // …and it must actually have built the corpus. A `cargo build` that
    // selected nothing exits 0 too, so without this the proof is a no-op the
    // day a package is renamed or a `-p` argument is lost.
    let built: BTreeSet<String> = String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter(|m| {
            m.get("reason").and_then(serde_json::Value::as_str) == Some("compiler-artifact")
        })
        .filter_map(|m| {
            m.get("package_id")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        })
        .collect();
    for package in WORKFLOW_PACKAGES {
        assert!(
            built.iter().any(|id| id.contains(package)),
            "the `-D warnings` build reported no artifact for {package}; it \
             compiled nothing, so it proves nothing about HVG001–HVG011. \
             Artifacts seen: {built:?}"
        );
    }
}

// ── Test 2: the oracle is a bijection with the corpus ────────────────────────

#[test]
fn expectations_cover_every_corpus_workflow_and_vice_versa() {
    let oracle = oracle();
    assert_eq!(oracle.schema_version, 1, "expectations.toml schema_version");

    let discovered = discover_workflows();
    let on_disk: BTreeSet<&str> = discovered.iter().map(|d| d.path.as_str()).collect();
    let in_oracle: BTreeSet<&str> = oracle
        .workflow
        .iter()
        .map(|e| e.workflow.as_str())
        .collect();

    let missing_rows: Vec<&&str> = on_disk.difference(&in_oracle).collect();
    let stale_rows: Vec<&&str> = in_oracle.difference(&on_disk).collect();
    assert!(
        missing_rows.is_empty() && stale_rows.is_empty(),
        "corpus/expectations.toml is not a bijection with the corpus sources.\n\
         workflows on disk with no expectation row: {missing_rows:#?}\n\
         expectation rows with no workflow on disk: {stale_rows:#?}"
    );
    assert_eq!(
        discovered.len(),
        on_disk.len(),
        "two corpus workflows share a fully-qualified path: {discovered:#?}"
    );

    // Every row's package must match where the workflow actually lives.
    let package_of: BTreeMap<&str, &str> = discovered
        .iter()
        .map(|d| (d.path.as_str(), d.package.as_str()))
        .collect();
    for expectation in &oracle.workflow {
        assert_eq!(
            package_of.get(expectation.workflow.as_str()).copied(),
            Some(expectation.package.as_str()),
            "`crate` key is wrong for {}",
            expectation.workflow
        );
        assert!(
            !expectation.mechanism.trim().is_empty(),
            "{} has an empty `mechanism`",
            expectation.workflow
        );
        assert!(
            !expectation.launder.trim().is_empty(),
            "{} has an empty `launder` — every row must say why the syntactic \
             layer reports nothing",
            expectation.workflow
        );
    }

    // Counts.
    let seeded = oracle
        .workflow
        .iter()
        .filter(|e| e.verdict == "nondeterminism-found");
    let seeded_count = seeded.clone().count();
    let proven = oracle
        .workflow
        .iter()
        .filter(|e| e.verdict == "proven-deterministic")
        .count();
    let unknown = oracle
        .workflow
        .iter()
        .filter(|e| e.verdict == "unknown")
        .count();
    let known: BTreeSet<&str> = oracle.workflow.iter().map(|e| e.verdict.as_str()).collect();
    assert!(
        known.iter().all(|v| matches!(
            *v,
            "nondeterminism-found" | "proven-deterministic" | "unknown"
        )),
        "unrecognized verdict in expectations.toml: {known:?}"
    );
    assert!(
        seeded_count >= 22,
        "need >= 22 seeded rows, have {seeded_count}"
    );
    assert!(
        proven >= 10,
        "need >= 10 proven-deterministic rows, have {proven}"
    );
    assert!(unknown >= 4, "need >= 4 unknown rows, have {unknown}");

    // Every seeded row must name at least one trace substring; every unknown row
    // must name its boundary; clean rows must claim neither.
    for expectation in &oracle.workflow {
        match expectation.verdict.as_str() {
            "nondeterminism-found" => {
                assert!(
                    !expectation.trace_contains.is_empty(),
                    "{} is seeded but names nothing its trace must contain",
                    expectation.workflow
                );
                assert!(
                    expectation.boundary.is_none(),
                    "{} is seeded but declares a boundary",
                    expectation.workflow
                );
            }
            "unknown" => assert!(
                expectation.boundary.is_some(),
                "{} expects `unknown` but names no boundary — AC2 requires the \
                 boundary to be named",
                expectation.workflow
            ),
            _ => assert!(
                expectation.trace_contains.is_empty() && expectation.boundary.is_none(),
                "{} expects `proven-deterministic` and must claim no trace or boundary",
                expectation.workflow
            ),
        }
    }

    // The five AC3-mandatory cases, each present exactly once.
    let mandatory: Vec<&str> = oracle
        .workflow
        .iter()
        .filter_map(|e| e.mandatory.as_deref())
        .collect();
    for tag in ["AC3-1", "AC3-2", "AC3-3", "AC3-4", "AC3-5"] {
        let hits = mandatory.iter().filter(|m| **m == tag).count();
        assert_eq!(
            hits, 1,
            "AC3 tag {tag} must appear on exactly one expectation row, found {hits}"
        );
    }
    for expectation in oracle.workflow.iter().filter(|e| e.mandatory.is_some()) {
        assert_eq!(
            expectation.verdict, "nondeterminism-found",
            "{} is AC3-mandatory and must be expected to be found",
            expectation.workflow
        );
    }
}

// ── The shared analyzer run ──────────────────────────────────────────────────

/// Runs `harvest-verify` over the whole corpus, exactly once per test binary.
fn analyzer_report() -> &'static Report {
    static REPORT: OnceLock<Report> = OnceLock::new();
    REPORT.get_or_init(|| {
        let build = BuildRequest {
            manifest_path: Some(workspace_root().join("Cargo.toml")),
            packages: ALL_CORPUS_PACKAGES
                .iter()
                .map(|p| (*p).to_string())
                .collect(),
            lib: true,
            target_dir: Some(workspace_root().join("target/harvest-verify/corpus")),
            ..BuildRequest::default()
        };
        verify(&build, &Options::default()).expect("harvest-verify run failed")
    })
}

/// Every verdict from the shared run, keyed by fully-qualified workflow path.
fn verdicts() -> BTreeMap<&'static str, &'static Verdict> {
    analyzer_report()
        .workflows
        .iter()
        .map(|w| (w.workflow.as_str(), &w.verdict))
        .collect()
}

/// The haystack a `trace_contains` substring is searched in: the finding's hops
/// (function *and* step), its source and sink sites (function *and* what), and
/// its message.
fn finding_text(finding: &Finding) -> String {
    let mut text = String::new();
    text.push_str(&finding.message);
    for site in [&finding.source, &finding.sink] {
        text.push('\n');
        text.push_str(&site.function);
        text.push(' ');
        text.push_str(&site.what);
    }
    for hop in &finding.trace {
        text.push('\n');
        text.push_str(&hop.function);
        text.push(' ');
        text.push_str(&hop.step);
    }
    text
}

/// One line of the per-case matrix printed when tests 3–5 fail.
fn matrix_line(expectation: &Expectation, actual: Option<&Verdict>) -> String {
    let actual_name = actual.map_or("<not analyzed>", Verdict::name);
    let detail = match actual {
        Some(Verdict::NondeterminismFound { findings }) => findings
            .iter()
            .map(|f| format!("{:?}/{:?}: {}", f.kind, f.taint, f.message))
            .collect::<Vec<_>>()
            .join(" | "),
        Some(Verdict::Unknown { boundaries }) => boundaries
            .iter()
            .map(|b| format!("{}: {}", b.kind.name(), b.detail))
            .collect::<Vec<_>>()
            .join(" | "),
        _ => String::new(),
    };
    let mark = if actual.map(Verdict::name) == Some(expectation.verdict.as_str()) {
        "ok  "
    } else {
        "FAIL"
    };
    format!(
        "  {mark} {}\n         expected {} ({})\n         actual   {actual_name} {detail}",
        expectation.workflow, expectation.verdict, expectation.mechanism
    )
}

// ── Test 3: the analyzer agrees with the oracle ──────────────────────────────

#[test]
fn analyzer_matches_the_expectations_oracle() {
    let verdicts = verdicts();
    let mut matrix = Vec::new();
    let mut failures = Vec::new();

    for expectation in &oracle().workflow {
        let actual = verdicts.get(expectation.workflow.as_str()).copied();
        matrix.push(matrix_line(expectation, actual));

        let Some(actual) = actual else {
            failures.push(format!("{} was not analyzed at all", expectation.workflow));
            continue;
        };
        if actual.name() != expectation.verdict {
            failures.push(format!(
                "{}: expected {}, got {}",
                expectation.workflow,
                expectation.verdict,
                actual.name()
            ));
            continue;
        }
        if let Verdict::NondeterminismFound { findings } = actual {
            let haystack = findings
                .iter()
                .map(finding_text)
                .collect::<Vec<_>>()
                .join("\n");
            for needle in &expectation.trace_contains {
                assert!(
                    !needle.is_empty(),
                    "{} has an empty trace_contains entry",
                    expectation.workflow
                );
                if !haystack.contains(needle.as_str()) {
                    failures.push(format!(
                        "{}: trace does not name `{needle}`\n--- trace ---\n{haystack}",
                        expectation.workflow
                    ));
                }
            }
        }
    }

    assert!(
        failures.is_empty(),
        "harvest-verify disagrees with corpus/expectations.toml.\n\
         --- per-case matrix ---\n{}\n--- failures ({}) ---\n{}",
        matrix.join("\n"),
        failures.len(),
        failures.join("\n")
    );
}

// ── Test 4: the success metric ───────────────────────────────────────────────

#[test]
fn detection_rate_meets_the_success_metric() {
    let verdicts = verdicts();
    let seeded: Vec<&Expectation> = oracle()
        .workflow
        .iter()
        .filter(|e| e.verdict == "nondeterminism-found")
        .collect();
    let seeded_total = seeded.len();
    assert!(
        seeded_total >= 22,
        "the seeded corpus must have >= 22 cases"
    );

    let mut detected = 0usize;
    let mut misses = Vec::new();
    for expectation in seeded {
        // `unknown` NEVER counts as a detection, and neither does a finding
        // whose trace fails to name the helper it flowed through: the metric is
        // "detects with a named source→sink trace".
        let named = match verdicts.get(expectation.workflow.as_str()).copied() {
            Some(Verdict::NondeterminismFound { findings }) => {
                let haystack = findings
                    .iter()
                    .map(finding_text)
                    .collect::<Vec<_>>()
                    .join("\n");
                expectation
                    .trace_contains
                    .iter()
                    .all(|needle| haystack.contains(needle.as_str()))
            }
            _ => false,
        };
        if named {
            detected += 1;
        } else {
            misses.push(expectation.workflow.as_str());
        }
    }

    #[allow(clippy::cast_precision_loss)]
    let rate = detected as f64 / seeded_total as f64;
    println!(
        "harvest-verify detection rate: {detected}/{seeded_total} = {:.1}% \
         (success metric: >= 90%, with a named source→sink trace; \
         `unknown` never counts)",
        rate * 100.0
    );
    assert!(
        rate >= 0.90,
        "detection rate {detected}/{seeded_total} = {:.1}% is below the 90% \
         success metric. Missed with no named trace:\n{}",
        rate * 100.0,
        misses.join("\n")
    );
}

// ── Test 6: the ratchet ──────────────────────────────────────────────────────

/// The floor in test 4 is a *threshold*, and a threshold has slack: at 29
/// seeded rows, two cases can stop being detected and 27/29 = 93 % still passes.
/// The rustc 1.94 → 1.98 `Atomic<T>` regression cost five at once and would have
/// been caught, but nothing says the next one arrives in fives.
///
/// So this test has no slack at all: every seeded row must come back
/// `nondeterminism-found` with the trace it claims. It is the "detection
/// ratchet" the feasibility report's C1 condition asks for, and the number it
/// pins is not written down anywhere — it is the count of seeded rows in
/// `corpus/expectations.toml`, so adding a case raises the bar automatically.
#[test]
fn every_seeded_case_is_detected() {
    let verdicts = verdicts();
    let seeded: Vec<&Expectation> = oracle()
        .workflow
        .iter()
        .filter(|e| e.verdict == "nondeterminism-found")
        .collect();

    let mut matrix = Vec::new();
    let mut undetected = Vec::new();
    for expectation in &seeded {
        let actual = verdicts.get(expectation.workflow.as_str()).copied();
        let missing_needles: Vec<&str> = match actual {
            Some(Verdict::NondeterminismFound { findings }) => {
                let haystack = findings
                    .iter()
                    .map(finding_text)
                    .collect::<Vec<_>>()
                    .join("\n");
                expectation
                    .trace_contains
                    .iter()
                    .filter(|needle| !haystack.contains(needle.as_str()))
                    .map(String::as_str)
                    .collect()
            }
            _ => Vec::new(),
        };
        let detected = matches!(actual, Some(Verdict::NondeterminismFound { .. }))
            && missing_needles.is_empty();
        matrix.push(format!(
            "  {} {:<58} {}",
            if detected { "ok  " } else { "LOST" },
            expectation.workflow,
            actual.map_or("<not analyzed>", Verdict::name)
        ));
        if !detected {
            undetected.push(format!(
                "{} — got {}{}",
                expectation.workflow,
                actual.map_or("<not analyzed>", Verdict::name),
                if missing_needles.is_empty() {
                    String::new()
                } else {
                    format!(" (trace does not name {missing_needles:?})")
                }
            ));
        }
    }

    let detected = seeded.len() - undetected.len();
    println!(
        "seeded-case ratchet: {detected}/{} detected with a named trace\n{}",
        seeded.len(),
        matrix.join("\n")
    );
    assert_eq!(
        detected,
        seeded.len(),
        "the detection ratchet dropped from {}/{} to {detected}/{}. Every seeded \
         row in corpus/expectations.toml must come back `nondeterminism-found` \
         with the trace it claims — a case that stops being detected is coverage \
         rot, and the >= 90% metric has enough slack to hide two of them. Lost:\n{}",
        seeded.len(),
        seeded.len(),
        seeded.len(),
        undetected.join("\n")
    );
}

// ── Test 5: three-valued honesty ─────────────────────────────────────────────

#[test]
fn every_unknown_names_its_boundary() {
    let verdicts = verdicts();
    let mut failures = Vec::new();

    for expectation in oracle().workflow.iter().filter(|e| e.verdict == "unknown") {
        let expected = expectation.boundary.as_deref().unwrap_or_else(|| {
            panic!(
                "{} expects unknown but names no boundary",
                expectation.workflow
            )
        });
        match verdicts.get(expectation.workflow.as_str()).copied() {
            Some(Verdict::Unknown { boundaries }) => {
                let names: Vec<&str> = boundaries.iter().map(|b| b.kind.name()).collect();
                if !names.contains(&expected) {
                    failures.push(format!(
                        "{}: expected boundary `{expected}`, got {names:?}",
                        expectation.workflow
                    ));
                }
            }
            other => failures.push(format!(
                "{}: expected `unknown` with boundary `{expected}`, got {}",
                expectation.workflow,
                other.map_or("<not analyzed>", Verdict::name)
            )),
        }
    }

    assert!(
        failures.is_empty(),
        "AC2 requires every `unknown` to name the boundary it hit:\n{}",
        failures.join("\n")
    );
}
