//! Formal-model coverage guard (issue #1819). No DB, no feature gate.
//!
//! The TLA+ models and the Kani proofs run in CI jobs that this suite cannot
//! run. This suite checks that those jobs exist and see every model and proof.
//!
//! - Every `formal/tla/*.cfg` has a row in `formal/tla/models.txt`.
//! - Each row names a spec, a config and an expected result.
//! - The claim-epoch model has a passing config and a pre-fix counter-example.
//! - `ci.yml` runs the model runner and `cargo kani` on every PR.
//! - The crate holds at least three Kani proofs.
//! - `docs/testing/formal-methods.md` names every spec and every proof.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use serde_yaml::Value;

use super::ci_run_coverage::{
    SHELL_OPERATORS, parse_workflow, parse_workflow_text, repo_root, ungated,
};

/// The job whose `if:` the formal jobs copy: a draft skip and a docs-only skip.
const GATE_TEMPLATE_JOB: &str = "msrv";

fn read(rel: &str) -> String {
    let path = repo_root().join(rel);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// The expected TLC result of one manifest row.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Expect {
    /// TLC finds no error.
    Pass,
    /// TLC reports a violation of this invariant.
    Violation(String),
}

#[derive(Debug, Clone)]
struct ModelRow {
    spec: String,
    config: String,
    expect: Expect,
}

fn parse_expect(field: &str) -> Expect {
    if field == "pass" {
        return Expect::Pass;
    }
    let name = field
        .strip_prefix("violation:")
        .unwrap_or_else(|| panic!("models.txt: bad expect field `{field}`"));
    assert!(!name.is_empty(), "models.txt: empty invariant name");
    Expect::Violation(name.to_string())
}

fn parse_manifest(text: &str) -> Vec<ModelRow> {
    text.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(|line| {
            let cols: Vec<&str> = line.split_whitespace().collect();
            assert_eq!(cols.len(), 3, "models.txt: want 3 columns in `{line}`");
            ModelRow {
                spec: cols[0].to_string(),
                config: cols[1].to_string(),
                expect: parse_expect(cols[2]),
            }
        })
        .collect()
}

fn manifest() -> Vec<ModelRow> {
    parse_manifest(&read("formal/tla/models.txt"))
}

/// The TLC config keywords that start a new block.
const CFG_KEYWORDS: &[&str] = &[
    "SPECIFICATION",
    "INIT",
    "NEXT",
    "CONSTANT",
    "CONSTANTS",
    "INVARIANT",
    "INVARIANTS",
    "PROPERTY",
    "PROPERTIES",
    "CONSTRAINT",
    "CONSTRAINTS",
    "ACTION_CONSTRAINT",
    "ACTION_CONSTRAINTS",
    "SYMMETRY",
    "VIEW",
    "CHECK_DEADLOCK",
    "POSTCONDITION",
    "ALIAS",
];

/// The names that a TLC config lists under `INVARIANT` or `INVARIANTS`.
fn config_invariants(cfg: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let mut in_block = false;
    for line in cfg.lines() {
        let code = line.split("\\*").next().unwrap_or_default();
        for word in code.split_whitespace() {
            if CFG_KEYWORDS.contains(&word) {
                in_block = matches!(word, "INVARIANT" | "INVARIANTS");
            } else if in_block {
                out.insert(word.to_string());
            }
        }
    }
    out
}

/// True when `spec` defines `name` at the start of a line.
fn defines(spec: &str, name: &str) -> bool {
    let head = format!("{name} ==");
    spec.lines().any(|l| l.starts_with(&head))
}

/// The names of the `#[kani::proof]` functions under `autumn-harvest/src`.
fn kani_proofs() -> Vec<String> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(dir).expect("read src dir").flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, out);
            } else if path.extension().is_some_and(|e| e == "rs") {
                out.push(path);
            }
        }
    }
    let mut files = Vec::new();
    walk(&repo_root().join("autumn-harvest/src"), &mut files);
    files.sort();
    let mut names = Vec::new();
    for file in files {
        let source = std::fs::read_to_string(&file).expect("read source");
        let mut pending = false;
        for line in source.lines().map(str::trim) {
            if line.starts_with("#[kani::proof") {
                pending = true;
            } else if pending && line.starts_with("#[") {
                // Another attribute, such as `#[kani::unwind]`.
            } else if pending {
                let name = line.split("fn ").nth(1).and_then(|r| r.split('(').next());
                names.push(name.unwrap_or(line).to_string());
                pending = false;
            }
        }
    }
    names
}

#[test]
fn manifest_parser_reads_rows_and_expectations() {
    let rows = parse_manifest("# c\n\nA A.cfg pass\nA B.cfg violation:Inv\n");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].expect, Expect::Pass);
    assert_eq!(rows[1].expect, Expect::Violation("Inv".to_string()));
}

#[test]
fn config_parser_reads_invariant_blocks() {
    let cfg = "\\* INVARIANT Nope\nCONSTANTS\n  N = 2\nINVARIANTS\n  TypeOK \\* note\n  NI\n\
               INVARIANT Other\nCHECK_DEADLOCK FALSE\n";
    let names = config_invariants(cfg);
    let want: BTreeSet<String> = ["TypeOK", "NI", "Other"].map(String::from).into();
    assert_eq!(names, want);
    assert!(defines("TypeOK == TRUE\n", "TypeOK"));
    assert!(!defines("XTypeOK == TRUE\n", "TypeOK"));
}

/// The `formal/tla` file names with extension `ext`, without the extension.
fn tla_files(ext: &str) -> BTreeSet<String> {
    let dir = repo_root().join("formal/tla");
    std::fs::read_dir(&dir)
        .expect("read formal/tla")
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == ext))
        .filter_map(|p| Some(p.file_stem()?.to_string_lossy().into_owned()))
        .collect()
}

#[test]
fn every_tla_spec_and_config_has_a_manifest_row() {
    let rows = manifest();
    let configs: BTreeSet<String> = rows.iter().map(|r| r.config.clone()).collect();
    let specs: BTreeSet<&str> = rows.iter().map(|r| r.spec.as_str()).collect();
    let missing: Vec<String> = tla_files("cfg")
        .into_iter()
        .map(|c| format!("{c}.cfg"))
        .filter(|c| !configs.contains(c))
        .chain(
            tla_files("tla")
                .into_iter()
                .filter(|s| !specs.contains(s.as_str()))
                .map(|s| format!("{s}.tla")),
        )
        .collect();
    assert!(
        missing.is_empty(),
        "files with no models.txt row: {missing:?}"
    );
}

#[test]
fn every_manifest_row_names_real_files_and_invariants() {
    for row in manifest() {
        let spec = read(&format!("formal/tla/{}.tla", row.spec));
        let cfg = read(&format!("formal/tla/{}", row.config));
        let invariants = config_invariants(&cfg);
        assert!(!invariants.is_empty(), "{} checks no invariant", row.config);
        for inv in &invariants {
            assert!(
                defines(&spec, inv),
                "{} lists `{inv}`, which {}.tla does not define",
                row.config,
                row.spec
            );
        }
        if let Expect::Violation(name) = &row.expect {
            assert!(
                invariants.contains(name),
                "{} expects a violation of `{name}` but does not check it",
                row.config
            );
        }
    }
}

#[test]
fn claim_epoch_model_has_a_pre_fix_counter_example() {
    let rows = manifest();
    let claim: Vec<&ModelRow> = rows.iter().filter(|r| r.spec == "ActivityClaim").collect();
    assert!(
        claim.iter().any(|r| r.expect == Expect::Pass),
        "ActivityClaim needs a config that passes (the #1789 fix)"
    );
    assert!(
        claim
            .iter()
            .any(|r| matches!(r.expect, Expect::Violation(_))),
        "ActivityClaim needs a config that reproduces the pre-#1789 bug"
    );
}

/// The Kani script must run every proof and check the verified count.
#[test]
fn kani_script_runs_every_proof_and_checks_the_count() {
    let script = read("scripts/check-kani-proofs.sh");
    let run = script
        .lines()
        .find(|l| l.starts_with("cargo kani -p autumn-harvest"))
        .expect("the script runs `cargo kani -p autumn-harvest`");
    for flag in ["--features chaos", "-Z stubbing"] {
        assert!(run.contains(flag), "the Kani run needs `{flag}`");
    }
    for flag in ["--harness", "--exact", "--only-codegen"] {
        assert!(!run.contains(flag), "`{flag}` narrows the Kani run");
    }
    assert!(script.contains("successfully verified harnesses, 0 failures"));
}

#[test]
fn model_runner_reads_the_manifest_and_pins_tlc() {
    let script = read("scripts/check-formal-models.sh");
    assert!(script.contains("formal/tla/models.txt"));
    assert!(
        script.contains("sha256sum"),
        "the TLC download must be pinned"
    );
}

/// Why `doc` does not run `command` in `job` on every PR, or `None` when it
/// does.
///
/// The job must have the same `if:` as [`GATE_TEMPLATE_JOB`] and no
/// `continue-on-error`. One ungated step must be one plain command that
/// starts with `command`.
fn formal_job_defect(doc: &Value, job: &str, command: &str) -> Option<String> {
    let jobs = doc.get("jobs");
    let Some(node) = jobs.and_then(|j| j.get(job)) else {
        return Some(format!("ci.yml must define a `{job}` job"));
    };
    let template_if = jobs
        .and_then(|j| j.get(GATE_TEMPLATE_JOB))
        .and_then(|j| j.get("if"));
    if node.get("if") != template_if {
        return Some(format!(
            "job `{job}` must use the same `if:` as `{GATE_TEMPLATE_JOB}`"
        ));
    }
    if node
        .get("continue-on-error")
        .is_some_and(|v| v.as_bool() != Some(false))
    {
        return Some(format!("job `{job}` must not set `continue-on-error`"));
    }
    let runs = node
        .get("steps")
        .and_then(Value::as_sequence)
        .into_iter()
        .flatten()
        .filter(|step| ungated(step))
        .filter_map(|step| step.get("run").and_then(Value::as_str))
        .any(|run| {
            let run = run.trim();
            run.starts_with(command) && !SHELL_OPERATORS.iter().any(|op| run.contains(op))
        });
    (!runs).then(|| format!("job `{job}` must have an ungated step that runs `{command}`"))
}

#[test]
fn ci_runs_the_model_checker_and_kani() {
    let ci = parse_workflow(".github/workflows/ci.yml");
    let jobs = [
        ("formal-models", "scripts/check-formal-models.sh"),
        ("kani", "scripts/check-kani-proofs.sh"),
    ];
    for (job, command) in jobs {
        if let Some(defect) = formal_job_defect(&ci, job, command) {
            panic!("{defect} (issue #1819)");
        }
    }
}

/// Self-test: a gated job, a soft failure or a run hidden in a shell list
/// must not count.
#[test]
fn formal_job_check_rejects_gated_and_hidden_runs() {
    let gate = "(github.event_name != 'pull_request' || github.event.pull_request.draft == false)";
    let doc = |job_if: &str, job_extra: &str, run: &str| {
        let text = format!(
            "jobs:\n  msrv:\n    if: \"{gate}\"\n    steps: []\n  kani:\n    if: \"{job_if}\"\n\
             {job_extra}    steps:\n      - run: '{run}'\n"
        );
        parse_workflow_text(&text).expect("synthetic workflow must parse")
    };
    let cmd = "cargo kani -p autumn-harvest";
    assert_eq!(formal_job_defect(&doc(gate, "", cmd), "kani", cmd), None);
    let rejected = [
        (
            "dispatch-only",
            doc("github.event_name == 'workflow_dispatch'", "", cmd),
        ),
        ("soft job", doc(gate, "    continue-on-error: true\n", cmd)),
        ("echo", doc(gate, "", &format!("echo {cmd}"))),
        ("shell list", doc(gate, "", &format!("{cmd} || true"))),
    ];
    for (case, d) in &rejected {
        assert!(
            formal_job_defect(d, "kani", cmd).is_some(),
            "`{case}` must not count"
        );
    }
}

#[test]
fn crate_holds_at_least_three_kani_proofs() {
    let proofs = kani_proofs();
    assert!(proofs.len() >= 3, "want >= 3 Kani proofs, found {proofs:?}");
    let unique: BTreeSet<&String> = proofs.iter().collect();
    assert_eq!(
        unique.len(),
        proofs.len(),
        "duplicate proof names: {proofs:?}"
    );
}

#[test]
fn formal_methods_doc_names_every_model_and_proof() {
    let doc = read("docs/testing/formal-methods.md");
    let specs: BTreeSet<String> = manifest().into_iter().map(|r| r.spec).collect();
    for name in specs.iter().chain(kani_proofs().iter()) {
        assert!(
            doc.contains(&format!("`{name}`")),
            "docs/testing/formal-methods.md does not name `{name}`"
        );
    }
}
