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

/// The names that a TLC config lists under `INVARIANT` or `INVARIANTS`.
fn config_invariants(cfg: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let mut in_block = false;
    for line in cfg.lines().map(str::trim) {
        let mut words = line.split_whitespace();
        match words.next() {
            Some("INVARIANT" | "INVARIANTS") => {
                in_block = true;
                out.extend(words.map(str::to_string));
            }
            Some(w) if w.chars().all(|c| c.is_ascii_uppercase() || c == '_') => {
                in_block = false;
            }
            Some(w) if in_block && !w.starts_with('\\') => {
                out.insert(w.to_string());
                out.extend(words.map(str::to_string));
            }
            _ => {}
        }
    }
    out
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
            if line == "#[kani::proof]" {
                pending = true;
            } else if pending && let Some(rest) = line.strip_prefix("fn ") {
                let name = rest.split('(').next().unwrap_or_default();
                names.push(name.to_string());
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
    let cfg = "CONSTANTS\n  N = 2\nINVARIANTS\n  TypeOK\n  Safe\nINVARIANT Other\n";
    let names = config_invariants(cfg);
    let want: BTreeSet<String> = ["TypeOK", "Safe", "Other"].map(String::from).into();
    assert_eq!(names, want);
}

#[test]
fn every_tla_config_has_a_manifest_row() {
    let rows = manifest();
    let listed: BTreeSet<&str> = rows.iter().map(|r| r.config.as_str()).collect();
    let dir = repo_root().join("formal/tla");
    let mut on_disk = BTreeSet::new();
    for entry in std::fs::read_dir(&dir).expect("read formal/tla").flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.ends_with(".cfg") {
            on_disk.insert(name);
        }
    }
    let missing: Vec<&String> = on_disk
        .iter()
        .filter(|c| !listed.contains(c.as_str()))
        .collect();
    assert!(
        missing.is_empty(),
        "configs with no models.txt row: {missing:?}"
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
                spec.contains(&format!("{inv} ==")),
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
        ("kani", "cargo kani -p autumn-harvest"),
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
