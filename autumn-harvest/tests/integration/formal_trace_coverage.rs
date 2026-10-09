//! Trace-validation coverage guard (issue #2003). No DB, no feature gate.
//!
//! The chaos suite records each task row's history. CI checks each history
//! against a TLA+ spec with TLC. This suite checks the parts that CI cannot
//! check on itself:
//!
//! - Each trace fixture is well formed.
//! - Each traced spec has a clean fixture and a red fixture. The red fixture
//!   is rejected by the fixed spec and accepted by the pre-fix spec, so the
//!   fence causes the rejection.
//! - Each trace spec extends its model.
//! - `ci.yml` checks the fixtures on every PR.
//! - `chaos.yml` records the chaos traces and checks them.
//! - The chaos suite installs the recorder and exports its traces.
//! - `docs/testing/formal-methods.md` names each part.

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde_json::Value;
use serde_yaml::Value as Yaml;

use super::ci_run_coverage::{SHELL_OPERATORS, parse_workflow, repo_root, ungated};
use super::formal_models_coverage::formal_job_defect;

/// The specs that the chaos traces are checked against.
const TRACED_SPECS: &[&str] = &["ActivityClaim", "WorkflowTaskClaim"];

/// The guard settings that a trace check can name.
const GUARDS: &[&str] = &["fixed", "pre-fix"];

/// The step kinds that a trace line can have.
const OPS: &[&str] = &["init", "write", "start", "heartbeat"];

/// The trace runner.
const RUNNER: &str = "scripts/check-formal-traces.sh";

/// The variable that names the chaos trace directory.
const TRACE_DIR_VAR: &str = "HARVEST_TLA_TRACE_DIR";

fn read(rel: &str) -> String {
    let path = repo_root().join(rel);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// One parsed trace file.
struct Trace {
    name: String,
    spec: String,
    checks: BTreeMap<String, String>,
    lines: Vec<Value>,
}

/// Parse one NDJSON trace. The first line is the header.
fn parse_trace(name: &str, text: &str) -> Trace {
    let mut rows = text
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str::<Value>(l).unwrap_or_else(|e| panic!("{name}: {e}")));
    let header = rows.next().unwrap_or_else(|| panic!("{name}: no header"));
    let spec = header["spec"]
        .as_str()
        .unwrap_or_else(|| panic!("{name}: header has no spec"))
        .to_string();
    let checks = header["checks"]
        .as_object()
        .unwrap_or_else(|| panic!("{name}: header has no checks"))
        .iter()
        .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_string()))
        .collect();
    Trace {
        name: name.to_string(),
        spec,
        checks,
        lines: rows.collect(),
    }
}

/// The fixture traces in `formal/tla/trace/fixtures`.
fn fixtures() -> Vec<Trace> {
    let dir = repo_root().join("formal/tla/trace/fixtures");
    let mut paths: Vec<PathBuf> = std::fs::read_dir(&dir)
        .expect("read formal/tla/trace/fixtures")
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "ndjson"))
        .collect();
    paths.sort();
    paths
        .iter()
        .map(|p| {
            let name = p.file_name().unwrap_or_default().to_string_lossy();
            parse_trace(&name, &std::fs::read_to_string(p).expect("read fixture"))
        })
        .collect()
}

/// Why `trace` is malformed, or `None` when it is well formed.
fn trace_defect(trace: &Trace) -> Option<String> {
    let name = &trace.name;
    if !TRACED_SPECS.contains(&trace.spec.as_str()) {
        return Some(format!("{name}: unknown spec `{}`", trace.spec));
    }
    if trace.checks.is_empty() {
        return Some(format!("{name}: no checks"));
    }
    for (guard, expect) in &trace.checks {
        if !GUARDS.contains(&guard.as_str()) || !matches!(expect.as_str(), "accept" | "reject") {
            return Some(format!("{name}: bad check `{guard}: {expect}`"));
        }
    }
    let Some(first) = trace.lines.first() else {
        return Some(format!("{name}: no lines"));
    };
    if first["op"] != "init" {
        return Some(format!("{name}: the first line must be `init`"));
    }
    for (i, line) in trace.lines.iter().enumerate() {
        let op = line["op"].as_str().unwrap_or_default();
        if !OPS.contains(&op) || (i > 0 && op == "init") {
            return Some(format!("{name}: line {}: bad op `{op}`", i + 2));
        }
        for key in ["state", "attempt", "strikes", "terminal"] {
            if line.get(key).is_none() {
                return Some(format!("{name}: line {}: no `{key}`", i + 2));
            }
        }
    }
    None
}

#[test]
fn trace_defect_flags_malformed_traces() {
    let ok = "{\"spec\":\"ActivityClaim\",\"checks\":{\"fixed\":\"accept\"}}\n\
              {\"op\":\"init\",\"state\":\"PENDING\",\"attempt\":0,\"strikes\":0,\"terminal\":0}\n";
    assert_eq!(trace_defect(&parse_trace("ok", ok)), None);
    let bad = [
        ok.replace("ActivityClaim", "CodecRotation"),
        ok.replace("\"fixed\"", "\"loose\""),
        ok.replace("accept", "maybe"),
        ok.replace("\"init\"", "\"write\""),
        ok.replace("\"terminal\":0", "\"x\":0"),
    ];
    for text in &bad {
        assert!(trace_defect(&parse_trace("bad", text)).is_some(), "{text}");
    }
}

#[test]
fn every_trace_fixture_is_well_formed() {
    let traces = fixtures();
    assert!(
        !traces.is_empty(),
        "formal/tla/trace/fixtures holds no trace"
    );
    let defects: Vec<String> = traces.iter().filter_map(trace_defect).collect();
    assert!(defects.is_empty(), "{defects:#?}");
}

/// The expected result of `guard` for `trace`.
fn check<'a>(trace: &'a Trace, guard: &str) -> Option<&'a str> {
    trace.checks.get(guard).map(String::as_str)
}

/// AC2: each spec has a red fixture that only the fence rejects.
#[test]
fn each_traced_spec_has_a_clean_and_a_red_fixture() {
    let traces = fixtures();
    for spec in TRACED_SPECS {
        let of_spec: Vec<&Trace> = traces.iter().filter(|t| t.spec == *spec).collect();
        assert!(
            of_spec.iter().any(|t| check(t, "fixed") == Some("accept")),
            "{spec} needs a fixture that the fixed spec accepts"
        );
        assert!(
            of_spec
                .iter()
                .any(|t| check(t, "fixed") == Some("reject")
                    && check(t, "pre-fix") == Some("accept")),
            "{spec} needs a red fixture: rejected by `fixed`, accepted by `pre-fix`"
        );
    }
}

#[test]
fn each_trace_spec_extends_its_model() {
    for spec in TRACED_SPECS {
        let text = read(&format!("formal/tla/trace/{spec}Trace.tla"));
        assert!(
            text.contains(&format!("EXTENDS {spec},")),
            "{spec}Trace.tla must extend {spec}"
        );
        for name in ["TraceInit ==", "TraceNext ==", "LogNotConsumed =="] {
            assert!(
                text.lines().any(|l| l.starts_with(name)),
                "{spec}Trace.tla must define `{name}`"
            );
        }
    }
}

/// Both runners must pin the same TLC release.
#[test]
fn trace_runner_pins_the_same_tlc_as_the_model_runner() {
    let pins = |text: &str| -> Vec<String> {
        text.lines()
            .map(str::trim)
            .filter(|l| l.starts_with("tlc_version=") || l.starts_with("tlc_sha256="))
            .map(str::to_string)
            .collect()
    };
    let models = pins(&read("scripts/check-formal-models.sh"));
    assert_eq!(models.len(), 2, "check-formal-models.sh must pin TLC");
    assert_eq!(
        pins(&read(RUNNER)),
        models,
        "{RUNNER} must pin the same TLC"
    );
    assert!(
        read(RUNNER).contains("sha256sum"),
        "{RUNNER} must check the jar"
    );
}

/// AC2: CI checks the fixtures, red ones included, on every PR.
#[test]
fn ci_checks_the_trace_fixtures_on_every_pr() {
    let ci = parse_workflow(".github/workflows/ci.yml");
    if let Some(defect) = formal_job_defect(&ci, "formal-models", RUNNER) {
        panic!("{defect} (issue #2003)");
    }
}

/// The steps of the `chaos` job in `chaos.yml`.
fn chaos_steps(doc: &Yaml) -> Vec<Yaml> {
    doc.get("jobs")
        .and_then(|j| j.get("chaos"))
        .and_then(|j| j.get("steps"))
        .and_then(Yaml::as_sequence)
        .cloned()
        .unwrap_or_default()
}

/// Why `doc` does not check the chaos traces, or `None` when it does.
fn chaos_trace_defect(doc: &Yaml) -> Option<String> {
    let env_set = [
        doc.get("env"),
        doc.get("jobs")
            .and_then(|j| j.get("chaos"))
            .and_then(|j| j.get("env")),
    ]
    .into_iter()
    .flatten()
    .any(|env| {
        env.get(TRACE_DIR_VAR)
            .and_then(Yaml::as_str)
            .is_some_and(|v| !v.is_empty())
    });
    if !env_set {
        return Some(format!("chaos.yml must set `{TRACE_DIR_VAR}`"));
    }
    let runs: Vec<(bool, String)> = chaos_steps(doc)
        .iter()
        .map(|s| {
            let run = s.get("run").and_then(Yaml::as_str).unwrap_or_default();
            (ungated(s), run.trim().to_string())
        })
        .collect();
    let suite = runs.iter().position(|(_, r)| r.contains("chaos_tests::"));
    let check = runs.iter().position(|(free, r)| {
        *free
            && r.starts_with(RUNNER)
            && r.contains(TRACE_DIR_VAR)
            && !SHELL_OPERATORS.iter().any(|op| r.contains(op))
    });
    match (suite, check) {
        (Some(s), Some(c)) if c > s => None,
        (None, _) => Some("chaos.yml must run the chaos suite".into()),
        _ => Some(format!(
            "chaos.yml must run `{RUNNER} \"${TRACE_DIR_VAR}\"` in an ungated step after the suite"
        )),
    }
}

#[test]
fn chaos_trace_check_rejects_missing_or_soft_wiring() {
    let doc = |env: &str, check: &str| {
        let text = format!(
            "env:\n  {env}\njobs:\n  chaos:\n    steps:\n      - run: 'cargo test --test integration chaos_tests:: --nocapture'\n{check}"
        );
        serde_yaml::from_str::<Yaml>(&text).expect("synthetic workflow parses")
    };
    let env = format!("{TRACE_DIR_VAR}: /tmp/t");
    let good = format!("      - run: {RUNNER} \"${TRACE_DIR_VAR}\"\n");
    assert_eq!(chaos_trace_defect(&doc(&env, &good)), None);
    let rejected = [
        ("no env", doc("OTHER: x", &good)),
        ("no step", doc(&env, "")),
        (
            "gated",
            doc(&env, &good.replace("- run:", "- if: false\n        run:")),
        ),
        (
            "soft",
            doc(
                &env,
                &good.replace("- run:", "- continue-on-error: true\n        run:"),
            ),
        ),
        ("shell list", doc(&env, &good.replace('\n', " || true\n"))),
    ];
    for (case, d) in &rejected {
        assert!(chaos_trace_defect(d).is_some(), "`{case}` must not count");
    }
}

/// AC1: the chaos workflow records the traces and checks them.
#[test]
fn chaos_workflow_checks_the_chaos_traces() {
    let doc = parse_workflow(".github/workflows/chaos.yml");
    if let Some(defect) = chaos_trace_defect(&doc) {
        panic!("{defect} (issue #2003)");
    }
}

/// AC1: the chaos suite installs the recorder and exports what it records.
#[test]
fn chaos_suite_records_and_exports_traces() {
    let suite = read("autumn-harvest/tests/integration/chaos_tests.rs");
    for call in ["tla_trace::install(", "tla_trace::export("] {
        assert!(suite.contains(call), "chaos_tests.rs must call `{call}`");
    }
    let infra = read("autumn-harvest/tests/integration/chaos_tests/infra_faults.rs");
    assert!(
        infra.contains("tla_trace::install("),
        "infra_faults.rs must install the recorder"
    );
}

#[test]
fn formal_methods_doc_names_the_trace_check() {
    let doc = read("docs/testing/formal-methods.md");
    let names = [
        "ActivityClaimTrace",
        "WorkflowTaskClaimTrace",
        RUNNER,
        TRACE_DIR_VAR,
    ];
    for name in names {
        assert!(
            doc.contains(&format!("`{name}`")),
            "docs/testing/formal-methods.md does not name `{name}`"
        );
    }
}
