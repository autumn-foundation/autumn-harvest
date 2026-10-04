//! Guards for the optional SLO burn-rate pack (issue #1816).
//!
//! `promtool` proves the rules load and fire. These tests pin what
//! `promtool` cannot see. That is the Workbook burn-rate pairs, a fixture
//! for each alert, the metric names, the runbook links, CI, and the docs.

use serde_yaml::Value;
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

const RULES_PATH: &str = "docs/alerts/slo-pack-v0.1.0.rules.yml";
const TESTS_PATH: &str = "docs/alerts/slo-pack-v0.1.0.test.yml";
const SLO_DOC_PATH: &str = "docs/alerts/slo.md";
const ALERTS_README_PATH: &str = "docs/alerts/README.md";
const RUNBOOK_PATH: &str = "docs/runbooks/harvest-alerts.md";
const CI_PATH: &str = ".github/workflows/ci.yml";

/// The record that holds each SLO objective, keyed by an `slo` label.
const OBJECTIVE_RECORD: &str = "harvest:slo_objective:ratio";

/// The windows that each SLI records an error ratio over.
const WINDOWS: &[&str] = &["5m", "30m", "1h", "6h", "3d"];

/// One reference SLI.
struct Sli {
    /// The `slo` label value and the alert-name stem.
    key: &'static str,
    /// The recorded error-ratio name, less its window suffix.
    ratio_prefix: &'static str,
    /// The objective, as written in the rules file.
    objective: &'static str,
    /// The objective, as written in the docs.
    objective_percent: &'static str,
}

const SLIS: &[Sli] = &[
    Sli {
        key: "workflow_task",
        ratio_prefix: "harvest:workflow_task_error:ratio_rate",
        objective: "0.999",
        objective_percent: "99.9%",
    },
    Sli {
        key: "schedule_to_start",
        ratio_prefix: "harvest:schedule_to_start_slow:ratio_rate",
        objective: "0.99",
        objective_percent: "99%",
    },
    Sli {
        key: "canary",
        ratio_prefix: "harvest:canary_error:ratio_rate",
        objective: "0.99",
        objective_percent: "99%",
    },
];

/// One multi-window burn-rate tier from the SRE Workbook.
struct Tier {
    suffix: &'static str,
    factor: &'static str,
    long: &'static str,
    short: &'static str,
    severity: &'static str,
}

/// For a 30-day budget, these tiers spend 2%, 5%, and 10% of it.
const TIERS: &[Tier] = &[
    Tier {
        suffix: "burn_1h",
        factor: "14.4",
        long: "1h",
        short: "5m",
        severity: "page",
    },
    Tier {
        suffix: "burn_6h",
        factor: "6",
        long: "6h",
        short: "30m",
        severity: "page",
    },
    Tier {
        suffix: "burn_3d",
        factor: "1",
        long: "3d",
        short: "6h",
        severity: "ticket",
    },
];

#[test]
fn every_sli_records_its_objective_and_each_window() {
    let rules = all_rules();
    for sli in SLIS {
        let objective = rules
            .iter()
            .find(|rule| {
                rule["record"].as_str() == Some(OBJECTIVE_RECORD)
                    && rule["labels"]["slo"].as_str() == Some(sli.key)
            })
            .unwrap_or_else(|| panic!("no {OBJECTIVE_RECORD} record for slo={}", sli.key));
        assert_eq!(
            squeeze(objective["expr"].as_str().unwrap_or_default()),
            format!("vector({})", sli.objective),
            "slo={} objective drifted from the documented value",
            sli.key
        );
        for window in WINDOWS {
            let name = format!("{}{window}", sli.ratio_prefix);
            assert!(
                rules
                    .iter()
                    .any(|rule| rule["record"].as_str() == Some(name.as_str())),
                "missing recording rule {name}"
            );
        }
    }
}

/// Each tier pairs a long and a short window at one burn factor.
/// The short window lets the alert reset soon after the burn stops.
#[test]
fn every_alert_uses_the_workbook_burn_rate_pair() {
    let rules = all_rules();
    for sli in SLIS {
        let budget = format!("(1 - scalar({OBJECTIVE_RECORD}{{slo=\"{}\"}}))", sli.key);
        for tier in TIERS {
            let name = alert_name(sli, tier);
            let rule = rules
                .iter()
                .find(|rule| rule["alert"].as_str() == Some(name.as_str()))
                .unwrap_or_else(|| panic!("missing alert {name}"));
            let expr = squeeze(rule["expr"].as_str().unwrap_or_default());
            for window in [tier.long, tier.short] {
                let clause = format!("{}{window} > {} * {budget}", sli.ratio_prefix, tier.factor);
                assert!(
                    expr.contains(&clause),
                    "{name} must compare the {window} ratio at {}x: {expr}",
                    tier.factor
                );
            }
            assert!(
                expr.contains(" and "),
                "{name} must require both windows: {expr}"
            );
            assert_eq!(
                rule["labels"]["severity"].as_str(),
                Some(tier.severity),
                "{name} has the wrong severity"
            );
            assert_eq!(rule["labels"]["slo"].as_str(), Some(sli.key));
        }
    }
}

/// The AC asks for a fixture per alert. A fixture that only proves
/// silence proves nothing, so each alert needs a firing case too.
#[test]
fn every_alert_has_a_firing_and_a_silent_promtool_case() {
    let tests = read_yaml(TESTS_PATH);
    assert_eq!(
        tests["rule_files"][0].as_str(),
        Some("slo-pack-v0.1.0.rules.yml"),
        "the fixture must load the shipped rules file"
    );
    let cases: Vec<&Value> = tests["tests"]
        .as_sequence()
        .expect("tests must be a list")
        .iter()
        .filter_map(|group| group["alert_rule_test"].as_sequence())
        .flatten()
        .collect();
    for name in all_alert_names() {
        let mine: Vec<&&Value> = cases
            .iter()
            .filter(|case| case["alertname"].as_str() == Some(name.as_str()))
            .collect();
        assert!(
            mine.iter().any(|case| !exp_alerts(case).is_empty()),
            "{name} has no promtool case that expects it to fire"
        );
        assert!(
            mine.iter().any(|case| exp_alerts(case).is_empty()),
            "{name} has no promtool case that expects it to stay silent"
        );
    }
}

/// Each alert links to a runbook section with the five standard parts.
#[test]
fn every_alert_links_to_a_complete_runbook_section() {
    let runbook = read_doc(RUNBOOK_PATH);
    for rule in all_rules().iter().filter(|rule| rule["alert"].is_string()) {
        let name = rule["alert"].as_str().unwrap_or_default();
        let link = rule["annotations"]["runbook_url"]
            .as_str()
            .unwrap_or_else(|| panic!("{name} has no runbook_url"));
        let anchor = link
            .strip_prefix("docs/runbooks/harvest-alerts.md#")
            .unwrap_or_else(|| panic!("{name} must link into {RUNBOOK_PATH}: {link}"));
        let section = markdown_section(&runbook, anchor)
            .unwrap_or_else(|| panic!("{name} links to missing section ## {anchor}"));
        for part in [
            "### Triage steps",
            "### Likely causes",
            "### False positives",
            "### Safe actions",
            "### Escalation criteria",
        ] {
            assert!(section.contains(part), "## {anchor} is missing {part}");
        }
    }
}

/// Every raw metric must exist in the engine catalogue.
/// A typo here makes an SLI silent, not wrong.
#[test]
fn rules_reference_only_catalogued_bounded_metrics() {
    let catalogue = metric_catalogue();
    for rule in all_rules() {
        let expr = rule["expr"].as_str().unwrap_or_default();
        for forbidden in ["execution_id", "execution.id", "workflow_id", "task_id"] {
            assert!(
                !expr.contains(forbidden),
                "rule uses unbounded label {forbidden}: {expr}"
            );
        }
        for token in metric_tokens(expr) {
            let base = ["_total", "_bucket", "_count", "_sum"]
                .iter()
                .find_map(|suffix| token.strip_suffix(suffix))
                .unwrap_or(&token);
            assert!(
                catalogue.contains(base),
                "rule uses uncatalogued metric {token}: {expr}"
            );
        }
    }
}

/// A missing addend must count as zero, not empty the whole ratio.
/// Without this, a fleet whose every workflow task times out is silent.
#[test]
fn workflow_task_ratio_survives_a_missing_series() {
    for window in WINDOWS {
        let expr = recording_expr(&format!("harvest:workflow_task_error:ratio_rate{window}"));
        assert_eq!(
            expr.matches("or vector(0)").count(),
            4,
            "each of the four addends must default to zero: {expr}"
        );
    }
}

/// Prometheus 3 normalizes `le="5"` to `le="5.0"` at scrape time.
/// An exact match then finds no bucket and the SLI goes silent.
#[test]
fn schedule_to_start_ratio_matches_both_bucket_label_forms() {
    for window in WINDOWS {
        let expr = recording_expr(&format!(
            "harvest:schedule_to_start_slow:ratio_rate{window}"
        ));
        assert!(
            expr.contains(r#"le=~"5(\\.0+)?""#),
            "the bucket matcher must accept le=\"5\" and le=\"5.0\": {expr}"
        );
    }
}

/// The AC needs both promtool commands to gate CI, docs-only PRs too.
/// A step-level `if:` can skip a step, so each step must have none.
#[test]
fn ci_runs_promtool_and_these_guards_in_the_lint_job() {
    let ci = read_yaml(CI_PATH);
    let steps = ci["jobs"]["lint"]["steps"]
        .as_sequence()
        .expect("ci.yml must have a lint job with steps");
    for required in [
        "check rules docs/alerts/slo-pack-v0.1.0.rules.yml",
        "test rules docs/alerts/slo-pack-v0.1.0.test.yml",
        "sha256sum -c",
        "--test integration slo_pack_docs::",
    ] {
        let step = steps
            .iter()
            .find(|step| {
                step["run"]
                    .as_str()
                    .is_some_and(|run| run.contains(required))
            })
            .unwrap_or_else(|| panic!("no lint step runs {required:?}"));
        assert!(
            step.get("if").is_none(),
            "the lint step that runs {required:?} must not be conditional"
        );
    }
}

/// The docs must state the same objectives that the rules enforce.
#[test]
fn docs_explain_each_sli_target_and_how_to_tune_it() {
    let doc = read_doc(SLO_DOC_PATH);
    for sli in SLIS {
        let row = doc
            .lines()
            .find(|line| line.starts_with(&format!("| `{}` |", sli.key)))
            .unwrap_or_else(|| panic!("{SLO_DOC_PATH} has no table row for `{}`", sli.key));
        assert!(
            row.contains(sli.objective_percent),
            "the `{}` row must state {}: {row}",
            sli.key,
            sli.objective_percent
        );
    }
    for required in ["## Tune the SLO Target", "14.4", "promtool test rules"] {
        assert!(
            doc.contains(required),
            "{SLO_DOC_PATH} must contain {required}"
        );
    }
    assert!(
        read_doc(ALERTS_README_PATH).contains("slo.md"),
        "{ALERTS_README_PATH} must link to the SLO pack docs"
    );
}

fn alert_name(sli: &Sli, tier: &Tier) -> String {
    format!("harvest_slo_{}_{}", sli.key, tier.suffix)
}

fn all_alert_names() -> Vec<String> {
    SLIS.iter()
        .flat_map(|sli| TIERS.iter().map(move |tier| alert_name(sli, tier)))
        .collect()
}

fn all_rules() -> Vec<Value> {
    read_yaml(RULES_PATH)["groups"]
        .as_sequence()
        .expect("groups must be a list")
        .iter()
        .flat_map(|group| {
            group["rules"]
                .as_sequence()
                .cloned()
                .expect("each group must hold a rules list")
        })
        .collect()
}

fn recording_expr(record: &str) -> String {
    let rules = all_rules();
    let rule = rules
        .iter()
        .find(|rule| rule["record"].as_str() == Some(record))
        .unwrap_or_else(|| panic!("missing recording rule {record}"));
    squeeze(rule["expr"].as_str().unwrap_or_default())
}

fn exp_alerts(case: &Value) -> Vec<Value> {
    case["exp_alerts"]
        .as_sequence()
        .cloned()
        .unwrap_or_default()
}

/// Collects the Prometheus form of every `harvest.*` metric constant.
fn metric_catalogue() -> BTreeSet<String> {
    let src = workspace_path("autumn-harvest/src");
    let mut names = BTreeSet::new();
    for entry in fs::read_dir(&src).expect("src must be readable") {
        let path = entry.expect("dir entry").path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("rs") {
            continue;
        }
        let source = fs::read_to_string(&path).expect("source must be readable");
        for line in source.lines() {
            let line = line.trim();
            if !line.starts_with("pub const METRIC_") {
                continue;
            }
            if let Some(name) = line
                .split('"')
                .nth(1)
                .filter(|name| name.starts_with("harvest."))
            {
                names.insert(name.replace('.', "_"));
            }
        }
    }
    assert!(
        !names.is_empty(),
        "no METRIC_* constants found under {}",
        src.display()
    );
    names
}

fn metric_tokens(expr: &str) -> Vec<String> {
    expr.split(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_' && ch != ':')
        .filter(|token| token.starts_with("harvest_"))
        .map(ToOwned::to_owned)
        .collect()
}

fn markdown_section<'a>(document: &'a str, heading: &str) -> Option<&'a str> {
    let marker = format!("\n## {heading}\n");
    let start = document.find(&marker)? + 1;
    let after_start = start + marker.len() - 1;
    let end = document[after_start..]
        .find("\n## ")
        .map_or(document.len(), |relative| after_start + relative);
    Some(&document[start..end])
}

fn squeeze(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn read_yaml(relative: &str) -> Value {
    serde_yaml::from_str(&read_doc(relative))
        .unwrap_or_else(|error| panic!("{relative} must be valid YAML: {error}"))
}

fn read_doc(relative: &str) -> String {
    fs::read_to_string(workspace_path(relative))
        .unwrap_or_else(|error| panic!("failed to read {relative}: {error}"))
}

fn workspace_path(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join(relative)
}
