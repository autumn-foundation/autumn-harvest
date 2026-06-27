use serde_json::Value;
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

const REQUIRED_ALERTS: &[&str] = &[
    "harvest_preflight_failed",
    "harvest_no_active_workers",
    "harvest_worker_saturation",
    "harvest_queue_schedule_to_start_high",
    "harvest_queue_backlog_growth",
    "harvest_activity_failure_surge",
    "harvest_dlq_growth",
    "harvest_schedule_missed_runs",
    "harvest_retention_lag",
    "harvest_shard_unready",
    "harvest_no_compatible_worker",
    "harvest_workflow_non_determinism",
];

const REQUIRED_DRILLS: &[&str] = &[
    "queue-backlog",
    "dlq-spike",
    "stale-worker-fleet",
    "missed-schedule",
    "shard-unready",
];

const REQUIRED_RUNBOOK_SUBSECTIONS: &[&str] = &[
    "### Triage steps",
    "### Likely causes",
    "### False positives",
    "### Safe actions",
    "### Escalation criteria",
];

const STABLE_PROMETHEUS_METRICS: &[&str] = &[
    "harvest_workflow_started_total",
    "harvest_workflow_duration_count",
    "harvest_workflow_duration_sum",
    "harvest_workflow_duration_bucket",
    "harvest_activity_duration_count",
    "harvest_activity_duration_sum",
    "harvest_activity_duration_bucket",
    "harvest_timer_started_total",
    "harvest_queue_depth",
    "harvest_queue_schedule_to_start_count",
    "harvest_queue_schedule_to_start_sum",
    "harvest_queue_schedule_to_start_bucket",
    "harvest_queue_oldest_pending_age",
    "harvest_dlq_entries",
    "harvest_schedule_runs_total",
    "harvest_schedule_skipped_total",
    "harvest_retention_deleted_total",
    "harvest_schedule_fire_attempts_total",
    "harvest_workflow_terminal_total",
    "harvest_workflow_non_determinism_total",
    "harvest_activity_attempts_total",
    "harvest_activity_retries_total",
    "harvest_worker_slots_in_use",
    "harvest_worker_slots_available",
];

#[test]
fn starter_alert_pack_is_versioned_and_complete() {
    let pack = read_pack();
    assert_eq!(
        pack["pack_version"].as_str(),
        Some("0.1.0"),
        "starter alert pack must carry the documented version"
    );
    assert!(
        pack["threshold_policy"]
            .as_str()
            .is_some_and(|policy| policy.contains("starter defaults")),
        "threshold policy must tell embedders to tune these defaults"
    );

    let rules = pack["rules"]
        .as_array()
        .expect("rules must be a JSON array");
    let found: BTreeSet<&str> = rules
        .iter()
        .map(|rule| rule["id"].as_str().expect("rule id must be a string"))
        .collect();

    for required in REQUIRED_ALERTS {
        assert!(
            found.contains(required),
            "starter alert pack is missing required alert {required}"
        );
    }

    for rule in rules {
        for field in [
            "id",
            "signal_source",
            "default_threshold",
            "severity",
            "owner_persona",
            "description",
            "first_action",
            "runbook",
        ] {
            assert!(
                rule[field].as_str().is_some_and(|value| !value.is_empty()),
                "rule {} must include non-empty {field}",
                rule["id"]
            );
        }

        assert!(
            rule["dependencies"]
                .as_array()
                .is_some_and(|deps| !deps.is_empty()),
            "rule {} must be dependency-tagged",
            rule["id"]
        );
        assert!(
            rule["management_checks"]
                .as_array()
                .is_some_and(|checks| !checks.is_empty()),
            "rule {} must include CLI/API checks for non-Prometheus operators",
            rule["id"]
        );
        assert!(
            rule["runbook"]
                .as_str()
                .is_some_and(|path| path.starts_with("docs/runbooks/harvest-alerts.md#")),
            "rule {} must link into the Harvest alert runbook",
            rule["id"]
        );
    }
}

#[test]
fn prometheus_examples_use_stable_bounded_harvest_metrics() {
    let pack = read_pack();
    let stable: BTreeSet<&str> = STABLE_PROMETHEUS_METRICS.iter().copied().collect();
    let forbidden = ["execution.id", "harvest.execution.id", "execution_id"];
    let rules = pack["rules"].as_array().expect("rules must be an array");

    for rule in rules {
        let Some(expressions) = rule["prometheus"]["expressions"].as_array() else {
            continue;
        };
        for expression in expressions {
            let expr = expression["expr"]
                .as_str()
                .expect("Prometheus expression must be a string");
            for forbidden_label in forbidden {
                assert!(
                    !expr.contains(forbidden_label),
                    "rule {} Prometheus expression uses forbidden label {forbidden_label}: {expr}",
                    rule["id"]
                );
            }
            for token in harvest_metric_tokens(expr) {
                assert!(
                    stable.contains(token.as_str()),
                    "rule {} Prometheus expression uses non-catalog metric {token}: {expr}",
                    rule["id"]
                );
            }
        }
    }
}

#[test]
fn every_alert_links_to_a_complete_runbook_section() {
    let runbook = read_doc("docs/runbooks/harvest-alerts.md");
    for alert in REQUIRED_ALERTS {
        let section = markdown_section(&runbook, alert)
            .unwrap_or_else(|| panic!("missing runbook section ## {alert}"));
        for required in REQUIRED_RUNBOOK_SUBSECTIONS {
            assert!(
                section.contains(required),
                "runbook section {alert} is missing {required}"
            );
        }
    }
}

#[test]
fn synthetic_incident_drills_cover_required_failure_modes() {
    let drills = read_doc("docs/runbooks/synthetic-incident-drills.md");
    for drill in REQUIRED_DRILLS {
        let section = markdown_section(&drills, drill)
            .unwrap_or_else(|| panic!("missing synthetic drill section ## {drill}"));
        assert!(
            section.contains("Expected alert:"),
            "drill {drill} must name the expected alert"
        );
        assert!(
            section.contains("Runbook step:"),
            "drill {drill} must name the resolving or escalating runbook step"
        );
    }
}

#[test]
fn non_prometheus_path_documents_cli_and_api_checks() {
    let guide = read_doc("docs/alerts/README.md");
    for required in [
        "harvest preflight",
        "GET /api/harvest/admin/preflight",
        "harvest worker health",
        "GET /api/harvest/workers/health",
        "harvest shard health",
        "GET /api/harvest/admin/shards/health",
        "harvest dlq list",
        "GET /api/harvest/dead-letters",
        "harvest schedule list",
        "GET /api/harvest/admin/schedules",
        "harvest concurrency status",
        "GET /api/harvest/admin/concurrency",
        "harvest workflow stack",
        "GET /api/harvest/workflows/{execution_id}/stack",
        "GET /api/harvest/admin/retention",
        "POST /api/harvest/admin/retention/run-now",
    ] {
        assert!(
            guide.contains(required),
            "non-Prometheus alert guide must document {required}"
        );
    }
    assert!(
        !guide.contains("GET /api/harvest/admin/retention/status"),
        "non-Prometheus alert guide must not document the nonexistent retention /status endpoint"
    );
}

#[test]
fn retention_alert_uses_mounted_management_api_paths() {
    let pack = read_pack();
    let rules = pack["rules"].as_array().expect("rules must be an array");
    let retention = rules
        .iter()
        .find(|rule| rule["id"].as_str() == Some("harvest_retention_lag"))
        .expect("retention alert must exist");
    let checks: Vec<&str> = retention["management_checks"]
        .as_array()
        .expect("management_checks must be an array")
        .iter()
        .map(|check| check.as_str().expect("management check must be a string"))
        .collect();

    assert!(
        checks.contains(&"GET /api/harvest/admin/retention"),
        "retention status check must use mounted GET /admin/retention endpoint"
    );
    assert!(
        checks.contains(&"POST /api/harvest/admin/retention/run-now"),
        "retention run-now check must use mounted POST /admin/retention/run-now endpoint"
    );
    assert!(
        !checks.contains(&"GET /api/harvest/admin/retention/status"),
        "retention alert must not point at nonexistent /admin/retention/status endpoint"
    );
}

fn read_pack() -> Value {
    let contents = read_doc("docs/alerts/starter-pack-v0.1.0.json");
    serde_json::from_str(&contents).expect("starter alert pack must be valid JSON")
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

fn harvest_metric_tokens(expr: &str) -> Vec<String> {
    expr.split(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_')
        .filter(|token| token.starts_with("harvest_"))
        .map(ToOwned::to_owned)
        .collect()
}

fn markdown_section<'a>(document: &'a str, heading: &str) -> Option<&'a str> {
    let marker = format!("## {heading}");
    let start = document.find(&marker)?;
    let after_start = start + marker.len();
    let end = document[after_start..]
        .find("\n## ")
        .map_or(document.len(), |relative| after_start + relative);
    Some(&document[start..end])
}
