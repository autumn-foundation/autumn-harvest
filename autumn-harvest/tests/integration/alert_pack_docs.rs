use serde_json::Value;
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

const REQUIRED_ALERTS: &[&str] = &[
    "harvest_preflight_failed",
    "harvest_no_active_workers",
    "harvest_queue_uncovered",
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
    "harvest_saga_compensation_spike",
    "harvest_saga_compensation_failed",
    "harvest_update_rejected_rate",
    "harvest_signal_unhandled_rate",
    "harvest_workflow_population_leak",
    "harvest_queue_paused_too_long",
    "harvest_workflow_history_bloat",
    "harvest_scanner_stalled",
    "harvest_no_capable_worker",
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
    "harvest_queue_paused",
    "harvest_schedule_runs_total",
    "harvest_schedule_skipped_total",
    "harvest_schedule_overdue",
    "harvest_retention_deleted_total",
    "harvest_schedule_fire_attempts_total",
    "harvest_workflow_terminal_total",
    "harvest_workflow_non_determinism_total",
    "harvest_workflow_nondeterministic_block_total",
    "harvest_activity_attempts_total",
    "harvest_activity_retries_total",
    "harvest_worker_slots_in_use",
    "harvest_worker_slots_available",
    "harvest_saga_compensated_total",
    "harvest_saga_compensation_failed_total",
    "harvest_update_rejected_total",
    "harvest_signal_unhandled_total",
    "harvest_workflow_active",
    "harvest_workflow_history_bloat_total",
    "harvest_scanner_tick_total",
    "harvest_task_capability_miss_total",
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
fn saga_spike_alert_absolute_floor_fires_without_a_baseline() {
    // Round-4 hardening (issue #801, Codex review): a NEW or long-dormant
    // workflow has no samples in the `offset 1h` baseline window, so a bare
    // `> 4 * rate(... offset 1h)` comparison yields no series and the
    // absolute 1/min floor never fires for the FIRST rollback wave — the
    // exact case the alert exists for. The `or <current> * 0` arm defaults
    // the baseline to a zero-valued series with matching labels; pin that
    // fallback textually so it cannot be silently simplified away.
    let pack = read_pack();
    let rules = pack["rules"].as_array().expect("rules must be an array");
    let spike = rules
        .iter()
        .find(|rule| rule["id"].as_str() == Some("harvest_saga_compensation_spike"))
        .expect("saga compensation spike alert must exist");
    let expr = spike["prometheus"]["expressions"][0]["expr"]
        .as_str()
        .expect("spike alert must carry a PromQL expression");
    assert!(
        expr.contains("or sum by (workflow) (rate(harvest_saga_compensated_total[5m])) * 0"),
        "spike alert baseline must default to a zero-valued, label-matched series so the \
         absolute floor fires with no baseline samples: {expr}"
    );
}

/// Round-10 hardening (issue #619, Codex review): the queue-pause alert's
/// description originally said a pause is safe because "nothing fails, retries,
/// or dead-letters". That is true of the *relative* `schedule_to_start` timer,
/// which a pause suspends and credits back on resume, but **not** of the
/// *absolute* `schedule_to_close` deadline (issue #378), which keeps running for
/// the whole hold. This is the one alert an operator consults after the 1h/4h
/// threshold, so an unqualified "nothing fails" there invites leaving a hold in
/// place under the false assumption that all held work is protected. Pin the
/// exception textually — the API contract and the runbook both carry it, and a
/// future "tighten the wording" pass must not quietly drop it.
#[test]
fn queue_pause_alert_states_the_schedule_to_close_exception() {
    let pack = read_pack();
    let rules = pack["rules"].as_array().expect("rules must be an array");
    let rule = rules
        .iter()
        .find(|rule| rule["id"].as_str() == Some("harvest_queue_paused_too_long"))
        .expect("queue-pause alert must exist");
    let description = rule["description"]
        .as_str()
        .expect("description must be a string");
    assert!(
        description.contains("schedule_to_close"),
        "the pause-safety claim must name the absolute-deadline exception: {description}"
    );
    // The runbook says the same thing; the two must not drift apart.
    let runbook = read_doc("docs/runbooks/harvest-alerts.md");
    let section = markdown_section(&runbook, "harvest_queue_paused_too_long")
        .expect("queue-pause runbook section must exist");
    assert!(
        section.contains("schedule_to_close"),
        "the runbook section must carry the same exception as the alert description"
    );
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

/// Issue #797, Codex review: the scanner tick series is initialized at **zero**
/// when each loop registers, at spawn time. That is what lets a loop wedging on
/// its first iteration still page — but it also means a fresh
/// retention-enabled process exports `harvest_scanner_tick_total{scanner="retention"} = 0`
/// for the whole first hour, while its hourly janitor sleeps toward its first
/// pass. Prometheus does not wait for a full `[3h]` history before evaluating
/// `increase(...)`; two scrapes are enough for it to return `0`, so a bare
/// `increase(...[3h]) == 0` pages through every healthy startup.
///
/// The pack's schema carries no `for:` field (only `mode`/`notes`/`expressions`),
/// so the hold has to live in the expression. Gating on the counter's **own
/// value** is the portable form: it needs no `process_start_time_seconds` (which
/// the metrics exporter does not emit) and no assumption about scrape interval.
///
/// Pin it textually — a future "simplify the expression" pass must not drop the
/// gate and reintroduce a rule that pages on every deploy.
#[test]
fn scanner_stalled_retention_expression_cannot_fire_during_the_startup_hour() {
    let pack = read_pack();
    let rules = pack["rules"].as_array().expect("rules must be an array");
    let stalled = rules
        .iter()
        .find(|rule| rule["id"].as_str() == Some("harvest_scanner_stalled"))
        .expect("scanner stalled alert must exist");
    let exprs: Vec<&str> = stalled["prometheus"]["expressions"]
        .as_array()
        .expect("scanner stalled alert must carry PromQL expressions")
        .iter()
        .filter_map(|expr| expr["expr"].as_str())
        .collect();
    let retention = exprs
        .iter()
        .find(|expr| expr.contains("scanner=\"retention\""))
        .expect("scanner stalled alert must carry a retention-specific expression");

    assert!(
        retention.contains("and max_over_time(harvest_scanner_tick_total{scanner=\"retention\"}"),
        "the retention expression must gate on the counter having ticked at least once, so the \
         registration-time zero cannot page through the healthy first hour: {retention}"
    );

    // The gate must be a FUNCTION result, not a bare selector. PromQL set
    // operators match on the full label set INCLUDING `__name__`; `increase()`
    // drops `__name__` while a bare selector keeps it, so
    // `increase(...) == 0 and harvest_scanner_tick_total{...} > 0` matches
    // nothing and silently disables the alert. Both sides being functions over
    // the same selector keeps their label sets identical.
    let (_, gate) = retention
        .split_once(" and ")
        .expect("the retention expression must carry an `and` gate");
    assert!(
        !gate.trim_start().starts_with("harvest_scanner_tick_total"),
        "the gate must not be a bare selector -- it would carry __name__ while the increase() \
         side does not, so the `and` would match no series and the alert would never fire: {gate}"
    );

    // The sub-minute loops must NOT carry the gate: they tick within seconds of
    // registration, so there is no startup window to ride out -- and the gate
    // would blind the alert to a loop that wedges on iteration one, which is
    // exactly what initializing the series at zero exists to catch.
    let sub_minute = exprs
        .iter()
        .find(|expr| expr.contains("scanner!=\"retention\""))
        .expect("scanner stalled alert must carry a sub-minute expression");
    assert!(
        !sub_minute.contains("> 0"),
        "the sub-minute expression must NOT gate on a prior tick -- that would hide a loop that \
         wedges on its first iteration: {sub_minute}"
    );

    // A follow-up review asked for a reset-/uptime-scoped gate, on the grounds
    // that `max_over_time` is not scoped to the current counter lifetime. It is
    // not: `increase()` IS reset-aware (`last - first + correction`), so a
    // healthy loop restarted mid-window yields a NONZERO increase and the rule
    // cannot fire at all. Adding `unless resets(...) > 0` would instead blind
    // the alert for a full window after every deploy. Pin both halves: no
    // `resets()` term in the expression, and the reasoning recorded in `notes`
    // so a future pass does not "fix" this back.
    assert!(
        !retention.contains("resets("),
        "the retention expression must not carry a resets()-scoped gate -- it would blind the \
         alert for a full window after every deploy: {retention}"
    );
    let notes = stalled["prometheus"]["notes"]
        .as_str()
        .expect("scanner stalled alert must carry prometheus notes");
    assert!(
        notes.contains("RESTART SEMANTICS"),
        "the notes must record why the startup gate needs no resets()/uptime term, so the \
         reset-aware increase() argument is not re-derived on every review"
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
