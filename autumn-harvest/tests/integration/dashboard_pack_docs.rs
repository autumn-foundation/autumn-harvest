//! CI validation for the Grafana starter dashboard pack (issue #754).
//!
//! Mirrors `alert_pack_docs.rs`: the dashboard JSON, its README, and the
//! runbook cross-links are docs artifacts that rot silently without a
//! machine check. These tests are the machine check:
//!
//! - the dashboard model parses, is versioned, and follows the Grafana ≥ 10
//!   conventions (stable uid, schemaVersion, templated datasource);
//! - **every** metric in the live catalogue (extracted from
//!   `src/telemetry.rs` at test runtime, so a future `METRIC_*` constant
//!   with no panel turns this red) appears on at least one panel;
//! - every `PromQL` token resolves to a real, correctly-suffixed Prometheus
//!   series (`_total` on counters, `_bucket`/`_count`/`_sum` on histograms,
//!   bare gauges — the docs/alerts/README.md normalization table);
//! - no unbounded/forbidden label ever appears in a query;
//! - counters are rated, histogram quantiles have a bucket-less fallback;
//! - template variables are only applied to series that carry the label;
//! - every alert-pack rule maps to a panel and a resolvable runbook anchor.

use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

const DASHBOARD_PATH: &str = "docs/dashboards/starter-pack-v0.1.0.json";
const DASHBOARD_README_PATH: &str = "docs/dashboards/README.md";
const ALERT_PACK_PATH: &str = "docs/alerts/starter-pack-v0.1.0.json";
const RUNBOOK_PATH: &str = "docs/runbooks/harvest-alerts.md";
const DASHBOARD_UID: &str = "harvest-starter-pack";
const PACK_VERSION: &str = "0.1.0";

/// Minimum number of `METRIC_*` constants the telemetry.rs extraction must
/// find. If the declaration format changes and the extraction silently finds
/// fewer, this floor turns the rot red instead of green.
const EXTRACTION_SANITY_FLOOR: usize = 55;

/// Prometheus-visible gauges emitted by the metrics-rs adapter under literal
/// names (no `METRIC_*` constant exists in telemetry.rs for them).
const SUPPLEMENTAL_METRICS: &[&str] = &[
    "harvest_concurrency_in_flight",
    "harvest_concurrency_deferred",
];

/// Catalogue metrics that are knowingly NOT bridged in
/// `metrics_rs_adapter.rs` and therefore cannot populate a Prometheus panel.
///
/// Deliberately empty: the issue #754 adapter fix bridged
/// `harvest.workflow.timeout`, `harvest.payload.bytes`, and
/// `harvest.payload.rejected`. If a future `METRIC_*` constant ships without
/// an adapter bridge, either bridge it or add it here with a comment naming
/// the issue that tracks the gap — never silently drop dashboard coverage.
const EXPECTED_UNBRIDGED: &[&str] = &[];

/// Every Prometheus series name a dashboard query is allowed to reference.
///
/// This is the type-suffix-aware ground truth, derived from the instrument
/// each metric is registered as in `metrics_rs_adapter.rs` and the
/// normalization table in `docs/alerts/README.md` (dots → underscores;
/// counters gain `_total`; histograms surface only as
/// `_bucket`/`_count`/`_sum`; gauges are bare). A bare counter token or a
/// suffixed gauge token is not in this list and fails the token check.
const DASHBOARD_PROMETHEUS_SERIES: &[&str] = &[
    // --- counters (`_total`) ------------------------------------------------
    "harvest_workflow_started_total",
    "harvest_workflow_unfinished_handlers_total",
    "harvest_workflow_terminal_total",
    "harvest_workflow_continue_as_new_total",
    "harvest_workflow_non_determinism_total",
    "harvest_workflow_nondeterministic_block_total",
    "harvest_workflow_cache_hit_total",
    "harvest_workflow_cache_miss_total",
    "harvest_workflow_external_signal_sent_total",
    "harvest_workflow_timeout_total",
    "harvest_workflow_task_timeout_total",
    "harvest_workflow_sla_breached_total",
    "harvest_workflow_retries_total",
    "harvest_workflow_paused_total",
    "harvest_workflow_debounced_total",
    "harvest_workflow_debounce_fired_total",
    "harvest_workflow_start_throttled_total",
    "harvest_activity_failed_total",
    "harvest_activity_attempts_total",
    "harvest_activity_retries_total",
    "harvest_activity_circuit_tripped_total",
    "harvest_activity_circuit_closed_total",
    "harvest_timer_started_total",
    "harvest_queue_dispatched_total",
    "harvest_schedule_runs_total",
    "harvest_schedule_skipped_total",
    "harvest_schedule_manual_trigger_total",
    "harvest_schedule_fire_attempts_total",
    "harvest_schedule_auto_paused_total",
    "harvest_schedule_decision_write_failed_total",
    "harvest_completion_trigger_fires_total",
    "harvest_retention_deleted_total",
    "harvest_task_quarantined_total",
    "harvest_dlq_redriven_total",
    "harvest_payload_rejected_total",
    "harvest_payload_offloaded_total",
    "harvest_admission_blocked_total",
    "harvest_rate_limit_throttled_total",
    "harvest_webhook_received_total",
    "harvest_webhook_rejected_total",
    "harvest_session_acquisition_total",
    "harvest_worker_tuner_decisions_total",
    // --- histograms (`_bucket` / `_count` / `_sum`) -------------------------
    "harvest_workflow_duration_bucket",
    "harvest_workflow_duration_count",
    "harvest_workflow_duration_sum",
    "harvest_workflow_history_size_bucket",
    "harvest_workflow_history_size_count",
    "harvest_workflow_history_size_sum",
    "harvest_workflow_pause_duration_bucket",
    "harvest_workflow_pause_duration_count",
    "harvest_workflow_pause_duration_sum",
    "harvest_activity_duration_bucket",
    "harvest_activity_duration_count",
    "harvest_activity_duration_sum",
    "harvest_timer_duration_bucket",
    "harvest_timer_duration_count",
    "harvest_timer_duration_sum",
    "harvest_queue_schedule_to_start_bucket",
    "harvest_queue_schedule_to_start_count",
    "harvest_queue_schedule_to_start_sum",
    "harvest_query_duration_bucket",
    "harvest_query_duration_count",
    "harvest_query_duration_sum",
    "harvest_payload_bytes_bucket",
    "harvest_payload_bytes_count",
    "harvest_payload_bytes_sum",
    "harvest_payload_offload_fetch_duration_bucket",
    "harvest_payload_offload_fetch_duration_count",
    "harvest_payload_offload_fetch_duration_sum",
    // --- gauges (bare) -------------------------------------------------------
    "harvest_queue_depth",
    "harvest_queue_oldest_pending_age",
    "harvest_dlq_entries",
    "harvest_worker_slots_in_use",
    "harvest_worker_slots_available",
    "harvest_worker_slot_target",
    "harvest_shard_stranded_pending",
    "harvest_admission_gates_active",
    "harvest_workflow_history_oversized",
    "harvest_rate_limit_tokens_available",
    "harvest_rate_limit_refill_rate",
    "harvest_concurrency_in_flight",
    "harvest_concurrency_deferred",
];

/// Per-series label ground truth (Prometheus-normalized label names), taken
/// from the label sets each bridge method registers in
/// `metrics_rs_adapter.rs`. Drives the variable-applicability check: a
/// `workflow=~`/`queue=~`/`shard=~` selector may only appear in an expression
/// whose series actually carry that label.
const SERIES_LABELS: &[(&str, &[&str])] = &[
    ("harvest_workflow_started", &["workflow", "queue"]),
    (
        "harvest_workflow_unfinished_handlers",
        &["workflow", "kind"],
    ),
    (
        "harvest_workflow_duration",
        &["workflow", "queue", "status"],
    ),
    (
        "harvest_workflow_terminal",
        &["workflow", "queue", "outcome"],
    ),
    ("harvest_workflow_history_size", &["workflow_type"]),
    ("harvest_workflow_continue_as_new", &["workflow_type"]),
    (
        "harvest_workflow_non_determinism",
        &["workflow", "build_id"],
    ),
    (
        "harvest_workflow_nondeterministic_block",
        &["workflow", "queue"],
    ),
    ("harvest_workflow_cache_hit", &["workflow", "queue"]),
    ("harvest_workflow_cache_miss", &["workflow", "queue"]),
    (
        "harvest_workflow_external_signal_sent",
        &["outcome", "reason_code"],
    ),
    ("harvest_workflow_timeout", &["workflow", "queue"]),
    ("harvest_workflow_task_timeout", &["workflow", "queue"]),
    ("harvest_workflow_sla_breached", &["workflow", "queue"]),
    ("harvest_workflow_retries", &["workflow", "queue"]),
    ("harvest_workflow_paused", &["workflow", "queue"]),
    ("harvest_workflow_pause_duration", &["workflow", "queue"]),
    ("harvest_workflow_debounced", &["workflow"]),
    ("harvest_workflow_debounce_fired", &["workflow", "queue"]),
    ("harvest_workflow_start_throttled", &["workflow"]),
    ("harvest_workflow_history_oversized", &["workflow"]),
    (
        "harvest_activity_duration",
        &["activity", "queue", "status", "error_type"],
    ),
    (
        "harvest_activity_failed",
        &["activity", "workflow_type", "error_type", "non_retryable"],
    ),
    (
        "harvest_activity_attempts",
        &["activity", "queue", "outcome"],
    ),
    ("harvest_activity_retries", &["activity", "queue"]),
    ("harvest_activity_circuit_tripped", &["activity_name"]),
    ("harvest_activity_circuit_closed", &["activity_name"]),
    ("harvest_timer_started", &[]),
    ("harvest_timer_duration", &[]),
    ("harvest_queue_depth", &["queue"]),
    ("harvest_queue_schedule_to_start", &["queue"]),
    ("harvest_queue_oldest_pending_age", &["queue"]),
    ("harvest_queue_dispatched", &["queue"]),
    ("harvest_shard_stranded_pending", &["shard"]),
    ("harvest_worker_slots_in_use", &["slot_type"]),
    ("harvest_worker_slots_available", &["slot_type"]),
    ("harvest_worker_slot_target", &["slot_type"]),
    ("harvest_worker_tuner_decisions", &["slot_type", "decision"]),
    ("harvest_dlq_entries", &["shard"]),
    ("harvest_dlq_redriven", &["queue", "outcome"]),
    ("harvest_schedule_runs", &["kind", "name"]),
    ("harvest_schedule_skipped", &["kind", "name", "reason"]),
    ("harvest_schedule_fire_attempts", &["name", "outcome"]),
    ("harvest_schedule_auto_paused", &["name"]),
    ("harvest_schedule_manual_trigger", &["name", "outcome"]),
    ("harvest_schedule_decision_write_failed", &[]),
    ("harvest_completion_trigger_fires", &["trigger", "outcome"]),
    ("harvest_retention_deleted", &["shard"]),
    ("harvest_query_duration", &["query_name", "status"]),
    ("harvest_task_quarantined", &["queue", "reason"]),
    ("harvest_concurrency_in_flight", &["key"]),
    ("harvest_concurrency_deferred", &["key"]),
    ("harvest_rate_limit_tokens_available", &["key"]),
    ("harvest_rate_limit_refill_rate", &["key"]),
    ("harvest_rate_limit_throttled", &["key"]),
    ("harvest_admission_blocked", &["scope", "reason"]),
    ("harvest_admission_gates_active", &[]),
    (
        "harvest_payload_bytes",
        &["payload_kind", "workflow_type", "activity_name"],
    ),
    (
        "harvest_payload_rejected",
        &["payload_kind", "workflow_type"],
    ),
    ("harvest_payload_offloaded", &["payload_field", "store_id"]),
    ("harvest_payload_offload_fetch_duration", &["store_id"]),
    ("harvest_webhook_received", &["path", "outcome"]),
    ("harvest_webhook_rejected", &["path", "outcome"]),
    ("harvest_session_acquisition", &["queue", "outcome"]),
];

/// Unbounded / dotted label forms that must never appear in an expression or
/// legend. `execution.id` is span-only (ADR-0001 §7); dotted label selectors
/// are the pre-normalization spelling and silently match nothing in
/// Prometheus (the exporter normalizes them to underscores).
const FORBIDDEN_QUERY_SUBSTRINGS: &[&str] = &[
    "execution.id",
    "execution_id",
    "harvest.execution.id",
    "activity.name=",
    "workflow.type=",
    "error.type=",
    "payload.kind=",
    "payload.field=",
    "store.id=",
    "query.name=",
];

const REQUIRED_RUNBOOK_SUBSECTIONS: &[&str] = &[
    "### Triage steps",
    "### Likely causes",
    "### False positives",
    "### Safe actions",
    "### Escalation criteria",
];

// ---------------------------------------------------------------------------
// 1. Structure / versioning
// ---------------------------------------------------------------------------

#[test]
fn dashboard_pack_is_versioned_and_importable() {
    let dashboard = read_dashboard();
    assert_eq!(
        dashboard["uid"].as_str(),
        Some(DASHBOARD_UID),
        "dashboard uid must be the stable `{DASHBOARD_UID}` so re-import upgrades in place"
    );
    assert!(
        dashboard["title"]
            .as_str()
            .is_some_and(|title| !title.is_empty() && title.contains("Harvest")),
        "dashboard title must be non-empty and name Harvest"
    );

    let tags: Vec<&str> = dashboard["tags"]
        .as_array()
        .expect("dashboard must carry a tags array")
        .iter()
        .filter_map(Value::as_str)
        .collect();
    assert!(
        tags.contains(&"harvest"),
        "dashboard tags must include `harvest`, got {tags:?}"
    );
    assert!(
        tags.contains(&format!("v{PACK_VERSION}").as_str()),
        "dashboard tags must include the filename version `v{PACK_VERSION}`, got {tags:?}"
    );

    let schema_version = dashboard["schemaVersion"]
        .as_i64()
        .expect("dashboard schemaVersion must be an integer");
    assert!(
        schema_version >= 36,
        "schemaVersion {schema_version} is below the Grafana ≥ 10 floor of 36"
    );

    assert!(
        dashboard["panels"]
            .as_array()
            .is_some_and(|panels| !panels.is_empty()),
        "dashboard must contain panels"
    );
    assert!(
        dashboard["description"]
            .as_str()
            .is_some_and(|desc| desc.contains("starter defaults")),
        "dashboard description must carry the `starter defaults` tuning framing \
         (parity with the alert pack's threshold_policy)"
    );
}

#[test]
fn template_variables_cover_datasource_workflow_queue_shard() {
    let dashboard = read_dashboard();
    let variables = dashboard["templating"]["list"]
        .as_array()
        .expect("dashboard must define templating.list");

    let find = |name: &str| -> &Value {
        variables
            .iter()
            .find(|variable| variable["name"].as_str() == Some(name))
            .unwrap_or_else(|| panic!("dashboard must define a `{name}` template variable"))
    };

    assert_eq!(
        find("datasource")["type"].as_str(),
        Some("datasource"),
        "the `datasource` variable must be a datasource-type variable"
    );

    for name in ["workflow", "queue", "shard"] {
        let variable = find(name);
        assert_eq!(
            variable["multi"].as_bool(),
            Some(true),
            "template variable `{name}` must be multi-select"
        );
        assert_eq!(
            variable["includeAll"].as_bool(),
            Some(true),
            "template variable `{name}` must include an All option"
        );
    }
}

#[test]
fn no_hardcoded_datasource_uids() {
    let dashboard = read_dashboard();
    for panel in all_panels(&dashboard) {
        let datasource = &panel["datasource"];
        if datasource.is_null() {
            continue;
        }
        // `${datasource}` is Grafana's variable-interpolation syntax, not a
        // Rust formatting argument.
        #[allow(clippy::literal_string_with_formatting_args)]
        let templated_uid = "${datasource}";
        assert_eq!(
            datasource["uid"].as_str(),
            Some(templated_uid),
            "panel {} must reference the templated datasource, not a hardcoded uid: {datasource}",
            panel_name(panel)
        );
    }
}

#[test]
fn panel_structure_is_grafana10_clean() {
    let dashboard = read_dashboard();
    let panels = all_panels(&dashboard);
    assert!(!panels.is_empty(), "dashboard must contain panels");

    let mut seen_ids = BTreeSet::new();
    for panel in &panels {
        let id = panel["id"]
            .as_i64()
            .unwrap_or_else(|| panic!("panel {} must carry an integer id", panel_name(panel)));
        assert!(
            seen_ids.insert(id),
            "panel id {id} is duplicated (panel {})",
            panel_name(panel)
        );

        assert!(
            panel["type"].as_str().is_some_and(|ty| !ty.is_empty()),
            "panel {} must declare a type",
            panel_name(panel)
        );
        let grid = &panel["gridPos"];
        for dim in ["h", "w", "x", "y"] {
            assert!(
                grid[dim].is_u64(),
                "panel {} must declare gridPos.{dim}",
                panel_name(panel)
            );
        }
        if panel["type"].as_str() != Some("row") {
            assert!(
                panel["title"].as_str().is_some_and(|t| !t.is_empty()),
                "non-row panel id {id} must carry a non-empty title"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// 2. Catalogue coverage + query validity
// ---------------------------------------------------------------------------

#[test]
fn every_catalogue_metric_appears_on_a_panel() {
    let dashboard = read_dashboard();
    let exprs = all_exprs(&dashboard);
    assert!(!exprs.is_empty(), "dashboard must contain PromQL targets");

    let tokens: BTreeSet<String> = exprs
        .iter()
        .flat_map(|expr| harvest_metric_tokens(expr))
        .collect();

    let unbridged: BTreeSet<&str> = EXPECTED_UNBRIDGED.iter().copied().collect();
    let mut missing = Vec::new();
    for base in catalogue_base_series() {
        if unbridged.contains(base.as_str()) {
            continue;
        }
        let covered = tokens.iter().any(|token| {
            token == &base
                || ["_total", "_bucket", "_count", "_sum"]
                    .iter()
                    .any(|suffix| *token == format!("{base}{suffix}"))
        });
        if !covered {
            missing.push(base);
        }
    }
    assert!(
        missing.is_empty(),
        "catalogue metrics with no dashboard panel (add a panel per metric — \
         this is the anti-drift coverage gate): {missing:?}"
    );
}

#[test]
fn panel_queries_use_only_stable_series() {
    let dashboard = read_dashboard();
    let allowed: BTreeSet<&str> = DASHBOARD_PROMETHEUS_SERIES.iter().copied().collect();
    for panel in all_panels(&dashboard) {
        for expr in panel_exprs(panel) {
            for token in harvest_metric_tokens(expr) {
                assert!(
                    allowed.contains(token.as_str()),
                    "panel {} uses unknown/incorrectly-suffixed series `{token}` \
                     (counters need `_total`, histograms `_bucket`/`_count`/`_sum`, \
                     gauges bare): {expr}",
                    panel_name(panel)
                );
            }
        }
    }
}

#[test]
fn panel_queries_never_use_forbidden_labels() {
    let dashboard = read_dashboard();
    for panel in all_panels(&dashboard) {
        let Some(targets) = panel["targets"].as_array() else {
            continue;
        };
        for target in targets {
            for field in ["expr", "legendFormat"] {
                let Some(text) = target[field].as_str() else {
                    continue;
                };
                for forbidden in FORBIDDEN_QUERY_SUBSTRINGS {
                    assert!(
                        !text.contains(forbidden),
                        "panel {} {field} contains forbidden token `{forbidden}` \
                         (unbounded cardinality or pre-normalization dotted label): {text}",
                        panel_name(panel)
                    );
                }
            }
        }
    }
}

#[test]
fn metric_types_are_handled_correctly() {
    let dashboard = read_dashboard();
    for panel in all_panels(&dashboard) {
        let exprs = panel_exprs(panel);
        for expr in &exprs {
            for token in harvest_metric_tokens(expr) {
                if token.ends_with("_total") {
                    assert!(
                        expr.contains("rate(") || expr.contains("increase("),
                        "panel {} graphs counter `{token}` without rate()/increase(): {expr}",
                        panel_name(panel)
                    );
                }
                if token.ends_with("_bucket") {
                    assert!(
                        expr.contains("histogram_quantile("),
                        "panel {} uses `{token}` outside histogram_quantile(): {expr}",
                        panel_name(panel)
                    );
                }
            }
        }

        // Every histogram-quantile panel must carry a bucket-less fallback
        // target (`_sum`/`_count`) on the same panel: the Prometheus exporter
        // only renders `_bucket` series when `set_buckets_for_metric` is
        // configured (docs/telemetry.md), so a quantile-only panel can be
        // permanently blank on a default exporter install.
        let bucket_bases: BTreeSet<String> = exprs
            .iter()
            .flat_map(|expr| harvest_metric_tokens(expr))
            .filter_map(|token| token.strip_suffix("_bucket").map(ToOwned::to_owned))
            .collect();
        for base in bucket_bases {
            let has_fallback = exprs.iter().any(|expr| {
                expr.contains(&format!("{base}_sum")) || expr.contains(&format!("{base}_count"))
            });
            assert!(
                has_fallback,
                "panel {} quantile query on `{base}_bucket` has no `_sum`/`_count` \
                 fallback target for bucket-less exporter installs",
                panel_name(panel)
            );
        }
    }
}

#[test]
fn template_variables_apply_only_where_labels_exist() {
    let labels_by_base: BTreeMap<&str, &[&str]> = SERIES_LABELS.iter().copied().collect();

    // Self-consistency: the label map must cover the full catalogue so a new
    // metric cannot silently skip the applicability check.
    for base in catalogue_base_series() {
        assert!(
            labels_by_base.contains_key(base.as_str()),
            "SERIES_LABELS is missing catalogue metric `{base}` — add its label set"
        );
    }

    let dashboard = read_dashboard();
    for panel in all_panels(&dashboard) {
        for expr in panel_exprs(panel) {
            for (selector, label) in [
                ("workflow=~", "workflow"),
                ("queue=~", "queue"),
                ("shard=~", "shard"),
            ] {
                if !expr.contains(selector) {
                    continue;
                }
                for token in harvest_metric_tokens(expr) {
                    let base = base_series(&token);
                    let labels = labels_by_base.get(base.as_str()).unwrap_or_else(|| {
                        panic!(
                            "no label map entry for series `{base}` used by panel {}",
                            panel_name(panel)
                        )
                    });
                    assert!(
                        labels.contains(&label),
                        "panel {} applies `{selector}` to series `{base}` which has no \
                         `{label}` label (the selector silently empties the panel): {expr}",
                        panel_name(panel)
                    );
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// 3. Alert ↔ panel ↔ runbook loop
// ---------------------------------------------------------------------------

#[test]
fn every_alert_rule_maps_to_a_panel() {
    let readme = read_doc(DASHBOARD_README_PATH);
    let dashboard_raw = read_doc(DASHBOARD_PATH);
    // Data-driven off the alert pack's own rule list so a new rule (e.g. a
    // saga alert) goes red here until the dashboard grows a matching panel.
    for rule_id in alert_rule_ids() {
        assert!(
            readme.contains(&rule_id),
            "docs/dashboards/README.md alert↔panel mapping table is missing rule `{rule_id}`"
        );
        assert!(
            dashboard_raw.contains(&rule_id),
            "dashboard JSON has no panel referencing alert rule `{rule_id}` \
             (panel description or readiness text panel)"
        );
    }
}

#[test]
fn alert_runbook_anchors_resolve() {
    let pack = read_alert_pack();
    let runbook = read_doc(RUNBOOK_PATH);
    let rules = pack["rules"].as_array().expect("rules must be an array");
    for rule in rules {
        let runbook_link = rule["runbook"]
            .as_str()
            .unwrap_or_else(|| panic!("rule {} must carry a runbook link", rule["id"]));
        let anchor = runbook_link
            .strip_prefix("docs/runbooks/harvest-alerts.md#")
            .unwrap_or_else(|| {
                panic!(
                    "rule {} runbook link must anchor into harvest-alerts.md: {runbook_link}",
                    rule["id"]
                )
            });
        let section = markdown_section(&runbook, anchor).unwrap_or_else(|| {
            panic!(
                "rule {} links to dangling runbook anchor `#{anchor}`: \
                 missing runbook section ## {anchor}",
                rule["id"]
            )
        });
        for required in REQUIRED_RUNBOOK_SUBSECTIONS {
            assert!(
                section.contains(required),
                "runbook section {anchor} is missing {required}"
            );
        }
    }
}

#[test]
fn dashboard_readme_documents_prerequisites() {
    let readme = read_doc(DASHBOARD_README_PATH);
    for required in [
        // scrape prerequisites: the metrics-rs adapter path is mandatory —
        // the plugin scrape endpoint exposes no _bucket series.
        "metrics-rs",
        "MetricsRsRecorder",
        "set_buckets_for_metric",
        "docs/telemetry.md",
        // tuning framing parity with the alert pack.
        "starter defaults",
        // versioning / upgrade-in-place semantics.
        DASHBOARD_UID,
        PACK_VERSION,
        // the alert↔panel mapping table headers.
        "Alert",
        "Panel",
    ] {
        assert!(
            readme.contains(required),
            "docs/dashboards/README.md must document {required}"
        );
    }
}

#[test]
fn runbook_cross_links_dashboard() {
    let runbook = read_doc(RUNBOOK_PATH);
    assert!(
        runbook.contains(DASHBOARD_PATH),
        "docs/runbooks/harvest-alerts.md must point first-responders at the \
         dashboard pack ({DASHBOARD_PATH})"
    );
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn read_dashboard() -> Value {
    let contents = read_doc(DASHBOARD_PATH);
    serde_json::from_str(&contents)
        .expect("starter dashboard pack must be valid Grafana dashboard JSON")
}

fn read_alert_pack() -> Value {
    let contents = read_doc(ALERT_PACK_PATH);
    serde_json::from_str(&contents).expect("starter alert pack must be valid JSON")
}

fn alert_rule_ids() -> Vec<String> {
    let pack = read_alert_pack();
    let ids: Vec<String> = pack["rules"]
        .as_array()
        .expect("alert pack rules must be an array")
        .iter()
        .map(|rule| {
            rule["id"]
                .as_str()
                .expect("alert rule id must be a string")
                .to_owned()
        })
        .collect();
    assert!(!ids.is_empty(), "alert pack must contain rules");
    ids
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

/// Extracts every `METRIC_*` constant value from `src/telemetry.rs` and
/// normalizes it to a Prometheus base series name (dots → underscores).
///
/// This is the anti-drift device: a newly added `METRIC_*` constant appears
/// here automatically and fails the coverage test until the dashboard grows
/// a panel for it. Label constants (`METRIC_LABEL_*`) carry non-`harvest.`
/// values and are skipped.
fn extract_catalogue() -> BTreeSet<String> {
    let telemetry =
        fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("src/telemetry.rs"))
            .expect("failed to read src/telemetry.rs");

    let mut metrics = BTreeSet::new();
    for declaration in telemetry.split("pub const METRIC_").skip(1) {
        // Tolerate reformatting: scan the declaration up to its terminating
        // semicolon for a quoted `harvest.` value, wherever the line breaks.
        let declaration = declaration.split(';').next().unwrap_or(declaration);
        let Some(start) = declaration.find("\"harvest.") else {
            continue;
        };
        let value = &declaration[start + 1..];
        let Some(end) = value.find('"') else {
            continue;
        };
        metrics.insert(value[..end].replace('.', "_"));
    }

    assert!(
        metrics.len() >= EXTRACTION_SANITY_FLOOR,
        "telemetry.rs METRIC_* extraction found only {} metrics (floor {EXTRACTION_SANITY_FLOOR}) — \
         the extraction logic has rotted and would silently under-enforce coverage",
        metrics.len()
    );
    metrics
}

/// Full coverage set: telemetry.rs constants plus the literal-named
/// concurrency gauges the adapter emits without a constant.
fn catalogue_base_series() -> BTreeSet<String> {
    let mut catalogue = extract_catalogue();
    for supplemental in SUPPLEMENTAL_METRICS {
        catalogue.insert((*supplemental).to_owned());
    }
    catalogue
}

/// Recursively collects every panel, including panels nested inside
/// (collapsed) row panels.
fn all_panels(dashboard: &Value) -> Vec<&Value> {
    fn walk<'a>(panels: &'a [Value], out: &mut Vec<&'a Value>) {
        for panel in panels {
            out.push(panel);
            if let Some(nested) = panel["panels"].as_array() {
                walk(nested, out);
            }
        }
    }
    let mut out = Vec::new();
    if let Some(panels) = dashboard["panels"].as_array() {
        walk(panels, &mut out);
    }
    out
}

fn panel_exprs(panel: &Value) -> Vec<&str> {
    panel["targets"]
        .as_array()
        .map(|targets| {
            targets
                .iter()
                .filter_map(|target| target["expr"].as_str())
                .collect()
        })
        .unwrap_or_default()
}

fn all_exprs(dashboard: &Value) -> Vec<&str> {
    all_panels(dashboard)
        .into_iter()
        .flat_map(panel_exprs)
        .collect()
}

fn panel_name(panel: &Value) -> String {
    match (panel["title"].as_str(), panel["id"].as_i64()) {
        (Some(title), Some(id)) if !title.is_empty() => format!("`{title}` (id {id})"),
        (_, Some(id)) => format!("id {id}"),
        (Some(title), None) => format!("`{title}`"),
        _ => "<unnamed>".to_owned(),
    }
}

/// Strips a type suffix back to the base series name when the stripped form
/// is a known catalogue base (so gauge names that happen to contain no
/// suffix pass through unchanged).
fn base_series(token: &str) -> String {
    let bases: BTreeSet<&str> = SERIES_LABELS.iter().map(|(base, _)| *base).collect();
    for suffix in ["_total", "_bucket", "_count", "_sum"] {
        if let Some(stripped) = token.strip_suffix(suffix)
            && bases.contains(stripped)
        {
            return stripped.to_owned();
        }
    }
    token.to_owned()
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
