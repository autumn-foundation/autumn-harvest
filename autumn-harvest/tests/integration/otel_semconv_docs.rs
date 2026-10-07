//! The OpenTelemetry semantic convention mapping and its Collector recipe
//! (issue #1838).
//!
//! `SEMCONV_METRIC_MAPPINGS` is the source of truth. This test renders the
//! Collector recipe from it and requires the docs to hold the same YAML. It
//! also checks each mapping against the metric catalogue and the semconv
//! names, and checks that ADR 0004 records a decision for each sub-item.

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};

use autumn_harvest::telemetry::{
    SEMCONV_MESSAGING_SYSTEM, SEMCONV_METRIC_MAPPINGS, SemconvInstrument, SemconvMapping,
};

const RECIPE_DOC: &str = "docs/operations/otel-collector.md";
const ADR: &str = "docs/adr/0004-security-extras.md";

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .to_path_buf()
}

/// Read a repository file with `\n` line endings.
///
/// A Windows checkout can convert the files to CRLF.
fn read(relative: &str) -> String {
    fs::read_to_string(repo_root().join(relative))
        .unwrap_or_else(|e| panic!("failed to read {relative}: {e}"))
        .replace("\r\n", "\n")
}

/// Every `harvest.*` value of a `pub const METRIC_*` in telemetry.rs.
fn catalogue() -> BTreeSet<String> {
    let telemetry = read("autumn-harvest/src/telemetry.rs");
    telemetry
        .split("pub const METRIC_")
        .skip(1)
        .filter_map(|declaration| {
            let declaration = declaration.split(';').next()?;
            let value = &declaration[declaration.find("\"harvest.")? + 1..];
            Some(value[..value.find('"')?].to_owned())
        })
        .collect()
}

/// The Prometheus series regex the Collector matches for `mapping`.
///
/// The Collector may or may not trim `_total` from a counter, depending on
/// its `NormalizeName` setting. The pattern accepts both.
fn series_pattern(mapping: &SemconvMapping) -> String {
    let base = mapping.source.replace('.', "_");
    match mapping.instrument {
        SemconvInstrument::Counter => format!("^{base}(_total)?$"),
        SemconvInstrument::Histogram => format!("^{base}$"),
    }
}

/// The Collector configuration the docs must hold, rendered from the table.
fn render_recipe() -> String {
    let mut yaml = String::from(
        "receivers:\n  prometheus:\n    config:\n      scrape_configs:\n        \
         - job_name: harvest\n          scrape_interval: 15s\n          \
         metrics_path: /metrics\n          static_configs:\n            \
         - targets: [\"harvest:9000\"]\n\nprocessors:\n  metricstransform/harvest-semconv:\n    \
         transforms:\n",
    );
    for mapping in SEMCONV_METRIC_MAPPINGS {
        let _ = write!(
            yaml,
            "      - include: {pattern}\n        match_type: regexp\n        action: insert\n        \
             new_name: {target}\n        operations:\n          - action: update_label\n            \
             label: {label}\n            new_label: messaging.destination.name\n          \
             - action: add_label\n            new_label: messaging.system\n            \
             new_value: {system}\n          - action: add_label\n            \
             new_label: messaging.operation.name\n            new_value: {name}\n          \
             - action: add_label\n            new_label: messaging.operation.type\n            \
             new_value: {kind}\n",
            pattern = series_pattern(mapping),
            target = mapping.target,
            label = mapping.destination_label,
            system = SEMCONV_MESSAGING_SYSTEM,
            name = mapping.operation_name,
            kind = mapping.operation_type,
        );
    }
    // `metricstransform` cannot set a unit, so a `transform` processor does.
    yaml.push_str(
        "  transform/harvest-semconv-units:\n    metric_statements:\n      \
         - context: metric\n        statements:\n",
    );
    for mapping in SEMCONV_METRIC_MAPPINGS {
        let _ = writeln!(
            yaml,
            "          - 'set(unit, \"{unit}\") where name == \"{target}\"'",
            unit = mapping.unit,
            target = mapping.target,
        );
    }
    yaml.push_str(
        "  batch: {}\n\nexporters:\n  otlp:\n    endpoint: otel-backend:4317\n\n\
         service:\n  pipelines:\n    metrics:\n      receivers: [prometheus]\n      \
         processors: [metricstransform/harvest-semconv, transform/harvest-semconv-units, \
         batch]\n      exporters: [otlp]\n",
    );
    yaml
}

/// The body of the first ```` ```yaml ```` fence in `doc`.
fn first_yaml_block(doc: &str) -> String {
    let start = doc.find("```yaml\n").expect("a yaml fence") + "```yaml\n".len();
    let end = doc[start..].find("```").expect("a closed fence") + start;
    doc[start..end].to_owned()
}

#[test]
fn the_mapping_table_is_not_empty() {
    assert!(
        SEMCONV_METRIC_MAPPINGS.len() >= 2,
        "map at least the queue claim counter and the activity duration"
    );
}

#[test]
fn every_mapping_names_a_cataloged_harvest_metric() {
    let catalogue = catalogue();
    assert!(catalogue.len() > 100, "catalogue extraction rotted");
    for mapping in SEMCONV_METRIC_MAPPINGS {
        assert!(
            catalogue.contains(mapping.source),
            "{} is not a METRIC_* constant",
            mapping.source
        );
    }
}

#[test]
fn every_target_is_a_messaging_semconv_metric_of_the_right_kind() {
    for mapping in SEMCONV_METRIC_MAPPINGS {
        let (expected, unit) = match mapping.target {
            "messaging.client.sent.messages" | "messaging.client.consumed.messages" => {
                (SemconvInstrument::Counter, "{message}")
            }
            "messaging.process.duration" | "messaging.client.operation.duration" => {
                (SemconvInstrument::Histogram, "s")
            }
            other => panic!("{other} is not a messaging semconv metric"),
        };
        assert_eq!(mapping.instrument, expected, "{}", mapping.source);
        assert_eq!(
            mapping.unit, unit,
            "{} needs the semconv unit",
            mapping.target
        );
        assert!(
            ["create", "send", "receive", "process", "settle"].contains(&mapping.operation_type),
            "{} is not a messaging.operation.type value",
            mapping.operation_type
        );
        assert_ne!(mapping.operation_name, "");
        assert_ne!(mapping.destination_label, "");
    }
}

#[test]
fn no_source_is_mapped_twice() {
    let mut seen = BTreeSet::new();
    for mapping in SEMCONV_METRIC_MAPPINGS {
        assert!(
            seen.insert(mapping.source),
            "{} mapped twice",
            mapping.source
        );
    }
}

#[test]
fn the_docs_hold_the_rendered_collector_recipe() {
    let doc = read(RECIPE_DOC);
    assert_eq!(
        first_yaml_block(&doc),
        render_recipe(),
        "{RECIPE_DOC} drifted from SEMCONV_METRIC_MAPPINGS; paste the rendered recipe"
    );
}

#[test]
fn the_docs_list_every_mapping_in_the_table() {
    let doc = read(RECIPE_DOC);
    for mapping in SEMCONV_METRIC_MAPPINGS {
        let row = format!(
            "| `{}` | `{}` | `{}` |",
            mapping.source, mapping.target, mapping.unit
        );
        assert!(doc.contains(&row), "{RECIPE_DOC} lacks the row {row}");
    }
}

#[test]
fn telemetry_docs_link_the_collector_recipe() {
    assert!(read("docs/telemetry.md").contains("operations/otel-collector.md"));
}

#[test]
fn the_adr_records_a_decision_for_each_sub_item() {
    let adr = read(ADR);
    assert!(adr.contains("#1838"));
    let sections: Vec<&str> = adr.split("\n## ").collect();
    for heading in [
        "1. Tamper-evident audit log",
        "2. Signed WASM modules",
        "3. OTel semantic conventions",
    ] {
        let section = sections
            .iter()
            .find(|section| section.starts_with(heading))
            .unwrap_or_else(|| panic!("{ADR} lacks the section {heading}"));
        assert!(
            section.contains("**Decision: implemented"),
            "{heading} records no decision"
        );
        assert!(
            section.contains("**Declined:**"),
            "{heading} records no declined option"
        );
    }
}

/// `(const name, value)` for each single-line `pub const METRIC_*`.
fn metric_consts() -> Vec<(String, String)> {
    read("autumn-harvest/src/telemetry.rs")
        .lines()
        .filter_map(|line| {
            let rest = line.strip_prefix("pub const ")?;
            let (name, value) = rest.split_once(": &str = \"")?;
            Some((name.to_owned(), value.split('"').next()?.to_owned()))
        })
        .collect()
}

#[test]
fn every_mapped_metric_carries_its_destination_label() {
    let consts = metric_consts();
    let name_of = |value: &str, prefix: &str| {
        consts
            .iter()
            .find(|(name, v)| v == value && name.starts_with(prefix))
            .map_or_else(
                || panic!("no {prefix}* constant has the value {value}"),
                |(name, _)| name.clone(),
            )
    };
    let adapter = read("autumn-harvest/src/metrics_rs_adapter.rs");
    for mapping in SEMCONV_METRIC_MAPPINGS {
        let metric = name_of(mapping.source, "METRIC_");
        let label = name_of(mapping.destination_label, "METRIC_LABEL_");
        let calls: Vec<&str> = adapter
            .match_indices(&format!("{metric},"))
            .filter(|(at, _)| adapter[..*at].trim_end().ends_with("!("))
            .map(|(at, _)| {
                let call = &adapter[at..];
                // The statement ends at the `;` after `.record(..)` or
                // `.increment(..)`.
                &call[..call.find(';').unwrap_or(call.len())]
            })
            .collect();
        assert!(!calls.is_empty(), "the adapter never emits {metric}");
        for call in calls {
            assert!(call.contains(&label), "{metric} is emitted without {label}");
        }
    }
}
