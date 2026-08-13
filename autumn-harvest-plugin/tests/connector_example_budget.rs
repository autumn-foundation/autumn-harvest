//! Success-metric guard for issue #944.
//!
//! > an embedder wires a Kafka topic to a workflow in ≤ 30 lines of
//! > configuration/mapping code (measured in the shipped example)
//!
//! "Measured in the shipped example" is the falsifiable part: this test reads
//! `examples/kafka_connector_quickstart.rs` and counts the code between the
//! two `connector` markers. If the connector API ever grows enough boilerplate
//! to push a realistic wiring past the budget, CI says so.
//!
//! Deliberately feature-free so it runs in every CI leg (it reads the file, it
//! does not compile it).

const KAFKA_EXAMPLE: &str = include_str!("../examples/kafka_connector_quickstart.rs");
const SQS_EXAMPLE: &str = include_str!("../examples/sqs_connector_quickstart.rs");

const OPEN: &str = "──── connector ────";
const CLOSE: &str = "── end connector ──";

/// Non-blank, non-comment lines between the markers.
fn wiring_lines(source: &str) -> Vec<&str> {
    let mut inside = false;
    let mut lines = Vec::new();
    for line in source.lines() {
        if line.contains(CLOSE) {
            inside = false;
            continue;
        }
        if line.contains(OPEN) {
            inside = true;
            continue;
        }
        if !inside {
            continue;
        }
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with("//") {
            continue;
        }
        lines.push(line);
    }
    lines
}

#[test]
fn kafka_wiring_fits_the_thirty_line_budget() {
    let lines = wiring_lines(KAFKA_EXAMPLE);

    assert!(
        !lines.is_empty(),
        "the `connector` markers moved or were removed; this guard would be \
         vacuous. Re-add them around the source + binding wiring in \
         examples/kafka_connector_quickstart.rs."
    );
    assert!(
        lines.len() <= 30,
        "issue #944 success metric: wiring a Kafka topic to a workflow must \
         take ≤ 30 lines of configuration/mapping code, but the shipped \
         example now needs {}:\n{}",
        lines.len(),
        lines.join("\n"),
    );
}

#[test]
fn both_examples_wire_a_real_binding_and_source() {
    // Guard against the budget being met by an example that no longer
    // demonstrates the thing (e.g. the source moved out of the marked block).
    for (name, source) in [("kafka", KAFKA_EXAMPLE), ("sqs", SQS_EXAMPLE)] {
        let block = wiring_lines(source).join("\n");
        assert!(
            block.contains("SourceBinding::"),
            "{name} example's marked block must construct the binding"
        );
        assert!(
            block.contains("map_json") || block.contains("map_raw"),
            "{name} example's marked block must show the mapping function"
        );
        assert!(
            block.contains("Source::connect"),
            "{name} example's marked block must construct the broker source"
        );
    }
}
