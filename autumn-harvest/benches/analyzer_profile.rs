//! Deterministic (non-criterion) instruction/allocation-count profiling
//! harness for `autumn_harvest::analyzer::HistoryAnalyzer::analyze` with the
//! three built-in rules. Wall-clock timing is not admissible on this
//! (shared-vCPU) machine, so every number comes from
//! `valgrind --tool=callgrind` or `valgrind --tool=dhat`.
//!
//! # Workload
//!
//! An operator lints a long production history with the full built-in rule
//! set. The history holds `ANALYZER_PROFILE_N` events (default 6,000). It is
//! activity-heavy. JSON payloads range from tens of bytes to a few KiB.
//! A few activities retry, a few timers last zero seconds, and a few payloads
//! exceed the large-payload threshold.
//!
//! # Running
//!
//! ```text
//! BIN=$(cargo bench -p autumn-harvest --no-default-features \
//!   --bench analyzer_profile --no-run --message-format=json 2>/dev/null \
//!   | jq -r 'select(.reason=="compiler-artifact" and .target.name=="analyzer_profile") | .executable')
//! valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
//! callgrind_annotate --threshold=98 cg.out
//! valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
//! ```

use autumn_harvest::analyzer::{
    ExcessiveRetriesRule, HistoryAnalyzer, LargePayloadRule, SuspiciousTimerRule,
};
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::types::{ActivityExecId, TimerId};
use serde_json::{Value, json};

const THRESHOLD_BYTES: usize = 16 * 1024;

fn env_usize(key: &str, default: usize) -> usize {
    match std::env::var(key) {
        Ok(raw) => raw
            .parse()
            .unwrap_or_else(|e| panic!("{key}={raw:?} is not a valid usize: {e}")),
        Err(std::env::VarError::NotPresent) => default,
        Err(std::env::VarError::NotUnicode(raw)) => {
            panic!("{key}={} is not valid Unicode", raw.to_string_lossy())
        }
    }
}

/// A JSON object of roughly `lines` order lines.
fn payload(lines: usize, seed: usize) -> Value {
    let items: Vec<Value> = (0..lines)
        .map(|i| {
            json!({
                "sku": format!("SKU-{:06}", seed * 31 + i),
                "qty": (i % 5) + 1,
                "price_cents": 1999 + i * 7,
                "tags": ["promo", "gift"],
            })
        })
        .collect();
    json!({ "order_id": format!("ord-{seed}"), "customer": { "id": seed, "tier": "gold" }, "items": items })
}

fn build_history(n: usize) -> Vec<WorkflowEvent> {
    let mut history = Vec::with_capacity(n);
    let mut i = 0;
    while history.len() < n {
        let id = ActivityExecId::new();
        // Mostly small payloads, an occasional mid one, a rare oversized one.
        let lines = match i % 100 {
            0 => 400,
            1..=10 => 20,
            _ => 3,
        };
        history.push(WorkflowEvent::ActivityScheduled {
            activity_id: id,
            name: format!("activity_{}", i % 12),
            input: payload(lines, i),
            queue: "default".to_string(),
        });
        if i % 25 == 0 {
            for attempt in 1..=4 {
                history.push(WorkflowEvent::ActivityFailed {
                    activity_id: id,
                    error: "connection reset".to_string(),
                    attempt,
                    error_type: "Error".into(),
                    non_retryable: false,
                    details: None,
                });
            }
        }
        if i % 40 == 0 {
            history.push(WorkflowEvent::TimerStarted {
                timer_id: TimerId::new(format!("t-{i}")),
                duration_secs: if i % 80 == 0 { 0 } else { 30 },
            });
        }
        history.push(WorkflowEvent::ActivityCompleted {
            activity_id: id,
            output: payload(lines, i + 1),
        });
        i += 1;
    }
    history.truncate(n);
    history
}

fn main() {
    let n = env_usize("ANALYZER_PROFILE_N", 6_000);
    let reps = env_usize("ANALYZER_PROFILE_REPS", 5);
    assert!(n >= 100 && reps >= 1, "N must be >= 100 and REPS >= 1");
    let history = build_history(n);
    let analyzer = HistoryAnalyzer::new()
        .with_rule(ExcessiveRetriesRule::new(3))
        .with_rule(SuspiciousTimerRule::new())
        .with_rule(LargePayloadRule::new(THRESHOLD_BYTES));

    let sanity = analyzer.analyze(&history);
    assert!(
        sanity.iter().any(|w| w.rule_name == "LargePayload"),
        "fixture must trip LargePayload"
    );
    assert!(sanity.iter().any(|w| w.rule_name == "ExcessiveRetries"));
    assert!(sanity.iter().any(|w| w.rule_name == "SuspiciousTimer"));

    let mut total = 0;
    for _ in 0..reps {
        total += std::hint::black_box(analyzer.analyze(std::hint::black_box(&history))).len();
    }
    println!(
        "analyzer_profile: n={n} reps={reps} events={} warnings_total={total}",
        history.len()
    );
}
