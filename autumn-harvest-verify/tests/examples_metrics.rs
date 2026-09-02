//! The precision metric on *real* code (D11): `harvest-verify` over every
//! example of `autumn-harvest`, with the checked-in allowlist applied.
//!
//! The corpus proves detection; this proves the other half of the success
//! metric — that the tool is quiet enough to leave switched on. It asserts
//! `(found + allowed) / analyzed <= 10 %`: a workflow that needs an allowlist
//! entry counts *against* the budget exactly as a finding does, so the hatch
//! cannot be used to buy the number down.
//!
//! `unknown` is free by design. An honest boundary is the tool saying what it
//! did not see; it costs a reader nothing but a look, whereas a false
//! `nondeterminism-found` costs them the habit of believing the tool at all.
//!
//! Env-gated on `HARVEST_VERIFY_EXAMPLES=1` because it builds the examples with
//! `--emit=mir` into its own target directory, which is minutes on a cold cache.
//! The gate is an env var rather than `#[ignore]` so that CI runs it as part of
//! the ordinary suite, and the skip path prints a **loud** line: a metric that
//! quietly reports success when it measured nothing is worse than no metric.
//! `tests/ci_wiring.rs` asserts that the CI step which sets the variable still
//! exists, so the measurement cannot lapse unnoticed either.

use std::path::{Path, PathBuf};

use autumn_harvest_verify::driver::BuildRequest;
use autumn_harvest_verify::{Options, Report, Verdict, verify};

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("autumn-harvest-verify has a parent")
        .to_path_buf()
}

fn enabled() -> bool {
    std::env::var("HARVEST_VERIFY_EXAMPLES").is_ok_and(|value| value == "1")
}

/// Run over `-p autumn-harvest --all-examples`, with the root allowlist.
fn run() -> Report {
    let root = workspace_root();
    let build = BuildRequest {
        manifest_path: Some(root.join("Cargo.toml")),
        packages: vec!["autumn-harvest".to_string()],
        all_examples: true,
        no_default_features: true,
        features: vec!["testing".to_string()],
        // Its own emit directory. `corpus.rs` drives cargo over the same
        // workspace with a different feature set; sharing one directory makes
        // each test binary invalidate the other's units for no benefit.
        target_dir: Some(root.join("target/harvest-verify/examples")),
        ..BuildRequest::default()
    };
    let options = Options {
        allowlist: Some(root.join("harvest-verify.allow.toml")),
        ..Options::default()
    };
    verify(&build, &options).expect("harvest-verify run over the examples")
}

/// Everything a reader needs to act on a non-`proven` verdict.
fn describe(report: &Report) {
    for workflow in &report.workflows {
        if matches!(workflow.verdict, Verdict::ProvenDeterministic) && workflow.allowed.is_none() {
            continue;
        }
        println!("\n{} — {}", workflow.workflow, workflow.verdict.name());
        if let Some(justification) = &workflow.allowed {
            println!("  allowed: {justification}");
        }
        match &workflow.verdict {
            Verdict::NondeterminismFound { findings } => {
                for finding in findings {
                    println!(
                        "  {:?}/{:?}: {}",
                        finding.kind, finding.taint, finding.message
                    );
                    for hop in &finding.trace {
                        println!("      {} :: {}", hop.function, hop.step);
                    }
                }
            }
            Verdict::Unknown { boundaries } => {
                for boundary in boundaries {
                    println!(
                        "  boundary {}: {} (at {} {})",
                        boundary.kind.name(),
                        boundary.detail,
                        boundary.site.function,
                        boundary.site.block
                    );
                }
            }
            Verdict::ProvenDeterministic => {}
        }
    }
    for warning in &report.warnings {
        println!("warning: {warning}");
    }
    for entry in &report.unused_allowlist {
        println!("unused allowlist entry: {entry}");
    }
}

#[test]
fn examples_corpus_allowlist_ratio_within_budget() {
    if !enabled() {
        println!(
            "\n\
             ============================================================\n\
             examples_metrics: SKIPPED (set HARVEST_VERIFY_EXAMPLES=1)\n\
             ============================================================\n\
             Nothing was measured. This test is the precision half of issue\n\
             #962's success metric — `(found + allowed) / analyzed <= 10 %`\n\
             over autumn-harvest's own examples — and it reports `ok` when it\n\
             is skipped, so read this line as `not run`, not as `passed`.\n\
             The CI step that sets the variable is asserted by\n\
             `tests/ci_wiring.rs::examples_metric_step_is_wired_into_ci`."
        );
        return;
    }
    let report = run();
    let summary = report.summary();
    println!(
        "harvest-verify over autumn-harvest examples: analyzed {}, proven {}, \
         unknown {}, found {}, allowed {}",
        summary.analyzed, summary.proven, summary.unknown, summary.found, summary.allowed
    );
    describe(&report);

    assert!(
        summary.analyzed > 0,
        "no `#[workflow]` fn was analyzed — the build produced no MIR, which is \
         a driver failure, not a clean run"
    );
    let charged = summary.found.saturating_add(summary.allowed);
    #[allow(clippy::cast_precision_loss)]
    let ratio = charged as f64 / summary.analyzed as f64;
    println!(
        "precision budget: (found {} + allowed {}) / analyzed {} = {:.1}% (limit 10%)",
        summary.found,
        summary.allowed,
        summary.analyzed,
        ratio * 100.0
    );
    assert!(
        ratio <= 0.10,
        "the tool must flag at most 10% of real workflows: \
         (found {} + allowed {}) / {} = {:.1}%",
        summary.found,
        summary.allowed,
        summary.analyzed,
        ratio * 100.0
    );
}
