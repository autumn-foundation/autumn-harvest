//! The group commit simulation of issue #2256.
//!
//! A seed drives two owners of one decision partition. Each owner commits the
//! decisions of four executions through one lane. A batch holds the
//! decisions of several executions. The nemeses fail one decision, fail a
//! commit, or take the lease over. The design must pass every seed. Each
//! mutant must fail on its own invariant.

use std::path::PathBuf;

use autumn_harvest::dst::SeedPlan;
use autumn_harvest::dst::group::{
    self, Defect, GroupConfig, GroupInvariant, GroupReport, GroupStats, Nemesis,
};

/// The sweep size of a normal test run. The nightly job sets
/// `HARVEST_DST_GROUP_SEEDS` much higher.
const DEFAULT_SEEDS: u64 = 300;

/// A mutant must fail within these seeds.
const RED_SEEDS: SeedPlan = SeedPlan {
    first: 0,
    count: 300,
};

/// The seeds of a coverage sweep.
const COVER_SEEDS: SeedPlan = SeedPlan {
    first: 0,
    count: 200,
};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("the crate is in the workspace")
        .to_path_buf()
}

fn read(rel: &str) -> String {
    let path = repo_root().join(rel);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// The first seed of `RED_SEEDS` on which `defect` breaks `invariant`.
fn first_failure(defect: Defect, invariant: GroupInvariant) -> Option<GroupReport> {
    RED_SEEDS
        .seeds()
        .map(|seed| {
            group::run(
                &GroupConfig::new(seed)
                    .with_defect(defect)
                    .checking(&[invariant]),
            )
        })
        .find(|report| report.violation.is_some())
}

fn cover(nemeses: &[Nemesis]) -> GroupStats {
    group::sweep(&COVER_SEEDS, |seed| {
        GroupConfig::new(seed).with_nemeses(nemeses)
    })
    .unwrap_or_else(|failure| panic!("{failure}"))
    .stats
}

/// Each mutant and an invariant that it must break.
///
/// `shared-execution` has two rows. `OneDecisionPerExecution` states the
/// plan rule. `NoConflictInsideABatch` shows the harm: the second decision
/// of the execution fails on a write of its own batch.
const MUTANTS: [(Defect, GroupInvariant); 7] = [
    (
        Defect::NoRollback,
        GroupInvariant::FailedDecisionWritesNothing,
    ),
    (Defect::AbortOnFailure, GroupInvariant::FailureIsolated),
    (Defect::EarlyAck, GroupInvariant::AckAfterCommit),
    (Defect::NoFence, GroupInvariant::CommitAtPinnedEpoch),
    (Defect::UnlockedFence, GroupInvariant::CommitAtPinnedEpoch),
    (
        Defect::SharedExecution,
        GroupInvariant::OneDecisionPerExecution,
    ),
    (
        Defect::SharedExecution,
        GroupInvariant::NoConflictInsideABatch,
    ),
];

#[test]
fn the_default_config_is_the_design_with_all_nemeses() {
    let config = GroupConfig::new(7);
    assert_eq!(config.seed, 7);
    assert_eq!(config.defect, Defect::None);
    assert_eq!(config.nemeses, Nemesis::ALL.to_vec());
    assert_eq!(config.checks, GroupInvariant::ALL.to_vec());
    assert!(config.executions >= 3, "a batch spans executions");
    assert!(
        config.max_batch >= 3,
        "a batch can hold a failing decision and two others"
    );
}

#[test]
fn the_design_passes_every_seed_and_reaches_every_nemesis() {
    let stats = cover(&Nemesis::ALL);
    assert!(
        stats.grouped_batches > 0,
        "batches of two or more: {stats:?}"
    );
    assert!(stats.largest >= 3, "{stats:?}");
    assert!(stats.isolated_failures > 0, "{stats:?}");
    assert!(stats.failed_commits > 0, "{stats:?}");
    assert!(stats.fenced_batches > 0, "{stats:?}");
    assert!(stats.takeovers > 0, "{stats:?}");
    assert!(
        stats.conflicts > 0,
        "the event-id backstop fires: {stats:?}"
    );
    assert!(stats.drains > 0, "{stats:?}");
    for nemesis in Nemesis::ALL {
        assert!(stats.fired(nemesis) > 0, "{} never fired", nemesis.as_str());
    }
}

#[test]
fn a_batch_with_one_failing_decision_commits_the_others() {
    let report = COVER_SEEDS
        .seeds()
        .map(|seed| group::run(&GroupConfig::new(seed).with_nemeses(&[Nemesis::FailDecision])))
        .find(|report| report.stats.isolated_injected > 0)
        .expect("a seed fails one decision of a batch that commits the others");
    assert_eq!(report.violation, None, "{}", report.trace_tail(40));
    assert!(report.completed, "{}", report.trace_tail(40));
    // The commit line names a committed decision and an injected failure.
    let line = report
        .trace
        .iter()
        .find(|line| line.contains("commit: committed [d") && line.contains("injected [d"))
        .expect("the trace names the committed batch and its injected failure");
    assert!(line.contains("rolled back [d"), "{line}");
}

#[test]
fn the_design_passes_at_every_width() {
    let mut wide = GroupStats::default();
    for width in [1, 2, 4] {
        let stats = group::sweep(&COVER_SEEDS, |seed| GroupConfig {
            width,
            ..GroupConfig::new(seed)
        })
        .unwrap_or_else(|failure| panic!("width {width}: {failure}"))
        .stats;
        if width > 1 {
            assert!(stats.overlaps > 0, "width {width}: {stats:?}");
            wide.merge(&stats);
        }
    }
    // Two open batches of one lane write one execution. The later stage
    // waits, and a cycle fails one stage as a deadlock victim.
    assert!(wide.waits > 0, "{wide:?}");
    assert!(wide.deadlocks > 0, "{wide:?}");
}

#[test]
fn each_nemesis_fires_and_adds_its_fault() {
    let quiet = cover(&[]);
    assert_eq!(quiet.injected_failures, 0, "{quiet:?}");
    assert_eq!(quiet.failed_commits, 0, "{quiet:?}");
    assert!(quiet.fired.iter().all(|n| *n == 0), "{quiet:?}");

    let fail = cover(&[Nemesis::FailDecision]);
    assert!(fail.injected_failures > 0, "{fail:?}");
    assert!(fail.isolated_failures > 0, "{fail:?}");
    let commit = cover(&[Nemesis::FailCommit]);
    assert!(commit.failed_commits > 0, "{commit:?}");
    let takeover = cover(&[Nemesis::Takeover]);
    assert!(
        takeover.takeovers > 0 && takeover.fenced_batches > 0,
        "{takeover:?}"
    );
}

#[test]
fn each_mutant_fails_on_its_own_invariant() {
    for (defect, invariant) in MUTANTS {
        let report = first_failure(defect, invariant).unwrap_or_else(|| {
            panic!(
                "{} never broke {} within {} seeds",
                defect.as_str(),
                invariant.as_str(),
                RED_SEEDS.count
            )
        });
        let violation = report.violation.expect("a violation");
        assert_eq!(violation.invariant, invariant, "{}", defect.as_str());
        // The design passes the same seed.
        let fixed = group::run(&GroupConfig::new(report.config.seed));
        assert_eq!(fixed.violation, None, "{}", fixed.trace_tail(40));
    }
}

#[test]
fn every_defect_has_a_mutant_row() {
    for defect in Defect::ALL {
        if defect == Defect::None {
            continue;
        }
        assert!(
            MUTANTS.iter().any(|(d, _)| *d == defect),
            "{} has no mutant row",
            defect.as_str()
        );
    }
}

#[test]
fn a_seed_replays_byte_for_byte() {
    for seed in 0..32 {
        let config = GroupConfig::new(seed);
        let report = group::run_twice(&config).unwrap_or_else(|error| panic!("{error}"));
        assert_ne!(report.trace.len(), 0);
        assert!(report.completed, "seed {seed}: {}", report.trace_tail(40));
    }
}

#[test]
fn distinct_seeds_give_distinct_runs() {
    let a = group::run(&GroupConfig::new(1));
    let b = group::run(&GroupConfig::new(2));
    assert_ne!(a.trace, b.trace);
}

#[test]
fn the_drain_injects_no_fault() {
    for seed in COVER_SEEDS.seeds() {
        let report = group::run(&GroupConfig::new(seed));
        let start = report
            .trace
            .iter()
            .position(|line| line.contains("drain starts"));
        let Some(start) = start else {
            continue;
        };
        for line in &report.trace[start..] {
            assert!(
                !line.contains("NEMESIS"),
                "seed {seed}: a fault in the drain: {line}"
            );
        }
    }
}

#[test]
fn a_sweep_failure_prints_a_local_replay_command() {
    let failure = group::sweep(&RED_SEEDS, |seed| {
        GroupConfig::new(seed)
            .with_defect(Defect::EarlyAck)
            .checking(&[GroupInvariant::AckAfterCommit])
    })
    .expect_err("the early-ack sweep fails");
    let text = failure.to_string();
    let seed = failure.config.seed;
    assert!(
        text.contains(&format!("{}={seed}", group::SEED_VAR)),
        "{text}"
    );
    assert!(
        text.contains("HARVEST_DST_GROUP_DEFECT=early-ack"),
        "{text}"
    );
    let replay = group::config_from_vars(seed, Some("early-ack"), None, Some("AckAfterCommit"))
        .expect("valid variables");
    assert_eq!(replay, failure.config);
    assert!(group::run(&replay).violation.is_some());
}

#[test]
fn config_vars_parse_and_reject_unknown_names() {
    let config = group::config_from_vars(3, Some("no-fence"), Some("takeover"), None)
        .expect("valid variables");
    assert_eq!(config.defect, Defect::NoFence);
    assert_eq!(config.nemeses, vec![Nemesis::Takeover]);
    assert!(group::config_from_vars(3, Some("nope"), None, None).is_err());
    assert!(group::config_from_vars(3, None, Some("nope"), None).is_err());
    assert!(group::config_from_vars(3, None, None, Some("nope")).is_err());
}

#[test]
fn the_simulator_plans_and_acknowledges_with_the_engine_kernels() {
    let source = read("autumn-harvest/src/dst/group.rs");
    for kernel in ["group_commit::plan_batch", "group_commit::acknowledges"] {
        assert!(source.contains(kernel), "the simulator must call {kernel}");
    }
}

#[test]
fn the_docs_name_every_defect_nemesis_and_invariant() {
    let docs = read("docs/testing/simulation.md");
    for name in Defect::ALL.iter().map(|d| d.as_str()) {
        assert!(
            docs.contains(&format!("`{name}`")),
            "simulation.md misses {name}"
        );
    }
    for name in Nemesis::ALL.iter().map(|n| n.as_str()) {
        assert!(
            docs.contains(&format!("`{name}`")),
            "simulation.md misses {name}"
        );
    }
    for name in GroupInvariant::ALL.iter().map(|i| i.as_str()) {
        assert!(
            docs.contains(&format!("`{name}`")),
            "simulation.md misses {name}"
        );
    }
    assert!(
        docs.contains(group::SEED_VAR),
        "simulation.md misses the seed variable"
    );
}

/// The sweep that CI and the nightly job run.
#[test]
fn group_sweep() {
    let plan = SeedPlan::from_env_named(group::GROUP_SEED_VARS, DEFAULT_SEEDS)
        .unwrap_or_else(|error| panic!("{error}"));
    let template = group::config_from_env(0).unwrap_or_else(|error| panic!("{error}"));
    let summary = group::sweep(&plan, |seed| GroupConfig {
        seed,
        ..template.clone()
    })
    .unwrap_or_else(|failure| panic!("{failure}"));
    assert_eq!(summary.seeds, plan.count, "every planned seed ran");
    println!(
        "dst group: {} seeds from {} passed: {:?}",
        summary.seeds, plan.first, summary.stats
    );
}

/// Print the full trace of the seed in `HARVEST_DST_GROUP_SEED`.
#[test]
fn replay_one_group_seed() {
    let Ok(text) = std::env::var(group::SEED_VAR) else {
        return;
    };
    let plan = SeedPlan::parse(Some(&text), None, None, 1).unwrap_or_else(|e| panic!("{e}"));
    let config = group::config_from_env(plan.first).unwrap_or_else(|error| panic!("{error}"));
    let report = group::run_twice(&config).unwrap_or_else(|error| panic!("{error}"));
    for line in &report.trace {
        println!("{line}");
    }
    if let Some(reason) = report.failure() {
        panic!("seed {}: {reason}", config.seed);
    }
}
