//! Deterministic simulation sweeps (issue #1830).
//!
//! These tests need no database. `docs/testing/simulation.md` lists the
//! environment variables and the replay command.

use autumn_harvest::dst::{
    self, Fencing, Invariant, Outcome, SeedPlan, SimConfig, SimReport, WriteOutcome,
};

/// The sweep size of a normal test run. The nightly job sets
/// `HARVEST_DST_SEEDS` much higher.
const DEFAULT_SEEDS: u64 = 200;

/// The pre-fix sweep must find the bug within these seeds.
const PRE_FIX_SEEDS: SeedPlan = SeedPlan {
    first: 0,
    count: 64,
};

fn pre_fix(seed: u64) -> SimConfig {
    SimConfig::new(seed)
        .with_fencing(Fencing::StateOnly)
        .checking(&[Invariant::TerminalByCurrentClaim])
}

/// The first seed of `PRE_FIX_SEEDS` that breaks `TerminalByCurrentClaim`
/// under the pre-fix guard.
fn first_pre_fix_failure() -> SimReport {
    PRE_FIX_SEEDS
        .seeds()
        .map(|seed| dst::run(&pre_fix(seed)))
        .find(|report| report.violation.is_some())
        .expect("the pre-fix guard fails within PRE_FIX_SEEDS")
}

#[test]
fn three_workers_drive_claim_heartbeat_reclaim_and_complete() {
    let plan = SeedPlan {
        first: 0,
        count: 32,
    };
    let summary = dst::sweep(&plan, SimConfig::new).unwrap_or_else(|failure| panic!("{failure}"));
    let stats = summary.stats;
    assert_eq!(summary.seeds, 32);
    assert_eq!(stats.claimers, 3, "all three workers claim a row");
    assert!(stats.claims > 0, "{stats:?}");
    assert!(stats.starts > 0, "{stats:?}");
    assert!(stats.heartbeats > 0, "{stats:?}");
    assert!(stats.reclaims > 0, "{stats:?}");
    assert!(stats.completes > 0, "{stats:?}");
    assert!(stats.stalls > 0 && stats.crashes > 0, "{stats:?}");
}

#[test]
fn pre_fix_guard_reproduces_issue_1789() {
    let report = first_pre_fix_failure();
    let violation = report.violation.clone().expect("a violation");
    assert_eq!(violation.invariant, Invariant::TerminalByCurrentClaim);

    // The stale write took effect while another claim held the row.
    let step = report
        .steps
        .iter()
        .find(|record| record.step == violation.step)
        .expect("the violating step is a store operation");
    assert!(matches!(step.op, dst::Op::Complete { .. }), "{:?}", step.op);
    assert_eq!(step.outcome, Outcome::Write(WriteOutcome::Applied));

    // The same seed replays to the same trace and the same violation.
    let again = dst::run(&report.config);
    assert_eq!(again.trace, report.trace);
    assert_eq!(again.violation, report.violation);
}

#[test]
fn claim_epoch_guard_rejects_the_stale_write_on_the_same_seed() {
    let failing = first_pre_fix_failure();
    let fixed = SimConfig::new(failing.config.seed);
    let report = dst::run(&fixed);
    assert_eq!(report.violation, None, "{}", report.trace_tail(40));
    assert!(
        report.stats.stale_completes_rejected > 0,
        "the fixed run reaches the race and the fence wins: {:?}",
        report.stats
    );
}

#[test]
fn a_sweep_failure_prints_a_local_replay_command() {
    let failure = dst::sweep(&PRE_FIX_SEEDS, pre_fix).expect_err("the pre-fix sweep fails");
    let text = failure.to_string();
    assert!(
        text.contains(&format!("HARVEST_DST_SEED={}", failure.seed)),
        "{text}"
    );
    assert!(text.contains("HARVEST_DST_FENCING=state-only"), "{text}");
    assert!(text.contains("TerminalByCurrentClaim"), "{text}");
}

#[test]
fn every_seed_runs_twice_with_identical_traces() {
    for seed in 0..32 {
        let config = SimConfig::new(seed);
        let report = dst::run_twice(&config).unwrap_or_else(|error| panic!("{error}"));
        assert!(!report.trace.is_empty());
    }
}

#[test]
fn distinct_seeds_give_distinct_runs() {
    let a = dst::run(&SimConfig::new(1));
    let b = dst::run(&SimConfig::new(2));
    assert_ne!(a.trace, b.trace);
}

#[test]
fn first_divergence_finds_the_first_differing_line() {
    let lines = |items: &[&str]| items.iter().map(ToString::to_string).collect::<Vec<_>>();
    assert_eq!(
        dst::first_divergence(&lines(&["a", "b"]), &lines(&["a", "b"])),
        None
    );
    assert_eq!(
        dst::first_divergence(&lines(&["a", "b"]), &lines(&["a", "c"])),
        Some(1)
    );
    assert_eq!(
        dst::first_divergence(&lines(&["a"]), &lines(&["a", "b"])),
        Some(1)
    );
}

#[test]
fn seed_plan_reads_one_seed_or_a_range() {
    let one = SeedPlan::parse(Some("17"), Some("9"), Some("3"), 5).expect("valid");
    assert_eq!(one.seeds(), 17..18, "one seed wins over a range");
    let range = SeedPlan::parse(None, Some("9"), Some("3"), 5).expect("valid");
    assert_eq!(range.seeds(), 3..12);
    let default = SeedPlan::parse(None, None, None, 5).expect("valid");
    assert_eq!(default.seeds(), 0..5);
    assert!(SeedPlan::parse(Some("x"), None, None, 5).is_err());
    assert!(SeedPlan::parse(None, Some("-1"), None, 5).is_err());
}

/// The sweep that CI and the nightly job run.
///
/// `HARVEST_DST_SEEDS` and `HARVEST_DST_SEED_BASE` pick the seeds.
/// `HARVEST_DST_SEED` runs one seed.
#[test]
fn seed_sweep() {
    let plan = SeedPlan::from_env(DEFAULT_SEEDS).unwrap_or_else(|error| panic!("{error}"));
    let fencing = dst::fencing_from_env().unwrap_or_else(|error| panic!("{error}"));
    let summary = dst::sweep(&plan, |seed| SimConfig::new(seed).with_fencing(fencing))
        .unwrap_or_else(|failure| panic!("{failure}"));
    println!(
        "dst: {} seeds from {} passed, fencing {}: {:?}",
        summary.seeds,
        plan.first,
        fencing.as_str(),
        summary.stats
    );
}

/// Print the full trace of the seed in `HARVEST_DST_SEED`.
///
/// Without that variable, the test does nothing.
#[test]
fn replay_one_seed() {
    let Ok(seed) = std::env::var(dst::SEED_VAR) else {
        return;
    };
    let seed: u64 = seed.parse().expect("HARVEST_DST_SEED is a decimal u64");
    let fencing = dst::fencing_from_env().unwrap_or_else(|error| panic!("{error}"));
    let report = dst::run(&SimConfig::new(seed).with_fencing(fencing));
    for line in &report.trace {
        println!("{line}");
    }
    if let Some(violation) = report.violation {
        panic!("seed {seed}: {violation}");
    }
}
