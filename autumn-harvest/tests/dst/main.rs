//! Deterministic simulation sweeps (issue #1830).
//!
//! These tests need no database. `docs/testing/simulation.md` lists the
//! environment variables and the replay command.

use autumn_harvest::dst::{
    self, ClaimStore, Fencing, Invariant, Outcome, SeedPlan, SimConfig, SimReport, WriteOutcome,
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
    assert!(stats.releases > 0, "{stats:?}");
    assert!(stats.stalls > 0 && stats.crashes > 0, "{stats:?}");
    // The faults have an effect: stale owners lose writes, a terminal write
    // included, and some lose after another claim finished the task.
    assert!(stats.stale_completes_rejected > 0, "{stats:?}");
    assert!(stats.stale_after_finish > 0, "{stats:?}");
    // A worker beats between a scan and its requeue, and the row stays.
    assert!(stats.requeues_kept > 0, "{stats:?}");
}

#[test]
fn pre_fix_guard_reproduces_issue_1789() {
    let report = first_pre_fix_failure();
    let violation = report.violation.clone().expect("a violation");
    assert_eq!(violation.invariant, Invariant::TerminalByCurrentClaim);
    assert_eq!(
        report.config.seed, 3,
        "docs/testing/simulation.md and the changelog fragment name seed 3; update both"
    );

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

    // The same operations, in the same order, on a store with the fence.
    let mut fenced = SimConfig::new(failing.config.seed).oracle();
    let outcomes: Vec<Outcome> = failing
        .steps
        .iter()
        .map(|record| fenced.apply(&record.op))
        .collect();
    assert_eq!(
        outcomes.last(),
        Some(&Outcome::Write(WriteOutcome::LeaseLost)),
        "the fence rejects the write that broke the invariant"
    );

    // The fixed run of the seed breaks no invariant.
    let fixed = dst::run(&SimConfig::new(failing.config.seed));
    assert_eq!(fixed.violation, None, "{}", fixed.trace_tail(40));
}

#[test]
fn pre_fix_release_reuses_a_live_fencing_token() {
    let found = (0..1_000)
        .map(|seed| {
            let config = SimConfig::new(seed)
                .with_fencing(Fencing::StateOnly)
                .checking(&[Invariant::ClaimIdsAreUnique]);
            dst::run(&config)
        })
        .find(|report| report.violation.is_some())
        .expect("the pre-fix guard reuses a token within 1000 seeds");
    let fixed = dst::run(&SimConfig::new(found.config.seed));
    assert_eq!(fixed.violation, None, "{}", fixed.trace_tail(40));
}

#[test]
fn a_sweep_failure_prints_a_local_replay_command() {
    let failure = dst::sweep(&PRE_FIX_SEEDS, pre_fix).expect_err("the pre-fix sweep fails");
    let text = failure.to_string();
    let seed = failure.config.seed;
    assert!(text.contains(&format!("HARVEST_DST_SEED={seed}")), "{text}");
    assert!(text.contains("HARVEST_DST_FENCING=state-only"), "{text}");
    assert!(
        text.contains("HARVEST_DST_CHECKS=TerminalByCurrentClaim"),
        "{text}"
    );

    // The variables in the command rebuild the config, so the replay stops
    // at the same violation.
    let replay = dst::config_from_vars(seed, Some("state-only"), Some("TerminalByCurrentClaim"))
        .expect("valid variables");
    assert_eq!(replay, failure.config);
    let report = dst::run(&replay);
    let violation = report.violation.expect("the replay fails too");
    assert!(
        failure.reason.starts_with(&violation.to_string()),
        "{}",
        failure.reason
    );
}

#[test]
fn every_seed_runs_twice_with_identical_traces() {
    for seed in 0..32 {
        let config = SimConfig::new(seed);
        let report = dst::run_twice(&config).unwrap_or_else(|error| panic!("{error}"));
        assert_ne!(report.trace.len(), 0);
    }
}

/// FNV-1a over the trace text.
fn trace_hash(report: &SimReport) -> u64 {
    report
        .trace
        .join("\n")
        .bytes()
        .fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
        })
}

/// The traces of a few seeds are fixed.
///
/// A run-twice check runs in one process, so it cannot see a difference
/// between platforms. CI runs this test on Linux, macOS and Windows. A
/// change to the scheduler or its weights changes these values on purpose.
/// Update them in the same change.
#[test]
fn golden_traces_are_equal_on_every_platform() {
    let golden = [
        (0, Fencing::ClaimEpoch, 276, 0x82fa_8634_9ead_5f35),
        (1, Fencing::ClaimEpoch, 76, 0x1cd1_2fc8_7839_5537),
        (2, Fencing::ClaimEpoch, 314, 0x18a9_744d_50c2_d969),
        (6, Fencing::StateOnly, 47, 0xf8f4_7697_a41b_2e0b),
    ];
    for (seed, fencing, lines, hash) in golden {
        let report = dst::run(&SimConfig::new(seed).with_fencing(fencing));
        assert_eq!(
            (report.trace.len(), trace_hash(&report)),
            (lines, hash),
            "seed {seed}: the trace changed"
        );
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
    assert_eq!(one.seeds(), 17..=17, "one seed wins over a range");
    let range = SeedPlan::parse(None, Some("9"), Some("3"), 5).expect("valid");
    assert_eq!(range.seeds(), 3..=11);
    let default = SeedPlan::parse(None, None, None, 5).expect("valid");
    assert_eq!(default.seeds(), 0..=4);
    let spaced = SeedPlan::parse(Some(" 4 "), None, None, 5).expect("valid");
    assert_eq!(spaced.seeds(), 4..=4);
    assert!(SeedPlan::parse(Some("x"), None, None, 5).is_err());
    assert!(SeedPlan::parse(None, Some("-1"), None, 5).is_err());
}

/// The sweep that CI and the nightly job run.
///
/// `HARVEST_DST_SEEDS` and `HARVEST_DST_SEED_BASE` pick the seeds.
/// `HARVEST_DST_SEED` runs one seed. `HARVEST_DST_FENCING` and
/// `HARVEST_DST_CHECKS` change the config.
#[test]
fn seed_sweep() {
    let plan = SeedPlan::from_env(DEFAULT_SEEDS).unwrap_or_else(|error| panic!("{error}"));
    let template = dst::config_from_env(0).unwrap_or_else(|error| panic!("{error}"));
    let summary = dst::sweep(&plan, |seed| SimConfig {
        seed,
        ..template.clone()
    })
    .unwrap_or_else(|failure| panic!("{failure}"));
    assert_eq!(summary.seeds, plan.count, "every planned seed ran");
    println!(
        "dst: {} seeds from {} passed, fencing {}: {:?}",
        summary.seeds,
        plan.first,
        template.fencing.as_str(),
        summary.stats
    );
}

/// Print the full trace of the seed in `HARVEST_DST_SEED`.
///
/// The seed runs twice, as in a sweep, so a nondeterminism failure
/// reproduces too. Without that variable, the test does nothing.
#[test]
fn replay_one_seed() {
    let Ok(text) = std::env::var(dst::SEED_VAR) else {
        return;
    };
    let plan = SeedPlan::parse(Some(&text), None, None, 1).unwrap_or_else(|e| panic!("{e}"));
    let seed = plan.first;
    let config = dst::config_from_env(seed).unwrap_or_else(|error| panic!("{error}"));
    let report = dst::run_twice(&config).unwrap_or_else(|error| panic!("{error}"));
    for line in &report.trace {
        println!("{line}");
    }
    if let Some(violation) = report.violation {
        panic!("seed {seed}: {violation}");
    }
}
