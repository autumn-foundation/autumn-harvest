//! The speculation model of issue #2011.
//!
//! A worker runs the next decision while the previous commit flushes. These
//! tests check the model, its invariants and the results that
//! `docs/rnd/speculative-execution-spike.md` cites.

use autumn_harvest::dst::SeedPlan;
use autumn_harvest::dst::speculate::{
    self, Faults, Fence, Logging, Mode, Plant, SpecConfig, SpecInvariant, SpecReport, Timing,
    Workload,
};

/// The seeds of one cited sweep.
const SWEEP: SeedPlan = SeedPlan {
    first: 0,
    count: 64,
};

/// Durations with no spread, so the latency of a run has a closed form.
const FIXED: Timing = Timing::fixed(100, 2_000, 15_000, 500, 40_000);

fn quiet(seed: u64, mode: Mode, workload: Workload) -> SpecConfig {
    SpecConfig::new(seed)
        .with_mode(mode)
        .with_workload(workload)
        .with_timing(FIXED)
        .with_faults(Faults::NONE)
}

fn trace_hash(report: &SpecReport) -> u64 {
    report
        .trace
        .join("\n")
        .bytes()
        .fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
        })
}

/// The first seed of `SWEEP` whose run breaks an invariant.
fn first_failure(config: impl Fn(u64) -> SpecConfig) -> SpecReport {
    SWEEP
        .seeds()
        .map(|seed| speculate::run(&config(seed)))
        .find(|report| report.violation.is_some())
        .expect("a seed of the sweep fails")
}

#[test]
fn the_default_config_is_todays_engine() {
    let config = SpecConfig::new(7);
    assert_eq!(config.seed, 7);
    assert_eq!(config.mode, Mode::Serial);
    assert_eq!(config.fence, Fence::Epoch);
    assert_eq!(config.logging, Logging::Full);
    assert_eq!(config.plant, Plant::None);
    assert_eq!(config.workload, Workload::Chain);
    assert_eq!(config.timing, Timing::BENCH);
    assert_eq!(config.faults, Faults::DEFAULT);
    assert_eq!(config.checks, SpecInvariant::ALL.to_vec());
    assert!(config.workers >= 2 && config.executions >= 2, "{config:?}");
}

#[test]
fn every_name_parses_back() {
    for mode in Mode::ALL {
        assert_eq!(Mode::parse(mode.as_str()), Ok(mode));
    }
    for fence in Fence::ALL {
        assert_eq!(Fence::parse(fence.as_str()), Ok(fence));
    }
    for logging in Logging::ALL {
        assert_eq!(Logging::parse(logging.as_str()), Ok(logging));
    }
    for plant in Plant::ALL {
        assert_eq!(Plant::parse(plant.as_str()), Ok(plant));
    }
    for workload in Workload::ALL {
        assert_eq!(Workload::parse(workload.as_str()), Ok(workload));
    }
    for check in SpecInvariant::ALL {
        assert_eq!(SpecInvariant::parse(check.name()), Ok(check));
    }
    let error = Mode::parse("bogus").expect_err("an unknown mode");
    assert!(
        error.contains("serial") && error.contains("eager"),
        "{error}"
    );
}

#[test]
fn a_seed_replays_exactly_in_every_mode() {
    for mode in Mode::ALL {
        for workload in Workload::ALL {
            for seed in 0..8 {
                let config = SpecConfig::new(seed)
                    .with_mode(mode)
                    .with_workload(workload)
                    .checking(&[]);
                let report = speculate::run_twice(&config).unwrap_or_else(|e| panic!("{e}"));
                assert!(
                    report.trace.len() >= config.executions,
                    "{:?}",
                    report.trace
                );
            }
        }
    }
}

#[test]
fn run_twice_reports_a_difference() {
    let mut calls = 0;
    let config = SpecConfig::new(3);
    let error = speculate::run_twice_with(&config, |config| {
        calls += 1;
        let mut report = speculate::run(config);
        if calls == 2 {
            report.trace[5].push_str(" changed");
        }
        report
    })
    .expect_err("the second run differs");
    assert_eq!(error.seed, 3);
    assert_eq!(error.line, 5);
}

/// The traces of a few seeds are fixed. A change to the model changes them
/// on purpose. Update them in the same change.
#[test]
fn golden_speculation_traces_are_equal_on_every_platform() {
    let golden = [
        (0, Mode::Serial, Workload::Chain, 78, 0xae67_96ad_eb1b_e48d),
        (1, Mode::Gated, Workload::FanOut, 102, 0xd8e4_5a76_6128_dd86),
        (2, Mode::Eager, Workload::Chain, 64, 0xc82c_d871_328c_1072),
    ];
    for (seed, mode, workload, lines, hash) in golden {
        let config = SpecConfig::new(seed)
            .with_mode(mode)
            .with_workload(workload)
            .checking(&[]);
        let report = speculate::run(&config);
        assert_eq!(
            (report.trace.len(), trace_hash(&report)),
            (lines, hash),
            "seed {seed} {mode:?} {workload:?}: the trace changed"
        );
    }
}

#[test]
fn serial_chain_latency_has_a_closed_form() {
    let report = speculate::run(&quiet(0, Mode::Serial, Workload::Chain));
    assert!(report.violation.is_none(), "{:?}", report.violation);
    let (decide, commit, dispatch, activity, wake) = (100, 2_000, 15_000, 500, 40_000);
    let expected = decide + commit + 3 * (dispatch + activity + wake + decide + commit);
    assert_eq!(report.latencies_us.len(), report.config.executions);
    assert!(
        report.latencies_us.iter().all(|&l| l == expected),
        "{:?} != {expected}",
        report.latencies_us
    );
}

/// The end-to-end p50 of the bench workflow, unloaded, `fsync` on.
const MEASURED_CHAIN_P50_US: u64 = 470_500;

#[test]
fn the_calibrated_model_matches_the_measured_bench() {
    let summary = speculate::sweep(&SWEEP, |seed| {
        SpecConfig::new(seed).with_faults(Faults::NONE)
    })
    .unwrap_or_else(|failure| panic!("{failure}"));
    let mean = summary.latency.mean_us;
    assert!(
        mean.abs_diff(MEASURED_CHAIN_P50_US) * 50 < MEASURED_CHAIN_P50_US,
        "model mean {mean} us is not within 2 % of the measured {MEASURED_CHAIN_P50_US} us"
    );
}

#[test]
fn gated_speculation_has_nothing_to_run_on_a_chain() {
    for seed in 0..8 {
        let serial = speculate::run(&quiet(seed, Mode::Serial, Workload::Chain));
        let gated = speculate::run(&quiet(seed, Mode::Gated, Workload::Chain));
        assert!(gated.violation.is_none(), "{:?}", gated.violation);
        assert_eq!(gated.stats.speculative, 0, "{:?}", gated.stats);
        assert_eq!(gated.latencies_us, serial.latencies_us);
    }
}

/// Bench durations, but with activities from 1 ms to 100 ms. Results then
/// arrive while a commit flushes, so speculation has input to run on.
const SPREAD: Timing = Timing {
    activity_us: (1_000, 100_000),
    ..Timing::BENCH
};

#[test]
fn bench_timing_gives_fan_out_no_speculation() {
    let summary = speculate::sweep(&SWEEP, |seed| {
        SpecConfig::new(seed)
            .with_mode(Mode::Gated)
            .with_workload(Workload::FanOut)
            .with_faults(Faults::NONE)
    })
    .unwrap_or_else(|failure| panic!("{failure}"));
    assert_eq!(summary.stats.speculative, 0, "{:?}", summary.stats);
}

#[test]
fn gated_speculation_moves_fan_out_latency_by_under_one_percent() {
    let sweep = |mode| {
        speculate::sweep(&SWEEP, |seed| {
            SpecConfig::new(seed)
                .with_mode(mode)
                .with_workload(Workload::FanOut)
                .with_timing(SPREAD)
                .with_faults(Faults::NONE)
        })
        .unwrap_or_else(|failure| panic!("{failure}"))
    };
    let serial = sweep(Mode::Serial);
    let gated = sweep(Mode::Gated);
    assert!(gated.stats.speculative > 0, "{:?}", gated.stats);
    let (s, g) = (serial.latency.mean_us, gated.latency.mean_us);
    assert!(s.abs_diff(g) * 100 < s, "serial {s} us, gated {g} us");
    // A speculative decision overlaps only a commit flight, so it saves at
    // most one decide time.
    let bound = gated.stats.speculative * SPREAD.decide_us;
    let saved: u64 = serial.latency.mean_us.saturating_sub(g) * serial.latency.runs;
    assert!(saved <= bound, "saved {saved} us, bound {bound} us");
}

#[test]
fn eager_release_hides_one_commit_per_hop_on_a_chain() {
    let serial = speculate::run(&quiet(0, Mode::Serial, Workload::Chain));
    let eager = speculate::run(&quiet(0, Mode::Eager, Workload::Chain).checking(&[]));
    let commit = 2_000;
    for (s, e) in serial.latencies_us.iter().zip(&eager.latencies_us) {
        assert_eq!(s - e, 3 * commit, "serial {s}, eager {e}");
    }
}

#[test]
fn eager_release_breaks_the_commit_gate() {
    let config =
        quiet(0, Mode::Eager, Workload::Chain).checking(&[SpecInvariant::EffectAfterCommit]);
    let report = speculate::run(&config);
    let violation = report
        .violation
        .expect("an effect starts before its commit");
    assert_eq!(violation.invariant, SpecInvariant::EffectAfterCommit);
}

#[test]
fn eager_release_runs_an_effect_twice_after_a_failed_commit() {
    let report = first_failure(|seed| {
        SpecConfig::new(seed)
            .with_mode(Mode::Eager)
            .checking(&[SpecInvariant::EffectOnce])
    });
    let violation = report.violation.expect("a violation");
    assert_eq!(violation.invariant, SpecInvariant::EffectOnce);
}

#[test]
fn eager_release_can_strand_an_execution() {
    // A stall before the send lets the effect finish first. The store
    // rejects its result, then the commit lands and nothing runs it again.
    let report = first_failure(|seed| {
        SpecConfig::new(seed)
            .with_mode(Mode::Eager)
            .checking(&[SpecInvariant::Converges])
    });
    let violation = report.violation.expect("a violation");
    assert_eq!(violation.invariant, SpecInvariant::Converges);
    assert!(report.stats.orphan_results > 0, "{:?}", report.stats);
}

#[test]
fn a_prefix_only_fence_lets_a_stale_owner_commit() {
    let report = first_failure(|seed| {
        SpecConfig::new(seed)
            .with_mode(Mode::Gated)
            .with_fence(Fence::PrefixOnly)
    });
    let violation = report.violation.expect("a violation");
    assert_eq!(violation.invariant, SpecInvariant::CommitByOwner);
}

#[test]
fn a_prefix_only_fence_keeps_every_other_invariant() {
    let others: Vec<_> = SpecInvariant::ALL
        .into_iter()
        .filter(|check| *check != SpecInvariant::CommitByOwner)
        .collect();
    for workload in Workload::ALL {
        let summary = speculate::sweep(&SWEEP, |seed| {
            SpecConfig::new(seed)
                .with_mode(Mode::Gated)
                .with_workload(workload)
                .with_fence(Fence::PrefixOnly)
                .checking(&others)
        })
        .unwrap_or_else(|failure| panic!("{failure}"));
        assert!(summary.stats.stale_commits > 0, "{:?}", summary.stats);
    }
}

#[test]
fn serial_and_gated_keep_every_invariant_under_faults() {
    for mode in [Mode::Serial, Mode::Gated] {
        for workload in Workload::ALL {
            for logging in Logging::ALL {
                let summary = speculate::sweep(&SWEEP, |seed| {
                    SpecConfig::new(seed)
                        .with_mode(mode)
                        .with_workload(workload)
                        .with_logging(logging)
                })
                .unwrap_or_else(|failure| panic!("{failure}"));
                let stats = summary.stats;
                assert_eq!(summary.seeds, SWEEP.count);
                assert_eq!(stats.completed, SWEEP.count * 4, "{stats:?}");
                assert!(stats.crashes > 0 && stats.stalls > 0, "{stats:?}");
                assert!(stats.reclaims > 0 && stats.cold_loads > 0, "{stats:?}");
                assert!(
                    stats.fenced > 0,
                    "a stale owner loses its commit: {stats:?}"
                );
                assert!(stats.failed_commits > 0, "{stats:?}");
                assert!(
                    stats.lost_in_crash > 0 && stats.landed_in_crash > 0,
                    "{stats:?}"
                );
                if mode == Mode::Gated && workload == Workload::FanOut {
                    assert!(stats.speculative > 0, "{stats:?}");
                    assert!(stats.discarded > 0, "a failed commit discards: {stats:?}");
                }
            }
        }
    }
}

#[test]
fn a_planted_repair_defect_is_found_and_replays_from_its_seed() {
    let config = |seed| {
        SpecConfig::new(seed)
            .with_mode(Mode::Gated)
            .with_workload(Workload::FanOut)
            .with_plant(Plant::KeepOnFailure)
    };
    let report = first_failure(config);
    let violation = report.violation.clone().expect("a violation");
    assert!(
        matches!(
            violation.invariant,
            SpecInvariant::ReplayEquivalent | SpecInvariant::ExpectedOutput
        ),
        "{violation}"
    );
    let again = speculate::run_twice(&config(report.config.seed)).expect("deterministic");
    assert_eq!(again.violation, Some(violation));
    assert!(speculate::repro_command(&report.config).contains("keep-on-failure"));
}

#[test]
fn reads_only_logging_drops_exactly_the_write_rows() {
    for workload in Workload::ALL {
        let full = speculate::run(&quiet(0, Mode::Serial, workload));
        let reads =
            speculate::run(&quiet(0, Mode::Serial, workload).with_logging(Logging::ReadsOnly));
        assert!(reads.violation.is_none(), "{:?}", reads.violation);
        assert_eq!(full.latencies_us, reads.latencies_us);
        assert!(full.stats.write_rows > 0);
        assert_eq!(reads.stats.write_rows, 0);
        assert_eq!(
            full.stats.log_rows - full.stats.write_rows,
            reads.stats.log_rows
        );
    }
}

#[test]
fn the_repro_command_carries_every_knob() {
    let config = SpecConfig::new(42)
        .with_mode(Mode::Gated)
        .with_fence(Fence::PrefixOnly)
        .with_logging(Logging::ReadsOnly)
        .with_plant(Plant::KeepOnFailure)
        .with_workload(Workload::FanOut);
    let command = speculate::repro_command(&config);
    for part in [
        "HARVEST_DST_SEED=42",
        "HARVEST_DST_SPEC_MODE=gated",
        "HARVEST_DST_SPEC_FENCE=prefix-only",
        "HARVEST_DST_SPEC_LOGGING=reads-only",
        "HARVEST_DST_SPEC_PLANT=keep-on-failure",
        "HARVEST_DST_SPEC_WORKLOAD=fan-out",
        "speculate::",
    ] {
        assert!(command.contains(part), "{command} lacks {part}");
    }
    let vars = |name: &str| match name {
        "HARVEST_DST_SPEC_MODE" => Some("gated".to_string()),
        "HARVEST_DST_SPEC_FENCE" => Some("prefix-only".to_string()),
        "HARVEST_DST_SPEC_LOGGING" => Some("reads-only".to_string()),
        "HARVEST_DST_SPEC_PLANT" => Some("keep-on-failure".to_string()),
        "HARVEST_DST_SPEC_WORKLOAD" => Some("fan-out".to_string()),
        _ => None,
    };
    assert_eq!(speculate::config_from_vars(42, vars), Ok(config));
    let bad = speculate::config_from_vars(0, |name| {
        (name == "HARVEST_DST_SPEC_MODE").then(|| "x".to_string())
    });
    assert!(bad.is_err());
}

/// The sweep that CI and the nightly job run.
///
/// `HARVEST_DST_SEEDS` and `HARVEST_DST_SEED_BASE` pick the seeds. The
/// `HARVEST_DST_SPEC_*` variables pick the config.
#[test]
fn speculation_sweep() {
    let plan = SeedPlan::from_env(SWEEP.count).unwrap_or_else(|error| panic!("{error}"));
    let template = speculate::config_from_env(0).unwrap_or_else(|error| panic!("{error}"));
    let summary = speculate::sweep(&plan, |seed| SpecConfig {
        seed,
        ..template.clone()
    })
    .unwrap_or_else(|failure| panic!("{failure}"));
    assert_eq!(summary.seeds, plan.count);
    println!(
        "speculate: {} seeds from {}, {} {}: {:?} latency {:?}",
        summary.seeds,
        plan.first,
        template.mode.as_str(),
        template.workload.as_str(),
        summary.stats,
        summary.latency
    );
}

/// Print the trace of the seed in `HARVEST_DST_SEED`. Without that variable,
/// the test does nothing.
#[test]
fn replay_one_speculation_seed() {
    let Ok(text) = std::env::var(autumn_harvest::dst::SEED_VAR) else {
        return;
    };
    let plan = SeedPlan::parse(Some(&text), None, None, 1).unwrap_or_else(|e| panic!("{e}"));
    let config = speculate::config_from_env(plan.first).unwrap_or_else(|e| panic!("{e}"));
    let report = speculate::run_twice(&config).unwrap_or_else(|e| panic!("{e}"));
    for line in &report.trace {
        println!("{line}");
    }
    if let Some(violation) = report.violation {
        panic!("seed {}: {violation}", plan.first);
    }
}
