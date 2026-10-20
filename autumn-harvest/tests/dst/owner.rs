//! The owner-lease simulation of issue #2250.
//!
//! These tests drive the four nemeses of `formal/tla/OwnerLease.tla` from a
//! seed. Each TLA+ defect must fail here on the same invariant. The design
//! must pass every seed.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use autumn_harvest::dst::SeedPlan;
use autumn_harvest::dst::owner::{
    self, Decision, Defect, Epoch, Fence, LeaseRow, Nemesis, OracleOwnerStore, OwnerConfig,
    OwnerInvariant, OwnerReport, OwnerStats, OwnerStore, Renewal, SignalId, WorkerId,
};

/// The sweep size of a normal test run. The nightly job sets
/// `HARVEST_DST_OWNER_SEEDS` much higher.
const DEFAULT_SEEDS: u64 = 300;

/// A defect must fail within these seeds.
const RED_SEEDS: SeedPlan = SeedPlan {
    first: 0,
    count: 200,
};

/// The seeds of a coverage sweep.
const COVER_SEEDS: SeedPlan = SeedPlan {
    first: 0,
    count: 200,
};

/// The table that stage 1a adds. ADR 0010 names it.
const OWNER_TABLE: &str = "harvest_decision_owner";

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

/// The lines of a TLC config with `\*` comments removed.
fn cfg_lines(cfg: &str) -> impl Iterator<Item = &str> {
    cfg.lines()
        .map(|l| l.split("\\*").next().unwrap_or_default().trim())
}

/// The first report of `RED_SEEDS` that breaks `invariant` under `defect`.
fn first_failure(defect: Defect, invariant: OwnerInvariant) -> Option<OwnerReport> {
    RED_SEEDS
        .seeds()
        .map(|seed| {
            owner::run(
                &OwnerConfig::new(seed)
                    .with_defect(defect)
                    .checking(&[invariant]),
            )
        })
        .find(|report| report.violation.is_some())
}

/// The `Defect` and expected invariant of each red `OwnerLease` row in
/// `formal/tla/models.txt`.
fn tla_red_rows() -> BTreeMap<String, String> {
    let mut rows = BTreeMap::new();
    for line in read("formal/tla/models.txt").lines().map(str::trim) {
        let cols: Vec<&str> = line.split_whitespace().collect();
        let [spec, config, expect] = cols[..] else {
            continue;
        };
        let Some(invariant) = expect.strip_prefix("violation:") else {
            continue;
        };
        if spec != "OwnerLease" {
            continue;
        }
        let cfg = read(&format!("formal/tla/{config}"));
        let defect = cfg_lines(&cfg)
            .find_map(|l| l.strip_prefix("Defect = "))
            .map_or_else(
                || panic!("{config} sets no Defect"),
                |v| v.trim_matches('"').to_string(),
            );
        if defect != "none" {
            rows.insert(defect, invariant.to_string());
        }
    }
    rows
}

/// The coverage of `COVER_SEEDS` on the design with `nemeses`.
fn cover(nemeses: &[Nemesis]) -> OwnerStats {
    owner::sweep(&COVER_SEEDS, |seed| {
        OwnerConfig::new(seed).with_nemeses(nemeses)
    })
    .unwrap_or_else(|f| panic!("{f}"))
    .stats
}

/// The counter that shows the fault of `nemesis`.
const fn effect(nemesis: Nemesis, stats: &OwnerStats) -> u64 {
    match nemesis {
        Nemesis::StaleOwner => stats.stale_commits_fenced,
        Nemesis::LeaseFlap => stats.flaps_fenced + stats.appends_after_expiry,
        Nemesis::RenewInFlight => stats.renewals_in_flight,
        Nemesis::AckBeforeBump => stats.acks_before_bump,
    }
}

#[test]
fn the_default_config_is_the_design_with_all_nemeses() {
    let config = OwnerConfig::new(5);
    assert_eq!(config.seed, 5);
    assert_eq!(config.defect, Defect::None);
    assert_eq!(config.nemeses, Nemesis::ALL.to_vec());
    assert_eq!(config.checks, OwnerInvariant::ALL.to_vec());
    assert!(config.workers >= 2, "a takeover needs two workers");
}

#[test]
fn the_design_passes_every_seed_and_reaches_every_nemesis() {
    let stats = cover(&Nemesis::ALL);
    assert!(stats.commits > 0, "{stats:?}");
    assert!(stats.takeovers > 0, "{stats:?}");
    assert!(stats.releases > 0, "{stats:?}");
    // Nemesis 1: a stale owner reaches its commit after a takeover.
    assert!(stats.stale_commits_fenced > 0, "{stats:?}");
    // Nemesis 2: the lease flaps under a decision, and ends mid-append.
    assert!(stats.flaps_fenced > 0, "{stats:?}");
    assert!(stats.appends_after_expiry > 0, "{stats:?}");
    // Nemesis 3: a renewal lands while a commit holds the fence lock.
    assert!(stats.renewals_in_flight > 0, "{stats:?}");
    // Nemesis 4: an acknowledged signal waits at an epoch bump.
    assert!(stats.acks_before_bump > 0, "{stats:?}");
    // The design never fences its own commit.
    assert_eq!(stats.self_fences, 0, "{stats:?}");
}

#[test]
fn each_nemesis_fires_and_adds_its_fault() {
    let baseline = cover(&[]);
    for nemesis in Nemesis::ALL {
        let stats = cover(&[nemesis]);
        assert!(stats.fired(nemesis) > 0, "{}: {stats:?}", nemesis.as_str());
        assert!(
            effect(nemesis, &stats) > effect(nemesis, &baseline),
            "{} adds no fault over the baseline: {stats:?} against {baseline:?}",
            nemesis.as_str()
        );
        for other in Nemesis::ALL.iter().filter(|n| **n != nemesis) {
            assert_eq!(stats.fired(*other), 0, "{} is off", other.as_str());
        }
    }
}

#[test]
fn every_tla_defect_fails_here_on_the_same_invariant() {
    let rows = tla_red_rows();
    let want: BTreeSet<&str> = Defect::ALL
        .iter()
        .filter(|d| **d != Defect::None)
        .map(|d| d.as_str())
        .collect();
    let have: BTreeSet<&str> = rows.keys().map(String::as_str).collect();
    assert_eq!(
        have, want,
        "the simulator and the TLA+ spec list the same defects"
    );
    for (defect, invariant) in &rows {
        let defect = Defect::parse(defect).expect("a simulator defect");
        let invariant = OwnerInvariant::parse(invariant).expect("a simulator invariant");
        assert!(
            first_failure(defect, invariant).is_some(),
            "{} must break {} within {RED_SEEDS:?}",
            defect.as_str(),
            invariant.as_str()
        );
    }
}

#[test]
fn the_simulator_checks_the_invariants_of_the_tla_design_config() {
    let cfg = read("formal/tla/OwnerLease.cfg");
    let mut in_block = false;
    let mut tla = BTreeSet::new();
    for line in cfg_lines(&cfg) {
        if line == "INVARIANTS" {
            in_block = true;
        } else if in_block && line.chars().all(|c| c.is_ascii_alphanumeric()) && !line.is_empty() {
            tla.insert(line.to_string());
        } else {
            in_block = false;
        }
    }
    assert!(tla.remove("TypeOK"), "OwnerLease.cfg checks TypeOK");
    let sim: BTreeSet<String> = OwnerInvariant::ALL
        .iter()
        .map(|i| i.as_str().to_string())
        .collect();
    assert_eq!(sim, tla);
}

#[test]
fn the_no_fence_mutant_shows_a_stale_owner_commit() {
    let report = first_failure(Defect::NoFence, OwnerInvariant::OneOwnerPerEpoch)
        .expect("the no-fence mutant fails");
    let violation = report.violation.as_ref().expect("a violation");
    // A worker committed at an epoch that it did not pin.
    assert!(
        report.history.iter().any(|e| e.pinned != e.at),
        "{violation}\n{}",
        report.trace.join("\n")
    );
}

#[test]
fn a_seed_replays_byte_for_byte() {
    for seed in 0..16 {
        for defect in Defect::ALL {
            let config = OwnerConfig::new(seed).with_defect(defect);
            owner::run_twice(&config).unwrap_or_else(|n| panic!("{n}"));
        }
    }
}

#[test]
fn run_with_and_sweep_with_drive_a_caller_store() {
    // Stage 1a passes its Postgres store here. The oracle stands in for it.
    let config = OwnerConfig::new(11);
    let mut store = OracleOwnerStore::new(config.defect);
    let with = owner::run_with(&config, &mut store);
    let oracle_run = owner::run(&config);
    assert_eq!(with.trace, oracle_run.trace);
    assert_eq!(with.history, oracle_run.history);

    let plan = SeedPlan { first: 0, count: 8 };
    let mut made = 0;
    let summary = owner::sweep_with(&plan, OwnerConfig::new, |config| {
        made += 1;
        OracleOwnerStore::new(config.defect)
    })
    .unwrap_or_else(|f| panic!("{f}"));
    assert_eq!(
        summary,
        owner::sweep(&plan, OwnerConfig::new).expect("green")
    );
    assert_eq!(made, 16, "each seed runs twice on a fresh store");
}

/// A store whose takeover never succeeds, as with a leaked lock.
struct StuckStore(OracleOwnerStore);

impl OwnerStore for StuckStore {
    fn acquire(&mut self, _worker: WorkerId) -> Option<Epoch> {
        None
    }
    fn release(&mut self, worker: WorkerId, epoch: Epoch) -> bool {
        self.0.release(worker, epoch)
    }
    fn renew(&mut self, worker: WorkerId, epoch: Epoch) -> Renewal {
        self.0.renew(worker, epoch)
    }
    fn expire(&mut self) {
        self.0.expire();
    }
    fn write_signal(&mut self, signal: SignalId) {
        self.0.write_signal(signal);
    }
    fn lock(&mut self, decision: &Decision) -> Fence {
        self.0.lock(decision)
    }
    fn commit(&mut self, decision: &Decision) -> Epoch {
        self.0.commit(decision)
    }
    fn row(&self) -> LeaseRow {
        self.0.row()
    }
    fn pending(&self) -> Vec<SignalId> {
        self.0.pending()
    }
    fn consumed(&self) -> Vec<SignalId> {
        self.0.consumed()
    }
}

#[test]
fn a_store_that_makes_no_progress_fails_the_sweep() {
    let plan = SeedPlan { first: 0, count: 1 };
    let failure = owner::sweep_with(&plan, OwnerConfig::new, |config| {
        StuckStore(OracleOwnerStore::new(config.defect))
    })
    .expect_err("a store with no takeover consumes no signal");
    assert!(
        failure.reason.contains("did not consume every signal"),
        "{failure}"
    );
    assert!(owner::run(&OwnerConfig::new(0)).completed);
}

#[test]
fn config_vars_parse_and_reject_unknown_names() {
    let config = owner::config_from_vars(3, Some("memory-ack"), Some("ack-before-bump"), None)
        .expect("known names");
    assert_eq!(config.defect, Defect::MemoryAck);
    assert_eq!(config.nemeses, vec![Nemesis::AckBeforeBump]);
    assert!(owner::config_from_vars(3, Some("nope"), None, None).is_err());
    assert!(owner::config_from_vars(3, None, Some("nope"), None).is_err());
    assert!(owner::config_from_vars(3, None, None, Some("Nope")).is_err());
    let command = owner::repro_command(&config);
    assert!(command.contains("HARVEST_DST_OWNER_SEED=3"), "{command}");
    assert!(
        command.contains("HARVEST_DST_OWNER_DEFECT=memory-ack"),
        "{command}"
    );
}

/// The sweep that CI runs. The nightly job runs many more seeds.
#[test]
fn owner_sweep() {
    let plan = SeedPlan::from_env_named(owner::OWNER_SEED_VARS, DEFAULT_SEEDS)
        .unwrap_or_else(|e| panic!("{e}"));
    let summary = owner::sweep(&plan, |seed| {
        owner::config_from_env(seed).unwrap_or_else(|e| panic!("{e}"))
    })
    .unwrap_or_else(|f| panic!("{f}"));
    assert_eq!(summary.seeds, plan.count);
}

/// Replay the one seed in `HARVEST_DST_OWNER_SEED` and print its trace.
#[test]
fn replay_one_owner_seed() {
    let Ok(seed) = std::env::var(owner::OWNER_SEED_VARS.seed) else {
        return;
    };
    let seed: u64 = seed.trim().parse().expect("a decimal seed");
    let config = owner::config_from_env(seed).unwrap_or_else(|e| panic!("{e}"));
    let report = owner::run_twice(&config).unwrap_or_else(|n| panic!("{n}"));
    println!("{}", report.trace.join("\n"));
    if let Some(violation) = report.violation {
        panic!("{violation}");
    }
}

/// Every file under `dir`, in a fixed order.
fn files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    let mut paths: Vec<PathBuf> = entries.flatten().map(|e| e.path()).collect();
    paths.sort();
    for path in paths {
        if path.is_dir() {
            out.extend(files(&path));
        } else {
            out.push(path);
        }
    }
    out
}

/// True when one source implements `OwnerStore` for a type other than the
/// oracle and drives it with `run_with` or `sweep_with`.
fn drives_a_real_store(sources: &[String]) -> bool {
    let marker = concat!("impl Owner", "Store for ");
    sources.iter().any(|src| {
        let real = src
            .match_indices(marker)
            .any(|(at, _)| !src[at + marker.len()..].starts_with("Oracle"));
        let driven = src.contains("run_with(") || src.contains("sweep_with(");
        real && driven
    })
}

#[test]
fn the_real_store_matcher_rejects_the_oracle_and_skips_tests_dst() {
    let oracle = format!(
        "{} OracleOwnerStore {{}} run_with(",
        concat!("impl Owner", "Store for")
    );
    let real = format!(
        "{} PgOwnerStore {{}} sweep_with(",
        concat!("impl Owner", "Store for")
    );
    let undriven = format!("{} PgOwnerStore {{}}", concat!("impl Owner", "Store for"));
    assert!(!drives_a_real_store(&[oracle]));
    assert!(!drives_a_real_store(&[undriven]));
    assert!(drives_a_real_store(&[real]));
    // This file drives a test store, so the tripwire must not read it.
    assert!(drives_a_real_store(&[read(
        "autumn-harvest/tests/dst/owner.rs"
    )]));
    let here = std::fs::canonicalize(file_path("autumn-harvest/tests/dst/owner.rs"))
        .expect("this file exists");
    assert!(
        stage_1a_test_files()
            .iter()
            .all(|p| std::fs::canonicalize(p).ok().as_ref() != Some(&here)),
        "the tripwire reads tests/dst"
    );
}

fn file_path(rel: &str) -> PathBuf {
    repo_root().join(rel)
}

/// The test sources that the tripwire reads: every `.rs` file under
/// `autumn-harvest/tests`, except `tests/dst`.
fn stage_1a_test_files() -> Vec<PathBuf> {
    let tests = repo_root().join("autumn-harvest").join("tests");
    let dst = tests.join("dst");
    files(&tests)
        .into_iter()
        .filter(|p| !p.starts_with(&dst))
        .filter(|p| p.extension().is_some_and(|e| e == "rs"))
        .collect()
}

/// AC 2 of issue #2250 binds once stage 1a lands. A migration that adds
/// the owner table needs a test outside `tests/dst` that runs this
/// harness on the real store.
#[test]
fn stage_1a_owner_store_gets_a_simulator_run() {
    let root = repo_root().join("autumn-harvest");
    let owner_table = files(&root.join("migrations")).iter().any(|p| {
        p.extension().is_some_and(|e| e == "sql")
            && std::fs::read_to_string(p).is_ok_and(|sql| sql.contains(OWNER_TABLE))
    });
    if !owner_table {
        return;
    }
    let sources: Vec<String> = stage_1a_test_files()
        .into_iter()
        .filter_map(|p| std::fs::read_to_string(p).ok())
        .collect();
    assert!(
        drives_a_real_store(&sources),
        "a migration adds {OWNER_TABLE}, but no test outside tests/dst implements \
         OwnerStore for the real store and calls owner::run_with or owner::sweep_with \
         (issue #2250, AC 2)"
    );
}
