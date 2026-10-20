//! Stateright model checks of the DST oracles.
//!
//! The seeded sweeps in `tests/dst/` sample the oracles. These models visit
//! every reachable state of the same oracles within a bound. The TLA+ specs
//! in `formal/tla/` are exhaustive too, but they restate the protocol by
//! hand. Here the checker drives the Rust code that the differential test
//! compares with Postgres.
//!
//! Each model has a passing design check, a counter-example for each
//! defect, and reachability witnesses. See `docs/testing/stateright.md`.

#![cfg(not(any(loom, shuttle)))]

mod activity_claim;
mod owner_lease;

use std::collections::BTreeSet;
use std::fmt::Debug;
use std::hash::Hash;
use std::path::PathBuf;

use stateright::{Checker, HasDiscoveries, Model};

/// Set to `1` to check the larger bounds of the nightly run.
const DEEP_VAR: &str = "HARVEST_STATERIGHT_DEEP";

/// One checker thread for each core.
fn threads() -> usize {
    std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get)
}

/// The larger bounds of the nightly run are on.
fn deep() -> bool {
    std::env::var(DEEP_VAR).is_ok_and(|v| v == "1")
}

/// Check every reachable state of `model` breadth first.
///
/// Breadth first makes each counter-example a shortest one.
fn check_all<M>(model: M) -> impl Checker<M>
where
    M: Model + Send + Sync + 'static,
    M::State: Clone + Debug + Hash + PartialEq + Send + Sync + 'static,
    M::Action: Clone + Debug + PartialEq,
{
    model.checker().threads(threads()).spawn_bfs().join()
}

/// Check `model` until it finds `property`, breadth first.
fn check_until<M>(model: M, property: &'static str) -> impl Checker<M>
where
    M: Model + Send + Sync + 'static,
    M::State: Clone + Debug + Hash + PartialEq + Send + Sync + 'static,
    M::Action: Clone + Debug + PartialEq,
{
    model
        .checker()
        .threads(threads())
        .finish_when(HasDiscoveries::AnyOf(BTreeSet::from([property])))
        .spawn_bfs()
        .join()
}

/// The workspace root.
fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("the crate is in the workspace")
        .to_path_buf()
}

/// The text of a file under the workspace root.
fn read(rel: &str) -> String {
    let path = repo_root().join(rel);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// The `(config, invariant)` pair of each red row of `spec` in
/// `formal/tla/models.txt`.
fn tla_red_rows(spec: &str) -> Vec<(String, String)> {
    read("formal/tla/models.txt")
        .lines()
        .filter_map(|line| {
            let cols: Vec<&str> = line.split_whitespace().collect();
            let [row_spec, config, expect] = cols[..] else {
                return None;
            };
            let invariant = expect.strip_prefix("violation:")?;
            (row_spec == spec).then(|| (config.to_string(), invariant.to_string()))
        })
        .collect()
}

/// The lines of a TLC config with `\*` comments removed.
fn cfg_lines(cfg: &str) -> impl Iterator<Item = &str> {
    cfg.lines()
        .map(|l| l.split("\\*").next().unwrap_or_default().trim())
}
