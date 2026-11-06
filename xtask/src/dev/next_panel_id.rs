//! `cargo xtask dev next-panel-id`: the port of
//! `docs/dashboards/next-panel-id.py`.
//!
//! It prints a free Grafana panel id for `starter-pack-v0.1.0.json`.
//!
//! To hand-increment near the current maximum caused five duplicate-id
//! collisions between PRs drafted at the same time (ids 956 to 960). Two
//! branches that each read the same maximum and add one land on the same id.
//! The JSON merges cleanly either way. So nothing conflicts until
//! `panel_structure_is_grafana10_clean` runs against the merged file. A draw
//! from a wide, mostly empty range makes that collision astronomically
//! unlikely, not near-certain.
//!
//! Usage: `cargo xtask dev next-panel-id`.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::Value;
use smop::prelude::*;

use crate::util::read_repo_file;

/// Arguments of `cargo xtask dev next-panel-id`.
#[derive(Debug, Clone, Copy, Args)]
pub struct Args {}

const DASHBOARD: &str = "docs/dashboards/starter-pack-v0.1.0.json";
const ID_LOW: u64 = 100_000;
const ID_HIGH: u64 = 999_999;
const TRIES: usize = 1000;

/// Returns the numeric value of each `id` in `panels` and their nested panels.
///
/// A panel id that is not a number can never equal a drawn id, as in the
/// Python original. So the set skips it.
fn used_ids(panels: &[Value], ids: &mut Vec<f64>) {
    for panel in panels {
        if let Some(id) = panel.get("id").and_then(Value::as_f64) {
            ids.push(id);
        }
        if let Some(nested) = panel.get("panels").and_then(Value::as_array) {
            used_ids(nested, ids);
        }
    }
}

/// Returns a fresh random number from the standard library hasher seed.
fn random_u64(counter: u64) -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let mut hasher = RandomState::new().build_hasher();
    hasher.write_u64(counter);
    hasher.write_u128(nanos);
    hasher.finish()
}

/// Draws up to `TRIES` ids in the range and returns the first free one.
fn pick_free_id(taken: &[f64], mut draw: impl FnMut() -> u64) -> Option<u64> {
    let span = ID_HIGH - ID_LOW + 1;
    for _ in 0..TRIES {
        let candidate = ID_LOW + draw() % span;
        // Each id in the range is far below 2^53, so the conversion is exact.
        #[allow(clippy::cast_precision_loss)]
        let as_float = candidate as f64;
        if !taken.contains(&as_float) {
            return Some(candidate);
        }
    }
    None
}

/// Runs `cargo xtask dev next-panel-id`.
pub fn run(args: Args) -> Result<ExitCode> {
    let Args {} = args;
    let dashboard: Value = serde_json::from_str(&read_repo_file(DASHBOARD)?)
        .with_context(|| format!("parsing {DASHBOARD}"))?;
    let mut taken = Vec::new();
    if let Some(panels) = dashboard.get("panels").and_then(Value::as_array) {
        used_ids(panels, &mut taken);
    }
    let mut counter = 0u64;
    let picked = pick_free_id(&taken, || {
        counter += 1;
        random_u64(counter)
    });
    if let Some(id) = picked {
        println!("{id}");
        return Ok(ExitCode::SUCCESS);
    }
    eprintln!("could not find a free id after {TRIES} tries");
    Ok(ExitCode::FAILURE)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nested_ids_are_collected() {
        let panels: Value = serde_json::json!([
            {"id": 1, "panels": [{"id": 2}, {"title": "no id"}]},
            {"id": "text"},
            {"id": 3.0}
        ]);
        let mut ids = Vec::new();
        used_ids(panels.as_array().unwrap(), &mut ids);
        assert_eq!(ids, [1.0, 2.0, 3.0]);
    }

    #[test]
    fn a_taken_id_is_skipped() {
        let mut draws = [0u64, 0, 5].into_iter();
        let picked = pick_free_id(&[100_000.0], || draws.next().unwrap());
        assert_eq!(picked, Some(100_005));
    }

    #[test]
    fn an_exhausted_draw_fails() {
        assert_eq!(pick_free_id(&[100_000.0], || 0), None);
    }

    #[test]
    fn a_draw_stays_in_range() {
        let picked = pick_free_id(&[], || u64::MAX).unwrap();
        assert!((ID_LOW..=ID_HIGH).contains(&picked));
        let real = pick_free_id(&[], || random_u64(7)).unwrap();
        assert!((ID_LOW..=ID_HIGH).contains(&real));
    }
}
