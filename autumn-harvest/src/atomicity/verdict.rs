//! The pre-registered go or no-go criteria (issue #2012).
//!
//! Section 3 of `DESIGN-2012.md` fixed the criteria before the measurement.
//! [`judge`] applies them to measured cells, so the verdict is a function of
//! the data and not a judgement made after it.

use super::harness::Contention;
use super::rule::Atomicity;

/// The rule pick counts as right within this share of the best goodput.
pub const TIE_BAND: f64 = 0.10;

/// The measured result of one arm in one cell.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ArmResult {
    /// The arm.
    pub arm: Atomicity,
    /// Committed runs per second.
    pub goodput: f64,
    /// The cell kept both invariants for this arm.
    pub invariants_held: bool,
}

/// One cell of the matrix: one contention level and one step length.
#[derive(Debug, Clone, PartialEq)]
pub struct Cell {
    /// The contention level.
    pub contention: Contention,
    /// The steps take 20 ms, not 0 ms.
    pub long_steps: bool,
    /// One result per arm.
    pub arms: Vec<ArmResult>,
    /// The arm that the rule picks for this cell.
    pub pick: Atomicity,
}

/// The verdict of the spike.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Every criterion holds.
    Go,
    /// G1 and G4 hold, but G2 or G3 fails.
    GoWithChanges,
    /// G1 or G4 fails.
    NoGo,
}

/// The value of each criterion and the outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Verdict {
    /// Backout has the highest goodput in every low-contention cell.
    pub g1: bool,
    /// Backout does not have the highest goodput in the hot, long-step cell.
    pub g2: bool,
    /// The rule pick is within [`TIE_BAND`] of the best goodput in every cell.
    pub g3: bool,
    /// Every arm keeps both invariants in every cell.
    pub g4: bool,
    /// The outcome.
    pub outcome: Outcome,
}

/// Apply the criteria to `cells`.
#[must_use]
pub fn judge(cells: &[Cell]) -> Verdict {
    let low: Vec<&Cell> = cells
        .iter()
        .filter(|cell| cell.contention == Contention::Low)
        .collect();
    let g1 = !low.is_empty() && low.iter().all(|cell| backout_is_best(cell));

    let hot_long: Vec<&Cell> = cells
        .iter()
        .filter(|cell| cell.contention == Contention::High && cell.long_steps)
        .collect();
    let g2 = !hot_long.is_empty() && hot_long.iter().all(|cell| !backout_is_best(cell));

    let g3 = cells.iter().all(pick_is_near_best);
    let g4 = cells
        .iter()
        .flat_map(|cell| &cell.arms)
        .all(|result| result.invariants_held);

    let outcome = if !g1 || !g4 {
        Outcome::NoGo
    } else if g2 && g3 {
        Outcome::Go
    } else {
        Outcome::GoWithChanges
    };
    Verdict {
        g1,
        g2,
        g3,
        g4,
        outcome,
    }
}

fn goodput_of(cell: &Cell, arm: Atomicity) -> Option<f64> {
    cell.arms
        .iter()
        .find(|result| result.arm == arm)
        .map(|result| result.goodput)
}

fn best_goodput(cell: &Cell) -> f64 {
    cell.arms
        .iter()
        .map(|result| result.goodput)
        .fold(f64::NEG_INFINITY, f64::max)
}

/// Backout has a result, and no arm beats it.
fn backout_is_best(cell: &Cell) -> bool {
    goodput_of(cell, Atomicity::Backout).is_some_and(|backout| backout >= best_goodput(cell))
}

/// The picked arm has a result within [`TIE_BAND`] of the best goodput.
fn pick_is_near_best(cell: &Cell) -> bool {
    goodput_of(cell, cell.pick).is_some_and(|pick| pick >= (1.0 - TIE_BAND) * best_goodput(cell))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arm(arm: Atomicity, goodput: f64) -> ArmResult {
        ArmResult {
            arm,
            goodput,
            invariants_held: true,
        }
    }

    fn cell(contention: Contention, long_steps: bool, goodput: [f64; 3], pick: Atomicity) -> Cell {
        Cell {
            contention,
            long_steps,
            arms: vec![
                arm(Atomicity::Backout, goodput[0]),
                arm(Atomicity::Saga, goodput[1]),
                arm(Atomicity::Hybrid, goodput[2]),
            ],
            pick,
        }
    }

    /// Four cells where every criterion holds.
    fn passing() -> Vec<Cell> {
        vec![
            cell(
                Contention::Low,
                false,
                [300.0, 150.0, 200.0],
                Atomicity::Backout,
            ),
            cell(
                Contention::Low,
                true,
                [200.0, 120.0, 150.0],
                Atomicity::Backout,
            ),
            cell(
                Contention::High,
                false,
                [250.0, 100.0, 180.0],
                Atomicity::Backout,
            ),
            cell(
                Contention::High,
                true,
                [20.0, 41.0, 45.0],
                Atomicity::Hybrid,
            ),
        ]
    }

    #[test]
    fn every_criterion_holds_gives_go() {
        let verdict = judge(&passing());
        assert_eq!(
            verdict,
            Verdict {
                g1: true,
                g2: true,
                g3: true,
                g4: true,
                outcome: Outcome::Go,
            }
        );
    }

    #[test]
    fn backout_losing_a_cold_cell_gives_no_go() {
        let mut cells = passing();
        cells[1].arms[1].goodput = 250.0;
        let verdict = judge(&cells);
        assert!(!verdict.g1);
        assert_eq!(verdict.outcome, Outcome::NoGo);
    }

    #[test]
    fn a_broken_invariant_gives_no_go() {
        let mut cells = passing();
        cells[2].arms[2].invariants_held = false;
        let verdict = judge(&cells);
        assert!(!verdict.g4);
        assert_eq!(verdict.outcome, Outcome::NoGo);
    }

    #[test]
    fn backout_winning_the_hot_long_cell_gives_go_with_changes() {
        let mut cells = passing();
        cells[3].arms[0].goodput = 50.0;
        let verdict = judge(&cells);
        assert!(!verdict.g2);
        assert_eq!(verdict.outcome, Outcome::GoWithChanges);
    }

    #[test]
    fn a_pick_inside_the_tie_band_counts_as_right() {
        let mut cells = passing();
        cells[3].pick = Atomicity::Saga;
        assert!(judge(&cells).g3, "41 is within 10 % of 45");
    }

    #[test]
    fn a_pick_outside_the_tie_band_gives_go_with_changes() {
        let mut cells = passing();
        cells[2].pick = Atomicity::Saga;
        let verdict = judge(&cells);
        assert!(!verdict.g3);
        assert_eq!(verdict.outcome, Outcome::GoWithChanges);
    }

    #[test]
    fn a_missing_cell_fails_its_criterion() {
        assert!(!judge(&[]).g1, "G1 needs a low-contention cell");
        assert!(
            !judge(&passing()[..3]).g2,
            "G2 needs the hot, long-step cell"
        );
    }

    #[test]
    fn a_pick_with_no_result_fails_g3() {
        let mut cells = passing();
        cells[0]
            .arms
            .retain(|result| result.arm != Atomicity::Backout);
        assert!(!judge(&cells).g3);
    }
}
