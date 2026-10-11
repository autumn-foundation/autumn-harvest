//! The pre-registered go or no-go criteria (issue #2012).
//!
//! Section 3 of `DESIGN-2012.md` fixed the criteria before the measurement.
//! Section 3.1 added the overlap rule, also before any data. [`judge`]
//! applies both to measured cells, so the verdict is a function of the data
//! and not a judgement made after it.

use super::harness::Contention;
use super::rule::Atomicity;

/// The rule pick counts as right within this share of the best goodput.
pub const TIE_BAND: f64 = 0.10;

/// The measured result of one arm in one cell, over its repetitions.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ArmResult {
    /// The arm.
    pub arm: Atomicity,
    /// The median goodput, in committed runs per second.
    pub goodput: f64,
    /// The lowest goodput of a repetition.
    pub min: f64,
    /// The highest goodput of a repetition.
    pub max: f64,
    /// Every repetition kept both invariants for this arm.
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

/// The value of one criterion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Criterion {
    /// The criterion holds, and the ranges do not overlap where it matters.
    Holds,
    /// The medians agree, but an overlap of ranges leaves the order open.
    Inconclusive,
    /// The criterion fails.
    Fails,
}

impl Criterion {
    /// The worse of two values. A failure outranks an open result.
    #[must_use]
    pub fn and(self, other: Self) -> Self {
        self.max(other)
    }

    const fn from_bool(holds: bool) -> Self {
        if holds { Self::Holds } else { Self::Fails }
    }
}

/// The verdict of the spike.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Every criterion holds.
    Go,
    /// G1 and G4 do not fail, but a criterion does not hold.
    GoWithChanges,
    /// G1 or G4 fails.
    NoGo,
}

/// The value of each criterion and the outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Verdict {
    /// Backout has the highest goodput in every low-contention cell.
    pub g1: Criterion,
    /// Backout does not have the highest goodput in the hot, long-step cell.
    pub g2: Criterion,
    /// The rule pick is within [`TIE_BAND`] of the best goodput in every cell.
    pub g3: Criterion,
    /// Every arm keeps both invariants in every cell.
    pub g4: Criterion,
    /// The outcome.
    pub outcome: Outcome,
}

/// Why [`judge`] refused the cells.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncompleteMatrix(pub String);

/// Apply the criteria to `cells`.
///
/// # Errors
///
/// Returns [`IncompleteMatrix`] unless the cells are exactly the four
/// pre-registered cells, each with one result for each arm.
pub fn judge(cells: &[Cell]) -> Result<Verdict, IncompleteMatrix> {
    check_complete(cells)?;

    let g1 = cells
        .iter()
        .filter(|cell| cell.contention == Contention::Low)
        .map(backout_wins)
        .fold(Criterion::Holds, Criterion::and);
    let g2 = cells
        .iter()
        .filter(|cell| cell.contention == Contention::High && cell.long_steps)
        .map(backout_loses)
        .fold(Criterion::Holds, Criterion::and);
    let g3 = Criterion::from_bool(cells.iter().all(pick_is_near_best));
    let g4 = Criterion::from_bool(
        cells
            .iter()
            .flat_map(|cell| &cell.arms)
            .all(|result| result.invariants_held),
    );

    let outcome = if g1 == Criterion::Fails || g4 == Criterion::Fails {
        Outcome::NoGo
    } else if [g1, g2, g3, g4].iter().all(|c| *c == Criterion::Holds) {
        Outcome::Go
    } else {
        Outcome::GoWithChanges
    };
    Ok(Verdict {
        g1,
        g2,
        g3,
        g4,
        outcome,
    })
}

fn check_complete(cells: &[Cell]) -> Result<(), IncompleteMatrix> {
    if cells.len() != 4 {
        return Err(IncompleteMatrix(format!("{} cells, not 4", cells.len())));
    }
    for contention in [Contention::Low, Contention::High] {
        for long_steps in [false, true] {
            let found = cells
                .iter()
                .filter(|cell| cell.contention == contention && cell.long_steps == long_steps)
                .count();
            if found != 1 {
                return Err(IncompleteMatrix(format!(
                    "{found} cells for {contention:?}, long steps {long_steps}"
                )));
            }
        }
    }
    for cell in cells {
        let complete = cell.arms.len() == Atomicity::ALL.len()
            && Atomicity::ALL
                .iter()
                .all(|arm| cell.arms.iter().filter(|r| r.arm == *arm).count() == 1);
        if !complete {
            return Err(IncompleteMatrix(format!(
                "{:?}, long steps {}: each arm needs exactly one result",
                cell.contention, cell.long_steps
            )));
        }
    }
    Ok(())
}

fn result_of(cell: &Cell, arm: Atomicity) -> &ArmResult {
    cell.arms
        .iter()
        .find(|result| result.arm == arm)
        .unwrap_or_else(|| unreachable!("check_complete found every arm"))
}

fn others(cell: &Cell, arm: Atomicity) -> impl Iterator<Item = &ArmResult> {
    cell.arms.iter().filter(move |result| result.arm != arm)
}

/// G1 in one cell: backout has the best median, and its range clears the rest.
fn backout_wins(cell: &Cell) -> Criterion {
    let backout = result_of(cell, Atomicity::Backout);
    if others(cell, Atomicity::Backout).any(|other| other.goodput > backout.goodput) {
        Criterion::Fails
    } else if others(cell, Atomicity::Backout).any(|other| other.max >= backout.min) {
        Criterion::Inconclusive
    } else {
        Criterion::Holds
    }
}

/// G2 in one cell: another arm has a better median, and its range clears backout.
fn backout_loses(cell: &Cell) -> Criterion {
    let backout = result_of(cell, Atomicity::Backout);
    if others(cell, Atomicity::Backout).all(|other| other.goodput <= backout.goodput) {
        Criterion::Fails
    } else if others(cell, Atomicity::Backout).any(|other| other.min > backout.max) {
        Criterion::Holds
    } else {
        Criterion::Inconclusive
    }
}

/// The picked arm has a median within [`TIE_BAND`] of the best median.
fn pick_is_near_best(cell: &Cell) -> bool {
    let best = cell
        .arms
        .iter()
        .map(|result| result.goodput)
        .fold(f64::NEG_INFINITY, f64::max);
    result_of(cell, cell.pick).goodput >= (1.0 - TIE_BAND) * best
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One arm whose three repetitions all gave `goodput`.
    const fn arm(arm: Atomicity, goodput: f64) -> ArmResult {
        ArmResult {
            arm,
            goodput,
            min: goodput,
            max: goodput,
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

    fn verdict(cells: &[Cell]) -> Verdict {
        judge(cells).expect("a complete matrix")
    }

    #[test]
    fn every_criterion_holds_gives_go() {
        assert_eq!(
            verdict(&passing()),
            Verdict {
                g1: Criterion::Holds,
                g2: Criterion::Holds,
                g3: Criterion::Holds,
                g4: Criterion::Holds,
                outcome: Outcome::Go,
            }
        );
    }

    #[test]
    fn backout_losing_a_cold_cell_gives_no_go() {
        let mut cells = passing();
        cells[1].arms[1] = arm(Atomicity::Saga, 250.0);
        let verdict = verdict(&cells);
        assert_eq!(verdict.g1, Criterion::Fails);
        assert_eq!(verdict.outcome, Outcome::NoGo);
    }

    #[test]
    fn an_overlap_in_a_cold_cell_makes_g1_inconclusive() {
        let mut cells = passing();
        cells[1].arms[0].min = 140.0;
        cells[1].arms[2].max = 160.0;
        let verdict = verdict(&cells);
        assert_eq!(verdict.g1, Criterion::Inconclusive);
        assert_eq!(verdict.outcome, Outcome::GoWithChanges);
    }

    #[test]
    fn ranges_that_only_touch_still_overlap() {
        let mut cells = passing();
        cells[0].arms[0].min = 200.0;
        cells[0].arms[2].max = 200.0;
        assert_eq!(verdict(&cells).g1, Criterion::Inconclusive);
    }

    #[test]
    fn a_broken_invariant_gives_no_go() {
        let mut cells = passing();
        cells[2].arms[2].invariants_held = false;
        let verdict = verdict(&cells);
        assert_eq!(verdict.g4, Criterion::Fails);
        assert_eq!(verdict.outcome, Outcome::NoGo);
    }

    #[test]
    fn backout_winning_the_hot_long_cell_gives_go_with_changes() {
        let mut cells = passing();
        cells[3].arms[0] = arm(Atomicity::Backout, 50.0);
        let verdict = verdict(&cells);
        assert_eq!(verdict.g2, Criterion::Fails);
        assert_eq!(verdict.outcome, Outcome::GoWithChanges);
    }

    #[test]
    fn an_overlap_with_backout_in_the_hot_long_cell_makes_g2_inconclusive() {
        let mut cells = passing();
        cells[3].arms[0].max = 46.0;
        assert_eq!(verdict(&cells).g2, Criterion::Inconclusive);
    }

    #[test]
    fn an_overlap_between_two_other_arms_leaves_g2_holding() {
        let mut cells = passing();
        cells[3].arms[1].max = 46.0;
        assert_eq!(verdict(&cells).g2, Criterion::Holds);
    }

    #[test]
    fn a_pick_inside_the_tie_band_counts_as_right() {
        let mut cells = passing();
        cells[3].pick = Atomicity::Saga;
        assert_eq!(
            verdict(&cells).g3,
            Criterion::Holds,
            "41 is within 10 % of 45"
        );
    }

    #[test]
    fn a_pick_outside_the_tie_band_gives_go_with_changes() {
        let mut cells = passing();
        cells[2].pick = Atomicity::Saga;
        let verdict = verdict(&cells);
        assert_eq!(verdict.g3, Criterion::Fails);
        assert_eq!(verdict.outcome, Outcome::GoWithChanges);
    }

    #[test]
    fn a_missing_cell_is_an_incomplete_matrix() {
        assert!(judge(&[]).is_err());
        for missing in 0..4 {
            let mut cells = passing();
            cells.remove(missing);
            assert!(judge(&cells).is_err(), "cell {missing} is missing");
        }
    }

    #[test]
    fn a_duplicate_cell_is_an_incomplete_matrix() {
        let mut cells = passing();
        cells[1].long_steps = false;
        assert!(judge(&cells).is_err());
    }

    #[test]
    fn a_missing_or_duplicate_arm_is_an_incomplete_matrix() {
        let mut cells = passing();
        cells[0]
            .arms
            .retain(|result| result.arm != Atomicity::Backout);
        assert!(judge(&cells).is_err(), "a missing arm");

        let mut cells = passing();
        cells[0].arms[2] = arm(Atomicity::Saga, 999.0);
        assert!(judge(&cells).is_err(), "a duplicate arm");
    }
}
