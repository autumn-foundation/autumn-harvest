//! `set_aside` moves a Harvest file and its sidecars aside (issue #2759).
//!
//! `move_aside` in `autumn-harvest-plugin/src/sqlite/replica.rs` renames
//! each sidecar first, and the main file last. A failed rename moves back
//! each moved sidecar. Then the mover refuses.
//!
//! A sidecar at `from` with no main file there is the fault. The next open
//! starts a new file at `from`, and SQLite replays the old frames onto it.
//!
//! This model runs the mover under each fault script. `fails` tells which
//! forward rename fails, in call order. `back` tells which sidecar cannot
//! move back. A rename of an absent sidecar that does not fail gives
//! `NotFound`, and nothing moves. The main file can be absent. Its stray
//! sidecars then go aside, and the mover sends no rename for the main file.
//!
//! The model has no mirror, because the mover does I/O. The proptest
//! `the_mover_keeps_the_files_together` checks the real mover against the
//! same outcome.

use vstd::prelude::*;

verus! {

/// Where a file is.
pub enum Place {
    /// No file.
    Absent,
    /// At the path of the Harvest file.
    From,
    /// At the aside path.
    To,
}

/// The main file and its sidecars.
pub struct Files {
    pub main: Place,
    pub sides: Seq<Place>,
}

/// The end of a set-aside. `moved` is true when the mover succeeds.
/// `trace` holds each state on the way, the first and the last included.
pub struct Run {
    pub files: Files,
    pub moved: bool,
    pub trace: Seq<Files>,
}

/// No sidecar stays at `from` without its main file.
pub open spec fn safe(f: Files) -> bool {
    f.main == Place::From || forall|j: int| 0 <= j < f.sides.len() ==> #[trigger] f.sides[j] != Place::From
}

/// A set-aside starts with nothing at `to`. The aside path is free.
pub open spec fn start(f: Files) -> bool {
    f.main != Place::To && forall|j: int| 0 <= j < f.sides.len() ==> #[trigger] f.sides[j] != Place::To
}

/// Whether `script[k]` names a fault. An index past the end names none.
pub open spec fn fails_at(script: Seq<bool>, k: int) -> bool {
    0 <= k < script.len() && script[k]
}

/// Where a file goes when its move works.
pub open spec fn moved_to(p: Place) -> Place {
    if p == Place::From { Place::To } else { p }
}

/// `f` with sidecar `j` at `p`.
pub open spec fn put(f: Files, j: int, p: Place) -> Files {
    Files { main: f.main, sides: f.sides.update(j, p) }
}

/// Move back each moved sidecar below `n`, last first. A sidecar was moved
/// when it was at `from` in `start`. A failed move back leaves its sidecar
/// aside, and the undo goes on.
pub open spec fn undo(f: Files, start: Seq<Place>, n: int, back: Seq<bool>) -> Run
    decreases n,
{
    if n <= 0 {
        Run { files: f, moved: false, trace: seq![f] }
    } else if start[n - 1] != Place::From {
        undo(f, start, n - 1, back)
    } else {
        let g = if fails_at(back, n - 1) { f } else { put(f, n - 1, Place::From) };
        let rest = undo(g, start, n - 1, back);
        Run { files: rest.files, moved: false, trace: seq![f] + rest.trace }
    }
}

/// The mover from sidecar `i` on. Forward rename `k` is next. After the
/// last sidecar, the main file moves, when it is present.
pub open spec fn forward(
    f: Files,
    start: Seq<Place>,
    i: int,
    fails: Seq<bool>,
    back: Seq<bool>,
    k: int,
) -> Run
    decreases f.sides.len() - i,
{
    if i < 0 || i >= f.sides.len() {
        if f.main == Place::Absent || !fails_at(fails, k) {
            let g = Files { main: moved_to(f.main), sides: f.sides };
            Run { files: g, moved: true, trace: seq![f, g] }
        } else {
            let rest = undo(f, start, f.sides.len() as int, back);
            Run { files: rest.files, moved: false, trace: seq![f] + rest.trace }
        }
    } else if fails_at(fails, k) {
        let rest = undo(f, start, i, back);
        Run { files: rest.files, moved: false, trace: seq![f] + rest.trace }
    } else {
        let g = if f.sides[i] == Place::From { put(f, i, Place::To) } else { f };
        let rest = forward(g, start, i + 1, fails, back, k + 1);
        Run { files: rest.files, moved: rest.moved, trace: seq![f] + rest.trace }
    }
}

/// The mover on `f` under the fault scripts `fails` and `back`.
pub open spec fn set_aside(f: Files, fails: Seq<bool>, back: Seq<bool>) -> Run {
    forward(f, f.sides, 0, fails, back, 0)
}

/// The state of the mover before sidecar `i`.
pub open spec fn partway(start: Seq<Place>, f: Files, i: int) -> bool {
    &&& f.main != Place::To
    &&& 0 <= i <= start.len()
    &&& f.sides.len() == start.len()
    &&& forall|j: int| 0 <= j < i ==> #[trigger] f.sides[j] == moved_to(start[j])
    &&& forall|j: int| i <= j < start.len() ==> #[trigger] f.sides[j] == start[j]
}

/// Where sidecar `j` ends after a failure: back where it was, unless its
/// own move back failed.
pub open spec fn kept(start: Seq<Place>, back: Seq<bool>, end: Seq<Place>, j: int) -> bool {
    end[j] == start[j] || (start[j] == Place::From && fails_at(back, j) && end[j] == Place::To)
}

proof fn lemma_undo(f: Files, start: Seq<Place>, n: int, back: Seq<bool>)
    requires
        0 <= n <= f.sides.len(),
        f.sides.len() == start.len(),
    ensures
        ({
            let r = undo(f, start, n, back);
            &&& r.files.main == f.main
            &&& !r.moved
            &&& r.files.sides.len() == f.sides.len()
            &&& r.trace.len() >= 1
            &&& forall|t: int| 0 <= t < r.trace.len() ==> (#[trigger] r.trace[t]).main == f.main
            &&& forall|j: int| 0 <= j < f.sides.len() && !(j < n && start[j] == Place::From)
                ==> #[trigger] r.files.sides[j] == f.sides[j]
            &&& forall|j: int| 0 <= j < n && start[j] == Place::From
                ==> #[trigger] r.files.sides[j] == (if fails_at(back, j) { f.sides[j] } else { Place::From })
        }),
    decreases n,
{
    if n <= 0 {
    } else if start[n - 1] != Place::From {
        lemma_undo(f, start, n - 1, back);
    } else {
        let g = if fails_at(back, n - 1) { f } else { put(f, n - 1, Place::From) };
        lemma_undo(g, start, n - 1, back);
        let rest = undo(g, start, n - 1, back);
        let r = undo(f, start, n, back);
        assert forall|t: int| 0 <= t < r.trace.len() implies (#[trigger] r.trace[t]).main == f.main by {
            if t > 0 {
                assert(r.trace[t] == rest.trace[t - 1]);
            }
        }
    }
}

/// The trace `r` is safe when its head `f` is safe and its tail is safe.
proof fn lemma_safe_trace(f: Files, rest: Seq<Files>, r: Seq<Files>)
    requires
        safe(f),
        r == seq![f] + rest,
        forall|t: int| 0 <= t < rest.len() ==> safe(#[trigger] rest[t]),
    ensures
        forall|t: int| 0 <= t < r.len() ==> safe(#[trigger] r[t]),
{
    assert forall|t: int| 0 <= t < r.len() implies safe(#[trigger] r[t]) by {
        if t > 0 {
            assert(r[t] == rest[t - 1]);
        }
    }
}

proof fn lemma_forward(
    f: Files,
    start: Seq<Place>,
    i: int,
    fails: Seq<bool>,
    back: Seq<bool>,
    k: int,
)
    requires
        partway(start, f, i),
        forall|j: int| 0 <= j < start.len() ==> #[trigger] start[j] != Place::To,
    ensures
        ({
            let r = forward(f, start, i, fails, back, k);
            &&& r.trace.len() >= 1
            &&& f.main == Place::From ==> forall|t: int| 0 <= t < r.trace.len()
                ==> safe(#[trigger] r.trace[t])
            &&& r.moved ==> safe(r.files)
            &&& r.files.sides.len() == start.len()
            &&& r.moved ==> r.files.main == moved_to(f.main)
            &&& r.moved ==> forall|j: int| 0 <= j < start.len()
                ==> #[trigger] r.files.sides[j] == moved_to(start[j])
            &&& !r.moved ==> r.files.main == f.main
            &&& !r.moved ==> forall|j: int| 0 <= j < start.len()
                ==> #[trigger] kept(start, back, r.files.sides, j)
            &&& (forall|m: int| k <= m ==> !#[trigger] fails_at(fails, m)) ==> r.moved
        }),
    decreases f.sides.len() - i,
{
    let r = forward(f, start, i, fails, back, k);
    if i >= f.sides.len() {
        if f.main == Place::Absent || !fails_at(fails, k) {
            let g = Files { main: moved_to(f.main), sides: f.sides };
            assert(safe(g));
            if f.main == Place::From {
                assert(r.trace =~= seq![f] + seq![g]);
                lemma_safe_trace(f, seq![g], r.trace);
            }
        } else {
            lemma_undo(f, start, f.sides.len() as int, back);
            let rest = undo(f, start, f.sides.len() as int, back);
            if f.main == Place::From {
                lemma_safe_trace(f, rest.trace, r.trace);
            }
        }
    } else if fails_at(fails, k) {
        lemma_undo(f, start, i, back);
        let rest = undo(f, start, i, back);
        if f.main == Place::From {
            lemma_safe_trace(f, rest.trace, r.trace);
        }
    } else {
        let g = if f.sides[i] == Place::From { put(f, i, Place::To) } else { f };
        assert(partway(start, g, i + 1));
        lemma_forward(g, start, i + 1, fails, back, k + 1);
        let rest = forward(g, start, i + 1, fails, back, k + 1);
        if f.main == Place::From {
            lemma_safe_trace(f, rest.trace, r.trace);
        }
        if forall|m: int| k <= m ==> !#[trigger] fails_at(fails, m) {
            assert(forall|m: int| k + 1 <= m ==> !#[trigger] fails_at(fails, m));
        }
    }
}

/// At each step, a sidecar at `from` has its main file at `from`. So no
/// open starts a new file beside an old sidecar.
pub proof fn theorem_no_sidecar_stays_without_its_file(f: Files, fails: Seq<bool>, back: Seq<bool>)
    requires
        start(f),
        f.main == Place::From,
    ensures
        forall|t: int| 0 <= t < set_aside(f, fails, back).trace.len()
            ==> safe(#[trigger] set_aside(f, fails, back).trace[t]),
{
    assert(partway(f.sides, f, 0));
    lemma_forward(f, f.sides, 0, fails, back, 0);
}

/// A success puts the main file and each present sidecar at `to`. No
/// sidecar stays at `from`, also when the main file was absent.
pub proof fn theorem_a_success_moves_each_file(f: Files, fails: Seq<bool>, back: Seq<bool>)
    requires
        start(f),
        set_aside(f, fails, back).moved,
    ensures
        set_aside(f, fails, back).files.main == moved_to(f.main),
        safe(set_aside(f, fails, back).files),
        forall|j: int| 0 <= j < f.sides.len()
            ==> #[trigger] set_aside(f, fails, back).files.sides[j] == moved_to(f.sides[j]),
{
    assert(partway(f.sides, f, 0));
    lemma_forward(f, f.sides, 0, fails, back, 0);
}

/// A failure keeps the main file where it was. Each sidecar is back where
/// it was, unless its own move back failed. So the undo goes on after a
/// failed move back.
pub proof fn theorem_a_failure_keeps_the_files(f: Files, fails: Seq<bool>, back: Seq<bool>)
    requires
        start(f),
        !set_aside(f, fails, back).moved,
    ensures
        set_aside(f, fails, back).files.main == f.main,
        forall|j: int| 0 <= j < f.sides.len()
            ==> #[trigger] kept(f.sides, back, set_aside(f, fails, back).files.sides, j),
{
    assert(partway(f.sides, f, 0));
    lemma_forward(f, f.sides, 0, fails, back, 0);
}

/// A failure with no failed move back puts each file back where it was.
/// So the main file keeps its WAL.
pub proof fn theorem_a_clean_failure_restores_each_file(f: Files, fails: Seq<bool>, back: Seq<bool>)
    requires
        start(f),
        !set_aside(f, fails, back).moved,
        forall|j: int| 0 <= j < f.sides.len() ==> !#[trigger] fails_at(back, j),
    ensures
        set_aside(f, fails, back).files.main == f.main,
        set_aside(f, fails, back).files.sides =~= f.sides,
{
    assert(partway(f.sides, f, 0));
    lemma_forward(f, f.sides, 0, fails, back, 0);
    let end = set_aside(f, fails, back).files.sides;
    assert forall|j: int| 0 <= j < f.sides.len() implies #[trigger] end[j] == f.sides[j] by {
        assert(kept(f.sides, back, end, j));
    }
}

/// With no failed forward rename, the mover succeeds. So the model does not
/// prove the properties above by always refusing.
pub proof fn theorem_a_clean_run_moves(f: Files, fails: Seq<bool>, back: Seq<bool>)
    requires
        start(f),
        forall|m: int| 0 <= m ==> !#[trigger] fails_at(fails, m),
    ensures
        set_aside(f, fails, back).moved,
{
    assert(partway(f.sides, f, 0));
    lemma_forward(f, f.sides, 0, fails, back, 0);
}

} // verus!
