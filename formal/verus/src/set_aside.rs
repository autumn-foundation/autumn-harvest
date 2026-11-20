//! `set_aside` moves a Harvest file and its sidecars aside (issue #2759).
//!
//! The mover in `autumn-harvest-plugin/src/sqlite/replica.rs` renames each
//! sidecar first, and the main file last. A failed rename moves back each
//! moved sidecar, last first. Then the mover refuses.
//!
//! A sidecar at `from` with no main file there is the fault. The next open
//! starts a new file at `from`, and SQLite replays the old frames onto it.
//!
//! This model runs the mover under each fault script. A fault script tells
//! which rename fails. A rename of an absent sidecar that does not fail
//! gives `NotFound`, and nothing moves. The model has no mirror, because the
//! mover does I/O. The proptest `the_mover_keeps_the_files_together` checks
//! the real mover against the same properties.

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
/// `restored` is false when a move back fails. `trace` holds each state on
/// the way, the first and the last included.
pub struct Run {
    pub files: Files,
    pub moved: bool,
    pub restored: bool,
    pub trace: Seq<Files>,
}

/// No sidecar stays at `from` without its main file.
pub open spec fn safe(f: Files) -> bool {
    f.main == Place::From || forall|j: int| 0 <= j < f.sides.len() ==> #[trigger] f.sides[j] != Place::From
}

/// A set-aside starts with the main file at `from`, and nothing at `to`.
/// The aside path is unique.
pub open spec fn start(f: Files) -> bool {
    f.main == Place::From && forall|j: int| 0 <= j < f.sides.len() ==> #[trigger] f.sides[j] != Place::To
}

/// Whether rename `k` works. `fails[k]` is true when it fails. A rename
/// past the end of the script works.
pub open spec fn works(fails: Seq<bool>, k: int) -> bool {
    !(0 <= k < fails.len() && fails[k])
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
/// when it was at `from` in `start`. Rename `k` is next. A failed move
/// back leaves its sidecar aside, and the undo goes on.
pub open spec fn undo(f: Files, start: Seq<Place>, n: int, fails: Seq<bool>, k: int) -> Run
    decreases n,
{
    if n <= 0 {
        Run { files: f, moved: false, restored: true, trace: seq![f] }
    } else if start[n - 1] != Place::From {
        undo(f, start, n - 1, fails, k)
    } else {
        let ok = works(fails, k);
        let g = if ok { put(f, n - 1, Place::From) } else { f };
        let rest = undo(g, start, n - 1, fails, k + 1);
        Run {
            files: rest.files,
            moved: false,
            restored: ok && rest.restored,
            trace: seq![f] + rest.trace,
        }
    }
}

/// The mover from sidecar `i` on. Rename `k` is next. After the last
/// sidecar, the main file moves.
pub open spec fn forward(f: Files, start: Seq<Place>, i: int, fails: Seq<bool>, k: int) -> Run
    decreases f.sides.len() - i,
{
    if i < 0 || i >= f.sides.len() {
        if works(fails, k) {
            let g = Files { main: Place::To, sides: f.sides };
            Run { files: g, moved: true, restored: true, trace: seq![f, g] }
        } else {
            let rest = undo(f, start, f.sides.len() as int, fails, k + 1);
            Run { files: rest.files, moved: false, restored: rest.restored, trace: seq![f] + rest.trace }
        }
    } else if !works(fails, k) {
        let rest = undo(f, start, i, fails, k + 1);
        Run { files: rest.files, moved: false, restored: rest.restored, trace: seq![f] + rest.trace }
    } else {
        let g = if f.sides[i] == Place::From { put(f, i, Place::To) } else { f };
        let rest = forward(g, start, i + 1, fails, k + 1);
        Run { files: rest.files, moved: rest.moved, restored: rest.restored, trace: seq![f] + rest.trace }
    }
}

/// The mover on `f` under the fault script `fails`.
pub open spec fn set_aside(f: Files, fails: Seq<bool>) -> Run {
    forward(f, f.sides, 0, fails, 0)
}

/// The state of the mover before sidecar `i`.
pub open spec fn partway(start: Seq<Place>, f: Files, i: int) -> bool {
    &&& f.main == Place::From
    &&& 0 <= i <= start.len()
    &&& f.sides.len() == start.len()
    &&& forall|j: int| 0 <= j < i ==> #[trigger] f.sides[j] == moved_to(start[j])
    &&& forall|j: int| i <= j < start.len() ==> #[trigger] f.sides[j] == start[j]
}

proof fn lemma_undo(f: Files, start: Seq<Place>, n: int, fails: Seq<bool>, k: int)
    requires
        0 <= n <= f.sides.len(),
        f.sides.len() == start.len(),
    ensures
        ({
            let r = undo(f, start, n, fails, k);
            &&& r.files.main == f.main
            &&& !r.moved
            &&& r.files.sides.len() == f.sides.len()
            &&& r.trace.len() >= 1
            &&& forall|t: int| 0 <= t < r.trace.len() ==> (#[trigger] r.trace[t]).main == f.main
            &&& forall|j: int| 0 <= j < f.sides.len() && !(j < n && start[j] == Place::From)
                ==> #[trigger] r.files.sides[j] == f.sides[j]
            &&& r.restored ==> forall|j: int| 0 <= j < n && start[j] == Place::From
                ==> #[trigger] r.files.sides[j] == Place::From
        }),
    decreases n,
{
    if n <= 0 {
    } else if start[n - 1] != Place::From {
        lemma_undo(f, start, n - 1, fails, k);
    } else {
        let ok = works(fails, k);
        let g = if ok { put(f, n - 1, Place::From) } else { f };
        lemma_undo(g, start, n - 1, fails, k + 1);
        let rest = undo(g, start, n - 1, fails, k + 1);
        let r = undo(f, start, n, fails, k);
        assert forall|t: int| 0 <= t < r.trace.len() implies (#[trigger] r.trace[t]).main == f.main by {
            if t > 0 {
                assert(r.trace[t] == rest.trace[t - 1]);
            }
        }
    }
}

proof fn lemma_forward(f: Files, start: Seq<Place>, i: int, fails: Seq<bool>, k: int)
    requires
        partway(start, f, i),
        forall|j: int| 0 <= j < start.len() ==> #[trigger] start[j] != Place::To,
    ensures
        ({
            let r = forward(f, start, i, fails, k);
            &&& r.trace.len() >= 1
            &&& forall|t: int| 0 <= t < r.trace.len() ==> safe(#[trigger] r.trace[t])
            &&& r.files.sides.len() == start.len()
            &&& r.moved ==> r.files.main == Place::To
            &&& r.moved ==> forall|j: int| 0 <= j < start.len()
                ==> #[trigger] r.files.sides[j] == moved_to(start[j])
            &&& !r.moved ==> r.files.main == Place::From
            &&& (!r.moved && r.restored) ==> r.files.sides =~= start
            &&& (forall|m: int| k <= m ==> #[trigger] works(fails, m)) ==> r.moved
        }),
    decreases f.sides.len() - i,
{
    let r = forward(f, start, i, fails, k);
    if i >= f.sides.len() {
        if works(fails, k) {
            let g = Files { main: Place::To, sides: f.sides };
            assert(safe(g));
            assert forall|t: int| 0 <= t < r.trace.len() implies safe(#[trigger] r.trace[t]) by {
                if t == 1 {
                    assert(r.trace[t] == g);
                }
            }
        } else {
            lemma_undo(f, start, f.sides.len() as int, fails, k + 1);
            let rest = undo(f, start, f.sides.len() as int, fails, k + 1);
            assert forall|t: int| 0 <= t < r.trace.len() implies safe(#[trigger] r.trace[t]) by {
                if t > 0 {
                    assert(r.trace[t] == rest.trace[t - 1]);
                }
            }
        }
    } else if !works(fails, k) {
        lemma_undo(f, start, i, fails, k + 1);
        let rest = undo(f, start, i, fails, k + 1);
        assert forall|t: int| 0 <= t < r.trace.len() implies safe(#[trigger] r.trace[t]) by {
            if t > 0 {
                assert(r.trace[t] == rest.trace[t - 1]);
            }
        }
    } else {
        let g = if f.sides[i] == Place::From { put(f, i, Place::To) } else { f };
        assert(partway(start, g, i + 1));
        lemma_forward(g, start, i + 1, fails, k + 1);
        let rest = forward(g, start, i + 1, fails, k + 1);
        assert forall|t: int| 0 <= t < r.trace.len() implies safe(#[trigger] r.trace[t]) by {
            if t > 0 {
                assert(r.trace[t] == rest.trace[t - 1]);
            }
        }
        if forall|m: int| k <= m ==> #[trigger] works(fails, m) {
            assert(forall|m: int| k + 1 <= m ==> #[trigger] works(fails, m));
        }
    }
}

/// At each step, a sidecar at `from` has its main file at `from`. So no
/// open starts a new file beside an old sidecar.
pub proof fn theorem_no_sidecar_stays_without_its_file(f: Files, fails: Seq<bool>)
    requires
        start(f),
    ensures
        forall|t: int| 0 <= t < set_aside(f, fails).trace.len()
            ==> safe(#[trigger] set_aside(f, fails).trace[t]),
{
    assert(partway(f.sides, f, 0));
    lemma_forward(f, f.sides, 0, fails, 0);
}

/// A success puts the main file and each present sidecar at `to`.
pub proof fn theorem_a_success_moves_each_file(f: Files, fails: Seq<bool>)
    requires
        start(f),
        set_aside(f, fails).moved,
    ensures
        set_aside(f, fails).files.main == Place::To,
        forall|j: int| 0 <= j < f.sides.len()
            ==> #[trigger] set_aside(f, fails).files.sides[j] == moved_to(f.sides[j]),
{
    assert(partway(f.sides, f, 0));
    lemma_forward(f, f.sides, 0, fails, 0);
}

/// A failure keeps the main file at `from`, even when a move back fails.
pub proof fn theorem_a_failure_keeps_the_main_file(f: Files, fails: Seq<bool>)
    requires
        start(f),
        !set_aside(f, fails).moved,
    ensures
        set_aside(f, fails).files.main == Place::From,
{
    assert(partway(f.sides, f, 0));
    lemma_forward(f, f.sides, 0, fails, 0);
}

/// A failure with no failed move back puts each file back where it was.
/// So the main file keeps its WAL.
pub proof fn theorem_a_clean_failure_restores_each_file(f: Files, fails: Seq<bool>)
    requires
        start(f),
        !set_aside(f, fails).moved,
        set_aside(f, fails).restored,
    ensures
        set_aside(f, fails).files.main == f.main,
        set_aside(f, fails).files.sides =~= f.sides,
{
    assert(partway(f.sides, f, 0));
    lemma_forward(f, f.sides, 0, fails, 0);
}

/// With no failed rename, the mover succeeds. So the model does not prove
/// the properties above by always refusing.
pub proof fn theorem_a_clean_run_moves(f: Files, fails: Seq<bool>)
    requires
        start(f),
        forall|m: int| 0 <= m ==> #[trigger] works(fails, m),
    ensures
        set_aside(f, fails).moved,
{
    assert(partway(f.sides, f, 0));
    lemma_forward(f, f.sides, 0, fails, 0);
}

} // verus!
