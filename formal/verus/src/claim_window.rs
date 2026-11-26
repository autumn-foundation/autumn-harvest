//! The split claim takes the row that the one full statement takes.
//!
//! Each scan of the full statement is an input: the row it would take if it
//! ran, and the tests of its gate. The copy below proves the kernel in
//! `autumn-harvest/src/queue.rs`.
//!
//! The lean form is a smaller statement in front of the window form. It
//! holds no gate of a concurrency key, a rate limit, a build pin or a
//! capability list. It takes a row only when each window row is plain: those
//! four columns are NULL. Each dropped gate starts with the NULL test of its
//! column, so a plain row passes it. In every other case the lean form locks
//! no row and sets its flag. The window form then runs as a fresh attempt.
//! So the pair takes the row of the window form.
//!
//! A claim loop skips the lean form for some claims after a fallback. The
//! skip state is a pure state machine. The skip selects the form that runs
//! first. Both forms are exact, so the skip never changes the row.

use vstd::prelude::*;

verus! {

pub enum WindowStep {
    Claimed,
    Empty,
    Fallback,
}

pub open spec fn spec_window_step(claimed: bool, fallback: bool) -> WindowStep {
    if claimed {
        WindowStep::Claimed
    } else if fallback {
        WindowStep::Fallback
    } else {
        WindowStep::Empty
    }
}

// mirror: autumn-harvest/src/queue.rs window_step
pub fn window_step(claimed: bool, fallback: bool) -> (r: WindowStep)
    ensures
        r == spec_window_step(claimed, fallback),
        r is Claimed <==> claimed,
        r is Fallback <==> !claimed && fallback,
        r is Empty <==> !claimed && !fallback,
{
    if claimed {
        WindowStep::Claimed
    } else if fallback {
        WindowStep::Fallback
    } else {
        WindowStep::Empty
    }
}

/// One snapshot of a claim. Each `Option` is the row that a scan takes if
/// it runs. `None` is a scan that finds no free row.
pub struct Snapshot {
    /// `seek_candidate`.
    pub window: Option<int>,
    /// `seek_deep_candidate`, without its gate.
    pub deep: Option<int>,
    /// `legacy_candidate`, without its gate.
    pub full: Option<int>,
    /// `seek_contended` holds a row.
    pub contended: bool,
    /// `seek_bounds` holds a row.
    pub bounds: bool,
    /// `seek_deep_bounds` holds a row.
    pub deep_bounds: bool,
    pub ageing: bool,
}

/// The deep scan of the full statement, behind its gate.
pub open spec fn deep_candidate(s: Snapshot) -> Option<int> {
    if s.window is None && s.contended {
        s.deep
    } else {
        None
    }
}

pub open spec fn full_statement(s: Snapshot) -> Option<int> {
    if s.window is Some {
        s.window
    } else if deep_candidate(s) is Some {
        deep_candidate(s)
    } else if (s.bounds && (!s.contended || s.deep_bounds)) || s.ageing {
        s.full
    } else {
        None
    }
}

pub open spec fn window_flag(s: Snapshot) -> bool {
    s.window is None && (s.contended || s.bounds || s.ageing)
}

/// The row that the split claim takes. The window form reads `first`. The
/// full statement, when it runs, reads `second`, a later snapshot.
pub open spec fn split_claim(first: Snapshot, second: Snapshot) -> Option<int> {
    match spec_window_step(first.window is Some, window_flag(first)) {
        WindowStep::Claimed => first.window,
        WindowStep::Empty => None,
        WindowStep::Fallback => full_statement(second),
    }
}

/// A clear flag with no window row proves that the full statement takes
/// no row from the same snapshot.
proof fn lemma_a_clear_flag_means_no_row(s: Snapshot)
    requires
        s.window is None,
        !window_flag(s),
    ensures
        full_statement(s) is None,
{
}

/// A set flag means that the window form took no row. So the full
/// statement after it is a fresh claim attempt.
proof fn lemma_a_set_flag_took_no_row(s: Snapshot)
    requires
        window_flag(s),
    ensures
        s.window is None,
{
}

/// The split claim takes the row of the one full statement when both read
/// the same snapshot. With a later snapshot, it takes the row of a full
/// statement that starts at that later point.
pub proof fn theorem_the_split_claim_takes_the_full_statement_row(
    first: Snapshot,
    second: Snapshot,
)
    ensures
        split_claim(first, first) == full_statement(first),
        window_flag(first) ==> split_claim(first, second) == full_statement(second),
        !window_flag(first) ==> split_claim(first, second) == full_statement(first),
{
    if first.window is None && !window_flag(first) {
        lemma_a_clear_flag_means_no_row(first);
    }
    if window_flag(first) {
        lemma_a_set_flag_took_no_row(first);
    }
}

/// One row of the seek window, as one claim reads it.
pub struct WindowRow {
    pub id: int,
    /// The concurrency key, the rate-limit key, the build pin and the
    /// capability list of the row are NULL.
    pub plain: bool,
    /// The row passes each gate that both forms hold, and no peer holds it.
    pub kept_gates: bool,
    /// The row passes each gate that only the window form holds.
    pub dropped_gates: bool,
    /// The run of the row is live at the clock read after the row lock.
    pub run_live: bool,
    /// The key lock, the cap re-check and the debit test of `claimed` pass.
    pub key_tests: bool,
}

/// One snapshot of the seek window. The rows are in claim order.
pub struct Window {
    pub rows: Seq<WindowRow>,
    /// `seek_contended` holds a row.
    pub contended: bool,
    /// `seek_bounds` holds a row.
    pub bounds: bool,
    pub ageing: bool,
}

/// What the statement text gives for each snapshot.
///
/// Each dropped gate and each key test starts with the NULL test of its
/// column, so a plain row passes them. `seek_contended` and `seek_bounds`
/// read window rows only, so an empty window leaves both empty.
pub open spec fn well_formed(w: Window) -> bool {
    &&& forall|i: int|
        0 <= i < w.rows.len() && (#[trigger] w.rows[i]).plain ==> w.rows[i].dropped_gates
            && w.rows[i].key_tests
    &&& w.rows.len() == 0 ==> !w.contended && !w.bounds
}

pub open spec fn all_plain(rows: Seq<WindowRow>) -> bool {
    forall|i: int| 0 <= i < rows.len() ==> (#[trigger] rows[i]).plain
}

/// The row gates of `seek_candidate` in one form. The lean form holds the
/// NULL tests in place of the dropped gates.
pub open spec fn passes(r: WindowRow, lean: bool) -> bool {
    r.kept_gates && if lean {
        r.plain
    } else {
        r.dropped_gates
    }
}

/// The first row in claim order that passes the gates of one form.
pub open spec fn first_row(rows: Seq<WindowRow>, lean: bool) -> Option<WindowRow>
    decreases rows.len(),
{
    if rows.len() == 0 {
        None
    } else if passes(rows[0], lean) {
        Some(rows[0])
    } else {
        first_row(rows.drop_first(), lean)
    }
}

/// What one claim form returns to the claim.
pub enum Outcome {
    /// The form claimed the row with this id.
    Claimed(int),
    /// The form claimed no row, and no other form can.
    Empty,
    /// The form claimed no row and asks for the next form.
    Fallback,
}

/// The result of one statement: its claimed row and its flag.
pub open spec fn outcome(row: Option<int>, flag: bool) -> Outcome {
    match row {
        Some(id) => Outcome::Claimed(id),
        None => if flag {
            Outcome::Fallback
        } else {
            Outcome::Empty
        },
    }
}

/// `seek_candidate` of the window form.
pub open spec fn window_candidate(w: Window) -> Option<WindowRow> {
    first_row(w.rows, false)
}

/// The window form. `claimed` takes the candidate when its run is live and
/// its key tests pass. The flag is clear when a candidate exists.
pub open spec fn window_form(w: Window) -> Outcome {
    match window_candidate(w) {
        Some(c) => outcome(
            if c.run_live && c.key_tests {
                Some(c.id)
            } else {
                None
            },
            false,
        ),
        None => outcome(None, w.contended || w.bounds || w.ageing),
    }
}

/// `seek_candidate` of the lean form. Its one-time gate lets no row pass
/// when a window row is not plain. So the form locks no row then.
pub open spec fn lean_candidate(w: Window) -> Option<WindowRow> {
    if all_plain(w.rows) {
        first_row(w.rows, true)
    } else {
        None
    }
}

/// The lean form. `claimed` takes the candidate when its run is live. The
/// flag is set when no candidate exists and the window holds a row, or
/// ageing is on.
pub open spec fn lean_form(w: Window) -> Outcome {
    match lean_candidate(w) {
        Some(c) => outcome(
            if c.run_live {
                Some(c.id)
            } else {
                None
            },
            false,
        ),
        None => outcome(None, w.rows.len() > 0 || w.ageing),
    }
}

/// The lean form, then the window form when the flag asks for it. The lean
/// form reads `first`. The window form, when it runs, reads `second`, a
/// later snapshot.
pub open spec fn lean_then_window(first: Window, second: Window) -> Outcome {
    match lean_form(first) {
        Outcome::Fallback => window_form(second),
        taken => taken,
    }
}

/// The decoded result is the step that `window_step` gives for the same
/// row and flag. So the lean form needs no decoder of its own.
proof fn lemma_an_outcome_is_its_window_step(row: Option<int>, flag: bool)
    ensures
        outcome(row, flag) is Claimed <==> spec_window_step(row is Some, flag) is Claimed,
        outcome(row, flag) is Fallback <==> spec_window_step(row is Some, flag) is Fallback,
        outcome(row, flag) is Empty <==> spec_window_step(row is Some, flag) is Empty,
{
}

/// The first row is a window row that passes the gates. With no first row,
/// no window row passes them.
proof fn lemma_the_first_row_is_a_window_row(rows: Seq<WindowRow>, lean: bool)
    ensures
        match first_row(rows, lean) {
            Some(c) => exists|i: int| 0 <= i < rows.len() && #[trigger] rows[i] == c && passes(c, lean),
            None => forall|i: int| 0 <= i < rows.len() ==> !passes(#[trigger] rows[i], lean),
        },
    decreases rows.len(),
{
    if rows.len() > 0 && !passes(rows[0], lean) {
        let rest = rows.drop_first();
        lemma_the_first_row_is_a_window_row(rest, lean);
        match first_row(rest, lean) {
            Some(c) => {
                let i = choose|i: int| 0 <= i < rest.len() && #[trigger] rest[i] == c && passes(c, lean);
                assert(rows[i + 1] == c);
            },
            None => {
                assert forall|i: int| 0 <= i < rows.len() implies !passes(#[trigger] rows[i], lean) by {
                    if i > 0 {
                        assert(rest[i - 1] == rows[i]);
                    }
                }
            },
        }
    } else if rows.len() > 0 {
        assert(rows[0] == rows[0]);
    }
}

/// With plain rows only, both forms take the same first row. A plain row
/// passes each dropped gate, and it passes each NULL test.
proof fn lemma_plain_rows_pass_the_same_gates(rows: Seq<WindowRow>)
    requires
        forall|i: int| 0 <= i < rows.len() ==> (#[trigger] rows[i]).plain && rows[i].dropped_gates,
    ensures
        first_row(rows, true) == first_row(rows, false),
    decreases rows.len(),
{
    if rows.len() > 0 {
        assert(rows[0].plain && rows[0].dropped_gates);
        let rest = rows.drop_first();
        assert forall|i: int| 0 <= i < rest.len() implies (#[trigger] rest[i]).plain
            && rest[i].dropped_gates by {
            assert(rest[i] == rows[i + 1]);
        }
        lemma_plain_rows_pass_the_same_gates(rest);
    }
}

/// The spine. With plain rows only, the lean form locks the candidate of
/// the window form and claims a row exactly when the window form claims it.
/// It returns `Empty` only when the window form returns `Empty`.
pub proof fn theorem_a_plain_window_takes_the_window_form_row(w: Window)
    requires
        well_formed(w),
        all_plain(w.rows),
    ensures
        lean_candidate(w) == window_candidate(w),
        lean_form(w) is Claimed ==> lean_form(w) == window_form(w),
        window_form(w) is Claimed ==> lean_form(w) == window_form(w),
        lean_form(w) is Empty ==> window_form(w) is Empty,
{
    assert forall|i: int| 0 <= i < w.rows.len() implies (#[trigger] w.rows[i]).plain
        && w.rows[i].dropped_gates by {
        assert(w.rows[i].plain);
    }
    lemma_plain_rows_pass_the_same_gates(w.rows);
    lemma_the_first_row_is_a_window_row(w.rows, false);
    match window_candidate(w) {
        Some(c) => {
            let i = choose|i: int| 0 <= i < w.rows.len() && #[trigger] w.rows[i] == c && passes(c, false);
            assert(w.rows[i].plain);
            assert(c.key_tests);
        },
        None => {},
    }
}

/// With a window row that is not plain, the lean form locks no row, claims
/// no row and sets its flag.
pub proof fn theorem_a_keyed_window_falls_back(w: Window)
    requires
        !all_plain(w.rows),
    ensures
        lean_candidate(w) is None,
        lean_form(w) is Fallback,
{
    let i = choose|i: int| 0 <= i < w.rows.len() && !(#[trigger] w.rows[i]).plain;
    assert(w.rows.len() > 0);
}

/// A set flag means that the lean form locked no row. So the window form
/// after it is a fresh claim attempt.
pub proof fn theorem_a_lean_fallback_took_no_row(w: Window)
    requires
        lean_form(w) is Fallback,
    ensures
        lean_candidate(w) is None,
{
}

/// The pair takes the row of the window form when both read the same
/// snapshot. With a later snapshot, it takes the row of a window form that
/// starts at that later point.
pub proof fn theorem_the_lean_claim_takes_the_window_form_row(first: Window, second: Window)
    requires
        well_formed(first),
    ensures
        lean_then_window(first, first) == window_form(first),
        lean_form(first) is Fallback ==> lean_then_window(first, second) == window_form(second),
        !(lean_form(first) is Fallback) ==> lean_then_window(first, second) == window_form(first),
{
    if all_plain(first.rows) {
        theorem_a_plain_window_takes_the_window_form_row(first);
    } else {
        theorem_a_keyed_window_falls_back(first);
    }
}

/// The longest run of claims that skip the lean form.
pub const LEAN_SKIP_CAP: u32 = 64;

/// The skip state of one claim loop.
pub struct LeanSkip {
    /// The claims left that send the window form first.
    pub left: u32,
    /// The length of the last skip, or 0 after a claimed row.
    pub span: u32,
}

/// The state stays in its bound.
pub open spec fn skip_ok(s: LeanSkip) -> bool {
    s.left <= s.span <= LEAN_SKIP_CAP
}

pub open spec fn spec_lean_skip_span(span: int) -> int {
    if span == 0 {
        1
    } else if span < 32 {
        span * 2
    } else {
        64
    }
}

pub open spec fn spec_lean_skip_after(skip: LeanSkip, lean: Option<WindowStep>) -> LeanSkip {
    match lean {
        None => if skip.left == 0 {
            skip
        } else {
            LeanSkip { left: (skip.left - 1) as u32, span: skip.span }
        },
        Some(WindowStep::Claimed) => LeanSkip { left: 0, span: 0 },
        Some(WindowStep::Empty) => skip,
        Some(WindowStep::Fallback) => LeanSkip {
            left: spec_lean_skip_span(skip.span as int) as u32,
            span: spec_lean_skip_span(skip.span as int) as u32,
        },
    }
}

// mirror: autumn-harvest/src/queue.rs lean_first
pub fn lean_first(skip: LeanSkip) -> (r: bool)
    ensures
        r == (skip.left == 0),
{
    skip.left == 0
}

// mirror: autumn-harvest/src/queue.rs lean_skip_span
pub fn lean_skip_span(span: u32) -> (r: u32)
    ensures
        r == spec_lean_skip_span(span as int),
        1 <= r <= LEAN_SKIP_CAP,
        span <= LEAN_SKIP_CAP ==> span <= r,
{
    if span == 0 {
        1
    } else if span < LEAN_SKIP_CAP / 2 {
        span * 2
    } else {
        LEAN_SKIP_CAP
    }
}

// mirror: autumn-harvest/src/queue.rs lean_skip_after
pub fn lean_skip_after(skip: LeanSkip, lean: Option<WindowStep>) -> (r: LeanSkip)
    ensures
        r == spec_lean_skip_after(skip, lean),
        skip_ok(skip) ==> skip_ok(r),
{
    match lean {
        None => {
            if skip.left == 0 {
                skip
            } else {
                LeanSkip {
                    left: skip.left - 1,
                    span: skip.span,
                }
            }
        }
        Some(WindowStep::Claimed) => LeanSkip { left: 0, span: 0 },
        Some(WindowStep::Empty) => skip,
        Some(WindowStep::Fallback) => {
            let span = lean_skip_span(skip.span);
            LeanSkip { left: span, span }
        }
    }
}

/// The state after `n` claims that skip the lean form.
pub open spec fn after_skips(skip: LeanSkip, n: nat) -> LeanSkip
    decreases n,
{
    if n == 0 {
        skip
    } else {
        after_skips(spec_lean_skip_after(skip, None), (n - 1) as nat)
    }
}

/// A new claim loop sends the lean form first, and each step keeps the
/// skip at or below the cap.
pub proof fn theorem_the_skip_stays_in_its_bound(skip: LeanSkip, lean: Option<WindowStep>)
    requires
        skip_ok(skip),
    ensures
        skip_ok(LeanSkip { left: 0, span: 0 }),
        skip_ok(spec_lean_skip_after(skip, lean)),
        spec_lean_skip_after(skip, lean).left <= LEAN_SKIP_CAP,
{
}

/// The first fallback starts a skip. Each next fallback doubles the skip
/// up to the cap. A claimed row ends it, and an empty poll keeps it.
pub proof fn theorem_a_fallback_doubles_the_skip_and_a_claim_resets_it(skip: LeanSkip)
    requires
        skip_ok(skip),
    ensures
        spec_lean_skip_after(skip, Some(WindowStep::Fallback)).left >= 1,
        skip.span == 0 ==> spec_lean_skip_after(skip, Some(WindowStep::Fallback)).span == 1,
        0 < skip.span < 32 ==> spec_lean_skip_after(skip, Some(WindowStep::Fallback)).span == 2
            * skip.span,
        skip.span >= 32 ==> spec_lean_skip_after(skip, Some(WindowStep::Fallback)).span
            == LEAN_SKIP_CAP,
        spec_lean_skip_after(skip, Some(WindowStep::Claimed)) == (LeanSkip { left: 0, span: 0 }),
        spec_lean_skip_after(skip, Some(WindowStep::Empty)) == skip,
{
}

/// Each claim that skips the lean form takes one claim off the skip.
proof fn lemma_each_skipped_claim_counts_down(skip: LeanSkip, n: nat)
    requires
        n <= skip.left,
    ensures
        after_skips(skip, n).left == skip.left - n,
        after_skips(skip, n).span == skip.span,
    decreases n,
{
    if n > 0 {
        lemma_each_skipped_claim_counts_down(spec_lean_skip_after(skip, None), (n - 1) as nat);
    }
}

/// A skip of `left` claims skips exactly `left` claims. The claim after
/// them sends the lean form first, and `left` is at most the cap.
pub proof fn theorem_the_lean_form_returns_within_the_cap(skip: LeanSkip, n: nat)
    requires
        skip_ok(skip),
        n <= skip.left,
    ensures
        skip.left <= LEAN_SKIP_CAP,
        n < skip.left ==> after_skips(skip, n).left > 0,
        after_skips(skip, skip.left as nat).left == 0,
{
    lemma_each_skipped_claim_counts_down(skip, n);
    lemma_each_skipped_claim_counts_down(skip, skip.left as nat);
}

/// One claim of a loop: the lean form first, or the window form alone.
pub open spec fn skipped_claim(skip: LeanSkip, first: Window, second: Window) -> Outcome {
    if skip.left == 0 {
        lean_then_window(first, second)
    } else {
        window_form(first)
    }
}

/// The skip selects the form that runs first, and no more. In each skip
/// state the claim takes the row of a window form.
pub proof fn theorem_the_skip_never_changes_the_row(skip: LeanSkip, first: Window, second: Window)
    requires
        well_formed(first),
    ensures
        skipped_claim(skip, first, first) == window_form(first),
        skipped_claim(skip, first, second) == window_form(first) || skipped_claim(
            skip,
            first,
            second,
        ) == window_form(second),
{
    theorem_the_lean_claim_takes_the_window_form_row(first, second);
}

} // verus!
