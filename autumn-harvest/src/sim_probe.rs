//! `always!` and `sometimes!` for workflow code (issue #2033).
//!
//! The macros are in every build, so a workflow body can keep them. Inside a
//! simulation run (`testing::sim::Simulation`) they record into that run.
//! Elsewhere, and in a build without the `testing` feature, they only
//! evaluate the condition. The label is lazy. Only a run that records it
//! evaluates it.

/// The function behind [`always!`](crate::always). It returns `cond`.
#[doc(hidden)]
#[allow(clippy::must_use_candidate)]
pub fn record_always<L: AsRef<str>>(cond: bool, label: impl FnOnce() -> L) -> bool {
    #[cfg(any(test, feature = "testing"))]
    if !cond {
        crate::testing::sim::note_always(label);
    }
    #[cfg(not(any(test, feature = "testing")))]
    let _ = label;
    cond
}

/// The function behind [`sometimes!`](crate::sometimes). It returns `cond`.
#[doc(hidden)]
#[allow(clippy::must_use_candidate)]
pub fn record_sometimes<L: AsRef<str>>(cond: bool, label: impl FnOnce() -> L) -> bool {
    #[cfg(any(test, feature = "testing"))]
    crate::testing::sim::note_sometimes(cond, label);
    #[cfg(not(any(test, feature = "testing")))]
    let _ = label;
    cond
}

/// A hard invariant of a workflow simulation run (issue #2033).
///
/// `always!(cond, "label")` fails the run when `cond` is false. Use it in a
/// workflow body or in the check closure of a simulation sweep. Outside a
/// simulation run it only evaluates `cond`. A run evaluates `label` only when
/// `cond` is false. It returns `cond`.
#[macro_export]
macro_rules! always {
    ($cond:expr, $label:expr $(,)?) => {
        $crate::sim_probe::record_always($cond, || ::std::string::ToString::to_string(&$label))
    };
}

/// A reachability target of a workflow simulation sweep (issue #2033).
///
/// `sometimes!(cond, "label")` must be true on at least one seed of a sweep,
/// or the sweep fails. So a sweep cannot pass without reaching the label.
/// Outside a simulation run it only evaluates `cond`, not `label`. It returns
/// `cond`.
#[macro_export]
macro_rules! sometimes {
    ($cond:expr, $label:expr $(,)?) => {
        $crate::sim_probe::record_sometimes($cond, || ::std::string::ToString::to_string(&$label))
    };
}
