//! `harvest-verify-corpus-boundary` — the honesty corpus for issue #962 (AC2's
//! "three-valued honesty is required" clause).
//!
//! Every module here contains exactly one `#[workflow]` function, named
//! identically to its module, that **must** come back `unknown` with a
//! **named** boundary — never `proven-deterministic` (which would be unsound)
//! and never `nondeterminism-found` (which would be a guess dressed up as a
//! finding).
//!
//! The four boundaries exercised are exactly the four the issue's Out-of-Scope
//! section names — unsafe, FFI, arbitrary `dyn` dispatch, and indirect calls —
//! spelled with the kebab-case names `BoundaryKind::name()` prints:
//! `dyn-dispatch`, `indirect-call`, `ffi`, `unsafe-raw-pointer`.
//!
//! Two of these cases are deliberately paired with a seeded sibling that *is*
//! resolvable, so the analyzer cannot improve its detection rate by guessing:
//! `wf_dyn_unknown_impl` (two impls, unknown) against
//! `harvest_verify_corpus_seeded::wf_rand_behind_dyn` (one impl, found).
//!
//! Naming convention (shared with the seeded and clean crates): the workflow
//! function has the same name as its module, so the fully-qualified path is
//! `harvest_verify_corpus_boundary::wf_<case>::wf_<case>`.

pub mod wf_dyn_unknown_impl;
pub mod wf_extern_c;
pub mod wf_fn_pointer;
pub mod wf_raw_pointer_static_mut;
