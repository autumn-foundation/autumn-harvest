//! `harvest-verify-corpus-boundary` — the honesty corpus for issue #962 (AC2's
//! "three-valued honesty is required" clause).
//!
//! Every module here holds exactly one `#[workflow]` function with its
//! module's name.
//! Each **must** come back `unknown` with a **named** boundary.
//! `proven-deterministic` would be unsound. `nondeterminism-found` would be a
//! guess dressed up as a finding.
//!
//! The four boundaries are the four that the issue's Out-of-Scope section
//! names: unsafe, FFI, arbitrary `dyn` dispatch, and indirect calls. Their
//! kebab-case names from `BoundaryKind::name()` are `dyn-dispatch`,
//! `indirect-call`, `ffi` and `unsafe-raw-pointer`.
//!
//! Two of these cases deliberately pair with a resolvable seeded sibling, so
//! the analyzer cannot raise its detection rate by guessing. The pair is
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
