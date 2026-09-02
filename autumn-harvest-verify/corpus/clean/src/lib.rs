//! `harvest-verify-corpus-clean` — the false-positive corpus for issue #962
//! (AC4 and the "≤ 10 % need an allowlist entry" half of the success metric).
//!
//! Every module here contains exactly one `#[workflow]` function, named
//! identically to its module, that **must** come back `proven-deterministic`.
//! Several of them are deliberately uncomfortable: they route genuinely
//! non-deterministic values into `ctx.side_effect`, `ctx.metrics()` and
//! `ctx.logger()`, or branch on `ctx.version(..)`, or use a `HashMap` in ways
//! that are perfectly safe. Each one is a false positive the analyzer is not
//! allowed to have, because each corresponds to something real workflows do
//! constantly — and a determinism gate that fires on `?`, on the logger, or on
//! the fix its own sibling lint recommends will simply be turned off.
//!
//! Naming convention (shared with the seeded and boundary crates): the workflow
//! function has the same name as its module, so the fully-qualified path is
//! `harvest_verify_corpus_clean::wf_<case>::wf_<case>`.

pub mod helpers_ctx;

pub mod wf_btreemap_dispatch;
pub mod wf_collect_into_btreemap;
pub mod wf_ctx_random;
pub mod wf_ctx_side_effect;
pub mod wf_ctx_system_now;
pub mod wf_ctx_version_branch;
pub mod wf_hashmap_lookup_only;
pub mod wf_helper_emits_clean_activity;
pub mod wf_local_refcell_no_escape;
pub mod wf_logger_ambient;
pub mod wf_metrics_with_ambient_label;
pub mod wf_sorted_keys;
pub mod wf_try_on_clean_result;
