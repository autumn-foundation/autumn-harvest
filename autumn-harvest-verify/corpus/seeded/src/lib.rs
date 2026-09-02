//! `harvest-verify-corpus-seeded` — the seeded non-determinism corpus for issue
//! #962 (AC3 and the ≥ 90 % detection success metric).
//!
//! **The contract of this crate.** Every module here contains exactly one
//! `#[workflow]` function, named identically to its module, that carries exactly
//! one distinct, deliberately *laundered* determinism bug. Every one of them
//! must come back `nondeterminism-found` from `harvest-verify`, with a trace
//! that names the helper it flowed through — and every one of them passes the
//! entire syntactic layer cleanly:
//!
//! * **Zero HVG findings of any severity.** The `#[workflow]` proc macro turns
//!   hard blockers into `compile_error!` and warnings into a `#[deprecated]`
//!   const, so a build of this crate under `RUSTFLAGS="-D warnings"` *is* the
//!   proof — there is no other way to get it, since `autumn-harvest-macros` is
//!   a `proc-macro` crate and cannot export its visitor to a test.
//! * **Zero `det_check` findings and zero suppressions.**
//!   `autumn_harvest::det_check::check_paths` over `corpus/*/src` must return an
//!   empty report. There is no `allow_nondeterministic_apis` and no
//!   `// harvest-suppress:` comment anywhere in the corpus; both are grepped for
//!   by `autumn-harvest-verify/tests/corpus.rs`.
//!
//! **Why they escape.** Three structural gaps, documented per module:
//! 1. HVG001–HVG011 are a `syn` visitor over the **annotated body only**.
//! 2. DET001–DET011 are a line-oriented substring scan over `#[workflow]` bodies
//!    plus **one** helper hop resolved **same-file, same-module**.
//! 3. HVG011/DET010 (hash iteration) track only **locally bound** hash idents
//!    iterated by a bare ident or a *single* argument-free method — never
//!    parameters, never struct fields, never adaptor chains.
//!
//! **Naming convention (used verbatim by `corpus/expectations.toml`).** The
//! workflow function has the same name as its module, so the fully-qualified
//! path is `harvest_verify_corpus_seeded::wf_<case>::wf_<case>` (Rust crate name
//! with underscores, then module, then fn).
//!
//! This crate is deliberately pathological code. It is `publish = false`, it is
//! not a library anyone should import, and it deliberately does **not** enable
//! `[lints] workspace = true`.

pub mod helpers_ctx;

pub mod wf_atomic_shard_pick;
pub mod wf_cell_ambient_counter;
pub mod wf_closure_captures_ambient;
pub mod wf_collect_into_hashmap_then_reiterate;
pub mod wf_conditional_command_emission;
pub mod wf_env_var_os_in_helper;
pub mod wf_hashmap_adaptor_chain_fanout;
pub mod wf_hashmap_generic_dispatch;
pub mod wf_hashmap_join_string;
pub mod wf_hashmap_keys_next;
pub mod wf_hashset_via_trait_method;
pub mod wf_instant_elapsed_field;
pub mod wf_order_dependence_which_first;
pub mod wf_process_id_payload;
pub mod wf_rand_behind_dyn;
pub mod wf_rand_in_helper_branch;
pub mod wf_refcell_captured_state;
pub mod wf_replay_varying_ctx_read;
pub mod wf_rwlock_config_read;
pub mod wf_static_counter_in_helper;
pub mod wf_static_mutex_queue;
pub mod wf_systemtime_two_crates_deep;
pub mod wf_tainted_child_workflow_input;
pub mod wf_thread_id_branch;
pub mod wf_threadlocal_refcell_seq;
pub mod wf_three_hop_chain;
pub mod wf_time_dependent_timer_duration;
pub mod wf_tokio_sleep_in_helper;
pub mod wf_uuid_in_helper;
