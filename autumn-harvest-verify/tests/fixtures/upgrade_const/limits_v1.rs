//! Fixture: build 1 of a dependency crate that a workflow reads a `const` from
//! (issue #1995). `limits_v1.rs` and `limits_v2.rs` differ only in `ATTEMPTS`.
//! See `flow.rs` for how the fixtures are produced.

pub const ATTEMPTS: u64 = 3;
