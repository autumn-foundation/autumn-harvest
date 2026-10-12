//! Fixture: build 2 of the crate of the trait and of the impl (issue #1995).
//! `limits_v1.rs` and `limits_v2.rs` differ only in the impl's `MAX`.

pub trait Limits {
    const MAX: u64;
}

impl Limits for types::Plan {
    const MAX: u64 = 4;
}
