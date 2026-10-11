//! Fixture: the crate of the type in `<types::Plan as limits::Limits>::MAX`
//! (issue #1995). Its own `MAX` is unrelated to the impl. See `flow.rs`.

pub struct Plan;

pub const MAX: u64 = 1;
