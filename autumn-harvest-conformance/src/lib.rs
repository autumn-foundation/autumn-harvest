//! Cross-backend conformance suite for `autumn-harvest` (issue #2030).
//!
//! This crate has no library code. The suite is the `conformance` test target
//! in `tests/conformance/`. It runs one corpus of workflows on Postgres and on
//! `SQLite` and asserts that each scenario gives the same outcome and a
//! replay-equivalent history. Read `README.md` for the rules.
