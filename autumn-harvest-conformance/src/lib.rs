//! Cross-backend conformance suite for `autumn-harvest` (issue #2030).
//!
//! This crate has no library code. The suite is the `conformance` test target
//! in `tests/conformance/`. It runs one corpus of workflows on Postgres and on
//! `SQLite`. Each scenario that `SQLite` supports must give the same outcome
//! and a replay-equivalent history on both. Each scenario that `SQLite`
//! rejects runs on `SQLite` only. It must fail there with `UnsupportedFeature`
//! and name the rejected feature. Read `README.md` for the rules.
