//! The self-test cases: a fixture source, a contract and the findings it
//! must produce.
//!
//! Each expected string must match exactly one finding. The check must
//! report nothing else. `cases.json` holds the cases of the Python original
//! as data. `sources` holds the fixture sources they name.

use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::Value;
use smop::prelude::*;

mod sources;

/// One self-test case.
#[derive(Debug, Deserialize)]
pub struct Case {
    /// What the case shows.
    pub name: String,
    /// The name of its fixture source in `sources`.
    pub source: String,
    /// The contract routes of the case.
    pub routes: Vec<Value>,
    /// The findings it must produce, by check.
    pub expected: BTreeMap<String, Vec<String>>,
}

/// Every self-test case, in order.
pub fn cases() -> Result<Vec<Case>> {
    serde_json::from_str(include_str!("cases.json")).context("parsing the self-test cases")
}

/// The fixture source named `name`.
pub fn source(name: &str) -> Result<&'static str> {
    sources::ALL
        .iter()
        .find(|(known, _)| *known == name)
        .map(|(_, text)| *text)
        .with_context(|| format!("no fixture source named {name}"))
}
