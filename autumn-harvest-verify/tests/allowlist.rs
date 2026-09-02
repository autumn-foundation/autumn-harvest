//! Allowlist loading and validation (D9): an escape hatch that cannot be used silently.

use std::collections::BTreeSet;
use std::io::Write;

use autumn_harvest_verify::Error;
use autumn_harvest_verify::allowlist::{AllowEntry, Allowlist};

fn write_temp(body: &str) -> tempfile::NamedTempFile {
    let mut f = tempfile::Builder::new()
        .prefix("harvest-verify-allow")
        .suffix(".toml")
        .tempfile()
        .expect("temp file");
    f.write_all(body.as_bytes()).expect("write");
    f.flush().expect("flush");
    f
}

fn entry(workflow: &str, justification: &str) -> AllowEntry {
    AllowEntry {
        workflow: workflow.to_string(),
        justification: justification.to_string(),
    }
}

fn used(names: &[&str]) -> BTreeSet<String> {
    names.iter().map(|s| (*s).to_string()).collect()
}

#[test]
fn loads_a_well_formed_file() {
    let f = write_temp(
        r#"
[[allow]]
workflow = "seeded::wf_rand_behind_dyn"
justification = "tracked in #963; the trait object is resolved at runtime by config"

[[allow]]
workflow = "seeded::wf_legacy_uuid"
justification = "pre-#384 workflow, frozen history; rewrite lands in #970"
"#,
    );
    let allow = Allowlist::load(f.path()).expect("well-formed allowlist");
    assert_eq!(allow.allow.len(), 2);
    assert_eq!(allow.allow[0].workflow, "seeded::wf_rand_behind_dyn");
    assert!(allow.allow[1].justification.contains("#970"));
}

#[test]
fn an_empty_file_is_an_empty_allowlist() {
    let f = write_temp("");
    let allow = Allowlist::load(f.path()).expect("an empty allowlist is legal");
    assert!(allow.allow.is_empty());
    assert!(allow.validate().is_ok());
}

#[test]
fn a_missing_file_is_an_io_error_not_a_panic() {
    let err = Allowlist::load(std::path::Path::new("/definitely/not/here.toml"))
        .expect_err("missing file");
    assert!(matches!(err, Error::Io { .. }), "got {err:?}");
}

#[test]
fn malformed_toml_is_an_allowlist_error() {
    let f = write_temp("[[allow]\nworkflow = ");
    let err = Allowlist::load(f.path()).expect_err("malformed TOML");
    assert!(matches!(err, Error::Allowlist(_)), "got {err:?}");
}

#[test]
fn a_blank_justification_is_a_hard_error() {
    for blank in ["", " ", "\t", "\n", "   \t \n "] {
        let list = Allowlist {
            allow: vec![entry("crate::wf", blank)],
        };
        let err = list
            .validate()
            .expect_err("blank justification must be rejected");
        assert!(matches!(err, Error::Allowlist(_)), "got {err:?}");
        let message = err.to_string();
        assert!(
            message.contains("crate::wf"),
            "the error must name the offending workflow: {message}"
        );
    }
}

#[test]
fn a_blank_justification_is_rejected_on_load_too() {
    let f = write_temp(
        r#"
[[allow]]
workflow = "seeded::wf_x"
justification = "   "
"#,
    );
    let err = Allowlist::load(f.path()).expect_err("blank justification via load");
    assert!(matches!(err, Error::Allowlist(_)), "got {err:?}");
}

#[test]
fn a_duplicate_workflow_is_a_hard_error() {
    let list = Allowlist {
        allow: vec![
            entry("crate::wf", "first reason"),
            entry("crate::wf", "second reason"),
        ],
    };
    let err = list
        .validate()
        .expect_err("duplicate workflow must be rejected");
    assert!(matches!(err, Error::Allowlist(_)), "got {err:?}");
    assert!(err.to_string().contains("crate::wf"), "{err}");
}

#[test]
fn a_duplicate_workflow_is_rejected_on_load_too() {
    let f = write_temp(
        r#"
[[allow]]
workflow = "seeded::wf_x"
justification = "one"

[[allow]]
workflow = "seeded::wf_x"
justification = "two"
"#,
    );
    let err = Allowlist::load(f.path()).expect_err("duplicate via load");
    assert!(matches!(err, Error::Allowlist(_)), "got {err:?}");
}

#[test]
fn justification_lookup_is_exact() {
    let list = Allowlist {
        allow: vec![
            entry("seeded::wf_a", "reason a"),
            entry("seeded::wf_b", "reason b"),
        ],
    };
    assert_eq!(list.justification("seeded::wf_a"), Some("reason a"));
    assert_eq!(list.justification("seeded::wf_b"), Some("reason b"));
    assert_eq!(list.justification("seeded::wf_c"), None);
    assert_eq!(
        list.justification("wf_a"),
        None,
        "lookup is by full path, not by suffix"
    );
    assert_eq!(list.justification(""), None);
}

#[test]
fn unused_reports_entries_that_matched_nothing() {
    let list = Allowlist {
        allow: vec![
            entry("seeded::wf_a", "reason a"),
            entry("seeded::wf_b", "reason b"),
            entry("seeded::wf_gone", "workflow was deleted in #971"),
        ],
    };
    let unused = list.unused(&used(&["seeded::wf_a", "seeded::wf_b", "clean::wf_z"]));
    assert_eq!(unused.len(), 1);
    assert_eq!(unused[0].workflow, "seeded::wf_gone");

    assert!(
        list.unused(&used(&["seeded::wf_a", "seeded::wf_b", "seeded::wf_gone"]))
            .is_empty()
    );
    assert_eq!(
        list.unused(&BTreeSet::new()).len(),
        3,
        "nothing analyzed => everything unused"
    );
    assert!(
        Allowlist::default()
            .unused(&used(&["seeded::wf_a"]))
            .is_empty()
    );
}
