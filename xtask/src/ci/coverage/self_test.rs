//! The `--self-test` of `cargo xtask ci coverage`.
//!
//! It checks the LCOV merge, the `llvm-cov` JSON line rule and the ratchet in
//! seconds. So a break shows on the pull request, not on the next nightly.
//! `cargo test -p xtask` runs it too.

use std::collections::BTreeMap;

use smop::prelude::*;

use super::pyjson::{self, Value};
use super::{
    BranchKey, FileRow, check, files_markdown, from_export, merge, parse_lcov, raised,
    ratchet_markdown, render_lcov, round_down, summarize, summary_value,
};

/// The first shard of the merge fixture.
pub const SHARD_A: &str = "TN:
SF:/w/r/autumn-harvest/src/queue.rs
FN:3,f
FNDA:1,f
DA:3,1
DA:4,0
BRDA:4,0,0,1
BRDA:4,0,1,-
BRF:2
BRH:1
LF:2
LH:1
end_of_record
SF:/w/r/autumn-harvest/src/erase.rs
DA:1,0
LF:1
LH:0
end_of_record
";

/// The second shard of the merge fixture.
pub const SHARD_B: &str = "SF:/w/r/autumn-harvest/src/queue.rs
DA:4,2
DA:5,0
BRDA:4,0,0,-
BRDA:4,0,1,3
end_of_record
";

/// An `llvm-cov export` JSON file.
///
/// A segment is `[line, column, count, has_count, is_region_entry,
/// is_gap_region]`. A branch is `[line, column, end_line, end_column,
/// true_count, false_count, file_id, expanded_id, kind]`.
///
/// The expansion is a macro call on line 6, column 3. Its branch is at line
/// 40 of the macro definition. A second branch sits at the same place in
/// another file. A third is a second nested call of the first.
pub const EXPORT: &str = r#"{
  "type": "llvm.coverage.json.export",
  "data": [{"files": [{
    "filename": "/w/r/autumn-harvest/src/timeout.rs",
    "segments": [
      [1, 1, 5, true, true, false], [1, 10, 0, false, false, false],
      [3, 1, 2, true, true, false], [3, 5, 7, true, true, false],
      [3, 9, 0, false, false, false],
      [5, 1, 0, true, true, false], [6, 3, 0, false, false, false],
      [8, 1, 0, false, true, false],
      [10, 1, 4, true, true, true], [10, 20, 3, true, true, false],
      [12, 1, 0, false, true, false], [12, 8, 6, true, true, false],
      [14, 1, 0, false, false, false],
      [16, 1, 5, true, true, true]
    ],
    "branches": [[3, 5, 3, 8, 2, 0, 0, 0, 4], [3, 5, 3, 8, 1, 1, 0, 0, 4]],
    "expansions": [{
      "source_region": [6, 3, 6, 20, 1, 0, 1, 1],
      "filenames": ["/w/r/autumn-harvest/src/timeout.rs", "/w/r/m.rs", "/w/r/n.rs"],
      "branches": [
        [40, 9, 40, 14, 1, 0, 1, 0, 4], [40, 9, 40, 14, 0, 2, 2, 0, 4],
        [40, 9, 40, 14, 4, 0, 1, 0, 4]
      ]
    }]
  }]}]
}"#;

/// Fails the self-test with `message` unless `cond` holds.
macro_rules! check_that {
    ($cond:expr, $($message:tt)+) => {
        ensure!($cond, "self-test assertion failed: {}", format!($($message)+))
    };
}

/// A line map from pairs.
fn lines(pairs: &[(i64, i128)]) -> BTreeMap<i64, i128> {
    pairs.iter().copied().collect()
}

/// A branch map from tuples.
fn branches(items: &[(i64, &str, &str, i128)]) -> BTreeMap<BranchKey, i128> {
    items
        .iter()
        .map(|(l, b, s, h)| ((*l, (*b).to_string(), (*s).to_string()), *h))
        .collect()
}

/// Parses a JSON literal of the test.
fn json(text: &str) -> Result<Value> {
    pyjson::loads(text)
}

/// A baseline with one module and a tolerance.
fn base(path: &str, floors: &str, tolerance: f64) -> Result<Value> {
    json(&format!(
        r#"{{"tolerance": {}, "modules": {{"{path}": {floors}}}}}"#,
        pyjson::float_repr(tolerance)
    ))
}

/// The module floors of `baseline` at `path`.
fn floors_of<'a>(baseline: &'a Value, path: &str) -> Result<&'a Value> {
    baseline.index("modules")?.index(path)
}

/// A row of the summary with its branch percent replaced.
const fn with_branches_pct(row: FileRow, pct: f64) -> FileRow {
    FileRow {
        branches_pct: Some(pct),
        ..row
    }
}

/// Runs the self-test.
pub fn run() -> Result<()> {
    export_rules()?;
    let summary_files = merge_rules()?;
    ratchet_rules(&summary_files)
}

/// The line rule and the branch keys of the `llvm-cov` JSON conversion.
fn export_rules() -> Result<()> {
    let doc: serde_json::Value = serde_json::from_str(EXPORT)?;
    let records = from_export(&doc, "/w/r")?;
    let timeout = records
        .get("autumn-harvest/src/timeout.rs")
        .ok_or_else(|| anyhow!("the export has no timeout.rs"))?;
    // Lines 2 and 4 follow a segment with no count. Line 6 follows a counted
    // region. Line 8 starts a skipped region. Line 10 starts with a gap, so
    // only its second region counts.
    // Line 11 follows the counted region of line 10. Line 12 starts a skipped
    // region, but a counted region starts later on it, so it is mapped. Lines
    // 13 and 14 follow that region. Line 16 holds only a counted gap entry
    // after a segment with no count. So it is mapped with no hits.
    let want = lines(&[
        (1, 5),
        (3, 7),
        (5, 0),
        (6, 0),
        (10, 3),
        (11, 3),
        (12, 6),
        (13, 6),
        (14, 6),
        (16, 0),
    ]);
    check_that!(timeout.lines == want, "{:?}", timeout.lines);
    // Two instantiations of one branch add up. A branch inside a macro
    // expansion counts at the line of the call, as the LCOV exporter does.
    // Its key names its source file, so two files stay apart. It also names
    // the occurrence, so two nested calls stay apart.
    let want = branches(&[
        (3, "5", "0", 3),
        (3, "5", "1", 1),
        (6, "3@m.rs:40:9", "0", 1),
        (6, "3@m.rs:40:9", "1", 0),
        (6, "3@n.rs:40:9", "0", 0),
        (6, "3@n.rs:40:9", "1", 2),
        (6, "3@m.rs:40:9#1", "0", 4),
        (6, "3@m.rs:40:9#1", "1", 0),
    ]);
    check_that!(timeout.branches == want, "{:?}", timeout.branches);
    let other: serde_json::Value = serde_json::from_str(r#"{"type": "other"}"#)?;
    check_that!(
        from_export(&other, "").is_err(),
        "a non-export file must fail"
    );
    Ok(())
}

/// The LCOV parse, the merge, the render round trip and the summary.
fn merge_rules() -> Result<BTreeMap<String, FileRow>> {
    let a = parse_lcov(SHARD_A, "/w/r")?;
    let b = parse_lcov(SHARD_B, "/w/r/")?;
    let keys: Vec<&str> = a.keys().map(String::as_str).collect();
    check_that!(
        keys == ["autumn-harvest/src/erase.rs", "autumn-harvest/src/queue.rs"],
        "{keys:?}"
    );
    for root in ["", "/other"] {
        let parsed = parse_lcov(SHARD_B, root)?;
        let keys: Vec<&str> = parsed.keys().map(String::as_str).collect();
        check_that!(keys == ["/w/r/autumn-harvest/src/queue.rs"], "{keys:?}");
    }

    let merged = merge(&[a.clone(), b]);
    let queue = merged
        .get("autumn-harvest/src/queue.rs")
        .ok_or_else(|| anyhow!("the merge has no queue.rs"))?;
    check_that!(
        queue.lines == lines(&[(3, 1), (4, 2), (5, 0)]),
        "{:?}",
        queue.lines
    );
    check_that!(
        queue.branches == branches(&[(4, "0", "0", 1), (4, "0", "1", 3)]),
        "{:?}",
        queue.branches
    );
    let twice = merge(&[a.clone(), a]);
    let twice_lines = twice
        .get("autumn-harvest/src/queue.rs")
        .map(|d| d.lines.clone())
        .unwrap_or_default();
    check_that!(twice_lines == lines(&[(3, 2), (4, 0)]), "{twice_lines:?}");

    let again = merge(&[parse_lcov(&render_lcov(&merged), "/elsewhere")?]);
    check_that!(again == merged, "{again:?}");

    let files = summarize(&merged);
    let queue = files
        .get("autumn-harvest/src/queue.rs")
        .copied()
        .ok_or_else(|| anyhow!("the summary has no queue.rs"))?;
    check_that!(
        queue
            == FileRow {
                lines_hit: 2,
                lines_total: 3,
                lines_pct: Some(66.67),
                branches_hit: 2,
                branches_total: 2,
                branches_pct: Some(100.0),
            },
        "{queue:?}"
    );
    let erase = files
        .get("autumn-harvest/src/erase.rs")
        .copied()
        .ok_or_else(|| anyhow!("the summary has no erase.rs"))?;
    check_that!(
        erase.lines_pct == Some(0.0) && erase.branches_pct.is_none(),
        "{erase:?}"
    );
    Ok(files)
}

/// The ratchet: the floor check, the floor raise and the Markdown.
#[allow(clippy::too_many_lines)]
fn ratchet_rules(files: &BTreeMap<String, FileRow>) -> Result<()> {
    const QUEUE: &str = "autumn-harvest/src/queue.rs";
    let summary = summary_value(files);
    let queue_floor = r#"{"lines": 66.0, "branches": 100.0}"#;

    check_that!(
        check(&base(QUEUE, queue_floor, 0.5)?, &summary)?.is_empty(),
        "at the floor"
    );
    let within = r#"{"lines": 67.1, "branches": 100.0}"#;
    check_that!(
        check(&base(QUEUE, within, 0.5)?, &summary)?.is_empty(),
        "within the tolerance"
    );
    let drop = r#"{"lines": 67.2, "branches": 100.0}"#;
    let found = check(&base(QUEUE, drop, 0.5)?, &summary)?;
    check_that!(
        found.len() == 1 && found[0].contains("lines") && found[0].contains("67.2"),
        "{found:?}"
    );
    let found = check(
        &base("autumn-harvest/src/gone.rs", queue_floor, 0.5)?,
        &summary,
    )?;
    check_that!(
        found.len() == 1 && found[0].contains("no coverage data"),
        "{found:?}"
    );
    let erase_floor = r#"{"lines": 0.0, "branches": 10.0}"#;
    let found = check(
        &base("autumn-harvest/src/erase.rs", erase_floor, 0.5)?,
        &summary,
    )?;
    check_that!(
        found.len() == 1 && found[0].contains("no branch data"),
        "{found:?}"
    );
    let pending = r#"{"lines": null, "branches": null}"#;
    check_that!(
        check(&base(QUEUE, pending, 0.5)?, &summary)?.is_empty(),
        "a null floor passes"
    );

    let raised_floors = json(r#"{"lines": 66.67, "branches": 100.0}"#)?;
    let low = base(QUEUE, r#"{"lines": 50.0, "branches": 100.0}"#, 0.5)?;
    let (new, _) = raised(&low, &summary)?;
    check_that!(floors_of(&new, QUEUE)? == &raised_floors, "{new:?}");
    check_that!(
        floors_of(&low, QUEUE)?.get("lines") == Some(&Value::Float(50.0)),
        "raised must copy"
    );
    let high = base(QUEUE, r#"{"lines": 90.0, "branches": 100.0}"#, 0.5)?;
    let (same, changed) = raised(&high, &summary)?;
    check_that!(same == high && !changed, "a floor never drops");
    let (new, _) = raised(&base(QUEUE, pending, 0.5)?, &summary)?;
    check_that!(floors_of(&new, QUEUE)? == &raised_floors, "{new:?}");

    let text = ratchet_markdown(&low, &summary, &check(&low, &summary)?, false)?;
    check_that!(
        text.contains("| `autumn-harvest/src/queue.rs` | 66.67% | 50.00% | 100.00% | 100.00% |"),
        "{text}"
    );
    check_that!(
        text.contains("A floor can rise") && !text.contains("FAIL"),
        "{text}"
    );
    let text = ratchet_markdown(&high, &summary, &check(&high, &summary)?, false)?;
    check_that!(
        text.contains("- FAIL: autumn-harvest/src/queue.rs: lines") && !text.contains("can rise"),
        "{text}"
    );
    let text = files_markdown(files);
    check_that!(
        text.contains("| `autumn-harvest` | 50.00% | 100.00% |"),
        "{text}"
    );
    check_that!(
        text.contains("| `autumn-harvest/src/erase.rs` | 0.00% (0/1) | n/a (0/0) |"),
        "{text}"
    );
    let erase = files
        .get("autumn-harvest/src/erase.rs")
        .copied()
        .ok_or_else(|| anyhow!("the summary has no erase.rs"))?;
    let root_crate = BTreeMap::from([("src/lib.rs".to_string(), erase)]);
    check_that!(
        files_markdown(&root_crate).contains("| `.` |"),
        "a root crate"
    );

    // A module tolerance overrides the global one, and `raised` keeps it.
    let mut small = base(QUEUE, r#"{"lines": 66.67, "branches": 100.0}"#, 0.5)?;
    let queue_row = files
        .get(QUEUE)
        .copied()
        .ok_or_else(|| anyhow!("the summary has no queue.rs"))?;
    let one_branch = summary_value(&BTreeMap::from([(
        QUEUE.to_string(),
        with_branches_pct(queue_row, 98.6),
    )]));
    check_that!(
        check(&small, &one_branch)?.len() == 1,
        "the global tolerance"
    );
    small
        .items_mut()?
        .iter_mut()
        .find(|(k, _)| k == "modules")
        .map(|(_, m)| m)
        .ok_or_else(|| anyhow!("no modules"))?
        .items_mut()?
        .iter_mut()
        .find(|(k, _)| k == QUEUE)
        .map(|(_, f)| f)
        .ok_or_else(|| anyhow!("no queue module"))?
        .set("tolerance", Value::Float(1.5))?;
    check_that!(check(&small, &one_branch)?.is_empty(), "a module tolerance");
    let (kept, _) = raised(&small, &summary)?;
    check_that!(
        floors_of(&kept, QUEUE)?.get("tolerance") == Some(&Value::Float(1.5)),
        "raised must keep a module tolerance"
    );

    // Float error must not move a floor or the tolerance edge.
    check_that!(
        (round_down(37.8) - 37.8).abs() < f64::EPSILON
            && (round_down(19.49) - 19.49).abs() < f64::EPSILON,
        "round_down"
    );
    let edge = json(
        r#"{"files": {"m.rs": {"lines_total": 1, "lines_pct": 0.01, "branches_pct": null}}}"#,
    )?;
    let at_edge = base("m.rs", r#"{"lines": 0.51, "branches": null}"#, 0.5)?;
    check_that!(check(&at_edge, &edge)?.is_empty(), "the tolerance edge");
    let past_edge = base("m.rs", r#"{"lines": 0.52, "branches": null}"#, 0.5)?;
    check_that!(check(&past_edge, &edge)?.len() == 1, "past the edge");
    // A run with a failed test job shows no verdict.
    let text = ratchet_markdown(&high, &summary, &check(&high, &summary)?, true)?;
    check_that!(
        text.contains("Ratchet skipped") && !text.contains("FAIL") && !text.contains("can rise"),
        "{text}"
    );
    // With no floor recorded, the summary says what the gate still checks.
    let unarmed = base(QUEUE, pending, 0.5)?;
    check_that!(
        ratchet_markdown(&unarmed, &summary, &[], false)?.contains("No floor is recorded yet"),
        "an unarmed baseline"
    );
    check_that!(
        !ratchet_markdown(&low, &summary, &[], false)?.contains("No floor is recorded yet"),
        "an armed baseline"
    );
    Ok(())
}
