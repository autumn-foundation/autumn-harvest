//! The `results.tsv` log, in the upstream format.
//!
//! The file has a header and five tab-separated columns: `commit`, `val_bpb`,
//! `memory_gb`, `status` and `description`. A crash logs `0.000000` and
//! `0.0`. The file stays untracked by git.

use std::fmt::Write as _;

use serde::{Deserialize, Serialize};

use crate::rules::Status;

/// The header line of `results.tsv`.
pub const HEADER: &str = "commit\tval_bpb\tmemory_gb\tstatus\tdescription";

/// One row of `results.tsv`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResultRow {
    /// The short commit hash, 7 characters.
    pub commit: String,
    /// The validation bits per byte. Zero for a crash.
    pub val_bpb: f64,
    /// The peak memory in GB. Zero for a crash.
    pub memory_gb: f64,
    /// The status column.
    pub status: Status,
    /// What the experiment tried, on one line.
    pub description: String,
}

impl ResultRow {
    /// The row as one TSV line, with no line end.
    ///
    /// A tab or a line break in the description becomes a space, so the row
    /// always has five columns.
    #[must_use]
    pub fn to_line(&self) -> String {
        format!(
            "{}\t{:.6}\t{:.1}\t{}\t{}",
            self.commit,
            self.val_bpb,
            self.memory_gb,
            self.status.as_str(),
            one_line(&self.description)
        )
    }

    /// Parse one TSV line.
    ///
    /// # Errors
    ///
    /// Returns an error when the line does not have five valid columns.
    pub fn parse_line(line: &str) -> Result<Self, String> {
        let columns: Vec<&str> = line.splitn(5, '\t').collect();
        let [commit, val_bpb, memory_gb, status, description] = columns[..] else {
            return Err(format!(
                "expected 5 tab-separated columns, got {}",
                columns.len()
            ));
        };
        let number = |name: &str, text: &str| {
            text.trim()
                .parse::<f64>()
                .ok()
                .filter(|value| value.is_finite())
                .ok_or_else(|| format!("{name} is not a number: {text:?}"))
        };
        let status = match status.trim() {
            "keep" => Status::Keep,
            "discard" => Status::Discard,
            "crash" => Status::Crash,
            other => return Err(format!("status is not keep, discard or crash: {other:?}")),
        };
        Ok(Self {
            commit: commit.trim().to_string(),
            val_bpb: number("val_bpb", val_bpb)?,
            memory_gb: number("memory_gb", memory_gb)?,
            status,
            description: description.trim().to_string(),
        })
    }
}

/// Parse a whole `results.tsv` text. The header is optional. Blank lines are
/// skipped.
///
/// # Errors
///
/// Returns the first line that does not parse, with its line number.
pub fn parse(text: &str) -> Result<Vec<ResultRow>, String> {
    text.lines()
        .enumerate()
        .filter(|(_, line)| !line.trim().is_empty() && *line != HEADER)
        .map(|(index, line)| {
            ResultRow::parse_line(line).map_err(|error| format!("line {}: {error}", index + 1))
        })
        .collect()
}

/// Render rows as a short Markdown table for a prompt or a report.
#[must_use]
pub fn table(rows: &[ResultRow]) -> String {
    if rows.is_empty() {
        return "(no results yet)".into();
    }
    let mut text = String::from(
        "| commit | val_bpb | memory_gb | status | description |\n|---|---|---|---|---|\n",
    );
    for row in rows {
        let _ = writeln!(
            text,
            "| {} | {:.6} | {:.1} | {} | {} |",
            row.commit,
            row.val_bpb,
            row.memory_gb,
            row.status.as_str(),
            one_line(&row.description).replace('|', "/")
        );
    }
    text
}

/// `text` on one line: each tab and line break becomes a space.
fn one_line(text: &str) -> String {
    text.replace(['\t', '\r', '\n'], " ").trim().to_string()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::float_cmp)]

    use proptest::prelude::*;

    use super::*;

    fn row(status: Status) -> ResultRow {
        ResultRow {
            commit: "b2c3d4e".into(),
            val_bpb: 0.9932,
            memory_gb: 44.2,
            status,
            description: "increase LR to 0.04".into(),
        }
    }

    #[test]
    fn a_row_renders_in_the_upstream_format() {
        assert_eq!(
            row(Status::Keep).to_line(),
            "b2c3d4e\t0.993200\t44.2\tkeep\tincrease LR to 0.04"
        );
        let crash = ResultRow {
            val_bpb: 0.0,
            memory_gb: 0.0,
            ..row(Status::Crash)
        };
        assert_eq!(
            crash.to_line(),
            "b2c3d4e\t0.000000\t0.0\tcrash\tincrease LR to 0.04"
        );
    }

    #[test]
    fn a_tab_or_a_newline_in_a_description_cannot_add_a_column() {
        let messy = ResultRow {
            description: "a\tb\nc".into(),
            ..row(Status::Discard)
        };
        let line = messy.to_line();
        assert_eq!(line.split('\t').count(), 5);
        assert!(!line.contains('\n'));
        assert_eq!(ResultRow::parse_line(&line).unwrap().description, "a b c");
    }

    #[test]
    fn the_upstream_example_parses() {
        let text = "commit\tval_bpb\tmemory_gb\tstatus\tdescription\n\
                    a1b2c3d\t0.997900\t44.0\tkeep\tbaseline\n\
                    b2c3d4e\t0.993200\t44.2\tkeep\tincrease LR to 0.04\n\
                    c3d4e5f\t1.005000\t44.0\tdiscard\tswitch to GeLU activation\n\
                    d4e5f6g\t0.000000\t0.0\tcrash\tdouble model width (OOM)\n";
        let rows = parse(text).unwrap();
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0].description, "baseline");
        assert_eq!(rows[2].status, Status::Discard);
        assert_eq!(rows[3].status, Status::Crash);
        assert_eq!(rows[3].val_bpb, 0.0);
    }

    #[test]
    fn a_bad_line_names_its_line_number() {
        let text = format!("{HEADER}\nnot a row\n");
        assert!(parse(&text).unwrap_err().contains("line 2"));
        let text = format!("{HEADER}\na1b2c3d\t0.9\t1.0\tmaybe\tx\n");
        assert!(parse(&text).unwrap_err().contains("status"));
    }

    #[test]
    fn a_table_lists_each_row() {
        let text = table(&[row(Status::Keep), row(Status::Discard)]);
        assert!(text.contains("| commit |"));
        assert_eq!(text.matches("b2c3d4e").count(), 2);
        assert_eq!(table(&[]), "(no results yet)");
    }

    proptest! {
        #[test]
        fn a_row_round_trips(
            commit in "[0-9a-f]{7}",
            val in 0.0_f64..5.0,
            mem in 0.0_f64..200.0,
            description in "[ -~]{0,60}",
            keep in any::<bool>(),
        ) {
            let original = ResultRow {
                commit,
                val_bpb: (val * 1e6).round() / 1e6,
                memory_gb: (mem * 10.0).round() / 10.0,
                status: if keep { Status::Keep } else { Status::Discard },
                description: description.trim().to_string(),
            };
            let back = ResultRow::parse_line(&original.to_line()).unwrap();
            prop_assert_eq!(back, original);
        }
    }
}
