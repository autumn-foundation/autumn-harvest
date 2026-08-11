//! Guards that keep `docs/performance.md`'s prose tied to its own tables.
//!
//! That page is the published baseline for attributing claim-path costs, and
//! its narrative quotes percentages, sample counts and multipliers that are
//! supposed to be read straight off the tables above them. Twice now the
//! tables were regenerated from a fresh benchmark run while the surrounding
//! prose kept the previous run's figures — the second time badly enough that
//! two scenarios swapped which was slower. Nothing caught either drift,
//! because a stale number in prose still renders perfectly.
//!
//! These tests make the tables authoritative. They parse the published tables
//! out of the Markdown and assert that (a) each table's own percentage column
//! is arithmetically consistent with its own latency column, and (b) every
//! per-gate bullet quotes the figure the table publishes for that gate.
//!
//! Deliberately *not* covered: the reproducibility paragraph and the
//! "earlier revision" narrative, which quote other runs on purpose and say so.
//! A guard that forbade those would force the page to hide the run-to-run
//! spread, which is the most operationally honest thing on it.

use std::path::{Path, PathBuf};

fn performance_doc_path() -> PathBuf {
    // CARGO_MANIFEST_DIR is `<repo>/autumn-harvest`; the doc lives at the root.
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate directory must have a parent")
        .join("docs/performance.md")
}

fn read_performance_doc() -> String {
    let path = performance_doc_path();
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// One row of the per-gate attribution table.
#[derive(Debug, Clone)]
struct GateRow {
    gate: String,
    /// The `n` column: how many samples the percentiles describe.
    samples: u32,
    p50_ms: f64,
    /// The `p50 vs` column, as a whole-percent integer (may be negative).
    delta_pct: i64,
    /// The `vs what` column: which row `delta_pct` is measured against.
    comparand: String,
}

/// Strip the ASCII spaces used as thousands separators (`1 324.88`).
fn parse_number(field: &str) -> Option<f64> {
    field.replace(' ', "").parse().ok()
}

/// Parse `**+1383%**` / `+2%` / `−1%` into a signed whole percent.
///
/// The page uses U+2212 MINUS SIGN, not an ASCII hyphen, so both are accepted.
fn parse_percent(field: &str) -> Option<i64> {
    let cleaned = field
        .replace("**", "")
        .replace('\u{2212}', "-")
        .replace(' ', "");
    let digits = cleaned.strip_suffix('%')?;
    digits.parse().ok()
}

/// Pull the gate name out of a cell like ``` `paused_rows` ⚠ ```.
fn parse_gate_name(field: &str) -> Option<String> {
    let start = field.find('`')?;
    let rest = &field[start + 1..];
    let end = rest.find('`')?;
    Some(rest[..end].to_string())
}

/// Split one Markdown table row into its cells, if it is a 7-column row.
///
/// The per-gate table's shape is
/// `gate | seeded rows | claimable | n | p50 ms | p50 vs | vs what`.
fn gate_table_cells(line: &str) -> Option<Vec<&str>> {
    let trimmed = line.trim();
    if !trimmed.starts_with('|') {
        return None;
    }
    let cells: Vec<&str> = trimmed
        .trim_matches('|')
        .split('|')
        .map(str::trim)
        .collect();
    (cells.len() == 7).then_some(cells)
}

fn parse_gate_table(doc: &str) -> Vec<GateRow> {
    let mut rows = Vec::new();
    for line in doc.lines() {
        let Some(cells) = gate_table_cells(line) else {
            continue;
        };
        let (Some(gate), Some(samples), Some(p50)) = (
            parse_gate_name(cells[0]),
            parse_number(cells[3]),
            parse_number(cells[4]),
        ) else {
            continue;
        };
        // `baseline` publishes an em-dash rather than a delta; it is the origin.
        let Some(delta_pct) = parse_percent(cells[5]) else {
            continue;
        };
        let Some(comparand) = parse_gate_name(cells[6]) else {
            continue;
        };
        rows.push(GateRow {
            gate,
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            samples: samples as u32,
            p50_ms: p50,
            delta_pct,
            comparand,
        });
    }
    rows
}

/// The `baseline` row is the table's origin, so it publishes an em-dash rather
/// than a delta and `parse_gate_table` skips it. Read its p50 separately.
fn baseline_p50(doc: &str) -> f64 {
    doc.lines()
        .filter(|line| line.trim_start().starts_with("| `baseline`"))
        .find_map(|line| parse_number(gate_table_cells(line)?[4]))
        .expect("docs/performance.md must publish a `baseline` row in the per-gate table")
}

#[test]
fn gate_table_percentages_agree_with_its_own_latency_column() {
    let doc = read_performance_doc();
    let rows = parse_gate_table(&doc);
    assert!(
        rows.len() >= 7,
        "expected the per-gate table to parse; got {} rows",
        rows.len()
    );

    let baseline = baseline_p50(&doc);
    let by_name: std::collections::BTreeMap<&str, f64> =
        rows.iter().map(|r| (r.gate.as_str(), r.p50_ms)).collect();

    for row in &rows {
        let comparand_p50 = if row.comparand == "baseline" {
            baseline
        } else {
            *by_name.get(row.comparand.as_str()).unwrap_or_else(|| {
                panic!(
                    "`{}` is measured against `{}`, which is not a row of the table",
                    row.gate, row.comparand
                )
            })
        };
        #[allow(clippy::cast_possible_truncation)]
        let computed = ((row.p50_ms / comparand_p50 - 1.0) * 100.0).round() as i64;
        assert_eq!(
            computed, row.delta_pct,
            "`{}` publishes {}% against `{}`, but {:.2} ms / {:.2} ms is {}%. \
             The percentage column and the latency column came from different runs.",
            row.gate, row.delta_pct, row.comparand, row.p50_ms, comparand_p50, computed
        );
    }
}

/// The per-gate bullets under "What each row exercises" must quote the table.
///
/// This is the exact drift a reviewer caught twice: the table was regenerated
/// and these bullets were not, leaving the page asserting two different costs
/// for the same scenario a hundred lines apart.
#[test]
fn per_gate_bullets_quote_the_published_gate_table() {
    let doc = read_performance_doc();
    let rows = parse_gate_table(&doc);
    let by_name: std::collections::BTreeMap<&str, &GateRow> =
        rows.iter().map(|r| (r.gate.as_str(), r)).collect();

    let mut checked = 0_usize;
    for line in doc.lines() {
        let trimmed = line.trim();
        // Shape: * **`gate_name` (+644%)** — prose
        //    or: * **`paused_rows` (+1% vs the control)** — prose
        let Some(rest) = trimmed.strip_prefix("* **`") else {
            continue;
        };
        let Some(name_end) = rest.find('`') else {
            continue;
        };
        let gate = &rest[..name_end];
        let Some(row) = by_name.get(gate) else {
            continue;
        };
        let after = &rest[name_end + 1..];
        let Some(open) = after.find('(') else {
            continue;
        };
        let Some(close) = after.find(')') else {
            continue;
        };
        let inside = &after[open + 1..close];

        let against_control = inside.contains("vs the control");
        let percent_text = inside.replace("vs the control", "");
        let Some(quoted) = parse_percent(percent_text.trim()) else {
            continue;
        };

        assert_eq!(
            quoted, row.delta_pct,
            "the `{gate}` bullet quotes {quoted}%, but the table publishes {}% for it. \
             Regenerate the prose from the table.",
            row.delta_pct
        );

        // A bullet says "vs the control" exactly when the table measures that
        // row against something other than `baseline`. Getting this wrong
        // silently reattributes the number to the wrong comparand.
        let table_uses_control = row.comparand != "baseline";
        assert_eq!(
            against_control,
            table_uses_control,
            "the `{gate}` bullet {} \"vs the control\", but the table measures it against `{}`.",
            if against_control {
                "says"
            } else {
                "does not say"
            },
            row.comparand
        );
        checked += 1;
    }

    assert!(
        checked >= 6,
        "expected to check the per-gate bullets against the table; matched only {checked}. \
         Did the bullet list change shape?"
    );
}

/// The truncated rows must describe the sample count they actually published.
#[test]
fn truncated_sample_counts_in_prose_match_the_table() {
    let doc = read_performance_doc();
    let rows = parse_gate_table(&doc);
    let by_name: std::collections::BTreeMap<&str, &GateRow> =
        rows.iter().map(|r| (r.gate.as_str(), r)).collect();

    let double_backlog = by_name
        .get("double_backlog")
        .expect("the control row must exist");
    let paused_rows = by_name
        .get("paused_rows")
        .expect("the paused_rows row must exist");

    // The control-vs-anti-join discussion quotes both n values as `n=A/B`.
    let needle = format!("n={}/{}", double_backlog.samples, paused_rows.samples);
    assert!(
        doc.contains(&needle),
        "the control discussion must quote the published sample counts as `{needle}`; \
         the table shows double_backlog n={} and paused_rows n={}.",
        double_backlog.samples,
        paused_rows.samples
    );
}
