//! `cargo xtask perf classify-dhat-allocations`: the port of
//! `autumn-harvest-sqlite/scripts/classify_dhat_allocations.py`.
//!
//! A deterministic allocation-site categorizer for the DHAT capture of
//! `runtime_drive_profile`. It is the auditable tool that produces the
//! "Allocation-site attribution" table in
//! `docs/performance-sqlite-runtime-drive.md`.
//!
//! It exists so that the table is not a hand-eyeballed one-off. The
//! "Reproduce" section of that doc gives the exact `valgrind --tool=dhat`
//! invocation, including the required `--num-callers=30`. A re-run against a
//! freshly captured `dhat.json` reproduces the table byte for byte. After a
//! code change, a new categorization is a one-command re-run, not a
//! re-derivation from scratch.
//!
//! # How it works
//!
//! The JSON output of DHAT (schema version 2) holds a list of "program
//! points" (`pps`). There is one per unique resolved allocation call stack.
//! Each carries total bytes and blocks (`tb` and `tbk`). Each also carries a
//! list of frame indices (`fs`, innermost first) into a flat, symbolized
//! frame table (`ftbl`).
//!
//! The command resolves the stack of each `pp` to frame-name strings. It
//! assigns the stack to exactly one category through a fixed sequence of
//! marker-substring checks against the joined stack text. The sequence is
//! PRECEDENCE-ORDERED. Every `pp` lands in exactly one bucket. So the
//! categories partition the total bytes and blocks of the capture by
//! construction, mutually exclusive and exhaustive. The command asserts
//! this before it prints.
//!
//! # Precedence rationale (checked in this order, first match wins)
//!
//! 1. Clone markers: the `Clone` impls of `Vec`, `BTreeMap` or `String`
//!    anywhere on the stack. `serde_json::Value` objects are backed by a
//!    `BTreeMap`, because this workspace has no `preserve_order` feature. So
//!    the clone of any JSON value recurses into these `Clone` impls.
//!
//!    This workload has THREE distinct call sites that match the same
//!    generic markers. They are separate in the architecture. The split
//!    depends on whether `drive_suspension` or `drain_ready` is ALSO on the
//!    stack.
//!
//!    - `history.clone()`: neither `drive_suspension` nor `drain_ready` is on
//!      the stack. This is the clone in `SqliteRuntime::drive_one_cycle` of the
//!      freshly reloaded, accumulated `Vec<WorkflowEvent>`. The function makes
//!      that clone before it consumes the vector by value. It is the ONE clone
//!      site that the whole finding of the doc is about. It happens once per
//!      DECISION CYCLE.
//!
//!      `drive_one_cycle` itself never appears as a literal frame. It is fully
//!      inlined into `run_until_blocked::{{closure}}`, its sole call site. So
//!      its absence, not its presence, is what a matching stack looks like.
//!    - `activity_input_clone`: `drive_suspension` or `drain_ready` IS on the
//!      stack. These are two smaller, bounded clones of the same
//!      `serde_json::Value` payload. They happen per ACTIVITY, not per cycle.
//!
//!      `persist_scheduled_activity` clones the input of an activity once, at
//!      SCHEDULE time, to build the durable `ActivityScheduled` event. That
//!      call is inlined through `apply_commands` and `apply_schedule_activity`
//!      into `drive_suspension`, its only caller. `worker::drain_ready` clones
//!      the same input a second time, at DRAIN time. It does so right before
//!      it invokes the registered closure of the activity body.
//!
//!      Neither clone is part of the story of N decision cycles that reload
//!      the whole history. Each happens exactly once per dispatched activity.
//!      An earlier cut of this tool put them into `history.clone()`. That
//!      overstated the bucket and how addressable it is. The Methodology note
//!      of the doc gives the numbers of this split.
//!
//!    Both sub-cases are checked FIRST. The clone of an already
//!    deserialized `Value` can pass through frames that also appear on the
//!    deserialize path. One example is `BTreeMap::insert` during the internal
//!    rebuild of a clone. That is a *cloning* cost, not a *parsing* cost. The two causes
//!    need distinct fixes.
//! 2. Deserialize markers (`serde_json::de::`, `serde_json::value::de::`,
//!    `serde_core::de::`): the recursive descent of
//!    `serde_json::Value::deserialize`. The split is
//!    `load_history_deserialize` when the `store::load_history` frame is also
//!    on the stack. That is the JSON payload of an already recorded event,
//!    parsed again on every reload. Any other deserialize call site is
//!    `serde_json_elsewhere`. One example is `queue::claim_next_ready_task_tx`,
//!    which decodes the stored input of a claimed task.
//! 3. `workflow_reexecution`: the user workflow closure
//!    (`sequential_workflow`) or its payload-construction helper
//!    (`activity_payload`) is on the stack. This is the cost of the code of
//!    the AUTHOR, which runs again from the top on every replay cycle. The
//!    bookkeeping of the engine is not part of it.
//! 4. sqlite3-C-frame allocations: a frame is a raw sqlite3 C-engine symbol,
//!    such as the parser, the VDBE or the memory allocator. No Rust marker
//!    above matched first. The split depends on whether `store::load_history`
//!    is ALSO on the stack.
//!
//!    An earlier cut checked `LOAD_HISTORY_MARKER` BEFORE the sqlite3-C check,
//!    unconditionally. So no sqlite3-C allocation could ever count toward the
//!    reload query. The `load_history` check claimed every stack that carried
//!    both markers. At the same time, the doc prose called the whole, much
//!    larger, non-reload `sqlite3_internals` bucket "sqlite3 internals
//!    executing the reload query". The precedence here restores the actual
//!    causal split.
//!
//!    - `sqlite3_internals_reload_query`: sqlite3-C AND `load_history` are
//!      both on the stack. This is the sliver of sqlite3 C-engine work that
//!      the reload query itself reaches.
//!    - `load_history_vec_query`: `load_history` is on the stack, and no
//!      sqlite3-C frame above matched first. This is the growth of the
//!      `Vec<WorkflowEvent>` and the underlying rusqlite query execution. The
//!      per-event JSON deserialize is already carved out above.
//!    - `sqlite3_internals`: sqlite3-C is on the stack, and `load_history` is
//!      not. These are the own allocations of the storage engine from OTHER
//!      call sites: task claims, activity enqueues and timer bookkeeping.
//!      They do not depend on the history reload.
//! 5. `other_uncategorized`: everything else. That holds the setup and teardown of the tokio runtime
//!    and thread pool. It also holds the rusqlite plumbing that is not already
//!    attributed to `load_history`, such as the statement cache and row access. It also holds the UUID-to-string formatting for
//!    diagnostics and the small accessor and glue functions of `store::`,
//!    `queue::`, `worker::` and `context::`. One-time allocations at process
//!    startup are in it too.
//!
//!    This is a real, coherent "infrastructure glue" bucket. An inspection of
//!    every stack in it confirmed that. It is not a sign of an
//!    under-specified rule set. The Methodology note of the doc says why it
//!    is not folded into `sqlite3_internals` or `workflow_reexecution` on a
//!    looser match.
//!
//! # Usage
//!
//! ```text
//! cargo xtask perf classify-dhat-allocations path/to/dhat.json
//! ```

use std::cmp::Reverse;
use std::fmt::Write as _;
use std::process::ExitCode;

use serde_json::Value;
use smop::prelude::*;

const CLONE_MARKERS: [&str; 4] = [
    "Vec<T,A> as core::clone::Clone>::clone",
    "BTreeMap<K,V,A> as core::clone::Clone>::clone",
    "clone::clone_subtree",
    "alloc::string::String as core::clone::Clone>::clone",
];
const SERDE_DESER_MARKERS: [&str; 3] = [
    "serde_json::de::",
    "serde_json::value::de::",
    "serde_core::de::",
];
const LOAD_HISTORY_MARKER: &str = "autumn_harvest_sqlite::store::load_history";
const WORKFLOW_REEXEC_MARKERS: [&str; 2] = [
    "runtime_drive_profile::sequential_workflow",
    "runtime_drive_profile::activity_payload",
];
const SQLITE_C_PREFIXES: [&str; 6] = [
    "sqlite3",
    "yy_reduce",
    "yy_shift",
    "resolveP2Values",
    "sqlite3VdbeExec",
    "sqlite3RunParser",
];
/// The two smaller, per-activity clone sites (see precedence rationale #1).
///
/// Only these two functions ever reach them. The own history clone of
/// `drive_one_cycle` never has either one on its stack.
const ACTIVITY_INPUT_CLONE_MARKERS: [&str; 2] = [
    "autumn_harvest_sqlite::runtime::SqliteRuntime::drive_suspension",
    "autumn_harvest_sqlite::worker::drain_ready",
];

/// Assigns one resolved stack to exactly one category.
pub fn classify(stack: &[&str]) -> &'static str {
    let joined = stack.join("\n");
    let has = |markers: &[&str]| markers.iter().any(|marker| joined.contains(marker));
    if has(&CLONE_MARKERS) {
        if has(&ACTIVITY_INPUT_CLONE_MARKERS) {
            return "activity_input_clone";
        }
        return "history.clone()";
    }
    if has(&SERDE_DESER_MARKERS) {
        if joined.contains(LOAD_HISTORY_MARKER) {
            return "load_history_deserialize";
        }
        return "serde_json_elsewhere";
    }
    if has(&WORKFLOW_REEXEC_MARKERS) {
        return "workflow_reexecution";
    }
    let is_sqlite_c = stack.iter().any(|frame| {
        let symbol = frame
            .split_once(':')
            .map_or(*frame, |(_, rest)| rest)
            .trim();
        SQLITE_C_PREFIXES
            .iter()
            .any(|prefix| symbol.starts_with(prefix))
    });
    let has_load_history = joined.contains(LOAD_HISTORY_MARKER);
    match (is_sqlite_c, has_load_history) {
        (true, true) => "sqlite3_internals_reload_query",
        (false, true) => "load_history_vec_query",
        (true, false) => "sqlite3_internals",
        (false, false) => "other_uncategorized",
    }
}

/// Returns the call site of a `serde_json_elsewhere` stack.
///
/// The leading `0xADDR: ` and the trailing ` (in /path/to/binary)` that DHAT
/// writes on every frame are stripped. The symbol name alone identifies the
/// call site across a rebuild. The address is not stable across binary
/// layouts.
pub fn caller(stack: &[&str]) -> String {
    let frame = stack
        .iter()
        .find(|frame| frame.contains("autumn_harvest_sqlite::"))
        .copied()
        .unwrap_or("???");
    let symbol = frame.split_once(": ").map_or(frame, |(_, rest)| rest);
    symbol.split(" (in ").next().unwrap_or(symbol).to_string()
}

/// Running byte and block totals, in first-seen order like a Python dict.
#[derive(Debug, Default)]
struct Totals(Vec<(String, u128, u128)>);

impl Totals {
    fn add(&mut self, key: &str, bytes: u128, blocks: u128) {
        if let Some(entry) = self.0.iter_mut().find(|entry| entry.0 == key) {
            entry.1 += bytes;
            entry.2 += blocks;
        } else {
            self.0.push((key.to_string(), bytes, blocks));
        }
    }

    /// Returns the entries by descending bytes. Ties keep first-seen order.
    fn sorted(&self) -> Vec<&(String, u128, u128)> {
        let mut entries: Vec<_> = self.0.iter().collect();
        entries.sort_by_key(|entry| Reverse(entry.1));
        entries
    }
}

/// Formats an integer with `,` thousands separators, as `{:,d}` does.
pub fn commas(value: u128) -> String {
    let digits = value.to_string();
    let mut out = String::new();
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            out.push(',');
        }
        out.push(digit);
    }
    out
}

/// Returns `100 * part / whole` as a float, as Python true division does.
#[allow(clippy::cast_precision_loss)]
fn percent(part: u128, whole: u128) -> f64 {
    (100 * part) as f64 / whole as f64
}

/// Returns the frame at `index`, as the Python list index `ftbl[index]` did.
///
/// A negative index counts from the end of the table, as it does in Python.
fn frame_at<'a>(ftbl: &[&'a str], index: i64) -> Option<&'a str> {
    let position = if index < 0 {
        let back = usize::try_from(index.unsigned_abs()).ok()?;
        ftbl.len().checked_sub(back)?
    } else {
        usize::try_from(index).ok()?
    };
    ftbl.get(position).copied()
}

fn field<'a>(value: &'a Value, key: &str) -> Result<&'a Value> {
    value
        .get(key)
        .with_context(|| format!("missing key {key:?}"))
}

fn count(value: &Value, key: &str) -> Result<u128> {
    field(value, key)?
        .as_u64()
        .map(u128::from)
        .with_context(|| format!("{key:?} is not a non-negative integer"))
}

/// Classifies a DHAT capture and renders the table that the doc publishes.
#[cfg(test)]
pub fn report(dhat: &Value) -> Result<String> {
    let mut out = String::new();
    write_report(dhat, &mut out)?;
    Ok(out)
}

/// Writes the table into `out`, line by line, as the Python tool printed it.
///
/// A failure part way leaves the lines before it in `out`. The Python tool
/// had printed those lines before it raised.
pub fn write_report(dhat: &Value, out: &mut String) -> Result<()> {
    let pps = field(dhat, "pps")?
        .as_array()
        .context("\"pps\" is not an array")?;
    let ftbl: Vec<&str> = field(dhat, "ftbl")?
        .as_array()
        .context("\"ftbl\" is not an array")?
        .iter()
        .map(|frame| frame.as_str().context("a frame is not a string"))
        .collect::<Result<_>>()?;

    let mut totals = Totals::default();
    let mut elsewhere_sites = Totals::default();
    let (mut reported_b, mut reported_k) = (0_u128, 0_u128);

    for pp in pps {
        let stack: Vec<&str> = field(pp, "fs")?
            .as_array()
            .context("\"fs\" is not an array")?
            .iter()
            .map(|index| {
                let index = index.as_i64().context("a frame index is not an integer")?;
                frame_at(&ftbl, index)
                    .with_context(|| format!("frame index {index} is out of range"))
            })
            .collect::<Result<_>>()?;
        let (tb, tbk) = (count(pp, "tb")?, count(pp, "tbk")?);
        reported_b += tb;
        reported_k += tbk;
        let category = classify(&stack);
        totals.add(category, tb, tbk);
        if category == "serde_json_elsewhere" {
            elsewhere_sites.add(&caller(&stack), tb, tbk);
        }
    }

    let grand_total_b: u128 = totals.0.iter().map(|entry| entry.1).sum();
    let grand_total_k: u128 = totals.0.iter().map(|entry| entry.2).sum();

    // Every pp lands in exactly one bucket, so the categories partition the
    // totals of the capture by construction. A future edit to `classify` that
    // counts a pp twice or drops one must fail loudly. Otherwise the table
    // silently stops summing to the total that DHAT reports.
    ensure!(
        grand_total_b == reported_b,
        "AssertionError: ({grand_total_b}, {reported_b})"
    );
    ensure!(
        grand_total_k == reported_k,
        "AssertionError: ({grand_total_k}, {reported_k})"
    );

    let _ = writeln!(
        out,
        "{:28} {:>12} {:>7} {:>8} {:>7}",
        "category", "bytes", "%", "blocks", "%"
    );
    for (category, b, k) in totals.sorted() {
        ensure!(
            grand_total_b != 0 && grand_total_k != 0,
            "ZeroDivisionError: division by zero"
        );
        let _ = writeln!(
            out,
            "{category:28} {:>12} {:>6.2}% {:>8} {:>6.2}%",
            commas(*b),
            percent(*b, grand_total_b),
            commas(*k),
            percent(*k, grand_total_k)
        );
    }
    let _ = writeln!(
        out,
        "{:28} {:>12} {:>7} {:>8} {:>7}",
        "TOTAL",
        commas(grand_total_b),
        "100.00%",
        commas(grand_total_k),
        "100.00%"
    );

    if !elsewhere_sites.0.is_empty() {
        out.push('\n');
        out.push_str("serde_json_elsewhere call sites:\n");
        for (site, b, k) in elsewhere_sites.sorted() {
            let _ = writeln!(
                out,
                "  {site}: {} bytes / {} blocks",
                commas(*b),
                commas(*k)
            );
        }
    }
    Ok(())
}

/// Arguments of `cargo xtask perf classify-dhat-allocations`.
#[derive(Debug, Args)]
pub struct Args {
    /// The `dhat.json` capture to classify.
    #[arg(allow_hyphen_values = true)]
    pub dhat: Vec<String>,
}

/// Runs `cargo xtask perf classify-dhat-allocations`.
#[allow(
    clippy::unnecessary_wraps,
    reason = "every xtask command returns the same Result type"
)]
pub fn run(args: Args) -> Result<ExitCode> {
    let dhat = args.dhat;
    let [path] = dhat.as_slice() else {
        eprintln!("usage: cargo xtask perf classify-dhat-allocations <dhat.json>");
        return Ok(ExitCode::from(2));
    };
    // The Python tool exited 1 on an unreadable file and on a failed check.
    let outcome = fs::read_json::<Value, _>(path).and_then(|dhat| {
        let mut out = String::new();
        let written = write_report(&dhat, &mut out);
        print!("{out}");
        written
    });
    match outcome {
        Ok(()) => Ok(ExitCode::SUCCESS),
        Err(error) => {
            eprintln!("{error:#}");
            Ok(ExitCode::FAILURE)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A small synthetic capture with one program point per category.
    pub const FIXTURE: &str = r#"{
  "dhatFileVersion": 2,
  "ftbl": [
    "[root]",
    "0x1: alloc::vec::Vec<T,A> as core::clone::Clone>::clone (in /bin/x)",
    "0x2: autumn_harvest_sqlite::runtime::SqliteRuntime::drive_suspension (in /bin/x)",
    "0x3: serde_json::de::from_str (in /bin/x)",
    "0x4: autumn_harvest_sqlite::store::load_history (in /bin/x)",
    "0x5: autumn_harvest_sqlite::queue::claim_next_ready_task_tx (in /bin/x)",
    "0x6: runtime_drive_profile::sequential_workflow (in /bin/x)",
    "0x7: sqlite3VdbeExec (in /bin/x)",
    "0x8: tokio::runtime::spawn (in /bin/x)",
    "0x9: autumn_harvest_sqlite::worker::poll (in /bin/x)"
  ],
  "pps": [
    {"tb": 1000000, "tbk": 10, "fs": [1, 8]},
    {"tb": 2500, "tbk": 25, "fs": [1, 2]},
    {"tb": 300000, "tbk": 3000, "fs": [3, 4]},
    {"tb": 1234, "tbk": 2, "fs": [3, 5]},
    {"tb": 4321, "tbk": 1, "fs": [3, 9]},
    {"tb": 77, "tbk": 7, "fs": [6]},
    {"tb": 500, "tbk": 5, "fs": [7, 4]},
    {"tb": 600, "tbk": 6, "fs": [4]},
    {"tb": 700, "tbk": 7, "fs": [7]},
    {"tb": 1234, "tbk": 3, "fs": [8, 0]},
    {"tb": 1000, "tbk": 1, "fs": [1]}
  ]
}"#;

    /// The output of `classify_dhat_allocations.py` on [`FIXTURE`].
    pub const EXPECTED: &str = r"category                            bytes       %   blocks       %
history.clone()                 1,001,000  76.29%       11   0.36%
load_history_deserialize          300,000  22.86%    3,000  97.82%
serde_json_elsewhere                5,555   0.42%        3   0.10%
activity_input_clone                2,500   0.19%       25   0.82%
other_uncategorized                 1,234   0.09%        3   0.10%
sqlite3_internals                     700   0.05%        7   0.23%
load_history_vec_query                600   0.05%        6   0.20%
sqlite3_internals_reload_query          500   0.04%        5   0.16%
workflow_reexecution                   77   0.01%        7   0.23%
TOTAL                           1,312,166 100.00%    3,067 100.00%

serde_json_elsewhere call sites:
  autumn_harvest_sqlite::worker::poll: 4,321 bytes / 1 blocks
  autumn_harvest_sqlite::queue::claim_next_ready_task_tx: 1,234 bytes / 2 blocks
";

    #[test]
    fn report_matches_the_python_tool_byte_for_byte() {
        let dhat: Value = serde_json::from_str(FIXTURE).unwrap();
        assert_eq!(report(&dhat).unwrap(), EXPECTED);
    }

    #[test]
    fn classify_follows_the_precedence_order() {
        let clone = "0x1: alloc::string::String as core::clone::Clone>::clone (in x)";
        let deser = "0x2: serde_core::de::Deserialize (in x)";
        assert_eq!(classify(&[clone, deser]), "history.clone()");
        assert_eq!(
            classify(&[clone, "autumn_harvest_sqlite::worker::drain_ready"]),
            "activity_input_clone"
        );
        assert_eq!(
            classify(&[deser, LOAD_HISTORY_MARKER]),
            "load_history_deserialize"
        );
        assert_eq!(classify(&["0x9:   sqlite3_malloc"]), "sqlite3_internals");
        assert_eq!(classify(&["sqlite3_malloc"]), "sqlite3_internals");
        assert_eq!(classify(&["0x9: libc::malloc"]), "other_uncategorized");
        assert_eq!(classify(&[]), "other_uncategorized");
    }

    #[test]
    fn caller_strips_the_address_and_the_binary() {
        assert_eq!(
            caller(&["0x5: autumn_harvest_sqlite::queue::claim (in /bin/x)"]),
            "autumn_harvest_sqlite::queue::claim"
        );
        assert_eq!(caller(&["0x1: other"]), "???");
    }

    #[test]
    fn commas_groups_by_thousands() {
        assert_eq!(commas(0), "0");
        assert_eq!(commas(999), "999");
        assert_eq!(commas(1000), "1,000");
        assert_eq!(commas(1_234_567), "1,234,567");
    }

    #[test]
    fn report_rejects_a_frame_index_out_of_range() {
        let dhat: Value =
            serde_json::from_str(r#"{"ftbl": ["a"], "pps": [{"tb": 1, "tbk": 1, "fs": [3]}]}"#)
                .unwrap();
        assert!(report(&dhat).is_err());
    }

    #[test]
    fn a_negative_frame_index_counts_from_the_end_as_in_python() {
        let ftbl = ["a", "b", "c"];
        assert_eq!(frame_at(&ftbl, -1), Some("c"));
        assert_eq!(frame_at(&ftbl, -3), Some("a"));
        assert_eq!(frame_at(&ftbl, -4), None);
        assert_eq!(frame_at(&ftbl, 2), Some("c"));
        assert_eq!(frame_at(&ftbl, 3), None);
        let dhat: Value =
            serde_json::from_str(r#"{"ftbl": ["a"], "pps": [{"tb": 1, "tbk": 1, "fs": [-1]}]}"#)
                .unwrap();
        assert!(report(&dhat).unwrap().contains("other_uncategorized"));
    }

    #[test]
    fn report_of_an_empty_capture_prints_only_the_header_and_total() {
        let dhat: Value = serde_json::from_str(r#"{"ftbl": [], "pps": []}"#).unwrap();
        let table = report(&dhat).unwrap();
        assert_eq!(table.lines().count(), 2);
        assert!(table.lines().nth(1).unwrap().starts_with("TOTAL"));
    }

    #[test]
    fn a_missing_path_is_a_usage_error() {
        assert_eq!(run(Args { dhat: Vec::new() }).unwrap(), ExitCode::from(2));
    }
}
