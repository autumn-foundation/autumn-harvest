//! The window form of the default claim (issue #2680).
//!
//! The default claim statement holds four candidate scans: `seek_candidate`,
//! `seek_held`, `seek_deep_candidate` and `legacy_candidate`. Postgres
//! initializes every CTE subplan of a statement at executor startup, also a
//! subplan that never runs. On the e2e bench the claim initialized 204 plan
//! nodes per call, and 110 of them never ran. Its executor startup cost as
//! much as its execution. `pg_stat_statements` does not count startup, so the
//! census of issue #2651 did not show it.
//!
//! So a claim with the seek window sends two statements:
//!
//! 1. **The window form.** It keeps the window scan, `seek_held` and
//!    `seek_contended`. Its `candidate` is `seek_candidate` alone. It drops
//!    the deep window, the full scan and the backlog-wide concurrency counts.
//!    It returns one row: the claimed task, or no task, and a flag.
//! 2. **The full statement, unchanged.** It runs only when the flag is set.
//!
//! # Why the split claims the same row
//!
//! In the full statement, `candidate` is the union of the three scans. The
//! deep gate and the full-scan gate each start with
//! `NOT EXISTS (SELECT 1 FROM seek_candidate)`. So when `seek_candidate`
//! holds a row, the two other scans are empty, and `candidate` is
//! `seek_candidate`. The window form then claims the same row from the same
//! snapshot.
//!
//! With `seek_candidate` empty, the deep scan runs only when
//! `seek_contended` holds a row. The full scan runs only when `seek_bounds`
//! holds a row, or when ageing is on. The flag is the disjunction of those
//! three tests, after an empty `seek_candidate`. So a clear flag with no task
//! proves that the full statement claims no row either.
//!
//! The flag over-approximates in one case only: a held head that the deep
//! window reads to its end. The full statement then finds no row, and the
//! claim pays one more statement. It never takes a different row.
//!
//! # Why the full statement after a set flag is safe
//!
//! A set flag means that `seek_candidate` was empty. The window form then
//! locked no row, debited no token and took no advisory lock. The
//! `claimed` update matched nothing. So the transaction holds only its
//! `SET LOCAL` settings and, for an owned claim, its holder setting. The full
//! statement sets the holder again to the same value. It reads a new
//! snapshot under `READ COMMITTED`, so it is a fresh claim attempt. A claim
//! attempt that starts later is always allowed to see newer rows.
//!
//! `formal/verus/src/claim_window.rs` proves the decision for every input.
//!
//! # How the text is built
//!
//! [`window_claim_sql`] derives the window form from the final text of a full
//! statement, after every splice. So the DR fence, the kind predicate and the
//! owned gate are already in it. The function parses the top-level CTE list.
//! It asserts that each CTE it drops exists once, and that no kept CTE names
//! a dropped one. An edit that breaks either rule panics at first use, and
//! `window_claim_sql_holds_for_every_variant` runs it for every variant.

/// The CTEs that only the deep window and the full scan read.
///
/// `concurrency_pending_keys` and `concurrency_running_counts` serve the full
/// scan alone. The window scan counts its keys in `seek_running_counts`.
pub(super) const WINDOW_DROPPED_CTES: [&str; 9] = [
    "concurrency_pending_keys",
    "concurrency_running_counts",
    "seek_deep_rows",
    "seek_deep_window",
    "seek_deep_bounds",
    "seek_deep_pending_keys",
    "seek_deep_running_counts",
    "seek_deep_candidate",
    "legacy_candidate",
];

/// The body of `candidate` in every full statement that has the window.
const FULL_CANDIDATE_BODY: &str = "SELECT * FROM seek_candidate \
     UNION ALL SELECT * FROM seek_deep_candidate \
     UNION ALL SELECT * FROM legacy_candidate";

/// The body of `candidate` in the window form.
const WINDOW_CANDIDATE_BODY: &str = "SELECT * FROM seek_candidate";

/// The name of the flag column of the window form.
pub(super) const WINDOW_FALLBACK_COLUMN: &str = "claim_fallback";

/// The flag of the window form: true when the full statement could claim a
/// row that the window form did not.
///
/// The three disjuncts are the gates of the deep scan and the full scan in
/// the full statement, without their `seek_candidate` test. See the module
/// documentation.
const WINDOW_FALLBACK_SQL: &str = "NOT EXISTS (SELECT 1 FROM seek_candidate) \
     AND (EXISTS (SELECT 1 FROM seek_contended) \
          OR EXISTS (SELECT 1 FROM seek_bounds) \
          OR ($4::BIGINT IS NOT NULL AND $4::BIGINT > 0))";

/// What a claim does after its window statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WindowStep {
    /// The window statement claimed a row.
    Claimed,
    /// No row is free. The full statement would claim no row either.
    Empty,
    /// Send the full statement.
    Fallback,
}

/// The next step of a claim after its window statement (issue #2680).
///
/// `claimed` is true when the statement returned a task. `fallback` is the
/// flag column. A claimed row wins over the flag. The flag is false whenever
/// `seek_candidate` holds a row, so the two are never both true. The order
/// only keeps the claim safe if that ever changes.
///
/// `formal/verus/src/claim_window.rs` holds the proved copy.
// mirror: formal/verus/src/claim_window.rs window_step
pub(super) const fn window_step(claimed: bool, fallback: bool) -> WindowStep {
    if claimed {
        WindowStep::Claimed
    } else if fallback {
        WindowStep::Fallback
    } else {
        WindowStep::Empty
    }
}

/// One top-level CTE of a claim statement.
#[derive(Debug)]
struct Cte<'a> {
    name: &'a str,
    /// The whole definition, from the name to the closing parenthesis.
    text: &'a str,
    /// The text inside the outer parentheses.
    body: &'a str,
}

/// Find the parenthesis that closes the one at byte `open` of `sql`.
///
/// It skips single-quoted literals, with `''` as an escaped quote. The claim
/// text holds no comment and no dollar quote. `window_claim_sql` asserts
/// both.
fn closing_paren(sql: &str, open: usize) -> usize {
    let bytes = sql.as_bytes();
    let mut depth = 0_usize;
    let mut in_literal = false;
    let mut i = open;
    while i < bytes.len() {
        let byte = bytes[i];
        if in_literal {
            if byte == b'\'' {
                if bytes.get(i + 1) == Some(&b'\'') {
                    i += 1;
                } else {
                    in_literal = false;
                }
            }
        } else if byte == b'\'' {
            in_literal = true;
        } else if byte == b'(' {
            depth += 1;
        } else if byte == b')' {
            depth -= 1;
            if depth == 0 {
                return i;
            }
        }
        i += 1;
    }
    panic!("claim query has an unbalanced parenthesis at byte {open}");
}

/// Split a `WITH` statement into its top-level CTEs and its final query.
fn split_ctes(sql: &str) -> (Vec<Cte<'_>>, &str) {
    let mut rest = sql
        .strip_prefix("WITH ")
        .unwrap_or_else(|| panic!("claim query starts with WITH"));
    let mut ctes = Vec::new();
    loop {
        let trimmed = rest.trim_start();
        let start = sql.len() - trimmed.len();
        let name_end = trimmed
            .find(" AS ")
            .unwrap_or_else(|| panic!("claim query CTE at byte {start} has no AS"));
        let name = &trimmed[..name_end];
        assert!(
            !name.is_empty() && name.bytes().all(|b| b.is_ascii_lowercase() || b == b'_'),
            "claim query CTE name {name:?} is not a plain identifier"
        );
        let open = start
            + trimmed
                .find('(')
                .unwrap_or_else(|| panic!("claim query CTE {name} has no body"));
        let close = closing_paren(sql, open);
        ctes.push(Cte {
            name,
            text: &sql[start..=close],
            body: sql[open + 1..close].trim(),
        });
        let after = sql[close + 1..].trim_start();
        match after.strip_prefix(',') {
            Some(next) => rest = next,
            None => return (ctes, after),
        }
    }
}

/// True when `text` names `ident` as a whole word.
fn names(text: &str, ident: &str) -> bool {
    let word = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    text.match_indices(ident).any(|(at, _)| {
        let before = text.as_bytes().get(at.wrapping_sub(1)).copied();
        let after = text.as_bytes().get(at + ident.len()).copied();
        !before.is_some_and(word) && !after.is_some_and(word)
    })
}

/// Collapse each run of whitespace to one space.
fn squash(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// The window form of one full claim statement (issue #2680).
///
/// `full` is the final text of a default claim with the seek window: any
/// kind, with or without the DR fence, with or without the owned gate. The
/// window form binds the same parameters. It returns the columns of the full
/// statement, then [`WINDOW_FALLBACK_COLUMN`]. It always returns one row.
/// The task columns are `NULL` when it claimed no row.
///
/// # Panics
///
/// Panics when `full` does not have the shape that the module documentation
/// describes. Every caller passes a constant text, so a panic means an edit
/// broke the shape. The guard tests run it for every variant.
#[must_use]
pub(super) fn window_claim_sql(full: &str) -> String {
    assert!(
        !full.contains("--") && !full.contains("/*") && !full.contains("$$"),
        "the window form needs a claim text with no comment and no dollar quote"
    );
    let (ctes, tail) = split_ctes(full);
    for dropped in WINDOW_DROPPED_CTES {
        assert_eq!(
            ctes.iter().filter(|c| c.name == dropped).count(),
            1,
            "claim query must define CTE {dropped} once"
        );
    }
    let mut kept = Vec::with_capacity(ctes.len());
    for cte in &ctes {
        if WINDOW_DROPPED_CTES.contains(&cte.name) {
            continue;
        }
        if cte.name == "candidate" {
            assert_eq!(
                squash(cte.body),
                FULL_CANDIDATE_BODY,
                "claim query candidate must be the union of the three scans"
            );
            kept.push(format!("candidate AS ( {WINDOW_CANDIDATE_BODY} )"));
            continue;
        }
        for dropped in WINDOW_DROPPED_CTES {
            assert!(
                !names(cte.text, dropped),
                "kept claim CTE {} reads dropped CTE {dropped}",
                cte.name
            );
        }
        kept.push(cte.text.to_owned());
    }
    for needed in ["seek_candidate", "seek_contended", "seek_bounds", "claimed"] {
        assert!(
            ctes.iter().any(|c| c.name == needed),
            "claim query must define CTE {needed}"
        );
    }
    let columns = tail
        .strip_prefix("SELECT ")
        .and_then(|rest| rest.trim_end().strip_suffix(" FROM claimed"))
        .unwrap_or_else(|| panic!("claim query must end with SELECT <columns> FROM claimed"));
    assert!(
        !names(tail, "legacy_candidate") && !names(tail, "seek_deep_candidate"),
        "the final query of the claim reads a dropped CTE"
    );
    format!(
        "WITH {}, claim_window_fallback AS MATERIALIZED ( SELECT {WINDOW_FALLBACK_SQL} AS \
         {WINDOW_FALLBACK_COLUMN} ) SELECT {columns}, fb.{WINDOW_FALLBACK_COLUMN} \
         FROM claim_window_fallback fb LEFT JOIN claimed ON TRUE",
        kept.join(", ")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_claimed_row_wins_and_the_flag_picks_the_full_statement() {
        assert_eq!(window_step(true, false), WindowStep::Claimed);
        assert_eq!(window_step(true, true), WindowStep::Claimed);
        assert_eq!(window_step(false, true), WindowStep::Fallback);
        assert_eq!(window_step(false, false), WindowStep::Empty);
    }

    #[test]
    fn the_parser_skips_parentheses_inside_literals() {
        let sql = "WITH a AS ( SELECT ')' AS x ), b AS MATERIALIZED ( SELECT '(''' FROM a ) \
                   SELECT x FROM b";
        let (ctes, tail) = split_ctes(sql);
        let found: Vec<_> = ctes.iter().map(|c| (c.name, c.body)).collect();
        assert_eq!(
            found,
            vec![("a", "SELECT ')' AS x"), ("b", "SELECT '(''' FROM a")]
        );
        assert_eq!(tail, "SELECT x FROM b");
    }

    #[test]
    fn a_name_matches_only_as_a_whole_word() {
        assert!(names("FROM seek_deep_window w", "seek_deep_window"));
        assert!(!names("FROM seek_deep_window_x w", "seek_deep_window"));
        assert!(!names("FROM my_legacy_candidate", "legacy_candidate"));
        assert!(names(
            "(SELECT 1 FROM legacy_candidate)",
            "legacy_candidate"
        ));
    }

    /// The `$n` binds that `sql` names, in ascending order.
    fn binds(sql: &str) -> Vec<u32> {
        let mut found: Vec<u32> = sql
            .match_indices('$')
            .filter_map(|(at, _)| {
                let digits: String = sql[at + 1..]
                    .chars()
                    .take_while(char::is_ascii_digit)
                    .collect();
                digits.parse().ok()
            })
            .collect();
        found.sort_unstable();
        found.dedup();
        found
    }

    #[test]
    fn window_claim_sql_holds_for_every_variant() {
        use crate::queue::{
            ClaimFairness, TaskType, claim_query_for, owned_claim_query_for, window_claim_query_for,
        };
        for kind in [None, Some(TaskType::Workflow), Some(TaskType::Activity)] {
            for fenced in [false, true] {
                for owned in [false, true] {
                    let case = format!("kind {kind:?}, fenced {fenced}, owned {owned}");
                    let full = if owned {
                        owned_claim_query_for(kind, fenced, ClaimFairness::Off)
                    } else {
                        claim_query_for(kind, fenced, ClaimFairness::Off)
                    };
                    let window = window_claim_query_for(kind, fenced, owned);
                    for dropped in WINDOW_DROPPED_CTES {
                        assert!(names(full, dropped), "{case}: the full form has {dropped}");
                        assert!(
                            !names(window, dropped),
                            "{case}: the window keeps {dropped}"
                        );
                    }
                    for kept in ["seek_heads", "seek_candidate", "seek_held", "claimed"] {
                        assert!(names(window, kept), "{case}: the window drops {kept}");
                    }
                    assert_eq!(
                        binds(window),
                        binds(full),
                        "{case}: both forms bind the same parameters"
                    );
                    assert!(
                        window.ends_with(&format!(
                            "fb.{WINDOW_FALLBACK_COLUMN} FROM claim_window_fallback fb \
                             LEFT JOIN claimed ON TRUE"
                        )),
                        "{case}: the window form returns its flag"
                    );
                    if fenced {
                        assert!(window.contains("CROSS JOIN fence "), "{case}: fence kept");
                    }
                    if owned {
                        assert!(window.starts_with("WITH owner_gate"), "{case}: gate kept");
                    }
                    // Each copy of the window form is smaller than its full form.
                    assert!(
                        window.len() * 10 < full.len() * 7,
                        "{case}: {}",
                        window.len()
                    );
                }
            }
        }
    }

    /// The premise of the split: both deeper scans run only after an empty
    /// `seek_candidate`, and their gates are the flag's disjuncts.
    #[test]
    fn every_deeper_scan_of_the_full_form_waits_for_an_empty_window() {
        let full = crate::queue::claim_task_query();
        let (ctes, _) = split_ctes(full);
        let body = |name: &str| {
            squash(
                ctes.iter()
                    .find(|c| c.name == name)
                    .unwrap_or_else(|| panic!("CTE {name}"))
                    .body,
            )
        };
        let deep = body("seek_deep_candidate");
        assert!(deep.contains(
            "AND NOT EXISTS (SELECT 1 FROM seek_candidate) \
             AND EXISTS (SELECT 1 FROM seek_contended)"
        ));
        let legacy = body("legacy_candidate");
        assert!(legacy.contains(
            "AND NOT EXISTS (SELECT 1 FROM seek_candidate) \
             AND NOT EXISTS (SELECT 1 FROM seek_deep_candidate) \
             AND (EXISTS (SELECT 1 FROM seek_bounds) \
             AND (NOT EXISTS (SELECT 1 FROM seek_contended) \
             OR EXISTS (SELECT 1 FROM seek_deep_bounds)) \
             OR ($4::BIGINT IS NOT NULL AND $4::BIGINT > 0))"
        ));
        assert_eq!(body("candidate"), FULL_CANDIDATE_BODY);
    }

    #[test]
    #[should_panic(expected = "reads dropped CTE legacy_candidate")]
    fn a_kept_cte_that_reads_a_dropped_one_panics() {
        let mut sql = String::from("WITH ");
        for name in WINDOW_DROPPED_CTES {
            sql.push_str(name);
            sql.push_str(" AS ( SELECT 1 ), ");
        }
        sql.push_str(
            "seek_bounds AS ( SELECT 1 ), seek_contended AS ( SELECT 1 ), \
             seek_candidate AS ( SELECT 1 FROM legacy_candidate ), \
             candidate AS ( SELECT * FROM seek_candidate UNION ALL SELECT * FROM \
             seek_deep_candidate UNION ALL SELECT * FROM legacy_candidate ), \
             claimed AS ( SELECT 1 ) SELECT claimed.id FROM claimed",
        );
        let _ = window_claim_sql(&sql);
    }
}
