//! Keep a keyed task row statement on its key index.
//!
//! Many statements find `harvest_task_queue` rows by a key, then guard them
//! on `state` and `worker_id`. The key is `id` or `workflow_exec_id`. A
//! partial index matches each such guard. Both paths estimate few rows, so
//! the planner can pick the small partial index over the key index. That
//! path reads each index entry that matches the guard, then filters on the
//! key. A cached generic plan keeps that choice.
//!
//! The one-row claims test `state` beside an id test in the same way.
//!
//! `row_guard_sql!` and `row_guard` hide such a guard from the planner.
//! [`guarded_statements`] lists each raw statement in scope and a sample of
//! each Diesel guard shape. A DB test checks that the generic and the custom
//! plan of each one read the table through its key index. See
//! `docs/performance-claim-release-guard.md` and
//! `docs/performance-claim-by-id-guard.md`.

#[cfg(feature = "db")]
use diesel::QueryDsl;
#[cfg(feature = "db")]
use diesel::expression::Expression;
#[cfg(feature = "db")]
use diesel::pg::Pg;
#[cfg(feature = "db")]
use diesel::sql_types::{Bool, BoolOrNullableBool};

/// Wrap the guard of a keyed statement so that no index can match it.
///
/// `CASE WHEN g THEN true END` is true when `g` is true and NULL in all
/// other cases. `WHERE` drops a NULL row as it drops a false row, so the
/// statement keeps the same rows. The planner cannot use a `CASE` as an
/// index condition. It also cannot prove a partial index predicate from
/// it. So it finds the rows through the key test and its index.
///
/// Use it only beside a key test, such as `id = $1` or
/// `workflow_exec_id = ANY($1)`. A scan without
/// a key needs its partial index. Use it only as a top-level `AND` term.
/// `NOT` or `OR` on a NULL result changes the rows.
macro_rules! row_guard_sql {
    ($guard:literal) => {
        concat!("(CASE WHEN ", $guard, " THEN true END)")
    };
}
pub(crate) use row_guard_sql;

/// The Diesel form of `row_guard_sql!`.
///
/// The `true` goes as a bind, so the statement keeps a static query id and
/// stays in the statement cache. The rules of `row_guard_sql!` apply.
#[cfg(feature = "db")]
pub(crate) fn row_guard<G>(guard: G) -> diesel::dsl::case_when<G, bool, Bool>
where
    G: Expression,
    G::SqlType: BoolOrNullableBool,
{
    diesel::dsl::case_when::<_, _, Bool>(guard, true)
}

/// The primary key of `harvest_task_queue`.
#[cfg(feature = "db")]
#[doc(hidden)]
pub const PRIMARY_KEY: &str = "harvest_task_queue_pkey";

/// The index of every task row of a run, by `workflow_exec_id`.
#[cfg(feature = "db")]
#[doc(hidden)]
pub const EXECUTION_INDEX: &str = "idx_harvest_tq_workflow";

/// The index of the one workflow task row of each run. A statement reads it
/// when it keeps `task_type = 'workflow'` as a plain test.
#[cfg(feature = "db")]
#[doc(hidden)]
pub const WORKFLOW_TASK_INDEX: &str = "idx_harvest_tq_workflow_task";

/// The index of the task rows of one activity of a run.
#[cfg(feature = "db")]
#[doc(hidden)]
pub const ACTIVITY_INDEX: &str = "idx_harvest_tq_activity_id";

/// One guarded statement and the types of its parameters.
#[cfg(feature = "db")]
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GuardedStatement {
    /// A name for test output.
    pub name: &'static str,
    /// The SQL text, with `$n` parameters.
    pub sql: String,
    /// The SQL type of each parameter, in order.
    pub param_types: Vec<&'static str>,
    /// True when `sql` is the production text with a `CASE` guard. The
    /// differential test runs it in both forms. A Diesel entry is false: it
    /// has the production guard, but its other clauses are a sample. A
    /// timeout batch entry is false too: it loads its ids through the primary
    /// key in a materialized CTE.
    pub raw: bool,
    /// The full index that finds the rows. The plan may also read the
    /// primary key.
    pub index: &'static str,
}

#[cfg(feature = "db")]
impl GuardedStatement {
    fn raw(name: &'static str, sql: &str, param_types: &[&'static str]) -> Self {
        Self {
            name,
            sql: sql.to_owned(),
            param_types: param_types.to_vec(),
            raw: true,
            index: PRIMARY_KEY,
        }
    }

    /// The same statement, with its rows found through `index`.
    const fn via(mut self, index: &'static str) -> Self {
        self.index = index;
        self
    }

    fn diesel<Q>(name: &'static str, query: &Q, param_types: &[&'static str]) -> Self
    where
        Q: diesel::query_builder::QueryFragment<Pg>,
    {
        let text = diesel::debug_query::<Pg, _>(query).to_string();
        let sql = text
            .split_once(" -- binds: ")
            .map_or(text.as_str(), |(sql, _)| sql);
        Self {
            raw: false,
            ..Self::raw(name, sql, param_types)
        }
    }
}

/// Each by-id statement that the plan test checks.
///
/// The raw statements are the production text. The Diesel statements are
/// samples of each production guard shape. `queue::fence` and
/// `queue::claim_held` are the production helpers. A source walk in the
/// tests below checks each Diesel call site.
#[cfg(feature = "db")]
#[doc(hidden)]
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn guarded_statements() -> Vec<GuardedStatement> {
    use crate::error::CapabilityMissPhase;
    use crate::queue;
    use crate::schema::harvest_task_queue::dsl;
    use diesel::{BoolExpressionMethods, ExpressionMethods};
    use jiff_diesel::ToDiesel;

    const ID_WORKER: &[&str] = &["uuid", "text"];
    const CAPABILITY_MISS: &[&str] = &[
        "uuid",
        "text",
        "double precision",
        "integer",
        "text",
        "integer",
    ];

    let mut statements = vec![
        GuardedStatement::raw(
            "release_claim_if_queue_paused",
            crate::queue_pause::release_claim_if_queue_paused_query(),
            ID_WORKER,
        ),
        GuardedStatement::raw(
            "release_claim",
            crate::queue_pause::release_claim_query(),
            ID_WORKER,
        ),
        GuardedStatement::raw(
            "release_claim_if_activity_paused",
            crate::activity_pause::release_claim_if_activity_paused_query(),
            ID_WORKER,
        ),
        GuardedStatement::raw(
            "release_claim_if_workflow_paused",
            crate::execution::release_claim_if_workflow_paused_query(),
            ID_WORKER,
        ),
        GuardedStatement::raw(
            "release_claim_if_over_cap",
            queue::release_claim_if_over_cap_query(),
            ID_WORKER,
        ),
        GuardedStatement::raw(
            "release_for_capability_miss_after_handler",
            queue::release_task_for_capability_miss_query(CapabilityMissPhase::AfterHandler),
            CAPABILITY_MISS,
        ),
        GuardedStatement::raw(
            "release_for_capability_miss_before_handler",
            queue::release_task_for_capability_miss_query(CapabilityMissPhase::BeforeHandler),
            CAPABILITY_MISS,
        ),
        GuardedStatement::raw(
            "release_for_capability_miss_during_handler",
            queue::release_task_for_capability_miss_query(CapabilityMissPhase::DuringHandler),
            CAPABILITY_MISS,
        ),
        GuardedStatement::raw(
            "reset_capability_misses_after_inline_progress",
            queue::reset_capability_misses_after_inline_progress_query(),
            ID_WORKER,
        ),
        GuardedStatement::raw(
            "read_capability_miss_state",
            queue::read_capability_miss_state_query(),
            &["uuid", "text", "text"],
        ),
        GuardedStatement::raw(
            "release_suspended_workflow_claim",
            queue::release_suspended_workflow_claim_query(),
            &["uuid", "text", "integer", "integer"],
        ),
        GuardedStatement::raw(
            "pending_hint_rows",
            queue::pending_hint_rows_query(),
            &["uuid[]"],
        ),
        GuardedStatement::raw(
            "requeue_orphan",
            crate::poison_pill::requeue_orphan_stmt(),
            &[
                "uuid",
                "text",
                "integer",
                "integer",
                "bigint",
                "timestamptz",
            ],
        ),
    ];
    for reset in [true, false] {
        statements.push(GuardedStatement::raw(
            if reset {
                "park_workflow_task_sticky_reset"
            } else {
                "park_workflow_task_sticky_keep"
            },
            queue::park_workflow_task_sticky_query(reset),
            &["uuid", "text", "interval"],
        ));
        statements.push(GuardedStatement::raw(
            if reset {
                "park_workflow_task_reset"
            } else {
                "park_workflow_task_keep"
            },
            queue::park_workflow_task_query(reset),
            &["uuid"],
        ));
    }

    statements.extend([
        GuardedStatement::raw(
            "wake_repend",
            queue::primary_repend_workflow_task_query(),
            &["uuid", "timestamptz"],
        )
        .via(WORKFLOW_TASK_INDEX),
        GuardedStatement::raw(
            "wake_fallback",
            queue::wake_requested_fallback_query(),
            &["uuid"],
        )
        .via(WORKFLOW_TASK_INDEX),
        GuardedStatement::raw(
            "wake_already_due",
            queue::already_due_workflow_task_query(),
            &["uuid", "timestamptz"],
        )
        .via(WORKFLOW_TASK_INDEX),
        GuardedStatement::raw("owner_keys", queue::OWNER_KEYS_SQL, &["uuid[]"])
            .via(WORKFLOW_TASK_INDEX),
        GuardedStatement::raw(
            "shift_schedule_to_close_on_resume",
            crate::execution::shift_schedule_to_close_on_resume_query(),
            &["bigint", "uuid"],
        )
        .via(EXECUTION_INDEX),
        GuardedStatement::raw(
            "migration_staged_task",
            crate::shard_rebalance::STAGED_TASK_SQL,
            &["uuid"],
        )
        .via(WORKFLOW_TASK_INDEX),
        GuardedStatement::raw(
            "migration_signal_repend",
            crate::shard_rebalance::SIGNAL_REPEND_SQL,
            &["uuid"],
        )
        .via(WORKFLOW_TASK_INDEX),
    ]);

    for (reason, batch, probe) in crate::timeout::timeout_batch_statements() {
        let (batch_name, probe_name) = match reason {
            crate::timeout::TimeoutReason::Heartbeat => {
                ("timeout_batch_heartbeat", "timeout_probe_heartbeat")
            }
            crate::timeout::TimeoutReason::StartToClose => (
                "timeout_batch_start_to_close",
                "timeout_probe_start_to_close",
            ),
            crate::timeout::TimeoutReason::ScheduleToStart => (
                "timeout_batch_schedule_to_start",
                "timeout_probe_schedule_to_start",
            ),
            crate::timeout::TimeoutReason::ScheduleToClose => (
                "timeout_batch_schedule_to_close",
                "timeout_probe_schedule_to_close",
            ),
        };
        for (name, sql) in [(batch_name, batch), (probe_name, probe)] {
            statements.push(GuardedStatement {
                raw: false,
                ..GuardedStatement::raw(name, &sql, &["uuid[]"])
            });
        }
    }

    statements.extend(one_row_claims());

    let id = uuid::Uuid::nil();
    let claim = queue::TaskClaim::new(id, "worker", 1);

    let fenced = queue::fence(
        diesel::update(dsl::harvest_task_queue.find(id))
            .set(dsl::state.eq("COMPLETED"))
            .into_boxed(),
        Some(&claim),
    );
    statements.push(GuardedStatement::diesel(
        "claim_fence",
        &fenced,
        &["text", "uuid", "text", "text", "integer", "boolean"],
    ));

    let running = diesel::update(
        dsl::harvest_task_queue
            .find(id)
            .filter(row_guard(dsl::state.eq("RUNNING"))),
    )
    .set(dsl::state.eq("PENDING"));
    statements.push(GuardedStatement::diesel(
        "running_guard",
        &running,
        &["text", "uuid", "text", "boolean"],
    ));

    let abandoned = diesel::update(
        dsl::harvest_task_queue.find(id).filter(row_guard(
            dsl::state
                .eq("RUNNING")
                .and(dsl::started_at.eq(jiff::Timestamp::UNIX_EPOCH.to_diesel())),
        )),
    )
    .set(dsl::state.eq("PENDING"));
    statements.push(GuardedStatement::diesel(
        "started_guard",
        &abandoned,
        &["text", "uuid", "text", "timestamptz", "boolean"],
    ));

    let workflow = diesel::update(dsl::harvest_task_queue.find(id).filter(row_guard(
        dsl::state.eq("RUNNING").and(dsl::task_type.eq("workflow")),
    )))
    .set(dsl::state.eq("PENDING"));
    statements.push(GuardedStatement::diesel(
        "workflow_guard",
        &workflow,
        &["text", "uuid", "text", "text", "boolean"],
    ));

    let worker = dsl::harvest_task_queue
        .find(id)
        .filter(row_guard(
            dsl::state
                .eq("RUNNING")
                .and(dsl::worker_id.eq("worker"))
                .and(dsl::crash_strikes.eq(0))
                .and(dsl::attempt.ne(1)),
        ))
        .select(dsl::id);
    statements.push(GuardedStatement::diesel(
        "worker_guard",
        &worker,
        &["uuid", "text", "text", "integer", "integer", "boolean"],
    ));

    let lock = dsl::harvest_task_queue
        .find(id)
        .filter(row_guard(
            queue::claim_held("worker", 1).and(dsl::crash_strikes.eq(0)),
        ))
        .select(dsl::id)
        .for_update();
    statements.push(GuardedStatement::diesel(
        "claim_lock",
        &lock,
        &["uuid", "text", "text", "integer", "integer", "boolean"],
    ));

    let live = diesel::update(
        dsl::harvest_task_queue
            .find(id)
            .filter(row_guard(dsl::state.eq_any(["PENDING", "RUNNING"]))),
    )
    .set(dsl::state.eq("FAILED"));
    statements.push(GuardedStatement::diesel(
        "live_guard",
        &live,
        &["text", "uuid", "text[]", "boolean"],
    ));

    let pending = diesel::update(
        dsl::harvest_task_queue
            .filter(dsl::id.eq(id))
            .filter(row_guard(
                dsl::workflow_exec_id
                    .eq(Some(id))
                    .and(dsl::state.eq("PENDING")),
            )),
    )
    .set(dsl::state.eq("PENDING"));
    statements.push(GuardedStatement::diesel(
        "pending_guard",
        &pending,
        &["text", "uuid", "uuid", "text", "boolean"],
    ));

    let open = diesel::update(
        dsl::harvest_task_queue
            .filter(dsl::workflow_exec_id.eq(Some(id)))
            .filter(row_guard(dsl::state.eq_any(["PENDING", "RUNNING"]))),
    )
    .set(dsl::state.eq("FAILED"));
    statements.push(
        GuardedStatement::diesel(
            "open_tasks_guard",
            &open,
            &["text", "uuid", "text[]", "boolean"],
        )
        .via(EXECUTION_INDEX),
    );

    let drain = diesel::update(
        dsl::harvest_task_queue
            .filter(dsl::workflow_exec_id.eq(Some(id)))
            .filter(row_guard(
                dsl::state.eq("PENDING").or(dsl::state.eq("RUNNING")),
            )),
    )
    .set(dsl::state.eq("FAILED"));
    statements.push(
        GuardedStatement::diesel(
            "drain_guard",
            &drain,
            &["text", "uuid", "text", "text", "boolean"],
        )
        .via(EXECUTION_INDEX),
    );

    let loser = diesel::update(
        dsl::harvest_task_queue
            .filter(dsl::workflow_exec_id.eq(Some(id)))
            .filter(dsl::activity_id.eq(Some(id)))
            .filter(row_guard(dsl::state.eq_any(["PENDING", "RUNNING"]))),
    )
    .set(dsl::state.eq("CANCELLED"));
    statements.push(
        GuardedStatement::diesel(
            "fork_loser_guard",
            &loser,
            &["text", "uuid", "uuid", "text[]", "boolean"],
        )
        .via(ACTIVITY_INDEX),
    );

    let session = diesel::select(diesel::dsl::exists(
        dsl::harvest_task_queue
            .filter(dsl::workflow_exec_id.eq(Some(id)))
            .filter(dsl::session_id.eq(Some(id)))
            .filter(dsl::id.ne(id))
            .filter(row_guard(dsl::state.eq_any(["PENDING", "RUNNING"]))),
    ));
    statements.push(
        GuardedStatement::diesel(
            "session_guard",
            &session,
            &["uuid", "uuid", "uuid", "text[]", "boolean"],
        )
        .via(EXECUTION_INDEX),
    );

    let cancel = diesel::delete(
        dsl::harvest_task_queue
            .filter(dsl::workflow_exec_id.eq(Some(id)))
            .filter(row_guard(
                dsl::task_type.eq("workflow").and(dsl::state.eq("PENDING")),
            ))
            .filter(dsl::scheduled_at.gt(jiff::Timestamp::UNIX_EPOCH.to_diesel())),
    );
    statements.push(
        GuardedStatement::diesel(
            "cancel_pending_guard",
            &cancel,
            &["uuid", "text", "text", "boolean", "timestamptz"],
        )
        .via(EXECUTION_INDEX),
    );

    statements
}

/// Each variant of the one-row claim.
///
/// The by-id claim has a plain, a fenced, an owned and an owned fenced
/// form. Its fair form is the same text as the plain form. The owner take
/// claims the workflow task of one execution, plain or fenced. It finds the
/// row through `idx_harvest_tq_workflow_task`.
///
/// Each claim binds `$1..$6`. The fence binds two arrays before the id. The
/// owned gate binds the holder and the partition mask after the id.
#[cfg(feature = "db")]
fn one_row_claims() -> Vec<GuardedStatement> {
    use crate::queue::{
        ClaimFairness, claim_by_id_query_for, owned_claim_by_execution_query,
        owned_claim_by_id_query,
    };

    const CLAIM: [&str; 6] = ["text", "text[]", "text", "bigint", "text[]", "text[]"];
    const FENCE: [&str; 2] = ["integer[]", "bigint[]"];
    const OWNER: [&str; 2] = ["text", "bytea"];

    let types = |fenced: bool, owned: bool| {
        let mut types = CLAIM.to_vec();
        if fenced {
            types.extend(FENCE);
        }
        types.push("uuid");
        if owned {
            types.extend(OWNER);
        }
        types
    };
    vec![
        GuardedStatement::raw(
            "claim_by_id",
            claim_by_id_query_for(false, ClaimFairness::Off),
            &types(false, false),
        ),
        GuardedStatement::raw(
            "claim_by_id_fenced",
            claim_by_id_query_for(true, ClaimFairness::Off),
            &types(true, false),
        ),
        GuardedStatement::raw(
            "claim_by_id_owned",
            owned_claim_by_id_query(false),
            &types(false, true),
        ),
        GuardedStatement::raw(
            "claim_by_id_owned_fenced",
            owned_claim_by_id_query(true),
            &types(true, true),
        ),
        GuardedStatement::raw(
            "take_by_execution",
            owned_claim_by_execution_query(false),
            &types(false, true),
        )
        .via(WORKFLOW_TASK_INDEX),
        GuardedStatement::raw(
            "take_by_execution_fenced",
            owned_claim_by_execution_query(true),
            &types(true, true),
        )
        .via(WORKFLOW_TASK_INDEX),
    ]
}

#[cfg(all(test, feature = "db"))]
mod tests {
    use super::{guarded_statements, one_row_claims};
    use std::path::{Path, PathBuf};

    /// Each engine source file that names the task queue, with its text.
    fn engine_sources() -> Vec<(PathBuf, String)> {
        fn walk(dir: &Path, out: &mut Vec<(PathBuf, String)>) {
            let mut entries: Vec<_> = std::fs::read_dir(dir)
                .unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
                .map(|e| {
                    e.unwrap_or_else(|e| panic!("a directory entry: {e}"))
                        .path()
                })
                .collect();
            entries.sort();
            for path in entries {
                if path.is_dir() {
                    walk(&path, out);
                } else if path.extension().is_some_and(|e| e == "rs")
                    && path.file_name().is_some_and(|n| n != "row_guard.rs")
                {
                    let text = std::fs::read_to_string(&path)
                        .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
                    if text.contains("harvest_task_queue") {
                        out.push((path, text));
                    }
                }
            }
        }
        let mut out = Vec::new();
        walk(&Path::new(env!("CARGO_MANIFEST_DIR")).join("src"), &mut out);
        assert!(out.len() >= 20, "the walk found only {} files", out.len());
        out
    }

    // ── Raw SQL ─────────────────────────────────────────────────────────

    /// The table alias of `term` and the term without it. Leading
    /// parentheses are removed first.
    fn aliased(term: &str) -> (&str, &str) {
        let term = term.trim_start_matches(['(', ' ']);
        match term.split_once('.') {
            Some((alias, rest))
                if !alias.is_empty()
                    && alias.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') =>
            {
                (alias, rest)
            }
            _ => ("", term),
        }
    }

    /// The names that refer to the task queue in `sql`: no alias, the table
    /// name, and each alias that follows the table name.
    fn queue_aliases(sql: &str) -> Vec<&str> {
        const NOT_ALIAS: [&str; 12] = [
            "WHERE", "SET", "JOIN", "ON", "USING", "INNER", "LEFT", "WHEN", "THEN", "AND", "OR",
            "VALUES",
        ];
        let mut aliases = vec!["", "harvest_task_queue"];
        for (at, _) in sql.match_indices("harvest_task_queue ") {
            let rest = &sql[at + "harvest_task_queue ".len()..];
            let rest = rest.strip_prefix("AS ").unwrap_or(rest);
            let word: &str = rest
                .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                .next()
                .unwrap_or_default();
            if !word.is_empty() && !NOT_ALIAS.contains(&word) {
                aliases.push(word);
            }
        }
        aliases
    }

    /// Each `WHERE` clause of `sql`, up to its end at the same depth.
    fn where_clauses(sql: &str) -> Vec<&str> {
        const ENDS: [&str; 12] = [
            " RETURNING ",
            " ORDER BY ",
            " LIMIT ",
            " FOR UPDATE",
            " FOR NO KEY",
            " FOR SHARE",
            " FOR KEY",
            " GROUP BY ",
            " OFFSET ",
            " UNION ",
            " EXCEPT ",
            " ON CONFLICT",
        ];
        let mut out = Vec::new();
        for (at, _) in sql.match_indices("WHERE ") {
            let body = &sql[at + "WHERE ".len()..];
            let mut depth = 0_i64;
            let mut end = body.len();
            for (i, c) in body.char_indices() {
                match c {
                    '(' => depth += 1,
                    ')' if depth == 0 => {
                        end = i;
                        break;
                    }
                    ')' => depth -= 1,
                    ' ' if depth == 0 && ENDS.iter().any(|e| body[i..].starts_with(e)) => {
                        end = i;
                        break;
                    }
                    _ => {}
                }
            }
            out.push(&body[..end]);
        }
        out
    }

    /// The top-level `AND` terms of `clause`.
    fn and_terms(clause: &str) -> Vec<&str> {
        let mut out = Vec::new();
        let mut depth = 0_i64;
        let mut start = 0;
        for (i, c) in clause.char_indices() {
            match c {
                '(' => depth += 1,
                ')' => depth -= 1,
                ' ' if depth == 0 && clause[i..].starts_with(" AND ") => {
                    out.push(clause[start..i].trim());
                    start = i + " AND ".len();
                }
                _ => {}
            }
        }
        out.push(clause[start..].trim());
        out
    }

    /// True when `term` tests a key column of the task queue against a
    /// parameter.
    fn is_key(term: &str, aliases: &[&str]) -> bool {
        let (alias, bare) = aliased(term);
        aliases.contains(&alias)
            && ["id = ", "workflow_exec_id = "].iter().any(|column| {
                bare.strip_prefix(column).is_some_and(|value| {
                    let value = value.strip_prefix("ANY(").unwrap_or(value);
                    value.starts_with('$')
                })
            })
    }

    /// True when `term` tests a column of the task queue that a partial
    /// index matches.
    fn is_plain_guard(term: &str, aliases: &[&str]) -> bool {
        let (alias, bare) = aliased(term);
        aliases.contains(&alias)
            && [
                "state = ",
                "state IN ",
                "worker_id = ",
                "worker_id IS NOT NULL",
            ]
            .iter()
            .any(|column| bare.starts_with(column))
    }

    /// Each plain guard beside a row key in `sql`, as a message.
    ///
    /// A key is `id` or `workflow_exec_id` against a parameter, or the `ANY`
    /// form of either. Each top-level `AND` term of a keyed `WHERE` clause is
    /// read, before and after the key.
    fn plain_keyed_guards(sql: &str) -> Vec<String> {
        let sql = sql.split_whitespace().collect::<Vec<_>>().join(" ");
        let aliases = queue_aliases(&sql);
        let mut found = Vec::new();
        for clause in where_clauses(&sql) {
            let terms = and_terms(clause);
            if !terms.iter().any(|t| is_key(t, &aliases)) {
                continue;
            }
            for term in terms {
                if is_plain_guard(term, &aliases) {
                    found.push(format!("{term} in WHERE {clause}"));
                }
            }
        }
        found
    }

    /// Each string literal of `source`, its start and the byte offset after
    /// it. Each continuation joins as rustc joins it. Comments are skipped.
    fn string_literals(source: &str) -> Vec<(usize, usize, String)> {
        let mut out = Vec::new();
        let mut chars = source.char_indices().peekable();
        while let Some((at, c)) = chars.next() {
            match c {
                '/' if source[at..].starts_with("//") => {
                    for (_, c) in chars.by_ref() {
                        if c == '\n' {
                            break;
                        }
                    }
                }
                '\'' if source[at..].starts_with("'\"'") => {
                    chars.next();
                    chars.next();
                }
                '"' => {
                    let mut text = String::new();
                    let mut end = source.len();
                    while let Some((i, c)) = chars.next() {
                        match c {
                            '\\' => match chars.next() {
                                Some((_, '\n' | '\r')) => {
                                    while chars.peek().is_some_and(|(_, c)| c.is_whitespace()) {
                                        chars.next();
                                    }
                                }
                                Some((_, c)) => {
                                    text.push('\\');
                                    text.push(c);
                                }
                                None => {}
                            },
                            '"' => {
                                end = i + 1;
                                break;
                            }
                            c => text.push(c),
                        }
                    }
                    out.push((at, end, text));
                }
                _ => {}
            }
        }
        out
    }

    /// Each raw statement in `source` with a plain guard beside its key.
    ///
    /// A literal that ends in `AND` after a key must be followed by
    /// `row_guard_sql!`. Otherwise a `concat!` can join a plain guard to it.
    fn raw_failures(source: &str) -> Vec<String> {
        let mut failures = Vec::new();
        // The end of the last keyed literal and its aliases. A later literal
        // of the same expression that starts with `AND` continues its clause.
        let mut keyed_before: Option<(usize, Vec<String>)> = None;
        for (start, end, literal) in string_literals(source) {
            if source[..start].trim_end().ends_with("row_guard_sql!(") {
                continue;
            }
            let sql = literal.split_whitespace().collect::<Vec<_>>().join(" ");
            if let Some((before, aliases)) = &keyed_before {
                let between = &source[*before..end];
                let aliases: Vec<&str> = aliases.iter().map(String::as_str).collect();
                if !between.contains(';') && sql.starts_with("AND ") {
                    for term in and_terms(&format!("x = 1 {sql}")).into_iter().skip(1) {
                        if is_plain_guard(term, &aliases) {
                            failures.push(format!("a later literal adds a plain guard: {sql}"));
                        }
                    }
                    continue;
                }
            }
            keyed_before = None;
            if !literal.contains("harvest_task_queue") {
                continue;
            }
            failures.extend(plain_keyed_guards(&literal));
            let aliases = queue_aliases(&sql);
            let Some(clause) = where_clauses(&sql).pop() else {
                continue;
            };
            if and_terms(clause).iter().any(|t| is_key(t, &aliases)) {
                keyed_before = Some((end, aliases.iter().map(|a| (*a).to_owned()).collect()));
            }
            let Some(open) = clause.strip_suffix(" AND") else {
                continue;
            };
            let next = source[end..].trim_start_matches(|c: char| c == ',' || c.is_whitespace());
            let next = next
                .strip_prefix("$crate::row_guard::")
                .or_else(|| next.strip_prefix("crate::row_guard::"))
                .unwrap_or(next);
            if and_terms(open).iter().any(|t| is_key(t, &aliases))
                && !next.starts_with("row_guard_sql!")
            {
                failures.push(format!(
                    "a key ends a literal with no row_guard_sql!: {clause}"
                ));
            }
        }
        failures
    }

    /// No raw statement tests `state` or `worker_id` plainly beside its key.
    #[test]
    fn every_keyed_raw_statement_hides_its_guard() {
        let mut failures = Vec::new();
        for (path, source) in engine_sources() {
            for failure in raw_failures(&source) {
                failures.push(format!("{}: {failure}", path.display()));
            }
        }
        assert!(
            failures.is_empty(),
            "these raw statements have a plain guard:\n{}",
            failures.join("\n")
        );
    }

    // ── Diesel ──────────────────────────────────────────────────────────

    /// The top-level `.filter(` arguments of a Diesel chain.
    fn filter_arguments(chain: &str) -> Vec<&str> {
        let mut out = Vec::new();
        let mut rest = chain;
        while let Some(at) = rest.find(".filter(") {
            let body = &rest[at + ".filter(".len()..];
            let mut depth = 0_usize;
            let mut end = body.len();
            for (i, c) in body.char_indices() {
                match c {
                    '(' => depth += 1,
                    ')' if depth == 0 => {
                        end = i;
                        break;
                    }
                    ')' => depth -= 1,
                    _ => {}
                }
            }
            out.push(body[..end].trim());
            rest = &body[end..];
        }
        out
    }

    /// `argument` with its leading `path::` segments removed. `None` when
    /// the path names another `harvest_` table.
    fn unpathed(argument: &str) -> Option<&str> {
        let mut rest = argument;
        while let Some((head, tail)) = rest.split_once("::") {
            if !head.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                break;
            }
            if head.starts_with("harvest_") && head != "harvest_task_queue" {
                return None;
            }
            rest = tail;
        }
        Some(rest)
    }

    /// True when `argument` starts a plain guard.
    fn plain_diesel_guard(argument: &str) -> bool {
        unpathed(argument).is_some_and(|a| {
            ["state.", "worker_id.", "claim_held("]
                .iter()
                .any(|plain| a.starts_with(plain))
        })
    }

    /// True when `argument` is a key test.
    fn diesel_key(argument: &str) -> bool {
        unpathed(argument).is_some_and(|a| {
            [
                "id.eq(",
                "id.eq_any(",
                "workflow_exec_id.eq(",
                "workflow_exec_id.eq_any(",
            ]
            .iter()
            .any(|key| a.starts_with(key))
        })
    }

    /// The operands of `argument` that `.and(` joins at its top level. A
    /// `row_guard(` operand is one operand, whatever it holds.
    fn and_operands(argument: &str) -> Vec<&str> {
        let mut out = vec![argument];
        let mut depth = 0_i64;
        for (i, c) in argument.char_indices() {
            match c {
                '(' => depth += 1,
                ')' => depth -= 1,
                '.' if depth == 0 && argument[i..].starts_with(".and(") => {
                    out.push(&argument[i + ".and(".len()..]);
                }
                _ => {}
            }
        }
        out
    }

    /// Each Diesel chain on the task queue in `source`, as one line of text.
    ///
    /// A chain starts after the table name, where the next text is a call.
    /// It runs until its parentheses close below the start, or until a `;`.
    /// The trimmed lines join with no space, so a call split over lines
    /// reads as one call. A `//` comment is dropped.
    fn queue_chains(source: &str) -> Vec<(usize, String)> {
        let lines: Vec<&str> = source
            .lines()
            .map(|l| l.split_once("//").map_or(l, |(code, _)| code).trim())
            .collect();
        let mut out = Vec::new();
        for (at, line) in lines.iter().enumerate() {
            for (start, _) in line.match_indices("harvest_task_queue") {
                let after = &line[start + "harvest_task_queue".len()..];
                let after = after.strip_prefix("::table").unwrap_or(after);
                if !(after.is_empty() || after.starts_with('.')) {
                    continue;
                }
                let mut chain = String::new();
                let mut depth = 0_i64;
                let rest = std::iter::once(after).chain(lines[at + 1..].iter().copied());
                'lines: for next in rest {
                    for c in next.chars() {
                        match c {
                            '(' => depth += 1,
                            ')' => depth -= 1,
                            ';' if depth <= 0 => break 'lines,
                            _ => {}
                        }
                        if depth < 0 {
                            break 'lines;
                        }
                        chain.push(c);
                    }
                }
                out.push((at + 1, chain));
            }
        }
        out
    }

    /// The keyed chains of `source`, and each plain guard in them.
    fn diesel_failures(source: &str) -> (usize, Vec<String>) {
        let mut keyed = 0;
        let mut failures = Vec::new();
        for (line, chain) in queue_chains(source) {
            let arguments = filter_arguments(&chain);
            let keyed_chain = chain.starts_with(".find(")
                || arguments
                    .iter()
                    .any(|a| and_operands(a).into_iter().any(diesel_key));
            if !keyed_chain {
                continue;
            }
            keyed += 1;
            for argument in arguments {
                if and_operands(argument).into_iter().any(plain_diesel_guard) {
                    failures.push(format!("{line}: {argument}"));
                }
            }
        }
        (keyed, failures)
    }

    /// Each Diesel chain that finds task rows by a key hides its guard.
    ///
    /// The plan test checks samples of the Diesel guards. This walk checks
    /// each call site, so a site that drops `row_guard` fails here.
    #[test]
    fn every_keyed_diesel_chain_hides_its_guard() {
        let mut chains = 0;
        let mut failures = Vec::new();
        for (path, source) in engine_sources() {
            let (keyed, found) = diesel_failures(&source);
            chains += keyed;
            for failure in found {
                failures.push(format!("{}:{failure}", path.display()));
            }
        }
        assert!(chains >= 45, "the walk found only {chains} keyed chains");
        assert!(
            failures.is_empty(),
            "these keyed chains have a plain guard:\n{}",
            failures.join("\n")
        );
    }

    // ── The walks find each shape ───────────────────────────────────────

    #[test]
    fn the_raw_walk_finds_each_plain_guard_shape() {
        for sql in [
            "UPDATE harvest_task_queue SET x = 1 WHERE id = $1 AND state = 'RUNNING'",
            "SELECT 1 FROM harvest_task_queue WHERE id = ANY($1) AND state = 'PENDING'",
            "UPDATE harvest_task_queue SET x = 1 WHERE id = $1 AND worker_id = $2",
            "UPDATE harvest_task_queue SET x = 1 WHERE workflow_exec_id = $1 \
             AND task_type = 'workflow' AND state = 'RUNNING'",
            "UPDATE harvest_task_queue t SET x = 1 WHERE t.workflow_exec_id = $1 \
             AND t.task_type = 'workflow' AND ((t.state = 'RUNNING'))",
            "SELECT 1 FROM harvest_task_queue WHERE workflow_exec_id = ANY($1) AND state = 'PENDING'",
            "UPDATE harvest_task_queue SET x = 1 WHERE workflow_exec_id = $2 AND state IN ('PENDING')",
            "SELECT 1 FROM harvest_task_queue WHERE state = 'RUNNING' AND id = ANY($1)",
            "UPDATE harvest_task_queue SET x = 1 WHERE id = $1 AND attempt = $3 AND state = 'RUNNING'",
            "SELECT 1 FROM harvest_task_queue AS inner_q WHERE inner_q.id = $1 \
             AND inner_q.worker_id IS NOT NULL",
        ] {
            assert_eq!(plain_keyed_guards(sql).len(), 1, "{sql}");
        }
        for sql in [
            "UPDATE harvest_task_queue SET x = 1 WHERE id = $1 AND (CASE WHEN state = 'RUNNING' THEN true END)",
            "UPDATE harvest_task_queue SET x = 1 WHERE workflow_exec_id = $1 \
             AND task_type = 'workflow' AND (CASE WHEN state = 'RUNNING' THEN true END)",
            "UPDATE harvest_task_queue SET x = 1 WHERE queue_id = $1 AND state = 'RUNNING'",
            "UPDATE harvest_task_queue SET x = 1 WHERE worker_id = $1 AND state = 'RUNNING'",
            "SELECT 1 FROM harvest_task_queue t JOIN e ON e.id = t.workflow_exec_id \
             WHERE t.id = $2 AND e.state = 'RUNNING'",
            "SELECT 1 FROM harvest_task_queue t, harvest_workflow_executions e \
             WHERE e.id = $1 AND e.state = 'RUNNING' AND t.workflow_exec_id = e.id",
            "SELECT 1 FROM harvest_task_queue WHERE state = 'RUNNING' AND id = ANY(SELECT x FROM y)",
        ] {
            assert!(plain_keyed_guards(sql).is_empty(), "{sql}");
        }
    }

    #[test]
    fn the_raw_walk_reads_each_literal_as_rustc_does() {
        let source = "let a = \"SELECT 1 FROM harvest_task_queue \\\n    WHERE id = $1\"; // \"not a literal\"\nlet q = '\"';\nlet b = \"x\";";
        let literals: Vec<String> = string_literals(source)
            .into_iter()
            .map(|(_, _, l)| l)
            .collect();
        assert_eq!(
            literals,
            ["SELECT 1 FROM harvest_task_queue WHERE id = $1", "x"]
        );
    }

    /// A Windows checkout has CRLF line ends. The walks read it as rustc does.
    #[test]
    fn the_walks_read_crlf_sources() {
        let guarded = "concat!(\r\n    \"UPDATE harvest_task_queue SET x = 1 \\\r\n     WHERE id = $1 AND \",\r\n    \
                       crate::row_guard::row_guard_sql!(\"state = 'RUNNING'\")\r\n)";
        assert_eq!(raw_failures(guarded), Vec::<String>::new());
        let plain = guarded.replace(
            "crate::row_guard::row_guard_sql!(\"state = 'RUNNING'\")",
            "\"state = 'R'\"",
        );
        assert_eq!(raw_failures(&plain).len(), 1, "{plain}");
        let literals: Vec<String> = string_literals("\"a \\\r\n    b\"")
            .into_iter()
            .map(|(_, _, l)| l)
            .collect();
        assert_eq!(literals, ["a b"]);
        let chain = "dsl::harvest_task_queue\r\n    .filter(dsl::id.eq(id))\r\n    .filter(dsl::state.eq(\"R\"));";
        assert_eq!(diesel_failures(chain).1.len(), 1);
    }

    #[test]
    fn the_raw_walk_finds_a_concat_with_no_guard_macro() {
        let plain = "concat!(\"UPDATE harvest_task_queue SET x = 1 WHERE id = $1 AND \", \"state = 'RUNNING'\")";
        assert_eq!(raw_failures(plain).len(), 1, "{plain}");
        let guarded = "concat!(\"UPDATE harvest_task_queue SET x = 1 WHERE id = $1 AND \", \
                       crate::row_guard::row_guard_sql!(\"state = 'RUNNING'\"))";
        assert_eq!(raw_failures(guarded), Vec::<String>::new());
        let trailing = "concat!(\"UPDATE harvest_task_queue SET x = 1 WHERE id = $1 AND \", \
                        row_guard_sql!(\"state = 'RUNNING'\"), \" AND worker_id = $2\")";
        assert_eq!(raw_failures(trailing).len(), 1, "{trailing}");
        let split =
            "concat!(\"UPDATE harvest_task_queue SET x = 1 WHERE id = $1\", \" AND state = 'R'\")";
        assert_eq!(raw_failures(split).len(), 1, "{split}");
        let in_macro = "concat!(\"UPDATE harvest_task_queue SET x = 1 WHERE id = $1 AND \", \
                        $crate::row_guard::row_guard_sql!(\"state = 'RUNNING'\"))";
        assert_eq!(raw_failures(in_macro), Vec::<String>::new());
    }

    #[test]
    fn the_diesel_walk_finds_each_plain_guard_shape() {
        let flagged = [
            "diesel::update(\n    dsl::harvest_task_queue\n        .filter(dsl::workflow_exec_id.eq(Some(exec)))\n        .filter(\n            dsl::state\n                .eq(\"PENDING\")\n                .or(dsl::state.eq(\"RUNNING\")),\n        ),\n)\n.set(dsl::state.eq(\"FAILED\"));",
            "diesel::update(dsl::harvest_task_queue.find(id).filter(dsl::state.eq(\"RUNNING\")))\n    .set(dsl::state.eq(\"PENDING\"));",
            "harvest_task_queue::table\n    .filter(harvest_task_queue::id.eq(id).and(harvest_task_queue::state.eq(\"RUNNING\")))\n    .count();",
            "q::harvest_task_queue // a comment\n    .filter(q::id.eq(id))\n    .filter(q::worker_id.eq(w));",
            "dsl::harvest_task_queue\n    .filter(dsl::id.eq_any(ids))\n    .filter(dsl::state.eq(\"R\"));",
            "dsl::harvest_task_queue\n    .filter(dsl::state.eq(\"R\").and(dsl::id.eq(id)));",
            "dsl::harvest_task_queue\n    .filter(dsl::id.eq(id))\n    .filter(dsl::attempt.eq(1).and(dsl::state.eq(\"R\")));",
        ];
        for source in flagged {
            let (keyed, failures) = diesel_failures(source);
            assert_eq!((keyed, failures.len()), (1, 1), "{source}: {failures:?}");
        }
        let clean = [
            "diesel::update(dsl::harvest_task_queue.find(id).filter(row_guard(dsl::state.eq(\"RUNNING\"))))\n    .set(dsl::state.eq(\"PENDING\"));",
            "dsl::harvest_task_queue\n    .filter(dsl::worker_id.eq(w))\n    .filter(dsl::state.eq(\"RUNNING\"));",
            "harvest_task_queue::table\n    .inner_join(harvest_workflow_executions::table)\n    .filter(harvest_task_queue::id.eq(id))\n    .filter(harvest_workflow_executions::state.eq(\"RUNNING\"));",
            "dsl::harvest_task_queue\n    .filter(dsl::id.eq(id).and(row_guard(dsl::attempt.eq(1).and(dsl::state.eq(\"R\")))));",
        ];
        for source in clean {
            assert!(diesel_failures(source).1.is_empty(), "{source}");
        }
        assert_eq!(
            diesel_failures(clean[1]).0,
            0,
            "a scan by worker has no key"
        );
    }

    /// A `SET` clause can hold its own `CASE`, so the test reads the end of
    /// the guard form. A Diesel guard binds its `true`. A timeout batch
    /// loads its ids through the primary key in a fence.
    #[test]
    fn every_guarded_statement_hides_its_guard() {
        const FENCE: &str = "WITH batch AS MATERIALIZED (SELECT ";
        const KEYED: &str = " FROM harvest_task_queue WHERE id = ANY($1))";
        for statement in guarded_statements() {
            let hidden = if statement.name.starts_with("timeout_") {
                statement.sql.starts_with(FENCE) && statement.sql.contains(KEYED)
            } else if statement.raw {
                statement.sql.contains("CASE WHEN") && statement.sql.contains(" THEN true END)")
            } else {
                statement.sql.contains("CASE WHEN") && statement.sql.contains(" THEN ($")
            };
            assert!(
                hidden,
                "{} must hide its row guard: {}",
                statement.name, statement.sql
            );
        }
    }

    /// A one-row claim guards the state test of both of its scans.
    #[test]
    fn every_one_row_claim_has_two_guards() {
        let guard = row_guard_sql!("state = 'PENDING'");
        let claims = one_row_claims();
        assert_eq!(claims.len(), 6);
        for claim in claims {
            assert_eq!(claim.sql.matches(guard).count(), 2, "{}", claim.name);
        }
    }

    #[test]
    fn statement_names_are_unique() {
        let statements = guarded_statements();
        let mut names: Vec<_> = statements.iter().map(|s| s.name).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), statements.len());
    }
}
