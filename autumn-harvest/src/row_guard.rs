//! Keep a by-id task row statement on the primary key.
//!
//! Many statements find one `harvest_task_queue` row by `id`, then guard
//! it on `state` and `worker_id`. A partial index matches each such guard.
//! Both paths estimate one row, so the planner can pick the small partial
//! index over the primary key. That path reads each index entry that
//! matches the guard, then filters on `id`. A cached generic plan keeps that
//! choice.
//!
//! Statements that find task rows by `workflow_exec_id` have the same
//! defect. Their key index is `idx_harvest_tq_workflow`.
//!
//! `row_guard_sql!` and `row_guard` hide such a guard from the planner.
//! [`guarded_statements`] lists each raw statement in scope and a sample of
//! each Diesel guard shape. A DB test checks that the generic and the custom
//! plan of each one read the table through its key index. See
//! `docs/performance-claim-release-guard.md`.

#[cfg(feature = "db")]
use diesel::QueryDsl;
#[cfg(feature = "db")]
use diesel::expression::Expression;
#[cfg(feature = "db")]
use diesel::pg::Pg;
#[cfg(feature = "db")]
use diesel::sql_types::{Bool, BoolOrNullableBool};

/// Wrap the guard of a by-id statement so that no index can match it.
///
/// `CASE WHEN g THEN true END` is true when `g` is true and NULL in all
/// other cases. `WHERE` drops a NULL row as it drops a false row, so the
/// statement keeps the same rows. The planner cannot use a `CASE` as an
/// index condition. It also cannot prove a partial index predicate from
/// it. So it finds the row through `id = $1` and the primary key.
///
/// Use it only beside a key test: `id = $1`, or `workflow_exec_id = $1` with
/// the execution index. A scan without a key needs its index. Use it only as
/// a top-level `AND` term. `NOT` or `OR` on a NULL result changes the rows.
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

/// The column that a guarded statement finds its task rows by.
#[cfg(feature = "db")]
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Key {
    /// `id`, through the primary key.
    Id,
    /// `workflow_exec_id`, through the execution index.
    Execution,
    /// `workflow_exec_id` and `activity_id`, through the activity index.
    Activity,
}

#[cfg(feature = "db")]
impl Key {
    /// The index that a plan must read for this key.
    #[must_use]
    pub const fn index(self) -> &'static str {
        match self {
            Self::Id => "harvest_task_queue_pkey",
            Self::Execution => "idx_harvest_tq_workflow",
            Self::Activity => "idx_harvest_tq_activity_id",
        }
    }
}

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
    /// True when `sql` is the production text. A Diesel entry is false: it
    /// has the production guard, but its other clauses are a sample.
    pub raw: bool,
    /// The column that the statement finds its rows by.
    pub key: Key,
}

#[cfg(feature = "db")]
impl GuardedStatement {
    fn raw(name: &'static str, sql: &str, param_types: &[&'static str]) -> Self {
        Self {
            name,
            sql: sql.to_owned(),
            param_types: param_types.to_vec(),
            raw: true,
            key: Key::Id,
        }
    }

    /// The same statement, found by `key`.
    fn by(self, key: Key) -> Self {
        Self { key, ..self }
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
        .by(Key::Execution),
        GuardedStatement::raw(
            "wake_fallback",
            queue::wake_requested_fallback_query(),
            &["uuid"],
        )
        .by(Key::Execution),
        GuardedStatement::raw(
            "wake_already_due",
            queue::already_due_workflow_task_query(),
            &["uuid", "timestamptz"],
        )
        .by(Key::Execution),
        GuardedStatement::raw("owner_keys", queue::OWNER_KEYS_SQL, &["uuid[]"]).by(Key::Execution),
        GuardedStatement::raw(
            "shift_schedule_to_close_on_resume",
            crate::execution::shift_schedule_to_close_on_resume_query(),
            &["bigint", "uuid"],
        )
        .by(Key::Execution),
        GuardedStatement::raw(
            "migration_staged_task",
            crate::shard_rebalance::STAGED_TASK_SQL,
            &["uuid"],
        )
        .by(Key::Execution),
        GuardedStatement::raw(
            "migration_signal_repend",
            crate::shard_rebalance::SIGNAL_REPEND_SQL,
            &["uuid"],
        )
        .by(Key::Execution),
    ]);

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
        .by(Key::Execution),
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
        .by(Key::Execution),
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
        .by(Key::Activity),
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
        .by(Key::Execution),
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
        .by(Key::Execution),
    );

    statements
}

#[cfg(all(test, feature = "db"))]
mod tests {
    use super::guarded_statements;
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

    /// The table alias of `term` and the term without it. Leading
    /// parentheses are removed first.
    fn aliased(term: &str) -> (&str, &str) {
        let term = term.trim_start_matches(['(', ' ']);
        match term.split_once('.') {
            Some((alias, rest))
                if !alias.is_empty() && alias.chars().all(|c| c.is_ascii_alphanumeric()) =>
            {
                (alias, rest)
            }
            _ => ("", term),
        }
    }

    /// `term` with its leading parentheses and table alias removed.
    fn bare(term: &str) -> &str {
        aliased(term).1
    }

    /// True when `term` tests a column of the `alias` table that a partial
    /// index matches.
    fn plain_guard(term: &str, alias: &str) -> bool {
        let (own, term) = aliased(term);
        own == alias
            && ["state ", "state=", "worker_id "]
                .iter()
                .any(|column| term.starts_with(column))
    }

    /// Each plain guard beside a row key in `sql`, as a message.
    ///
    /// A key is `id = $n`, `workflow_exec_id = $n` or the `ANY` form of
    /// either. A `task_type` test after the key is skipped. The term after
    /// it must not test `state` or `worker_id` plainly.
    fn plain_keyed_guards(sql: &str) -> Vec<String> {
        let sql = sql.split_whitespace().collect::<Vec<_>>().join(" ");
        let mut found = Vec::new();
        for (at, _) in sql.match_indices(" = ") {
            let left = sql[..at].rsplit([' ', '(']).next().unwrap_or_default();
            let (alias, column) = left.rsplit_once('.').unwrap_or(("", left));
            if column != "id" && column != "workflow_exec_id" {
                continue;
            }
            let value = &sql[at + 3..];
            let Some(param) = value
                .strip_prefix("ANY(")
                .or(Some(value))
                .and_then(|v| v.strip_prefix('$'))
            else {
                continue;
            };
            let mut rest = param.trim_start_matches(|c: char| {
                c.is_ascii_digit()
                    || c == ')'
                    || c == ':'
                    || c.is_ascii_alphabetic()
                    || c == '['
                    || c == ']'
            });
            while let Some(term) = rest.strip_prefix(" AND ") {
                if bare(term).starts_with("task_type = '") {
                    let close = term.find("' ").map_or(term.len(), |c| c + 1);
                    rest = &term[close..];
                    continue;
                }
                if plain_guard(term, alias) {
                    found.push(sql[at.saturating_sub(40)..(at + 120).min(sql.len())].to_owned());
                }
                break;
            }
        }
        found
    }

    /// Each string literal of `source`, with each continuation joined as
    /// rustc joins it. Comments are skipped.
    fn string_literals(source: &str) -> Vec<String> {
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
                    while let Some((_, c)) = chars.next() {
                        match c {
                            '\\' => match chars.next() {
                                Some((_, '\n')) => {
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
                            '"' => break,
                            c => text.push(c),
                        }
                    }
                    out.push(text);
                }
                _ => {}
            }
        }
        out
    }

    /// No raw statement tests `state` or `worker_id` plainly beside its key.
    #[test]
    fn every_keyed_raw_statement_hides_its_guard() {
        let mut failures = Vec::new();
        for (path, source) in engine_sources() {
            for literal in string_literals(&source) {
                if !literal.contains("harvest_task_queue") {
                    continue;
                }
                for at in plain_keyed_guards(&literal) {
                    failures.push(format!("{}: {at}", path.display()));
                }
            }
        }
        assert!(
            failures.is_empty(),
            "these raw statements have a plain guard:\n{}",
            failures.join("\n")
        );
    }

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

    /// `argument` with its leading `path::` segments removed.
    fn unpathed(argument: &str) -> &str {
        let mut rest = argument;
        while let Some((head, tail)) = rest.split_once("::") {
            if head.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                rest = tail;
            } else {
                break;
            }
        }
        rest
    }

    /// Each Diesel chain on the task queue in `source`, as one line of text.
    ///
    /// A chain starts at a line that ends with the table. It runs until its
    /// parentheses close below the start, or until a `;`. The trimmed lines
    /// join with no space, so a call split over lines reads as one call.
    fn queue_chains(source: &str) -> Vec<(usize, String)> {
        let lines: Vec<&str> = source.lines().collect();
        let mut out = Vec::new();
        for (at, line) in lines.iter().enumerate() {
            let line = line.trim();
            if line.starts_with("//")
                || !(line.ends_with("harvest_task_queue")
                    || line.ends_with("harvest_task_queue::table"))
            {
                continue;
            }
            let mut chain = String::new();
            let mut depth = 0_i64;
            'lines: for next in &lines[at + 1..] {
                let next = next.trim();
                if next.starts_with("//") {
                    continue;
                }
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
        out
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
            for (line, chain) in queue_chains(&source) {
                let arguments = filter_arguments(&chain);
                let keyed = chain.trim_start().starts_with(".find(")
                    || arguments.iter().any(|a| {
                        let a = unpathed(a);
                        a.starts_with("id.eq(") || a.starts_with("workflow_exec_id.eq(")
                    });
                if !keyed {
                    continue;
                }
                chains += 1;
                for argument in arguments {
                    let argument = unpathed(argument);
                    if ["state.", "worker_id.", "claim_held("]
                        .iter()
                        .any(|plain| argument.starts_with(plain))
                    {
                        failures.push(format!("{}:{line}: {argument}", path.display()));
                    }
                }
            }
        }
        assert!(chains >= 20, "the walk found only {chains} keyed chains");
        assert!(
            failures.is_empty(),
            "these keyed chains have a plain guard:\n{}",
            failures.join("\n")
        );
    }

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
        ] {
            assert_eq!(plain_keyed_guards(sql).len(), 1, "{sql}");
        }
        for sql in [
            "UPDATE harvest_task_queue SET x = 1 WHERE id = $1 AND (CASE WHEN state = 'RUNNING' THEN true END)",
            "UPDATE harvest_task_queue SET x = 1 WHERE workflow_exec_id = $1 \
             AND (CASE WHEN task_type = 'workflow' AND state = 'RUNNING' THEN true END)",
            "UPDATE harvest_task_queue SET x = 1 WHERE queue_id = $1 AND state = 'RUNNING'",
            "UPDATE harvest_task_queue SET x = 1 WHERE worker_id = $1 AND state = 'RUNNING'",
            "SELECT 1 FROM harvest_task_queue t JOIN e ON e.id = t.workflow_exec_id \
             WHERE t.id = $2 AND e.state = 'RUNNING'",
        ] {
            assert!(plain_keyed_guards(sql).is_empty(), "{sql}");
        }
    }

    #[test]
    fn the_diesel_walk_finds_each_plain_guard_shape() {
        let source = "
            diesel::update(
                dsl::harvest_task_queue
                    .filter(dsl::workflow_exec_id.eq(Some(exec)))
                    .filter(
                        dsl::state
                            .eq(\"PENDING\")
                            .or(dsl::state.eq(\"RUNNING\")),
                    ),
            )
            .set(dsl::state.eq(\"FAILED\"));
        ";
        let chains = queue_chains(source);
        assert_eq!(chains.len(), 1);
        let arguments = filter_arguments(&chains[0].1);
        assert_eq!(arguments.len(), 2, "{arguments:?}");
        assert!(unpathed(arguments[1]).starts_with("state."));
        assert!(
            !chains[0].1.contains(".set("),
            "the chain stops at its close"
        );
    }

    /// A `SET` clause can hold its own `CASE`, so the test reads the end
    /// of the guard form. A Diesel guard binds its `true`.
    #[test]
    fn every_guarded_statement_hides_its_guard() {
        for statement in guarded_statements() {
            let end = if statement.raw {
                " THEN true END)"
            } else {
                " THEN ($"
            };
            assert!(
                statement.sql.contains("CASE WHEN") && statement.sql.contains(end),
                "{} must wrap its row guard in a CASE: {}",
                statement.name,
                statement.sql
            );
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
