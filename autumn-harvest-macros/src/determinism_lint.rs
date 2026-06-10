use proc_macro2::Span;
use std::collections::HashMap;
use syn::{Expr, visit::Visit};

#[derive(Debug, Clone)]
pub struct RuleInfo {
    pub id: String,
    pub severity: String, // "HardBlocker" or "Warning"
    pub explanation: String,
    pub alternative: String,
}

pub struct LinterFinding {
    pub rule_id: String,
    pub severity: String,
    pub message: String,
    pub alternative: String,
    pub span: Span,
}

pub struct DeterminismVisitor {
    pub findings: Vec<LinterFinding>,
    catalog: HashMap<String, RuleInfo>,
    in_await_condition_closure: bool,
}

impl DeterminismVisitor {
    #[allow(clippy::missing_const_for_fn)]
    pub fn new(catalog: HashMap<String, RuleInfo>) -> Self {
        Self {
            findings: Vec::new(),
            catalog,
            in_await_condition_closure: false,
        }
    }

    fn add_finding(&mut self, rule_id: &str, span: Span) {
        let actual_rule_id =
            if self.in_await_condition_closure && (rule_id == "HVG001" || rule_id == "HVG002") {
                "HVG008"
            } else {
                rule_id
            };
        if let Some(rule) = self.catalog.get(actual_rule_id) {
            self.findings.push(LinterFinding {
                rule_id: rule.id.clone(),
                severity: rule.severity.clone(),
                message: rule.explanation.clone(),
                alternative: rule.alternative.clone(),
                span,
            });
        }
    }
}

fn path_to_string(path: &syn::Path) -> String {
    let mut s = String::new();
    for (i, segment) in path.segments.iter().enumerate() {
        if i > 0 {
            s.push_str("::");
        }
        s.push_str(&segment.ident.to_string());
    }
    s
}

impl<'ast> Visit<'ast> for DeterminismVisitor {
    fn visit_expr_call(&mut self, i: &'ast syn::ExprCall) {
        if let Expr::Path(expr_path) = &*i.func {
            let path_str = path_to_string(&expr_path.path);

            // HVG001: WallClock
            if path_str == "chrono::Utc::now"
                || path_str == "Utc::now"
                || path_str == "chrono::Local::now"
                || path_str == "Local::now"
                || path_str == "std::time::Instant::now"
                || path_str == "Instant::now"
                || path_str == "std::time::SystemTime::now"
                || path_str == "SystemTime::now"
            {
                self.add_finding(
                    "HVG001",
                    expr_path.path.segments.last().unwrap().ident.span(),
                );
            }

            // HVG002: Randomness
            if path_str == "rand::random"
                || path_str == "random"
                || path_str == "rand::thread_rng"
                || path_str == "thread_rng"
                || path_str == "Uuid::new_v4"
                || path_str == "uuid::Uuid::new_v4"
                || path_str == "Uuid::now_v7"
                || path_str == "uuid::Uuid::now_v7"
            {
                self.add_finding(
                    "HVG002",
                    expr_path.path.segments.last().unwrap().ident.span(),
                );
            }

            // HVG003: ProcessEnv
            if path_str == "std::env::var"
                || path_str == "env::var"
                || path_str == "std::env::args"
                || path_str == "env::args"
            {
                self.add_finding(
                    "HVG003",
                    expr_path.path.segments.last().unwrap().ident.span(),
                );
            }

            // HVG004: SleepTimer
            if path_str == "std::thread::sleep"
                || path_str == "thread::sleep"
                || path_str == "tokio::time::sleep"
                || path_str == "time::sleep"
                || path_str == "async_std::task::sleep"
            {
                self.add_finding(
                    "HVG004",
                    expr_path.path.segments.last().unwrap().ident.span(),
                );
            }

            // HVG005: BackgroundTask
            if path_str == "tokio::spawn"
                || path_str == "std::thread::spawn"
                || path_str == "thread::spawn"
                || path_str == "async_std::task::spawn"
                || path_str == "rayon::spawn"
            {
                self.add_finding(
                    "HVG005",
                    expr_path.path.segments.last().unwrap().ident.span(),
                );
            }

            // HVG006: DirectIo
            if path_str.starts_with("std::fs::")
                || path_str.starts_with("fs::")
                || path_str.starts_with("tokio::fs::")
                || path_str.starts_with("std::net::TcpStream::")
                || path_str.starts_with("std::net::TcpListener::")
                || path_str.starts_with("std::net::UdpSocket::")
                || path_str.starts_with("tokio::net::TcpStream::")
                || path_str.starts_with("tokio::net::TcpListener::")
                || path_str.starts_with("tokio::net::UdpSocket::")
                || path_str.starts_with("tokio::net::UnixStream::")
                || path_str.starts_with("tokio::net::UnixListener::")
                || path_str.starts_with("tokio::net::UnixDatagram::")
                || path_str == "TcpStream::connect"
                || path_str == "TcpListener::bind"
                || path_str == "UdpSocket::bind"
                || path_str == "UnixStream::connect"
                || path_str == "UnixListener::bind"
                || path_str == "UnixDatagram::bind"
                || path_str.starts_with("reqwest::")
                || path_str.starts_with("hyper::")
                || path_str.starts_with("diesel::")
                || path_str.starts_with("sqlx::")
                || path_str.starts_with("tonic::")
                || path_str.starts_with("tokio_postgres::")
                || path_str == "Command::new"
                || path_str == "std::process::Command::new"
            {
                self.add_finding(
                    "HVG006",
                    expr_path.path.segments.last().unwrap().ident.span(),
                );
            }
        }

        // Delegate to nested traversal
        syn::visit::visit_expr_call(self, i);
    }

    #[allow(clippy::collapsible_if)]
    fn visit_expr_method_call(&mut self, i: &'ast syn::ExprMethodCall) {
        // HVG007: ProcessGlobal (lock call on uppercase static/const path receiver)
        if i.method == "lock" {
            if let Some(last_seg) = match &*i.receiver {
                Expr::Path(expr_path) => expr_path.path.segments.last(),
                _ => None,
            } {
                let ident_str = last_seg.ident.to_string();
                if ident_str.chars().all(|c| !c.is_lowercase()) {
                    self.add_finding("HVG007", i.method.span());
                }
            }
        }

        let is_await_cond = i.method == "await_condition" || i.method == "await_condition_timeout";
        let old_await_cond = self.in_await_condition_closure;
        if is_await_cond {
            self.in_await_condition_closure = true;
        }

        // HVG002: Randomness method calls (gen, gen_range, etc. on Rng trait)
        let method_str = i.method.to_string();
        if method_str == "gen"
            || method_str == "gen_range"
            || method_str == "gen_bool"
            || method_str == "gen_ratio"
            || method_str == "sample"
            || method_str == "fill"
            || method_str == "try_fill"
        {
            self.add_finding("HVG002", i.method.span());
        }

        let is_ctx_side_effect = if i.method == "side_effect" {
            if let syn::Expr::Path(expr_path) = &*i.receiver {
                expr_path.path.is_ident("ctx")
            } else {
                false
            }
        } else {
            false
        };

        // Delegate to nested traversal
        if is_ctx_side_effect {
            self.visit_expr(&i.receiver);
            if let Some(first_arg) = i.args.first() {
                self.visit_expr(first_arg);
            }
        } else {
            syn::visit::visit_expr_method_call(self, i);
        }

        self.in_await_condition_closure = old_await_cond;
    }

    fn visit_expr_macro(&mut self, i: &'ast syn::ExprMacro) {
        // HVG009: UnsafeLogging
        let path_str = path_to_string(&i.mac.path);
        if path_str == "info"
            || path_str == "warn"
            || path_str == "error"
            || path_str == "debug"
            || path_str == "trace"
            || path_str == "tracing::info"
            || path_str == "tracing::warn"
            || path_str == "tracing::error"
            || path_str == "tracing::debug"
            || path_str == "tracing::trace"
            || path_str == "log::info"
            || path_str == "log::warn"
            || path_str == "log::error"
            || path_str == "log::debug"
            || path_str == "log::trace"
        {
            self.add_finding("HVG009", i.mac.path.segments.last().unwrap().ident.span());
        }

        // Delegate to nested traversal
        syn::visit::visit_expr_macro(self, i);
    }

    fn visit_path(&mut self, i: &'ast syn::Path) {
        let path_str = path_to_string(i);
        if path_str == "rand::rngs::OsRng" || path_str == "OsRng" {
            self.add_finding("HVG002", i.segments.last().unwrap().ident.span());
        }
        syn::visit::visit_path(self, i);
    }
}

pub fn load_catalog_metadata() -> HashMap<String, RuleInfo> {
    let mut rules = HashMap::new();
    let entries = vec![
        RuleInfo {
            id: "HVG001".to_string(),
            severity: "HardBlocker".to_string(),
            explanation: "Reading wall-clock time inside a workflow (std::time::Instant::now(), SystemTime::now(), chrono::Utc::now(), etc.) produces a different value on every replay. The workflow engine re-executes the function body deterministically against recorded history; a fresh timestamp breaks that contract and causes a non-determinism error.".to_string(),
            alternative: "Use ctx.now() (WorkflowContext) to read the workflow-logical clock, which returns the WorkflowStarted timestamp and replays identically on every subsequent run. For a real wall-clock instant captured at the call site (e.g. \"skip the notification if the event is older than 24h *now*\"), use ctx.system_now() -> DateTime<Utc> (or ctx.system_time_now() -> SystemTime), which captures the current time once and replays it deterministically via a recorded SideEffectRecorded event.".to_string(),
        },
        RuleInfo {
            id: "HVG002".to_string(),
            severity: "HardBlocker".to_string(),
            explanation: "Calling rand::random(), rand::thread_rng(), Uuid::new_v4(), or any other source of randomness directly in a workflow body produces a different value on each replay pass. Since the workflow function is re-run from the top on every resume, the random sequence diverges from what was recorded in harvest_events.".to_string(),
            alternative: "Use the deterministic primitives on WorkflowContext, which capture the value once and replay it verbatim: ctx.new_uuid() for a UUIDv7 (idempotency keys), ctx.random_u64() / ctx.random_f64() / ctx.random_range(range) for sampling draws, or ctx.side_effect(name, f) to capture any one-shot non-deterministic value. For cryptographically secure randomness, generate it inside an activity (ActivityContext) and return it as the durably-recorded activity result instead.".to_string(),
        },
        RuleInfo {
            id: "HVG003".to_string(),
            severity: "HardBlocker".to_string(),
            explanation: "Reading std::env::var(), std::env::args(), or process environment at workflow execution time couples replay correctness to the environment of whichever worker process happens to run the replay. Environment variables may differ between the original execution host and a replay host, causing divergence.".to_string(),
            alternative: "Read configuration at worker startup (WorkerConfig) and pass it as typed state via ctx.state::<T>(). For values that must vary per workflow run, pass them as workflow input parameters. Signal-based reconfiguration can use ctx.wait_for_signal() to update workflow-local state durably.".to_string(),
        },
        RuleInfo {
            id: "HVG004".to_string(),
            severity: "HardBlocker".to_string(),
            explanation: "Calling std::thread::sleep(), tokio::time::sleep(), async_std::task::sleep(), or any OS/runtime sleep primitive in a workflow body does not record a durable timer. The workflow worker's task is blocked but no timer event is appended to harvest_events. After a worker restart the sleep is replayed as a no-op, changing observable timing.".to_string(),
            alternative: "Use ctx.timer(timer_id, duration_secs) (WorkflowContext) which emits a TimerStarted event into the durable history. The timer is enforced by the harvest timeout scanner and survives worker restarts. For periodic scheduling, use DagBuilder with an Interval or Cron schedule.".to_string(),
        },
        RuleInfo {
            id: "HVG005".to_string(),
            severity: "HardBlocker".to_string(),
            explanation: "Spawning a background task with tokio::spawn(), std::thread::spawn(), async_std::task::spawn(), or rayon::spawn() from inside a workflow function creates untracked side-effects. The spawned task runs outside Harvest's supervision: its completion is not recorded in harvest_events, it is not retried on failure, and it is silently abandoned on worker restart.".to_string(),
            alternative: "Model concurrent work as parallel activity branches using ctx.execute_activity_raw(name, input, queue) calls combined with futures::join! or futures::try_join!. Harvest records each branch's result durably and re-joins them correctly on replay. For fire-and-forget side-effects, use a local activity (#[activity(local = true)]) so the result is at least logged to history.".to_string(),
        },
        RuleInfo {
            id: "HVG006".to_string(),
            severity: "HardBlocker".to_string(),
            explanation: "Performing network requests (reqwest, hyper, tonic), database queries (sqlx, diesel, tokio-postgres), or filesystem I/O (std::fs, tokio::fs) directly inside a workflow body creates non-idempotent side-effects that are invisible to the event store. On replay the I/O is re-executed, potentially sending duplicate requests, corrupting database state, or failing because external state has changed.".to_string(),
            alternative: "Wrap all I/O in activities (#[activity]). Activities are the unit of durable, retryable side-effect in Harvest. Their inputs and outputs are recorded in harvest_events so the workflow can replay without re-executing I/O. Use ctx.execute_activity_raw(name, input, queue) from the workflow body to schedule the activity.".to_string(),
        },
        RuleInfo {
            id: "HVG007".to_string(),
            severity: "HardBlocker".to_string(),
            explanation: "Mutating process-global state — static mut variables, std::sync::Mutex guards wrapping shared counters, lazy_static or once_cell singletons, global metrics registries, or similar — from inside a workflow body creates a side-effect that is not recorded in harvest_events. On replay the mutation is re-applied, producing double-counting or inconsistent global state across worker processes.".to_string(),
            alternative: "Keep workflow execution stateless with respect to process globals. Accumulate workflow-local state in local variables across ctx.timer() and ctx.execute_activity_raw() boundaries. If you need to emit metrics or update a registry, do so inside an activity where the side-effect is bounded to a single retryable execution unit and is not re-applied on replay.".to_string(),
        },
        RuleInfo {
            id: "HVG008".to_string(),
            severity: "HardBlocker".to_string(),
            explanation: "Evaluating non-deterministic predicates inside await_condition or await_condition_timeout (such as checking Instant::now(), SystemTime::now(), or calling random generators inside the closure) leads to non-deterministic execution paths during replay. Predicates must be pure projections of deterministic workflow local state variables rehydrated by replaying events.".to_string(),
            alternative: "Use durable timers (ctx.timer()) for time-based pauses, and ensure the predicate closure relies purely on local variables mutated by deterministic signals or activities.".to_string(),
        },
        RuleInfo {
            id: "HVG009".to_string(),
            severity: "Warning".to_string(),
            explanation: "Calling tracing::info!(), tracing::warn!(), tracing::error!(), or any other bare tracing macro directly inside a #[workflow] body emits one log event per replay cycle. Because the workflow executor re-runs the function from the top on every suspension/resume, a single log statement fires N times for a workflow that suspends N times. This amplifies log volume in proportion to replay depth and fills Loki/Elastic with duplicate lines that lack correlation keys, making incident triage harder.".to_string(),
            alternative: "Use ctx.logger().info(message), ctx.logger().warn(message), ctx.logger().error(message), or the convenience wrappers ctx.log_info(message), ctx.log_warn(message), ctx.log_error(message). These are suppressed automatically during replay (is_replaying() == true) and auto-tag every event with workflow_id, execution_id, workflow_type, and replay = false for log correlation.".to_string(),
        },
    ];

    for entry in entries {
        rules.insert(entry.id.clone(), entry);
    }
    rules
}
