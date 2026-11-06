//! The bridge between a synchronous Rhai script and the async workflow body.
//!
//! Rhai is synchronous, and a Harvest wait is an `async` call on a borrowed
//! `WorkflowContext`. So the script runs on its own thread. Each host
//! function sends one [`Request`] to the workflow body and blocks on the
//! reply. The body owns the context, awaits the Harvest call, and sends the
//! result back.
//!
//! The body blocks on the request channel. It does not await it. So the
//! executor sees only Harvest futures, exactly as for inline workflow code.
//! An awaited channel is a foreign future, and a query replay treats a
//! foreign wait as a park. The block is short, because the script thread runs
//! only pure Rhai code between two requests, and [`MAX_OPERATIONS`] bounds it.
//!
//! A call that has no recorded result suspends the workflow. The executor
//! then drops the body, and that drop closes the reply channel. The blocked
//! host function sees the closed channel and stops the script with a
//! terminate error, which `try`/`catch` in a script cannot catch. The next
//! decision replays the script from the top. Each recorded call returns its
//! stored result at once.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;

use rhai::{Dynamic, Engine, EvalAltResult, Position, Scope};
use serde_json::Value;

use crate::host;

/// The default number of Rhai operations that one replay may run.
///
/// The bound stops a script that loops forever between two host calls. Such a
/// script would never reach a point where the engine saves progress.
pub const MAX_OPERATIONS: u64 = 10_000_000;

/// One durable operation that a script asks the workflow body to run.
#[derive(Debug)]
pub enum Op {
    /// Run a registered host activity with this JSON input.
    Activity {
        /// The activity name, one of the `host::*` constants.
        name: &'static str,
        /// The activity input.
        input: Value,
    },
    /// Start a durable timer for this many seconds.
    Sleep {
        /// The timer length in seconds.
        secs: u64,
    },
    /// Wait for the next signal with this name.
    WaitFor {
        /// The signal name.
        signal: String,
    },
}

/// One message from the script thread to the workflow body.
#[derive(Debug)]
pub enum Request {
    /// Run `op`, then send its result on `reply`.
    Call {
        /// The operation to run.
        op: Op,
        /// The channel that carries the result back to the script.
        reply: mpsc::Sender<Result<Value, String>>,
    },
    /// A `print` or `debug` line of the script.
    Log(String),
    /// A new value for the `status` query, set by `progress(..)`.
    Progress(String),
    /// The script ended with this value or this error.
    Done(Result<Value, String>),
}

/// The values that a script run needs from its workflow input.
#[derive(Debug, Clone)]
pub struct ScriptJob {
    /// The script source.
    pub source: String,
    /// The script arguments. The script reads them as the `ARGS` array.
    pub args: Vec<String>,
    /// The directory that relative paths and shell commands use.
    pub cwd: String,
    /// The operation bound of one replay.
    pub max_operations: u64,
}

/// Start the script on a new thread. Its requests arrive on `tx`.
///
/// # Errors
///
/// Returns an error if the operating system cannot start the thread.
pub fn spawn(job: ScriptJob, tx: mpsc::Sender<Request>) -> std::io::Result<()> {
    std::thread::Builder::new()
        .name("rhai-script".to_string())
        .spawn(move || {
            let result = run(&job, &tx);
            // After a suspension the body is gone, so a failed send is expected.
            let _ = tx.send(Request::Done(result));
        })
        .map(drop)
}

/// Run the script to its end, or to its first unrecorded call.
fn run(job: &ScriptJob, tx: &mpsc::Sender<Request>) -> Result<Value, String> {
    let stopped = Arc::new(AtomicBool::new(false));
    let engine = engine(job, tx, &stopped);
    let mut scope = Scope::new();
    let args: rhai::Array = job.args.iter().cloned().map(Dynamic::from).collect();
    scope.push_constant("ARGS", args);
    let value = engine
        .eval_with_scope::<Dynamic>(&mut scope, &job.source)
        .map_err(|err| err.to_string())?;
    rhai::serde::from_dynamic::<Value>(&value).map_err(|err| err.to_string())
}

/// Check that `source` parses, without running it.
///
/// # Errors
///
/// Returns the parse error as text.
pub fn check(source: &str) -> Result<(), String> {
    sandbox(MAX_OPERATIONS)
        .compile(source)
        .map(drop)
        .map_err(|e| e.to_string())
}

/// An engine with no access to the clock, the environment or the disk.
///
/// `no_time` removes `timestamp()`. The dummy resolver refuses each `import`,
/// because a module file could change between two replays.
fn sandbox(max_operations: u64) -> Engine {
    let mut engine = Engine::new();
    engine.set_module_resolver(rhai::module_resolvers::DummyModuleResolver::new());
    engine.set_max_operations(max_operations);
    engine
}

/// The sandbox plus the host functions of one run.
fn engine(job: &ScriptJob, tx: &mpsc::Sender<Request>, stopped: &Arc<AtomicBool>) -> Engine {
    let mut engine = sandbox(job.max_operations);

    // A stopped script ends at its next operation, even inside `catch`.
    let flag = Arc::clone(stopped);
    engine.on_progress(move |_| flag.load(Ordering::SeqCst).then_some(Dynamic::UNIT));

    let log = tx.clone();
    engine.on_print(move |line| {
        let _ = log.send(Request::Log(line.to_string()));
    });
    let log = tx.clone();
    engine.on_debug(move |line, _, _| {
        let _ = log.send(Request::Log(line.to_string()));
    });

    let caller = Caller {
        tx: tx.clone(),
        stopped: Arc::clone(stopped),
        cwd: job.cwd.clone(),
    };

    let c = caller.clone();
    engine.register_fn(
        "sh",
        move |cmd: &str| -> Result<String, Box<EvalAltResult>> {
            c.text(host::SH, serde_json::json!({ "cmd": cmd, "cwd": c.cwd }))
        },
    );
    let c = caller.clone();
    engine.register_fn(
        "read_file",
        move |path: &str| -> Result<String, Box<EvalAltResult>> {
            c.text(
                host::READ_FILE,
                serde_json::json!({ "path": path, "cwd": c.cwd }),
            )
        },
    );
    let c = caller.clone();
    engine.register_fn(
        "write_file",
        move |path: &str, text: &str| -> Result<(), Box<EvalAltResult>> {
            let input = serde_json::json!({ "path": path, "text": text, "cwd": c.cwd });
            c.call(Op::Activity {
                name: host::WRITE_FILE,
                input,
            })
            .map(drop)
        },
    );
    let c = caller.clone();
    engine.register_fn(
        "env",
        move |name: &str| -> Result<Dynamic, Box<EvalAltResult>> {
            let value = c.call(Op::Activity {
                name: host::ENV,
                input: serde_json::json!({ "name": name }),
            })?;
            rhai::serde::to_dynamic(value)
        },
    );
    let c = caller.clone();
    engine.register_fn(
        "sleep",
        move |secs: i64| -> Result<(), Box<EvalAltResult>> {
            let secs =
                u64::try_from(secs).map_err(|_| format!("sleep({secs}): negative length"))?;
            c.call(Op::Sleep { secs }).map(drop)
        },
    );
    let c = caller.clone();
    engine.register_fn(
        "wait_for",
        move |signal: &str| -> Result<Dynamic, Box<EvalAltResult>> {
            let payload = c.call(Op::WaitFor {
                signal: signal.to_string(),
            })?;
            rhai::serde::to_dynamic(payload)
        },
    );
    let c = caller;
    engine.register_fn("progress", move |message: &str| {
        let _ = c.tx.send(Request::Progress(message.to_string()));
    });
    engine
}

/// The handle that each host function uses to reach the workflow body.
#[derive(Clone)]
struct Caller {
    tx: mpsc::Sender<Request>,
    stopped: Arc<AtomicBool>,
    cwd: String,
}

impl Caller {
    /// Send `op` and block until the workflow body replies.
    ///
    /// A closed channel means that the workflow suspended. The script then
    /// stops with a terminate error. A failed operation is an ordinary
    /// runtime error, so a script can catch it.
    fn call(&self, op: Op) -> Result<Value, Box<EvalAltResult>> {
        let (reply, answer) = mpsc::channel();
        if self.tx.send(Request::Call { op, reply }).is_err() {
            return Err(self.stop());
        }
        match answer.recv() {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(error)) => Err(error.into()),
            Err(mpsc::RecvError) => Err(self.stop()),
        }
    }

    /// Run a host activity whose result is a string.
    fn text(&self, name: &'static str, input: Value) -> Result<String, Box<EvalAltResult>> {
        match self.call(Op::Activity { name, input })? {
            Value::String(text) => Ok(text),
            other => Err(format!("{name} returned {other}, not a string").into()),
        }
    }

    /// Mark the script as stopped and build the error that ends it.
    // Every Rhai function returns its error boxed, so the box is the API.
    #[allow(clippy::unnecessary_box_returns)]
    fn stop(&self) -> Box<EvalAltResult> {
        self.stopped.store(true, Ordering::SeqCst);
        EvalAltResult::ErrorTerminated(Dynamic::UNIT, Position::NONE).into()
    }
}
