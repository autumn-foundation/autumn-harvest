//! The host activities. Each one is the only way a script reaches the world.
//!
//! The engine records the result of each attempt in history. A replay reads
//! that result and does not run the body again. A body that was running when
//! the process died runs again on resume, so a step runs at least once.

use std::path::{Path, PathBuf};

use autumn_harvest_sqlite::{ActivitySpec, SqliteRuntime};
use serde_json::{Value, json};
use smop::sh;

/// Runs a shell command and returns its trimmed standard output.
pub const SH: &str = "rhai.sh";
/// Reads a UTF-8 file.
pub const READ_FILE: &str = "rhai.read_file";
/// Writes a UTF-8 file. It creates the parent directories.
pub const WRITE_FILE: &str = "rhai.write_file";
/// Reads an environment variable. An unset variable reads as `()`.
pub const ENV: &str = "rhai.env";

/// Register every host activity on `rt`. Each one makes one attempt.
///
/// A script decides what a failure means, so the engine does not retry.
pub fn register(rt: &mut SqliteRuntime) {
    rt.register_activity_raw(SH, ActivitySpec::new(1, run_sh));
    rt.register_activity_raw(READ_FILE, ActivitySpec::new(1, read_file));
    rt.register_activity_raw(WRITE_FILE, ActivitySpec::new(1, write_file));
    rt.register_activity_raw(ENV, ActivitySpec::new(1, read_env));
}

/// The `rhai.sh` body.
// The activity signature takes the input by value.
#[allow(clippy::needless_pass_by_value)]
fn run_sh(input: Value) -> Result<Value, String> {
    let cmd = field(&input, "cmd")?;
    let cwd = field(&input, "cwd")?;
    shell(cmd)
        .dir(cwd)
        .output()
        .map(Value::String)
        .map_err(|err| format!("{err:#}"))
}

/// A POSIX shell with `cmd` as its script.
///
/// The example needs `sh` on `PATH`. Its CI step skips Windows.
fn shell(cmd: &str) -> sh::CommandBuilder {
    sh::cmd("sh").arg("-c").arg(cmd)
}

/// The `rhai.read_file` body.
#[allow(clippy::needless_pass_by_value)]
fn read_file(input: Value) -> Result<Value, String> {
    let path = resolve(&input)?;
    smop::fs::read_string(&path)
        .map(Value::String)
        .map_err(|err| format!("{err:#}"))
}

/// The `rhai.write_file` body.
#[allow(clippy::needless_pass_by_value)]
fn write_file(input: Value) -> Result<Value, String> {
    let path = resolve(&input)?;
    let text = field(&input, "text")?;
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        smop::fs::create_dir_all(parent).map_err(|err| format!("{err:#}"))?;
    }
    smop::fs::write_string(&path, text)
        .map(|()| Value::Null)
        .map_err(|err| format!("{err:#}"))
}

/// The `rhai.env` body.
#[allow(clippy::needless_pass_by_value)]
fn read_env(input: Value) -> Result<Value, String> {
    let name = field(&input, "name")?;
    Ok(std::env::var(name).map_or(Value::Null, Value::String))
}

/// The string field `key` of an activity input.
fn field<'a>(input: &'a Value, key: &str) -> Result<&'a str, String> {
    input
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("activity input has no string `{key}`: {}", json!(input)))
}

/// The `path` field, resolved against the `cwd` field.
fn resolve(input: &Value) -> Result<PathBuf, String> {
    let path = Path::new(field(input, "path")?);
    Ok(Path::new(field(input, "cwd")?).join(path))
}
