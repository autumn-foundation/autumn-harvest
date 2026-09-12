//! The local toolbox one agent session can act with.
//!
//! Three tools, all confined to one workspace directory: list a directory,
//! read a file, and write a file. A write changes the machine, so it is the
//! one tool the workflow gates on human approval.
//!
//! Every path is relative to the workspace root. [`resolve`] rejects an
//! absolute path and any path that leaves the root, so a model cannot reach
//! the rest of the disk.

use std::io::Read;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};

use serde_json::{Value, json};

use autumn_harvest::failure::{ActivityFailure, IntoActivityErrorString};

use crate::session::{ToolCall, ToolOutcome, ToolRequest};

/// List the entries of one directory.
pub const TOOL_LIST_FILES: &str = "list_files";
/// Read one text file.
pub const TOOL_READ_FILE: &str = "read_file";
/// Write one text file. This tool needs approval.
pub const TOOL_WRITE_FILE: &str = "write_file";

/// The largest file this toolbox reads or writes.
const MAX_FILE_BYTES: usize = 64 * 1024;
/// The largest directory listing this toolbox returns.
pub const MAX_ENTRIES: usize = 200;

/// How many scratch names one write tries before it gives up.
const SCRATCH_ATTEMPTS: u32 = 16;

/// The longest scratch basename this module builds.
///
/// A file name component is limited by the filesystem, and 255 bytes is the
/// common limit. A 255-byte target is therefore legal, and a scratch name that
/// copies it whole is not. All 16 attempts then fail with `ENAMETOOLONG`, and
/// an approved write becomes impossible.
const SCRATCH_CAP: usize = 255;

/// The scratch length a SHORT target still allows.
///
/// The cap above assumes the common limit. This floor removes the assumption
/// for a long target. The scratch name never exceeds the target's own name
/// once that name is longer than this floor. The target name is itself proof
/// that the length is allowed. An ordinary short name keeps its whole stem in
/// the scratch name, which is what makes a leftover file identifiable.
const SCRATCH_FLOOR: usize = 96;

/// The `setuid` and `setgid` bits, which a written file never keeps.
const SET_ID_BITS: u32 = 0o6000;

/// The mode a file this toolbox CREATES is given.
///
/// A file the agent brings into being starts private. An existing file keeps
/// its own mode instead, because a content change is not a permission change.
const NEW_FILE_MODE: u32 = 0o600;

/// Does a call to this tool wait for human approval?
pub fn needs_approval(tool_name: &str) -> bool {
    tool_name == TOOL_WRITE_FILE
}

/// The `tools` array of the Messages API request.
pub fn definitions() -> Value {
    json!([
        {
            "name": TOOL_LIST_FILES,
            "description": "List the files and directories under a path in the workspace. \
                            Use \".\" for the workspace root.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Directory path, relative to the workspace root." }
                },
                "required": ["path"],
                "additionalProperties": false
            },
            "strict": true
        },
        {
            "name": TOOL_READ_FILE,
            "description": "Read one UTF-8 text file from the workspace.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "File path, relative to the workspace root." }
                },
                "required": ["path"],
                "additionalProperties": false
            },
            "strict": true
        },
        {
            "name": TOOL_WRITE_FILE,
            "description": "Write one UTF-8 text file in the workspace, replacing any previous \
                            content. A human approves each call before it runs.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "File path, relative to the workspace root." },
                    "content": { "type": "string", "description": "The complete new file content." }
                },
                "required": ["path", "content"],
                "additionalProperties": false
            },
            "strict": true
        }
    ])
}

/// Build the synchronous activity body the runtime registers for `run_tool`.
///
/// The daemon's workspace root is captured here. The call carries the workspace
/// its session was started in, and the two must be the same one. A daemon
/// restarted on another directory therefore refuses the call instead of running
/// an already-approved write against the wrong project. The refusal is
/// non-retryable, so the session fails loudly and the operator can restart the
/// daemon where the session belongs.
pub fn activity_body(
    workspace: PathBuf,
) -> impl Fn(Value) -> Result<Value, String> + Send + Sync + 'static {
    move |input| {
        let request: ToolRequest =
            serde_json::from_value(input).map_err(|e| format!("malformed tool call: {e}"))?;
        if !serves(&workspace, &request.workspace) {
            return Err(ActivityFailure::non_retryable(
                "WorkspaceMismatch",
                format!(
                    "this session belongs to the workspace `{}`, and this daemon serves `{}`",
                    request.workspace,
                    workspace.display()
                ),
            )
            .into_error_payload());
        }
        let outcome = dispatch(&workspace, &request.call);
        serde_json::to_value(outcome).map_err(|e| format!("tool result is not JSON: {e}"))
    }
}

/// Does this daemon serve the workspace the session was started in?
///
/// The comparison is between resolved paths, so a different spelling of one
/// directory still matches.
fn serves(root: &Path, recorded: &str) -> bool {
    root.canonicalize()
        .is_ok_and(|real| real == Path::new(recorded))
}

/// Run one tool call.
///
/// A tool failure is a `ToolOutcome` with `is_error`, never an activity error.
/// The model reads the message and picks its next step, which is how a real
/// harness recovers from a bad path or a missing file.
fn dispatch(workspace: &Path, call: &ToolCall) -> ToolOutcome {
    let result = match call.name.as_str() {
        TOOL_LIST_FILES => string_arg(&call.input, "path").and_then(|p| list_files(workspace, &p)),
        TOOL_READ_FILE => string_arg(&call.input, "path").and_then(|p| read_file(workspace, &p)),
        TOOL_WRITE_FILE => string_arg(&call.input, "path").and_then(|p| {
            let content = string_arg(&call.input, "content")?;
            write_file(workspace, &p, &content)
        }),
        other => Err(format!("unknown tool `{other}`")),
    };

    match result {
        Ok(output) => ToolOutcome {
            output,
            is_error: false,
        },
        Err(message) => ToolOutcome::error(message),
    }
}

/// Read one required string argument out of a tool input.
fn string_arg(input: &Value, key: &str) -> Result<String, String> {
    input
        .get(key)
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| format!("the `{key}` argument is missing or is not a string"))
}

/// Resolve a caller path against the workspace root, and refuse a target that
/// leaves it.
///
/// The check has three parts, because a lexical rule alone is not enough. A
/// symbolic link inside the workspace redirects a read or a write after the
/// path has already passed a lexical test.
///
/// 1. **Lexical.** An absolute path, a parent traversal, and a root prefix are
///    rejected.
/// 2. **The final component.** A symbolic link there is refused outright, even
///    one that points inside the workspace. A dangling link reports
///    `exists() == false`, so the link itself is tested, not its target.
/// 3. **The path above it.** The deepest EXISTING ancestor is resolved through
///    every symbolic link and must sit under the real workspace root. That
///    catches a link in the middle of the path, and it works for a write target
///    that does not exist yet.
fn resolve(workspace: &Path, relative: &str) -> Result<PathBuf, String> {
    // The root itself can sit behind a link, so compare against its real path.
    let root = workspace
        .canonicalize()
        .map_err(|e| format!("cannot resolve the workspace: {e}"))?;

    let candidate = Path::new(relative);
    if candidate.is_absolute() {
        return Err(format!(
            "`{relative}` is absolute; use a workspace-relative path"
        ));
    }
    for component in candidate.components() {
        match component {
            Component::Normal(_) | Component::CurDir => {}
            _ => return Err(format!("`{relative}` leaves the workspace")),
        }
    }
    let path = root.join(candidate);

    if std::fs::symlink_metadata(&path).is_ok_and(|meta| meta.file_type().is_symlink()) {
        return Err(format!(
            "`{relative}` is a symbolic link, and the toolbox refuses one"
        ));
    }

    let mut probe = path.as_path();
    let resolved = loop {
        if let Ok(real) = probe.canonicalize() {
            break real;
        }
        probe = probe
            .parent()
            .ok_or_else(|| format!("cannot resolve `{relative}`"))?;
    };
    if !resolved.starts_with(&root) {
        return Err(format!("`{relative}` leaves the workspace"));
    }

    Ok(path)
}

/// List one directory, sorted, with a trailing slash on each subdirectory.
fn list_files(workspace: &Path, relative: &str) -> Result<String, String> {
    let dir = resolve(workspace, relative)?;
    let mut entries = Vec::new();
    let mut more = false;
    for entry in std::fs::read_dir(&dir).map_err(|e| format!("cannot list `{relative}`: {e}"))? {
        // The cap stops the read, and does not trim the result afterwards. A
        // directory of a million entries would otherwise be named in full
        // before the trim. The tool bodies run on the one runtime, so that
        // blocks every session and every control command too.
        if entries.len() == MAX_ENTRIES {
            more = true;
            break;
        }
        let entry = entry.map_err(|e| format!("cannot list `{relative}`: {e}"))?;
        let name = entry.file_name().to_string_lossy().into_owned();
        let is_dir = entry.file_type().is_ok_and(|t| t.is_dir());
        entries.push(if is_dir { format!("{name}/") } else { name });
    }
    entries.sort();
    // The count of the rest is not reported, because counting it is the work
    // this cap exists to refuse.
    if more {
        entries.push(format!(
            "... more entries; the listing stops at {MAX_ENTRIES}"
        ));
    }
    if entries.is_empty() {
        return Ok(format!("`{relative}` is empty"));
    }
    Ok(entries.join("\n"))
}

/// Read one text file, up to the size cap.
///
/// The cap is applied BEFORE the file is allocated. A plain read of a
/// multi-gigabyte file would exhaust the daemon and stop every session. So the
/// size is checked first, and the read itself is bounded. The second bound
/// matters because the file can grow between the two steps.
fn read_file(workspace: &Path, relative: &str) -> Result<String, String> {
    let path = resolve(workspace, relative)?;
    let file = open_regular(&path, relative)?;

    // The size comes from the OPEN descriptor, so it describes the file that
    // was opened rather than whatever the path named a moment earlier.
    let size = file
        .metadata()
        .map_err(|e| format!("cannot read `{relative}`: {e}"))?
        .len();
    if size > MAX_FILE_BYTES as u64 {
        return Err(format!(
            "`{relative}` is {size} bytes; the limit is {MAX_FILE_BYTES}"
        ));
    }

    let mut bytes = Vec::new();
    file.take(MAX_FILE_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| format!("cannot read `{relative}`: {e}"))?;
    if bytes.len() > MAX_FILE_BYTES {
        return Err(format!(
            "`{relative}` grew past the {MAX_FILE_BYTES} byte limit while it was read"
        ));
    }

    String::from_utf8(bytes).map_err(|_| format!("`{relative}` is not UTF-8 text"))
}

/// Open a path for reading, and prove it is an ordinary file.
///
/// Two flags carry the safety here. `O_NOFOLLOW` refuses a symbolic link at the
/// final component, even one that appears between the check and this open.
/// `O_NONBLOCK` stops a FIFO from blocking the open itself. A named pipe with
/// no writer would otherwise hang this body, and with it the whole daemon. One
/// runtime serves every session and every command.
///
/// The file type is then read from the descriptor, so the answer describes what
/// was opened and cannot be swapped afterwards.
fn open_regular(path: &Path, relative: &str) -> Result<std::fs::File, String> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc_o_nofollow() | libc_o_nonblock())
        .open(path)
        .map_err(|e| format!("cannot read `{relative}`: {e}"))?;

    let kind = file
        .metadata()
        .map_err(|e| format!("cannot read `{relative}`: {e}"))?
        .file_type();
    if !kind.is_file() {
        return Err(format!("`{relative}` is not an ordinary file"));
    }
    Ok(file)
}

/// `O_NOFOLLOW`, from the platform's own headers.
const fn libc_o_nofollow() -> i32 {
    rustix::fs::OFlags::NOFOLLOW.bits().cast_signed()
}

/// `O_NONBLOCK`, from the platform's own headers.
const fn libc_o_nonblock() -> i32 {
    rustix::fs::OFlags::NONBLOCK.bits().cast_signed()
}

/// Write one text file, creating the parent directories.
///
/// The body is idempotent: the same call writes the same bytes. That matters
/// because activity execution is at-least-once. A crash between the write and
/// its commit re-runs this body on resume.
fn write_file(workspace: &Path, relative: &str, content: &str) -> Result<String, String> {
    if content.len() > MAX_FILE_BYTES {
        return Err(format!(
            "the content is {} bytes; the limit is {MAX_FILE_BYTES}",
            content.len()
        ));
    }
    let path = resolve(workspace, relative)?;

    // An existing target must be an ordinary file. A FIFO would block this
    // body, and with it the whole daemon, and a device is not something a tool
    // call should write through.
    if std::fs::symlink_metadata(&path).is_ok_and(|existing| !existing.file_type().is_file()) {
        return Err(format!("`{relative}` is not an ordinary file"));
    }

    if let Some(parent) = path.parent() {
        create_enterable(parent)
            .map_err(|e| format!("cannot create the parent of `{relative}`: {e}"))?;
    }
    // Every directory between the target and the workspace root can hold an
    // entry this write created, so the whole chain is flushed after the
    // rename. See [`directories_to_flush`].
    let flush = directories_to_flush(&path, workspace);

    // Write through a temporary file beside the target, then rename over it. A
    // write that fails part way, on a full disk or a quota, would otherwise
    // leave the approved file truncated. The tool reports that as a result
    // rather than an activity error, so nothing would retry it. The rename is
    // atomic inside one directory, so the target holds the whole content or it
    // is untouched.
    // The rename replaces the target's inode, so the scratch file carries the
    // mode the result must have. An existing target keeps its own mode. The
    // operator approved a change of content. Making a private file
    // world-readable, or dropping a script's execute bits, is not that.
    //
    // The set-ID bits are the exception, and they are dropped. The new inode
    // belongs to the daemon, and the model chose every byte in it. A `setuid`
    // file here would run as the daemon's user for anyone who could execute
    // it. The approval shows the path and the content, and never the mode, so
    // an operator cannot see that they approved such a thing.
    let mode = std::fs::metadata(&path).map_or(NEW_FILE_MODE, |existing| {
        existing.permissions().mode() & 0o7777 & !SET_ID_BITS
    });

    let (temporary, file) = create_scratch(&path)?;
    match write_through(file, &temporary, &path, content, mode, &flush) {
        Ok(()) => Ok(format!("wrote {} bytes to `{relative}`", content.len())),
        Err(WriteFailure::BeforeRename(e)) => {
            // Nothing replaced the target, so the scratch file is litter.
            drop(std::fs::remove_file(&temporary));
            Err(format!("cannot write `{relative}`: {e}"))
        }
        // The target IS replaced. Reporting that nothing was written would be
        // false, and the model could undo work that landed. What failed is the
        // durability of the change, not the change.
        Err(WriteFailure::AfterRename(e)) => Err(format!(
            "`{relative}` now holds the {} bytes, and the change is not flushed \
             to the disk yet: {e}. A host crash could still lose it.",
            content.len()
        )),
    }
}

/// The directories a finished write must flush, deepest first.
///
/// The chain runs from the target's own directory up to the workspace root.
/// Each one can hold an entry this write created, and an entry is durable only
/// after the directory that names it is flushed.
///
/// The chain is walked, rather than collected while the directories are
/// created. Activity execution is at-least-once. A crash after a directory is
/// created, but before its entry is flushed, leaves that directory in place.
/// The retry then creates nothing. A list of its own creations would name
/// nothing to flush, while the entry naming the directory is still unwritten.
pub fn directories_to_flush(target: &Path, workspace: &Path) -> Vec<PathBuf> {
    let root = workspace
        .canonicalize()
        .unwrap_or_else(|_| workspace.to_path_buf());
    let chain: Vec<PathBuf> = target
        .parent()
        .into_iter()
        .flat_map(Path::ancestors)
        .take_while(|directory| directory.starts_with(&root))
        .map(Path::to_path_buf)
        .collect();

    // The target sits under the root, so the chain holds the root at least. A
    // spelling that did not compare would otherwise flush nothing at all. The
    // directory entry of the target is the one that must not be lost.
    if chain.is_empty() {
        return target.parent().map(Path::to_path_buf).into_iter().collect();
    }
    chain
}

/// Create a directory and its missing parents, each one the owner can enter.
///
/// `create_dir_all` asks for mode `0777`, and the umask decides what survives.
/// A umask that masks the owner bits therefore gives a new directory mode
/// `000`. The daemon cannot enter its own directory after that. The next level
/// down fails, and so does the scratch file of the write. An approved write
/// would need the operator to repair the permissions by hand.
///
/// Only a directory this call creates is adjusted, and only by adding the
/// owner bits. A directory the operator already made narrow keeps the mode
/// they chose.
///
/// The mode is widened after creation, rather than through the umask. The
/// umask is one value for the whole process, and this daemon creates its
/// private files on other threads.
pub fn create_enterable(directory: &Path) -> std::io::Result<()> {
    // The deepest existing level decides. Every level above it has a child.
    // Every level above it is therefore a directory this daemon can enter,
    // which the stat that found the deepest one proves.
    if let Some(deepest) = directory.ancestors().find(|level| level.exists()) {
        // A regular file named as the workspace has nothing missing above it.
        // Nothing would be created, and the daemon would start over a
        // workspace no tool can use.
        if !deepest.is_dir() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotADirectory,
                format!("{} exists and is not a directory", deepest.display()),
            ));
        }
        // A directory that exists and cannot be entered is REFUSED, and not
        // repaired. This call cannot prove it created that directory. An
        // operator can lock one deliberately, and a daemon killed between the
        // creation and the mode leaves the same thing. The two are identical
        // on disk, so widening it would undo a choice that may have been
        // meant. The refusal names the path, which a silent stall did not.
        if !owner_can_enter(deepest) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!(
                    "{} exists and its owner cannot enter it. Repair it or \
                     remove it: this daemon does not change the mode of a \
                     directory it cannot prove it created.",
                    deepest.display()
                ),
            ));
        }
    }

    let missing: Vec<&Path> = directory
        .ancestors()
        .take_while(|level| !level.exists())
        .collect();
    for level in missing.into_iter().rev() {
        match std::fs::create_dir(level) {
            Ok(()) => grant_owner_entry(level)?,
            // Another process reached the same name first. It owns the mode.
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// Can the owner enter this directory?
///
/// A directory without the owner's `x` bit cannot be entered or listed by its
/// owner. Nothing below it can be created, so the caller is refused rather
/// than left to fail one level down.
///
/// Every existing mode is left as it is. A directory that is narrow but
/// usable, such as `0500`, is a mode an operator can mean. A write under it
/// fails with a plain permission error that names the path, which is honest.
fn owner_can_enter(directory: &Path) -> bool {
    std::fs::metadata(directory).is_ok_and(|entry| entry.permissions().mode() & 0o100 != 0)
}

/// Give the owner `rwx` on a directory, keeping the rest of the mode.
fn grant_owner_entry(directory: &Path) -> std::io::Result<()> {
    let mut mode = std::fs::metadata(directory)?.permissions();
    mode.set_mode(mode.mode() | 0o700);
    std::fs::set_permissions(directory, mode)
}

/// Create a scratch file beside the target, and return it with its path.
///
/// The same directory matters: a rename is only atomic within one filesystem.
/// The name is unique per attempt, and the file is created with `create_new`.
/// Nothing that already occupies a name is removed or written through. A fixed
/// name would have to be deleted first, and that name can belong to something
/// a person wants to keep.
fn create_scratch(path: &Path) -> Result<(PathBuf, std::fs::File), String> {
    let parent = path
        .parent()
        .ok_or_else(|| "the target has no directory".to_string())?;
    let name = path
        .file_name()
        .ok_or_else(|| "the target has no file name".to_string())?
        .to_string_lossy()
        .into_owned();

    let pid = std::process::id();
    let nonce = scratch_nonce();
    let mut last = None;
    for attempt in 0..SCRATCH_ATTEMPTS {
        let candidate = parent.join(scratch_name(&name, pid, nonce, attempt));
        // `O_NOFOLLOW` refuses a link, as everywhere else in this module. The
        // mode is deliberately conservative here; the target's mode is applied
        // to the descriptor below, where no umask can filter it.
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(NEW_FILE_MODE)
            .custom_flags(libc_o_nofollow())
            .open(&candidate)
        {
            Ok(file) => return Ok((candidate, file)),
            Err(e) => last = Some(e),
        }
    }

    Err(last.map_or_else(
        || format!("cannot create a scratch file beside `{name}`"),
        |e| format!("cannot create a scratch file beside `{name}`: {e}"),
    ))
}

/// A value that does not repeat across restarts.
///
/// The process id is not enough on its own. A container can start the daemon
/// as pid 1 every time. A crash between the create and the rename leaves that
/// scratch name behind, and nothing removes a file this module did not make.
/// The next start would try the same names, and sixteen such crashes would
/// leave an approved write with no name to use.
///
/// The hasher is seeded by the operating system, once per process, so two
/// daemons that start in the same nanosecond still differ.
pub fn scratch_nonce() -> u64 {
    use std::hash::{BuildHasher, Hash, Hasher};

    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_nanos())
        .unwrap_or_default()
        .hash(&mut hasher);
    std::process::id().hash(&mut hasher);
    hasher.finish()
}

/// The scratch basename for one attempt.
///
/// The name carries the target's stem, so a leftover file says what it was
/// for. The stem is cut when the whole name would not fit (see [`SCRATCH_CAP`]
/// and [`SCRATCH_FLOOR`]). The cut lands on a character boundary, so a name
/// of multi-byte characters is never split through one.
pub fn scratch_name(name: &str, pid: u32, nonce: u64, attempt: u32) -> String {
    let suffix = format!(".agentd-{pid}-{nonce:x}-{attempt}.tmp");
    let cap = name.len().clamp(SCRATCH_FLOOR, SCRATCH_CAP);
    // One byte for the leading dot.
    let room = cap.saturating_sub(suffix.len() + 1);
    format!(".{}{suffix}", &name[..floor_boundary(name, room)])
}

/// The largest character boundary of `text` at or below `limit`.
const fn floor_boundary(text: &str, limit: usize) -> usize {
    if limit >= text.len() {
        return text.len();
    }
    let mut index = limit;
    while index > 0 && !text.is_char_boundary(index) {
        index -= 1;
    }
    index
}

/// Fill the scratch file, give it the target's mode, and rename it over it.
fn write_through(
    mut file: std::fs::File,
    temporary: &Path,
    target: &Path,
    content: &str,
    mode: u32,
    flush: &[PathBuf],
) -> Result<(), WriteFailure> {
    use std::io::Write;

    file.write_all(content.as_bytes())
        .map_err(WriteFailure::BeforeRename)?;

    // `chmod` on the open descriptor, NOT a creation mode. A mode passed to
    // `open` is filtered through the umask, so a `0660` target would come back
    // `0640` under the common one. This sets exactly what was captured.
    file.set_permissions(std::fs::Permissions::from_mode(mode))
        .map_err(WriteFailure::BeforeRename)?;

    // Flush before the rename, so a crash cannot leave the target naming a file
    // whose content never reached the disk.
    file.sync_all().map_err(WriteFailure::BeforeRename)?;
    drop(file);

    std::fs::rename(temporary, target).map_err(WriteFailure::BeforeRename)?;

    // The rename itself is durable only once the DIRECTORY entry is. Without
    // this, a host crash can restore the old target, or lose a new one. The
    // history meanwhile records the write as done and never re-runs it.
    // Syncing the file alone does not cover the entry that names it.
    // A new directory needs the same treatment. A write to `a/b/notes.md` in an
    // empty workspace creates two directories and the file. Flushing only the
    // file's own parent leaves `b` missing from `a` after a crash, and the
    // flushed file goes with it.
    for directory in flush {
        std::fs::File::open(directory)
            .and_then(|handle| handle.sync_all())
            .map_err(WriteFailure::AfterRename)?;
    }

    Ok(())
}

/// Where a write stopped.
///
/// The two sides of the rename are not the same outcome. Before it, the target
/// is untouched and the tool reports that nothing was written. After it, the
/// target IS replaced, and a report of "nothing was written" would be false.
/// The model could then undo work that had in fact landed.
enum WriteFailure {
    /// The target was not touched.
    BeforeRename(std::io::Error),
    /// The target was replaced, and the durability work did not finish.
    AfterRename(std::io::Error),
}
