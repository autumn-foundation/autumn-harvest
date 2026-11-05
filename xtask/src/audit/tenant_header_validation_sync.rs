//! `cargo xtask audit tenant-header-validation-sync`: the port of
//! `docs/audits/tenant-header-validation-sync.py`.
//!
//! It keeps the tenant-header-validation clone class in step (issue #2341).
//!
//! Two functions parse and validate the `x-harvest-tenant` header the same
//! way: `tenant_key` in `autumn-harvest-plugin/src/authz.rs`, and
//! `declared_tenant` in `autumn-harvest-plugin/src/tenant.rs`. Each refuses a
//! repeated header, trims, then refuses an empty or over-length value. The
//! two instances are real duplication. The second copied the first when
//! `tenant.rs` was added (see issue #2341). They fall short of the
//! rule-of-three merge bar, so they stay linked rather than merged.
//!
//! This command substitutes for the abstraction that the numerosity gate did
//! not clear. It extracts the body of each function by brace matching. It
//! normalizes the one difference that the two files may have. That is the
//! name of the function, and the spelling of the length constant. The file
//! `authz.rs` reads its local alias `MAX_TENANT_KEY_LEN`. The file
//! `tenant.rs` reads `autumn_harvest::tenant::MAX_TENANT_LEN` directly. Both
//! are the same value. The build fails as soon as anything else diverges.
//!
//! Usage: `cargo xtask audit tenant-header-validation-sync [--self-test]`.

use std::process::ExitCode;

use smop::prelude::*;

use super::libpq_keyword_sync::py_read_repo_file;
use super::vantage_flash_live_region::compile;

/// Arguments of `cargo xtask audit tenant-header-validation-sync`.
#[derive(Debug, Clone, Copy, Args)]
pub struct Args {
    /// Run the fixtures instead of the real files.
    #[arg(long)]
    pub self_test: bool,
}

/// Each copy: its path, its function name and its length-constant spelling.
const TARGETS: [(&str, &str, &str); 2] = [
    (
        "autumn-harvest-plugin/src/authz.rs",
        "tenant_key",
        "MAX_TENANT_KEY_LEN",
    ),
    (
        "autumn-harvest-plugin/src/tenant.rs",
        "declared_tenant",
        "autumn_harvest::tenant::MAX_TENANT_LEN",
    ),
];

/// Returns the full text of the named function, by brace matching.
///
/// The text runs from the signature line through the closing brace. Returns
/// `None` when the function is absent, or when its body opens on a line with
/// a different signature shape. An unclosed body runs to the end of
/// `source`, as in the Python original.
fn extract_fn_body<'s>(source: &'s str, fn_name: &str) -> Option<&'s str> {
    let signature = compile(&format!(
        r"(?m)^fn {}\(request: &Request\) -> Result<Option<String>, \(\)> \{{",
        regex::escape(fn_name)
    ));
    let found = signature.find(source)?;
    let mut end = found.end();
    let mut depth = 1;
    while depth > 0 && end < source.len() {
        let ch = source[end..].chars().next()?;
        if ch == '{' {
            depth += 1;
        } else if ch == '}' {
            depth -= 1;
        }
        end += ch.len_utf8();
    }
    Some(&source[found.start()..end])
}

/// Collapses the one allowed difference so the rest compares exactly.
fn normalize(body: &str, fn_name: &str, const_expr: &str) -> String {
    body.replacen(fn_name, "<FN>", 1)
        .replace(const_expr, "<MAX_LEN>")
}

/// Returns one message per finding over the real files.
fn findings() -> Result<Vec<String>> {
    let mut bodies = Vec::new();
    for (rel_path, fn_name, const_expr) in TARGETS {
        let source = py_read_repo_file(rel_path)?;
        let Some(body) = extract_fn_body(&source, fn_name) else {
            return Ok(vec![format!(
                "{rel_path}: could not find fn {fn_name}(request: &Request) -> ..."
            )]);
        };
        bodies.push((rel_path, normalize(body, fn_name, const_expr)));
    }
    let [(first_path, first_norm), (second_path, second_norm)] = &bodies[..] else {
        bail!("expected two copies, found {}", bodies.len());
    };
    if first_norm != second_norm {
        return Ok(vec![format!(
            "{first_path} and {second_path} have diverged beyond the function \
             name and the length-constant spelling; update both the same way \
             or merge them (issue #2341)."
        )]);
    }
    Ok(Vec::new())
}

/// Checks the real files and prints the findings.
fn check() -> Result<ExitCode> {
    let found = findings()?;
    for message in &found {
        println!("{message}");
    }
    println!("tenant-header-validation-sync: {} findings", found.len());
    Ok(crate::util::gate_exit(found.is_empty()))
}

/// Exercises each failure case on fixtures.
fn self_test() -> Result<()> {
    let in_sync_a = concat!(
        "fn tenant_key(request: &Request) -> Result<Option<String>, ()> {\n",
        "    let value = 1;\n",
        "    if value > MAX_TENANT_KEY_LEN {\n",
        "        return Err(());\n",
        "    }\n",
        "}\n",
    );
    let in_sync_b = concat!(
        "fn declared_tenant(request: &Request) -> Result<Option<String>, ()> {\n",
        "    let value = 1;\n",
        "    if value > autumn_harvest::tenant::MAX_TENANT_LEN {\n",
        "        return Err(());\n",
        "    }\n",
        "}\n",
    );
    let body_a = extract_fn_body(in_sync_a, "tenant_key").context("no tenant_key")?;
    let body_b = extract_fn_body(in_sync_b, "declared_tenant").context("no declared_tenant")?;
    ensure!(body_a.ends_with('}'), "{body_a:?}");
    ensure!(body_b.ends_with('}'), "{body_b:?}");
    let norm_a = normalize(body_a, "tenant_key", "MAX_TENANT_KEY_LEN");
    let norm_b = normalize(
        body_b,
        "declared_tenant",
        "autumn_harvest::tenant::MAX_TENANT_LEN",
    );
    ensure!(norm_a == norm_b, "{norm_a:?} != {norm_b:?}");

    let drifted_b = in_sync_b.replace(
        "if value > autumn_harvest::tenant::MAX_TENANT_LEN {",
        "if value >= autumn_harvest::tenant::MAX_TENANT_LEN {",
    );
    let body_b2 = extract_fn_body(&drifted_b, "declared_tenant").context("no drifted fn")?;
    let norm_b2 = normalize(
        body_b2,
        "declared_tenant",
        "autumn_harvest::tenant::MAX_TENANT_LEN",
    );
    ensure!(
        norm_a != norm_b2,
        "a drifted comparison operator must be caught"
    );

    ensure!(extract_fn_body(in_sync_a, "no_such_fn").is_none());
    Ok(())
}

/// Runs `cargo xtask audit tenant-header-validation-sync`.
pub fn run(args: Args) -> Result<ExitCode> {
    if !args.self_test {
        return check();
    }
    match self_test() {
        Ok(()) => {
            println!("tenant-header-validation-sync self-test: OK");
            Ok(ExitCode::SUCCESS)
        }
        Err(error) => {
            eprintln!("tenant-header-validation-sync self-test failed: {error:#}");
            Ok(ExitCode::FAILURE)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_self_test_passes() -> Result<()> {
        self_test()
    }

    #[test]
    fn an_unclosed_body_runs_to_the_end() {
        let src = "fn tenant_key(request: &Request) -> Result<Option<String>, ()> {\n    {\n";
        assert_eq!(extract_fn_body(src, "tenant_key"), Some(src));
    }
}
