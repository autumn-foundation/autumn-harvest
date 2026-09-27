## Fix — contract audit reads raw-byte bodies and typed queries (issue #1411)

Closes the two audit blind spots left open in #1411. The contract fixes
shipped in #1690 and #1699. This change makes those defect classes fail CI.

**Raw-byte bodies.** `docs/audits/openapi-response-coverage.py` now reads a
body that a handler parses itself with `serde_json::from_slice`, in the
handler or one helper down. The type comes from a turbofish, a typed `let`,
or a `Result<T, _>` return type. Checks 2 and 3 apply to that body.

**Check 5.** A body the handler cannot run without is marked required. A
bare `Json<T>` is mandatory. A raw-byte parse is mandatory unless an
`.is_empty()` test on the same variable comes first. This separates the
three DLQ routes from the three optional-body routes in #1411.

**Check 6.** Each `Query<T>` field has one query parameter with the same
name, OpenAPI type and required flag. A documented key the struct ignores
is a finding.

**Check 7.** The audit fails closed. A body with no readable type, or a
struct it cannot find, is a finding and not a skip.

**Tooling.** `--self-test` runs 18 fixtures through the checks. CI runs it
before the scan. The audit now indexes functions and structs once, so a run
takes about 2 s instead of about 2 min 45 s. It prints the same findings as
before.

No engine, schema or contract change. The real contract passes every check.
Six defects seeded into a copy of it were each caught.
