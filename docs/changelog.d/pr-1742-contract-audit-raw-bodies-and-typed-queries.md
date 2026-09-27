## Tooling — contract audit reads raw-byte bodies and typed queries (issue #1411)

This change closes the two audit blind spots left open in #1411. The
contract fixes shipped in #1690 and #1699. Now those defect classes fail CI.

**Raw-byte bodies.** `docs/audits/openapi-response-coverage.py` now reads a
body that a handler parses itself with `serde_json::from_slice`. The parse
can be in the handler, or in a helper that the handler passes the body to.
The type comes from a turbofish, a typed `let`, or a `Result<T, _>` return
type. Checks 2 and 3 apply to that body.

**Check 5.** The contract must mark a body required when the handler cannot
run without it. A bare `Json<T>` is mandatory. A raw-byte parse is mandatory
unless an `.is_empty()` test lets an empty body skip it. This separates the
three DLQ routes from the three optional-body routes in #1411.

**Check 6.** Each `Query<T>` field has one query parameter with the same
name, OpenAPI type and required flag. A documented key that no struct
accepts is a finding.

**Check 7.** The audit does not skip what it cannot read. An unreadable
`from_slice` call or `Query<..>` extractor is a finding. So is an unresolved
type or a struct the audit cannot find.

**Tooling.** `--self-test` runs the fixtures through the checks, and CI runs
it before the scan. The fixtures also pin checks 1 and 4, which had none.
The audit now indexes functions and structs once. A run takes about 2 s,
not about 2 min 30 s. The speed change alone does not change any finding.

**Contract.** The stricter check 3 found one gap.
`POST /admin/schedules/{id}/resume` accepts `reason` through
`PauseResumeRequest`, but the contract listed no fields. The contract now
documents it as accepted and ignored, since resume clears `pause_reason`.

No engine or schema change. The real contract passes every check. Each of
six defects seeded into a copy of the contract fails the audit.
