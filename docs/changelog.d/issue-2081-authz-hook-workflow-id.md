## Security — Authorizer hook sees the body `workflow_id` (issue #2081)

`AuthzRequest` has two new fields: `workflow_id` and `start_input_present`.
Harvest reads them from the body of `start`, `signal-with-start` and
`update-with-start` before the hook runs. They are `None` and `false` on every
other route. The hook never gets the body.

- The id is the raw value the handler reads. A body the handler rejects names
  no key, so a hook that confines keys must deny `None`.
- No new `WorkflowEvent` variant. No migration. `harvest_events` is unchanged.
- Tests: `authz` unit tests, and `start_family_*` and
  `other_routes_carry_no_workflow_id` in `authz_integration`.
- `docs/security-posture.md` documents the fields. The keyed-entity section no
  longer advises denying signal-with-start.
