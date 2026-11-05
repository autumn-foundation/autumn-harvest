## Recall sweep fingerprint reads the active overlay (issue #2556)

The `recall::sweep` fingerprint now hashes the op profile. With an overlay
store, that is the bare profile with the active overlay applied, through
`AgentHarness::overlaid_profile`. The model ids come from that profile too.
An activation therefore gives new agents, and a rerun on the same file calls
the model. A harness with no overlay store hashes the same bytes as before.
An overlay that does not apply fails its item, as it fails an op.

Do not activate an overlay while a sweep runs.

Tests are in `autumn-harvest-agent/tests/recall.rs`: an activation, no
activation, a rollback, a model-only overlay and an overlay that does not
apply.
