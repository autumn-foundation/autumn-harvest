## Phase — Continue-as-new provenance in `ctx.info()` (issue #2082)

`ctx.info()` now reports `start_source` and `continued_from`. They come from
the execution row columns `start_source` and `continued_from_exec_id`. The row
never changes after the insert, so the values are replay-stable. A run with no
recorded source reports `StartSource::Unknown`.

`Entity::run` uses them. It drops the input of a start that a caller chose and
starts from `S::default()`. A continue-as-new, reset or re-run keeps its
checkpoint, and so does a run with an unknown source. `Entity::initial_state`
seeds a fresh entity from code. `Entity::trust_start_input` restores the old
behavior.

**Design.** No new `WorkflowEvent` field and no migration. The test harness and
the replayer take `with_start_provenance`, so `replay_check` matches the live
run. [ADR 0006](../adr/0006-keyed-entity.md) and
[`security-posture.md`](../security-posture.md) record the change.

**Tests.** Unit tests cover the source classifier, the row mapping, the
`ctx.info()` fields and the entity gate, including `replay_check`.
