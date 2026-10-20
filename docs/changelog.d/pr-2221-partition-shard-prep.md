## Phase — Share the DR-fence prelude of `harvest partition` (issue #2221)

`harvest partition enable`, `maintain` and `disable` repeated one prelude:
connect, check write authority, take the DR fence (issue #1823). The new
`prepare_partition_shard` holds it once. Each command keeps its own mutation
and result handling. The report rows, the error text and the fence lifetime
do not change.

The slow path of `decode_event_moving_payloads` now decodes with plain serde.
It used to lift the payloads again through `event_from_wire`, which drops a
payload that `payload_slot` omits. A payload field that `payload_slot` omits
therefore keeps its data.

**No migration. No new `WorkflowEvent` variant. No public API change.**

**Checks.** `partition_shard_prep_tests` fails to compile without the helper,
then passes. It covers the unreachable shard and DSN redaction. The authority
and fence paths need Postgres and are not covered here.
