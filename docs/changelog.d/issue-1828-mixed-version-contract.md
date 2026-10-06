## Operations — the N/N-1 rolling-deploy contract and its smoke job (issue #1828)

`docs/upgrading/README.md` states the contract. N-1 and N run together on
the schema of N. A release adds expand migrations only. A contract step
ships at least one minor release after the last user of the object. N must
not write an event, field or envelope that N-1 cannot read. The page also
lists the known limits for 0.6 and 0.7: payload codecs, escaped envelopes,
erasure from 0.6, rate-limit bucket GC, audit retention, API token scopes
and the features that 0.6 does not know.

The smoke job found the payload-codec limit. A 0.6 worker does not apply
the codec of `HarvestBuilder::payload_codec`. It fails a run when it reads
an envelope that a 0.7 worker wrote. With a codec, the page says to stop
the 0.6 workers before the 0.7 workers start.

The `mixed-version-smoke` CI job runs `scripts/run-mixed-version-smoke.sh`.
It builds `scripts/mixed-version-smoke` against this tree and against the
previous release tag. It applies the migrations of this tree. Then it runs
three scenarios on one database: roll forward, roll back and a mixed fleet.
In each roll, the first version stops while the runs wait on a durable
timer, and the other version finishes them. The check asserts which version
ran each step. In the mixed fleet, it asserts that each version ran a step.
The script then repeats the scenarios with a test payload codec in each
worker, as far as the previous release supports it. For 0.6, that is the
roll forward only. The check then requires an envelope in the
`WorkflowCompleted` event that decodes to the run output.

The script finds N-1 as the highest release tag of an earlier minor
version. The 0.7.0 guide no longer says that a `mutate` token admits the
admin routes. That stopped with issue #1803.

`docs/audits/mixed-version-contract.py` runs in `lint`. It fails when the
page loses a section or the job or script goes missing.

No engine change. No migration. No `WorkflowEvent` change.
`harvest_events` is not touched.
