## Feature — history shape fingerprints

`history_shape` reads a slice of `WorkflowEvent` and returns a type
histogram, a stable FNV-1a fingerprint of the ordered event type names, and
the index where two histories first differ. The module is pure. It adds no
event variant, no SQL and no migration. Payload changes do not move the
fingerprint.

**Tests.** Unit tests for the histogram, the fingerprint and the divergence
index, plus a doc test.
