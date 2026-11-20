## Phase X.Y — Shape census

`shape_census` counts history shapes over a corpus of runs. It builds on
`history_shape`.

- `ShapeCensus::observe` counts one history by its ordered event type names
  and returns its `shape_fingerprint`.
- `dominant` names the most common shape. `rare_shapes(max_share)` lists the
  shapes at or under a share bound, the rarest first.
- `judge` reports how often the shape of a new run occurs, and the first event
  index where it leaves the dominant path. It does not change the census.
- Keys are full name sequences, so a fingerprint collision does not merge two
  shapes. Every order is deterministic.
- The module is pure. It adds no `WorkflowEvent` variant, no SQL and no
  migration. Seven unit tests and one doctest cover it.
