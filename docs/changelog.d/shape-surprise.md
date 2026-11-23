## Phase X.Y — Shape surprise

`shape_surprise` scores each step of a run against a corpus of runs. It
builds on the shape census, which matches whole paths only.

- `TransitionModel::observe` counts the transitions between the event type
  names of one history. A `Start` and an `End` state frame each history.
- `probability` uses add-one smoothing, so an unseen step has a finite cost.
- `score` gives each step its surprise in bits, `-log2(p)`. It reports the
  total, the mean, the peak step and the number of novel steps.
- `predict_next` names the most likely next state and its probability.
- Every order is deterministic. A tie keeps the first state or step.
- The module is pure. It adds no `WorkflowEvent` variant, no SQL and no
  migration. Ten unit tests and one doctest cover it.
