# Design — Issue #1986: fan-out failure tolerance and result writer

Issue #1986 asks for two options on the activity fan-out helpers:

1. **Failure tolerance.** A fan-out completes when at most N items fail. It
   fails at N+1. N is a count or a percentage of the items.
2. **Result writer.** The worker writes each item result through the
   `PayloadStore`. History keeps a small reference. The fan-out returns a
   manifest of references.

**No migration. No new `WorkflowEvent` variant. No route change.**

---

## 0. Planning record

### 0.1 Brainstorm — failure tolerance

| # | Idea | Verdict |
|---|------|---------|
| T1 | Run the collect-all helper, then count the failures. | Rejected. It does not stop. A windowed fan-out dispatches every wave after the limit is exceeded. |
| T2 | Count failures as slots resolve. The slot that exceeds the limit returns an error, so the join stops. | **Adopted.** See §1.2. |
| T3 | Record the tolerance in a marker. | Rejected. The decision is a function of recorded outcomes and code. A marker adds an event and no safety. |
| T4 | Take the percentage as `f64`. | Rejected. Float rounding is hard to explain. An integer percent with a floor is exact. |
| T5 | Count every error, engine errors included. | Rejected. `Cancelled` and `NonDeterministic` are not item failures. They abort, as today. |

### 0.2 Brainstorm — result writer

| # | Idea | Verdict |
|---|------|---------|
| W1 | Write from workflow code. | Rejected. I/O in workflow code breaks replay. |
| W2 | Force the offloader threshold to 0. | Rejected. Replay inflates every envelope back. Memory and replay time still scale with width times result size. |
| W3 | Let each activity write its own result. | Rejected. Each author repeats the work, and the blob gets no GC reference. |
| W4 | The worker writes the result when the task row carries a reserved header. History records a fixed-size reference. | **Adopted.** See §1.3. |
| W5 | A map run of child workflows with an item reader, as Step Functions does. | Deferred. Items still enter history as inputs. Without an item reader this gives no gain. |

### 0.3 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | Count off by one, so a tolerance of N fails at N. | Boundary tests at N and at N+1. |
| R2 | Round the percentage up, so the fan-out tolerates more than asked. | Floor: `total * percent / 100`. A test uses 10% of 15 items. |
| R3 | Write plaintext blobs when a codec encrypts history. | The worker encodes with the codecs before the `put`. |
| R4 | Write a blob with no `harvest_payload_refs` row. Retention never deletes it. | The completion transaction inserts the row. |
| R5 | Repeat a result write after an upload. | The upload goes through the counting offloader. An upload stops the repeats (issue #1788). |
| R6 | A worker without a store drops the result. | A fresh dispatch with no store returns `Config`. A worker without a store writes the result inline. The manifest then holds a value, not a reference. |
| R7 | Make the outcome depend on the window or on completion order. | Slots resolve in input order on replay. A test replays a recorded history and gets the same outcome. |
| R8 | Re-check the store on replay. A removed store then fails a run that passed. | The check runs on a fresh dispatch only. |
| R9 | Read the reserved header as user data. | The header name has the `x-harvest-` prefix. The docs name it. |
| R10 | Stop the join at the first failure past the limit. Unpolled slots leave recorded events unconsumed, and issue #1791 blocks a run that catches the error. | The join polls every slot before it decides. A test catches the error and completes. |
| R11 | Put a failure count in the error. A later replay sees more results and a new count. | The error carries `tolerated` and `total` only. |

### 0.4 Six thinking hats

| Hat | Notes |
|-----|-------|
| White | Each fan-out item records `ActivityScheduled`, `ActivityStarted` and `ActivityCompleted`. The offloader already moves large fields out of line, but replay inflates them. The worker measures history bytes with `pg_column_size(event_data)`. |
| Red | "Up to N failures" is the Step Functions model. Users know it. |
| Black | Event count still grows with width: three events per item. Only a map run with an item reader removes that (W5). `ScheduleActivity` gets a new field, so a struct literal outside the crate breaks. The crate is 0.x. |
| Yellow | Result bytes leave history. The bytes per item are fixed, whatever the result size. Replay fetches no blobs. Tolerance needs no new event. |
| Green | W5 as a follow-up. A typed reader for a stored result. |
| Blue | Red phase: tests for both options fail. Green phase: the options pass. Refactor phase: docs, gates, review. |

---

## 1. Design

### 1.1 API

```rust
pub enum FailureTolerance { None, Count(usize), Percent(u8) }

pub struct FanOutOptions { /* max_in_flight, tolerance, result_writer */ }

impl WorkflowContext {
    pub async fn execute_activity_fan_out_raw_with(
        &self, activities: Vec<(String, Value, String)>, options: &FanOutOptions,
    ) -> HarvestResult<FanOutResults<Value>>;

    pub async fn execute_activity_fan_out_with<I, O>(
        &self, info: &ActivityInfo, inputs: Vec<I>, options: &FanOutOptions,
    ) -> HarvestResult<FanOutResults<O>>;
}

pub enum FanOutItem<T> { Value(T), Stored(StoredResult), Failed(String) }
```

`FanOutResults` is the manifest. It is serializable, so a workflow can pass it
to a later activity.

### 1.2 Failure tolerance

`FailureTolerance::max_failures(total)` gives the limit. `Percent(p)` gives
`total * min(p, 100) / 100`, rounded down.

Each slot classifies its outcome. `ActivityFailed` and `Timeout` are item
failures. Other errors abort the fan-out, as in the collect-all helpers.

The join polls every slot of a wave before it decides. It does not stop at
the first failure, as `try_join_all` does. A stop there leaves recorded
`ActivityScheduled` events unconsumed. If the workflow then catches the error
and completes, issue #1791 blocks the run as drift. After the poll, the join
counts the failures. Above the limit, it returns
`FanOutFailureThresholdExceeded`. A windowed fan-out does not dispatch a later
wave.

The decision does not depend on poll order. Failures only accumulate, so a
replay with more results makes the same decision. The error carries only
`tolerated` and `total`. A failure count would change when a replay sees more
results.

Activities in flight when the limit is exceeded keep running. This is the
same as the fail-fast helpers.

### 1.3 Result writer

1. On a fresh dispatch, the fan-out checks that a store is configured. If not,
   it returns `Config` before it records the `fan_out` marker.
2. Each `ScheduleActivity` command carries `result_writer: true`.
3. The enqueue plan adds the header `x-harvest-result-writer: 1` to the task
   row's `context_headers`. No migration is needed.
4. The worker sees the header when the handler returns `Ok`. It encodes the
   output with the codecs and writes the bytes through the counting offloader.
   The completion transaction inserts the `harvest_payload_refs` row.
5. `ActivityCompleted.output` holds a `StoredResult` reference: store id, key,
   length and SHA-256. Its size does not depend on the result.
6. The fan-out returns `FanOutItem::Stored` for a reference.
   `StoredResult::fetch` reads the blob, checks it and decodes it.

A worker without a store, or an older worker, writes the output inline. The
fan-out then returns `FanOutItem::Value`.

Retention deletes a blob when it purges the run. Read the results before then.

## 2. Tests

| Test | Phase |
|------|-------|
| `fanout_tolerance_tests` (pure): count at N and N+1, percent floor, windowed stop, replay, engine errors | Red, then green |
| `fanout_tolerance_tests` (pure): writer flag on commands, `Stored` items, no-store `Config` | Red, then green |
| `fanout_result_writer_db_tests` (Postgres): history bytes per item are fixed across widths and result sizes | Red, then green |
| `FailureTolerance` unit tests | Red, then green |
