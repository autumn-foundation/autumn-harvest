import sys

def get_lines(filename, search_str, context=5):
    with open(filename, 'r') as f:
        lines = f.readlines()
    for i, line in enumerate(lines):
        if search_str in line:
            start = max(0, i - context)
            end = min(len(lines), i + context + 1)
            print(f"=== {filename}:{i+1} ===")
            for j in range(start, end):
                prefix = "*" if j == i else " "
                print(f"{j+1:4} {prefix} {lines[j].rstrip()}")

files = [
    ("autumn-harvest/src/admission_gate.rs", "TooManyGates"),
    ("autumn-harvest/src/builder.rs", "HarvestError::QueryTimedOut"),
    ("autumn-harvest/src/concurrency.rs", "resolve_concurrency_key"),
    ("autumn-harvest/src/context.rs", "ActivityInfo"),
    ("autumn-harvest/src/context.rs", "WorkflowInfo"),
    ("autumn-harvest/src/context.rs", "QueryHandlerInfo"),
    ("autumn-harvest/src/context.rs", "UpdateHandlerInfo"),
    ("autumn-harvest/src/context.rs", "Ok(())"),
    ("autumn-harvest/src/dag.rs", "MarkerRecorded"),
    ("autumn-harvest/src/executor.rs", "WorkflowLogger"),
    ("autumn-harvest/src/info.rs", "with_schemas"),
    ("autumn-harvest/src/info.rs", "OverlapPolicy::Skip"),
    ("autumn-harvest/src/info.rs", "OverlapPolicy::BufferAll"),
    ("autumn-harvest/src/replay.rs", "HarvestError::ActivityFailed"),
    ("autumn-harvest/src/types.rs", "WorkflowContext::spawn_child_workflow_detached_raw"),
    ("autumn-harvest/src/schedule_decision.rs", "database_error"),
]

for filename, search_str in files:
    get_lines(filename, search_str)
