import re

with open("autumn-harvest/src/analyzer.rs", "r") as f:
    content = f.read()

# Replace the inner `if` with match guard as suggested by clippy:
old_code = """                | WorkflowEvent::LocalActivityFailed { .. }
                | WorkflowEvent::LocalActivityExhausted { .. } => {
                    if active_activities > 0 {
                        active_activities -= 1;
                        if active_activities == 0 {
                            sequential_count += 1;
                            max_sequential = max_sequential.max(sequential_count);
                        }
                    }
                }"""

new_code = """                | WorkflowEvent::LocalActivityFailed { .. }
                | WorkflowEvent::LocalActivityExhausted { .. }
                    if active_activities > 0 =>
                {
                    active_activities -= 1;
                    if active_activities == 0 {
                        sequential_count += 1;
                        max_sequential = max_sequential.max(sequential_count);
                    }
                }"""

content = content.replace(old_code, new_code)

with open("autumn-harvest/src/analyzer.rs", "w") as f:
    f.write(content)
