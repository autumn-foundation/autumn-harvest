import re

with open("autumn-harvest-plugin/src/plugin.rs", "r") as f:
    content = f.read()

# Fix the compiler error
content = content.replace("wf.retry_policy,", "wf.retry_policy.clone(),")

with open("autumn-harvest-plugin/src/plugin.rs", "w") as f:
    f.write(content)
