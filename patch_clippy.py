import re

with open('autumn-harvest/src/worker.rs', 'r') as f:
    content = f.read()

target = """        let Some(val) = extractor(cmd) else {
            return None;
        };"""

replacement = """        let val = extractor(cmd)?;"""

content = content.replace(target, replacement)

with open('autumn-harvest/src/worker.rs', 'w') as f:
    f.write(content)

with open('autumn-harvest/src/scheduler.rs', 'r') as f:
    content = f.read()

target = """        attempt = attempt.saturating_add(1);
        tokio::time::sleep(delay).await;
        continue;
    }
}"""

replacement = """        attempt = attempt.saturating_add(1);
        tokio::time::sleep(delay).await;
    }
}"""

content = content.replace(target, replacement)

with open('autumn-harvest/src/scheduler.rs', 'w') as f:
    f.write(content)
