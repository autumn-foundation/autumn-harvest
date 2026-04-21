import re

with open('autumn-harvest/src/context.rs', 'r') as f:
    content = f.read()

target = """        if let Some(ref tx) = self.heartbeat_tx {
            tx.send(payload).await.map_err(|_| {
                HarvestError::Cancelled("activity cancelled: heartbeat channel closed".into())
            })?;
        }

        Ok(())"""

replacement = """        let Some(ref tx) = self.heartbeat_tx else {
            return Ok(());
        };
        tx.send(payload).await.map_err(|_| {
            HarvestError::Cancelled("activity cancelled: heartbeat channel closed".into())
        })?;

        Ok(())"""

content = content.replace(target, replacement)

with open('autumn-harvest/src/context.rs', 'w') as f:
    f.write(content)
