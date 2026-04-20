import re

with open('autumn-harvest/src/scheduler.rs', 'r') as f:
    content = f.read()

target = """        match result {
            Ok(_) => return TaskStatus::Succeeded,
            Err(error) => {
                if let Some(policy) = retry_policy.as_ref() {
                    if policy
                        .non_retryable_errors
                        .iter()
                        .any(|non_retryable| non_retryable == &error)
                    {
                        return TaskStatus::Failed;
                    }
                    if let Some(delay) = next_retry_delay(policy, attempt) {
                        attempt = attempt.saturating_add(1);
                        tokio::time::sleep(delay).await;
                        continue;
                    }
                }
                return TaskStatus::Failed;
            }
        }"""

replacement = """        let Err(error) = result else {
            return TaskStatus::Succeeded;
        };

        let Some(policy) = retry_policy.as_ref() else {
            return TaskStatus::Failed;
        };

        if policy
            .non_retryable_errors
            .iter()
            .any(|non_retryable| non_retryable == &error)
        {
            return TaskStatus::Failed;
        }

        let Some(delay) = next_retry_delay(policy, attempt) else {
            return TaskStatus::Failed;
        };

        attempt = attempt.saturating_add(1);
        tokio::time::sleep(delay).await;
        continue;"""

content = content.replace(target, replacement)

with open('autumn-harvest/src/scheduler.rs', 'w') as f:
    f.write(content)
