import sys

with open("./autumn-harvest/src/worker.rs", "r") as f:
    content = f.read()

search_str = """    store::append_events(conn, exec_id, &events, *next_event_id).await?;
    *next_event_id = next_event_id.saturating_add(i32::try_from(events.len()).unwrap_or(i32::MAX));
    Ok(())"""

replace_str = """    store::append_events(conn, exec_id, &events, *next_event_id).await?;
    *next_event_id = next_event_id.checked_add(i32::try_from(events.len()).unwrap_or(i32::MAX))
        .ok_or_else(|| crate::error::database_error("event ID overflow in worker update"))?;
    Ok(())"""

content = content.replace(search_str, replace_str)

with open("./autumn-harvest/src/worker.rs", "w") as f:
    f.write(content)
