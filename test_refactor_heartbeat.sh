sed -i.bak -e '/async fn heartbeat_loop(/i \
async fn flush_heartbeat(\
    task_id: Uuid,\
    pool: &Pool<AsyncPgConnection>,\
    payload: Value,\
) {\
    let mut conn = match pool.get().await {\
        Ok(c) => c,\
        Err(e) => {\
            tracing::warn!(\
                task_id = %task_id,\
                error = %e,\
                "failed to acquire DB connection for heartbeat flush"\
            );\
            return;\
        }\
    };\
\
    if let Err(e) = crate::queue::record_heartbeat(&mut conn, task_id, payload).await {\
        tracing::warn!(\
            task_id = %task_id,\
            error = %e,\
            "failed to flush heartbeat to database"\
        );\
    }\
}\
' autumn-harvest/src/heartbeat.rs
