sed -i.bak -e '/async fn register_in_fleet(&self, pool: &DbPool) {/i \
    async fn try_register_worker(\
        &self,\
        pool: &DbPool,\
        shard_ids: &[i32],\
        max_concurrency: i32,\
        host: &str,\
        version: &str,\
    ) -> Result<(), anyhow::Error> {\
        let mut conn = pool.get().await?;\
        crate::workers::register_worker(\
            &mut conn,\
            &self.config.worker_id,\
            &self.config.queues,\
            shard_ids,\
            max_concurrency,\
            host,\
            Some(version),\
            &self.config.build_id,\
            self.config.deployment_name.as_deref(),\
        )\
        .await?;\
        Ok(())\
    }\
' autumn-harvest/src/worker.rs
