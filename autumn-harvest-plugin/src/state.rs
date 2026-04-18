use std::ops::Deref;

use autumn_harvest::worker::DbPool;

/// Application/business database role exposed to Harvest activities.
///
/// In embedded mode this may point to the same underlying Postgres pool as
/// Harvest system storage. In split mode it intentionally stays bound to the
/// app database so activities can touch business tables explicitly.
#[derive(Clone)]
pub struct AppDbPool(DbPool);

impl AppDbPool {
    #[must_use]
    pub fn clone_inner(&self) -> DbPool {
        self.0.clone()
    }
}

impl From<DbPool> for AppDbPool {
    fn from(pool: DbPool) -> Self {
        Self(pool)
    }
}

impl Deref for AppDbPool {
    type Target = DbPool;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Debug for AppDbPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppDbPool")
            .field("max_size", &self.0.status().max_size)
            .finish()
    }
}

/// Harvest system-storage database role used for queue, history, and scheduler
/// tables.
#[derive(Clone)]
pub struct HarvestDbPool(DbPool);

impl HarvestDbPool {
    #[must_use]
    pub fn clone_inner(&self) -> DbPool {
        self.0.clone()
    }
}

impl From<DbPool> for HarvestDbPool {
    fn from(pool: DbPool) -> Self {
        Self(pool)
    }
}

impl Deref for HarvestDbPool {
    type Target = DbPool;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Debug for HarvestDbPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HarvestDbPool")
            .field("max_size", &self.0.status().max_size)
            .finish()
    }
}
