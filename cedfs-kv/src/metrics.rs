use chrono::{DateTime, Utc};
use std::sync::Mutex;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct ReplicationRpcRecord {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub block_count: usize,
}

#[derive(Debug, Clone)]
pub struct MigrationSelectionRecord {
    pub duration: Duration,
    pub src_server_id: u32,
    pub dst_server_id: u32,
    pub candidate_count: usize,
}

#[derive(Debug, Default)]
pub struct MetricsCollector {
    replication_rpcs: Mutex<Vec<ReplicationRpcRecord>>,
    migration_selections: Mutex<Vec<MigrationSelectionRecord>>,
}

impl MetricsCollector {
    pub fn record_replication_rpc(&self, record: ReplicationRpcRecord) {
        self.replication_rpcs
            .lock()
            .expect("metrics replication_rpcs poisoned")
            .push(record);
    }

    pub fn record_migration_selection(&self, record: MigrationSelectionRecord) {
        self.migration_selections
            .lock()
            .expect("metrics migration_selections poisoned")
            .push(record);
    }

    pub fn drain_replication_rpcs(&self) -> Vec<ReplicationRpcRecord> {
        std::mem::take(
            &mut *self
                .replication_rpcs
                .lock()
                .expect("metrics replication_rpcs poisoned"),
        )
    }

    pub fn drain_migration_selections(&self) -> Vec<MigrationSelectionRecord> {
        std::mem::take(
            &mut *self
                .migration_selections
                .lock()
                .expect("metrics migration_selections poisoned"),
        )
    }
}
