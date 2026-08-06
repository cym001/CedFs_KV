use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
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
    pub candidate_count: usize,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct MetricsSnapshot {
    pub replication_rpc_total: u64,
    pub replication_rpc_blocks_total: u64,
    pub replication_rpc_duration_ms_total: u64,
    pub migration_selection_total: u64,
    pub migration_selection_candidates_total: u64,
    pub migration_selection_duration_ms_total: u64,
    pub v2_transfer_blocks_total: u64,
    pub v2_transfer_bytes_total: u64,
    pub v2_transfer_failed_blocks_total: u64,
    pub v2_transfer_blocks_by_status: BTreeMap<&'static str, u64>,
    pub v2_rebalance_success_total: u64,
    pub v2_rebalance_failure_total: u64,
}

#[derive(Debug, Default)]
pub struct MetricsCollector {
    replication_rpc_total: AtomicU64,
    replication_rpc_blocks_total: AtomicU64,
    replication_rpc_duration_ms_total: AtomicU64,
    migration_selection_total: AtomicU64,
    migration_selection_candidates_total: AtomicU64,
    migration_selection_duration_ms_total: AtomicU64,
    v2_transfer_blocks_total: AtomicU64,
    v2_transfer_bytes_total: AtomicU64,
    v2_transfer_failed_blocks_total: AtomicU64,
    v2_transfer_blocks_by_status: [AtomicU64; 10],
    v2_rebalance_success_total: AtomicU64,
    v2_rebalance_failure_total: AtomicU64,
}

impl MetricsCollector {
    pub fn record_replication_rpc(&self, record: ReplicationRpcRecord) {
        let duration_ms = record
            .end
            .signed_duration_since(record.start)
            .num_milliseconds()
            .max(0) as u64;
        self.replication_rpc_total.fetch_add(1, Ordering::Relaxed);
        self.replication_rpc_blocks_total
            .fetch_add(record.block_count as u64, Ordering::Relaxed);
        self.replication_rpc_duration_ms_total
            .fetch_add(duration_ms, Ordering::Relaxed);
    }

    pub fn record_migration_selection(&self, record: MigrationSelectionRecord) {
        self.migration_selection_total
            .fetch_add(1, Ordering::Relaxed);
        self.migration_selection_candidates_total
            .fetch_add(record.candidate_count as u64, Ordering::Relaxed);
        self.migration_selection_duration_ms_total.fetch_add(
            record.duration.as_millis().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
    }

    pub fn record_v2_transfer_result(&self, status: usize, bytes: u64) {
        let status = status.min(9);
        self.v2_transfer_blocks_total
            .fetch_add(1, Ordering::Relaxed);
        self.v2_transfer_bytes_total
            .fetch_add(bytes, Ordering::Relaxed);
        self.v2_transfer_blocks_by_status[status].fetch_add(1, Ordering::Relaxed);
        if !matches!(status, 1 | 2) {
            self.v2_transfer_failed_blocks_total
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_v2_rebalance(&self, success: bool) {
        let counter = if success {
            &self.v2_rebalance_success_total
        } else {
            &self.v2_rebalance_failure_total
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            replication_rpc_total: self.replication_rpc_total.load(Ordering::Relaxed),
            replication_rpc_blocks_total: self
                .replication_rpc_blocks_total
                .load(Ordering::Relaxed),
            replication_rpc_duration_ms_total: self
                .replication_rpc_duration_ms_total
                .load(Ordering::Relaxed),
            migration_selection_total: self
                .migration_selection_total
                .load(Ordering::Relaxed),
            migration_selection_candidates_total: self
                .migration_selection_candidates_total
                .load(Ordering::Relaxed),
            migration_selection_duration_ms_total: self
                .migration_selection_duration_ms_total
                .load(Ordering::Relaxed),
            v2_transfer_blocks_total: self
                .v2_transfer_blocks_total
                .load(Ordering::Relaxed),
            v2_transfer_bytes_total: self.v2_transfer_bytes_total.load(Ordering::Relaxed),
            v2_transfer_failed_blocks_total: self
                .v2_transfer_failed_blocks_total
                .load(Ordering::Relaxed),
            v2_transfer_blocks_by_status: [
                "unspecified",
                "copied",
                "already_present",
                "source_missing",
                "target_no_capacity",
                "read_failed",
                "not_attempted",
                "incompatible",
                "stale_target_epoch",
                "protocol_error",
            ]
            .into_iter()
            .enumerate()
            .map(|(index, name)| {
                (
                    name,
                    self.v2_transfer_blocks_by_status[index].load(Ordering::Relaxed),
                )
            })
            .collect(),
            v2_rebalance_success_total: self
                .v2_rebalance_success_total
                .load(Ordering::Relaxed),
            v2_rebalance_failure_total: self
                .v2_rebalance_failure_total
                .load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collector_aggregates_without_retaining_event_records() {
        let collector = MetricsCollector::default();
        let start = Utc::now();
        let end = start.clone() + chrono::Duration::milliseconds(5);
        collector.record_replication_rpc(ReplicationRpcRecord {
            start,
            end,
            block_count: 3,
        });
        collector.record_v2_transfer_result(1, 64);
        collector.record_v2_transfer_result(5, 64);
        collector.record_v2_rebalance(false);

        let snapshot = collector.snapshot();
        assert_eq!(snapshot.replication_rpc_total, 1);
        assert_eq!(snapshot.replication_rpc_blocks_total, 3);
        assert_eq!(snapshot.replication_rpc_duration_ms_total, 5);
        assert_eq!(snapshot.v2_transfer_blocks_total, 2);
        assert_eq!(snapshot.v2_transfer_bytes_total, 128);
        assert_eq!(snapshot.v2_transfer_failed_blocks_total, 1);
        assert_eq!(snapshot.v2_transfer_blocks_by_status["copied"], 1);
        assert_eq!(snapshot.v2_transfer_blocks_by_status["read_failed"], 1);
        assert_eq!(snapshot.v2_rebalance_failure_total, 1);
    }
}
