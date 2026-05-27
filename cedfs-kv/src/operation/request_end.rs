use std::sync::atomic::Ordering;

use crate::Shared;

pub struct RequestEndOp {
    pub request_id: String,
    pub shared: Shared,
}

impl RequestEndOp {
    pub async fn run(&self) -> anyhow::Result<()> {
        self.shared.active_squence.free(&self.request_id);

        if !self.shared.config.transfer_strategy {
            tracing::debug!(
                "RequestEndOp: request_id={} skip pressure migration because transfer_strategy=false",
                self.request_id
            );
            //tracing::info!("RequestEndOp: removed request {}", self.request_id);
            return Ok(());
        }

        let request_count = self
            .shared
            .pressure_migration_request_count
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        let migration_check_request_interval = self.shared.config.migration_check_request_interval;
        if !should_run_migration_check(request_count, migration_check_request_interval) {
            tracing::debug!(
                "RequestEndOp: request_id={} skip pressure migration because request_count={} interval={}",
                self.request_id,
                request_count,
                migration_check_request_interval
            );
            //tracing::info!("RequestEndOp: removed request {}", self.request_id);
            return Ok(());
        }

        let shared = self.shared.clone();
        let request_id = self.request_id.clone();
        tokio::spawn(async move {
            match shared.rebalance_by_pressure().await {
                Ok(migration_result) => {
                    tracing::info!(
                        "RequestEndOp: request_id={} pressure migration result: {:?}",
                        request_id,
                        migration_result
                    );
                },
                Err(e) => {
                    tracing::warn!(
                        "RequestEndOp: request_id={} pressure migration failed: {:?}",
                        request_id,
                        e
                    );
                },
            }
        });

        Ok(())
    }
}

fn should_run_migration_check(request_count: u64, interval: u64) -> bool {
    request_count % interval == 0
}

#[cfg(test)]
mod tests {
    use super::should_run_migration_check;

    #[test]
    fn migration_check_interval_one_runs_every_request() {
        assert!(should_run_migration_check(1, 1));
        assert!(should_run_migration_check(2, 1));
    }

    #[test]
    fn migration_check_interval_runs_on_every_nth_request() {
        assert!(!should_run_migration_check(1, 3));
        assert!(!should_run_migration_check(2, 3));
        assert!(should_run_migration_check(3, 3));
        assert!(!should_run_migration_check(4, 3));
    }
}
