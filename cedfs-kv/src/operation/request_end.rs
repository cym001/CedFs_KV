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
            tracing::info!("RequestEndOp: removed request {}", self.request_id);
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
