use crate::types::DataServer;
use crate::Shared;

pub struct RequestEndOp {
    pub request_id: String,
    pub shared: Shared,
}

impl RequestEndOp {
    async fn resolve_source_server(&self, source_server_id: u32) -> Option<DataServer> {
        let local_servers = self.shared.local_data_server_collect.read().await;
        local_servers
            .iter()
            .find(|ds| ds.id == source_server_id)
            .cloned()
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        let expired_count = self.shared.cleanup_expired_pending_migrations();
        if expired_count > 0 {
            tracing::debug!(
                "RequestEndOp: cleaned {} expired pending migration task(s) before execute",
                expired_count
            );
        }

        self.shared.active_squence.free(&self.request_id);

        let Some(pending_task) = self.shared.pop_pending_migration_task(&self.request_id) else {
            tracing::debug!(
                "RequestEndOp: request_id={} has no pending migration task",
                self.request_id
            );
            tracing::info!("RequestEndOp: removed request {}", self.request_id);
            return Ok(());
        };

        if pending_task.is_expired() {
            tracing::info!(
                "RequestEndOp: expired_pending_migration request_id={}, source_server_id={}, blocks={}",
                self.request_id,
                pending_task.source_server_id,
                pending_task.eligible_blocks.len()
            );
            tracing::info!("RequestEndOp: removed request {}", self.request_id);
            return Ok(());
        }

        let Some(source_server) = self.resolve_source_server(pending_task.source_server_id).await else {
            tracing::warn!(
                "RequestEndOp: source server {} not found, skip pending migration for request {}",
                pending_task.source_server_id,
                self.request_id
            );
            tracing::debug!("RequestEndOp: removed request {}", self.request_id);
            return Ok(());
        };

        if !self.shared.config.transfer_strategy {
            tracing::debug!(
                "RequestEndOp: request_id={} skip migration because transfer_strategy=false",
                self.request_id
            );
            return Ok(());
        }

        let shared = self.shared.clone();
        let request_id = self.request_id.clone();
        let eligible_blocks = pending_task.eligible_blocks;
        let token_ids = pending_task.token_ids;
        let source_server = source_server.clone();

        tokio::spawn(async move {
            match shared
                .migrate_hash_seq_with_rr_target(&source_server, &eligible_blocks, &token_ids)
                .await
            {
                Ok(migration_result) => {
                    tracing::info!(
                        "RequestEndOp: request_id={} migration result: {:?}",
                        request_id,
                        migration_result
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        "RequestEndOp: request_id={} async migration failed: {:?}",
                        request_id,
                        e
                    );
                }
            }
        });

        Ok(())
    }
}
