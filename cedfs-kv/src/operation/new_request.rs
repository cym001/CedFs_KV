use std::time::Duration;

use crate::types::DataServer;
// use crate::{PendingMigrationTask, Shared};
use crate::{Shared};

const MIGRATION_TRIGGER_THRESHOLD: u64 = 2;
const MIGRATION_COOLDOWN_SECS: u64 = 60;

pub struct NewRequestOp {
    pub request_id: String,
    pub server_id: u32,
    pub tokens: Vec<u32>,
    pub shared: Shared,
}

impl NewRequestOp {
    async fn resolve_source_server(&self) -> Option<DataServer> {
        // if let Some(meta_server_id) = self.shared.data_server_to_meta_server.get(&self.server_id) {
        //     let meta_id = *meta_server_id;
        //     if let Some(data_servers) = self.shared.global_data_server_collect.get(&meta_id) {
        //         if let Some(server) = data_servers.iter().find(|ds| ds.id == self.server_id) {
        //             return Some(server.clone());
        //         }
        //     }
        // }

        let local_servers = self.shared.local_data_server_collect.read().await;
        local_servers
            .iter()
            .find(|ds| ds.id == self.server_id)
            .cloned()
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        // let expired_count = self.shared.cleanup_expired_pending_migrations();
        // if expired_count > 0 {
        //     tracing::debug!(
        //         "NewRequestOp: cleaned {} expired pending migration task(s) before enqueue",
        //         expired_count
        //     );
        // }

        // let hash_results = self
        //     .shared
        //     .hasher
        //     .hash_tokens_with_blocks_all(&self.tokens, self.shared.config.block_size);
        // let token_hashes: Vec<[u8; 32]> = hash_results
        //     .iter()
        //     .map(|(hash, _offset)| hash.to_u256())
        //     .collect();

        // // tracing::info!(
        // //     "New Request - server_id: {}, blocks: {}, hashes: {:?}",
        // //     self.server_id,
        // //     token_hashes.len(),
        // //     token_hashes.iter().map(|h| h.iter().map(|b| format!("{:02x}", b)).collect::<String>()).collect::<Vec<_>>()
        // // );

        // let _expired = self
        //     .shared
        //     .active_squence
        //     .add_request(self.request_id.clone(), Some(token_hashes.clone()));

        // let hold_counts = self.shared.active_squence.sequence_hold_counts(&token_hashes);
        // let high_hold_blocks: Vec<([u8; 32], u64)> = token_hashes
        //     .iter()
        //     .zip(hold_counts.iter())
        //     .filter_map(|(token_hash, hold)| {
        //         if *hold >= MIGRATION_TRIGGER_THRESHOLD {
        //             Some((*token_hash, *hold))
        //         } else {
        //             None
        //         }
        //     })
        //     .collect();

        // if high_hold_blocks.is_empty() {
        //     tracing::debug!(
        //         "NewRequestOp: request_id={}, high-hold blocks (hold<{}) = {:?}",
        //         self.request_id,
        //         MIGRATION_TRIGGER_THRESHOLD,
        //         high_hold_blocks
        //     );
        //     return Ok(())
        // }

        // let cooldown = Duration::from_secs(MIGRATION_COOLDOWN_SECS);
        // let eligible_blocks: Vec<([u8; 32], u64)> = high_hold_blocks
        //     .into_iter()
        //     .filter(|(token_hash, _)| {
        //         if let Some(last) = self.shared.recent_migrations.get(token_hash) {
        //             last.elapsed() >= cooldown
        //         } else {
        //             true
        //         }
        //     })
        //     .collect();

        // if eligible_blocks.is_empty() {
        //     tracing::info!(
        //         "NewRequestOp: request_id={} has no migration-eligible blocks after cooldown",
        //         self.request_id
        //     );
        //     return Ok(());
        // }

        // let Some(source_server) = self.resolve_source_server().await else {
        //     tracing::warn!(
        //         "NewRequestOp: source server {} not found, skip migration for request {}",
        //         self.server_id,
        //         self.request_id
        //     );
        //     return Ok(());
        // };

        // // let pending_task = PendingMigrationTask::new(source_server.id, eligible_blocks.clone());
        // // self.shared
        // //     .upsert_pending_migration_task(self.request_id.clone(), pending_task);
        // // tracing::info!(
        // //     "NewRequestOp: request_id={} queued pending migration: source_server_id={}, blocks={}",
        // //     self.request_id,
        // //     source_server.id,
        // //     eligible_blocks.len()
        // // );

        // tracing::debug!("NewRequestOp: added request {}", self.request_id);
        Ok(())
    }
}