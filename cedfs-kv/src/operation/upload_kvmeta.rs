use crate::Shared;

pub struct UploadKvMetaOp {
    pub server_id: u32,
    pub tokens: Vec<u32>,
    pub shared: Shared,
}

impl UploadKvMetaOp {
    pub async fn run(&self) -> anyhow::Result<()> {
        let hash_results = self
            .shared
            .hasher
            .hash_tokens_with_blocks_all(&self.tokens, self.shared.config.block_size);

        let token_blocks: Vec<([u8; 32], u32)> = hash_results
            .iter()
            .map(|(hash, offset)| (hash.to_u256(), *offset))
            .collect();
        let _ = self
            .shared
            .report_kvcache_by_blocks(self.server_id, token_blocks);

        tracing::debug!(
            "Upload KV metadata - server_id: {}, blocks: {}",
            self.server_id,
            hash_results.len()
        );

        Ok(())
    }
}
