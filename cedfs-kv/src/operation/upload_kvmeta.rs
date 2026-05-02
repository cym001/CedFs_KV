use anyhow::Ok;

use crate::Shared;

pub struct UploadKvMetaOp {
    pub server_id: u32,
    pub tokens: Vec<u32>,
    pub shared: Shared,
}

impl UploadKvMetaOp {
    pub async fn run(&self) -> anyhow::Result<()> {
        // 在日志中输出 tokens
        // tracing::info!("Uploading KVMeta, server_id: {}, tokens: {:?}", self.server_id, self.tokens);

        let block_infos = self
            .shared
            .hasher
            .hash_tokens_with_block_infos_all(&self.tokens, self.shared.config.block_size);

        let _ = self
            .shared
            .create_new_kvblock(self.server_id, block_infos.clone());

        // 输出哈希值和偏移量信息
        // tracing::info!(
        //     "Upload KV metadata - server_id: {}, blocks: {}, offsets: {:?}, hashes: {:?}",
        //     self.server_id,
        //     hash_results.len(),
        //     offsets,
        //     tokens_hash.iter().map(|h| h.iter().map(|b| format!("{:02x}", b)).collect::<String>()).collect::<Vec<_>>()
        // );
        tracing::debug!(
            "Upload KV metadata - server_id: {}, blocks: {}",
            self.server_id,
            block_infos.len()
        );

        Ok(())
    }
}
