use anyhow::Ok;

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
        
        // 提取哈希值和偏移量
        let tokens_hash: Vec<[u8; 32]> = hash_results
            .iter()
            .map(|(hash, _offset)| hash.to_u256())
            .collect();
        
        // 提取所有偏移量
        let offsets: Vec<u32> = hash_results
            .iter()
            .map(|(_hash, offset)| *offset)
            .collect();

        self.shared.create_new_kvblock(self.server_id, offsets.clone(), tokens_hash.clone());
        self.shared.ref_count.batch_increment_local_incremental_count(&tokens_hash, 1);
        
        // 输出哈希值和偏移量信息
        tracing::info!(
            "Upload KV metadata - server_id: {}, blocks: {}, offsets: {:?}, hashes: {:?}",
            self.server_id,
            hash_results.len(),
            offsets,
            tokens_hash.iter().map(|h| {
                // 将32字节数组转换为256位无符号整数（大端序）
                let mut result = num_bigint::BigUint::from(0u32);
                for &byte in h.iter() {
                    result = result << 8;
                    result = result + num_bigint::BigUint::from(byte);
                }
                result.to_string()
            }).collect::<Vec<_>>()
        );
        
        Ok(())
    }
}
