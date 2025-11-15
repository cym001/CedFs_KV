use anyhow::Ok;

use crate::types::UpdateKvOp;
use crate::Shared;

pub struct UploadKvMetaOp {
    pub server_id: u32,
    pub tokens: Vec<i64>,
    pub shared: Shared,
}

impl UploadKvMetaOp {
    pub async fn run(&self) -> anyhow::Result<()> {
        let tokens_hash = self
            .shared
            .hasher
            .hash_tokens_with_blocks_all(&self.tokens, self.shared.config.block_size);
        
        Ok(())
    }
}
