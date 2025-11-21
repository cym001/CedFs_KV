use anyhow::Ok;

use crate::Shared;

pub struct UploadKvMetaOp {
    pub server_id: u32,
    pub tokens: Vec<u32>,
    pub shared: Shared,
}

impl UploadKvMetaOp {
    pub async fn run(&self) -> anyhow::Result<()> {
        let tokens_hash: Vec<[u8; 32]> = self
            .shared
            .hasher
            .hash_tokens_with_blocks_all(&self.tokens, self.shared.config.block_size)
            .iter().map(|x|x.to_u256()).collect();

        self.shared.create_new_kvblock(self.server_id, tokens_hash.clone());
        self.shared.ref_count.batch_increment_local_incremental_count(&tokens_hash, 1);

        Ok(())
    }
}
