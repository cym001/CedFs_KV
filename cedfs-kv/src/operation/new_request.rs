pub struct NewRequestOp {
    pub request_id: String,
    pub server_id: u32,
    pub tokens: Vec<u32>,
    pub shared: crate::Shared,
}

impl NewRequestOp {
    pub async fn run(&self) -> anyhow::Result<()> {
        let block_infos = self
            .shared
            .hasher
            .hash_tokens_with_block_infos_all(&self.tokens, self.shared.config.block_size);
        let token_hashes: Vec<[u8; 32]> = block_infos.iter().map(|info| info.seq_hash).collect();

        for info in &block_infos {
            self.shared.kv_radix.increment_heat(info.seq_hash);
        }

        let _expired = self
            .shared
            .active_squence
            .add_request(self.request_id.clone(), Some(token_hashes));

        tracing::debug!(
            "NewRequestOp: recorded request_id={}, server_id={}, blocks={}",
            self.request_id,
            self.server_id,
            block_infos.len()
        );
        Ok(())
    }
}
