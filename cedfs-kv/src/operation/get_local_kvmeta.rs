use cedfs_proto::kvcache::UploadKvBlockMeta;

use crate::Shared;

pub struct GetLocalKvMetaOp {
    pub kv_meta: Vec<UploadKvBlockMeta>,
    pub shared: Shared,
}

impl GetLocalKvMetaOp {
    pub fn run(self) -> anyhow::Result<()>{
        
        // 初始化本地kv元数据
        self.shared.clear_local_kvcache();
        for block in self.kv_meta.iter() {
            let block_id = self.shared.find_or_create_kv_block(block.model_hash, block.token_hash, block.tokens.clone());
            self.shared.ref_count.increment_local_incremental_count(block_id, block.kv_ref as u64);
        }

        tracing::info!("GetLocalKvMetaOp: Initialized {} local KV block metas",
            self.kv_meta.len());

        Ok(())
    }
}