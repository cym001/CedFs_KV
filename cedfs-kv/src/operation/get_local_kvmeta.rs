use std::collections::HashMap;

use crate::Shared;
use crate::types::{KvBlockMeta};

pub struct GetLocalKvMetaOp {
    pub kv_meta: Vec<KvBlockMeta>,
    pub kv_ref: HashMap<u64,u64>,
    pub shared: Shared,
}

impl GetLocalKvMetaOp {
    pub fn run(self) -> anyhow::Result<()>{
        
        // 初始化本地kv元数据
        self.shared.clear_local_kvcache();
        self.shared.batch_insert_local_kvcache(self.kv_meta.clone());

        // 更新引用计数
        self.shared.ref_count.clear_local_ref_counts();
        for (k, v) in self.kv_ref.iter() {
            self.shared.ref_count.insert_or_update_local_ref_count(*k, *v);
        }

        tracing::info!("GetLocalKvMetaOp: Initialized {} local KV block metas and {} local block counts.",
            self.kv_meta.len(), self.kv_ref.len());

        Ok(())
    }
}