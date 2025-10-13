use std::collections::HashMap;

use anyhow::Ok;

use crate::Shared;
use crate::types::{KvBlockMeta};

pub struct UpdateKvMetaOp {
    pub kv_meta: Vec<KvBlockMeta>,
    pub kv_ref: HashMap<u64, u64>,
    pub shared: Shared,
}

impl UpdateKvMetaOp {
    pub fn run(&self) -> anyhow::Result<()>{
        // 更新本地kv元数据
        for block in self.kv_meta.iter() {
            self.shared.insert_remote_kvcache(block.clone());
        }
        // 更新引用计数
        for (k, v) in self.kv_ref.iter() {
            self.shared.ref_count.increment_global_ref_count(*k,*v);
        }

        Ok(())
    }
}