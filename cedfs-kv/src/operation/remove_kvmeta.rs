use anyhow::Ok;

use crate::Shared;
use crate::types::UpdateKvOp;

pub struct RemoveKvMetaOp {
    pub remove_nums: i32,
    pub tokens_hash: Vec<Vec<u8>>,
    pub shared: Shared,
}

impl RemoveKvMetaOp {
    pub async fn run(&self) -> anyhow::Result<()> {
        // 将 Vec<u8> 分割为 [u8; 32] 的 token_hash 序列
        let token_hashes: Vec<[u8; 32]> = self.tokens_hash
            .iter()
            .filter_map(|v| {
                if v.len() != 32 {
                    tracing::error!("RemoveKvMetaOp: Invalid token hash length {}, expected 32", v.len());
                    None
                } else {
                    let mut arr = [0u8; 32];
                    arr.copy_from_slice(v);
                    Some(arr)
                }
            })
            .collect();

        // 验证解析的 token_hash 数量是否与 remove_nums 一致
        if token_hashes.len() != self.remove_nums as usize {
            tracing::warn!(
                "RemoveKvMetaOp: Expected {} token hashes but got {}",
                self.remove_nums,
                token_hashes.len()
            );
        }

        // 遍历每个 token_hash 进行删除操作
        for token_hash in token_hashes.iter() {
            // 1. 从 local_kv_index 中移除
            {
                let mut local_index = self.shared.local_kv_index.write().await;
                local_index.remove(token_hash);
            }

            // 2. 从 global_kvcache_table 中移除或更新
            if let Some(mut meta) = self.shared.global_kvcache_table.get_mut(token_hash) {
                // 获取当前服务器 ID (假设从配置中获取)
                let server_id = self.shared.config.local_meta_server.hash_id();
                
                // 从 server_id 列表中移除当前服务器
                meta.server_id.retain(|&id| id != server_id);
                
                // 如果没有服务器持有该块，则完全删除
                if meta.server_id.is_empty() {
                    drop(meta); // 释放可变引用
                    self.shared.remove_global_kvcache(*token_hash);
                    self.shared.ref_count.remove_global_ref_count(*token_hash);
                    tracing::debug!(
                        "RemoveKvMetaOp: Completely removed token_hash {:?} from global cache",
                        token_hash
                    );
                } else {
                    tracing::debug!(
                        "RemoveKvMetaOp: Updated token_hash {:?}, remaining servers: {:?}",
                        token_hash,
                        meta.server_id
                    );
                }
            }

            // 3. 添加删除操作到 update_kvop_table
            let update_op = UpdateKvOp {
                token_hash: *token_hash,
                operation: 2, // 删除副本操作
                server_id: self.shared.config.local_meta_server.hash_id(),
            };
            self.shared.insert_update_kvop(update_op);

            // 4. 从本地引用计数中移除
            self.shared.ref_count.remove_local_ref_count(*token_hash);
        }

        tracing::info!(
            "RemoveKvMetaOp: Successfully removed {} token hashes",
            token_hashes.len()
        );

        Ok(())
    }
}
