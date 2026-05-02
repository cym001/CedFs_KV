use anyhow::Ok;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::Shared;

pub struct RemoveKvMetaOp {
    pub server_id: u32,
    pub remove_nums: i32,
    pub tokens_hash: Vec<Vec<u8>>,
    pub shared: Shared,
}

impl RemoveKvMetaOp {
    pub async fn run(&self) -> anyhow::Result<()> {
        // 将 Vec<u8> 分割为 [u8; 32] 的 token_hash 序列
        let token_hashes: Vec<[u8; 32]> = self
            .tokens_hash
            .iter()
            .filter_map(|v| {
                if v.len() != 32 {
                    tracing::error!(
                        "RemoveKvMetaOp: Invalid token hash length {}, expected 32",
                        v.len()
                    );
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
                let removed = local_index.remove(token_hash);
                if removed {
                    let counter = self
                        .shared
                        .local_kv_cache_block_count
                        .entry(self.server_id)
                        .or_insert_with(|| AtomicUsize::new(0));
                    let _ = counter
                        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| v.checked_sub(1));
                }
            }

            // 2. 从 KV 元数据索引中移除当前服务器副本
            let before = self.shared.kv_meta_index.replica_count(*token_hash);
            let removed = self
                .shared
                .kv_meta_index
                .remove_server(*token_hash, self.server_id);
            if removed && before <= 1 {
                tracing::debug!(
                    "RemoveKvMetaOp: Completely removed token_hash {:?} from kv_meta_index",
                    token_hash
                );
            }
        }

        tracing::info!(
            "RemoveKvMetaOp: Successfully removed {} token hashes",
            token_hashes.len()
        );

        Ok(())
    }
}
