use anyhow::Ok;

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
            // 从 KV 元数据树中移除当前服务器副本，并扣减该副本分摊的热度。
            let Some(report) = self
                .shared
                .kv_radix
                .apply_eviction(self.server_id, *token_hash)
            else {
                continue;
            };
            if report.removed && report.replica_count_before <= 1 {
                tracing::debug!(
                    "RemoveKvMetaOp: Completely removed token_hash {:?} from kv_radix",
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
