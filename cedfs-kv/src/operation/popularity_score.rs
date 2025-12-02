use crate::types::{DataServer};
use crate::Shared;

pub struct PopularityScoreOp {
    pub shared: Shared,
}

impl PopularityScoreOp {
    pub async fn run(&self) -> Vec<([u8; 32], u32, DataServer, DataServer)> {
        let token_hashs = Self::top_k_popularity(&self, self.shared.config.replica_pull_count as usize);
        self.get_instance_from_token_hash(token_hashs).await
    }

    /// 获取远程引用计数中频率最大的k个token_hash，且这些token_hash不在本地引用计数中
    pub fn top_k_popularity(&self, k: usize) -> Vec<[u8; 32]> {
        use std::collections::BinaryHeap;
        use std::cmp::Reverse;
        
        let local_ref_counts = &self.shared.ref_count.local_ref_counts;
        let global_ref_counts = &self.shared.ref_count.global_ref_counts;
        
        // 使用最小堆来维护top k
        let mut heap: BinaryHeap<Reverse<(u64, [u8; 32])>> = BinaryHeap::new();
        
        // 遍历全局引用计数
        for entry in global_ref_counts.iter() {
            let token_hash = *entry.key();
            let count = *entry.value();
            
            // 跳过在本地引用计数中出现的token_hash
            if local_ref_counts.contains_key(&token_hash) {
                continue;
            }
            
            // 维护大小为k的最小堆
            if heap.len() < k {
                heap.push(Reverse((count, token_hash)));
            } else if let Some(&Reverse((min_count, _))) = heap.peek() {
                if count > min_count {
                    heap.pop();
                    heap.push(Reverse((count, token_hash)));
                }
            }
        }
        
        // 从堆中提取结果并按频率降序排序
        let mut result: Vec<(u64, [u8; 32])> = heap.into_iter()
            .map(|Reverse(pair)| pair)
            .collect();
        result.sort_by(|a, b| b.0.cmp(&a.0));
        
        // 只返回token_hash
        result.into_iter().map(|(_, token_hash)| token_hash).collect()
    }
    
    /// 根据token_hash获取对应的源DataServer、offset和目标DataServer
    /// 返回: Vec<(token_hash, offset, source_server, target_server)>
    /// 其中target_server为不在KvBlockMeta的server_id中的data_server_collect中的一个dataserver
    pub async fn get_instance_from_token_hash(&self, ids: Vec<[u8; 32]>) -> Vec<([u8; 32], u32, DataServer, DataServer)> {
        
        let remote_kvcache_table = &self.shared.global_kvcache_table;
        let data_server_collect = self.shared.data_server_collect.read().await;
        
        let mut result = Vec::new();
        
        for token_hash in ids {
            // 从远程kv块元数据中获取block元数据
            if let Some(kv_meta) = remote_kvcache_table.get(&token_hash) {
                let offset = kv_meta.offset;
                
                // 选择一个源server（取第一个）
                if let Some(source_server_id) = kv_meta.server_id.first() {
                    // 在data_server_collect中查找源DataServer
                    let source_server = data_server_collect.iter()
                        .find(|ds| ds.id == *source_server_id);
                    
                    if let Some(source_server) = source_server {
                        // 查找不在kv_meta.server_id中的目标server
                        let target_server = data_server_collect.iter()
                            .find(|ds| !kv_meta.server_id.contains(&ds.id));
                        
                        if let Some(target_server) = target_server {
                            result.push((
                                token_hash,
                                offset,
                                source_server.clone(),
                                target_server.clone()
                            ));
                        }
                    }
                }
            }
        }
        
        result
    }
}