use crate::types::{DataServer};
use crate::Shared;

pub struct PopularityScoreOp {
    pub shared: Shared,
}

impl PopularityScoreOp {
    pub async fn run(&self) -> Vec<([u8; 32], u32, DataServer, DataServer)> {
        let token_hashs = self.top_k_popularity(self.shared.config.replica_pull_count as usize).await;
        self.get_instance_from_token_hash(token_hashs).await
    }

    /// 获取远程引用计数中频率最大的k个token_hash，且这些token_hash至少不在一个本地dataserver中
    pub async fn top_k_popularity(&self, k: usize) -> Vec<[u8; 32]> {
        use std::collections::BinaryHeap;
        use std::cmp::Reverse;
        
        let global_ref_counts = &self.shared.ref_count.global_ref_counts;
        let global_kvcache_table = &self.shared.global_kvcache_table;
        let local_data_servers = self.shared.local_data_server_collect.read().await;
        
        // 获取所有本地dataserver的id集合
        let local_server_ids: std::collections::HashSet<u32> = local_data_servers
            .iter()
            .map(|ds| ds.id)
            .collect();
        
        // 使用最小堆来维护top k
        let mut heap: BinaryHeap<Reverse<(u64, [u8; 32])>> = BinaryHeap::new();
        
        // 遍历全局引用计数
        for entry in global_ref_counts.iter() {
            let token_hash = *entry.key();
            let count = *entry.value();
            
            // 检查该token_hash是否至少不在一个本地dataserver中
            // 即：该token_hash对应的server_id不包含所有本地dataserver
            let should_include = if let Some(kv_meta) = global_kvcache_table.get(&token_hash) {
                // 检查是否至少有一个本地dataserver不在kv_meta.server_id中
                local_server_ids.iter().any(|local_id| !kv_meta.server_id.contains(local_id))
            } else {
                // 如果在global_kvcache_table中找不到，说明不在任何本地dataserver中
                true
            };
            
            if !should_include {
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
    /// 其中target_server必须是local_data_server_collect中不在KvBlockMeta的server_id中的dataserver
    /// source_server可以从global_data_server_collect中的任意server选择
    pub async fn get_instance_from_token_hash(&self, ids: Vec<[u8; 32]>) -> Vec<([u8; 32], u32, DataServer, DataServer)> {
        
        let remote_kvcache_table = &self.shared.global_kvcache_table;
        let local_data_servers = self.shared.local_data_server_collect.read().await;
        
        let mut result = Vec::new();
        
        for token_hash in ids {
            // 从远程kv块元数据中获取block元数据
            if let Some(kv_meta) = remote_kvcache_table.get(&token_hash) {
                let offset = kv_meta.offset;
                
                // 首先从local_data_server中查找不在kv_meta.server_id中的目标server
                let target_server = local_data_servers.iter()
                    .find(|ds| !kv_meta.server_id.contains(&ds.id));
                
                if let Some(target_server) = target_server {
                    // 选择一个源server（取第一个）
                    if let Some(source_server_id) = kv_meta.server_id.first() {
                        // 从global_data_server_collect中查找源DataServer
                        let mut source_server_found: Option<DataServer> = None;
                        
                        // 通过映射找到源server所属的meta_server
                        if let Some(meta_server_id) = self.shared.data_server_to_meta_server.get(source_server_id) {
                            let meta_id = *meta_server_id;
                            
                            // 从global_data_server_collect中查找
                            if let Some(data_servers) = self.shared.global_data_server_collect.get(&meta_id) {
                                source_server_found = data_servers.iter()
                                    .find(|ds| ds.id == *source_server_id)
                                    .cloned();
                            }
                        }
                        
                        if let Some(source_server) = source_server_found {
                            result.push((
                                token_hash,
                                offset,
                                source_server,
                                target_server.clone()
                            ));
                        } else {
                            tracing::warn!(
                                "Source server {} not found in global_data_server_collect for token_hash {:?}",
                                source_server_id,
                                token_hash
                            );
                        }
                    }
                } else {
                    tracing::debug!(
                        "No available target server in local_data_server_collect for token_hash {:?}",
                        token_hash
                    );
                }
            }
        }
        
        result
    }
}