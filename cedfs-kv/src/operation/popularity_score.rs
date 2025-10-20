use crate::types::{ServerSocket, DataServer};
use crate::Shared;

pub struct PopularityScoreOp {
    pub shared: Shared,
}

impl PopularityScoreOp {
    pub async fn run(&self) -> Vec<DataServer> {
        let block_ids = Self::top_k_popularity(&self, self.shared.config.replica_pull_count as usize);
        let data_servers = self.get_instance_from_block_id(block_ids).await;
        data_servers
    }

    /// 获取远程引用计数中频率最大的k个block_id，且这些block_id不在本地引用计数中
    pub fn top_k_popularity(&self, k: usize) -> Vec<u64> {
        use std::collections::BinaryHeap;
        use std::cmp::Reverse;
        
        let local_ref_counts = &self.shared.ref_count.local_ref_counts;
        let global_ref_counts = &self.shared.ref_count.global_ref_counts;
        
        // 使用最小堆来维护top k
        let mut heap: BinaryHeap<Reverse<(u64, u64)>> = BinaryHeap::new();
        
        // 遍历全局引用计数
        for entry in global_ref_counts.iter() {
            let block_id = *entry.key();
            let count = *entry.value();
            
            // 跳过在本地引用计数中出现的block_id
            if local_ref_counts.contains_key(&block_id) {
                continue;
            }
            
            // 维护大小为k的最小堆
            if heap.len() < k {
                heap.push(Reverse((count, block_id)));
            } else if let Some(&Reverse((min_count, _))) = heap.peek() {
                if count > min_count {
                    heap.pop();
                    heap.push(Reverse((count, block_id)));
                }
            }
        }
        
        // 从堆中提取结果并按频率降序排序
        let mut result: Vec<(u64, u64)> = heap.into_iter()
            .map(|Reverse(pair)| pair)
            .collect();
        result.sort_by(|a, b| b.0.cmp(&a.0));
        
        // 只返回block_id
        result.into_iter().map(|(_, block_id)| block_id).collect()
    }
    
    /// 根据block_id获取对应的DataServer，为每个block_id任取一个server_socket
    pub async fn get_instance_from_block_id(&self, ids: Vec<u64>) -> Vec<DataServer> {
        use std::collections::HashSet;
        
        let remote_kvcache_table = &self.shared.remote_kvcache_table;
        let data_server_collect = self.shared.data_server_collect.read().await;
        
        let mut result = Vec::new();
        let mut used_servers = HashSet::new();
        
        for block_id in ids {
            // 从远程kv块元数据中获取block元数据
            if let Some(kv_meta) = remote_kvcache_table.get(&block_id) {
                if let Some(server_socket) = kv_meta.server_socket.first() {
                    // 在data_server_collect中查找匹配的DataServer
                    for data_server in data_server_collect.iter() {
                        if data_server.ip == server_socket.ip 
                            && data_server.http_port == server_socket.http_port 
                            && data_server.rpc_port == server_socket.rpc_port {
                            // 使用HashSet去重，避免返回重复的DataServer
                            let server_key = (data_server.ip, data_server.http_port, data_server.rpc_port);
                            if !used_servers.contains(&server_key) {
                                result.push(data_server.clone());
                                used_servers.insert(server_key);
                            }
                            break;
                        }
                    }
                }
            }
        }
        
        result
    }
}