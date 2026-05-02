// use crate::types::DataServer;
// use crate::Shared;
// use std::collections::{HashMap, HashSet};

// /// 根块的前驱标识（全零表示没有前驱）
// const ROOT_PRE_TOKEN: [u8; 32] = [0u8; 32];

// pub struct PopularityScoreOp {
//     pub shared: Shared,
// }

// impl PopularityScoreOp {
//     pub async fn run(&self) -> Vec<([u8; 32], u32, DataServer, DataServer)> {
//         let token_hashs = self.top_k_popularity(self.shared.config.replica_pull_count as usize).await;
//         self.get_instance_from_token_hash(token_hashs).await
//     }

//     /// 计算块的链深度（从根块到当前块的距离）
//     /// 根块（pre_token 为全零的块）深度为 0
//     fn compute_chain_depth(
//         &self,
//         token_hash: &[u8; 32],
//         depth_cache: &mut HashMap<[u8; 32], u32>,
//     ) -> u32 {
//         // 检查缓存
//         if let Some(&depth) = depth_cache.get(token_hash) {
//             return depth;
//         }
        
//         let global_kvcache_table = &self.shared.global_kvcache_table;
        
//         // 获取块的元数据
//         let depth = if let Some(kv_meta) = global_kvcache_table.get(token_hash) {
//             if kv_meta.pre_token == ROOT_PRE_TOKEN {
//                 // 根块，深度为 0
//                 0
//             } else {
//                 // 递归计算前驱的深度，然后加 1
//                 self.compute_chain_depth(&kv_meta.pre_token, depth_cache) + 1
//             }
//         } else {
//             // 找不到元数据，视为根块
//             0
//         };
        
//         depth_cache.insert(*token_hash, depth);
//         depth
//     }

//     /// 获取远程引用计数中频率最大的k个token_hash，且这些token_hash至少不在一个本地dataserver中
//     /// 
//     /// 排序规则：
//     /// 1. 首先按热度（引用计数）降序排序
//     /// 2. 热度相同时，按链深度升序排序（优先选择链头部的块）
//     /// 
//     /// 设计说明：
//     /// 由于每个kv cache块在首次命中时拥有相同的热度，如果仅按热度排序，可能不会从链的首个块开始迁移。
//     /// 例如链 A→B→C→D 中，若只迁移了 B、C、D 而没有 A，查询时由于 A 不存在，整个链都无法命中。
//     /// 因此，热度相同时优先选择链深度小的块（越靠近根块），确保链前部的块优先被迁移，提高迁移后的命中率。
//     /// 
//     /// 链深度定义：从根块（pre_token 为全零的块）到当前块需要经过的边数。根块深度为 0。
//     pub async fn top_k_popularity(&self, k: usize) -> Vec<[u8; 32]> {
//         use std::collections::BinaryHeap;
//         use std::cmp::Reverse;
        
//         let global_ref_counts = &self.shared.ref_count.global_ref_counts;
//         let global_kvcache_table = &self.shared.global_kvcache_table;
//         let local_data_servers = self.shared.local_data_server_collect.read().await;
        
//         // 获取所有本地dataserver的id集合
//         let local_server_ids: HashSet<u32> = local_data_servers
//             .iter()
//             .map(|ds| ds.id)
//             .collect();
        
//         // 深度缓存，避免重复计算
//         let mut depth_cache: HashMap<[u8; 32], u32> = HashMap::new();
        
//         // 使用最小堆来维护top k
//         // 堆元素: (count, reverse_depth, token_hash)
//         // - count: 热度（引用计数），越大越优先
//         // - reverse_depth: 链深度的反转值（u32::MAX - depth），用于在热度相同时优先选择深度小的块
//         //   深度越小表示越靠近链的根部，优先迁移可确保命中率
//         let mut heap: BinaryHeap<Reverse<(u64, u32, [u8; 32])>> = BinaryHeap::new();
        
//         // 遍历全局引用计数
//         for entry in global_ref_counts.iter() {
//             let token_hash = *entry.key();
//             let count = *entry.value();
            
//             // 只迁移热度大于2的kv块
//             if count <= 3 {
//                 continue;
//             }
            
//             // 检查该token_hash是否至少不在一个本地dataserver中
//             // 即：该token_hash对应的server_id不包含所有本地dataserver
//             let should_include = if let Some(kv_meta) = global_kvcache_table.get(&token_hash) {
//                 // 检查是否至少有一个本地dataserver不在kv_meta.server_id中
//                 local_server_ids.iter().any(|local_id| !kv_meta.server_id.contains(local_id))
//             } else {
//                 // 如果在global_kvcache_table中找不到，说明不在任何本地dataserver中
//                 true
//             };
            
//             if !should_include {
//                 continue;
//             }
            
//             // 计算链深度
//             let depth = self.compute_chain_depth(&token_hash, &mut depth_cache);
            
//             // 计算reverse_depth：深度越小，reverse_depth越大，优先级越高
//             let reverse_depth = u32::MAX - depth;
            
//             // 维护大小为k的最小堆
//             // 比较顺序: 先比count，再比reverse_depth
//             if heap.len() < k {
//                 heap.push(Reverse((count, reverse_depth, token_hash)));
//             } else if let Some(&Reverse((min_count, min_reverse_depth, _))) = heap.peek() {
//                 // 新元素优先级更高的条件：
//                 // 1. count更大，或
//                 // 2. count相同但reverse_depth更大（即depth更小）
//                 if count > min_count || (count == min_count && reverse_depth > min_reverse_depth) {
//                     heap.pop();
//                     heap.push(Reverse((count, reverse_depth, token_hash)));
//                 }
//             }
//         }
        
//         // 从堆中提取结果并按优先级降序排序
//         // 排序规则：先按count降序，再按depth升序（即reverse_depth降序）
//         let mut result: Vec<(u64, u32, [u8; 32])> = heap.into_iter()
//             .map(|Reverse(tuple)| tuple)
//             .collect();
//         result.sort_by(|a, b| {
//             match b.0.cmp(&a.0) {
//                 std::cmp::Ordering::Equal => b.1.cmp(&a.1), // reverse_depth降序 = depth升序
//                 other => other,
//             }
//         });
        
//         // 只返回token_hash
//         result.into_iter().map(|(_, _, token_hash)| token_hash).collect()
//     }
    
//     /// 根据token_hash获取对应的源DataServer、offset和目标DataServer
//     /// 返回: Vec<(token_hash, offset, source_server, target_server)>
//     /// 其中target_server必须是local_data_server_collect中不在KvBlockMeta的server_id中的dataserver
//     /// source_server必须是global_data_server_collect中不在local_data_server_collect中的server
//     pub async fn get_instance_from_token_hash(&self, ids: Vec<[u8; 32]>) -> Vec<([u8; 32], u32, DataServer, DataServer)> {
        
//         let remote_kvcache_table = &self.shared.global_kvcache_table;
//         let local_data_servers = self.shared.local_data_server_collect.read().await;
        
//         // 获取所有本地dataserver的id集合
//         let local_server_ids: HashSet<u32> = local_data_servers
//             .iter()
//             .map(|ds| ds.id)
//             .collect();
        
//         let mut result = Vec::new();
        
//         for token_hash in ids {
//             // 从远程kv块元数据中获取block元数据
//             if let Some(kv_meta) = remote_kvcache_table.get(&token_hash) {
//                 let offset = kv_meta.offset;
                
//                 // 首先从local_data_server中查找不在kv_meta.server_id中的目标server
//                 let target_server = local_data_servers.iter()
//                     .find(|ds| !kv_meta.server_id.contains(&ds.id));
                
//                 if let Some(target_server) = target_server {
//                     // 从kv_meta.server_id中选择一个不在local_data_server_collect中的源server
//                     let remote_source_id = kv_meta.server_id.iter()
//                         .find(|id| !local_server_ids.contains(id));
                    
//                     if let Some(source_server_id) = remote_source_id {
//                         // 从global_data_server_collect中查找源DataServer
//                         let mut source_server_found: Option<DataServer> = None;
                        
//                         // 通过映射找到源server所属的meta_server
//                         if let Some(meta_server_id) = self.shared.data_server_to_meta_server.get(source_server_id) {
//                             let meta_id = *meta_server_id;
                            
//                             // 从global_data_server_collect中查找
//                             if let Some(data_servers) = self.shared.global_data_server_collect.get(&meta_id) {
//                                 source_server_found = data_servers.iter()
//                                     .find(|ds| ds.id == *source_server_id)
//                                     .cloned();
//                             }
//                         }
                        
//                         if let Some(source_server) = source_server_found {
//                             result.push((
//                                 token_hash,
//                                 offset,
//                                 source_server,
//                                 target_server.clone()
//                             ));
//                         } else {
//                             tracing::warn!(
//                                 "Source server {} not found in global_data_server_collect for token_hash {:?}",
//                                 source_server_id,
//                                 token_hash
//                             );
//                         }
//                     } else {
//                         tracing::debug!(
//                             "No remote source server available (all replicas are in local_data_server_collect) for token_hash {:?}",
//                             token_hash
//                         );
//                     }
//                 } else {
//                     tracing::debug!(
//                         "No available target server in local_data_server_collect for token_hash {:?}",
//                         token_hash
//                     );
//                 }
//             }
//         }
        
//         result
//     }

    
// }