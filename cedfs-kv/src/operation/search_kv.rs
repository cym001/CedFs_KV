use cedfs_proto::kvcache::{SearchKvBlockResponse, SearchResult, KvBlockPos};

use crate::Shared;
//use crate::types::KvBlockKey;

pub struct SearchKvOp {
    pub shared: Shared,
    pub query_lists: Vec<Vec<i64>>,
}

impl SearchKvOp {
    pub async fn run(self) -> anyhow::Result<SearchKvBlockResponse> {
        let mut res = Vec::new();

        // // 遍历每个查询列表
        // for token_lists in self.query_lists.iter() {
        //     let mut search_result = SearchResult {
        //         block_pos: Vec::new(),
        //     };

        //     if token_lists.is_empty() {
        //         // 如果token_lists为空，直接添加空结果
        //         res.push(search_result);
        //         continue;
        //     }

        //     // 从第一个tokens开始匹配，获取初始的server_id集合
        //     let first_tokens = &token_lists[0];
        //     let first_token_hash = calculate_token_hash(first_tokens);
            
        //     // 用于存储每个server_id的匹配计数 (server_id -> matched_count)
        //     let mut server_match_count: std::collections::HashMap<u32, u32> = std::collections::HashMap::new();
            
        //     // 查找第一个tokens的匹配
        //     if let Some(block_ids) = self.shared.global_kv_index.get(&first_token_hash) {
        //         for &block_id in block_ids.value() {
        //             let mut kv_meta = None;

        //             if let Some(meta) = self.shared.global_kvcache_table.get(&block_id) {
        //                 if meta.tokens_match(first_tokens) {
        //                     kv_meta = Some(meta.clone());
        //                 }
        //             }
                    
        //             // 如果找到匹配，初始化server_id的匹配计数
        //             if let Some(meta) = kv_meta {
        //                 for &server_id in &meta.server_id {
        //                     server_match_count.insert(server_id, 1);
        //                 }
        //             }
        //         }
        //     }

        //     // 如果第一个tokens就没有匹配，返回空结果
        //     if server_match_count.is_empty() {
        //         res.push(search_result);
        //         continue;
        //     }

        //     // 继续匹配后续的tokens
        //     for i in 1..token_lists.len() {
        //         let tokens = &token_lists[i];
        //         let token_hash = calculate_token_hash(tokens);
                
        //         // 用于存储当前仍然匹配的server_id
        //         let mut still_matching: std::collections::HashSet<u32> = std::collections::HashSet::new();
                
        //         // 查找当前tokens的匹配
        //         if let Some(block_ids) = self.shared.global_kv_index.get(&token_hash) {
        //             for &block_id in block_ids.value() {
        //                 let mut kv_meta = None;
                        
        //                 if let Some(meta) = self.shared.global_kvcache_table.get(&block_id) {
        //                     if meta.tokens_match(tokens) {
        //                         kv_meta = Some(meta.clone());
        //                     }
        //                 }
                        
        //                 // 检查是否包含之前匹配的server_id
        //                 if let Some(meta) = kv_meta {
        //                     for &server_id in &meta.server_id {
        //                         if server_match_count.contains_key(&server_id) {
        //                             still_matching.insert(server_id);
        //                         }
        //                     }
        //                 }
        //             }
        //         }
                
        //         // 更新匹配计数：只保留仍在匹配的server_id
        //         server_match_count.retain(|server_id, count| {
        //             if still_matching.contains(server_id) {
        //                 *count += 1;
        //                 true
        //             } else {
        //                 false
        //             }
        //         });
                
        //         // 如果没有server_id能继续匹配，停止
        //         if server_match_count.is_empty() {
        //             break;
        //         }
        //     }

        //     // 生成最终结果：为每个匹配的server_id创建KvBlockPos
        //     let data_servers = self.shared.data_server_collect.read().await;
        //     for (server_id, matched_count) in server_match_count {
        //         // 查找对应的DataServer信息
        //         if let Some(data_server) = data_servers.iter().find(|s| s.id == server_id) {
        //             let url = format!("{}:{}", data_server.ip, data_server.http_port);
        //             let kv_block_pos = KvBlockPos {
        //                 model_name: data_server.model_name.clone(),
        //                 url,
        //                 len: matched_count,
        //             };
        //             search_result.block_pos.push(kv_block_pos);
        //         }
        //     }
            
        //     // 将当前查询的搜索结果添加到总结果中（即使为空也添加）
        //     res.push(search_result);
        // }
        
        Ok(SearchKvBlockResponse {
            results: res,
        })
        
    }

}

