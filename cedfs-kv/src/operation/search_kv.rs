use cedfs_proto::kvcache::{KvBlockPos, SearchKvBlockResponse, SearchResult};

use crate::Shared;
//use crate::types::KvBlockKey;

pub struct SearchKvOp {
    pub shared: Shared,
    pub query_lists: Vec<Vec<i64>>,
}

impl SearchKvOp {
    pub async fn run(self) -> anyhow::Result<SearchKvBlockResponse> {
        let mut res = Vec::new();

        // 遍历每个查询列表
        for token_list in self.query_lists.iter() {
            let mut search_result = SearchResult {
                block_pos: Vec::new(),
            };

            if token_list.is_empty() {
                // 如果token_list为空，直接添加空结果
                res.push(search_result);
                continue;
            }

            // 将 Vec<i64> 转换为 Vec<[u8; 32]>
            let token_hashes = self
                .shared
                .hasher
                .hash_tokens_with_blocks_all(&token_list, self.shared.config.block_size)
                .iter()
                .map(|x| x.to_u256())
                .collect();

            // 使用 search_tokens 查找所有 server 的匹配结果
            let match_results = self.shared.search_tokens(token_hashes);

            // 生成最终结果：为每个匹配的 server_id 创建 KvBlockPos
            let data_servers = self.shared.data_server_collect.read().await;
            for (server_id, matched_length) in match_results {
                // 查找对应的 DataServer 信息
                if let Some(data_server) = data_servers.iter().find(|s| s.id == server_id) {
                    let url = format!("{}:{}", data_server.ip, data_server.http_port);
                    let kv_block_pos = KvBlockPos {
                        model_name: data_server.model_name.clone(),
                        url,
                        len: matched_length,
                    };
                    search_result.block_pos.push(kv_block_pos);
                }
            }

            res.push(search_result);
        }

        Ok(SearchKvBlockResponse { results: res })
    }
}
