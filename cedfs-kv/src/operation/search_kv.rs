use cedfs_proto::kvcache::{
    KvBlockPos, SearchKvBlockByPromptsResponse, SearchKvBlockResponse, SearchResult,
};

use crate::types::BlockHashInfo;
use crate::Shared;

pub struct SearchKvOp {
    pub shared: Shared,
    pub query_lists: Vec<Vec<u32>>,
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

            let block_infos = self
                .shared
                .hasher
                .hash_tokens_with_block_infos_all(&token_list, self.shared.config.block_size);

            // 使用新索引查找所有 server 的匹配结果
            let match_results = self.shared.search_tokens_by_infos(&block_infos);

            // 生成最终结果：为每个匹配的 server_id 创建 KvBlockPos
            // 从全局数据节点集合中查找
            for (server_id, matched_length) in match_results {
                // 首先通过映射找到该 data_server 所属的 meta_server
                if let Some(meta_server_id) = self.shared.data_server_to_meta_server.get(&server_id)
                {
                    let meta_id = *meta_server_id;

                    // 从 global_data_server_collect 中查找对应的 DataServer 信息
                    if let Some(data_servers) = self.shared.global_data_server_collect.get(&meta_id)
                    {
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
                }
            }

            res.push(search_result);
        }

        Ok(SearchKvBlockResponse { results: res })
    }
}

pub struct SearchKvByPromptsOp {
    pub shared: Shared,
    pub model_names: Vec<String>,
    pub prompts: Vec<String>,
}

impl SearchKvByPromptsOp {
    /// 对单个 model 和 prompt 编码，返回 token_hashes
    async fn search_one_prompt_one_model(
        shared: &Shared,
        model_name: &str,
        prompt: &str,
    ) -> Option<Vec<BlockHashInfo>> {
        let token_list = shared
            .tokenizer_manager
            .encode_async(model_name, prompt)
            .await
            .map_err(|e| {
                tracing::warn!("Failed to encode prompt with model '{}': {}", model_name, e);
            })
            .ok()?;
        if token_list.is_empty() {
            return None;
        }
        let block_infos = shared
            .hasher
            .hash_tokens_with_block_infos_all(&token_list, shared.config.block_size);
        Some(block_infos)
    }

    /// 对 block_infos 执行索引查询，再根据匹配结果和 model_name 从全局数据节点集合中生成 KvBlockPos 列表
    fn match_results_to_block_pos(
        shared: &Shared,
        block_infos: &[BlockHashInfo],
        model_name: &str,
    ) -> Vec<KvBlockPos> {
        let match_results = shared.search_tokens_by_infos(block_infos);
        let mut block_pos = Vec::new();
        for (server_id, matched_length) in match_results {
            if let Some(meta_server_id) = shared.data_server_to_meta_server.get(&server_id) {
                let meta_id = *meta_server_id;
                if let Some(data_servers) = shared.global_data_server_collect.get(&meta_id) {
                    if let Some(data_server) = data_servers.iter().find(|s| s.id == server_id) {
                        if data_server.model_name == model_name {
                            block_pos.push(KvBlockPos {
                                model_name: data_server.model_name.clone(),
                                url: data_server.url.clone(),
                                len: matched_length,
                            });
                        }
                    }
                }
            }
        }
        block_pos
    }

    pub async fn run(self) -> anyhow::Result<SearchKvBlockByPromptsResponse> {
        let mut res = Vec::new();

        for prompt in self.prompts.iter() {
            let mut search_result = SearchResult {
                block_pos: Vec::new(),
            };

            if prompt.is_empty() {
                res.push(search_result);
                continue;
            }

            for model_name in self.model_names.iter() {
                if let Some(block_infos) =
                    Self::search_one_prompt_one_model(&self.shared, model_name, prompt).await
                {
                    let positions =
                        Self::match_results_to_block_pos(&self.shared, &block_infos, model_name);
                    search_result.block_pos.extend(positions);
                }
            }

            res.push(search_result);
        }

        Ok(SearchKvBlockByPromptsResponse { results: res })
    }
}
