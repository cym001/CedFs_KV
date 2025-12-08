use cedfs_proto::kvcache::{KvBlockPos, SearchKvBlockResponse, SearchResult, SearchKvBlockByPromptsResponse};

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

            let token_hashes = self
                .shared
                .hasher
                .hash_tokens_with_blocks_all(&token_list, self.shared.config.block_size)
                .iter()
                .map(|(hash, _offset)| hash.to_u256())
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

pub struct SearchKvByPromptsOp {
    pub shared: Shared,
    pub model_names: Vec<String>,
    pub prompts: Vec<String>,
}

impl SearchKvByPromptsOp {
    pub async fn run(self) -> anyhow::Result<SearchKvBlockByPromptsResponse> {
        let mut res = Vec::new();

        // 遍历每个prompt
        for prompt in self.prompts.iter() {
            let mut search_result = SearchResult {
                block_pos: Vec::new(),
            };

            if prompt.is_empty() {
                // 如果prompt为空，直接添加空结果
                res.push(search_result);
                continue;
            }

            // 对每个model_name，生成token并进行匹配
            for model_name in self.model_names.iter() {
                // 使用tokenizer对prompt进行编码
                let token_list = match self.shared.tokenizer_manager.encode_async(model_name, prompt).await {
                    Ok(token_list) => token_list,
                    Err(e) => {
                        tracing::warn!("Failed to encode prompt with model '{}': {}", model_name, e);
                        continue; // 跳过这个model_name
                    }
                };

                // 输出前50个token用于调试
                // let preview_tokens: Vec<u32> = token_list.iter().take(50).copied().collect();
                // tracing::info!(
                //     "Encoded prompt with model '{}': {} tokens total, first 50: {:?}",
                //     model_name,
                //     token_list.len(),
                //     preview_tokens
                // );

                if token_list.is_empty() {
                    continue;
                }

                let token_hashes = self
                    .shared
                    .hasher
                    .hash_tokens_with_blocks_all(&token_list, self.shared.config.block_size)
                    .iter()
                    .map(|(hash, _offset)| hash.to_u256())
                    .collect();

                // 使用 search_tokens 查找所有 server 的匹配结果
                let match_results = self.shared.search_tokens(token_hashes);

                // 生成结果：为每个匹配的 server_id 创建 KvBlockPos
                let data_servers = self.shared.data_server_collect.read().await;
                for (server_id, matched_length) in match_results {
                    // 查找对应的 DataServer 信息
                    if let Some(data_server) = data_servers.iter().find(|s| s.id == server_id) {
                        // 只添加model_name匹配的结果
                        if &data_server.model_name == model_name {
                            let kv_block_pos = KvBlockPos {
                                model_name: data_server.model_name.clone(),
                                url: data_server.url.clone(),
                                len: matched_length,
                            };
                            search_result.block_pos.push(kv_block_pos);
                        }
                    }
                }
            }

            res.push(search_result);
        }

        Ok(SearchKvBlockByPromptsResponse { results: res })
    }
}