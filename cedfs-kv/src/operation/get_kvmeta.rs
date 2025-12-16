use cedfs_proto::kvcache::GetKvMetaResponse;
use cedfs_proto::kvcache::LocalBlockCount as ProtoLocalBlockCount;

use crate::Shared;
use crate::types::{KvBlockMeta, DataServer, MetaServer};
use crate::convert::{hash2bytes};



pub struct GetKvMetaOp {
    pub shared: Shared,
    pub new_meta_server: MetaServer,
    pub new_data_server: Vec<DataServer>,
}

impl GetKvMetaOp {
    pub async fn run(self) -> anyhow::Result<GetKvMetaResponse> {
        // 0. 添加新的元数据服务器和数据服务器
        let new_meta_id = self.new_meta_server.id;
        
        // 添加新的元数据服务器
        {
            let mut meta_servers = self.shared.meta_server_collect.write().await;
            if !meta_servers.iter().any(|m| m.id == new_meta_id) {
                meta_servers.push(self.new_meta_server.clone());
                tracing::info!(
                    "Added new meta_server {} ({}:{}) to meta_server_collect",
                    new_meta_id,
                    self.new_meta_server.ip,
                    self.new_meta_server.port
                );
            } else {
                tracing::debug!(
                    "Meta_server {} already exists in meta_server_collect",
                    new_meta_id
                );
            }
        }
        
        // 添加新的数据服务器到全局集合
        for data_server in &self.new_data_server {
            let data_server_id = data_server.id;
            
            // 添加到 global_data_server_collect（按 meta_server_id 分组）
            self.shared.global_data_server_collect
                .entry(new_meta_id)
                .and_modify(|servers| {
                    if !servers.iter().any(|ds| ds.id == data_server_id) {
                        servers.push(data_server.clone());
                        tracing::info!(
                            "Added data_server {} (model: {}) to global_data_server_collect under meta_server {}",
                            data_server_id,
                            data_server.model_name,
                            new_meta_id
                        );
                    } else {
                        tracing::debug!(
                            "Data_server {} already exists in global_data_server_collect under meta_server {}",
                            data_server_id,
                            new_meta_id
                        );
                    }
                })
                .or_insert_with(|| {
                    tracing::info!(
                        "Created new entry in global_data_server_collect for meta_server {} with data_server {}",
                        new_meta_id,
                        data_server_id
                    );
                    vec![data_server.clone()]
                });
            
            // 建立 data_server 到 meta_server 的映射
            self.shared.data_server_to_meta_server
                .insert(data_server_id, new_meta_id);
            tracing::info!(
                "Mapped data_server {} to meta_server {} in data_server_to_meta_server",
                data_server_id,
                new_meta_id
            );
        }
        
        // 1. 收集本地 KV 块元数据
        let indexs = self.shared.local_kv_index.read().await.clone();
        let meta: Vec<KvBlockMeta> = indexs
            .iter()
            .filter_map(|token_hash| {
                self.shared.global_kvcache_table.get(token_hash).map(|entry| entry.value().clone())
            })
            .collect();
        
        // 2. 获取已知的元数据/数据服务器信息
        let meta_server: Vec<MetaServer> = (*self.shared.meta_server_collect).read().await.clone();
        let data_server: Vec<DataServer> = (*self.shared.local_data_server_collect).read().await.clone();
        
        // 3. 收集本地所有 block 的计数
        let local_counts: Vec<ProtoLocalBlockCount> = self.shared
            .ref_count
            .get_all_local_total_counts()
            .iter()
            .map(|(token_hash, count)| {
                ProtoLocalBlockCount {
                    token_hash: hash2bytes(*token_hash),
                    count: *count,
                }
            })
            .collect();

        tracing::info!("GetKvMetaOp: Send {} local KV block metas, {} meta servers, {} data servers, and {} local block counts.",
            meta.len(), meta_server.len(), data_server.len(), local_counts.len());
        
        Ok(GetKvMetaResponse {
            meta: meta.into_iter().map(|m| m.into()).collect(),
            meta_server: meta_server.into_iter().map(|m| m.into()).collect(),
            data_server: data_server.into_iter().map(|d| d.into()).collect(),
            local_counts,
        })
    }
}