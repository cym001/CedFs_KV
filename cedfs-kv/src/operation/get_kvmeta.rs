use cedfs_proto::kvcache::GetKvMetaResponse;
use cedfs_proto::kvcache::LocalBlockCount as ProtoLocalBlockCount;

use crate::Shared;
use crate::types::{KvBlockMeta, DataServer, MetaServer};
use crate::convert::{hash2bytes};
// use crate::client::KvCacheClient;



pub struct GetKvMetaOp {
    pub shared: Shared,
    pub new_meta_server: MetaServer,
    pub new_data_server: Vec<DataServer>,
}

impl GetKvMetaOp {
    pub async fn run(self) -> anyhow::Result<GetKvMetaResponse> {
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
        let data_server: Vec<DataServer> = (*self.shared.data_server_collect).read().await.clone();
        
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

        // 更新本地元数据和数据服务器信息
        //KvCacheClient::update_server_status(&self.shared, vec![self.new_meta_server.clone()], self.new_data_server.clone()).await;

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