use cedfs_proto::kvcache::GetKvMetaResponse;
use cedfs_proto::kvcache::LocalBlockCount as ProtoLocalBlockCount;

use crate::Shared;
use crate::types::{KvBlockMeta, DataServer, MetaServer};
use crate::client::KvCacheClient;



pub struct GetKvMetaOp {
    pub shared: Shared,
    pub new_meta_server: MetaServer,
    pub new_data_server: DataServer,
}

impl GetKvMetaOp {
    pub async fn run(self) -> anyhow::Result<GetKvMetaResponse> {
        // 1. 收集本地 KV 块元数据
        let meta: Vec<KvBlockMeta> = self.shared
            .local_kvcache_table
            .iter()
            .map(|entry| entry.value().clone())
            .collect();
        // 2. 获取已知的元数据/数据服务器信息
        let meta_server: Vec<MetaServer> = (*self.shared.meta_server_collect).read().await.clone();
        let data_server: Vec<DataServer> = (*self.shared.data_server_collect).read().await.clone();
        
        // 3. 收集本地所有 block 的计数
        let local_counts: Vec<ProtoLocalBlockCount> = self.shared
            .ref_count
            .get_all_local_total_counts()
            .iter()
            .map(|(block_id, count)| {
                ProtoLocalBlockCount {
                    block_id: *block_id,
                    count: *count,
                }
            })
            .collect();

        // 更新本地元数据和数据服务器信息
        KvCacheClient::update_server_status(&self.shared, vec![self.new_meta_server.clone()], vec![self.new_data_server.clone()]).await;
        
        Ok(GetKvMetaResponse {
            meta: meta.into_iter().map(|m| m.into()).collect(),
            meta_server: meta_server.into_iter().map(|m| m.into()).collect(),
            data_server: data_server.into_iter().map(|d| d.into()).collect(),
            local_counts,
        })
    }
}