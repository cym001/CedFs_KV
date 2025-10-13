use tokio::time::{interval, Duration};

use cedfs_proto::kvcache::kv_meta2_meta_client::KvMeta2MetaClient;
use cedfs_proto::kvcache::{UpdateKvMetaRequest, UpdateKvMetaResponse, GetKvMetaRequest, GetKvMetaResponse};
use cedfs_proto::kvcache::{KvBlockMeta as ProtoKvBlockMeta, LocalBlockCount};

use crate::types::{DataServer, MetaServer};
use crate::Shared;

pub struct KvCacheClient {
    pub shared: Shared,
}

impl KvCacheClient {
    pub async fn launch(&self) -> anyhow::Result<()> {
        let sync_interval = self.shared.config.sync_interval;
        let shared = self.shared.clone();
        
        // 启动后台任务进行定期同步
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(sync_interval));
            
            loop {
                ticker.tick().await;
                
                if let Err(e) = Self::sync_metadata(&shared).await {
                    tracing::error!("Metadata sync error: {:?}", e);
                }
            }
        });
        
        Ok(())
    }
    
    /// 执行一次元数据同步
    async fn sync_metadata(shared: &Shared) -> anyhow::Result<()> {
        // 获取当前时间戳
        let update_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();
        
        // 克隆数据并清空原有数据
        let update_meta = {
            let meta_snapshot: Vec<ProtoKvBlockMeta> = shared.update_kvmeta_table
                .iter()
                .map(|entry| entry.value().clone().into())
                .collect();
            
            shared.update_kvmeta_table.clear();
            meta_snapshot
        };
        
        let local_counts = {
            let counts_snapshot: Vec<LocalBlockCount> = shared.ref_count
                .local_ref_counts
                .iter()
                .map(|entry| LocalBlockCount {
                    block_id: *entry.key(),
                    count: *entry.value(),
                })
                .collect();
            
            shared.ref_count.clear_and_consolidate_incremental_counts();
            counts_snapshot
        };
        
        // 如果没有数据需要同步，直接返回
        if update_meta.is_empty() && local_counts.is_empty() {
            return Ok(());
        }
        
        let req = UpdateKvMetaRequest {
            meta: update_meta,
            local_counts,
            update_time,
        };
        
        // 获取元数据服务器列表的读锁
        let meta_servers = {
            let servers = shared.meta_server_collect.read().await;
            servers.clone()
        };
        
        let mut tasks = Vec::new();

        // 只对layer为0,1,2的服务器进行元数据同步
        for (idx, meta_server) in meta_servers.iter().enumerate() {
            // 跳过不可用的服务器(layer为3及以上)
            if meta_server.layer >= 3 {
                continue;
            }
            
            let addr = format!("http://{}:{}", meta_server.ip, meta_server.port);
            let req_clone = req.clone();
            let shared_clone = shared.clone();
            
            let task = tokio::spawn(async move {
                match KvMeta2MetaClient::connect(addr.clone()).await {
                    Ok(mut client) => {
                        match client.update_kv_meta(req_clone).await {
                            Ok(response) => {
                                let resp: UpdateKvMetaResponse = response.into_inner();
                                
                                // 更新本地的MetaServerCollect信息
                                Self::update_server_status(
                                    &shared_clone,
                                    resp.meta_server.into_iter().map(|d| d.into()).collect(),
                                    resp.data_server.into_iter().map(|d| d.into()).collect(),
                                ).await;
                                
                                Ok(())
                            }
                            // RPC调用失败，将该元数据服务器的layer标记为4
                            Err(e) => {
                                Self::mark_meta_server_unavailable(&shared_clone, idx, 4).await;
                                Err(anyhow::anyhow!("Failed to update meta on {}: {:?}", addr, e))
                            }
                        }
                    }
                    // 连接失败，将该元数据服务器的layer标记为5
                    Err(e) => {
                        Self::mark_meta_server_unavailable(&shared_clone, idx, 5).await;
                        Err(anyhow::anyhow!("Failed to connect to {}: {:?}", addr, e))
                    }
                }
            });
            
            tasks.push(task);
        }

        // 等待所有任务完成
        for task in tasks {
            match task.await {
                Ok(Ok(_)) => {
                    // 成功
                }
                Ok(Err(e)) => {
                    tracing::error!("Metadata sync task error: {:?}", e);
                }
                Err(e) => {
                    tracing::error!("Metadata sync task join error: {:?}", e);
                }
            }
        }
        
        Ok(())
    }
    
    /// 更新服务器状态（包括元数据服务器和数据服务器）
    pub async fn update_server_status(
        shared: &Shared,
        meta_servers: Vec<MetaServer>,
        data_servers: Vec<DataServer>,
    ) {
        // 更新元数据服务器状态
        {
            let mut meta_collect = shared.meta_server_collect.write().await;
            for updated_server in meta_servers {
                // 根据IP和端口查找并更新对应的服务器
                if let Some(pos) = meta_collect.iter().position(|s| 
                    s.ip == updated_server.ip && s.port == updated_server.port
                ) {
                    meta_collect[pos] = updated_server;
                } else {
                    // 如果没有找到对应的服务器，说明是新加入的元数据服务器
                    // 先释放锁，然后进行异步操作
                    drop(meta_collect);
                    
                    if let Err(e) = Self::get_kvmeta(shared, updated_server.clone()).await {
                        tracing::error!("Failed to sync with new meta server {}: {:?}", updated_server.ip, e);
                    } else {
                        // 重新获取锁并添加服务器
                        let mut meta_collect = shared.meta_server_collect.write().await;
                        meta_collect.push(updated_server);
                    }
                    
                    // 重新获取锁以便继续循环
                    meta_collect = shared.meta_server_collect.write().await;
                }
            }
        }
        
        // 更新数据服务器状态
        {
            let mut data_collect = shared.data_server_collect.write().await;
            for updated_server in data_servers {
                // 根据IP和端口查找并更新对应的服务器
                if let Some(pos) = data_collect.iter().position(|s| 
                    s.ip == updated_server.ip && 
                    s.http_port == updated_server.http_port &&
                    s.rpc_port == updated_server.rpc_port
                ) {
                    data_collect[pos] = updated_server;
                }
            }
        }
    }

    /// 标记元数据服务器为不可用状态
    async fn mark_meta_server_unavailable(shared: &Shared, idx: usize, layer: u32) {
        let mut meta_collect = shared.meta_server_collect.write().await;
        if idx < meta_collect.len() {
            meta_collect[idx].layer = layer;
        }
    }

    /// 手动触发一次同步（可选）
    pub async fn sync_now(&self) -> anyhow::Result<()> {
        Self::sync_metadata(&self.shared).await
    }

    /// 向新加入的元数据服务器发起全量同步请求
    pub async fn get_kvmeta(shared: &Shared, meta_server: MetaServer) -> anyhow::Result<()> {
        let addr = format!("http://{}:{}", meta_server.ip, meta_server.port);
        let req = GetKvMetaRequest {
            meta_server: Some(shared.config.local_meta_server.clone().into()),
            data_server: Some(shared.config.local_data_server.clone().into()),
        };
        
        match KvMeta2MetaClient::connect(addr.clone()).await {
            Ok(mut client) => {
                match client.get_kv_meta(req).await {
                    Ok(response) => {
                        let resp: GetKvMetaResponse = response.into_inner();

                        // 更新本地kv元数据
                        for block in resp.meta.iter() {
                            shared.insert_remote_kvcache((*block).clone().into());
                        }
                        // 更新引用计数
                        for count in resp.local_counts.iter() {
                            shared.ref_count.increment_global_ref_count(count.block_id, count.count);
                        }
                        
                        Ok(())
                    }
                    // RPC调用失败
                    Err(e) => {
                        Err(anyhow::anyhow!("Failed to update meta on {}: {:?}", addr, e))
                    }
                }
            }
            // 连接失败
            Err(e) => {
                Err(anyhow::anyhow!("Failed to connect to {}: {:?}", addr, e))
            }
        }
    }
}