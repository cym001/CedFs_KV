use tokio::time::{interval, Duration};

use cedfs_proto::kvcache::kv_meta2_meta_client::KvMeta2MetaClient;
use cedfs_proto::kvcache::{
    GetKvMetaRequest, GetKvMetaResponse, UpdateKvMetaRequest, UpdateKvMetaResponse,
};
use cedfs_proto::kvcache::{KvBlockMeta as ProtoKvBlockMeta, LocalBlockCount};

use crate::convert::{bytes2hash, hash2bytes};
use crate::operation::move_kvreplica::MoveKVReplicaOp;
use crate::operation::popularity_score::PopularityScoreOp;
use crate::operation::transfer_kv::TransferKvOp;
use crate::types::{DataServer, MetaServer, UpdateKvOp};
use crate::Shared;

pub struct KvCacheClient {
    pub shared: Shared,
}

impl KvCacheClient {
    pub async fn launch(&self) -> anyhow::Result<()> {
        // 启动元数据同步任务
        self.launch_metadata_sync_task();

        // 启动 KV 迁移任务（如果配置开启了副本拉取）
        if self.shared.config.replica_pull {
            self.launch_kv_migration_task();
        }

        Ok(())
    }

    /// 启动元数据同步后台任务
    fn launch_metadata_sync_task(&self) {
        let sync_interval = self.shared.config.sync_interval;
        let shared = self.shared.clone();

        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(sync_interval));

            loop {
                ticker.tick().await;
                if let Err(e) = Self::sync_metadata(&shared).await {
                    tracing::error!("Metadata sync error: {:?}", e);
                }
            }
        });
    }

    /// 启动 KV 迁移后台任务
    fn launch_kv_migration_task(&self) {
        let migration_interval = self.shared.config.replica_pull_interval;
        let shared = self.shared.clone();

        tokio::spawn(async move {
            // 首次执行前等待一个间隔周期，让系统先完成初始化
            let mut ticker = interval(Duration::from_secs(migration_interval));
            ticker.tick().await; // 跳过首次立即执行

            loop {
                ticker.tick().await;
                tracing::info!("Starting scheduled KV migration task");
                
                if let Err(e) = Self::execute_kv_migration(&shared).await {
                    tracing::error!("KV migration error: {:?}", e);
                }
            }
        });
    }

    /// 执行 KV 迁移操作
    async fn execute_kv_migration(shared: &Shared) -> anyhow::Result<()> {
        // 获取需要迁移的 KV 块
        let transfer_items = PopularityScoreOp {
            shared: shared.clone(),
        }
        .run()
        .await;

        if transfer_items.is_empty() {
            tracing::debug!("No KV blocks need migration");
            return Ok(());
        }

        tracing::info!(
            "Found {} KV blocks to migrate based on popularity score",
            transfer_items.len()
        );

        let mut success_count = 0;
        let mut fail_count = 0;

        // 对每个需要迁移的 KV 块发起迁移请求
        for (token_hash, offset, src_server, dst_server) in transfer_items {
            let position = "LocalCPUBackend".to_string();

            // 发送迁移请求到源服务器 (使用 gRPC)
            let url = format!("http://{}:{}", src_server.ip, src_server.rpc_port);
            let client = TransferKvOp::new(&url);

            match client
                .send_transfer_request(
                    token_hash,
                    position,
                    vec![offset],
                    dst_server.ip.to_string(),
                    dst_server.init_port as i32,
                    true,
                )
                .await
            {
                Ok(response) => {
                    if response.success {
                        success_count += 1;
                        tracing::info!(
                            "Successfully transferred KV block from server {} to server {}",
                            src_server.id,
                            dst_server.id
                        );

                        // 更新本地元数据：将目标服务器添加到块的 server_id 列表中
                        Self::update_kv_meta_after_migration(shared, token_hash, dst_server.id)
                            .await;
                    } else {
                        fail_count += 1;
                        tracing::warn!(
                            "Transfer request returned failure for server {} to server {}",
                            src_server.id,
                            dst_server.id
                        );
                    }
                }
                Err(e) => {
                    fail_count += 1;
                    tracing::error!(
                        "Failed to transfer KV block from server {} to server {}: {:?}\n\
                        Details: token_hash={:?}, offset={}, \n\
                        src_server=(id={}, ip={}, rpc_port={}), \n\
                        dst_server=(id={}, ip={}, init_port={}), \n\
                        request_url=http://{}:{}",
                        src_server.id,
                        dst_server.id,
                        e,
                        token_hash,
                        offset,
                        src_server.id,
                        src_server.ip,
                        src_server.rpc_port,
                        dst_server.id,
                        dst_server.ip,
                        dst_server.init_port,
                        src_server.ip,
                        src_server.rpc_port
                    );
                }
            }
        }

        tracing::info!(
            "KV migration task completed: {} succeeded, {} failed",
            success_count,
            fail_count
        );

        Ok(())
    }

    /// 迁移完成后更新 KV 元数据
    async fn update_kv_meta_after_migration(
        shared: &Shared,
        token_hash: [u8; 32],
        new_server_id: u32,
    ) {
        // 更新 global_kvcache_table
        if let Some(mut meta) = shared.global_kvcache_table.get_mut(&token_hash) {
            if !meta.server_id.contains(&new_server_id) {
                meta.server_id.push(new_server_id);
            }
        }

        // 添加到本地 KV 索引
        shared.insert_local_kvcache(token_hash).await;

        // 更新本地引用计数
        shared.ref_count.increment_local_ref_count(token_hash, 1);

        // 生成更新操作，用于同步给其他元数据服务器
        let update_op = crate::types::UpdateKvOp {
            token_hash,
            operation: 1, // 添加副本操作
            server_id: new_server_id,
        };
        shared.insert_update_kvop(update_op);

        tracing::debug!(
            "Updated KV metadata after migration for token_hash {:?}, added server_id {}",
            token_hash,
            new_server_id
        );
    }

    /// 执行一次元数据同步
    async fn sync_metadata(shared: &Shared) -> anyhow::Result<()> {
        // 获取当前时间戳
        let update_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();

        // 克隆数据并清空原有数据
        let update_meta = {
            let meta_snapshot: Vec<ProtoKvBlockMeta> = shared
                .update_kvmeta_table
                .iter()
                .map(|entry| entry.value().clone().into())
                .collect();

            shared.update_kvmeta_table.clear();
            meta_snapshot
        };

        let update_kvop = {
            let op_snapshot: Vec<cedfs_proto::kvcache::UpdateKvOp> = shared
                .update_kvop_table
                .iter()
                .map(|entry| entry.value().clone().into())
                .collect();

            shared.update_kvop_table.clear();
            op_snapshot
        };

        let local_counts = {
            let counts_snapshot: Vec<LocalBlockCount> = shared
                .ref_count
                .local_incremental_count
                .iter()
                .map(|entry| LocalBlockCount {
                    token_hash: hash2bytes(*entry.key()),
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
            update_op: update_kvop,
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
            // 跳过不可用的服务器(layer为3及以上)和本地节点
            if meta_server.layer >= 3 {
                continue;
            }
            if meta_server.ip == shared.config.local_meta_server.ip
                && meta_server.port == shared.config.local_meta_server.port
            {
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
                                )
                                .await;

                                // 写入日志：与*元数据服务器同步成功
                                tracing::info!("Successfully synced metadata with {}.", addr);
                                Ok(())
                            },
                            // RPC调用失败，将该元数据服务器的layer标记为4
                            Err(e) => {
                                Self::mark_meta_server_unavailable(&shared_clone, idx, 4).await;
                                tracing::error!("Failed to sync metadata with {}: {:?}", addr, e);
                                Err(anyhow::anyhow!(
                                    "Failed to update meta on {}: {:?}",
                                    addr,
                                    e
                                ))
                            },
                        }
                    },
                    // 连接失败，将该元数据服务器的layer标记为5
                    Err(e) => {
                        Self::mark_meta_server_unavailable(&shared_clone, idx, 5).await;
                        Err(anyhow::anyhow!("Failed to connect to {}: {:?}", addr, e))
                    },
                }
            });

            tasks.push(task);
        }

        // 等待所有任务完成
        for task in tasks {
            match task.await {
                Ok(Ok(_)) => {
                    // 成功
                },
                Ok(Err(e)) => {
                    tracing::error!("Metadata sync task error: {:?}", e);
                },
                Err(e) => {
                    tracing::error!("Metadata sync task join error: {:?}", e);
                },
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
                if let Some(pos) = meta_collect
                    .iter()
                    .position(|s| s.ip == updated_server.ip && s.port == updated_server.port)
                {
                    meta_collect[pos] = updated_server;
                } else {
                    // 如果没有找到对应的服务器，说明是新加入的元数据服务器
                    // 先释放锁，然后进行异步操作
                    meta_collect.push(updated_server.clone());
                    drop(meta_collect);

                    if let Err(e) = Self::get_kvmeta(shared, updated_server.clone()).await {
                        // 如果失败,从列表中移除
                        let mut meta_collect = shared.meta_server_collect.write().await;
                        meta_collect.retain(|s| {
                            !(s.ip == updated_server.ip && s.port == updated_server.port)
                        });
                        tracing::error!(
                            "Failed to get initial KV meta from new meta server {}: {:?}",
                            updated_server.ip,
                            e
                        );
                    }

                    // 重新获取锁以便继续循环
                    meta_collect = shared.meta_server_collect.write().await;
                }
            }
        }

        // 更新数据服务器状态
        {
            let mut data_collect = shared.local_data_server_collect.write().await;
            for updated_server in data_servers {
                // 根据IP和端口查找并更新对应的服务器
                if let Some(pos) = data_collect.iter().position(|s| {
                    s.ip == updated_server.ip && s.http_port == updated_server.http_port
                }) {
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
        let data_servers: Vec<cedfs_proto::kvcache::DataServer> = shared
            .local_data_server_collect
            .read()
            .await
            .clone()
            .into_iter()
            .map(|d| d.into())
            .collect();
        let req = GetKvMetaRequest {
            meta_server: Some(shared.config.local_meta_server.clone().into()),
            data_server: data_servers,
        };

        match KvMeta2MetaClient::connect(addr.clone()).await {
            Ok(mut client) => {
                match client.get_kv_meta(req).await {
                    Ok(response) => {
                        let resp: GetKvMetaResponse = response.into_inner();

                        // 更新本地kv元数据
                        for block in resp.meta.into_iter() {
                            shared.insert_global_kvcache(block.into());
                        }
                        // 更新引用计数
                        for count in resp.local_counts.into_iter() {
                            shared.ref_count.increment_global_ref_count(
                                bytes2hash(count.token_hash),
                                count.count,
                            );
                        }

                        Ok(())
                    },
                    // RPC调用失败
                    Err(e) => Err(anyhow::anyhow!(
                        "Failed to update meta on {}: {:?}",
                        addr,
                        e
                    )),
                }
            },
            // 连接失败
            Err(e) => Err(anyhow::anyhow!("Failed to connect to {}: {:?}", addr, e)),
        }
    }

    /// kv cache 根据热度迁移
    pub async fn move_kv_replica(&self) -> anyhow::Result<()> {
        let transfer_items = PopularityScoreOp {
            shared: self.shared.clone(),
        }
        .run()
        .await;

        // 对每个源服务器发起迁移请求
        for (token_hash, offset, src_server, dst_server) in transfer_items {
            let old_position = (
                src_server.ip.to_string(),
                src_server.http_port.to_string(),
            );

            // 发送迁移请求到源服务器
            let url = format!("http://{}:{}", src_server.ip, src_server.http_port);
            let client = MoveKVReplicaOp::new(&url);

            match client
                .send_transfer_request(
                    vec![token_hash],
                    old_position,
                    vec![offset as i64],
                    dst_server.ip.to_string(),
                    dst_server.init_port as i32,
                    true,
                )
                .await
            {
                Ok(response) => {
                    tracing::info!(
                        "Successfully transferred from server {} to server {}, num_tokens: {}",
                        src_server.id,
                        dst_server.id,
                        response.num_tokens
                    );

                    // 更新本地元数据
                },
                Err(e) => {
                    tracing::error!(
                        "Failed to transfer from server {} to server {} : {:?}",
                        src_server.id,
                        dst_server.id,
                        e
                    );
                },
            }
        }

        Ok(())
    }

    /// kv cache 根据热度迁移 (使用 gRPC TransferKv)
    pub async fn transfer_kv(&self) -> anyhow::Result<()> {
        let transfer_items = PopularityScoreOp {
            shared: self.shared.clone(),
        }
        .run()
        .await;

        // 对每个源服务器发起迁移请求
        for (token_hash, offset, src_server, dst_server) in transfer_items {
            let position = format!("LocalCPUBackend");

            // 发送迁移请求到源服务器 (使用 gRPC)
            let url = format!("http://{}:{}", src_server.ip, src_server.rpc_port);
            let client = TransferKvOp::new(&url);

            match client
                .send_transfer_request(
                    token_hash,
                    position,
                    vec![offset],
                    dst_server.ip.to_string(),
                    dst_server.init_port as i32,
                    true,
                )
                .await
            {
                Ok(response) => {
                    tracing::info!(
                        "Successfully transferred from server {} to server {}, success: {}",
                        src_server.id,
                        dst_server.id,
                        response.success
                    );

                    // 更新本地元数据
                },
                Err(e) => {
                    tracing::error!(
                        "Failed to transfer from server {} to server {} : {:?}",
                        src_server.id,
                        dst_server.id,
                        e
                    );
                },
            }
        }

        Ok(())
    }
}

