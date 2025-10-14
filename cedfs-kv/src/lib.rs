use std::path::PathBuf;
use std::sync::Arc;
use dashmap::DashMap;
use tokio::sync::RwLock;
use tracing::info;

use cedfs_proto::kvcache::kv_meta2_meta_server::KvMeta2MetaServer;
use cedfs_proto::kvcache::kv_meta2_data_server::KvMeta2DataServer;

use crate::types::{KvBlockMeta, DataServer, RefCount, MetaServer, ServerSocket};
use crate::config::Config;
use crate::network::kv_meta2meta::KvCacheMetaService;
use crate::network::kv_meta2data::KvCacheDataService;

pub mod config;
pub mod types;
//pub mod persistence;
pub mod network;
pub mod operation;
pub mod client;

#[derive(Clone)]
pub struct Shared{
    // 推理节点信息
    pub data_server_collect: Arc<RwLock<Vec<DataServer>>>,

    // 元数据服务器信息
    pub meta_server_collect: Arc<RwLock<Vec<MetaServer>>>,

    // 本地kv块元数据
    pub local_kvcache_table: Arc<DashMap<u64, KvBlockMeta>>,

    // 远程kv块元数据
    pub remote_kvcache_table: Arc<DashMap<u64, KvBlockMeta>>,

    // 待更新的kvmeta
    pub update_kvmeta_table: Arc<DashMap<u64, KvBlockMeta>>,

    // 引用计数
    pub ref_count: Arc<RefCount>,

    // 节点配置
    pub config: Arc<Config>, 

}
pub struct KVServer{
    pub shared: Shared,
}

//todo() 只需要为推理节点添加路由，后续修改
impl KVServer {
    pub async fn new(config_path: PathBuf) -> anyhow::Result<Self>  {
        match Config::build_with_config(config_path) {
            Ok(config) => {
                let meta_servers = Arc::new(RwLock::new(Vec::new()));
                match config.load_remote_meta_from_config() {
                    Ok(remote_servers) => {
                        meta_servers.write().await.extend(remote_servers);
                        tracing::info!("Loaded remote meta servers from config: {:?}", *meta_servers.read().await);
                    }
                    Err(e) => {
                        tracing::error!("Failed to load remote meta servers from config: {}", e);
                        return Err(anyhow::anyhow!("Failed to load remote meta servers from config: {}", e));
                    }
                }
                let shared = Shared{
                    data_server_collect: Arc::new(RwLock::new(Vec::new())),
                    meta_server_collect: meta_servers,
                    local_kvcache_table: Arc::new(DashMap::new()),
                    remote_kvcache_table: Arc::new(DashMap::new()),
                    update_kvmeta_table: Arc::new(DashMap::new()),
                    ref_count: Arc::new(RefCount::new()),
                    config: Arc::new(config),
                };
                tracing::info!("Loaded config: {:?}", shared.config);
                Ok(KVServer{
                    shared,
                })
            }
            Err(e) => {
                tracing::error!("Failed to load config: {}", e);
                Err(anyhow::anyhow!("Failed to load config: {}", e))
            }
    }
        
    }

    pub async fn serve(self){
        let ip = self.shared.config.local_meta_server.ip.clone();
        let port = self.shared.config.local_meta_server.port;

        // start rpc server
        info!("start kvcache server on: {}", format!("{}:{}", ip, port));

        let meta_server = KvMeta2MetaServer::new(
            KvCacheMetaService{
                shared: self.shared.clone(),
            },
        );
        let data_server = KvMeta2DataServer::new(
            KvCacheDataService{
                shared: self.shared.clone(),
            }
        );



        tonic::transport::Server::builder()
            .add_service(meta_server)
            .add_service(data_server)
            .serve(format!("{}:{}", ip, port).parse().unwrap())
            .await
            .unwrap();
        
    }
    
}

impl Shared {
    /// 插入或更新本地 KV 缓存块元数据
    /// 
    /// # 参数
    /// - `block_meta`: 要插入或更新的块元数据
    /// 
    /// # 返回
    /// - `true`: 更新已存在的块
    /// - `false`: 插入新块
    pub fn insert_local_kvcache(&self, block_meta: KvBlockMeta) -> bool {
        let block_id = block_meta.block_id;
        if let Some(mut existing) = self.local_kvcache_table.get_mut(&block_id) {
            // 块已存在,更新元数据
            *existing = block_meta;
            true
        } else {
            // 块不存在,插入新块
            self.local_kvcache_table.insert(block_id, block_meta);
            false
        }
    }

    /// 插入或更新远程 KV 缓存块元数据
    /// 
    /// # 参数
    /// - `block_meta`: 要插入或更新的块元数据
    /// 
    /// # 返回
    /// - `true`: 更新已存在的块
    /// - `false`: 插入新块
    pub fn insert_remote_kvcache(&self, block_meta: KvBlockMeta) -> bool {
        let block_id = block_meta.block_id;
        
        if let Some(mut existing) = self.remote_kvcache_table.get_mut(&block_id) {
            // 块已存在,更新元数据
            //保留server_socket的交集，若为空则删除该块
            let existing_servers = &existing.server_socket;
            let new_servers = &block_meta.server_socket;
            let intersection: Vec<ServerSocket>  = existing_servers.iter()
                .filter(|s| new_servers.contains(s))
                .cloned()
                .collect();
            if intersection.is_empty(){
                self.remote_kvcache_table.remove(&block_id);
                self.ref_count.remove_global_ref_count(block_id);
                return false;
            }
            let mut block_meta = block_meta.clone();
            block_meta.server_socket = intersection;
            *existing = block_meta;
            true
        } else {
            // 块不存在,插入新块
            self.remote_kvcache_table.insert(block_id, block_meta);
            false
        }
    }

    /// 批量插入或更新本地 KV 缓存块
    pub fn batch_insert_local_kvcache(&self, blocks: Vec<KvBlockMeta>) -> (usize, usize) {
        let mut updated = 0;
        let mut inserted = 0;
        
        for block in blocks {
            if self.insert_local_kvcache(block) {
                updated += 1;
            } else {
                inserted += 1;
            }
        }
        
        (inserted, updated)
    }

    /// 批量插入或更新远程 KV 缓存块
    pub fn batch_insert_remote_kvcache(&self, blocks: Vec<KvBlockMeta>) -> (usize, usize) {
        let mut updated = 0;
        let mut inserted = 0;
        
        for block in blocks {
            if self.insert_remote_kvcache(block) {
                updated += 1;
            } else {
                inserted += 1;
            }
        }
        
        (inserted, updated)
    }

    /// 插入或更新待更新 KV 缓存块元数据
    /// 
    /// # 参数
    /// - `block_meta`: 要插入或更新的块元数据
    /// 
    /// # 返回
    /// - `true`: 更新已存在的块
    /// - `false`: 插入新块
    pub fn insert_update_kvcache(&self, block_meta: KvBlockMeta) -> bool {
        let block_id = block_meta.block_id;
        if let Some(mut existing) = self.update_kvmeta_table.get_mut(&block_id) {
            // 块已存在,更新元数据
            *existing = block_meta;
            true
        } else {
            // 块不存在,插入新块
            self.update_kvmeta_table.insert(block_id, block_meta);
            false
        }
    }
}

// 辅助函数示例
impl Shared {
    /// 从本地表删除块
    pub fn remove_local_kvcache(&self, block_id: u64) -> Option<KvBlockMeta> {
        self.local_kvcache_table.remove(&block_id).map(|(_, v)| v)
    }

    /// 从远程表删除块
    pub fn remove_remote_kvcache(&self, block_id: u64) -> Option<KvBlockMeta> {
        self.remote_kvcache_table.remove(&block_id).map(|(_, v)| v)
    }

    /// 获取本地块元数据
    pub fn get_local_kvcache(&self, block_id: u64) -> Option<KvBlockMeta> {
        self.local_kvcache_table.get(&block_id).map(|v| v.clone())
    }

    /// 获取远程块元数据
    pub fn get_remote_kvcache(&self, block_id: u64) -> Option<KvBlockMeta> {
        self.remote_kvcache_table.get(&block_id).map(|v| v.clone())
    }

    /// 清除本地 KV 缓存块元数据
    pub fn clear_local_kvcache(&self) {
        self.local_kvcache_table.clear();
    }
}
