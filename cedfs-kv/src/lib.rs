use std::path::PathBuf;
use std::sync::Arc;
use dashmap::DashMap;
use tokio::sync::RwLock;
use tracing::info;

use cedfs_proto::kvcache::kv_meta2_meta_server::KvMeta2MetaServer;
use cedfs_proto::kvcache::kv_meta2_data_server::KvMeta2DataServer;

use crate::types::{KvBlockMeta, DataServer, RefCount, MetaServer, UpdateKvOp, BlockIdGenerator};
use crate::config::Config;
use crate::network::kv_meta2meta::KvCacheMetaService;
use crate::network::kv_meta2data::KvCacheDataService;

pub mod config;
pub mod types;
//pub mod persistence;
pub mod network;
pub mod operation;
pub mod client;
pub mod convert;

#[derive(Clone)]
pub struct Shared{
    // 推理节点信息
    pub data_server_collect: Arc<RwLock<Vec<DataServer>>>,

    // 元数据服务器信息
    pub meta_server_collect: Arc<RwLock<Vec<MetaServer>>>,

    // Block ID 生成器
    pub block_id_generator: Arc<BlockIdGenerator>,

    // 本地kv块元数据
    pub local_kvcache_table: Arc<DashMap<u64, KvBlockMeta>>,

    // 全局KV块索引：(model_hash, token_hash) -> Vec<block_id>
    // 使用 Vec 存储多个 block_id 来处理哈希冲突
    pub global_kv_index: Arc<DashMap<u64, Vec<u64>>>,

    // 远程kv块元数据
    pub global_kvcache_table: Arc<DashMap<u64, KvBlockMeta>>,

    // 待更新的kvmeta
    pub update_kvmeta_table: Arc<DashMap<u64, KvBlockMeta>>,

    // 待更新的kvmeta副本操作
    pub update_kvop_table: Arc<DashMap<u64, UpdateKvOp>>,

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
                meta_servers.write().await.push(config.local_meta_server.clone());
                
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
                let data_servers = Arc::new(RwLock::new(Vec::new()));
                data_servers.write().await.push(config.local_data_server.clone());

                let meta_hash_id = config.local_meta_server.hash_id();

                let shared = Shared{
                    data_server_collect: data_servers,
                    meta_server_collect: meta_servers,
                    block_id_generator: Arc::new(BlockIdGenerator::new(meta_hash_id)),
                    global_kv_index: Arc::new(DashMap::new()),
                    local_kvcache_table: Arc::new(DashMap::new()),
                    global_kvcache_table: Arc::new(DashMap::new()),
                    update_kvmeta_table: Arc::new(DashMap::new()),
                    update_kvop_table: Arc::new(DashMap::new()),
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
        Self::insert_update_kvcache(&self, block_meta.clone());
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
        
        if let Some(mut existing) = self.global_kvcache_table.get_mut(&block_id) {
            // 块已存在,更新元数据
            //保留server_socket的交集，若为空则删除该块
            let existing_servers = &existing.server_id;
            let new_servers = &block_meta.server_id;
            let intersection: Vec<u32>  = existing_servers.iter()
                .filter(|s| new_servers.contains(s))
                .cloned()
                .collect();
            if intersection.is_empty(){
                self.global_kvcache_table.remove(&block_id);
                self.ref_count.remove_global_ref_count(block_id);
                return false;
            }
            let mut block_meta = block_meta.clone();
            block_meta.server_id = intersection;
            *existing = block_meta;
            true
        } else {
            // 块不存在,插入新块
            self.global_kvcache_table.insert(block_id, block_meta);
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

    /// 插入或更新待更新kvmeta副本操作
    /// 
    /// # 参数
    /// - `updatekv_op`: 要插入或更新的块元数据
    /// 
    /// # 返回
    /// - `true`: 更新已存在的块
    /// - `false`: 插入新块
    
    pub fn insert_update_kvop(&self, updatekv_op: UpdateKvOp) -> bool {
        let block_id = updatekv_op.block_id;
        if let Some(mut existing) = self.update_kvop_table.get_mut(&block_id) {
            // 块已存在,更新元数据
            *existing = updatekv_op;
            true
        } else {
            // 块不存在,插入新块
            self.update_kvop_table.insert(block_id, updatekv_op);
            false
        }
    }

    // 执行updatekv_op
    pub fn execute_update_kvop(&self, updatekv_op: UpdateKvOp) -> anyhow::Result<()> {
        match updatekv_op.operation {
            1 => { // 添加副本操作
                if let Some(mut kv_meta) = self.get_local_kvcache(updatekv_op.block_id) {
                    if !kv_meta.server_id.contains(&updatekv_op.server_id) {
                        kv_meta.server_id.push(updatekv_op.server_id);
                        self.insert_local_kvcache(kv_meta);
                    }
                }
                if let Some(mut kv_meta) = self.get_remote_kvcache(updatekv_op.block_id){
                    if !kv_meta.server_id.contains(&updatekv_op.server_id) {
                        kv_meta.server_id.push(updatekv_op.server_id);
                        self.insert_remote_kvcache(kv_meta);
                    }
                }
                tracing::info!("Executed add replica operation for block_id {} on server_id {}.",
                    updatekv_op.block_id, updatekv_op.server_id);
            },
            2 => { // 删除副本操作
                if let Some(mut kv_meta) = self.get_local_kvcache(updatekv_op.block_id) {
                    kv_meta.server_id.retain(|&id| id != updatekv_op.server_id);
                    if kv_meta.server_id.is_empty() {
                        self.remove_local_kvcache(updatekv_op.block_id);
                    } else {
                        self.insert_local_kvcache(kv_meta);
                    }
                }
                if let Some(mut kv_meta) = self.get_remote_kvcache(updatekv_op.block_id){
                    kv_meta.server_id.retain(|&id| id != updatekv_op.server_id);
                    if kv_meta.server_id.is_empty() {
                        self.remove_remote_kvcache(updatekv_op.block_id);
                    } else {
                        self.insert_remote_kvcache(kv_meta);
                    }
                }
                tracing::info!("Executed delete replica operation for block_id {} on server_id {}.",
                    updatekv_op.block_id, updatekv_op.server_id);
            },
            _ => {
                tracing::error!("Unknown operation type: {}", updatekv_op.operation);
            }
        }
        Ok(())
    }
}

// 辅助函数
impl Shared {
    /// 从本地表删除块
    pub fn remove_local_kvcache(&self, block_id: u64) -> Option<KvBlockMeta> {
        self.local_kvcache_table.remove(&block_id).map(|(_, v)| v)
    }

    /// 从远程表删除块
    pub fn remove_remote_kvcache(&self, block_id: u64) -> Option<KvBlockMeta> {
        self.global_kvcache_table.remove(&block_id).map(|(_, v)| v)
    }

    /// 获取本地块元数据
    pub fn get_local_kvcache(&self, block_id: u64) -> Option<KvBlockMeta> {
        self.local_kvcache_table.get(&block_id).map(|v| v.clone())
    }

    /// 获取远程块元数据
    pub fn get_remote_kvcache(&self, block_id: u64) -> Option<KvBlockMeta> {
        self.global_kvcache_table.get(&block_id).map(|v| v.clone())
    }

    /// 清除本地 KV 缓存块元数据
    pub fn clear_local_kvcache(&self) {
        self.local_kvcache_table.clear();
    }

    /// 查找或创建KV块
    /// 返回: (block_id, is_new, is_primary)
    pub fn find_or_create_kv_block(
        &self,
        token_hash: u64,
        tokens: Vec<i32>,
    ) -> u64 {

        // 第一步：通过哈希快速查找所有候选块
        if let Some(block_ids) = self.global_kv_index.get(&token_hash) {
            // 第二步：遍历所有候选块，验证tokens是否完全匹配
            for &block_id in block_ids.value() {
                if let Some(meta) = self.local_kvcache_table.get_mut(&block_id){
                    if meta.tokens_match(&tokens){
                        return block_id;
                    }
                }
                if let Some(mut meta) = self.global_kvcache_table.get_mut(&block_id) {
                    if meta.tokens_match(&tokens) {
                        meta.add_replica(self.config.local_data_server.id);
                        self.local_kvcache_table.insert(block_id, meta.clone());
                        let update_op = UpdateKvOp{
                            block_id: meta.block_id,
                            operation: 1, 
                            server_id: self.config.local_data_server.id,
                        };
                        self.update_kvop_table.insert(meta.block_id, update_op);
                        return block_id;
                    }
                }
            }
            // 哈希相同但tokens不同，这是哈希冲突
            // 继续创建新块，稍后会添加到同一个hash key的列表中
        }

        // 未找到匹配的块，创建新块
        let block_id = self.block_id_generator.next_id();
        let meta = KvBlockMeta {
            block_id,
            token_hash,
            tokens,
            server_id: vec![self.config.local_data_server.id],
        };

        // 插入元数据
        self.insert_local_kvcache(meta);
        
        // 将新的 block_id 添加到索引的 Vec 中（处理哈希冲突）
        self.global_kv_index
            .entry(token_hash)
            .or_insert_with(Vec::new)
            .push(block_id);
        

        block_id
    }

    /// 查找已存在的KV块（不创建）
    pub fn find_kv_block(
        &self,
        token_hash: u64,
        tokens: &[i32],
    ) -> Option<u64> {
        
        // 遍历所有同哈希的块，查找tokens匹配的
        self.global_kv_index.get(&token_hash).and_then(|block_ids| {
            for &block_id in block_ids.value() {
                if let Some(meta) = self.global_kvcache_table.get(&block_id) {
                    if meta.tokens_match(tokens) {
                        return Some(block_id);
                    }
                }
            }
            None
        })
    }
}
