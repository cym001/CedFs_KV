use dashmap::DashMap;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

use cedfs_proto::kvcache::kv_meta2_data_server::KvMeta2DataServer;
use cedfs_proto::kvcache::kv_meta2_meta_server::KvMeta2MetaServer;

use crate::config::Config;
use crate::hash::{HashAlgorithm, TokenHasher};
use crate::network::kv_meta2data::KvCacheDataService;
use crate::network::kv_meta2meta::KvCacheMetaService;
use crate::types::{DataServer, KvBlockMeta, MetaServer, RefCount, UpdateKvOp};
use crate::tokenizers::TokenizerManager;

pub mod config;
pub mod types;
//pub mod persistence;
pub mod client;
pub mod convert;
pub mod hash;
pub mod network;
pub mod operation;
pub mod tokenizers;
pub mod transfer;

#[derive(Clone)]
pub struct Shared {
    // 本域内推理节点信息
    pub data_server_collect: Arc<RwLock<Vec<DataServer>>>,

    // 各域间元数据服务器信息
    pub meta_server_collect: Arc<RwLock<Vec<MetaServer>>>,

    // hash生成器
    pub hasher: Arc<TokenHasher>,

    // 本地kv块元数据
    //pub local_kvcache_table: Arc<DashMap<[u8; 32], KvBlockMeta>>,

    // 分词器
    pub tokenizer_manager:Arc<TokenizerManager>,

    // 本地KV块索引
    pub local_kv_index: Arc<RwLock<HashSet<[u8; 32]>>>,

    // 全局kv块元数据
    pub global_kvcache_table: Arc<DashMap<[u8; 32], KvBlockMeta>>,

    // 待更新的kvmeta
    pub update_kvmeta_table: Arc<DashMap<[u8; 32], KvBlockMeta>>,

    // 待更新的kvmeta副本操作
    pub update_kvop_table: Arc<DashMap<[u8; 32], UpdateKvOp>>,

    // 引用计数
    pub ref_count: Arc<RefCount>,

    // 节点配置
    pub config: Arc<Config>,
}
pub struct KVServer {
    pub shared: Shared,
}

//todo() 只需要为推理节点添加路由，后续修改
impl KVServer {
    pub async fn new(config_path: PathBuf) -> anyhow::Result<Self> {
        match Config::build_with_config(config_path) {
            Ok(config) => {
                let meta_servers = Arc::new(RwLock::new(Vec::new()));
                meta_servers
                    .write()
                    .await
                    .push(config.local_meta_server.clone());

                match config.load_remote_meta_from_config() {
                    Ok(remote_servers) => {
                        meta_servers.write().await.extend(remote_servers);
                        tracing::info!(
                            "Loaded remote meta servers from config: {:?}",
                            *meta_servers.read().await
                        );
                    },
                    Err(e) => {
                        tracing::error!("Failed to load remote meta servers from config: {}", e);
                        return Err(anyhow::anyhow!(
                            "Failed to load remote meta servers from config: {}",
                            e
                        ));
                    },
                }
                let data_servers = Arc::new(RwLock::new(Vec::new()));

                let algorithm = match config.hash_algorithm.clone().as_str() {
                    "builtin" => HashAlgorithm::Builtin,
                    "sha256" => HashAlgorithm::Sha256,
                    "sha256_cbor" => HashAlgorithm::Sha256Cbor,
                    _ => {
                        tracing::warn!(
                            "Unknown hash algorithm '{}', using default 'builtin'",
                            config.hash_algorithm.clone()
                        );
                        HashAlgorithm::Builtin
                    },
                };

                let hasher = TokenHasher::new(algorithm, config.unfull_chunk);

                // 初始化TokenizerManager并预加载所有配置的tokenizer
                let tokenizer_manager = Arc::new(
                    TokenizerManager::new_with_preload(config.model_tokenizer_map.clone()).await
                );
                
                let shared = Shared {
                    data_server_collect: data_servers,
                    meta_server_collect: meta_servers,
                    hasher: Arc::new(hasher),
                    tokenizer_manager,
                    local_kv_index: Arc::new(RwLock::new(HashSet::new())),
                    global_kvcache_table: Arc::new(DashMap::new()),
                    update_kvmeta_table: Arc::new(DashMap::new()),
                    update_kvop_table: Arc::new(DashMap::new()),
                    ref_count: Arc::new(RefCount::new()),
                    config: Arc::new(config),
                };
                tracing::info!("Loaded config: {:?}", shared.config);
                Ok(KVServer { shared })
            },
            Err(e) => {
                tracing::error!("Failed to load config: {}", e);
                Err(anyhow::anyhow!("Failed to load config: {}", e))
            },
        }
    }

    pub async fn serve(self) {
        let ip = self.shared.config.local_meta_server.ip.clone();
        let port = self.shared.config.local_meta_server.port;

        // start rpc server
        info!("start kvcache server on: {}", format!("{}:{}", ip, port));

        let meta_server = KvMeta2MetaServer::new(KvCacheMetaService {
            shared: self.shared.clone(),
        });
        let data_server = KvMeta2DataServer::new(KvCacheDataService {
            shared: self.shared.clone(),
        });

        tonic::transport::Server::builder()
            .add_service(meta_server)
            .add_service(data_server)
            .serve(format!("{}:{}", ip, port).parse().unwrap())
            .await
            .unwrap();
    }
}

impl Shared {
    /// 插入本地 KV 索引
    ///
    /// # 参数
    /// - `token_hash`: 要插入的块hash
    ///
    /// # 返回
    /// - `true`: 更新已存在的块
    /// - `false`: 插入新块
    pub async fn insert_local_kvcache(&self, token_hash: [u8; 32]) -> bool {
        let mut local_table = self.local_kv_index.write().await;

        if local_table.contains(&token_hash) {
            // 块已存在，不需要更新
            true
        } else {
            // 块不存在，插入新块
            local_table.insert(token_hash);
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
    pub fn insert_global_kvcache(&self, block_meta: KvBlockMeta) -> bool {
        let token_hash = block_meta.token_hash;

        if let Some(mut existing) = self.global_kvcache_table.get_mut(&token_hash) {
            // 块已存在,更新元数据
            //保留server_socket的交集，若为空则删除该块
            let existing_servers = &existing.server_id;
            let new_servers = &block_meta.server_id;
            let intersection: Vec<u32> = existing_servers
                .iter()
                .filter(|s| new_servers.contains(s))
                .cloned()
                .collect();
            if intersection.is_empty() {
                self.global_kvcache_table.remove(&token_hash);
                self.ref_count.remove_global_ref_count(token_hash);
                return false;
            }
            let mut block_meta = block_meta.clone();
            block_meta.server_id = intersection;
            *existing = block_meta;
            true
        } else {
            // 块不存在,插入新块
            self.global_kvcache_table.insert(token_hash, block_meta);
            false
        }
    }

    /// 批量插入或更新远程 KV 缓存块
    pub fn batch_insert_global_kvcache(&self, blocks: Vec<KvBlockMeta>) -> (usize, usize) {
        let mut updated = 0;
        let mut inserted = 0;

        for block in blocks {
            if self.insert_global_kvcache(block) {
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
        let token_hash = block_meta.token_hash;
        if let Some(mut existing) = self.update_kvmeta_table.get_mut(&token_hash) {
            // 块已存在,更新元数据
            *existing = block_meta;
            true
        } else {
            // 块不存在,插入新块
            self.update_kvmeta_table.insert(token_hash, block_meta);
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
        let token_hash = updatekv_op.token_hash;
        if let Some(mut existing) = self.update_kvop_table.get_mut(&token_hash) {
            // 块已存在,更新元数据
            *existing = updatekv_op;
            true
        } else {
            // 块不存在,插入新块
            self.update_kvop_table.insert(token_hash, updatekv_op);
            false
        }
    }

    pub async fn execute_update_kvop(&self, op: UpdateKvOp) -> anyhow::Result<()> {
        let hash = op.token_hash;
        let server = op.server_id;

        match op.operation {
            // 添加副本
            1 => {
                {
                    let mut local = self.local_kv_index.write().await;
                    local.insert(hash);
                }

                if let Some(mut meta) = self.get_global_kvcache(hash) {
                    if !meta.server_id.contains(&server) {
                        meta.server_id.push(server);
                    }
                    self.insert_global_kvcache(meta);
                }

                tracing::info!(
                    "Executed add replica operation for token_hash {:?} on server_id {}.",
                    hash,
                    server
                );
            },

            // 删除副本
            2 => {
                {
                    let mut local = self.local_kv_index.write().await;
                    local.remove(&hash);
                }

                if let Some(mut meta) = self.get_global_kvcache(hash) {
                    meta.server_id.retain(|&id| id != server);

                    if meta.server_id.is_empty() {
                        self.remove_global_kvcache(hash);
                    } else {
                        self.insert_global_kvcache(meta);
                    }
                }

                tracing::info!(
                    "Executed delete replica operation for token_hash {:?} on server_id {}.",
                    hash,
                    server
                );
            },

            // 未知操作
            _ => {
                tracing::error!("Unknown operation type: {}", op.operation);
            },
        }

        Ok(())
    }
}

// 辅助函数
impl Shared {
    /// 从远程表删除块
    pub fn remove_global_kvcache(&self, token_hash: [u8; 32]) -> Option<KvBlockMeta> {
        self.global_kvcache_table
            .remove(&token_hash)
            .map(|(_, v)| v)
    }

    /// 获取远程块元数据
    pub fn get_global_kvcache(&self, token_hash: [u8; 32]) -> Option<KvBlockMeta> {
        self.global_kvcache_table
            .get(&token_hash)
            .map(|v| v.clone())
    }

    // /// 查找或创建KV块
    // /// 返回: (token_hash, is_new, is_primary)
    // pub fn find_or_create_kv_block(
    //     &self,
    //     server_id: u32,
    //     //model_hash: i64,
    //     token_hash: [u8; 32],
    // ) -> [u8; 32] {
    //     // let key = KvBlockKey::new(model_hash, token_hash);
    //     let key = token_hash;

    //     // 第一步：通过哈希快速查找所有候选块
    //     if let Some(token_hashs) = self.global_kv_index.get(&key) {
    //         // 第二步：遍历所有候选块，验证tokens是否完全匹配
    //         for &token_hash in token_hashs.value() {
    //             if let Some(meta) = self.local_kvcache_table.get_mut(&token_hash){
    //                 if meta.tokens_match(&tokens){
    //                     return token_hash;
    //                 }
    //             }
    //             if let Some(mut meta) = self.global_kvcache_table.get_mut(&token_hash) {
    //                 if meta.tokens_match(&tokens) {
    //                     meta.add_replica(server_id);
    //                     self.local_kvcache_table.insert(token_hash, meta.clone());
    //                     let update_op = UpdateKvOp{
    //                         token_hash: meta.token_hash,
    //                         operation: 1,
    //                         server_id: server_id,
    //                     };
    //                     self.update_kvop_table.insert(meta.token_hash, update_op);
    //                     self.local_kvcache_table.insert(token_hash, meta.clone());
    //                     return token_hash;
    //                 }
    //             }
    //         }
    //         // 哈希相同但tokens不同，这是哈希冲突
    //         // 继续创建新块，稍后会添加到同一个hash key的列表中
    //     }

    //     // 未找到匹配的块，创建新块
    //     let token_hash = self.token_hash_generator.next_id();
    //     let meta = KvBlockMeta {
    //         token_hash,
    //         token_hash,
    //         //model_hash,
    //         tokens,
    //         server_id: vec![server_id],
    //     };

    //     // 插入元数据
    //     self.insert_local_kvcache(meta.clone());
    //     self.insert_remote_kvcache(meta.clone());
    //     self.insert_update_kvcache(meta);

    //     // 将新的 token_hash 添加到索引的 Vec 中（处理哈希冲突）
    //     self.global_kv_index
    //         .entry(key)
    //         .or_insert_with(Vec::new)
    //         .push(token_hash);

    //     token_hash
    // }

    /// 从global_kvcache_table中查找token_hash序列的最大前缀匹配
    ///
    /// # 参数
    /// - `token_hash`: 待匹配的token_hash序列
    ///
    /// # 返回
    /// - Vec<(server_id, matched_length)>: 每个server_id的最长前缀匹配长度
    pub fn search_tokens(&self, token_hash: Vec<[u8; 32]>) -> Vec<(u32, u32)> {
        if token_hash.is_empty() {
            return Vec::new();
        }

        // 记录每个server_id的最长匹配长度
        let mut server_matched_lengths: HashMap<u32, u32> = HashMap::new();
        let mut current_hash = token_hash[0];

        // 从第一个token开始匹配
        for i in 0..token_hash.len() {
            // 查找当前hash对应的KvBlockMeta
            if let Some(meta) = self.global_kvcache_table.get(&current_hash) {
                // 更新每个server的匹配长度
                let current_length = (i + 1) as u32;
                for &server_id in &meta.server_id {
                    server_matched_lengths.insert(server_id, current_length);
                }

                // 如果还有下一个token需要匹配
                if i + 1 < token_hash.len() {
                    let next_hash = token_hash[i + 1];

                    // 检查next_tokens中是否包含下一个hash
                    if meta.next_tokens.contains(&next_hash) {
                        // 继续匹配下一个token
                        current_hash = next_hash;
                    } else {
                        // next_tokens中不包含下一个hash，停止匹配
                        break;
                    }
                } else {
                    // 已经是最后一个token了
                    break;
                }
            } else {
                // 当前hash不存在于global_kvcache_table中，停止匹配
                break;
            }
        }

        // 转换为Vec返回
        server_matched_lengths.into_iter().collect()
    }

    /// 查找指定server_id的最大前缀匹配长度
    /// 
    /// # 参数
    /// - `server_id`: 服务器ID
    /// - `token_hash`: 待匹配的token_hash序列
    /// 
    /// # 返回
    /// - 指定server_id的最大前缀匹配长度
    pub fn search_tokens_with_server(&self, server_id: u32, token_hash: Vec<[u8; 32]>) -> u32 {
        if token_hash.is_empty() {
            return 0;
        }

        let mut matched_length: u32 = 0;
        let mut current_hash = token_hash[0];

        // 从第一个token开始匹配
        for i in 0..token_hash.len() {
            // 查找当前hash对应的KvBlockMeta
            if let Some(meta) = self.global_kvcache_table.get(&current_hash) {
                // 检查该块是否包含指定的server_id
                if meta.server_id.contains(&server_id) {
                    // 找到匹配，更新匹配长度
                    matched_length = (i + 1) as u32;
                    
                    // 如果还有下一个token需要匹配
                    if i + 1 < token_hash.len() {
                        let next_hash = token_hash[i + 1];
                        
                        // 检查next_tokens中是否包含下一个hash
                        if meta.next_tokens.contains(&next_hash) {
                            // 继续匹配下一个token
                            current_hash = next_hash;
                        } else {
                            // next_tokens中不包含下一个hash，停止匹配
                            break;
                        }
                    } else {
                        // 已经是最后一个token了
                        break;
                    }
                } else {
                    // 当前块不包含指定的server_id，停止匹配
                    break;
                }
            } else {
                // 当前hash不存在于global_kvcache_table中，停止匹配
                break;
            }
        }

        matched_length
    }

    
    /// 创建新的KV块
    /// 
    /// # 参数
    /// - `server_id`: 服务器ID
    /// - `token_hash`: token_hash序列
    /// 
    /// # 返回
    /// - `true`: 成功创建或更新
    /// - `false`: 失败
    pub fn create_new_kvblock(&self, server_id: u32, offset: u32, token_hash: Vec<[u8; 32]>) -> bool {
        if token_hash.is_empty() {
            return false;
        }

        // 1. 查找当前server_id的最大前缀匹配长度
        let matched_length = self.search_tokens_with_server(server_id, token_hash.clone()) as usize;

        // 2. 处理未匹配的token块
        for i in matched_length..token_hash.len() {
            let current_hash = token_hash[i];
            
            // 确定next_tokens: 如果不是最后一个token，则包含下一个token的hash
            let next_tokens = if i + 1 < token_hash.len() {
                vec![token_hash[i + 1]]
            } else {
                Vec::new()
            };

            // 检查全局是否存在相同hash的块
            if let Some(mut existing_meta) = self.global_kvcache_table.get_mut(&current_hash) {
                // 存在相同hash的块，检查server_id
                if !existing_meta.server_id.contains(&server_id) {
                    // server_id不同，需要更新
                    existing_meta.server_id.push(server_id);
                    
                    // 合并next_tokens（去重）
                    for &next_token in &next_tokens {
                        if !existing_meta.next_tokens.contains(&next_token) {
                            existing_meta.next_tokens.push(next_token);
                        }
                    }
                    
                    // 生成UpdateKvOp
                    let update_op = UpdateKvOp {
                        token_hash: current_hash,
                        operation: 1, // 添加副本操作
                        server_id,
                    };
                    self.insert_update_kvop(update_op);
                    
                    tracing::debug!(
                        "Updated existing KvBlockMeta for token_hash {:?}, added server_id {}",
                        current_hash,
                        server_id
                    );
                } else {
                    // server_id已存在，只需要更新next_tokens
                    for &next_token in &next_tokens {
                        if !existing_meta.next_tokens.contains(&next_token) {
                            existing_meta.next_tokens.push(next_token);
                        }
                    }
                }
            } else {
                // 不存在相同hash的块，创建新的KvBlockMeta
                let new_meta = KvBlockMeta {
                    token_hash: current_hash,
                    offset: offset,
                    next_tokens: next_tokens.clone(),
                    server_id: vec![server_id],
                };
                
                // 插入到global_kvcache_table
                self.insert_global_kvcache(new_meta.clone());
                
                // 插入到update_kvmeta_table
                self.insert_update_kvcache(new_meta);
                
                tracing::debug!(
                    "Created new KvBlockMeta for token_hash {:?}, server_id {}",
                    current_hash,
                    server_id
                );
            }
        }

        true
    }

}
