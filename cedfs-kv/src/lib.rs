use dashmap::DashMap;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
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
use crate::concurrency_counter::ConcurrencyCounter;
use crate::http::controller::inference_load_tracker::InferenceLoadTracker;
use crate::operation::transfer_kv::TransferKvOp;

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
pub mod concurrency_counter;
pub mod http;

#[derive(Clone)]
pub struct Shared {
    // 各域间元数据服务器信息
    pub meta_server_collect: Arc<RwLock<Vec<MetaServer>>>,

    // 全局推理节点信息（按域划分）
    pub global_data_server_collect: Arc<DashMap<u32, Vec<DataServer>>>,

    // 本域内推理节点信息
    pub local_data_server_collect: Arc<RwLock<Vec<DataServer>>>,

    // 推理节点ID到元数据服务器ID的映射
    pub data_server_to_meta_server: Arc<DashMap<u32, u32>>,

    // hash生成器
    pub hasher: Arc<TokenHasher>,

    // 本地kv块元数据
    //pub local_kvcache_table: Arc<DashMap<[u8; 32], KvBlockMeta>>,

    // 分词器
    pub tokenizer_manager:Arc<TokenizerManager>,

    // 本地KV块索引
    pub local_kv_index: Arc<RwLock<HashSet<[u8; 32]>>>,

    // 每个 dataserver 持有的本地 KV Cache 块数量
    pub local_kv_cache_block_count: Arc<DashMap<u32, AtomicUsize>>,

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

    // 并发度统计器
    pub concurrency_counter: Arc<ConcurrencyCounter>,

    // 推理实例未完成请求长度总和（用于最小负载调度）
    pub inference_load_tracker: Arc<InferenceLoadTracker>,

    // 近期迁移记录，防止同一 token 在短时间内重复迁移
    pub recent_migrations: Arc<DashMap<[u8; 32], std::time::Instant>>,
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
                    "sha256_cross_language" => HashAlgorithm::Sha256CrossLanguage,
                    _ => {
                        tracing::warn!(
                            "Unknown hash algorithm '{}', using default 'builtin'",
                            config.hash_algorithm.clone()
                        );
                        HashAlgorithm::Builtin
                    },
                };

                let hasher = TokenHasher::new(algorithm, config.unfull_chunk, config.hash_seed);

                // 初始化TokenizerManager并预加载所有配置的tokenizer
                let tokenizer_manager = Arc::new(
                    TokenizerManager::new_with_preload(config.model_tokenizer_map.clone()).await
                );

                // 创建并发度统计器（5秒过期时间）
                let concurrency_counter = Arc::new(ConcurrencyCounter::new(config.transfer_strategy));

                // 启动并发度统计器的定期清理任务（每秒清理一次）
                ConcurrencyCounter::start_cleanup_task(
                    concurrency_counter.clone(),
                    std::time::Duration::from_secs(1)
                );
                
                let shared = Shared {
                    meta_server_collect: meta_servers,
                    global_data_server_collect: Arc::new(DashMap::new()),
                    local_data_server_collect: data_servers,
                    data_server_to_meta_server: Arc::new(DashMap::new()),
                    hasher: Arc::new(hasher),
                    tokenizer_manager,
                    local_kv_index: Arc::new(RwLock::new(HashSet::new())),
                    local_kv_cache_block_count: Arc::new(DashMap::new()),
                    global_kvcache_table: Arc::new(DashMap::new()),
                    update_kvmeta_table: Arc::new(DashMap::new()),
                    update_kvop_table: Arc::new(DashMap::new()),
                    ref_count: Arc::new(RefCount::new()),
                    config: Arc::new(config),
                    concurrency_counter,
                    inference_load_tracker: Arc::new(InferenceLoadTracker::new()),
                    recent_migrations: Arc::new(DashMap::new()),
                };
                tracing::debug!("Loaded config: {:?}", shared.config);
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
    /// 将 token_hash 转为 16 进制字符串（无 0x 前缀）
    fn token_hash_to_hex(token_hash: &[u8; 32]) -> String {
        let mut s = String::with_capacity(64);
        for b in token_hash {
            use std::fmt::Write as _;
            let _ = write!(&mut s, "{:02x}", b);
        }
        s
    }

    /// 插入本地 KV 索引
    ///
    /// # 参数
    /// - `token_hash`: 要插入的块hash
    ///
    /// # 返回
    /// - `true`: 更新已存在的块
    /// - `false`: 插入新块
    pub async fn insert_local_kvcache(&self, token_hash: [u8; 32], server_id: u32) -> bool {
        let mut local_table = self.local_kv_index.write().await;

        if local_table.contains(&token_hash) {
            // 块已存在，不需要更新
            true
        } else {
            // 块不存在，插入新块
            local_table.insert(token_hash);
            self.local_kv_cache_block_count
                .entry(server_id)
                .or_insert_with(|| AtomicUsize::new(0))
                .fetch_add(1, Ordering::Relaxed);
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
                    let inserted = local.insert(hash);
                    if inserted {
                        self.local_kv_cache_block_count
                            .entry(server)
                            .or_insert_with(|| AtomicUsize::new(0))
                            .fetch_add(1, Ordering::Relaxed);
                    }
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
                    let removed = local.remove(&hash);
                    if removed {
                        let counter = self
                            .local_kv_cache_block_count
                            .entry(server)
                            .or_insert_with(|| AtomicUsize::new(0));
                        let _ = counter.fetch_update(
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                            |v| v.checked_sub(1),
                        );
                    }
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

    /// 根据 token_hash 列表返回各块在 global_kvcache_table 中的副本数（server_id.len()），不存在则为 0
    pub fn get_replica_counts(&self, token_hashes: Vec<[u8; 32]>) -> Vec<u32> {
        token_hashes
            .iter()
            .map(|h| {
                self.global_kvcache_table
                    .get(h)
                    .map(|meta| meta.server_id.len() as u32)
                    .unwrap_or(0)
            })
            .collect()
    }

    /// 对指定 model 和 prompt 编码得到 token_hashes（与 SearchKvByPromptsOp::search_one_prompt_one_model 逻辑一致）
    pub async fn get_token_hashes_for_prompt(
        &self,
        model: &str,
        prompt: &str,
    ) -> Option<Vec<[u8; 32]>> {
        let token_list = self
            .tokenizer_manager
            .encode_async(model, prompt)
            .await
            .map_err(|e| {
                tracing::warn!("Failed to encode prompt with model '{}': {}", model, e);
            })
            .ok()?;
        if token_list.is_empty() {
            return None;
        }
        let token_hashes = self
            .hasher
            .hash_tokens_with_blocks_all(&token_list, self.config.block_size)
            .iter()
            .map(|(hash, _offset)| hash.to_u256())
            .collect();
        Some(token_hashes)
    }

    /// 从 KV 元数据中移除指定的 server_id（当 KV cache 不存在时调用）
    /// 
    /// # 参数
    /// - `token_hash`: 块的哈希值
    /// - `server_id`: 要移除的服务器ID
    pub fn remove_server_from_kv_meta(&self, token_hash: [u8; 32], server_id: u32) {
        // 更新 global_kvcache_table，移除对应的 server_id
        let should_remove_block = if let Some(mut meta) = self.global_kvcache_table.get_mut(&token_hash) {
            meta.server_id.retain(|&id| id != server_id);
            meta.server_id.is_empty()
        } else {
            false
        };

        // 如果该块没有任何副本了，从 global_kvcache_table 中移除
        if should_remove_block {
            self.global_kvcache_table.remove(&token_hash);
            tracing::info!(
                "Removed KV block {:?} from global_kvcache_table as it has no replicas",
                token_hash
            );
        }

        // 生成更新操作，用于同步给其他元数据服务器
        let update_op = UpdateKvOp {
            token_hash,
            operation: 2, // 删除副本操作
            server_id,
        };
        self.insert_update_kvop(update_op);

        tracing::debug!(
            "Removed server_id {} from KV metadata for token_hash {:?}",
            server_id,
            token_hash
        );
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
    /// - Vec<(server_id, matched_token_count)>: 每个server_id匹配的token数量（offset之和）
    pub fn search_tokens(&self, token_hash: Vec<[u8; 32]>) -> Vec<(u32, u32)> {
        if token_hash.is_empty() {
            return Vec::new();
        }

        // 记录每个server_id的匹配token总数（offset之和）
        let mut server_matched_token_counts: HashMap<u32, u32> = HashMap::new();
        let mut current_hash = token_hash[0];

        // 从第一个token开始匹配
        for i in 0..token_hash.len() {
            // 查找当前hash对应的KvBlockMeta
            if let Some(meta) = self.global_kvcache_table.get(&current_hash) {
                // 更新每个server的匹配token数量（累加offset）
                for &server_id in &meta.server_id {
                    server_matched_token_counts
                        .entry(server_id)
                        .and_modify(|count| *count += meta.offset)
                        .or_insert(meta.offset);
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
        server_matched_token_counts.into_iter().collect()
    }

    /// 查找指定server_id的匹配token数量
    /// 
    /// # 参数
    /// - `server_id`: 服务器ID
    /// - `token_hash`: 待匹配的token_hash序列
    /// 
    /// # 返回
    /// - 指定server_id匹配的token_hash数量
    pub fn search_tokens_with_server(&self, server_id: u32, token_hash: Vec<[u8; 32]>) -> u32 {
        if token_hash.is_empty() {
            return 0;
        }

        let mut matched_token_count: u32 = 0;
        let mut current_hash = token_hash[0];

        // 从第一个token开始匹配
        for i in 0..token_hash.len() {
            // 查找当前hash对应的KvBlockMeta
            if let Some(meta) = self.global_kvcache_table.get(&current_hash) {
                // 检查该块是否包含指定的server_id
                if meta.server_id.contains(&server_id) {
                    // 找到匹配，累加1
                    matched_token_count += 1;
                    
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

        matched_token_count
    }

    
    /// 创建新的KV块
    /// 
    /// # 参数
    /// - `server_id`: 服务器ID
    /// - `token_hash`: token_hash序列
    /// 
    /// # 返回
    /// - `Some(vec)`: 成功，vec 为本次涉及的各块的 (token_hash, 副本数)
    /// - `None`: token_hash 为空，未做任何修改
    pub fn create_new_kvblock(
        &self,
        server_id: u32,
        offset: Vec<u32>,
        token_hash: Vec<[u8; 32]>,
    ) -> Option<Vec<([u8; 32], u32)>> {
        if token_hash.is_empty() {
            return None;
        }

        // 1. 查找当前server_id的最大前缀匹配长度
        let matched_length = self.search_tokens_with_server(server_id, token_hash.clone()) as usize;
        let mut replica_counts = Vec::with_capacity(token_hash.len() - matched_length);

        // 2. 处理未匹配的token块
        for i in matched_length..token_hash.len() {
            let current_hash = token_hash[i];

            // 确定pre_token: 如果不是第一个token，则为前一个token的hash，否则为全零（表示根块）
            let pre_token = if i > 0 {
                token_hash[i - 1]
            } else {
                [0u8; 32]
            };

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
                    self.local_kv_cache_block_count
                        .entry(server_id)
                        .or_insert_with(|| AtomicUsize::new(0))
                        .fetch_add(1, Ordering::Relaxed);

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

                    let replica_count = existing_meta.server_id.len() as u32;
                    replica_counts.push((current_hash, replica_count));

                    tracing::debug!(
                        "Updated existing KvBlockMeta for token_hash {:?}, added server_id {}, replica_count {}",
                        current_hash,
                        server_id,
                        replica_count
                    );
                } else {
                    // server_id已存在，只需要更新next_tokens
                    for &next_token in &next_tokens {
                        if !existing_meta.next_tokens.contains(&next_token) {
                            existing_meta.next_tokens.push(next_token);
                        }
                    }
                    let replica_count = existing_meta.server_id.len() as u32;
                    replica_counts.push((current_hash, replica_count));
                }
            } else {
                // 不存在相同hash的块，创建新的KvBlockMeta
                let new_meta = KvBlockMeta {
                    token_hash: current_hash,
                    offset: offset[i],
                    pre_token,
                    next_tokens: next_tokens.clone(),
                    server_id: vec![server_id],
                };

                self.local_kv_cache_block_count
                    .entry(server_id)
                    .or_insert_with(|| AtomicUsize::new(0))
                    .fetch_add(1, Ordering::Relaxed);

                // 插入到global_kvcache_table
                self.insert_global_kvcache(new_meta.clone());

                // 插入到update_kvmeta_table
                self.insert_update_kvcache(new_meta);

                replica_counts.push((current_hash, 1));

                tracing::debug!(
                    "Created new KvBlockMeta for token_hash {:?}, server_id {}, replica_count 1",
                    current_hash,
                    server_id
                );
            }
        }

        Some(replica_counts)
    }

    /// 获取指定 token_hash 的并发度
    ///
    /// # 参数
    /// - `token_hash`: 块的哈希值
    ///
    /// # 返回
    /// - 当前有效的并发度计数
    pub fn get_kvcache_concurrency(&self, token_hash: [u8; 32]) -> usize {
        self.concurrency_counter.get_concurrency(token_hash)
    }

    /// 获取所有 KV 块的并发度统计信息
    ///
    /// # 返回
    /// - (total_entries, total_concurrency, max_concurrency):
    ///   总条目数、总并发度、最大并发度
    pub fn get_concurrency_statistics(&self) -> (usize, usize, usize) {
        self.concurrency_counter.get_statistics()
    }

    /// 获取所有 token_hash 的并发度信息
    ///
    /// # 返回
    /// - Vec<(token_hash, concurrency)>: 所有块的并发度信息
    pub fn get_all_kvcache_concurrency(&self) -> Vec<([u8; 32], usize)> {
        self.concurrency_counter.get_all_concurrency()
    }

    pub async fn intra_domain_transfer(
        &self,
        token_hash: &[u8; 32],
    ) -> Option<([u8; 32], u32, DataServer, DataServer)> {
        let global_kvcache_table = &self.global_kvcache_table;
        let local_data_servers = self.local_data_server_collect.read().await;

        let kv_meta = global_kvcache_table.get(token_hash)?;
        let offset = kv_meta.offset;

        let source = local_data_servers
            .iter()
            .find(|ds| kv_meta.server_id.contains(&ds.id))?;
        let target = local_data_servers
            .iter()
            .find(|ds| !kv_meta.server_id.contains(&ds.id))?;

        let hex = Shared::token_hash_to_hex(token_hash);
        tracing::info!(
            "Intra-domain transfer: token_hash 0x{}, replicas: {}, source: {}, target: {}",
            hex,
            kv_meta.server_id.len(),
            source.id,
            target.id
        );
        Some((*token_hash, offset, source.clone(), target.clone()))
    }

    /// 执行单次 KV 块迁移（与 client 中 execute_kv_migration 单条逻辑一致）
    pub async fn execute_single_kv_migration(
        &self,
        (token_hash, offset, src_server, dst_server): ([u8; 32], u32, DataServer, DataServer),
    ) -> anyhow::Result<()> {
        let url = format!("http://{}:{}", src_server.ip, src_server.rpc_port);
        let client = TransferKvOp::new(&url);
        let position = "LocalCPUBackend".to_string();
        let response = client
            .send_transfer_request(
                token_hash,
                position,
                vec![offset],
                dst_server.ip.to_string(),
                dst_server.init_port as i32,
                true,
            )
            .await?;
        if response.status > 0 {
            self.update_kv_meta_after_migration(token_hash, dst_server.id)
                .await;
            tracing::info!(
                "Successfully transferred KV block (concurrency-triggered) from server {} to server {}",
                src_server.id,
                dst_server.id
            );
        } else if response.status == -1 {
            tracing::warn!(
                "KV cache not found on server {}, removing from metadata for token_hash {:?}",
                src_server.id,
                token_hash
            );
            self.remove_server_from_kv_meta(token_hash, src_server.id);
        }
        Ok(())
    }

    /// 迁移完成后更新 KV 元数据（与 client 中 update_kv_meta_after_migration 一致）
    async fn update_kv_meta_after_migration(&self, token_hash: [u8; 32], new_server_id: u32) {
        if let Some(mut meta) = self.global_kvcache_table.get_mut(&token_hash) {
            if !meta.server_id.contains(&new_server_id) {
                meta.server_id.push(new_server_id);
            }
        }
        let _ = self.insert_local_kvcache(token_hash, new_server_id).await;
        self.ref_count.increment_local_ref_count(token_hash, 1);
        let update_op = UpdateKvOp {
            token_hash,
            operation: 1,
            server_id: new_server_id,
        };
        self.insert_update_kvop(update_op);
    }

    /// 若 concurrent_count > replica_count*2 时，可调用此方法：对单个 token_hash 尝试域内迁移并执行
    pub async fn try_trigger_intra_domain_migration_for_token(
        &self,
        token_hash: [u8; 32],
    ) {
        if let Some(transfer_item) = self.intra_domain_transfer(&token_hash).await {
            if let Err(e) = self.execute_single_kv_migration(transfer_item).await {
                let hex = Shared::token_hash_to_hex(&token_hash);
                tracing::warn!(
                    "Concurrency-triggered intra-domain migration failed for token_hash 0x{}: {:?}",
                    hex,
                    e
                );
            }
        }
    }

    /// 增加并发度并更新副本数；若某 token 满足 concurrent_count > replica_count*2 则异步触发域内迁移。
    /// 为避免抖动，对同一 token_hash 在短时间内的重复迁移会被抑制。
    pub fn increment_concurrency_and_maybe_migrate(&self, items: &[([u8; 32], u32)]) {
        let to_migrate = self.concurrency_counter.batch_increment(items);
        let now = std::time::Instant::now();
        let cooldown = std::time::Duration::from_secs(5);

        for token_hash in to_migrate {
            let mut should_spawn = false;

            if let Some(mut entry) = self.recent_migrations.get_mut(&token_hash) {
                // 已有迁移记录，检查是否超过冷却时间
                if now.duration_since(*entry) >= cooldown {
                    *entry = now;
                    should_spawn = true;
                }
            } else {
                // 首次迁移该 token_hash，记录时间并允许迁移
                self.recent_migrations.insert(token_hash, now);
                should_spawn = true;
            }

            if should_spawn {
                let hex = Shared::token_hash_to_hex(&token_hash);
                tracing::info!(
                    "Concurrency-triggered intra-domain migration scheduled for token_hash 0x{}",
                    hex
                );
                let shared = self.clone();
                tokio::spawn(async move {
                    shared
                        .try_trigger_intra_domain_migration_for_token(token_hash)
                        .await;
                });
            }
        }
    }
}


