use dashmap::DashMap;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::info;

use cedfs_proto::kvcache::kv_meta2_data_server::KvMeta2DataServer;
use cedfs_proto::kvcache::kv_meta2_meta_server::KvMeta2MetaServer;

use crate::config::Config;
use crate::hash::{HashAlgorithm, TokenHasher};
use crate::network::kv_meta2data::KvCacheDataService;
use crate::network::kv_meta2meta::KvCacheMetaService;
use crate::operation::transfer_kv::TransferKvOp;
use crate::tokenizers::TokenizerManager;
use crate::transfer::squnence::ActiveSequences;
use crate::types::{BlockHashInfo, DataServer, KvMetaIndex, MetaServer};

pub mod config;
pub mod types;
//pub mod persistence;
//pub mod client;
pub mod convert;
pub mod hash;
pub mod network;
pub mod operation;
pub mod tokenizers;
pub mod transfer;

pub const PENDING_MIGRATION_TTL_SECS: u64 = 60;

#[derive(Debug, Clone)]
pub struct PendingMigrationTask {
    pub source_server_id: u32,
    pub eligible_blocks: Vec<([u8; 32], u64)>,
    pub token_ids: Vec<u32>,
    pub created_at: Instant,
    pub ttl: Duration,
}

impl PendingMigrationTask {
    pub fn new(
        source_server_id: u32,
        eligible_blocks: Vec<([u8; 32], u64)>,
        token_ids: Vec<u32>,
    ) -> Self {
        Self {
            source_server_id,
            eligible_blocks,
            token_ids,
            created_at: Instant::now(),
            ttl: Duration::from_secs(PENDING_MIGRATION_TTL_SECS),
        }
    }

    pub fn is_expired(&self) -> bool {
        self.created_at.elapsed() > self.ttl
    }
}

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
    pub tokenizer_manager: Arc<TokenizerManager>,

    // 本地KV块索引
    pub local_kv_index: Arc<RwLock<HashSet<[u8; 32]>>>,

    // 每个 dataserver 持有的本地 KV Cache 块数量
    pub local_kv_cache_block_count: Arc<DashMap<u32, AtomicUsize>>,

    // 全局 KV 元数据索引
    pub kv_meta_index: Arc<KvMetaIndex>,

    // 节点配置
    pub config: Arc<Config>,

    // 近期迁移记录，防止同一 token 在短时间内重复迁移
    pub recent_migrations: Arc<DashMap<[u8; 32], std::time::Instant>>,

    //活跃请求序列
    pub active_squence: Arc<ActiveSequences>,

    // 迁移目标节点轮转索引（用于在候选目标中做 round-robin）
    pub migration_target_rr_index: Arc<AtomicUsize>,

    // 待执行的迁移任务（按 request_id 缓存，延后到 request_end 执行）
    pub pending_migrations: Arc<DashMap<String, PendingMigrationTask>>,
}

#[derive(Debug, Clone, Default)]
pub struct HashSeqMigrationResult {
    pub candidate_count: usize,
    pub selected_target_server_id: Option<u32>,
    pub total_hash_count: usize,
    pub known_meta_count: usize,
    pub missing_meta_count: usize,
    pub to_migrate_count: usize,
    pub success_count: usize,
    pub fail_count: usize,
    pub status_not_found_count: usize,
    pub skipped_reason: Option<String>,
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

                let hasher = TokenHasher::new(
                    algorithm,
                    config.unfull_chunk,
                    config.hash_seed,
                    config.python_hash_seed.clone(),
                )?;

                // 初始化TokenizerManager并预加载所有配置的tokenizer
                let tokenizer_manager = Arc::new(
                    TokenizerManager::new_with_preload(config.model_tokenizer_map.clone()).await,
                );

                let active_squence = Arc::new(ActiveSequences::new(config.block_size));

                let shared = Shared {
                    meta_server_collect: meta_servers,
                    global_data_server_collect: Arc::new(DashMap::new()),
                    local_data_server_collect: data_servers,
                    data_server_to_meta_server: Arc::new(DashMap::new()),
                    hasher: Arc::new(hasher),
                    tokenizer_manager,
                    local_kv_index: Arc::new(RwLock::new(HashSet::new())),
                    local_kv_cache_block_count: Arc::new(DashMap::new()),
                    kv_meta_index: Arc::new(KvMetaIndex::new()),
                    config: Arc::new(config),
                    recent_migrations: Arc::new(DashMap::new()),
                    active_squence,
                    migration_target_rr_index: Arc::new(AtomicUsize::new(0)),
                    pending_migrations: Arc::new(DashMap::new()),
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
    pub fn upsert_pending_migration_task(&self, request_id: String, task: PendingMigrationTask) {
        self.pending_migrations.insert(request_id, task);
    }

    pub fn pop_pending_migration_task(&self, request_id: &str) -> Option<PendingMigrationTask> {
        self.pending_migrations
            .remove(request_id)
            .map(|(_request_id, task)| task)
    }

    pub fn cleanup_expired_pending_migrations(&self) -> usize {
        let expired_request_ids: Vec<String> = self
            .pending_migrations
            .iter()
            .filter_map(|entry| {
                if entry.value().is_expired() {
                    Some(entry.key().clone())
                } else {
                    None
                }
            })
            .collect();

        let mut removed = 0usize;
        for request_id in expired_request_ids {
            if self.pending_migrations.remove(&request_id).is_some() {
                removed += 1;
            }
        }
        removed
    }

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

    /// 从 KV 元数据中移除指定的 server_id（当 KV cache 不存在时调用）
    ///
    /// # 参数
    /// - `token_hash`: 块的哈希值
    /// - `server_id`: 要移除的服务器ID
    pub fn remove_server_from_kv_meta(&self, token_hash: [u8; 32], server_id: u32) {
        let before = self.kv_meta_index.replica_count(token_hash);
        let removed = self.kv_meta_index.remove_server(token_hash, server_id);
        if removed && before <= 1 {
            tracing::info!(
                "Removed KV block {:?} from kv_meta_index as it has no replicas",
                token_hash
            );
        }

        tracing::debug!(
            "Removed server_id {} from KV metadata for token_hash {:?}",
            server_id,
            token_hash
        );
    }

    pub fn search_tokens_by_infos(&self, blocks: &[BlockHashInfo]) -> Vec<(u32, u32)> {
        self.kv_meta_index.find_matches(blocks)
    }

    /// 创建新的 KV 块。
    pub fn create_new_kvblock(
        &self,
        server_id: u32,
        blocks: Vec<BlockHashInfo>,
    ) -> Option<Vec<([u8; 32], u32)>> {
        if blocks.is_empty() {
            return None;
        }

        let store_results = self.kv_meta_index.store_blocks(server_id, &blocks);
        let mut replica_counts = Vec::with_capacity(store_results.len());

        for result in store_results {
            if result.server_added {
                self.local_kv_cache_block_count
                    .entry(server_id)
                    .or_insert_with(|| AtomicUsize::new(0))
                    .fetch_add(1, Ordering::Relaxed);
            }
            replica_counts.push((result.seq_hash, result.replica_count));
        }

        Some(replica_counts)
    }

    pub async fn intra_domain_transfer(
        &self,
        token_hash: &[u8; 32],
    ) -> Option<([u8; 32], u32, DataServer, DataServer)> {
        let local_data_servers = self.local_data_server_collect.read().await;

        let snapshot = self.kv_meta_index.get_block(*token_hash)?;
        let offset = snapshot.meta.offset;

        let source = local_data_servers
            .iter()
            .find(|ds| snapshot.servers.contains(&ds.id))?;
        let target = local_data_servers
            .iter()
            .find(|ds| !snapshot.servers.contains(&ds.id))?;

        let hex = Shared::token_hash_to_hex(token_hash);
        tracing::info!(
            "Intra-domain transfer: token_hash 0x{}, replicas: {}, source: {}, target: {}",
            hex,
            snapshot.servers.len(),
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
                token_hash.to_vec(),
                position,
                vec![offset],
                vec![],
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

    /// 按 hash 序列迁移 KV 块：
    /// - 源节点由调用方指定
    /// - 目标节点从 global_data_server_collect 候选中轮转选择
    /// - 输入为按顺序的 (hash, concurrency)
    /// - 顺序门控：遇到第一个 concurrency < replica_count*2 后，后续块全部不触发迁移
    pub async fn migrate_hash_seq_with_rr_target(
        &self,
        source_server: &DataServer,
        hash_seq_with_concurrency: &[([u8; 32], u64)],
        token_ids: &[u32],
    ) -> anyhow::Result<HashSeqMigrationResult> {
        let mut result = HashSeqMigrationResult {
            total_hash_count: hash_seq_with_concurrency.len(),
            ..HashSeqMigrationResult::default()
        };

        if hash_seq_with_concurrency.is_empty() {
            result.skipped_reason = Some("empty_hash_seq".to_string());
            return Ok(result);
        }

        // 1) 构建候选目标（排除源节点、按 id 去重）
        let mut candidate_targets = Vec::new();
        let mut seen_target_ids = HashSet::new();
        for entry in self.global_data_server_collect.iter() {
            for ds in entry.value().iter() {
                if ds.id == source_server.id {
                    continue;
                }
                if seen_target_ids.insert(ds.id) {
                    candidate_targets.push(ds.clone());
                }
            }
        }
        result.candidate_count = candidate_targets.len();
        if candidate_targets.is_empty() {
            result.skipped_reason = Some("no_target_candidates".to_string());
            return Ok(result);
        }

        // 2) 先解析可迁移元数据（hash -> offset + replica_count + concurrency）
        let mut known_hash_with_state: Vec<([u8; 32], u32, u64, u64)> = Vec::new();
        for (token_hash, concurrency) in hash_seq_with_concurrency {
            if let Some(snapshot) = self.kv_meta_index.get_block(*token_hash) {
                known_hash_with_state.push((
                    *token_hash,
                    snapshot.meta.offset,
                    *concurrency,
                    snapshot.servers.len() as u64,
                ));
            } else {
                result.missing_meta_count += 1;
            }
        }
        result.known_meta_count = known_hash_with_state.len();
        if known_hash_with_state.is_empty() {
            result.skipped_reason = Some("no_known_meta".to_string());
            return Ok(result);
        }

        // 3) 顺序门控：第一个 concurrency < replica_count*2 之后全部截断
        let mut gated_hashes: Vec<([u8; 32], u32, u64)> = Vec::new();
        for (token_hash, offset, concurrency, _replica_count) in known_hash_with_state {
            // if concurrency < replica_count.saturating_mul(2) {
            //     break;
            // }
            if concurrency < 2 {
                break;
            }
            gated_hashes.push((token_hash, offset, concurrency));
        }
        if gated_hashes.is_empty() {
            result.skipped_reason = Some("below_replica_concurrency_gate".to_string());
            return Ok(result);
        }

        // 4) 从轮转起点开始环形扫描，选择第一个“未完全匹配 hash 序列”的目标
        //    前缀缓存语义：一旦首个块未命中，后续块默认均未命中
        let start = self
            .migration_target_rr_index
            .fetch_add(1, Ordering::Relaxed)
            % candidate_targets.len();
        let mut selected_target: Option<DataServer> = None;

        for step in 0..candidate_targets.len() {
            let idx = (start + step) % candidate_targets.len();
            let target = &candidate_targets[idx];
            let mut first_miss_at: Option<usize> = None;
            for (i, (token_hash, _, _)) in gated_hashes.iter().enumerate() {
                let exists_on_target = self.kv_meta_index.contains_server(*token_hash, target.id);
                if !exists_on_target {
                    first_miss_at = Some(i);
                    break;
                }
            }
            let missing = if let Some(first_idx) = first_miss_at {
                gated_hashes[first_idx..].to_vec()
            } else {
                Vec::new()
            };

            if !missing.is_empty() {
                tracing::debug!(
                    "Prefix-miss target selected: server_id={}, first_miss_index={}, migrate_count={}",
                    target.id,
                    first_miss_at.unwrap_or(0),
                    missing.len()
                );
                selected_target = Some(target.clone());
                break;
            }
        }

        let Some(target_server) = selected_target else {
            result.skipped_reason = Some("all_targets_fully_matched".to_string());
            return Ok(result);
        };
        result.selected_target_server_id = Some(target_server.id);

        // 5) 调用 transfer_kv：不再只打包 miss 块，而是打包整条序列（可解析 offset 的全部块）
        //    miss 判定与具体拷贝策略交由实际迁移节点处理
        let url = format!("http://{}:{}", source_server.ip, source_server.rpc_port);
        let client = TransferKvOp::new(&url);
        let position = "LocalCPUBackend".to_string();

        let all_request_hashes: Vec<[u8; 32]> = gated_hashes
            .iter()
            .map(|(token_hash, _, _)| *token_hash)
            .collect();
        let mut concatenated_hash_bytes: Vec<u8> =
            Vec::with_capacity(all_request_hashes.len() * 32);
        for token_hash in &all_request_hashes {
            concatenated_hash_bytes.extend_from_slice(token_hash);
        }
        let all_request_offsets: Vec<u32> =
            gated_hashes.iter().map(|(_, offset, _)| *offset).collect();
        result.to_migrate_count = all_request_hashes.len();

        match client
            .send_transfer_request(
                concatenated_hash_bytes,
                position,
                all_request_offsets,
                token_ids.to_vec(),
                target_server.ip.to_string(),
                target_server.init_port as i32,
                true,
            )
            .await
        {
            Ok(response) => {
                if response.status > 0 {
                    result.success_count += all_request_hashes.len();
                    let now = std::time::Instant::now();
                    for token_hash in all_request_hashes {
                        self.update_kv_meta_after_migration(token_hash, target_server.id)
                            .await;
                        self.recent_migrations.insert(token_hash, now);
                    }
                } else if response.status == -1 {
                    result.status_not_found_count += all_request_hashes.len();
                    for token_hash in all_request_hashes {
                        self.remove_server_from_kv_meta(token_hash, source_server.id);
                    }
                } else {
                    result.fail_count += all_request_hashes.len();
                }
            },
            Err(e) => {
                result.fail_count += all_request_hashes.len();
                tracing::warn!(
                    "migrate_hash_seq_with_rr_target batch request failed: source_server={}, target_server={}, batch_size={}, err={:?}",
                    source_server.id,
                    target_server.id,
                    all_request_hashes.len(),
                    e
                );
            },
        }

        Ok(result)
    }

    /// 迁移完成后更新 KV 元数据（与 client 中 update_kv_meta_after_migration 一致）
    async fn update_kv_meta_after_migration(&self, token_hash: [u8; 32], new_server_id: u32) {
        self.kv_meta_index.add_server(token_hash, new_server_id);
        let _ = self.insert_local_kvcache(token_hash, new_server_id).await;
    }

    /// 若 concurrent_count > replica_count*2 时，可调用此方法：对单个 token_hash 尝试域内迁移并执行
    pub async fn try_trigger_intra_domain_migration_for_token(&self, token_hash: [u8; 32]) {
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
}
