use dashmap::{DashMap, DashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::info;

use cedfs_proto::kvcache::kv_meta2_data_server::KvMeta2DataServer;
use cedfs_proto::kvcache::kv_meta2_meta_server::KvMeta2MetaServer;

use crate::config::Config;
use crate::hash::{HashAlgorithm, TokenHasher};
use crate::kv_radix::KvRadixTree;
use crate::network::kv_meta2data::KvCacheDataService;
use crate::network::kv_meta2meta::KvCacheMetaService;
use crate::operation::transfer_kv::{
    TransferKvOp, KV_TRANSFER_ALREADY_SATISFIED, KV_TRANSFER_FAILED, KV_TRANSFER_NOT_FOUND,
};
use crate::tokenizers::TokenizerManager;
use crate::transfer::squnence::ActiveSequences;
use crate::types::{BlockHashInfo, DataServer, MetaServer};

pub mod config;
pub mod types;
//pub mod persistence;
//pub mod client;
pub mod convert;
pub mod hash;
pub mod kv_radix;
pub mod network;
pub mod operation;
pub mod tokenizers;
pub mod transfer;

const MAX_PRESSURE_REBALANCE_ROUNDS: usize = 16;

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

    // 分词器
    pub tokenizer_manager: Arc<TokenizerManager>,

    // 全局 KV 元数据索引
    pub kv_radix: Arc<KvRadixTree>,

    // 节点配置
    pub config: Arc<Config>,

    //活跃请求序列
    pub active_squence: Arc<ActiveSequences>,

    pub pressure_migration_in_flight: Arc<DashSet<(u32, u32)>>,
}

#[derive(Debug, Clone, Default)]
pub struct PressureMigrationResult {
    pub rounds: usize,
    pub selected_source_server_id: Option<u32>,
    pub selected_target_server_id: Option<u32>,
    pub candidate_count: usize,
    pub success_count: usize,
    pub fail_count: usize,
    pub status_not_found_count: usize,
    pub skipped_reason: Option<String>,
}

pub struct KVServer {
    pub shared: Shared,
}

struct PressureMigrationGuard {
    in_flight: Arc<DashSet<(u32, u32)>>,
    pair: (u32, u32),
}

impl Drop for PressureMigrationGuard {
    fn drop(&mut self) {
        self.in_flight.remove(&self.pair);
    }
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
                    kv_radix: Arc::new(KvRadixTree::new()),
                    config: Arc::new(config),
                    active_squence,
                    pressure_migration_in_flight: Arc::new(DashSet::new()),
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
        let ip = self.shared.config.local_meta_server.ip;
        let port = self.shared.config.local_meta_server.port;

        // start rpc server
        info!("start kvcache server on: {}", format!("{}:{}", ip, port));
        self.shared.launch_metrics_reporter();

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
    pub fn launch_metrics_reporter(&self) {
        if !self.config.enable_metrics {
            return;
        }

        let metrics_time = self.config.metrics_time;
        let kv_radix = self.kv_radix.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(metrics_time));

            loop {
                interval.tick().await;
                let snapshots = kv_radix.instance_metrics_snapshots();

                if snapshots.is_empty() {
                    tracing::info!("kv metrics: no registered kv blocks");
                    continue;
                }

                for snapshot in snapshots {
                    tracing::info!(
                        "kv metrics: server_id={}, total_heat={}, kv_block_count={}, total_replica_count={}",
                        snapshot.server_id,
                        snapshot.total_heat,
                        snapshot.kv_block_count,
                        snapshot.total_replica_count
                    );
                }
            }
        });
    }

    /// 从 KV 元数据中移除指定的 server_id（当 KV cache 不存在时调用）
    ///
    /// # 参数
    /// - `token_hash`: 块的哈希值
    /// - `server_id`: 要移除的服务器ID
    pub fn remove_server_from_kv_meta(&self, token_hash: [u8; 32], server_id: u32) {
        let before = self.kv_radix.replica_count(token_hash);
        let removed = self.kv_radix.remove_server(token_hash, server_id);
        if removed && before <= 1 {
            tracing::info!(
                "Removed KV block {:?} from kv_radix as it has no replicas",
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
        self.kv_radix.find_matches(blocks)
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

        let store_results = self.kv_radix.store_blocks(server_id, &blocks);
        let mut replica_counts = Vec::with_capacity(store_results.len());

        for result in store_results {
            replica_counts.push((result.seq_hash, result.replica_count));
        }

        Some(replica_counts)
    }

    pub async fn rebalance_by_pressure(&self) -> anyhow::Result<PressureMigrationResult> {
        let beta = self.config.migration_beta;
        let delta = self.config.migration_delta;
        let mut result = PressureMigrationResult::default();

        for _ in 0..MAX_PRESSURE_REBALANCE_ROUNDS {
            let stats = self.kv_radix.pressure_stats();
            let Some(src_server_id) = stats.max_server else {
                result.skipped_reason = Some("no_source_server".to_string());
                return Ok(result);
            };
            let Some(dst_server_id) = stats.min_server else {
                result.skipped_reason = Some("no_target_server".to_string());
                return Ok(result);
            };

            if stats.avg_pressure <= 0.0 {
                result.skipped_reason = Some("zero_average_pressure".to_string());
                return Ok(result);
            }

            let gap = stats.max_pressure - stats.min_pressure;
            if result.rounds == 0 {
                if gap <= beta * stats.avg_pressure {
                    result.skipped_reason = Some("below_beta_threshold".to_string());
                    return Ok(result);
                }
            } else if gap.abs() <= delta * stats.avg_pressure {
                return Ok(result);
            }

            let migration_pair = (src_server_id, dst_server_id);
            if !self.pressure_migration_in_flight.insert(migration_pair) {
                result.skipped_reason = Some("migration_pair_in_flight".to_string());
                tracing::info!(
                    "pressure migration skipped because pair is already in-flight: source_server={}, target_server={}",
                    src_server_id,
                    dst_server_id
                );
                return Ok(result);
            }
            let _migration_guard = PressureMigrationGuard {
                in_flight: self.pressure_migration_in_flight.clone(),
                pair: migration_pair,
            };

            let candidates =
                self.kv_radix
                    .select_replication_candidates(src_server_id, dst_server_id, delta);
            if candidates.is_empty() {
                result.skipped_reason = Some("no_replication_candidates".to_string());
                return Ok(result);
            }

            result.selected_source_server_id = Some(src_server_id);
            result.selected_target_server_id = Some(dst_server_id);
            result.candidate_count += candidates.len();

            let Some(src_server) = self.find_data_server(src_server_id).await else {
                result.skipped_reason = Some("source_server_not_found".to_string());
                return Ok(result);
            };
            let Some(dst_server) = self.find_data_server(dst_server_id).await else {
                result.skipped_reason = Some("target_server_not_found".to_string());
                return Ok(result);
            };

            let mut hashes = Vec::with_capacity(candidates.len());
            let mut offsets = Vec::with_capacity(candidates.len());
            let mut token_ids = Vec::new();
            for candidate in candidates {
                if let Some(snapshot) = self.kv_radix.block_snapshot(candidate.seq_hash) {
                    hashes.push(candidate.seq_hash);
                    offsets.push(snapshot.offset);
                    token_ids.extend(snapshot.tokens);
                }
            }

            if hashes.is_empty() {
                result.skipped_reason = Some("no_known_candidate_meta".to_string());
                return Ok(result);
            }

            let migrated_count = hashes.len();
            match self
                .transfer_pressure_candidates(&src_server, &dst_server, &hashes, offsets, token_ids)
                .await
            {
                Ok(KV_TRANSFER_ALREADY_SATISFIED) => {
                    tracing::info!(
                        "pressure migration already satisfied: source_server={}, target_server={}, batch_size={}, status={}",
                        src_server.id,
                        dst_server.id,
                        migrated_count,
                        KV_TRANSFER_ALREADY_SATISFIED
                    );
                    for token_hash in hashes {
                        self.update_kv_meta_after_migration(token_hash, dst_server.id)
                            .await;
                    }
                    result.success_count += migrated_count;
                },
                Ok(status) if status > 0 => {
                    for token_hash in hashes {
                        self.update_kv_meta_after_migration(token_hash, dst_server.id)
                            .await;
                    }
                    result.success_count += migrated_count;
                },
                Ok(KV_TRANSFER_NOT_FOUND) => {
                    for token_hash in hashes {
                        self.remove_server_from_kv_meta(token_hash, src_server.id);
                    }
                    result.status_not_found_count += migrated_count;
                },
                Ok(KV_TRANSFER_FAILED) => {
                    tracing::warn!(
                        "pressure migration failed with zero satisfied chunks: source_server={}, target_server={}, batch_size={}, status={}",
                        src_server.id,
                        dst_server.id,
                        migrated_count,
                        KV_TRANSFER_FAILED
                    );
                    result.fail_count += migrated_count;
                },
                Ok(status) => {
                    tracing::warn!(
                        "pressure migration returned unknown non-success status: source_server={}, target_server={}, batch_size={}, status={}",
                        src_server.id,
                        dst_server.id,
                        migrated_count,
                        status
                    );
                    result.fail_count += migrated_count;
                },
                Err(e) => {
                    tracing::warn!(
                        "pressure migration failed: source_server={}, target_server={}, batch_size={}, err={:?}",
                        src_server.id,
                        dst_server.id,
                        migrated_count,
                        e
                    );
                    result.fail_count += migrated_count;
                },
            }

            result.rounds += 1;
        }

        Ok(result)
    }

    async fn transfer_pressure_candidates(
        &self,
        src_server: &DataServer,
        dst_server: &DataServer,
        hashes: &[[u8; 32]],
        offsets: Vec<u32>,
        token_ids: Vec<u32>,
    ) -> anyhow::Result<i32> {
        let url = format!("http://{}:{}", src_server.ip, src_server.rpc_port);
        let client = TransferKvOp::new(&url);
        let position = "LocalCPUBackend".to_string();
        let mut concatenated_hash_bytes = Vec::with_capacity(hashes.len() * 32);
        for token_hash in hashes {
            concatenated_hash_bytes.extend_from_slice(token_hash);
        }

        let response = client
            .send_transfer_request(
                concatenated_hash_bytes,
                position,
                offsets,
                token_ids,
                dst_server.ip.to_string(),
                dst_server.init_port as i32,
                true,
            )
            .await?;
        Ok(response.status)
    }

    async fn find_data_server(&self, server_id: u32) -> Option<DataServer> {
        {
            let local_servers = self.local_data_server_collect.read().await;
            if let Some(server) = local_servers.iter().find(|ds| ds.id == server_id) {
                return Some(server.clone());
            }
        }

        for entry in self.global_data_server_collect.iter() {
            if let Some(server) = entry.value().iter().find(|ds| ds.id == server_id) {
                return Some(server.clone());
            }
        }

        None
    }

    /// 迁移完成后更新 KV 元数据（与 client 中 update_kv_meta_after_migration 一致）
    async fn update_kv_meta_after_migration(&self, token_hash: [u8; 32], new_server_id: u32) {
        self.kv_radix.add_server(token_hash, new_server_id);
    }
}
