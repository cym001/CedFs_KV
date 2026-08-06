use dashmap::{DashMap, DashSet};
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::sync::Semaphore;
use tracing::info;

use chrono::Utc;
use cedfs_proto::kvcache::kv_meta2_data_server::KvMeta2DataServer;
use cedfs_proto::kvcache::kv_meta2_meta_server::KvMeta2MetaServer;
use cedfs_proto::kvcache_v2::kv_meta2_data_v2_server::KvMeta2DataV2Server;
use cedfs_proto::lmcache_v2::{TransferKvV2Request, TransferKvV2Response};

use crate::config::{Config, ProtocolMode};
use crate::hash::{HashAlgorithm, TokenHasher};
use crate::kv_radix::KvRadixTree;
use crate::metrics::{MetricsCollector, MigrationSelectionRecord, ReplicationRpcRecord};
use crate::network::kv_meta2data::KvCacheDataService;
use crate::network::kv_meta2data_v2::KvCacheDataServiceV2;
use crate::network::kv_meta2meta::KvCacheMetaService;
use crate::operation::transfer_kv::{
    TransferKvOp, TransferV2Limits, KV_TRANSFER_ALREADY_SATISFIED, KV_TRANSFER_FAILED,
    KV_TRANSFER_NOT_FOUND,
};
use crate::tokenizers::TokenizerManager;
use crate::transfer::squnence::ActiveSequences;
use crate::types::{BlockHashInfo, DataServer, MetaServer};

pub mod config;
pub mod types;
pub mod convert;
pub mod hash;
pub mod kv_radix;
pub mod metrics;
pub mod network;
pub mod operation;
pub mod state;
pub mod tokenizers;
pub mod transfer;

const MAX_PRESSURE_REBALANCE_ROUNDS: usize = 16;
const KV_CACHE_BYTES_PER_TOKEN: f64 = 96.0 * 1024.0;
const BITS_PER_BYTE: f64 = 8.0;
const BITS_PER_MEGABIT: f64 = 1_000_000.0;

fn migration_cooldown_duration(token_count: usize, bandwidth_mbps: u64) -> Duration {
    if token_count == 0 {
        return Duration::ZERO;
    }

    let seconds = token_count as f64 * KV_CACHE_BYTES_PER_TOKEN * BITS_PER_BYTE
        / (bandwidth_mbps as f64 * BITS_PER_MEGABIT);
    Duration::from_secs_f64(seconds)
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

    // 分词器
    pub tokenizer_manager: Arc<TokenizerManager>,

    // 全局 KV 元数据索引
    pub kv_radix: Arc<KvRadixTree>,

    // 节点配置
    pub config: Arc<Config>,

    //活跃请求序列
    pub active_squence: Arc<ActiveSequences>,

    pub pressure_migration_in_flight: Arc<DashSet<(u32, u32)>>,

    pub pressure_migration_next_allowed_at: Arc<DashMap<(u32, u32), Instant>>,

    pub pressure_migration_request_count: Arc<AtomicU64>,

    pub metrics_collector: Option<Arc<MetricsCollector>>,

    // V2 state exists only when protocol_mode enables the V2 service.
    pub v2_state: Option<Arc<state::v2::V2State>>,

    pub v2_instance_transfer_slots: Arc<DashMap<String, Arc<Semaphore>>>,

    pub v2_network_slots: Arc<Semaphore>,

    pub v2_rebalance_groups: Arc<DashSet<Vec<u8>>>,
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

#[derive(Debug, Serialize)]
struct LivenessStatus {
    live: bool,
}

#[derive(Debug, Serialize)]
struct ControlPlaneStatus {
    live: bool,
    ready: bool,
    protocol_mode: &'static str,
    v2_transfer_enabled: bool,
    metrics: crate::metrics::MetricsSnapshot,
    v2: Option<state::v2::V2StatusSnapshot>,
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
                if config.protocol_mode == ProtocolMode::V1 {
                    tracing::warn!(
                        "protocol_mode=v1 is deprecated; migrate through dual_shadow to v2"
                    );
                }
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
                        return Err(anyhow::anyhow!(
                            "unsupported hash_algorithm: {}",
                            config.hash_algorithm
                        ));
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

                let active_squence = Arc::new(ActiveSequences::new_with_ttl(
                    config.block_size,
                    Duration::from_millis(config.v2_request_ttl_ms),
                ));

                let metrics_collector = if config.enable_metrics {
                    Some(Arc::new(MetricsCollector::default()))
                } else {
                    None
                };

                let v2_state = if config.protocol_mode == ProtocolMode::V1 {
                    None
                } else {
                    Some(Arc::new(state::v2::V2State::new_with_demand_window(
                        Duration::from_millis(config.v2_lease_ttl_ms),
                        Duration::from_millis(config.v2_request_ttl_ms),
                        config.v2_inventory_page_limit,
                        Duration::from_millis(config.v2_demand_window_ms),
                    )))
                };
                let v2_network_slots = Arc::new(Semaphore::new(
                    config.v2_network_concurrency,
                ));
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
                    pressure_migration_next_allowed_at: Arc::new(DashMap::new()),
                    pressure_migration_request_count: Arc::new(AtomicU64::new(0)),
                    metrics_collector,
                    v2_state,
                    v2_instance_transfer_slots: Arc::new(DashMap::new()),
                    v2_network_slots,
                    v2_rebalance_groups: Arc::new(DashSet::new()),
                };
                tracing::debug!("Loaded config: {:?}", shared.config);
                if let Some(v2_state) = shared.v2_state.clone() {
                    let active_sequences = Arc::clone(&shared.active_squence);
                    let maintenance_interval = Duration::from_millis(
                        shared.config.v2_maintenance_interval_ms,
                    );
                    tokio::spawn(async move {
                        let mut interval = tokio::time::interval(maintenance_interval);
                        loop {
                            interval.tick().await;
                            v2_state.cleanup_expired();
                            active_sequences.force_expiry();
                        }
                    });
                }
                if shared.config.enable_v2_transfer {
                    let rebalance_shared = shared.clone();
                    tokio::spawn(async move {
                        rebalance_shared.run_v2_rebalance_worker().await;
                    });
                }
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
        self.shared.launch_status_server().await;

        let meta_server = KvMeta2MetaServer::new(KvCacheMetaService {
            shared: self.shared.clone(),
        });
        let data_server = KvMeta2DataServer::new(KvCacheDataService {
            shared: self.shared.clone(),
        });

        let address = format!("{}:{}", ip, port).parse().unwrap();
        match self.shared.config.protocol_mode {
            ProtocolMode::V1 => {
                tonic::transport::Server::builder()
                    .add_service(meta_server)
                    .add_service(data_server)
                    .serve(address)
                    .await
                    .unwrap();
            },
            ProtocolMode::DualShadow => {
                let data_server_v2 = KvMeta2DataV2Server::new(KvCacheDataServiceV2 {
                    shared: self.shared.clone(),
                });
                tonic::transport::Server::builder()
                    .add_service(meta_server)
                    .add_service(data_server)
                    .add_service(data_server_v2)
                    .serve(address)
                    .await
                    .unwrap();
            },
            ProtocolMode::V2 => {
                let data_server_v2 = KvMeta2DataV2Server::new(KvCacheDataServiceV2 {
                    shared: self.shared.clone(),
                });
                tonic::transport::Server::builder()
                    .add_service(meta_server)
                    .add_service(data_server_v2)
                    .serve(address)
                    .await
                    .unwrap();
            },
        }
    }
}

impl Shared {
    async fn launch_status_server(&self) {
        let app = Router::new()
            .route("/live", get(liveness))
            .route("/health", get(liveness))
            .route("/ready", get(readiness))
            .route("/status", get(control_plane_status))
            .with_state(self.clone());
        let address = std::net::SocketAddr::new(
            self.config.local_meta_server.ip,
            self.config.status_port,
        );
        let listener = tokio::net::TcpListener::bind(address)
            .await
            .expect("failed to bind CEDFS status endpoint");
        tracing::info!("CEDFS status endpoint listening on http://{address}");
        tokio::spawn(async move {
            if let Err(error) = axum::serve(listener, app).await {
                tracing::error!("CEDFS status endpoint stopped: {error}");
            }
        });
    }

    async fn run_v2_rebalance_worker(self) {
        let mut interval = tokio::time::interval(Duration::from_millis(
            self.config.v2_rebalance_interval_ms,
        ));
        loop {
            interval.tick().await;
            self.run_v2_rebalance_round().await;
        }
    }

    async fn run_v2_rebalance_round(&self) {
        let Some(state) = self.v2_state.as_ref() else {
            return;
        };
        for group_id in state.group_ids() {
            if !self.v2_rebalance_groups.insert(group_id.clone()) {
                continue;
            }
            let Some(candidate) = state
                .select_rebalance_candidates(
                    &group_id,
                    self.config.v2_rebalance_reserve_bytes,
                    self.config.v2_rebalance_bytes_per_token,
                    self.config.v2_rebalance_max_replicas,
                    self.config.v2_rebalance_min_benefit,
                    self.config.v2_rebalance_max_evictions_per_second,
                    self.config.v2_rebalance_target_max_usage_ratio,
                    self.config.v2_rebalance_max_blocks,
                )
                .into_iter()
                .next()
            else {
                self.v2_rebalance_groups.remove(&group_id);
                continue;
            };
            let source_key = transfer_slot_key(&candidate.source);
            let target_key = transfer_slot_key(&candidate.target);
            let source_slots = self
                .v2_instance_transfer_slots
                .entry(source_key)
                .or_insert_with(|| {
                    Arc::new(Semaphore::new(
                        self.config.v2_source_target_concurrency,
                    ))
                })
                .clone();
            let target_slots = self
                .v2_instance_transfer_slots
                .entry(target_key)
                .or_insert_with(|| {
                    Arc::new(Semaphore::new(
                        self.config.v2_source_target_concurrency,
                    ))
                })
                .clone();
            let Ok(_network_permit) = self.v2_network_slots.clone().try_acquire_owned() else {
                self.v2_rebalance_groups.remove(&group_id);
                continue;
            };
            let Ok(_source_permit) = source_slots.try_acquire_owned() else {
                self.v2_rebalance_groups.remove(&group_id);
                continue;
            };
            let Ok(_target_permit) = target_slots.try_acquire_owned() else {
                self.v2_rebalance_groups.remove(&group_id);
                continue;
            };
            let transfer_id = format!(
                "rebalance-{}-{}",
                Utc::now().timestamp_millis(),
                candidate.source.key.as_ref().map_or(0, |key| key.worker_id),
            );
            let source_host = if candidate.source_endpoints.host.contains(':') {
                format!("[{}]", candidate.source_endpoints.host)
            } else {
                candidate.source_endpoints.host.clone()
            };
            let source_url = format!(
                "http://{}:{}",
                source_host, candidate.source_endpoints.transfer_rpc_port
            );
            let request = TransferKvV2Request {
                transfer_id,
                compatibility_group_id: candidate.group_id,
                source: Some(candidate.source),
                target: Some(candidate.target),
                target_endpoints: Some(candidate.target_endpoints),
                blocks: candidate.blocks,
                do_copy: true,
                deadline_unix_ms: Utc::now().timestamp_millis() as u64
                    + self.config.v2_transfer_rpc_timeout_ms,
            };
            let shared = self.clone();
            tokio::spawn(async move {
                let _group_guard = V2RebalanceGroupGuard {
                    groups: shared.v2_rebalance_groups.clone(),
                    group_id,
                };
                let result = shared.execute_transfer_v2(&source_url, request).await;
                if let Some(collector) = &shared.metrics_collector {
                    collector.record_v2_rebalance(result.is_ok());
                }
                if let Err(error) = result {
                    tracing::warn!("V2 rebalance transfer failed: {error:?}");
                }
                drop((_network_permit, _source_permit, _target_permit));
            });
        }
    }

    pub async fn execute_transfer_v2(
        &self,
        source_rpc_url: &str,
        mut request: TransferKvV2Request,
    ) -> anyhow::Result<TransferKvV2Response> {
        if !self.config.enable_v2_transfer {
            anyhow::bail!("V2 transfer is disabled");
        }
        if !request.do_copy {
            anyhow::bail!("V2 transfer supports copy only");
        }
        let state = self
            .v2_state
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("V2 state is unavailable"))?;
        if request.deadline_unix_ms == 0 {
            request.deadline_unix_ms = Utc::now().timestamp_millis() as u64
                + self.config.v2_transfer_rpc_timeout_ms;
        }
        let client = TransferKvOp::new(source_rpc_url);
        let response = client
            .send_transfer_requests_v2(
                request.clone(),
                TransferV2Limits {
                    max_blocks: self.config.v2_transfer_max_blocks,
                    max_tokens: self.config.v2_transfer_max_tokens,
                    max_bytes: self.config.v2_transfer_max_bytes,
                    estimated_bytes_per_token: self.config.v2_rebalance_bytes_per_token,
                    timeout: Duration::from_millis(
                        self.config.v2_transfer_rpc_timeout_ms,
                    ),
                },
            )
            .await?;
        if let Some(collector) = &self.metrics_collector {
            for result in &response.results {
                let status = usize::try_from(result.status)
                    .ok()
                    .filter(|status| *status <= 9)
                    .unwrap_or(0);
                collector.record_v2_transfer_result(
                    status,
                    result.bytes_transferred,
                );
            }
        }
        state
            .commit_transfer_results(&request, &response.results)
            .map_err(anyhow::Error::msg)?;
        Ok(response)
    }

    pub fn launch_metrics_reporter(&self) {
        let Some(collector) = self.metrics_collector.clone() else {
            return;
        };

        let kv_radix = self.kv_radix.clone();
        let v2_state = self.v2_state.clone();
        let interval_duration = Duration::from_millis(self.config.metrics_interval_ms);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(interval_duration);
            loop {
                interval.tick().await;
                let instances = kv_radix.instance_metrics_snapshots();
                let blocks = kv_radix.all_block_metrics();
                let snapshot = collector.snapshot();
                tracing::info!(
                    v1_instances = instances.len(),
                    v1_blocks = blocks.len(),
                    v1_replicas = blocks
                        .iter()
                        .map(|block| u64::from(block.replica_count))
                        .sum::<u64>(),
                    replication_rpc_total = snapshot.replication_rpc_total,
                    replication_rpc_blocks_total = snapshot.replication_rpc_blocks_total,
                    migration_selection_total = snapshot.migration_selection_total,
                    v2_transfer_blocks_total = snapshot.v2_transfer_blocks_total,
                    v2_transfer_bytes_total = snapshot.v2_transfer_bytes_total,
                    v2_transfer_failed_blocks_total = snapshot.v2_transfer_failed_blocks_total,
                    v2_transfer_blocks_by_status = ?snapshot.v2_transfer_blocks_by_status,
                    v2_rebalance_success_total = snapshot.v2_rebalance_success_total,
                    v2_rebalance_failure_total = snapshot.v2_rebalance_failure_total,
                    "cedfs metrics snapshot"
                );
                if let Some(state) = &v2_state {
                    let status = state.status_snapshot();
                    tracing::info!(
                        ready = status.ready,
                        groups = status.groups.len(),
                        pending_inventory_syncs = status.pending_inventory_syncs,
                        active_requests = status.active_requests,
                        lease_expired_total = status.lease_expired_total,
                        mutation_sequence_gap_total = status.mutation_sequence_gap_total,
                        inventory_sync_success_total = status.inventory_sync_success_total,
                        inventory_sync_failure_total = status.inventory_sync_failure_total,
                        inventory_sync_blocks_total = status.inventory_sync_blocks_total,
                        inventory_sync_duration_ms_total = status.inventory_sync_duration_ms_total,
                        reconcile_mismatch_total = status.reconcile_mismatch_total,
                        "cedfs v2 status snapshot"
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
        let Some(report) = self.kv_radix.apply_eviction(server_id, token_hash) else {
            tracing::debug!(
                "remove_server_from_kv_meta: no replica to evict for server_id={}, token_hash {:?}",
                server_id,
                token_hash
            );
            return;
        };

        if report.removed && report.replica_count_before <= 1 {
            tracing::info!(
                "Removed KV block {:?} from kv_radix as it has no replicas",
                token_hash
            );
        }

        tracing::debug!(
            "Removed server_id {} from KV metadata for token_hash {:?}: heat {} -> {}, replicas {} -> {}",
            server_id,
            token_hash,
            report.heat_before,
            report.heat_after,
            report.replica_count_before,
            report.replica_count_after
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
        let delta = self.config.migration_delta;
        let absolute_threshold =
            delta * self.config.max_num_batch_tokens as f64 / self.config.block_size as f64;
        let mut result = PressureMigrationResult::default();

        for _ in 0..MAX_PRESSURE_REBALANCE_ROUNDS {
            let extremes = self.kv_radix.pressure_extremes();
            let Some(src_server_id) = extremes.max_server else {
                result.skipped_reason = Some("no_source_server".to_string());
                return Ok(result);
            };
            let Some(dst_server_id) = extremes.min_server else {
                result.skipped_reason = Some("no_target_server".to_string());
                return Ok(result);
            };

            let gap = extremes.max_pressure - extremes.min_pressure;
            if gap <= absolute_threshold {
                if result.rounds == 0 {
                    result.skipped_reason = Some("below_absolute_threshold".to_string());
                }
                return Ok(result);
            }

            let migration_pair = (src_server_id, dst_server_id);
            if let Some(next_allowed_at) = self
                .pressure_migration_next_allowed_at
                .get(&migration_pair)
                .map(|entry| *entry.value())
            {
                let now = Instant::now();
                if now < next_allowed_at {
                    if result.rounds == 0 {
                        result.skipped_reason =
                            Some("migration_pair_bandwidth_cooldown".to_string());
                    }
                    tracing::debug!(
                        "pressure migration skipped by bandwidth cooldown: source_server={}, target_server={}, remaining_ms={}",
                        src_server_id,
                        dst_server_id,
                        next_allowed_at.saturating_duration_since(now).as_millis()
                    );
                    return Ok(result);
                }
            }

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

            let selection_started = Instant::now();
            let candidates = self
                .kv_radix
                .select_replication_candidates(src_server_id, dst_server_id);
            let selection_duration = selection_started.elapsed();
            if candidates.is_empty() {
                result.skipped_reason = Some("no_replication_candidates".to_string());
                return Ok(result);
            }

            let candidate_count = candidates.len();

            result.selected_source_server_id = Some(src_server_id);
            result.selected_target_server_id = Some(dst_server_id);
            result.candidate_count += candidate_count;

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
            let migrated_token_count = token_ids.len();
            match self
                .transfer_pressure_candidates(
                    &src_server,
                    &dst_server,
                    &hashes,
                    offsets,
                    token_ids,
                )
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
                    self.record_migration_selection(
                        selection_duration,
                        candidate_count,
                    );
                },
                Ok(status) if status > 0 => {
                    for token_hash in hashes {
                        self.update_kv_meta_after_migration(token_hash, dst_server.id)
                            .await;
                    }
                    result.success_count += migrated_count;
                    self.record_migration_selection(
                        selection_duration,
                        candidate_count,
                    );
                    self.update_migration_pair_cooldown(migration_pair, migrated_token_count);
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

        let rpc_start = Utc::now();
        let transfer_result = client
            .send_transfer_request(
                concatenated_hash_bytes,
                position,
                offsets,
                token_ids,
                dst_server.ip.to_string(),
                dst_server.init_port as i32,
                true,
            )
            .await;
        let rpc_end = Utc::now();

        if let Some(collector) = &self.metrics_collector {
            collector.record_replication_rpc(ReplicationRpcRecord {
                start: rpc_start,
                end: rpc_end,
                block_count: hashes.len(),
            });
        }

        Ok(transfer_result?.status)
    }

    fn record_migration_selection(
        &self,
        duration: Duration,
        candidate_count: usize,
    ) {
        if let Some(collector) = &self.metrics_collector {
            collector.record_migration_selection(MigrationSelectionRecord {
                duration,
                candidate_count,
            });
        }
    }

    fn update_migration_pair_cooldown(&self, migration_pair: (u32, u32), token_count: usize) {
        let bandwidth_mbps = self.config.migration_network_bandwidth_mbps;
        let cooldown = migration_cooldown_duration(token_count, bandwidth_mbps);
        if cooldown.is_zero() {
            return;
        }

        let next_allowed_at = Instant::now() + cooldown;
        self.pressure_migration_next_allowed_at
            .insert(migration_pair, next_allowed_at);
        tracing::info!(
            "pressure migration bandwidth cooldown updated: source_server={}, target_server={}, bandwidth_mbps={}, migrated_token_count={}, cooldown_ms={}",
            migration_pair.0,
            migration_pair.1,
            bandwidth_mbps,
            token_count,
            cooldown.as_millis()
        );
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

    /// 迁移完成后更新 V1 KV 元数据。
    async fn update_kv_meta_after_migration(&self, token_hash: [u8; 32], new_server_id: u32) {
        self.kv_radix.add_server(token_hash, new_server_id);
    }
}

fn transfer_slot_key(identity: &cedfs_proto::kvcache_v2::InstanceIdentityV2) -> String {
    let key = identity.key.as_ref();
    format!(
        "{}:{}:{}",
        key.map_or("", |value| value.lmcache_instance_id.as_str()),
        key.map_or(0, |value| value.worker_id),
        identity.epoch,
    )
}

struct V2RebalanceGroupGuard {
    groups: Arc<DashSet<Vec<u8>>>,
    group_id: Vec<u8>,
}

impl Drop for V2RebalanceGroupGuard {
    fn drop(&mut self) {
        self.groups.remove(&self.group_id);
    }
}

async fn liveness() -> Json<LivenessStatus> {
    Json(LivenessStatus { live: true })
}

async fn readiness(State(shared): State<Shared>) -> (StatusCode, Json<ControlPlaneStatus>) {
    let status = build_control_plane_status(&shared);
    let code = if status.ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (code, Json(status))
}

async fn control_plane_status(State(shared): State<Shared>) -> Json<ControlPlaneStatus> {
    Json(build_control_plane_status(&shared))
}

fn build_control_plane_status(shared: &Shared) -> ControlPlaneStatus {
    let v2 = shared.v2_state.as_ref().map(|state| state.status_snapshot());
    let ready = match shared.config.protocol_mode {
        ProtocolMode::V1 => true,
        ProtocolMode::DualShadow | ProtocolMode::V2 => {
            v2.as_ref().is_some_and(|status| status.ready)
        },
    };
    let protocol_mode = match shared.config.protocol_mode {
        ProtocolMode::V1 => "v1",
        ProtocolMode::DualShadow => "dual_shadow",
        ProtocolMode::V2 => "v2",
    };
    ControlPlaneStatus {
        live: true,
        ready,
        protocol_mode,
        v2_transfer_enabled: shared.config.enable_v2_transfer,
        metrics: shared
            .metrics_collector
            .as_ref()
            .map_or_else(Default::default, |collector| collector.snapshot()),
        v2,
    }
}
