//! 负载均衡调度：按各推理实例当前未完成请求的 prompt 长度总和，或将新请求调度到 KV cache 命中长度最大的实例

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::Mutex;

use crate::types::DataServer;
use crate::Shared;

type TokenHashes = Vec<[u8; 32]>;
type TokenBlocks = Vec<([u8; 32], u32)>;

/// 负载不平衡判断阈值：最高负载 >= 最低负载 * LOAD_IMBALANCE_RATIO 时触发负载均衡
const LOAD_IMBALANCE_RATIO: u64 = 3;

/// KV cache 前缀匹配率阈值：超过该值时启用缓存感知路由策略
const CACHE_THRESHOLD: f64 = 0.5;
const DEFAULT_SCORE_CACHE_WEIGHT: f64 = 0.7;
const DEFAULT_SCORE_LOAD_WEIGHT: f64 = 0.3;

pub struct Scheduler {
    strategy: String,
    stat_total_tokens: AtomicU64,
    stat_hit_tokens: AtomicU64,
    stat_request_count: AtomicU64,
    stat_last_report: Mutex<Instant>,
}

impl Scheduler {
    pub fn new(strategy: impl Into<String>) -> Self {
        let strategy = strategy.into().trim().to_ascii_lowercase();
        Self {
            strategy,
            stat_total_tokens: AtomicU64::new(0),
            stat_hit_tokens: AtomicU64::new(0),
            stat_request_count: AtomicU64::new(0),
            stat_last_report: Mutex::new(Instant::now()),
        }
    }

    async fn update_cache_hit_stats_and_maybe_report(&self, total_tokens: u64, hit_tokens: u64) {
        self.stat_total_tokens
            .fetch_add(total_tokens, Ordering::Relaxed);
        self.stat_hit_tokens.fetch_add(hit_tokens, Ordering::Relaxed);
        self.stat_request_count.fetch_add(1, Ordering::Relaxed);

        let mut last_report = self.stat_last_report.lock().await;
        if last_report.elapsed() >= Duration::from_secs(30) {
            let total = self.stat_total_tokens.swap(0, Ordering::Relaxed);
            let hit = self.stat_hit_tokens.swap(0, Ordering::Relaxed);
            let reqs = self.stat_request_count.swap(0, Ordering::Relaxed);
            let hit_ratio = if total > 0 {
                hit as f64 / total as f64
            } else {
                0.0
            };

            tracing::warn!(
                "kv cache hit stats (last 1m): requests={}, total_tokens={}, hit_tokens={}, hit_ratio={:.2}%",
                reqs,
                total,
                hit,
                hit_ratio * 100.0
            );
            *last_report = Instant::now();
        }
    }
}

/// 按 model_filter 过滤实例：若指定 model 则只保留 model_name == model 的节点；若无匹配则返回全部
fn filter_servers_by_model(servers: &[DataServer], model_name: &str) -> Vec<DataServer> {
    let filtered: Vec<DataServer> = servers
        .iter()
        .filter(|s| s.model_name == model_name)
        .cloned()
        .collect();
    if filtered.is_empty() {
        servers.to_vec()
    } else {
        filtered
    }
}

fn normalize_score_weights(cache_weight: f64, load_weight: f64) -> (f64, f64) {
    let safe_cache = if cache_weight.is_finite() && cache_weight >= 0.0 {
        cache_weight
    } else {
        DEFAULT_SCORE_CACHE_WEIGHT
    };
    let safe_load = if load_weight.is_finite() && load_weight >= 0.0 {
        load_weight
    } else {
        DEFAULT_SCORE_LOAD_WEIGHT
    };
    let total = safe_cache + safe_load;
    if total <= f64::EPSILON {
        return (DEFAULT_SCORE_CACHE_WEIGHT, DEFAULT_SCORE_LOAD_WEIGHT);
    }
    (safe_cache / total, safe_load / total)
}

/// 从 shared.local_data_server_collect 中选出当前未完成推理请求长度总和最小的实例
///
/// - 若有 `model_filter`，优先只从 `model_name == model` 的节点中选；若无匹配则从全部节点中选
impl Scheduler {
    /// 统一调度入口：根据初始化时注入的 `strategy` 选择具体调度算法。
    ///
    /// 支持的策略值（大小写不敏感）：
    /// - `workload` / `min_load`
    /// - `kvcache` / `cache`
    /// - `hybrid`
    /// - `hybrid2`
    /// - `score`
    ///
    /// 当配置值未知时，默认回退到 `hybrid`。
    pub async fn select_server_by_strategy(
        &self,
        shared: &Shared,
        model_name: &str,
        prompt: &str,
    ) -> Option<(DataServer, TokenBlocks)> {
        match self.strategy.as_str() {
            "workload" | "min_load" => {
                self.select_server_by_workload(shared, model_name, prompt).await
            }
            "kvcache" | "cache" => {
                self.select_server_by_kvcache(shared, model_name, prompt).await
            }
            "hybrid" => self.select_server_hybrid(shared, model_name, prompt).await,
            "hybrid2" => self.select_server_hybrid2(shared, model_name, prompt).await,
            "score" => self.select_server_by_score(shared, model_name, prompt).await,
            _ => {
                tracing::warn!(
                    "unknown scheduler_strategy='{}', fallback to 'hybrid'",
                    self.strategy
                );
                self.select_server_hybrid(shared, model_name, prompt).await
            }
        }
    }

    pub async fn select_server_by_workload(
        &self,
        shared: &Shared,
        model_name: &str,
        prompt: &str,
    ) -> Option<(DataServer, TokenBlocks)> {
        let servers = shared.local_data_server_collect.read().await;
        let candidates = filter_servers_by_model(&servers, model_name);
        drop(servers);
        let server = shared
            .inference_load_tracker
            .select_server_with_min_load(&candidates)?;
        let token_blocks = shared
            .get_token_hashes_for_prompt(model_name, prompt)
            .await
            .unwrap_or_default();
        Some((server, token_blocks))
    }
}


/// 根据 prompt 的 token 在 KV cache 中的命中情况，在指定 model 的实例中选出命中长度最大的实例
///
/// - 先对 prompt 做 tokenize 并得到 token_hashes，再 `search_tokens` 得到各实例的匹配长度
/// - 在 `model_name` 过滤后的候选中，返回匹配长度最大的 `DataServer`；无 token 或无匹配时返回 `None`
impl Scheduler {
    pub async fn select_server_by_score(
        &self,
        shared: &Shared,
        model_name: &str,
        prompt: &str,
    ) -> Option<(DataServer, TokenBlocks)> {
        let servers = shared.local_data_server_collect.read().await;
        let candidates = filter_servers_by_model(&servers, model_name);
        drop(servers);

        if candidates.is_empty() {
            return None;
        }

        let tokenized = shared.tokenizer_manager.encode_async(model_name, prompt).await;
        let (token_blocks, match_results, req_total_tokens) = match tokenized {
            Ok(token_list) if !token_list.is_empty() => {
                let token_blocks: TokenBlocks = shared
                    .hasher
                    .hash_tokens_with_blocks_all(&token_list, shared.config.block_size)
                    .iter()
                    .map(|(hash, offset)| (hash.to_u256(), *offset))
                    .collect();
                let token_hashes: TokenHashes = token_blocks.iter().map(|(h, _)| *h).collect();
                let match_results = shared.search_tokens(token_hashes);
                (token_blocks, match_results, token_list.len() as u64)
            }
            Ok(_) => (Vec::new(), Vec::new(), 0u64),
            Err(e) => {
                tracing::warn!("Failed to encode prompt with model '{}': {}", model_name, e);
                (Vec::new(), Vec::new(), 0u64)
            }
        };

        let candidate_ids: HashSet<u32> = candidates.iter().map(|s| s.id).collect();
        let hit_by_server: HashMap<u32, u32> = match_results
            .iter()
            .filter(|(id, _)| candidate_ids.contains(id))
            .copied()
            .collect();
        let max_hit_tokens = hit_by_server.values().copied().max().unwrap_or(0) as u64;

        self.update_cache_hit_stats_and_maybe_report(req_total_tokens, max_hit_tokens)
            .await;

        let max_load = candidates
            .iter()
            .map(|s| shared.inference_load_tracker.get_load(&s.id))
            .max()
            .unwrap_or(0);

        let (cache_weight, load_weight) = normalize_score_weights(
            shared.config.scheduler_score_cache_weight,
            shared.config.scheduler_score_load_weight,
        );

        let mut best_server: Option<DataServer> = None;
        let mut best_score = f64::NEG_INFINITY;
        let mut best_load = u64::MAX;
        let mut best_hit = 0u64;

        for server in &candidates {
            let hit_tokens = hit_by_server.get(&server.id).copied().unwrap_or(0) as u64;
            let load = shared.inference_load_tracker.get_load(&server.id);
            let normalized_hit = if max_hit_tokens > 0 {
                hit_tokens as f64 / max_hit_tokens as f64
            } else {
                0.0
            };
            let normalized_load = if max_load > 0 {
                load as f64 / max_load as f64
            } else {
                0.0
            };
            let score = cache_weight * normalized_hit - load_weight * normalized_load;

            tracing::debug!(
                "select_server_by_score: model={}, server_id={}, hit_tokens={}, load={}, normalized_hit={:.3}, normalized_load={:.3}, score={:.6}, weights=({:.3}, {:.3})",
                model_name,
                server.id,
                hit_tokens,
                load,
                normalized_hit,
                normalized_load,
                score,
                cache_weight,
                load_weight
            );

            let score_better = score > best_score;
            let tie_break = (score - best_score).abs() <= f64::EPSILON
                && (load < best_load || (load == best_load && hit_tokens > best_hit));

            if best_server.is_none() || score_better || tie_break {
                best_server = Some(server.clone());
                best_score = score;
                best_load = load;
                best_hit = hit_tokens;
            }
        }

        let selected = best_server.or_else(|| {
            shared
                .inference_load_tracker
                .select_server_with_min_load(&candidates)
        })?;
        tracing::info!(
            "select_server_by_score: model={}, selected_server_id={}, score={:.6}, hit_tokens={}, load={}, max_hit_tokens={}, max_load={}, weights=({:.3}, {:.3})",
            model_name,
            selected.id,
            best_score,
            best_hit,
            best_load,
            max_hit_tokens,
            max_load,
            cache_weight,
            load_weight
        );
        Some((selected, token_blocks))
    }
}

impl Scheduler {
    pub async fn select_server_by_kvcache(
        &self,
        shared: &Shared,
        model_name: &str,
        prompt: &str,
    ) -> Option<(DataServer, TokenBlocks)> {
        let servers = shared.local_data_server_collect.read().await;
        let candidates = filter_servers_by_model(&servers, model_name);
        drop(servers);

        let tokenized = shared.tokenizer_manager.encode_async(model_name, prompt).await;
        let (token_blocks, match_results, req_total_tokens) = match tokenized {
            Ok(token_list) if !token_list.is_empty() => {
                let token_blocks: TokenBlocks = shared
                    .hasher
                    .hash_tokens_with_blocks_all(&token_list, shared.config.block_size)
                    .iter()
                    .map(|(hash, offset)| (hash.to_u256(), *offset))
                    .collect();
                let token_hashes: TokenHashes = token_blocks.iter().map(|(h, _)| *h).collect();
                let match_results = shared.search_tokens(token_hashes.clone());
                (token_blocks, match_results, token_list.len() as u64)
            }
            Ok(_) => (Vec::new(), Vec::new(), 0u64),
            Err(e) => {
                tracing::warn!("Failed to encode prompt with model '{}': {}", model_name, e);
                (Vec::new(), Vec::new(), 0u64)
            }
        };

        let candidate_ids: HashSet<u32> = candidates.iter().map(|s| s.id).collect();
        let best = match_results
            .iter()
            .filter(|(id, _)| candidate_ids.contains(id))
            .max_by_key(|(_, len)| *len)
            .copied();

        let req_hit_tokens = best.map(|(_, len)| len as u64).unwrap_or(0);
        self.update_cache_hit_stats_and_maybe_report(req_total_tokens, req_hit_tokens)
            .await;

        let req_hit_ratio = if req_total_tokens > 0 {
            req_hit_tokens as f64 / req_total_tokens as f64
        } else {
            0.0
        };
        tracing::info!(
            "select_server_by_kvcache: model={}, best_hit_tokens={}, req_total_tokens={}, req_hit_ratio={:.3}, match_results={:?}",
            model_name,
            req_hit_tokens,
            req_total_tokens,
            req_hit_ratio,
            match_results
        );

        if req_hit_tokens == 0 {
            let server = shared
                .inference_load_tracker
                .select_server_with_min_load(&candidates)?;
            return Some((server, token_blocks));
        }

        let server = best
            .and_then(|(server_id, _)| candidates.iter().find(|s| s.id == server_id).cloned())
            .or_else(|| {
                shared
                    .inference_load_tracker
                    .select_server_with_min_load(&candidates)
            })?;
        Some((server, token_blocks))
    }
}

/// 综合调度算法，按以下优先级选取目标实例：
///
/// 1. **缓存感知路由**：当请求 prompt 的 KV cache 前缀匹配率 > [`CACHE_THRESHOLD`] 时，
///    选择匹配长度最大的实例（与 `select_server_by_kvcache` 相同逻辑）。
/// 2. **负载均衡路由**：当系统负载不平衡（最高负载 >= 最低负载 * 3）时，
///    选择当前负载最小的实例。
/// 3. **最少 KV Cache 路由**：上述两个策略均不触发时，选择拥有 KV cache 块数量最少的实例。
///
/// 每级策略均在候选集为空时以最小负载实例兜底。
impl Scheduler {
    pub async fn select_server_hybrid(
        &self,
        shared: &Shared,
        model_name: &str,
        prompt: &str,
    ) -> Option<(DataServer, TokenBlocks)> {
        let servers = shared.local_data_server_collect.read().await;
        let candidates = filter_servers_by_model(&servers, model_name);
        drop(servers);

        if candidates.is_empty() {
            return None;
        }

        // --- 计算 KV cache 前缀匹配率 ---
        // prefix_ratio = max_matched / total_tokens
        // 其中 max_matched 为 search_tokens 返回的最大匹配 token 数。
        let tokenized = shared.tokenizer_manager.encode_async(model_name, prompt).await;
        let (token_blocks, match_results, prefix_ratio, req_total_tokens, req_hit_tokens) =
            match tokenized {
                Ok(token_list) if !token_list.is_empty() => {
                    let total_tokens = token_list.len() as f64;
                    let token_blocks: TokenBlocks = shared
                        .hasher
                        .hash_tokens_with_blocks_all(&token_list, shared.config.block_size)
                        .iter()
                        .map(|(hash, offset)| (hash.to_u256(), *offset))
                        .collect();
                    let token_hashes: TokenHashes = token_blocks.iter().map(|(h, _)| *h).collect();

                    let results = shared.search_tokens(token_hashes.clone());
                    let max_matched = results.iter().map(|(_, len)| *len).max().unwrap_or(0);
                    let ratio = if total_tokens > 0.0 {
                        max_matched as f64 / total_tokens
                    } else {
                        0.0
                    };
                    (
                        token_blocks,
                        results,
                        ratio,
                        token_list.len() as u64,
                        max_matched as u64,
                    )
                }
                Ok(_) => (Vec::new(), Vec::new(), 0.0, 0u64, 0u64),
                Err(e) => {
                    tracing::warn!("Failed to encode prompt with model '{}': {}", model_name, e);
                    (Vec::new(), Vec::new(), 0.0, 0u64, 0u64)
                }
            };

        self.update_cache_hit_stats_and_maybe_report(req_total_tokens, req_hit_tokens)
            .await;

        tracing::info!(
            "select_server_hybrid: model={}, prefix_ratio={:.3}",
            model_name, prefix_ratio
        );

        // --- 策略一：缓存感知路由 ---
        if prefix_ratio > CACHE_THRESHOLD && !match_results.is_empty() {
            let candidate_ids: HashSet<u32> = candidates.iter().map(|s| s.id).collect();
            let best = match_results
                .into_iter()
                .filter(|(id, _)| candidate_ids.contains(id))
                .max_by_key(|(_, len)| *len);

            if let Some((server_id, matched_len)) = best {
                tracing::info!(
                    "select_server_hybrid: cache-aware route -> server_id={}, matched_len={}",
                    server_id,
                    matched_len
                );
                if let Some(server) = candidates.iter().find(|s| s.id == server_id).cloned() {
                    return Some((server, token_blocks.clone()));
                }
            }
        }

        // --- 策略二：负载均衡路由 ---
        {
            let loads: Vec<u64> = candidates
                .iter()
                .map(|s| shared.inference_load_tracker.get_load(&s.id))
                .collect();
            let min_load = loads.iter().copied().min().unwrap_or(0);
            let max_load = loads.iter().copied().max().unwrap_or(0);

            let imbalanced = max_load >= min_load.saturating_mul(LOAD_IMBALANCE_RATIO)
                && !(min_load == 0 && max_load == 0);

            if imbalanced {
                tracing::info!(
                    "select_server_hybrid: load-balance route -> min_load={}, max_load={}",
                    min_load,
                    max_load
                );
                let server = shared
                    .inference_load_tracker
                    .select_server_with_min_load(&candidates)?;
                return Some((server, token_blocks.clone()));
            }
        }

        // --- 策略三：最少 KV Cache 路由 ---
        {
            let selected = candidates
                .iter()
                .min_by_key(|s| {
                    shared
                        .local_kv_cache_block_count
                        .get(&s.id)
                        .map(|count| count.load(Ordering::Relaxed) as u64)
                        .unwrap_or(0)
                })
                .cloned();

            if let Some(ref s) = selected {
                let kvcache_count = shared
                    .local_kv_cache_block_count
                    .get(&s.id)
                    .map(|count| count.load(Ordering::Relaxed) as u64)
                    .unwrap_or(0);
                tracing::info!(
                    "select_server_hybrid: min-kvcache route -> server_id={}, kvcache_count={}",
                    s.id,
                    kvcache_count
                );
            }
            let server = selected.or_else(|| {
                shared
                    .inference_load_tracker
                    .select_server_with_min_load(&candidates)
            })?;
            Some((server, token_blocks))
        }
    }
}


/// 综合调度算法2，按以下优先级选取目标实例：
///
/// 1. **负载均衡路由**：当系统负载不平衡（最高负载 >= 最低负载 * 3）时，
///    选择当前负载最小的实例。
/// 2. **缓存感知路由**：当请求 prompt 的 KV cache 前缀匹配率 > [`CACHE_THRESHOLD`] 时，
///    选择匹配长度最大的实例（与 `select_server_by_kvcache` 相同逻辑）。
/// 3. **最少 KV Cache 路由**：上述两个策略均不触发时，选择拥有 KV cache 块数量最少的实例。
///
/// 每级策略均在候选集为空时以最小负载实例兜底。

impl Scheduler {
    pub async fn select_server_hybrid2(
        &self,
        shared: &Shared,
        model_name: &str,
        prompt: &str,
    ) -> Option<(DataServer, TokenBlocks)> {
        let servers = shared.local_data_server_collect.read().await;
        let candidates = filter_servers_by_model(&servers, model_name);
        drop(servers);

        if candidates.is_empty() {
            return None;
        }

        // --- 计算 KV cache 前缀匹配率 ---
        // prefix_ratio = max_matched / total_tokens
        // 其中 max_matched 为 search_tokens 返回的最大匹配 token 数。
        let tokenized = shared.tokenizer_manager.encode_async(model_name, prompt).await;
        let (token_blocks, match_results, prefix_ratio, req_total_tokens, req_hit_tokens) =
            match tokenized {
                Ok(token_list) if !token_list.is_empty() => {
                    let total_tokens = token_list.len() as f64;
                    let token_blocks: TokenBlocks = shared
                        .hasher
                        .hash_tokens_with_blocks_all(&token_list, shared.config.block_size)
                        .iter()
                        .map(|(hash, offset)| (hash.to_u256(), *offset))
                        .collect();
                    let token_hashes: TokenHashes = token_blocks.iter().map(|(h, _)| *h).collect();

                    let results = shared.search_tokens(token_hashes.clone());
                    let max_matched = results.iter().map(|(_, len)| *len).max().unwrap_or(0);
                    let ratio = if total_tokens > 0.0 {
                        max_matched as f64 / total_tokens
                    } else {
                        0.0
                    };
                    (
                        token_blocks,
                        results,
                        ratio,
                        token_list.len() as u64,
                        max_matched as u64,
                    )
                }
                Ok(_) => (Vec::new(), Vec::new(), 0.0, 0u64, 0u64),
                Err(e) => {
                    tracing::warn!("Failed to encode prompt with model '{}': {}", model_name, e);
                    (Vec::new(), Vec::new(), 0.0, 0u64, 0u64)
                }
            };

        self.update_cache_hit_stats_and_maybe_report(req_total_tokens, req_hit_tokens)
            .await;

        tracing::info!(
            "select_server_hybrid2: model={}, prefix_ratio={:.3}",
            model_name,
            prefix_ratio
        );

        // --- 策略一：负载均衡路由 ---
        {
            let loads: Vec<u64> = candidates
                .iter()
                .map(|s| shared.inference_load_tracker.get_load(&s.id))
                .collect();
            let min_load = loads.iter().copied().min().unwrap_or(0);
            let max_load = loads.iter().copied().max().unwrap_or(0);

            let imbalanced = max_load >= min_load.saturating_mul(LOAD_IMBALANCE_RATIO)
                && !(min_load == 0 && max_load == 0);

            if imbalanced {
                tracing::info!(
                    "select_server_hybrid2: load-balance route -> min_load={}, max_load={}",
                    min_load,
                    max_load
                );
                let server = shared
                    .inference_load_tracker
                    .select_server_with_min_load(&candidates)?;
                return Some((server, token_blocks.clone()));
            }
        }

        // --- 策略二：缓存感知路由 ---
        if prefix_ratio > CACHE_THRESHOLD && !match_results.is_empty() {
            let candidate_ids: HashSet<u32> = candidates.iter().map(|s| s.id).collect();
            let best = match_results
                .into_iter()
                .filter(|(id, _)| candidate_ids.contains(id))
                .max_by_key(|(_, len)| *len);

            if let Some((server_id, matched_len)) = best {
                tracing::info!(
                    "select_server_hybrid2: cache-aware route -> server_id={}, matched_len={}",
                    server_id,
                    matched_len
                );
                if let Some(server) = candidates.iter().find(|s| s.id == server_id).cloned() {
                    return Some((server, token_blocks.clone()));
                }
            }
        }

        // --- 策略三：最少 KV Cache 路由 ---
        {
            let selected = candidates
                .iter()
                .min_by_key(|s| {
                    shared
                        .local_kv_cache_block_count
                        .get(&s.id)
                        .map(|count| count.load(Ordering::Relaxed) as u64)
                        .unwrap_or(0)
                })
                .cloned();

            if let Some(ref s) = selected {
                let kvcache_count = shared
                    .local_kv_cache_block_count
                    .get(&s.id)
                    .map(|count| count.load(Ordering::Relaxed) as u64)
                    .unwrap_or(0);
                tracing::info!(
                    "select_server_hybrid2: min-kvcache route -> server_id={}, kvcache_count={}",
                    s.id,
                    kvcache_count
                );
            }
            let server = selected.or_else(|| {
                shared
                    .inference_load_tracker
                    .select_server_with_min_load(&candidates)
            })?;
            Some((server, token_blocks))
        }
    }
}