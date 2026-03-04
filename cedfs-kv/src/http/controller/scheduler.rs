//! 负载均衡调度：按各推理实例当前未完成请求的 prompt 长度总和，或将新请求调度到 KV cache 命中长度最大的实例

use std::collections::HashSet;

use crate::types::DataServer;
use crate::Shared;

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

/// 从 shared.local_data_server_collect 中选出当前未完成推理请求长度总和最小的实例
///
/// - 若有 `model_filter`，优先只从 `model_name == model` 的节点中选；若无匹配则从全部节点中选
pub async fn select_server_by_workload(shared: &Shared, model_name: &str) -> Option<DataServer> {
    let servers = shared.local_data_server_collect.read().await;
    let candidates = filter_servers_by_model(&servers, model_name);
    drop(servers);
    shared
        .inference_load_tracker
        .select_server_with_min_load(&candidates)
}


/// 根据 prompt 的 token 在 KV cache 中的命中情况，在指定 model 的实例中选出命中长度最大的实例
///
/// - 先对 prompt 做 tokenize 并得到 token_hashes，再 `search_tokens` 得到各实例的匹配长度
/// - 在 `model_name` 过滤后的候选中，返回匹配长度最大的 `DataServer`；无 token 或无匹配时返回 `None`
pub async fn select_server_by_kvcache(
    shared: &Shared,
    model_name: &str,
    prompt: &str,
) -> Option<DataServer> {
    let servers = shared.local_data_server_collect.read().await;
    let candidates = filter_servers_by_model(&servers, model_name);
    drop(servers);

    let token_hashes = shared
        .get_token_hashes_for_prompt(model_name, prompt)
        .await?;
    let match_results = shared.search_tokens(token_hashes);
    tracing::info!(
        "select_server_by_kvcache: match_results={:?}",
        match_results
    );

    if match_results.is_empty() {
        return shared
            .inference_load_tracker
            .select_server_with_min_load(&candidates);
    }

    let candidate_ids: HashSet<u32> = candidates.iter().map(|s| s.id).collect();
    let best = match_results
        .into_iter()
        .filter(|(id, _)| candidate_ids.contains(id))
        .max_by_key(|(_, len)| *len);

    best.and_then(|(server_id, _)| {
        candidates.iter().find(|s| s.id == server_id).cloned()
    })
    .or_else(|| {
        shared
            .inference_load_tracker
            .select_server_with_min_load(&candidates)
    })
}
