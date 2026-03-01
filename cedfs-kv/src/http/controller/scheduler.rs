//! 负载均衡调度：按各推理实例当前未完成请求的 prompt 长度总和，将新请求调度到负载最小的实例

use crate::http::controller::inference_load_tracker::InferenceLoadTracker;
use crate::types::DataServer;
use std::sync::Arc;
use tokio::sync::RwLock;

/// 从 local_data_server_collect 中选出当前未完成推理请求长度总和最小的实例
///
/// - 若有 `model_filter`，优先只从 `model_name == model` 的节点中选；若无匹配则从全部节点中选
/// - 返回选中的 `(DataServer, server_key)`，便于调用方对 server_key 做 add/sub 负载
pub async fn select_server(
    local_data_server_collect: &Arc<RwLock<Vec<DataServer>>>,
    load_tracker: &InferenceLoadTracker,
    model_filter: Option<&str>,
) -> Option<DataServer> {
    let servers = local_data_server_collect.read().await;
    let candidates: Vec<DataServer> = if let Some(model) = model_filter {
        let filtered: Vec<DataServer> = servers
            .iter()
            .filter(|s| s.model_name == model)
            .cloned()
            .collect();
        if filtered.is_empty() {
            servers.clone()
        } else {
            filtered
        }
    } else {
        servers.clone()
    };
    drop(servers);
    load_tracker.select_server_with_min_load(&candidates)
}
